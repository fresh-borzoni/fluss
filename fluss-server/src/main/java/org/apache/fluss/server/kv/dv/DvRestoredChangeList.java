/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.kv.dv;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.DvChangeListHandle;
import org.apache.fluss.utils.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;

/**
 * The change list a new leader restored with its KV snapshot, and the view of the KV at the
 * snapshot's offset X, until a load uses them or they are released.
 *
 * <p>A load whose readable offset is o uses them only if the snapshot has a change list this build
 * reads, which ends at the snapshot's offset, the view is linked and not expired, and {@code
 * max(from, validFrom) <= o <= X} ({@link #decide}). Otherwise the load replays the change list
 * from o as without a checkpoint, and logs why. Tables with row TTL never use one: TTL compaction
 * may remove from the KV snapshot a version a later write supersedes, which only a full replay
 * names. Correctness never depends on the checkpoint.
 *
 * <p>Thread-safe: the bucket's loads, its close and the expiry timer call it.
 */
@Internal
public final class DvRestoredChangeList {

    private static final Logger LOG = LoggerFactory.getLogger(DvRestoredChangeList.class);

    /** How long the view is kept after its link or its last use, while no load uses it. */
    static final long VIEW_MAX_AGE_MS = TimeUnit.MINUTES.toMillis(15);

    /** Why a load uses the checkpoint, or why not; the {@code ckpt} field of the rebuild line. */
    enum Reason {
        USED("used"),
        // the server does not save or use change lists
        OFF("off"),
        // the table has row TTL
        TTL("ttl"),
        // the KV snapshot has no change list, or the bucket restored none
        NONE("none"),
        // the change list is of a version this build does not read
        VERSION("version"),
        // the change list does not end at the KV snapshot's offset
        END_MISMATCH("end_mismatch"),
        // the view of the restored KV could not be linked
        KV_VIEW("kv_view"),
        // no load used the view within VIEW_MAX_AGE_MS of its link or last use
        VIEW_EXPIRED("view_expired"),
        // the view could not be opened or read: the load replays from the readable offset
        VIEW_FAILED("view_failed"),
        // the readable offset is past the KV snapshot's: a replay from it is short anyway
        LAKE_AHEAD("lake_ahead"),
        // the readable offset is below what the change list covers exactly
        RANGE("range"),
        // a segment is gone, e.g. its snapshot was subsumed
        MISSING("missing"),
        // a segment is not readable: corrupt, or of an unknown version
        CORRUPT("corrupt"),
        // a change list was installed since: the bucket replays its own from now on
        RELEASED("released"),
        // the bucket closed
        CLOSED("closed");

        final String field;

        Reason(String field) {
            this.field = field;
        }
    }

    // names the bucket in log lines
    private final String bucket;
    // the bucket whose segments are read: their headers name it
    private final TableBucket tableBucket;
    private final long kvSnapshotId;
    private final long kvSnapshotOffset;
    @Nullable private final DvChangeListHandle changeList;
    // the reason a load cannot use it whatever its readable offset, or null
    @Nullable private final Reason fixedReason;
    private final long kvViewMs;
    @Nullable private DvKvView view;
    @Nullable private Reason releasedAs;
    // how long the view was held when it was released, or -1
    private long viewHeldMs = -1;
    // the loads reading the view now, and when the last one stopped (or the view was linked)
    private int viewUsers;
    private long viewLastUseMs;

    private DvRestoredChangeList(
            String bucket,
            TableBucket tableBucket,
            long kvSnapshotId,
            long kvSnapshotOffset,
            @Nullable DvChangeListHandle changeList,
            @Nullable Reason fixedReason,
            @Nullable DvKvView view,
            long kvViewMs) {
        this.bucket = bucket;
        this.tableBucket = tableBucket;
        this.kvSnapshotId = kvSnapshotId;
        this.kvSnapshotOffset = kvSnapshotOffset;
        this.changeList = changeList;
        this.fixedReason = fixedReason;
        this.view = view;
        this.kvViewMs = kvViewMs;
    }

    /**
     * What the new leader of {@code bucket} restored: the change list saved with KV snapshot {@code
     * kvSnapshotId} at {@code kvSnapshotOffset}, or null, and the view linked for it, or null if
     * none could be. The view is closed here at once if no load may use it.
     */
    static DvRestoredChangeList restored(
            String bucket,
            TableBucket tableBucket,
            boolean enabled,
            boolean rowTtl,
            long kvSnapshotId,
            long kvSnapshotOffset,
            @Nullable DvChangeListHandle changeList,
            @Nullable DvKvView view,
            long kvViewMs) {
        Reason fixed = fixedReason(enabled, rowTtl, changeList, kvSnapshotOffset, view != null);
        if (fixed != null && view != null) {
            view.close();
            view = null;
        }
        return new DvRestoredChangeList(
                bucket,
                tableBucket,
                kvSnapshotId,
                kvSnapshotOffset,
                changeList,
                fixed,
                view,
                kvViewMs);
    }

    /** Nothing restored: the leader restored no KV snapshot, or one without a change list. */
    static DvRestoredChangeList none(
            String bucket, TableBucket tableBucket, boolean enabled, boolean rowTtl) {
        return new DvRestoredChangeList(
                bucket,
                tableBucket,
                -1,
                -1,
                null,
                fixedReason(enabled, rowTtl, null, -1, false),
                null,
                -1);
    }

    // ---- what a new leader's replica calls, around the restore of its KV ----

    /** A view of the restored KV linked at the leader's start. */
    @Internal
    public static final class LinkedView {
        private final Path dir;
        private final long bytes;
        private final long linkedAtMs;
        private final long linkMs;

        private LinkedView(Path dir, long bytes, long linkedAtMs, long linkMs) {
            this.dir = dir;
            this.bytes = bytes;
            this.linkedAtMs = linkedAtMs;
            this.linkMs = linkMs;
        }

        /** Deletes the view's files, as the KV init that linked it failed before handing it on. */
        public void discard() {
            FileUtils.deleteDirectoryQuietly(dir.toFile());
        }
    }

    /** The directories of views under a replica's log directory start with this. */
    static final String VIEW_DIR_PREFIX = "deletion-vectors-kv-view-";

    /**
     * Links the view of the restored KV in {@code dbDir} if the change list of {@code snapshot} may
     * be used: call it after the snapshot is downloaded and before the KV opens it. Removes the
     * views earlier leaderships left under {@code logDir}. Returns null if the change list may not
     * be used or the view could not be linked; {@link #restored} then tells why.
     */
    @Nullable
    public static LinkedView linkIfUsable(
            boolean enabled,
            boolean rowTtl,
            @Nullable CompletedSnapshot snapshot,
            Path dbDir,
            File logDir) {
        File[] stale = logDir.listFiles((dir, name) -> name.startsWith(VIEW_DIR_PREFIX));
        if (stale != null) {
            for (File dir : stale) {
                FileUtils.deleteDirectoryQuietly(dir);
            }
        }
        if (snapshot == null
                || fixedReason(
                                enabled,
                                rowTtl,
                                snapshot.getKvSnapshotHandle().getDvChangeList(),
                                snapshot.getLogOffset(),
                                true)
                        != null) {
            return null;
        }
        long start = System.currentTimeMillis();
        Path dir =
                new File(
                                logDir,
                                VIEW_DIR_PREFIX
                                        + snapshot.getSnapshotID()
                                        + "-"
                                        + System.nanoTime())
                        .toPath();
        long bytes = DvKvView.link(dbDir, dir);
        long end = System.currentTimeMillis();
        return bytes < 0 ? null : new LinkedView(dir, bytes, end, end - start);
    }

    /**
     * What the new leader of {@code tableBucket}, named {@code bucket} in logs, restored with
     * {@code snapshot}, or with none, and the view {@link #linkIfUsable} linked for it, or null.
     * Its values' version offsets are read with {@code versionOffset}.
     */
    public static DvRestoredChangeList restored(
            String bucket,
            TableBucket tableBucket,
            boolean enabled,
            boolean rowTtl,
            @Nullable CompletedSnapshot snapshot,
            @Nullable LinkedView linked,
            ToLongFunction<byte[]> versionOffset) {
        if (snapshot == null) {
            if (linked != null) {
                FileUtils.deleteDirectoryQuietly(linked.dir.toFile());
            }
            return none(bucket, tableBucket, enabled, rowTtl);
        }
        return restored(
                bucket,
                tableBucket,
                enabled,
                rowTtl,
                snapshot.getSnapshotID(),
                snapshot.getLogOffset(),
                snapshot.getKvSnapshotHandle().getDvChangeList(),
                linked == null
                        ? null
                        : DvKvView.of(linked.dir, linked.bytes, linked.linkedAtMs, versionOffset),
                linked == null ? -1 : linked.linkMs);
    }

    @Nullable
    private static Reason fixedReason(
            boolean enabled,
            boolean rowTtl,
            @Nullable DvChangeListHandle changeList,
            long kvSnapshotOffset,
            boolean viewLinked) {
        if (!enabled) {
            return Reason.OFF;
        }
        if (rowTtl) {
            return Reason.TTL;
        }
        if (changeList == null) {
            return Reason.NONE;
        }
        if (!changeList.isSupported()) {
            return Reason.VERSION;
        }
        if (changeList.end() != kvSnapshotOffset) {
            return Reason.END_MISMATCH;
        }
        if (!viewLinked) {
            return Reason.KV_VIEW;
        }
        return null;
    }

    /**
     * The rule of {@link #decide} for a change list that passed the fixed checks: whether a load
     * with {@code readableOffset} may use the change list ending at {@code end}.
     */
    static Reason rangeRule(DvChangeListHandle changeList, long readableOffset) {
        if (readableOffset > changeList.end()) {
            return Reason.LAKE_AHEAD;
        }
        if (readableOffset < Math.max(changeList.from(), changeList.validFrom())) {
            return Reason.RANGE;
        }
        return Reason.USED;
    }

    /**
     * Whether a load whose readable offset is {@code readableOffset} uses the change list, at
     * {@code nowMs}. An expired view is released here.
     */
    synchronized Reason decide(long readableOffset, long nowMs) {
        if (fixedReason != null) {
            return fixedReason;
        }
        if (releasedAs != null) {
            return releasedAs;
        }
        if (expireIfDue(nowMs)) {
            return Reason.VIEW_EXPIRED;
        }
        return rangeRule(changeList, readableOffset);
    }

    /**
     * Releases the view if no load uses it and none did for {@link #VIEW_MAX_AGE_MS} since its link
     * or its last use; returns whether it did now. The expiry timer and {@link #decide} call it.
     */
    synchronized boolean expireIfDue(long nowMs) {
        if (view != null && viewUsers == 0 && nowMs - lastUse() >= VIEW_MAX_AGE_MS) {
            release(Reason.VIEW_EXPIRED);
            return true;
        }
        return false;
    }

    /** How long the expiry timer waits before it looks again, or -1 once the view is gone. */
    synchronized long nextExpiryCheckMs(long nowMs) {
        if (view == null) {
            return -1;
        }
        return viewUsers > 0 ? VIEW_MAX_AGE_MS : Math.max(1, lastUse() + VIEW_MAX_AGE_MS - nowMs);
    }

    private long lastUse() {
        return Math.max(viewLastUseMs, view == null ? 0 : view.linkedAtMs());
    }

    /**
     * A load starts reading the view, which does not expire until it {@link #endUse ends}; null if
     * the view is gone.
     */
    @Nullable
    synchronized DvKvView beginUse(long nowMs) {
        if (view == null) {
            return null;
        }
        viewUsers++;
        viewLastUseMs = nowMs;
        return view;
    }

    /** A load stopped reading the view. */
    synchronized void endUse(long nowMs) {
        viewUsers = Math.max(0, viewUsers - 1);
        viewLastUseMs = nowMs;
    }

    /** Why loads replay without the change list now, or null while one may use it. */
    @Nullable
    synchronized Reason releasedAs() {
        return fixedReason != null ? fixedReason : releasedAs;
    }

    /**
     * Releases the view and makes later loads replay without the change list, {@code why}: an
     * installed change list ({@link Reason#RELEASED}), expiry, or the bucket's close. Logs the
     * bytes the view held. Later calls do nothing.
     */
    synchronized void release(Reason why) {
        if (releasedAs != null || fixedReason != null) {
            return;
        }
        releasedAs = why;
        DvKvView v = view;
        view = null;
        if (v != null) {
            viewHeldMs = v.heldMs();
            LOG.info(
                    "Released the restored KV view of the deletion vectors of {}: kv_snapshot={}"
                            + " kv_offset={} reason={} bytes_held={} held_ms={}",
                    bucket,
                    kvSnapshotId,
                    kvSnapshotOffset,
                    why.field,
                    v.linkedBytes(),
                    v.heldMs());
            v.close();
        }
    }

    /** How long the view was held until its release, or -1 if none was released. */
    synchronized long viewHeldMs() {
        return viewHeldMs;
    }

    /** The header segment {@code s} of the change list must have. */
    DvChangeListSegment.Header expectedHeader(DvChangeListHandle.Segment s) {
        return new DvChangeListSegment.Header(
                tableBucket.getTableId(),
                tableBucket.getPartitionId() == null ? -1 : tableBucket.getPartitionId(),
                tableBucket.getBucket(),
                s.from(),
                s.end(),
                changeList == null ? 0 : changeList.validFrom());
    }

    /** Opens segment {@code s} in remote storage; the caller closes it. */
    InputStream openSegment(DvChangeListHandle.Segment s) throws IOException {
        FsPath path = new FsPath(s.file().getKvFileHandle().getFilePath());
        return path.getFileSystem().open(path);
    }

    /** The view of the KV at {@link #kvSnapshotOffset()}, while not released. */
    @Nullable
    synchronized DvKvView view() {
        return view;
    }

    /** The segments a load from {@code readableOffset} reads: those ending after it. */
    List<DvChangeListHandle.Segment> segmentsFrom(long readableOffset) {
        List<DvChangeListHandle.Segment> out = new ArrayList<>();
        if (changeList != null) {
            for (DvChangeListHandle.Segment s : changeList.segments()) {
                if (s.end() > readableOffset) {
                    out.add(s);
                }
            }
        }
        return out;
    }

    @Nullable
    DvChangeListHandle changeList() {
        return changeList;
    }

    long kvSnapshotId() {
        return kvSnapshotId;
    }

    /** The KV snapshot's log offset X: where a load that uses the change list replays from. */
    long kvSnapshotOffset() {
        return kvSnapshotOffset;
    }

    /** How long linking the view took at the leader's start, in ms, or -1. */
    long kvViewMs() {
        return kvViewMs;
    }
}
