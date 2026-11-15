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

import org.apache.fluss.server.kv.dv.DerivedDvManager.RecoverySource;
import org.apache.fluss.server.kv.dv.DvChangeListSegment.UnreadableSegmentException;
import org.apache.fluss.server.kv.dv.DvOwner.TailDiskException;
import org.apache.fluss.server.kv.dv.DvRestoredChangeList.Reason;
import org.apache.fluss.server.kv.snapshot.DvChangeListHandle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

/**
 * Starts the replay of a full or tail load: from the change list the new leader restored with its
 * KV snapshot if the load may use it ({@link DvRestoredChangeList#decide}), else from the readable
 * offset. With the change list, the replay holds its changes from the readable offset o up to the
 * KV snapshot's offset X, and reads the log only from X, naming a key's first superseded version
 * after X from the view of the KV at X. A segment that cannot be read releases the change list, and
 * the load replays from o instead: it never fails for it. Reports how, for the rebuild's line.
 *
 * <p>Whichever way, the first change list a load installs releases the restored one: the bucket
 * replays its own from then on.
 */
final class DvCheckpointReplay {

    private static final Logger LOG = LoggerFactory.getLogger(DvCheckpointReplay.class);

    private final DvOwner m;
    @Nullable private DvRestoredChangeList restored;
    private Reason reason = Reason.NONE;
    private long changes;
    private int segments;
    private long bytes;
    private long loadNanos;
    @Nullable private DvReplay replay;

    DvCheckpointReplay(DvOwner m) {
        this.m = m;
    }

    /**
     * Starts the load's replay of the change list from {@code readableOffset}, on its job thread;
     * null if the load stopped.
     */
    @Nullable
    DvReplay start(RecoverySource src, long readableOffset, BooleanSupplier stopped) {
        restored = src.restoredChangeList();
        replay = restored == null ? null : fromCheckpoint(src, readableOffset, stopped);
        if (replay == null && !stopped.getAsBoolean()) {
            replay = DvReplay.start(m, src, readableOffset, stopped);
        }
        if (replay != null && restored != null) {
            DvReplay r = replay;
            DvRestoredChangeList used = reason == Reason.USED ? restored : null;
            List<DvChangeListHandle.Segment> loaded =
                    used == null ? new ArrayList<>() : restored.segmentsFrom(readableOffset);
            DvRestoredChangeList released = restored;
            r.onInstall =
                    () -> {
                        released.release(Reason.RELEASED);
                        if (used != null) {
                            m.metrics.rebuiltFromCheckpoint();
                            m.changeListExport.seed(r.tail, loaded, used.kvSnapshotOffset());
                        }
                    };
        }
        return replay;
    }

    @Nullable
    private DvReplay fromCheckpoint(
            RecoverySource src, long readableOffset, BooleanSupplier stopped) {
        DvRestoredChangeList d = restored;
        reason = d.decide(readableOffset, System.currentTimeMillis());
        if (reason != Reason.USED) {
            return null;
        }
        // the view does not expire while this load reads it
        DvKvView view = d.beginUse(System.currentTimeMillis());
        if (view == null) {
            Reason released = d.releasedAs();
            reason = released == null ? Reason.RELEASED : released;
            return null;
        }
        Runnable endUse = () -> d.endUse(System.currentTimeMillis());
        // a view that cannot be opened is released, and this load replays from o at once
        try {
            view.open(m.db.sharedBlockCache());
        } catch (IOException e) {
            endUse.run();
            reason = Reason.VIEW_FAILED;
            LOG.warn(
                    "Could not open the view of the KV restored with KV snapshot {} for the"
                            + " deletion vectors of {}; the change list is replayed from the"
                            + " readable offset.",
                    d.kvSnapshotId(),
                    m.logName,
                    e);
            d.release(Reason.VIEW_FAILED);
            return null;
        }
        long x = d.kvSnapshotOffset();
        DvReplay r;
        try {
            r = DvReplay.start(m, src, readableOffset, x, view, stopped);
        } catch (RuntimeException e) {
            endUse.run();
            throw e;
        }
        if (r == null) {
            endUse.run();
            return null;
        }
        r.onFinish = endUse;
        // a view that fails mid-replay is not used again: the retry replays from o
        r.onBaseFailure = () -> d.release(Reason.VIEW_FAILED);
        long start = System.nanoTime();
        try {
            load(r, readableOffset, x, stopped);
            loadNanos = System.nanoTime() - start;
            return r;
        } catch (UnreadableSegmentException e) {
            reason = Reason.CORRUPT;
            fallBack(r, e);
        } catch (IOException e) {
            reason = Reason.MISSING;
            fallBack(r, e);
        } catch (StoppedException e) {
            r.finish();
            return null;
        } catch (RuntimeException e) {
            // e.g. the change list's disk failed, or the bucket closes: the load fails, and the
            // replay it started is undone (its backlog, its change list)
            r.finish();
            throw e;
        }
        return null;
    }

    /** Drops the half-built change list and the restored one; the load replays from o. */
    private void fallBack(DvReplay r, IOException e) {
        LOG.warn(
                "Could not load the change list saved with KV snapshot {} for the deletion"
                        + " vectors of {} ({}); the change list is replayed from the readable"
                        + " offset.",
                restored.kvSnapshotId(),
                m.logName,
                reason.field,
                e);
        r.finish();
        restored.release(reason);
        changes = 0;
        segments = 0;
        bytes = 0;
    }

    /** Loads the segments' changes in [o, x) into the replay's change list. */
    private void load(DvReplay r, long o, long x, BooleanSupplier stopped) throws IOException {
        List<DvChangeList.Pending> batch = new ArrayList<>(DvChangeList.WRITE_BATCH);
        for (DvChangeListHandle.Segment segment : restored.segmentsFrom(o)) {
            if (stopped.getAsBoolean()) {
                throw new StoppedException();
            }
            try (InputStream in = restored.openSegment(segment)) {
                DvChangeListSegment.read(
                        in,
                        restored.expectedHeader(segment),
                        segment.file().getKvFileHandle().getSize(),
                        (offset, superseded, key) -> {
                            if (offset < o || offset >= x) {
                                return;
                            }
                            batch.add(new DvChangeList.Pending(offset, superseded, key));
                            if (batch.size() >= DvChangeList.WRITE_BATCH) {
                                append(r, batch, o, stopped);
                            }
                        });
            }
            segments++;
            bytes += segment.file().getKvFileHandle().getSize();
        }
        append(r, batch, o, stopped);
    }

    private void append(
            DvReplay r, List<DvChangeList.Pending> batch, long o, BooleanSupplier stopped) {
        if (batch.isEmpty()) {
            return;
        }
        if (stopped.getAsBoolean()) {
            throw new StoppedException();
        }
        m.leases.acquire();
        try {
            r.tail.appendCommitted(batch, o);
        } catch (IOException e) {
            // the change list's own disk, as when a replay writes it
            throw new TailDiskException(e);
        } finally {
            m.leases.release();
        }
        changes += batch.size();
        batch.clear();
    }

    /**
     * The checkpoint's fields of the rebuild's log line, for the replay {@code r} the load
     * installed.
     */
    String logFields(DvReplay r) {
        DvRestoredChangeList d = restored;
        DvChangeListHandle part = d == null ? null : d.changeList();
        boolean hasPart = part != null && part.isSupported();
        String fields =
                String.format(
                        "ckpt=%s ckpt_kv_snapshot=%s ckpt_from=%s ckpt_end=%s ckpt_changes=%d"
                                + " ckpt_segments=%d ckpt_bytes=%d ckpt_load_ms=%d replay_from=%d"
                                + " view_reads=%d view_ms=%d kv_view_ms=%d kv_view_held_ms=%d",
                        reason.field,
                        d == null || d.kvSnapshotId() < 0 ? "-" : d.kvSnapshotId(),
                        hasPart ? part.from() : "-",
                        hasPart ? part.end() : "-",
                        changes,
                        segments,
                        bytes,
                        loadNanos / 1_000_000,
                        r.resumedAt,
                        r.baseReads,
                        r.baseNanos / 1_000_000,
                        d == null ? -1 : d.kvViewMs(),
                        d == null ? -1 : d.viewHeldMs());
        m.lastRebuildCheckpointFields = fields;
        return fields;
    }

    /** The load stopped while it loaded the change list. */
    private static final class StoppedException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
