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

import org.apache.fluss.exception.DeletionVectorsNotReadyException;
import org.apache.fluss.exception.InvalidRequestException;
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.server.utils.RoaringBitmapUtils;

import io.github.fluss_contrib.rocksdb.Snapshot;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.fluss.server.kv.dv.DvMaskReads.NOT_READY_WAIT_MS;
import static org.apache.fluss.server.kv.dv.DvOwner.RESOLVE_CHUNK;

/**
 * Tiering's reads of its rounds from the change list, on the request thread: their deletes and
 * their log masks. A round on the covered snapshot tiers the log from the readable offset o up to
 * its end e, which is fixed when the round is planned.
 *
 * <p>A round deletes the rows of the snapshot whose keys changed in [o, e), which the change list
 * names. A change whose superseded version is before o names its key's row in the snapshot, which
 * the covered version looks up; under row TTL an insert names one too.
 *
 * <p>A round's log mask is the offsets in [o, e) whose version a change below e supersedes; the
 * round writes the other versions only. A change at or after e marks nothing: the round does not
 * write the version it brings, so the version it replaces is still the key's latest at e. Under row
 * TTL the next write of a key names its version at or after o while o is at least the offset the
 * TTL filter was capped for ({@link DvRowTtlGuard}). On Iceberg o always is, and a bucket whose TTL
 * state is corrupt answers its rounds as not ready; on Paimon, which merges by key, a round below
 * it may come with a key twice.
 *
 * <p>An owner step checks that every change below e is in the change list on disk and takes a view
 * of it and the covered version. The request thread then reads the changes, and waits for no step
 * of the owner. A part reads at most {@link #PART_CHANGES} changes; a part of the deletes looks up
 * at most {@link #PART_LAKE_KEYS} keys, and one of the mask stops at about {@link #PART_MASK_BYTES}
 * serialized bytes. So a request takes bounded time and its answer has a bounded size; the next
 * part starts where it stopped.
 */
final class DvRoundReads {

    // A part looks up at most this many keys, which bounds the rows its answer carries.
    static final int PART_LAKE_KEYS = 100_000;
    // It reads at most this many changes: a change of a key the lake does not hold costs a step of
    // the iterator only.
    static final int PART_CHANGES = 1_000_000;
    // A part of the mask stops once its bitmap serializes to this many bytes, the default page of a
    // union read's log mask.
    static final int PART_MASK_BYTES = DvPinnedCut.DEFAULT_LOG_PAGE_BYTES;
    // how often a part that waits for the bucket asks again
    private static final long RECHECK_MS = 20;

    private final DvOwner m;
    private final int partLakeKeys;
    private final int partChanges;
    private final int partMaskBytes;

    DvRoundReads(DvOwner m) {
        this(m, PART_LAKE_KEYS, PART_CHANGES, PART_MASK_BYTES);
    }

    DvRoundReads(DvOwner m, int partLakeKeys, int partChanges, int partMaskBytes) {
        this.m = m;
        this.partLakeKeys = partLakeKeys;
        this.partChanges = partChanges;
        this.partMaskBytes = partMaskBytes;
    }

    /**
     * The part of the deletes of the round on {@code snapshotId} that ends at {@code end}, from
     * {@code from} on, or from the readable offset if null. Waits a moment for the bucket to switch
     * to the snapshot and to apply the changes below {@code end}.
     */
    DvRoundDeletes deletes(long snapshotId, @Nullable Long from, long end)
            throws IOException, StaleSnapshotException {
        return part(snapshotId, from, end, this::deriveDeletes);
    }

    /**
     * The part of the log mask of the round on {@code snapshotId} that ends at {@code end}, from
     * {@code from} on, or from the readable offset if null. Waits as {@link #deletes} does.
     */
    DvRoundMask mask(long snapshotId, @Nullable Long from, long end)
            throws IOException, StaleSnapshotException {
        return part(snapshotId, from, end, this::deriveMask);
    }

    private <T> T part(long snapshotId, @Nullable Long from, long end, PartReader<T> reader)
            throws IOException, StaleSnapshotException {
        long deadline = System.currentTimeMillis() + NOT_READY_WAIT_MS;
        while (true) {
            Views views;
            try {
                views = take(snapshotId, end, deadline);
            } catch (DeletionVectorsNotReadyException e) {
                if (System.currentTimeMillis() + RECHECK_MS >= deadline) {
                    throw e;
                }
                pause();
                continue;
            }
            try {
                long start = views.startOffset;
                long partFrom = from == null ? start : from;
                if (partFrom < start || partFrom > end) {
                    throw new InvalidRequestException(
                            String.format(
                                    "A part of the round [%d, %d) starts at offset %d.",
                                    start, end, partFrom));
                }
                return reader.read(views, partFrom, end);
            } finally {
                views.release();
            }
        }
    }

    /** Reads the changes of a part and looks up the rows of the snapshot they name. */
    private DvRoundDeletes deriveDeletes(Views views, long from, long end) throws IOException {
        long start = views.startOffset;
        DvLakeMasks masks = new DvLakeMasks();
        long next;
        try (DvChangeListStore.PendingCursor cursor =
                m.db.changeListStore()
                        .pendingCursor(views.changeListId, views.changeList, from, end)) {
            List<byte[]> keys = new ArrayList<>();
            int keysLeft = partLakeKeys;
            int changesLeft = partChanges;
            while (keysLeft > 0 && changesLeft > 0) {
                if (m.closed) {
                    throw DerivedDvManager.closedError();
                }
                int read =
                        cursor.nextLakeKeys(
                                Math.min(changesLeft, RESOLVE_CHUNK),
                                Math.min(keysLeft, RESOLVE_CHUNK),
                                start,
                                keys);
                if (read == 0) {
                    break;
                }
                changesLeft -= read;
                if (!keys.isEmpty()) {
                    keysLeft -= keys.size();
                    masks.addAll(views.version.positions(keys));
                    keys.clear();
                }
            }
            // a cursor that read every change below the round's end names no next one
            next = Math.min(cursor.nextOffset(), end);
        }
        masks.serializeDirty(Long.MAX_VALUE);
        Map<String, byte[]> deletes = new HashMap<>();
        for (Map.Entry<Integer, byte[]> e : masks.serialized().entrySet()) {
            String path = views.version.filePath(e.getKey());
            if (path != null) {
                deletes.put(path, e.getValue());
            }
        }
        return new DvRoundDeletes(deletes, start, next);
    }

    /** Reads the changes of a part and collects the versions in the log they supersede. */
    private DvRoundMask deriveMask(Views views, long from, long end) throws IOException {
        long start = views.startOffset;
        Roaring64Bitmap mask = new Roaring64Bitmap();
        long changes = 0;
        long next;
        try (DvChangeListStore.PendingCursor cursor =
                m.db.changeListStore()
                        .pendingCursor(views.changeListId, views.changeList, from, end)) {
            int changesLeft = partChanges;
            while (changesLeft > 0) {
                if (m.closed) {
                    throw DerivedDvManager.closedError();
                }
                int read =
                        cursor.nextLogVersions(Math.min(changesLeft, RESOLVE_CHUNK), start, mask);
                if (read == 0) {
                    break;
                }
                changes += read;
                changesLeft -= read;
                if (mask.serializedSizeInBytes() >= partMaskBytes) {
                    break;
                }
            }
            // a cursor that read every change below the round's end names no next one
            next = Math.min(cursor.nextOffset(), end);
        }
        return new DvRoundMask(
                mask.isEmpty() ? null : RoaringBitmapUtils.serializeRoaringBitmap64(mask),
                start,
                next,
                changes,
                m.rowTtl);
    }

    /** Takes the views of a part in an owner step, waiting for it until {@code deadline}. */
    private Views take(long snapshotId, long end, long deadline)
            throws IOException, StaleSnapshotException {
        CompletableFuture<Views> taken = new CompletableFuture<>();
        try {
            m.post(() -> takeStep(snapshotId, end, taken));
        } catch (RejectedExecutionException e) {
            throw DerivedDvManager.closedError();
        }
        try {
            return taken.get(
                    Math.max(1, deadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw rethrow(e.getCause());
        } catch (TimeoutException e) {
            taken.completeExceptionally(
                    new DeletionVectorsNotReadyException(
                            "The deletion vectors of the bucket are busy; retry later."));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            taken.completeExceptionally(
                    new InterruptedIOException("Interrupted while waiting for deletion vectors"));
        }
        // the step may have taken the views meanwhile
        try {
            return taken.join();
        } catch (CompletionException e) {
            throw rethrow(e.getCause());
        }
    }

    /** Takes the views of a part, unless the request gave up; an owner step. */
    private void takeStep(long snapshotId, long end, CompletableFuture<Views> taken) {
        if (taken.isDone()) {
            return;
        }
        try {
            Views views = views(snapshotId, end);
            if (!taken.complete(views)) {
                views.release();
            }
        } catch (Exception e) {
            taken.completeExceptionally(e);
        }
    }

    /**
     * Takes the views a part reads once every change of the covered snapshot below {@code end} is
     * in the change list on disk; an owner step. Starts the switch or the replay the part needs.
     */
    private Views views(long snapshotId, long end) throws StaleSnapshotException {
        if (m.closed) {
            throw DerivedDvManager.closedError();
        }
        if (m.recoverySource == null) {
            throw new DeletionVectorsNotReadyException(
                    String.format(
                            "Deletion vectors of this bucket are not recovered yet; lake"
                                    + " snapshot %d is requested.",
                            snapshotId));
        }
        if (m.coveredSnapshotId != snapshotId) {
            if (snapshotId < 0 || m.passed(snapshotId) || m.unreachable == snapshotId) {
                throw m.stale(snapshotId);
            }
            if (m.planner.scheduleAdvance(snapshotId) < 0) {
                throw m.planner.advanceFailed(snapshotId);
            }
            throw new DeletionVectorsNotReadyException(
                    String.format(
                            "Deletion vectors cover lake snapshot %d and are switching to"
                                    + " snapshot %d.",
                            m.coveredSnapshotId, snapshotId));
        }
        long start = m.snapshotStartOffset;
        if (end < start) {
            throw new InvalidRequestException(
                    String.format(
                            "A round on lake snapshot %d ends at offset %d, before its readable"
                                    + " offset %d.",
                            snapshotId, end, start));
        }
        m.drain.drainHandOff();
        m.maskState.applyUpTo(m.highWatermark.getAsLong());
        // the drain may have lost the change list, and the apply may have dropped it
        if (m.maskState.tailMissing()) {
            String corruptRowTtl = m.rowTtlGuard.corruption(start);
            if (corruptRowTtl != null) {
                // no replay serves the change list: the round waits
                throw new DeletionVectorsNotReadyException(corruptRowTtl);
            }
            if (!m.rebuild.retryPending()) {
                m.rebuild.scheduleReplay(snapshotId);
            }
            throw new DeletionVectorsNotReadyException(
                    String.format(
                            "Deletion vectors of lake snapshot %d are replaying the log from"
                                    + " offset %d.",
                            snapshotId, start));
        }
        if (m.handedOffEnd < end || m.maskState.tail.hasBelow(end)) {
            throw new DeletionVectorsNotReadyException(
                    String.format(
                            "Deletion vectors of lake snapshot %d have not applied the changes"
                                    + " up to offset %d yet.",
                            snapshotId, end));
        }
        DvVersion version = m.retainForJob();
        if (version == null) {
            throw DerivedDvManager.closedError();
        }
        if (!m.leases.tryAcquire()) {
            version.release();
            throw DerivedDvManager.closedError();
        }
        return new Views(start, version, m.db.snapshot(), m.maskState.tail.id);
    }

    private static void pause() throws InterruptedIOException {
        try {
            Thread.sleep(RECHECK_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("Interrupted while waiting for deletion vectors");
        }
    }

    private static IOException rethrow(Throwable cause) throws StaleSnapshotException {
        if (cause instanceof StaleSnapshotException) {
            throw (StaleSnapshotException) cause;
        }
        if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        if (cause instanceof IOException) {
            return (IOException) cause;
        }
        return new IOException(cause);
    }

    /** Reads a part from the views an owner step took. */
    @FunctionalInterface
    private interface PartReader<T> {
        T read(Views views, long from, long end) throws IOException;
    }

    /** What a part reads: the covered version and a view of the change list. */
    private final class Views {
        final long startOffset;
        final DvVersion version;
        // holds a lease of the RocksDB
        final Snapshot changeList;
        final long changeListId;

        Views(long startOffset, DvVersion version, Snapshot changeList, long changeListId) {
            this.startOffset = startOffset;
            this.version = version;
            this.changeList = changeList;
            this.changeListId = changeListId;
        }

        void release() {
            m.db.releaseSnapshot(changeList);
            m.leases.release();
            version.release();
        }
    }
}
