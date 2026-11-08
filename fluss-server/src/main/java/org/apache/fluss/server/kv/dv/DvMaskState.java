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

import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.server.kv.dv.DvFrontiers.KeepReady;
import org.apache.fluss.server.kv.dv.DvOwner.TailDiskException;
import org.apache.fluss.server.utils.RoaringBitmapUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.server.kv.dv.DvOwner.RESOLVE_CHUNK;

/**
 * The mask state of the version a bucket covers: its change list, the lake masks derived from it,
 * the keys waiting for their lake positions, and what is derived again. It applies the changes the
 * high watermark passed, looks the keys up on the key-lookup threads a chunk at a time, and builds
 * frontiers once the masks are exact up to a cut.
 *
 * <p>The state is the owner's ({@link DvOwner}): only its steps read and write it, and its methods
 * run as owner steps unless their javadoc names another thread. A job names the epoch of the state
 * it was computed for, and the owner drops the results of an epoch that was renewed since, as the
 * version, the change list or the masks were replaced.
 */
final class DvMaskState {

    private static final Logger LOG = LoggerFactory.getLogger(DvMaskState.class);

    /** The identity of a mask state: renewed whenever its masks are derived again. */
    static final class Epoch {}

    private final DvOwner o;

    DvMaskState(DvOwner o) {
        this.o = o;
        this.tail = newTail();
    }

    /**
     * Starts a new epoch: results of jobs of the one before are dropped. The frontier of the state
     * before is kept as {@code keep} allows.
     */
    void renew(KeepReady keep) {
        // a build in progress ends: the positions it held back join the masks the renewal starts
        // from
        endBuild();
        o.frontiers.newState(keep, o.coveredSnapshotId, System.nanoTime());
        epoch = new Epoch();
        fromRebuild = false;
    }

    // ---- bounded steps ----

    // the most queued changes a step applies; the rest are applied in the steps after it
    static final int APPLY_CHUNK = 8_192;
    // about the most serialized bitmap bytes a step of a frontier build produces
    static final long BUILD_CHUNK_BYTES = 4L << 20;
    // as above; tests make them small, so applies and builds take many steps
    int applyChunk = APPLY_CHUNK;
    long buildChunkBytes = BUILD_CHUNK_BYTES;

    /** A frontier build that takes more than one step. */
    private static final class MaskBuild {
        final long cut;
        final long startNanos;

        MaskBuild(long cut, long startNanos) {
            this.cut = cut;
            this.startNanos = startNanos;
        }
    }

    // A frontier build in progress. The masks stay as they are at its cut until it ends: changes
    // are not applied, and the positions lookups find are held back.
    @Nullable private MaskBuild building;
    private final List<FilePos> heldBack = new ArrayList<>();
    private boolean applyScheduled;
    // the serialized dead versions of a change list, and the change of them it is of
    @Nullable private byte[] deadVersionBytes;
    private long deadVersionsOf = -1;
    private long deadVersionChangesAt = -1;

    /** Adds the positions a lookup found, or holds them back while a build is in progress. */
    private void merge(List<FilePos> found) {
        if (building != null) {
            heldBack.addAll(found);
        } else {
            lakeMasks.addAll(found);
        }
    }

    /** Ends the build in progress, if any: the positions held back join the masks. */
    private void endBuild() {
        if (building == null) {
            return;
        }
        building = null;
        lakeMasks.addAll(heldBack);
        heldBack.clear();
        if (!tail.queue.isEmpty() || !unresolved.isEmpty()) {
            scheduleResolve(0);
        }
    }

    /** Applies the next chunk of the queued changes. */
    private void applyStep() {
        applyScheduled = false;
        if (o.closed) {
            return;
        }
        applyUpTo(o.highWatermark.getAsLong());
        if (!unresolved.isEmpty()) {
            scheduleResolve(0);
        }
        refreshLatest();
    }

    /**
     * Whether the change list may switch: no build is in progress and no change below {@code hw} is
     * queued. Applies the next chunk otherwise.
     */
    boolean readyToSwitch(long hw) {
        if (building != null) {
            return false;
        }
        applyUpTo(hw);
        return building == null && !tail.hasBelow(hw);
    }

    /**
     * Applies every queued change below {@code hw}, however many: for the swap of a switch, which
     * {@link #readyToSwitch} left a chunk at most, and for tests. Returns the nanoseconds it took.
     */
    long applyAll(long hw) {
        if (building != null) {
            finishBuild();
        }
        long start = System.nanoTime();
        try {
            tail.applyUpTo(hw, o.snapshotStartOffset, o.coveredSnapshotId >= 0 ? unresolved : null);
            appliedCut = Math.max(appliedCut, o.servableAt(hw));
        } catch (IOException e) {
            tailDiskFailed(e);
        }
        return System.nanoTime() - start;
    }

    /** Ends the build in progress in this step, however large. */
    private void finishBuild() {
        MaskBuild b = building;
        try {
            lakeMasks.serializeDirty(Long.MAX_VALUE);
            publishFrontier(b.cut);
        } catch (IOException | RuntimeException e) {
            LOG.warn("Building the deletion vector masks of {} failed.", o.logName, e);
        }
        endBuild();
    }

    /** The next step of the build in progress. */
    private void continueBuild() {
        MaskBuild b = building;
        if (b == null) {
            return; // renewed meanwhile
        }
        if (o.closed) {
            endBuild();
            return;
        }
        try {
            if (!lakeMasks.serializeDirty(buildChunkBytes)) {
                o.post(this::continueBuild);
                return;
            }
            publishFrontier(b.cut);
            LOG.debug(
                    "Building the deletion vector masks of {} at offset {} took {} ms in"
                            + " steps.",
                    o.logName,
                    b.cut,
                    (System.nanoTime() - b.startNanos) / 1_000_000);
        } catch (IOException | RuntimeException e) {
            LOG.warn("Building the deletion vector masks of {} failed.", o.logName, e);
        }
        endBuild();
    }

    // Reports of the tail's memory between two measurements of its bitmaps.
    static final int BITMAP_MEASURE_INTERVAL = 64;
    // set when the covered snapshot's index is in place but its log tail is not; masks without
    // the tail would let old versions through, so union reads fail until a replay succeeds
    @Nullable Exception tailFailure;

    // The tail of the covered snapshot, in the bucket's RocksDB. Replaced as a whole when a replay
    // installs a new tail.
    DvChangeList tail;
    // The tail's masks are new: the lake positions of its keys are looked up from disk in the
    // background before masks are served.
    boolean fullResolvePending = false;
    boolean fullResolveScheduled = false;
    // A full resolve reads the changes below this offset, the ones applied when the masks became
    // new. The keys of the changes applied since go to the unresolved ones as they are applied, so
    // reading them from disk again would only look them up twice.
    long fullResolveTo = 0;
    // where the next chunk of a full resolve starts reading the changes
    long fullResolveNext = 0;
    // what was last added to the budget and the server's metrics
    long reportedMemory = 0;
    long reportedStoredBytes = 0;
    // mask bitmap bytes; measuring takes a pass over the bitmaps, so it runs every few reports
    long bitmapBytes = 0;
    long deadVersionHeapBytes = 0;
    int reportsSinceBitmaps = 0;
    // Every change below it is applied. Masks built while no lookup is pending cover the log up to
    // it exactly.
    long appliedCut = -1;

    final DvUnresolvedKeys unresolved = new DvUnresolvedKeys();
    final DvLakeMasks lakeMasks = new DvLakeMasks();
    // A switch to a newer snapshot keeps the lake masks of the keys its index changes do not name
    // and
    // derives the others again. Meanwhile no masks are served and no lookups start, as it removes
    // the positions those keys' rows had.
    boolean targetedPending = false;
    // the chunks of keys resolves look up on the key-lookup threads; a switch looks their keys up
    // again
    final Set<List<ByteBuffer>> resolving = Collections.newSetFromMap(new IdentityHashMap<>());

    static final long RESOLVE_DELAY_MS = 2;
    static final long RESOLVE_WAIT_HW_MS = 20;
    boolean resolveScheduled = false;
    // a background resolve's chunk is looked up: the next one starts once it is in
    boolean chunkInFlight = false;
    // the epoch of the masks: jobs name it, and their results are dropped once it is renewed
    Epoch epoch = new Epoch();
    // the masks are a rebuild's or a replay's, installed with this epoch: reads that do not wait
    // do not wait for their lookups
    boolean fromRebuild = false;

    /** A new, empty tail in the bucket's RocksDB. */
    DvChangeList newTail() {
        return new DvChangeList(o.db.changeListStore(), o.tailIds.incrementAndGet(), o.testHooks);
    }

    /**
     * Drops the tail after its disk failed, e.g. on a write of the tail. No masks are served and
     * union reads are answered as not ready until the tail is replayed from the log after a
     * backoff.
     */
    void tailDiskFailed(IOException e) {
        LOG.warn(
                "The deletion vector change list of snapshot {} could not be kept on"
                        + " disk; it is dropped and replayed from the log.",
                o.coveredSnapshotId,
                e);
        renew(KeepReady.SAME_SNAPSHOT);
        resetTail();
        tailFailure = new TailDiskException(e);
        o.rebuild.retryAfterDiskFailure();
        reportTail(true);
    }

    /** Whether union reads of the covered snapshot need a log replay first. */
    boolean tailMissing() {
        return tailFailure != null || (o.coveredSnapshotId < 0 && !o.emptyLakeTracked);
    }

    void scheduleResolve() {
        scheduleResolve(RESOLVE_DELAY_MS);
    }

    /** Schedules a resolve step after {@code delayMs}. */
    void scheduleResolve(long delayMs) {
        if (!resolveScheduled) {
            resolveScheduled = true;
            try {
                o.runtime
                        .scheduler()
                        .schedule(() -> o.post(this::resolveStep), delayMs, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                // the server shuts down; the next change tries again
                resolveScheduled = false;
                LOG.warn("A deletion vector key lookup could not be scheduled ({}).", e.toString());
            }
        }
    }

    /**
     * Applies what the high watermark passed and starts a lookup of the next chunk of keys whose
     * lake positions the masks need. One chunk is looked up at a time, each in a task of its own on
     * the key-lookup threads, which the server's buckets share.
     */
    void resolveStep() {
        resolveScheduled = false;
        if (o.closed) {
            return;
        }
        if (o.testHooks.pausesResolves()) {
            scheduleResolve(RESOLVE_WAIT_HW_MS);
            return;
        }
        o.drain.drainHandOff();
        applyUpTo(o.highWatermark.getAsLong());
        if (targetedPending) {
            return; // the switch's derivation schedules a resolve when it is done
        }
        if (chunkInFlight) {
            return; // the chunk in flight schedules the next one
        }
        if (unresolved.isEmpty()) {
            if (!tail.queue.isEmpty()) {
                // The high watermark has not passed the queue; back off rather than spin.
                scheduleResolve(RESOLVE_WAIT_HW_MS);
            }
            refreshLatest();
            return;
        }
        DvVersion v = o.retainForJob();
        if (v == null) {
            return;
        }
        List<ByteBuffer> chunk = unresolved.take(RESOLVE_CHUNK);
        Epoch jobEpoch = epoch;
        o.inFlight++;
        chunkInFlight = true;
        resolving.add(chunk);
        try {
            o.runtime.keyLookup().execute(() -> lookUpChunk(v, chunk, jobEpoch));
        } catch (RejectedExecutionException e) {
            // the server shuts down
            v.release();
            resolving.remove(chunk);
            o.inFlight--;
            chunkInFlight = false;
            unresolved.addAll(chunk);
        }
    }

    /** Looks a chunk of keys up in the covered version, on a resolver thread. */
    void lookUpChunk(DvVersion v, List<ByteBuffer> chunk, Epoch jobEpoch) {
        List<FilePos> found = null;
        Exception failure = null;
        Throwable error = null;
        boolean released = false;
        try {
            o.testHooks.beforeResolveLookup();
            if (!o.closed) {
                List<byte[]> keys = new ArrayList<>(chunk.size());
                for (ByteBuffer k : chunk) {
                    keys.add(k.array());
                }
                released = true;
                found = lookUp(v, keys);
                o.lookedUpKeys.addAndGet(keys.size());
            }
        } catch (IOException | RuntimeException e) {
            failure = e;
        } catch (Throwable t) {
            // the keys are looked up again, as after any failed chunk
            failure = o.faults.jobError(t, "a lookup of lake positions");
            error = t;
        } finally {
            if (!released) {
                v.release();
            }
        }
        List<FilePos> positions = found;
        Exception f = failure;
        o.post(() -> chunkResolved(chunk, jobEpoch, positions, f));
        if (error != null) {
            DvOwnerFaults.rethrowIfFatal(error);
        }
    }

    /**
     * Adds the lake positions of a chunk to the masks, unless the mask state moved on meanwhile. A
     * failed chunk's keys go back to the unresolved ones: looking a key up twice only sets its bit
     * again.
     */
    void chunkResolved(
            List<ByteBuffer> chunk,
            Epoch jobEpoch,
            @Nullable List<FilePos> found,
            @Nullable Exception failure) {
        resolving.remove(chunk);
        o.inFlight--;
        chunkInFlight = false;
        if (o.closed) {
            return;
        }
        if (jobEpoch == epoch) {
            if (found != null) {
                merge(found);
            } else if (failure != null) {
                LOG.warn(
                        "A deletion vector key lookup failed; its keys are looked up again",
                        failure);
                unresolved.addAll(chunk);
            }
        }
        reportTail(false);
        refreshLatest();
        if (!tail.queue.isEmpty() || !unresolved.isEmpty()) {
            scheduleResolve(failure == null ? 0 : RESOLVE_WAIT_HW_MS);
        }
    }

    /**
     * Looks keys up in a version of the index, on a job thread. A load that ingests the next
     * snapshot's changes meanwhile does not show through.
     *
     * @throws DvDbLeases.DvClosedException if the bucket closed
     */
    static List<FilePos> lookUp(DvVersion v, List<byte[]> keys) throws IOException {
        try {
            return v.positions(keys);
        } finally {
            v.release();
        }
    }

    /**
     * Applies the queued changes below {@code hw}, a chunk of {@link #applyChunk} at most; the rest
     * are applied in the steps after. Applies nothing while a frontier build is in progress, which
     * needs the masks as they are. Returns the nanoseconds it took.
     */
    long applyUpTo(long hw) {
        if (building != null) {
            return 0;
        }
        long start = System.nanoTime();
        try {
            // the empty lake has no positions to look up
            boolean more =
                    tail.applyUpTo(
                            hw,
                            o.snapshotStartOffset,
                            o.coveredSnapshotId >= 0 ? unresolved : null,
                            applyChunk);
            // the queue holds only changes handed over; all of them below this are applied now
            long applied = o.servableAt(hw);
            if (more) {
                applied = Math.min(applied, tail.queue.peekFirst().getRecordOffset());
                if (!applyScheduled) {
                    applyScheduled = true;
                    o.post(this::applyStep);
                }
            }
            appliedCut = Math.max(appliedCut, applied);
        } catch (IOException e) {
            tailDiskFailed(e);
        }
        return System.nanoTime() - start;
    }

    /**
     * Makes a complete replay's tail, which read the log up to {@code replayedTo}, the tail of the
     * covered snapshot.
     */
    void installTail(DvChangeList replayed, long replayedTo) {
        dropTail();
        tail = replayed;
        tailFailure = null;
        o.rebuild.replayInstalled();
        lakeMasks.clear();
        unresolved.clear();
        fullResolvePending = false;
        targetedPending = false;
        // the replay read the log up to here, so the tail has every change below it
        o.handedOffEnd = Math.max(o.handedOffEnd, replayedTo);
        // nothing of the new list is applied yet: its queue starts back at the readable offset
        appliedCut = -1;
        applyUpTo(o.highWatermark.getAsLong());
        if (o.coveredSnapshotId >= 0 && !tailMissing()) {
            // the empty lake has no positions to look up; the tail's keys are looked up from disk,
            // in chunks, before masks are served
            unresolved.clear();
            resolveWholeTail();
        }
        reportTail(true);
        scheduleResolve();
    }

    /**
     * Reports the bucket's heap bytes (queued changes, keys waiting for lake positions, mask
     * bitmaps, masks kept for reads that do not wait) to the tablet server's budget and metrics,
     * and the bytes of the tail on disk to the metrics. The bitmaps are measured every few reports,
     * or now if {@code measureBitmaps}.
     */
    void reportTail(boolean measureBitmaps) {
        if (measureBitmaps || ++reportsSinceBitmaps >= BITMAP_MEASURE_INTERVAL) {
            reportsSinceBitmaps = 0;
            deadVersionHeapBytes = tail.deadVersions.getLongSizeInBytes();
            lakeMasks.measure();
        }
        // the masks of a file are measured again as they are serialized, so a build counts them
        bitmapBytes = deadVersionHeapBytes + lakeMasks.heapBytes();
        long now = System.nanoTime();
        o.frontiers.dropIfUnused(o.closed, now);
        if (building == null && !o.frontiers.readsCameWithin(now)) {
            // no union read comes: the serialized masks go with the frontiers
            lakeMasks.forgetSerialized();
            deadVersionBytes = null;
            deadVersionsOf = -1;
        }
        // the latest frontier shares its arrays with the serialized masks
        long serialized =
                Math.max(
                        lakeMasks.serializedBytes(),
                        o.frontiers.heapBytes() - o.frontiers.keptBytes());
        long memory =
                o.closed
                        ? 0
                        : tail.heapBytes()
                                + bitmapBytes
                                + unresolvedBytes()
                                + serialized
                                + o.frontiers.keptBytes();
        long storedBytes = o.closed ? 0 : tail.storedBytes();
        o.metrics.heapBytesChanged(memory - reportedMemory);
        o.metrics.changeListBytesChanged(storedBytes - reportedStoredBytes);
        reportedMemory = memory;
        reportedStoredBytes = storedBytes;
        // only the kept masks can be dropped to make room
        o.heapBudget.update(o.tailOwner, memory, o.closed ? 0 : o.frontiers.keptBytes());
    }

    /**
     * Estimated heap bytes of the keys waiting for their lake positions: each key's bytes, its
     * wrapper and its list slot, as the change that named the key is applied and gone. They stay in
     * memory until a key lookup, a few milliseconds behind the writes, looks them up, or longer
     * while a switch derives its masks again.
     */
    long unresolvedBytes() {
        return unresolved.bytes();
    }

    /** Removes the tail from disk, unless the bucket closed. */
    void dropTail() {
        if (!o.closed) {
            try {
                tail.drop();
            } catch (IOException e) {
                // cleared when the RocksDB opens again
                LOG.warn("Could not drop a deletion vector change list.", e);
            }
        }
    }

    /** Drops the tail and the masks derived from it. */
    void resetTail() {
        dropTail();
        tail = newTail();
        appliedCut = -1;
        lakeMasks.clear();
        unresolved.clear();
        fullResolvePending = false;
        targetedPending = false;
    }

    /**
     * Starts looking up the lake positions of the keys of the tail, whose masks are new: installed,
     * or advanced without keeping them. Only the changes applied so far are read from disk, in the
     * background; the keys of later ones reach the masks as they are applied, so each key is looked
     * up once. A tail with no change needs no lookup.
     */
    void resolveWholeTail() {
        if (tail.changeCount() == 0) {
            return;
        }
        fullResolvePending = true;
        fullResolveTo = tail.lastApplied() + 1;
        fullResolveNext = 0;
        scheduleFullResolve();
    }

    /**
     * Looks up the lake positions of the keys of the tail whose masks are new in the background,
     * reading a chunk of changes from disk at a time. Masks are served once it is done. An owner
     * step.
     */
    void scheduleFullResolve() {
        scheduleFullResolve(0);
    }

    void scheduleFullResolve(long delayMs) {
        if (fullResolveScheduled) {
            return;
        }
        // the version whose positions the masks of this state are
        DvVersion v = o.retainForJob();
        if (v == null) {
            return;
        }
        fullResolveScheduled = true;
        final Epoch jobEpoch = epoch;
        final long tailId = tail.id;
        final long startOffset = o.snapshotStartOffset;
        final long toOffset = fullResolveTo;
        final long fromOffset = fullResolveNext;
        o.inFlight++;
        try {
            o.runtime.executeAfter(
                    delayMs,
                    o.runtime.keyLookup(),
                    () -> fullResolve(v, jobEpoch, tailId, startOffset, fromOffset, toOffset),
                    () -> {
                        // the server shuts down; the bucket is closed
                        v.release();
                        LOG.warn("A full deletion vector key lookup could not start.");
                    });
        } catch (RejectedExecutionException e) {
            v.release();
            o.inFlight--;
            fullResolveScheduled = false;
            LOG.warn(
                    "A full deletion vector key lookup could not be scheduled ({}).", e.toString());
        }
    }

    /**
     * Looks up the lake keys of one chunk of the changes in [{@code fromOffset}, {@code toOffset}),
     * on a resolver thread, and posts the positions to the owner, which schedules the next chunk,
     * so a long change list does not hold a resolver thread, which the server's buckets share, for
     * its whole run.
     */
    void fullResolve(
            DvVersion v,
            Epoch jobEpoch,
            long tailId,
            long startOffset,
            long fromOffset,
            long toOffset) {
        boolean done = false;
        boolean paused = false;
        boolean released = false;
        long next = fromOffset;
        Exception failure = null;
        Throwable error = null;
        List<FilePos> found = null;
        try {
            if (o.closed) {
                return;
            }
            if (o.testHooks.pausesResolves()) {
                paused = true;
                return;
            }
            List<byte[]> keys = new ArrayList<>();
            if (!o.leases.tryAcquire()) {
                return;
            }
            try (DvChangeListStore.PendingCursor cursor =
                    o.db.changeListStore().pendingCursor(tailId, null, fromOffset, toOffset)) {
                done = !cursor.nextLakeKeys(RESOLVE_CHUNK, startOffset, keys);
                next = cursor.nextOffset();
            } finally {
                o.leases.release();
            }
            done |= next == Long.MAX_VALUE;
            if (keys.isEmpty()) {
                return; // superseded versions in the log only
            }
            o.testHooks.beforeResolveLookup();
            released = true;
            found = lookUp(v, keys);
            o.lookedUpKeys.addAndGet(keys.size());
        } catch (IOException | RuntimeException e) {
            failure = e;
        } catch (Throwable t) {
            failure = o.faults.jobError(t, "a full resolve");
            error = t;
        } finally {
            if (!released) {
                v.release();
            }
            boolean d = done;
            boolean p = paused;
            long n = next;
            Exception f = failure;
            List<FilePos> positions = found;
            o.post(() -> fullChunkResolved(jobEpoch, positions, d, p, n, f));
        }
        if (error != null) {
            DvOwnerFaults.rethrowIfFatal(error);
        }
    }

    /** Merges a chunk of a full resolve and schedules the next one. */
    void fullChunkResolved(
            Epoch jobEpoch,
            @Nullable List<FilePos> found,
            boolean done,
            boolean paused,
            long next,
            @Nullable Exception failure) {
        o.inFlight--;
        fullResolveScheduled = false;
        if (jobEpoch == epoch && !o.closed) {
            if (found != null) {
                merge(found);
            }
            if (failure != null) {
                tailDiskFailed(
                        failure instanceof IOException
                                ? (IOException) failure
                                : new IOException(
                                        "A full deletion vector key lookup failed", failure));
            } else if (done) {
                fullResolvePending = false;
                reportTail(true);
            } else {
                fullResolveNext = next;
            }
        }
        if (fullResolvePending && !o.closed) {
            // the next chunk, once paused resolves resume, or a resolve of masks that
            // became new again while this one ran
            scheduleFullResolve(paused ? RESOLVE_WAIT_HW_MS : 0);
        }
        refreshLatest();
    }

    /**
     * Builds the masks of the covered snapshot up to {@code hw}, which must be exact up to there,
     * and makes them the latest frontier of the covered state. Only the files whose masks changed
     * since the last build are serialized. If they are more than a step serializes, the build goes
     * on in the steps after this one and returns null: its frontier then answers the reads, as the
     * latest one. Null also while another build is in progress.
     */
    @Nullable
    DvSnapshot buildMasks(long hw) throws IOException {
        if (building != null) {
            return null;
        }
        if (!lakeMasks.serializeDirty(buildChunkBytes)) {
            building = new MaskBuild(hw, System.nanoTime());
            o.post(this::continueBuild);
            return null;
        }
        return publishFrontier(hw);
    }

    /** Makes the serialized masks at {@code cut} the latest frontier. */
    private DvSnapshot publishFrontier(long cut) throws IOException {
        Map<String, byte[]> resolved = new HashMap<>();
        for (Map.Entry<Integer, byte[]> e : lakeMasks.serialized().entrySet()) {
            int fileId = e.getKey();
            String path = lakeMasks.path(fileId);
            if (path == null) {
                path = o.version.filePath(fileId);
                if (path == null) {
                    continue;
                }
                lakeMasks.cachePath(fileId, path);
            }
            resolved.put(path, e.getValue());
        }
        if (deadVersionsOf != tail.id || deadVersionChangesAt != tail.deadVersionChanges) {
            deadVersionBytes =
                    tail.deadVersions.isEmpty()
                            ? null
                            : RoaringBitmapUtils.serializeRoaringBitmap64(tail.deadVersions);
            deadVersionHeapBytes = tail.deadVersions.getLongSizeInBytes();
            deadVersionsOf = tail.id;
            deadVersionChangesAt = tail.deadVersionChanges;
        }
        // Masks cover exactly [snapshotStartOffset, cut), so the reader must read the log up to it.
        DvSnapshot masks =
                new DvSnapshot(
                        resolved,
                        deadVersionBytes,
                        cut,
                        o.snapshotStartOffset,
                        o.coveredIdentity,
                        o.highWatermark.getAsLong());
        o.frontiers.built(new DvFrontier(o.coveredSnapshotId, masks, System.nanoTime()));
        reportTail(false);
        return masks;
    }

    /**
     * Rebuilds the latest frontier once lookups drained, while reads that do not wait come: at most
     * every {@link DvOwner#readyRefreshNanos}, and only if more changes were applied. An owner
     * step.
     */
    void refreshLatest() {
        long now = System.nanoTime();
        if (o.closed
                || o.coveredSnapshotId < 0
                || !o.frontiers.readsCome(now)
                || !lookupsDrained()) {
            return;
        }
        long cut = Math.max(appliedCut, o.snapshotStartOffset);
        if (tail.hasBelow(cut)) {
            return; // the masks are exact up to the cut only once everything below it is applied
        }
        DvFrontier latest = o.frontiers.latest();
        if (latest != null
                && (latest.cut() >= cut || now - latest.builtAtNanos < o.readyRefreshNanos)) {
            return;
        }
        try {
            buildMasks(cut);
        } catch (IOException | RuntimeException e) {
            LOG.debug("Could not rebuild the latest deletion vector masks.", e);
        }
    }

    /**
     * Whether no lake-position lookup is pending, so the masks cover exactly the changes applied.
     */
    boolean lookupsDrained() {
        return o.inFlight == 0
                && unresolved.isEmpty()
                && !fullResolvePending
                && !targetedPending
                && !tailMissing();
    }
}
