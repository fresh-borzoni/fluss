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
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.exception.DeletionVectorsNotReadyException;
import org.apache.fluss.exception.InvalidRequestException;
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Keeps the deletion vector state of a primary key bucket on its leader and derives the masks of
 * union reads from it.
 *
 * <p>The state covers one registered lake snapshot, the covered snapshot. It holds the snapshot's
 * index (primary key to the file and row position of the key's live row), read through an immutable
 * version ({@link DvVersion}), and the change list after the snapshot's readable offset ({@link
 * DvChangeList}). Each change names the version it supersedes, the log offset its KV value is
 * tagged with. A read's log mask is the versions in the change list a later change superseded. Its
 * lake mask is the snapshot positions of the keys whose superseded version is in the snapshot. Both
 * go up to the high watermark.
 *
 * <p>The change list keeps its updates and deletes in the bucket's RocksDB, so masks are served
 * however far tiering lags. The heap holds the changes waiting for the high watermark, the mask
 * bitmaps and the frontiers kept for reads; the tablet server bounds those with a shared budget
 * ({@code kv.deletion-vectors.heap-memory}).
 *
 * <p>One owner per bucket ({@link DvOwner}) runs every transition, one step at a time, and
 * publishes the state after each step ({@link DvState}); its javadoc has the transitions. A new
 * leader rebuilds the state from the published chain and the log ({@link DvLoad.FullLoad}). The
 * state then switches to newer snapshots on its own by applying their index changes ({@link
 * DvLoad.SwitchLoad}). A reader of a snapshot the bucket moved past gets a {@link
 * StaleSnapshotException} and plans again. This class is the bucket's facade: the write path hands
 * its changes over ({@link DvHandOff}), lookups read the published version, and union reads post
 * themselves to the owner ({@link DvMaskReads}).
 *
 * <p>Requests never catch up, rebuild or replay the log on their own thread. A request that needs
 * such work starts it in the background. Unless it is done within a short wait, the request is
 * answered as not ready ({@link DeletionVectorsNotReadyException}) and the client retries. A read
 * that does not wait is answered at once instead ({@link DvReadPolicy}).
 *
 * <p>A union read whose workers fetch a bucket's masks in parts first pins a cut ({@link #pinCut}):
 * the masks up to the high watermark, frozen in the tablet server's {@link DerivedDvPinnedCuts}.
 * Every worker then reads the lake and the log as of that one offset, while the bucket's own masks
 * move on with writes and newer snapshots. The read releases the cut when it ends.
 *
 * <p>Some field and method names call a switch a move, and the change list the tail.
 */
@Internal
public class DerivedDvManager implements Closeable {

    // the bucket's state and the steps that change it
    private final DvOwner owner;
    // lookups of the published version, on the request thread
    private final DvLookups lookups;

    /**
     * Creates the manager of a bucket's state in {@code dvRocksDB}, which {@link #close} closes.
     * Its background work runs on the tablet server's {@code runtime}.
     */
    public DerivedDvManager(
            DvRocksDB dvRocksDB,
            LongSupplier highWatermark,
            int checkpointInterval,
            DerivedDvMetrics metrics,
            DerivedDvHeapBudget heapBudget,
            DerivedDvRuntime runtime) {
        this(
                dvRocksDB,
                highWatermark,
                checkpointInterval,
                metrics,
                heapBudget,
                runtime,
                runtime.bucketState(),
                DvTestHooks.NONE);
    }

    /** As above, with the owner's steps run by {@code stateExecutor}, and {@code testHooks}. */
    @VisibleForTesting
    DerivedDvManager(
            DvRocksDB dvRocksDB,
            LongSupplier highWatermark,
            int checkpointInterval,
            DerivedDvMetrics metrics,
            DerivedDvHeapBudget heapBudget,
            DerivedDvRuntime runtime,
            Executor stateExecutor,
            DvTestHooks testHooks) {
        this.owner =
                new DvOwner(
                        dvRocksDB,
                        highWatermark,
                        checkpointInterval,
                        metrics,
                        heapBudget,
                        runtime,
                        stateExecutor,
                        testHooks);
        this.lookups = new DvLookups(owner);
    }

    /** The most keys one lookup of lake positions takes. */
    @VisibleForTesting static final int RESOLVE_CHUNK = DvOwner.RESOLVE_CHUNK;

    @VisibleForTesting
    DvState state() {
        return owner.state;
    }

    @VisibleForTesting
    int leasesHeld() {
        return owner.leases.held();
    }

    @VisibleForTesting
    DvRocksDB db() {
        return owner.db;
    }

    /** Frees what the tablet server's heap budget may ask this bucket to free. */
    @VisibleForTesting
    boolean shedForBudget() {
        return owner.shedForBudget();
    }

    /** The most keys a lookup request names; the tiering service sends 50,000 at most. */
    public static final int MAX_LOOKUP_KEYS = 100_000;

    /**
     * Marks the table as one with row TTL. Its cleanup removes a key's value without a change in
     * the log, so an insert does not show that the key had no live version: it may follow an
     * expired one, in the lake or in the log. A replayed insert then supersedes the key's last
     * version, as the write path's insert into such a table does (see {@code KvWriteProcessor}).
     * Call it before the first recovery.
     */
    public void useRowTtl() {
        owner.useRowTtl();
    }

    /** Names the bucket in the lines that log mask requests, moves and rebuilds. */
    public void setLogContext(TablePath tablePath, TableBucket bucket) {
        Long partitionId = bucket.getPartitionId();
        owner.setLogName(
                "table="
                        + tablePath
                        + " partition="
                        + (partitionId == null ? "-" : partitionId)
                        + " bucket="
                        + bucket.getBucket());
    }

    /**
     * Hands over the key changes of a KV batch whose changelog was appended; {@code entries} may be
     * empty. The owner applies them once the high watermark passes them: applying reads nothing,
     * and writes the updates and deletes to the change list in the bucket's RocksDB.
     *
     * @param entries the batch's changes, which the hand-off keeps: the caller does not change the
     *     list afterwards
     * @param logEnd the log end offset after the batch, or -1 if not known
     */
    public void handleChangelogSynced(List<DvEntry> entries, long logEnd) {
        owner.drain.handOver(entries, logEnd);
    }

    /**
     * Drops the change list after the write path failed to hand over the changes of a batch it
     * appended. The change list misses those changes, so union reads replay it from the log, which
     * has the batch, before masks are served again.
     */
    public void handOffFailed(Throwable cause) {
        owner.drain.handOffFailed(cause);
    }

    /** Durable data the derived state is rebuilt and advanced from. */
    public interface RecoverySource {
        /**
         * {snapshotId, readable offset of this bucket} of the latest readable snapshot, or null.
         */
        @Nullable
        long[] readableSnapshot() throws Exception;

        /**
         * Returns this bucket's readable offset in a lake snapshot, where its log tail starts, or
         * null if the snapshot is not registered as readable. A lake that takes deletes is readable
         * at its tiered offsets. A lake that merges by key may be readable from earlier offsets.
         */
        @Nullable
        Long snapshotOffset(long snapshotId) throws Exception;

        /** Returns the registered position-index changes in commit order. */
        List<PositionIndexStore.ChainEntry> chain() throws Exception;

        /** The identity of the lake snapshot the entry was registered with, or null if none. */
        @Nullable
        default String snapshotIdentity(PositionIndexStore.ChainEntry entry) throws Exception {
            return null;
        }

        /** Downloads this bucket's index part of a change into fresh files; throws if gone. */
        PositionIndexStore.BucketData readChange(PositionIndexStore.ChainEntry entry)
                throws Exception;

        /**
         * Replays the log from {@code fromOffset} to {@code toOffset} or the log end, whichever
         * comes first, in whole record batches of (key, record offset, version or -1). Returns the
         * offset after the last replayed batch, where a later replay continues.
         */
        long replayLog(long fromOffset, long toOffset, ChangeConsumer consumer) throws Exception;

        /** The current end offset of the local log. */
        long logEndOffset();

        /** Whether old chain entries were trimmed (a rebuild must then start at a checkpoint). */
        boolean chainTrimmed() throws Exception;

        /** This bucket's newest complete checkpoint at or before chain index upTo, or null. */
        @Nullable
        PositionIndexStore.Checkpoint latestCheckpoint(
                List<PositionIndexStore.ChainEntry> chain, int upTo) throws Exception;

        /** Downloads a checkpoint: its SST (none for an empty index) and file ids. */
        PositionIndexStore.BucketData readCheckpoint(PositionIndexStore.Checkpoint cp)
                throws Exception;

        /** Publishes this bucket's full index covering {@code snapshotId}. */
        void uploadCheckpoint(
                long snapshotId, @Nullable String localSst, Map<Integer, String> fileDict)
                throws Exception;
    }

    /** Receives one replayed change. */
    public interface ChangeConsumer {
        /**
         * Receives one replayed change; {@code insert} is set if the change inserted the key, which
         * had no live version before it.
         */
        void accept(byte[] key, long recordOffset, long versionOffset, boolean insert);
    }

    /**
     * Rebuilds the derived state of a new leader in the background. {@code limiter} bounds how many
     * buckets of the tablet server rebuild at once. The bucket takes writes meanwhile, and its
     * union reads and lookups are answered as not ready until the rebuild is done. Returns once the
     * bucket took the leadership over.
     */
    public void recoverInBackground(RecoverySource src, DerivedDvRebuildLimiter limiter) {
        owner.work.recover(src, limiter);
    }

    /**
     * Rebuilds the derived state on a new leader and waits for it: applies the published chain up
     * to the readable snapshot and replays the log from the snapshot's readable offset. With
     * nothing readable, covers the empty lake and replays the log in the background.
     */
    public void recover(RecoverySource src) throws Exception {
        await(owner.work.recover(src));
    }

    /** Waits for an owner's work, rethrowing its failure. */
    private static boolean await(CompletableFuture<Boolean> done) throws Exception {
        try {
            return done.get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw e;
        }
    }

    /**
     * Stops background work and closes the DV RocksDB once nothing calls it any more ({@link
     * DvDbLeases}): a lookup, a resolve, a checkpoint's export, a load's ingest or a replay's
     * batch. The owner closes the state in a step; each job holds its lease for one call and ends
     * at its next unit once {@code closed} is set. If one does not end within the wait, the last of
     * them closes the RocksDB, so it is never closed under an iterator or snapshot. {@link
     * #dbClosed} completes once it is closed: the RocksDB shares the tablet server's block cache
     * and write buffer manager, which must outlive it.
     */
    @Override
    public void close() {
        owner.faults.close(this);
    }

    /**
     * Completes once the DV RocksDB is closed, by {@link #close} or by the last background user
     * after it.
     */
    public CompletableFuture<Void> dbClosed() {
        return owner.leases.dbClosed();
    }

    /**
     * The error for a request to a bucket whose leader closed while the request waited or read: a
     * new leader serves the bucket, which the client looks up.
     */
    static NotLeaderOrFollowerException closedError() {
        return new NotLeaderOrFollowerException(
                "The deletion vectors of the bucket closed, as this server is no longer its"
                        + " leader.");
    }

    /** Starts a background catch-up to a newly registered snapshot. */
    public void advanceAsync(long snapshotId) {
        owner.post(() -> owner.registered(snapshotId));
    }

    /**
     * Advances to {@code snapshotId} from the published index chain if it is newer than the covered
     * one, and waits for it. Returns whether {@code snapshotId} is covered now.
     */
    public boolean ensureCovered(long snapshotId) throws Exception {
        return await(owner.work.reach(snapshotId, -1));
    }

    /**
     * Pins the cut of a union read of {@code requestedSnapshotId}: its masks up to the high
     * watermark, frozen. The read's workers later fetch their parts of exactly these masks ({@link
     * #pinnedCut}) while the bucket's own masks move on. Pins at the same high watermark share one
     * cut. The pin takes {@code holds} holds, which the read releases one by one, as its parts end
     * ({@link #releaseCut}). A retry of a pin request ({@code pinRequestId}) the bucket took
     * already gets the same cut without another pin.
     *
     * <p>Unless {@code waitForCatchUp}, a leader that covers the snapshot registered right before
     * {@code requestedSnapshotId} does not wait to switch to it: it fails at once with a {@link
     * StaleSnapshotException} that {@link StaleSnapshotException#isBehind() is behind}, and the
     * reader reads the covered snapshot. Nor does the read wait for lake-position lookups: while
     * some are pending it gets the last masks built while none was, up to an offset below the high
     * watermark, or the lake snapshot alone right after a switch. A bucket with nothing to serve
     * yet (rebuilding, replaying its change list, or looking up what a rebuild needs) fails it at
     * once with a {@link DeletionVectorsNotReadyException} that {@link
     * DeletionVectorsNotReadyException#isRebuilding() names a cut}, and the reader reads the
     * bucket's log by key up to it.
     *
     * @throws StaleSnapshotException if the bucket does not cover {@code requestedSnapshotId} and
     *     will not
     * @throws DeletionVectorsNotReadyException while the bucket switches to the snapshot or replays
     *     its change list
     */
    public DvPinnedCut pinCut(
            long requestedSnapshotId,
            boolean waitForCatchUp,
            @Nullable Long pinRequestId,
            int holds)
            throws IOException, StaleSnapshotException {
        return owner.maskReads.pin(this, requestedSnapshotId, waitForCatchUp, pinRequestId, holds);
    }

    /**
     * Returns the cut of snapshot {@code snapshotId} at log offset {@code cut} that a union read
     * pinned. A cut this bucket does not hold (dropped, or pinned on another leader) is built again
     * only if {@code snapshotId} is the covered snapshot and {@code cut} the current high
     * watermark, since that gives the same masks.
     *
     * @throws StaleSnapshotException if the cut is not held and cannot be built again
     */
    public DvPinnedCut pinnedCut(long snapshotId, long cut)
            throws IOException, StaleSnapshotException {
        return owner.maskReads.pinned(this, snapshotId, cut);
    }

    /**
     * Releases {@code holds} holds of the cut of snapshot {@code snapshotId} at {@code cut}, as the
     * union read that pinned it, or some of its parts, end. The cut is dropped once all its holds
     * are released; otherwise, or if they never are, it is dropped when no request asked for it
     * within the idle timeout. Returns whether the cut was dropped.
     */
    public boolean releaseCut(long snapshotId, long cut, int holds) {
        return owner.heapBudget.pinnedCuts().release(this, snapshotId, cut, holds);
    }

    /** The cuts of union reads this bucket holds. */
    @VisibleForTesting
    public int pinnedCutCount() {
        return owner.heapBudget.pinnedCuts().count(this);
    }

    /** Whether the bucket's leader closed. */
    boolean isClosed() {
        return owner.isClosed();
    }

    /** The cut of the ready masks kept from the mask state before, or -1. */
    @VisibleForTesting
    long previousReadyCut() {
        DvFrontier kept = owner.frontiers.kept();
        return kept == null ? -1 : kept.cut();
    }

    /**
     * Returns {key, file path, row position} of each key found in snapshot {@code snapshotId}.
     * Meanwhile a newer registered snapshot is caught up to in the background.
     *
     * @throws DeletionVectorsNotReadyException while the bucket catches up to the snapshot
     */
    public List<Object[]> lookup(long snapshotId, List<byte[]> keys)
            throws IOException, StaleSnapshotException {
        if (keys.size() > MAX_LOOKUP_KEYS) {
            throw tooManyLookupKeys(keys.size());
        }
        return lookups.lookup(snapshotId, keys);
    }

    /** The error for a lookup of more than {@link #MAX_LOOKUP_KEYS} keys. */
    public static InvalidRequestException tooManyLookupKeys(int keys) {
        return new InvalidRequestException(
                String.format(
                        "A lookup of deletion vector positions names %d keys, more than the %d"
                                + " a request may name.",
                        keys, MAX_LOOKUP_KEYS));
    }

    /** Returns the lake snapshot this bucket covers, or -1 for the empty lake. */
    public long getReadableSnapshotId() {
        return owner.state.coveredSnapshotId;
    }

    /**
     * Returns the first log offset the state of this bucket needs: the readable offset of the
     * covered snapshot, or an earlier one that a pinned cut or the masks kept from the snapshot
     * before still read from. Union reads read the log from there, and a retried replay of the tail
     * starts there. A new leader replays from the readable offset of the latest readable snapshot,
     * which is the covered one or a later one. It is 0 for the empty lake and until a rebuild
     * covers a snapshot, so all of the log is needed then. Log retention keeps the log from this
     * offset on. It reads volatile fields and the published state only, so the remote log task
     * never waits for the owner.
     */
    public long logRetainOffset() {
        return owner.logRetainOffset(this);
    }

    /**
     * Records a cut handed out to a reader that reads the bucket by key up to it ({@link
     * DvReadPolicy.Action#FOLD}): no later read is served masks below it.
     */
    public void foldCutServed(long cut) {
        owner.servedCuts.raise(cut);
    }

    @VisibleForTesting
    void setCheckpointJitterMillis(long millis) {
        owner.index.checkpointJitterMs = millis;
    }

    /** Delays the start of checkpoints on {@code timer} instead of the server's scheduler. */
    @VisibleForTesting
    void useCheckpointTimer(ScheduledExecutorService timer) {
        owner.index.checkpointTimer = timer;
    }

    /** Whether lake positions are looked up, or keys wait for a lookup. */
    @VisibleForTesting
    boolean resolving() {
        boolean[] resolving = new boolean[1];
        onOwner(() -> resolving[0] = owner.inFlight > 0 || !owner.maskState.unresolved.isEmpty());
        return resolving[0];
    }

    @VisibleForTesting
    DvTestHooks testHooks() {
        return owner.testHooks;
    }

    /** How long {@link #close} waits for the background users of the RocksDB. */
    @VisibleForTesting
    void setCloseWaitMillis(long millis) {
        owner.closeWaitMs = millis;
    }

    /** Makes replays keep their map of last versions on disk from the start. */
    @VisibleForTesting
    void setSpillReplayVersions(boolean spill) {
        owner.rebuild.spillReplayVersions = spill;
    }

    /** When off, a move looks up every lake key again instead of the keys it changed. */
    @VisibleForTesting
    void setTargetedMoves(boolean targeted) {
        owner.targetedSwitches = targeted;
    }

    /** The keys looked up in the index for masks so far. */
    @VisibleForTesting
    long lookedUpKeys() {
        return owner.lookedUpKeys.get();
    }

    /** {moves that kept the masks, moves whose derivation failed and looked up every key}. */
    @VisibleForTesting
    long[] targetedMoves() {
        return new long[] {owner.targetedCount.get(), owner.targetedFallbacks.get()};
    }

    /**
     * Whether the latest index holds changes the covered version does not: a load ingests them, or
     * failed to.
     */
    @VisibleForTesting
    boolean indexAhead() {
        boolean[] ahead = new boolean[1];
        onOwner(() -> ahead[0] = owner.latestIndex.ingesting() || owner.latestIndex.isDirty());
        return ahead[0];
    }

    /** How often the ready masks are rebuilt at most as lookups drain. */
    @VisibleForTesting
    void setReadyRefreshMillis(long millis) {
        owner.readyRefreshNanos = TimeUnit.MILLISECONDS.toNanos(millis);
    }

    @VisibleForTesting
    void setMaskCacheMillis(long millis) {
        owner.maskCacheNanos = TimeUnit.MILLISECONDS.toNanos(millis);
    }

    /** The changes waiting for the high watermark, including a replay's (from its thread). */
    @VisibleForTesting
    int queuedChanges() {
        int[] queued = new int[1];
        onOwner(
                () -> {
                    owner.drain.drainHandOff();
                    queued[0] = owner.queuedNow();
                });
        return queued[0];
    }

    /** The heap bytes of the bucket last reported to the budget. */
    @VisibleForTesting
    long tailMemoryBytes() {
        long[] reported = new long[1];
        onOwner(
                () -> {
                    owner.drain.drainHandOff();
                    owner.maskState.reportTail(false);
                    reported[0] = owner.maskState.reportedMemory;
                });
        return reported[0];
    }

    /**
     * Sets the hand-off's bounds: writes wait for the owner past {@code threshold} queued changes,
     * at most {@code waitMs}; the replay after an overflow backs off from {@code retryInitialMs} to
     * {@code retryMaxMs}, and waits while the owner's steps wait over {@code behindMs}.
     */
    @VisibleForTesting
    void setHandOffBounds(
            long threshold, long waitMs, long retryInitialMs, long retryMaxMs, long behindMs) {
        owner.drain.handOffThreshold = threshold;
        owner.drain.handOffWaitMs = waitMs;
        owner.drain.behindNanos = TimeUnit.MILLISECONDS.toNanos(behindMs);
        owner.rebuild.overflowRetryInitialMs = retryInitialMs;
        owner.rebuild.overflowRetryMaxMs = retryMaxMs;
    }

    /** {replays put off as the owner was behind, the backoff of the next overflow in ms}. */
    @VisibleForTesting
    long[] overflowRetries() {
        long[] seen = new long[2];
        onOwner(
                () -> {
                    seen[0] = owner.rebuild.overflowRetriesPutOff;
                    seen[1] = owner.rebuild.nextOverflowBackoffMs();
                });
        return seen;
    }

    /**
     * Makes the owner's steps apply at most {@code applyChunk} changes and serialize about {@code
     * buildChunkBytes} of masks, so applies and frontier builds take many steps.
     */
    @VisibleForTesting
    void setStepChunks(int applyChunk, long buildChunkBytes) {
        onOwner(
                () -> {
                    owner.maskState.applyChunk = applyChunk;
                    owner.maskState.buildChunkBytes = buildChunkBytes;
                });
    }

    /**
     * Drains the hand-off, applies the changes the high watermark passed and reports the bucket's
     * heap, as its background work does next; waits for the owner to do so.
     */
    @VisibleForTesting
    void settle() {
        onOwner(
                () -> {
                    owner.drain.drainHandOff();
                    owner.maskState.applyAll(owner.highWatermark.getAsLong());
                    owner.maskState.reportTail(true);
                });
    }

    /** Runs {@code step} on the owner and waits for it. */
    @VisibleForTesting
    void onOwner(Runnable step) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        owner.post(
                () -> {
                    try {
                        step.run();
                        done.complete(null);
                    } catch (Throwable t) {
                        done.completeExceptionally(t);
                    }
                });
        try {
            done.get(60, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "A test step of the deletion vector owner did not run", e);
        }
    }

    /** Records a registration newer than any covered, as a notification does, without a switch. */
    @VisibleForTesting
    void noteRegistration(long snapshotId) {
        onOwner(() -> owner.newestRegistration = snapshotId);
    }

    /** The cut a read that does not wait would be told to read by key up to, not yet served. */
    @VisibleForTesting
    long foldCut() {
        return DvReadPolicy.foldCut(owner.highWatermark.getAsLong(), owner.servedCuts.get());
    }

    /** Takes a lease of the RocksDB, which a close then waits for until {@link #releaseDb}. */
    @VisibleForTesting
    void holdDb() {
        owner.leases.acquire();
    }

    /** Ends a use of the RocksDB that {@link #holdDb} started. */
    @VisibleForTesting
    void releaseDb() {
        owner.leases.release();
    }

    /** What a test sees of the bucket's state, read at one point. */
    @VisibleForTesting
    DvInspection inspect() {
        DvInspection[] seen = new DvInspection[1];
        onOwner(
                () -> {
                    owner.drain.drainHandOff();
                    seen[0] = owner.inspectNow();
                });
        return seen[0];
    }
}
