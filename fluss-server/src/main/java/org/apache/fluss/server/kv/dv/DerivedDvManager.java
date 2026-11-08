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

/**
 * Keeps the deletion vector state of a primary key bucket on its leader: the position index of a
 * registered lake snapshot, which tiering looks the positions of its changed keys up in.
 *
 * <p>The state covers one registered lake snapshot, the covered snapshot. It holds the snapshot's
 * index (primary key to the file and row position of the key's live row), read through an immutable
 * version ({@link DvVersion}).
 *
 * <p>One owner per bucket ({@link DvOwner}) runs every transition, one step at a time, and
 * publishes the state after each step ({@link DvState}); its javadoc has the transitions. A new
 * leader rebuilds the state from the published chain ({@link DvLoad.FullLoad}). The state then
 * switches to newer snapshots on its own by applying their index changes ({@link
 * DvLoad.SwitchLoad}). A lookup of a snapshot the bucket moved past gets a {@link
 * StaleSnapshotException} and plans again. This class is the bucket's facade: lookups read the
 * published version.
 *
 * <p>Requests never catch up or rebuild on their own thread. A request that needs such work starts
 * it in the background. Unless it is done within a short wait, the request is answered as not ready
 * ({@link DeletionVectorsNotReadyException}) and the client retries.
 *
 * <p>Some field and method names call a switch a move.
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
            int checkpointInterval,
            DerivedDvMetrics metrics,
            DerivedDvRuntime runtime) {
        this(
                dvRocksDB,
                checkpointInterval,
                metrics,
                runtime,
                runtime.bucketState(),
                DvTestHooks.NONE);
    }

    /** As above, with the owner's steps run by {@code stateExecutor}, and {@code testHooks}. */
    @VisibleForTesting
    DerivedDvManager(
            DvRocksDB dvRocksDB,
            int checkpointInterval,
            DerivedDvMetrics metrics,
            DerivedDvRuntime runtime,
            Executor stateExecutor,
            DvTestHooks testHooks) {
        this.owner =
                new DvOwner(
                        dvRocksDB, checkpointInterval, metrics, runtime, stateExecutor, testHooks);
        this.lookups = new DvLookups(owner);
    }

    /** The state the owner published last. */
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

    /** The most keys a lookup request names; the tiering service sends 50,000 at most. */
    public static final int MAX_LOOKUP_KEYS = 100_000;

    /** Names the bucket in the lines that log moves and rebuilds. */
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

    /**
     * Rebuilds the derived state of a new leader in the background. {@code limiter} bounds how many
     * buckets of the tablet server rebuild at once. The bucket takes writes meanwhile, and its
     * lookups are answered as not ready until the rebuild is done. Returns once the bucket took the
     * leadership over.
     */
    public void recoverInBackground(RecoverySource src, DerivedDvRebuildLimiter limiter) {
        owner.work.recover(src, limiter);
    }

    /**
     * Rebuilds the derived state on a new leader and waits for it: applies the published chain up
     * to the readable snapshot. With nothing readable, covers the empty lake.
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
     * DvDbLeases}): a lookup, a checkpoint's export or a load's ingest. The owner closes the state
     * in a step; each job holds its lease for one call and ends at its next unit once {@code
     * closed} is set. If one does not end within the wait, the last of them closes the RocksDB, so
     * it is never closed under an iterator or snapshot. {@link #dbClosed} completes once it is
     * closed: the RocksDB shares the tablet server's block cache and write buffer manager, which
     * must outlive it.
     */
    @Override
    public void close() {
        owner.faults.close();
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

    /** Whether the bucket's leader closed. */
    boolean isClosed() {
        return owner.isClosed();
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

    @VisibleForTesting
    void setCheckpointJitterMillis(long millis) {
        owner.index.checkpointJitterMs = millis;
    }

    /** Delays the start of checkpoints on {@code timer} instead of the server's scheduler. */
    @VisibleForTesting
    void useCheckpointTimer(ScheduledExecutorService timer) {
        owner.index.checkpointTimer = timer;
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

    /** Runs {@code step} on the owner and waits for it, as a test needs. */
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
}
