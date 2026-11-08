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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.DeletionVectorsNotReadyException;
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.server.utils.RoaringBitmapUtils;

import io.github.fluss_contrib.rocksdb.Snapshot;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

/**
 * Keeps the deletion vector state of a primary key bucket on its leader and derives the masks of
 * union reads from it.
 *
 * <p>The state covers one registered lake snapshot, the covered snapshot. It holds the snapshot's
 * index (primary key to the file and row position of the key's live row) and the log tail after the
 * snapshot's readable offset. Each change of the tail names the version it supersedes, the log
 * offset its KV value is tagged with. A read's log mask is the tail versions a later change
 * superseded. Its lake mask is the snapshot positions of the keys whose superseded version is in
 * the snapshot. Both go up to the high watermark.
 *
 * <p>A new leader rebuilds the state from the published chain and the log. The state then advances
 * to newer snapshots on its own by applying their index changes. A reader of a snapshot the bucket
 * moved past gets a {@link StaleSnapshotException} and plans again.
 *
 * <p>Requests never catch up, rebuild or replay the log on their own thread. A request that needs
 * such work starts it in the background. Unless it is done within a short wait, the request is
 * answered as not ready ({@link DeletionVectorsNotReadyException}) and the client retries.
 *
 * <p>The tails of a tablet server share a memory budget ({@code kv.deletion-vectors.tail-memory}).
 * When it is exceeded, the largest tails spill their changes to their bucket's RocksDB, so masks
 * are served however far tiering lags.
 *
 * <p>A rebuild replays the log into a fresh tail without the manager lock, so the write path keeps
 * handing over its changes. It then replays the few changes written meanwhile and installs the tail
 * under the lock. Rebuilds of all buckets of a tablet server share a {@link
 * DerivedDvRebuildLimiter}.
 */
@Internal
public class DerivedDvManager implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(DerivedDvManager.class);
    private static final int APPLY_THRESHOLD = 100_000;
    private static final int REPLAY_APPLY_BATCH = 4096;
    // A replay makes another pass without the lock while the log end is more than this many
    // offsets ahead. This keeps the part replayed under the lock, which writes wait for, short.
    private static final long LOCKED_REPLAY_MAX_OFFSETS = 4096;
    private static final int MAX_UNLOCKED_REPLAY_PASSES = 8;
    // How long a request waits for background work it started before it is answered as not ready.
    private static final long NOT_READY_WAIT_MS = 500;
    // Reports of the tail's memory between two measurements of its bitmaps.
    private static final int BITMAP_MEASURE_INTERVAL = 64;
    // How long a replay waits after a disk failure.
    private static final long REPLAY_RETRY_INITIAL_BACKOFF_MS = 1_000;
    private static final long REPLAY_RETRY_MAX_BACKOFF_MS = 30_000;
    // How long a failed background catch-up waits before a request starts it again.
    private static final long ADVANCE_RETRY_INITIAL_BACKOFF_MS = 1_000;
    private static final long ADVANCE_RETRY_MAX_BACKOFF_MS = 30_000;

    private final DvRocksDB db;
    private final LongSupplier highWatermark;

    private long coveredSnapshotId = -1;
    // what the covered snapshot's id named when it was registered, for readers to check
    @Nullable private String coveredIdentity;
    // coveredSnapshotId -1 is the empty lake at offset 0, whose tail is the whole log.
    private long snapshotStartOffset = 0;
    // The empty lake's tail is built on the first union read that needs it, so an initial load
    // before the first snapshot does not grow memory.
    private boolean emptyLakeTracked = false;
    // A batch is in the log before the write path hands its changes over, so the high watermark
    // can pass changes the tail does not have yet. Once the write path reports log ends
    // (trackHandOffs), masks go up to the log end as of the last hand-off or tail install at most;
    // every change below it is in the tail. -1 until then.
    private boolean tracksHandOffs = false;
    private long handedOffEnd = -1;
    // set when the covered snapshot's index is in place but its log tail is not; masks without
    // the tail would let old versions through, so union reads fail until a replay succeeds
    @Nullable private Exception tailFailure;
    /** Default of {@code kv.deletion-vectors.checkpoint-interval}. */
    public static final int DEFAULT_CHECKPOINT_INTERVAL = 50;

    // The index is exported every checkpointInterval chain entries, so a rebuild replays only the
    // later entries and tiering can trim the chain.
    private final int checkpointInterval;
    private final DerivedDvMetrics metrics;
    private long entriesApplied = 0;
    private long entriesAtCheckpoint = 0;
    private boolean checkpointing = false;
    // Background readers of the DV RocksDB run outside the lock and must drain before it closes.
    // Volatile for replays, which run outside the lock and stop early once the tablet closes.
    private volatile boolean closed = false;
    private int backgroundReaders = 0;
    // The manager owns the DV RocksDB: close() closes it, or the last background reader does.
    private boolean dbCloseDeferred = false;
    private boolean dbClosed = false;
    private final CompletableFuture<Void> dbClosedFuture = new CompletableFuture<>();
    // How long close() waits for the background users of the RocksDB.
    private static final long CLOSE_WAIT_MS = 30_000;

    // Replaced as a whole when a replay installs a new tail.
    private DvTail tail = new DvTail();
    // The tablet server's memory for log tails; the largest tails spill their changes to disk.
    private volatile DerivedDvTailBudget tailBudget;
    private final TailOwner tailOwner = new TailOwner();
    // ids of tails spilled to disk, unique per bucket
    private long tailIds = 0;
    // the tail is spilled and its masks are new: the lake positions of its keys are looked up
    // from disk in the background before masks are served
    private boolean fullResolvePending = false;
    private boolean fullResolveScheduled = false;
    // what was last added to the budget and the server's metrics
    private volatile long reportedMemory = 0;
    private long reportedSpilledBytes = 0;
    private boolean reportedSpilled = false;
    // mask bitmap bytes; measuring takes a pass over the bitmaps, so it runs every few reports
    private long bitmapBytes = 0;
    private int reportsSinceBitmaps = 0;
    // a replay retried after a disk failure waits this long
    private long replayRetryAtMs = 0;
    private long replayBackoffMs = REPLAY_RETRY_INITIAL_BACKOFF_MS;
    // background work a request started: the snapshot a catch-up goes to, a tail replay
    private long advancing = Long.MIN_VALUE;
    private boolean replayScheduled = false;
    // background replays are numbered; a request reports the failure of the one it waited for
    private long replays = 0;
    private long lastFailedReplay = -1;
    @Nullable private Exception lastReplayFailure;
    // a snapshot a background catch-up could not reach, answered as stale like a passed one
    private long unreachable = Long.MIN_VALUE;
    // background catch-ups are numbered; one that failed is tried again after a backoff, and
    // the requests that waited for it are answered as not ready
    private long advances = 0;
    private long lastFailedAdvance = -1;
    private long lastFailedAdvanceSnapshot = Long.MIN_VALUE;
    @Nullable private Exception lastAdvanceFailure;
    private long advanceRetryAtMs = 0;
    private long advanceBackoffMs = ADVANCE_RETRY_INITIAL_BACKOFF_MS;
    private final List<ByteBuffer> unresolved = new ArrayList<>();
    private final Map<Integer, Roaring64Bitmap> lakeMasks = new HashMap<>();
    // A move to a newer snapshot keeps the lake masks of the keys its index changes do not name and
    // derives the others again (completeTargetedMove). Meanwhile no masks are served and no lookups
    // start, as it removes the positions those keys' rows had.
    private boolean targetedPending = false;
    // the batches of keys resolves look up outside the lock; a move looks their keys up again
    private final Set<List<ByteBuffer>> resolving =
            Collections.newSetFromMap(new IdentityHashMap<>());
    // a test switch: a move looks up every lake key again, as after a rebuild
    private volatile boolean targetedMoves = true;
    // keys looked up in the index for masks, and moves that kept the masks or fell back, for tests
    private final AtomicLong lookedUpKeys = new AtomicLong();
    private final AtomicLong targetedMoveCount = new AtomicLong();
    private final AtomicLong targetedFallbacks = new AtomicLong();
    // the most keys a move's index changes may name for it to keep the other masks: their hashes
    // take 8 bytes each; beyond, looking up every lake key of the tail again is cheaper anyway
    private static final long TARGETED_MAX_KEYS = 8_000_000;

    // Applies queued changes once the high watermark passes them and looks up the lake positions
    // of keys whose superseded version is in the lake, so a union read finds the masks ready.
    private static final ScheduledExecutorService RESOLVER =
            Executors.newScheduledThreadPool(
                    4,
                    r -> {
                        Thread t = new Thread(r, "derived-dv-resolver");
                        t.setDaemon(true);
                        return t;
                    });
    private static final long RESOLVE_DELAY_MS = 2;
    /** The most keys a lookup request names; the tiering service sends 50,000 at most. */
    public static final int MAX_LOOKUP_KEYS = 100_000;
    // The most keys one lookup in the index takes; a closing bucket waits for one such lookup.
    @VisibleForTesting static final int RESOLVE_CHUNK = 10_000;
    private static final long RESOLVE_WAIT_HW_MS = 20;
    private boolean resolveScheduled = false;
    private int inFlight = 0;
    // CHECKPOINTER, or a test's
    private volatile ScheduledExecutorService checkpointer = CHECKPOINTER;
    private long generation = 0;
    @Nullable private RecoverySource recoverySource;
    private volatile DerivedDvRebuildLimiter rebuildLimiter = DerivedDvRebuildLimiter.DEFAULT;
    // the replay in progress, for tests to see its queue
    @Nullable private volatile Replay replaying;

    // Masks built for a union read are served again for a short while: each covers exactly
    // [snapshotStartOffset, hw) of its own hw, so a slightly older one is still a correct pair.
    private static final long MASK_CACHE_NANOS =
            Long.parseLong(System.getProperty("fluss.derived.mask-cache-ms", "100")) * 1_000_000L;
    private long maskCacheNanos = MASK_CACHE_NANOS;
    @Nullable private DvSnapshot cachedMasks;
    private long cachedMasksSnapshotId = Long.MIN_VALUE;
    private long cachedMasksGeneration = -1;
    private long cachedMasksAtNanos;

    // Snapshots already moved past, so a late notification or request never moves the state
    // backwards; lake snapshot ids are not ordered, only the chain is.
    private final LinkedHashSet<Long> passed = new LinkedHashSet<>();
    private static final int PASSED_LIMIT = 4096;
    // One catch-up, rebuild or replay at a time; downloads and log replays hold this lock, not the
    // manager's. Taken before the manager lock, never while holding it.
    private final ReentrantLock advanceLock = new ReentrantLock();
    // Runs catch-ups, which never wait: a bucket busy with a rebuild or replay is tried again
    // later, and a catch-up that needs a full rebuild goes to the rebuild limiter's threads.
    private static final ScheduledExecutorService ADVANCER =
            Executors.newScheduledThreadPool(
                    2,
                    r -> {
                        Thread t = new Thread(r, "derived-dv-advance");
                        t.setDaemon(true);
                        return t;
                    });
    // Exports and uploads index checkpoints, apart from the catch-ups.
    private static final ScheduledExecutorService CHECKPOINTER =
            Executors.newScheduledThreadPool(
                    2,
                    r -> {
                        Thread t = new Thread(r, "derived-dv-checkpoint");
                        t.setDaemon(true);
                        return t;
                    });
    // checkpoints of the buckets that catch up to one snapshot together start apart
    private static final long CHECKPOINT_JITTER_MS = 10_000;
    private long checkpointJitterMs = CHECKPOINT_JITTER_MS;
    private static final long BUSY_RETRY_INITIAL_MS = 100;
    private static final long BUSY_RETRY_MAX_MS = 2_000;

    /** What a background catch-up got to. */
    private enum Advance {
        COVERED,
        NOT_COVERED,
        // the bucket rebuilds or replays: tried again later
        BUSY,
        // a full rebuild is needed, which waits for a rebuild slot
        REBUILD
    }

    /**
     * Creates the manager of a bucket's state in {@code dvRocksDB}, which {@link #close} closes.
     */
    public DerivedDvManager(DvRocksDB dvRocksDB, LongSupplier highWatermark) {
        this(dvRocksDB, highWatermark, DEFAULT_CHECKPOINT_INTERVAL, DerivedDvMetrics.noop());
    }

    public DerivedDvManager(
            DvRocksDB dvRocksDB,
            LongSupplier highWatermark,
            int checkpointInterval,
            DerivedDvMetrics metrics) {
        this(dvRocksDB, highWatermark, checkpointInterval, metrics, DerivedDvTailBudget.DEFAULT);
    }

    public DerivedDvManager(
            DvRocksDB dvRocksDB,
            LongSupplier highWatermark,
            int checkpointInterval,
            DerivedDvMetrics metrics,
            DerivedDvTailBudget tailBudget) {
        this.db = dvRocksDB;
        this.highWatermark = highWatermark;
        this.checkpointInterval = checkpointInterval;
        this.metrics = metrics;
        this.tailBudget = tailBudget;
    }

    /** Moves the bucket's tail memory to the tablet server's budget. */
    public synchronized void useTailBudget(DerivedDvTailBudget budget) {
        if (budget != tailBudget) {
            tailBudget.remove(tailOwner);
            tailBudget = budget;
            reportTail(true);
        }
    }

    /**
     * Makes masks go up to the log end the write path last handed over at most ({@link
     * #handleChangelogSynced(List, long)}), not to a high watermark that passed a batch whose
     * changes are not handed over yet. A tablet calls it before its first write.
     */
    public synchronized void trackHandOffs() {
        tracksHandOffs = true;
    }

    /**
     * The offset up to which masks are built: the high watermark, but no changes not handed over.
     */
    private long servableHighWatermark() {
        long hw = highWatermark.getAsLong();
        return tracksHandOffs && handedOffEnd >= 0 ? Math.min(hw, handedOffEnd) : hw;
    }

    /** As {@link #handleChangelogSynced(List, long)}, without a log end, for tests. */
    @VisibleForTesting
    public void handleChangelogSynced(List<DvEntry> entries) {
        handleChangelogSynced(entries, -1);
    }

    /**
     * Queues the key changes of a KV batch whose changelog was appended, also none. The resolver
     * applies them once the high watermark passes them; applying reads nothing, also for a spilled
     * tail.
     *
     * @param logEnd the log end offset after the batch, or -1 if not known
     */
    public synchronized void handleChangelogSynced(List<DvEntry> entries, long logEnd) {
        if (closed) {
            return; // the RocksDB may be closed; a new leader replays the log
        }
        if (logEnd >= 0) {
            handedOffEnd = Math.max(handedOffEnd, logEnd);
        }
        if (entries.isEmpty()) {
            return;
        }
        if (recoverySource != null && tailMissing()) {
            // No masks are served until a replay builds the tail. The replay reads these changes
            // from the log: they are appended by the time of this call, and a replay installs its
            // tail under this lock after reading the log up to its end.
            return;
        }
        tail.enqueueAll(entries);
        if (tail.queue.size() > APPLY_THRESHOLD) {
            applyUpTo(highWatermark.getAsLong());
        }
        reportTail(false);
        scheduleResolve();
    }

    /**
     * Drops the tail after its disk failed, e.g. on a write of a spilled tail. No masks are served
     * and union reads are answered as not ready until the tail is replayed from the log after a
     * backoff. The caller holds the lock.
     */
    private void tailDiskFailed(IOException e) {
        LOG.warn(
                "Derived DV: the log tail of snapshot {} could not be kept on disk; it is dropped"
                        + " and replayed from the log.",
                coveredSnapshotId,
                e);
        generation++;
        resetTail();
        tailFailure = new TailDiskException(e);
        replayRetryAtMs = System.currentTimeMillis() + replayBackoffMs;
        replayBackoffMs = Math.min(2 * replayBackoffMs, REPLAY_RETRY_MAX_BACKOFF_MS);
        reportTail(true);
    }

    /**
     * Spills the tail's pending changes to the bucket's RocksDB when the tablet server's tail
     * memory is exceeded. Returns false if it cannot: the tail is spilled already, the manager is
     * closing, or there is no log to rebuild the tail from.
     */
    private synchronized boolean spillForBudget() {
        if (closed || tail.spilled() || recoverySource == null || tail.size() == 0) {
            return false;
        }
        long changesBefore = tail.changeCount();
        try {
            tail.spill(db.tailStore(), ++tailIds);
        } catch (IOException e) {
            tailDiskFailed(e);
            return true;
        }
        LOG.info(
                "Derived DV: spilled the {} pending changes of the log tail of snapshot {} to disk,"
                        + " as the log tails of the tablet server exceed {}.",
                changesBefore,
                coveredSnapshotId,
                ConfigOptions.KV_DELETION_VECTORS_TAIL_MEMORY.key());
        reportTail(true);
        return true;
    }

    /** The tail's account in the tablet server's budget. */
    private final class TailOwner implements DerivedDvTailBudget.Owner {
        @Override
        public boolean spill() {
            return spillForBudget();
        }
    }

    /**
     * Drops the log tail after the write path failed to hand over the changes of a batch it
     * appended. The tail misses those changes, so union reads replay it from the log, which has the
     * batch, before masks are served again.
     */
    public synchronized void handOffFailed(Throwable cause) {
        if (closed) {
            return;
        }
        generation++;
        resetTail();
        reportTail(true);
        tailFailure =
                cause instanceof Exception
                        ? (Exception) cause
                        : new IOException("Derived DV hand-off failed", cause);
    }

    /** Whether union reads of the covered snapshot need a log replay first. */
    private boolean tailMissing() {
        return tailFailure != null || (coveredSnapshotId < 0 && !emptyLakeTracked);
    }

    private void scheduleResolve() {
        scheduleResolve(RESOLVE_DELAY_MS);
    }

    private void scheduleResolve(long delayMs) {
        if (!resolveScheduled) {
            resolveScheduled = true;
            RESOLVER.schedule(this::backgroundResolve, delayMs, TimeUnit.MILLISECONDS);
        }
    }

    private void backgroundResolve() {
        List<ByteBuffer> batch;
        long gen;
        synchronized (this) {
            resolveScheduled = false;
            if (closed) {
                return;
            }
            applyUpTo(highWatermark.getAsLong());
            if (targetedPending) {
                return; // the move's re-derivation schedules a resolve when it is done
            }
            if (unresolved.isEmpty()) {
                if (!tail.queue.isEmpty()) {
                    // The high watermark has not passed the queue; back off rather than spin.
                    scheduleResolve(RESOLVE_WAIT_HW_MS);
                }
                return;
            }
            batch = new ArrayList<>(unresolved);
            unresolved.clear();
            gen = generation;
            inFlight++;
            resolving.add(batch);
        }
        try {
            resolveInChunks(batch, gen);
        } catch (IOException e) {
            // the keys not looked up went back to the unresolved ones
            LOG.warn("Derived DV background resolve failed, reads will resolve inline", e);
        } catch (RuntimeException e) {
            LOG.warn("Derived DV background resolve failed, reads will resolve inline", e);
            synchronized (this) {
                if (gen == generation) {
                    unresolved.addAll(batch); // looking a key up twice only sets its bit again
                }
            }
        } finally {
            synchronized (this) {
                resolving.remove(batch);
                inFlight--;
                closeDbIfDeferred();
                notifyAll();
                if (!closed) {
                    reportTail(false);
                    if (!tail.queue.isEmpty() || !unresolved.isEmpty()) {
                        scheduleResolve();
                    }
                }
            }
        }
    }

    /**
     * Looks up the lake positions of keys new to the tail outside the lock, in chunks of at most
     * {@link #RESOLVE_CHUNK} keys, and adds each chunk's positions to the masks as it completes.
     * Stops once the bucket closes or the state moves on, since the keys are moot then. If a lookup
     * fails, the keys not looked up go back to the unresolved ones. The caller counted itself in
     * {@link #inFlight}, which keeps the RocksDB open meanwhile.
     */
    private void resolveInChunks(List<ByteBuffer> batch, long gen) throws IOException {
        for (int from = 0; from < batch.size(); from += RESOLVE_CHUNK) {
            List<ByteBuffer> chunk =
                    batch.subList(from, Math.min(batch.size(), from + RESOLVE_CHUNK));
            if (closed) {
                return;
            }
            Runnable hook = beforeResolveLookup;
            if (hook != null) {
                hook.run();
            }
            List<FilePos> found;
            try {
                List<byte[]> keys = new ArrayList<>(chunk.size());
                for (ByteBuffer k : chunk) {
                    keys.add(k.array());
                }
                found = db.rowPosIndex().multiGetByKeys(keys);
                lookedUpKeys.addAndGet(keys.size());
            } catch (IOException e) {
                synchronized (this) {
                    if (gen == generation) {
                        unresolved.addAll(batch.subList(from, batch.size()));
                    }
                }
                throw e;
            }
            synchronized (this) {
                if (gen != generation) {
                    return;
                }
                for (FilePos fp : found) {
                    if (fp != null) {
                        lakeMasks
                                .computeIfAbsent(fp.fileId(), k -> new Roaring64Bitmap())
                                .addLong(fp.rowPosition());
                    }
                }
            }
        }
    }

    /** Applies the queued changes below {@code hw}; the caller holds the lock. */
    private void applyUpTo(long hw) {
        try {
            // the empty lake has no positions to look up
            tail.applyUpTo(hw, snapshotStartOffset, coveredSnapshotId >= 0 ? unresolved : null);
        } catch (IOException e) {
            tailDiskFailed(e);
        }
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
        default boolean chainTrimmed() throws Exception {
            return false;
        }

        /** This bucket's newest complete checkpoint at or before chain index upTo, or null. */
        @Nullable
        default PositionIndexStore.Checkpoint latestCheckpoint(
                List<PositionIndexStore.ChainEntry> chain, int upTo) throws Exception {
            return null;
        }

        /** Downloads a checkpoint: its SST (none for an empty index) and file ids. */
        default PositionIndexStore.BucketData readCheckpoint(PositionIndexStore.Checkpoint cp)
                throws Exception {
            throw new UnsupportedOperationException("no checkpoints");
        }

        /** Publishes this bucket's full index covering {@code snapshotId}. */
        default void uploadCheckpoint(
                long snapshotId, @Nullable String localSst, Map<Integer, String> fileDict)
                throws Exception {}
    }

    /** Receives one replayed change. */
    public interface ChangeConsumer {
        void accept(byte[] key, long recordOffset, long versionOffset);

        /**
         * Receives one replayed change; {@code insert} is set if the change inserted the key, which
         * had no live version before it.
         */
        default void accept(byte[] key, long recordOffset, long versionOffset, boolean insert) {
            accept(key, recordOffset, versionOffset);
        }
    }

    /**
     * Rebuilds the derived state of a new leader in the background. {@code limiter} bounds how many
     * buckets of the tablet server rebuild at once. The bucket takes writes meanwhile, and its
     * union reads and lookups are answered as not ready until the rebuild is done.
     */
    public void recoverInBackground(RecoverySource src, DerivedDvRebuildLimiter limiter) {
        synchronized (this) {
            rebuildLimiter = limiter;
            // From here the write path hands over nothing until a replay has built the tail.
            recoverySource = src;
        }
        limiter.execute(
                () -> {
                    try {
                        recover(src);
                    } catch (Exception e) {
                        LOG.warn(
                                "Derived DV recovery failed; the next union read or lookup of the"
                                        + " bucket rebuilds it.",
                                e);
                    }
                });
    }

    /**
     * Rebuilds the derived state on a new leader. Applies the published chain up to the readable
     * snapshot and replays the log from the snapshot's readable offset.
     */
    public void recover(RecoverySource src) throws Exception {
        advanceLock.lock();
        try {
            synchronized (this) {
                if (closed) {
                    return;
                }
                this.recoverySource = src;
            }
            long[] readable = src.readableSnapshot();
            if (readable == null) {
                // Nothing is tiered yet: cover the empty lake and replay the log on the first
                // union read. An empty lake tail that a union read already replayed is kept.
                synchronized (this) {
                    if (coveredSnapshotId >= 0 || !emptyLakeTracked) {
                        generation++;
                        coveredSnapshotId = -1;
                        coveredIdentity = null;
                        snapshotStartOffset = 0;
                        emptyLakeTracked = false;
                        tailFailure = null;
                        resetTail();
                    }
                }
                LOG.info(
                        "Derived DV recovery: no lake snapshot yet; covers the empty lake from offset"
                                + " 0 (the log is replayed on the first union read).");
                return;
            }
            long covered;
            synchronized (this) {
                covered = coveredSnapshotId;
            }
            if (covered >= 0) {
                // A union read or lookup rebuilt the state first; move on from there.
                ensureCovered(readable[0]);
                return;
            }
            rebuildTo(src, readable[0], readable[1], src.chain());
        } finally {
            advanceLock.unlock();
        }
    }

    /**
     * Rebuilds the state to {@code snapshotId}. Downloads its index and replays the log from {@code
     * startOffset} without the manager lock, then ingests the index and installs the tail under the
     * lock. Until then the covered snapshot keeps serving. Returns false if the manager was closed.
     * The caller holds advanceLock.
     */
    private boolean rebuildTo(
            RecoverySource src,
            long snapshotId,
            long startOffset,
            List<PositionIndexStore.ChainEntry> chain)
            throws Exception {
        if (closed) {
            // A catch-up that started before close() must not write into the closing RocksDB.
            return false;
        }
        int upTo = -1;
        for (int i = 0; i < chain.size(); i++) {
            if (chain.get(i).snapshotId == snapshotId) {
                upTo = i;
            }
        }
        if (upTo < 0) {
            throw new IllegalStateException(
                    "Derived DV recovery: readable snapshot "
                            + snapshotId
                            + " is not in the published index chain ("
                            + chain.size()
                            + " entries)");
        }
        int from = 0;
        PositionIndexStore.Checkpoint checkpoint = null;
        if (startOffset > 0) {
            checkpoint = src.latestCheckpoint(chain, upTo);
            if (checkpoint == null && src.chainTrimmed()) {
                // The trimmed chain is complete only if the bucket had nothing tiered at the oldest
                // kept entry (offsets are cumulative), e.g. a partition created after the trim.
                Long firstOffset =
                        chain.isEmpty() ? null : src.snapshotOffset(chain.get(0).snapshotId);
                if (firstOffset == null || firstOffset > 0) {
                    throw new IllegalStateException(
                            "Derived DV recovery: the index chain was trimmed but no checkpoint of "
                                    + "this bucket covers it; refusing to rebuild a partial index");
                }
            }
        }
        if (checkpoint != null) {
            from = checkpoint.chainIndex + 1;
        }
        DerivedDvRebuildLimiter limiter = startRebuild();
        List<PositionIndexStore.BucketData> index = new ArrayList<>();
        Replay replay = null;
        try {
            synchronized (this) {
                if (closed) {
                    return false;
                }
                if (coveredSnapshotId < 0 && emptyLakeTracked) {
                    // Drop the empty lake's tail (the whole log) now rather than hold it next to
                    // the replayed one. A union read of the empty lake is not ready until the
                    // rebuild is done, then gets stale and plans again on the snapshot.
                    generation++;
                    emptyLakeTracked = false;
                    resetTail();
                }
            }
            long t0 = System.currentTimeMillis();
            int sstFiles = 0;
            if (checkpoint != null) {
                index.add(src.readCheckpoint(checkpoint));
            }
            // Offset 0 means nothing of this bucket is tiered, so no index change holds its rows.
            for (int i = from; startOffset > 0 && i <= upTo; i++) {
                if (closed) {
                    return false;
                }
                index.add(src.readChange(chain.get(i))); // in commit order: later wins
            }
            for (PositionIndexStore.BucketData d : index) {
                sstFiles += d.localSsts.size();
            }
            long t1 = System.currentTimeMillis();
            // Replay into a queue: the local log can run past the high watermark, and a change
            // above it must not mask a version below it.
            replay = new Replay(src, startOffset);
            Exception replayFailure = replayUnlocked(replay);
            String identity = src.snapshotIdentity(chain.get(upTo));
            synchronized (this) {
                if (closed) {
                    return false;
                }
                if (replayFailure == null) {
                    replayFailure = replayRemainder(replay);
                }
                for (PositionIndexStore.BucketData d : index) {
                    apply(d);
                }
                coveredIdentity = identity;
                generation++;
                markPassed(coveredSnapshotId);
                for (int i = 0; i < upTo; i++) {
                    markPassed(chain.get(i).snapshotId);
                }
                passed.remove(snapshotId);
                coveredSnapshotId = snapshotId;
                snapshotStartOffset = startOffset;
                entriesApplied = upTo + 1;
                entriesAtCheckpoint = checkpoint == null ? 0 : checkpoint.chainIndex + 1;
                if (replayFailure != null) {
                    // the index stays, union reads retry the replay and fail until it succeeds
                    resetTail();
                    tailFailure =
                            isDiskFailure(replayFailure)
                                    ? new TailDiskException(replayFailure)
                                    : replayFailure;
                    reportTail(true);
                    LOG.warn(
                            "Derived DV recovery: covers snapshot {} but cannot replay the log from"
                                    + " offset {}; union reads fail until a replay succeeds.",
                            snapshotId,
                            startOffset,
                            replayFailure);
                    return true;
                }
                installTail(replay);
                maybeCheckpoint();
                metrics.rebuilt(System.currentTimeMillis() - t0);
                LOG.info(
                        "Derived DV recovery: covers snapshot {} from offset {}; replayed {} index"
                                + " changes ({} SSTs{}) in {} ms and the log tail ({} changes, to"
                                + " offset {}) in {} ms.",
                        snapshotId,
                        startOffset,
                        upTo + 1 - from,
                        sstFiles,
                        checkpoint == null ? "" : ", from checkpoint " + checkpoint.snapshotId,
                        t1 - t0,
                        tail.changeCount(),
                        replay.next,
                        System.currentTimeMillis() - t1);
                return true;
            }
        } finally {
            if (replay != null) {
                replay.finish();
            }
            // deletes what was not ingested; ingestion moves the ingested files away
            deleteDownloads(index);
            endRebuild(limiter);
        }
    }

    /** Takes a rebuild slot of the tablet server, waiting for one if all are taken. */
    private DerivedDvRebuildLimiter startRebuild() throws InterruptedIOException {
        DerivedDvRebuildLimiter limiter = rebuildLimiter;
        try {
            limiter.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("Interrupted while waiting for a DV rebuild slot");
        }
        metrics.rebuildStarted();
        return limiter;
    }

    private void endRebuild(DerivedDvRebuildLimiter limiter) {
        metrics.rebuildEnded();
        limiter.release();
    }

    /** A log replay into a fresh tail, which readers see only once it is installed. */
    private final class Replay implements ChangeConsumer, DerivedDvTailBudget.Owner {
        private final RecoverySource src;
        private final long startOffset;
        private final DvTail tail = new DvTail();
        // The last version of each key replayed, to name the version a replayed change supersedes,
        // as the log does not. In memory until the budget asks the replay to spill; then on disk
        // under versionsId, and the changes wait in unversioned to be named a batch at a time.
        @Nullable private HashMap<ByteBuffer, Long> lastVersions = new HashMap<>();
        private long versionsId = -1;
        private final List<DvEntry> unversioned = new ArrayList<>();
        private long lastVersionBytes;
        // where the replay continues
        private long next;
        private int sinceProgress = 0;
        private long reportedBacklog = 0;
        // the budget asked the replay to spill its tail; it does at its next batch
        private volatile boolean spillRequested;
        // the spilled tail uses the RocksDB, so the bucket does not close it meanwhile
        private boolean usesDb;
        // the tail became the bucket's
        private boolean installed;
        private long batches;
        private long bitmapBytes;

        Replay(RecoverySource src, long startOffset) {
            this.src = src;
            this.startOffset = startOffset;
            this.next = startOffset;
            reportBacklog(startOffset);
            replaying = this;
        }

        @Override
        public void accept(byte[] key, long recordOffset, long versionOffset) {
            accept(key, recordOffset, versionOffset, false);
        }

        @Override
        public void accept(byte[] key, long recordOffset, long versionOffset, boolean insert) {
            if (versionsId >= 0 || spillReplayVersions) {
                if (versionsId < 0) {
                    spillVersions();
                }
                // the map is on disk: the superseded versions are named a batch at a time
                unversioned.add(new DvEntry(key, recordOffset, versionOffset, insert, -1));
                if (unversioned.size() >= REPLAY_APPLY_BATCH) {
                    nameVersions();
                }
                progress(recordOffset);
                return;
            }
            // -1: the superseded version is before the replay's start, so in the lake
            Long last = lastVersions.put(ByteBuffer.wrap(key), versionOffset);
            if (last == null) {
                // a map node, a ByteBuffer, a Long and the key array
                lastVersionBytes += 112 + ((16 + key.length + 7) & ~7L);
            }
            enqueue(
                    new DvEntry(
                            key, recordOffset, versionOffset, insert, last == null ? -1 : last));
            progress(recordOffset);
        }

        /**
         * Names the version each waiting change supersedes from the spilled map, reading the
         * changes' keys in one lookup and writing their new last versions back together.
         */
        private void nameVersions() {
            if (unversioned.isEmpty()) {
                return;
            }
            try {
                Map<ByteBuffer, Long> last = new HashMap<>();
                List<byte[]> distinct = new ArrayList<>();
                for (DvEntry e : unversioned) {
                    if (last.putIfAbsent(ByteBuffer.wrap(e.getKey()), -1L) == null) {
                        distinct.add(e.getKey());
                    }
                }
                long[] read = db.tailStore().getVersions(versionsId, distinct);
                for (int i = 0; i < distinct.size(); i++) {
                    last.put(ByteBuffer.wrap(distinct.get(i)), read[i]);
                }
                List<DvEntry> named = new ArrayList<>(unversioned.size());
                for (DvEntry e : unversioned) {
                    ByteBuffer k = ByteBuffer.wrap(e.getKey());
                    named.add(
                            new DvEntry(
                                    e.getKey(),
                                    e.getRecordOffset(),
                                    e.getVersionOffset(),
                                    e.isInsert(),
                                    last.put(k, e.getVersionOffset())));
                }
                db.tailStore().putVersions(versionsId, last);
                unversioned.clear();
                for (DvEntry e : named) {
                    enqueue(e);
                }
            } catch (IOException e) {
                throw new TailDiskException(e);
            }
        }

        /** Moves the map of last versions to the bucket's RocksDB. Runs on the replay's thread. */
        private void spillVersions() {
            synchronized (DerivedDvManager.this) {
                if (closed) {
                    throw new IllegalStateException("Derived DV manager is closed");
                }
                useDb();
                versionsId = ++tailIds;
            }
            try {
                Map<ByteBuffer, Long> batch = new HashMap<>();
                for (Map.Entry<ByteBuffer, Long> e : lastVersions.entrySet()) {
                    batch.put(e.getKey(), e.getValue());
                    if (batch.size() >= REPLAY_APPLY_BATCH) {
                        db.tailStore().putVersions(versionsId, batch);
                        batch.clear();
                    }
                }
                db.tailStore().putVersions(versionsId, batch);
            } catch (IOException e) {
                throw new TailDiskException(e);
            }
            lastVersions = null;
            lastVersionBytes = 0;
            LOG.info(
                    "Derived DV: a replay of the log from offset {} spilled its map of last versions"
                            + " to disk, as the log tails of the tablet server exceed {}.",
                    startOffset,
                    ConfigOptions.KV_DELETION_VECTORS_TAIL_MEMORY.key());
        }

        private void enqueue(DvEntry entry) {
            tail.enqueue(entry);
            // A lake far behind the log can leave tens of millions of changes to replay. They are
            // applied once the high watermark passes them, so only the part above it waits in the
            // queue; the applied ones can spill to disk.
            if (tail.queue.size() >= REPLAY_APPLY_BATCH) {
                try {
                    tail.applyUpTo(highWatermark.getAsLong(), startOffset, null);
                    if (spillRequested && !tail.spilled()) {
                        spillTail();
                    }
                    if (spillRequested && lastVersions != null && !lastVersions.isEmpty()) {
                        spillVersions();
                    }
                } catch (IOException e) {
                    throw new TailDiskException(e);
                }
                // its changes count against the tablet server's tail memory as they grow
                if (batches++ % BITMAP_MEASURE_INTERVAL == 0) {
                    bitmapBytes = tail.deadVersions.getLongSizeInBytes();
                }
                tailBudget.update(
                        this,
                        tail.heapBytes() + bitmapBytes + lastVersionBytes,
                        tail.pendingHeapBytes() + lastVersionBytes);
            }
        }

        private void progress(long recordOffset) {
            if (++sinceProgress >= REPLAY_APPLY_BATCH) {
                sinceProgress = 0;
                if (closed) {
                    throw new IllegalStateException("Derived DV manager is closed");
                }
                reportBacklog(recordOffset + 1);
            }
        }

        /** Replays from where the replay stopped to the current log end, which writes move on. */
        void replayToLogEnd() throws Exception {
            next = src.replayLog(next, src.logEndOffset(), this);
            // the changes still waiting for their superseded versions, before the tail is used
            nameVersions();
            reportBacklog(next);
        }

        long behindLogEnd() {
            return src.logEndOffset() - next;
        }

        private void reportBacklog(long position) {
            long backlog = Math.max(0, src.logEndOffset() - position);
            metrics.replayBacklogChanged(backlog - reportedBacklog);
            reportedBacklog = backlog;
        }

        @Override
        public boolean spill() {
            if (tail.spilled() && (lastVersions == null || lastVersions.isEmpty())) {
                // nothing left in memory to spill
                return false;
            }
            spillRequested = true;
            return true;
        }

        /** Counts the replay as a user of the RocksDB once; the caller holds the lock. */
        private void useDb() {
            if (!usesDb) {
                backgroundReaders++;
                usesDb = true;
            }
        }

        /** Spills the replayed changes to the bucket's RocksDB. Runs on the replay's thread. */
        private void spillTail() throws IOException {
            long tailId;
            synchronized (DerivedDvManager.this) {
                if (closed) {
                    throw new IllegalStateException("Derived DV manager is closed");
                }
                useDb();
                tailId = ++tailIds;
            }
            tail.spill(db.tailStore(), tailId);
            LOG.info(
                    "Derived DV: a replay of the log from offset {} spilled its tail to disk, as"
                            + " the log tails of the tablet server exceed {}.",
                    startOffset,
                    ConfigOptions.KV_DELETION_VECTORS_TAIL_MEMORY.key());
        }

        void finish() {
            metrics.replayBacklogChanged(-reportedBacklog);
            reportedBacklog = 0;
            tailBudget.remove(this);
            if (replaying == this) {
                replaying = null;
            }
            if (usesDb) {
                synchronized (DerivedDvManager.this) {
                    if (!dbClosed) {
                        try {
                            if (!installed) {
                                tail.drop();
                            }
                            // the map of last versions is the replay's own
                            if (versionsId >= 0) {
                                db.tailStore().drop(versionsId);
                            }
                        } catch (IOException e) {
                            LOG.warn("Derived DV: could not drop a spilled log tail.", e);
                        }
                    }
                }
                releaseDb();
            }
        }
    }

    /**
     * Replays the log from the replay's start without the manager lock. Makes further passes while
     * writes keep the log end far ahead. Returns the failure, or null.
     */
    @Nullable
    private Exception replayUnlocked(Replay replay) {
        try {
            replay.replayToLogEnd();
            for (int pass = 1;
                    pass < MAX_UNLOCKED_REPLAY_PASSES
                            && replay.behindLogEnd() > LOCKED_REPLAY_MAX_OFFSETS;
                    pass++) {
                replay.replayToLogEnd();
            }
            return null;
        } catch (Exception e) {
            return e;
        }
    }

    /**
     * Replays what was written after the unlocked passes. The caller holds the manager lock, so the
     * write path hands over nothing meanwhile. Every change appended so far is replayed here, and
     * every later one is queued after the tail is installed. Returns the failure, or null.
     */
    @Nullable
    private Exception replayRemainder(Replay replay) {
        try {
            if (replay.behindLogEnd() > 0) {
                replay.replayToLogEnd();
            }
            return null;
        } catch (Exception e) {
            return e;
        }
    }

    /** Makes a complete replay the tail of the covered snapshot; the caller holds the lock. */
    private void installTail(Replay replay) {
        dropTail();
        tail = replay.tail;
        replay.installed = true;
        tailFailure = null;
        lastReplayFailure = null;
        replayBackoffMs = REPLAY_RETRY_INITIAL_BACKOFF_MS;
        lakeMasks.clear();
        unresolved.clear();
        fullResolvePending = false;
        targetedPending = false;
        // the replay read the log up to here under this lock, so the tail has every change below it
        handedOffEnd = Math.max(handedOffEnd, replay.next);
        applyUpTo(highWatermark.getAsLong());
        if (coveredSnapshotId >= 0 && !tailMissing()) {
            // the empty lake has no positions to look up
            unresolved.clear();
            if (!tail.spilled()) {
                unresolved.addAll(tail.lakeKeys(snapshotStartOffset));
            } else {
                // a spilled tail is looked up from disk, in chunks, before masks are served
                fullResolvePending = true;
            }
        }
        reportTail(true);
        scheduleResolve();
    }

    /**
     * Reports the tail's heap bytes (pending and queued changes, keys waiting for lake positions,
     * mask bitmaps) to the tablet server's budget and metrics, and the bytes spilled to disk to the
     * metrics. The bitmaps are measured every few reports, or now if {@code measureBitmaps}. The
     * caller holds the lock.
     */
    private void reportTail(boolean measureBitmaps) {
        if (measureBitmaps || ++reportsSinceBitmaps >= BITMAP_MEASURE_INTERVAL) {
            reportsSinceBitmaps = 0;
            long bytes = tail.deadVersions.getLongSizeInBytes();
            for (Roaring64Bitmap mask : lakeMasks.values()) {
                bytes += mask.getLongSizeInBytes();
            }
            bitmapBytes = bytes;
        }
        long memory = closed ? 0 : tail.heapBytes() + bitmapBytes + unresolvedBytes();
        boolean spilled = !closed && tail.spilled();
        long spilledBytes = spilled ? tail.spilledBytes() : 0;
        metrics.tailMemoryChanged(memory - reportedMemory);
        metrics.spilledTailBytesChanged(spilledBytes - reportedSpilledBytes);
        if (spilled != reportedSpilled) {
            metrics.spilledTailsChanged(spilled ? 1 : -1);
        }
        reportedMemory = memory;
        reportedSpilledBytes = spilledBytes;
        reportedSpilled = spilled;
        tailBudget.update(tailOwner, memory, closed ? 0 : tail.pendingHeapBytes());
    }

    /**
     * Estimated heap bytes of the keys waiting for their lake positions: a list slot each, as the
     * keys belong to the changes. The keys of a spilled tail's changes stay in memory only until
     * the resolver, a few milliseconds behind the writes, looks them up. Counting those would spill
     * more tails for memory that is freed right after.
     */
    private long unresolvedBytes() {
        return unresolved.size() * 8L;
    }

    /** Removes a spilled tail from disk, unless the RocksDB is closed; holds the lock. */
    private void dropTail() {
        if (tail.spilled() && !dbClosed) {
            try {
                tail.drop();
            } catch (IOException e) {
                // cleared when the RocksDB opens again
                LOG.warn("Derived DV: could not drop a spilled log tail.", e);
            }
        }
    }

    /** Whether a failure is one of the local disk keeping a spilled tail. */
    private static boolean isDiskFailure(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof TailDiskException) {
                return true;
            }
        }
        return false;
    }

    /** Removes the local files of index data that was downloaded but not ingested. */
    private static void deleteDownloads(List<PositionIndexStore.BucketData> index) {
        for (PositionIndexStore.BucketData d : index) {
            deleteFiles(d.localSsts);
        }
    }

    /**
     * Starts an export of the covered index every checkpointInterval chain entries, after a random
     * delay, so the buckets that catch up to one snapshot together do not export at once.
     */
    private void maybeCheckpoint() {
        if (recoverySource == null
                || closed
                || checkpointing
                || coveredSnapshotId < 0
                || entriesApplied - entriesAtCheckpoint < checkpointInterval) {
            return;
        }
        checkpointing = true;
        long delayMs =
                checkpointJitterMs <= 0
                        ? 0
                        : java.util.concurrent.ThreadLocalRandom.current()
                                .nextLong(checkpointJitterMs);
        try {
            checkpointer.schedule(this::startCheckpoint, delayMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            // e.g. the executor shuts down
            checkpointing = false;
            LOG.warn(
                    "Derived DV: index checkpoint at snapshot {} not started ({}); retried after"
                            + " the next index change.",
                    coveredSnapshotId,
                    e.toString());
        }
    }

    /**
     * Takes the RocksDB snapshot of a checkpoint when its export starts, and runs the export. The
     * export has the index the bucket covers at that point, which may be newer than when the
     * checkpoint was due. The index changes only under the manager lock, together with the covered
     * snapshot, so the RocksDB snapshot is exactly that index. A bucket that advances more often
     * than the delay therefore still checkpoints.
     */
    private void startCheckpoint() {
        RecoverySource src;
        long snapshotId;
        long applied;
        io.github.fluss_contrib.rocksdb.Snapshot view;
        synchronized (this) {
            src = recoverySource;
            if (closed || src == null || coveredSnapshotId < 0) {
                checkpointing = false;
                notifyAll();
                return;
            }
            snapshotId = coveredSnapshotId;
            applied = entriesApplied;
            // The snapshot uses the RocksDB until it is released, so a closing tablet waits.
            backgroundReaders++;
            view = db.snapshot();
        }
        checkpoint(src, snapshotId, applied, view);
    }

    /** Exports and uploads a checkpoint of the index as of {@code view}, then releases it. */
    private void checkpoint(
            RecoverySource src,
            long snapshotId,
            long applied,
            io.github.fluss_contrib.rocksdb.Snapshot view) {
        long t0 = System.currentTimeMillis();
        File tmp = null;
        try {
            Map<Integer, String> dict;
            long n;
            try {
                if (closed) {
                    return;
                }
                // on the tablet's disk, which is sized for the index, not the JVM's temp dir
                tmp = Files.createTempFile(db.tmpDir().toPath(), "ckpt-", ".sst").toFile();
                tmp.delete();
                dict = db.fileDict().idToPathEntries(view);
                // Leaves out entries of dropped files; a closing tablet stops the export.
                n = db.rowPosIndex().exportTo(tmp.getPath(), view, dict::containsKey, () -> closed);
            } finally {
                // Released before the upload, which can take long on an object store and needs
                // only the exported file.
                db.releaseSnapshot(view);
                releaseDb();
            }
            if (closed) {
                return;
            }
            src.uploadCheckpoint(snapshotId, n > 0 ? tmp.getPath() : null, dict);
            synchronized (DerivedDvManager.this) {
                entriesAtCheckpoint = applied;
            }
            metrics.checkpointed(System.currentTimeMillis() - t0);
            LOG.info(
                    "Derived DV: checkpointed the position index at snapshot {} "
                            + "({} entries, {} files) in {} ms.",
                    snapshotId,
                    n,
                    dict.size(),
                    System.currentTimeMillis() - t0);
        } catch (Exception e) {
            if (closed) {
                LOG.info(
                        "Derived DV: index checkpoint at snapshot {} stopped as the"
                                + " bucket closed.",
                        snapshotId);
            } else {
                LOG.warn(
                        "Derived DV: index checkpoint at snapshot {} failed; retried "
                                + "after the next index change.",
                        snapshotId,
                        e);
            }
        } finally {
            if (tmp != null) {
                tmp.delete();
            }
            synchronized (DerivedDvManager.this) {
                checkpointing = false;
                notifyAll();
            }
        }
    }

    /**
     * Stops background work and closes the DV RocksDB once nothing uses it outside the lock: a
     * resolve, a lookup, a checkpoint's export or a replay's spilled tail. Those end quickly once
     * {@code closed} is set. If one does not end within the wait, the last of them closes the
     * RocksDB, so it is never closed under an iterator or snapshot. {@link #dbClosed} completes
     * once it is closed: the RocksDB shares the tablet server's block cache and write buffer
     * manager, which must outlive it.
     */
    @Override
    public synchronized void close() {
        closed = true;
        reportTail(true);
        long deadline = System.currentTimeMillis() + CLOSE_WAIT_MS;
        boolean interrupted = false;
        // an interrupt does not cut the wait short: the RocksDB would outlive the close
        while (dbInUse() && System.currentTimeMillis() < deadline) {
            try {
                wait(Math.max(1, Math.min(500, deadline - System.currentTimeMillis())));
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        if (dbInUse()) {
            dbCloseDeferred = true;
            LOG.warn(
                    "Derived DV: {} background readers still use the deletion vector RocksDB;"
                            + " the last of them closes it.",
                    inFlight + backgroundReaders);
            return;
        }
        closeDb();
    }

    /** Whether a thread reads the DV RocksDB outside the lock. */
    private boolean dbInUse() {
        return inFlight > 0 || backgroundReaders > 0;
    }

    /** Ends a {@link #backgroundReaders} use of the RocksDB. */
    private synchronized void releaseDb() {
        backgroundReaders--;
        closeDbIfDeferred();
        notifyAll();
    }

    /** Closes the RocksDB if close() left that to the last background reader; holds the lock. */
    private void closeDbIfDeferred() {
        if (dbCloseDeferred && !dbInUse()) {
            closeDb();
        }
    }

    private void closeDb() {
        if (!dbClosed) {
            dbClosed = true;
            try {
                db.close();
            } finally {
                dbClosedFuture.complete(null);
            }
        }
    }

    /**
     * Completes once the DV RocksDB is closed, by {@link #close} or by the last background user
     * after it.
     */
    public CompletableFuture<Void> dbClosed() {
        return dbClosedFuture;
    }

    /**
     * Removes files that left the lake snapshot from the dictionary, so no lookup or mask resolves
     * into them. File ids are never reused; checkpoints leave out their stale index entries.
     */
    private void dropFiles(List<String> fileNames) throws IOException {
        for (String name : fileNames) {
            int id = db.fileDict().getFileId(name);
            if (id >= 0) {
                db.fileDict().delete(id);
                lakeMasks.remove(id);
            }
        }
    }

    /**
     * Applies an index change or a checkpoint and returns the number of SSTs ingested. A change
     * that removes files is a rewrite, which must not move a key that a newer change moved already.
     */
    private int apply(PositionIndexStore.BucketData change) throws IOException {
        Set<Integer> inputs = new HashSet<>();
        for (String name : change.removedFiles) {
            int id = db.fileDict().getFileId(name);
            if (id >= 0) {
                inputs.add(id);
            }
        }
        int newestAdded = -1;
        for (Map.Entry<Integer, String> e : change.fileId2Name.entrySet()) {
            db.fileDict().put(e.getKey(), e.getValue());
            newestAdded = Math.max(newestAdded, e.getKey());
        }
        if (!change.localSsts.isEmpty()) {
            if (change.removedFiles.isEmpty()) {
                db.rowPosIndex().ingestExternalFile(change.localSsts);
            } else {
                // File ids grow with every commit, so a file newer than all the rewrite added
                // comes from a later change. Any other current file may be a rewrite input: a
                // Paimon compaction also moves rows of level-0 files, which are not indexed and
                // not listed as removed.
                final int newest = newestAdded;
                long skipped =
                        db.rowPosIndex()
                                .ingestRewrite(
                                        change.localSsts,
                                        current ->
                                                current == null
                                                        || current.fileId() <= newest
                                                        || inputs.contains(current.fileId()),
                                        db.tmpDir());
                if (skipped > 0) {
                    LOG.warn(
                            "Derived DV: a rewrite of {} would move {} keys out of files that a"
                                    + " later change added; they keep their position. The index"
                                    + " changes were applied out of order or twice.",
                            change.removedFiles,
                            skipped);
                }
            }
        }
        dropFiles(change.removedFiles);
        return change.localSsts.size();
    }

    /** The error for a snapshot this bucket does not cover; the caller re-plans on a newer one. */
    private StaleSnapshotException stale(long requestedSnapshotId) {
        metrics.staleSnapshot();
        return new StaleSnapshotException(coveredSnapshotId, requestedSnapshotId);
    }

    /** Drops the tail and the masks derived from it. */
    private void resetTail() {
        dropTail();
        tail = new DvTail();
        lakeMasks.clear();
        unresolved.clear();
        fullResolvePending = false;
        targetedPending = false;
    }

    /**
     * Replays the log tail of the covered snapshot if union reads need it: for the empty lake on
     * its first union read (the whole log), or again after a failed replay (the index is in place).
     * The replay runs without the manager lock and is installed only once complete, so a replay
     * that fails part-way leaves nothing behind.
     */
    private void replayTailIfMissing(long snapshotId) throws IOException {
        advanceLock.lock();
        try {
            long startOffset;
            RecoverySource src;
            synchronized (this) {
                if (closed
                        || coveredSnapshotId != snapshotId
                        || recoverySource == null
                        || !tailMissing()) {
                    return; // the union read answers
                }
                src = recoverySource;
                startOffset = snapshotStartOffset;
            }
            DerivedDvRebuildLimiter limiter = startRebuild();
            Replay replay = null;
            try {
                long t0 = System.currentTimeMillis();
                replay = new Replay(src, startOffset);
                Exception failure = replayUnlocked(replay);
                synchronized (this) {
                    if (closed) {
                        return;
                    }
                    if (failure == null) {
                        failure = replayRemainder(replay);
                    }
                    if (failure != null) {
                        if (isDiskFailure(failure)) {
                            tailFailure = new TailDiskException(failure);
                            replayRetryAtMs = System.currentTimeMillis() + replayBackoffMs;
                            replayBackoffMs =
                                    Math.min(2 * replayBackoffMs, REPLAY_RETRY_MAX_BACKOFF_MS);
                            throw new IOException(failure.getMessage(), tailFailure);
                        }
                        if (coveredSnapshotId < 0) {
                            throw new IOException(
                                    "Derived DV: no lake snapshot yet and the log cannot be"
                                            + " replayed from offset 0",
                                    failure);
                        }
                        tailFailure = failure;
                        throw new IOException(
                                String.format(
                                        "Derived DV: cannot replay the log of snapshot %d from"
                                                + " offset %d, so no masks are served",
                                        coveredSnapshotId, startOffset),
                                failure);
                    }
                    generation++;
                    installTail(replay);
                    if (coveredSnapshotId < 0) {
                        emptyLakeTracked = true;
                    }
                    metrics.rebuilt(System.currentTimeMillis() - t0);
                    LOG.info(
                            "Derived DV: replayed the log of {} from offset {} for a union read"
                                    + " ({} tail changes, {} changes above the high watermark, to"
                                    + " offset {}) in {} ms.",
                            coveredSnapshotId < 0
                                    ? "the empty lake"
                                    : "snapshot " + coveredSnapshotId,
                            startOffset,
                            tail.changeCount(),
                            tail.queue.size(),
                            replay.next,
                            System.currentTimeMillis() - t0);
                }
            } finally {
                if (replay != null) {
                    replay.finish();
                }
                endRebuild(limiter);
            }
        } finally {
            advanceLock.unlock();
        }
    }

    private void markPassed(long snapshotId) {
        if (snapshotId >= 0 && passed.add(snapshotId) && passed.size() > PASSED_LIMIT) {
            Iterator<Long> it = passed.iterator();
            it.next();
            it.remove();
        }
    }

    private static int chainIndex(List<PositionIndexStore.ChainEntry> chain, long snapshotId) {
        for (int i = chain.size() - 1; i >= 0; i--) {
            if (chain.get(i).snapshotId == snapshotId) {
                return i;
            }
        }
        return -1;
    }

    /** Starts a background catch-up to a newly registered snapshot. */
    public void advanceAsync(long snapshotId) {
        synchronized (this) {
            if (recoverySource == null
                    || coveredSnapshotId == snapshotId
                    || passed.contains(snapshotId)) {
                return;
            }
        }
        runInBackground(() -> advanceInBackground(snapshotId, -1, BUSY_RETRY_INITIAL_MS), 0);
    }

    /**
     * Catches up to {@code snapshotId} on a catch-up thread without waiting. If the bucket is busy
     * with a rebuild or replay, the catch-up is tried again later. A full rebuild goes to the
     * rebuild limiter's threads. {@code attempt} numbers the request-started catch-up, or is -1.
     */
    private void advanceInBackground(long snapshotId, long attempt, long busyRetryMs) {
        Advance result;
        Exception failure = null;
        try {
            result = tryAdvance(snapshotId);
        } catch (Exception e) {
            result = Advance.NOT_COVERED;
            failure = e;
            LOG.warn("Derived DV catch-up to snapshot {} failed.", snapshotId, e);
        }
        if (result == Advance.BUSY) {
            synchronized (this) {
                if (closed || coveredSnapshotId == snapshotId || passed.contains(snapshotId)) {
                    advanceDone(snapshotId, attempt, coveredSnapshotId == snapshotId, null);
                    return;
                }
            }
            runInBackground(
                    () ->
                            advanceInBackground(
                                    snapshotId,
                                    attempt,
                                    Math.min(2 * busyRetryMs, BUSY_RETRY_MAX_MS)),
                    busyRetryMs);
            return;
        }
        if (result == Advance.REBUILD) {
            rebuildLimiter.execute(
                    () -> {
                        boolean covered = false;
                        Exception rebuildFailure = null;
                        try {
                            covered = ensureCovered(snapshotId);
                        } catch (Exception e) {
                            rebuildFailure = e;
                            LOG.warn("Derived DV rebuild to snapshot {} failed.", snapshotId, e);
                        }
                        advanceDone(snapshotId, attempt, covered, rebuildFailure);
                    });
            return;
        }
        advanceDone(snapshotId, attempt, result == Advance.COVERED, failure);
    }

    private void runInBackground(Runnable task, long delayMs) {
        try {
            ADVANCER.schedule(task, delayMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            LOG.warn("Derived DV: a catch-up could not be scheduled ({}).", e.toString());
        }
    }

    /** Catches up unless the bucket is busy with a rebuild, replay or catch-up. */
    private Advance tryAdvance(long snapshotId) throws Exception {
        if (!advanceLock.tryLock()) {
            return Advance.BUSY;
        }
        try {
            return advance(snapshotId, true);
        } finally {
            advanceLock.unlock();
        }
    }

    /**
     * Advances to {@code snapshotId} from the published index chain if it is newer than the covered
     * one. Returns whether {@code snapshotId} is covered now.
     */
    public boolean ensureCovered(long snapshotId) throws Exception {
        advanceLock.lock();
        try {
            return advance(snapshotId, false) == Advance.COVERED;
        } finally {
            advanceLock.unlock();
        }
    }

    /**
     * Advances to {@code snapshotId}. The caller holds advanceLock. With {@code background}, a
     * catch-up that needs a full rebuild returns {@link Advance#REBUILD} instead of running it.
     */
    private Advance advance(long snapshotId, boolean background) throws Exception {
        {
            RecoverySource src;
            long from;
            long fromOffset;
            synchronized (this) {
                if (coveredSnapshotId == snapshotId) {
                    return Advance.COVERED;
                }
                if (recoverySource == null || passed.contains(snapshotId)) {
                    return Advance.NOT_COVERED;
                }
                src = recoverySource;
                from = coveredSnapshotId;
                fromOffset = snapshotStartOffset;
            }
            Long offset = src.snapshotOffset(snapshotId);
            if (offset == null) {
                return Advance.NOT_COVERED; // not registered as readable yet
            }
            List<PositionIndexStore.ChainEntry> chain = src.chain();
            int target = chainIndex(chain, snapshotId);
            if (target < 0) {
                return Advance.NOT_COVERED; // chain marker not visible yet
            }
            int start = from < 0 ? -1 : chainIndex(chain, from);
            // The tail holds changes from the covered snapshot's offset only; a readable offset
            // below it (a lake whose readable offsets lag its commits) needs the log replayed.
            if (start < 0 || (target > start && offset < fromOffset)) {
                // Full rebuild. It checks close() in the same lock hold that writes, so a closed
                // tablet is never written and a shutdown is not reported as a failure. It waits
                // for a rebuild slot, so a catch-up thread hands it to the rebuild limiter.
                if (background) {
                    return Advance.REBUILD;
                }
                return rebuildTo(src, snapshotId, offset, chain)
                        ? Advance.COVERED
                        : Advance.NOT_COVERED;
            }
            if (target < start) {
                return Advance.NOT_COVERED; // older than the covered snapshot
            }
            long t0 = System.currentTimeMillis();
            // Downloads run outside the lock so writes are not blocked.
            List<PositionIndexStore.BucketData> changes = new ArrayList<>();
            int sstFiles = 0;
            try {
                for (int i = start + 1; i <= target; i++) {
                    PositionIndexStore.BucketData c = src.readChange(chain.get(i));
                    changes.add(c);
                    sstFiles += c.localSsts.size();
                }
            } catch (Exception e) {
                deleteDownloads(changes);
                throw e;
            }
            String identity = src.snapshotIdentity(chain.get(target));
            // the keys the changes name, and their positions in the covered snapshot's index,
            // which only this catch-up changes
            TargetedMove move = targetedMoves ? prepareTargetedMove(changes, offset) : null;
            long tailChanges;
            synchronized (this) {
                if (closed) {
                    deleteDownloads(changes);
                    return Advance.NOT_COVERED;
                }
                if (coveredSnapshotId != from) {
                    // Moved concurrently by a rebuild; the next call starts over.
                    deleteDownloads(changes);
                    return coveredSnapshotId == snapshotId ? Advance.COVERED : Advance.NOT_COVERED;
                }
                // the changes below the high watermark join the tail before the move
                applyUpTo(highWatermark.getAsLong());
                if (move != null && targetable()) {
                    // the index and the spilled tail as they are before the move
                    backgroundReaders++;
                    move.pre = db.snapshot();
                } else {
                    move = null;
                }
                try {
                    for (PositionIndexStore.BucketData c : changes) {
                        apply(c); // commit order
                    }
                } catch (IOException | RuntimeException e) {
                    if (move != null) {
                        db.releaseSnapshot(move.pre);
                        releaseDb();
                    }
                    throw e;
                }
                for (int i = start + 1; i < target; i++) {
                    markPassed(chain.get(i).snapshotId);
                }
                advanceTo(snapshotId, offset, move);
                coveredIdentity = identity;
                entriesApplied += target - start;
                maybeCheckpoint();
                tailChanges = tail.changeCount();
            }
            if (move != null) {
                completeTargetedMove(move);
            }
            metrics.caughtUp(System.currentTimeMillis() - t0);
            LOG.info(
                    "Derived DV caught up from snapshot {} to {} on its own: {} index changes "
                            + "({} SSTs) in {} ms; {} tail changes left; {}.",
                    from,
                    snapshotId,
                    target - start,
                    sstFiles,
                    System.currentTimeMillis() - t0,
                    tailChanges,
                    move == null
                            ? "every lake key looked up again"
                            : String.format(
                                    "masks kept but for the %d keys the changes name (%d"
                                            + " positions removed, %d keys looked up again)",
                                    move.changeKeys, move.removed, move.lookedUpAgain));
            return Advance.COVERED;
        }
    }

    /**
     * Covers {@code snapshotId} from {@code readableOffset}. Call after ingesting its index
     * changes; the caller holds the lock. The changes the new snapshot holds leave the tail, and
     * the masks are derived again from the rest: the keys whose superseded version the new snapshot
     * holds are looked up in its index. A spilled tail's changes are read from disk for that in the
     * background, and no masks are served until then.
     */
    private void advanceTo(long snapshotId, long readableOffset, @Nullable TargetedMove move) {
        long previousStart = snapshotStartOffset;
        generation++;
        markPassed(coveredSnapshotId);
        coveredSnapshotId = snapshotId;
        snapshotStartOffset = readableOffset;
        lastReplayFailure = null;
        unreachable = Long.MIN_VALUE;
        fullResolvePending = false;
        if (move != null) {
            // The keys resolves were to look up, or look up now: their lookups of the old index
            // are dropped, and they are looked up again if they are still lake keys.
            move.pendingKeys.addAll(unresolved);
            for (List<ByteBuffer> batch : resolving) {
                move.pendingKeys.addAll(batch);
            }
            move.oldStart = previousStart;
            move.newStart = readableOffset;
            move.gen = generation;
            if (!tail.spilled()) {
                move.dropped = tail.lakeKeysBefore(readableOffset, previousStart);
            } else {
                move.tailId = tail.id;
            }
            targetedPending = true;
        } else {
            lakeMasks.clear();
        }
        unresolved.clear();
        try {
            // one range delete for a spilled tail
            tail.retainFrom(readableOffset);
        } catch (IOException e) {
            tailDiskFailed(e);
            return;
        }
        if (move == null) {
            if (tail.spilled()) {
                fullResolvePending = true;
            } else {
                unresolved.addAll(tail.lakeKeys(readableOffset));
            }
        }
        reportTail(true);
        scheduleResolve();
    }

    /**
     * What a move to a newer snapshot derives again. A key the move's index changes do not name
     * keeps its row: the index of the new snapshot is the old one plus the changes. Nor did the key
     * change in the log the new snapshot tiered, or that change would name the version the snapshot
     * holds and the key would be in the changes; so none of its pending changes left the tail, and
     * its lake mask stays valid. Only the keys the changes name, the keys whose masking changes
     * left the tail, and the keys a resolve was looking up are derived again.
     */
    private static final class TargetedMove {
        // hashes of the keys derived again, sorted once complete; a collision only looks a key up
        // again
        private long[] hashes;
        private int hashCount;
        // positions the derived-again keys had in the covered snapshot, removed from the masks
        final Map<Integer, Roaring64Bitmap> oldPositions = new HashMap<>();
        final List<ByteBuffer> pendingKeys = new ArrayList<>();
        // the index and the spilled tail before the move
        @Nullable Snapshot pre;
        long gen;
        long oldStart;
        long newStart;
        // keys of the pending changes the move dropped that masked a lake row (an in-memory tail)
        @Nullable List<byte[]> dropped;
        long tailId = -1;
        long changeKeys;
        long removed;
        long lookedUpAgain;

        TargetedMove(int expectedKeys) {
            hashes = new long[Math.max(16, expectedKeys)];
        }

        void add(byte[] key) {
            if (hashCount == hashes.length) {
                hashes = Arrays.copyOf(hashes, hashCount * 2);
            }
            hashes[hashCount++] = keyHash(key);
        }

        void addOld(List<FilePos> positions) {
            for (FilePos fp : positions) {
                if (fp != null) {
                    oldPositions
                            .computeIfAbsent(fp.fileId(), k -> new Roaring64Bitmap())
                            .addLong(fp.rowPosition());
                }
            }
        }

        void seal() {
            Arrays.sort(hashes, 0, hashCount);
        }

        boolean names(byte[] key) {
            return Arrays.binarySearch(hashes, 0, hashCount, keyHash(key)) >= 0;
        }
    }

    /** A 64-bit hash of an encoded key. */
    @VisibleForTesting
    static long keyHash(byte[] key) {
        long h = 0xcbf29ce484222325L;
        for (byte b : key) {
            h = (h ^ (b & 0xFF)) * 0x100000001b3L;
        }
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        return h ^ (h >>> 33);
    }

    /**
     * Whether a move can keep the masks and derive only what it changes: they must be complete for
     * the covered snapshot but for the keys waiting for their positions. The caller holds the lock.
     */
    private boolean targetable() {
        return targetedMoves
                && !closed
                && coveredSnapshotId >= 0
                && recoverySource != null
                && tailFailure == null
                && !fullResolvePending
                && !targetedPending
                && !tailMissing();
    }

    /**
     * Reads the keys a move's downloaded index changes name and looks up their positions in the
     * covered snapshot's index, which only the move changes (the caller holds advanceLock). Runs
     * without the manager lock. Returns null if the move should look up every lake key again: the
     * masks are not complete, or the changes name more than half as many keys as the tail keeps
     * after the move. A targeted move looks up the keys it names and reads every change it keeps,
     * while looking every key up again costs one lookup per change kept; a lookup costs about three
     * times reading a change, so naming half as many keys is about where the two cost the same. A
     * catch-up after a burst of inserts keeps nothing and looks nothing up.
     */
    @Nullable
    private TargetedMove prepareTargetedMove(
            List<PositionIndexStore.BucketData> changes, long readableOffset) throws IOException {
        long kept;
        synchronized (this) {
            if (!targetable()) {
                return null;
            }
            kept = tail.changesFrom(readableOffset);
            backgroundReaders++;
        }
        try {
            long keys = 0;
            for (PositionIndexStore.BucketData c : changes) {
                for (String sst : c.localSsts) {
                    keys += RowPosIndex.countPositions(sst);
                }
            }
            if (keys > TARGETED_MAX_KEYS || keys > Math.max(kept / 2, RESOLVE_CHUNK)) {
                return null;
            }
            TargetedMove move = new TargetedMove((int) keys);
            move.changeKeys = keys;
            for (PositionIndexStore.BucketData c : changes) {
                for (String sst : c.localSsts) {
                    if (closed) {
                        return null;
                    }
                    RowPosIndex.forEachPositionKeys(
                            sst,
                            RESOLVE_CHUNK,
                            chunk -> {
                                for (byte[] k : chunk) {
                                    move.add(k);
                                }
                                move.addOld(db.rowPosIndex().multiGetByKeys(chunk));
                                lookedUpKeys.addAndGet(chunk.size());
                            });
                }
            }
            return move;
        } finally {
            releaseDb();
        }
    }

    /**
     * Derives again the lake masks of the keys a move changed, without the manager lock (the caller
     * holds advanceLock, so the next move waits instead of starting over). The pending changes the
     * move dropped stop masking their lake rows; a deleted key is no position in the changes, so
     * those keys come from the dropped changes, as of the snapshot taken before the move. The
     * positions all these keys had are removed, and the ones that still mask a lake row are looked
     * up again in the new index. If anything fails, every lake key is looked up again.
     */
    private void completeTargetedMove(TargetedMove move) {
        Exception failure = null;
        try {
            List<byte[]> dropped = move.dropped;
            if (dropped == null) {
                dropped = new ArrayList<>();
                try (TailStore.PendingCursor cursor =
                        db.tailStore().pendingCursor(move.tailId, move.pre, 0, move.newStart)) {
                    while (!closed && cursor.nextLakeKeys(RESOLVE_CHUNK, move.oldStart, dropped)) {
                        // reads the changes the move dropped
                    }
                }
            }
            // a dropped change of a key the changes name had its position looked up already;
            // the others are deletes, which the changes hold no position of
            move.seal();
            List<byte[]> deleted = new ArrayList<>();
            for (byte[] k : dropped) {
                if (!move.names(k)) {
                    deleted.add(k);
                }
            }
            for (int from = 0; from < deleted.size(); from += RESOLVE_CHUNK) {
                List<byte[]> chunk =
                        deleted.subList(from, Math.min(deleted.size(), from + RESOLVE_CHUNK));
                move.addOld(db.rowPosIndex().multiGetByKeys(chunk, move.pre));
                lookedUpKeys.addAndGet(chunk.size());
                for (byte[] k : chunk) {
                    move.add(k);
                }
            }
            for (ByteBuffer k : move.pendingKeys) {
                move.add(k.array());
            }
            move.seal();
            Predicate<byte[]> named = move::names;
            long tailId;
            synchronized (this) {
                if (move.gen != generation || closed) {
                    return;
                }
                move.removed = removeFromMasks(move.oldPositions);
                if (!tail.spilled()) {
                    List<ByteBuffer> keys = tail.lakeKeys(move.newStart, named);
                    move.lookedUpAgain = keys.size();
                    unresolved.addAll(keys);
                    targetedDone();
                    return;
                }
                tailId = tail.id;
            }
            // the pending changes are read from disk in offset order, without the lock
            List<byte[]> keys = new ArrayList<>();
            try (TailStore.PendingCursor cursor =
                    db.tailStore().pendingCursor(tailId, null, move.newStart, Long.MAX_VALUE)) {
                while (!closed && cursor.nextLakeKeys(RESOLVE_CHUNK, move.newStart, named, keys)) {
                    // collects the lake keys the move named
                }
            }
            synchronized (this) {
                if (move.gen != generation || closed || tail.id != tailId) {
                    return;
                }
                for (byte[] k : keys) {
                    unresolved.add(ByteBuffer.wrap(k));
                }
                move.lookedUpAgain = keys.size();
                targetedDone();
            }
        } catch (IOException | RuntimeException e) {
            failure = e;
        } finally {
            synchronized (this) {
                if (move.gen == generation && !closed && targetedPending) {
                    // failed: every lake key is looked up again
                    LOG.warn(
                            "Derived DV: the masks of snapshot {} could not be kept on its move;"
                                    + " every lake key is looked up again.",
                            coveredSnapshotId,
                            failure);
                    targetedFallbacks.incrementAndGet();
                    generation++;
                    targetedPending = false;
                    lakeMasks.clear();
                    unresolved.clear();
                    if (tail.spilled()) {
                        fullResolvePending = true;
                    } else {
                        unresolved.addAll(tail.lakeKeys(snapshotStartOffset));
                    }
                    reportTail(true);
                    notifyAll();
                    scheduleResolve();
                } else if (move.gen != generation) {
                    // the tail was replaced meanwhile, and its masks derived from scratch
                    targetedPending = false;
                    notifyAll();
                }
                db.releaseSnapshot(move.pre);
                releaseDb();
            }
        }
    }

    /** Ends a move's derivation of the masks: they are served again. The caller holds the lock. */
    private void targetedDone() {
        targetedMoveCount.incrementAndGet();
        targetedPending = false;
        reportTail(true);
        notifyAll();
        scheduleResolve();
    }

    /** Removes positions from the lake masks and returns how many were there. */
    private long removeFromMasks(Map<Integer, Roaring64Bitmap> positions) {
        long removed = 0;
        for (Map.Entry<Integer, Roaring64Bitmap> e : positions.entrySet()) {
            Roaring64Bitmap mask = lakeMasks.get(e.getKey());
            if (mask == null) {
                continue;
            }
            long before = mask.getLongCardinality();
            mask.andNot(e.getValue());
            removed += before - mask.getLongCardinality();
            if (mask.isEmpty()) {
                lakeMasks.remove(e.getKey());
            }
        }
        return removed;
    }

    /** Deletes local files of index data, if still there (ingestion moves them). */
    private static void deleteFiles(@Nullable List<String> paths) {
        if (paths == null) {
            return;
        }
        for (String p : paths) {
            try {
                Files.deleteIfExists(Paths.get(p));
            } catch (IOException e) {
                LOG.warn("Derived DV: could not delete {}.", p, e);
            }
        }
    }

    /**
     * Waits until the bucket can answer a request for {@code requestedSnapshotId}. Catch-ups,
     * rebuilds and log replays run in the background, never on the request thread. Unless that work
     * finishes within a short wait, the request is answered as not ready ({@link
     * DeletionVectorsNotReadyException}) and the client retries. The caller holds the lock.
     *
     * @param needsTail whether the request needs the log tail (a union read's masks)
     */
    private void awaitServable(long requestedSnapshotId, boolean needsTail)
            throws IOException, StaleSnapshotException {
        long deadline = System.currentTimeMillis() + NOT_READY_WAIT_MS;
        long replay = -1;
        long advance = -1;
        while (true) {
            if (closed) {
                throw new IOException("Derived DV manager is closed");
            }
            if (coveredSnapshotId != requestedSnapshotId) {
                if (requestedSnapshotId < 0
                        || recoverySource == null
                        || passed.contains(requestedSnapshotId)
                        || unreachable == requestedSnapshotId) {
                    throw stale(requestedSnapshotId);
                }
                if (advance >= 0 && lastFailedAdvance >= advance) {
                    // the catch-up this request waited for failed
                    throw advanceFailed(requestedSnapshotId);
                }
                long attempt = scheduleAdvance(requestedSnapshotId);
                if (attempt < 0) {
                    // the last catch-up to it failed; it is tried again after a backoff
                    throw advanceFailed(requestedSnapshotId);
                }
                if (advance < 0) {
                    advance = attempt;
                }
            } else if (!needsTail) {
                return;
            } else if (recoverySource != null && tailMissing()) {
                if (tailFailure instanceof TailDiskException
                        && System.currentTimeMillis() < replayRetryAtMs) {
                    // the local disk failed: retried after a backoff
                    throw new DeletionVectorsNotReadyException(
                            String.format(
                                    "Deletion vectors of lake snapshot %d could not keep their log"
                                            + " tail on disk (%s); it is replayed again.",
                                    coveredSnapshotId, tailFailure.getCause()));
                }
                if (replay >= 0 && lastFailedReplay >= replay && lastReplayFailure != null) {
                    if (isDiskFailure(lastReplayFailure)) {
                        throw new DeletionVectorsNotReadyException(
                                String.format(
                                        "Deletion vectors of lake snapshot %d could not keep"
                                                + " their log tail on disk (%s); it is replayed"
                                                + " again.",
                                        coveredSnapshotId, lastReplayFailure));
                    }
                    // the replay this request waited for failed
                    throw new IOException(
                            String.format(
                                    "Derived DV: the log of snapshot %d from offset %d is not"
                                            + " replayed, so no masks are served",
                                    coveredSnapshotId, snapshotStartOffset),
                            lastReplayFailure);
                }
                if (replay < 0) {
                    replay = scheduleReplay(requestedSnapshotId);
                }
            } else if (fullResolvePending) {
                scheduleFullResolve();
            } else if (!targetedPending) {
                return;
            }
            long left = deadline - System.currentTimeMillis();
            if (left <= 0) {
                throw new DeletionVectorsNotReadyException(
                        coveredSnapshotId != requestedSnapshotId
                                ? String.format(
                                        "Deletion vectors cover lake snapshot %d and are catching"
                                                + " up to snapshot %d.",
                                        coveredSnapshotId, requestedSnapshotId)
                                : fullResolvePending || targetedPending
                                        ? String.format(
                                                "Deletion vectors of lake snapshot %d are looking"
                                                        + " up the lake positions of the changes"
                                                        + " of the log from offset %d.",
                                                coveredSnapshotId, snapshotStartOffset)
                                        : String.format(
                                                "Deletion vectors of lake snapshot %d are"
                                                        + " replaying the log from offset %d.",
                                                coveredSnapshotId, snapshotStartOffset));
            }
            try {
                wait(left);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        }
    }

    /** The retriable answer to a request whose catch-up failed; holds the lock. */
    private DeletionVectorsNotReadyException advanceFailed(long snapshotId) {
        return new DeletionVectorsNotReadyException(
                String.format(
                        "Deletion vectors cover lake snapshot %d; catching up to snapshot %d"
                                + " failed (%s) and is retried.",
                        coveredSnapshotId, snapshotId, lastAdvanceFailure),
                lastAdvanceFailure);
    }

    /**
     * Starts a background catch-up to {@code snapshotId} unless one is running, and returns its
     * number. Returns -1 if the last catch-up to it failed less than a backoff ago, so requests do
     * not start it again and again. The caller holds the lock.
     */
    private long scheduleAdvance(long snapshotId) {
        if (advancing == snapshotId) {
            return advances;
        }
        if (lastFailedAdvanceSnapshot == snapshotId
                && System.currentTimeMillis() < advanceRetryAtMs) {
            return -1;
        }
        advancing = snapshotId;
        long attempt = ++advances;
        runInBackground(() -> advanceInBackground(snapshotId, attempt, BUSY_RETRY_INITIAL_MS), 0);
        return attempt;
    }

    /**
     * Records how a background catch-up to {@code snapshotId} ended: covered, not reachable (not
     * registered, or older than the covered snapshot), or failed. A failure is reported to waiting
     * requests and retried after a backoff. {@code attempt} numbers the request-started catch-up,
     * or is -1.
     */
    private synchronized void advanceDone(
            long snapshotId, long attempt, boolean covered, @Nullable Exception failure) {
        if (advancing == snapshotId) {
            advancing = Long.MIN_VALUE;
        }
        if (failure != null) {
            // e.g. a storage error: retriable, after a backoff
            advanceBackoffMs =
                    lastFailedAdvanceSnapshot == snapshotId
                            ? Math.min(2 * advanceBackoffMs, ADVANCE_RETRY_MAX_BACKOFF_MS)
                            : ADVANCE_RETRY_INITIAL_BACKOFF_MS;
            lastFailedAdvanceSnapshot = snapshotId;
            lastFailedAdvance = Math.max(lastFailedAdvance, attempt);
            lastAdvanceFailure = failure;
            advanceRetryAtMs = System.currentTimeMillis() + advanceBackoffMs;
        } else {
            if (lastFailedAdvanceSnapshot == snapshotId) {
                lastFailedAdvanceSnapshot = Long.MIN_VALUE;
                lastAdvanceFailure = null;
            }
            if (!covered && attempt >= 0 && coveredSnapshotId != snapshotId) {
                // e.g. not registered: requests for it are answered as stale and start no catch-up
                unreachable = snapshotId;
            }
        }
        notifyAll();
    }

    /**
     * Looks up the lake positions of the keys of a spilled tail whose masks are new (installed, or
     * advanced) in the background, reading a chunk of changes from disk at a time. Masks are served
     * once it is done. The caller holds the lock.
     */
    private void scheduleFullResolve() {
        if (fullResolveScheduled || !tail.spilled()) {
            return;
        }
        fullResolveScheduled = true;
        final long gen = generation;
        final long tailId = tail.id;
        final long startOffset = snapshotStartOffset;
        inFlight++;
        try {
            RESOLVER.execute(() -> fullResolve(gen, tailId, startOffset));
        } catch (RejectedExecutionException e) {
            inFlight--;
            fullResolveScheduled = false;
            closeDbIfDeferred();
        }
    }

    private void fullResolve(long gen, long tailId, long startOffset) {
        boolean done = false;
        Exception failure = null;
        try (TailStore.PendingCursor cursor = db.tailStore().pendingCursor(tailId)) {
            while (!closed) {
                synchronized (this) {
                    if (gen != generation) {
                        break;
                    }
                }
                List<byte[]> keys = new ArrayList<>();
                if (!cursor.nextLakeKeys(RESOLVE_CHUNK, startOffset, keys)) {
                    done = true;
                    break;
                }
                if (keys.isEmpty()) {
                    continue; // superseded versions in the log only
                }
                List<FilePos> found = db.rowPosIndex().multiGetByKeys(keys);
                lookedUpKeys.addAndGet(keys.size());
                synchronized (this) {
                    if (gen != generation) {
                        break;
                    }
                    for (FilePos fp : found) {
                        if (fp != null) {
                            lakeMasks
                                    .computeIfAbsent(fp.fileId(), k -> new Roaring64Bitmap())
                                    .addLong(fp.rowPosition());
                        }
                    }
                }
            }
        } catch (IOException | RuntimeException e) {
            failure = e;
        } finally {
            synchronized (this) {
                inFlight--;
                fullResolveScheduled = false;
                if (gen == generation && !closed) {
                    if (done) {
                        fullResolvePending = false;
                        reportTail(true);
                    } else if (failure != null) {
                        tailDiskFailed(
                                failure instanceof IOException
                                        ? (IOException) failure
                                        : new IOException(
                                                "Derived DV full resolve failed", failure));
                    }
                }
                closeDbIfDeferred();
                notifyAll();
            }
        }
    }

    /**
     * Replays the log tail of the covered snapshot in the background unless a replay is scheduled
     * already, and returns the replay's number. The caller holds the lock.
     */
    private long scheduleReplay(long snapshotId) {
        if (replayScheduled) {
            return replays;
        }
        replayScheduled = true;
        long replay = ++replays;
        rebuildLimiter.execute(
                () -> {
                    Exception failure = null;
                    try {
                        replayTailIfMissing(snapshotId);
                    } catch (Exception e) {
                        failure = e;
                        LOG.warn(
                                "Derived DV: replay of the log tail of snapshot {} failed.",
                                snapshotId,
                                e);
                    }
                    synchronized (DerivedDvManager.this) {
                        replayScheduled = false;
                        if (failure != null) {
                            lastReplayFailure = failure;
                            lastFailedReplay = replay;
                        }
                        DerivedDvManager.this.notifyAll();
                    }
                });
        return replay;
    }

    /**
     * Returns the masks of a union read of {@code requestedSnapshotId}, covering the log from the
     * snapshot's readable offset to the high watermark. Meanwhile a newer registered snapshot is
     * caught up to in the background.
     *
     * @throws StaleSnapshotException if the bucket does not cover {@code requestedSnapshotId} and
     *     will not
     * @throws DeletionVectorsNotReadyException while the bucket catches up to the snapshot or
     *     replays its log tail
     */
    public DvSnapshot getDvForUnionRead(long requestedSnapshotId)
            throws IOException, StaleSnapshotException {
        long deadline = System.currentTimeMillis() + NOT_READY_WAIT_MS;
        while (true) {
            List<ByteBuffer> batch;
            long gen;
            synchronized (this) {
                awaitServable(requestedSnapshotId, true);
                if (cachedMasks != null
                        && cachedMasksSnapshotId == coveredSnapshotId
                        && cachedMasksGeneration == generation
                        && System.nanoTime() - cachedMasksAtNanos < maskCacheNanos) {
                    return cachedMasks;
                }
                if (inFlight > 0) {
                    // a background resolve runs; wait for its positions to reach the masks
                    awaitResolves(deadline);
                    continue;
                }
                long hw = servableHighWatermark();
                // applying reads nothing and never waits, so the state stays as checked above
                applyUpTo(hw);
                if (tailFailure != null) {
                    // the tail's disk failed while applying: answered by awaitServable
                    continue;
                }
                if (unresolved.isEmpty()) {
                    return buildMasks(hw);
                }
                if (unresolved.size() > RESOLVE_CHUNK) {
                    // too many keys for the request thread; the resolver looks them up
                    scheduleResolve(0);
                    awaitResolves(deadline);
                    continue;
                }
                // Look up the positions of keys new to the tail outside the lock, as a background
                // resolve does, because the write path takes the lock.
                batch = new ArrayList<>(unresolved);
                unresolved.clear();
                gen = generation;
                inFlight++;
                resolving.add(batch);
            }
            resolve(batch, gen);
        }
    }

    /**
     * Waits for background resolves, at most until {@code deadline}. Past the deadline, answers the
     * request as not ready. The caller holds the lock.
     */
    private void awaitResolves(long deadline) throws IOException {
        long left = deadline - System.currentTimeMillis();
        if (left <= 0) {
            throw new DeletionVectorsNotReadyException(
                    String.format(
                            "Deletion vectors of lake snapshot %d are applying %d changes and"
                                    + " looking up the lake positions of %d changed keys.",
                            coveredSnapshotId, tail.queue.size(), unresolved.size()));
        }
        try {
            wait(left);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException(ie);
        }
    }

    /** Builds the masks of the covered snapshot up to {@code hw}; holds the lock. */
    private DvSnapshot buildMasks(long hw) throws IOException {
        Map<String, byte[]> resolved = new HashMap<>();
        for (Map.Entry<Integer, Roaring64Bitmap> e : lakeMasks.entrySet()) {
            String path = db.fileDict().getFilePath(e.getKey());
            if (path != null) {
                resolved.put(path, RoaringBitmapUtils.serializeRoaringBitmap64(e.getValue()));
            }
        }
        byte[] deadVersionBytes =
                tail.deadVersions.isEmpty()
                        ? null
                        : RoaringBitmapUtils.serializeRoaringBitmap64(tail.deadVersions);
        reportTail(true);
        // Masks cover exactly [snapshotStartOffset, hw), so the reader must read the log up to hw.
        DvSnapshot masks =
                new DvSnapshot(
                        resolved, deadVersionBytes, hw, snapshotStartOffset, coveredIdentity);
        if (maskCacheNanos > 0) {
            cachedMasks = masks;
            cachedMasksSnapshotId = coveredSnapshotId;
            cachedMasksGeneration = generation;
            cachedMasksAtNanos = System.nanoTime();
        }
        return masks;
    }

    /**
     * Looks up the lake positions of keys new to the tail, outside the lock, and adds them to the
     * masks unless the state moved meanwhile. The caller counted itself in {@link #inFlight}.
     */
    private void resolve(List<ByteBuffer> batch, long gen) throws IOException {
        try {
            resolveInChunks(batch, gen);
        } catch (RuntimeException e) {
            synchronized (this) {
                if (gen == generation) {
                    unresolved.addAll(batch); // looking a key up twice only sets its bit again
                }
            }
            throw e;
        } finally {
            synchronized (this) {
                resolving.remove(batch);
                inFlight--;
                closeDbIfDeferred();
                notifyAll();
            }
        }
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
        return lookupCovered(snapshotId, keys);
    }

    /** The error for a lookup of more than {@link #MAX_LOOKUP_KEYS} keys. */
    public static IllegalArgumentException tooManyLookupKeys(int keys) {
        return new IllegalArgumentException(
                String.format(
                        "A lookup of deletion vector positions names %d keys, more than the %d"
                                + " a request may name.",
                        keys, MAX_LOOKUP_KEYS));
    }

    private List<Object[]> lookupCovered(long snapshotId, List<byte[]> keys)
            throws IOException, StaleSnapshotException {
        long gen;
        synchronized (this) {
            awaitServable(snapshotId, false);
            gen = generation;
            backgroundReaders++;
        }
        try {
            return lookupOutsideLock(snapshotId, keys, gen);
        } finally {
            releaseDb();
        }
    }

    private List<Object[]> lookupOutsideLock(long snapshotId, List<byte[]> keys, long gen)
            throws IOException, StaleSnapshotException {
        // The index changes only when the generation advances, so read it outside the lock and
        // check the generation after. Tiering lookups must not block the write path.
        List<Object[]> out = new ArrayList<>();
        List<FilePos> fps = new ArrayList<>(keys.size());
        for (int from = 0; from < keys.size(); from += RESOLVE_CHUNK) {
            if (closed) {
                throw new IOException("Derived DV manager is closed");
            }
            fps.addAll(
                    db.rowPosIndex()
                            .multiGetByKeys(
                                    keys.subList(
                                            from, Math.min(keys.size(), from + RESOLVE_CHUNK))));
        }
        Map<Integer, String> paths = new HashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            FilePos fp = fps.get(i);
            if (fp != null) {
                String path = paths.get(fp.fileId());
                if (path == null) {
                    path = db.fileDict().getFilePath(fp.fileId());
                    if (path != null) {
                        paths.put(fp.fileId(), path);
                    }
                }
                if (path != null) {
                    out.add(new Object[] {keys.get(i), path, fp.rowPosition()});
                }
            }
        }
        synchronized (this) {
            if (coveredSnapshotId != snapshotId || generation != gen) {
                // Advanced while reading, so the answer may mix two snapshots; the round retries.
                throw stale(snapshotId);
            }
        }
        return out;
    }

    /** Returns the lake snapshot this bucket covers, or -1 for the empty lake. */
    public synchronized long getReadableSnapshotId() {
        return coveredSnapshotId;
    }

    /** Returns the readable offset of the covered snapshot, where its log tail starts. */
    public synchronized long getSnapshotStartLogOffset() {
        return snapshotStartOffset;
    }

    @VisibleForTesting
    synchronized void setCheckpointJitterMillis(long millis) {
        this.checkpointJitterMs = millis;
    }

    @VisibleForTesting
    void useCheckpointExecutor(ScheduledExecutorService executor) {
        this.checkpointer = executor;
    }

    /** Whether lake positions are being looked up outside the lock. */
    @VisibleForTesting
    synchronized boolean resolving() {
        return inFlight > 0;
    }

    // a test hook run before each lookup of a resolve
    @Nullable private volatile Runnable beforeResolveLookup;

    @VisibleForTesting
    void setBeforeResolveLookup(@Nullable Runnable hook) {
        this.beforeResolveLookup = hook;
    }

    // a test hook: replays keep their map of last versions on disk from the start
    private volatile boolean spillReplayVersions = false;

    @VisibleForTesting
    void setSpillReplayVersions(boolean spill) {
        this.spillReplayVersions = spill;
    }

    /** When off, a move looks up every lake key again instead of the keys it changed. */
    @VisibleForTesting
    void setTargetedMoves(boolean targeted) {
        this.targetedMoves = targeted;
    }

    /** The keys looked up in the index for masks so far. */
    @VisibleForTesting
    long lookedUpKeys() {
        return lookedUpKeys.get();
    }

    /** {moves that kept the masks, moves whose derivation failed and looked up every key}. */
    @VisibleForTesting
    long[] targetedMoves() {
        return new long[] {targetedMoveCount.get(), targetedFallbacks.get()};
    }

    @VisibleForTesting
    synchronized void setMaskCacheMillis(long millis) {
        this.maskCacheNanos = millis * 1_000_000L;
        this.cachedMasks = null;
    }

    /** The changes waiting for the high watermark, including a replay's (from its thread). */
    @VisibleForTesting
    synchronized int queuedChanges() {
        Replay r = replaying;
        return tail.queue.size() + (r == null ? 0 : r.tail.queue.size());
    }

    /** The local disk failed to keep a spilled log tail; the tail is replayed from the log. */
    private static final class TailDiskException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private TailDiskException(Throwable cause) {
            super("Derived DV: the log tail could not be kept on disk: " + cause, cause);
        }
    }

    /** Whether the tail's changes are spilled to disk. */
    @VisibleForTesting
    synchronized boolean tailSpilled() {
        return tail.spilled();
    }

    /** Spills the tail's changes as the budget does; false if it cannot. */
    @VisibleForTesting
    boolean spillTail() {
        return spillForBudget();
    }

    /** The heap bytes of the tail last reported to the budget. */
    @VisibleForTesting
    long tailMemoryBytes() {
        return reportedMemory;
    }

    /** Returns the size of the derived state, for diagnostics. */
    public synchronized String stateSummary() {
        return "tailChanges="
                + tail.changeCount()
                + ", queued="
                + tail.queue.size()
                + ", deadTailVersions="
                + tail.deadVersions.getLongCardinality()
                + ", maskedLakeFiles="
                + lakeMasks.size();
    }
}
