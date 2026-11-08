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

import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.server.kv.dv.DerivedDvManager.RecoverySource;
import org.apache.fluss.server.kv.dv.DvFrontiers.KeepReady;
import org.apache.fluss.server.kv.dv.DvOwner.TailDiskException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.fluss.server.kv.dv.DvMaskRequestTiming.ms;
import static org.apache.fluss.server.kv.dv.DvOwner.deleteDownloads;
import static org.apache.fluss.server.kv.dv.DvOwner.isDiskFailure;

/**
 * A load of a bucket's state: the one path by which the bucket moves to a new version or a new
 * change list. A load does its long work on a job thread, then the owner swaps it in:
 *
 * <ul>
 *   <li>{@link SwitchLoad} (SWITCH): applies the index changes after the covered snapshot, up to a
 *       newer one. It downloads them, ingests them into the latest index, and swaps.
 *   <li>{@link FullLoad} (FULL): rebuilds the index of a snapshot from its newest checkpoint and
 *       the changes after it, and replays the log from its readable offset into a fresh change
 *       list. It clears the latest index first: what the bucket covered may hold keys the snapshot
 *       deleted.
 *   <li>{@link DvTailLoad} (TAIL): replays the change list of the covered snapshot from the log,
 *       after it went missing.
 * </ul>
 *
 * <p>The covered version is read through its own RocksDB snapshot ({@link DvVersion}), so what a
 * load ingests never shows through. The swap's first step is its only fallible write: the file
 * dictionary batch of a switch, or of a full load. If it fails, nothing of the load is applied, and
 * the latest index is marked as ahead of the version ({@link DvLatestIndexState}). From there the
 * swap changes only memory, but for the trim of the change list, whose failure drops the change
 * list, which is then replayed.
 *
 * <p>A load owns its downloads and its replayed change list until they are swapped in, and {@link
 * #close} releases what is left on every exit. Each RocksDB call takes a lease ({@link DvDbLeases})
 * and the load checks {@code closed} between calls, so a closing bucket waits for one call at most,
 * but for a full load's compaction, which it cancels.
 */
abstract class DvLoad implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(DvLoad.class);

    /** What a load builds. */
    enum Kind {
        SWITCH,
        FULL,
        TAIL
    }

    final DvOwner m;
    final RecoverySource src;
    final Kind kind;
    // the snapshot the load covers, and where its change list starts
    final long snapshotId;
    final long readableOffset;
    final long startNanos = System.nanoTime();
    // the index data the load downloaded and did not ingest yet; ingesting moves the files away
    final List<PositionIndexStore.BucketData> downloads = new ArrayList<>();
    // the replay of a full or tail load, until its change list is installed
    @Nullable DvReplay replay;
    // A fault of the owner stopped the load: its job ends at its next unit, and its swap step
    // aborts and closes it. Volatile: the owner sets it, the job reads it.
    private volatile boolean cancelled;

    DvLoad(DvOwner m, RecoverySource src, Kind kind, long snapshotId, long readableOffset) {
        this.m = m;
        this.src = src;
        this.kind = kind;
        this.snapshotId = snapshotId;
        this.readableOffset = readableOffset;
    }

    /** Downloads, replays and ingests on a job thread. Returns false if the load ends early. */
    abstract boolean load() throws Exception;

    /** Stops the load at its next unit, as a fault of the owner ended its work; an owner step. */
    void cancel() {
        cancelled = true;
    }

    /** Whether the load was stopped by a fault of the owner. */
    boolean cancelled() {
        return cancelled;
    }

    /** Whether the load's job stops: the bucket closes, or a fault stopped the load. */
    boolean stopped() {
        return m.closed || cancelled;
    }

    /**
     * Takes what the load reads of the state before its job starts; an owner step. Returns false if
     * the load is moot.
     */
    boolean begin() {
        return true;
    }

    /** What a load that ended early answers; an owner step. */
    boolean notLoaded() {
        return false;
    }

    /** Records what a load that ended without a swap left in the latest index; an owner step. */
    void abort() {}

    /**
     * Swaps the loaded state in, fallible steps first; an owner step. Returns whether the bucket
     * covers the load's snapshot now.
     */
    abstract boolean swap() throws Exception;

    /**
     * Whether the swap can run in this step. A load whose swap needs more than one step of work
     * does a part of it and returns false; the owner posts the swap again. An owner step.
     */
    boolean readyToSwap() {
        return true;
    }

    /** Runs after a swap, on a job thread, before the owner starts its next work. */
    void afterSwap() {}

    /** Releases what the load still owns: downloads not ingested, a replay not installed. */
    @Override
    public void close() {
        DvReplay r = replay;
        replay = null;
        if (r != null) {
            r.finish();
        }
        // deletes what was not ingested; ingestion moves the ingested files away
        deleteDownloads(downloads);
        downloads.clear();
    }

    /** Ingests one downloaded change into the latest index under a lease; false once closing. */
    boolean ingest(PositionIndexStore.BucketData data) throws IOException {
        if (stopped() || !m.leases.tryAcquire()) {
            return false;
        }
        try {
            m.index.ingest(data);
        } finally {
            m.leases.release();
        }
        m.testHooks.afterIngest();
        return true;
    }

    // ---------------------------------------------------------------------------------------------

    /**
     * Switches from the covered snapshot {@code from}, readable from {@code fromOffset}, to {@code
     * snapshotId} by applying the index changes after chain index {@code start} up to {@code
     * target}. The masks are kept but for the keys the changes name ({@link DvTargetedDerivation}),
     * unless that costs more than looking up every lake key of the change list.
     */
    static final class SwitchLoad extends DvLoad {
        private final List<PositionIndexStore.ChainEntry> chain;
        private final int start;
        private final int target;
        private final long from;
        private final long fromOffset;
        private final boolean dirty;
        private String identity;
        @Nullable private DvTargetedDerivation targeted;
        // the switch may keep the masks and derive only what it changes, as begin() found
        private boolean targetable;
        private long keptChanges;
        // the chain index of the last change the ingest started
        private int reached;
        @Nullable private Exception ingestFailure;
        private int sstFiles;
        private long metaNanos;
        private long downloadNanos;
        private long ingestNanos;
        private long applyNanos;
        private long swapNanos;
        private long trimNanos;
        private long rederiveNanos;
        private long changesLeft;
        private final long t0 = System.currentTimeMillis();

        SwitchLoad(
                DvOwner m,
                RecoverySource src,
                List<PositionIndexStore.ChainEntry> chain,
                int start,
                int target,
                long snapshotId,
                long readableOffset,
                long from,
                long fromOffset,
                boolean dirty,
                long plannedAtNanos) {
            super(m, src, Kind.SWITCH, snapshotId, readableOffset);
            this.chain = chain;
            this.start = start;
            this.target = target;
            this.from = from;
            this.fromOffset = fromOffset;
            this.dirty = dirty;
            this.metaNanos = System.nanoTime() - plannedAtNanos;
            this.reached = start;
        }

        @Override
        boolean begin() {
            if (m.coveredSnapshotId != from) {
                return false; // moved on meanwhile; the next plan starts over
            }
            m.latestIndex.startIngest();
            // Only values are taken here: the covered version is retained after the downloads,
            // so no lease of the RocksDB is held across remote reads.
            if (m.targetedSwitches && !dirty && DvTargetedDerivation.targetable(m)) {
                targetable = true;
                keptChanges = m.maskState.tail.changesFrom(readableOffset);
            }
            return true;
        }

        /**
         * Retains the published covered version for the derivation's local reads, on the job
         * thread; null if the bucket moved on from it or closes. The derivation releases it.
         */
        @Nullable
        private DvVersion retainCovered() {
            DvVersion v = m.state.version;
            return v != null && v.snapshotId == from && !stopped() && v.tryRetain() ? v : null;
        }

        @Override
        void abort() {
            if (reached > start) {
                m.latestIndex.ingestFailed(chain, reached);
            } else {
                m.latestIndex.ingestEnded();
            }
        }

        @Override
        boolean load() throws Exception {
            // downloads run off the owner, so writes are not blocked
            long downloadStart = System.nanoTime();
            for (int i = start + 1; i <= target; i++) {
                if (stopped()) {
                    return false;
                }
                PositionIndexStore.BucketData c = src.readChange(chain.get(i));
                downloads.add(c);
                sstFiles += c.localSsts.size();
            }
            downloadNanos = System.nanoTime() - downloadStart;
            long prepareStart = System.nanoTime();
            identity = src.snapshotIdentity(chain.get(target));
            // the keys the changes name, and their positions in the covered version, retained
            // only for these local reads; a dirty latest index is no base for that
            DvVersion v = targetable ? retainCovered() : null;
            targeted =
                    v == null ? null : DvTargetedDerivation.prepare(m, downloads, v, keptChanges);
            rederiveNanos += System.nanoTime() - prepareStart;
            if (stopped()) {
                return false;
            }
            // The expensive part, off the owner: mask requests are served meanwhile.
            long ingestStart = System.nanoTime();
            try {
                for (PositionIndexStore.BucketData c : downloads) {
                    reached++;
                    if (!ingest(c)) {
                        break; // the bucket closes
                    }
                }
            } catch (IOException | RuntimeException e) {
                ingestFailure = e;
            }
            ingestNanos = System.nanoTime() - ingestStart;
            return true;
        }

        @Override
        boolean notLoaded() {
            return m.coveredSnapshotId == snapshotId;
        }

        @Override
        boolean readyToSwap() {
            if (ingestFailure != null || m.closed || m.coveredSnapshotId != from) {
                return true;
            }
            // the changes below the high watermark join the change list first, a chunk a step
            long start = System.nanoTime();
            boolean ready = m.maskState.readyToSwitch(m.highWatermark.getAsLong());
            applyNanos += System.nanoTime() - start;
            return ready;
        }

        @Override
        boolean swap() throws Exception {
            long swapStart = System.nanoTime();
            if (ingestFailure != null || m.closed || m.coveredSnapshotId != from) {
                // The latest index may hold part of the changes; the covered version does not
                // see them, and the next switch applies them all again. A switch or rebuild to an
                // earlier snapshot is refused, as it would keep the later changes.
                m.latestIndex.ingestFailed(chain, reached);
                if (ingestFailure != null) {
                    throw ingestFailure;
                }
                return m.coveredSnapshotId == snapshotId;
            }
            // Files the new snapshot removed leave the dictionary in one write, before anything
            // else changes: if it fails, the covered version stays whole, as after a failed
            // ingest.
            Set<Integer> droppedFiles;
            try {
                List<String> removed = new ArrayList<>();
                for (PositionIndexStore.BucketData c : downloads) {
                    removed.addAll(c.removedFiles);
                }
                droppedFiles = m.index.dropFiles(removed);
            } catch (IOException | RuntimeException e) {
                m.latestIndex.ingestFailed(chain, reached);
                throw e;
            }
            // From here the switch is committed.
            DvTargetedDerivation derivation = targeted;
            try {
                // the changes below the high watermark join the change list before the switch;
                // readyToSwap applied all but a chunk at most
                applyNanos += m.maskState.applyAll(m.highWatermark.getAsLong());
                if (derivation != null && !derivation.holdViews()) {
                    derivation.abandonUnlessPosted();
                    derivation = null;
                }
                for (int i = start + 1; i < target; i++) {
                    m.markPassed(chain.get(i).snapshotId);
                }
                // a switch of one registration keeps the latest frontier of the snapshot it leaves
                switchTo(
                        DvVersion.open(
                                m.db,
                                m.leases,
                                snapshotId,
                                readableOffset,
                                identity,
                                m.entriesApplied + target - start),
                        derivation,
                        target == start + 1);
                // the masks of the files the new snapshot removed go only once it is covered
                m.maskState.lakeMasks.dropFiles(droppedFiles);
                m.coveredIdentity = identity;
                m.entriesApplied += target - start;
            } catch (RuntimeException e) {
                // the masks cannot be kept: they are derived again once the change list is
                // replayed
                if (derivation != null) {
                    derivation.abandonUnlessPosted();
                }
                derivation = null;
                m.maskState.renew(KeepReady.NONE);
                m.maskState.resetTail();
                m.maskState.tailFailure = e;
                m.maskState.reportTail(true);
                throw e;
            } finally {
                targeted = derivation;
                // the latest index is the covered version's now
                m.latestIndex.covered(target, chain, false);
            }
            m.index.maybeCheckpoint();
            changesLeft = m.maskState.tail.changeCount();
            swapNanos = System.nanoTime() - swapStart;
            return true;
        }

        /**
         * Covers the new version: the changes it holds leave the change list, and the masks are
         * derived again from the rest. An owner step. The keys whose superseded version the new
         * snapshot holds are looked up in its index, read from disk in the background, and no masks
         * are served until then.
         */
        private void switchTo(
                DvVersion next,
                @Nullable DvTargetedDerivation derivation,
                boolean nextRegistration) {
            long previousStart = m.snapshotStartOffset;
            m.maskState.renew(nextRegistration ? KeepReady.NEXT_SNAPSHOT : KeepReady.NONE);
            m.markPassed(m.coveredSnapshotId);
            m.replaceVersion(next);
            m.coveredSnapshotId = next.snapshotId;
            m.snapshotStartOffset = next.readableOffset;
            m.rebuild.clearReplayFailure();
            m.unreachable = Long.MIN_VALUE;
            m.maskState.fullResolvePending = false;
            if (derivation != null) {
                derivation.swapped(previousStart, next.readableOffset);
            } else {
                m.maskState.lakeMasks.clear();
            }
            m.maskState.unresolved.clear();
            long trimStart = System.nanoTime();
            try {
                // one range delete
                m.maskState.tail.retainFrom(next.readableOffset);
            } catch (IOException e) {
                m.maskState.tailDiskFailed(e);
                return;
            } finally {
                trimNanos += System.nanoTime() - trimStart;
            }
            if (derivation == null) {
                m.maskState.resolveWholeTail();
            }
            m.maskState.reportTail(true);
            m.maskState.scheduleResolve();
        }

        @Override
        public void close() {
            super.close();
            // a derivation that did not post its result ends here: views and budget
            DvTargetedDerivation d = targeted;
            if (d != null) {
                d.abandonUnlessPosted();
            }
        }

        @Override
        void afterSwap() {
            DvTargetedDerivation derivation = targeted;
            if (derivation != null) {
                long completeStart = System.nanoTime();
                derivation.complete();
                rederiveNanos += System.nanoTime() - completeStart;
            }
            m.metrics.caughtUp(System.currentTimeMillis() - t0);
            // A targeted switch kept the masks but for the keys the changes name; the others look
            // up every lake key again in the background resolves. lock_ms is the swap step, and
            // drain_wait_ms is always 0: the covered version is read through its own RocksDB
            // snapshot, so nothing waits for its readers.
            LOG.info(
                    "Switched the deletion vectors of {} to snapshot {}: from_snapshot={}"
                            + " to_snapshot={} index_changes={} load_ms={} trim_ms={} rederive_ms={}"
                            + " total_ms={} from_offset={} to_offset={}"
                            + " ssts={} targeted={} change_keys={} removed_positions={}"
                            + " relookup_keys={} tail_changes_left={} meta_ms={} download_ms={}"
                            + " ingest_ms={} apply_ms={} lock_ms={} drain_wait_ms={}",
                    m.logName,
                    snapshotId,
                    from,
                    snapshotId,
                    target - start,
                    ms(downloadNanos + ingestNanos),
                    ms(trimNanos),
                    ms(rederiveNanos),
                    ms(System.nanoTime() - startNanos + metaNanos),
                    fromOffset,
                    readableOffset,
                    sstFiles,
                    derivation != null,
                    derivation == null ? -1 : derivation.changeKeys,
                    derivation == null ? -1 : derivation.removed,
                    derivation == null ? -1 : derivation.lookedUpAgain,
                    changesLeft,
                    ms(metaNanos),
                    ms(downloadNanos),
                    ms(ingestNanos),
                    ms(applyNanos),
                    ms(swapNanos),
                    ms(0));
        }
    }

    // ---------------------------------------------------------------------------------------------

    /**
     * Rebuilds the state to {@code snapshotId}: downloads its newest checkpoint and the index
     * changes after it, and replays the log from its readable offset into a fresh change list, on a
     * job thread. Then it clears the latest index and ingests the download, and swaps: the file
     * dictionary keeps the snapshot's files only, in one write, and the version and change list are
     * installed. Until then the covered version keeps serving.
     */
    static final class FullLoad extends DvLoad {
        private final List<PositionIndexStore.ChainEntry> chain;
        private int upTo = -1;
        private int from;
        @Nullable private PositionIndexStore.Checkpoint checkpoint;
        @Nullable private DerivedDvRebuildLimiter slots;
        private String identity;
        @Nullable private Exception replayFailure;
        @Nullable private Exception ingestFailure;
        // the load cleared the latest index: only a full load may build on it now
        private volatile boolean cleared;
        // the compaction of the index ingested in one call while it runs, which cancel() cancels
        @Nullable private volatile RowPosIndex.Compaction compaction;
        private int sstFiles;
        // the SSTs ingested in one call, or -1 if the downloads were ingested one by one
        private int oneCallSsts = -1;
        private long compactNanos = -1;
        private long slotWaitNanos;
        private long downloadNanos;
        private long replayEarlyNanos;
        private long replayFinalNanos;
        private long ingestNanos;
        private long t0;

        FullLoad(
                DvOwner m,
                RecoverySource src,
                long snapshotId,
                long startOffset,
                List<PositionIndexStore.ChainEntry> chain) {
            super(m, src, Kind.FULL, snapshotId, startOffset);
            this.chain = chain;
        }

        @Override
        boolean begin() {
            if (m.coveredSnapshotId < 0 && m.emptyLakeTracked) {
                // Drop the empty lake's change list (the whole log) now rather than hold it next to
                // the replayed one. A union read of the empty lake is not ready until the rebuild
                // is done, then gets stale and plans again on the snapshot.
                m.maskState.renew(KeepReady.NONE);
                m.emptyLakeTracked = false;
                m.maskState.resetTail();
            }
            m.latestIndex.startIngest();
            return true;
        }

        @Override
        void abort() {
            if (cleared) {
                m.latestIndex.markFull();
            }
            m.latestIndex.ingestEnded();
        }

        @Override
        boolean load() throws Exception {
            if (stopped()) {
                // A catch-up that started before close() must not write into the closing RocksDB.
                return false;
            }
            for (int i = 0; i < chain.size(); i++) {
                if (chain.get(i).snapshotId == snapshotId) {
                    upTo = i;
                }
            }
            if (upTo < 0) {
                throw new IllegalStateException(
                        "Cannot rebuild the deletion vectors: readable snapshot "
                                + snapshotId
                                + " is not in the published index chain ("
                                + chain.size()
                                + " entries)");
            }
            if (readableOffset > 0) {
                checkpoint = src.latestCheckpoint(chain, upTo);
                if (checkpoint == null && src.chainTrimmed()) {
                    // The trimmed chain is complete only if the bucket had nothing tiered at the
                    // oldest kept entry (offsets are cumulative), e.g. a partition created after
                    // the trim.
                    Long firstOffset =
                            chain.isEmpty() ? null : src.snapshotOffset(chain.get(0).snapshotId);
                    if (firstOffset == null || firstOffset > 0) {
                        throw new IllegalStateException(
                                "Cannot rebuild the deletion vectors: the position index chain was"
                                        + " trimmed but no checkpoint of this bucket covers it, and a partial"
                                        + " index is refused");
                    }
                }
            }
            if (checkpoint != null) {
                from = checkpoint.chainIndex + 1;
            }
            long slotStart = System.nanoTime();
            slots = startSlot(m);
            slotWaitNanos = System.nanoTime() - slotStart;
            slots.beforeRebuild();
            if (stopped()) {
                return false;
            }
            t0 = System.currentTimeMillis();
            long downloadStart = System.nanoTime();
            if (checkpoint != null) {
                downloads.add(src.readCheckpoint(checkpoint));
            }
            // Offset 0 means nothing of this bucket is tiered, so no index change holds its rows.
            for (int i = from; readableOffset > 0 && i <= upTo; i++) {
                if (stopped()) {
                    return false;
                }
                downloads.add(src.readChange(chain.get(i))); // in commit order: later wins
            }
            for (PositionIndexStore.BucketData d : downloads) {
                sstFiles += d.localSsts.size();
            }
            downloadNanos = System.nanoTime() - downloadStart;
            // DvReplay into a queue: the local log can run past the high watermark, and a change
            // above it must not mask a version below it.
            replay = DvReplay.start(m, src, readableOffset, this::stopped);
            if (replay == null) {
                return false;
            }
            long replayStart = System.nanoTime();
            replayFailure = replay.replayEarlyPasses();
            replayEarlyNanos = System.nanoTime() - replayStart;
            identity = src.snapshotIdentity(chain.get(upTo));
            // The index is loaded off the owner, as a switch ingests, so writes go on. It is
            // cleared first: what the bucket covered may hold keys the snapshot deleted, which
            // neither a checkpoint nor the changes after it name. The covered version is read
            // through its own RocksDB snapshot meanwhile.
            if (stopped()) {
                return false;
            }
            long ingestStart = System.nanoTime();
            try {
                cleared = true;
                m.leases.acquire();
                try {
                    m.db.rowPosIndex().clear();
                } finally {
                    m.leases.release();
                }
                if (DvOwnerIndex.mergesInOrder(downloads)) {
                    // one ingest, then one sorted run rather than an overlapping file per change
                    if (ingestInOrder() && sstFiles > 0 && !compact()) {
                        return false; // cancelled: the bucket closes, or a fault stopped the load
                    }
                } else {
                    for (PositionIndexStore.BucketData d : downloads) {
                        if (!ingest(d)) {
                            break; // the bucket closes
                        }
                    }
                }
            } catch (DvDbLeases.DvClosedException e) {
                // the bucket closes: the swap sees it closed
            } catch (IOException | RuntimeException e) {
                ingestFailure = e;
            }
            ingestNanos = System.nanoTime() - ingestStart - Math.max(0, compactNanos);
            if (replayFailure == null && ingestFailure == null) {
                long finalStart = System.nanoTime();
                if (!m.work.enterFinalPass(replay)) {
                    return false;
                }
                replayFailure = replay.finalRead();
                replayFinalNanos = System.nanoTime() - finalStart;
            }
            return true;
        }

        @Override
        boolean swap() throws Exception {
            long swapStart = System.nanoTime();
            if (ingestFailure == null && !m.closed) {
                try {
                    // the dictionary keeps the files of the rebuilt snapshot only, in one write
                    m.index.retainFilesOf(downloads);
                } catch (IOException | RuntimeException e) {
                    ingestFailure = e;
                }
            }
            if (ingestFailure != null || m.closed) {
                // the covered version stays; the next catch-up rebuilds in full
                m.latestIndex.ingestFailed(chain, upTo);
                abort();
                if (ingestFailure != null) {
                    throw ingestFailure;
                }
                return false;
            }
            m.coveredIdentity = identity;
            // the rebuilt version is the latest index
            m.latestIndex.covered(upTo, chain, true);
            m.replaceVersion(
                    DvVersion.open(m.db, m.leases, snapshotId, readableOffset, identity, upTo + 1));
            if (m.coveredSnapshotId == snapshotId) {
                m.maskState.renew(KeepReady.SAME_SNAPSHOT);
            } else if (m.coveredSnapshotId >= 0
                    && upTo > 0
                    && chain.get(upTo - 1).snapshotId == m.coveredSnapshotId) {
                m.maskState.renew(KeepReady.NEXT_SNAPSHOT);
            } else {
                m.maskState.renew(KeepReady.NONE);
            }
            m.maskState.fromRebuild = true;
            m.markPassed(m.coveredSnapshotId);
            for (int i = 0; i < upTo; i++) {
                m.markPassed(chain.get(i).snapshotId);
            }
            m.unmarkPassed(snapshotId);
            m.coveredSnapshotId = snapshotId;
            m.snapshotStartOffset = readableOffset;
            m.entriesApplied = upTo + 1;
            m.index.entriesAtCheckpoint = checkpoint == null ? 0 : checkpoint.chainIndex + 1;
            if (replayFailure != null) {
                // the index stays, union reads retry the replay and fail until it succeeds
                m.maskState.resetTail();
                m.maskState.tailFailure =
                        isDiskFailure(replayFailure)
                                ? new TailDiskException(replayFailure)
                                : replayFailure;
                m.maskState.reportTail(true);
                LOG.warn(
                        "The deletion vectors cover snapshot {} but cannot replay the log from"
                                + " offset {}; union reads fail until a replay succeeds.",
                        snapshotId,
                        readableOffset,
                        replayFailure);
                return true;
            }
            DvReplay r = replay;
            long installStart = System.nanoTime();
            r.install();
            long installNanos = System.nanoTime() - installStart;
            m.index.maybeCheckpoint();
            m.metrics.rebuilt(System.currentTimeMillis() - t0);
            long end = System.nanoTime();
            LOG.info(
                    "Rebuilt the deletion vectors of {}: kind=full snapshot={}"
                            + " readable_offset={} log_end={} changes_replayed={} load_index_ms={}"
                            + " replay_ms={} total_ms={} versions_spilled={} tail_changes={}"
                            + " queued={} index_changes={} ssts={} checkpoint={} download_ms={}"
                            + " ingest_ms={} replay_locked_ms={} install_ms={} lock_ms={}"
                            + " slot_wait_ms={} one_call_ssts={} compact_ms={}",
                    m.logName,
                    snapshotId,
                    readableOffset,
                    r.next,
                    r.changes,
                    ms(downloadNanos + ingestNanos + Math.max(0, compactNanos)),
                    ms(replayEarlyNanos + replayFinalNanos),
                    ms(end - startNanos),
                    r.versionsId >= 0,
                    m.maskState.tail.changeCount(),
                    m.maskState.tail.queue.size(),
                    upTo + 1 - from,
                    sstFiles,
                    checkpoint == null ? "-" : checkpoint.snapshotId,
                    ms(downloadNanos),
                    ms(ingestNanos),
                    ms(replayFinalNanos),
                    ms(installNanos),
                    ms(end - swapStart),
                    ms(slotWaitNanos),
                    oneCallSsts,
                    compactNanos < 0 ? -1 : ms(compactNanos));
            return true;
        }

        @Override
        void cancel() {
            super.cancel();
            RowPosIndex.Compaction c = compaction;
            if (c != null) {
                c.cancel();
            }
        }

        /**
         * Ingests every download in one call under a lease, as {@link #ingest} does a change; false
         * once the bucket closes.
         */
        private boolean ingestInOrder() throws IOException {
            if (stopped() || !m.leases.tryAcquire()) {
                return false;
            }
            try {
                m.index.ingestInOrder(downloads);
            } finally {
                m.leases.release();
            }
            oneCallSsts = sstFiles;
            m.testHooks.afterIngest();
            return true;
        }

        /**
         * Compacts the index the one call left in overlapping level-0 files, under a lease a close
         * cancels the compaction through rather than wait for it, as {@link #cancel} does. Returns
         * false if it was cancelled.
         *
         * <p>Meanwhile the bucket's disk holds about three times the index: the cleared entries,
         * which the covered version's snapshot keeps until the swap, the ingested files, and the
         * compaction's output.
         */
        private boolean compact() throws IOException {
            long start = System.nanoTime();
            try (RowPosIndex.Compaction c = new RowPosIndex.Compaction()) {
                // set before stopped() is read, as cancel() sets that before it reads this
                compaction = c;
                Runnable cancel = c::cancel;
                if (stopped() || !m.leases.tryAcquire(cancel)) {
                    return false;
                }
                try {
                    m.testHooks.beforeCompaction(c::cancelled);
                    return m.db.rowPosIndex().compact(c);
                } finally {
                    m.leases.release(cancel);
                }
            } finally {
                compaction = null;
                compactNanos = System.nanoTime() - start;
            }
        }

        @Override
        public void close() {
            super.close();
            if (slots != null) {
                endSlot(m, slots);
                slots = null;
            }
        }
    }

    // ---------------------------------------------------------------------------------------------

    /** Takes a rebuild slot of the tablet server, waiting for one if all are taken. */
    static DerivedDvRebuildLimiter startSlot(DvOwner m) throws InterruptedIOException {
        DerivedDvRebuildLimiter slots = m.rebuild.limiter;
        try {
            slots.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException(
                    "Interrupted while waiting for a deletion vector rebuild slot");
        }
        m.metrics.rebuildStarted();
        return slots;
    }

    static void endSlot(DvOwner m, DerivedDvRebuildLimiter slots) {
        m.metrics.rebuildEnded();
        slots.release();
    }
}
