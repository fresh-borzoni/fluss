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

import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.server.kv.dv.DerivedDvManager.RecoverySource;
import org.apache.fluss.server.kv.dv.DvFrontiers.KeepReady;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * The owner of a bucket's derived deletion vector state: the one place its transitions run, one
 * step at a time. It holds the state, changes it only in its steps, and publishes it as an
 * immutable {@link DvState} after each step, which reads and lookups take without a lock.
 *
 * <h3>Mailbox</h3>
 *
 * <p>The owner is a mailbox on the tablet server's {@code kv-deletion-vectors-bucket-state} pool
 * ({@link DvOwnerMailbox}). A step runs to completion and never waits for another step, a job or a
 * request; it calls the RocksDB only for one write or read at a time. Steps of a bucket run one at
 * a time, in the order they were posted. The long work runs in jobs on the runtime's pools ({@link
 * DerivedDvRuntime}), which post their results back as steps. A job gets immutable inputs: a
 * retained version, its keys, offsets. Its results name the epoch of the mask state they were
 * computed for ({@link DvMaskState.Epoch}), and the owner drops those of an epoch renewed since.
 * What jobs and requests read off the owner is the published state, or a field that is volatile or
 * atomic and says why.
 *
 * <p>This class holds the bucket's state and publishes it. Its steps live in parts: the work queue
 * and the loads' swaps ({@link DvOwnerWork}), planning and the catch-up backoff ({@link
 * DvOwnerPlanner}), the index's dictionary writes, ingest and checkpoints ({@link DvOwnerIndex}),
 * the hand-off's drain and overflow ({@link DvOwnerHandOff}), and faults and close ({@link
 * DvOwnerFaults}). The mask state ({@link DvMaskState}) and the union reads ({@link DvMaskReads})
 * build on them.
 *
 * <h3>Transitions</h3>
 *
 * <p>The states are those of {@link DvState.Kind}; v is the covered version, T a target snapshot,
 * and the empty lake is a version too: snapshot -1, readable from 0, with an empty index.
 *
 * <table>
 *   <tr><th>State</th><th>Recover(src)</th><th>Want(T)</th><th>Hand-off</th><th>Load done</th>
 *       <th>Load or swap fails</th><th>Close</th></tr>
 *   <tr><td>UNRECOVERED</td><td>served cut := max(hw, log end); REBUILDING while the readable
 *       snapshot is read, then REBUILDING(none to T) with a FULL load, or, with nothing readable,
 *       REPLAYING(the empty lake)</td><td>not ready</td><td>queued; the recovery replaces the
 *       change list</td><td>-</td><td>-</td><td>CLOSED</td></tr>
 *   <tr><td>REBUILDING(none to T)</td><td>-</td><td>queued</td><td>dropped; buffered in the
 *       final pass</td><td>swap: COVERING(v_T)</td><td>released; the full load is planned again
 *       by the next request or registration</td><td>CLOSED</td></tr>
 *   <tr><td>COVERING(v)</td><td>-</td><td>covered or passed: answered; else planned:
 *       SWITCHING, REBUILDING(v to T), or unreachable</td><td>into v's queue, applied as the high
 *       watermark passes, looked up; on overflow, released and the change list dropped, then
 *       dropped until a TAIL load's final pass</td><td>-</td><td>-</td><td>CLOSED</td></tr>
 *   <tr><td>SWITCHING(v to T)</td><td>-</td><td>queued</td><td>as COVERING</td><td>swap:
 *       COVERING(v_T); the next target</td><td>released; the latest index is dirty through what
 *       was ingested; backoff; COVERING(v)</td><td>CLOSED; the load stops before its next
 *       change</td></tr>
 *   <tr><td>REBUILDING(v to T)</td><td>-</td><td>queued</td><td>as COVERING, and buffered in the
 *       final pass</td><td>swap: COVERING(v_T)</td><td>released; dirty FULL if it cleared the
 *       index; COVERING(v)</td><td>CLOSED</td></tr>
 *   <tr><td>REPLAYING(v)</td><td>-</td><td>queued</td><td>dropped; buffered in the final
 *       pass</td><td>install: COVERING(v), every lake key looked up</td><td>COVERING(v) with the
 *       change list missing, retried after a backoff on a disk failure</td><td>CLOSED</td></tr>
 *   <tr><td>CLOSED</td><td>-</td><td>ignored</td><td>ignored</td><td>released</td><td>released
 *       </td><td>-</td></tr>
 * </table>
 *
 * <p>A failed apply, trim or hand-off drops the change list: masks are served again once a TAIL
 * load replayed it. A step that fails unexpectedly does the same, logs at ERROR and counts in
 * {@code dvOwnerFaultPerSecond}; only a {@link VirtualMachineError} then stops the tablet server,
 * as on its other pools. If the owner falls behind its writes, the hand-off overflows ({@link
 * DvHandOff}) and the change list is replayed after a backoff ({@link DvRebuild}). A FULL or TAIL
 * load ends with a final pass ({@link DvReplay}): the owner buffers the changes it drains from then
 * on, and the swap installs them with the replayed change list.
 */
final class DvOwner {

    private static final Logger LOG = LoggerFactory.getLogger(DvOwner.class);

    // The owner's parts (see the class javadoc); each keeps the bookkeeping of its own steps.
    final DvOwnerMailbox steps;
    final DvOwnerWork work = new DvOwnerWork(this);
    final DvOwnerPlanner planner = new DvOwnerPlanner(this);
    final DvOwnerIndex index = new DvOwnerIndex(this);
    final DvOwnerHandOff drain = new DvOwnerHandOff(this);
    final DvOwnerFaults faults = new DvOwnerFaults(this);

    DvOwner(
            DvRocksDB dvRocksDB,
            LongSupplier highWatermark,
            int checkpointInterval,
            DerivedDvMetrics metrics,
            DerivedDvHeapBudget heapBudget,
            DerivedDvRuntime runtime,
            Executor stateExecutor,
            DvTestHooks testHooks) {
        this.db = dvRocksDB;
        this.leases = new DvDbLeases(dvRocksDB);
        this.highWatermark = highWatermark;
        this.checkpointInterval = checkpointInterval;
        this.metrics = metrics;
        this.heapBudget = heapBudget;
        this.runtime = runtime;
        this.testHooks = testHooks;
        this.index.checkpointTimer = runtime.scheduler();
        this.steps = new DvOwnerMailbox(this, stateExecutor);
        // the server's rebuild slots, unless a background recovery names others
        this.rebuild.limiter = runtime.rebuilds();
        this.maskState = new DvMaskState(this);
        this.version = DvVersion.open(db, leases, -1, 0, null, 0);
        this.state = buildState();
    }

    /** Posts a step; it runs after the steps posted before it ({@link DvOwnerMailbox}). */
    void post(Runnable step) {
        steps.post(step);
    }

    final DvRocksDB db;
    final DvMaskState maskState;
    final LongSupplier highWatermark;

    // Names the bucket in log lines. Volatile: jobs and the write path log it too.
    volatile String logName = "table=- partition=- bucket=-";

    // The fields below are the owner's: only its steps read and write them, unless noted
    // otherwise. Reads and lookups take the published state instead.
    long coveredSnapshotId = -1;
    // what the covered snapshot's id named when it was registered, for readers to check
    @Nullable String coveredIdentity;
    // coveredSnapshotId -1 is the empty lake at offset 0, whose change list is the whole log.
    // Volatile for log retention, which reads it off the owner.
    volatile long snapshotStartOffset = 0;
    // The empty lake's tail is replayed in the background once the bucket becomes leader, and
    // kept until a snapshot is covered.
    boolean emptyLakeTracked = false;
    // A batch is in the log before the write path hands its changes over, so the high watermark
    // can pass changes the change list does not have yet. Masks go up to the log end as of the
    // last hand-off or install of the change list at most; every change below it is in the change
    // list. -1 until then. Volatile: a pinned cut's request reads it to tell whether the cut can be
    // built again.
    volatile long handedOffEnd = -1;
    // The index is exported every checkpointInterval chain entries, so a rebuild replays only the
    // later entries and tiering can trim the chain.
    final int checkpointInterval;
    final DerivedDvMetrics metrics;
    long entriesApplied = 0;
    // Volatile for jobs and the write path, which stop early once the tablet closes.
    volatile boolean closed = false;
    // Who may still call the RocksDB, which the manager owns: it closes once the last lease after
    // close() is released.
    final DvDbLeases leases;
    // The version of the index the bucket covers: lookups and lake positions are read through it.
    // Starts as the empty lake's. The owner's; readers take it from the published state.
    DvVersion version;
    // The state the owner published after its last step; reads and lookups take it without a lock.
    volatile DvState state;
    // The replay whose final pass runs: the owner buffers the changes it drains for it.
    @Nullable DvReplay finalPass;
    // How long close() waits for the background users of the RocksDB.
    static final long CLOSE_WAIT_MS = 30_000;
    // volatile: close() reads it on the caller's thread, and tests set it
    volatile long closeWaitMs = CLOSE_WAIT_MS;
    // The table has row TTL, which removes values without a change in the log: a replayed insert
    // may follow an expired version of its key, so it supersedes the key's last version.
    // Volatile: set on the caller's thread before the first recovery, read by replay jobs.
    volatile boolean rowTtl;
    // The tablet server's memory for deletion vector state.
    final DerivedDvHeapBudget heapBudget;
    // The tablet server's threads for resolves, catch-ups and checkpoints.
    final DerivedDvRuntime runtime;
    // DvTestHooks.NONE outside tests
    final DvTestHooks testHooks;
    final TailOwner tailOwner = new TailOwner();
    // ids of the change lists and replay maps in the bucket's RocksDB
    final AtomicLong tailIds = new AtomicLong();
    // a snapshot a background catch-up could not reach, answered as stale like a passed one
    long unreachable = Long.MIN_VALUE;
    // keys looked up in the index for masks
    final AtomicLong lookedUpKeys = new AtomicLong();
    // The most keys one lookup in the index takes; a closing bucket waits for one such lookup.
    static final int RESOLVE_CHUNK = 10_000;
    // lookups of lake positions running on the key-lookup threads: the owner counts them as it
    // starts them
    // and as their results come
    int inFlight = 0;
    // where the state is rebuilt and advanced from; null until the bucket's leader recovers
    @Nullable RecoverySource recoverySource;

    // A frontier covers exactly [snapshotStartOffset, cut), so it answers any read whose floor its
    // cut reaches. A read that does not wait reuses the latest frontier for this long while no
    // lookup is pending, before a newer one is built; a read that waits takes it on the request
    // thread. 0 turns both off.
    static final long MASK_CACHE_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    // volatile: request threads read it, and tests set it
    volatile long maskCacheNanos = MASK_CACHE_NANOS;
    // The latest frontier is rebuilt as lookups drain, at most this often, while reads that do not
    // wait come.
    static final long READY_REFRESH_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    // volatile as tests set it
    volatile long readyRefreshNanos = READY_REFRESH_NANOS;

    // Snapshots already moved past, so a late notification or request never moves the state
    // backwards; lake snapshot ids are not ordered, only the chain is. The owner writes it and
    // request threads read it through passed(), under its own monitor.
    final LinkedHashSet<Long> passed = new LinkedHashSet<>();
    static final int PASSED_LIMIT = 4096;

    // the frontiers kept for union reads, and the highest cut served to them
    final DvFrontiers frontiers = new DvFrontiers();
    final ServedCuts servedCuts = new ServedCuts();
    // The snapshot registered right after the covered one, as the last catch-up listed the chain
    // (nextRegistrationOf is the covered snapshot it follows). A read that does not wait and asks
    // for it reads the covered snapshot instead.
    long nextRegistration = Long.MIN_VALUE;
    long nextRegistrationOf = Long.MIN_VALUE;
    // The newest registration the bucket heard of: the end of the chain a catch-up listed, or a
    // snapshot a notification named since. A read is sent back only to the snapshot right before
    // it, as on Paimon only the readable snapshot and the one before stay protected from expiry.
    long newestRegistration = Long.MIN_VALUE;
    // what the latest index holds beyond the covered version, after a load ingested into it
    final DvLatestIndexState latestIndex = new DvLatestIndexState();
    // false: a switch looks up every lake key again instead of the keys it changed; volatile as
    // tests set it
    volatile boolean targetedSwitches = true;
    // switches that kept the masks, and those whose derivation failed and looked up every key
    final AtomicLong targetedCount = new AtomicLong();
    final AtomicLong targetedFallbacks = new AtomicLong();
    final DvRebuild rebuild = new DvRebuild(this);
    // the write path's key changes, which the owner drains
    final DvHandOff handOff = new DvHandOff(() -> drain.postDrain());
    // the union reads waiting for their masks, which the owner answers
    final DvMaskReads maskReads = new DvMaskReads(this);

    /**
     * Makes {@code next} the covered version and releases the owner's reference to the one it
     * replaces; readers that retained it keep it until they end.
     */
    void replaceVersion(DvVersion next) {
        DvVersion previous = version;
        version = next;
        previous.release();
        // the paths of the masked files are read through the new version
        maskState.lakeMasks.forgetPaths();
    }

    /**
     * The offset up to which masks are built: the high watermark, but no changes not handed over.
     */
    long servableHighWatermark() {
        return servableAt(highWatermark.getAsLong());
    }

    /** The offset up to which masks are built once the changes below {@code hw} are applied. */
    long servableAt(long hw) {
        return handedOffEnd >= 0 ? Math.min(hw, handedOffEnd) : hw;
    }

    /**
     * Takes the bucket's leadership over with {@code src}; an owner step. The first time, the
     * highest cut served starts at what the bucket had at takeover, the high watermark or the local
     * log end, whichever is higher. The last leader served masks up to its high watermark at most,
     * and every in-sync replica, this one too, had the log up to there. A follower's own high
     * watermark may lag the last leader's, so the log end is the safe bound.
     */
    void takeOver(RecoverySource src) {
        if (recoverySource == null) {
            servedCuts.raise(Math.max(highWatermark.getAsLong(), src.logEndOffset()));
        }
        recoverySource = src;
    }

    /**
     * Frees heap when the tablet server's deletion vector memory is exceeded: drops the frontier
     * kept for reads that do not wait across a switch. Returns false if there is none.
     */
    boolean shedForBudget() {
        if (!frontiers.dropKept()) {
            return false;
        }
        post(() -> maskState.reportTail(false));
        return true;
    }

    /** The bucket's account in the tablet server's budget. */
    final class TailOwner implements DerivedDvHeapBudget.Owner {
        @Override
        public boolean free() {
            return shedForBudget();
        }
    }

    /** Retains the covered version for a job, an owner step; null once the bucket closed. */
    @Nullable
    DvVersion retainForJob() {
        return !closed && version.tryRetain() ? version : null;
    }

    /**
     * Covers the empty lake, as nothing is readable yet; an owner step. Its change list is the
     * whole log, which is replayed now rather than on the first union read, as on Paimon reads come
     * before the first compacted snapshot. An empty lake change list replayed already is kept.
     */
    void coverTheEmptyLake() {
        if (coveredSnapshotId >= 0 || !emptyLakeTracked) {
            maskState.renew(KeepReady.NONE);
            if (coveredSnapshotId >= 0) {
                replaceVersion(DvVersion.open(db, leases, -1, 0, null, 0));
            }
            coveredSnapshotId = -1;
            coveredIdentity = null;
            snapshotStartOffset = 0;
            emptyLakeTracked = false;
            maskState.tailFailure = null;
            maskState.resetTail();
        }
        if (!closed && maskState.tailMissing()) {
            rebuild.scheduleReplay(-1);
        }
        LOG.info(
                "No lake snapshot yet: the deletion vectors cover the empty lake from"
                        + " offset 0 and replay the log in the background.");
    }

    /** The error for a snapshot this bucket does not cover; the caller re-plans on a newer one. */
    StaleSnapshotException stale(long requestedSnapshotId) {
        metrics.staleSnapshot();
        return new StaleSnapshotException(
                coveredSnapshotId, requestedSnapshotId, false, snapshotStartOffset);
    }

    /** The error for a snapshot the published state {@code s} does not cover. */
    StaleSnapshotException stale(long requestedSnapshotId, DvState s) {
        metrics.staleSnapshot();
        return new StaleSnapshotException(
                s.coveredSnapshotId, requestedSnapshotId, false, s.readableOffset);
    }

    /** Records that the bucket moved past {@code snapshotId}, an owner step. */
    void markPassed(long snapshotId) {
        synchronized (passed) {
            if (snapshotId >= 0 && passed.add(snapshotId) && passed.size() > PASSED_LIMIT) {
                Iterator<Long> it = passed.iterator();
                it.next();
                it.remove();
            }
        }
    }

    /** Records that the bucket covers {@code snapshotId} again, an owner step. */
    void unmarkPassed(long snapshotId) {
        synchronized (passed) {
            passed.remove(snapshotId);
        }
    }

    /** Whether the bucket moved past {@code snapshotId}; any thread. */
    boolean passed(long snapshotId) {
        synchronized (passed) {
            return passed.contains(snapshotId);
        }
    }

    /** Builds the state the owner publishes; an owner step. */
    DvState buildState() {
        DvState.Kind kind;
        DvLoad loading = work.load();
        if (closed) {
            kind = DvState.Kind.CLOSED;
        } else if (recoverySource == null) {
            kind = DvState.Kind.UNRECOVERED;
        } else if (rebuild.pending > 0) {
            kind = DvState.Kind.REBUILDING;
        } else if (loading != null && loading.kind == DvLoad.Kind.SWITCH) {
            kind = DvState.Kind.SWITCHING;
        } else if (loading != null && loading.kind == DvLoad.Kind.TAIL) {
            kind = DvState.Kind.REPLAYING;
        } else {
            kind = DvState.Kind.COVERING;
        }
        boolean drained = maskState.lookupsDrained();
        return new DvState(
                kind,
                closed ? null : version,
                coveredSnapshotId,
                snapshotStartOffset,
                coveredIdentity,
                !maskState.tailMissing(),
                rebuild.retryPending(),
                drained,
                !drained && maskState.fromRebuild,
                frontiers.latest(),
                frontiers.kept(),
                nextRegistration,
                nextRegistrationOf,
                newestRegistration,
                unreachable,
                handOff.failuresTaken());
    }

    /**
     * Runs after every owner step: answers the reads that can be answered now and publishes the
     * state.
     */
    void afterStep() {
        if (closed) {
            maskReads.closed();
        } else {
            maskReads.evaluateWaiting();
        }
        publish();
    }

    /** Publishes the state as it is now, an owner step. */
    void publish() {
        state = buildState();
    }

    int queuedNow() {
        DvReplay replay = rebuild.replaying;
        return maskState.tail.queue.size() + (replay == null ? 0 : replay.tail.queue.size());
    }

    DvInspection inspectNow() {
        return new DvInspection(
                coveredSnapshotId,
                maskState.tail.changeCount(),
                maskState.tail.queue.size(),
                maskState.tail.deadVersions.getLongCardinality(),
                maskState.lakeMasks.files(),
                maskState.unresolved.size(),
                servedCuts.get(),
                inFlight > 0,
                recoverySource != null && !maskState.tailMissing());
    }

    /** A snapshot was registered: the bucket catches up to it in the background. An owner step. */
    void registered(long snapshotId) {
        if (recoverySource == null || coveredSnapshotId == snapshotId || passed(snapshotId)) {
            return;
        }
        // a newer registration than any known: reads are not sent back past the covered one
        newestRegistration = snapshotId;
        work.want(snapshotId, -1, null);
    }

    // ---- the facade's calls, on the callers' threads ----

    /** Marks the table as one with row TTL; before the first recovery. */
    void useRowTtl() {
        rowTtl = true;
    }

    /** Names the bucket in log lines. */
    void setLogName(String name) {
        logName = name;
    }

    /** Whether the bucket's leader closed. */
    boolean isClosed() {
        return closed;
    }

    /**
     * The first log offset {@code bucket} needs; see {@link DerivedDvManager#logRetainOffset}. Off
     * the owner: it reads the volatile readable offset, the kept frontier's, the published state's,
     * and the pinned cuts'.
     */
    long logRetainOffset(DerivedDvManager bucket) {
        long retain = Math.min(snapshotStartOffset, frontiers.keptStartOffset());
        // Reads are served from the published state until the owner publishes the next one, so
        // the log its version and kept frontier read stays too: a frontier the budget dropped, or
        // the snapshot before a switch, until the step that changed them published.
        DvState published = state;
        if (published != null) {
            retain = Math.min(retain, published.readableOffset);
            DvFrontier kept = published.kept;
            if (kept != null) {
                retain = Math.min(retain, kept.readableOffset());
            }
        }
        return Math.max(0L, Math.min(retain, heapBudget.pinnedCuts().minStartOffset(bucket)));
    }

    // ---- the local disk ----

    /** Whether a failure is one of the local disk keeping a tail. */
    static boolean isDiskFailure(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof TailDiskException) {
                return true;
            }
        }
        return false;
    }

    /** Removes the local files of index data that was downloaded but not ingested. */
    static void deleteDownloads(List<PositionIndexStore.BucketData> index) {
        for (PositionIndexStore.BucketData d : index) {
            deleteFiles(d.localSsts);
        }
    }

    /** Deletes local files of index data, if still there (ingestion moves them). */
    static void deleteFiles(@Nullable List<String> paths) {
        if (paths == null) {
            return;
        }
        for (String p : paths) {
            try {
                Files.deleteIfExists(Paths.get(p));
            } catch (IOException e) {
                LOG.warn("Could not delete the deletion vector file {}.", p, e);
            }
        }
    }

    /** The change list was dropped as the hand-off overflowed: the owner fell behind its writes. */
    static final class HandOffOverflowException extends IOException {
        private static final long serialVersionUID = 1L;

        HandOffOverflowException(String message) {
            super(message);
        }
    }

    /** The local disk failed to keep the change list; it is replayed from the log. */
    static final class TailDiskException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        TailDiskException(Throwable cause) {
            super("The deletion vector change list could not be kept on disk: " + cause, cause);
        }
    }
}
