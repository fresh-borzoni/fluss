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

/**
 * The owner of a bucket's derived deletion vector state: the one place its transitions run, one
 * step at a time. It holds the state, changes it only in its steps, and publishes it as an
 * immutable {@link DvState} after each step, which lookups take without a lock.
 *
 * <h3>Mailbox</h3>
 *
 * <p>The owner is a mailbox on the tablet server's {@code kv-deletion-vectors-bucket-state} pool
 * ({@link DvOwnerMailbox}). A step runs to completion and never waits for another step, a job or a
 * request; it calls the RocksDB only for one write or read at a time. Steps of a bucket run one at
 * a time, in the order they were posted. The long work runs in jobs on the runtime's pools ({@link
 * DerivedDvRuntime}), which post their results back as steps. A job gets immutable inputs: a
 * retained version, offsets. What jobs and requests read off the owner is the published state, or a
 * field that is volatile or atomic and says why.
 *
 * <p>This class holds the bucket's state and publishes it. Its steps live in parts: the work queue
 * and the loads' swaps ({@link DvOwnerWork}), planning and the catch-up backoff ({@link
 * DvOwnerPlanner}), the index's dictionary writes, ingest and checkpoints ({@link DvOwnerIndex}),
 * and faults and close ({@link DvOwnerFaults}).
 *
 * <h3>States</h3>
 *
 * <table>
 *   <tr><th>State</th><th>Serves</th><th>Load in flight</th></tr>
 *   <tr><td>UNRECOVERED</td><td>nothing: lookups are not ready</td><td>none</td></tr>
 *   <tr><td>REBUILDING(none to T)</td><td>the empty lake's version answers lookups of snapshot
 *       -1</td><td>FULL load to T</td></tr>
 *   <tr><td>COVERING(v)</td><td>version v</td><td>none</td></tr>
 *   <tr><td>SWITCHING(v to T)</td><td>v</td><td>SWITCH: the index changes after v up to T</td>
 *       </tr>
 *   <tr><td>REBUILDING(v to T)</td><td>v</td><td>FULL load, when v left the chain or T is
 *       readable below v</td></tr>
 *   <tr><td>CLOSED</td><td>nothing: requests go to the new leader</td><td>jobs stop at their
 *       next unit</td></tr>
 * </table>
 *
 * <p>The empty lake is a version too: snapshot -1, readable from 0, with an empty index. Besides
 * the kind, the state carries the snapshot the last catch-up found unreachable, and the targets
 * wanted, which wait in a queue while one load runs.
 *
 * <h3>Transitions</h3>
 *
 * <p>The states are those of {@link DvState.Kind}; v is the covered version, T a target snapshot,
 * and the empty lake is a version too: snapshot -1, readable from 0, with an empty index.
 *
 * <table>
 *   <tr><th>State</th><th>Recover(src)</th><th>Want(T)</th><th>Load done</th>
 *       <th>Load or swap fails</th><th>Close</th></tr>
 *   <tr><td>UNRECOVERED</td><td>plan: REBUILDING(none to T), or COVERING(v-1)</td><td>not
 *       ready</td><td>-</td><td>-</td><td>CLOSED</td></tr>
 *   <tr><td>REBUILDING(none to T)</td><td>-</td><td>queued</td><td>swap: COVERING(v_T)</td>
 *       <td>released; the full load is planned again by the next request or registration</td>
 *       <td>CLOSED</td></tr>
 *   <tr><td>COVERING(v)</td><td>-</td><td>covered or passed: answered; else planned:
 *       SWITCHING, REBUILDING(v to T), or unreachable</td><td>-</td><td>-</td><td>CLOSED</td>
 *       </tr>
 *   <tr><td>SWITCHING(v to T)</td><td>-</td><td>queued</td><td>swap: COVERING(v_T); the next
 *       target</td><td>released; the latest index is dirty through what was ingested; backoff;
 *       COVERING(v)</td><td>CLOSED; the load stops before its next change</td></tr>
 *   <tr><td>REBUILDING(v to T)</td><td>-</td><td>queued</td><td>swap: COVERING(v_T)</td>
 *       <td>released; dirty FULL if it ingested; COVERING(v)</td><td>CLOSED</td></tr>
 *   <tr><td>CLOSED</td><td>-</td><td>ignored</td><td>released</td><td>released</td><td>-</td>
 *       </tr>
 * </table>
 *
 * <p>A step that fails unexpectedly logs at ERROR and counts in {@code dvOwnerFaultPerSecond}; only
 * a {@link VirtualMachineError} then stops the tablet server, as on its other pools.
 */
final class DvOwner {

    private static final Logger LOG = LoggerFactory.getLogger(DvOwner.class);

    // The owner's parts (see the class javadoc); each keeps the bookkeeping of its own steps.
    final DvOwnerMailbox steps;
    final DvOwnerWork work = new DvOwnerWork(this);
    final DvOwnerPlanner planner = new DvOwnerPlanner(this);
    final DvOwnerIndex index = new DvOwnerIndex(this);
    final DvOwnerFaults faults = new DvOwnerFaults(this);

    DvOwner(
            DvRocksDB dvRocksDB,
            int checkpointInterval,
            DerivedDvMetrics metrics,
            DerivedDvRuntime runtime,
            Executor stateExecutor,
            DvTestHooks testHooks) {
        this.db = dvRocksDB;
        this.leases = new DvDbLeases(dvRocksDB);
        this.checkpointInterval = checkpointInterval;
        this.metrics = metrics;
        this.runtime = runtime;
        this.testHooks = testHooks;
        this.index.checkpointTimer = runtime.scheduler();
        this.steps = new DvOwnerMailbox(this, stateExecutor);
        // the server's rebuild slots, unless a background recovery names others
        this.rebuild.limiter = runtime.rebuilds();
        this.version = DvVersion.open(db, leases, -1, 0, null, 0);
        this.state = buildState();
    }

    /** Posts a step; it runs after the steps posted before it ({@link DvOwnerMailbox}). */
    void post(Runnable step) {
        steps.post(step);
    }

    final DvRocksDB db;

    // Names the bucket in log lines. Volatile: jobs log it too.
    volatile String logName = "table=- partition=- bucket=-";

    // The fields below are the owner's: only its steps read and write them, unless noted
    // otherwise. Lookups take the published state instead.
    long coveredSnapshotId = -1;
    // what the covered snapshot's id named when it was registered
    @Nullable String coveredIdentity;
    // the covered snapshot's readable offset; coveredSnapshotId -1 is the empty lake at offset 0
    long snapshotStartOffset = 0;
    // The index is exported every checkpointInterval chain entries, so a rebuild replays only the
    // later entries and tiering can trim the chain.
    final int checkpointInterval;
    final DerivedDvMetrics metrics;
    long entriesApplied = 0;
    // Volatile for jobs and lookups, which stop early once the tablet closes.
    volatile boolean closed = false;
    // Who may still call the RocksDB, which the manager owns: it closes once the last lease after
    // close() is released.
    final DvDbLeases leases;
    // The version of the index the bucket covers: lookups and lake positions are read through it.
    // Starts as the empty lake's. The owner's; readers take it from the published state.
    DvVersion version;
    // The state the owner published after its last step; lookups take it without a lock.
    volatile DvState state;
    // How long close() waits for the background users of the RocksDB.
    static final long CLOSE_WAIT_MS = 30_000;
    // volatile: close() reads it on the caller's thread, and tests set it
    volatile long closeWaitMs = CLOSE_WAIT_MS;
    // the tablet server's deletion vector threads
    final DerivedDvRuntime runtime;
    // DvTestHooks.NONE outside tests
    final DvTestHooks testHooks;
    // a snapshot a background catch-up could not reach, answered as stale like a passed one
    long unreachable = Long.MIN_VALUE;
    // The most keys one lookup in the index takes; a closing bucket waits for one such lookup.
    static final int RESOLVE_CHUNK = 10_000;
    // where the state is rebuilt and advanced from; null until the bucket's leader recovers
    @Nullable RecoverySource recoverySource;

    // Snapshots already moved past, so a late notification or request never moves the state
    // backwards; lake snapshot ids are not ordered, only the chain is. The owner writes it and
    // request threads read it through passed(), under its own monitor.
    final LinkedHashSet<Long> passed = new LinkedHashSet<>();
    static final int PASSED_LIMIT = 4096;

    // what the latest index holds beyond the covered version, after a load ingested into it
    final DvLatestIndexState latestIndex = new DvLatestIndexState();
    final DvRebuild rebuild = new DvRebuild();

    /**
     * Makes {@code next} the covered version and releases the owner's reference to the one it
     * replaces; readers that retained it keep it until they end.
     */
    void replaceVersion(DvVersion next) {
        DvVersion previous = version;
        version = next;
        previous.release();
    }

    /** Takes the bucket's leadership over with {@code src}; an owner step. */
    void takeOver(RecoverySource src) {
        recoverySource = src;
    }

    /** Retains the covered version for a job, an owner step; null once the bucket closed. */
    @Nullable
    DvVersion retainForJob() {
        return !closed && version.tryRetain() ? version : null;
    }

    /** Covers the empty lake, as nothing is readable yet; an owner step. */
    void coverTheEmptyLake() {
        if (coveredSnapshotId >= 0) {
            replaceVersion(DvVersion.open(db, leases, -1, 0, null, 0));
        }
        coveredSnapshotId = -1;
        coveredIdentity = null;
        snapshotStartOffset = 0;
        LOG.info("No lake snapshot yet: the deletion vectors cover the empty lake.");
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
        } else {
            kind = DvState.Kind.COVERING;
        }
        return new DvState(
                kind,
                closed ? null : version,
                coveredSnapshotId,
                snapshotStartOffset,
                coveredIdentity,
                unreachable);
    }

    /** Runs after every owner step: publishes the state. */
    void afterStep() {
        publish();
    }

    /** Publishes the state as it is now, an owner step. */
    void publish() {
        state = buildState();
    }

    // ---- the facade's calls, on the callers' threads ----

    /** Names the bucket in log lines. */
    void setLogName(String name) {
        logName = name;
    }

    /** Whether the bucket's leader closed. */
    boolean isClosed() {
        return closed;
    }

    /** A snapshot was registered: the bucket catches up to it in the background. An owner step. */
    void registered(long snapshotId) {
        if (recoverySource == null || coveredSnapshotId == snapshotId || passed(snapshotId)) {
            return;
        }
        work.want(snapshotId, -1, null);
    }

    // ---- the local disk ----

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
}
