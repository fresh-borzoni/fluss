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
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.server.kv.dv.DerivedDvManager.RecoverySource;

import javax.annotation.Nullable;

import java.util.List;

/**
 * What a bucket's owner ({@link DvOwner}) plans for a target snapshot: covered already, a switch, a
 * full rebuild, or unreachable, from the published chain and the state, and the backoff of
 * catch-ups that failed. Planning reads the chain off the owner; the other methods run as owner
 * steps.
 */
final class DvOwnerPlanner {

    private final DvOwner o;

    DvOwnerPlanner(DvOwner o) {
        this.o = o;
    }

    // How long a failed background catch-up waits before a request starts it again.
    static final long ADVANCE_RETRY_INITIAL_BACKOFF_MS = 1_000;
    static final long ADVANCE_RETRY_MAX_BACKOFF_MS = 30_000;
    // background work a request started: the snapshot a catch-up goes to
    long advancing = Long.MIN_VALUE;
    // background catch-ups are numbered; one that failed is tried again after a backoff, and
    // the requests that waited for it are answered as not ready
    long advances = 0;
    long lastFailedAdvance = -1;
    long lastFailedAdvanceSnapshot = Long.MIN_VALUE;
    @Nullable Exception lastAdvanceFailure;
    long advanceRetryAtMs = 0;
    long advanceBackoffMs = ADVANCE_RETRY_INITIAL_BACKOFF_MS;

    /** What planning a target decided. */
    enum PlanKind {
        // the bucket covers it
        COVERED,
        // not reachable: not registered yet, passed, or older than the covered snapshot
        NOT_COVERED,
        // a test holds the switches: tried again later
        HELD,
        // a switch load reaches it
        SWITCH,
        // a full load reaches it, which waits for a rebuild slot
        FULL
    }

    /** A plan of the owner's: what reaches a target, with the load that does. */
    static final class Plan {
        static final Plan COVERED = new Plan(PlanKind.COVERED, null);
        static final Plan NOT_COVERED = new Plan(PlanKind.NOT_COVERED, null);
        static final Plan HELD = new Plan(PlanKind.HELD, null);

        final PlanKind kind;
        @Nullable final DvLoad load;

        private Plan(PlanKind kind, @Nullable DvLoad load) {
            this.kind = kind;
            this.load = load;
        }

        static Plan of(DvLoad load) {
            return new Plan(
                    load.kind == DvLoad.Kind.SWITCH ? PlanKind.SWITCH : PlanKind.FULL, load);
        }
    }

    static int chainIndex(List<PositionIndexStore.ChainEntry> chain, long snapshotId) {
        for (int i = chain.size() - 1; i >= 0; i--) {
            if (chain.get(i).snapshotId == snapshotId) {
                return i;
            }
        }
        return -1;
    }

    /** What planning a target reads of the state, taken by the owner. */
    static final class PlanInput {
        final long target;
        final RecoverySource src;
        final long from;
        final long fromOffset;
        final boolean dirty;
        final boolean reset;
        final long dirtyThrough;
        final long plannedAtNanos = System.nanoTime();

        PlanInput(
                long target,
                RecoverySource src,
                long from,
                long fromOffset,
                boolean dirty,
                boolean reset,
                long dirtyThrough) {
            this.target = target;
            this.src = src;
            this.from = from;
            this.fromOffset = fromOffset;
            this.dirty = dirty;
            this.reset = reset;
            this.dirtyThrough = dirtyThrough;
        }
    }

    /**
     * Takes what planning {@code snapshotId} reads of the state; null if the bucket covers it,
     * passed it, or covers nothing yet.
     */
    @Nullable
    PlanInput planInput(long snapshotId) {
        if (o.coveredSnapshotId == snapshotId
                || o.closed
                || o.recoverySource == null
                || o.passed(snapshotId)) {
            return null;
        }
        return new PlanInput(
                snapshotId,
                o.recoverySource,
                o.coveredSnapshotId,
                o.snapshotStartOffset,
                o.latestIndex.isDirty(),
                o.latestIndex.isFull(),
                o.latestIndex.throughSnapshot());
    }

    /**
     * Plans the way to a target, on a job of the owner, which runs one load at a time: not
     * reachable, a switch load or a full load. Reads the lake, not the state.
     */
    Plan plan(PlanInput in) throws Exception {
        RecoverySource src = in.src;
        long snapshotId = in.target;
        Long offset = src.snapshotOffset(snapshotId);
        if (offset == null) {
            return Plan.NOT_COVERED; // not registered as readable yet
        }
        List<PositionIndexStore.ChainEntry> chain = src.chain();
        int target = chainIndex(chain, snapshotId);
        if (target < 0) {
            return Plan.NOT_COVERED; // chain marker not visible yet
        }
        int start = in.from < 0 ? -1 : chainIndex(chain, in.from);
        int dirtyThrough =
                in.dirtyThrough == Long.MIN_VALUE ? -1 : chainIndex(chain, in.dirtyThrough);
        if (target < dirtyThrough) {
            // A failed ingest left changes of a later snapshot in the index, which a switch or
            // rebuild to this one would keep. Readers plan again on a later snapshot.
            return Plan.NOT_COVERED;
        }
        // A readable offset below the covered snapshot's (a lake whose readable offsets lag its
        // commits) is rebuilt in full, as is a full rebuild that failed after it cleared the index.
        if (start < 0 || in.reset || (target > start && offset < in.fromOffset)) {
            // Full rebuild. It waits for a rebuild slot, so the owner hands it to the rebuild
            // limiter.
            return Plan.of(new DvLoad.FullLoad(o, src, snapshotId, offset, chain));
        }
        if (target < start) {
            return Plan.NOT_COVERED; // older than the covered snapshot
        }
        if (o.rebuild.limiter.switchesHeld()) {
            // a test reads while the bucket covers the snapshot before: tried again later
            return Plan.HELD;
        }
        return Plan.of(
                new DvLoad.SwitchLoad(
                        o,
                        src,
                        chain,
                        start,
                        target,
                        snapshotId,
                        offset,
                        in.from,
                        in.fromOffset,
                        in.dirty,
                        in.plannedAtNanos));
    }

    /** The retriable answer to a request whose catch-up failed. */
    DeletionVectorsNotReadyException advanceFailed(long snapshotId) {
        return new DeletionVectorsNotReadyException(
                String.format(
                        "Deletion vectors cover lake snapshot %d; switching to snapshot %d"
                                + " failed (%s) and is retried.",
                        o.coveredSnapshotId, snapshotId, lastAdvanceFailure),
                lastAdvanceFailure);
    }

    /**
     * Starts a background catch-up to {@code snapshotId} unless one is running, and returns its
     * number. Returns -1 if the last catch-up to it failed less than a backoff ago, so requests do
     * not start it again and again.
     */
    long scheduleAdvance(long snapshotId) {
        if (advancing == snapshotId) {
            return advances;
        }
        if (lastFailedAdvanceSnapshot == snapshotId
                && System.currentTimeMillis() < advanceRetryAtMs) {
            return -1;
        }
        advancing = snapshotId;
        long attempt = ++advances;
        o.work.want(snapshotId, attempt, null);
        return attempt;
    }

    /**
     * Records how a background catch-up to {@code snapshotId} ended: covered, not reachable (not
     * registered, or older than the covered snapshot), or failed. A failure is reported to waiting
     * requests and retried after a backoff. {@code attempt} numbers the request-started catch-up,
     * or is -1.
     */
    void advanceDone(long snapshotId, long attempt, boolean covered, @Nullable Exception failure) {
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
            if (!covered && attempt >= 0 && o.coveredSnapshotId != snapshotId) {
                // e.g. not registered: requests for it are answered as stale and start no catch-up
                o.unreachable = snapshotId;
            }
        }
    }
}
