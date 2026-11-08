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

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.function.LongPredicate;

/**
 * How a union read that does not wait ({@code client.scanner.deletion-vectors.wait-for-catch-up} =
 * false) is answered, as a pure function of the bucket's state, the highest cut served and the
 * clock. The caller carries the answer out: it serves the frontier, starts what the answer asks to
 * start, or waits.
 *
 * <ul>
 *   <li>A read of the snapshot registered right after the covered one reads the covered one: the
 *       leader answers at once that it is behind ({@link Action#BEHIND}).
 *   <li>While lake-position lookups are pending, a read gets the latest frontier of the covered
 *       state, exact up to its own cut, or the lake snapshot alone, exact up to its readable
 *       offset. With no lookup pending, the owner builds a frontier up to the first change that
 *       needs a lookup ({@link Action#BUILD}).
 *   <li>Across a switch or rebuild, the frontier of the state before is kept for the snapshot it is
 *       of, until the new state has a frontier of its own ({@link DvState#kept}).
 *   <li>A leader with nothing to serve yet (rebuilding, replaying its change list, or covering no
 *       snapshot) answers at once with a cut, and the reader reads the bucket's log by key up to it
 *       ({@link Action#FOLD}).
 *   <li>No answer goes below the highest cut served ({@link ServedCuts}), so a bucket's reads never
 *       go back in time, also across a leader change.
 * </ul>
 */
final class DvReadPolicy {

    private DvReadPolicy() {}

    /** What the answer is. */
    enum Action {
        // serve the answer's masks
        SERVE,
        // the leader covers the snapshot before the requested one: the reader reads that one
        BEHIND,
        // the requested snapshot is not covered and will not be
        STALE,
        // nothing to serve yet: the reader reads the bucket's log by key up to the fold cut
        FOLD,
        // nothing exact is ready as new as what was served: the owner builds a frontier up to the
        // first change that needs a lookup, or the read waits for the lookups
        BUILD,
        // the lookups pending must end first; the read waits for them, as one that waits
        WAIT_FOR_LOOKUPS,
        // the bucket switches to the requested snapshot; the read waits for it, as one that waits
        WAIT_FOR_SWITCH,
        // not recovered yet: answered as not ready
        NOT_READY,
        // the bucket closed
        CLOSED
    }

    /** The answer to a read that does not wait. */
    static final class Answer {
        final Action action;
        // SERVE: the masks; BEHIND: null
        @Nullable final DvSnapshot masks;
        // how the masks were found, for the request's log line: previous, ready, lake; null
        // otherwise
        @Nullable final String frontier;
        // when the served frontier was built, for the request's log line
        final long builtAtNanos;
        // BEHIND: the snapshot the reader reads and where its masks start
        final long behindSnapshotId;
        final long behindStartOffset;
        // FOLD: why
        @Nullable final String foldCause;
        // what the caller starts
        final boolean startSwitch;
        final boolean startReplay;
        final boolean startResolve;

        private Answer(
                Action action,
                @Nullable DvSnapshot masks,
                @Nullable String frontier,
                long builtAtNanos,
                long behindSnapshotId,
                long behindStartOffset,
                @Nullable String foldCause,
                boolean startSwitch,
                boolean startReplay,
                boolean startResolve) {
            this.action = action;
            this.masks = masks;
            this.frontier = frontier;
            this.builtAtNanos = builtAtNanos;
            this.behindSnapshotId = behindSnapshotId;
            this.behindStartOffset = behindStartOffset;
            this.foldCause = foldCause;
            this.startSwitch = startSwitch;
            this.startReplay = startReplay;
            this.startResolve = startResolve;
        }

        private static Answer of(Action action) {
            return new Answer(action, null, null, 0, -1, -1, null, false, false, false);
        }

        private static Answer serve(
                DvSnapshot masks, String frontier, long builtAtNanos, boolean startResolve) {
            return new Answer(
                    Action.SERVE,
                    masks,
                    frontier,
                    builtAtNanos,
                    -1,
                    -1,
                    null,
                    false,
                    false,
                    startResolve);
        }

        private static Answer behind(long snapshotId, long startOffset, boolean startSwitch) {
            return new Answer(
                    Action.BEHIND,
                    null,
                    null,
                    0,
                    snapshotId,
                    startOffset,
                    null,
                    startSwitch,
                    false,
                    false);
        }

        private static Answer fold(String cause, boolean startSwitch, boolean startReplay) {
            return new Answer(
                    Action.FOLD, null, null, 0, -1, -1, cause, startSwitch, startReplay, false);
        }

        private static Answer waitForLookups(boolean startResolve) {
            return new Answer(
                    Action.WAIT_FOR_LOOKUPS,
                    null,
                    null,
                    0,
                    -1,
                    -1,
                    null,
                    false,
                    false,
                    startResolve);
        }

        @Override
        public String toString() {
            return action + (frontier == null ? "" : "(" + frontier + ")");
        }
    }

    /**
     * Answers a read of {@code requested} that does not wait.
     *
     * @param s the bucket's state
     * @param servedCut the highest cut served
     * @param hw the high watermark, which the served masks report
     * @param nowNanos the clock
     * @param reuseNanos how long the latest frontier serves such reads while no lookup is pending,
     *     before the owner builds a newer one
     * @param passed whether the bucket moved past a snapshot
     */
    static Answer noWait(
            DvState s,
            long requested,
            long servedCut,
            long hw,
            long nowNanos,
            long reuseNanos,
            LongPredicate passed) {
        if (s.kind == DvState.Kind.CLOSED) {
            return Answer.of(Action.CLOSED);
        }
        if (s.kind == DvState.Kind.UNRECOVERED) {
            return Answer.of(Action.NOT_READY);
        }
        long covered = s.coveredSnapshotId;
        // The covered state is still being derived: no frontier of its own yet.
        boolean deriving = !s.changeListPresent || !s.lookupsDrained;
        DvFrontier kept = s.kept;
        if (kept != null && s.latest == null && kept.cut() >= servedCut) {
            if (requested == kept.snapshotId) {
                // a read planned again on the snapshot before the switch, or a change list reset
                // on the covered one: the kept frontier, unless the covered one's can be built
                if (requested != covered || deriving) {
                    return Answer.serve(
                            kept.at(hw), "previous", kept.builtAtNanos, requested == covered);
                }
            } else if (requested == covered && s.newestRegistration == covered && deriving) {
                // the snapshot before the covered one is the one before the newest registered
                return Answer.behind(kept.snapshotId, kept.readableOffset(), false);
            }
        }
        if (requested != covered) {
            if (requested < 0 || passed.test(requested) || s.unreachable == requested) {
                return Answer.of(Action.STALE);
            }
            if (s.servesTheSnapshotBefore(requested)) {
                // the bucket keeps switching; this read takes the snapshot it covers
                return Answer.behind(covered, s.readableOffset, true);
            }
            if (covered < 0 || s.kind == DvState.Kind.REBUILDING) {
                // nothing to serve until a rebuild is done: this read reads by key
                return Answer.fold(
                        s.kind == DvState.Kind.REBUILDING ? "rebuild" : "none", true, false);
            }
            return Answer.of(Action.WAIT_FOR_SWITCH);
        }
        if (!s.changeListPresent) {
            // nothing to serve until the change list is replayed: this read reads by key
            return Answer.fold("replay", false, !s.replayRetryPending);
        }
        DvFrontier latest = s.latest;
        if (latest != null
                && latest.cut() >= servedCut
                && (!s.lookupsDrained || nowNanos - latest.builtAtNanos < reuseNanos)) {
            return Answer.serve(latest.at(hw), "ready", latest.builtAtNanos, !s.lookupsDrained);
        }
        if (s.lookupsDrained) {
            return Answer.of(Action.BUILD);
        }
        // the lookups pending go on in the background
        if (s.readableOffset >= servedCut) {
            // No frontier since the bucket switched or rebuilt, and none kept from before: the
            // lake snapshot alone, with the masks of the empty log range up to its readable
            // offset. It is exact without a lookup, and no older than what was served.
            return Answer.serve(lakeOnly(s, hw), "lake", nowNanos, true);
        }
        if (s.rebuiltLookups) {
            // a rebuild's lookups, which may take long: this read reads by key
            return Answer.fold("lookups", false, false);
        }
        return Answer.waitForLookups(true);
    }

    /** The lake snapshot alone: masks of the empty log range up to its readable offset. */
    static DvSnapshot lakeOnly(DvState s, long hw) {
        return new DvSnapshot(
                Collections.emptyMap(), null, s.readableOffset, s.readableOffset, s.identity, hw);
    }

    /**
     * The cut a read that does not wait is told to read by key up to, for a bucket with nothing to
     * serve yet: the high watermark, or the highest cut served if that is higher, as the old
     * leader's log end right after a takeover.
     */
    static long foldCut(long hw, long servedCut) {
        return Math.max(0L, Math.max(hw, servedCut));
    }

    /**
     * Whether the latest frontier serves a read that waits: it is of the covered state, and exact
     * up to a cut no lower than {@code floor}, the high watermark and the changes handed over when
     * the read came, and no lower than the highest cut served.
     */
    static boolean servesWaitingRead(DvState s, long requested, long floor) {
        DvFrontier latest = s.latest;
        return s.kind != DvState.Kind.CLOSED
                && s.kind != DvState.Kind.UNRECOVERED
                && s.coveredSnapshotId == requested
                && latest != null
                && latest.cut() >= floor;
    }
}
