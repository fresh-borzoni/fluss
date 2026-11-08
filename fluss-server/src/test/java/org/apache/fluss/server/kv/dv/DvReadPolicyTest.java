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

import org.apache.fluss.server.kv.dv.DvReadPolicy.Action;
import org.apache.fluss.server.kv.dv.DvReadPolicy.Answer;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests of {@link DvReadPolicy}: the answers to union reads, as a function of the state. */
class DvReadPolicyTest {

    private static final long NOW = TimeUnit.SECONDS.toNanos(1_000);
    private static final long REUSE = TimeUnit.MILLISECONDS.toNanos(100);

    private final Set<Long> passed = new HashSet<>();

    @Test
    void testClosedAndUnrecoveredBucketsServeNothing() {
        assertThat(answer(state().kind(DvState.Kind.CLOSED), 100, -1).action)
                .isEqualTo(Action.CLOSED);
        assertThat(answer(state().kind(DvState.Kind.UNRECOVERED).covered(-1), -1, -1).action)
                .isEqualTo(Action.NOT_READY);
    }

    @Test
    void testOlderPassedOrUnreachableSnapshotsAreStale() {
        passed.add(90L);
        assertThat(answer(state(), 90, -1).action).isEqualTo(Action.STALE);
        assertThat(answer(state(), -1, -1).action).isEqualTo(Action.STALE);
        assertThat(answer(state().unreachable(105), 105, -1).action).isEqualTo(Action.STALE);
    }

    @Test
    void testReadOfTheNextAndNewestRegistrationIsBehind() {
        Answer a = answer(state().registrations(101, 100, 101), 101, -1);
        assertThat(a.action).isEqualTo(Action.BEHIND);
        assertThat(a.behindSnapshotId).isEqualTo(100L);
        assertThat(a.behindStartOffset).isEqualTo(10L);
        assertThat(a.startSwitch).isTrue();

        // a newer registration is known: the covered snapshot is two back, so the read waits
        assertThat(answer(state().registrations(101, 100, 102), 101, -1).action)
                .isEqualTo(Action.WAIT_FOR_SWITCH);
        // the change list is not in place: no masks of the covered snapshot to read
        assertThat(answer(state().registrations(101, 100, 101).present(false), 101, -1).action)
                .isEqualTo(Action.WAIT_FOR_SWITCH);
    }

    @Test
    void testBucketWithNothingToServeFoldsWithACause() {
        Answer none = answer(state().covered(-1), 100, -1);
        assertThat(none.action).isEqualTo(Action.FOLD);
        assertThat(none.foldCause).isEqualTo("none");
        assertThat(none.startSwitch).isTrue();

        Answer rebuild = answer(state().kind(DvState.Kind.REBUILDING), 101, -1);
        assertThat(rebuild.foldCause).isEqualTo("rebuild");

        Answer replay = answer(state().present(false), 100, -1);
        assertThat(replay.action).isEqualTo(Action.FOLD);
        assertThat(replay.foldCause).isEqualTo("replay");
        assertThat(replay.startReplay).isTrue();
        // after a disk failure the replay waits for its backoff
        assertThat(answer(state().present(false).diskRetry(true), 100, -1).startReplay).isFalse();

        Answer lookups = answer(state().drained(false).rebuiltLookups(true), 100, 12);
        assertThat(lookups.action).isEqualTo(Action.FOLD);
        assertThat(lookups.foldCause).isEqualTo("lookups");
    }

    @Test
    void testFoldCutIsNeverBelowTheServedCut() {
        assertThat(DvReadPolicy.foldCut(20, 14)).isEqualTo(20L);
        assertThat(DvReadPolicy.foldCut(12, 14)).isEqualTo(14L);
        assertThat(DvReadPolicy.foldCut(-1, -1)).isEqualTo(0L);
    }

    @Test
    void testPendingLookupsServeTheLatestFrontierOrTheLakeAlone() {
        DvFrontier latest = frontier(100, 14, NOW - TimeUnit.SECONDS.toNanos(5));
        Answer ready = answer(state().drained(false).latest(latest), 100, 14);
        assertThat(ready.action).isEqualTo(Action.SERVE);
        assertThat(ready.frontier).isEqualTo("ready");
        assertThat(ready.masks.getLogEndOffset()).isEqualTo(14L);
        assertThat(ready.masks.getHighWatermark()).isEqualTo(20L);
        assertThat(ready.startResolve).isTrue();

        // no frontier as new as the served cut: the lake alone, if its readable offset is
        Answer lake = answer(state().drained(false).latest(latest).readable(16), 100, 15);
        assertThat(lake.action).isEqualTo(Action.SERVE);
        assertThat(lake.frontier).isEqualTo("lake");
        assertThat(lake.masks.getLogEndOffset()).isEqualTo(16L);
        assertThat(lake.masks.getLakeDvEntries()).isEmpty();

        // else the read waits for the lookups
        assertThat(answer(state().drained(false).latest(latest), 100, 15).action)
                .isEqualTo(Action.WAIT_FOR_LOOKUPS);
    }

    @Test
    void testDrainedLookupsReuseTheLatestFrontierOnlyForAMoment() {
        DvFrontier fresh = frontier(100, 14, NOW - REUSE / 2);
        Answer reused = answer(state().latest(fresh), 100, 14);
        assertThat(reused.action).isEqualTo(Action.SERVE);
        assertThat(reused.startResolve).isFalse();

        DvFrontier old = frontier(100, 14, NOW - 2 * REUSE);
        assertThat(answer(state().latest(old), 100, 14).action).isEqualTo(Action.BUILD);
        assertThat(answer(state(), 100, 14).action).isEqualTo(Action.BUILD);
    }

    @Test
    void testKeptFrontierServesTheSnapshotBeforeUntilTheCoveredOneHasItsOwn() {
        // the bucket switched from 100 to 101
        passed.add(100L);
        DvFrontier kept = frontier(100, 14, NOW);
        // covered 101 derives its masks: a read of 100 gets the kept frontier
        DvState deriving = state().covered(101).readable(12).drained(false).kept(kept).build();
        Answer previous = answer(deriving, 100, 14);
        assertThat(previous.action).isEqualTo(Action.SERVE);
        assertThat(previous.frontier).isEqualTo("previous");
        assertThat(previous.masks.getLogEndOffset()).isEqualTo(14L);

        // a read of 101, the newest registration, is sent back to 100
        DvState newest =
                state().covered(101)
                        .readable(12)
                        .drained(false)
                        .kept(kept)
                        .registrations(Long.MIN_VALUE, Long.MIN_VALUE, 101)
                        .build();
        Answer behind = answer(newest, 101, 14);
        assertThat(behind.action).isEqualTo(Action.BEHIND);
        assertThat(behind.behindSnapshotId).isEqualTo(100L);
        assertThat(behind.behindStartOffset).isEqualTo(10L);

        // a newer registration is known: 100 is two back, the read waits instead
        DvState newer =
                state().covered(101)
                        .readable(12)
                        .drained(false)
                        .kept(kept)
                        .registrations(Long.MIN_VALUE, Long.MIN_VALUE, 102)
                        .build();
        assertThat(answer(newer, 101, 14).action).isEqualTo(Action.WAIT_FOR_LOOKUPS);

        // a kept frontier below the served cut serves nothing
        assertThat(answer(deriving, 100, 15).action).isEqualTo(Action.STALE);
        // nor once the covered state has a frontier of its own
        DvState own =
                state().covered(101)
                        .readable(12)
                        .drained(false)
                        .kept(kept)
                        .latest(frontier(101, 15, NOW))
                        .build();
        assertThat(answer(own, 100, 14).action).isEqualTo(Action.STALE);
    }

    @Test
    void testWaitingReadTakesTheLatestFrontierOnlyUpToItsFloor() {
        DvState s = state().latest(frontier(100, 14, 0)).build();
        assertThat(DvReadPolicy.servesWaitingRead(s, 100, 14)).isTrue();
        assertThat(DvReadPolicy.servesWaitingRead(s, 100, 15)).isFalse();
        assertThat(DvReadPolicy.servesWaitingRead(s, 101, 14)).isFalse();
        assertThat(DvReadPolicy.servesWaitingRead(state().build(), 100, 0)).isFalse();
    }

    /** No answer, in any state, serves masks below the served cut or goes back in time. */
    @Test
    void testServedMasksAreNeverBelowTheServedCut() {
        Random random = new Random(42);
        for (int i = 0; i < 20_000; i++) {
            long servedCut = random.nextInt(30);
            long covered = 100 + random.nextInt(3) - 1;
            Builder b =
                    state().covered(random.nextInt(8) == 0 ? -1 : covered)
                            .readable(random.nextInt(30))
                            .present(random.nextInt(5) != 0)
                            .drained(random.nextBoolean())
                            .rebuiltLookups(random.nextBoolean())
                            .registrations(
                                    100 + random.nextInt(3),
                                    100 + random.nextInt(3) - 1,
                                    100 + random.nextInt(4));
            if (random.nextBoolean()) {
                b.latest(frontier(covered, random.nextInt(30), NOW - random.nextInt(200_000_000)));
            }
            if (random.nextBoolean()) {
                b.kept(frontier(covered - 1, random.nextInt(30), NOW));
            }
            long requested = 98 + random.nextInt(5);
            DvState s = b.build();
            Answer a = answer(s, requested, servedCut);
            if (a.action == Action.SERVE) {
                assertThat(a.masks.getLogEndOffset())
                        .as("%s %s", s, a)
                        .isGreaterThanOrEqualTo(servedCut);
            }
        }
    }

    // ------------------------------------------------------------------------------------------

    private Answer answer(Builder b, long requested, long servedCut) {
        return answer(b.build(), requested, servedCut);
    }

    private Answer answer(DvState s, long requested, long servedCut) {
        return DvReadPolicy.noWait(s, requested, servedCut, 20, NOW, REUSE, passed::contains);
    }

    private static DvFrontier frontier(long snapshotId, long cut, long builtAtNanos) {
        long start = snapshotId == 100 ? 10 : 12;
        DvSnapshot masks =
                new DvSnapshot(
                        Collections.emptyMap(), null, Math.max(cut, start), start, null, cut);
        return new DvFrontier(snapshotId, masks, builtAtNanos);
    }

    /** A bucket covering snapshot 100 from offset 10, its masks exact, nothing kept. */
    private static Builder state() {
        return new Builder();
    }

    private static final class Builder {
        private DvState.Kind kind = DvState.Kind.COVERING;
        private long covered = 100;
        private long readable = 10;
        private boolean present = true;
        private boolean diskRetry;
        private boolean drained = true;
        private boolean rebuiltLookups;
        @Nullable private DvFrontier latest;
        @Nullable private DvFrontier kept;
        private long next = Long.MIN_VALUE;
        private long nextOf = Long.MIN_VALUE;
        private long newest = Long.MIN_VALUE;
        private long unreachable = Long.MIN_VALUE;

        Builder kind(DvState.Kind kind) {
            this.kind = kind;
            return this;
        }

        Builder covered(long covered) {
            this.covered = covered;
            return this;
        }

        Builder readable(long readable) {
            this.readable = readable;
            return this;
        }

        Builder present(boolean present) {
            this.present = present;
            return this;
        }

        Builder diskRetry(boolean diskRetry) {
            this.diskRetry = diskRetry;
            return this;
        }

        Builder drained(boolean drained) {
            this.drained = drained;
            return this;
        }

        Builder rebuiltLookups(boolean rebuiltLookups) {
            this.rebuiltLookups = rebuiltLookups;
            return this;
        }

        Builder latest(DvFrontier latest) {
            this.latest = latest;
            return this;
        }

        Builder kept(DvFrontier kept) {
            this.kept = kept;
            return this;
        }

        Builder registrations(long next, long nextOf, long newest) {
            this.next = next;
            this.nextOf = nextOf;
            this.newest = newest;
            return this;
        }

        Builder unreachable(long unreachable) {
            this.unreachable = unreachable;
            return this;
        }

        DvState build() {
            return new DvState(
                    kind,
                    null,
                    covered,
                    readable,
                    null,
                    present,
                    diskRetry,
                    drained,
                    rebuiltLookups,
                    latest,
                    kept,
                    next,
                    nextOf,
                    newest,
                    unreachable,
                    0);
        }
    }
}
