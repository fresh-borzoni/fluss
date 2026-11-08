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

package org.apache.fluss.server.kv;

import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.server.kv.snapshot.RowTtlFilterState;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.server.kv.RowTtlFilterClock.EXPIRES_NOTHING;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The capped time of the row TTL filter ({@link RowTtlFilterClock}): min(now, B(o) + TTL - 1),
 * where B(o) is the oldest tracked TTL time at or after the readable offset o. The TTL is 10 min;
 * the version at offset 10 was written at {@code W}.
 */
class RowTtlFilterClockTest {

    private static final long TTL = Duration.ofMinutes(10).toMillis();
    private static final long W = 1_000_000;
    private static final long CAP = W + TTL - 1;

    private final ManualClock clock = new ManualClock(W);
    private final RowTtlFilterClock filter = new RowTtlFilterClock(clock, TTL);

    /** Until the bucket covers a snapshot, the filter expires nothing, however much time passes. */
    @Test
    void testTheFilterExpiresNothingUntilTheBucketCoversASnapshot() {
        filter.track(10, W);
        clock.advanceTime(Duration.ofDays(1));
        assertThat(filter.getAsLong()).isEqualTo(EXPIRES_NOTHING);
        assertThat(filter.cappedFrom()).isZero();
        assertThat(filter.capLagMs()).isEqualTo(Duration.ofDays(1).toMillis());
    }

    /**
     * The time is the clock until the cap, then the cap however long tiering stalls: below the
     * expiry of the version at offset 10, the oldest at or after it.
     */
    @Test
    void testTheCapHoldsThroughAStall() {
        filter.track(10, W);
        filter.track(11, W + 5);
        filter.coveredAt(10);
        assertThat(filter.getAsLong()).isEqualTo(W);
        assertThat(filter.capLagMs()).isZero();
        clock.advanceTime(TTL - 1, TimeUnit.MILLISECONDS);
        assertThat(filter.getAsLong()).isEqualTo(CAP);
        clock.advanceTime(Duration.ofDays(30));
        assertThat(filter.getAsLong()).isEqualTo(CAP);
        assertThat(filter.capLagMs()).isEqualTo(clock.milliseconds() - CAP);
        assertThat(filter.safe(10)).isTrue();
        // with nothing tracked at or after the readable offset, nothing is held back
        filter.coveredAt(12);
        assertThat(filter.getAsLong()).isEqualTo(clock.milliseconds());
    }

    /**
     * Any TTL time bounds the cap, also one far in the past, as a backfill by event time or a
     * default of 0: a late row lowers it at once, and it holds until a snapshot holds the row. A
     * value that never expires is not tracked.
     */
    @Test
    void testALateRowHoldsTheCapUntilItIsTiered() {
        filter.track(10, W);
        filter.coveredAt(10);
        clock.advanceTime(Duration.ofDays(1));
        filter.track(11, 0);
        assertThat(filter.getAsLong()).isEqualTo(TTL - 1);
        filter.track(12, KvValueLayout.NEVER_EXPIRE_TIMESTAMP);
        assertThat(filter.getAsLong()).isEqualTo(TTL - 1);
        filter.coveredAt(13);
        assertThat(filter.getAsLong()).isEqualTo(clock.milliseconds());
    }

    /**
     * P is the highest offset the filter was capped for: a bucket that covers an earlier offset
     * again is unsafe below P, and its filter expires nothing there, as the tracker dropped what it
     * held below P.
     */
    @Test
    void testAReadableOffsetMovingBackKeepsP() {
        filter.track(10, W);
        clock.advanceTime(Duration.ofSeconds(1));
        filter.track(14, W + TTL);
        filter.coveredAt(10);
        filter.coveredAt(14);
        assertThat(filter.cappedFrom()).isEqualTo(14);
        clock.advanceTime(Duration.ofDays(1));
        assertThat(filter.getAsLong()).isEqualTo(CAP + TTL);

        filter.coveredAt(10);
        assertThat(filter.cappedFrom()).isEqualTo(14);
        assertThat(filter.safe(10)).isFalse();
        assertThat(filter.safe(14)).isTrue();
        assertThat(filter.getAsLong()).isEqualTo(EXPIRES_NOTHING);
    }

    /**
     * A KV restored from a snapshot at 14 takes P and the tracked times below 14 its leader saved;
     * without them, or with slices that do not follow each other, P is 14 and the filter expires
     * nothing until the bucket covers a snapshot readable at or above 14.
     */
    @Test
    void testARestoreTakesTheSavedStateOrNone() {
        RowTtlFilterClock saved = new RowTtlFilterClock(clock, TTL);
        saved.restored(
                14,
                new RowTtlFilterState(10, 10, Collections.singletonList(new long[] {14, W - 5})));
        saved.track(14, W);
        saved.coveredAt(10);
        assertThat(saved.safe(10)).isTrue();
        clock.advanceTime(Duration.ofDays(1));
        assertThat(saved.getAsLong()).isEqualTo(CAP - 5);

        for (RowTtlFilterState state :
                Arrays.asList(
                        null,
                        new RowTtlFilterState(
                                10,
                                10,
                                Arrays.asList(new long[] {14, W - 5}, new long[] {12, W})))) {
            RowTtlFilterClock without = new RowTtlFilterClock(clock, TTL);
            without.restored(14, state);
            without.track(14, W);
            without.coveredAt(10);
            assertThat(without.getAsLong()).isEqualTo(EXPIRES_NOTHING);
            assertThat(without.safe(10)).isFalse();
            without.coveredAt(14);
            assertThat(without.getAsLong()).isEqualTo(CAP);
            assertThat(without.safe(14)).isTrue();
        }
    }

    /** The state a snapshot saves: P, and the slices below its offset, the one across it cut. */
    @Test
    void testTheSavedStateIsBelowTheSnapshotOffset() {
        filter.track(10, W);
        clock.advanceTime(Duration.ofSeconds(1));
        filter.track(12, W - 7);
        filter.track(15, W + 9);
        filter.coveredAt(10);
        RowTtlFilterState state = filter.saved(13);
        assertThat(state.cappedFrom()).isEqualTo(10);
        assertThat(state.from()).isZero();
        assertThat(state.slices()).hasSize(2);
        assertThat(state.slices().get(0)).containsExactly(11, W);
        assertThat(state.slices().get(1)).containsExactly(13, W - 7);
    }

    /** The tracker's heap goes to the listener as it grows by slices, and when it empties. */
    @Test
    void testTheTrackerReportsItsHeap() {
        AtomicLong heap = new AtomicLong(-1);
        filter.setHeapListener(heap::set);
        assertThat(heap.get()).isZero();
        for (int i = 0; i < 64; i++) {
            filter.track(i, W);
            clock.advanceTime(Duration.ofSeconds(1));
        }
        assertThat(heap.get()).isPositive();
        filter.coveredAt(64);
        assertThat(heap.get()).isZero();
    }

    @Test
    void testExtremeTimesDoNotOverflow() {
        RowTtlFilterClock forever = new RowTtlFilterClock(clock, Long.MAX_VALUE / 2);
        forever.track(10, Long.MAX_VALUE - 1);
        forever.coveredAt(10);
        assertThat(forever.getAsLong()).isEqualTo(W);

        filter.track(10, Long.MIN_VALUE + 1);
        filter.coveredAt(10);
        assertThat(filter.getAsLong()).isEqualTo(Long.MIN_VALUE + TTL);
    }
}
