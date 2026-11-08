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

import org.apache.fluss.server.kv.RowTtlFilterClock;
import org.apache.fluss.server.kv.dv.DerivedDvManagerTest.Index;
import org.apache.fluss.server.kv.snapshot.RowTtlFilterState;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.dict;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The cap of the row TTL filter and the fold ({@link DvRowTtlGuard}): each time the bucket covers a
 * snapshot, it caps its TTL filter below the TTL of the oldest version above the readable offset,
 * and has union reads read it by key only when its KV may have lost one anyway. Snapshot 100 is
 * readable from offset 10, snapshot 101 from 14; the cut of 14 reads the log [10, 14). A test calls
 * the filter where the write path and KV compaction would.
 */
class DvRowTtlFoldTest extends DerivedDvManagerTestBase {

    private static final Duration TTL = Duration.ofMinutes(10);
    private static final long WRITTEN = 1_000_000;
    private static final long CAP = WRITTEN + TTL.toMillis() - 1;

    private final ManualClock clock = new ManualClock(WRITTEN);

    /**
     * Once the bucket covers a snapshot, the filter's time stops below the TTL of the oldest
     * version above it, however long tiering stalls; masks stay exact.
     */
    @Test
    void testAStallLongerThanTheTtlKeepsMasks() throws Exception {
        manager.useRowTtl(TTL, clock);
        RowTtlFilterClock filter = manager.rowTtlFilterClock();
        filter.track(10, WRITTEN);
        filter.track(12, WRITTEN + 1);
        assertThat(filter.getAsLong()).isEqualTo(RowTtlFilterClock.EXPIRES_NOTHING);
        recoverAtSnapshot100();
        waitUntil(() -> filter.getAsLong() != RowTtlFilterClock.EXPIRES_NOTHING);

        clock.advanceTime(Duration.ofDays(30));
        assertThat(filter.getAsLong()).isEqualTo(CAP);
        assertThat(manager.foldsForRowTtl(10, 14)).isFalse();
        assertThat(metrics.rowTtlFolds()).isZero();
    }

    /**
     * A pin of an unsafe bucket fails, whether the read waits or not, before any masks are built or
     * pinned: it takes no cut, so it neither holds one nor evicts the cut of another reader. A pin
     * while the bucket is safe pins its cut.
     */
    @Test
    void testAPinOfAnUnsafeBucketPinsNoCut() throws Exception {
        manager.useRowTtl(TTL, clock);
        recoverAtSnapshot100();
        waitUntil(() -> manager.rowTtlFilterClock().cappedFrom() == 10);
        assertThat(manager.pinCut(100, true, null, 1).logEndOffset()).isEqualTo(14L);
        assertThat(manager.pinnedCutCount()).isEqualTo(1);
        assertThat(manager.releaseCut(100, 14, 1)).isTrue();

        // the filter was capped for an offset above the readable one: a version in between may
        // have expired after it was tiered there
        manager.rowTtlFilterClock().coveredAt(12);
        for (boolean waits : new boolean[] {true, false}) {
            assertThatThrownBy(() -> manager.pinCut(100, waits, 7L, 1))
                    .isInstanceOfSatisfying(
                            DvRowTtlFoldException.class, e -> assertThat(e.cut()).isEqualTo(14L));
            assertThat(manager.pinnedCutCount()).isZero();
        }
        assertThat(metrics.rowTtlFolds()).isEqualTo(2L);
    }

    /** A switch to a snapshot readable from a later offset lifts the cap past what it tiered. */
    @Test
    void testASwitchToANewerSnapshotLiftsTheCap() throws Exception {
        manager.useRowTtl(TTL, clock);
        RowTtlFilterClock filter = manager.rowTtlFilterClock();
        filter.track(10, WRITTEN);
        clock.advanceTime(Duration.ofSeconds(1));
        filter.track(14, WRITTEN + TTL.toMillis());
        recoverAtSnapshot100();
        clock.advanceTime(Duration.ofDays(1));
        waitUntil(() -> filter.getAsLong() == CAP);

        src.publish(101, 2, 14, dict(2, F2), new Index().put("k1", 2, 0).build());
        src.readable = new long[] {101, 14};
        assertThat(manager.ensureCovered(101)).isTrue();
        waitUntil(() -> filter.getAsLong() == CAP + TTL.toMillis());
        assertThat(filter.cappedFrom()).isEqualTo(14);
        assertThat(manager.foldsForRowTtl(14, 16)).isFalse();
    }

    /**
     * A bucket that covers a snapshot readable from an earlier offset than its filter was capped
     * for folds there, as a version in between may have expired after it was tiered; its filter
     * expires nothing there, as the tracker dropped what was below the later offset.
     */
    @Test
    void testABucketCoveringAnEarlierOffsetThanItsCapFolds() throws Exception {
        manager.useRowTtl(TTL, clock);
        RowTtlFilterClock filter = manager.rowTtlFilterClock();
        filter.track(10, WRITTEN);
        clock.advanceTime(Duration.ofSeconds(1));
        filter.track(14, WRITTEN + TTL.toMillis());
        recoverAtSnapshot100();
        src.publish(101, 2, 14, dict(2, F2), new Index().put("k1", 2, 0).build());
        src.readable = new long[] {101, 14};
        assertThat(manager.ensureCovered(101)).isTrue();
        waitUntil(() -> filter.cappedFrom() == 14);

        // a read pinned on snapshot 100 after the switch
        assertThat(manager.foldsForRowTtl(10, 14)).isTrue();
        assertThat(manager.foldsForRowTtl(14, 16)).isFalse();
        // a rebuild back to snapshot 100
        filter.coveredAt(10);
        assertThat(filter.getAsLong()).isEqualTo(RowTtlFilterClock.EXPIRES_NOTHING);
        assertThat(manager.foldsForRowTtl(10, 14)).isTrue();
    }

    /**
     * A KV restored from a snapshot at 14 saved without the filter's state was compacted below 14
     * by a filter this leader did not cap: reads fold, and the filter expires nothing, until the
     * covered snapshot is readable at or above 14.
     */
    @Test
    void testARestoreWithoutTheFiltersStateFoldsBelowTheSnapshotOffset() throws Exception {
        assertARestoreWithoutStateFoldsBelow14(null);
    }

    /** Saved slices that do not follow each other count as no state. */
    @Test
    void testARestoreWithSlicesOutOfOrderUsesNoState() throws Exception {
        assertARestoreWithoutStateFoldsBelow14(
                new RowTtlFilterState(
                        10, 10, Arrays.asList(new long[] {14, WRITTEN}, new long[] {12, 0})));
    }

    private void assertARestoreWithoutStateFoldsBelow14(RowTtlFilterState state) throws Exception {
        manager.useRowTtl(TTL, clock);
        manager.restoredRowTtlFilter(14, state);
        RowTtlFilterClock filter = manager.rowTtlFilterClock();
        recoverAtSnapshot100();
        assertThat(manager.foldsForRowTtl(10, 14)).isTrue();
        assertThat(manager.foldsForRowTtl(10, 10)).isFalse();
        assertThat(filter.getAsLong()).isEqualTo(RowTtlFilterClock.EXPIRES_NOTHING);

        filter.track(14, WRITTEN);
        src.publish(101, 2, 14, dict(2, F2), new Index().put("k1", 2, 0).build());
        src.readable = new long[] {101, 14};
        assertThat(manager.ensureCovered(101)).isTrue();
        clock.advanceTime(Duration.ofDays(1));
        waitUntil(() -> filter.getAsLong() == CAP);
        assertThat(manager.foldsForRowTtl(14, 16)).isFalse();
    }

    /**
     * A KV restored with the state its leader saved, P = 10 and a version written at {@code
     * WRITTEN} below 14, keeps masks long after the TTL; one whose leader capped for 12 may have
     * lost a version in [10, 12).
     */
    @Test
    void testARestoreWithTheFiltersStateKeepsMasks() throws Exception {
        manager.useRowTtl(TTL, clock);
        manager.restoredRowTtlFilter(
                14,
                new RowTtlFilterState(10, 10, Collections.singletonList(new long[] {14, WRITTEN})));
        recoverAtSnapshot100();
        waitUntil(() -> manager.rowTtlFilterClock().getAsLong() == WRITTEN);
        clock.advanceTime(Duration.ofDays(1));
        assertThat(manager.rowTtlFilterClock().getAsLong()).isEqualTo(CAP);
        assertThat(manager.foldsForRowTtl(10, 14)).isFalse();
    }

    @Test
    void testARestoreWithALaterPFolds() throws Exception {
        manager.useRowTtl(TTL, clock);
        manager.restoredRowTtlFilter(
                14,
                new RowTtlFilterState(12, 10, Collections.singletonList(new long[] {14, WRITTEN})));
        recoverAtSnapshot100();
        assertThat(manager.foldsForRowTtl(10, 14)).isTrue();
    }

    /** The tracker's heap counts towards the deletion vectors' heap budget. */
    @Test
    void testTheTrackerCountsTowardsTheHeapBudget() throws Exception {
        manager.useRowTtl(TTL, clock);
        long before = DvTestManagers.budget().totalBytes();
        for (int i = 0; i < 128; i++) {
            manager.rowTtlFilterClock().track(20 + i, WRITTEN);
            clock.advanceTime(Duration.ofSeconds(1));
        }
        assertThat(DvTestManagers.budget().totalBytes() - before).isGreaterThanOrEqualTo(64 * 32L);
    }

    /** A table without row TTL never folds for it. */
    @Test
    void testATableWithoutRowTtlNeverFolds() throws Exception {
        recoverAtSnapshot100();
        clock.advanceTime(Duration.ofDays(365));

        assertThat(manager.foldsForRowTtl(10, 14)).isFalse();
        assertThat(manager.rowTtlFilterClock()).isNull();
        assertThat(metrics.rowTtlFolds()).isZero();
    }
}
