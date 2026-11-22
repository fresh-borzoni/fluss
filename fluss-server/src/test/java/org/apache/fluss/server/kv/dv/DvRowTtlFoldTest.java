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
import org.apache.fluss.server.kv.RowTtlFilterClock;
import org.apache.fluss.server.kv.dv.DerivedDvManagerTest.Index;
import org.apache.fluss.server.kv.snapshot.RowTtlFilterState;
import org.apache.fluss.utils.clock.ManualClock;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.dict;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The cap of the row TTL filter, and what a bucket whose masks may miss an expired version serves
 * ({@link DvRowTtlGuard}). Each time the bucket covers a snapshot, it caps its TTL filter below the
 * TTL of the oldest version above the readable offset. On Iceberg the readable offset never goes
 * below P, the offset the filter was capped for, unless the bucket's saved TTL state is corrupt:
 * the leader logs it at ERROR, counts it, and serves no masks, as after a failed rebuild. On Paimon
 * it can, and union reads read the bucket by key below P. Snapshot 100 is readable from offset 10,
 * snapshot 101 from 14; the cut of 14 reads the log [10, 14). A test calls the filter where the
 * write path and KV compaction would.
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
        assertThat(unionRead(100).getLogEndOffset()).isEqualTo(14L);
        assertThat(manager.foldsForRowTtl(10, 14)).isFalse();
        assertThat(metrics.rowTtlFolds()).isZero();
        assertThat(metrics.rowTtlCorruptions()).isZero();
    }

    /**
     * On Paimon, a pin of an unsafe bucket fails, whether the read waits or not, before any masks
     * are built or pinned: it takes no cut, so it neither keeps one nor evicts the cut of another
     * reader. A pin while the bucket is safe pins its cut.
     */
    @Test
    void testAPinOfAnUnsafePaimonBucketPinsNoCut() throws Exception {
        manager.useRowTtl(TTL, clock, true);
        recoverAtSnapshot100();
        waitUntil(() -> manager.rowTtlFilterClock().cappedFrom() == 10);
        assertThat(manager.pinCut(100, true, null).logEndOffset()).isEqualTo(14L);
        assertThat(manager.pinnedCutCount()).isEqualTo(1);
        manager.dropPinnedCut(100, 14);

        // the filter was capped for an offset above the readable one: a version in between may
        // have expired after it was tiered there
        manager.rowTtlFilterClock().coveredAt(12);
        for (boolean waits : new boolean[] {true, false}) {
            assertThatThrownBy(() -> manager.pinCut(100, waits, 7L))
                    .isInstanceOfSatisfying(
                            DvRowTtlFoldException.class, e -> assertThat(e.cut()).isEqualTo(14L));
            assertThat(manager.pinnedCutCount()).isZero();
        }
        assertThat(metrics.rowTtlFolds()).isEqualTo(2L);
        assertThat(metrics.rowTtlCorruptions()).isZero();
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
     * On Paimon, a bucket that covers a snapshot readable from an earlier offset than its filter
     * was capped for folds there, as a version in between may have expired after it was tiered; its
     * filter expires nothing there, as the tracker dropped what was below the later offset.
     */
    @Test
    void testAPaimonBucketCoveringAnEarlierOffsetThanItsCapFolds() throws Exception {
        manager.useRowTtl(TTL, clock, true);
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
        assertThat(metrics.rowTtlCorruptions()).isZero();
    }

    /**
     * On Iceberg, a KV restored from a snapshot at 14 that saved no state of the filter is corrupt:
     * the leader logs it, and a bucket that covers a snapshot readable below 14 serves no masks.
     * Reads read it by key, its rounds wait, and its filter expires nothing, until it covers a
     * snapshot readable from 14.
     */
    @Test
    void testARestoreWithoutTheFiltersStateServesNoMasksBelowTheSnapshotOffset() throws Exception {
        assertARestoreWithCorruptStateServesNoMasksBelow14(null, "saved no state");
    }

    /** Saved slices that do not follow each other are corrupt too. */
    @Test
    void testARestoreWithSlicesOutOfOrderServesNoMasksBelowTheSnapshotOffset() throws Exception {
        assertARestoreWithCorruptStateServesNoMasksBelow14(
                new RowTtlFilterState(
                        10, 10, Arrays.asList(new long[] {14, WRITTEN}, new long[] {12, 0})),
                "slices out of order");
    }

    private void assertARestoreWithCorruptStateServesNoMasksBelow14(
            @Nullable RowTtlFilterState state, String saved) throws Exception {
        manager.useRowTtl(TTL, clock);
        RowTtlFilterClock filter = manager.rowTtlFilterClock();
        try (ErrorLog errors = ErrorLog.of(DvRowTtlGuard.class)) {
            manager.restoredRowTtlFilter(14, state);
            assertThat(errors.messages).hasSize(1);
            assertThat(errors.messages.get(0)).contains(saved, "corrupt");

            recoverAtSnapshot100();
            assertThat(errors.messages).hasSize(2);
            assertThat(errors.messages.get(1))
                    .contains("readable from offset 10, below offset 14", "serves no masks");
        }
        assertServesNoMasks(100, 14);
        // the reads and rounds started no replay of the change list
        assertThat(src.replayedFrom).containsExactly(10L);
        assertThat(filter.getAsLong()).isEqualTo(RowTtlFilterClock.EXPIRES_NOTHING);
        assertThat(metrics.rowTtlCorruptions()).isEqualTo(2L);

        filter.track(14, WRITTEN);
        src.publish(101, 2, 14, dict(2, F2), new Index().put("k1", 2, 0).build());
        src.readable = new long[] {101, 14};
        assertThat(manager.ensureCovered(101)).isTrue();
        assertThat(unionRead(101).getLogEndOffset()).isEqualTo(14L);
        clock.advanceTime(Duration.ofDays(1));
        waitUntil(() -> filter.getAsLong() == CAP);
        assertThat(metrics.rowTtlCorruptions()).isEqualTo(2L);
        assertThat(metrics.rowTtlFolds()).isZero();
    }

    /**
     * On Iceberg, a cover below P, here as the KV snapshot's leader capped for 14, is corrupt, and
     * so is a switch to a snapshot readable from 13, still below 14: neither serves masks. A switch
     * to one readable from 14 serves them again.
     */
    @Test
    void testACoverBelowTheCapServesNoMasks() throws Exception {
        manager.useRowTtl(TTL, clock);
        manager.restoredRowTtlFilter(
                14,
                new RowTtlFilterState(14, 10, Collections.singletonList(new long[] {14, WRITTEN})));
        assertThat(metrics.rowTtlCorruptions()).isZero();
        try (ErrorLog errors = ErrorLog.of(DvRowTtlGuard.class)) {
            recoverAtSnapshot100();
            assertThat(errors.messages).hasSize(1);
            assertThat(errors.messages.get(0)).contains("readable from offset 10, below offset 14");
        }
        assertServesNoMasks(100, 14);
        assertThat(metrics.rowTtlCorruptions()).isEqualTo(1L);

        src.publish(101, 2, 13, dict(2, F2), new Index().put("k1", 2, 0).build());
        src.readable = new long[] {101, 13};
        assertThat(manager.ensureCovered(101)).isTrue();
        assertServesNoMasks(101, 14);
        assertThat(metrics.rowTtlCorruptions()).isEqualTo(2L);

        src.publish(102, 3, 14, dict(3, F3), new Index().put("k4", 3, 0).build());
        src.readable = new long[] {102, 14};
        assertThat(manager.ensureCovered(102)).isTrue();
        assertThat(unionRead(102).getLogEndOffset()).isEqualTo(14L);
        assertThat(roundMask(102, 14)).isNotNull();
        assertThat(metrics.rowTtlCorruptions()).isEqualTo(2L);
        assertThat(metrics.rowTtlFolds()).isZero();
    }

    /**
     * On Iceberg, a lake without a snapshot is covered from offset 0. Below a cap of 5, which a
     * restore without the filter's state took, it serves no masks either, and replays nothing.
     */
    @Test
    void testAnEmptyLakeBelowTheCapServesNoMasks() throws Exception {
        manager.useRowTtl(TTL, clock);
        manager.restoredRowTtlFilter(5, null);
        src.append("k1", 0, 0);
        src.append("k2", 1, 1);
        highWatermark.set(2);
        manager.recover(src);
        assertServesNoMasks(-1, 2);
        assertThat(src.replayedFrom).isEmpty();
        assertThat(metrics.rowTtlCorruptions()).isEqualTo(2L);
    }

    /**
     * On Paimon, a KV restored from a snapshot at 14 that saved no state of the filter is corrupt
     * too, and reads fold below 14, as the readable offset may move back there.
     */
    @Test
    void testAPaimonRestoreWithoutTheFiltersStateFoldsBelowTheSnapshotOffset() throws Exception {
        manager.useRowTtl(TTL, clock, true);
        manager.restoredRowTtlFilter(14, null);
        RowTtlFilterClock filter = manager.rowTtlFilterClock();
        recoverAtSnapshot100();
        assertThat(manager.foldsForRowTtl(10, 14)).isTrue();
        assertThat(manager.foldsForRowTtl(10, 10)).isFalse();
        assertThat(filter.getAsLong()).isEqualTo(RowTtlFilterClock.EXPIRES_NOTHING);
        assertThat(metrics.rowTtlCorruptions()).isEqualTo(1L);

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
     * WRITTEN} below 14, keeps masks long after the TTL.
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
        assertThat(unionRead(100).getLogEndOffset()).isEqualTo(14L);
        assertThat(manager.foldsForRowTtl(10, 14)).isFalse();
        assertThat(metrics.rowTtlCorruptions()).isZero();
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

    /**
     * The bucket covers {@code snapshotId} and serves no masks, as after a failed rebuild: a read
     * that does not wait reads it by key up to {@code cut}, one that waits is not ready, and so are
     * the parts of its rounds. No read folds for row TTL.
     */
    private void assertServesNoMasks(long snapshotId, long cut) throws Exception {
        waitUntil(() -> manager.state().coveredSnapshotId == snapshotId);
        assertThatThrownBy(() -> manager.pinCut(snapshotId, false, null))
                .isInstanceOfSatisfying(
                        DeletionVectorsNotReadyException.class,
                        e -> assertThat(e.getRebuildingCut()).isEqualTo(cut));
        assertThatThrownBy(() -> manager.pinCut(snapshotId, true, null))
                .isInstanceOfSatisfying(
                        DeletionVectorsNotReadyException.class,
                        e -> {
                            assertThat(e.isRebuilding()).isFalse();
                            assertThat(e).hasMessageContaining("corrupt");
                        });
        assertThatThrownBy(() -> manager.roundMask(snapshotId, null, cut))
                .isInstanceOf(DeletionVectorsNotReadyException.class)
                .hasMessageContaining("corrupt");
        assertThatThrownBy(() -> manager.roundDeletes(snapshotId, null, cut))
                .isInstanceOf(DeletionVectorsNotReadyException.class)
                .hasMessageContaining("corrupt");
        assertThat(manager.pinnedCutCount()).isZero();
        assertThat(manager.foldsForRowTtl(10, cut)).isFalse();
        assertThat(metrics.rowTtlFolds()).isZero();
    }

    /** The round's log mask, once the bucket serves it. */
    private DvRoundMask roundMask(long snapshotId, long end) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (true) {
            try {
                return manager.roundMask(snapshotId, null, end);
            } catch (DeletionVectorsNotReadyException e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(10);
            }
        }
    }

    /** Captures the ERROR messages of a logger until closed. */
    private static final class ErrorLog extends AbstractAppender implements AutoCloseable {
        private final String loggerName;
        final List<String> messages = new CopyOnWriteArrayList<>();

        private ErrorLog(String loggerName) {
            super("errors-" + loggerName, null, null, true, Property.EMPTY_ARRAY);
            this.loggerName = loggerName;
        }

        static ErrorLog of(Class<?> logger) {
            ErrorLog log = new ErrorLog(logger.getName());
            log.start();
            LoggerContext context = (LoggerContext) LogManager.getContext(false);
            Configuration config = context.getConfiguration();
            LoggerConfig loggerConfig = new LoggerConfig(log.loggerName, Level.ERROR, false);
            loggerConfig.addAppender(log, Level.ERROR, null);
            config.addLogger(log.loggerName, loggerConfig);
            context.updateLoggers();
            return log;
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
        }

        @Override
        public void close() {
            LoggerContext context = (LoggerContext) LogManager.getContext(false);
            context.getConfiguration().removeLogger(loggerName);
            context.updateLoggers();
            stop();
        }
    }
}
