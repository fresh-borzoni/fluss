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

package org.apache.fluss.flink.source.epochs;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.flink.source.deserializer.FlussDeserializationSchema;
import org.apache.fluss.flink.source.emitter.FlinkRecordEmitter;
import org.apache.fluss.flink.source.reader.RecordAndPos;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplit;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplitState;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.LogSplitState;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.split.SourceSplitState;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.RowType;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.core.io.InputStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ReaderEpochs}: how a reader ends epochs, and replays them after a restore. */
class ReaderEpochsTest {

    private static final long TABLE_ID = 7L;
    private static final long INTERVAL_MS = 100L;
    private static final TableBucket B0 = new TableBucket(TABLE_ID, 0);
    private static final TableBucket B1 = new TableBucket(TABLE_ID, 1);

    private TestingEpochStore store;
    private ReaderEpochs<String> epochs;
    private RecordEmitter<RecordAndPos, String, SourceSplitState> emitter;
    private RecordingOutput output;
    private long now;

    @BeforeEach
    void before() {
        start(INTERVAL_MS);
    }

    private void start(long intervalMs) {
        store = new TestingEpochStore();
        now = 0L;
        epochs =
                new ReaderEpochs<>(
                        new EpochSettings("domain", intervalMs),
                        store,
                        bucket -> 0L,
                        () -> now,
                        () -> {});
        emitter = epochs.wrap(new FlinkRecordEmitter<>(new KeyAtOffset()));
        output = new RecordingOutput();
    }

    /**
     * Splits that start at random times, some empty, some with a snapshot that may be empty, under
     * short epochs and a clock that jumps: every bucket records its start before its progress, and
     * every ended epoch resolves to where the bucket was when the epoch ended.
     */
    @Test
    void randomSplitsAndClockEndEveryEpochWhereTheirBucketsWere() throws Exception {
        long[] intervals = {1L, 50L, 100L};
        for (int seed = 0; seed < 60; seed++) {
            Random random = new Random(seed);
            long intervalMs = intervals[seed % intervals.length];
            start(intervalMs);
            now = random.nextInt(1000) * intervalMs;
            List<SimulatedSplit> splits = new ArrayList<>();
            int buckets = 1 + random.nextInt(4);
            for (int step = 0; step < 300; step++) {
                int action = random.nextInt(10);
                if (splits.size() < buckets && (splits.isEmpty() || action == 0)) {
                    splits.add(addRandomSplit(random, new TableBucket(TABLE_ID, splits.size())));
                } else if (action < 3) {
                    // the clock stays in the epoch, passes a few, or many while nothing polls
                    int jump = random.nextInt(10);
                    now +=
                            jump < 4
                                    ? random.nextInt((int) intervalMs + 1)
                                    : jump < 9
                                            ? intervalMs * (1 + random.nextInt(3))
                                            : intervalMs * (5 + random.nextInt(20));
                } else if (action < 7 && !splits.isEmpty()) {
                    splits.get(random.nextInt(splits.size())).emitNext(random);
                } else {
                    store.holdWrites = random.nextBoolean();
                    if (random.nextInt(3) == 0) {
                        store.completeWrites();
                    }
                    drive();
                }
            }
            // complete every update and the last epoch
            store.holdWrites = false;
            store.completeWrites();
            for (SimulatedSplit split : splits) {
                split.completeUpdate();
            }
            drive();
            now += 2 * intervalMs;
            drive();
            store.completeWrites();
            drive();
            for (SimulatedSplit split : splits) {
                split.verify(String.format("seed %s interval %s", seed, intervalMs));
            }
        }
    }

    private SimulatedSplit addRandomSplit(Random random, TableBucket bucket) throws Exception {
        int kind = random.nextInt(3);
        long offset = random.nextInt(5);
        SourceSplitBase split =
                kind == 0
                        ? new LogSplit(bucket, null, offset)
                        : new HybridSnapshotLogSplit(
                                bucket,
                                null,
                                1L,
                                0L,
                                false,
                                offset,
                                LogSplit.NO_STOPPING_OFFSET,
                                false);
        // a hybrid split of kind 1 has an empty snapshot
        int snapshotRows = kind == 2 ? 1 + random.nextInt(4) : 0;
        boolean wasStarted = epochs.getLastWatermark() != Long.MIN_VALUE;
        // the reader waits for the writes of a split it adds; writes held before stay held
        boolean hold = store.holdWrites;
        store.holdWrites = false;
        SimulatedSplit simulated =
                new SimulatedSplit(bucket, add(split), kind != 0, snapshotRows, offset);
        store.holdWrites = hold;
        // the start of the bucket is durable before any progress of it
        int startWrite = -1;
        int progressWrite = -1;
        for (int i = 0; i < store.writes.size(); i++) {
            for (EpochRecord record : store.writes.get(i)) {
                if (record.getBucket().equals(bucket) && record.isProgress()) {
                    progressWrite = progressWrite < 0 ? i : progressWrite;
                } else if (record.getBucket().equals(bucket)) {
                    startWrite = startWrite < 0 ? i : startWrite;
                }
            }
        }
        assertThat(startWrite).isGreaterThanOrEqualTo(0);
        if (wasStarted) {
            assertThat(progressWrite).isGreaterThan(startWrite);
        }
        return simulated;
    }

    /** A split whose changes a test emits, which remembers where its bucket was after each. */
    private final class SimulatedSplit {
        final TableBucket bucket;
        final SourceSplitState state;
        final boolean hybrid;
        final int snapshotRows;
        final long startPosition;
        long nextOffset;
        int snapshotEmitted;
        boolean snapshotFinished;
        boolean updateOpen;

        /** Per emitted change: its epoch and the position of the bucket after it. */
        final List<long[]> emitted = new ArrayList<>();

        /** Positions between the two halves of an update, where no epoch may end. */
        final List<Long> insideUpdates = new ArrayList<>();

        SimulatedSplit(
                TableBucket bucket,
                SourceSplitState state,
                boolean hybrid,
                int snapshotRows,
                long offset) {
            this.bucket = bucket;
            this.state = state;
            this.hybrid = hybrid;
            this.snapshotRows = snapshotRows;
            this.startPosition = hybrid ? Epochs.EMPTY : offset;
            this.nextOffset = offset;
        }

        void emitNext(Random random) throws Exception {
            if (!readyToEmit()) {
                return;
            }
            if (hybrid && !snapshotFinished) {
                if (snapshotEmitted < snapshotRows) {
                    snapshotEmitted++;
                    emitted(() -> emitSnapshotRow(state, snapshotEmitted, snapshotEmitted));
                } else {
                    snapshotFinished = true;
                    emitted(
                            () ->
                                    emitter.emitRecord(
                                            RecordAndPos.snapshotPhaseFinished(), output, state));
                }
            } else if (updateOpen) {
                updateOpen = false;
                long offset = nextOffset++;
                emitted(() -> emit(state, offset, ChangeType.UPDATE_AFTER, (int) offset));
            } else if (random.nextInt(3) == 0) {
                updateOpen = true;
                long offset = nextOffset++;
                emitted(() -> emit(state, offset, ChangeType.UPDATE_BEFORE, (int) offset));
                insideUpdates.add(nextOffset);
            } else {
                // sometimes an offset is missing, e.g. skipped by a filter
                nextOffset += random.nextInt(5) == 0 ? 1 : 0;
                long offset = nextOffset++;
                emitted(() -> emit(state, offset, ChangeType.INSERT, (int) offset));
            }
        }

        /**
         * Polls like the reader before it emits a record: it emits one only when the epochs leave
         * the poll to the records, not while it waits for its ends to be recorded.
         */
        private boolean readyToEmit() {
            for (int i = 0; i < 1000; i++) {
                InputStatus status = epochs.beforePoll(output);
                if (status != InputStatus.MORE_AVAILABLE) {
                    return status == null;
                }
            }
            throw new AssertionError("The epochs do not settle.");
        }

        void completeUpdate() throws Exception {
            while (updateOpen && !readyToEmit()) {
                store.completeWrites();
            }
            if (updateOpen) {
                updateOpen = false;
                long offset = nextOffset++;
                emitted(() -> emit(state, offset, ChangeType.UPDATE_AFTER, (int) offset));
            }
        }

        private void emitted(ThrowingRunnable emission) throws Exception {
            // a fresh scan holds nothing back: the change is of the epoch after the watermark
            long epoch = epochs.getLastWatermark() + 1;
            int stamped = output.stamps.size();
            emission.run();
            emitted.add(new long[] {epoch, position()});
            // and it comes with its epoch as its timestamp
            for (String stamp : output.stamps.subList(stamped, output.stamps.size())) {
                assertThat(stamp).endsWith("=" + epoch);
            }
        }

        private long position() {
            if (state.isLogSplitState()) {
                return state.asLogSplitState().getNextOffset();
            }
            HybridSnapshotLogSplitState hybridState = state.asHybridSnapshotLogSplitState();
            if (hybridState.isSnapshotFinished()) {
                return hybridState.getNextOffset();
            }
            return hybridState.getRecordsToSkip() > 0 ? Epochs.IN_SNAPSHOT : Epochs.EMPTY;
        }

        void verify(String run) throws Exception {
            Long endedThrough = store.endedThrough(bucket).get();
            assertThat(endedThrough).as("%s: progress of %s", run, bucket).isNotNull();
            assertThat(endedThrough).isEqualTo(epochs.getLastWatermark());
            List<Long> checked = new ArrayList<>();
            checked.add(Epochs.BEFORE_FIRST);
            checked.add(endedThrough);
            for (long[] change : emitted) {
                checked.add(change[0] - 1);
                if (change[0] <= endedThrough) {
                    checked.add(change[0]);
                }
            }
            for (long epoch : checked) {
                long expected = startPosition;
                for (long[] change : emitted) {
                    if (change[0] <= epoch) {
                        expected = change[1];
                    }
                }
                EpochEnd end = store.latestEnd(bucket, epoch).get();
                assertThat(end).as("%s: end of epoch %s in %s", run, epoch, bucket).isNotNull();
                assertThat(Epochs.positionToString(end.getEnd()))
                        .as("%s: end of epoch %s in %s", run, epoch, bucket)
                        .isEqualTo(Epochs.positionToString(expected));
            }
            for (long end : store.ends(bucket).values()) {
                assertThat(insideUpdates).as("%s: ends of %s", run, bucket).doesNotContain(end);
                assertThat(end).isNotEqualTo(Epochs.IN_SNAPSHOT);
            }
        }
    }

    /** An action that may throw. */
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    @Test
    void freshScanEndsTheEpochsThatTheClockPassed() throws Exception {
        LogSplitState split = addLogSplit(B0, 0L);
        // the start of the bucket is recorded before anything else
        assertThat(store.ends(B0)).containsExactly(entry(-1L, 0L));
        assertThat(store.progress(B0)).isNull();

        store.holdWrites = true;
        // the first watermark waits until the bucket recorded that the epochs before it ended
        assertThat(drive()).isEqualTo(InputStatus.NOTHING_AVAILABLE);
        assertThat(output.events).isEmpty();
        store.completeWrites();
        assertThat(drive()).isNull();
        assertThat(output.events).containsExactly("W-1");

        emit(split, 0, ChangeType.INSERT, 1);
        emit(split, 1, ChangeType.INSERT, 2);
        // the clock passed epoch 9: epoch 0 ends after both changes, epochs 1 to 9 are empty
        now = 10 * INTERVAL_MS;
        assertThat(drive()).isEqualTo(InputStatus.NOTHING_AVAILABLE);
        assertThat(output.events).containsExactly("W-1", "+I1@0", "+I2@1");
        store.completeWrites();
        assertThat(drive()).isNull();
        assertThat(output.events).containsExactly("W-1", "+I1@0", "+I2@1", "W9");
        assertThat(store.ends(B0)).containsExactly(entry(-1L, 0L), entry(0L, 2L));
        assertThat(store.progress(B0)).isEqualTo(9L);

        emit(split, 2, ChangeType.INSERT, 3);
        now = 12 * INTERVAL_MS + 50;
        assertThat(drive()).isEqualTo(InputStatus.NOTHING_AVAILABLE);
        store.completeWrites();
        drive();
        // a bucket without changes only records progress
        now = 14 * INTERVAL_MS;
        drive();
        store.completeWrites();
        drive();
        assertThat(output.events)
                .containsExactly("W-1", "+I1@0", "+I2@1", "W9", "+I3@2", "W11", "W13");
        assertThat(store.ends(B0)).containsExactly(entry(-1L, 0L), entry(0L, 2L), entry(10L, 3L));
        assertThat(store.progress(B0)).isEqualTo(13L);
    }

    @Test
    void noEpochEndsBetweenTheTwoHalvesOfAnUpdate() throws Exception {
        LogSplitState split = addLogSplit(B0, 0L);
        drive();
        emit(split, 0, ChangeType.INSERT, 1);
        emit(split, 1, ChangeType.UPDATE_BEFORE, 1);
        now = 10 * INTERVAL_MS;
        assertThat(drive()).isNull();
        assertThat(output.events).containsExactly("W-1", "+I1@0", "-U1@1");

        emit(split, 2, ChangeType.UPDATE_AFTER, 1);
        drive();
        assertThat(output.events).containsExactly("W-1", "+I1@0", "-U1@1", "+U1@2", "W9");
        assertThat(store.ends(B0)).containsExactly(entry(-1L, 0L), entry(0L, 3L));
    }

    @Test
    void restoredSplitReplaysTheEndsItRecorded() throws Exception {
        // a run ended epochs 5 at offset 3 and 8 at offset 6, and others without changes up to 12
        store.putEnd(B0, -1L, 0L);
        store.putEnd(B0, 5L, 3L);
        store.putEnd(B0, 8L, 6L);
        store.putProgress(B0, 12L);
        // its checkpoint was taken at offset 2, inside epoch 5
        LogSplitState split = addLogSplit(B0, 2L);
        int writes = store.writes.size();

        // the first watermark tells that the next change is of epoch 5
        assertThat(drive()).isNull();
        assertThat(output.events).containsExactly("W4");
        for (long offset = 2; offset < 8; offset++) {
            emit(split, offset, ChangeType.INSERT, (int) offset);
        }
        // the changes after the end of epoch 5 wait
        assertThat(output.events).containsExactly("W4", "+I2@2");

        assertThat(drive()).isNull();
        assertThat(output.events)
                .containsExactly(
                        "W4", "+I2@2", "W7", "+I3@3", "+I4@4", "+I5@5", "W12", "+I6@6", "+I7@7");
        // the replayed changes, held back or not, come with the epochs they were in before
        assertThat(output.stamps)
                .containsExactly(
                        "+I2@2=5", "+I3@3=8", "+I4@4=8", "+I5@5=8", "+I6@6=13", "+I7@7=13");
        // replayed ends are not recorded again
        assertThat(store.writes).hasSize(writes);

        // then the clock ends epochs again, after the recorded ones
        now = 100 * INTERVAL_MS;
        drive();
        assertThat(output.events).endsWith("+I7@7", "W99");
        assertThat(store.ends(B0))
                .containsExactly(entry(-1L, 0L), entry(5L, 3L), entry(8L, 6L), entry(13L, 8L));
        assertThat(store.progress(B0)).isEqualTo(99L);
    }

    @Test
    void recordAfterAGapAtARecordedEndWaitsForTheEnd() throws Exception {
        store.putEnd(B0, -1L, 0L);
        store.putEnd(B0, 5L, 4L);
        store.putProgress(B0, 6L);
        LogSplitState split = addLogSplit(B0, 2L);
        drive();
        emit(split, 2, ChangeType.INSERT, 2);
        // offset 3 does not come, e.g. a filter skipped it: the next change is past the end
        emit(split, 4, ChangeType.INSERT, 4);
        assertThat(output.events).containsExactly("W4", "+I2@2");
        drive();
        assertThat(output.events).containsExactly("W4", "+I2@2", "W6", "+I4@4");
    }

    @Test
    void restoredSplitsOfDifferentReadersInterleaveByTheirEpochs() throws Exception {
        // two readers recorded different ends before a restore assigns both buckets to one
        store.putEnd(B0, -1L, 0L);
        store.putEnd(B0, 5L, 10L);
        store.putEnd(B0, 8L, 20L);
        store.putProgress(B0, 12L);
        store.putEnd(B1, -1L, 0L);
        store.putEnd(B1, 6L, 7L);
        store.putEnd(B1, 9L, 9L);
        store.putProgress(B1, 10L);
        LogSplitState first = addLogSplit(B0, 15L);
        LogSplitState second = addLogSplit(B1, 7L);

        drive();
        assertThat(output.events).containsExactly("W7");
        for (long offset = 15; offset < 22; offset++) {
            emit(first, offset, ChangeType.INSERT, (int) offset);
        }
        for (long offset = 7; offset < 10; offset++) {
            emit(second, offset, ChangeType.INSERT, 100 + (int) offset);
        }
        drive();
        assertThat(output.events)
                .containsExactly(
                        "W7", "+I15@15", "+I16@16", "+I17@17", "+I18@18", "+I19@19", "W8",
                        "+I107@7", "+I108@8", "W10", "+I109@9", "W12", "+I20@20", "+I21@21");
        // the second bucket changed in epoch 11, after the ends it recorded before
        assertThat(store.ends(B1))
                .containsExactly(entry(-1L, 0L), entry(6L, 7L), entry(9L, 9L), entry(11L, 10L));
        assertThat(store.progress(B1)).isEqualTo(12L);
        assertThat(store.ends(B0)).containsExactly(entry(-1L, 0L), entry(5L, 10L), entry(8L, 20L));
    }

    @Test
    void restoredSplitAfterItsRecordedEndsChangesAfterItsProgress() throws Exception {
        // the checkpoint was taken in the open epoch after the last recorded end
        store.putEnd(B0, -1L, 0L);
        store.putEnd(B0, 4L, 2L);
        store.putProgress(B0, 6L);
        LogSplitState split = addLogSplit(B0, 3L);

        drive();
        emit(split, 3, ChangeType.INSERT, 3);
        now = 20 * INTERVAL_MS;
        drive();
        // offset 2 was emitted before the checkpoint in epoch 7, which the end covers
        assertThat(output.events).containsExactly("W6", "+I3@3", "W19");
        assertThat(store.ends(B0)).containsExactly(entry(-1L, 0L), entry(4L, 2L), entry(7L, 4L));
    }

    @Test
    void snapshotOfABucketFallsIntoOneEpoch() throws Exception {
        HybridSnapshotLogSplitState split =
                (HybridSnapshotLogSplitState)
                        add(
                                new HybridSnapshotLogSplit(
                                        B0,
                                        null,
                                        1L,
                                        0L,
                                        false,
                                        5L,
                                        LogSplit.NO_STOPPING_OFFSET,
                                        false));
        // before the snapshot that the scan emits as inserts, the bucket is empty
        assertThat(store.ends(B0)).containsExactly(entry(-1L, Epochs.EMPTY));
        drive();
        emitSnapshotRow(split, 1, 1);
        now = 10 * INTERVAL_MS;
        assertThat(drive()).isNull();
        emitSnapshotRow(split, 2, 2);
        emitter.emitRecord(RecordAndPos.snapshotPhaseFinished(), output, split);
        drive();
        emit(split, 5, ChangeType.INSERT, 3);
        now = 20 * INTERVAL_MS;
        drive();
        assertThat(output.events).containsExactly("W-1", "+I1@-1", "+I2@-1", "W9", "+I3@5", "W19");
        // the rows of the snapshot are changes of the first epoch
        assertThat(output.stamps).containsExactly("+I1@-1=0", "+I2@-1=0", "+I3@5=10");
        assertThat(store.ends(B0))
                .containsExactly(entry(-1L, Epochs.EMPTY), entry(0L, 5L), entry(10L, 6L));
    }

    @Test
    void splitAddedLaterStartsAfterTheEndedEpochs() throws Exception {
        LogSplitState first = addLogSplit(B0, 0L);
        drive();
        now = 10 * INTERVAL_MS;
        drive();
        LogSplitState second = addLogSplit(B1, 4L);
        // the epochs ended so far did not change its bucket
        assertThat(store.ends(B1)).containsExactly(entry(-1L, 4L));
        assertThat(store.progress(B1)).isEqualTo(9L);
        emit(second, 4, ChangeType.INSERT, 4);
        emit(first, 0, ChangeType.INSERT, 0);
        now = 20 * INTERVAL_MS;
        drive();
        assertThat(output.events).containsExactly("W-1", "W9", "+I4@4", "+I0@0", "W19");
        assertThat(store.ends(B1)).containsExactly(entry(-1L, 4L), entry(10L, 5L));
        assertThat(store.ends(B0)).containsExactly(entry(-1L, 0L), entry(10L, 1L));
    }

    @Test
    void splitWhoseRecordedEndsTheReaderPassedFails() throws Exception {
        addLogSplit(B0, 0L);
        drive();
        now = 10 * INTERVAL_MS;
        drive();
        store.putEnd(B1, -1L, 0L);
        store.putEnd(B1, 3L, 2L);
        store.putProgress(B1, 5L);
        assertThatThrownBy(() -> addLogSplit(B1, 0L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("too late to replay");
    }

    @Test
    void endOfInputEndsEveryEpoch() throws Exception {
        LogSplitState split = addLogSplit(B0, 0L);
        drive();
        emit(split, 0, ChangeType.INSERT, 1);
        assertThat(epochs.afterPoll(InputStatus.END_OF_INPUT))
                .isEqualTo(InputStatus.NOTHING_AVAILABLE);
        assertThat(epochs.beforePoll(output)).isEqualTo(InputStatus.END_OF_INPUT);
        assertThat(store.ends(B0)).containsExactly(entry(-1L, 0L), entry(0L, 1L));
        assertThat(store.progress(B0)).isEqualTo(Epochs.AFTER_LAST);
    }

    @Test
    void readerWithoutSplitsIsIdle() {
        assertThat(epochs.beforePoll(output)).isNull();
        assertThat(epochs.beforePoll(output)).isNull();
        assertThat(output.events).containsExactly("idle");
    }

    @Test
    void readerWaitsForTheRecordedEnds() throws Exception {
        addLogSplit(B0, 0L);
        store.holdWrites = true;
        drive();
        assertThat(epochs.availability(new CompletableFuture<>())).isNotDone();
        store.completeWrites();
        assertThat(epochs.availability(new CompletableFuture<>())).isDone();
    }

    // ------------------------------------------------------------------------------------------

    /**
     * Polls like the reader: until the epochs leave the poll to the records, or wait for writes
     * that the test did not complete yet.
     */
    @Nullable
    private InputStatus drive() {
        for (int i = 0; i < 1000; i++) {
            InputStatus status = epochs.beforePoll(output);
            boolean writing = status == InputStatus.NOTHING_AVAILABLE && store.pendingWrites() > 0;
            if (status != InputStatus.MORE_AVAILABLE
                    && (status != InputStatus.NOTHING_AVAILABLE || writing)) {
                return status;
            }
        }
        throw new AssertionError("The epochs do not settle.");
    }

    private LogSplitState addLogSplit(TableBucket bucket, long offset) throws Exception {
        return (LogSplitState) add(new LogSplit(bucket, null, offset));
    }

    private SourceSplitState add(SourceSplitBase split) throws Exception {
        SourceSplitBase added = epochs.addingSplits(Collections.singletonList(split)).get(0);
        SourceSplitState state =
                added.isLogSplit()
                        ? new LogSplitState(added.asLogSplit())
                        : new HybridSnapshotLogSplitState(added.asHybridSnapshotLogSplit());
        epochs.track(added, state);
        return state;
    }

    private void emit(SourceSplitState split, long offset, ChangeType changeType, int key)
            throws Exception {
        emitter.emitRecord(
                new RecordAndPos(new ScanRecord(offset, 1L, changeType, GenericRow.of(key))),
                output,
                split);
    }

    private void emitSnapshotRow(SourceSplitState split, int key, long readRecords)
            throws Exception {
        emitter.emitRecord(
                new RecordAndPos(new ScanRecord(GenericRow.of(key)), readRecords), output, split);
    }

    private static Map.Entry<Long, Long> entry(long epoch, long end) {
        return new AbstractMap.SimpleImmutableEntry<>(epoch, end);
    }

    /** Deserializes a change into its kind, key and offset. */
    private static final class KeyAtOffset implements FlussDeserializationSchema<String> {
        @Override
        public void open(InitializationContext context) {}

        @Override
        public String deserialize(LogRecord record) {
            return record.getChangeType().shortString()
                    + record.getRow().getInt(0)
                    + "@"
                    + record.logOffset();
        }

        @Override
        public TypeInformation<String> getProducedType(RowType scanRowType) {
            return Types.STRING;
        }
    }

    /** Records the records and watermarks in the order of their emission. */
    private static final class RecordingOutput implements ReaderOutput<String> {
        final List<String> events = new ArrayList<>();

        /** Per record, the timestamp it came with. */
        final List<String> stamps = new ArrayList<>();

        @Override
        public void collect(String record) {
            events.add(record);
            stamps.add(record + "=none");
        }

        @Override
        public void collect(String record, long timestamp) {
            events.add(record);
            stamps.add(record + "=" + timestamp);
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            events.add("W" + watermark.getTimestamp());
        }

        @Override
        public void markIdle() {
            events.add("idle");
        }

        @Override
        public void markActive() {}

        @Override
        public SourceOutput<String> createOutputForSplit(String splitId) {
            return this;
        }

        @Override
        public void releaseOutputForSplit(String splitId) {}
    }
}
