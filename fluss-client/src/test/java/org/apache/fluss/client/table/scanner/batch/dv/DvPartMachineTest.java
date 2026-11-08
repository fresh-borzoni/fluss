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

package org.apache.fluss.client.table.scanner.batch.dv;

import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.table.scanner.batch.IndexedRowIterator;
import org.apache.fluss.client.table.scanner.batch.LogRowIterator;
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketPart.ReadMode;
import org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.PinningServer;
import org.apache.fluss.exception.DeletionVectorsNotReadyException;
import org.apache.fluss.exception.InvalidRequestException;
import org.apache.fluss.exception.LakeTableRolledBackException;
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.ExceptionUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.fluss.client.table.scanner.batch.dv.DvPartState.CLOSED;
import static org.apache.fluss.client.table.scanner.batch.dv.DvPartState.DONE;
import static org.apache.fluss.client.table.scanner.batch.dv.DvPartState.FAILED;
import static org.apache.fluss.client.table.scanner.batch.dv.DvPartState.FOLDING;
import static org.apache.fluss.client.table.scanner.batch.dv.DvPartState.PINNED;
import static org.apache.fluss.client.table.scanner.batch.dv.DvPartState.PINNED_AT_PLANNING;
import static org.apache.fluss.client.table.scanner.batch.dv.DvPartState.PLANNED;
import static org.apache.fluss.client.table.scanner.batch.dv.DvPartState.READING_LAKE;
import static org.apache.fluss.client.table.scanner.batch.dv.DvPartState.READING_LOG;
import static org.apache.fluss.client.table.scanner.batch.dv.DvPartState.READY;
import static org.apache.fluss.client.table.scanner.batch.dv.DvPartState.REPLANNING;
import static org.apache.fluss.client.table.scanner.batch.dv.DvPartState.RESUMING;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.BUCKET;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.EXACT;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.SNAPSHOT;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.TABLE_ID;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.covered;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.masks;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.rowString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests of {@link DvPartMachine}: each answer of a bucket's leader in each state of a part, given
 * by a fake leader, and a close and a restore in every state. The transitions they check are those
 * of the table in the machine's javadoc.
 */
class DvPartMachineTest {

    // a lake snapshot registered after the planned one, with the same files of the bucket
    private static final long NEWER = 6L;
    // the leader's high watermark: the cut it pins
    private static final long CUT = 107L;
    // the rows of the standard bucket as of cut 107, after offset 106 updated key 0
    private static final List<String> AT_107 =
            Arrays.asList("0=late", "1=b", "2=a", "3=a", "5=a", "6=b");
    // the rows of the standard bucket's lake files, unmasked
    private static final List<String> LAKE_ROWS =
            Arrays.asList("0=a", "1=a", "2=a", "3=a", "4=a", "5=a");

    private final DvTestBucket bucket = new DvTestBucket();
    private final DvTestBucket.MaskServer server = bucket.server;
    private final DvTestBucket.FileLakeSource lake = bucket.lake;
    private final DvTestBucket.Context context = bucket.context;
    private PinningServer pins;
    // the cuts the leader pinned for parts that pinned their own
    private final AtomicInteger takenPins = new AtomicInteger();

    @BeforeEach
    void setUp() {
        bucket.standardBucket();
        lake.plan(NEWER, lake.plans.get(SNAPSHOT).toArray(new LakeSplit[0]));
        server.readable = new LakeSnapshot(SNAPSHOT, Collections.singletonMap(BUCKET, 100L));
        pins = bucket.pinningServer();
        // the cut planning pinned for the parts pinned at planning
        assertThat(pins.pin()).isEqualTo(CUT);
        answer(Answer.MASKS, -1);
    }

    // ------------------------------------------------------------------------------------------
    // Each answer in each state

    /** An answer of the bucket's leader to a request about masks. */
    enum Answer {
        MASKS,
        REBUILDING,
        COVERED,
        NOT_READY,
        LEADER_LOST,
        PARTITION_DROPPED,
        ROLLED_BACK,
        INTERRUPTED,
        ERROR
    }

    /** A state a part asks its leader in, with the part, and the index of the request asked. */
    enum Where {
        /** A whole-bucket part that pins its own cut: its first request. */
        PLANNED(DvPartState.PLANNED, 0, 0),
        /** That part once pinned: the fetch of the masks of its second lake file. */
        PINNED(DvPartState.PINNED, 1, 1),
        /** A whole-bucket part pinned at planning: its first fetch. */
        PINNED_AT_PLANNING(DvPartState.PINNED_AT_PLANNING, 0, 0),
        /** A part of the first lake file of a bucket pinned at planning: its first fetch. */
        PINNED_AT_PLANNING_SHARED(DvPartState.PINNED_AT_PLANNING, 0, 0),
        /** A whole-bucket part pinned at planning, restored at its second lake file. */
        PINNED_AT_PLANNING_STARTED(DvPartState.PINNED_AT_PLANNING, 0, 0);

        final DvPartState state;
        final int request;
        final int position;

        Where(DvPartState state, int request, int position) {
            this.state = state;
            this.request = request;
            this.position = position;
        }
    }

    static Stream<Arguments> answersInStates() {
        return Arrays.stream(Where.values())
                .flatMap(w -> Arrays.stream(Answer.values()).map(a -> Arguments.of(w, a)));
    }

    /** The state an answer moves a part to, as the machine's transition table has it. */
    private static DvPartState expectedNext(Where where, Answer answer) {
        switch (where) {
            case PLANNED:
                switch (answer) {
                    case MASKS:
                        return PINNED;
                    case REBUILDING:
                        return FOLDING;
                    case PARTITION_DROPPED:
                        return READY;
                    case ROLLED_BACK:
                    case INTERRUPTED:
                        return FAILED;
                    default:
                        // COVERED plans the covered snapshot; the others plan the readable one
                        return REPLANNING;
                }
            case PINNED:
                switch (answer) {
                    case MASKS:
                    case PARTITION_DROPPED:
                        return READY;
                    case ROLLED_BACK:
                    case INTERRUPTED:
                    case ERROR:
                        return FAILED;
                    default:
                        // the cut is lost: pin again
                        return PLANNED;
                }
            default:
                switch (answer) {
                    case MASKS:
                        return READY;
                    case ROLLED_BACK:
                    case INTERRUPTED:
                        return FAILED;
                    default:
                        return where == Where.PINNED_AT_PLANNING
                                ? PLANNED
                                : where == Where.PINNED_AT_PLANNING_SHARED ? FOLDING : FAILED;
                }
        }
    }

    @ParameterizedTest(name = "{0} x {1}")
    @MethodSource("answersInStates")
    void testEachAnswerInEachState(Where where, Answer answer) {
        answer(answer, where.request);

        Outcome outcome = read(partOf(where));

        DvPartState next = expectedNext(where, answer);
        assertThat(outcome.history.get(where.position)).isEqualTo(where.state);
        assertThat(outcome.history.get(where.position + 1))
                .as("states %s", outcome.history)
                .isEqualTo(next);
        assertThat(outcome.history).last().isEqualTo(CLOSED);
        // every pin of the part's own is released as its read ends, also if it failed
        assertThat(outcome.releasesBeforeClose).isEqualTo(takenPins.get());
        assertThat(bucket.releases()).hasSize(takenPins.get());
        if (next == FAILED) {
            assertThat(outcome.failure).isNotNull();
            assertThat(outcome.history).containsSubsequence(FAILED, CLOSED);
            return;
        }
        assertThat(outcome.failure).isNull();
        assertThat(outcome.history).endsWith(DONE, CLOSED);
        assertThat(outcome.rows)
                .containsExactlyInAnyOrderElementsOf(expectedRows(where, outcome.resolved));
    }

    /** A part folded when it starts fails if it cannot derive its masks. */
    @Test
    void testFailureWhileFoldingFailsThePart() {
        answer(Answer.REBUILDING, 0);
        context.keyReadFailure = new IllegalStateException("lake file gone");

        Outcome outcome = read(bucket.bucketPart());

        assertThat(outcome.history).containsExactly(PLANNED, FOLDING, FAILED, CLOSED);
        assertThat(outcome.failure).hasMessageContaining("lake file gone");
        assertThat(bucket.releases()).isEmpty();
    }

    /** A part that cannot plan the snapshot its leader covers fails, also when interrupted. */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testFailureWhileReplanningFailsThePart(boolean interrupted) {
        answer(Answer.COVERED, 0);
        if (interrupted) {
            context.interruptSleeps = true;
        } else {
            context.planFailure = new IllegalStateException("planning failed");
        }

        Outcome outcome = read(bucket.bucketPart());

        assertThat(outcome.history).containsExactly(PLANNED, REPLANNING, FAILED, CLOSED);
        assertThat(outcome.failure).isNotNull();
    }

    /**
     * A restored part checks the masks it kept: of its cut, and of the commit its snapshot names.
     */
    @Test
    void testResumingChecksTheKeptMasks() throws Exception {
        Function<org.apache.fluss.rpc.messages.GetDvSnapshotRequest, GetDvSnapshotResponse> base =
                server.answer;
        server.answer = request -> base.apply(request).setLakeSnapshotIdentity("commit-a");
        lake.identities.put(SNAPSHOT, "commit-a");
        DvBucketPart resolved = read(bucket.bucketPart()).resolved;
        DvBucketPart started = checkpoint(resolved, 1, -1L);

        // its masks
        assertThat(read(started).history).startsWith(RESUMING, READY).endsWith(DONE, CLOSED);
        // masks of another cut
        DvBucketMaskState otherCut =
                new DvBucketMaskState(100L, 106L, null, Collections.emptyMap(), null);
        Outcome another = read(started.withMaskState(otherCut));
        assertThat(another.history).containsExactly(RESUMING, FAILED, CLOSED);
        assertThat(another.failure).hasMessageContaining("of another cut");
        // a snapshot id that names another commit now
        lake.identities.put(SNAPSHOT, "commit-b");
        Outcome rolledBack = read(started);
        assertThat(rolledBack.history).containsExactly(RESUMING, FAILED, CLOSED);
        assertThat(rolledBack.failure).isInstanceOf(LakeTableRolledBackException.class);
    }

    /** The reading states, and how a dropped partition ends the log of a part. */
    @Test
    void testReadingStates() {
        assertThat(read(bucket.bucketPart()).history)
                .containsExactly(PLANNED, PINNED, READY, READING_LAKE, READING_LOG, DONE, CLOSED);

        server.clearRequests();
        answer(Answer.PARTITION_DROPPED, 0);
        assertThat(read(bucket.bucketPart()).history)
                .containsExactly(PLANNED, READY, READING_LAKE, DONE, CLOSED);
    }

    /**
     * A part whose partition is dropped before its log is read ends without rows if it reads its
     * whole bucket and returned none, and fails if it is the log part of a split bucket.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testPartitionDroppedWhileReadingTheLog(boolean whole) {
        TableBucket partitioned = new TableBucket(TABLE_ID, 7L, 0);
        bucket.log.bucket = partitioned;
        bucket.log.subscribeFails = true;
        context.partitionDropped = true;
        server.answer = request -> masks(SNAPSHOT, 100, 106);
        DvBucketPart part =
                whole
                        ? DvBucketPart.ofBucket(partitioned, "p", SNAPSHOT, -1L, 106L, null)
                        : DvBucketPart.pinned(
                                partitioned,
                                "p",
                                SNAPSHOT,
                                100L,
                                106L,
                                null,
                                DvBucketPart.BUCKET_LOG,
                                106L,
                                100L);

        Outcome outcome = read(part);

        assertThat(outcome.rows).isEmpty();
        assertThat(outcome.history).containsSubsequence(READING_LOG, whole ? DONE : FAILED);
    }

    /**
     * A part that reads its whole bucket, restored while it read its log, fails when the partition
     * was dropped since its checkpoint, although it returned nothing since: the rows it emitted
     * before were masked by versions in that log.
     */
    @Test
    void testPartitionDroppedSinceARestoredPartReadItsLogFails() throws Exception {
        Checkpoint checkpoint = checkpointIn(READING_LOG);
        assertThat(checkpoint.part.sharesBucket()).isFalse();
        assertThat(checkpoint.rows).isNotEmpty();
        bucket.log.subscribeFails = true;

        Outcome outcome = read(checkpoint.part);

        assertThat(outcome.rows).isEmpty();
        assertThat(outcome.history).containsSubsequence(RESUMING, READING_LOG, FAILED);
        assertThat(outcome.failure)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("was dropped while a union read was reading the log");
    }

    /**
     * A part whose own cut is lost before its masks are fetched pins again {@link
     * DvPartMachine#MAX_REPINS} times. The next lost cut has it read the bucket by key up to it.
     */
    @ParameterizedTest
    @ValueSource(ints = {DvPartMachine.MAX_REPINS, DvPartMachine.MAX_REPINS + 1})
    void testOwnCutIsPinnedAgainUpToTheLimit(int lostCuts) {
        AtomicInteger ownPins = new AtomicInteger();
        server.answer =
                request -> {
                    GetDvSnapshotResponse response = pins.answer(request);
                    if (!request.hasLogEndOffset() && ownPins.incrementAndGet() <= lostCuts) {
                        // the leader loses the cut right after it pinned it
                        pins.cuts.clear();
                    }
                    return response;
                };

        Outcome outcome = read(bucket.bucketPart());

        assertThat(outcome.failure).isNull();
        assertThat(outcome.rows).containsExactlyInAnyOrderElementsOf(AT_107);
        assertThat(ownPins.get()).isEqualTo(DvPartMachine.MAX_REPINS + 1);
        if (lostCuts == DvPartMachine.MAX_REPINS) {
            // the last pin is kept: read with its masks
            assertThat(outcome.resolved.getReadMode()).isEqualTo(ReadMode.MASKED);
            assertThat(outcome.history).doesNotContain(FOLDING);
        } else {
            assertThat(outcome.resolved.getReadMode()).isEqualTo(ReadMode.FOLDED);
            assertThat(outcome.history).containsSubsequence(PINNED, FOLDING, READY);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Close and restore in every state

    /**
     * A part closed in any state releases the pin of its own it holds, and asks its leader nothing
     * more. It holds one exactly while it is PINNED, or reads with the masks of that pin.
     */
    @ParameterizedTest
    @EnumSource(DvPartState.class)
    void testCloseInEveryState(DvPartState state) throws Exception {
        DvBucketPartReader reader = readerIn(state);
        assertThat(reader.state()).isEqualTo(state);
        boolean holdsOwnPin =
                state == PINNED || state == READY || state == READING_LAKE || state == READING_LOG;
        assertThat(takenPins.get() - bucket.releases().size()).isEqualTo(holdsOwnPin ? 1 : 0);
        int requests = server.requests().size();

        reader.close();

        assertThat(reader.state()).isEqualTo(CLOSED);
        assertThat(bucket.releases()).hasSize(takenPins.get());
        assertThat(server.requests()).hasSize(requests + (holdsOwnPin ? 1 : 0));
        assertThatThrownBy(() -> reader.pollBatch(Duration.ofMillis(10)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    /**
     * An engine restores a part from the split state it checkpointed in any state, and reads it on
     * exactly: the rows it emitted before and those it emits after are the bucket's rows once.
     */
    @ParameterizedTest
    @EnumSource(DvPartState.class)
    void testRestoreInEveryState(DvPartState state) throws Exception {
        Checkpoint checkpoint = checkpointIn(state);
        if (checkpoint == null) {
            // a part read to the end is not restored
            assertThat(state).isEqualTo(DONE);
            return;
        }

        DvBucketPartReader restored = new DvBucketPartReader(checkpoint.part, context);
        assertThat(restored.state()).isEqualTo(restoredState(state));
        Outcome outcome = read(restored);

        assertThat(outcome.failure).isNull();
        List<String> rows = new ArrayList<>(checkpoint.rows);
        rows.addAll(outcome.rows);
        assertThat(rows).containsExactlyInAnyOrderElementsOf(AT_107);
        assertThat(bucket.releases()).hasSize(takenPins.get());
    }

    /** The state a part restored from its checkpoint in {@code state} starts in. */
    private static DvPartState restoredState(DvPartState state) {
        switch (state) {
            case PINNED_AT_PLANNING:
            case FOLDING:
            case RESUMING:
                return state;
            case READING_LAKE:
            case READING_LOG:
            case CLOSED:
                // started, with the masks it kept
                return RESUMING;
            default:
                // nothing emitted: the part as assigned, or as resolved without its masks
                return PLANNED;
        }
    }

    /** A part starts in the state its stored form names, as the machine's javadoc has it. */
    @Test
    void testInitialStateOfAStoredPart() throws Exception {
        List<LakeSplit> files = lake.plans.get(SNAPSHOT);
        DvBucketPart masked = read(bucket.bucketPart()).resolved;
        answer(Answer.REBUILDING, 0);
        DvBucketPart folded = read(bucket.bucketPart()).resolved;
        answer(Answer.PARTITION_DROPPED, 0);
        DvBucketPart lakeOnly = read(bucket.bucketPart()).resolved;
        assertThat(folded.getReadMode()).isEqualTo(ReadMode.FOLDED);
        assertThat(lakeOnly.getReadMode()).isEqualTo(ReadMode.LAKE_ONLY);

        assertThat(initialState(bucket.bucketPart())).isEqualTo(PLANNED);
        assertThat(initialState(bucket.bucketPart().withLakeSplitsPlannedAtStart()))
                .isEqualTo(REPLANNING);
        assertThat(initialState(bucket.pinnedPart(files, DvBucketPart.WHOLE_BUCKET, CUT)))
                .isEqualTo(PINNED_AT_PLANNING);
        assertThat(initialState(masked.withMaskState(null))).isEqualTo(PLANNED);
        assertThat(initialState(checkpoint(masked, 1, -1L))).isEqualTo(RESUMING);
        assertThat(initialState(checkpoint(masked, 1, -1L).withMaskState(null))).isEqualTo(FAILED);
        assertThat(initialState(folded.withMaskState(null))).isEqualTo(FOLDING);
        assertThat(initialState(checkpoint(folded, 2, 103L))).isEqualTo(RESUMING);
        assertThat(initialState(lakeOnly)).isEqualTo(PLANNED);
        assertThat(initialState(lakeOnly.atPosition(1, 0, -1L))).isEqualTo(READY);
    }

    private DvPartState initialState(DvBucketPart part) throws Exception {
        try (DvBucketPartReader reader = new DvBucketPartReader(part, context)) {
            return reader.state();
        }
    }

    // ------------------------------------------------------------------------------------------

    /** A reader of the standard bucket brought to {@code state}. */
    private DvBucketPartReader readerIn(DvPartState state) throws Exception {
        List<LakeSplit> files = lake.plans.get(SNAPSHOT);
        DvBucketPartReader reader;
        switch (state) {
            case PLANNED:
                return new DvBucketPartReader(bucket.bucketPart(), context);
            case REPLANNING:
                answer(Answer.COVERED, 0);
                reader = new DvBucketPartReader(bucket.bucketPart(), context);
                reader.step();
                return reader;
            case PINNED:
                reader = new DvBucketPartReader(bucket.bucketPart(), context);
                reader.step();
                return reader;
            case PINNED_AT_PLANNING:
                return new DvBucketPartReader(
                        bucket.pinnedPart(files, DvBucketPart.WHOLE_BUCKET, CUT), context);
            case FOLDING:
                answer(Answer.REBUILDING, 0);
                reader = new DvBucketPartReader(bucket.bucketPart(), context);
                reader.step();
                return reader;
            case RESUMING:
                DvBucketPart resolved = read(bucket.bucketPart()).resolved;
                server.clearRequests();
                takenPins.set(0);
                return new DvBucketPartReader(checkpoint(resolved, 1, -1L), context);
            case FAILED:
                answer(Answer.ROLLED_BACK, 0);
                reader = new DvBucketPartReader(bucket.bucketPart(), context);
                assertThatThrownBy(reader::resolve)
                        .satisfies(
                                e ->
                                        assertThat(
                                                        ExceptionUtils.findThrowable(
                                                                e,
                                                                LakeTableRolledBackException.class))
                                                .isPresent());
                return reader;
            case CLOSED:
                reader = new DvBucketPartReader(bucket.bucketPart(), context);
                reader.resolve();
                reader.close();
                return reader;
            default:
                reader = new DvBucketPartReader(bucket.bucketPart(), context);
                reader.resolve();
                while (reader.state() != state) {
                    CloseableIterator<InternalRow> batch = reader.pollBatch(Duration.ofMillis(10));
                    if (batch != null) {
                        drain(batch, new ArrayList<>());
                    }
                }
                return reader;
        }
    }

    /** What an engine checkpointed of a part, and the rows it emitted before. */
    private static final class Checkpoint {
        final DvBucketPart part;
        final List<String> rows;

        Checkpoint(DvBucketPart part, List<String> rows) {
            this.part = part;
            this.rows = rows;
        }
    }

    /**
     * What an engine checkpoints of a part of the standard bucket in {@code state}: the part as
     * assigned until it resolved, then as resolved, then with its position and the masks it still
     * needs. Null once the part was read to the end.
     */
    @Nullable
    private Checkpoint checkpointIn(DvPartState state) throws Exception {
        List<LakeSplit> files = lake.plans.get(SNAPSHOT);
        switch (state) {
            case PLANNED:
            case REPLANNING:
            case PINNED:
            case FAILED:
                // nothing resolved yet: the part as assigned
                readerIn(state).close();
                return new Checkpoint(bucket.bucketPart(), Collections.emptyList());
            case PINNED_AT_PLANNING:
                return new Checkpoint(
                        bucket.pinnedPart(files, DvBucketPart.WHOLE_BUCKET, CUT),
                        Collections.emptyList());
            case FOLDING:
                // folded at planning
                return new Checkpoint(
                        DvBucketPart.pinnedFolded(
                                BUCKET,
                                null,
                                SNAPSHOT,
                                100L,
                                CUT,
                                files,
                                DvBucketPart.WHOLE_BUCKET,
                                CUT),
                        Collections.emptyList());
            case READY:
                DvBucketPartReader ready = readerIn(READY);
                DvBucketPart resolved = ready.resolve();
                ready.close();
                return new Checkpoint(checkpoint(resolved, 0, -1L), Collections.emptyList());
            case DONE:
                return null;
            default:
                // RESUMING, READING_LAKE, CLOSED: after the first lake file; READING_LOG: after
                // the first log row
                return readUntil(state == READING_LOG);
        }
    }

    /**
     * Reads the standard bucket's part as an engine does up to the end of its first lake file, or
     * its first log row, and returns the checkpoint the engine takes there. RESUMING restores it.
     */
    private Checkpoint readUntil(boolean firstLogRow) throws Exception {
        List<String> rows = new ArrayList<>();
        try (DvBucketPartReader reader = new DvBucketPartReader(bucket.bucketPart(), context)) {
            DvBucketPart resolved = reader.resolve();
            while (true) {
                CloseableIterator<InternalRow> batch = reader.pollBatch(Duration.ofMillis(10));
                assertThat(batch).isNotNull();
                if (batch instanceof IndexedRowIterator && !(batch instanceof LogRowIterator)) {
                    int index = ((IndexedRowIterator) batch).getCurrentLakeSplitIndex();
                    drain(batch, rows);
                    if (!firstLogRow) {
                        return new Checkpoint(checkpoint(resolved, index + 1, -1L), rows);
                    }
                    continue;
                }
                LogRowIterator logRows = (LogRowIterator) batch;
                if (logRows.hasNext()) {
                    rows.add(rowString(logRows.next()));
                    long next = logRows.currentLogOffset() + 1;
                    batch.close();
                    return new Checkpoint(
                            checkpoint(resolved, resolved.lakeSplitCount(), next), rows);
                }
                batch.close();
            }
        }
    }

    /** The part an engine checkpoints at a position, with the masks it still needs. */
    private static DvBucketPart checkpoint(
            DvBucketPart resolved, int lakeSplitIndex, long nextLogOffset) {
        DvBucketPart split = resolved.atPosition(lakeSplitIndex, 0, nextLogOffset);
        DvBucketMaskState masks = split.getMaskState();
        if (masks == null) {
            return split;
        }
        if (!split.isStarted()) {
            // nothing emitted yet: a restore fetches masks again
            return split.withMaskState(null);
        }
        int fromLakeSplit = nextLogOffset >= 0 ? split.lakeSplitCount() : lakeSplitIndex;
        return split.withMaskState(
                masks.withoutLakeSplitsBefore(fromLakeSplit, split.getLakeSplits()));
    }

    private DvBucketPart partOf(Where where) {
        List<LakeSplit> files = lake.plans.get(SNAPSHOT);
        switch (where) {
            case PLANNED:
            case PINNED:
                return bucket.bucketPart();
            case PINNED_AT_PLANNING:
                return bucket.pinnedPart(files, DvBucketPart.WHOLE_BUCKET, CUT);
            case PINNED_AT_PLANNING_SHARED:
                return bucket.pinnedPart(files.subList(0, 1), 0, CUT);
            default:
                return bucket.pinnedPart(files, DvBucketPart.WHOLE_BUCKET, CUT)
                        .atPosition(1, 0, -1L);
        }
    }

    /** The rows a part read to its end returns. */
    private static List<String> expectedRows(Where where, DvBucketPart resolved) {
        if (resolved.getReadMode() == ReadMode.LAKE_ONLY) {
            return LAKE_ROWS;
        }
        List<String> rows = resolved.getMaskLogEndOffset() == CUT ? AT_107 : EXACT;
        switch (where) {
            case PINNED_AT_PLANNING_SHARED:
                // the live rows of the first lake file
                return Collections.singletonList("2=a");
            case PINNED_AT_PLANNING_STARTED:
                // all but those of the first lake file, read before
                return rows.stream().filter(r -> !r.equals("2=a")).collect(Collectors.toList());
            default:
                return rows;
        }
    }

    /**
     * Gives {@code answer} to the leader's {@code at}th request about masks, and to every retry of
     * it the mask fetch makes; the other requests are answered by the pinning leader.
     */
    private void answer(Answer answer, int at) {
        int attempts = attemptsOf(answer);
        AtomicInteger seen = new AtomicInteger();
        server.answer =
                request -> {
                    int n = seen.getAndIncrement();
                    if (answer == Answer.MASKS || n < at || n >= at + attempts) {
                        GetDvSnapshotResponse response = pins.answer(request);
                        if (!request.hasLogEndOffset()) {
                            takenPins.incrementAndGet();
                        }
                        return response;
                    }
                    switch (answer) {
                        case REBUILDING:
                            return new GetDvSnapshotResponse().setFoldCut(CUT);
                        case COVERED:
                            return covered(NEWER, false, 100);
                        case NOT_READY:
                            throw new CompletionException(
                                    new DeletionVectorsNotReadyException("recovering"));
                        case LEADER_LOST:
                            throw new CompletionException(
                                    new NotLeaderOrFollowerException("no leader"));
                        case PARTITION_DROPPED:
                            throw new CompletionException(
                                    new PartitionNotExistException("dropped"));
                        case ROLLED_BACK:
                            throw new CompletionException(
                                    new LakeTableRolledBackException("rolled back"));
                        case INTERRUPTED:
                            // the fetch waits, and the engine cancels the read meanwhile
                            context.interruptSleeps = true;
                            throw new CompletionException(
                                    new DeletionVectorsNotReadyException("switching"));
                        default:
                            throw new CompletionException(new InvalidRequestException("bad"));
                    }
                };
    }

    /** The requests a mask fetch sends before it gives up on {@code answer}. */
    private static int attemptsOf(Answer answer) {
        if (answer == Answer.LEADER_LOST) {
            return DvMaskFetcher.MAX_LEADER_RETRIES + 1;
        }
        if (answer != Answer.NOT_READY) {
            return 1;
        }
        int attempts = 1;
        long waitedMs = 0;
        long backoffMs = DvMaskFetcher.INITIAL_BACKOFF_MS;
        while (waitedMs < DvMaskFetcher.MAX_NOT_READY_WAIT_MS) {
            waitedMs += backoffMs;
            backoffMs = Math.min(backoffMs * 2, DvMaskFetcher.MAX_NOT_READY_BACKOFF_MS);
            attempts++;
        }
        return attempts;
    }

    /** What a reader read of a part to its end, how it resolved it, and its states. */
    private static final class Outcome {
        final List<String> rows = new ArrayList<>();
        @Nullable DvBucketPart resolved;
        @Nullable Exception failure;
        List<DvPartState> history;
        int releasesBeforeClose;
    }

    private Outcome read(DvBucketPart part) {
        return read(new DvBucketPartReader(part, context));
    }

    private Outcome read(DvBucketPartReader reader) {
        Outcome outcome = new Outcome();
        try {
            outcome.resolved = reader.resolve();
            CloseableIterator<InternalRow> batch;
            while ((batch = reader.pollBatch(Duration.ofMillis(10))) != null) {
                drain(batch, outcome.rows);
            }
        } catch (Exception e) {
            outcome.failure = e;
        } finally {
            outcome.releasesBeforeClose = bucket.releases().size();
            // a reader interrupted while it fetched masks interrupts its thread again
            Thread.interrupted();
            try {
                reader.close();
            } catch (Exception e) {
                outcome.failure = outcome.failure == null ? e : outcome.failure;
            }
        }
        outcome.history = new ArrayList<>(reader.history());
        return outcome;
    }

    private static void drain(CloseableIterator<InternalRow> batch, List<String> rows) {
        while (batch.hasNext()) {
            rows.add(rowString(batch.next()));
        }
        batch.close();
    }
}
