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

package org.apache.fluss.flink.tiering.committer;

import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.tiering.FlussTableLakeSnapshotCommitter;
import org.apache.fluss.client.tiering.TableBucketWriteResult;
import org.apache.fluss.client.tiering.TieringCommitter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.FlussConfigUtils;
import org.apache.fluss.exception.FencedTieringEpochException;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.flink.tiering.TestingWriteResult;
import org.apache.fluss.flink.tiering.source.TieringTestBase;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.CommitterInitContext;
import org.apache.fluss.lake.committer.LakeBaseChangedException;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.committer.PartitionMarkDoneCommitter;
import org.apache.fluss.lake.committer.SupportsPositionDeleteCommit;
import org.apache.fluss.lake.committer.SupportsPrimaryKeyMergeCommit;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.DataDeltaPlan;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.SupportsRewritePlanning;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.SupportsDeletionVectorTiering;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatRequest;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatResponse;
import org.apache.fluss.rpc.messages.PbLakeTieringTableInfo;
import org.apache.fluss.utils.ExceptionUtils;

import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that rounds of a table the coordinator took from their tiering service stop instead of
 * committing: before their lake commit, before running again on a lake table the new owner moved
 * on, and at their registration in Fluss, which the coordinator rejects. A service loses a table
 * this way e.g. when it is cut off from the coordinator for longer than the tiering service
 * timeout.
 */
class TieringEpochFencingTest extends TieringTestBase {

    private Configuration flussConfig;
    private FlussTableLakeSnapshotCommitter snapshotCommitter;
    private FakeLakeCommitter lakeCommitter;
    private FakeLakeTieringFactory lakeTieringFactory;

    @BeforeEach
    protected void beforeEach() throws Exception {
        super.beforeEach();
        flussConfig = new Configuration(clientConf);
        // the derived committer's position index lives under the remote data dir
        flussConfig.set(ConfigOptions.REMOTE_DATA_DIR, FLUSS_CLUSTER_EXTENSION.getRemoteDataDir());
        snapshotCommitter = new FlussTableLakeSnapshotCommitter(flussConfig);
        snapshotCommitter.open();
        lakeCommitter = new FakeLakeCommitter();
        lakeTieringFactory = new FakeLakeTieringFactory(lakeCommitter);
    }

    @AfterEach
    void afterEach() throws Exception {
        snapshotCommitter.close();
    }

    @Test
    void testRoundOfTakenTableStopsBeforeItsLakeCommit() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "taken_before_lake_commit");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        TieringCommitter<TestingWriteResult, TestingCommittable> committer = plainCommitter();
        long epoch = takeTable(tableId);
        snapshotCommitter.fenceOnTieringEpoch(tableId, epoch);
        committer.commitWriteResults(tableId, tablePath, roundResults(tableId, tablePath, 10));
        assertThat(registeredSnapshot(tablePath).getSnapshotId()).isEqualTo(1);

        // the table is handed to another tiering service: this one's next round stops before
        // its lake commit
        failTable(tableId, epoch);
        assertThat(takeTable(tableId)).isEqualTo(epoch + 1);
        assertThatThrownBy(
                        () ->
                                committer.commitWriteResults(
                                        tableId, tablePath, roundResults(tableId, tablePath, 20)))
                .satisfies(TieringEpochFencingTest::assertFenced);
        assertThat(lakeCommitter.commits).hasValue(1);
        LakeSnapshot registered = registeredSnapshot(tablePath);
        assertThat(registered.getSnapshotId()).isEqualTo(1);
        assertThat(registered.getTableBucketsOffset())
                .containsEntry(new TableBucket(tableId, 0), 10L);
    }

    /**
     * The tiering committer of a service that does not derive deletion vectors refuses a table with
     * them before its lake commit, which would add the round's rows without deleting their old
     * versions.
     */
    @Test
    void testPlainCommitterRefusesTableWithDeletionVectors() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "plain_committer_dv_table");
        long tableId =
                createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(DEFAULT_PK_TABLE_SCHEMA)
                                .distributedBy(DEFAULT_BUCKET_NUM, "id")
                                .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                                .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED, true)
                                .build());

        assertThatThrownBy(
                        () ->
                                plainCommitter()
                                        .commitWriteResults(
                                                tableId,
                                                tablePath,
                                                roundResults(tableId, tablePath, 10)))
                .hasMessageContaining(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key());
        assertThat(lakeCommitter.commits).hasValue(0);
        assertThatThrownBy(() -> admin.getLatestLakeSnapshot(tablePath).get())
                .hasCauseInstanceOf(LakeTableSnapshotNotExistException.class);
    }

    @Test
    void testFencedRegistrationStopsTheRound() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "taken_during_lake_commit");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        TieringCommitter<TestingWriteResult, TestingCommittable> committer = plainCommitter();
        long epoch = takeTable(tableId);
        snapshotCommitter.fenceOnTieringEpoch(tableId, epoch);

        // the table is handed to another tiering service while the lake commit runs: the lake
        // snapshot stays unregistered, for the new owner's next round to register
        lakeCommitter.onCommit = () -> failTable(tableId, epoch);
        assertThatThrownBy(
                        () ->
                                committer.commitWriteResults(
                                        tableId, tablePath, roundResults(tableId, tablePath, 10)))
                .satisfies(TieringEpochFencingTest::assertFenced);
        assertThat(lakeCommitter.commits).hasValue(1);
        assertThat(lakeCommitter.aborts).hasValue(0);
        assertThatThrownBy(() -> admin.getLatestLakeSnapshot(tablePath).get())
                .hasCauseInstanceOf(LakeTableSnapshotNotExistException.class);
    }

    @Test
    void testBaseChangeOfTakenTableStopsTheRound() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "taken_on_base_change");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        long epoch = takeTable(tableId);
        snapshotCommitter.fenceOnTieringEpoch(tableId, epoch);

        // the new owner commits to the lake on the round's base: the round does not run again
        // on top of it
        lakeCommitter.onCommitWithDeletes =
                () -> {
                    failTable(tableId, epoch);
                    throw new LakeBaseChangedException(
                            "moved past base", new IOException("new owner committed"));
                };
        assertThatThrownBy(
                        () ->
                                derivedCommitter()
                                        .commitRound(
                                                tableId,
                                                tablePath,
                                                admin.getTableInfo(tablePath).get(),
                                                roundResults(tableId, tablePath, 10)))
                .satisfies(TieringEpochFencingTest::assertFenced);
        assertThat(lakeCommitter.commitsWithDeletes).hasValue(1);
        // not planned again
        assertThat(lakeCommitter.attempts).hasValue(1);
        // the lake rejected the commit, so the round's files are deleted
        assertThat(lakeCommitter.aborts).hasValue(1);
        assertThatThrownBy(() -> admin.getLatestLakeSnapshot(tablePath).get())
                .hasCauseInstanceOf(LakeTableSnapshotNotExistException.class);
    }

    @Test
    void testBaseChangeOfOwnedTableRunsTheRoundAgain() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "owned_on_base_change");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        snapshotCommitter.fenceOnTieringEpoch(tableId, takeTable(tableId));

        // another engine commits to the lake on the round's base, once
        lakeCommitter.onCommitWithDeletes =
                () -> {
                    lakeCommitter.onCommitWithDeletes = null;
                    throw new LakeBaseChangedException(
                            "moved past base", new IOException("another engine committed"));
                };
        RoundOutcome<TestingCommittable> outcome =
                derivedCommitter()
                        .commitRound(
                                tableId,
                                tablePath,
                                admin.getTableInfo(tablePath).get(),
                                roundResults(tableId, tablePath, 10));
        assertThat(outcome.reindex()).isNull();
        assertThat(lakeCommitter.attempts).hasValue(2);
        assertThat(lakeCommitter.commitsWithDeletes).hasValue(2);
        LakeSnapshot registered = registeredSnapshot(tablePath);
        assertThat(registered.getSnapshotId()).isEqualTo(lakeCommitter.lastSnapshotId);
        assertThat(registered.getTableBucketsOffset())
                .containsEntry(new TableBucket(tableId, 0), 10L);
    }

    /**
     * A compaction of a service cut off from the coordinator reaches the index chain but not Fluss.
     * Another compaction on top of it must not stop the table: no change from the registered
     * snapshot applies to the chain's end, so every round of the table would fail.
     */
    @Test
    void testRewriteLeftOutOfFlussByCutOffServiceIsTakenOver() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "rewrite_left_out_of_fluss");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        snapshotCommitter.fenceOnTieringEpoch(tableId, takeTable(tableId));
        DerivedDvTieringCommitter<TestingWriteResult, TestingCommittable> committer =
                derivedCommitter();
        committer.commitRound(
                tableId,
                tablePath,
                admin.getTableInfo(tablePath).get(),
                roundResults(tableId, tablePath, 10));
        long base = registeredSnapshot(tablePath).getSnapshotId();

        // the cut-off service's compaction: rewrite base + 1, in the chain, never in Fluss
        PositionIndexStore store =
                PositionIndexStore.forTable(
                        FlussConfigUtils.getDefaultRemoteDataDir(flussConfig),
                        tablePath,
                        tableId,
                        null);
        store.commitChange(77L, Collections.emptyMap());
        store.register(base, base + 1, 77L);
        // the next service's compaction, rewrite base + 2 on top of it
        lakeTieringFactory.lakeSource = new FakeLakeSource(base + 2);
        lakeCommitter.snapshots.set((int) base + 2);

        RoundOutcome<TestingCommittable> outcome =
                committer.commitRound(
                        tableId,
                        tablePath,
                        admin.getTableInfo(tablePath).get(),
                        roundResults(tableId, tablePath, 20));

        assertThat(outcome.reindex()).isNull();
        LakeSnapshot registered = registeredSnapshot(tablePath);
        assertThat(registered.getSnapshotId()).isEqualTo(base + 3);
        assertThat(registered.getTableBucketsOffset())
                .containsEntry(new TableBucket(tableId, 0), 20L);
        assertThat(store.chain())
                .extracting(e -> e.snapshotId)
                .containsExactly(base, base + 1, base + 2, base + 3);
    }

    private TieringCommitter<TestingWriteResult, TestingCommittable> plainCommitter() {
        return new TieringCommitter<>(
                admin, flussConfig, new Configuration(), lakeTieringFactory, snapshotCommitter);
    }

    private DerivedDvTieringCommitter<TestingWriteResult, TestingCommittable> derivedCommitter() {
        return new DerivedDvTieringCommitter<>(
                admin,
                flussConfig,
                new Configuration(),
                lakeTieringFactory,
                snapshotCommitter,
                UnregisteredMetricsGroup.createOperatorMetricGroup(),
                null);
    }

    private static List<TableBucketWriteResult<TestingWriteResult>> roundResults(
            long tableId, TablePath tablePath, long logEndOffset) {
        List<TableBucketWriteResult<TestingWriteResult>> results = new ArrayList<>();
        results.add(
                new TableBucketWriteResult<>(
                        tablePath,
                        new TableBucket(tableId, 0),
                        null,
                        new TestingWriteResult(1),
                        logEndOffset,
                        logEndOffset,
                        1));
        return results;
    }

    static void assertFenced(Throwable error) {
        assertThat(ExceptionUtils.findThrowable(error, FencedTieringEpochException.class))
                .as("%s", ExceptionUtils.stringifyException(error))
                .isPresent();
    }

    private static LakeSnapshot registeredSnapshot(TablePath tablePath) throws Exception {
        return admin.getLatestLakeSnapshot(tablePath).get();
    }

    /** Acts as the tiering service the coordinator hands the table to; returns its epoch. */
    static long takeTable(long tableId) {
        return waitValue(
                () -> {
                    LakeTieringHeartbeatResponse response =
                            coordinatorGateway
                                    .lakeTieringHeartbeat(
                                            new LakeTieringHeartbeatRequest().setRequestTable(true))
                                    .get();
                    if (!response.hasTieringTable()) {
                        return Optional.empty();
                    }
                    PbLakeTieringTableInfo table = response.getTieringTable();
                    // tables of other tests are left assigned
                    return table.getTableId() == tableId
                            ? Optional.of(table.getTieringEpoch())
                            : Optional.empty();
                },
                Duration.ofMinutes(1),
                "The coordinator did not hand out table " + tableId);
    }

    /** Fails the round of the table, so the coordinator hands it out under the next epoch. */
    static void failTable(long tableId, long tieringEpoch) throws Exception {
        int coordinatorEpoch =
                coordinatorGateway
                        .lakeTieringHeartbeat(new LakeTieringHeartbeatRequest())
                        .get()
                        .getCoordinatorEpoch();
        LakeTieringHeartbeatRequest request = new LakeTieringHeartbeatRequest();
        request.addFailedTable()
                .setTableId(tableId)
                .setCoordinatorEpoch(coordinatorEpoch)
                .setTieringEpoch(tieringEpoch);
        assertThat(
                        coordinatorGateway
                                .lakeTieringHeartbeat(request)
                                .get()
                                .getFailedTableRespAt(0)
                                .hasError())
                .isFalse();
    }

    /** A lake action run during a commit. */
    @FunctionalInterface
    interface CommitAction {
        void run() throws Exception;
    }

    /** A lake committer counting its commits, which runs the test's actions during them. */
    static final class FakeLakeCommitter
            implements PartitionMarkDoneCommitter<TestingWriteResult, TestingCommittable>,
                    SupportsPositionDeleteCommit<TestingWriteResult, TestingCommittable>,
                    SupportsPrimaryKeyMergeCommit {

        // attempted commits
        final AtomicInteger commits = new AtomicInteger();
        final AtomicInteger commitsWithDeletes = new AtomicInteger();
        final AtomicInteger pendingRewrites = new AtomicInteger();
        final AtomicInteger aborts = new AtomicInteger();
        final AtomicInteger markDones = new AtomicInteger();
        // one per attempt of a derived round, which looks for a lost commit first
        final AtomicInteger attempts = new AtomicInteger();
        final AtomicInteger snapshots = new AtomicInteger();
        @Nullable volatile CommitAction onCommit;
        @Nullable volatile CommitAction onCommitWithDeletes;
        // whether a partition mark-done changes the state
        volatile boolean markDoneChanges;
        // the commit of this service the lake has and Fluss has not registered, if any
        @Nullable volatile CommittedLakeSnapshot missing;
        // the snapshot properties of the last commit without deletes
        @Nullable volatile Map<String, String> lastProperties;
        // the snapshot properties of the last commit with deletes
        @Nullable volatile Map<String, String> lastDeleteProperties;
        // whether the round's compaction commits a rewrite
        volatile boolean rewrites;
        volatile long lastSnapshotId;

        @Override
        public TestingCommittable toCommittable(List<TestingWriteResult> writeResults) {
            List<Integer> results = new ArrayList<>();
            writeResults.forEach(r -> results.add(r.getWriteResult()));
            return new TestingCommittable(results);
        }

        @Override
        public LakeCommitResult commit(TestingCommittable committable, CommitContext context)
                throws IOException {
            commits.incrementAndGet();
            lastProperties = context.snapshotProperties();
            run(onCommit);
            return LakeCommitResult.committedIsReadable(nextSnapshot());
        }

        @Override
        public LakeCommitResult commitWithDeletes(
                TestingCommittable committable,
                Map<String, String> snapshotProperties,
                Map<String, long[]> deletedPositionsByFile,
                long baseSnapshotId)
                throws IOException {
            commitsWithDeletes.incrementAndGet();
            lastDeleteProperties = snapshotProperties;
            run(onCommitWithDeletes);
            return LakeCommitResult.unknownReadableSnapshot(nextSnapshot());
        }

        private long nextSnapshot() {
            lastSnapshotId = snapshots.incrementAndGet();
            return lastSnapshotId;
        }

        private static void run(@Nullable CommitAction action) throws IOException {
            if (action == null) {
                return;
            }
            try {
                action.run();
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        @Nullable
        @Override
        public Long commitPendingRewrite(TestingCommittable committable) {
            pendingRewrites.incrementAndGet();
            return rewrites ? nextSnapshot() : null;
        }

        @Override
        public void expireSnapshots(@Nullable Long keepFromSnapshotId) {}

        @Override
        public void checkCurrentSnapshotDescendsFrom(long snapshotId) {}

        @Override
        public void protectReadableSnapshot(long snapshotId) {}

        @Nullable
        @Override
        public LakeCommitResult getCommitResult(
                long snapshotId, Map<TableBucket, Long> tieredLogEndOffsets) {
            return null;
        }

        @Override
        public boolean readableSnapshotMayAdvanceSince(long snapshotId) {
            return false;
        }

        @Override
        public boolean preparePartitionMarkDone(TestingCommittable committable) {
            markDones.incrementAndGet();
            return markDoneChanges;
        }

        @Override
        public void abort(TestingCommittable committable) {
            aborts.incrementAndGet();
        }

        @Nullable
        @Override
        public CommittedLakeSnapshot getMissingLakeSnapshot(@Nullable Long knownSnapshotId) {
            attempts.incrementAndGet();
            return missing;
        }

        @Override
        public void close() {}
    }

    /**
     * A lake rewritten up to snapshot {@code lastRewrite} after the first round; no rewritten file
     * needs the tiering readers.
     */
    static final class FakeLakeSource
            implements LakeSource<LakeSplit>, SupportsRewritePlanning<LakeSplit> {

        private final long lastRewrite;

        FakeLakeSource(long lastRewrite) {
            this.lastRewrite = lastRewrite;
        }

        @Nullable
        @Override
        public DataDeltaPlan<LakeSplit> planRewrite(
                long baseSnapshotId, @Nullable Long targetSnapshotId) {
            long target = targetSnapshotId == null ? lastRewrite : targetSnapshotId;
            return baseSnapshotId >= target
                    ? null
                    : new DataDeltaPlan<>(target, Collections.emptyList(), Collections.emptyMap());
        }

        @Override
        public void withProject(int[][] project) {}

        @Override
        public void withLimit(int limit) {}

        @Override
        public FilterPushDownResult withFilters(List<Predicate> predicates) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Planner<LakeSplit> createPlanner(PlannerContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RecordReader createRecordReader(ReaderContext<LakeSplit> context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SimpleVersionedSerializer<LakeSplit> getSplitSerializer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LakeSource<LakeSplit> copy() {
            return this;
        }

        @Nullable
        @Override
        public String snapshotIdentity(long snapshotId) {
            return null;
        }
    }

    /** A lake tiering factory whose committer and lake source are the test's. */
    static final class FakeLakeTieringFactory
            implements LakeTieringFactory<TestingWriteResult, TestingCommittable>,
                    SupportsDeletionVectorTiering {

        private final FakeLakeCommitter lakeCommitter;
        @Nullable volatile LakeSource<?> lakeSource;
        // a lake that merges rows by primary key, like Paimon
        volatile boolean merging;

        FakeLakeTieringFactory(FakeLakeCommitter lakeCommitter) {
            this.lakeCommitter = lakeCommitter;
        }

        @Override
        public boolean mergesByPrimaryKey() {
            return merging;
        }

        @Override
        public boolean supportsPartitionMarkDone() {
            return true;
        }

        @Override
        public LakeWriter<TestingWriteResult> createLakeWriter(WriterInitContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SimpleVersionedSerializer<TestingWriteResult> getWriteResultSerializer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LakeCommitter<TestingWriteResult, TestingCommittable> createLakeCommitter(
                CommitterInitContext context) {
            return lakeCommitter;
        }

        @Override
        public SimpleVersionedSerializer<TestingCommittable> getCommittableSerializer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LakeSource<?> createLakeSource(TablePath tablePath) {
            // a lake source that finds no rewrite unless the test sets one
            LakeSource<?> source = lakeSource;
            return source == null ? new FakeLakeSource(0) : source;
        }

        @Override
        public void validateDeletionVectorTable(
                TableInfo tableInfo, @Nullable Long registeredSnapshotId) {}
    }
}
