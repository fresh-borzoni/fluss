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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FencedTieringEpochException;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.flink.tiering.TestingWriteResult;
import org.apache.fluss.flink.tiering.source.TieringTestBase;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.CommitterInitContext;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatRequest;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatResponse;
import org.apache.fluss.rpc.messages.PbLakeTieringTableInfo;
import org.apache.fluss.utils.ExceptionUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that rounds of a table the coordinator took from their tiering service stop instead of
 * committing: before their lake commit, and at their registration in Fluss, which the coordinator
 * rejects. A service loses a table this way e.g. when it is cut off from the coordinator for longer
 * than the tiering service timeout.
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

    private TieringCommitter<TestingWriteResult, TestingCommittable> plainCommitter() {
        return new TieringCommitter<>(
                admin, flussConfig, new Configuration(), lakeTieringFactory, snapshotCommitter);
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

    private static void assertFenced(Throwable error) {
        assertThat(ExceptionUtils.findThrowable(error, FencedTieringEpochException.class))
                .as("%s", ExceptionUtils.stringifyException(error))
                .isPresent();
    }

    private static LakeSnapshot registeredSnapshot(TablePath tablePath) throws Exception {
        return admin.getLatestLakeSnapshot(tablePath).get();
    }

    /** Acts as the tiering service the coordinator hands the table to; returns its epoch. */
    private static long takeTable(long tableId) {
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
    private static void failTable(long tableId, long tieringEpoch) throws Exception {
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
    private interface CommitAction {
        void run() throws Exception;
    }

    /** A lake committer counting its commits, which runs the test's actions during them. */
    private static final class FakeLakeCommitter
            implements LakeCommitter<TestingWriteResult, TestingCommittable> {

        // attempted commits
        private final AtomicInteger commits = new AtomicInteger();
        private final AtomicInteger aborts = new AtomicInteger();
        private final AtomicInteger snapshots = new AtomicInteger();
        @Nullable private volatile CommitAction onCommit;
        private volatile long lastSnapshotId;

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
            run(onCommit);
            return LakeCommitResult.committedIsReadable(nextSnapshot());
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

        @Override
        public void abort(TestingCommittable committable) {
            aborts.incrementAndGet();
        }

        @Nullable
        @Override
        public CommittedLakeSnapshot getMissingLakeSnapshot(@Nullable Long knownSnapshotId) {
            return null;
        }

        @Override
        public void close() {}
    }

    /** A lake tiering factory whose committer is the test's. */
    private static final class FakeLakeTieringFactory
            implements LakeTieringFactory<TestingWriteResult, TestingCommittable> {

        private final FakeLakeCommitter lakeCommitter;

        private FakeLakeTieringFactory(FakeLakeCommitter lakeCommitter) {
            this.lakeCommitter = lakeCommitter;
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
    }
}
