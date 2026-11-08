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

package org.apache.fluss.lake.iceberg.flink;

import org.apache.fluss.client.tiering.FlussTableLakeSnapshotCommitter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.iceberg.testutils.CommitHookCatalog;
import org.apache.fluss.lake.iceberg.testutils.LogCapture;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatRequest;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatResponse;
import org.apache.fluss.rpc.messages.PbLakeTieringTableInfo;
import org.apache.fluss.server.coordinator.LakeTableTieringManager;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.Snapshot;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.flink.tiering.source.TieringSourceOptions.POLL_TIERING_TABLE_INTERVAL;
import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests two tiering services on one table with deletion vectors. The first is cut off from the
 * coordinator while its round waits at its lake commit, and the coordinator times the table out and
 * hands it to the second.
 *
 * <p>Whichever commits to the lake first, the rows end up exact. The cut-off service never
 * registers its snapshot or commits on top of the new owner's, and the new owner takes over a
 * snapshot the cut-off service committed first.
 */
class FlinkDerivedDvTieringFencingITCase extends FlinkUnionReadTestBase {

    private static final int BUCKETS = 3;
    private static final String DERIVED_INDEX_ID = "fluss-dv-index-id";
    private static final String STALE_ROUND = "is no longer tiered under epoch";
    private static final String SOURCE_COORDINATOR_LOGGER =
            "org.apache.flink.runtime.source.coordinator.SourceCoordinator";
    private static final String SOURCE_COORDINATOR_CLOSED = "Source coordinator for source";
    // Every wait below polls for the progress it needs. A loaded machine takes minutes for a
    // tiering job to start and tier a round, so only a hang should run into this.
    private static final Duration PROGRESS_TIMEOUT = Duration.ofMinutes(5);

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
        clientConf.set(ConfigOptions.REMOTE_DATA_DIR, FLUSS_CLUSTER_EXTENSION.getRemoteDataDir());
    }

    @ParameterizedTest(name = "cut-off service commits first: {0}")
    @ValueSource(booleans = {false, true})
    void testTableTakenFromCutOffTieringService(boolean cutOffCommitsFirst) throws Exception {
        String tableName =
                "derived_fenced_" + (cutOffCommitsFirst ? "cut_off_first" : "owner_first");
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        String lakeTableName = DEFAULT_DB + "." + tableName;
        long tableId = createDerivedTable(tablePath);
        CoordinatorGateway coordinator = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        LakeTableTieringManager tieringManager =
                FLUSS_CLUSTER_EXTENSION
                        .getCoordinatorServer()
                        .getCoordinatorService()
                        .getLakeTableTieringManager();
        Map<Integer, String> expected = new TreeMap<>();
        List<JobClient> jobs = new ArrayList<>();
        CompletableFuture<Void> releaseCutOff = new CompletableFuture<>();
        try (LogCapture snapshotCommitterLog =
                        LogCapture.of(FlussTableLakeSnapshotCommitter.class.getName());
                LogCapture committerLog = LogCapture.of("org.apache.fluss.flink.tiering");
                LogCapture sourceCoordinatorLog = LogCapture.of(SOURCE_COORDINATOR_LOGGER)) {
            // a first round, so later rounds have a base for the base fence
            upsert(tablePath, expected, 0, 20, "v_");
            JobClient first = buildTieringJob(newEnv(), getCommitHookCatalogConf());
            jobs.add(first);
            long base = waitUntilAllTiered(tablePath, tableId);
            cancelAndAwaitItsEnumerator(first, sourceCoordinatorLog);

            // the next round belongs to the service that is cut off at its lake commit
            upsert(tablePath, expected, 0, 10, "a_");
            CompletableFuture<Void> atLakeCommit = new CompletableFuture<>();
            CommitHookCatalog.armOnce(
                    lakeTableName,
                    DERIVED_INDEX_ID,
                    (b, pending, io) -> {
                        atLakeCommit.complete(null);
                        awaitIgnoringInterrupts(releaseCutOff);
                    });
            handToNextService(coordinator, tieringManager, tableId);
            // it asks for a table only when it starts and when a round ends, so it takes the
            // table only if nothing else holds it then
            Configuration rarelyPolling = new Configuration();
            rarelyPolling.set(POLL_TIERING_TABLE_INTERVAL, Duration.ofHours(1));
            JobClient cutOffJob =
                    buildTieringJob(newEnv(), getCommitHookCatalogConf(), rarelyPolling);
            jobs.add(cutOffJob);
            awaitLakeCommit(atLakeCommit, cutOffJob, committerLog);

            // Cut off: the service sends no heartbeat while it waits, so the coordinator times
            // the table out and hands it to the next service.
            handToNextService(coordinator, tieringManager, tableId);
            upsert(tablePath, expected, 5, 15, "b_");

            if (cutOffCommitsFirst) {
                // the cut-off service's commit lands on the base right before the new owner's
                CompletableFuture<Long> cutOffSnapshot = new CompletableFuture<>();
                CommitHookCatalog.armOnce(
                        lakeTableName,
                        DERIVED_INDEX_ID,
                        (b, pending, io) -> {
                            releaseCutOff.complete(null);
                            cutOffSnapshot.complete(
                                    waitValue(
                                            () ->
                                                    Optional.ofNullable(currentSnapshot(tablePath))
                                                            .filter(s -> s.snapshotId() != base)
                                                            .map(Snapshot::snapshotId),
                                            PROGRESS_TIMEOUT,
                                            "the cut-off service's commit did not land"));
                        });
                jobs.add(buildTieringJob(newEnv(), getCommitHookCatalogConf()));
                waitUntilAllTiered(tablePath, tableId);

                // The new owner registered the cut-off service's snapshot instead of it, and
                // runs on top of it.
                long cutOff =
                        cutOffSnapshot.get(PROGRESS_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                assertThat(
                                committerLog.messagesWith(
                                        "repaired unregistered lake snapshot " + cutOff))
                        .hasSize(1);
                assertThat(ancestorsOfCurrentSnapshot(tablePath)).contains(cutOff);
            } else {
                // the new owner's round commits first, then the cut-off service's round goes on
                jobs.add(buildTieringJob(newEnv(), getCommitHookCatalogConf()));
                long owners = waitUntilAllTiered(tablePath, tableId);
                assertThat(owners).isEqualTo(currentSnapshot(tablePath).snapshotId());
                releaseCutOff.complete(null);

                // The lake moved past its base, and the round stops instead of running again on
                // top of the new owner's snapshot.
                waitUntil(
                        () -> !snapshotCommitterLog.messagesWith(STALE_ROUND).isEmpty(),
                        PROGRESS_TIMEOUT,
                        "the cut-off service's round did not stop");
                assertThat(currentSnapshot(tablePath).snapshotId()).isEqualTo(owners);
                assertThat(admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId())
                        .isEqualTo(owners);
            }

            List<String> rows = new ArrayList<>();
            expected.forEach((key, value) -> rows.add(String.format("+I[%d, %s]", key, value)));
            assertLakeRead(tableName, rows);
            assertUnionRead(tableName, rows);
        } finally {
            releaseCutOff.complete(null);
            CommitHookCatalog.disarm(lakeTableName);
            for (JobClient job : jobs) {
                try {
                    job.cancel().get(1, TimeUnit.MINUTES);
                } catch (Exception e) {
                    // cancelled already
                }
            }
            admin.dropTable(tablePath, true).get();
        }
    }

    /**
     * Cancels a tiering job and waits until its enumerator is closed. Cancelling returns once the
     * request is acknowledged, and the enumerator goes on taking tables until the job's source
     * coordinator closes, which may be seconds later. Until then it could take the table handed to
     * the next service.
     */
    private static void cancelAndAwaitItsEnumerator(JobClient job, LogCapture sourceCoordinatorLog)
            throws Exception {
        int closed = sourceCoordinatorLog.messagesWith(SOURCE_COORDINATOR_CLOSED, "closed").size();
        job.cancel().get(1, TimeUnit.MINUTES);
        waitUntil(
                () ->
                        sourceCoordinatorLog
                                        .messagesWith(SOURCE_COORDINATOR_CLOSED, "closed")
                                        .size()
                                > closed,
                PROGRESS_TIMEOUT,
                "the cancelled tiering job's source coordinator did not close");
    }

    /**
     * Takes the table as a tiering service would once the coordinator hands it out, and fails the
     * round at once, so the table is pending again for the next service that asks. A short tiering
     * service timeout takes the table from a service that is cut off or gone without reporting it;
     * no service of the test tiers the table meanwhile.
     */
    private static void handToNextService(
            CoordinatorGateway coordinator, LakeTableTieringManager tieringManager, long tableId) {
        tieringManager.setTieringServiceTimeoutMs(1_000);
        PbLakeTieringTableInfo table;
        try {
            table = takeTable(coordinator, tableId);
        } finally {
            tieringManager.setTieringServiceTimeoutMs(Duration.ofMinutes(2).toMillis());
        }
        failRound(coordinator, tableId, table.getTieringEpoch());
    }

    private static PbLakeTieringTableInfo takeTable(CoordinatorGateway coordinator, long tableId) {
        return waitValue(
                () -> {
                    LakeTieringHeartbeatResponse response =
                            coordinator
                                    .lakeTieringHeartbeat(
                                            new LakeTieringHeartbeatRequest().setRequestTable(true))
                                    .get();
                    return response.hasTieringTable()
                                    && response.getTieringTable().getTableId() == tableId
                            ? Optional.of(response.getTieringTable())
                            : Optional.empty();
                },
                PROGRESS_TIMEOUT,
                "the coordinator did not hand out table " + tableId);
    }

    private static void failRound(CoordinatorGateway coordinator, long tableId, long tieringEpoch) {
        waitValue(
                () -> {
                    int coordinatorEpoch =
                            coordinator
                                    .lakeTieringHeartbeat(new LakeTieringHeartbeatRequest())
                                    .get()
                                    .getCoordinatorEpoch();
                    LakeTieringHeartbeatRequest failed = new LakeTieringHeartbeatRequest();
                    failed.addFailedTable()
                            .setTableId(tableId)
                            .setCoordinatorEpoch(coordinatorEpoch)
                            .setTieringEpoch(tieringEpoch);
                    return coordinator
                                    .lakeTieringHeartbeat(failed)
                                    .get()
                                    .getFailedTableRespAt(0)
                                    .hasError()
                            ? Optional.empty()
                            : Optional.of(true);
                },
                PROGRESS_TIMEOUT,
                "could not fail the round of table " + tableId);
    }

    /**
     * Waits until the cut-off service reaches its lake commit: its job starts, takes the table and
     * writes the round. A job that ends fails the wait at once, a hang after {@link
     * #PROGRESS_TIMEOUT}; both with the tail of the tiering log.
     */
    private static void awaitLakeCommit(
            CompletableFuture<Void> atLakeCommit, JobClient job, LogCapture committerLog) {
        try {
            waitUntil(
                    () ->
                            atLakeCommit.isDone()
                                    || job.getJobStatus().get().isGloballyTerminalState(),
                    PROGRESS_TIMEOUT,
                    Duration.ofMillis(100),
                    "timed out");
        } catch (AssertionError | RuntimeException e) {
            throw new AssertionError(
                    "the cut-off service did not reach its lake commit; tiering log:\n"
                            + lastMessages(committerLog),
                    e);
        }
        if (!atLakeCommit.isDone()) {
            throw new AssertionError(
                    "the cut-off service's job ended before its lake commit; tiering log:\n"
                            + lastMessages(committerLog));
        }
    }

    private static String lastMessages(LogCapture log) {
        List<String> messages = log.messages();
        return String.join(
                "\n", messages.subList(Math.max(0, messages.size() - 40), messages.size()));
    }

    /**
     * Waits for {@code future} the way a commit thread that ignores its interrupts would. The test
     * completes it, also when it fails; the deadline only bounds a hook the test lost.
     */
    private static void awaitIgnoringInterrupts(CompletableFuture<Void> future) {
        long deadline = System.nanoTime() + PROGRESS_TIMEOUT.multipliedBy(3).toNanos();
        while (!future.isDone() && System.nanoTime() < deadline) {
            try {
                future.get(100, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                // interrupted by a cancelled job or timed out: keep waiting
            }
        }
    }

    private StreamExecutionEnvironment newEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(2);
        return env;
    }

    private long createDerivedTable(TablePath tablePath) throws Exception {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("c1", DataTypes.INT())
                                        .column("c2", DataTypes.STRING())
                                        .primaryKey("c1")
                                        .build())
                        .distributedBy(BUCKETS)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key(), "true")
                        .customProperty("iceberg.format-version", "3")
                        .build();
        return createTable(tablePath, descriptor);
    }

    private void upsert(
            TablePath tablePath, Map<Integer, String> expected, int from, int to, String prefix)
            throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        for (int key = from; key < to; key++) {
            rows.add(row(key, prefix + key));
            expected.put(key, prefix + key);
        }
        writeRows(tablePath, rows, false);
    }

    /** Waits until every write is tiered and readable; returns the readable snapshot. */
    private long waitUntilAllTiered(TablePath tablePath, long tableId) {
        return waitValue(
                () -> {
                    long snapshotId;
                    Map<TableBucket, Long> offsets;
                    try {
                        org.apache.fluss.client.metadata.LakeSnapshot snapshot =
                                admin.getReadableLakeSnapshot(tablePath).get();
                        snapshotId = snapshot.getSnapshotId();
                        offsets = snapshot.getTableBucketsOffset();
                    } catch (Exception e) {
                        return Optional.empty(); // nothing readable yet
                    }
                    for (int b = 0; b < BUCKETS; b++) {
                        TableBucket tb = new TableBucket(tableId, b);
                        if (offsets.getOrDefault(tb, 0L)
                                < getLeaderReplica(tb).getLogHighWatermark()) {
                            return Optional.empty();
                        }
                    }
                    return Optional.of(snapshotId);
                },
                PROGRESS_TIMEOUT,
                "writes of " + tablePath + " not all tiered");
    }

    private static Snapshot currentSnapshot(TablePath tablePath) {
        return icebergCatalog.loadTable(toIceberg(tablePath)).currentSnapshot();
    }

    private static List<Long> ancestorsOfCurrentSnapshot(TablePath tablePath) {
        org.apache.iceberg.Table table = icebergCatalog.loadTable(toIceberg(tablePath));
        List<Long> ancestors = new ArrayList<>();
        for (Snapshot s = table.currentSnapshot();
                s != null;
                s = s.parentId() == null ? null : table.snapshot(s.parentId())) {
            ancestors.add(s.snapshotId());
        }
        return ancestors;
    }

    private void assertLakeRead(String tableName, List<String> expected) throws Exception {
        CloseableIterator<Row> rows =
                batchTEnv.executeSql("select c1, c2 from " + tableName + "$lake").collect();
        assertResultsIgnoreOrder(rows, expected, true);
    }

    private void assertUnionRead(String tableName, List<String> expected) throws Exception {
        CloseableIterator<Row> rows = batchTEnv.executeSql("select * from " + tableName).collect();
        assertResultsIgnoreOrder(rows, expected, true);
    }
}
