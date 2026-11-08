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

package org.apache.fluss.lake.paimon.flink;

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A bounded union read of a partitioned primary-key table whose partition is dropped from Fluss
 * while the read runs. It ends or fails with a clear error instead of waiting forever for a log
 * that is gone.
 */
class FlinkUnionReadDroppedPartitionITCase extends FlinkUnionReadTestBase {

    // what the read emitted, and the partition the test dropped during it
    private static final List<String> EMITTED = new CopyOnWriteArrayList<>();
    private static volatile String droppedPartition;
    private static volatile TablePath readTable;
    private static volatile long readTableId;

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
        clientConf.set(ConfigOptions.REMOTE_DATA_DIR, FLUSS_CLUSTER_EXTENSION.getRemoteDataDir());
    }

    /**
     * The partition read second is dropped before its bucket is read: its log is gone, so the read
     * returns its lake rows only.
     */
    @ParameterizedTest
    @CsvSource({"false", "true"})
    void testPartitionDroppedBeforeItsBucketIsRead(boolean deletionVectors) throws Exception {
        String tableName = "dropped_before_read_" + deletionVectors;
        prepareTable(tableName, deletionVectors);

        runRead(tableName, false);

        String kept = droppedPartition.equals("a") ? "b" : "a";
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            expected.add(rowString(i, i < 5 ? "u_" + i : "v_" + i, kept));
            // the dropped partition's updates were in its log only
            expected.add(rowString(i, "v_" + i, droppedPartition));
        }
        // tick rows may or may not be tiered in the dropped partition
        assertThat(EMITTED.stream().filter(row -> !row.startsWith("-1,")))
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    /**
     * The partition is dropped after its rows were emitted with masks of its log. The read fails
     * with a clear error, as finishing without that log would lose the masked keys.
     */
    @ParameterizedTest
    @CsvSource({"true"})
    void testPartitionDroppedWhileItsMaskedBucketIsRead(boolean deletionVectors) throws Exception {
        String tableName = "dropped_during_read_" + deletionVectors;
        prepareTable(tableName, deletionVectors);

        assertThatThrownBy(() -> runRead(tableName, true))
                .hasStackTraceContaining("was dropped while a union read was reading the log");
    }

    /** Ten keys tiered in partitions a and b, then the first five updated in the log only. */
    private void prepareTable(String tableName, boolean deletionVectors) throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        long tableId =
                createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("c1", DataTypes.INT())
                                                .column("c2", DataTypes.STRING())
                                                .column("c3", DataTypes.STRING())
                                                .primaryKey("c1", "c3")
                                                .build())
                                .partitionedBy("c3")
                                .distributedBy(1)
                                .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                                .property(
                                        ConfigOptions.TABLE_DATALAKE_FRESHNESS,
                                        Duration.ofMillis(500))
                                .property(
                                        ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key(),
                                        String.valueOf(deletionVectors))
                                // a compaction makes each round readable at its tiered offsets
                                .property(
                                        ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION.key(),
                                        String.valueOf(deletionVectors))
                                .build());
        List<InternalRow> rows = new ArrayList<>();
        for (String p : Arrays.asList("a", "b")) {
            for (int i = 0; i < 10; i++) {
                rows.add(row(i, "v_" + i, p));
            }
        }
        writeRows(tablePath, rows, false);
        JobClient tiering = buildTieringJob(execEnv);
        try {
            waitUntilReadable(tablePath, tableId);
        } finally {
            tiering.cancel().get();
        }
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (String p : Arrays.asList("a", "b")) {
                for (int i = 0; i < 5; i++) {
                    writer.upsert(row(i, "u_" + i, p));
                }
            }
            writer.flush();
        }
        readTable = tablePath;
        readTableId = tableId;
    }

    /**
     * Reads the table with one reader and drops a partition from Fluss while the read runs. It
     * drops the partition read second either once the first row of the other partition is emitted,
     * or, with {@code dropAfterItsFirstRow}, once its own first row is emitted.
     */
    private void runRead(String tableName, boolean dropAfterItsFirstRow) throws Exception {
        EMITTED.clear();
        droppedPartition = null;
        // a failed read is not retried: a retry would read the dropped partition's lake only
        Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, EnvironmentSettings.inBatchMode());
        tEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME,
                        BOOTSTRAP_SERVERS.key(),
                        String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS))));
        tEnv.executeSql("use catalog " + CATALOG_NAME);
        tEnv.executeSql("use " + DEFAULT_DB);
        tEnv.toDataStream(tEnv.from(tableName))
                .map(new DroppingGate(dropAfterItsFirstRow))
                .returns(Types.STRING)
                .addSink(
                        new SinkFunction<String>() {
                            @Override
                            public void invoke(String value, Context context) {
                                EMITTED.add(value);
                            }
                        });
        JobClient job = env.executeAsync("union read with a dropped partition");
        try {
            job.getJobExecutionResult().get(3, TimeUnit.MINUTES);
        } catch (TimeoutException e) {
            job.cancel().get();
            throw new AssertionError("The read waits for the log of the dropped partition", e);
        }
    }

    /** Drops a partition from Fluss when the read gets to it. */
    private static class DroppingGate extends RichMapFunction<Row, String> {
        private static final long serialVersionUID = 1L;

        private final boolean dropAfterItsFirstRow;
        @Nullable private String firstPartition;

        DroppingGate(boolean dropAfterItsFirstRow) {
            this.dropAfterItsFirstRow = dropAfterItsFirstRow;
        }

        @Override
        public String map(Row row) throws Exception {
            String partition = String.valueOf(row.getField(2));
            if (droppedPartition == null) {
                if (firstPartition == null) {
                    firstPartition = partition;
                    if (!dropAfterItsFirstRow) {
                        drop(partition.equals("a") ? "b" : "a");
                    }
                } else if (dropAfterItsFirstRow && !partition.equals(firstPartition)) {
                    drop(partition);
                }
            }
            return rowString((Integer) row.getField(0), String.valueOf(row.getField(1)), partition);
        }

        private static void drop(String partition) throws Exception {
            long partitionId = -1;
            for (PartitionInfo info : admin.listPartitionInfos(readTable).get()) {
                if (info.getPartitionName().equals(partition)) {
                    partitionId = info.getPartitionId();
                }
            }
            admin.dropPartition(
                            readTable,
                            new PartitionSpec(Collections.singletonMap("c3", partition)),
                            false)
                    .get();
            // the partition's log is gone once no tablet server holds its replica
            TableBucket bucket = new TableBucket(readTableId, partitionId, 0);
            waitUntil(
                    () ->
                            FLUSS_CLUSTER_EXTENSION.getTabletServers().stream()
                                    .allMatch(
                                            server ->
                                                    server.getReplicaManager().getReplica(bucket)
                                                            instanceof ReplicaManager.NoneReplica),
                    Duration.ofMinutes(1),
                    "replicas of " + bucket + " not deleted");
            droppedPartition = partition;
        }
    }

    /**
     * Waits until the rows written so far are readable from the lake. A readable snapshot of a
     * table with deletion vectors may come a round later, so tick rows (key -1) start more rounds.
     */
    private void waitUntilReadable(TablePath tablePath, long tableId) throws Exception {
        Map<TableBucket, Long> written = new HashMap<>();
        for (PartitionInfo partition : admin.listPartitionInfos(tablePath).get()) {
            TableBucket tb = new TableBucket(tableId, partition.getPartitionId(), 0);
            written.put(tb, getLeaderReplica(tb).getLogHighWatermark());
        }
        assertThat(written).hasSize(2);
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(3).toMillis();
        while (System.currentTimeMillis() < deadline) {
            try {
                Map<TableBucket, Long> readable =
                        admin.getReadableLakeSnapshot(tablePath).get().getTableBucketsOffset();
                if (written.entrySet().stream()
                        .allMatch(e -> readable.getOrDefault(e.getKey(), 0L) >= e.getValue())) {
                    return;
                }
            } catch (Exception e) {
                // nothing readable yet
            }
            writeRows(tablePath, Arrays.asList(row(-1, "tick", "a"), row(-1, "tick", "b")), false);
            Thread.sleep(1000);
        }
        throw new AssertionError("rows of " + tablePath + " not readable");
    }

    private static String rowString(int key, String value, String partition) {
        return String.format("%d,%s,%s", key, value, partition);
    }
}
