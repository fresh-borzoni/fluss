/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.flink;

import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.flink.source.FlussSource;
import org.apache.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for union reads of tables with deletion vectors ({@code table.deletion-vectors.enabled =
 * true}) on Iceberg v2 (file-scoped position deletes) and v3 (deletion vectors). Rows updated or
 * deleted after they were tiered must not surface, through the union read or in the lake itself.
 * The tiering of these tables is tested in {@link FlinkDerivedDvTieringITCase}.
 */
class FlinkUnionReadDerivedDvITCase extends FlinkDerivedDvTestBase {
    @ParameterizedTest
    @CsvSource({"2, false", "2, true", "3, false", "3, true"})
    void testUpdatesAndDeletesOfTieredRows(int formatVersion, boolean partitioned)
            throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName = "derived_v" + formatVersion + (partitioned ? "_part" : "");
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            createDerivedTable(tablePath, formatVersion, partitioned);
            List<String> parts =
                    partitioned ? Arrays.asList("p0", "p1") : Collections.singletonList(null);

            List<InternalRow> rows = new ArrayList<>();
            for (String p : parts) {
                for (int i = 0; i < 10; i++) {
                    rows.add(rowOf(i, "v_" + i, p));
                }
            }
            writeRows(tablePath, rows, false);
            long readable = tickUntilReadableAdvances(tablePath, Long.MIN_VALUE, parts.get(0));

            try (Table table = conn.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                for (String p : parts) {
                    for (int i = 0; i < 3; i++) {
                        writer.upsert(rowOf(i, "updated_" + i, p));
                    }
                    writer.delete(rowOf(3, "v_3", p));
                    writer.delete(rowOf(4, "v_4", p));
                    writer.delete(rowOf(5, "v_5", p));
                    writer.upsert(rowOf(5, "reinserted_5", p));
                }
                writer.flush();
            }

            List<String> expected = new ArrayList<>();
            for (String p : parts) {
                for (int i : new int[] {0, 1, 2, 5, 6, 7, 8, 9}) {
                    String value = i < 3 ? "updated_" + i : (i == 5 ? "reinserted_5" : "v_" + i);
                    expected.add(rowString(i, value, p));
                }
            }
            expected.add(rowString(-1, "tick", parts.get(0)));

            // Before the second round is tiered, the masks hide the old versions.
            assertUnionRead(tableName, expected);

            // The second round commits its deletes with its data, so after it the union read
            // and the lake alone are both exact.
            tickUntilReadableAdvances(tablePath, readable, parts.get(0));
            assertUnionRead(tableName, expected);
            String columns = partitioned ? "c1, c2, c3" : "c1, c2";
            CloseableIterator<Row> lakeIter =
                    batchTEnv
                            .executeSql("select " + columns + " from " + tableName + "$lake")
                            .collect();
            assertResultsIgnoreOrder(lakeIter, expected, true);
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * Tiering and union reads of a table in WAL changelog image mode stay exact, also with several
     * changes of one key in a round. Its log has no UPDATE_BEFORE records and turns upserts of new
     * keys into UPDATE_AFTER, but the deletion vectors need only the keys a round changes and their
     * last versions.
     */
    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testWalChangelogImage(int formatVersion) throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName = "derived_wal_v" + formatVersion;
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId =
                    createDerivedTable(
                            tablePath,
                            formatVersion,
                            false,
                            Collections.singletonMap(
                                    ConfigOptions.TABLE_CHANGELOG_IMAGE.key(), "WAL"));
            assertThat(admin.getTableInfo(tablePath).get().getTableConfig().getChangelogImage())
                    .isEqualTo(ChangelogImage.WAL);
            Map<Integer, String> expected = new TreeMap<>();
            List<InternalRow> rows = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                rows.add(rowOf(i, "v_" + i, null));
                expected.put(i, "v_" + i);
            }
            writeRows(tablePath, rows, false);
            waitUntilAllTiered(tablePath, tableId);
            assertExact(tableName, expected, true);

            for (int round = 0; round < 2; round++) {
                try (Table table = conn.getTable(tablePath)) {
                    UpsertWriter writer = table.newUpsert().createWriter();
                    // several updates of one key, and of a key new in this round
                    for (int n = 0; n < 3; n++) {
                        writer.upsert(rowOf(0, "updated_" + round + "_" + n, null));
                        writer.upsert(rowOf(20 + round, "new_" + round + "_" + n, null));
                    }
                    expected.put(0, "updated_" + round + "_2");
                    expected.put(20 + round, "new_" + round + "_2");
                    writer.upsert(rowOf(1 + round, "updated_" + round, null));
                    expected.put(1 + round, "updated_" + round);
                    writer.delete(rowOf(3 + round, "x", null));
                    expected.remove(3 + round);
                    writer.delete(rowOf(5 + round, "x", null));
                    writer.upsert(rowOf(5 + round, "reinserted_" + round, null));
                    expected.put(5 + round, "reinserted_" + round);
                    writer.upsert(rowOf(30 + round, "gone", null));
                    writer.delete(rowOf(30 + round, "gone", null));
                    if (round == 1) {
                        writer.delete(rowOf(20, "x", null));
                        expected.remove(20);
                    }
                    writer.flush();
                }
                // before the round is tiered, the masks hide the old versions
                assertExact(tableName, expected, false);
                waitUntilAllTiered(tablePath, tableId);
                assertExact(tableName, expected, true);
            }
        } finally {
            jobClient.cancel().get();
        }
    }

    private void assertExact(String tableName, Map<Integer, String> expected, boolean lakeToo)
            throws Exception {
        List<String> rowStrings = new ArrayList<>();
        expected.forEach((key, value) -> rowStrings.add(rowString(key, value, null)));
        assertUnionRead(tableName, rowStrings);
        if (lakeToo) {
            assertLakeRead(tableName, "c1, c2", rowStrings);
        }
    }

    /**
     * An idle table whose lake files another engine rewrote (e.g. Spark's rewrite_data_files) is
     * re-indexed without new writes, and its readable snapshot advances to the rewrite. Union reads
     * of the rewrite take the masks of the leaders' index of the rewritten files.
     */
    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testIdleTableRewrittenByAnotherEngine(int formatVersion) throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName = "derived_idle_rewrite_v" + formatVersion;
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId = createDerivedTable(tablePath, formatVersion, false);

            List<InternalRow> rows = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                rows.add(rowOf(i, "v_" + i, null));
            }
            writeRows(tablePath, rows, false);
            long readable = tickUntilReadableAdvances(tablePath, Long.MIN_VALUE, null);
            // a second round leaves deletes in the lake, so the rewrite drops masked rows
            try (Table table = conn.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                writer.upsert(rowOf(0, "updated_0", null));
                writer.delete(rowOf(1, "v_1", null));
                writer.flush();
            }
            tickUntilReadableAdvances(tablePath, readable, null);
            long beforeRewrite = waitUntilAllTiered(tablePath, tableId);

            // the writers are idle from here on; another engine compacts the lake table
            long rewriteSnapshotId = rewriteAllDataFiles(tablePath);
            assertThat(rewriteSnapshotId).isNotEqualTo(beforeRewrite);
            waitUntil(
                    () ->
                            admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId()
                                    == rewriteSnapshotId,
                    Duration.ofMinutes(2),
                    Duration.ofMillis(200),
                    "readable snapshot of idle " + tablePath + " never moved to the rewrite");

            List<String> expected = new ArrayList<>();
            expected.add(rowString(-1, "tick", null));
            expected.add(rowString(0, "updated_0", null));
            for (int i = 2; i < 10; i++) {
                expected.add(rowString(i, "v_" + i, null));
            }
            assertUnionRead(tableName, expected);
            assertLakeRead(tableName, "c1, c2", expected);

            // The next round resolves superseded rows against the rewritten files' index.
            try (Table table = conn.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                writer.upsert(rowOf(2, "updated_2", null));
                writer.delete(rowOf(3, "v_3", null));
                writer.flush();
            }
            expected.remove(rowString(2, "v_2", null));
            expected.remove(rowString(3, "v_3", null));
            expected.add(rowString(2, "updated_2", null));
            tickUntilReadableAdvances(tablePath, rewriteSnapshotId, null);
            assertUnionRead(tableName, expected);
            assertLakeRead(tableName, "c1, c2", expected);
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * After a tiered partition is dropped in Fluss, union reads stay exact and read its lake rows
     * from the lake alone, without asking a tablet server for masks. The next round stops carrying
     * its offsets.
     */
    @Test
    void testUnionReadAfterPartitionDrop() throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName = "derived_drop_partition";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId = createDerivedTable(tablePath, 3, true);
            Map<String, String> expected = new TreeMap<>();
            List<InternalRow> rows = new ArrayList<>();
            for (String p : Arrays.asList("a", "b")) {
                for (int i = 0; i < 10; i++) {
                    rows.add(rowOf(i, "v_" + i, p));
                    expected.put(i + "/" + p, rowString(i, "v_" + i, p));
                }
            }
            writeRows(tablePath, rows, false);
            tickUntilReadableAdvances(tablePath, Long.MIN_VALUE, "b");
            expected.put("-1/b", rowString(-1, "tick", "b"));
            long readable = waitUntilAllTiered(tablePath, tableId, true);
            long droppedId = partitionId(tablePath, "a");
            assertThat(admin.getReadableLakeSnapshot(tablePath).get().getTableBucketsOffset())
                    .containsKey(new TableBucket(tableId, droppedId, 0));

            admin.dropPartition(
                            tablePath,
                            new PartitionSpec(Collections.singletonMap("c3", "a")),
                            false)
                    .get();
            waitUntil(
                    () -> admin.listPartitionInfos(tablePath).get().size() == 1,
                    Duration.ofMinutes(1),
                    Duration.ofMillis(200),
                    "partition a of " + tablePath + " not dropped");
            try (Table table = conn.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                writer.upsert(rowOf(0, "updated_0", "b"));
                writer.delete(rowOf(1, "v_1", "b"));
                writer.flush();
            }
            expected.put("0/b", rowString(0, "updated_0", "b"));
            expected.remove("1/b");

            // The readable snapshot still has the dropped partition's offsets: its lake rows are
            // read without masks, the other partition with them.
            assertUnionRead(tableName, new ArrayList<>(expected.values()));
            assertCount(tableName, expected.size());

            // The next round does not carry the dropped partition, in Fluss or in the lake.
            long next = tickUntilReadableAdvances(tablePath, readable, "b");
            assertThat(next).isNotEqualTo(readable);
            long latest = waitUntilAllTiered(tablePath, tableId, true);
            LakeSnapshot snapshot = admin.getReadableLakeSnapshot(tablePath).get();
            assertThat(snapshot.getSnapshotId()).isEqualTo(latest);
            assertThat(snapshot.getTableBucketsOffset().keySet())
                    .isNotEmpty()
                    .noneMatch(tb -> Long.valueOf(droppedId).equals(tb.getPartitionId()));
            org.apache.iceberg.Table lakeTable = icebergCatalog.loadTable(toIceberg(tablePath));
            // "partitionId|bucket=offset,..."
            String offsets = lakeTable.snapshot(latest).summary().get("fluss-dv-offsets");
            assertThat(offsets).isNotEmpty();
            assertThat(Arrays.asList(offsets.split(",")))
                    .noneMatch(entry -> entry.startsWith(droppedId + "|"));
            assertUnionRead(tableName, new ArrayList<>(expected.values()));
            assertCount(tableName, expected.size());
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * A reader that fails mid-read fetches the masks again and reads its buckets again, so the read
     * is exact even when a key changed between the attempts. The second attempt reads as of the
     * pinned cut its splits were planned with if the leaders pin cuts, or as of its own start
     * otherwise.
     */
    @Test
    void testReaderFailoverRefetchesMasks() throws Exception {
        String tableName = "derived_reader_failover";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        long tableId = createDerivedTable(tablePath, 3, false);
        Map<Integer, String> expected = new TreeMap<>();
        List<InternalRow> rows = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            rows.add(rowOf(i, "v_" + i, null));
            expected.put(i, "v_" + i);
        }
        writeRows(tablePath, rows, false);
        JobClient tiering = buildTieringJob(execEnv);
        try {
            waitUntilAllTiered(tablePath, tableId);
        } finally {
            tiering.cancel().get();
        }
        // a log tail, so the masks hide the tiered versions of its keys
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (int i = 0; i < 10; i++) {
                writer.upsert(rowOf(i, "u_" + i, null));
                expected.put(i, "u_" + i);
            }
            for (int i = 10; i < 15; i++) {
                writer.delete(rowOf(i, "x", null));
                expected.remove(i);
            }
            writer.flush();
        }
        Map<Integer, String> beforeTheChange = new TreeMap<>(expected);
        expected.put(20, "between_attempts");

        FailingRead.reset(
                () -> {
                    try (Table table = conn.getTable(tablePath)) {
                        UpsertWriter writer = table.newUpsert().createWriter();
                        writer.upsert(rowOf(20, "between_attempts", null));
                        writer.flush();
                    }
                });
        Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 2);
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
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
                .map(new FailingRead(5))
                .returns(Types.STRING)
                .addSink(new FailingRead.AttemptSink());
        env.execute("union read with a reader failover");

        assertThat(FailingRead.ATTEMPTS.keySet()).contains(0, 1);
        int lastAttempt = Collections.max(FailingRead.ATTEMPTS.keySet());
        List<String> read = FailingRead.ATTEMPTS.get(lastAttempt);
        // each key once, as of one cut
        assertThat(read).doesNotHaveDuplicates();
        assertThat(new java.util.TreeSet<>(read))
                .isIn(
                        new java.util.TreeSet<>(rowsOf(beforeTheChange)),
                        new java.util.TreeSet<>(rowsOf(expected)));
    }

    private static List<String> rowsOf(Map<Integer, String> values) {
        List<String> rows = new ArrayList<>();
        values.forEach((key, value) -> rows.add(key + "=" + value));
        return rows;
    }

    /**
     * A streaming bounded union read stopped with a savepoint inside a bucket resumes with the
     * masks its state kept, though new writes moved the high watermark: the bucket reads as of its
     * first cut, each key once.
     */
    @Test
    void testSavepointInsideABucketRestoresWithKeptMasks(@TempDir java.io.File savepointDir)
            throws Exception {
        String tableName = "derived_savepoint";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        // one bucket, so the savepoint falls inside the bucket being read
        long tableId =
                createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("c1", DataTypes.INT())
                                                .column("c2", DataTypes.STRING())
                                                .primaryKey("c1")
                                                .build())
                                .distributedBy(1)
                                .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                                .property(
                                        ConfigOptions.TABLE_DATALAKE_FRESHNESS,
                                        Duration.ofMillis(500))
                                .property(
                                        ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key(), "true")
                                .customProperty("iceberg.format-version", "3")
                                .build());
        // rows large enough that a slow sink backpressures the reader inside the bucket
        String padding = String.join("", Collections.nCopies(10, "0123456789"));
        int keys = 20_000;
        Map<Integer, String> expected = new TreeMap<>();
        List<InternalRow> rows = new ArrayList<>();
        for (int i = 0; i < keys; i++) {
            rows.add(row(i, "v_" + i + "_" + padding));
            expected.put(i, "v_" + i + "_" + padding);
        }
        writeRows(tablePath, rows, false);
        TableBucket bucket = new TableBucket(tableId, 0);
        JobClient tiering = buildTieringJob(execEnv);
        try {
            long written = getLeaderReplica(bucket).getLogHighWatermark();
            waitUntil(
                    () -> {
                        try {
                            return admin.getReadableLakeSnapshot(tablePath)
                                            .get()
                                            .getTableBucketsOffset()
                                            .getOrDefault(bucket, 0L)
                                    >= written;
                        } catch (Exception e) {
                            return false;
                        }
                    },
                    Duration.ofMinutes(2),
                    Duration.ofMillis(200),
                    "rows of " + tablePath + " not readable");
        } finally {
            tiering.cancel().get();
        }
        // a log tail, so the masks hide the tiered versions of its keys
        List<InternalRow> updates = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            updates.add(row(i, "u_" + i));
            expected.put(i, "u_" + i);
        }
        writeRows(tablePath, updates, false);

        SlowSink.ROWS.clear();
        JobClient first = startStreamingRead(tableName, null);
        waitUntil(
                () -> SlowSink.ROWS.size() >= 2_000,
                Duration.ofMinutes(2),
                Duration.ofMillis(100),
                "the read emitted too few rows");
        String savepoint =
                first.stopWithSavepoint(
                                false,
                                savepointDir.getAbsolutePath(),
                                SavepointFormatType.CANONICAL)
                        .get(2, TimeUnit.MINUTES);
        assertThat(SlowSink.ROWS.size()).isLessThan(keys);

        // new writes move the high watermark past the cut the bucket is read with
        List<InternalRow> later = new ArrayList<>();
        for (int i = 100; i < 110; i++) {
            later.add(row(i, "later_" + i));
        }
        writeRows(tablePath, later, false);

        startStreamingRead(tableName, savepoint).getJobExecutionResult().get(3, TimeUnit.MINUTES);

        Map<Integer, String> read = new TreeMap<>();
        for (String change : SlowSink.ROWS) {
            String[] parts = change.split(",", 3);
            // each key once, as an insert
            assertThat(parts[0]).isEqualTo("+I");
            assertThat(read.put(Integer.valueOf(parts[1]), parts[2])).isNull();
        }
        assertThat(read).isEqualTo(expected);
    }

    /** Starts a streaming bounded union read of the table into {@link SlowSink}. */
    private JobClient startStreamingRead(String tableName, @Nullable String savepoint)
            throws Exception {
        Configuration conf = new Configuration();
        if (savepoint != null) {
            conf.setString("execution.savepoint.path", savepoint);
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // a bounded union read in a streaming job, the one that takes checkpoints
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        FlussSource<RowData> source =
                FlussSource.<RowData>builder()
                        .setBootstrapServers(
                                String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS)))
                        .setDatabase(DEFAULT_DB)
                        .setTable(tableName)
                        .setStartingOffsets(OffsetsInitializer.full())
                        .setBatch()
                        .setDeserializationSchema(new RowDataDeserializationSchema())
                        .build();
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "union read")
                .uid("union-read")
                .addSink(new SlowSink())
                .uid("slow-sink");
        return env.executeAsync("streaming union read of " + tableName);
    }

    /** Collects the changes of a read, slowly, so the reader stays inside a bucket. */
    private static class SlowSink implements SinkFunction<RowData> {
        private static final long serialVersionUID = 1L;

        static final List<String> ROWS = new java.util.concurrent.CopyOnWriteArrayList<>();

        @Override
        public void invoke(RowData value, Context context) throws Exception {
            Thread.sleep(1);
            ROWS.add(
                    value.getRowKind().shortString()
                            + ","
                            + value.getInt(0)
                            + ","
                            + value.getString(1));
        }
    }

    /** Fails the first attempt of a read after some rows, changing the table before it fails. */
    private static class FailingRead extends RichMapFunction<Row, String> {
        private static final long serialVersionUID = 1L;

        static final Map<Integer, List<String>> ATTEMPTS = new ConcurrentHashMap<>();
        private static volatile ThrowingRunnable beforeFailure;

        private final int failAfter;
        private int rows;

        FailingRead(int failAfter) {
            this.failAfter = failAfter;
        }

        static void reset(ThrowingRunnable beforeFailure) {
            ATTEMPTS.clear();
            FailingRead.beforeFailure = beforeFailure;
        }

        @Override
        public String map(Row row) throws Exception {
            if (getRuntimeContext().getTaskInfo().getAttemptNumber() == 0 && ++rows > failAfter) {
                beforeFailure.run();
                throw new RuntimeException("Failing the first attempt of the read");
            }
            return row.getField(0) + "=" + row.getField(1);
        }

        /** Collects the rows of each attempt. */
        static class AttemptSink extends RichSinkFunction<String> {
            private static final long serialVersionUID = 1L;

            @Override
            public void invoke(String value, Context context) {
                ATTEMPTS.computeIfAbsent(
                                getRuntimeContext().getTaskInfo().getAttemptNumber(),
                                k -> Collections.synchronizedList(new ArrayList<>()))
                        .add(value);
            }
        }
    }

    /** A runnable that may throw. */
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private void assertUnionRead(String tableName, List<String> expected) throws Exception {
        CloseableIterator<Row> rowIter =
                batchTEnv.executeSql("select * from " + tableName).collect();
        assertResultsIgnoreOrder(rowIter, expected, true);
    }
}
