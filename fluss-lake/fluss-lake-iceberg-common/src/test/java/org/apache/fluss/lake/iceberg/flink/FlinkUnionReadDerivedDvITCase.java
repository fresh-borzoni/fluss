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

import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.flink.source.FlussSource;
import org.apache.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import org.apache.fluss.lake.dv.FormatVersions;
import org.apache.fluss.lake.iceberg.source.IcebergSplitPlanner;
import org.apache.fluss.lake.iceberg.testutils.CommitHookCatalog;
import org.apache.fluss.lake.iceberg.testutils.ExternalWrites;
import org.apache.fluss.lake.iceberg.testutils.LogCapture;
import org.apache.fluss.lake.iceberg.tiering.IcebergLakeCommitter;
import org.apache.fluss.lake.iceberg.tiering.writer.TaskWriterFactory;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.kv.dv.DerivedDvManager;
import org.apache.fluss.server.zk.data.lake.LakeTable;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.FlussPaths;

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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.data.IcebergGenericReader;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.util.ContentFileUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for union reads of tables with deletion vectors ({@code table.deletion-vectors.enabled =
 * true}) on Iceberg v2 (file-scoped position deletes) and v3 (deletion vectors). Rows updated or
 * deleted after they were tiered must not surface, through the union read or in the lake itself.
 */
class FlinkUnionReadDerivedDvITCase extends FlinkUnionReadTestBase {

    private static final int BUCKETS = 3;
    // how long the trim tests keep writing rounds until a trim has seen the leaders' checkpoints
    private static final long TRIM_TIMEOUT_MS = 180_000;

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
        // Derived commits write the position index under the remote data dir; the tiering job
        // takes its Fluss config from clientConf, which has no remote dir by default.
        clientConf.set(ConfigOptions.REMOTE_DATA_DIR, FLUSS_CLUSTER_EXTENSION.getRemoteDataDir());
        // One split per rewritten file, so the re-index of a bucket takes several splits.
        clientConf.set(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_REINDEX_FILES_PER_SPLIT, 1);
    }

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
     * re-indexed without new writes, and its readable snapshot advances to the rewrite.
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
     * When another engine compacts the lake table after every round while keys keep changing, the
     * round that sees a rewrite first, idle or with data, re-indexes it and reads stay exact.
     */
    @Test
    void testExternalCompactionBetweenRounds() throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName = "derived_external_compaction";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId = createDerivedTable(tablePath, 3, false);
            Map<Integer, String> expected = new TreeMap<>();
            long rewriteSnapshotId = -1;
            for (int round = 0; round < 4; round++) {
                try (Table table = conn.getTable(tablePath)) {
                    UpsertWriter writer = table.newUpsert().createWriter();
                    for (int key = round * 5; key < round * 5 + 5; key++) {
                        writer.upsert(rowOf(key, "v_" + key, null));
                        expected.put(key, "v_" + key);
                    }
                    if (round > 0) {
                        // change rows that a previous rewrite moved to new files
                        int updated = (round - 1) * 5;
                        writer.upsert(rowOf(updated, "updated_" + round, null));
                        expected.put(updated, "updated_" + round);
                        int deleted = (round - 1) * 5 + 1;
                        writer.delete(rowOf(deleted, expected.remove(deleted), null));
                    }
                    writer.flush();
                }
                waitUntilAllTiered(tablePath, tableId);
                rewriteSnapshotId = rewriteAllDataFiles(tablePath);
            }
            long lastRewrite = rewriteSnapshotId;
            waitUntil(
                    () ->
                            admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId()
                                    == lastRewrite,
                    Duration.ofMinutes(2),
                    Duration.ofMillis(200),
                    "readable snapshot of " + tablePath + " never moved to the last rewrite");

            List<String> rows = new ArrayList<>();
            expected.forEach((key, value) -> rows.add(rowString(key, value, null)));
            assertUnionRead(tableName, rows);
            assertLakeRead(tableName, "c1, c2", rows);
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * A rewrite by another engine, committed while a round commits, of files the round's deletes do
     * not reference makes the round run again and re-index the rewrite first. Later changes of the
     * rewritten rows then supersede them, through the union read and in the lake.
     *
     * <p>Iceberg's validations pass for such a rewrite, and its retry would put the round on top of
     * it. The base fence lets every round's commit land on its base only.
     */
    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testRewriteCommittedDuringRound(int formatVersion) throws Exception {
        JobClient jobClient = buildTieringJob(execEnv, getCommitHookCatalogConf());
        String tableName = "derived_rewrite_in_round_v" + formatVersion;
        String lakeTableName = DEFAULT_DB + "." + tableName;
        try {
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId = createDerivedTable(tablePath, formatVersion, false);
            Map<Integer, String> expected = new TreeMap<>();
            List<InternalRow> rows = new ArrayList<>();
            for (int i = 0; i < 30; i++) {
                rows.add(rowOf(i, "v_" + i, null));
                expected.put(i, "v_" + i);
            }
            writeRows(tablePath, rows, false);
            waitUntilAllTiered(tablePath, tableId);

            // Right before the next round's commit, another engine rewrites every data file the
            // round's deletes do not reference.
            List<Long> rewrites = new ArrayList<>();
            CommitHookCatalog.armOnce(
                    lakeTableName,
                    "fluss-dv-index-id",
                    (base, pending, io) -> {
                        Set<String> referenced = new HashSet<>();
                        for (DeleteFile delete : pending.currentSnapshot().addedDeleteFiles(io)) {
                            referenced.add(ContentFileUtil.referencedDataFileLocation(delete));
                        }
                        rewrites.add(rewriteDataFilesExcept(tablePath, referenced));
                    });
            try (Table table = conn.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                writer.upsert(rowOf(0, "updated_0", null));
                expected.put(0, "updated_0");
                writer.flush();
            }
            waitUntilAllTiered(tablePath, tableId);
            assertThat(CommitHookCatalog.isArmed(lakeTableName)).isFalse();
            assertThat(rewrites).hasSize(1);

            // Change all the other rows, most of them in the rewritten files.
            try (Table table = conn.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                for (int i = 1; i < 30; i++) {
                    if (i % 7 == 0) {
                        writer.delete(rowOf(i, expected.remove(i), null));
                    } else {
                        writer.upsert(rowOf(i, "updated_" + i, null));
                        expected.put(i, "updated_" + i);
                    }
                }
                writer.flush();
            }
            waitUntilAllTiered(tablePath, tableId);

            List<String> rowStrings = new ArrayList<>();
            expected.forEach((key, value) -> rowStrings.add(rowString(key, value, null)));
            assertUnionRead(tableName, rowStrings);
            assertLakeRead(tableName, "c1, c2", rowStrings);

            // Every round's commit has its base as parent, and one of these bases is the rewrite.
            org.apache.iceberg.Table lakeTable = icebergCatalog.loadTable(toIceberg(tablePath));
            Set<Long> parents = new HashSet<>();
            for (Snapshot snapshot : lakeTable.snapshots()) {
                String base = snapshot.summary().get("fluss-dv-base");
                if (snapshot.summary().containsKey("fluss-dv-index-id") && !"-1".equals(base)) {
                    assertThat(snapshot.parentId())
                            .as("parent of round snapshot %s", snapshot.snapshotId())
                            .isEqualTo(Long.parseLong(base));
                    parents.add(snapshot.parentId());
                }
            }
            assertThat(parents).contains(rewrites.get(0));
        } finally {
            CommitHookCatalog.disarm(lakeTableName);
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
     * A round in flight when a partition with its rows is dropped does not commit that partition's
     * rows: the dropped partition keeps the rows of the round before, read from the lake alone, and
     * the other partition reads exact.
     *
     * <p>The round's compaction is registered first, without the dropped partition's buckets, and
     * the round runs again on that base. The old versions of the partition's rows cannot be looked
     * up there, so committing its new rows would leave both versions live in the lake.
     */
    @Test
    void testPartitionDroppedWhileARoundWithItsRowsIsInFlight() throws Exception {
        String tableName = "derived_drop_during_round";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        Map<String, String> properties = new HashMap<>();
        properties.put(ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION.key(), "true");
        properties.put(ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION_INTERVAL.key(), "0s");
        long tableId = createDerivedTable(tablePath, 3, true, properties);
        Map<String, String> expected = new TreeMap<>();
        JobClient jobClient = buildTieringJob(execEnv);
        try (LogCapture committerLog = LogCapture.of("org.apache.fluss.flink.tiering.committer")) {
            // three rounds leave three small files in every bucket, which the next round's
            // writers compact
            for (int round = 0; round < 3; round++) {
                writeRound(tablePath, round, Arrays.asList("keep", "drop"), expected);
                waitUntilAllTiered(tablePath, tableId, true);
            }
            long droppedId = partitionId(tablePath, "drop");
            Map<String, String> beforeTheRound = new TreeMap<>(expected);
            String compacted = "Fluss compaction of " + tablePath;
            int compactionsBefore = committerLog.messagesWith(compacted, "committed").size();
            try (LogCapture readerLog =
                            LogCapture.of(
                                    "org.apache.fluss.flink.tiering.source.TieringSplitReader");
                    LogCapture.Pause finishing =
                            readerLog.pauseAt(
                                    "Finish tier bucket TableBucket{tableId="
                                            + tableId
                                            + ", partitionId="
                                            + droppedId
                                            + ",")) {
                writeRound(tablePath, 3, Arrays.asList("keep", "drop"), expected);
                // the round wrote the partition's rows; drop the partition before it commits
                finishing.awaitReached(Duration.ofMinutes(2));
                admin.dropPartition(
                                tablePath,
                                new PartitionSpec(Collections.singletonMap("c3", "drop")),
                                false)
                        .get();
                waitUntil(
                        () -> admin.listPartitionInfos(tablePath).get().size() == 1,
                        Duration.ofMinutes(1),
                        Duration.ofMillis(200),
                        "partition 'drop' of " + tablePath + " not dropped");
            }
            for (Map.Entry<String, String> e : beforeTheRound.entrySet()) {
                if (e.getKey().endsWith("/drop")) {
                    expected.put(e.getKey(), e.getValue());
                }
            }
            waitUntilAllTiered(tablePath, tableId, true);
            // the round compacted, and registered the compaction before its own commit
            assertThat(committerLog.messagesWith(compacted, "committed"))
                    .hasSizeGreaterThan(compactionsBefore);

            List<String> rows = new ArrayList<>(expected.values());
            // the reads collect only as many rows as expected; the counts catch extra rows
            assertLakeRead(tableName, "c1, c2, c3", rows);
            assertCount(tableName + "$lake", rows.size());
            assertUnionRead(tableName, rows);
            assertCount(tableName, rows.size());
        } finally {
            jobClient.cancel().get();
        }
    }

    /** Upserts keys 0 to 29 of every partition with values of the round. */
    private void writeRound(
            TablePath tablePath, int round, List<String> partitions, Map<String, String> expected)
            throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        for (String p : partitions) {
            for (int i = 0; i < 30; i++) {
                String value = "r" + round + "_" + i;
                rows.add(rowOf(i, value, p));
                expected.put(i + "/" + p, rowString(i, value, p));
            }
        }
        writeRows(tablePath, rows, false);
    }

    /**
     * A rollback of the lake table outside Fluss stops the table's tiering, and undoing the
     * rollback resumes it. Stopped tiering writes no files, repairs nothing and does not register
     * the rolled-back snapshot with newer offsets.
     */
    @Test
    void testRollbackOfLakeTableStopsTiering() throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName = "derived_rollback";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId = createDerivedTable(tablePath, 3, false);
            Map<Integer, String> expected = new TreeMap<>();
            List<InternalRow> rows = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                rows.add(rowOf(i, "v_" + i, null));
                expected.put(i, "v_" + i);
            }
            writeRows(tablePath, rows, false);
            long first = waitUntilAllTiered(tablePath, tableId);
            try (Table table = conn.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                writer.upsert(rowOf(0, "updated_0", null));
                writer.delete(rowOf(1, "v_1", null));
                writer.flush();
            }
            expected.put(0, "updated_0");
            expected.remove(1);
            tickUntilReadableAdvances(tablePath, first, null);
            expected.put(-1, "tick");
            long second = waitUntilAllTiered(tablePath, tableId);

            org.apache.iceberg.Table lakeTable = icebergCatalog.loadTable(toIceberg(tablePath));
            int snapshots = snapshotCount(lakeTable);
            Set<String> offsetsFiles = offsetsFiles(tablePath, tableId);
            Set<String> dataFiles = filesUnder(lakeTable.location() + "/data");
            // As Spark's rollback_to_snapshot does.
            lakeTable.manageSnapshots().rollbackTo(first).commit();

            writeRows(tablePath, Collections.singletonList(rowOf(100, "v_100", null)), false);
            expected.put(100, "v_100");
            // Rounds keep being scheduled every second or so.
            Thread.sleep(10_000);

            assertThat(admin.getLatestLakeSnapshot(tablePath).get().getSnapshotId())
                    .isEqualTo(second);
            assertThat(admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId())
                    .isEqualTo(second);
            lakeTable.refresh();
            assertThat(lakeTable.currentSnapshot().snapshotId()).isEqualTo(first);
            assertThat(snapshotCount(lakeTable)).isEqualTo(snapshots);
            assertThat(offsetsFiles(tablePath, tableId)).isEqualTo(offsetsFiles);
            assertThat(filesUnder(lakeTable.location() + "/data")).isEqualTo(dataFiles);

            // Undone: tiering resumes on the registered snapshot.
            lakeTable.manageSnapshots().setCurrentSnapshot(second).commit();
            long resumed = waitUntilAllTiered(tablePath, tableId);
            assertThat(resumed).isNotEqualTo(second);
            List<String> rowStrings = new ArrayList<>();
            expected.forEach((key, value) -> rowStrings.add(rowString(key, value, null)));
            assertUnionRead(tableName, rowStrings);
            assertLakeRead(tableName, "c1, c2", rowStrings);
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * A write by another engine to the lake table (an INSERT, a merge-on-read DELETE or a
     * copy-on-write UPDATE) stops the table's tiering before a round writes any file. Tiering
     * resumes, exact, once the write is rolled back.
     */
    @ParameterizedTest
    @ValueSource(strings = {"append", "delete", "overwrite"})
    void testWriteByAnotherEngineStopsTiering(String operation) throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName = "derived_external_" + operation;
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId = createDerivedTable(tablePath, 3, false);
            Map<Integer, String> expected = new TreeMap<>();
            List<InternalRow> rows = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                rows.add(rowOf(i, "v_" + i, null));
                expected.put(i, "v_" + i);
            }
            writeRows(tablePath, rows, false);
            long registered = waitUntilAllTiered(tablePath, tableId);

            org.apache.iceberg.Table lakeTable = icebergCatalog.loadTable(toIceberg(tablePath));
            long write = ExternalWrites.write(lakeTable, operation);
            assertThat(lakeTable.snapshot(write).parentId()).isEqualTo(registered);
            int snapshots = snapshotCount(lakeTable);
            Set<String> offsetsFiles = offsetsFiles(tablePath, tableId);
            Set<String> dataFiles = filesUnder(lakeTable.location() + "/data");

            try (Table table = conn.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                writer.upsert(rowOf(0, "updated_0", null));
                writer.upsert(rowOf(100, "v_100", null));
                writer.flush();
            }
            expected.put(0, "updated_0");
            expected.put(100, "v_100");
            // Rounds keep being scheduled every second or so.
            Thread.sleep(10_000);

            assertThat(admin.getLatestLakeSnapshot(tablePath).get().getSnapshotId())
                    .isEqualTo(registered);
            lakeTable.refresh();
            assertThat(lakeTable.currentSnapshot().snapshotId()).isEqualTo(write);
            assertThat(snapshotCount(lakeTable)).isEqualTo(snapshots);
            assertThat(offsetsFiles(tablePath, tableId)).isEqualTo(offsetsFiles);
            assertThat(filesUnder(lakeTable.location() + "/data")).isEqualTo(dataFiles);

            // Undone: tiering resumes on the registered snapshot.
            lakeTable.manageSnapshots().setCurrentSnapshot(registered).commit();
            long resumed = waitUntilAllTiered(tablePath, tableId);
            assertThat(resumed).isNotEqualTo(registered);
            List<String> rowStrings = new ArrayList<>();
            expected.forEach((key, value) -> rowStrings.add(rowString(key, value, null)));
            assertUnionRead(tableName, rowStrings);
            assertLakeRead(tableName, "c1, c2", rowStrings);
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * A round that fails on a conflicting write by another engine, committed while the round
     * commits, deletes what it wrote: data files, deletion vectors, offsets files and its index
     * change.
     */
    @Test
    void testFailedRoundsLeaveNoOrphanFiles() throws Exception {
        JobClient jobClient = buildTieringJob(execEnv, getCommitHookCatalogConf());
        try {
            String tableName = "derived_conflict";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId = createDerivedTable(tablePath, 3, false);
            Map<Integer, String> expected = new TreeMap<>();
            List<InternalRow> rows = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                rows.add(rowOf(i, "v_" + i, null));
                expected.put(i, "v_" + i);
            }
            writeRows(tablePath, rows, false);
            long base = waitUntilAllTiered(tablePath, tableId);

            // Another engine appends to the lake table while the next round commits. The round
            // runs again on the append, finds the write and fails. Later rounds stop before
            // writing.
            org.apache.iceberg.Table lakeTable = icebergCatalog.loadTable(toIceberg(tablePath));
            String lakeTableName = DEFAULT_DB + "." + tableName;
            CommitHookCatalog.armOnce(
                    lakeTableName,
                    "fluss-dv-index-id",
                    (b, pending, io) ->
                            ExternalWrites.append(
                                    icebergCatalog.loadTable(toIceberg(tablePath)),
                                    1000,
                                    "external"));

            try (Table table = conn.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                for (int i = 10; i < 15; i++) {
                    writer.upsert(rowOf(i, "v_" + i, null));
                    expected.put(i, "v_" + i);
                }
                writer.upsert(rowOf(0, "updated_0", null));
                expected.put(0, "updated_0");
                writer.flush();
            }
            waitUntil(
                    () -> !CommitHookCatalog.isArmed(lakeTableName),
                    Duration.ofMinutes(1),
                    Duration.ofMillis(200),
                    "no round of " + tablePath + " committed");
            // Rounds keep being scheduled every second or so.
            Thread.sleep(5_000);
            assertThat(admin.getLatestLakeSnapshot(tablePath).get().getSnapshotId())
                    .isEqualTo(base);

            // Undo the conflicting write, so a round commits and the rest are idle.
            lakeTable.refresh();
            lakeTable.manageSnapshots().rollbackTo(base).commit();
            long last = waitUntilAllTiered(tablePath, tableId);
            assertThat(last).isNotEqualTo(base);

            lakeTable.refresh();
            Set<String> referenced = new HashSet<>();
            Set<String> referencedOffsets = new HashSet<>();
            for (Snapshot snapshot : lakeTable.snapshots()) {
                snapshot.addedDataFiles(lakeTable.io())
                        .forEach(f -> referenced.add(fileName(f.location())));
                snapshot.addedDeleteFiles(lakeTable.io())
                        .forEach(f -> referenced.add(fileName(f.location())));
                String offsets = snapshot.summary().get("fluss-offsets");
                if (offsets != null) {
                    referencedOffsets.add(fileName(offsets));
                }
            }
            for (LakeTable.LakeSnapshotMetadata metadata :
                    FLUSS_CLUSTER_EXTENSION
                            .getZooKeeperClient()
                            .getLakeTable(tableId)
                            .get()
                            .getLakeSnapshotMetadatas()) {
                referencedOffsets.add(metadata.getTieredOffsetsFilePath().getName());
                if (metadata.getReadableOffsetsFilePath() != null) {
                    referencedOffsets.add(metadata.getReadableOffsetsFilePath().getName());
                }
            }
            Set<String> orphanDataFiles = new HashSet<>();
            for (String f : filesUnder(lakeTable.location() + "/data")) {
                if (!referenced.contains(fileName(f))) {
                    orphanDataFiles.add(f);
                }
            }
            assertThat(orphanDataFiles).isEmpty();
            Set<String> orphanOffsetsFiles = new HashSet<>();
            for (String f : offsetsFiles(tablePath, tableId)) {
                if (!referencedOffsets.contains(fileName(f))) {
                    orphanOffsetsFiles.add(f);
                }
            }
            assertThat(orphanOffsetsFiles).isEmpty();
            // Every index change left is registered in the chain.
            String rowPos =
                    FlussPaths.remoteLakeTableSnapshotDir(
                                    FLUSS_CLUSTER_EXTENSION.getRemoteDataDir(), tablePath, tableId)
                            + "/rowPos";
            Set<String> registeredChanges = new HashSet<>();
            for (String marker : filesUnder(rowPos + "/_chain")) {
                registeredChanges.add(fileName(marker).split("_")[3]);
            }
            Set<String> changes = new HashSet<>();
            for (String f : filesUnder(rowPos)) {
                String relative = f.substring(f.indexOf("/rowPos/") + "/rowPos/".length());
                String first = relative.split("/")[0];
                if (first.matches("\\d+")) {
                    changes.add(first);
                }
            }
            assertThat(registeredChanges).containsAll(changes);

            List<String> rowStrings = new ArrayList<>();
            expected.forEach((key, value) -> rowStrings.add(rowString(key, value, null)));
            assertUnionRead(tableName, rowStrings);
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * A rewrite by another engine into several files per bucket is re-indexed in several splits per
     * bucket whose index parts make up one index change, and later changes of the rewritten rows
     * supersede them, through the union read and in the lake.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRewriteIntoManyFilesIsReindexedInSplits(boolean fewRowsPerSplit) throws Exception {
        org.apache.fluss.config.Configuration conf = new org.apache.fluss.config.Configuration();
        if (fewRowsPerSplit) {
            // a split per file, each of more rows than a split sorts in memory: the re-index
            // spills sorted runs and merges them
            conf.set(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_REINDEX_ROWS_PER_SPLIT, 2L);
        }
        JobClient jobClient = buildTieringJob(execEnv, getIcebergCatalogConf(), conf);
        try {
            String tableName = "derived_many_files_rewrite" + (fewRowsPerSplit ? "_rows" : "");
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId = createDerivedTable(tablePath, 3, false);
            Map<Integer, String> expected = new TreeMap<>();
            upsert(tablePath, expected, 0, 30, "v");
            waitUntilAllTiered(tablePath, tableId);
            upsert(tablePath, expected, 0, 5, "updated");
            waitUntilAllTiered(tablePath, tableId);

            long rewrite = rewriteAllDataFilesInto(tablePath, 4);
            waitUntil(
                    () -> admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId() == rewrite,
                    Duration.ofMinutes(2),
                    Duration.ofMillis(200),
                    "readable snapshot of " + tablePath + " never moved to the rewrite");
            List<String> expectedRows = new ArrayList<>();
            expected.forEach((key, value) -> expectedRows.add(rowString(key, value, null)));
            assertUnionRead(tableName, expectedRows);

            // every rewritten file has keys that change now
            upsert(tablePath, expected, 0, 30, "again");
            try (Table table = conn.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                for (int key = 25; key < 30; key++) {
                    writer.delete(rowOf(key, expected.remove(key), null));
                }
                writer.flush();
            }
            waitUntilAllTiered(tablePath, tableId);
            expectedRows.clear();
            expected.forEach((key, value) -> expectedRows.add(rowString(key, value, null)));
            assertUnionRead(tableName, expectedRows);
            assertLakeRead(tableName, "c1, c2", expectedRows);
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * When a registered rewrite by another engine expires, the next rounds re-index the table from
     * its current snapshot instead of failing every time. The rewrite is tagged like the snapshots
     * tiering commits, so expiration keeps it while union reads may plan on it, but it can expire
     * once newer readable snapshots rotated its tag away.
     */
    @Test
    void testExpiredRegisteredRewriteDoesNotStopTiering() throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName = "derived_expired_rewrite";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId = createDerivedTable(tablePath, 3, false);
            Map<Integer, String> expected = new TreeMap<>();
            upsert(tablePath, expected, 0, 10, "v");
            waitUntilAllTiered(tablePath, tableId);
            upsert(tablePath, expected, 0, 4, "updated");
            waitUntilAllTiered(tablePath, tableId);

            long rewrite = rewriteAllDataFiles(tablePath);
            waitUntil(
                    () -> admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId() == rewrite,
                    Duration.ofMinutes(2),
                    Duration.ofMillis(200),
                    "readable snapshot of " + tablePath + " never moved to the rewrite");
            org.apache.iceberg.Table lakeTable = icebergCatalog.loadTable(toIceberg(tablePath));
            String tag = IcebergLakeCommitter.READABLE_TAG_PREFIX + rewrite;
            assertThat(lakeTable.refs()).containsKey(tag);

            // the tag is rotated away, another engine commits on top without changing rows, and
            // the rewrite expires
            lakeTable.manageSnapshots().removeTag(tag).commit();
            lakeTable.newAppend().commit();
            lakeTable.expireSnapshots().expireOlderThan(System.currentTimeMillis() + 1).commit();
            assertThat(icebergCatalog.loadTable(toIceberg(tablePath)).snapshot(rewrite)).isNull();

            upsert(tablePath, expected, 4, 8, "updated");
            waitUntilAllTiered(tablePath, tableId);
            List<String> expectedRows = new ArrayList<>();
            expected.forEach((key, value) -> expectedRows.add(rowString(key, value, null)));
            assertUnionRead(tableName, expectedRows);
            assertLakeRead(tableName, "c1, c2", expectedRows);
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * Tiering goes on after the table's file-id counter moves back below ids already registered, as
     * racing allocations could move it. Later rounds still publish index changes the chain takes,
     * instead of failing every round after its lake commit.
     */
    @Test
    void testFileIdCounterMovedBackDoesNotStopTiering() throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName = "derived_counter_back";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId = createDerivedTable(tablePath, 3, false);
            Map<Integer, String> expected = new TreeMap<>();
            List<InternalRow> rows = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                rows.add(rowOf(i, "v_" + i, null));
                expected.put(i, "v_" + i);
            }
            writeRows(tablePath, rows, false);
            waitUntilAllTiered(tablePath, tableId);
            upsert(tablePath, expected, 0, 5, "updated");
            waitUntilAllTiered(tablePath, tableId);

            String counterPath =
                    FlussPaths.remoteLakeTableSnapshotDir(
                                    FLUSS_CLUSTER_EXTENSION.getRemoteDataDir(), tablePath, tableId)
                            + "/rowPos/_next_file_id";
            java.nio.file.Path counter =
                    Paths.get(
                            new URI(
                                    counterPath.startsWith("/")
                                            ? "file://" + counterPath
                                            : counterPath));
            assertThat(counter).exists();
            Files.write(
                    counter,
                    FormatVersions.withVersion(1, "0")
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8));

            upsert(tablePath, expected, 5, 10, "updated");
            waitUntilAllTiered(tablePath, tableId);
            List<String> expectedRows = new ArrayList<>();
            expected.forEach((key, value) -> expectedRows.add(rowString(key, value, null)));
            assertUnionRead(tableName, expectedRows);
            assertLakeRead(tableName, "c1, c2", expectedRows);
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * When the chain is trimmed, Fluss keeps the table's lake snapshot registrations from the
     * chain's oldest snapshot on and deletes the offsets files of older ones. The table's ZooKeeper
     * node stays as long as the chain instead of growing by an entry every round until ZooKeeper
     * cannot take it.
     */
    @Test
    void testRegisteredLakeSnapshotsFollowTheTrimmedChain() throws Exception {
        org.apache.fluss.config.Configuration trimOften =
                new org.apache.fluss.config.Configuration();
        trimOften.set(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_TRIM_INTERVAL, 5);
        trimOften.set(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_TRIM_RETAIN, 1);
        JobClient jobClient = buildTieringJob(execEnv, getIcebergCatalogConf(), trimOften);
        try {
            String tableName = "derived_bounded_registrations";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId = createDerivedTable(tablePath, 3, false);
            Map<Integer, String> expected = new TreeMap<>();
            // Every round changes keys of every bucket, so the buckets' leaders catch up to every
            // snapshot and checkpoint their index once per checkpoint interval. The checkpoints
            // let tiering trim the chain.
            int rounds = DerivedDvManager.DEFAULT_CHECKPOINT_INTERVAL + 15;
            for (int round = 0; round < rounds; round++) {
                upsert(tablePath, expected, 0, 12, "r" + round);
                waitUntilAllTiered(tablePath, tableId);
            }

            String rowPos =
                    FlussPaths.remoteLakeTableSnapshotDir(
                                    FLUSS_CLUSTER_EXTENSION.getRemoteDataDir(), tablePath, tableId)
                            + "/rowPos";
            // A leader exports its checkpoint up to 10 s after it is due, and the trim runs only
            // after a round. On a fast machine the rounds above can end before a trim sees the
            // checkpoints, so more rounds run until it does.
            long deadline = System.currentTimeMillis() + TRIM_TIMEOUT_MS;
            for (int round = rounds; filesUnder(rowPos + "/_chain").size() >= rounds / 2; round++) {
                assertThat(System.currentTimeMillis()).isLessThan(deadline);
                upsert(tablePath, expected, 0, 12, "r" + round);
                waitUntilAllTiered(tablePath, tableId);
            }
            List<Long> chain = new ArrayList<>();
            filesUnder(rowPos + "/_chain").stream()
                    .map(FlinkUnionReadDerivedDvITCase::fileName)
                    .sorted()
                    .forEach(marker -> chain.add(Long.parseLong(marker.split("_")[2])));
            assertThat(filesUnder(rowPos)).anyMatch(f -> f.endsWith("/_trimmed"));
            assertThat(chain.size()).isLessThan(rounds / 2);

            List<LakeTable.LakeSnapshotMetadata> registered =
                    FLUSS_CLUSTER_EXTENSION
                            .getZooKeeperClient()
                            .getLakeTable(tableId)
                            .get()
                            .getLakeSnapshotMetadatas();
            // from the chain's oldest snapshot on, not one per round
            assertThat(registered.get(0).getSnapshotId()).isEqualTo(chain.get(0));
            assertThat(registered.size()).isLessThanOrEqualTo(chain.size());
            Set<String> referencedOffsets = new HashSet<>();
            for (LakeTable.LakeSnapshotMetadata metadata : registered) {
                referencedOffsets.add(metadata.getTieredOffsetsFilePath().getName());
                if (metadata.getReadableOffsetsFilePath() != null) {
                    referencedOffsets.add(metadata.getReadableOffsetsFilePath().getName());
                }
            }
            Set<String> offsetsFiles = new HashSet<>();
            for (String f : offsetsFiles(tablePath, tableId)) {
                offsetsFiles.add(fileName(f));
            }
            assertThat(offsetsFiles).isEqualTo(referencedOffsets);

            List<String> expectedRows = new ArrayList<>();
            expected.forEach((key, value) -> expectedRows.add(rowString(key, value, null)));
            assertUnionRead(tableName, expectedRows);
            assertLakeRead(tableName, "c1, c2", expectedRows);
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * Buckets of a partition written only in the first rounds do not keep the chain from being
     * trimmed. Their leaders, which no later round notifies, still rebuild them exactly after a
     * restart, from checkpoints the trim built or carried over.
     */
    @Test
    void testIdlePartitionDoesNotHoldTheIndexChain() throws Exception {
        org.apache.fluss.config.Configuration trimOften =
                new org.apache.fluss.config.Configuration();
        trimOften.set(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_TRIM_INTERVAL, 5);
        trimOften.set(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_TRIM_RETAIN, 1);
        JobClient jobClient = buildTieringJob(execEnv, getIcebergCatalogConf(), trimOften);
        try {
            String tableName = "derived_idle_partition";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId = createDerivedTable(tablePath, 3, true);
            Map<String, String> expected = new TreeMap<>();
            for (int round = 0; round < 2; round++) {
                upsertPartition(tablePath, expected, "old", "o" + round);
                waitUntilAllTiered(tablePath, tableId, true);
            }
            int rounds = DerivedDvManager.DEFAULT_CHECKPOINT_INTERVAL + 15;
            for (int round = 0; round < rounds; round++) {
                upsertPartition(tablePath, expected, "new", "n" + round);
                waitUntilAllTiered(tablePath, tableId, true);
            }

            String rowPos =
                    FlussPaths.remoteLakeTableSnapshotDir(
                                    FLUSS_CLUSTER_EXTENSION.getRemoteDataDir(), tablePath, tableId)
                            + "/rowPos";
            // more rounds until a trim has seen the checkpoints, see the test above
            long deadline = System.currentTimeMillis() + TRIM_TIMEOUT_MS;
            for (int round = rounds; filesUnder(rowPos + "/_chain").size() >= rounds / 2; round++) {
                assertThat(System.currentTimeMillis()).isLessThan(deadline);
                upsertPartition(tablePath, expected, "new", "n" + round);
                waitUntilAllTiered(tablePath, tableId, true);
            }
            assertThat(filesUnder(rowPos)).anyMatch(f -> f.endsWith("/_trimmed"));
            assertThat(filesUnder(rowPos + "/_chain").size()).isLessThan(rounds / 2);
            long oldPartition = partitionId(tablePath, "old");
            assertThat(filesUnder(rowPos + "/_ckpt/" + oldPartition))
                    .anyMatch(f -> f.endsWith("/_DONE"));

            // the old partition's leaders rebuild its buckets from the trimmed chain
            Set<Integer> leaders = new HashSet<>();
            for (int b = 0; b < BUCKETS; b++) {
                leaders.add(
                        FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(
                                new TableBucket(tableId, oldPartition, b)));
            }
            for (int leader : leaders) {
                FLUSS_CLUSTER_EXTENSION.restartTabletServer(
                        leader, new org.apache.fluss.config.Configuration());
            }
            FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);
            assertUnionRead(tableName, new ArrayList<>(expected.values()));
            assertLakeRead(tableName, "c1, c2, c3", new ArrayList<>(expected.values()));
        } finally {
            jobClient.cancel().get();
            // The shared client may still know the restarted servers' old endpoints, which would
            // stall the writes of the tests after this one.
            admin.close();
            conn.close();
            conn = ConnectionFactory.createConnection(clientConf);
            admin = conn.getAdmin();
        }
    }

    /** Upserts keys 0..11 of a partition with values {@code prefix_key}. */
    private void upsertPartition(
            TablePath tablePath, Map<String, String> expected, String partition, String prefix)
            throws Exception {
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (int key = 0; key < 12; key++) {
                writer.upsert(rowOf(key, prefix + "_" + key, partition));
                expected.put(key + "/" + partition, rowString(key, prefix + "_" + key, partition));
            }
            writer.flush();
        }
    }

    /**
     * Rounds much larger than the write buffer the tiering task's writers share stay exact, through
     * the union read and in the lake. The writers spill sorted rows to local disk and write only
     * the last version of every key, also of hot keys with many versions in a round. Their staged
     * index parts are gone once the rounds commit.
     */
    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testRoundsLargerThanTheWriteBuffer(int formatVersion) throws Exception {
        String tableName = "derived_spilled_v" + formatVersion;
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        long tableId = createDerivedTable(tablePath, formatVersion, false);
        org.apache.fluss.config.Configuration smallBuffer =
                new org.apache.fluss.config.Configuration();
        smallBuffer.set(
                ConfigOptions.LAKE_TIERING_DELETION_VECTORS_WRITE_BUFFER_SIZE,
                MemorySize.parse("32kb"));
        JobClient jobClient = buildTieringJob(execEnv, getIcebergCatalogConf(), smallBuffer);
        try {
            Map<Integer, String> expected = new TreeMap<>();
            upsert(tablePath, expected, 0, 3_000, "v");
            waitUntilAllTiered(tablePath, tableId);
            assertExact(tableName, expected, true);

            // one round: hot keys changed many times, other keys updated, some deleted
            try (Table table = conn.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                for (int pass = 0; pass < 20; pass++) {
                    for (int key = 0; key < 30; key++) {
                        String value = "hot" + pass + "_" + key;
                        writer.upsert(rowOf(key, value, null));
                        expected.put(key, value);
                    }
                }
                for (int key = 30; key < 2_000; key++) {
                    writer.upsert(rowOf(key, "updated_" + key, null));
                    expected.put(key, "updated_" + key);
                }
                for (int key = 2_000; key < 2_300; key++) {
                    writer.delete(rowOf(key, "v_" + key, null));
                    expected.remove(key);
                }
                for (int key = 3_000; key < 3_200; key++) {
                    writer.upsert(rowOf(key, "new_" + key, null));
                    expected.put(key, "new_" + key);
                }
                writer.delete(rowOf(3_100, "new_3100", null));
                expected.remove(3_100);
                writer.flush();
            }
            waitUntilAllTiered(tablePath, tableId);
            assertExact(tableName, expected, true);
            assertCount(tableName + "$lake", expected.size());

            // every change under rowPos is registered, so no staged part outlived its round
            String rowPos =
                    FlussPaths.remoteLakeTableSnapshotDir(
                                    FLUSS_CLUSTER_EXTENSION.getRemoteDataDir(), tablePath, tableId)
                            + "/rowPos";
            waitUntil(
                    () -> unregisteredChanges(rowPos).isEmpty(),
                    Duration.ofMinutes(1),
                    Duration.ofMillis(200),
                    "staged index parts of " + tablePath + " left behind");
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * A bucket's first round from a KV snapshot larger than the write buffer writes the rows as
     * they come, one per key, and sorts and spills only their keys and positions for the index. The
     * next round deletes the rows it supersedes from those files.
     */
    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testFirstRoundFromAKvSnapshotLargerThanTheWriteBuffer(int formatVersion) throws Exception {
        String tableName = "derived_snapshot_v" + formatVersion;
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        long tableId = createDerivedTable(tablePath, formatVersion, false);
        Map<Integer, String> expected = new TreeMap<>();
        upsert(tablePath, expected, 0, 5_000, "v");
        // the buckets were never tiered and have a KV snapshot: their first round reads it
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);
        org.apache.fluss.config.Configuration smallBuffer =
                new org.apache.fluss.config.Configuration();
        smallBuffer.set(
                ConfigOptions.LAKE_TIERING_DELETION_VECTORS_WRITE_BUFFER_SIZE,
                MemorySize.parse("16kb"));
        JobClient jobClient = buildTieringJob(execEnv, getIcebergCatalogConf(), smallBuffer);
        try {
            waitUntilAllTiered(tablePath, tableId);
            assertExact(tableName, expected, true);

            upsert(tablePath, expected, 2_500, 5_500, "updated");
            try (Table table = conn.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                for (int key = 0; key < 100; key++) {
                    writer.delete(rowOf(key, "v_" + key, null));
                    expected.remove(key);
                }
                writer.flush();
            }
            waitUntilAllTiered(tablePath, tableId);
            assertExact(tableName, expected, true);
            assertCount(tableName + "$lake", expected.size());
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * A backlog larger than a round may hold, as after the tiering service was down, is tiered in
     * several bounded rounds whose snapshots are each exact, through the union read and in the
     * lake. A bucket's round ends once its writer holds the configured number of keys, and the next
     * round starts at once from there.
     */
    @Test
    void testBacklogIsTieredInBoundedRounds() throws Exception {
        String tableName = "derived_bounded_rounds";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        long tableId = createDerivedTable(tablePath, 3, false);
        Map<Integer, String> expected = new TreeMap<>();
        int keys = 60;
        int keysPerBucket = 5;
        // the backlog is there before the tiering service starts
        upsert(tablePath, expected, 0, keys, "v");
        org.apache.fluss.config.Configuration smallRounds =
                new org.apache.fluss.config.Configuration();
        smallRounds.set(
                ConfigOptions.LAKE_TIERING_DELETION_VECTORS_MAX_ROUND_KEYS_PER_BUCKET,
                keysPerBucket);
        JobClient jobClient = buildTieringJob(execEnv, getIcebergCatalogConf(), smallRounds);
        try {
            waitUntilAllTiered(tablePath, tableId);
            org.apache.iceberg.Table lakeTable = icebergCatalog.loadTable(toIceberg(tablePath));
            List<Long> addedPerRound = addedRecordsPerSnapshot(lakeTable);
            assertThat(addedPerRound.stream().mapToLong(Long::longValue).sum()).isEqualTo(keys);
            assertThat(addedPerRound).allMatch(added -> added <= (long) keysPerBucket * BUCKETS);
            assertThat(addedPerRound.size())
                    .isGreaterThanOrEqualTo(keys / (keysPerBucket * BUCKETS));

            // updates of every key, tiered in bounded rounds as well, each deleting the rows
            // it supersedes
            upsert(tablePath, expected, 0, keys, "updated");
            waitUntilAllTiered(tablePath, tableId);
            lakeTable.refresh();
            assertThat(addedRecordsPerSnapshot(lakeTable))
                    .allMatch(added -> added <= (long) keysPerBucket * BUCKETS);
            List<String> expectedRows = new ArrayList<>();
            expected.forEach((key, value) -> expectedRows.add(rowString(key, value, null)));
            assertUnionRead(tableName, expectedRows);
            assertLakeRead(tableName, "c1, c2", expectedRows);
        } finally {
            jobClient.cancel().get();
        }
    }

    /** The change directories under rowPos that no chain marker registers. */
    private static Set<String> unregisteredChanges(String rowPos) throws Exception {
        Set<String> registered = new HashSet<>();
        filesUnder(rowPos + "/_chain").stream()
                .map(FlinkUnionReadDerivedDvITCase::fileName)
                .forEach(marker -> registered.add(marker.split("_")[3]));
        Set<String> changes = new HashSet<>();
        for (String f : filesUnder(rowPos)) {
            String relative = f.substring(f.indexOf("/rowPos/") + "/rowPos/".length());
            String first = relative.split("/")[0];
            if (first.matches("\\d+")) {
                changes.add(first);
            }
        }
        changes.removeAll(registered);
        return changes;
    }

    /** The rows each snapshot of the lake table added, in commit order. */
    private static List<Long> addedRecordsPerSnapshot(org.apache.iceberg.Table lakeTable) {
        List<Long> added = new ArrayList<>();
        for (Snapshot snapshot : lakeTable.snapshots()) {
            String records = snapshot.summary().get("added-records");
            if (records != null) {
                added.add(Long.parseLong(records));
            }
        }
        return added;
    }

    /**
     * With {@code table.datalake.auto-expire-snapshot}, tiering expires old lake snapshots after
     * its commits but keeps the readable snapshots it tagged and every snapshot from the chain's
     * oldest on. Union and lake reads stay exact, and a rewrite by another engine is still
     * re-indexed.
     */
    @Test
    void testAutoExpireSnapshotsKeepsWhatFlussNeeds() throws Exception {
        org.apache.fluss.config.Configuration trimOften =
                new org.apache.fluss.config.Configuration();
        trimOften.set(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_TRIM_INTERVAL, 5);
        trimOften.set(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_TRIM_RETAIN, 1);
        long expireInterval = IcebergLakeCommitter.setExpireIntervalMs(0);
        JobClient jobClient = buildTieringJob(execEnv, getIcebergCatalogConf(), trimOften);
        try {
            String tableName = "derived_auto_expire";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId =
                    createDerivedTable(
                            tablePath,
                            3,
                            false,
                            Collections.singletonMap(
                                    ConfigOptions.TABLE_DATALAKE_AUTO_EXPIRE_SNAPSHOT.key(),
                                    "true"));
            // expire every snapshot not kept otherwise
            icebergCatalog
                    .loadTable(toIceberg(tablePath))
                    .updateProperties()
                    .set("history.expire.max-snapshot-age-ms", "1")
                    .set("history.expire.min-snapshots-to-keep", "1")
                    .commit();
            Map<Integer, String> expected = new TreeMap<>();
            int rounds = DerivedDvManager.DEFAULT_CHECKPOINT_INTERVAL + 15;
            for (int round = 0; round < rounds; round++) {
                upsert(tablePath, expected, 0, 12, "r" + round);
                waitUntilAllTiered(tablePath, tableId);
            }
            // more rounds until a trim has seen the checkpoints and expiry followed the chain,
            // see testRegisteredLakeSnapshotsFollowTheTrimmedChain
            long deadline = System.currentTimeMillis() + TRIM_TIMEOUT_MS;
            for (int round = rounds;
                    snapshotCount(icebergCatalog.loadTable(toIceberg(tablePath))) >= rounds / 2;
                    round++) {
                assertThat(System.currentTimeMillis()).isLessThan(deadline);
                upsert(tablePath, expected, 0, 12, "r" + round);
                waitUntilAllTiered(tablePath, tableId);
            }

            org.apache.iceberg.Table lakeTable = icebergCatalog.loadTable(toIceberg(tablePath));
            String rowPos =
                    FlussPaths.remoteLakeTableSnapshotDir(
                                    FLUSS_CLUSTER_EXTENSION.getRemoteDataDir(), tablePath, tableId)
                            + "/rowPos";
            List<Long> chain = new ArrayList<>();
            filesUnder(rowPos + "/_chain").stream()
                    .map(FlinkUnionReadDerivedDvITCase::fileName)
                    .sorted()
                    .forEach(marker -> chain.add(Long.parseLong(marker.split("_")[2])));
            int snapshots = snapshotCount(lakeTable);
            assertThat(snapshots).isLessThan(rounds / 2);
            // the chain's snapshots, and every tagged readable snapshot, are still there
            for (long snapshotId : chain) {
                assertThat(lakeTable.snapshot(snapshotId)).isNotNull();
            }
            lakeTable
                    .refs()
                    .forEach(
                            (name, ref) ->
                                    assertThat(lakeTable.snapshot(ref.snapshotId())).isNotNull());
            List<String> expectedRows = new ArrayList<>();
            expected.forEach((key, value) -> expectedRows.add(rowString(key, value, null)));
            assertUnionRead(tableName, expectedRows);
            assertLakeRead(tableName, "c1, c2", expectedRows);

            // another engine's rewrite is still re-indexed
            long rewrite = rewriteAllDataFiles(tablePath);
            waitUntil(
                    () -> admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId() == rewrite,
                    Duration.ofMinutes(2),
                    Duration.ofMillis(200),
                    "readable snapshot of " + tablePath + " never moved to the rewrite");
            upsert(tablePath, expected, 0, 6, "after_rewrite");
            waitUntilAllTiered(tablePath, tableId);
            expectedRows.clear();
            expected.forEach((key, value) -> expectedRows.add(rowString(key, value, null)));
            assertUnionRead(tableName, expectedRows);
            assertLakeRead(tableName, "c1, c2", expectedRows);
        } finally {
            IcebergLakeCommitter.setExpireIntervalMs(expireInterval);
            jobClient.cancel().get();
        }
    }

    /** Upserts keys [from, to) with values {@code prefix_key}. */
    private void upsert(
            TablePath tablePath, Map<Integer, String> expected, int from, int to, String prefix)
            throws Exception {
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (int key = from; key < to; key++) {
                writer.upsert(rowOf(key, prefix + "_" + key, null));
                expected.put(key, prefix + "_" + key);
            }
            writer.flush();
        }
    }

    private long partitionId(TablePath tablePath, String partitionName) throws Exception {
        for (PartitionInfo info : admin.listPartitionInfos(tablePath).get()) {
            if (info.getPartitionName().equals(partitionName)) {
                return info.getPartitionId();
            }
        }
        throw new AssertionError("no partition " + partitionName + " of " + tablePath);
    }

    /** The Fluss offsets files of the table's lake snapshots. */
    private static Set<String> offsetsFiles(TablePath tablePath, long tableId) throws Exception {
        return filesUnder(
                FlussPaths.remoteLakeTableSnapshotDir(
                                FLUSS_CLUSTER_EXTENSION.getRemoteDataDir(), tablePath, tableId)
                        + "/metadata");
    }

    /** The files under a directory, without checksum files. */
    private static Set<String> filesUnder(String dir) throws Exception {
        java.nio.file.Path path = Paths.get(new URI(dir.startsWith("/") ? "file://" + dir : dir));
        if (!Files.exists(path)) {
            return Collections.emptySet();
        }
        try (Stream<java.nio.file.Path> files = Files.walk(path)) {
            return files.filter(Files::isRegularFile)
                    .map(java.nio.file.Path::toString)
                    .filter(f -> !f.endsWith(".crc"))
                    .collect(Collectors.toSet());
        }
    }

    private static int snapshotCount(org.apache.iceberg.Table table) {
        int count = 0;
        for (Snapshot ignored : table.snapshots()) {
            count++;
        }
        return count;
    }

    private static String fileName(String path) {
        return path.substring(path.lastIndexOf('/') + 1);
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

    private void assertCount(String tableName, long expected) throws Exception {
        try (CloseableIterator<Row> it =
                batchTEnv.executeSql("select count(*) from " + tableName).collect()) {
            assertThat(it.next().getField(0)).isEqualTo(expected);
        }
    }

    /**
     * Waits until every write of every partition is tiered and readable; returns the readable
     * snapshot.
     */
    private long waitUntilAllTiered(TablePath tablePath, long tableId, boolean partitioned) {
        if (!partitioned) {
            return waitUntilAllTiered(tablePath, tableId);
        }
        return waitValue(
                () -> {
                    LakeSnapshot snapshot;
                    try {
                        snapshot = admin.getReadableLakeSnapshot(tablePath).get();
                    } catch (Exception e) {
                        Thread.sleep(200); // nothing readable yet
                        return Optional.empty();
                    }
                    for (PartitionInfo partition : admin.listPartitionInfos(tablePath).get()) {
                        for (int b = 0; b < BUCKETS; b++) {
                            TableBucket tb =
                                    new TableBucket(tableId, partition.getPartitionId(), b);
                            long highWatermark = getLeaderReplica(tb).getLogHighWatermark();
                            if (snapshot.getTableBucketsOffset().getOrDefault(tb, 0L)
                                    < highWatermark) {
                                Thread.sleep(200);
                                return Optional.empty();
                            }
                        }
                    }
                    return Optional.of(snapshot.getSnapshotId());
                },
                Duration.ofMinutes(2),
                "writes of " + tablePath + " not all tiered");
    }

    /** Waits until every write is tiered and readable; returns the readable snapshot. */
    private long waitUntilAllTiered(TablePath tablePath, long tableId) {
        return waitValue(
                () -> {
                    LakeSnapshot snapshot;
                    try {
                        snapshot = admin.getReadableLakeSnapshot(tablePath).get();
                    } catch (Exception e) {
                        Thread.sleep(200); // nothing readable yet
                        return Optional.empty();
                    }
                    for (int b = 0; b < BUCKETS; b++) {
                        TableBucket tb = new TableBucket(tableId, b);
                        long highWatermark = getLeaderReplica(tb).getLogHighWatermark();
                        if (snapshot.getTableBucketsOffset().getOrDefault(tb, 0L) < highWatermark) {
                            Thread.sleep(200);
                            return Optional.empty();
                        }
                    }
                    return Optional.of(snapshot.getSnapshotId());
                },
                Duration.ofMinutes(2),
                "writes of " + tablePath + " not all tiered");
    }

    /**
     * Rewrites every live data file of the unpartitioned lake table into one file per bucket,
     * dropping the delete files, like a compaction by another engine; returns the rewrite's
     * snapshot.
     */
    private long rewriteAllDataFiles(TablePath tablePath) throws Exception {
        org.apache.iceberg.Table table = icebergCatalog.loadTable(toIceberg(tablePath));
        long baseSnapshotId = table.currentSnapshot().snapshotId();
        Function<FileScanTask, Integer> bucketOf = IcebergSplitPlanner.createBucketExtractor(table);
        Map<String, DataFile> oldDataFiles = new HashMap<>();
        Map<String, DeleteFile> oldDeleteFiles = new HashMap<>();
        Map<Integer, List<Record>> liveByBucket = new TreeMap<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                oldDataFiles.put(task.file().location(), task.file());
                for (DeleteFile delete : task.deletes()) {
                    oldDeleteFiles.put(delete.location(), delete);
                }
                List<Record> live =
                        liveByBucket.computeIfAbsent(bucketOf.apply(task), k -> new ArrayList<>());
                // the reader applies the task's deletes
                try (CloseableIterable<Record> records =
                        new IcebergGenericReader(table.newScan(), true).open(task)) {
                    for (Record record : records) {
                        live.add(record.copy());
                    }
                }
            }
        }
        RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(baseSnapshotId);
        oldDataFiles.values().forEach(rewrite::deleteFile);
        oldDeleteFiles.values().forEach(rewrite::deleteFile);
        for (Map.Entry<Integer, List<Record>> e : liveByBucket.entrySet()) {
            TaskWriter<Record> writer =
                    TaskWriterFactory.createTaskWriter(table, null, e.getKey(), true);
            for (Record record : e.getValue()) {
                writer.write(record);
            }
            for (DataFile file : writer.complete().dataFiles()) {
                rewrite.addFile(file);
            }
        }
        rewrite.commit();
        table.refresh();
        return table.currentSnapshot().snapshotId();
    }

    /**
     * Rewrites every live data file of the unpartitioned lake table into {@code filesPerBucket}
     * files per bucket, dropping the delete files, like a compaction by another engine; returns the
     * rewrite's snapshot.
     */
    private long rewriteAllDataFilesInto(TablePath tablePath, int filesPerBucket) throws Exception {
        org.apache.iceberg.Table table = icebergCatalog.loadTable(toIceberg(tablePath));
        long baseSnapshotId = table.currentSnapshot().snapshotId();
        Function<FileScanTask, Integer> bucketOf = IcebergSplitPlanner.createBucketExtractor(table);
        RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(baseSnapshotId);
        Map<Integer, List<Record>> liveByBucket = new TreeMap<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                rewrite.deleteFile(task.file());
                task.deletes().forEach(rewrite::deleteFile);
                List<Record> live =
                        liveByBucket.computeIfAbsent(bucketOf.apply(task), k -> new ArrayList<>());
                // the reader applies the task's deletes
                try (CloseableIterable<Record> records =
                        new IcebergGenericReader(table.newScan(), true).open(task)) {
                    for (Record record : records) {
                        live.add(record.copy());
                    }
                }
            }
        }
        for (Map.Entry<Integer, List<Record>> e : liveByBucket.entrySet()) {
            List<Record> records = e.getValue();
            for (int f = 0; f < filesPerBucket; f++) {
                TaskWriter<Record> writer =
                        TaskWriterFactory.createTaskWriter(table, null, e.getKey(), true);
                for (int i = f; i < records.size(); i += filesPerBucket) {
                    writer.write(records.get(i));
                }
                for (DataFile file : writer.complete().dataFiles()) {
                    rewrite.addFile(file);
                }
            }
        }
        rewrite.commit();
        table.refresh();
        return table.currentSnapshot().snapshotId();
    }

    /**
     * Rewrites every live data file of the unpartitioned lake table except {@code keep} into one
     * file per bucket, dropping their delete files, like another engine's compaction of those
     * files; returns the rewrite's snapshot.
     */
    private long rewriteDataFilesExcept(TablePath tablePath, Set<String> keep) throws Exception {
        org.apache.iceberg.Table table = icebergCatalog.loadTable(toIceberg(tablePath));
        long baseSnapshotId = table.currentSnapshot().snapshotId();
        Function<FileScanTask, Integer> bucketOf = IcebergSplitPlanner.createBucketExtractor(table);
        RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(baseSnapshotId);
        Map<Integer, List<Record>> liveByBucket = new TreeMap<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                if (keep.contains(task.file().location())) {
                    continue;
                }
                rewrite.deleteFile(task.file());
                task.deletes().forEach(rewrite::deleteFile);
                List<Record> live =
                        liveByBucket.computeIfAbsent(bucketOf.apply(task), k -> new ArrayList<>());
                // the reader applies the task's deletes
                try (CloseableIterable<Record> records =
                        new IcebergGenericReader(table.newScan(), true).open(task)) {
                    for (Record record : records) {
                        live.add(record.copy());
                    }
                }
            }
        }
        assertThat(liveByBucket).isNotEmpty();
        for (Map.Entry<Integer, List<Record>> e : liveByBucket.entrySet()) {
            TaskWriter<Record> writer =
                    TaskWriterFactory.createTaskWriter(table, null, e.getKey(), true);
            for (Record record : e.getValue()) {
                writer.write(record);
            }
            for (DataFile file : writer.complete().dataFiles()) {
                rewrite.addFile(file);
            }
        }
        rewrite.commit();
        table.refresh();
        return table.currentSnapshot().snapshotId();
    }

    private void assertLakeRead(String tableName, String columns, List<String> expected)
            throws Exception {
        CloseableIterator<Row> lakeIter =
                batchTEnv
                        .executeSql("select " + columns + " from " + tableName + "$lake")
                        .collect();
        assertResultsIgnoreOrder(lakeIter, expected, true);
    }

    private void assertUnionRead(String tableName, List<String> expected) throws Exception {
        CloseableIterator<Row> rowIter =
                batchTEnv.executeSql("select * from " + tableName).collect();
        assertResultsIgnoreOrder(rowIter, expected, true);
    }

    private long createDerivedTable(TablePath tablePath, int formatVersion, boolean partitioned)
            throws Exception {
        return createDerivedTable(tablePath, formatVersion, partitioned, Collections.emptyMap());
    }

    private long createDerivedTable(
            TablePath tablePath,
            int formatVersion,
            boolean partitioned,
            Map<String, String> properties)
            throws Exception {
        Schema.Builder schema =
                Schema.newBuilder().column("c1", DataTypes.INT()).column("c2", DataTypes.STRING());
        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .distributedBy(BUCKETS)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key(), "true")
                        .customProperty("iceberg.format-version", String.valueOf(formatVersion));
        properties.forEach(builder::property);
        if (partitioned) {
            schema.column("c3", DataTypes.STRING()).primaryKey("c1", "c3");
            builder.partitionedBy("c3");
        } else {
            schema.primaryKey("c1");
        }
        return createTable(tablePath, builder.schema(schema.build()).build());
    }

    /** Upserts a tick row until the readable snapshot advances. */
    private long tickUntilReadableAdvances(
            TablePath tablePath, long previous, @Nullable String partition) throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(3).toMillis();
        while (System.currentTimeMillis() < deadline) {
            try {
                long id = admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId();
                if (id != previous) {
                    return id;
                }
            } catch (Exception e) {
                // no readable snapshot yet
            }
            writeRows(tablePath, Collections.singletonList(rowOf(-1, "tick", partition)), false);
            Thread.sleep(1000);
        }
        throw new AssertionError("readable snapshot of " + tablePath + " stuck at " + previous);
    }

    private static InternalRow rowOf(int key, String value, @Nullable String partition) {
        return partition == null ? row(key, value) : row(key, value, partition);
    }

    private static String rowString(int key, String value, @Nullable String partition) {
        return partition == null
                ? String.format("+I[%d, %s]", key, value)
                : String.format("+I[%d, %s, %s]", key, value, partition);
    }
}
