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

import org.apache.fluss.client.admin.DvAdmin;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.flink.utils.LogCapture;
import org.apache.fluss.lake.iceberg.tiering.IcebergLakeWriter;
import org.apache.fluss.lake.iceberg.tiering.writer.TaskWriterFactory;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.messages.GetDvSnapshotRequest;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.testutils.common.CommonTestUtils;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenericReader;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for union reads of an Iceberg primary-key table with deletion vectors and {@code
 * table.datalake.auto-compaction=true}, on the clean lake layout. Reads must be exact before and
 * after the compaction.
 *
 * <p>The tiering writer compacts a bucket once it has {@code MIN_FILES_TO_COMPACT} (3) or more
 * small data files. The rewrite reads the files with their deletes applied, and the round commits
 * it as a {@code REPLACE} snapshot before its own deletes. The compaction hands the new position of
 * every row it kept to the index, so the committer registers the rewrite without a re-index and the
 * round is not held. A rewrite by another engine is still re-indexed by the tiering readers.
 */
class FlinkUnionReadDvAutoCompactionITCase extends FlinkUnionReadTestBase {

    private static final String COMMITTER_LOGGER = "org.apache.fluss.flink.tiering.committer";
    private static final String WRITER_LOGGER = "org.apache.fluss.lake.iceberg.tiering";
    private static final Pattern WAITED = Pattern.compile("Waited (\\d+) ms");

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
        // Tiering writes the position index under the remote data dir; the tiering job takes its
        // Fluss config from clientConf, which has no remote dir by default.
        clientConf.set(ConfigOptions.REMOTE_DATA_DIR, FLUSS_CLUSTER_EXTENSION.getRemoteDataDir());
    }

    /**
     * Reads stay exact across Fluss's compactions, which hand their positions to the index without
     * holding a round. On v2 the deletes are position delete files, on v3 deletion vectors.
     */
    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testUnionReadDvTableWithAutoCompaction(int formatVersion) throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try (LogCapture committerLog = LogCapture.of(COMMITTER_LOGGER)) {
            String tableName = "dv_auto_compaction_v" + formatVersion;
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            int bucketNum = 1;
            long tableId = createDvAutoCompactionTable(tablePath, bucketNum, formatVersion);

            Map<Integer, String> expected = new HashMap<>();

            write(tablePath, expected, range(0, 5, "v"));
            waitUntilTiered(new TableBucket(tableId, 0));

            // Updates of the tiered keys 0, 1 and 3, and a new key 5. The old versions of these
            // rows must be deleted by position after the compaction.
            Map<Integer, String> round2 = new LinkedHashMap<>();
            round2.put(0, "v0_updated");
            round2.put(1, "v1_updated");
            round2.put(3, "v3_updated");
            round2.put(5, "v5");
            write(tablePath, expected, round2);
            waitUntilTiered(new TableBucket(tableId, 0));

            // the clean layout, and exact reads before any compaction
            Table lakeTable = icebergCatalog.loadTable(toIceberg(tablePath));
            assertThat(lakeTable.schema().columns()).hasSize(2);
            assertThat(hasCompactionSnapshot(tablePath)).isFalse();
            assertReads(tableName, expected);

            // Two more rounds, so the writer of the fourth finds MIN_FILES_TO_COMPACT files and
            // compacts them in a REPLACE snapshot.
            write(tablePath, expected, range(6, 11, "v"));
            waitUntilTiered(new TableBucket(tableId, 0));
            write(tablePath, expected, range(11, 16, "v"));
            waitUntilTiered(new TableBucket(tableId, 0));

            CommonTestUtils.retry(
                    Duration.ofMinutes(2),
                    () -> assertThat(hasCompactionSnapshot(tablePath)).isTrue());

            // The leader pins a cut of the readable snapshot.
            CommonTestUtils.retry(
                    Duration.ofMinutes(2),
                    () -> {
                        long readableSnapshotId =
                                admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId();
                        assertThat(readableSnapshotId).isGreaterThan(0);
                        GetDvSnapshotRequest pin =
                                new GetDvSnapshotRequest()
                                        .setTableId(tableId)
                                        .setBucketId(0)
                                        .setReadableSnapshotId(readableSnapshotId);
                        GetDvSnapshotResponse cut =
                                DvAdmin.of(admin).getDvSnapshot(tablePath, pin).get();
                        assertThat(cut.getSnapshotStartOffset()).isGreaterThanOrEqualTo(0L);
                        DvAdmin.of(admin)
                                .getDvSnapshot(
                                        tablePath,
                                        pin.setLogEndOffset(cut.getLogEndOffset())
                                                .setReleaseCut(true))
                                .get();
                    });

            // Exact reads across the compaction show that the index covers the compacted files
            // and dropped the removed ones: no row is over-masked and no stale row surfaces.
            assertReads(tableName, expected);
            assertNoDanglingDeletes(tablePath);

            // and the next round resolves its keys against the compacted files
            Map<Integer, String> round5 = new LinkedHashMap<>();
            round5.put(2, "v2_updated");
            round5.put(12, "v12_updated");
            write(tablePath, expected, round5);
            waitUntilTiered(new TableBucket(tableId, 0));
            assertReads(tableName, expected);

            // Fluss's compactions handed their positions to the index: no round was held for a
            // re-index of the files they wrote.
            String table = tablePath.toString();
            assertThat(compactionsHandedOver(committerLog, table)).isNotEmpty();
            assertThat(heldRounds(committerLog, table)).isEmpty();
            assertThat(reindexes(committerLog, table)).isEmpty();

            // A rewrite by another engine is still re-indexed from the lake, holding a round:
            // the lines the checks above find none of are logged then.
            rewriteAllDataFiles(tablePath);
            Map<Integer, String> round6 = new LinkedHashMap<>();
            round6.put(4, "v4_updated");
            round6.put(13, "v13_updated");
            write(tablePath, expected, round6);
            waitUntilTiered(new TableBucket(tableId, 0));
            CommonTestUtils.retry(
                    Duration.ofMinutes(1),
                    () -> assertThat(reindexes(committerLog, table)).isNotEmpty());
            assertThat(heldRounds(committerLog, table)).isNotEmpty();
            assertReads(tableName, expected);
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * Over many short rounds of inserts, updates and deletes, the tiered policy at a zero interval
     * keeps the bucket at few files and reads stay exact. Every compaction hands its positions
     * over, so no round is held, and no round waits long for its compaction.
     */
    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testTieredCompactionOverManyRounds(int formatVersion) throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try (LogCapture committerLog = LogCapture.of(COMMITTER_LOGGER);
                LogCapture writerLog = LogCapture.of(WRITER_LOGGER)) {
            String tableName = "dv_tiered_compaction_v" + formatVersion;
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId =
                    createTable(
                            tablePath,
                            dvAutoCompactionTable(formatVersion)
                                    .property(
                                            ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION_INTERVAL,
                                            Duration.ZERO)
                                    .build());
            TableBucket bucket = new TableBucket(tableId, 0);
            Map<Integer, String> expected = new HashMap<>();
            int maxFiles = 0;
            for (int round = 0; round < 12; round++) {
                Map<Integer, String> rows = range(5 * round, 5 * round + 5, "v");
                if (round > 0) {
                    rows.put(5 * (round - 1), "updated_" + round);
                    rows.put(round / 2, "updated_" + round);
                }
                write(tablePath, expected, rows);
                if (round > 1) {
                    int deleted = 5 * (round - 2) + 1;
                    try (org.apache.fluss.client.table.Table fluss = conn.getTable(tablePath)) {
                        UpsertWriter writer = fluss.newUpsert().createWriter();
                        writer.delete(row(deleted, expected.remove(deleted)));
                        writer.flush();
                    }
                }
                waitUntilTiered(bucket);
                maxFiles = Math.max(maxFiles, liveDataFiles(tablePath));
            }
            assertReads(tableName, expected);

            String table = tablePath.toString();
            long maxWaitMs = 0;
            for (String m : writerLog.messagesWith("Waited", "for the compaction of")) {
                Matcher matcher = WAITED.matcher(m);
                assertThat(matcher.find()).isTrue();
                maxWaitMs = Math.max(maxWaitMs, Long.parseLong(matcher.group(1)));
            }
            assertThat(compactionsHandedOver(committerLog, table)).hasSizeGreaterThanOrEqualTo(3);
            assertThat(heldRounds(committerLog, table)).isEmpty();
            assertThat(maxFiles).isLessThanOrEqualTo(4);
            assertThat(maxWaitMs).isLessThan(IcebergLakeWriter.COMPACTION_MAX_WAIT_MS + 5000);
        } finally {
            jobClient.cancel().get();
        }
    }

    // The committer's lines these tests count; testUnionReadDvTableWithAutoCompaction sees each
    // of them, so a check that finds none of a line cannot pass for a line that is worded anew.

    /** The Fluss compactions of {@code table} that handed their positions to the index. */
    private static List<String> compactionsHandedOver(LogCapture log, String table) {
        return log.messagesWith("The Fluss compaction of " + table + " committed as snapshot");
    }

    /** The rounds of {@code table} held for a re-index of rewritten files. */
    private static List<String> heldRounds(LogCapture log, String table) {
        return log.messagesWith("The round of " + table + " is held until");
    }

    /** The rewritten snapshots of {@code table} the tiering readers re-indexed. */
    private static List<String> reindexes(LogCapture log, String table) {
        return log.messagesWith("Re-indexed rewritten snapshot", " of " + table + " with");
    }

    private int liveDataFiles(TablePath tablePath) throws Exception {
        Table table = icebergCatalog.loadTable(toIceberg(tablePath));
        int files = 0;
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask ignored : tasks) {
                files++;
            }
        }
        return files;
    }

    /**
     * Rewrites every live data file of the single-bucket lake table into one file, dropping the
     * delete files, like a compaction by another engine.
     */
    private void rewriteAllDataFiles(TablePath tablePath) throws Exception {
        Table table = icebergCatalog.loadTable(toIceberg(tablePath));
        long baseSnapshotId = table.currentSnapshot().snapshotId();
        RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(baseSnapshotId);
        List<Record> live = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                rewrite.deleteFile(task.file());
                task.deletes().forEach(rewrite::deleteFile);
                try (CloseableIterable<Record> records =
                        new IcebergGenericReader(table.newScan(), true).open(task)) {
                    for (Record record : records) {
                        live.add(record.copy());
                    }
                }
            }
        }
        try (TaskWriter<Record> writer = TaskWriterFactory.createTaskWriter(table, null, 0, true)) {
            for (Record record : live) {
                writer.write(record);
            }
            for (DataFile file : writer.complete().dataFiles()) {
                rewrite.addFile(file);
            }
        }
        rewrite.commit();
    }

    /** Waits until the bucket is tiered up to its high watermark, so every write is a round. */
    private void waitUntilTiered(TableBucket tableBucket) {
        waitUntil(
                () -> {
                    Replica replica = getLeaderReplica(tableBucket);
                    return replica.getLakeLogEndOffset() == replica.getLogHighWatermark();
                },
                Duration.ofMinutes(2),
                "bucket " + tableBucket + " not tiered to its high watermark");
    }

    private static TableDescriptor.Builder dvAutoCompactionTable(int formatVersion) {
        return TableDescriptor.builder()
                .schema(
                        Schema.newBuilder()
                                .column("c1", DataTypes.INT())
                                .column("c2", DataTypes.STRING())
                                .primaryKey("c1")
                                .build())
                .distributedBy(1)
                .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                .property(ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION.key(), "true")
                .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key(), "true")
                .customProperty("iceberg.format-version", String.valueOf(formatVersion));
    }

    private long createDvAutoCompactionTable(TablePath tablePath, int bucketNum, int formatVersion)
            throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .primaryKey("c1")
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(bucketNum)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .property(ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION.key(), "true")
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key(), "true")
                        .customProperty("iceberg.format-version", String.valueOf(formatVersion))
                        .build();
        return createTable(tablePath, tableDescriptor);
    }

    /** True once Fluss committed a REPLACE snapshot that rewrote data files (a compaction). */
    private boolean hasCompactionSnapshot(TablePath tablePath) {
        Table table = icebergCatalog.loadTable(toIceberg(tablePath));
        table.refresh();
        for (Snapshot snapshot : table.snapshots()) {
            if (DataOperations.REPLACE.equals(snapshot.operation())
                    && "fluss-lake-compaction".equals(snapshot.summary().get("commit-user"))
                    && snapshot.removedDataFiles(table.io()).iterator().hasNext()) {
                return true;
            }
        }
        return false;
    }

    /** No delete file of the current snapshot points at a data file the compaction removed. */
    private void assertNoDanglingDeletes(TablePath tablePath) throws Exception {
        Table table = icebergCatalog.loadTable(toIceberg(tablePath));
        table.refresh();
        Set<String> liveDataFiles = new HashSet<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                liveDataFiles.add(task.file().location());
            }
        }
        for (ManifestFile manifest : table.currentSnapshot().deleteManifests(table.io())) {
            try (ManifestReader<DeleteFile> reader =
                    ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
                for (DeleteFile deleteFile : reader) {
                    assertThat(deleteFile.referencedDataFile()).isNotNull();
                    assertThat(liveDataFiles).contains(deleteFile.referencedDataFile().toString());
                }
            }
        }
    }

    /** The union read, and the lake on its own, both return the latest value of every key. */
    private void assertReads(String tableName, Map<Integer, String> expected) throws Exception {
        CloseableIterator<Row> rowIter =
                batchTEnv.executeSql("select * from " + tableName).collect();
        assertResultsIgnoreOrder(rowIter, expectedStrings(expected), true);
        CloseableIterator<Row> lakeIter =
                batchTEnv.executeSql("select c1, c2 from " + tableName + "$lake").collect();
        assertResultsIgnoreOrder(lakeIter, expectedStrings(expected), true);
    }

    private void write(TablePath tablePath, Map<Integer, String> expected, Map<Integer, String> kv)
            throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        for (Map.Entry<Integer, String> e : kv.entrySet()) {
            rows.add(row(e.getKey(), e.getValue()));
        }
        writeRows(tablePath, rows, false);
        expected.putAll(kv);
    }

    private static Map<Integer, String> range(int from, int to, String prefix) {
        Map<Integer, String> kv = new LinkedHashMap<>();
        for (int i = from; i < to; i++) {
            kv.put(i, prefix + "_" + i);
        }
        return kv;
    }

    private static List<String> expectedStrings(Map<Integer, String> expected) {
        return expected.entrySet().stream()
                .map(e -> "+I[" + e.getKey() + ", " + e.getValue() + "]")
                .collect(Collectors.toList());
    }
}
