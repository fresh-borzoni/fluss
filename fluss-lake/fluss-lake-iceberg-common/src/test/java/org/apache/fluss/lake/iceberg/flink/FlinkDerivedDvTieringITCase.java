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
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.flink.utils.LogCapture;
import org.apache.fluss.lake.iceberg.source.IcebergSplitPlanner;
import org.apache.fluss.lake.iceberg.testutils.CommitHookCatalog;
import org.apache.fluss.lake.iceberg.testutils.ExternalWrites;
import org.apache.fluss.lake.iceberg.tiering.IcebergLakeCommitter;
import org.apache.fluss.lake.iceberg.tiering.IcebergLakeTieringFactory;
import org.apache.fluss.lake.iceberg.tiering.writer.TaskWriterFactory;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.zk.data.lake.LakeTable;
import org.apache.fluss.utils.FlussPaths;

import org.apache.flink.core.execution.JobClient;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the tiering of tables with deletion vectors ({@code table.deletion-vectors.enabled =
 * true}) to Iceberg v2 (file-scoped position deletes) and v3 (deletion vectors). They check the
 * lake through Iceberg: every lake snapshot Fluss commits has one live row per key, also after
 * rewrites, rollbacks and writes by other engines, dropped partitions, failed rounds, trims and
 * snapshot expiry. The union reads of these tables are tested in {@link
 * FlinkUnionReadDerivedDvITCase}.
 */
class FlinkDerivedDvTieringITCase extends FlinkDerivedDvTestBase {
    // how long the trim tests keep writing rounds until a trim has seen the leaders' checkpoints
    private static final long TRIM_TIMEOUT_MS = 180_000;

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
            assertLakeRead(tableName, "c1, c2", rows);
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * A rewrite by another engine, committed while a round commits, of files the round's deletes do
     * not reference makes the round run again and re-index the rewrite first. Later changes of the
     * rewritten rows then supersede them in the lake.
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
     * A round in flight when a partition with its rows is dropped does not commit that partition's
     * rows: in the lake, the dropped partition keeps the rows of the round before, and the other
     * partition is exact.
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
            assertLakeRead(tableName, "c1, c2", rowStrings);
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * A rewrite by another engine into several files per bucket is re-indexed in several splits per
     * bucket whose index parts make up one index change, and later changes of the rewritten rows
     * supersede them in the lake.
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
            // the rewrite left no deletes behind
            assertLakeRead(tableName, "c1, c2", expectedRows);

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
            assertLakeRead(tableName, "c1, c2", expectedRows);
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * Tiering goes on after a racing allocation writes a counter marker below ids already
     * registered. Later rounds still publish index changes the chain takes, instead of failing
     * every round after its lake commit.
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
                            + "/rowPos/_file_ids";
            java.nio.file.Path counter =
                    Paths.get(
                            new URI(
                                    counterPath.startsWith("/")
                                            ? "file://" + counterPath
                                            : counterPath));
            assertThat(counter).isNotEmptyDirectory();
            // a racer that read the counter at 0 writes its marker last
            Files.write(counter.resolve("v1-0"), new byte[0]);

            upsert(tablePath, expected, 5, 10, "updated");
            waitUntilAllTiered(tablePath, tableId);
            List<String> expectedRows = new ArrayList<>();
            expected.forEach((key, value) -> expectedRows.add(rowString(key, value, null)));
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
            int rounds = ConfigOptions.KV_DELETION_VECTORS_CHECKPOINT_INTERVAL.defaultValue() + 15;
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
            assertThat(filesUnder(rowPos)).anyMatch(f -> f.contains("/_trimmed/"));
            // A round keeps Fluss's registrations from the chain's oldest snapshot as it was when
            // the round registered, before its trim: right after a trim they lag the chain by one
            // round. Never ahead of it: every snapshot on the chain stays registered.
            List<Long> chain = chainSnapshots(rowPos);
            assertThat(registeredSnapshots(tableId)).containsAll(chain);
            // one more round that does not trim brings them level
            for (int round = 0; ; round++) {
                assertThat(System.currentTimeMillis()).isLessThan(deadline);
                upsert(tablePath, expected, 0, 12, "after" + round);
                waitUntilAllTiered(tablePath, tableId);
                List<Long> after = chainSnapshots(rowPos);
                assertThat(registeredSnapshots(tableId)).containsAll(after);
                boolean trimmed = !after.get(0).equals(chain.get(0));
                chain = after;
                if (!trimmed) {
                    break;
                }
            }
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
            assertLakeRead(tableName, "c1, c2", expectedRows);
        } finally {
            jobClient.cancel().get();
        }
    }

    /** The snapshots of a table's index chain, in chain order. */
    private static List<Long> chainSnapshots(String rowPos) throws Exception {
        List<String> markers = new ArrayList<>();
        filesUnder(rowPos + "/_chain").stream()
                .map(FlinkDerivedDvTieringITCase::fileName)
                .sorted()
                .forEach(markers::add);
        List<Long> chain = new ArrayList<>();
        for (String marker : markers) {
            chain.add(Long.parseLong(marker.split("_")[2]));
        }
        return chain;
    }

    /** The lake snapshots Fluss has registered for a table. */
    private static List<Long> registeredSnapshots(long tableId) throws Exception {
        List<Long> ids = new ArrayList<>();
        for (LakeTable.LakeSnapshotMetadata metadata :
                FLUSS_CLUSTER_EXTENSION
                        .getZooKeeperClient()
                        .getLakeTable(tableId)
                        .get()
                        .getLakeSnapshotMetadatas()) {
            ids.add(metadata.getSnapshotId());
        }
        return ids;
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
            int rounds = ConfigOptions.KV_DELETION_VECTORS_CHECKPOINT_INTERVAL.defaultValue() + 15;
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
            assertThat(filesUnder(rowPos)).anyMatch(f -> f.contains("/_trimmed/"));
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
     * Rounds much larger than the write buffer the tiering task's writers share stay exact in the
     * lake. The writers spill sorted rows to local disk and write only the last version of every
     * key, also of hot keys with many versions in a round. Their staged index parts are gone once
     * the rounds commit.
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
            assertLakeExact(tableName, expected);

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
            assertLakeExact(tableName, expected);
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
            assertLakeExact(tableName, expected);

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
            assertLakeExact(tableName, expected);
            assertCount(tableName + "$lake", expected.size());
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * A backlog larger than a round may hold, as after the tiering service was down, is tiered in
     * several bounded rounds whose snapshots are each exact. A bucket's round ends once its writer
     * holds the configured number of keys, and the next round starts at once from there.
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
            assertLakeRead(tableName, "c1, c2", expectedRows);
        } finally {
            jobClient.cancel().get();
        }
    }

    /** The change directories under rowPos that no chain marker registers. */
    private static Set<String> unregisteredChanges(String rowPos) throws Exception {
        Set<String> registered = new HashSet<>();
        filesUnder(rowPos + "/_chain").stream()
                .map(FlinkDerivedDvTieringITCase::fileName)
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
     * oldest on. Lake reads stay exact, and a rewrite by another engine is still re-indexed.
     */
    @Test
    void testAutoExpireSnapshotsKeepsWhatFlussNeeds() throws Exception {
        org.apache.fluss.config.Configuration trimOften =
                new org.apache.fluss.config.Configuration();
        trimOften.set(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_TRIM_INTERVAL, 5);
        trimOften.set(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_TRIM_RETAIN, 1);
        // expires the snapshots after every commit
        JobClient jobClient =
                buildTieringJob(
                        execEnv,
                        getIcebergCatalogConf(),
                        trimOften,
                        new IcebergLakeTieringFactory(
                                org.apache.fluss.config.Configuration.fromMap(
                                        getIcebergCatalogConf()),
                                0));
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
            int rounds = ConfigOptions.KV_DELETION_VECTORS_CHECKPOINT_INTERVAL.defaultValue() + 15;
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
                    .map(FlinkDerivedDvTieringITCase::fileName)
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
            assertLakeRead(tableName, "c1, c2", expectedRows);
        } finally {
            jobClient.cancel().get();
        }
    }

    private void assertLakeExact(String tableName, Map<Integer, String> expected) throws Exception {
        List<String> rowStrings = new ArrayList<>();
        expected.forEach((key, value) -> rowStrings.add(rowString(key, value, null)));
        assertLakeRead(tableName, "c1, c2", rowStrings);
    }

    /** The Fluss offsets files of the table's lake snapshots. */
    private static Set<String> offsetsFiles(TablePath tablePath, long tableId) throws Exception {
        return filesUnder(
                FlussPaths.remoteLakeTableSnapshotDir(
                                FLUSS_CLUSTER_EXTENSION.getRemoteDataDir(), tablePath, tableId)
                        + "/metadata");
    }

    private static int snapshotCount(org.apache.iceberg.Table table) {
        int count = 0;
        for (Snapshot ignored : table.snapshots()) {
            count++;
        }
        return count;
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

    private static String fileName(String path) {
        return path.substring(path.lastIndexOf('/') + 1);
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
}
