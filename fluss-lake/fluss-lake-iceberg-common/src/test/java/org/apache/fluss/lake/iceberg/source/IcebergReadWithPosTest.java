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

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.committer.LakeCommitter.CommitContext;
import org.apache.fluss.lake.iceberg.IcebergLakeCatalog;
import org.apache.fluss.lake.iceberg.maintenance.IcebergRewriteDataFiles;
import org.apache.fluss.lake.iceberg.maintenance.RewriteDataFileResult;
import org.apache.fluss.lake.iceberg.tiering.IcebergCatalogProvider;
import org.apache.fluss.lake.iceberg.tiering.IcebergLakeCommitter;
import org.apache.fluss.lake.iceberg.tiering.IcebergLakeTieringFactory;
import org.apache.fluss.lake.iceberg.tiering.IcebergWriteResult;
import org.apache.fluss.lake.lakestorage.TestingLakeCatalogContext;
import org.apache.fluss.lake.source.DataDeltaPlan;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.RowWithPosResult;
import org.apache.fluss.lake.source.SupportsRowPositions;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.WriteResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@code IcebergRecordReader.readWithPos()}, {@code IcebergSplit.fileName()} and {@code
 * IcebergLakeSource.planRewrite()} of tables with deletion vectors.
 */
class IcebergReadWithPosTest {

    private static final int BUCKET_NUM = 1;

    @TempDir private File tempWarehouseDir;

    private Configuration configuration;
    private IcebergLakeTieringFactory tieringFactory;
    private IcebergCatalogProvider catalogProvider;

    @BeforeEach
    void beforeEach() {
        configuration = new Configuration();
        configuration.setString("warehouse", "file://" + tempWarehouseDir);
        configuration.setString("type", "hadoop");
        configuration.setString("name", "test");
        tieringFactory = new IcebergLakeTieringFactory(configuration);
        catalogProvider = new IcebergCatalogProvider(configuration);
    }

    @Test
    void readWithPosYieldsKeyPositionAndFileName() throws Exception {
        TablePath tablePath = createDvTableAndWrite("dv_readpos_table");
        Table table = catalogProvider.get().loadTable(toIceberg(tablePath));
        IcebergSplit split = currentSplit(table);
        assertThat(split.fileName()).isNotNull().endsWith(".parquet");

        IcebergLakeSource source = new IcebergLakeSource(configuration, tablePath);
        source.withProject(new int[][] {{0}});
        List<String> fileNames = new ArrayList<>();
        List<long[]> keyAndPos =
                collectKeyAndPos(source.createRecordReader(() -> split), fileNames);

        // a fresh append without deletes: keys 1, 2, 3 at positions 0, 1, 2
        assertThat(keyAndPos)
                .containsExactly(new long[] {1, 0}, new long[] {2, 1}, new long[] {3, 2});
        assertThat(fileNames).containsOnly(split.fileName());
    }

    @Test
    void readWithPosReportsOriginalPositionsWithCommittedDeletesApplied() throws Exception {
        TablePath tablePath = createDvTableAndWrite("dv_readpos_dv_table");
        Table table = catalogProvider.get().loadTable(toIceberg(tablePath));
        table.refresh();
        long baseSnapshotId = table.currentSnapshot().snapshotId();
        String fileName = currentSplit(table).fileName();

        // delete position 1 with a deletion vector committed on top of the base
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            committer.commitWithDeletes(
                    committer.toCommittable(Collections.emptyList()),
                    Collections.singletonMap("round", "2"),
                    Collections.singletonMap(fileName, new long[] {1L}),
                    baseSnapshotId);
        }

        // plan a split at the current snapshot so the FileScanTask carries the deletion vector
        IcebergSplit split = currentSplit(table);
        IcebergLakeSource source = new IcebergLakeSource(configuration, tablePath);
        source.withProject(new int[][] {{0}});
        List<long[]> keyAndPos =
                collectKeyAndPos(source.createRecordReader(() -> split), new ArrayList<>());

        // position 1 is deleted; the other rows keep their original positions 0 and 2
        assertThat(keyAndPos).containsExactly(new long[] {1, 0}, new long[] {3, 2});
    }

    @Test
    void planRewriteReportsRemovedFilesAndReindexesRewrittenFiles() throws Exception {
        // A compaction needs MIN_FILES_TO_COMPACT (3) files, so bucket 0 gets three data files:
        // one from the helper and two from more append rounds.
        TablePath tablePath = createDvTableAndWrite("dv_compaction_table");
        TableInfo tableInfo = dvTableInfo(tablePath);
        appendRound(tablePath, tableInfo, "2", insert(3, 4, "d"), insert(4, 5, "e"));
        appendRound(tablePath, tableInfo, "3", insert(5, 6, "f"), insert(6, 7, "g"));

        Table table = catalogProvider.get().loadTable(toIceberg(tablePath));
        table.refresh();
        long baseSnapshotId = table.currentSnapshot().snapshotId();

        IcebergLakeSource source = new IcebergLakeSource(configuration, tablePath);
        // nothing was rewritten since the base
        assertThat(source.planRewrite(baseSnapshotId, null)).isNull();

        Set<String> oldBasenames = new HashSet<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask t : tasks) {
                oldBasenames.add(IcebergSplit.fileNameOf(t));
            }
        }
        assertThat(oldBasenames).hasSize(3);

        // another engine compacts bucket 0's files into one, in a REPLACE snapshot
        RewriteDataFileResult rr =
                new IcebergRewriteDataFiles(table, null, new TableBucket(0L, 0))
                        .targetSizeInBytes(Long.MAX_VALUE)
                        .execute();
        assertThat(rr).isNotNull();
        table.newRewrite()
                .rewriteFiles(
                        new HashSet<>(rr.deletedDataFiles()), new HashSet<>(rr.addedDataFiles()))
                .commit();
        table.refresh();
        long compactSnapshotId = table.currentSnapshot().snapshotId();

        DataDeltaPlan<IcebergSplit> plan = source.planRewrite(baseSnapshotId, null);
        assertThat(plan).isNotNull();
        assertThat(plan.getCompactSnapshotId()).isEqualTo(compactSnapshotId);
        assertThat(deletedFiles(plan, 0)).containsExactlyInAnyOrderElementsOf(oldBasenames);
        // the rewritten files are re-indexed as one split each
        assertThat(plan.getSplits()).hasSize(rr.addedDataFiles().size());
    }

    @Test
    void compactionSnapshotIsNotMistakenForLatestTieredDataSnapshot() throws Exception {
        // three rounds commit three data files under the tiering user
        TablePath tablePath = createDvTableAndWrite("dv_compaction_reconcile_table");
        TableInfo tableInfo = dvTableInfo(tablePath);
        appendRound(tablePath, tableInfo, "2", insert(3, 4, "d"), insert(4, 5, "e"));
        appendRound(tablePath, tableInfo, "3", insert(5, 6, "f"), insert(6, 7, "g"));

        Table table = catalogProvider.get().loadTable(toIceberg(tablePath));
        table.refresh();
        long latestDataSnapshotId = table.currentSnapshot().snapshotId();

        // Commit the compaction as IcebergLakeCommitter#commitPendingRewrite does: under the
        // compaction committer user, not the tiering user.
        RewriteDataFileResult rr =
                new IcebergRewriteDataFiles(table, null, new TableBucket(0L, 0))
                        .targetSizeInBytes(Long.MAX_VALUE)
                        .execute();
        assertThat(rr).isNotNull();
        table.newRewrite()
                .rewriteFiles(
                        new HashSet<>(rr.deletedDataFiles()), new HashSet<>(rr.addedDataFiles()))
                .set("commit-user", "fluss-lake-compaction")
                .commit();
        table.refresh();
        assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.REPLACE);

        // The restart reconcile finds no missing snapshot. The compaction has its own commit
        // user, so the latest tiered data snapshot is still latestDataSnapshotId, which Fluss
        // knows. Under the tiering user, the compaction would be returned here.
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            assertThat(committer.getMissingLakeSnapshot(latestDataSnapshotId)).isNull();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void flussCompactionRewritesOnlyItsBucketWithDeletesApplied(int formatVersion)
            throws Exception {
        TablePath tablePath = TablePath.of("iceberg", "dv_compaction_v" + formatVersion);
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("c1", DataTypes.INT())
                                        .column("c2", DataTypes.STRING())
                                        .primaryKey("c1")
                                        .build())
                        .distributedBy(2, "c1")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED, true)
                        .customProperty("iceberg.format-version", String.valueOf(formatVersion))
                        .build();
        try (IcebergLakeCatalog catalog = new IcebergLakeCatalog(configuration)) {
            catalog.createTable(tablePath, descriptor, new TestingLakeCatalogContext());
        }
        TableInfo tableInfo =
                TableInfo.of(tablePath, 0, 1, descriptor, DEFAULT_REMOTE_DATA_DIR, 1L, 1L);
        // three small files in bucket 0, one in bucket 1
        appendRound(tablePath, tableInfo, 0, "1", insert(0, 1, "a"), insert(1, 2, "b"));
        appendRound(tablePath, tableInfo, 0, "2", insert(2, 3, "c"));
        appendRound(tablePath, tableInfo, 0, "3", insert(3, 4, "d"));
        appendRound(tablePath, tableInfo, 1, "4", insert(0, 5, "e"));

        // delete key 2 (position 1 of bucket 0's first file) and key 5 (bucket 1's only row):
        // position delete files on v2, deletion vectors on v3
        Table table = catalogProvider.get().loadTable(toIceberg(tablePath));
        table.refresh();
        Map<String, long[]> deletes = new HashMap<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                if (task.file().recordCount() == 2) {
                    deletes.put(IcebergSplit.fileNameOf(task), new long[] {1L});
                } else if (task.file().partition().get(0, Integer.class) == 1) {
                    deletes.put(IcebergSplit.fileNameOf(task), new long[] {0L});
                }
            }
        }
        assertThat(deletes).hasSize(2);
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            committer.commitWithDeletes(
                    committer.toCommittable(Collections.emptyList()),
                    Collections.singletonMap("round", "5"),
                    deletes,
                    table.currentSnapshot().snapshotId());
        }
        table.refresh();
        assertThat(liveDeleteFiles(table)).hasSize(2);

        RewriteDataFileResult rr =
                new IcebergRewriteDataFiles(table, null, new TableBucket(0L, 0))
                        .targetSizeInBytes(Long.MAX_VALUE)
                        .execute();

        // only bucket 0's files are rewritten, and without the deleted row
        assertThat(rr).isNotNull();
        assertThat(rr.deletedDataFiles())
                .hasSize(3)
                .allSatisfy(f -> assertThat(f.partition().get(0, Integer.class)).isZero());
        assertThat(rr.addedDataFiles().stream().mapToLong(DataFile::recordCount).sum())
                .isEqualTo(3);
        // v2: the rewrite drops the position delete file of the replaced file; v3: Iceberg drops
        // the deletion vector of a removed data file itself
        assertThat(rr.deletedDeleteFiles()).hasSize(formatVersion == 2 ? 1 : 0);
        // committed the way a tiering round commits it, before the round's deletes
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            assertThat(
                            committer.commitPendingRewrite(
                                    committer.toCommittable(
                                            Collections.singletonList(
                                                    new IcebergWriteResult(
                                                            WriteResult.builder().build(),
                                                            table.specs(),
                                                            rr)))))
                    .isNotNull();
        }
        table.refresh();
        assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.REPLACE);
        // no delete file is left pointing at a removed data file; bucket 1's one is kept
        Set<String> liveDataFiles = new HashSet<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                liveDataFiles.add(task.file().location());
            }
        }
        List<DeleteFile> liveDeletes = liveDeleteFiles(table);
        assertThat(liveDeletes).hasSize(1);
        assertThat(liveDataFiles).contains(liveDeletes.get(0).referencedDataFile().toString());
        List<Integer> keys = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record record : records) {
                keys.add((Integer) record.getField("c1"));
            }
        }
        assertThat(keys).containsExactlyInAnyOrder(1, 3, 4);
    }

    /** The delete files live in the current snapshot, dangling ones included. */
    private static List<DeleteFile> liveDeleteFiles(Table table) throws Exception {
        List<DeleteFile> deleteFiles = new ArrayList<>();
        for (ManifestFile manifest : table.currentSnapshot().deleteManifests(table.io())) {
            try (ManifestReader<DeleteFile> reader =
                    ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
                for (DeleteFile deleteFile : reader) {
                    deleteFiles.add(deleteFile.copy());
                }
            }
        }
        return deleteFiles;
    }

    private TableInfo dvTableInfo(TablePath tablePath) {
        Schema flussSchema =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .primaryKey("c1")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(flussSchema)
                        .distributedBy(BUCKET_NUM, "c1")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED, true)
                        .build();
        return TableInfo.of(tablePath, 0, 1, descriptor, DEFAULT_REMOTE_DATA_DIR, 1L, 1L);
    }

    private void appendRound(
            TablePath tablePath, TableInfo tableInfo, String round, LogRecord... records)
            throws Exception {
        appendRound(tablePath, tableInfo, 0, round, records);
    }

    private void appendRound(
            TablePath tablePath,
            TableInfo tableInfo,
            int bucket,
            String round,
            LogRecord... records)
            throws Exception {
        List<IcebergWriteResult> results = new ArrayList<>();
        try (LakeWriter<IcebergWriteResult> writer = createDvWriter(tablePath, tableInfo, bucket)) {
            for (LogRecord record : records) {
                writer.write(record);
            }
            results.add(writer.complete());
        }
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            committer.commit(
                    committer.toCommittable(results),
                    new CommitContext(
                            Collections.singletonMap("round", round), Collections.emptyMap()));
        }
    }

    private TablePath createDvTableAndWrite(String tableName) throws Exception {
        TablePath tablePath = TablePath.of("iceberg", tableName);
        Schema flussSchema =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .primaryKey("c1")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(flussSchema)
                        .distributedBy(BUCKET_NUM, "c1")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED, true)
                        .build();
        try (IcebergLakeCatalog catalog = new IcebergLakeCatalog(configuration)) {
            catalog.createTable(tablePath, descriptor, new TestingLakeCatalogContext());
        }
        TableInfo tableInfo =
                TableInfo.of(tablePath, 0, 1, descriptor, DEFAULT_REMOTE_DATA_DIR, 1L, 1L);

        List<IcebergWriteResult> writeResults = new ArrayList<>();
        try (LakeWriter<IcebergWriteResult> writer = createDvWriter(tablePath, tableInfo)) {
            writer.write(insert(0, 1, "a"));
            writer.write(insert(1, 2, "b"));
            writer.write(insert(2, 3, "c"));
            writeResults.add(writer.complete());
        }
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            committer.commit(
                    committer.toCommittable(writeResults),
                    new CommitContext(
                            Collections.singletonMap("round", "1"), Collections.emptyMap()));
        }
        return tablePath;
    }

    private static List<long[]> collectKeyAndPos(RecordReader reader, List<String> fileNames)
            throws Exception {
        List<long[]> out = new ArrayList<>();
        try (CloseableIterator<RowWithPosResult> it =
                ((SupportsRowPositions) reader).readWithPos()) {
            while (it.hasNext()) {
                RowWithPosResult rwp = it.next();
                out.add(new long[] {rwp.getRow().getInt(0), rwp.getPos()});
                fileNames.add(rwp.getFileName());
            }
        }
        return out;
    }

    private IcebergSplit currentSplit(Table table) throws Exception {
        table.refresh();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            return new IcebergSplit(tasks.iterator().next(), 0, Collections.emptyList());
        }
    }

    private LakeWriter<IcebergWriteResult> createDvWriter(TablePath tablePath, TableInfo tableInfo)
            throws Exception {
        return createDvWriter(tablePath, tableInfo, 0);
    }

    private LakeWriter<IcebergWriteResult> createDvWriter(
            TablePath tablePath, TableInfo tableInfo, int bucket) throws Exception {
        return tieringFactory.createLakeWriter(
                new WriterInitContext() {
                    @Override
                    public TablePath tablePath() {
                        return tablePath;
                    }

                    @Override
                    public TableBucket tableBucket() {
                        return new TableBucket(0, bucket);
                    }

                    @Nullable
                    @Override
                    public String partition() {
                        return null;
                    }

                    @Override
                    public TableInfo tableInfo() {
                        return tableInfo;
                    }

                    @Override
                    public int bucketCount() {
                        return tableInfo.getNumBuckets();
                    }
                });
    }

    private static LogRecord insert(long offset, int c1, String c2) {
        GenericRow row = new GenericRow(2);
        row.setField(0, c1);
        row.setField(1, BinaryString.fromString(c2));
        return new GenericRecord(offset, 1_000_000_000L + offset, ChangeType.INSERT, row);
    }

    /**
     * The files the plan's rewrite removed from bucket {@code bucket} of the unpartitioned table.
     */
    private static List<String> deletedFiles(DataDeltaPlan<?> plan, int bucket) {
        return plan.getDeletedFiles()
                .getOrDefault(null, Collections.emptyMap())
                .getOrDefault(bucket, Collections.emptyList());
    }
}
