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

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
