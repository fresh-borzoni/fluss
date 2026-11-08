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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.committer.LakeCommitter.CommitContext;
import org.apache.fluss.lake.iceberg.IcebergLakeCatalog;
import org.apache.fluss.lake.lakestorage.TestingLakeCatalogContext;
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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
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

/** Tests that the deletes a round commits are Iceberg v3 Puffin deletion vectors read back. */
class IcebergDeletionVectorTest {

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
    void puffinDeletionVectorMasksRowOnRead() throws Exception {
        TablePath tablePath = TablePath.of("iceberg", "dv_roundtrip_table");

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

        long baseSnapshotId;
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            IcebergCommittable committable = committer.toCommittable(writeResults);
            baseSnapshotId =
                    committer
                            .commit(
                                    committable,
                                    new CommitContext(
                                            Collections.singletonMap("round", "1"),
                                            Collections.emptyMap()))
                            .getCommittedSnapshotId();
        }

        Table table = catalogProvider.get().loadTable(toIceberg(tablePath));
        table.refresh();
        assertThat(countRows(table)).isEqualTo(3);

        DataFile dataFile = table.currentSnapshot().addedDataFiles(table.io()).iterator().next();
        String location = dataFile.location();
        String fileName = location.substring(location.lastIndexOf('/') + 1);
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            committer.commitWithDeletes(
                    committer.toCommittable(Collections.emptyList()),
                    Collections.singletonMap("round", "2"),
                    Collections.singletonMap(fileName, new long[] {0L}),
                    baseSnapshotId);
        }

        table.refresh();
        assertThat(countRows(table)).isEqualTo(2);
        List<DeleteFile> deletes = new ArrayList<>();
        table.currentSnapshot().addedDeleteFiles(table.io()).forEach(deletes::add);
        assertThat(deletes).hasSize(1);
        assertThat(deletes.get(0).format()).isEqualTo(FileFormat.PUFFIN);

        // a later round deleting another row of the file supersedes the deletion vector
        long secondBase = table.currentSnapshot().snapshotId();
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            committer.commitWithDeletes(
                    committer.toCommittable(Collections.emptyList()),
                    Collections.singletonMap("round", "3"),
                    Collections.singletonMap(fileName, new long[] {1L}),
                    secondBase);
        }
        table.refresh();
        assertThat(countRows(table)).isEqualTo(1);
        int liveDeleteFiles = 0;
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                liveDeleteFiles += task.deletes().size();
            }
        }
        assertThat(liveDeleteFiles).isEqualTo(1);
    }

    private static int countRows(Table table) throws Exception {
        int count = 0;
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record ignored : records) {
                count++;
            }
        }
        return count;
    }

    private LakeWriter<IcebergWriteResult> createDvWriter(TablePath tablePath, TableInfo tableInfo)
            throws Exception {
        return tieringFactory.createLakeWriter(
                new WriterInitContext() {
                    @Override
                    public TablePath tablePath() {
                        return tablePath;
                    }

                    @Override
                    public TableBucket tableBucket() {
                        return new TableBucket(0, 0);
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
}
