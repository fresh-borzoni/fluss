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
import org.apache.fluss.lake.committer.CommitterInitContext;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.committer.LakeCommitter.CommitContext;
import org.apache.fluss.lake.iceberg.maintenance.RewriteDataFileResult;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DataFileSet;
import org.apache.iceberg.util.DeleteFileSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the commits of Iceberg tables without deletion vectors to their behaviour before deletion
 * vectors: their commits never expire snapshots, also when the table expires them automatically,
 * and a compaction keeps the position delete files of the data files it replaces.
 */
class IcebergNonDvCommitTest {

    private static final TablePath TABLE_PATH = TablePath.of("iceberg", "non_dv_table");

    @TempDir private File tempWarehouseDir;

    private Configuration configuration;
    private Table table;
    private TableInfo tableInfo;

    @BeforeEach
    void beforeEach() {
        configuration = new Configuration();
        configuration.setString("warehouse", "file://" + tempWarehouseDir);
        configuration.setString("type", "hadoop");
        configuration.setString("name", "test");
        Catalog catalog = new IcebergCatalogProvider(configuration).get();
        ((org.apache.iceberg.catalog.SupportsNamespaces) catalog)
                .createNamespace(org.apache.iceberg.catalog.Namespace.of("iceberg"));
        Map<String, String> properties = new HashMap<>();
        properties.put(TableProperties.FORMAT_VERSION, "2");
        properties.put(TableProperties.MAX_SNAPSHOT_AGE_MS, "1");
        properties.put(TableProperties.MIN_SNAPSHOTS_TO_KEEP, "1");
        table =
                catalog.createTable(
                        toIceberg(TABLE_PATH),
                        new org.apache.iceberg.Schema(
                                Types.NestedField.required(1, "id", Types.IntegerType.get())),
                        PartitionSpec.unpartitioned(),
                        properties);
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .primaryKey("id")
                                        .build())
                        .distributedBy(1, "id")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DATALAKE_AUTO_EXPIRE_SNAPSHOT, true)
                        .build();
        tableInfo = TableInfo.of(TABLE_PATH, 1L, 1, descriptor, DEFAULT_REMOTE_DATA_DIR, 1L, 1L);
    }

    /** The commits of a table without deletion vectors never expire its snapshots. */
    @Test
    void testCommitsDoNotExpireSnapshots() throws Exception {
        IcebergLakeTieringFactory factory = new IcebergLakeTieringFactory(configuration, 0);
        Configuration lakeTieringConfig = new Configuration();
        lakeTieringConfig.set(ConfigOptions.LAKE_TIERING_AUTO_EXPIRE_SNAPSHOT, true);
        for (int round = 0; round < 3; round++) {
            try (LakeCommitter<IcebergWriteResult, IcebergCommittable> committer =
                    factory.createLakeCommitter(context(lakeTieringConfig))) {
                committer.commit(
                        IcebergCommittable.builder()
                                .addSpecs(table.specs())
                                .addDataFile(dataFile())
                                .build(),
                        new CommitContext(Collections.emptyMap(), Collections.emptyMap()));
            }
            Thread.sleep(5);
        }
        table.refresh();
        assertThat(Iterables.size(table.snapshots())).isEqualTo(3);
    }

    /**
     * A compaction of a table without deletion vectors replaces data files and leaves their
     * position delete files in the table.
     */
    @Test
    void testCompactionKeepsThePositionDeletesOfReplacedFiles() throws Exception {
        DataFile replaced = dataFile();
        // a file older than the delete file stays, so Iceberg keeps the delete file applicable
        table.newAppend().appendFile(replaced).appendFile(dataFile()).commit();
        DeleteFile positionDeletes =
                FileMetadata.deleteFileBuilder(table.spec())
                        .ofPositionDeletes()
                        .withPath(
                                tempWarehouseDir
                                        + "/data/"
                                        + UUID.randomUUID()
                                        + "-deletes.parquet")
                        .withFileSizeInBytes(10)
                        .withRecordCount(1)
                        .withReferencedDataFile(replaced.location())
                        .build();
        table.newRowDelta().addDeletes(positionDeletes).commit();
        table.refresh();
        assertThat(liveDeleteFiles(table))
                .extracting(DeleteFile::location)
                .containsExactly(positionDeletes.location());
        long compacted = table.currentSnapshot().snapshotId();
        DataFileSet deletedDataFiles = DataFileSet.create();
        deletedDataFiles.add(replaced);
        DataFileSet addedDataFiles = DataFileSet.create();
        addedDataFiles.add(dataFile());
        DeleteFileSet deletedDeleteFiles = DeleteFileSet.create();
        deletedDeleteFiles.add(positionDeletes);
        RewriteDataFileResult rewrite =
                new RewriteDataFileResult(
                        compacted,
                        deletedDataFiles,
                        addedDataFiles,
                        deletedDeleteFiles,
                        null,
                        null);

        IcebergLakeTieringFactory factory = new IcebergLakeTieringFactory(configuration);
        try (LakeCommitter<IcebergWriteResult, IcebergCommittable> committer =
                factory.createLakeCommitter(context(new Configuration()))) {
            committer.commit(
                    IcebergCommittable.builder()
                            .addSpecs(table.specs())
                            .addDataFile(dataFile())
                            .addRewriteDataFileResult(rewrite)
                            .build(),
                    new CommitContext(Collections.emptyMap(), Collections.emptyMap()));
        }

        table.refresh();
        assertThat(table.currentSnapshot().operation()).isEqualTo("replace");
        assertThat(liveDeleteFiles(table))
                .extracting(DeleteFile::location)
                .containsExactly(positionDeletes.location());
    }

    private DataFile dataFile() {
        return DataFiles.builder(table.spec())
                .withPath(tempWarehouseDir + "/data/" + UUID.randomUUID() + ".parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .build();
    }

    private CommitterInitContext context(Configuration lakeTieringConfig) {
        return new CommitterInitContext() {
            @Override
            public TablePath tablePath() {
                return TABLE_PATH;
            }

            @Override
            public TableInfo tableInfo() {
                return tableInfo;
            }

            @Override
            public Configuration lakeTieringConfig() {
                return lakeTieringConfig;
            }

            @Override
            public Configuration flussClientConfig() {
                return new Configuration();
            }
        };
    }

    private static List<DeleteFile> liveDeleteFiles(Table table) throws IOException {
        List<DeleteFile> deletes = new ArrayList<>();
        for (ManifestFile manifest : table.currentSnapshot().deleteManifests(table.io())) {
            try (ManifestReader<DeleteFile> reader =
                    ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
                for (DeleteFile delete : reader) {
                    deletes.add(delete.copy());
                }
            }
        }
        return deletes;
    }
}
