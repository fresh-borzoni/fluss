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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.client.tiering.TieringCommitterInitContext;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.time.Duration;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Snapshot retention of a Paimon table with deletion vector masks never expires the readable
 * snapshot Fluss registered, which union reads plan on. Nor does it expire the one registered
 * before it, which reads planned before the registration still read. Older snapshots expire as
 * usual.
 */
class PaimonReadableSnapshotProtectionTest {

    private static final TablePath TABLE_PATH = TablePath.of("fluss", "derived_protected");

    @TempDir private File warehouse;

    private Configuration paimonConfig;
    private Catalog catalog;
    private FileStoreTable table;
    private TableInfo tableInfo;
    private int nextKey;

    @BeforeEach
    void setUp() throws Exception {
        paimonConfig = new Configuration();
        paimonConfig.setString("metastore", "filesystem");
        paimonConfig.setString("warehouse", warehouse.getAbsolutePath());
        catalog = new PaimonCatalogProvider(paimonConfig).get();
        catalog.createDatabase(TABLE_PATH.getDatabaseName(), true);
        Identifier identifier =
                Identifier.create(TABLE_PATH.getDatabaseName(), TABLE_PATH.getTableName());
        catalog.createTable(
                identifier,
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .primaryKey("c1")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                        .build(),
                false);
        table = (FileStoreTable) catalog.getTable(identifier);
        tableInfo =
                TableInfo.of(
                        TABLE_PATH,
                        7L,
                        1,
                        TableDescriptor.builder()
                                .schema(
                                        org.apache.fluss.metadata.Schema.newBuilder()
                                                .column(
                                                        "c1",
                                                        org.apache.fluss.types.DataTypes.INT())
                                                .column(
                                                        "c2",
                                                        org.apache.fluss.types.DataTypes.STRING())
                                                .primaryKey("c1")
                                                .build())
                                .distributedBy(1)
                                .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                                .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED, true)
                                .build(),
                        "/remote",
                        System.currentTimeMillis(),
                        System.currentTimeMillis());
    }

    @AfterEach
    void tearDown() throws Exception {
        catalog.close();
    }

    @Test
    void testRetentionKeepsTheLatestTwoReadableSnapshots() throws Exception {
        long first = commitRow();
        commitRow();
        long readable = commitRow();
        protect(readable);
        commitRow();
        commitRow();

        // a retention of one snapshot stops at the registered readable snapshot
        expireAllButLatest();
        assertThat(table.snapshotManager().earliestSnapshotId()).isEqualTo(readable);
        assertThat(table.snapshotManager().snapshotExists(first)).isFalse();

        // the next readable snapshot keeps the previous one, for reads planned on it
        long next = commitRow();
        protect(next);
        protect(next);
        long latest = commitRow();
        expireAllButLatest();
        assertThat(table.snapshotManager().earliestSnapshotId()).isEqualTo(readable);

        // once the readable snapshot moves on again, the older ones expire
        protect(latest);
        expireAllButLatest();
        assertThat(table.snapshotManager().earliestSnapshotId()).isEqualTo(next);
    }

    private void protect(long snapshotId) throws Exception {
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> committer =
                new PaimonLakeTieringFactory(paimonConfig)
                        .createLakeCommitter(
                                new TieringCommitterInitContext(
                                        TABLE_PATH,
                                        tableInfo,
                                        new Configuration(),
                                        new Configuration()))) {
            committer.protectReadableSnapshot(snapshotId);
        }
    }

    private void expireAllButLatest() {
        table.newExpireSnapshots()
                .config(
                        ExpireConfig.builder()
                                .snapshotRetainMax(1)
                                .snapshotRetainMin(1)
                                .snapshotTimeRetain(Duration.ZERO)
                                .build())
                .expire();
    }

    private long commitRow() throws Exception {
        FileStoreTable writeTable =
                table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));
        BatchWriteBuilder builder = writeTable.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            int key = nextKey++;
            write.write(GenericRow.of(key, BinaryString.fromString("v" + key)));
            commit.commit(write.prepareCommit());
        }
        return table.snapshotManager().latestSnapshotId();
    }
}
