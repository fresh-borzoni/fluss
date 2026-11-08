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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Aborting a Paimon commit deletes the files it wrote. A file its compaction only moved to a higher
 * level keeps its name and is still in the table, so it is kept.
 */
class PaimonAbortTest {

    private static final TablePath TABLE_PATH = TablePath.of("fluss", "abort_upgrade");

    @TempDir private File warehouse;

    private Configuration paimonConfig;
    private Catalog catalog;
    private FileStoreTable table;
    private TableInfo tableInfo;

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
    void testAbortKeepsTheFilesACompactionOnlyUpgraded() throws Exception {
        commit(writeRow(1));
        DataFileMeta live = liveFiles().get(0);
        File liveFile = fileOf(live);
        // a file the compaction wrote: the output of a rewrite that never landed
        List<CommitMessage> rewrite = writeRow(2);
        DataFileMeta written =
                ((CommitMessageImpl) rewrite.get(0)).newFilesIncrement().newFiles().get(0);
        File writtenFile = fileOf(written);
        CommitMessage compaction =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        1,
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.singletonList(live),
                                Arrays.asList(live.upgrade(live.level() + 1), written),
                                Collections.emptyList()));
        assertThat(liveFile).exists();
        assertThat(writtenFile).exists();

        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> committer = committer()) {
            ManifestCommittable committable =
                    new ManifestCommittable(BatchWriteBuilder.COMMIT_IDENTIFIER);
            committable.addFileCommittable(compaction);
            committer.abort(new PaimonCommittable(committable));
        }

        assertThat(writtenFile).doesNotExist();
        // still in the table and on disk
        assertThat(liveFiles().get(0).fileName()).isEqualTo(live.fileName());
        assertThat(liveFile).exists();
    }

    private LakeCommitter<PaimonWriteResult, PaimonCommittable> committer() throws Exception {
        return new PaimonLakeTieringFactory(paimonConfig)
                .createLakeCommitter(
                        new TieringCommitterInitContext(
                                TABLE_PATH, tableInfo, new Configuration(), new Configuration()));
    }

    private List<CommitMessage> writeRow(int key) throws Exception {
        FileStoreTable writeTable =
                table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));
        try (BatchTableWrite write = writeTable.newBatchWriteBuilder().newWrite()) {
            write.write(GenericRow.of(key, BinaryString.fromString("v" + key)));
            return write.prepareCommit();
        }
    }

    private void commit(List<CommitMessage> messages) throws Exception {
        FileStoreTable writeTable =
                table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));
        try (TableCommitImpl commit = writeTable.newCommit("writer")) {
            commit.commit(messages);
        }
    }

    private List<DataFileMeta> liveFiles() {
        List<DataFileMeta> files = new ArrayList<>();
        table.newSnapshotReader().read().dataSplits().forEach(s -> files.addAll(s.dataFiles()));
        assertThat(files).hasSize(1);
        return files;
    }

    private File fileOf(DataFileMeta file) {
        return new File(
                table.store()
                        .pathFactory()
                        .createDataFilePathFactory(BinaryRow.EMPTY_ROW, 0)
                        .toPath(file)
                        .toUri()
                        .getPath());
    }
}
