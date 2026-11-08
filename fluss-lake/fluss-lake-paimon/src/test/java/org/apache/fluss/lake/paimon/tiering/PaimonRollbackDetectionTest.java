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
import org.apache.fluss.exception.LakeTableExternallyModifiedException;
import org.apache.fluss.exception.LakeTableRolledBackException;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.committer.SupportsDeletionVectorCommit;
import org.apache.fluss.lake.dv.FormatVersions;
import org.apache.fluss.lake.paimon.source.PaimonLakeSource;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.types.DataTypes;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.apache.fluss.lake.paimon.utils.PaimonTestUtils.CompactHelper;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A Paimon table with deletion vector masks rolled back outside Fluss stops its tiering. Paimon's
 * rollback deletes the later snapshots and reuses their ids, so the check tells the snapshot Fluss
 * registered from a later commit under the same id. It passes when the history that would tell is
 * gone.
 */
class PaimonRollbackDetectionTest {

    private static final long TABLE_ID = 7L;
    private static final TablePath TABLE_PATH = TablePath.of("fluss", "derived_rollback");
    private static final String TIERING_USER =
            LakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER + "-test";

    @TempDir private File warehouse;
    @TempDir private File compactionTempDir;

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
                        .option(CoreOptions.BUCKET.key(), "2")
                        .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                        .build(),
                false);
        table = (FileStoreTable) catalog.getTable(identifier);
        tableInfo =
                TableInfo.of(
                        TABLE_PATH,
                        TABLE_ID,
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
                                .distributedBy(2)
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
    void testRollbackToOlderSnapshotStopsTiering() throws Exception {
        long s1 = tieringCommit(null);
        long s2 = tieringCommit(s1);
        long s3 = tieringCommit(s2);
        assertDescends(s3);
        assertDescends(s2);

        // As Flink SQL's CALL sys.rollback_to('fluss.derived_rollback', s1) does.
        table.rollbackTo(s1);

        assertRolledBack(s3, "Its latest snapshot " + s1 + " is older than snapshot " + s3);
        assertRolledBack(s2, "Its latest snapshot " + s1 + " is older than snapshot " + s2);
        assertDescends(s1);
    }

    @Test
    void testSnapshotIdReusedAfterRollbackStopsTiering() throws Exception {
        long s1 = tieringCommit(null);
        long s2 = tieringCommit(s1);
        long s3 = tieringCommit(s2);
        table.rollbackTo(s1);
        // Another engine commits after the rollback: the ids of the deleted snapshots come back.
        assertThat(externalCommit()).isEqualTo(s2);
        assertThat(externalCommit()).isEqualTo(s3);
        assertThat(externalCommit()).isEqualTo(s3 + 1);

        assertRolledBack(s3, "the id was reused by the APPEND commit of 'external-engine'");
        assertRolledBack(s2, "the id was reused by the APPEND commit of 'external-engine'");
        assertDescends(s1);
    }

    @Test
    void testRoundCommittedOnRolledBackTableStopsTiering() throws Exception {
        long s1 = tieringCommit(null);
        long s2 = tieringCommit(s1);
        table.rollbackTo(s1);
        // A round that started before the rollback commits on top of it, based on s2.
        assertThat(tieringCommit(s2)).isEqualTo(s2);

        // The round's own check after its commit fails the same way, so it registers nothing.
        assertRolledBack(s2, "the id was reused by the APPEND commit of '" + TIERING_USER + "'");
    }

    @Test
    void testBaseOfANewerFormatIsRejected() throws Exception {
        long s1 = tieringCommit(null);
        long s2 = tieringCommit(s1, 2);

        assertThatThrownBy(() -> validator().validateDeletionVectorTable(tableInfo, s2))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Cannot read the deletion vector properties of lake snapshot "
                                + s2
                                + ": its format version 2 is unknown; this Fluss version reads"
                                + " format versions up to 1.");
        assertDescends(s1);
    }

    @Test
    void testUndoneRollbackResumesTiering() throws Exception {
        long s1 = tieringCommit(null);
        long s2 = tieringCommit(s1);
        // Paimon's rollback deletes later tags too; a branch keeps the snapshot.
        table.createTag("registered", s2);
        table.createBranch("keep", "registered");
        table.rollbackTo(s1);
        assertThat(externalCommit()).isEqualTo(s2);
        assertRolledBack(s2, "reused");

        // As Flink SQL's CALL sys.fast_forward('fluss.derived_rollback', 'keep') does.
        table.fastForward("keep");
        assertDescends(s2);
        long s3 = tieringCommit(s2);
        assertDescends(s2);
        assertDescends(s3);
    }

    @Test
    void testSnapshotsOfAnotherEngineAndExpiredOnesPass() throws Exception {
        // Fluss was enabled on a table another engine wrote: its first registered snapshot is not
        // a commit of the tiering.
        long e1 = externalCommit();
        long e2 = externalCommit();
        assertDescends(e1);
        assertDescends(e2);
        long s1 = tieringCommit(e2);
        long s2 = tieringCommit(s1);
        // Another engine compacts: a readable snapshot Fluss registers before the commit of the
        // round that found it.
        long compacted = compact();
        long s3 = tieringCommit(s2);
        assertDescends(s1);
        assertDescends(s2);
        assertDescends(compacted);
        assertDescends(s3);

        // Expired snapshots hide the history: they pass.
        table.newExpireSnapshots()
                .config(
                        ExpireConfig.builder()
                                .snapshotRetainMax(1)
                                .snapshotRetainMin(1)
                                .snapshotTimeRetain(Duration.ZERO)
                                .build())
                .expire();
        assertThat(table.snapshotManager().earliestSnapshotId()).isEqualTo(s3);
        assertDescends(s1);
        assertDescends(compacted);
        assertDescends(s3);
        // An expired index base is planned from scratch.
        assertThat(new PaimonLakeSource(paimonConfig, TABLE_PATH).planRewrite(compacted, null))
                .isNotNull();
    }

    @Test
    void testIndexPlanOfOlderSnapshotStops() throws Exception {
        long s1 = tieringCommit(null);
        compact();
        long s2 = tieringCommit(s1);
        long compacted = compact();
        table.rollbackTo(s2);

        PaimonLakeSource source = new PaimonLakeSource(paimonConfig, TABLE_PATH);
        assertThatThrownBy(() -> source.planRewrite(compacted, s2))
                .isInstanceOf(LakeTableRolledBackException.class)
                .hasMessageContaining("rolled back outside Fluss");
    }

    /**
     * Rows another engine appends after the snapshot Fluss registered stop the table's tiering,
     * also after Fluss committed on top. Rolling the write back resumes it.
     */
    @Test
    void testRowsAppendedByAnotherEngineStopTiering() throws Exception {
        long s1 = tieringCommit(null);
        long s2 = tieringCommit(s1);
        long write = commit("external-engine", committableOf(writeRow()));
        // A round that raced the write commits on top of it.
        tieringCommit(s2);
        assertExternalWrite(s2, write, "APPEND commit of 'external-engine'");

        // As Flink SQL's CALL sys.rollback_to('fluss.derived_rollback', s2) does.
        table.rollbackTo(write - 1);
        assertDescends(s2);
        assertDescends(tieringCommit(s2));
    }

    /**
     * An overwrite by another engine stops the tiering as well; compactions and empty commits do
     * not.
     */
    @Test
    void testOverwriteByAnotherEngineStopsTiering() throws Exception {
        long s1 = tieringCommit(null);
        compact();
        externalCommit();
        assertDescends(s1);
        long overwrite = overwriteRow();
        assertExternalWrite(s1, overwrite, "OVERWRITE commit of");
    }

    // ------------------------------------------------------------------------------------------

    private void assertExternalWrite(long registered, long write, String commit) {
        for (ThrowingCallable check :
                Arrays.<ThrowingCallable>asList(
                        () -> validator().validateDeletionVectorTable(tableInfo, registered),
                        () -> checkWithCommitter(registered))) {
            assertThatThrownBy(check)
                    .isInstanceOf(LakeTableExternallyModifiedException.class)
                    .isNotInstanceOf(LakeTableRolledBackException.class)
                    .hasMessageContaining("Tiering of fluss.derived_rollback stopped")
                    .hasMessageContaining("snapshot " + write + " of its Paimon table")
                    .hasMessageContaining(commit)
                    .hasMessageContaining("after snapshot " + registered)
                    .hasMessageContaining(
                            "CALL sys.rollback_to('fluss.derived_rollback', " + (write - 1) + ")");
        }
    }

    private static ManifestCommittable committableOf(List<CommitMessage> messages) {
        ManifestCommittable committable =
                new ManifestCommittable(BatchWriteBuilder.COMMIT_IDENTIFIER);
        messages.forEach(committable::addFileCommittable);
        return committable;
    }

    /** Overwrites the table with one row, as another engine's INSERT OVERWRITE does. */
    private long overwriteRow() throws Exception {
        FileStoreTable writeTable =
                table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));
        BatchWriteBuilder builder = writeTable.newBatchWriteBuilder().withOverwrite();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            int key = nextKey++;
            write.write(GenericRow.of(key, BinaryString.fromString("v" + key)));
            commit.commit(write.prepareCommit());
        }
        Snapshot latest = table.snapshotManager().latestSnapshot();
        assertThat(latest.commitKind()).isEqualTo(Snapshot.CommitKind.OVERWRITE);
        return latest.id();
    }

    private void assertDescends(long registered) throws Exception {
        validator().validateDeletionVectorTable(tableInfo, registered);
        checkWithCommitter(registered);
    }

    private void assertRolledBack(long registered, String detail) {
        assertThatThrownBy(() -> validator().validateDeletionVectorTable(tableInfo, registered))
                .isInstanceOf(LakeTableRolledBackException.class)
                .hasMessageContaining("Tiering of fluss.derived_rollback stopped")
                .hasMessageContaining("rolled back outside Fluss")
                .hasMessageContaining(detail)
                .hasMessageContaining(
                        "CALL sys.fast_forward('fluss.derived_rollback', '<branch>')");
        assertThatThrownBy(() -> checkWithCommitter(registered))
                .isInstanceOf(LakeTableRolledBackException.class)
                .hasMessageContaining(detail);
    }

    private PaimonLakeTieringFactory validator() {
        return new PaimonLakeTieringFactory(paimonConfig);
    }

    private void checkWithCommitter(long registered) throws Exception {
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> committer =
                new PaimonLakeTieringFactory(paimonConfig)
                        .createLakeCommitter(
                                new TieringCommitterInitContext(
                                        TABLE_PATH,
                                        tableInfo,
                                        new Configuration(),
                                        new Configuration()))) {
            ((SupportsDeletionVectorCommit) committer).checkCurrentSnapshotDescendsFrom(registered);
        }
    }

    /** A tiering round's commit of one row, based on the registered snapshot {@code base}. */
    private long tieringCommit(@Nullable Long base) throws Exception {
        return tieringCommit(base, FormatVersions.SNAPSHOT_PROPERTIES_VERSION);
    }

    /** Like {@link #tieringCommit(Long)}, with properties of format version {@code version}. */
    private long tieringCommit(@Nullable Long base, int version) throws Exception {
        String offsetsFile =
                "file:/remote/lake/fluss/derived_rollback-"
                        + TABLE_ID
                        + "/metadata/"
                        + UUID.randomUUID()
                        + ".offsets";
        ManifestCommittable committable =
                new ManifestCommittable(BatchWriteBuilder.COMMIT_IDENTIFIER);
        writeRow().forEach(committable::addFileCommittable);
        committable.addProperty(LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, offsetsFile);
        if (base != null) {
            committable.addProperty(
                    SupportsDeletionVectorCommit.FLUSS_LAKE_SNAP_BASE_PROPERTY, "" + base);
            committable.addProperty(FormatVersions.SNAPSHOT_FORMAT_VERSION_PROPERTY, "" + version);
        }
        return commit(TIERING_USER, committable);
    }

    /** An empty commit of another engine. */
    private long externalCommit() throws Exception {
        return commit(
                "external-engine", new ManifestCommittable(BatchWriteBuilder.COMMIT_IDENTIFIER));
    }

    private long compact() throws Exception {
        CompactHelper helper = new CompactHelper(table, compactionTempDir);
        for (int bucket = 0; bucket < 2; bucket++) {
            helper.compactBucket(bucket).commit();
        }
        Snapshot latest = table.snapshotManager().latestSnapshot();
        assertThat(latest.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
        return latest.id();
    }

    private List<CommitMessage> writeRow() throws Exception {
        FileStoreTable writeTable =
                table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));
        try (BatchTableWrite write = writeTable.newBatchWriteBuilder().newWrite()) {
            int key = nextKey++;
            write.write(GenericRow.of(key, BinaryString.fromString("v" + key)));
            return write.prepareCommit();
        }
    }

    private long commit(String user, ManifestCommittable committable) throws Exception {
        FileStoreTable writeTable =
                table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));
        try (TableCommitImpl commit = writeTable.newCommit(user)) {
            commit.ignoreEmptyCommit(false).commit(committable);
        }
        return table.snapshotManager().latestSnapshotId();
    }
}
