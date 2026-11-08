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
import org.apache.fluss.exception.LakeTableExternallyModifiedException;
import org.apache.fluss.exception.LakeTableRolledBackException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.LakeBaseChangedException;
import org.apache.fluss.lake.committer.LakeCommitRejectedException;
import org.apache.fluss.lake.committer.LakeCommitter.CommitContext;
import org.apache.fluss.lake.committer.SupportsDeletionVectorCommit;
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.FormatVersions;
import org.apache.fluss.lake.dv.RowPosSstIndex;
import org.apache.fluss.lake.dv.index.CompactionIndexPart;
import org.apache.fluss.lake.dv.index.FlussPositionIndexStore;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.lake.iceberg.IcebergLakeCatalog;
import org.apache.fluss.lake.iceberg.maintenance.CompactionIndexWriter;
import org.apache.fluss.lake.iceberg.maintenance.IcebergRewriteDataFiles;
import org.apache.fluss.lake.iceberg.maintenance.RewriteDataFileResult;
import org.apache.fluss.lake.iceberg.source.IcebergLakeSource;
import org.apache.fluss.lake.iceberg.source.IcebergSplit;
import org.apache.fluss.lake.iceberg.testutils.CommitHookCatalog;
import org.apache.fluss.lake.iceberg.testutils.ExternalWrites;
import org.apache.fluss.lake.iceberg.tiering.writer.TaskWriterFactory;
import org.apache.fluss.lake.lakestorage.TestingLakeCatalogContext;
import org.apache.fluss.lake.source.DataDeltaPlan;
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
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataTypes;

import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.ReadOptions;
import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.SstFileReader;
import io.github.fluss_contrib.rocksdb.SstFileReaderIterator;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenericReader;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.util.ContentFileUtil;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the failure handling, the rollback check and the dangling delete cleanup of {@link
 * IcebergLakeCommitter}'s commits of rounds with deletion vectors.
 */
class IcebergDerivedCommitTest {

    private static final String FAIL_COMMIT = "fail-commit";
    // a snapshot property that makes FailingCatalog fail the commit before it applies it
    private static final String FAIL_BEFORE_COMMIT = "fail-before-commit";
    private static final String ROUND_PROPERTY = "test-round";
    private static final String SKEW_MS = "test-skew-ms";

    @TempDir private File tempWarehouseDir;

    private Configuration configuration;
    private IcebergLakeTieringFactory tieringFactory;
    private IcebergCatalogProvider catalogProvider;
    private TablePath tablePath;
    private TableInfo tableInfo;

    @BeforeEach
    void beforeEach() {
        configuration = new Configuration();
        configuration.setString("warehouse", "file://" + tempWarehouseDir);
        configuration.setString("type", "hadoop");
        configuration.setString("name", "test");
        tieringFactory = new IcebergLakeTieringFactory(configuration);
        catalogProvider = new IcebergCatalogProvider(configuration);
    }

    /** A commit rejected by a conflicting write leaves none of the delete files it wrote. */
    @Test
    void testRejectedCommitDeletesItsDeleteFiles() throws Exception {
        Table table = createTable(3);
        long base = commitRows(0, 3);
        String fileName = onlyDataFileName(table);
        // Another engine appends a data file after the base, which the round must not miss.
        table.newAppend().appendFile(writeExternalDataFile(table, 100)).commit();
        Set<String> filesBefore = filesOnDisk();
        Snapshot snapshotBefore = refreshed(table).currentSnapshot();

        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            assertThatThrownBy(
                            () ->
                                    committer.commitWithDeletes(
                                            committer.toCommittable(Collections.emptyList()),
                                            Collections.emptyMap(),
                                            Collections.singletonMap(fileName, new long[] {0L}),
                                            base))
                    .isInstanceOf(LakeCommitRejectedException.class)
                    .hasMessageContaining("on base snapshot " + base);
        }

        assertThat(refreshed(table).currentSnapshot().snapshotId())
                .isEqualTo(snapshotBefore.snapshotId());
        // The deletion vector file written for the commit is gone again.
        assertThat(filesOnDisk()).isEqualTo(filesBefore);
    }

    /**
     * A round whose index has rows in a data file the base does not have is not committed. Leaving
     * their deletes out would leave the old rows live next to the round's new ones.
     */
    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testDeletesInAFileTheBaseDoesNotHaveFailTheRound(int formatVersion) throws Exception {
        Table table = createTable(formatVersion);
        long base = commitRows(0, 3);
        Map<String, long[]> deletes = new HashMap<>();
        deletes.put(onlyDataFileName(table), new long[] {0L});
        deletes.put("not-in-the-base.parquet", new long[] {1L});
        Set<String> filesBefore = filesOnDisk();
        Snapshot snapshotBefore = refreshed(table).currentSnapshot();

        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            assertThatThrownBy(
                            () ->
                                    committer.commitWithDeletes(
                                            committer.toCommittable(Collections.emptyList()),
                                            Collections.emptyMap(),
                                            deletes,
                                            base))
                    .isInstanceOf(LakeCommitRejectedException.class)
                    .hasStackTraceContaining("not-in-the-base.parquet");
        }

        assertThat(refreshed(table).currentSnapshot().snapshotId())
                .isEqualTo(snapshotBefore.snapshotId());
        assertThat(filesOnDisk()).isEqualTo(filesBefore);
    }

    /**
     * A rewrite by another engine, committed while a round commits, of a file the round's deletes
     * do not reference rejects the round's commit, which leaves none of its delete files.
     *
     * <p>Iceberg's validations let the rewrite pass, and its retry would rebase the round on it.
     * The round's index change is relative to the base, so it would miss the rewrite.
     */
    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testRewriteDuringCommitRejectsIt(int formatVersion) throws Exception {
        Table table = createTable(formatVersion);
        long first = commitRows(0, 3);
        long base = commitRows(3, 3);
        String fileOfFirst = addedDataFile(refreshed(table), first);
        String fileOfBase = addedDataFile(table, base);
        long[] rewrite = new long[1];
        CommitHookCatalog.armOnce(
                "iceberg." + tablePath.getTableName(),
                ROUND_PROPERTY,
                (b, pending, io) -> rewrite[0] = rewriteDataFile(loadTable(), fileOfBase));
        Set<String> filesBefore = filesOnDisk();

        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(hookCatalogProvider(), tablePath)) {
            assertThatThrownBy(
                            () ->
                                    committer.commitWithDeletes(
                                            committer.toCommittable(Collections.emptyList()),
                                            Collections.singletonMap(ROUND_PROPERTY, "1"),
                                            Collections.singletonMap(
                                                    fileName(fileOfFirst), new long[] {0L}),
                                            base))
                    .isInstanceOf(LakeBaseChangedException.class)
                    .hasMessageContaining("on base snapshot " + base)
                    .rootCause()
                    .hasMessageContaining(
                            "would follow snapshot "
                                    + rewrite[0]
                                    + " (replace) rather than its base snapshot "
                                    + base);
        } finally {
            CommitHookCatalog.disarm("iceberg." + tablePath.getTableName());
        }

        // The rewrite is the current snapshot, with nothing of the round on top of it.
        Snapshot current = refreshed(table).currentSnapshot();
        assertThat(current.snapshotId()).isEqualTo(rewrite[0]);
        assertThat(current.parentId()).isEqualTo(base);
        Set<String> added = new HashSet<>(filesOnDisk());
        added.removeAll(filesBefore);
        // Only the rewrite's data file is new: the round's delete file is gone again.
        assertThat(filesOf(added, "/data/"))
                .containsExactly(fileName(addedDataFile(refreshed(table), current.snapshotId())));
        assertThat(countRows(table)).isEqualTo(6);
    }

    /**
     * A rewrite that changes no data file, e.g. of the manifests, leaves the positions the round's
     * deletes and index change are relative to as they are, so the round commits on top of it.
     */
    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testRewriteOfManifestsDuringCommitIsAllowed(int formatVersion) throws Exception {
        Table table = createTable(formatVersion);
        long first = commitRows(0, 3);
        long base = commitRows(3, 3);
        String fileOfFirst = addedDataFile(refreshed(table), first);
        long[] rewrite = new long[1];
        CommitHookCatalog.armOnce(
                "iceberg." + tablePath.getTableName(),
                ROUND_PROPERTY,
                (b, pending, io) -> {
                    Table t = loadTable();
                    t.rewriteManifests().rewriteIf(m -> true).commit();
                    t.refresh();
                    rewrite[0] = t.currentSnapshot().snapshotId();
                });

        long committed;
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(hookCatalogProvider(), tablePath)) {
            committed =
                    committer
                            .commitWithDeletes(
                                    committer.toCommittable(Collections.emptyList()),
                                    Collections.singletonMap(ROUND_PROPERTY, "1"),
                                    Collections.singletonMap(
                                            fileName(fileOfFirst), new long[] {0L}),
                                    base)
                            .getCommittedSnapshotId();
        } finally {
            CommitHookCatalog.disarm("iceberg." + tablePath.getTableName());
        }

        Snapshot rewriteOfManifests = refreshed(table).snapshot(rewrite[0]);
        assertThat(rewriteOfManifests.operation()).isEqualTo("replace");
        assertThat(rewriteOfManifests.parentId()).isEqualTo(base);
        assertThat(table.currentSnapshot().snapshotId()).isEqualTo(committed);
        assertThat(table.currentSnapshot().parentId()).isEqualTo(rewrite[0]);
        assertThat(countRows(table)).isEqualTo(5);
    }

    /** A commit whose outcome is unknown keeps its delete files: the snapshot may be live. */
    @Test
    void testCommitWithUnknownOutcomeKeepsItsDeleteFiles() throws Exception {
        Table table = createTable(3);
        long base = commitRows(0, 3);
        String fileName = onlyDataFileName(table);
        Set<String> filesBefore = filesOnDisk();

        // The catalog applies the commit, but the response is lost.
        Catalog failingCatalog = new FailingCatalog();
        IcebergCatalogProvider failingProvider =
                new IcebergCatalogProvider(configuration) {
                    @Override
                    public Catalog get() {
                        return failingCatalog;
                    }
                };
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(failingProvider, tablePath)) {
            assertThatThrownBy(
                            () ->
                                    committer.commitWithDeletes(
                                            committer.toCommittable(Collections.emptyList()),
                                            Collections.singletonMap(FAIL_COMMIT, "true"),
                                            Collections.singletonMap(fileName, new long[] {0L}),
                                            base))
                    .isInstanceOf(IOException.class)
                    .isNotInstanceOf(LakeCommitRejectedException.class)
                    .hasCauseInstanceOf(CommitStateUnknownException.class);
        }

        Set<String> added = new HashSet<>(filesOnDisk());
        added.removeAll(filesBefore);
        Set<String> dataFiles = filesOf(added, "/data/");
        assertThat(dataFiles).hasSize(1);
        // The live snapshot references the kept deletion vector.
        List<DeleteFile> deletes = new ArrayList<>();
        refreshed(table).currentSnapshot().addedDeleteFiles(table.io()).forEach(deletes::add);
        assertThat(deletes).hasSize(1);
        assertThat(dataFiles).allMatch(f -> deletes.get(0).location().endsWith(f));
        assertThat(countRows(table)).isEqualTo(2);
    }

    /**
     * A commit whose outcome is unknown keeps the delete files it wrote, and its committable does
     * not list them: neither a commit nor an abort of the committable touches them.
     */
    @Test
    void testCommitWithUnknownOutcomeLeavesItsDeleteFilesOutOfTheCommittable() throws Exception {
        Table table = createTable(3);
        long base = commitRows(0, 3);
        String fileName = onlyDataFileName(table);
        Set<String> filesBefore = filesOnDisk();

        // The request is lost before the catalog applies the commit.
        Catalog failingCatalog = new FailingCatalog();
        IcebergCatalogProvider failingProvider =
                new IcebergCatalogProvider(configuration) {
                    @Override
                    public Catalog get() {
                        return failingCatalog;
                    }
                };
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(failingProvider, tablePath)) {
            IcebergCommittable committable = committer.toCommittable(Collections.emptyList());
            assertThatThrownBy(
                            () ->
                                    committer.commitWithDeletes(
                                            committable,
                                            Collections.singletonMap(FAIL_BEFORE_COMMIT, "true"),
                                            Collections.singletonMap(fileName, new long[] {0L}),
                                            base))
                    .isInstanceOf(IOException.class)
                    .isNotInstanceOf(LakeCommitRejectedException.class)
                    .hasCauseInstanceOf(CommitStateUnknownException.class);
            assertThat(refreshed(table).currentSnapshot().snapshotId()).isEqualTo(base);
            assertThat(committable.getDeleteFiles()).isEmpty();

            committer.abort(committable);
        }

        // the deletion vector the commit wrote stays, as its snapshot may still land
        Set<String> added = new HashSet<>(filesOnDisk());
        added.removeAll(filesBefore);
        assertThat(filesOf(added, "/data/")).hasSize(1);
        assertThat(countRows(table)).isEqualTo(3);
    }

    /**
     * The lineage check finds a rollback past the snapshot Fluss registered last, also after
     * another commit on top of it, and passes once the rollback is undone.
     */
    @Test
    void testRollbackIsFound() throws Exception {
        Table table = createTable(3);
        long first = commitRows(0, 3);
        long second = commitRows(3, 3);
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            committer.checkCurrentSnapshotDescendsFrom(first);
            committer.checkCurrentSnapshotDescendsFrom(second);

            // As Spark's rollback_to_snapshot does.
            table.manageSnapshots().rollbackTo(first).commit();
            committer.checkCurrentSnapshotDescendsFrom(first);
            assertThatThrownBy(() -> committer.checkCurrentSnapshotDescendsFrom(second))
                    .isInstanceOf(LakeTableRolledBackException.class)
                    .hasMessageContaining("rolled back outside Fluss")
                    .hasMessageContaining("current snapshot " + first)
                    .hasMessageContaining("is not snapshot " + second)
                    .hasMessageContaining(
                            "set_current_snapshot('iceberg.derived_commit_v3', " + second + ")");

            // Another engine commits on top of the rolled-back snapshot.
            refreshed(table).newAppend().appendFile(writeExternalDataFile(table, 100)).commit();
            assertThatThrownBy(() -> committer.checkCurrentSnapshotDescendsFrom(second))
                    .isInstanceOf(LakeTableRolledBackException.class);

            // Undone: the registered snapshot is current again, then a descendant of it.
            refreshed(table).manageSnapshots().setCurrentSnapshot(second).commit();
            committer.checkCurrentSnapshotDescendsFrom(second);
            commitRows(6, 1);
            committer.checkCurrentSnapshotDescendsFrom(second);
        }
        // The tiering source validates the same before writing any data.
        tieringFactory.validateDeletionVectorTable(tableInfo, second);
        refreshed(table).manageSnapshots().rollbackTo(first).commit();
        assertThatThrownBy(() -> tieringFactory.validateDeletionVectorTable(tableInfo, second))
                .isInstanceOf(LakeTableRolledBackException.class);
    }

    /**
     * A write by another engine after the snapshot Fluss registered (an INSERT, a DELETE or a
     * copy-on-write UPDATE) stops the table's tiering, also after Fluss committed on top. Undoing
     * the write resumes it.
     */
    @ParameterizedTest
    @CsvSource({"2, append", "3, append", "2, delete", "3, delete", "2, overwrite", "3, overwrite"})
    void testRowsWrittenByAnotherEngineAreFound(int formatVersion, String operation)
            throws Exception {
        Table table = createTable(formatVersion);
        long registered = commitRows(0, 3);
        long write = ExternalWrites.write(refreshed(table), operation);
        String lakeTable = "iceberg." + tablePath.getTableName();
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            assertExternalWrite(committer, registered, write, operation, lakeTable);
            // A commit of Fluss on top does not hide the write.
            commitRows(3, 1);
            assertExternalWrite(committer, registered, write, operation, lakeTable);

            // Undone: the snapshot before the write is current again.
            refreshed(table).manageSnapshots().setCurrentSnapshot(registered).commit();
            committer.checkCurrentSnapshotDescendsFrom(registered);
            tieringFactory.validateDeletionVectorTable(tableInfo, registered);
            commitRows(4, 1);
            committer.checkCurrentSnapshotDescendsFrom(registered);
        }
    }

    /** Rewrites by another engine keep the rows, so tiering goes on. */
    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testRewritesByAnotherEnginePass(int formatVersion) throws Exception {
        Table table = createTable(formatVersion);
        commitRows(0, 3);
        long registered = commitRows(3, 3);
        rewriteDataFile(refreshed(table), addedDataFile(table, registered));
        refreshed(table).rewriteManifests().rewriteIf(m -> true).commit();
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            committer.checkCurrentSnapshotDescendsFrom(registered);
        }
        tieringFactory.validateDeletionVectorTable(tableInfo, registered);
    }

    private void assertExternalWrite(
            IcebergLakeCommitter committer,
            long registered,
            long write,
            String operation,
            String lakeTable) {
        for (ThrowingCallable check :
                Arrays.<ThrowingCallable>asList(
                        () -> committer.checkCurrentSnapshotDescendsFrom(registered),
                        () -> tieringFactory.validateDeletionVectorTable(tableInfo, registered))) {
            assertThatThrownBy(check)
                    .isInstanceOf(LakeTableExternallyModifiedException.class)
                    .isNotInstanceOf(LakeTableRolledBackException.class)
                    .hasMessageContaining(
                            "snapshot "
                                    + write
                                    + " of its lake table changed rows outside Fluss (operation '"
                                    + operation
                                    + "'")
                    .hasMessageContaining("after snapshot " + registered)
                    .hasMessageContaining(
                            "set_current_snapshot('" + lakeTable + "', " + registered + ")");
        }
    }

    /**
     * Fluss's compaction of a bucket hands the positions of the rows it rewrote to the index. Its
     * index change maps every key that survived the deletes to its row in the new files, with keys
     * encoded as the re-index encodes them, and drops the replaced files.
     */
    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testCompactionHandsItsPositionsToTheIndex(int formatVersion) throws Exception {
        Table table = createTable(formatVersion);
        List<String> inputs = new ArrayList<>();
        for (int from = 0; from < 9; from += 3) {
            long snapshotId = commitRows(from, 3);
            inputs.add(addedDataFile(refreshed(table), snapshotId));
        }
        long third = table.currentSnapshot().snapshotId();
        // keys 1 and 7 are deleted before the compaction, by a deletion vector or position deletes
        long read;
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            Map<String, long[]> deletes = new HashMap<>();
            deletes.put(fileName(inputs.get(0)), new long[] {1L});
            deletes.put(fileName(inputs.get(2)), new long[] {1L});
            read =
                    committer
                            .commitWithDeletes(
                                    committer.toCommittable(Collections.emptyList()),
                                    Collections.emptyMap(),
                                    deletes,
                                    third)
                            .getCommittedSnapshotId();
        }

        FlussPositionIndexStore store =
                new FlussPositionIndexStore(
                        new FsPath(new File(tempWarehouseDir, "index").toURI()));
        TableBucket bucket = new TableBucket(tableInfo.getTableId(), 0);
        long roundTimestamp = 1234L;
        RewriteDataFileResult result =
                new IcebergRewriteDataFiles(refreshed(table), null, bucket)
                        .handOffPositions(
                                new CompactionIndexWriter(
                                        store, tableInfo, bucket, roundTimestamp, null))
                        .execute();
        assertThat(result).isNotNull();
        CompactionIndexPart part = result.indexPart();
        assertThat(part).isNotNull();
        // Compactions of different buckets run concurrently, so they leave the table's file-id
        // counter to the committer, which gives the part's files their ids when it publishes it.
        assertThat(new File(tempWarehouseDir, "index/rowPos/_next_file_id")).doesNotExist();
        assertThat(part.readSnapshotId()).isEqualTo(read);
        assertThat(part.changeId())
                .isEqualTo(
                        CompactionIndexPart.changeIdOf(
                                tableInfo.getTableId(), roundTimestamp, read));
        assertThat(part.removedFiles())
                .containsExactlyInAnyOrderElementsOf(
                        inputs.stream().map(f -> fileName(f)).collect(Collectors.toList()));
        List<DataFile> outputs = new ArrayList<>();
        result.addedDataFiles().forEach(outputs::add);
        assertThat(part.addedFiles().values())
                .containsExactlyInAnyOrderElementsOf(
                        outputs.stream()
                                .map(f -> fileName(f.location()))
                                .collect(Collectors.toList()));

        Map<String, String> indexed = handedOver(store, bucket, part);

        // The rewrite commits on the snapshot it read.
        long rewrite;
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            rewrite =
                    committer.commitPendingRewrite(
                            committer.toCommittable(
                                    Collections.singletonList(
                                            new IcebergWriteResult(
                                                    WriteResult.builder().build(),
                                                    table.specs(),
                                                    result))));
        }
        assertThat(refreshed(table).snapshot(rewrite).parentId()).isEqualTo(read);
        assertThat(lineage(table).keySet()).containsExactly(0, 2, 3, 4, 5, 6, 8);
        assertThat(indexed).isEqualTo(reindexed(table));
    }

    /**
     * A hand-off that holds fewer keys in memory than the compaction writes spills them in sorted
     * runs and merges them into what a re-index of the new files finds. No run is left behind.
     */
    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testCompactionHandOffSpillsSortedRuns(int formatVersion) throws Exception {
        Table table = createTable(formatVersion);
        // every file spans the key range, so the sorted runs overlap
        for (int i = 0; i < 8; i++) {
            commitKeys(i, i + 8, i + 16);
        }
        FlussPositionIndexStore store =
                new FlussPositionIndexStore(
                        new FsPath(new File(tempWarehouseDir, "index").toURI()));
        TableBucket bucket = new TableBucket(tableInfo.getTableId(), 0);
        File sortDir = new File(tempWarehouseDir, "sort");
        List<Integer> runsAfterGroup = new ArrayList<>();
        // at a 4 KB target the test's files are medium: two groups of four, of 12 rows each
        RewriteDataFileResult result =
                new IcebergRewriteDataFiles(refreshed(table), null, bucket) {
                    @Override
                    protected void groupCompleted(int groupIndex) {
                        runsAfterGroup.add(filesUnder(sortDir).size());
                    }
                }.targetSizeInBytes(4096)
                        .tiered(3, Duration.ZERO)
                        .handOffPositions(
                                new CompactionIndexWriter(
                                        store, tableInfo, bucket, 1234L, sortDir, 5))
                        .execute();
        assertThat(result).isNotNull();
        assertThat(result.deletedDataFiles()).hasSize(8);
        assertThat(runsAfterGroup).containsExactly(2, 4);
        assertThat(filesUnder(sortDir)).isEmpty();
        CompactionIndexPart part = result.indexPart();
        assertThat(part).isNotNull();
        Map<String, String> indexed = handedOver(store, bucket, part);

        commitRewrite(result);
        assertThat(lineage(refreshed(table)).keySet()).hasSize(24);
        assertThat(indexed).isEqualTo(reindexed(table));
    }

    /**
     * A compaction stopped after its first file group keeps that group: the round commits it, the
     * other files stay as they are, and nothing of the stopped group is left behind.
     */
    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testStoppedCompactionKeepsTheFileGroupsItFinished(int formatVersion) throws Exception {
        Table table = createTable(formatVersion);
        for (int i = 0; i < 8; i++) {
            commitRows(3 * i, 3);
        }
        TableBucket bucket = new TableBucket(tableInfo.getTableId(), 0);
        // at a 4 KB target the test's files are medium: two groups of four
        IcebergRewriteDataFiles stopping =
                new IcebergRewriteDataFiles(refreshed(table), null, bucket) {
                    @Override
                    protected void groupCompleted(int groupIndex) {
                        requestStop();
                    }
                }.targetSizeInBytes(4096).tiered(3, Duration.ZERO);
        Set<String> filesBefore = filesOnDisk();
        RewriteDataFileResult result = stopping.execute();
        assertThat(result).isNotNull();
        assertThat(result.deletedDataFiles()).hasSize(4);
        assertThat(result.addedDataFiles()).hasSize(1);
        assertThat(result.bucketKey()).isEqualTo(IcebergRewriteDataFiles.bucketKey(bucket));
        Set<String> added = new HashSet<>(filesOnDisk());
        added.removeAll(filesBefore);
        assertThat(filesOf(added, "/data/"))
                .containsExactly(fileName(result.addedDataFiles().iterator().next().location()));

        commitRewrite(result);
        assertThat(dataFiles(refreshed(table))).hasSize(5);
        assertThat(countRows(table)).isEqualTo(24);

        // a compaction stopped before it starts returns nothing
        IcebergRewriteDataFiles stopped =
                new IcebergRewriteDataFiles(refreshed(table), null, bucket)
                        .targetSizeInBytes(4096)
                        .tiered(3, Duration.ZERO);
        stopped.requestStop();
        assertThat(stopped.execute()).isNull();
    }

    /**
     * A bucket is compacted again only once the interval passed since its last compaction, which
     * the compaction snapshot records.
     */
    @Test
    void testCompactionWaitsForTheInterval() throws Exception {
        Table table = createTable(3);
        for (int i = 0; i < 3; i++) {
            commitRows(3 * i, 3);
        }
        TableBucket bucket = new TableBucket(tableInfo.getTableId(), 0);
        RewriteDataFileResult first =
                new IcebergRewriteDataFiles(refreshed(table), null, bucket)
                        .tiered(3, Duration.ofHours(1))
                        .execute();
        assertThat(first).isNotNull();
        long compaction = commitRewrite(first);
        assertThat(
                        refreshed(table)
                                .snapshot(compaction)
                                .summary()
                                .get(IcebergRewriteDataFiles.COMPACTED_BUCKETS_PROPERTY))
                .isEqualTo(IcebergRewriteDataFiles.bucketKey(bucket));
        assertThat(refreshed(table).snapshot(compaction).summary())
                .containsEntry(FormatVersions.SNAPSHOT_FORMAT_VERSION_PROPERTY, "1");

        for (int i = 3; i < 6; i++) {
            commitRows(3 * i, 3);
        }
        assertThat(
                        new IcebergRewriteDataFiles(refreshed(table), null, bucket)
                                .tiered(3, Duration.ofHours(1))
                                .execute())
                .isNull();
        assertThat(
                        new IcebergRewriteDataFiles(refreshed(table), null, bucket)
                                .tiered(3, Duration.ZERO)
                                .execute())
                .isNotNull();
    }

    /** The compacted buckets of a compaction snapshot of a newer format are not read. */
    @Test
    void testCompactionSnapshotOfANewerFormatIsRejected() throws Exception {
        Table table = createTable(3);
        for (int i = 0; i < 3; i++) {
            commitRows(3 * i, 3);
        }
        TableBucket bucket = new TableBucket(tableInfo.getTableId(), 0);
        refreshed(table)
                .newAppend()
                .set(
                        IcebergRewriteDataFiles.COMPACTED_BUCKETS_PROPERTY,
                        IcebergRewriteDataFiles.bucketKey(bucket))
                .set(FormatVersions.SNAPSHOT_FORMAT_VERSION_PROPERTY, "2")
                .commit();
        long compaction = refreshed(table).currentSnapshot().snapshotId();

        assertThatThrownBy(
                        () ->
                                new IcebergRewriteDataFiles(refreshed(table), null, bucket)
                                        .tiered(3, Duration.ofHours(1))
                                        .execute())
                .rootCause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Cannot read the deletion vector properties of lake snapshot "
                                + compaction
                                + ": its format version 2 is unknown; this Fluss version reads"
                                + " format versions up to 1.");
    }

    /** Commits a compaction's rewrite as the tiering committer does; returns its snapshot. */
    private long commitRewrite(RewriteDataFileResult result) throws Exception {
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            Long id =
                    committer.commitPendingRewrite(
                            committer.toCommittable(
                                    Collections.singletonList(
                                            new IcebergWriteResult(
                                                    WriteResult.builder().build(),
                                                    refreshed(loadTable()).specs(),
                                                    result))));
            assertThat(id).isNotNull();
            return id;
        }
    }

    private static List<DataFile> dataFiles(Table table) throws IOException {
        List<DataFile> files = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                files.add(task.file());
            }
        }
        return files;
    }

    /** A compaction's index part as a tablet server reads it once registered: key to file@pos. */
    private Map<String, String> handedOver(
            FlussPositionIndexStore store, TableBucket bucket, CompactionIndexPart part)
            throws Exception {
        List<RowPosSstIndex.SstFileEntry> ssts = new ArrayList<>();
        for (int i = 0; i < part.sstFileNames().size(); i++) {
            ssts.add(
                    new RowPosSstIndex.SstFileEntry(
                            part.sstFileNames().get(i), part.sstFileSizes().get(i)));
        }
        store.commitChange(
                part.changeId(),
                Collections.singletonMap(
                        bucket,
                        new PositionIndexStore.BucketChange(
                                new PositionIndexStore.BucketPart(ssts),
                                part.addedFiles(),
                                part.removedFiles())));
        store.register(-1, 100, part.changeId());
        PositionIndexStore.BucketData data =
                store.readChange(
                        store.chain().get(0),
                        bucket,
                        Files.createTempDirectory(tempWarehouseDir.toPath(), "download").toFile());
        assertThat(data.removedFiles).containsExactlyInAnyOrderElementsOf(part.removedFiles());
        Map<String, String> indexed = new TreeMap<>();
        for (String sst : data.localSsts) {
            indexed.putAll(readSst(sst, data.fileId2Name));
        }
        return indexed;
    }

    /**
     * What a re-index of the table's data files finds: every live row at its position, keyed as the
     * re-index keys rows. The files must have no deletes, so a file's rows come in position order.
     */
    private Map<String, String> reindexed(Table table) throws IOException {
        KeyEncoder reindexKeys =
                KeyEncoder.ofPrimaryKeyEncoder(
                        tableInfo.getRowType().project(new int[] {0}),
                        tableInfo.getPhysicalPrimaryKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());
        Map<String, String> out = new TreeMap<>();
        try (CloseableIterable<FileScanTask> tasks = refreshed(table).newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                assertThat(task.deletes()).isEmpty();
                long pos = 0;
                try (CloseableIterable<Record> records =
                        new IcebergGenericReader(table.newScan(), true).open(task)) {
                    for (Record record : records) {
                        int key = (Integer) record.getField("c1");
                        out.put(
                                Arrays.toString(reindexKeys.encodeKey(GenericRow.of(key))),
                                fileName(task.file().location()) + "@" + pos++);
                    }
                }
            }
        }
        return out;
    }

    private static List<Path> filesUnder(File dir) {
        if (!dir.exists()) {
            return Collections.emptyList();
        }
        try (Stream<Path> files = Files.walk(dir.toPath())) {
            return files.filter(Files::isRegularFile).collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Reads an index SST as key bytes to "file@position". */
    private static Map<String, String> readSst(String sst, Map<Integer, String> fileNames)
            throws Exception {
        RocksDB.loadLibrary();
        Map<String, String> out = new TreeMap<>();
        try (Options options = new Options();
                SstFileReader reader = new SstFileReader(options);
                ReadOptions readOptions = new ReadOptions()) {
            reader.open(sst);
            try (SstFileReaderIterator it = reader.newIterator(readOptions)) {
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    FilePos pos = FilePos.decode(it.value());
                    out.put(
                            Arrays.toString(it.key()),
                            fileNames.get(pos.fileId()) + "@" + pos.rowPosition());
                }
            }
        }
        return out;
    }

    /**
     * After the snapshot Fluss registered last expired, as a rewrite by another engine whose tag
     * was rotated away can, the latest tiered snapshot is missing in Fluss only if a round
     * committed it on top of the registered one. The round runs on and re-indexes the table from
     * its current snapshot instead of failing every time.
     */
    @Test
    void testMissingSnapshotAfterTheRegisteredOneExpired() throws Exception {
        createTable(3);
        long expired = 4242L;
        long older = 4141L;
        long onExpired;
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            Map<String, String> properties = new HashMap<>();
            properties.put(
                    SupportsDeletionVectorCommit.FLUSS_LAKE_SNAP_BASE_PROPERTY,
                    String.valueOf(expired));
            FormatVersions.putSnapshotVersion(properties);
            onExpired =
                    committer
                            .commitWithDeletes(
                                    committer.toCommittable(Collections.emptyList()),
                                    properties,
                                    Collections.emptyMap(),
                                    -1)
                            .getCommittedSnapshotId();
            // committed on the expired snapshot and never registered: missing
            assertThat(committer.getMissingLakeSnapshot(expired))
                    .isNotNull()
                    .extracting(CommittedLakeSnapshot::getLakeSnapshotId)
                    .isEqualTo(onExpired);
            // committed before the expired snapshot was registered: nothing is missing
            assertThat(committer.getMissingLakeSnapshot(older)).isNull();
        }
    }

    /** The properties of a snapshot of a newer format are not read for a missing snapshot. */
    @Test
    void testMissingSnapshotOfANewerFormatIsRejected() throws Exception {
        createTable(3);
        long expired = 4242L;
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            Map<String, String> properties = new HashMap<>();
            properties.put(
                    SupportsDeletionVectorCommit.FLUSS_LAKE_SNAP_BASE_PROPERTY,
                    String.valueOf(expired));
            properties.put(FormatVersions.SNAPSHOT_FORMAT_VERSION_PROPERTY, "2");
            long onExpired =
                    committer
                            .commitWithDeletes(
                                    committer.toCommittable(Collections.emptyList()),
                                    properties,
                                    Collections.emptyMap(),
                                    -1)
                            .getCommittedSnapshotId();

            assertThatThrownBy(() -> committer.getMissingLakeSnapshot(expired))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(
                            "Cannot read the deletion vector properties of lake snapshot "
                                    + onExpired
                                    + ": its format version 2 is unknown; this Fluss version"
                                    + " reads format versions up to 1.");
        }
    }

    /**
     * A round without its base in the lake, here the table's first, lands only on the state it
     * planned on. Of two rounds planned on the empty table, as by the tiering service the table was
     * taken from and the one it was handed to, the second to commit is rejected and may run again
     * on the first one's snapshot.
     */
    @Test
    void testFirstRoundsLandOnlyOnTheTableWithoutData() throws Exception {
        Table table = createTable(3);
        try (IcebergLakeCommitter first = new IcebergLakeCommitter(catalogProvider, tablePath);
                IcebergLakeCommitter second =
                        new IcebergLakeCommitter(catalogProvider, tablePath)) {
            long landed =
                    first.commitWithDeletes(
                                    first.toCommittable(Collections.emptyList()),
                                    Collections.emptyMap(),
                                    Collections.emptyMap(),
                                    -1)
                            .getCommittedSnapshotId();
            assertThatThrownBy(
                            () ->
                                    second.commitWithDeletes(
                                            second.toCommittable(Collections.emptyList()),
                                            Collections.emptyMap(),
                                            Collections.emptyMap(),
                                            -1))
                    .isInstanceOf(LakeBaseChangedException.class);
            assertThat(refreshed(table).currentSnapshot().snapshotId()).isEqualTo(landed);
        }
    }

    /**
     * A round whose base expired lands only on the snapshot that was current when it planned:
     * another commit since rejects it, and a round planned on that commit lands on it.
     */
    @Test
    void testRoundOnAnExpiredBaseLandsOnlyOnThePlannedSnapshot() throws Exception {
        Table table = createTable(3);
        long expired = 4242L;
        commitRows(0, 3);
        long other;
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            other = commitRows(3, 3);
            assertThatThrownBy(
                            () ->
                                    committer.commitWithDeletes(
                                            committer.toCommittable(Collections.emptyList()),
                                            Collections.emptyMap(),
                                            Collections.emptyMap(),
                                            expired))
                    .isInstanceOf(LakeBaseChangedException.class);
            assertThat(refreshed(table).currentSnapshot().snapshotId()).isEqualTo(other);
        }
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            long landed =
                    committer
                            .commitWithDeletes(
                                    committer.toCommittable(Collections.emptyList()),
                                    Collections.emptyMap(),
                                    Collections.emptyMap(),
                                    expired)
                            .getCommittedSnapshotId();
            assertThat(refreshed(table).snapshot(landed).parentId()).isEqualTo(other);
        }
    }

    /**
     * A v2 position delete file whose data file another engine rewrote away is removed by the next
     * round's commit.
     */
    @Test
    void testRoundRemovesPositionDeletesOfRewrittenFiles() throws Exception {
        Table table = createTable(2);
        long first = commitRows(0, 3);
        String fileName = onlyDataFileName(table);
        long withDeletes;
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            withDeletes =
                    committer
                            .commitWithDeletes(
                                    committer.toCommittable(Collections.emptyList()),
                                    Collections.emptyMap(),
                                    Collections.singletonMap(fileName, new long[] {0L}),
                                    first)
                            .getCommittedSnapshotId();
        }
        List<DeleteFile> positionDeletes = liveDeleteFiles(refreshed(table));
        assertThat(positionDeletes).hasSize(1);
        String danglingDelete = positionDeletes.get(0).location();

        // Another engine rewrites the data file, leaving the position delete file behind, as
        // Spark's rewrite_data_files does without remove-dangling-deletes.
        long rewrite = rewriteAllDataFilesKeepingDeletes(refreshed(table), withDeletes);
        assertThat(liveDeleteFiles(refreshed(table)))
                .extracting(DeleteFile::location)
                .containsExactly(danglingDelete);
        assertThat(countRows(table)).isEqualTo(2);

        // The next round, on the rewrite, removes it.
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            committer.commitWithDeletes(
                    committer.toCommittable(Collections.emptyList()),
                    Collections.emptyMap(),
                    Collections.emptyMap(),
                    rewrite);
        }
        assertThat(liveDeleteFiles(refreshed(table))).isEmpty();
        assertThat(countRows(table)).isEqualTo(2);
    }

    /**
     * Rounds on a table of many data files read only what changed since the round before. The
     * base's files the deletes reference, and their deletes, come from the files the snapshots
     * since then added and removed; only the first round after a start plans the whole base.
     * Committers that keep nothing across rounds plan the base every round.
     */
    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testRoundsReadOnlyWhatChangedSinceTheRoundBefore(int formatVersion) throws Exception {
        Table table = createTable(formatVersion);
        commitRows(0, 3);
        String template = onlyDataFileName(table);
        DataFile templateFile = null;
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                templateFile = task.file();
            }
        }
        // many data files in many manifests, as tiering leaves them
        int manifests = 100;
        int filesPerManifest = 100;
        List<String> files = new ArrayList<>();
        for (int m = 0; m < manifests; m++) {
            AppendFiles append = table.newAppend();
            for (int i = 0; i < filesPerManifest; i++) {
                String name = "copy-" + m + "-" + i + "-" + template;
                append.appendFile(
                        DataFiles.builder(table.spec())
                                .copy(templateFile)
                                .withPath(templateFile.location().replace(template, name))
                                .build());
                files.add(name);
            }
            append.commit();
        }
        long base = refreshed(table).currentSnapshot().snapshotId();

        int rounds = 10;
        long[] scans = new long[2];
        IcebergCommitterState kept =
                new IcebergCommitterState(IcebergCommitterState.DEFAULT_EXPIRE_INTERVAL_MS);
        for (int cached = 0; cached < 2; cached++) {
            for (int r = 0; r < rounds; r++) {
                String file = files.get(cached * rounds + r);
                IcebergCommitterState state =
                        cached == 1
                                ? kept
                                : new IcebergCommitterState(
                                        IcebergCommitterState.DEFAULT_EXPIRE_INTERVAL_MS);
                long scansBefore = state.liveFiles.scans();
                try (IcebergLakeCommitter committer =
                        new IcebergLakeCommitter(catalogProvider, tablePath, false, state)) {
                    base =
                            committer
                                    .commitWithDeletes(
                                            committer.toCommittable(Collections.emptyList()),
                                            Collections.emptyMap(),
                                            Collections.singletonMap(file, new long[] {1L}),
                                            base)
                                    .getCommittedSnapshotId();
                }
                scans[cached] += state.liveFiles.scans() - scansBefore;
            }
        }
        assertThat(scans[0]).isEqualTo(rounds);
        assertThat(scans[1]).isEqualTo(1);
        // every round's delete landed on its file
        Set<String> deleted = new HashSet<>();
        for (DeleteFile delete : liveDeleteFiles(refreshed(table))) {
            deleted.add(fileName(ContentFileUtil.referencedDataFileLocation(delete)));
        }
        assertThat(deleted).containsExactlyInAnyOrderElementsOf(files.subList(0, 2 * rounds));
    }

    /**
     * A Fluss compaction of a table with row lineage (format version 3) keeps every row's {@code
     * _row_id} and {@code _last_updated_sequence_number}, as the spec requires of a writer that
     * moves rows: the rewritten files store the values the rows had, stored or inherited. A version
     * 2 table has no lineage, and its compaction writes the table's columns only.
     */
    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testCompactionKeepsRowLineage(int formatVersion) throws Exception {
        Table table = createTable(formatVersion);
        List<String> inputs = new ArrayList<>();
        for (int from = 0; from < 9; from += 3) {
            long snapshotId = commitRows(from, 3);
            inputs.add(addedDataFile(refreshed(table), snapshotId));
        }
        long third = table.currentSnapshot().snapshotId();
        // key 4 is deleted before the compaction
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            committer.commitWithDeletes(
                    committer.toCommittable(Collections.emptyList()),
                    Collections.emptyMap(),
                    Collections.singletonMap(fileName(inputs.get(1)), new long[] {1L}),
                    third);
        }
        Map<Integer, List<Long>> before = lineage(refreshed(table));
        assertThat(before).hasSize(8).doesNotContainKey(4);

        RewriteDataFileResult result =
                new IcebergRewriteDataFiles(
                                refreshed(table), null, new TableBucket(tableInfo.getTableId(), 0))
                        .execute();
        assertThat(result).isNotNull();
        commitRewrite(result);
        assertThat(dataFiles(refreshed(table))).hasSize(1);

        Map<Integer, List<Long>> after = lineage(refreshed(table));
        assertThat(after).isEqualTo(before);
        if (formatVersion >= 3) {
            Set<Long> rowIds = new HashSet<>();
            for (List<Long> rowLineage : after.values()) {
                assertThat(rowLineage).doesNotContainNull();
                rowIds.add(rowLineage.get(0));
            }
            assertThat(rowIds).hasSize(after.size());
        } else {
            assertThat(after.values()).allSatisfy(l -> assertThat(l).isEmpty());
        }
    }

    /**
     * Every live row's lineage by key, read through Iceberg's metadata columns: [{@code _row_id},
     * {@code _last_updated_sequence_number}], or nothing on a table without row lineage.
     */
    private static Map<Integer, List<Long>> lineage(Table table) throws IOException {
        boolean rowLineage = TableUtil.supportsRowLineage(table);
        org.apache.iceberg.Schema projection =
                rowLineage ? MetadataColumns.schemaWithRowLineage(table.schema()) : table.schema();
        Map<Integer, List<Long>> out = new TreeMap<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                try (CloseableIterable<Record> records =
                        new IcebergGenericReader(table.newScan().project(projection), false)
                                .open(task)) {
                    for (Record record : records) {
                        List<Long> values = new ArrayList<>();
                        if (rowLineage) {
                            values.add((Long) record.getField(MetadataColumns.ROW_ID.name()));
                            values.add(
                                    (Long)
                                            record.getField(
                                                    MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER
                                                            .name()));
                        }
                        out.put((Integer) record.getField("c1"), values);
                    }
                }
            }
        }
        return out;
    }

    /**
     * A table with deletion vectors whose lake table has format version 1, which has no deletes by
     * position, is not tiered: every round would fail at its first delete.
     */
    @Test
    void testTableWithFormatVersion1IsNotTiered() throws Exception {
        tablePath = TablePath.of("iceberg", "derived_commit_v1");
        TableDescriptor plain =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("c1", DataTypes.INT())
                                        .column("c2", DataTypes.STRING())
                                        .primaryKey("c1")
                                        .build())
                        .distributedBy(1, "c1")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .customProperty("iceberg.format-version", "1")
                        .build();
        try (IcebergLakeCatalog catalog = new IcebergLakeCatalog(configuration)) {
            catalog.createTable(tablePath, plain, new TestingLakeCatalogContext());
        }
        assertThat(TableUtil.formatVersion(loadTable())).isEqualTo(1);
        TableDescriptor withDeletionVectors =
                plain.withProperties(
                        Collections.singletonMap(
                                ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key(), "true"));
        TableInfo dvTableInfo =
                TableInfo.of(tablePath, 0, 1, withDeletionVectors, DEFAULT_REMOTE_DATA_DIR, 1L, 1L);

        assertThatThrownBy(() -> tieringFactory.validateDeletionVectorTable(dvTableInfo, null))
                .hasMessageContaining("format version 1");
        // a table without deletion vectors passes
        tieringFactory.validateTable(
                TableInfo.of(tablePath, 0, 1, plain, DEFAULT_REMOTE_DATA_DIR, 1L, 1L));
    }

    /**
     * A snapshot between the registered base and the current one expired, here a rewrite of the
     * manifests. The history cannot be replayed, and the base fence cannot tell what the expired
     * snapshot changed, so a round's commit on the base is refused. The rewrite plan then moves the
     * index to the current snapshot, with an empty change when its live files are the base's, so
     * the round runs again on the current snapshot instead of failing every time.
     */
    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testRewritePlanAcrossAnExpiredSnapshotMovesToTheCurrentOne(int formatVersion)
            throws Exception {
        Table table = createTable(formatVersion);
        commitRows(0, 3);
        long base = commitRows(3, 3);
        refreshed(table).rewriteManifests().rewriteIf(m -> true).commit();
        long expired = refreshed(table).currentSnapshot().snapshotId();
        refreshed(table).rewriteManifests().rewriteIf(m -> true).commit();
        long current = refreshed(table).currentSnapshot().snapshotId();
        assertThat(current).isNotEqualTo(expired);
        refreshed(table).expireSnapshots().expireSnapshotId(expired).commit();
        assertThat(refreshed(table).snapshot(expired)).isNull();
        assertThat(table.snapshot(base)).isNotNull();

        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            assertThatThrownBy(
                            () ->
                                    committer.commitWithDeletes(
                                            committer.toCommittable(Collections.emptyList()),
                                            Collections.emptyMap(),
                                            Collections.emptyMap(),
                                            base))
                    .isInstanceOf(LakeBaseChangedException.class);
        }

        DataDeltaPlan<IcebergSplit> plan =
                new IcebergLakeSource(configuration, tablePath).planRewrite(base, null);
        assertThat(plan).isNotNull();
        assertThat(plan.getCompactSnapshotId()).isEqualTo(current);
        assertThat(plan.getSplits()).isEmpty();
        assertThat(plan.getDeletedFiles()).isEmpty();

        // Once the index is at the current snapshot, the round commits on it.
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            long landed =
                    committer
                            .commitWithDeletes(
                                    committer.toCommittable(Collections.emptyList()),
                                    Collections.emptyMap(),
                                    Collections.emptyMap(),
                                    current)
                            .getCommittedSnapshotId();
            assertThat(refreshed(table).snapshot(landed).parentId()).isEqualTo(current);
        }
    }

    /**
     * A rewrite across an expired snapshot that replaced data files is planned from the base's and
     * the current snapshot's files.
     */
    @Test
    void testRewritePlanAcrossAnExpiredSnapshotReindexesTheRewrite() throws Exception {
        Table table = createTable(3);
        long first = commitRows(0, 3);
        long base = commitRows(3, 3);
        String rewritten = addedDataFile(refreshed(table), first);
        long rewrite = rewriteDataFile(refreshed(table), rewritten);
        refreshed(table).rewriteManifests().rewriteIf(m -> true).commit();
        long current = refreshed(table).currentSnapshot().snapshotId();
        refreshed(table).expireSnapshots().expireSnapshotId(rewrite).commit();

        DataDeltaPlan<IcebergSplit> plan =
                new IcebergLakeSource(configuration, tablePath).planRewrite(base, null);
        assertThat(plan).isNotNull();
        assertThat(plan.getCompactSnapshotId()).isEqualTo(current);
        assertThat(plan.getSplits()).hasSize(1);
        assertThat(plan.getDeletedFiles().values())
                .flatExtracting(Map::values)
                .containsExactly(Collections.singletonList(fileName(rewritten)));
    }

    /**
     * Rows another engine wrote to the lake table before Fluss tiered it are not in the index, so
     * the first round would leave them live next to its own versions of their keys. The round is
     * refused before it writes anything, and so is the tiering service's check of the table.
     */
    @Test
    void testFirstRoundOnRowsAnotherEngineWroteIsRefused() throws Exception {
        Table table = createTable(3);
        table.newAppend().appendFile(writeExternalDataFile(table, 1)).commit();
        long external = refreshed(table).currentSnapshot().snapshotId();
        Set<String> filesBefore = filesOnDisk();

        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            assertThatThrownBy(
                            () ->
                                    committer.commitWithDeletes(
                                            committer.toCommittable(Collections.emptyList()),
                                            Collections.emptyMap(),
                                            Collections.emptyMap(),
                                            -1))
                    .isInstanceOf(LakeCommitRejectedException.class)
                    .hasRootCauseInstanceOf(LakeTableExternallyModifiedException.class)
                    .rootCause()
                    .hasMessageContaining("snapshot " + external)
                    .hasMessageContaining("before Fluss");
        }
        assertThat(refreshed(table).currentSnapshot().snapshotId()).isEqualTo(external);
        assertThat(filesOnDisk()).isEqualTo(filesBefore);
        assertThatThrownBy(() -> tieringFactory.validateDeletionVectorTable(tableInfo, null))
                .isInstanceOf(LakeTableExternallyModifiedException.class)
                .hasMessageContaining("snapshot " + external);
    }

    /** Fluss's own commits before the first registered snapshot do not stop the first round. */
    @Test
    void testFirstRoundOnFlussCommitsIsAllowed() throws Exception {
        Table table = createTable(3);
        long tiered = commitRows(0, 3);
        tieringFactory.validateDeletionVectorTable(tableInfo, null);
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            long landed =
                    committer
                            .commitWithDeletes(
                                    committer.toCommittable(Collections.emptyList()),
                                    Collections.emptyMap(),
                                    Collections.emptyMap(),
                                    -1)
                            .getCommittedSnapshotId();
            assertThat(refreshed(table).snapshot(landed).parentId()).isEqualTo(tiered);
        }
    }

    /**
     * A round that committed on top of the registered snapshot but was lost before it registered
     * its snapshot is missing while the lake table descends from it. Once the lake table is rolled
     * back to the registered snapshot, the lost round is no longer part of the table: rounds go on
     * from the registered snapshot instead of stopping on a rollback past the lost one.
     */
    @Test
    void testRollbackToTheRegisteredSnapshotDropsALostRound() throws Exception {
        createTable(3);
        long registered;
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            registered =
                    committer
                            .commitWithDeletes(
                                    committer.toCommittable(Collections.emptyList()),
                                    roundProperties(-1),
                                    Collections.emptyMap(),
                                    -1)
                            .getCommittedSnapshotId();
        }
        long lost;
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            lost =
                    committer
                            .commitWithDeletes(
                                    committer.toCommittable(Collections.emptyList()),
                                    roundProperties(registered),
                                    Collections.emptyMap(),
                                    registered)
                            .getCommittedSnapshotId();
            assertThat(committer.getMissingLakeSnapshot(registered))
                    .isNotNull()
                    .extracting(CommittedLakeSnapshot::getLakeSnapshotId)
                    .isEqualTo(lost);
        }

        loadTable().manageSnapshots().rollbackTo(registered).commit();

        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            assertThat(committer.getMissingLakeSnapshot(registered)).isNull();
            committer.checkCurrentSnapshotDescendsFrom(registered);
            long next =
                    committer
                            .commitWithDeletes(
                                    committer.toCommittable(Collections.emptyList()),
                                    roundProperties(registered),
                                    Collections.emptyMap(),
                                    registered)
                            .getCommittedSnapshotId();
            assertThat(refreshed(loadTable()).snapshot(next).parentId()).isEqualTo(registered);
        }
    }

    /**
     * A round whose delete files cannot all be written, here because the delete file it merges with
     * is gone, leaves none of the delete files it wrote before the failure.
     */
    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testDeleteFilesWrittenBeforeAFailedWriteAreDeleted(int formatVersion) throws Exception {
        Table table = createTable(formatVersion);
        long first = commitRows(0, 3);
        long second = commitRows(3, 3);
        String intact = fileName(addedDataFile(refreshed(table), first));
        String broken = fileName(addedDataFile(table, second));
        long withDeletes;
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            withDeletes =
                    committer
                            .commitWithDeletes(
                                    committer.toCommittable(Collections.emptyList()),
                                    Collections.emptyMap(),
                                    Collections.singletonMap(broken, new long[] {0L}),
                                    second)
                            .getCommittedSnapshotId();
        }
        // the delete file of the second data file is lost
        List<DeleteFile> deletes = liveDeleteFiles(refreshed(table));
        assertThat(deletes).hasSize(1);
        File lostFile = new File(java.net.URI.create(deletes.get(0).location()).getPath());
        assertThat(lostFile.delete()).isTrue();
        new File(lostFile.getParentFile(), "." + lostFile.getName() + ".crc").delete();
        Set<String> filesBefore = filesOnDisk();

        // the intact file's deletes come first, so they are written before the failure
        Map<String, long[]> deleted = new java.util.LinkedHashMap<>();
        deleted.put(intact, new long[] {1L});
        deleted.put(broken, new long[] {1L});
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            assertThatThrownBy(
                            () ->
                                    committer.commitWithDeletes(
                                            committer.toCommittable(Collections.emptyList()),
                                            Collections.emptyMap(),
                                            deleted,
                                            withDeletes))
                    .isInstanceOf(LakeCommitRejectedException.class);
        }
        assertThat(refreshed(table).currentSnapshot().snapshotId()).isEqualTo(withDeletes);
        assertThat(filesOnDisk()).isEqualTo(filesBefore);
    }

    /**
     * Expiry never removes the snapshot the index chain starts at, also when a snapshot after it
     * was committed by a clock behind. Iceberg keeps a branch's history only up to its first
     * snapshot older than the cut-off, so a timestamp cut-off alone would let the chain's start
     * expire.
     */
    @Test
    void testExpiryKeepsTheChainStartWhenALaterSnapshotIsOlder() throws Exception {
        Table table = createTable(3);
        table.updateProperties()
                .set(org.apache.iceberg.TableProperties.MAX_SNAPSHOT_AGE_MS, "1")
                .set(org.apache.iceberg.TableProperties.MIN_SNAPSHOTS_TO_KEEP, "1")
                .commit();
        // an old snapshot before the chain's start, which expires
        long older = commitRowsAt(0, 3, -50_000L);
        long keepFrom = commitRows(3, 3);
        // committed by a clock 30 s behind
        long skewed = commitRowsAt(6, 3, -30_000L);
        assertThat(refreshed(table).snapshot(skewed).timestampMillis())
                .isLessThan(table.snapshot(keepFrom).timestampMillis());
        long head = commitRows(9, 3);
        Thread.sleep(10);

        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(
                        catalogProvider, tablePath, true, new IcebergCommitterState(0))) {
            committer.expireSnapshots(keepFrom);
        }

        table = refreshed(table);
        assertThat(table.snapshot(head)).isNotNull();
        assertThat(table.snapshot(skewed)).isNotNull();
        assertThat(table.snapshot(keepFrom)).isNotNull();
        assertThat(table.snapshot(older)).isNull();
        assertThat(countRows(table)).isEqualTo(12);
    }

    // ------------------------------------------------------------------------------------------

    /** The snapshot properties of a round on {@code base}, as the tiering committer sets them. */
    private static Map<String, String> roundProperties(long base) {
        Map<String, String> properties = new HashMap<>();
        properties.put(
                SupportsDeletionVectorCommit.FLUSS_LAKE_SNAP_BASE_PROPERTY, String.valueOf(base));
        FormatVersions.putSnapshotVersion(properties);
        return properties;
    }

    /**
     * Tiers rows with keys [from, from + count) in one data file, committed with a timestamp {@code
     * skewMs} off the clock, and returns the snapshot.
     */
    private long commitRowsAt(int from, int count, long skewMs) throws Exception {
        List<IcebergWriteResult> writeResults = new ArrayList<>();
        try (LakeWriter<IcebergWriteResult> writer = createWriter()) {
            for (int key = from; key < from + count; key++) {
                writer.write(insert(key, key, "v" + key));
            }
            writeResults.add(writer.complete());
        }
        IcebergCatalogProvider skewing =
                new IcebergCatalogProvider(configuration) {
                    @Override
                    public Catalog get() {
                        return new SkewingCatalog();
                    }
                };
        try (IcebergLakeCommitter committer = new IcebergLakeCommitter(skewing, tablePath)) {
            return committer
                    .commit(
                            committer.toCommittable(writeResults),
                            new CommitContext(
                                    Collections.singletonMap(SKEW_MS, String.valueOf(skewMs)),
                                    Collections.emptyMap()))
                    .getCommittedSnapshotId();
        }
    }

    private Table createTable(int formatVersion) throws Exception {
        tablePath = TablePath.of("iceberg", "derived_commit_v" + formatVersion);
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("c1", DataTypes.INT())
                                        .column("c2", DataTypes.STRING())
                                        .primaryKey("c1")
                                        .build())
                        .distributedBy(1, "c1")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED, true)
                        .customProperty("iceberg.format-version", String.valueOf(formatVersion))
                        .build();
        try (IcebergLakeCatalog catalog = new IcebergLakeCatalog(configuration)) {
            catalog.createTable(tablePath, descriptor, new TestingLakeCatalogContext());
        }
        tableInfo = TableInfo.of(tablePath, 0, 1, descriptor, DEFAULT_REMOTE_DATA_DIR, 1L, 1L);
        return catalogProvider.get().loadTable(toIceberg(tablePath));
    }

    /** Tiers rows with keys [from, from + count) in one data file and returns the snapshot. */
    private long commitRows(int from, int count) throws Exception {
        int[] keys = new int[count];
        for (int i = 0; i < count; i++) {
            keys[i] = from + i;
        }
        return commitKeys(keys);
    }

    /** Tiers rows with the keys in one data file and returns the snapshot. */
    private long commitKeys(int... keys) throws Exception {
        List<IcebergWriteResult> writeResults = new ArrayList<>();
        try (LakeWriter<IcebergWriteResult> writer = createWriter()) {
            for (int key : keys) {
                writer.write(insert(key, key, "v" + key));
            }
            writeResults.add(writer.complete());
        }
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            return committer
                    .commit(
                            committer.toCommittable(writeResults),
                            new CommitContext(Collections.emptyMap(), Collections.emptyMap()))
                    .getCommittedSnapshotId();
        }
    }

    private Table loadTable() {
        return catalogProvider.get().loadTable(toIceberg(tablePath));
    }

    /** A catalog provider whose tables run the hooks of {@link CommitHookCatalog}. */
    private IcebergCatalogProvider hookCatalogProvider() {
        return new IcebergCatalogProvider(configuration) {
            @Override
            public Catalog get() {
                CommitHookCatalog catalog = new CommitHookCatalog();
                catalog.setConf(new org.apache.hadoop.conf.Configuration());
                catalog.initialize("test", new HashMap<>(configuration.toMap()));
                return catalog;
            }
        };
    }

    /** The location of the one data file {@code snapshotId} added. */
    private static String addedDataFile(Table table, long snapshotId) {
        List<String> locations = new ArrayList<>();
        table.snapshot(snapshotId)
                .addedDataFiles(table.io())
                .forEach(f -> locations.add(f.location()));
        assertThat(locations).hasSize(1);
        return locations.get(0);
    }

    private static String fileName(String location) {
        return location.substring(location.lastIndexOf('/') + 1);
    }

    /**
     * Rewrites the live rows of data file {@code location} into a new file, as another engine's
     * compaction of that file does; returns the rewrite's snapshot.
     */
    private static long rewriteDataFile(Table table, String location) throws Exception {
        long baseSnapshotId = table.currentSnapshot().snapshotId();
        RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(baseSnapshotId);
        try (TaskWriter<Record> writer = TaskWriterFactory.createTaskWriter(table, null, 0, true);
                CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                if (!task.file().location().equals(location)) {
                    continue;
                }
                rewrite.deleteFile(task.file());
                task.deletes().forEach(rewrite::deleteFile);
                try (CloseableIterable<Record> records =
                        new IcebergGenericReader(table.newScan(), true).open(task)) {
                    for (Record record : records) {
                        writer.write(record.copy());
                    }
                }
            }
            for (DataFile file : writer.complete().dataFiles()) {
                rewrite.addFile(file);
            }
        }
        rewrite.commit();
        table.refresh();
        return table.currentSnapshot().snapshotId();
    }

    private Table refreshed(Table table) {
        table.refresh();
        return table;
    }

    private static String onlyDataFileName(Table table) throws IOException {
        table.refresh();
        List<String> names = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                String location = task.file().location();
                names.add(location.substring(location.lastIndexOf('/') + 1));
            }
        }
        assertThat(names).hasSize(1);
        return names.get(0);
    }

    private static DataFile writeExternalDataFile(Table table, int key) throws Exception {
        try (TaskWriter<Record> writer = TaskWriterFactory.createTaskWriter(table, null, 0, true)) {
            Record record = org.apache.iceberg.data.GenericRecord.create(table.schema());
            record.setField("c1", key);
            record.setField("c2", "external");
            writer.write(record);
            DataFile[] files = writer.complete().dataFiles();
            assertThat(files).hasSize(1);
            return files[0];
        }
    }

    /**
     * Rewrites every live row into a new data file, removing the old data files but none of the
     * delete files; returns the rewrite's snapshot.
     */
    private static long rewriteAllDataFilesKeepingDeletes(Table table, long baseSnapshotId)
            throws Exception {
        RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(baseSnapshotId);
        try (TaskWriter<Record> writer = TaskWriterFactory.createTaskWriter(table, null, 0, true);
                CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                rewrite.deleteFile(task.file());
                // the reader applies the task's deletes
                try (CloseableIterable<Record> records =
                        new IcebergGenericReader(table.newScan(), true).open(task)) {
                    for (Record record : records) {
                        writer.write(record.copy());
                    }
                }
            }
            for (DataFile file : writer.complete().dataFiles()) {
                rewrite.addFile(file);
            }
        }
        rewrite.commit();
        table.refresh();
        return table.currentSnapshot().snapshotId();
    }

    /** All live delete files of the current snapshot, also those no data file matches. */
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

    private Set<String> filesOnDisk() throws IOException {
        try (Stream<Path> files = Files.walk(tempWarehouseDir.toPath())) {
            return files.filter(Files::isRegularFile)
                    .map(Path::toString)
                    .filter(p -> p.contains("/data/"))
                    .collect(Collectors.toSet());
        }
    }

    private static Set<String> filesOf(Set<String> paths, String marker) {
        Set<String> out = new HashSet<>();
        for (String p : paths) {
            if (p.contains(marker) && !p.endsWith(".crc")) {
                out.add(p.substring(p.lastIndexOf('/') + 1));
            }
        }
        return out;
    }

    private static int countRows(Table table) throws Exception {
        table.refresh();
        int count = 0;
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record ignored : records) {
                count++;
            }
        }
        return count;
    }

    private LakeWriter<IcebergWriteResult> createWriter() throws Exception {
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

    /**
     * A Hadoop catalog that applies a commit whose snapshot has the {@link #FAIL_COMMIT} property
     * but then fails it with an unknown outcome, as when the response of a REST catalog is lost.
     */
    private final class FailingCatalog extends HadoopCatalog {

        private FailingCatalog() {
            setConf(new org.apache.hadoop.conf.Configuration());
            Map<String, String> properties = new HashMap<>(configuration.toMap());
            initialize("test", properties);
        }

        @Override
        protected TableOperations newTableOps(TableIdentifier identifier) {
            TableOperations delegate = super.newTableOps(identifier);
            return new TableOperations() {
                @Override
                public void commit(TableMetadata base, TableMetadata metadata) {
                    Snapshot pending = metadata.currentSnapshot();
                    if (pending != null
                            && "true".equals(pending.summary().get(FAIL_BEFORE_COMMIT))) {
                        throw new CommitStateUnknownException(new IOException("request lost"));
                    }
                    delegate.commit(base, metadata);
                    Snapshot snapshot = metadata.currentSnapshot();
                    if (snapshot != null
                            && (base == null
                                    || base.currentSnapshot() == null
                                    || base.currentSnapshot().snapshotId() != snapshot.snapshotId())
                            && "true".equals(snapshot.summary().get(FAIL_COMMIT))) {
                        throw new CommitStateUnknownException(new IOException("response lost"));
                    }
                }

                @Override
                public TableMetadata current() {
                    return delegate.current();
                }

                @Override
                public TableMetadata refresh() {
                    return delegate.refresh();
                }

                @Override
                public FileIO io() {
                    return delegate.io();
                }

                @Override
                public EncryptionManager encryption() {
                    return delegate.encryption();
                }

                @Override
                public String metadataFileLocation(String fileName) {
                    return delegate.metadataFileLocation(fileName);
                }

                @Override
                public LocationProvider locationProvider() {
                    return delegate.locationProvider();
                }

                @Override
                public TableOperations temp(TableMetadata uncommittedMetadata) {
                    return delegate.temp(uncommittedMetadata);
                }

                @Override
                public long newSnapshotId() {
                    return delegate.newSnapshotId();
                }

                @Override
                public boolean requireStrictCleanup() {
                    return delegate.requireStrictCleanup();
                }
            };
        }
    }

    /**
     * A Hadoop catalog that commits a snapshot with the {@link #SKEW_MS} property with its
     * timestamp moved by that many milliseconds, as a committer with a skewed clock does.
     */
    private final class SkewingCatalog extends HadoopCatalog {

        private SkewingCatalog() {
            setConf(new org.apache.hadoop.conf.Configuration());
            initialize("test", new HashMap<>(configuration.toMap()));
        }

        @Override
        protected TableOperations newTableOps(TableIdentifier identifier) {
            TableOperations delegate = super.newTableOps(identifier);
            return new TableOperations() {
                @Override
                public void commit(TableMetadata base, TableMetadata metadata) {
                    Snapshot snapshot = metadata.currentSnapshot();
                    String skew = snapshot == null ? null : snapshot.summary().get(SKEW_MS);
                    if (skew != null && base != null) {
                        metadata =
                                TableMetadata.buildFrom(base)
                                        .setBranchSnapshot(
                                                new SkewedSnapshot(snapshot, Long.parseLong(skew)),
                                                org.apache.iceberg.SnapshotRef.MAIN_BRANCH)
                                        .build();
                    }
                    delegate.commit(base, metadata);
                }

                @Override
                public TableMetadata current() {
                    return delegate.current();
                }

                @Override
                public TableMetadata refresh() {
                    return delegate.refresh();
                }

                @Override
                public FileIO io() {
                    return delegate.io();
                }

                @Override
                public EncryptionManager encryption() {
                    return delegate.encryption();
                }

                @Override
                public String metadataFileLocation(String fileName) {
                    return delegate.metadataFileLocation(fileName);
                }

                @Override
                public LocationProvider locationProvider() {
                    return delegate.locationProvider();
                }

                @Override
                public TableOperations temp(TableMetadata uncommittedMetadata) {
                    return delegate.temp(uncommittedMetadata);
                }

                @Override
                public long newSnapshotId() {
                    return delegate.newSnapshotId();
                }

                @Override
                public boolean requireStrictCleanup() {
                    return delegate.requireStrictCleanup();
                }
            };
        }
    }

    /** A snapshot with its timestamp moved. */
    private static final class SkewedSnapshot implements Snapshot {

        private static final long serialVersionUID = 1L;

        private final Snapshot delegate;
        private final long skewMs;

        private SkewedSnapshot(Snapshot delegate, long skewMs) {
            this.delegate = delegate;
            this.skewMs = skewMs;
        }

        @Override
        public long sequenceNumber() {
            return delegate.sequenceNumber();
        }

        @Override
        public long snapshotId() {
            return delegate.snapshotId();
        }

        @Override
        public Long parentId() {
            return delegate.parentId();
        }

        @Override
        public long timestampMillis() {
            return delegate.timestampMillis() + skewMs;
        }

        @Override
        public List<ManifestFile> allManifests(FileIO io) {
            return delegate.allManifests(io);
        }

        @Override
        public List<ManifestFile> dataManifests(FileIO io) {
            return delegate.dataManifests(io);
        }

        @Override
        public List<ManifestFile> deleteManifests(FileIO io) {
            return delegate.deleteManifests(io);
        }

        @Override
        public String operation() {
            return delegate.operation();
        }

        @Override
        public Map<String, String> summary() {
            return delegate.summary();
        }

        @Override
        public Iterable<DataFile> addedDataFiles(FileIO io) {
            return delegate.addedDataFiles(io);
        }

        @Override
        public Iterable<DataFile> removedDataFiles(FileIO io) {
            return delegate.removedDataFiles(io);
        }

        @Override
        public Iterable<DeleteFile> addedDeleteFiles(FileIO io) {
            return delegate.addedDeleteFiles(io);
        }

        @Override
        public Iterable<DeleteFile> removedDeleteFiles(FileIO io) {
            return delegate.removedDeleteFiles(io);
        }

        @Override
        public String manifestListLocation() {
            return delegate.manifestListLocation();
        }

        @Override
        public Integer schemaId() {
            return delegate.schemaId();
        }

        @Override
        public Long firstRowId() {
            return delegate.firstRowId();
        }

        @Override
        public Long addedRows() {
            return delegate.addedRows();
        }

        @Override
        public String keyId() {
            return delegate.keyId();
        }
    }
}
