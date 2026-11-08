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
import org.apache.fluss.lake.committer.LakeCommitRejectedException;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.paimon.tiering.PaimonLakeCommitter.CommitOutcome;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
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
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.lake.paimon.utils.PaimonTestUtils.CompactHelper;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A failed commit of a Paimon table with deletion vectors deletes nothing at once: the commit could
 * still land. A later round of the same job deletes its files only if, once the failure is old
 * enough, no snapshot after the latest one before the commit is of its commit user; a commit that
 * landed, even in part or late, or one through a catalog with version management keeps its files.
 */
class PaimonCommitOutcomeTest {

    private static final TablePath TABLE_PATH = TablePath.of("fluss", "commit_outcome");

    @TempDir private File warehouse;
    @TempDir private File compactionTempDir;

    private static final long TABLE_ID = 7L;
    private static final long MIN_AGE_MS = 60_000L;

    private final AtomicLong clock = new AtomicLong(1_000_000L);
    // the fake clock ticks in the units of the ages
    private final PaimonCommitterState committerState =
            new PaimonCommitterState(MIN_AGE_MS, Long.MAX_VALUE / 2, 1_000, clock::get);

    private Configuration paimonConfig;
    private Catalog catalog;
    private FileStoreTable table;
    private int nextKey = 100;

    @AfterEach
    void tearDown() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    /**
     * Paimon refuses the commit before it writes a snapshot: it deletes a file the table never
     * added. Its files and offsets file stay at first, as the commit could still land, and the next
     * round deletes them once the failure is old enough and the commit still has not landed.
     */
    @Test
    void testCommitThatNeverLandsIsDeletedAtTheNextRound() throws Exception {
        createTable("filesystem");
        commit(writeRow(0));
        List<CommitMessage> refused = refusedCommit();
        List<File> written = newFiles(refused);
        File offsetsFile = offsetsFile();
        assertThat(written).isNotEmpty().allMatch(File::exists);

        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> committer = committer(true)) {
            assertThatThrownBy(() -> committer.commit(committableOf(refused), context(offsetsFile)))
                    .isInstanceOf(IOException.class)
                    .isNotInstanceOf(LakeCommitRejectedException.class)
                    .hasStackTraceContaining("not previously added");
        }
        assertThat(written).allMatch(File::exists);
        assertThat(offsetsFile).exists();
        assertThat(committerState.failedCommits(TABLE_ID)).isEqualTo(1);

        clock.addAndGet(MIN_AGE_MS);
        nextRound();
        assertThat(written).noneMatch(File::exists);
        assertThat(offsetsFile).doesNotExist();
        assertThat(liveFiles()).isNotEmpty().allMatch(File::exists);
        assertThat(committerState.failedCommits(TABLE_ID)).isZero();
    }

    /**
     * The commit had not landed when its call failed, but lands afterwards, as a request still in
     * flight may. The next round finds its snapshot and keeps its files.
     */
    @Test
    void testCommitThatLandsAfterTheFailureKeepsItsFiles() throws Exception {
        createTable("filesystem");
        commit(writeRow(0));
        List<CommitMessage> refused = refusedCommit();
        List<File> written = newFiles(refused);
        File offsetsFile = offsetsFile();
        String failedCommitUser;
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> committer = committer(true)) {
            failedCommitUser = ((PaimonLakeCommitter) committer).commitUser();
            assertThatThrownBy(() -> committer.commit(committableOf(refused), context(offsetsFile)))
                    .isInstanceOf(IOException.class)
                    .isNotInstanceOf(LakeCommitRejectedException.class);
        }
        // the commit's snapshot appears after the call failed
        commit(failedCommitUser, refused.subList(0, refused.size() - 1));

        clock.addAndGet(MIN_AGE_MS);
        nextRound();
        assertThat(written).isNotEmpty().allMatch(File::exists);
        assertThat(offsetsFile).exists();
        assertThat(committerState.failedCommits(TABLE_ID)).isZero();
    }

    /** A round soon after the failure decides nothing; a round once it is old enough does. */
    @Test
    void testFailedCommitIsDecidedOnlyOnceOldEnough() throws Exception {
        createTable("filesystem");
        commit(writeRow(0));
        List<CommitMessage> refused = refusedCommit();
        List<File> written = newFiles(refused);
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> committer = committer(true)) {
            assertThatThrownBy(() -> committer.commit(committableOf(refused), context(null)))
                    .isInstanceOf(IOException.class);
        }

        clock.addAndGet(MIN_AGE_MS - 1);
        nextRound();
        assertThat(written).allMatch(File::exists);
        assertThat(committerState.failedCommits(TABLE_ID)).isEqualTo(1);

        clock.addAndGet(1);
        nextRound();
        assertThat(written).noneMatch(File::exists);
    }

    /** The commit landed and then failed: its snapshot references its files, so they stay. */
    @Test
    void testCommitThatLandedAndThenFailedKeepsItsFiles() throws Exception {
        createTable("filesystem");
        commit(writeRow(0));
        List<CommitMessage> row = writeRow(1);
        long before = latestSnapshotId();

        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> committer = committer(true)) {
            // there is no Fluss cluster, so reading the readable snapshot fails after the commit
            assertThatThrownBy(() -> committer.commit(committableOf(row), context(null)))
                    .isInstanceOf(IOException.class)
                    .isNotInstanceOf(LakeCommitRejectedException.class);
        }
        Snapshot latest = table.snapshotManager().latestSnapshot();
        assertThat(latest.id()).isGreaterThan(before);
        assertThat(latest.commitUser())
                .startsWith(LakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER);
        assertThat(committerState.failedCommits(TABLE_ID)).isZero();
        clock.addAndGet(MIN_AGE_MS);
        nextRound();
        assertThat(newFiles(row)).isNotEmpty().allMatch(File::exists);
    }

    /**
     * A round's compaction conflicts with one that landed first. Paimon commits the round's data in
     * a snapshot of its own first, which lands, so nothing of the commit is deleted.
     */
    @Test
    void testCommitWhoseCompactionConflictsAfterItsDataLandedKeepsItsFiles() throws Exception {
        createTable("filesystem");
        List<CommitMessage> compaction = compactionThatConflicts();
        long before = latestSnapshotId();

        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> committer = committer(true)) {
            assertThatThrownBy(() -> committer.commit(committableOf(compaction), context(null)))
                    .isInstanceOf(IOException.class)
                    .isNotInstanceOf(LakeCommitRejectedException.class)
                    .hasStackTraceContaining("conflicts");
        }
        Snapshot latest = table.snapshotManager().latestSnapshot();
        assertThat(latest.id()).isEqualTo(before + 1);
        assertThat(latest.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
        assertThat(latest.commitUser())
                .startsWith(LakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER);
        assertThat(committerState.failedCommits(TABLE_ID)).isZero();
        assertThat(compactionOutputs(compaction)).isNotEmpty().allMatch(File::exists);
    }

    /**
     * A catalog with version management, e.g. REST, commits through the catalog, where a commit may
     * land after the call failed: nothing of a failed commit is ever deleted.
     */
    @Test
    void testCommitThroughAVersionManagedCatalogKeepsItsFiles() throws Exception {
        createTable(VersionManagedCatalogFactory.IDENTIFIER);
        assertThat(table.catalogEnvironment().supportsVersionManagement()).isTrue();
        List<CommitMessage> refused = refusedCommit();
        List<File> written = newFiles(refused);

        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> committer = committer(true)) {
            assertThatThrownBy(() -> committer.commit(committableOf(refused), context(null)))
                    .isInstanceOf(IOException.class)
                    .isNotInstanceOf(LakeCommitRejectedException.class);
        }
        assertThat(committerState.failedCommits(TABLE_ID)).isZero();
        clock.addAndGet(MIN_AGE_MS);
        nextRound();
        assertThat(written).isNotEmpty().allMatch(File::exists);
    }

    /** A table without deletion vectors fails as it did before, and keeps its files. */
    @Test
    void testCommitOfATableWithoutDeletionVectorsFailsAsBefore() throws Exception {
        createTable("filesystem");
        commit(writeRow(0));
        List<CommitMessage> refused = refusedCommit();

        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> committer = committer(false)) {
            assertThatThrownBy(() -> committer.commit(committableOf(refused), context(null)))
                    .isInstanceOf(IOException.class)
                    .isNotInstanceOf(LakeCommitRejectedException.class);
        }
        assertThat(committerState.failedCommits(TABLE_ID)).isZero();
    }

    /**
     * The outcome is read from the snapshots after the base. A later commit of another committer
     * does not hide one that landed; a changed base or a snapshot that is gone leave it unknown.
     */
    @Test
    void testOutcomeIsReadFromTheSnapshotsAfterTheBase() throws Exception {
        createTable("filesystem");
        commit(writeRow(0));
        SnapshotManager snapshots = table.snapshotManager();
        Snapshot base = snapshots.latestSnapshot();
        String ours = LakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER + "-ours";
        assertThat(outcomeSince(snapshots, ours, base)).isEqualTo(CommitOutcome.NOT_LANDED);

        commit(ours, writeRow(1));
        // another committer commits after ours
        commit(writeRow(2));
        assertThat(outcomeSince(snapshots, ours, base)).isEqualTo(CommitOutcome.LANDED);
        assertThat(outcomeSince(snapshots, "someone-else", base))
                .isEqualTo(CommitOutcome.NOT_LANDED);

        // the id names another snapshot now: rolled back past the base and committed again
        assertThat(
                        PaimonLakeCommitter.outcomeSince(
                                snapshots, "someone-else", base.id(), base.timeMillis() + 1))
                .isEqualTo(CommitOutcome.UNKNOWN);
        // a snapshot after the base is gone, so it may have been the commit
        snapshots.fileIO().delete(snapshots.snapshotPath(base.id() + 1), false);
        assertThat(outcomeSince(snapshots, "someone-else", base)).isEqualTo(CommitOutcome.UNKNOWN);
        // the base expired
        snapshots.fileIO().delete(snapshots.snapshotPath(base.id()), false);
        assertThat(outcomeSince(snapshots, "someone-else", base)).isEqualTo(CommitOutcome.UNKNOWN);
    }

    private static CommitOutcome outcomeSince(
            SnapshotManager snapshots, String commitUser, Snapshot base) throws IOException {
        return PaimonLakeCommitter.outcomeSince(
                snapshots, commitUser, base.id(), base.timeMillis());
    }

    private void createTable(String metastore) throws Exception {
        paimonConfig = new Configuration();
        paimonConfig.setString("metastore", metastore);
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
    }

    private LakeCommitter<PaimonWriteResult, PaimonCommittable> committer(boolean deletionVectors)
            throws Exception {
        TableInfo tableInfo =
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
                                .distributedBy(1)
                                .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                                .property(
                                        ConfigOptions.TABLE_DELETION_VECTORS_ENABLED,
                                        deletionVectors)
                                .build(),
                        "/remote",
                        1L,
                        1L);
        // no Fluss cluster: an empty client configuration
        return new PaimonLakeTieringFactory(paimonConfig, committerState)
                .createLakeCommitter(
                        new TieringCommitterInitContext(
                                TABLE_PATH, tableInfo, new Configuration(), new Configuration()));
    }

    private static LakeCommitter.CommitContext context(@Nullable File offsetsFile) {
        Map<TableBucket, Long> offsets = Collections.singletonMap(new TableBucket(TABLE_ID, 0), 1L);
        Map<String, String> properties =
                offsetsFile == null
                        ? Collections.emptyMap()
                        : Collections.singletonMap(
                                LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY,
                                offsetsFile.toURI().toString());
        return new LakeCommitter.CommitContext(properties, offsets);
    }

    /** An offsets file of a round, as Fluss writes it before the lake commit. */
    private File offsetsFile() throws IOException {
        File file = new File(warehouse, "offsets-" + UUID.randomUUID() + ".offsets");
        assertThat(file.createNewFile()).isTrue();
        return file;
    }

    /** The next round of the table: a committer of the same job commits a new row. */
    private void nextRound() throws Exception {
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> committer = committer(true)) {
            committer.commit(committableOf(writeRow(nextKey++)), context(null));
        } catch (IOException e) {
            // its own commit lands, then reading the readable snapshot fails without Fluss
        }
    }

    /**
     * A commit Paimon refuses before it writes a snapshot: a new row, and the deletion of a file
     * the table never added.
     */
    private List<CommitMessage> refusedCommit() throws Exception {
        List<CommitMessage> messages = new ArrayList<>(writeRow(1));
        DataFileMeta neverAdded =
                ((CommitMessageImpl) writeRow(2).get(0)).newFilesIncrement().newFiles().get(0);
        messages.add(
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        1,
                        new DataIncrement(
                                Collections.emptyList(),
                                Collections.singletonList(neverAdded),
                                Collections.emptyList()),
                        CompactIncrement.emptyIncrement()));
        return messages;
    }

    /**
     * A compaction of the table's files, prepared before another compaction of the same files
     * landed: Paimon refuses to commit it, as its input is gone.
     */
    private List<CommitMessage> compactionThatConflicts() throws Exception {
        commit(writeRow(0));
        commit(writeRow(1));
        CompactHelper helper = new CompactHelper(table, compactionTempDir);
        List<CommitMessage> late = helper.compactBucket(0).messages();
        helper.compactBucket(0).commit();
        assertThat(((CommitMessageImpl) late.get(0)).compactIncrement().compactBefore())
                .isNotEmpty();
        return late;
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
        commit("writer", messages);
    }

    private void commit(String user, List<CommitMessage> messages) throws Exception {
        FileStoreTable writeTable =
                table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));
        try (TableCommitImpl commit = writeTable.newCommit(user)) {
            commit.commit(messages);
        }
    }

    private static PaimonCommittable committableOf(List<CommitMessage> messages) {
        ManifestCommittable committable =
                new ManifestCommittable(BatchWriteBuilder.COMMIT_IDENTIFIER);
        messages.forEach(committable::addFileCommittable);
        return new PaimonCommittable(committable);
    }

    private long latestSnapshotId() {
        Long latest = table.snapshotManager().latestSnapshotId();
        return latest == null ? 0 : latest;
    }

    /** The files a compaction wrote; files it only moved to a higher level are not among them. */
    private List<File> compactionOutputs(List<CommitMessage> messages) {
        List<File> files = new ArrayList<>();
        for (CommitMessage message : messages) {
            CommitMessageImpl impl = (CommitMessageImpl) message;
            Set<String> inputs = new HashSet<>();
            impl.compactIncrement().compactBefore().forEach(f -> inputs.add(f.fileName()));
            for (DataFileMeta file : impl.compactIncrement().compactAfter()) {
                if (!inputs.contains(file.fileName())) {
                    files.add(fileOf(impl, file));
                }
            }
        }
        return files;
    }

    private List<File> newFiles(List<CommitMessage> messages) {
        List<File> files = new ArrayList<>();
        for (CommitMessage message : messages) {
            CommitMessageImpl impl = (CommitMessageImpl) message;
            impl.newFilesIncrement().newFiles().forEach(f -> files.add(fileOf(impl, f)));
        }
        return files;
    }

    private List<File> liveFiles() {
        List<File> files = new ArrayList<>();
        table.newSnapshotReader()
                .read()
                .dataSplits()
                .forEach(
                        split ->
                                split.dataFiles()
                                        .forEach(
                                                f ->
                                                        files.add(
                                                                fileOf(
                                                                        split.partition(),
                                                                        split.bucket(),
                                                                        f))));
        return files;
    }

    private File fileOf(CommitMessageImpl message, DataFileMeta file) {
        return fileOf(message.partition(), message.bucket(), file);
    }

    private File fileOf(BinaryRow partition, int bucket, DataFileMeta file) {
        return new File(
                table.store()
                        .pathFactory()
                        .createDataFilePathFactory(partition, bucket)
                        .toPath(file)
                        .toUri()
                        .getPath());
    }
}
