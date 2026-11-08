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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.CommitterInitContext;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.committer.PartitionMarkDoneCommitter;
import org.apache.fluss.lake.committer.SupportsPrimaryKeyMergeCommit;
import org.apache.fluss.lake.committer.TieringStats;
import org.apache.fluss.lake.paimon.tiering.markdone.PaimonPartitionMarkDone;
import org.apache.fluss.lake.paimon.tiering.markdone.PartitionMarkDoneState;
import org.apache.fluss.lake.paimon.tiering.markdone.PartitionMarkDoneStateJsonSerde;
import org.apache.fluss.lake.paimon.utils.DvTableReadableSnapshotRetriever;
import org.apache.fluss.lake.paimon.utils.PaimonSnapshotLineage;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SnapshotManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.fluss.lake.paimon.tiering.PaimonLakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.paimon.table.sink.BatchWriteBuilder.COMMIT_IDENTIFIER;

/**
 * Implementation of {@link LakeCommitter} for Paimon.
 *
 * <p>A failed {@link #commit} never deletes files and never throws {@link
 * org.apache.fluss.lake.committer.LakeCommitRejectedException}: Paimon raises the same exceptions
 * for a conflict and for an unknown outcome, and a commit request can still complete after the call
 * gave up on it. For a table with deletion vectors whose snapshots are committed by writing their
 * files (the catalog does no version management, see {@link
 * org.apache.paimon.table.CatalogEnvironment#supportsVersionManagement()}), the committer defers
 * the decision instead. It records the commit's user, which is its own (prefix and random id), the
 * latest snapshot before the commit and the files the commit wrote. A committer of a later round
 * decides once the failure is old enough for any request still in flight to have finished ({@link
 * PaimonCommitterState}): if the base is unchanged and every snapshot after it exists and has
 * another commit user, the commit provably never landed, and its files are deleted, with the Fluss
 * offsets file named in its own snapshot properties. Commits of other committers only add
 * snapshots, so they cannot hide this one. Anything else (the commit landed, a snapshot after the
 * base is gone, the base changed, a failed check) keeps the files. The record is in memory: a
 * failover loses it and leaves the files to the lake's orphan-file removal, as does any commit
 * through a catalog with version management, e.g. REST.
 */
public class PaimonLakeCommitter
        implements PartitionMarkDoneCommitter<PaimonWriteResult, PaimonCommittable>,
                SupportsPrimaryKeyMergeCommit {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonLakeCommitter.class);

    private final Catalog paimonCatalog;
    private final FileStoreTable fileStoreTable;
    private final String commitUser;
    private final TablePath tablePath;
    private final TablePath lakeTablePath;
    private final long tableId;
    private final Configuration flussClientConfig;
    private final TableInfo tableInfo;
    private final boolean markDoneEnabled;
    // keeps the failed commits whose files wait for a decision, see the class comment; null if
    // this committer defers no decision
    @Nullable private final PaimonCommitterState committerState;
    private TableCommitImpl tableCommit;

    private static final ThreadLocal<Long> currentCommitSnapshotId = new ThreadLocal<>();

    /** The consumer whose next snapshot is the readable snapshot Fluss registers. */
    public static final String READABLE_CONSUMER = "fluss-readable";
    /** The consumer whose next snapshot is the readable snapshot Fluss registered before. */
    public static final String PREVIOUS_READABLE_CONSUMER = "fluss-readable-previous";

    public PaimonLakeCommitter(
            PaimonCatalogProvider paimonCatalogProvider,
            CommitterInitContext committerInitContext,
            PaimonCommitterState committerState)
            throws IOException {
        this.paimonCatalog = paimonCatalogProvider.get();
        this.tableInfo = committerInitContext.tableInfo();
        this.tablePath = committerInitContext.tablePath();
        this.lakeTablePath = tableInfo.getLakeTablePath();
        this.tableId = tableInfo.getTableId();
        this.flussClientConfig = committerInitContext.flussClientConfig();
        this.fileStoreTable =
                getTable(
                        lakeTablePath,
                        tableInfo.getTableConfig().isDataLakeAutoExpireSnapshot()
                                || committerInitContext
                                        .lakeTieringConfig()
                                        .get(ConfigOptions.LAKE_TIERING_AUTO_EXPIRE_SNAPSHOT));
        this.commitUser = fileStoreTable.coreOptions().createCommitUser();
        // a commit through a catalog with version management may land after the call failed
        this.committerState =
                tableInfo.isDeletionVectorsEnabled()
                                && !fileStoreTable.catalogEnvironment().supportsVersionManagement()
                        ? committerState
                        : null;
        this.markDoneEnabled =
                committerInitContext
                                .lakeTieringConfig()
                                .get(ConfigOptions.LAKE_TIERING_PARTITION_MARK_DONE_ENABLED)
                        && PaimonPartitionMarkDone.isEnabled(fileStoreTable, tableInfo);
    }

    @Override
    public PaimonCommittable toCommittable(List<PaimonWriteResult> paimonWriteResults)
            throws IOException {
        ManifestCommittable committable = new ManifestCommittable(COMMIT_IDENTIFIER);
        for (PaimonWriteResult paimonWriteResult : paimonWriteResults) {
            for (CommitMessage commitMessage : paimonWriteResult.commitMessages()) {
                committable.addFileCommittable(commitMessage);
            }
        }
        return new PaimonCommittable(committable);
    }

    @Override
    public LakeCommitResult commit(PaimonCommittable committable, CommitContext context)
            throws IOException {
        ManifestCommittable manifestCommittable = committable.manifestCommittable();
        context.snapshotProperties().forEach(manifestCommittable::addProperty);

        if (committerState != null) {
            cleanUpFailedCommits(committerState);
        }
        // the latest snapshot before the commit, to tell later whether a failed commit landed
        BaseSnapshot base = committerState != null ? baseSnapshot() : null;
        currentCommitSnapshotId.remove();
        try {
            long committedSnapshotId = commit(manifestCommittable);

            // Collect cumulative table stats from the exact snapshot that was just committed.
            TieringStats stats = computeTableStats();
            return commitResult(committedSnapshotId, context.tieredLogEndOffsets(), stats);
        } catch (Throwable t) {
            if (committerState != null && base != null && notCommittedSince(base)) {
                // not landed yet: a later round decides on its files
                committerState.addFailedCommit(
                        tableId,
                        new PaimonCommitterState.FailedCommit(
                                commitUser,
                                base.id,
                                base.timeMillis,
                                filesWritten(manifestCommittable.fileCommittables()),
                                context.snapshotProperties()
                                        .get(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY),
                                committerState.nanoTime()));
                LOG.warn(
                        "The commit of {} failed and had not landed; a later round deletes its"
                                + " files if it still has not.",
                        tablePath,
                        t);
            }
            throw new IOException(t);
        } finally {
            currentCommitSnapshotId.remove();
        }
    }

    @Override
    public LakeCommitResult getCommitResult(
            long snapshotId, Map<TableBucket, Long> tieredLogEndOffsets) throws IOException {
        fileStoreTable.snapshotManager().invalidateCache();
        try {
            return commitResult(snapshotId, tieredLogEndOffsets, null);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /** The result of the commit of {@code committedSnapshotId} with the given tiered offsets. */
    private LakeCommitResult commitResult(
            long committedSnapshotId,
            Map<TableBucket, Long> tieredLogEndOffsets,
            @Nullable TieringStats stats)
            throws Exception {
        // deletion vector is disabled, committed snapshot is readable
        if (!fileStoreTable.coreOptions().deletionVectorsEnabled()) {
            return LakeCommitResult.committedIsReadable(committedSnapshotId, stats);
        }
        // retrieve the readable snapshot during commit
        try (DvTableReadableSnapshotRetriever retriever =
                new DvTableReadableSnapshotRetriever(
                        tablePath, tableId, fileStoreTable, flussClientConfig)) {
            DvTableReadableSnapshotRetriever.ReadableSnapshotResult readableSnapshotResult =
                    retriever.getReadableSnapshotAndOffsets(
                            committedSnapshotId, tieredLogEndOffsets);
            if (readableSnapshotResult == null) {
                return LakeCommitResult.unknownReadableSnapshot(committedSnapshotId, stats);
            }
            Long earliestSnapshotIdToKeep = readableSnapshotResult.getEarliestSnapshotIdToKeep();
            if (earliestSnapshotIdToKeep != null && earliestSnapshotIdToKeep >= 0) {
                LOG.info(
                        "earliest snapshot ID to keep for table {} is {}. "
                                + "Snapshots before this ID can be safely deleted from Fluss.",
                        tablePath,
                        earliestSnapshotIdToKeep);
            }
            return LakeCommitResult.withReadableSnapshot(
                    committedSnapshotId,
                    readableSnapshotResult.getReadableSnapshotId(),
                    readableSnapshotResult.getTieredOffsets(),
                    readableSnapshotResult.getReadableOffsets(),
                    earliestSnapshotIdToKeep,
                    stats);
        }
    }

    @Override
    public boolean preparePartitionMarkDone(PaimonCommittable committable) {
        if (!markDoneEnabled) {
            return false;
        }
        ManifestCommittable manifestCommittable = committable.manifestCommittable();
        String stateJson = null;
        boolean stateChanged = false;
        try (PaimonPartitionMarkDone partitionMarkDone =
                new PaimonPartitionMarkDone(fileStoreTable, tableInfo)) {
            CommittedLakeSnapshot latestCommit = loadLatestFlussCommit(null);
            stateJson =
                    latestCommit == null
                            ? null
                            : latestCommit
                                    .getSnapshotProperties()
                                    .get(PaimonPartitionMarkDone.MARK_DONE_STATE_PROPERTY);
            PartitionMarkDoneState previousState = parseMarkDoneState(stateJson);
            PartitionMarkDoneState newState =
                    partitionMarkDone.markIdlePartitionsDone(
                            previousState,
                            partitionMarkDone.extractTieredPartitions(manifestCommittable));
            stateChanged = !newState.equals(previousState);
            stateJson = PartitionMarkDoneStateJsonSerde.toJson(newState);
        } catch (Exception e) {
            stateChanged = false;
            LOG.warn(
                    "Failed to prepare partition mark-done for table {}, will retry in a later round.",
                    tablePath,
                    e);
        }
        if (stateJson != null) {
            manifestCommittable.addProperty(
                    PaimonPartitionMarkDone.MARK_DONE_STATE_PROPERTY, stateJson);
        }
        return stateChanged;
    }

    private PartitionMarkDoneState parseMarkDoneState(@Nullable String markDoneStateJson) {
        if (markDoneStateJson == null) {
            return PartitionMarkDoneState.empty();
        }
        try {
            return PartitionMarkDoneStateJsonSerde.fromJson(markDoneStateJson);
        } catch (UnsupportedVersionException e) {
            // The caller skips mark-done and retains the original JSON for data commits.
            throw e;
        } catch (Exception e) {
            LOG.warn(
                    "Corrupt mark-done state of table {}, re-initializing via cold start.",
                    tablePath,
                    e);
            return PartitionMarkDoneState.empty();
        }
    }

    /**
     * Decides on the failed commits of this table that are old enough: deletes the files and the
     * offsets file of each that never landed, and keeps those of any other.
     */
    private void cleanUpFailedCommits(PaimonCommitterState state) {
        for (PaimonCommitterState.FailedCommit failed : state.takeDue(tableId)) {
            CommitOutcome outcome;
            try {
                outcome =
                        outcomeSince(
                                fileStoreTable.snapshotManager(),
                                failed.commitUser,
                                failed.baseId,
                                failed.baseTimeMillis);
            } catch (Exception e) {
                LOG.warn(
                        "Could not tell whether a failed commit of {} landed; keeping its files.",
                        tablePath,
                        e);
                continue;
            }
            if (outcome != CommitOutcome.NOT_LANDED) {
                LOG.info(
                        "A failed commit of {} by {} is {}; keeping its files.",
                        tablePath,
                        failed.commitUser,
                        outcome);
                continue;
            }
            try {
                for (String file : failed.files) {
                    fileStoreTable.fileIO().deleteQuietly(new Path(file));
                }
                if (failed.offsetsFile != null) {
                    FsPath offsetsFile = new FsPath(failed.offsetsFile);
                    offsetsFile.getFileSystem().delete(offsetsFile, false);
                }
                LOG.info(
                        "Deleted the files of a failed commit of {} by {}, which never landed.",
                        tablePath,
                        failed.commitUser);
            } catch (Exception e) {
                LOG.warn("Could not delete the files of a failed commit of {}.", tablePath, e);
            }
        }
    }

    /** The commit user of this committer, unique to it. */
    @VisibleForTesting
    String commitUser() {
        return commitUser;
    }

    private static final long NO_SNAPSHOT = 0L;

    /** The latest snapshot before a commit: its id ({@link #NO_SNAPSHOT} if none) and time. */
    private static final class BaseSnapshot {
        final long id;
        final long timeMillis;

        BaseSnapshot(long id, long timeMillis) {
            this.id = id;
            this.timeMillis = timeMillis;
        }
    }

    /** The latest snapshot of the table, read from its files; null if it cannot be read. */
    @Nullable
    private BaseSnapshot baseSnapshot() {
        try {
            SnapshotManager snapshots = fileStoreTable.snapshotManager();
            Long latestId = snapshots.latestSnapshotIdFromFileSystem();
            if (latestId == null) {
                return new BaseSnapshot(NO_SNAPSHOT, 0L);
            }
            Snapshot latest = readSnapshot(snapshots, latestId);
            return new BaseSnapshot(latest.id(), latest.timeMillis());
        } catch (Exception e) {
            LOG.warn(
                    "Could not read the latest snapshot of {} before its commit; a failed commit"
                            + " keeps its files.",
                    tablePath,
                    e);
            return null;
        }
    }

    /**
     * Whether no commit of this committer landed after {@code base}, read from the snapshot files.
     * False if the outcome is unknown or the snapshots cannot be read.
     */
    private boolean notCommittedSince(BaseSnapshot base) {
        try {
            return outcomeSince(
                            fileStoreTable.snapshotManager(), commitUser, base.id, base.timeMillis)
                    == CommitOutcome.NOT_LANDED;
        } catch (Exception e) {
            LOG.warn(
                    "Could not tell whether the failed commit of {} landed; keeping its files.",
                    tablePath,
                    e);
            return false;
        }
    }

    /** What the snapshots after a commit's base tell of the commit. */
    @VisibleForTesting
    enum CommitOutcome {
        /** No snapshot after the base is of the commit user, and none is gone. */
        NOT_LANDED,
        /** A snapshot after the base is of the commit user. */
        LANDED,
        /** The base changed (a rollback), or a snapshot after it is gone (expired). */
        UNKNOWN
    }

    /**
     * Reads the snapshots after the base {@code baseId} (committed at {@code baseTimeMillis};
     * {@link #NO_SNAPSHOT} if there was none) for one of {@code commitUser}. Snapshot ids are
     * contiguous and commits only add snapshots, so a later commit of another user cannot hide one
     * of {@code commitUser}. Expiration deletes the oldest snapshots first, so an expired commit of
     * {@code commitUser} takes the base with it, and a rollback past the base changes or removes
     * it.
     */
    @VisibleForTesting
    static CommitOutcome outcomeSince(
            SnapshotManager snapshots, String commitUser, long baseId, long baseTimeMillis)
            throws IOException {
        if (baseId != NO_SNAPSHOT) {
            Snapshot base;
            try {
                base = readSnapshot(snapshots, baseId);
            } catch (FileNotFoundException e) {
                return CommitOutcome.UNKNOWN;
            }
            if (base.timeMillis() != baseTimeMillis) {
                return CommitOutcome.UNKNOWN; // rolled back past the base and committed again
            }
        }
        Long latest = snapshots.latestSnapshotIdFromFileSystem();
        if (latest == null) {
            return baseId == NO_SNAPSHOT ? CommitOutcome.NOT_LANDED : CommitOutcome.UNKNOWN;
        }
        if (latest < baseId) {
            return CommitOutcome.UNKNOWN;
        }
        for (long id = latest; id > baseId; id--) {
            Snapshot snapshot;
            try {
                snapshot = readSnapshot(snapshots, id);
            } catch (FileNotFoundException e) {
                return CommitOutcome.UNKNOWN; // expired: it may have been the commit
            }
            if (commitUser.equals(snapshot.commitUser())) {
                return CommitOutcome.LANDED;
            }
        }
        return CommitOutcome.NOT_LANDED;
    }

    /** Reads a snapshot from its file, past the snapshot cache, which may hold a reused id. */
    private static Snapshot readSnapshot(SnapshotManager snapshots, long id)
            throws FileNotFoundException {
        return SnapshotManager.tryFromPath(snapshots.fileIO(), snapshots.snapshotPath(id));
    }

    /** Commits a Paimon snapshot and returns its ID recorded by {@link PaimonCommitCallback}. */
    private long commit(ManifestCommittable manifestCommittable) throws Exception {
        // clear any residue left by a previous failed commit on the same thread
        currentCommitSnapshotId.remove();
        try {
            tableCommit = fileStoreTable.newCommit(commitUser);
            // don't skip empty commits: tiering relies on empty snapshots to persist bucket
            // offsets when only empty WAL batches were consumed, and partition mark-done
            // commits properties-only snapshots
            tableCommit.ignoreEmptyCommit(false);
            tableCommit.commit(manifestCommittable);
            return checkNotNull(
                    currentCommitSnapshotId.get(),
                    "Paimon committed snapshot id must be non-null.");
        } finally {
            currentCommitSnapshotId.remove();
        }
    }

    /** Computes cumulative table stats from the latest snapshot by REST API. */
    @Nullable
    private TieringStats computeTableStats() {
        Identifier identifier =
                new Identifier(lakeTablePath.getDatabaseName(), lakeTablePath.getTableName());
        try {
            Optional<TableSnapshot> snapshot = paimonCatalog.loadSnapshot(identifier);
            if (!snapshot.isPresent()) {
                LOG.warn(
                        "No snapshot found for table {}, "
                                + "fileSize and recordCount will be reported as -1.",
                        tablePath);
                return null;
            }
            TableSnapshot tableSnapshot = snapshot.get();
            return new TieringStats(tableSnapshot.fileSizeInBytes(), tableSnapshot.recordCount());
        } catch (Exception e) {
            LOG.debug(
                    "Failed to load snapshot for table {}, "
                            + "fileSize and recordCount will be reported as -1.",
                    tablePath,
                    e);
            return null;
        }
    }

    @Override
    public void abort(PaimonCommittable committable) throws IOException {
        tableCommit = fileStoreTable.newCommit(commitUser);
        tableCommit.abort(
                withoutUpgradedFiles(committable.manifestCommittable().fileCommittables()));
    }

    /**
     * The commit messages without the files a compaction only moved to a higher level. Such a file
     * keeps its name, so it is both an input and an output of the compaction, and it stays in the
     * table if the commit does not land. Paimon's abort deletes every compaction output, so it
     * would delete the file and lose its rows.
     */
    private static List<CommitMessage> withoutUpgradedFiles(List<CommitMessage> messages) {
        List<CommitMessage> result = new ArrayList<>(messages.size());
        for (CommitMessage message : messages) {
            CommitMessageImpl impl = (CommitMessageImpl) message;
            CompactIncrement compaction = impl.compactIncrement();
            Set<String> inputs = new HashSet<>();
            for (DataFileMeta file : compaction.compactBefore()) {
                inputs.add(file.fileName());
            }
            List<DataFileMeta> outputs = new ArrayList<>();
            for (DataFileMeta file : compaction.compactAfter()) {
                if (!inputs.contains(file.fileName())) {
                    outputs.add(file);
                }
            }
            if (outputs.size() == compaction.compactAfter().size()) {
                result.add(message);
                continue;
            }
            result.add(
                    new CommitMessageImpl(
                            impl.partition(),
                            impl.bucket(),
                            impl.totalBuckets(),
                            impl.newFilesIncrement(),
                            new CompactIncrement(
                                    compaction.compactBefore(),
                                    outputs,
                                    compaction.changelogFiles(),
                                    compaction.newIndexFiles(),
                                    compaction.deletedIndexFiles()),
                            impl.checkFromSnapshot()));
        }
        return result;
    }

    /**
     * The paths of the files the commit messages wrote, as Paimon's abort selects them: new data,
     * changelog and index files, and compaction outputs with their extra files, without the files a
     * compaction only moved to a higher level.
     */
    private List<String> filesWritten(List<CommitMessage> messages) {
        FileStorePathFactory pathFactory = fileStoreTable.store().pathFactory();
        List<String> files = new ArrayList<>();
        for (CommitMessage message : withoutUpgradedFiles(messages)) {
            CommitMessageImpl impl = (CommitMessageImpl) message;
            DataFilePathFactory dataFiles =
                    pathFactory.createDataFilePathFactory(impl.partition(), impl.bucket());
            List<DataFileMeta> written = new ArrayList<>();
            written.addAll(impl.newFilesIncrement().newFiles());
            written.addAll(impl.newFilesIncrement().changelogFiles());
            written.addAll(impl.compactIncrement().compactAfter());
            written.addAll(impl.compactIncrement().changelogFiles());
            for (DataFileMeta file : written) {
                for (Path path : file.collectFiles(dataFiles)) {
                    files.add(path.toString());
                }
            }
            IndexPathFactory indexFiles =
                    pathFactory.indexFileFactory(impl.partition(), impl.bucket());
            List<IndexFileMeta> writtenIndexFiles = new ArrayList<>();
            writtenIndexFiles.addAll(impl.newFilesIncrement().newIndexFiles());
            writtenIndexFiles.addAll(impl.compactIncrement().newIndexFiles());
            for (IndexFileMeta file : writtenIndexFiles) {
                files.add(indexFiles.toPath(file).toString());
            }
        }
        return files;
    }

    @Nullable
    @Override
    public CommittedLakeSnapshot getMissingLakeSnapshot(@Nullable Long latestLakeSnapshotIdOfFluss)
            throws IOException {
        CommittedLakeSnapshot latestCommit = loadLatestFlussCommit(latestLakeSnapshotIdOfFluss);
        if (latestCommit != null
                && !latestCommit
                        .getSnapshotProperties()
                        .containsKey(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY)) {
            throw new IOException(
                    String.format(
                            "Cannot recover tiering offsets for Paimon table %s (Fluss table %s, tableId=%s): "
                                    + "no recoverable snapshot containing required property '%s' was found. "
                                    + "Latest Fluss-committed Paimon snapshot ID: %s; Fluss-known snapshot ID: %s.",
                            lakeTablePath,
                            tablePath,
                            tableId,
                            FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY,
                            latestCommit.getLakeSnapshotId(),
                            latestLakeSnapshotIdOfFluss));
        }
        return latestCommit;
    }

    /**
     * Loads the latest Fluss snapshot ID and its commit properties. An empty properties map means
     * the commit exists but its metadata is unavailable; null means no newer Fluss commit exists.
     */
    @Nullable
    private CommittedLakeSnapshot loadLatestFlussCommit(@Nullable Long knownSnapshotId)
            throws IOException {
        Snapshot latestSnapshot = getCommittedLatestSnapshotOfLake();
        // we get the latest snapshot committed by fluss,
        // but the latest snapshot is not greater than knownSnapshotId, no any missing
        // snapshot, return directly
        if (latestSnapshot == null
                || (knownSnapshotId != null && latestSnapshot.id() <= knownSnapshotId)) {
            return null;
        }
        Snapshot propertiesSnapshot = findLatestSnapshotWithOffsets(latestSnapshot);
        return new CommittedLakeSnapshot(
                latestSnapshot.id(),
                propertiesSnapshot == null
                        ? Collections.emptyMap()
                        : propertiesSnapshot.properties());
    }

    @Nullable
    private Snapshot getCommittedLatestSnapshotOfLake() throws IOException {
        // get the latest snapshot committed by fluss or latest committed id
        SnapshotManager snapshotManager = fileStoreTable.snapshotManager();
        Long userCommittedSnapshotIdOrLatestCommitId =
                fileStoreTable
                        .snapshotManager()
                        .pickOrLatest(
                                snapshot -> isFlussLakeTieringCommitUser(snapshot.commitUser()));
        // no any snapshot, return null directly
        if (userCommittedSnapshotIdOrLatestCommitId == null) {
            return null;
        }

        // pick the snapshot
        Snapshot snapshot = snapshotManager.tryGetSnapshot(userCommittedSnapshotIdOrLatestCommitId);

        if (!isFlussLakeTieringCommitUser(snapshot.commitUser())) {
            // the snapshot is still not committed by Fluss, return directly
            return null;
        }
        return snapshot;
    }

    /**
     * Every Fluss commit advancing tiering offsets persists the offsets property. Later Paimon
     * maintenance snapshots do not advance those offsets and may omit the property.
     */
    @Nullable
    private Snapshot findLatestSnapshotWithOffsets(Snapshot latestSnapshot) throws IOException {
        SnapshotManager snapshotManager = fileStoreTable.snapshotManager();
        Long earliestId = snapshotManager.earliestSnapshotId();
        if (earliestId == null) {
            return null;
        }
        for (long id = latestSnapshot.id(); id >= earliestId; id--) {
            try {
                Snapshot snapshot =
                        id == latestSnapshot.id()
                                ? latestSnapshot
                                : snapshotManager.tryGetSnapshot(id);
                if (!isFlussLakeTieringCommitUser(snapshot.commitUser())) {
                    continue;
                }
                Map<String, String> properties = snapshot.properties();
                if (properties != null
                        && properties.containsKey(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY)) {
                    return snapshot;
                }
                // A legacy data commit without offsets must not borrow an older round's offsets.
                if (snapshot.commitKind() == Snapshot.CommitKind.APPEND) {
                    return null;
                }
            } catch (FileNotFoundException ignored) {
                // The snapshot may have expired during the lookup.
            }
        }
        return null;
    }

    @Override
    public void checkCurrentSnapshotDescendsFrom(long snapshotId) throws IOException {
        // Snapshot ids are reused after a rollback, so no snapshot is read from a cache.
        fileStoreTable.snapshotManager().invalidateCache();
        PaimonSnapshotLineage.checkDescendsFrom(
                fileStoreTable, lakeTablePath.toString(), tableId, snapshotId);
    }

    /**
     * Returns whether a COMPACT snapshot follows {@code snapshotId} in a table with deletion
     * vectors. The readable snapshot is the latest COMPACT one, so another engine's compaction of
     * an idle table may make a newer snapshot readable.
     */
    @Override
    public boolean readableSnapshotMayAdvanceSince(long snapshotId) throws IOException {
        if (!fileStoreTable.coreOptions().deletionVectorsEnabled()) {
            return false;
        }
        SnapshotManager snapshotManager = fileStoreTable.snapshotManager();
        snapshotManager.invalidateCache();
        Long latestId = snapshotManager.latestSnapshotId();
        if (latestId == null) {
            return false;
        }
        for (long id = latestId; id > snapshotId; id--) {
            try {
                Snapshot snapshot = snapshotManager.tryGetSnapshot(id);
                if (snapshot.commitKind() == Snapshot.CommitKind.COMPACT) {
                    return true;
                }
            } catch (FileNotFoundException ignored) {
                // expired meanwhile
            }
        }
        return false;
    }

    /**
     * Keeps the readable snapshot Fluss registers next, and the one it registered before, from
     * expiring. Union reads plan on the registered one, and reads planned before the registration
     * still read the one before it. Paimon expires no snapshot at or after the next snapshot of a
     * consumer, so the snapshots in between stay too until the readable snapshot moves on.
     */
    @Override
    public void protectReadableSnapshot(long snapshotId) {
        try {
            ConsumerManager consumers = fileStoreTable.consumerManager();
            Optional<Consumer> registered = consumers.consumer(READABLE_CONSUMER);
            if (registered.isPresent() && registered.get().nextSnapshot() == snapshotId) {
                return;
            }
            consumers.resetConsumer(
                    PREVIOUS_READABLE_CONSUMER, registered.orElse(new Consumer(snapshotId)));
            consumers.resetConsumer(READABLE_CONSUMER, new Consumer(snapshotId));
        } catch (Exception e) {
            LOG.warn(
                    "Could not keep readable snapshot {} of {} from expiring.",
                    snapshotId,
                    tablePath,
                    e);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (tableCommit != null) {
                tableCommit.close();
            }
            if (paimonCatalog != null) {
                paimonCatalog.close();
            }
        } catch (Exception e) {
            throw new IOException("Failed to close PaimonLakeCommitter.", e);
        }
    }

    private FileStoreTable getTable(TablePath tablePath, boolean isAutoSnapshotExpiration)
            throws IOException {
        try {
            FileStoreTable table = (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));

            Map<String, String> dynamicOptions = new HashMap<>();
            dynamicOptions.put(
                    CoreOptions.COMMIT_CALLBACKS.key(),
                    PaimonLakeCommitter.PaimonCommitCallback.class.getName());
            dynamicOptions.put(
                    CoreOptions.COMMIT_USER_PREFIX.key(), FLUSS_LAKE_TIERING_COMMIT_USER);

            boolean writeOnly = !isAutoSnapshotExpiration;
            dynamicOptions.put(CoreOptions.WRITE_ONLY.key(), Boolean.toString(writeOnly));

            // For non-write-only modes, we enable 'end-input.check-partition-expire' to ensure
            // Paimon triggers partition expiration on every commit.
            // Note: This is necessary even if 'paimon.partition.expiration-check-interval' is
            // already configured. Because the Fluss tiering service creates a fresh TableCommit
            // instance for each commit, the interval-based expiration check will not be triggered
            // correctly otherwise.
            if (!writeOnly) {
                dynamicOptions.put(
                        CoreOptions.END_INPUT_CHECK_PARTITION_EXPIRE.key(),
                        Boolean.TRUE.toString());
            }

            return table.copy(dynamicOptions);
        } catch (Exception e) {
            throw new IOException("Failed to get table " + tablePath + " in Paimon.", e);
        }
    }

    private static boolean isFlussLakeTieringCommitUser(String commitUser) {
        return commitUser.startsWith(FLUSS_LAKE_TIERING_COMMIT_USER);
    }

    /** A {@link CommitCallback} to save paimon commit snapshot info. */
    public static class PaimonCommitCallback implements CommitCallback {

        @Override
        public void call(Context context) {
            currentCommitSnapshotId.set(context.snapshot.id());
        }

        @Override
        public void retry(ManifestCommittable manifestCommittable) {
            // do-nothing
        }

        @Override
        public void close() throws Exception {
            // do-nothing
        }
    }
}
