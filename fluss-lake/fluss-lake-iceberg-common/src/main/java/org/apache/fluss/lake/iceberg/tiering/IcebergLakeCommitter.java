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

import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.LakeBaseChangedException;
import org.apache.fluss.lake.committer.LakeCommitRejectedException;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.committer.SupportsPositionDeleteCommit;
import org.apache.fluss.lake.dv.FormatVersions;
import org.apache.fluss.lake.iceberg.maintenance.IcebergRewriteDataFiles;
import org.apache.fluss.lake.iceberg.maintenance.RewriteDataFileResult;
import org.apache.fluss.lake.iceberg.tiering.writer.IcebergDvFileWriter;
import org.apache.fluss.lake.iceberg.tiering.writer.IcebergPositionDeleteFileWriter;
import org.apache.fluss.lake.iceberg.utils.IcebergSnapshotLineage;
import org.apache.fluss.metadata.TablePath;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.events.Listener;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.lake.writer.LakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;

/** Implementation of {@link LakeCommitter} for Iceberg. */
public class IcebergLakeCommitter
        implements LakeCommitter<IcebergWriteResult, IcebergCommittable>,
                SupportsPositionDeleteCommit<IcebergWriteResult, IcebergCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergLakeCommitter.class);

    private static final String COMMITTER_USER = IcebergSnapshotLineage.COMMIT_USER_PROPERTY;

    // A rewrite committed on its own, before a round's deletes, carries no bucket offsets. Its
    // distinct user keeps getMissingLakeSnapshot from taking it for a tiered data snapshot.
    private static final String FLUSS_LAKE_COMPACTION_COMMIT_USER =
            IcebergSnapshotLineage.FLUSS_COMPACTION_COMMIT_USER;

    private final Catalog icebergCatalog;
    private final Table icebergTable;
    private final boolean autoExpireSnapshots;
    private final IcebergCommitterState state;
    // The table's current snapshot when this committer loaded the table, null if none. The round
    // plans on it, and a commit whose base is not in the lake is fenced on it.
    @Nullable private Long plannedSnapshotId;
    private static final ThreadLocal<Long> currentCommitSnapshotId = new ThreadLocal<>();

    // The defaults of Paimon's snapshot.time-retained and snapshot.num-retained.min, which Fluss's
    // Paimon tables expire with.
    private static final long DEFAULT_MAX_SNAPSHOT_AGE_MS = 60 * 60_000L;
    private static final int DEFAULT_MIN_SNAPSHOTS_TO_KEEP = 10;

    static {
        Listeners.register(new IcebergSnapshotCreateListener(), CreateSnapshotEvent.class);
    }

    public IcebergLakeCommitter(IcebergCatalogProvider icebergCatalogProvider, TablePath tablePath)
            throws IOException {
        this(
                icebergCatalogProvider,
                tablePath,
                false,
                new IcebergCommitterState(IcebergCommitterState.DEFAULT_EXPIRE_INTERVAL_MS));
    }

    /**
     * Creates the committer of a table. With {@code autoExpireSnapshots}, it expires the table's
     * old snapshots after its commits. It keeps what it learns for the next rounds in {@code
     * state}.
     */
    IcebergLakeCommitter(
            IcebergCatalogProvider icebergCatalogProvider,
            TablePath tablePath,
            boolean autoExpireSnapshots,
            IcebergCommitterState state)
            throws IOException {
        this.icebergCatalog = icebergCatalogProvider.get();
        this.icebergTable = getTable(tablePath);
        this.autoExpireSnapshots = autoExpireSnapshots;
        this.state = state;
        Snapshot current = icebergTable.currentSnapshot();
        this.plannedSnapshotId = current == null ? null : current.snapshotId();
    }

    @Override
    public IcebergCommittable toCommittable(List<IcebergWriteResult> icebergWriteResults) {
        // Aggregate all write results into a single committable
        IcebergCommittable.Builder builder =
                IcebergCommittable.builder().addSpecs(icebergTable.specs());

        for (IcebergWriteResult result : icebergWriteResults) {
            WriteResult writeResult = result.getWriteResult();

            // Add data files
            for (DataFile dataFile : writeResult.dataFiles()) {
                builder.addDataFile(dataFile);
            }
            // Add delete files
            for (DeleteFile deleteFile : writeResult.deleteFiles()) {
                builder.addDeleteFile(deleteFile);
            }

            RewriteDataFileResult rewriteDataFileResult = result.rewriteDataFileResult();
            if (rewriteDataFileResult != null) {
                builder.addRewriteDataFileResult(rewriteDataFileResult);
            }
        }

        return builder.build();
    }

    @Override
    public LakeCommitResult commit(IcebergCommittable committable, CommitContext context)
            throws IOException {
        Map<String, String> snapshotProperties = context.snapshotProperties();
        try {
            // Refresh table to get latest metadata
            icebergTable.refresh();

            SnapshotUpdate<?> snapshotUpdate;
            if (committable.getDeleteFiles().isEmpty()) {
                // Simple append-only case: only data files, no delete files or compaction
                AppendFiles appendFiles = icebergTable.newAppend();
                committable.getDataFiles().forEach(appendFiles::appendFile);
                snapshotUpdate = appendFiles;
            } else {
                /*
                 Row delta validations are not needed for streaming changes that write equality
                 deletes. Equality deletes are applied to data in all previous sequence numbers,
                 so retries may push deletes further in the future, but do not affect correctness.
                 Position deletes committed to the table in this path are used only to delete rows
                 from data files that are being added in this commit. There is no way for data
                 files added along with the delete files to be concurrently removed, so there is
                 no need to validate the files referenced by the position delete files that are
                 being committed.
                */
                RowDelta rowDelta = icebergTable.newRowDelta();
                committable.getDataFiles().forEach(rowDelta::addRows);
                committable.getDeleteFiles().forEach(rowDelta::addDeletes);
                snapshotUpdate = rowDelta;
            }

            // commit written files
            long snapshotId = commit(snapshotUpdate, snapshotProperties);

            // There exists rewrite files, commit rewrite files
            List<RewriteDataFileResult> rewriteDataFileResults =
                    committable.rewriteDataFileResults();
            if (!rewriteDataFileResults.isEmpty()) {
                Long rewriteCommitSnapshotId =
                        commitRewrite(
                                rewriteDataFileResults,
                                snapshotProperties,
                                FLUSS_LAKE_TIERING_COMMIT_USER,
                                false);
                if (rewriteCommitSnapshotId != null) {
                    snapshotId = rewriteCommitSnapshotId;
                }
            }
            // Iceberg does not provide cumulative table stats API yet; leave stats as -1 (unknown).
            return LakeCommitResult.committedIsReadable(snapshotId);
        } catch (Exception e) {
            throw new IOException("Failed to commit to Iceberg table.", e);
        }
    }

    @Override
    @Nullable
    public Long commitPendingRewrite(IcebergCommittable committable) {
        if (committable.rewriteDataFileResults().isEmpty()) {
            return null;
        }
        // Runs before the round's deletes and lands only on the snapshot the compaction read: its
        // writers may have handed the positions of the rewritten rows to the index as a change
        // from that snapshot. If another commit changed data files since, the rewrite fails and is
        // dropped, which is safe.
        // The compacted buckets tell the next rounds' writers when each bucket was compacted.
        Set<String> buckets = new TreeSet<>();
        for (RewriteDataFileResult result : committable.rewriteDataFileResults()) {
            if (result.bucketKey() != null) {
                buckets.add(result.bucketKey());
            }
        }
        Map<String, String> properties = new HashMap<>();
        if (!buckets.isEmpty()) {
            properties.put(
                    IcebergRewriteDataFiles.COMPACTED_BUCKETS_PROPERTY, String.join(",", buckets));
            FormatVersions.putSnapshotVersion(properties);
        }
        Long id =
                commitRewrite(
                        committable.rewriteDataFileResults(),
                        properties,
                        FLUSS_LAKE_COMPACTION_COMMIT_USER,
                        true);
        LOG.info("Committed a Fluss compaction rewrite as snapshot {}.", id);
        Snapshot rewrite = id == null ? null : icebergTable.snapshot(id);
        if (rewrite != null && Objects.equals(rewrite.parentId(), plannedSnapshotId)) {
            // The round's own rewrite: the round plans on top of it.
            plannedSnapshotId = id;
        }
        return id;
    }

    /**
     * Commits this round's data files together with the deletes of the base rows they supersede, so
     * every snapshot is consistent at its tiered offsets.
     */
    @Override
    public LakeCommitResult commitWithDeletes(
            IcebergCommittable committable,
            Map<String, String> snapshotProperties,
            Map<String, long[]> deletedPositionsByFile,
            long baseSnapshotId)
            throws IOException {
        List<DeleteFile> newDvs = new ArrayList<>();
        boolean commitAttempted = false;
        // the snapshot the commit is fenced on, null for a table without data
        Long fence = plannedSnapshotId;
        try {
            icebergTable.refresh();
            boolean baseExists =
                    baseSnapshotId > 0 && icebergTable.snapshot(baseSnapshotId) != null;
            if (baseExists) {
                fence = baseSnapshotId;
                Snapshot current = icebergTable.currentSnapshot();
                if (current != null
                        && !IcebergSnapshotLineage.historyReaches(
                                icebergTable, current, baseSnapshotId)) {
                    // Iceberg cannot validate the commit across a snapshot that expired since the
                    // base, and fails it as if its outcome were unknown. The round runs again
                    // once the index moved to the current snapshot, or stops on a rollback.
                    throw new LakeBaseChangedException(
                            String.format(
                                    "Cannot commit deletion vectors to Iceberg table %s: the history of"
                                            + " its current snapshot %d does not reach base snapshot %d; a snapshot"
                                            + " in between expired, or the table was rolled back.",
                                    icebergTable.name(), current.snapshotId(), baseSnapshotId),
                            null);
                }
            } else if (baseSnapshotId <= 0) {
                // Nothing is registered yet, so the index has only what this tiering commits.
                // Rows another engine wrote before would stay live next to the round's.
                IcebergSnapshotLineage.checkNoExternalWrites(
                        icebergTable,
                        plannedSnapshotId == null
                                ? null
                                : icebergTable.snapshot(plannedSnapshotId));
            }
            // The deletes and the round's index change are relative to the base. So the commit
            // lands on the base, or on snapshots after it that changed no data file; otherwise
            // Iceberg's retry would rebase it on a rewrite of other files. When the base is not in
            // the lake (the table's first round, or the base expired), the commit lands on the
            // snapshot the round planned on. A commit by another tiering service since then, e.g.
            // one the table was handed to, fails it.
            RowDelta rowDelta = IcebergBaseFence.on(icebergTable, fence).newRowDelta();
            committable.getDataFiles().forEach(rowDelta::addRows);
            List<DeleteFile> superseded = new ArrayList<>();
            List<CharSequence> referenced = new ArrayList<>();
            if (!deletedPositionsByFile.isEmpty()) {
                if (baseSnapshotId <= 0 || icebergTable.snapshot(baseSnapshotId) == null) {
                    throw new IOException(
                            "Base snapshot "
                                    + baseSnapshotId
                                    + " is not found while deletes are pending");
                }
                // The base's files the deletes reference, with their delete files. The index reads
                // only the files the snapshots since the last round changed, not the whole base.
                LiveFilesIndex live =
                        LiveFilesIndex.of(state.liveFiles, icebergTable, baseSnapshotId);
                if (!live.hasFiles(deletedPositionsByFile.keySet())) {
                    // A cached file list may lag behind the base, so plan it again first.
                    live = LiveFilesIndex.rescan(state.liveFiles, icebergTable, baseSnapshotId);
                }
                Map<String, DataFile> fileByBase = new HashMap<>();
                Map<String, List<DeleteFile>> existingDvs = new HashMap<>();
                for (String name : deletedPositionsByFile.keySet()) {
                    DataFile file = live.file(name);
                    if (file == null) {
                        // The index of the base holds rows of the base's files only. Committing
                        // without these deletes would leave the old rows live next to the new.
                        throw new IOException(
                                String.format(
                                        "Cannot commit deletion vectors: the index of base snapshot %d has"
                                                + " rows in data file %s, which the base does not have; the round is not"
                                                + " committed.",
                                        baseSnapshotId, name));
                    }
                    fileByBase.put(name, file);
                    existingDvs.put(file.location(), live.deletesOf(file.location()));
                }
                DeleteLoader deleteLoader =
                        new BaseDeleteLoader(df -> icebergTable.io().newInputFile(df.location()));
                Function<String, PositionDeleteIndex> loadPrevious =
                        path -> {
                            List<DeleteFile> prev = positionDeletesOf(existingDvs.get(path));
                            return prev.isEmpty()
                                    ? null
                                    : deleteLoader.loadPositionDeletes(prev, path);
                        };
                // v3 tables get deletion vectors, v2 tables file-scoped position delete files.
                // Either replaces the data file's previous one, so deletes never pile up.
                boolean deletionVectors = TableUtil.formatVersion(icebergTable) >= 3;
                DeleteWriteResult dvResult;
                if (deletionVectors) {
                    try (IcebergDvFileWriter dvWriter =
                            new IcebergDvFileWriter(icebergTable, 0, loadPrevious)) {
                        for (Map.Entry<String, long[]> e : deletedPositionsByFile.entrySet()) {
                            DataFile df = fileByBase.get(e.getKey());
                            dvWriter.delete(df, e.getValue());
                            referenced.add(df.location());
                        }
                        dvResult = dvWriter.complete();
                    }
                } else {
                    try (IcebergPositionDeleteFileWriter posWriter =
                            new IcebergPositionDeleteFileWriter(
                                    icebergTable,
                                    0,
                                    loadPrevious,
                                    path -> positionDeletesOf(existingDvs.get(path)))) {
                        for (Map.Entry<String, long[]> e : deletedPositionsByFile.entrySet()) {
                            DataFile df = fileByBase.get(e.getKey());
                            posWriter.delete(df, e.getValue());
                            referenced.add(df.location());
                        }
                        dvResult = posWriter.complete();
                    }
                }
                newDvs.addAll(dvResult.deleteFiles());
                superseded.addAll(dvResult.rewrittenDeleteFiles());
            }
            newDvs.forEach(rowDelta::addDeletes);
            superseded.forEach(rowDelta::removeDeletes);
            List<DeleteFile> dangling = danglingPositionDeletes(baseSnapshotId);
            dangling.forEach(rowDelta::removeDeletes);
            if (baseExists) {
                // No data files may be added after the base; the index would not know their rows.
                rowDelta.validateFromSnapshot(baseSnapshotId)
                        .conflictDetectionFilter(Expressions.alwaysTrue())
                        .validateNoConflictingDataFiles();
            }
            if (!referenced.isEmpty()) {
                rowDelta.validateDataFilesExist(referenced);
            }
            commitAttempted = true;
            long snapshotId = commit(rowDelta, snapshotProperties);
            LOG.info(
                    "Committed snapshot {} on base {}: {} data files, {} deletion vectors"
                            + " ({} superseded, {} dangling position delete files removed).",
                    snapshotId,
                    baseSnapshotId,
                    committable.getDataFiles().size(),
                    newDvs.size(),
                    superseded.size(),
                    dangling.size());
            protectReadable(snapshotId);
            return LakeCommitResult.unknownReadableSnapshot(snapshotId);
        } catch (LakeCommitRejectedException e) {
            deleteWrittenDeleteFiles(newDvs);
            throw e;
        } catch (Exception e) {
            // Like Iceberg's SnapshotProducer with strict cleanup: the delete files written here
            // are deleted only if the commit definitely failed. On an unknown outcome
            // (CommitStateUnknownException, e.g. a REST 5xx or timeout) the snapshot may be live
            // and reference them.
            if (!commitAttempted || e instanceof CleanableFailure) {
                deleteWrittenDeleteFiles(newDvs);
                String message =
                        "Committing deletion vectors to Iceberg table "
                                + icebergTable.name()
                                + " on base snapshot "
                                + baseSnapshotId
                                + " failed.";
                if (commitAttempted && movedPast(fence)) {
                    // Another engine committed since the base: the round can run again on the
                    // table's current snapshot.
                    throw new LakeBaseChangedException(message, e);
                }
                throw new LakeCommitRejectedException(message, e);
            }
            // The outcome is unknown: the snapshot may land and reference them, so they are kept,
            // like every file of the commit. Lake orphan-file removal reclaims them if it never
            // lands.
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new IOException("Committing deletion vectors to Iceberg failed.", e);
        }
    }

    /**
     * Whether the table's current snapshot differs from {@code fence}. For a null {@code fence},
     * whether the table has a snapshot now.
     */
    private boolean movedPast(@Nullable Long fence) {
        try {
            icebergTable.refresh();
        } catch (RuntimeException e) {
            LOG.warn("Could not refresh {} after a rejected commit.", icebergTable, e);
            return false;
        }
        Snapshot current = icebergTable.currentSnapshot();
        return current == null ? fence != null : !Long.valueOf(current.snapshotId()).equals(fence);
    }

    private void deleteWrittenDeleteFiles(List<DeleteFile> deleteFiles) {
        // Deletion vectors of several data files share one Puffin file.
        List<String> locations =
                deleteFiles.stream()
                        .map(ContentFile::location)
                        .distinct()
                        .collect(Collectors.toList());
        if (!locations.isEmpty()) {
            CatalogUtil.deleteFiles(icebergTable.io(), locations, "delete file", true);
        }
    }

    /**
     * Returns the file-scoped v2 position delete files of the base snapshot whose data file is gone
     * from it.
     *
     * <p>A rewrite by another engine (e.g. Spark's rewrite_data_files without
     * remove-dangling-deletes) leaves them behind. They mask nothing but are planned with every
     * scan, so a round removes them in its commit. Fluss's own rewrites remove them, and Iceberg
     * removes the deletion vectors of rewritten files. So only a v2 base after another engine
     * removed data files since the last round is checked.
     */
    private List<DeleteFile> danglingPositionDeletes(long baseSnapshotId) throws IOException {
        Snapshot base = baseSnapshotId > 0 ? icebergTable.snapshot(baseSnapshotId) : null;
        if (base == null
                || TableUtil.formatVersion(icebergTable) >= 3
                || !externalFileRemovalSinceLastRound(base)) {
            return Collections.emptyList();
        }
        Set<String> liveDataFiles =
                new HashSet<>(
                        LiveFilesIndex.of(state.liveFiles, icebergTable, baseSnapshotId)
                                .locations());
        List<DeleteFile> dangling = new ArrayList<>();
        for (ManifestFile manifest : base.deleteManifests(icebergTable.io())) {
            try (ManifestReader<DeleteFile> reader =
                    ManifestFiles.readDeleteManifest(
                            manifest, icebergTable.io(), icebergTable.specs())) {
                for (DeleteFile deleteFile : reader) {
                    if (deleteFile.content() != FileContent.POSITION_DELETES
                            || deleteFile.format() == FileFormat.PUFFIN) {
                        continue;
                    }
                    String dataFile = ContentFileUtil.referencedDataFileLocation(deleteFile);
                    if (dataFile != null && !liveDataFiles.contains(dataFile)) {
                        dangling.add(deleteFile.copyWithoutStats());
                    }
                }
            }
        }
        return dangling;
    }

    /**
     * Whether a snapshot by another engine, after the last snapshot this tiering committed and up
     * to the base, removed data files.
     */
    private boolean externalFileRemovalSinceLastRound(Snapshot base) {
        for (Snapshot s = base; s != null; ) {
            Map<String, String> summary = s.summary();
            String user = summary == null ? null : summary.get(COMMITTER_USER);
            if (FLUSS_LAKE_TIERING_COMMIT_USER.equals(user)) {
                return false;
            }
            if (!FLUSS_LAKE_COMPACTION_COMMIT_USER.equals(user)
                    && summary != null
                    && Long.parseLong(summary.getOrDefault(SnapshotSummary.DELETED_FILES_PROP, "0"))
                            > 0) {
                return true;
            }
            s = s.parentId() == null ? null : icebergTable.snapshot(s.parentId());
        }
        return false;
    }

    /** Returns the position deletes (v2 files or v3 DVs) among a data file's delete files. */
    private static List<DeleteFile> positionDeletesOf(List<DeleteFile> deletes) {
        List<DeleteFile> out = new ArrayList<>();
        if (deletes != null) {
            for (DeleteFile d : deletes) {
                if (d.content() == org.apache.iceberg.FileContent.POSITION_DELETES) {
                    out.add(d);
                }
            }
        }
        return out;
    }

    @Override
    public void protectReadableSnapshot(long snapshotId) {
        protectReadable(snapshotId);
    }

    /** Tag prefix of the snapshots Fluss made readable; tagged snapshots survive expiration. */
    public static final String READABLE_TAG_PREFIX = "fluss-readable-";

    // how many recent readable snapshots stay tagged
    private static final int PROTECTED_SNAPSHOTS = 10;

    /**
     * Tags the readable snapshot and untags all but the most recent ones, so expiration keeps the
     * files an in-flight union read needs. Best effort: a failure never fails the data commit.
     */
    private void protectReadable(long snapshotId) {
        try {
            icebergTable.refresh();
            List<Map.Entry<String, org.apache.iceberg.SnapshotRef>> tags = new ArrayList<>();
            for (Map.Entry<String, org.apache.iceberg.SnapshotRef> e :
                    icebergTable.refs().entrySet()) {
                if (e.getKey().startsWith(READABLE_TAG_PREFIX) && e.getValue().isTag()) {
                    tags.add(e);
                }
            }
            // Newest first.
            tags.sort(
                    (a, b) ->
                            Long.compare(
                                    sequenceOf(b.getValue().snapshotId()),
                                    sequenceOf(a.getValue().snapshotId())));
            org.apache.iceberg.ManageSnapshots ms = icebergTable.manageSnapshots();
            String tag = READABLE_TAG_PREFIX + snapshotId;
            boolean exists = icebergTable.refs().containsKey(tag);
            if (!exists) {
                ms.createTag(tag, snapshotId);
            }
            int kept = 1; // the new tag
            int removed = 0;
            for (Map.Entry<String, org.apache.iceberg.SnapshotRef> e : tags) {
                if (e.getKey().equals(tag)) {
                    continue;
                }
                if (kept < PROTECTED_SNAPSHOTS) {
                    kept++;
                } else {
                    ms.removeTag(e.getKey());
                    removed++;
                }
            }
            if (!exists || removed > 0) {
                ms.commit();
            }
        } catch (Exception e) {
            LOG.warn(
                    "Could not tag readable snapshot {}; the next round tries again.",
                    snapshotId,
                    e);
        }
    }

    /**
     * Expires the table's old snapshots, at most once per interval of the committer state per
     * table, and deletes the files no remaining snapshot references.
     *
     * <p>It expires the snapshots older than {@code history.expire.max-snapshot-age-ms} (1 hour
     * unless the table sets it, as Paimon's {@code snapshot.time-retained}) except the last {@code
     * history.expire.min-snapshots-to-keep} (10 unless the table sets it, as Paimon's {@code
     * snapshot.num-retained.min}). It never expires the snapshots Fluss still needs: the readable
     * ones it tagged, which their tags keep, and {@code keepFromSnapshotId} and those after it,
     * whatever the clocks of their committers. {@code keepFromSnapshotId} is tagged {@value
     * #KEEP_FROM_TAG}. Best effort: a failure never fails a commit.
     */
    @Override
    public void expireSnapshots(@Nullable Long keepFromSnapshotId) {
        if (!autoExpireSnapshots) {
            return;
        }
        long now = System.currentTimeMillis();
        if (!state.expirationDue(icebergTable.location(), now)) {
            return;
        }
        try {
            icebergTable.refresh();
            long olderThan =
                    now
                            - PropertyUtil.propertyAsLong(
                                    icebergTable.properties(),
                                    TableProperties.MAX_SNAPSHOT_AGE_MS,
                                    DEFAULT_MAX_SNAPSHOT_AGE_MS);
            int retainLast =
                    PropertyUtil.propertyAsInt(
                            icebergTable.properties(),
                            TableProperties.MIN_SNAPSHOTS_TO_KEEP,
                            DEFAULT_MIN_SNAPSHOTS_TO_KEEP);
            if (keepFromSnapshotId != null) {
                Snapshot keepFrom = icebergTable.snapshot(keepFromSnapshotId);
                if (keepFrom != null) {
                    tagKeepFrom(keepFrom.snapshotId());
                    // Iceberg keeps a branch's history only up to its first snapshot older than
                    // the cut-off, and the clocks of committers differ. So the cut-off is the
                    // oldest timestamp from the current snapshot back to keepFrom.
                    olderThan = Math.min(olderThan, oldestTimestampSince(keepFrom));
                }
            }
            int before = Iterables.size(icebergTable.snapshots());
            icebergTable
                    .expireSnapshots()
                    .expireOlderThan(olderThan)
                    .retainLast(Math.max(1, retainLast))
                    .cleanExpiredFiles(true)
                    .commit();
            icebergTable.refresh();
            LOG.info(
                    "Expired {} snapshots of {} older than {} (keeping the last {}{}); {} left.",
                    before - Iterables.size(icebergTable.snapshots()),
                    icebergTable.name(),
                    olderThan,
                    retainLast,
                    keepFromSnapshotId == null ? "" : ", and from " + keepFromSnapshotId,
                    Iterables.size(icebergTable.snapshots()));
        } catch (Exception e) {
            LOG.warn("Could not expire the snapshots of {}.", icebergTable.name(), e);
        }
    }

    /** Tag of the oldest snapshot the index chain needs; expiry keeps tagged snapshots. */
    public static final String KEEP_FROM_TAG = "fluss-keep-from";

    /** Points {@link #KEEP_FROM_TAG} at {@code snapshotId}. */
    private void tagKeepFrom(long snapshotId) {
        org.apache.iceberg.SnapshotRef tag = icebergTable.refs().get(KEEP_FROM_TAG);
        if (tag != null && tag.snapshotId() == snapshotId) {
            return;
        }
        org.apache.iceberg.ManageSnapshots ms = icebergTable.manageSnapshots();
        if (tag == null) {
            ms.createTag(KEEP_FROM_TAG, snapshotId);
        } else {
            ms.replaceTag(KEEP_FROM_TAG, snapshotId);
        }
        ms.commit();
        icebergTable.refresh();
    }

    /**
     * The oldest timestamp of the current snapshot, {@code keepFrom} and the snapshots in between,
     * or that of {@code keepFrom} if the current snapshot does not descend from it.
     */
    private long oldestTimestampSince(Snapshot keepFrom) {
        long oldest = keepFrom.timestampMillis();
        for (Snapshot s = icebergTable.currentSnapshot(); s != null; ) {
            oldest = Math.min(oldest, s.timestampMillis());
            if (s.snapshotId() == keepFrom.snapshotId()) {
                return oldest;
            }
            s = s.parentId() == null ? null : icebergTable.snapshot(s.parentId());
        }
        return keepFrom.timestampMillis();
    }

    private long sequenceOf(long snapshotId) {
        Snapshot s = icebergTable.snapshot(snapshotId);
        return s == null ? -1 : s.sequenceNumber();
    }

    /**
     * Commits a compaction's rewrite. The rewrite of a table with deletion vectors ({@code
     * derived}) lands only on the snapshot it read, and also removes the position delete files of
     * the files it replaced; a rewrite of another table does neither.
     */
    private Long commitRewrite(
            List<RewriteDataFileResult> rewriteDataFileResults,
            Map<String, String> snapshotProperties,
            String committerUser,
            boolean derived) {
        icebergTable.refresh();
        boolean commitAttempted = false;
        try {
            if (rewriteDataFileResults.stream()
                            .map(RewriteDataFileResult::snapshotId)
                            .distinct()
                            .count()
                    > 1) {
                throw new IllegalArgumentException(
                        "Rewrite data file results must have same snapshot id.");
            }
            long readSnapshotId = rewriteDataFileResults.get(0).snapshotId();
            RewriteFiles rewriteFiles =
                    derived
                            ? IcebergBaseFence.on(icebergTable, readSnapshotId).newRewrite()
                            : icebergTable.newRewrite();
            rewriteFiles.validateFromSnapshot(readSnapshotId);
            for (RewriteDataFileResult rewriteDataFileResult : rewriteDataFileResults) {
                rewriteDataFileResult.addedDataFiles().forEach(rewriteFiles::addFile);
                rewriteDataFileResult.deletedDataFiles().forEach(rewriteFiles::deleteFile);
                if (derived) {
                    rewriteDataFileResult.deletedDeleteFiles().forEach(rewriteFiles::deleteFile);
                }
            }
            commitAttempted = true;
            return commit(rewriteFiles, snapshotProperties, committerUser);
        } catch (Exception e) {
            List<String> rewriteAddedDataFiles =
                    rewriteDataFileResults.stream()
                            .flatMap(
                                    rewriteDataFileResult ->
                                            rewriteDataFileResult.addedDataFiles().stream())
                            .map(ContentFile::location)
                            .collect(Collectors.toList());
            // Like Iceberg's SnapshotProducer with strict cleanup (as REST catalogs require), only
            // clean up when the commit definitely failed. Otherwise (CommitStateUnknownException
            // on a REST 5xx/timeout in particular) the rewrite snapshot may be live and reference
            // these files, so deleting them would corrupt the table; at worst they are orphans.
            if (!commitAttempted || e instanceof CleanableFailure) {
                LOG.error(
                        "Failed to commit rewrite files to iceberg table {}, delete rewrite added files {}.",
                        icebergTable.name(),
                        rewriteAddedDataFiles,
                        e);
                CatalogUtil.deleteFiles(
                        icebergTable.io(), rewriteAddedDataFiles, "data file", true);
            } else {
                LOG.warn(
                        "Commit state of the rewrite of iceberg table {} (validated from snapshot {}) "
                                + "is unknown, keep rewrite added files {} as a committed snapshot "
                                + "may reference them.",
                        icebergTable.name(),
                        rewriteDataFileResults.get(0).snapshotId(),
                        rewriteAddedDataFiles,
                        e);
            }
            // The same as a rejected rewrite: the caller falls back to the data snapshot.
            return null;
        }
    }

    private long commit(SnapshotUpdate<?> snapshotUpdate, Map<String, String> snapshotProperties) {
        return commit(snapshotUpdate, snapshotProperties, FLUSS_LAKE_TIERING_COMMIT_USER);
    }

    private long commit(
            SnapshotUpdate<?> snapshotUpdate,
            Map<String, String> snapshotProperties,
            String committerUser) {
        // add snapshot properties
        snapshotUpdate.set(COMMITTER_USER, committerUser);
        for (Map.Entry<String, String> entry : snapshotProperties.entrySet()) {
            snapshotUpdate.set(entry.getKey(), entry.getValue());
        }
        // do commit
        snapshotUpdate.commit();
        Long commitSnapshotId = currentCommitSnapshotId.get();
        currentCommitSnapshotId.remove();
        return commitSnapshotId;
    }

    @Override
    public void abort(IcebergCommittable committable) {
        List<String> dataFilesToDelete =
                committable.getDataFiles().stream()
                        .map(ContentFile::location)
                        .collect(Collectors.toList());
        CatalogUtil.deleteFiles(icebergTable.io(), dataFilesToDelete, "data file", true);

        List<String> deleteFilesToDelete =
                committable.getDeleteFiles().stream()
                        .map(ContentFile::location)
                        .collect(Collectors.toList());
        CatalogUtil.deleteFiles(icebergTable.io(), deleteFilesToDelete, "delete file", true);
    }

    @Override
    public void checkCurrentSnapshotDescendsFrom(long snapshotId) {
        icebergTable.refresh();
        IcebergSnapshotLineage.checkDescendsFrom(icebergTable, snapshotId);
    }

    @Nullable
    @Override
    public CommittedLakeSnapshot getMissingLakeSnapshot(@Nullable Long latestLakeSnapshotIdOfFluss)
            throws IOException {
        Snapshot latestLakeSnapshot =
                getCommittedLatestSnapshotOfLake(FLUSS_LAKE_TIERING_COMMIT_USER);

        if (latestLakeSnapshot == null) {
            return null;
        }

        // Check if there's a gap between Fluss and Iceberg snapshots
        if (latestLakeSnapshotIdOfFluss != null) {
            Snapshot latestLakeSnapshotOfFluss = icebergTable.snapshot(latestLakeSnapshotIdOfFluss);
            if (latestLakeSnapshotOfFluss == null) {
                String base =
                        latestLakeSnapshot.summary() == null
                                ? null
                                : latestLakeSnapshot.summary().get(FLUSS_LAKE_SNAP_BASE_PROPERTY);
                if (base == null) {
                    throw new IllegalStateException(
                            "Referenced Fluss snapshot "
                                    + latestLakeSnapshotIdOfFluss
                                    + " not found in Iceberg table");
                }
                FormatVersions.checkSnapshotVersion(
                        latestLakeSnapshot.snapshotId(), latestLakeSnapshot.summary());
                // A round of a table with deletion vectors records its base, the snapshot Fluss
                // had registered last. Here the registered snapshot expired, e.g. a rewrite by
                // another engine whose tag was rotated away. The latest tiered snapshot is missing
                // in Fluss only if a round committed it on top of the registered one. Otherwise
                // the round re-indexes the current snapshot.
                if (!base.equals(String.valueOf(latestLakeSnapshotIdOfFluss))) {
                    LOG.warn(
                            "Snapshot {} registered in Fluss expired from {}; the round re-indexes"
                                    + " the table's current snapshot.",
                            latestLakeSnapshotIdOfFluss,
                            icebergTable.name());
                    return null;
                }
                return new CommittedLakeSnapshot(
                        latestLakeSnapshot.snapshotId(), latestLakeSnapshot.summary());
            }
            // note: we need to use sequence number to compare,
            // we can't use snapshot id as the snapshot id is not ordered
            if (latestLakeSnapshot.sequenceNumber() <= latestLakeSnapshotOfFluss.sequenceNumber()) {
                return null;
            }
            if (isDerivedRound(latestLakeSnapshot)) {
                Snapshot inHistory =
                        missingDerivedRoundInHistory(
                                latestLakeSnapshot, latestLakeSnapshotOfFluss.snapshotId());
                if (inHistory != latestLakeSnapshot) {
                    if (inHistory == null) {
                        return null;
                    }
                    latestLakeSnapshot = inHistory;
                }
            }
        }

        // Reconstruct bucket offsets from snapshot properties
        Map<String, String> properties = latestLakeSnapshot.summary();
        if (properties == null) {
            throw new IOException(
                    "Failed to load committed lake snapshot properties from Iceberg.");
        }

        return new CommittedLakeSnapshot(latestLakeSnapshot.snapshotId(), properties);
    }

    /** Whether a round of a table with deletion vectors committed the snapshot. */
    private static boolean isDerivedRound(Snapshot snapshot) {
        Map<String, String> summary = snapshot.summary();
        return summary != null && summary.containsKey(FLUSS_LAKE_SNAP_BASE_PROPERTY);
    }

    /**
     * Returns the round of a table with deletion vectors missing in Fluss among the snapshots from
     * the current one back to {@code registered}, the snapshot Fluss registered last. It is {@code
     * latest}, the latest round this tiering committed, if the current snapshot descends from it.
     *
     * <p>Otherwise the lake table was rolled back while it descends from the registered snapshot,
     * e.g. to the registered snapshot itself, after a round committed on top of it but was lost
     * before registering it. That round is no longer part of the table, so it is not missing:
     * rounds go on from the registered snapshot. The latest round in the current snapshot's history
     * since the registered one is returned instead, or null if there is none. If the current
     * snapshot does not descend from the registered one, {@code latest} is returned, and the
     * rollback check reports the rollback.
     */
    @Nullable
    private Snapshot missingDerivedRoundInHistory(Snapshot latest, long registered) {
        Snapshot newestRound = null;
        boolean descendsFromLatest = false;
        for (Snapshot s = icebergTable.currentSnapshot(); s != null; ) {
            if (s.snapshotId() == registered) {
                if (descendsFromLatest) {
                    return latest;
                }
                if (newestRound == null) {
                    LOG.warn(
                            "Lake snapshot {} of {}, committed by a round that was not"
                                    + " registered, is not in the history of the current"
                                    + " snapshot, which descends from the registered snapshot {};"
                                    + " the round is dropped.",
                            latest.snapshotId(),
                            icebergTable.name(),
                            registered);
                }
                return newestRound;
            }
            if (s.snapshotId() == latest.snapshotId()) {
                descendsFromLatest = true;
            }
            if (newestRound == null
                    && isDerivedRound(s)
                    && FLUSS_LAKE_TIERING_COMMIT_USER.equals(s.summary().get(COMMITTER_USER))) {
                newestRound = s;
            }
            s = s.parentId() == null ? null : icebergTable.snapshot(s.parentId());
        }
        // not a descendant of the registered snapshot: a rollback past it
        return latest;
    }

    @Override
    public void close() throws Exception {
        try {
            if (icebergCatalog != null && icebergCatalog instanceof AutoCloseable) {
                ((AutoCloseable) icebergCatalog).close();
            }
        } catch (Exception e) {
            throw new IOException("Failed to close IcebergLakeCommitter.", e);
        }
    }

    private Table getTable(TablePath tablePath) throws IOException {
        try {
            TableIdentifier tableId = toIceberg(tablePath);
            return icebergCatalog.loadTable(tableId);
        } catch (Exception e) {
            throw new IOException("Failed to get table " + tablePath + " in Iceberg.", e);
        }
    }

    @Nullable
    private Snapshot getCommittedLatestSnapshotOfLake(String commitUser) {
        icebergTable.refresh();

        // Find the latest snapshot committed by Fluss
        List<Snapshot> snapshots = (List<Snapshot>) icebergTable.snapshots();
        // snapshots() returns snapshots in chronological order (oldest to newest), Reverse to find
        // most recent snapshot committed by Fluss
        for (int i = snapshots.size() - 1; i >= 0; i--) {
            Snapshot snapshot = snapshots.get(i);
            Map<String, String> summary = snapshot.summary();
            if (summary != null && commitUser.equals(summary.get(COMMITTER_USER))) {
                return snapshot;
            }
        }
        return null;
    }

    /** A {@link Listener} to listen the iceberg create snapshot event. */
    public static class IcebergSnapshotCreateListener implements Listener<CreateSnapshotEvent> {
        @Override
        public void notify(CreateSnapshotEvent createSnapshotEvent) {
            currentCommitSnapshotId.set(createSnapshotEvent.snapshotId());
        }
    }
}
