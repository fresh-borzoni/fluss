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
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.iceberg.IcebergLakeCatalog;
import org.apache.fluss.lake.iceberg.maintenance.RewriteDataFileResult;
import org.apache.fluss.lake.iceberg.tiering.writer.IcebergDvFileWriter;
import org.apache.fluss.lake.iceberg.tiering.writer.IcebergPositionDeleteFileWriter;
import org.apache.fluss.metadata.TablePath;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.events.Listener;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.WriteResult;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.lake.writer.LakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;

/** Implementation of {@link LakeCommitter} for Iceberg. */
public class IcebergLakeCommitter implements LakeCommitter<IcebergWriteResult, IcebergCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergLakeCommitter.class);

    private static final String COMMITTER_USER = "commit-user";

    // DV materialization snapshots carry no bucket offsets; tag them with a distinct user so the
    // tiering bookkeeping (getCommittedLatestSnapshotOfLake) never mistakes one for a data
    // snapshot.
    private static final String DV_MATERIALIZE_COMMIT_USER = "fluss-dv-materializer";

    // Iceberg ref pinning the current DV-readable snapshot; a tagged snapshot + its files are
    // exempt
    // from expireSnapshots, so the union read never loses the data/DVs it masks against.
    private static final String FLUSS_DV_READABLE_TAG = "fluss-dv-readable-snapshot";

    // Compaction (REPLACE) snapshots carry no NEW data offsets; tag them with a distinct user so
    // the
    // restart reconcile (getCommittedLatestSnapshotOfLake -> getMissingLakeSnapshot) never mistakes
    // a compaction for the latest tiered data snapshot. Mirrors Paimon's "skip COMPACT snapshots".
    private static final String FLUSS_LAKE_COMPACTION_COMMIT_USER = "fluss-lake-compaction";

    private final Catalog icebergCatalog;
    private final Table icebergTable;
    private static final ThreadLocal<Long> currentCommitSnapshotId = new ThreadLocal<>();

    public IcebergLakeCommitter(IcebergCatalogProvider icebergCatalogProvider, TablePath tablePath)
            throws IOException {
        this.icebergCatalog = icebergCatalogProvider.get();
        this.icebergTable = getTable(tablePath);
        // register iceberg listener
        Listeners.register(new IcebergSnapshotCreateListener(), CreateSnapshotEvent.class);
    }

    @Override
    public IcebergCommittable toCommittable(List<IcebergWriteResult> icebergWriteResults) {
        // Aggregate all write results into a single committable
        IcebergCommittable.Builder builder = IcebergCommittable.builder();

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
    public LakeCommitResult commit(
            IcebergCommittable committable, Map<String, String> snapshotProperties)
            throws IOException {
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
            boolean dvTable =
                    icebergTable.schema().findField(IcebergLakeCatalog.ROWID_COLUMN_NAME) != null;
            List<RewriteDataFileResult> rewriteDataFileResults =
                    committable.rewriteDataFileResults();
            if (!rewriteDataFileResults.isEmpty()) {
                Long rewriteCommitSnapshotId =
                        commitRewrite(rewriteDataFileResults, snapshotProperties);
                // DV tables report the append: readers must not move onto compacted files before
                // the next round indexes them (planDelta then finds this rewrite in its window)
                if (rewriteCommitSnapshotId != null && !dvTable) {
                    snapshotId = rewriteCommitSnapshotId;
                }
            }
            // Iceberg does not provide cumulative table stats API yet; leave stats as -1 (unknown).
            // DV tables: defer readability so the RowPos scan + coordinator readable-switch advance
            // the readable snapshot (the just-appended files must be indexed first). Non-DV tables
            // are readable immediately.
            if (dvTable) {
                return LakeCommitResult.unknownReadableSnapshot(snapshotId);
            }
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
        // Runs before this round's deletes; it only fails, and is then safely dropped, if another
        // commit changed the files it rewrites.
        Long id = commitRewrite(committable.rewriteDataFileResults(), new HashMap<>());
        LOG.info("Derived DV: Fluss compaction rewrite committed as snapshot {}.", id);
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
        try {
            icebergTable.refresh();
            RowDelta rowDelta = icebergTable.newRowDelta();
            committable.getDataFiles().forEach(rowDelta::addRows);
            List<DeleteFile> superseded = new ArrayList<>();
            List<DeleteFile> newDvs = new ArrayList<>();
            List<CharSequence> referenced = new ArrayList<>();
            if (!deletedPositionsByFile.isEmpty()) {
                if (baseSnapshotId <= 0 || icebergTable.snapshot(baseSnapshotId) == null) {
                    throw new IOException(
                            "Derived DV commit: base snapshot "
                                    + baseSnapshotId
                                    + " not found while deletes are pending");
                }
                Map<String, DataFile> fileByBase = new HashMap<>();
                Map<String, List<DeleteFile>> existingDvs = new HashMap<>();
                try (CloseableIterable<FileScanTask> tasks =
                        icebergTable.newScan().useSnapshot(baseSnapshotId).planFiles()) {
                    for (FileScanTask task : tasks) {
                        String loc = task.file().location();
                        String base = loc.substring(loc.lastIndexOf('/') + 1);
                        if (deletedPositionsByFile.containsKey(base)) {
                            fileByBase.put(base, task.file());
                            existingDvs.put(loc, new ArrayList<>(task.deletes()));
                        }
                    }
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
                // v3 tables get deletion vectors, v2 tables file-scoped position-delete files;
                // either replaces the data file's previous one, so deletes never pile up.
                boolean deletionVectors = TableUtil.formatVersion(icebergTable) >= 3;
                DeleteWriteResult dvResult;
                if (deletionVectors) {
                    try (IcebergDvFileWriter dvWriter =
                            new IcebergDvFileWriter(icebergTable, 0, loadPrevious)) {
                        for (Map.Entry<String, long[]> e : deletedPositionsByFile.entrySet()) {
                            DataFile df = dataFileOf(fileByBase, e.getKey(), baseSnapshotId);
                            if (df != null) {
                                dvWriter.delete(df, e.getValue());
                                referenced.add(df.location());
                            }
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
                            DataFile df = dataFileOf(fileByBase, e.getKey(), baseSnapshotId);
                            if (df != null) {
                                posWriter.delete(df, e.getValue());
                                referenced.add(df.location());
                            }
                        }
                        dvResult = posWriter.complete();
                    }
                }
                newDvs.addAll(dvResult.deleteFiles());
                superseded.addAll(dvResult.rewrittenDeleteFiles());
            }
            newDvs.forEach(rowDelta::addDeletes);
            superseded.forEach(rowDelta::removeDeletes);
            if (baseSnapshotId > 0 && icebergTable.snapshot(baseSnapshotId) != null) {
                // No data files may be added after the base; the index would not know their rows.
                rowDelta.validateFromSnapshot(baseSnapshotId)
                        .conflictDetectionFilter(Expressions.alwaysTrue())
                        .validateNoConflictingDataFiles();
            }
            if (!referenced.isEmpty()) {
                rowDelta.validateDataFilesExist(referenced);
            }
            long snapshotId = commit(rowDelta, snapshotProperties);
            LOG.info(
                    "Derived DV commit: snapshot {} on base {} added {} data files, {} DVs ({} superseded).",
                    snapshotId,
                    baseSnapshotId,
                    committable.getDataFiles().size(),
                    newDvs.size(),
                    superseded.size());
            protectReadable(snapshotId);
            return LakeCommitResult.unknownReadableSnapshot(snapshotId);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Derived DV commit to Iceberg failed.", e);
        }
    }

    /**
     * Returns the base snapshot's data file, or null if the file is gone from the base, in which
     * case its rows are gone too and there is nothing to delete.
     */
    @Nullable
    private static DataFile dataFileOf(
            Map<String, DataFile> fileByBase, String baseName, long baseSnapshotId) {
        DataFile df = fileByBase.get(baseName);
        if (df == null) {
            LOG.warn(
                    "Derived DV commit: data file {} is not in base snapshot {}; its rows are gone,"
                            + " nothing to delete.",
                    baseName,
                    baseSnapshotId);
        }
        return df;
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

    /** Tag prefix of the snapshots Fluss made readable; tagged snapshots survive expiration. */
    public static final String READABLE_TAG_PREFIX = "fluss-readable-";
    /** Iceberg table property: how many recent readable snapshots stay tagged (default 10). */
    public static final String PROTECTED_SNAPSHOTS_PROPERTY = "fluss.protected-snapshots";

    private static final int DEFAULT_PROTECTED_SNAPSHOTS = 10;

    /**
     * Tags the readable snapshot and untags all but the most recent ones, so expiration keeps the
     * files an in-flight union read needs. Best effort: a failure never fails the data commit.
     */
    private void protectReadable(long snapshotId) {
        try {
            icebergTable.refresh();
            int keep = DEFAULT_PROTECTED_SNAPSHOTS;
            String configured = icebergTable.properties().get(PROTECTED_SNAPSHOTS_PROPERTY);
            if (configured != null) {
                keep = Math.max(1, Integer.parseInt(configured.trim()));
            }
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
                if (kept < keep) {
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
                    "Derived DV: could not tag readable snapshot {} (retried next round).",
                    snapshotId,
                    e);
        }
    }

    private long sequenceOf(long snapshotId) {
        Snapshot s = icebergTable.snapshot(snapshotId);
        return s == null ? -1 : s.sequenceNumber();
    }

    /**
     * Commits Puffin deletion vectors that reference pre-existing data files. Validates the
     * referenced files still exist (guards against concurrent external compaction), per FIP-31.
     */
    public long commitDeletionVectors(
            List<DeleteFile> deletionVectors,
            Iterable<? extends CharSequence> referencedDataFiles,
            long baseSnapshotId,
            Map<String, String> snapshotProperties)
            throws IOException {
        try {
            icebergTable.refresh();
            RowDelta rowDelta = icebergTable.newRowDelta();
            deletionVectors.forEach(rowDelta::addDeletes);
            rowDelta.validateFromSnapshot(baseSnapshotId)
                    .validateDataFilesExist(referencedDataFiles);
            return commit(rowDelta, snapshotProperties);
        } catch (Exception e) {
            throw new IOException("Failed to commit Iceberg deletion vectors.", e);
        }
    }

    /**
     * Materializes the server's logical LakeDv (per-file deleted positions) into physical Iceberg
     * v3 Puffin deletion vectors on top of {@code baseSnapshotId}. Merges with each file's existing
     * DV (cumulative bitmaps, one DV per data file) and validates the referenced files still exist.
     */
    @Override
    public void materializeDeletionVectors(
            Map<String, byte[]> lakeDvByFilePath,
            long baseSnapshotId,
            Map<String, String> snapshotProperties)
            throws IOException {
        if (lakeDvByFilePath.isEmpty()) {
            return;
        }
        icebergTable.refresh();
        if (icebergTable.snapshot(baseSnapshotId) == null
                || icebergTable.currentSnapshot() == null) {
            // base snapshot expired/compacted away; nothing valid to materialize against.
            return;
        }
        // Protect the readable snapshot from expiration (FIP-31 §7): a tag exempts the snapshot and
        // every file it references from Iceberg expireSnapshots (internal or external), so the
        // union
        // read never loses the data/DVs it masks against. The tag moves as the readable advances.
        tagReadableSnapshot(baseSnapshotId);
        // Operate on the CURRENT snapshot (which already carries any prior round's DVs), so a prior
        // DV is part of the base rather than a "concurrently added DV". Data-file paths are
        // absolute.
        long validateFromSnapshotId = icebergTable.currentSnapshot().snapshotId();

        // Resolve each referenced data file. LakeDv keys are data-file BASENAMES (see
        // IcebergSplit.fileNameOf); index the current snapshot's files by basename. Existing DVs
        // are
        // keyed by FULL path because BaseDVFileWriter invokes the loader with the data-file path.
        Map<String, DataFile> dataFileByBaseName = new HashMap<>();
        Map<String, List<DeleteFile>> existingDvByFullPath = new HashMap<>();
        try (CloseableIterable<FileScanTask> tasks = icebergTable.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                String fullPath = task.file().location();
                String baseName = fullPath.substring(fullPath.lastIndexOf('/') + 1);
                if (lakeDvByFilePath.containsKey(baseName)) {
                    dataFileByBaseName.put(baseName, task.file());
                    existingDvByFullPath.put(fullPath, new ArrayList<>(task.deletes()));
                }
            }
        }
        if (dataFileByBaseName.isEmpty()) {
            return;
        }

        // Loader so the DV writer merges new positions with each file's existing DV and supersedes
        // it.
        DeleteLoader deleteLoader =
                new BaseDeleteLoader(
                        deleteFile -> icebergTable.io().newInputFile(deleteFile.location()));
        Function<String, PositionDeleteIndex> loadPreviousDvs =
                fullPath -> {
                    List<DeleteFile> dvs = existingDvByFullPath.get(fullPath);
                    return (dvs == null || dvs.isEmpty())
                            ? null
                            : deleteLoader.loadPositionDeletes(dvs, fullPath);
                };

        DeleteWriteResult writeResult;
        try (IcebergDvFileWriter dvWriter =
                new IcebergDvFileWriter(icebergTable, 0, loadPreviousDvs)) {
            for (Map.Entry<String, byte[]> entry : lakeDvByFilePath.entrySet()) {
                DataFile dataFile = dataFileByBaseName.get(entry.getKey());
                if (dataFile == null) {
                    continue;
                }
                Roaring64Bitmap bitmap = new Roaring64Bitmap();
                bitmap.deserialize(ByteBuffer.wrap(entry.getValue()));
                dvWriter.delete(dataFile, bitmap.toArray());
            }
            writeResult = dvWriter.complete();
        }

        List<DeleteFile> newDvs = writeResult.deleteFiles();
        List<DeleteFile> supersededDvs = writeResult.rewrittenDeleteFiles();
        if (newDvs.isEmpty()) {
            return;
        }
        try {
            icebergTable.refresh();
            if (supersededDvs.isEmpty()) {
                // First DV for these files: plain add.
                RowDelta rowDelta = icebergTable.newRowDelta();
                newDvs.forEach(rowDelta::addDeletes);
                rowDelta.validateFromSnapshot(validateFromSnapshotId)
                        .validateDataFilesExist(writeResult.referencedDataFiles());
                commit(rowDelta, snapshotProperties, DV_MATERIALIZE_COMMIT_USER);
            } else {
                // Files already have a DV: replace it (one DV per data file in Iceberg v3).
                RewriteFiles rewrite = icebergTable.newRewrite();
                rewrite.rewriteFiles(
                        Collections.emptySet(),
                        new HashSet<>(supersededDvs),
                        Collections.emptySet(),
                        new HashSet<>(newDvs));
                rewrite.validateFromSnapshot(validateFromSnapshotId);
                commit(rewrite, snapshotProperties, DV_MATERIALIZE_COMMIT_USER);
            }
        } catch (Exception e) {
            throw new IOException("Failed to materialize Iceberg deletion vectors.", e);
        }
    }

    private Long commitRewrite(
            List<RewriteDataFileResult> rewriteDataFileResults,
            Map<String, String> snapshotProperties) {
        icebergTable.refresh();
        RewriteFiles rewriteFiles = icebergTable.newRewrite();
        try {
            if (rewriteDataFileResults.stream()
                            .map(RewriteDataFileResult::snapshotId)
                            .distinct()
                            .count()
                    > 1) {
                throw new IllegalArgumentException(
                        "Rewrite data file results must have same snapshot id.");
            }
            rewriteFiles.validateFromSnapshot(rewriteDataFileResults.get(0).snapshotId());
            for (RewriteDataFileResult rewriteDataFileResult : rewriteDataFileResults) {
                rewriteDataFileResult.addedDataFiles().forEach(rewriteFiles::addFile);
                rewriteDataFileResult.deletedDataFiles().forEach(rewriteFiles::deleteFile);
            }
            return commit(rewriteFiles, snapshotProperties, FLUSS_LAKE_COMPACTION_COMMIT_USER);
        } catch (Exception e) {
            List<String> rewriteAddedDataFiles =
                    rewriteDataFileResults.stream()
                            .flatMap(
                                    rewriteDataFileResult ->
                                            rewriteDataFileResult.addedDataFiles().stream())
                            .map(ContentFile::location)
                            .collect(Collectors.toList());
            LOG.error(
                    "Failed to commit rewrite files to iceberg, delete rewrite added files {}.",
                    rewriteAddedDataFiles,
                    e);
            // we need to abort new rewrite files
            CatalogUtil.deleteFiles(icebergTable.io(), rewriteAddedDataFiles, "data file", true);
            return null;
        }
    }

    /**
     * Points the {@link #FLUSS_DV_READABLE_TAG} tag at the given readable snapshot so expiration
     * (Iceberg internal or external Spark/Trino) cannot drop it or the files it references.
     * Best-effort: a failed tag update must not fail DV materialization.
     */
    private void tagReadableSnapshot(long readableSnapshotId) {
        try {
            if (icebergTable.refs().containsKey(FLUSS_DV_READABLE_TAG)) {
                icebergTable
                        .manageSnapshots()
                        .replaceTag(FLUSS_DV_READABLE_TAG, readableSnapshotId)
                        .commit();
            } else {
                icebergTable
                        .manageSnapshots()
                        .createTag(FLUSS_DV_READABLE_TAG, readableSnapshotId)
                        .commit();
            }
        } catch (Exception e) {
            LOG.warn(
                    "Failed to pin readable snapshot {} with tag {}; expiration protection is "
                            + "not in place this round.",
                    readableSnapshotId,
                    FLUSS_DV_READABLE_TAG,
                    e);
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
                throw new IllegalStateException(
                        "Referenced Fluss snapshot "
                                + latestLakeSnapshotIdOfFluss
                                + " not found in Iceberg table");
            }
            // note: we need to use sequence number to compare,
            // we can't use snapshot id as the snapshot id is not ordered
            if (latestLakeSnapshot.sequenceNumber() <= latestLakeSnapshotOfFluss.sequenceNumber()) {
                return null;
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
