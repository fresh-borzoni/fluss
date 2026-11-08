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

package org.apache.fluss.flink.tiering.committer;

import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.dv.FormatVersions;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.json.TableBucketOffsets;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.apache.fluss.flink.tiering.committer.DerivedDvContext.DERIVED_BASE_PROPERTY;
import static org.apache.fluss.flink.tiering.committer.DerivedDvContext.DERIVED_INDEX_ID_PROPERTY;
import static org.apache.fluss.flink.tiering.committer.DerivedDvContext.DERIVED_OFFSETS_PROPERTY;
import static org.apache.fluss.flink.tiering.committer.DerivedDvContext.dvCommit;
import static org.apache.fluss.flink.tiering.committer.DerivedDvContext.keyMerging;
import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Recovers from rounds that failed or were lost.
 *
 * <p>A failed round deletes what it wrote only while no lake snapshot can reference it: before its
 * lake commit, or when the lake surely rejected it ({@link #cleanUpFailedRound}). Otherwise its
 * files stay, and the next round finds the commit if it landed. A round that takes deletes records
 * its index change, base and offsets in its snapshot, so {@link #checkFlussNotMissingLakeSnapshot}
 * registers a crashed round's snapshot from them before anything else is committed. A merging round
 * publishes its change after its commit, so {@link #registerLostCommit} indexes and registers the
 * lost commit as its round would have. Either way the round that recovered fails and runs again on
 * top of the recovered snapshot.
 */
final class DerivedDvRecovery<WriteResult, Committable> {

    private static final Logger LOG = DerivedDvContext.LOG;

    private final DerivedDvContext<WriteResult, Committable> context;

    private final DerivedDvRegistrations<WriteResult, Committable> registrations;
    private final DerivedDvReindexer<WriteResult, Committable> reindexer;

    DerivedDvRecovery(
            DerivedDvContext<WriteResult, Committable> context,
            DerivedDvRegistrations<WriteResult, Committable> registrations,
            DerivedDvReindexer<WriteResult, Committable> reindexer) {
        this.context = context;
        this.registrations = registrations;
        this.reindexer = reindexer;
    }

    /**
     * Deletes the files of a failed round: its data and delete files, its offsets file and its
     * index change. Keeps them if the lake commit was attempted and not definitely rejected. The
     * lake snapshot may then exist and reference them, and the next round repairs it.
     */
    void cleanUpFailedRound(
            long tableId,
            TablePath tablePath,
            LakeCommitter<WriteResult, Committable> lakeCommitter,
            Committable committable,
            RoundFiles round) {
        if (round.phase().lakeMayReferenceFiles) {
            LOG.warn(
                    "The failed round of {} reached its lake commit; its files are kept,"
                            + " as a lake snapshot may reference them.",
                    tablePath);
            return;
        }
        try {
            lakeCommitter.abort(committable);
        } catch (Exception e) {
            LOG.warn("Could not delete the data files of a failed round of {}.", tablePath, e);
        }
        discardAttempt(tableId, tablePath, round);
        LOG.info(
                "Deleted the files of the failed round of {} (offsets file {}, index"
                        + " change {}).",
                tablePath,
                round.offsetsFile,
                round.indexChangeId);
    }

    /**
     * Deletes the offsets file and the index change of a round's attempt that the lake rejected.
     * The round's data files stay for its next attempt.
     */
    void discardAttempt(long tableId, TablePath tablePath, RoundFiles round) {
        if (round.offsetsFile != null) {
            try {
                FsPath offsetsFile = new FsPath(round.offsetsFile);
                offsetsFile.getFileSystem().delete(offsetsFile, false);
            } catch (Exception e) {
                LOG.warn(
                        "Could not delete the offsets file {} of a failed round of {}.",
                        round.offsetsFile,
                        tablePath,
                        e);
            }
        }
        if (round.indexChangeId != null) {
            try {
                context.indexStore(tableId, tablePath).abortChange(round.indexChangeId);
            } catch (Exception e) {
                LOG.warn(
                        "Could not delete the position index change {} of a failed round of {}.",
                        round.indexChangeId,
                        tablePath,
                        e);
            }
        }
    }

    /**
     * Registers a lake snapshot a previous attempt committed but did not register, and fails the
     * round so it runs again on top of it. The caller deletes the round's files. Used for lakes
     * that take deletes; a merging round registers a lost commit with {@link #registerLostCommit}.
     */
    void checkFlussNotMissingLakeSnapshot(
            TablePath tablePath,
            long tableId,
            LakeCommitter<WriteResult, Committable> lakeCommitter,
            @Nullable Long flussCurrentLakeSnapshot)
            throws Exception {
        CommittedLakeSnapshot missingCommittedSnapshot =
                unregisteredCommit(tablePath, tableId, lakeCommitter, flussCurrentLakeSnapshot);
        if (missingCommittedSnapshot == null) {
            return;
        }
        // Every round of a lake that takes deletes records its index change in the commit.
        Map<String, String> props = missingCommittedSnapshot.getSnapshotProperties();
        if (!props.containsKey(DERIVED_INDEX_ID_PROPERTY)) {
            throw new IllegalStateException(
                    String.format(
                            "Lake snapshot %d of %s (table id %d), committed by Fluss after the"
                                    + " registered snapshot %s, records no index change.",
                            missingCommittedSnapshot.getLakeSnapshotId(),
                            tablePath,
                            tableId,
                            flussCurrentLakeSnapshot));
        }
        // A snapshot the lake table was rolled back past is not repaired: registering it would
        // only make the next round find the rollback again.
        dvCommit(lakeCommitter)
                .checkCurrentSnapshotDescendsFrom(missingCommittedSnapshot.getLakeSnapshotId());
        repairSnapshot(
                tableId,
                tablePath,
                missingCommittedSnapshot,
                props.get(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY));
        throw new IllegalStateException(
                "Repaired a lake snapshot left unregistered by a previous attempt;"
                        + " this round is retried on top of it.");
    }

    /**
     * Registers a lake snapshot whose round crashed after the lake commit; its index change is
     * already published under the id the snapshot records.
     */
    private void repairSnapshot(
            long tableId, TablePath tablePath, CommittedLakeSnapshot missing, String offsetsPath)
            throws Exception {
        Map<String, String> props = missing.getSnapshotProperties();
        FormatVersions.checkSnapshotVersion(missing.getLakeSnapshotId(), props);
        long indexId = Long.parseLong(props.get(DERIVED_INDEX_ID_PROPERTY));
        int buckets = DvSnapshotOffsets.decode(tableId, props.get(DERIVED_OFFSETS_PROPERTY)).size();
        // A round records its base with its index change.
        String baseProp = props.get(DERIVED_BASE_PROPERTY);
        checkState(
                baseProp != null,
                "Lake snapshot %s of %s records index change %s without its base.",
                missing.getLakeSnapshotId(),
                tablePath,
                indexId);
        context.indexStore(tableId, tablePath)
                .register(Long.parseLong(baseProp), missing.getLakeSnapshotId(), indexId);
        context.flussTableLakeSnapshotCommitter.commit(
                tableId,
                tablePath,
                DerivedDvRegistrations.readableAtTieredOffsets(
                        missing.getLakeSnapshotId(),
                        DvSnapshotOffsets.decode(tableId, props.get(DERIVED_OFFSETS_PROPERTY)),
                        null,
                        registrations.earliestSnapshotToKeep(tableId, tablePath)),
                offsetsPath,
                Collections.emptyMap(),
                Collections.emptyMap());
        LOG.warn(
                "Repaired the unregistered lake snapshot {} of {} (index {}, {}" + " buckets).",
                missing.getLakeSnapshotId(),
                tablePath,
                indexId,
                buckets);
    }

    /**
     * The latest lake snapshot of this table that Fluss committed after {@code
     * flussCurrentLakeSnapshot} but did not register, or null if there is none.
     */
    @Nullable
    private CommittedLakeSnapshot unregisteredCommit(
            TablePath tablePath,
            long tableId,
            LakeCommitter<WriteResult, Committable> lakeCommitter,
            @Nullable Long flussCurrentLakeSnapshot)
            throws Exception {
        CommittedLakeSnapshot missingCommittedSnapshot =
                lakeCommitter.getMissingLakeSnapshot(flussCurrentLakeSnapshot);
        if (missingCommittedSnapshot == null) {
            return null;
        }
        String lakeSnapshotOffsetPath =
                missingCommittedSnapshot
                        .getSnapshotProperties()
                        .get(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY);
        if (lakeSnapshotOffsetPath == null) {
            throw new IllegalStateException(
                    String.format(
                            "Can't find %s field from snapshot property.",
                            FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY));
        }
        // Offsets path: .../lake/{database}/{tableName}-{tableId}/metadata/{uuid}.offsets.
        String tableDirName = new FsPath(lakeSnapshotOffsetPath).getParent().getParent().getName();
        long snapshotTableId =
                Long.parseLong(tableDirName.substring(tableDirName.lastIndexOf('-') + 1));
        // A recreated Fluss table can reuse a lake table containing the old table's snapshots.
        if (snapshotTableId != tableId) {
            LOG.warn(
                    "Skipping recovery of lake snapshot {} for table {} (tableId={}) because "
                            + "offsets file {} belongs to tableId={}. Continuing the current table's commit.",
                    missingCommittedSnapshot.getLakeSnapshotId(),
                    tablePath,
                    tableId,
                    lakeSnapshotOffsetPath,
                    snapshotTableId);
            return null;
        }
        return missingCommittedSnapshot;
    }

    /**
     * Registers the lake commit of an earlier merging round that was lost before it registered it,
     * e.g. because the job failed over while the round was held. It registers the commit as that
     * round would have: with its readable snapshot, once the chain has that snapshot. Registering
     * it without one would leave the readable snapshot behind until a later round's compaction, and
     * for good if the table stays idle. Fails this round once the commit is registered, so the
     * round runs again on top of it. Returns the re-index the registration waits for, which this
     * round is held for instead, or null if no commit is unregistered.
     */
    @Nullable
    Reindex registerLostCommit(
            long tableId,
            TablePath tablePath,
            TableInfo tableInfo,
            LakeCommitter<WriteResult, Committable> lakeCommitter,
            @Nullable Long flussCurrentLakeSnapshot)
            throws Exception {
        CommittedLakeSnapshot lost =
                unregisteredCommit(tablePath, tableId, lakeCommitter, flussCurrentLakeSnapshot);
        if (lost == null) {
            return null;
        }
        // Not registered if the lake table was rolled back past it.
        dvCommit(lakeCommitter).checkCurrentSnapshotDescendsFrom(lost.getLakeSnapshotId());
        String offsetsFile =
                lost.getSnapshotProperties().get(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY);
        LakeCommitResult result =
                keyMerging(lakeCommitter)
                        .getCommitResult(
                                lost.getLakeSnapshotId(), readOffsets(new FsPath(offsetsFile)));
        if (result == null) {
            result = LakeCommitResult.unknownReadableSnapshot(lost.getLakeSnapshotId(), null);
        }
        Registration<Committable> registration =
                new Registration<>(
                        result, offsetsFile, Collections.emptyMap(), Collections.emptyMap(), null);
        Reindex reindex =
                reindexer.indexReadableSnapshot(
                        tableId, tablePath, tableInfo, registration, Collections.emptyList());
        if (reindex != null) {
            LOG.warn(
                    "Lake snapshot {} of {} was committed by a round lost before it"
                            + " registered it; this round registers it once its readable snapshot {}"
                            + " is indexed, and runs again after.",
                    lost.getLakeSnapshotId(),
                    tablePath,
                    reindex.compactSnapshotId);
            return reindex;
        }
        registrations.register(tableId, tablePath, registration);
        throw lostCommitRegistered(tablePath, lost.getLakeSnapshotId());
    }

    static IllegalStateException lostCommitRegistered(TablePath tablePath, long snapshotId) {
        return new IllegalStateException(
                String.format(
                        "Registered lake snapshot %d of %s, which a round lost before"
                                + " registering it had committed; this round runs again on top of it.",
                        snapshotId, tablePath));
    }

    private static Map<TableBucket, Long> readOffsets(FsPath offsetsFile) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (FSDataInputStream in = offsetsFile.getFileSystem().open(offsetsFile)) {
            IOUtils.copyBytes(in, bytes, false);
        }
        return TableBucketOffsets.fromJsonBytes(bytes.toByteArray()).getOffsets();
    }
}
