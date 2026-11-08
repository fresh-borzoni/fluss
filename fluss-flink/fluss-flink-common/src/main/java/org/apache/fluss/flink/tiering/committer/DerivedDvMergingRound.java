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

import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.tiering.TableBucketWriteResult;
import org.apache.fluss.client.tiering.TieringCommitResult;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.flink.tiering.committer.DerivedDvRound.Phase;
import org.apache.fluss.lake.committer.LakeCommitRejectedException;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.dv.FormatVersions;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.fluss.flink.tiering.committer.DerivedDvContext.DERIVED_BASE_PROPERTY;
import static org.apache.fluss.flink.tiering.committer.DerivedDvContext.checkSameTable;
import static org.apache.fluss.flink.tiering.committer.DerivedDvContext.dvCommit;
import static org.apache.fluss.flink.tiering.committer.DerivedDvContext.keyMerging;
import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;

/**
 * Runs a round of a lake that merges rows by primary key itself (Paimon with deletion vectors). See
 * {@link #commitMergingRound}.
 */
final class DerivedDvMergingRound<WriteResult, Committable> {

    private static final Logger LOG = DerivedDvContext.LOG;

    private final DerivedDvContext<WriteResult, Committable> context;

    private final DerivedDvRecovery<WriteResult, Committable> recovery;
    private final DerivedDvReindexer<WriteResult, Committable> reindexer;
    private final DerivedDvCompactionPublisher<WriteResult, Committable> compaction;
    private final DerivedDvRegistrations<WriteResult, Committable> registrations;

    DerivedDvMergingRound(
            DerivedDvContext<WriteResult, Committable> context,
            DerivedDvRecovery<WriteResult, Committable> recovery,
            DerivedDvReindexer<WriteResult, Committable> reindexer,
            DerivedDvCompactionPublisher<WriteResult, Committable> compaction,
            DerivedDvRegistrations<WriteResult, Committable> registrations) {
        this.context = context;
        this.recovery = recovery;
        this.reindexer = reindexer;
        this.compaction = compaction;
        this.registrations = registrations;
    }

    /**
     * Commits a round of a lake that merges rows by primary key (Paimon with deletion vectors).
     *
     * <p>The Paimon committer reports the readable snapshot and offsets. The readable snapshot R is
     * the latest COMPACT snapshot. A bucket without level-0 files in R reads from its tiered offset
     * at R. A bucket with level-0 files reads from the tiered offset of the last APPEND at or
     * before the latest snapshot whose level-0 files of the bucket are exactly those its last
     * compaction up to R moved up. If no such snapshot is found, R is not readable yet.
     *
     * <p>Paimon's deletion-vector read of R skips level 0 and keeps one live row per key. Every
     * change of the bucket below its readable offset X is in levels 1 and up. The union read takes
     * R's rows minus the masks (the rows of keys changed at or after X), plus the log from X with
     * superseded versions masked. A key not changed since X has its state at X from the lake. A
     * changed key has its latest version, or none if deleted, from the log only. A change at or
     * after X that a compaction already moved up is masked too, as its key changed at or after X.
     *
     * <p>The round commits without deletes. If the readable snapshot moved to one the chain does
     * not have, the files that became readable since the chain's end are indexed first, and the
     * round is held while the tiering readers read them. The snapshot is registered in Fluss only
     * after its change is in the chain, so a tablet server can always cover the readable snapshot.
     * An APPEND only adds level-0 files and changes no index. The change is planned after the
     * commit, from the readable files of the chain's end and of the readable snapshot. A compaction
     * another engine committed during the round is thus part of it: unlike a round with deletes,
     * nothing of it is relative to the round's base.
     *
     * <p>A round without data commits only if another engine compacted the table since the latest
     * registered snapshot. It commits no data at the registered offsets, so the compaction becomes
     * the readable snapshot and is indexed without waiting for a write. The registered snapshot
     * thus stays a commit of this tiering, which tells a rollback of the lake table apart from the
     * snapshot Fluss registered.
     */
    RoundOutcome<Committable> commitMergingRound(
            long tableId,
            TablePath tablePath,
            TableInfo tableInfo,
            List<TableBucketWriteResult<WriteResult>> committableWriteResults)
            throws Exception {
        checkSameTable(tableId, tablePath, tableInfo);
        List<TableBucketWriteResult<WriteResult>> results =
                context.withoutResultsOfDroppedPartitions(
                        tablePath, tableInfo, committableWriteResults);
        List<TableBucketWriteResult<WriteResult>> nonEmptyResults =
                results.stream().filter(r -> r.writeResult() != null).collect(Collectors.toList());
        try (LakeCommitter<WriteResult, Committable> lakeCommitter =
                context.createDvLakeCommitter(tablePath, tableInfo)) {
            Committable committable =
                    lakeCommitter.toCommittable(
                            nonEmptyResults.stream()
                                    .map(TableBucketWriteResult::writeResult)
                                    .collect(Collectors.toList()));
            // whether the partition mark-done is prepared on the committable already
            boolean markDonePrepared = false;
            if (nonEmptyResults.isEmpty()) {
                // The readable snapshot is computed on commits. After another engine compacted an
                // idle table, a round commits the registered offsets again, without data, so the
                // compaction becomes readable and is indexed without waiting for a write. A round
                // whose buckets advanced without a row commits too, to register the offsets, and
                // so does one that marked partitions done.
                LakeSnapshot registered = context.getLatestLakeSnapshot(tablePath);
                String reason = null;
                if (DerivedDvRound.advancesOffsets(results, registered)) {
                    reason = "advanced its buckets' offsets without rows";
                } else if (registered != null
                        && keyMerging(lakeCommitter)
                                .readableSnapshotMayAdvanceSince(registered.getSnapshotId())) {
                    reason = "found a compaction since snapshot " + registered.getSnapshotId();
                } else if (registered != null
                        && context.lakeTieringConfig.get(
                                ConfigOptions.LAKE_TIERING_PARTITION_MARK_DONE_ENABLED)
                        && context.lakeTieringFactory.supportsPartitionMarkDone()) {
                    markDonePrepared = true;
                    if (context.preparePartitionMarkDone(lakeCommitter, committable)) {
                        reason = "marked partitions done";
                    }
                }
                if (reason == null) {
                    LOG.info(
                            "Commit tiering write results is empty for table {} ({}).",
                            tablePath,
                            tableId);
                    return RoundOutcome.committed(new TieringCommitResult<>(null, null));
                }
                LOG.info("Idle round of {} {}; it commits no data.", tablePath, reason);
            }
            Map<TableBucket, Long> logEndOffsets = new HashMap<>();
            Map<TableBucket, Long> logMaxTieredTimestamps = new HashMap<>();
            for (TableBucketWriteResult<WriteResult> writeResult : results) {
                if (writeResult.logEndOffset() < 0) {
                    continue;
                }
                logEndOffsets.put(writeResult.tableBucket(), writeResult.logEndOffset());
                if (writeResult.maxTimestamp() >= 0) {
                    logMaxTieredTimestamps.put(
                            writeResult.tableBucket(), writeResult.maxTimestamp());
                }
            }
            RoundFiles round = new RoundFiles();
            String offsetsFile;
            Map<TableBucket, Long> tieredLogEndOffsets = new HashMap<>();
            LakeSnapshot flussCurrentLakeSnapshot;
            Reindex lostCommitReindex;
            try {
                flussCurrentLakeSnapshot = context.getLatestLakeSnapshot(tablePath);
                if (flussCurrentLakeSnapshot != null) {
                    dvCommit(lakeCommitter)
                            .checkCurrentSnapshotDescendsFrom(
                                    flussCurrentLakeSnapshot.getSnapshotId());
                }
                lostCommitReindex =
                        recovery.registerLostCommit(
                                tableId,
                                tablePath,
                                tableInfo,
                                lakeCommitter,
                                flussCurrentLakeSnapshot == null
                                        ? null
                                        : flussCurrentLakeSnapshot.getSnapshotId());
            } catch (Exception e) {
                recovery.cleanUpFailedRound(tableId, tablePath, lakeCommitter, committable, round);
                throw e;
            }
            if (lostCommitReindex != null) {
                // This round runs again on top of the lost commit once that is registered.
                recovery.cleanUpFailedRound(tableId, tablePath, lakeCommitter, committable, round);
                compaction.discardHandedOverIndex(
                        context.indexStore(tableId, tablePath), nonEmptyResults);
                return RoundOutcome.held(lostCommitReindex);
            }
            try {
                if (!markDonePrepared
                        && context.lakeTieringConfig.get(
                                ConfigOptions.LAKE_TIERING_PARTITION_MARK_DONE_ENABLED)
                        && context.lakeTieringFactory.supportsPartitionMarkDone()) {
                    context.preparePartitionMarkDone(lakeCommitter, committable);
                }
                if (flussCurrentLakeSnapshot != null) {
                    tieredLogEndOffsets.putAll(
                            context.withoutDroppedPartitions(
                                    tablePath, flussCurrentLakeSnapshot.getTableBucketsOffset()));
                }
                tieredLogEndOffsets.putAll(logEndOffsets);
                // The offsets file lists every bucket, so dropped partitions' offsets end here.
                offsetsFile =
                        context.flussTableLakeSnapshotCommitter.prepareCompleteLakeSnapshot(
                                tableId, tablePath, tieredLogEndOffsets);
                round.offsetsFile = offsetsFile;
                round.advance(Phase.OFFSETS_WRITTEN);
                // No base fence guards a merging commit. A round of a table taken from this
                // service would commit its older rows on top of the new owner's round.
                context.flussTableLakeSnapshotCommitter.checkTieringEpoch(tableId);
            } catch (Exception e) {
                recovery.cleanUpFailedRound(tableId, tablePath, lakeCommitter, committable, round);
                throw e;
            }
            long t0 = System.currentTimeMillis();
            Map<String, String> snapshotProperties = new HashMap<>();
            snapshotProperties.put(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, offsetsFile);
            FormatVersions.putSnapshotVersion(snapshotProperties);
            // Records the registered snapshot the commit descends from, so a commit that raced a
            // rollback of the lake table is told apart from the registered snapshot it replaced.
            snapshotProperties.put(
                    DERIVED_BASE_PROPERTY,
                    String.valueOf(
                            flussCurrentLakeSnapshot == null
                                    ? -1L
                                    : flussCurrentLakeSnapshot.getSnapshotId()));
            LakeCommitResult result;
            round.advance(Phase.LAKE_COMMITTING);
            try {
                result =
                        lakeCommitter.commit(
                                committable,
                                new LakeCommitter.CommitContext(
                                        snapshotProperties, tieredLogEndOffsets));
            } catch (LakeCommitRejectedException e) {
                // The lake surely did not take the commit, so its files are deleted.
                round.advance(Phase.LAKE_REJECTED);
                recovery.cleanUpFailedRound(tableId, tablePath, lakeCommitter, committable, round);
                compaction.discardHandedOverIndex(
                        context.indexStore(tableId, tablePath), nonEmptyResults);
                throw e;
            } catch (Exception e) {
                // The outcome is unknown: the commit may still land after the call failed, e.g. on
                // a REST catalog or an object store without atomic rename. Its files stay. The next
                // round registers a commit that landed. The files of one that never does are
                // deleted by the lake committer of a later round once it knows (Paimon), or by
                // orphan-file removal.
                recovery.cleanUpFailedRound(tableId, tablePath, lakeCommitter, committable, round);
                throw e;
            }
            round.advance(Phase.LAKE_COMMITTED);
            context.metrics.lastLakeCommitMs = System.currentTimeMillis() - t0;
            if (flussCurrentLakeSnapshot != null) {
                // A rollback during the round leaves the commit on the rolled-back table. The
                // commit is not registered, and the next rounds stop until the rollback is undone.
                dvCommit(lakeCommitter)
                        .checkCurrentSnapshotDescendsFrom(flussCurrentLakeSnapshot.getSnapshotId());
            }
            context.metrics.rounds.inc();
            Registration<Committable> registration =
                    new Registration<>(
                            result,
                            offsetsFile,
                            logEndOffsets,
                            logMaxTieredTimestamps,
                            committable);
            Reindex reindex =
                    reindexer.indexReadableSnapshot(
                            tableId, tablePath, tableInfo, registration, nonEmptyResults);
            if (reindex != null) {
                return RoundOutcome.held(reindex);
            }
            return RoundOutcome.committed(registrations.register(tableId, tablePath, registration));
        }
    }
}
