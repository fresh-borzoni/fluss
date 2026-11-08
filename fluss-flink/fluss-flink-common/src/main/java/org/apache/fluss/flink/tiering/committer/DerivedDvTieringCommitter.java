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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.tiering.FlussTableLakeSnapshotCommitter;
import org.apache.fluss.client.tiering.TableBucketWriteResult;
import org.apache.fluss.client.tiering.TieringCommitResult;
import org.apache.fluss.client.tiering.TieringCommitterInitContext;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.LakeTableExternallyModifiedException;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.dv.index.CompactionIndexPart;
import org.apache.fluss.lake.dv.index.CompactionIndexPartProvider;
import org.apache.fluss.lake.dv.index.StagedIndexPartProvider;
import org.apache.fluss.lake.source.RowPosResult;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.lake.writer.SupportsDeletionVectorTiering;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.ExceptionUtils;

import org.apache.flink.metrics.MetricGroup;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Commits the tiering rounds of tables with deletion vectors.
 *
 * <p>A round looks up the lake positions of the rows it supersedes in the base's index, on the
 * buckets' leaders. It publishes its index change and commits data and deletes in one lake commit,
 * so every lake snapshot is consistent at its tiered offsets. The round's own compaction hands the
 * index the new positions of the rows it rewrote ({@link CompactionIndexPart}), so its rewrite
 * needs no re-index. Files rewritten since the base by another engine, or by a compaction that
 * could not hand its positions over, are re-indexed first. The round is then held ({@link
 * RoundOutcome#held}) until the tiering readers report the re-index; {@link #resumeRound} resumes
 * it.
 *
 * <p>The base fence lets the lake commit land only on the base, or on snapshots after it that
 * changed no data file ({@link DerivedDvRound}), so a registered change from the base always covers
 * its snapshot.
 *
 * <p>A lake that merges rows by primary key itself ({@link
 * SupportsDeletionVectorTiering#mergesByPrimaryKey()}, Paimon) takes no deletes. A round commits
 * its data as usual and the lake committer reports the readable snapshot, which may lag the commit.
 * The index follows the readable snapshots: a newly readable one is indexed from the files that
 * became readable since the last indexed one, while the round is held. It is registered in Fluss
 * only once its index change is in the chain. See {@link DerivedDvMergingRound}.
 *
 * <p>Each step of a round has a class of its own, which states what the step leaves behind and how
 * a crash after it is recovered:
 *
 * <ul>
 *   <li>{@link DerivedDvRound}: the round of a lake that takes deletes, and its phases;
 *   <li>{@link DerivedDvMergingRound}: the round of a lake that merges by key;
 *   <li>{@link DerivedDvReindexer}: the re-index of rewritten lake files;
 *   <li>{@link DerivedDvCompactionPublisher}: the index changes of Fluss's own compactions;
 *   <li>{@link DerivedDvRegistrations}: registrations in Fluss, the chain's trim and the snapshots
 *       kept for it;
 *   <li>{@link DerivedDvRecovery}: failed and lost rounds.
 * </ul>
 *
 * <p>Rounds run on the commit threads of the {@link TieringCommitOperator}. Rounds of different
 * tables run concurrently, at most one round of a table at a time. The state kept across rounds is
 * per table and thread-safe; a held round's state travels in its {@link Reindex}.
 */
final class DerivedDvTieringCommitter<WriteResult, Committable> {

    private static final Logger LOG = DerivedDvContext.LOG;

    private final DerivedDvContext<WriteResult, Committable> context;

    // Tables whose rollback or write by another engine is already logged, so it is logged once.
    private final Set<Long> rolledBackTables = ConcurrentHashMap.newKeySet();

    private final DerivedDvRegistrations<WriteResult, Committable> registrations;
    private final DerivedDvCompactionPublisher<WriteResult, Committable> compaction;
    private final DerivedDvReindexer<WriteResult, Committable> reindexer;
    private final DerivedDvRound<WriteResult, Committable> round;
    private final DerivedDvMergingRound<WriteResult, Committable> mergingRound;

    DerivedDvTieringCommitter(
            Admin admin,
            Configuration flussConfig,
            Configuration lakeTieringConfig,
            LakeTieringFactory<WriteResult, Committable> lakeTieringFactory,
            FlussTableLakeSnapshotCommitter flussTableLakeSnapshotCommitter,
            MetricGroup metricGroup,
            @Nullable File localTmpDir) {
        this.context =
                new DerivedDvContext<>(
                        admin,
                        flussConfig,
                        lakeTieringConfig,
                        lakeTieringFactory,
                        flussTableLakeSnapshotCommitter,
                        new DerivedDvMetrics(metricGroup),
                        localTmpDir);
        this.registrations = new DerivedDvRegistrations<>(context);
        this.compaction = new DerivedDvCompactionPublisher<>(context, registrations);
        this.reindexer = new DerivedDvReindexer<>(context, registrations, compaction);
        DerivedDvRecovery<WriteResult, Committable> recovery =
                new DerivedDvRecovery<>(context, registrations, reindexer);
        this.round = new DerivedDvRound<>(context, recovery, reindexer, compaction, registrations);
        this.mergingRound =
                new DerivedDvMergingRound<>(
                        context, recovery, reindexer, compaction, registrations);
    }

    /** Commits a round of a table with deletion vectors, or holds it for a re-index. */
    RoundOutcome<Committable> commitRound(
            long tableId,
            TablePath tablePath,
            TableInfo tableInfo,
            List<TableBucketWriteResult<WriteResult>> writeResults)
            throws Exception {
        context.rememberLakeTableDir(tableId, tableInfo);
        return reportingRollback(
                tableId,
                () ->
                        context.dvTiering(tablePath).mergesByPrimaryKey()
                                ? mergingRound.commitMergingRound(
                                        tableId, tablePath, tableInfo, writeResults)
                                : round.commitRound(
                                        tableId, tablePath, tableInfo, writeResults, false));
    }

    /**
     * Publishes the re-index a held round waited for from the readers' results. Then commits the
     * round on the re-indexed snapshot, or holds it again if the table was rewritten meanwhile.
     */
    RoundOutcome<Committable> resumeRound(
            long tableId,
            Reindex reindex,
            Map<TableBucket, List<RowPosResult>> rowPosResults,
            List<TableBucketWriteResult<WriteResult>> writeResults)
            throws Exception {
        return reportingRollback(
                tableId, () -> resume(tableId, reindex, rowPosResults, writeResults));
    }

    private RoundOutcome<Committable> resume(
            long tableId,
            Reindex reindex,
            Map<TableBucket, List<RowPosResult>> rowPosResults,
            List<TableBucketWriteResult<WriteResult>> writeResults)
            throws Exception {
        @SuppressWarnings("unchecked")
        Registration<Committable> registration = (Registration<Committable>) reindex.registration;
        // The readers skip the splits of a table dropped during the re-index. The change then
        // misses their parts, so it is not published.
        TableInfo tableInfo = context.admin.getTableInfo(reindex.tablePath).get();
        if (tableInfo.getTableId() != tableId) {
            throw new IllegalStateException(
                    String.format(
                            "Table %s (id %d) was dropped while its round waited for a re-index.",
                            reindex.tablePath, tableId));
        }
        context.rememberLakeTableDir(tableId, tableInfo);
        reindexer.publishReindex(
                tableId,
                reindex.tablePath,
                reindex.baseSnapshotId,
                reindex.compactSnapshotId,
                reindex.changeId,
                rowPosResults,
                reindex.removedFiles,
                reindex.startedAtMs,
                registration == null);
        if (registration != null) {
            // The round committed to the lake before it was held; it only registers now.
            TieringCommitResult<Committable> result =
                    registrations.register(tableId, reindex.tablePath, registration);
            if (registration.committable == null) {
                // A lost commit of an earlier round was registered instead of this round's.
                throw DerivedDvRecovery.lostCommitRegistered(
                        reindex.tablePath, registration.result.getCommittedSnapshotId());
            }
            return RoundOutcome.committed(result);
        }
        // The round committed its own compaction before it was held.
        return round.commitRound(tableId, reindex.tablePath, tableInfo, writeResults, true);
    }

    /**
     * Deletes what a round that will not commit wrote and no lake snapshot references: a stale
     * round, or a held one ({@code reindex} not null) that will not resume. That is its re-index
     * change, its writers' staged index parts, and, if it did not commit to the lake before it was
     * held, its data files and the positions its compactions handed over. Rounds of tables without
     * deletion vectors have only data files. Best effort: leftovers are removed as orphans later.
     */
    void discardRound(
            long tableId,
            TablePath tablePath,
            @Nullable Reindex reindex,
            List<TableBucketWriteResult<WriteResult>> writeResults) {
        try {
            if (reindex != null) {
                context.indexStore(tableId, tablePath).abortChange(reindex.changeId);
            }
            if (hasStagedParts(writeResults)) {
                round.deleteStagedParts(tableId, tablePath, writeResults);
            }
            if (reindex != null && reindex.registration != null) {
                return; // its data is in a lake snapshot, which a later round registers
            }
            if (hasHandedOverIndex(writeResults)) {
                compaction.discardHandedOverIndex(
                        context.indexStore(tableId, tablePath), writeResults);
            }
            List<WriteResult> data = new ArrayList<>();
            for (TableBucketWriteResult<WriteResult> r : writeResults) {
                if (r.writeResult() != null) {
                    data.add(r.writeResult());
                }
            }
            TableInfo tableInfo = context.admin.getTableInfo(tablePath).get();
            if (data.isEmpty() || tableInfo.getTableId() != tableId) {
                return;
            }
            try (LakeCommitter<WriteResult, Committable> lakeCommitter =
                    context.lakeTieringFactory.createLakeCommitter(
                            new TieringCommitterInitContext(
                                    tablePath,
                                    tableInfo,
                                    context.lakeTieringConfig,
                                    context.flussConfig))) {
                lakeCommitter.abort(lakeCommitter.toCommittable(data));
            }
            LOG.info("Deleted the data files of a dropped round of {}.", tablePath);
        } catch (Exception e) {
            LOG.warn("Could not delete the files of a dropped round of {}.", tablePath, e);
        }
    }

    private static <WriteResult> boolean hasStagedParts(
            List<TableBucketWriteResult<WriteResult>> results) {
        return results.stream().anyMatch(r -> r.writeResult() instanceof StagedIndexPartProvider);
    }

    private static <WriteResult> boolean hasHandedOverIndex(
            List<TableBucketWriteResult<WriteResult>> results) {
        return results.stream()
                .anyMatch(r -> r.writeResult() instanceof CompactionIndexPartProvider);
    }

    /**
     * Runs a round. A rollback of the lake table, or rows another engine wrote to it, is logged
     * once as an error that names what to do. Rounds fail until the change is undone.
     */
    private RoundOutcome<Committable> reportingRollback(
            long tableId, Callable<RoundOutcome<Committable>> commit) throws Exception {
        try {
            RoundOutcome<Committable> outcome = commit.call();
            rolledBackTables.remove(tableId);
            return outcome;
        } catch (Exception e) {
            Optional<LakeTableExternallyModifiedException> rollback =
                    ExceptionUtils.findThrowable(e, LakeTableExternallyModifiedException.class);
            if (rollback.isPresent() && rolledBackTables.add(tableId)) {
                LOG.error(rollback.get().getMessage());
            }
            throw e;
        }
    }

    @VisibleForTesting
    DerivedDvReindexer<WriteResult, Committable> reindexer() {
        return reindexer;
    }
}
