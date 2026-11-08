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
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.tiering.FlussTableLakeSnapshotCommitter;
import org.apache.fluss.client.tiering.TableBucketWriteResult;
import org.apache.fluss.client.tiering.TieringCommitResult;
import org.apache.fluss.client.tiering.TieringCommitterInitContext;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.FlussConfigUtils;
import org.apache.fluss.exception.DeletionVectorsNotReadyException;
import org.apache.fluss.exception.LakeTableExternallyModifiedException;
import org.apache.fluss.exception.LakeTableRolledBackException;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.flink.tiering.event.RowPosRequestEvent;
import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.LakeBaseChangedException;
import org.apache.fluss.lake.committer.LakeCommitRejectedException;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.committer.PartitionMarkDoneCommitter;
import org.apache.fluss.lake.committer.TieringStats;
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.FormatVersions;
import org.apache.fluss.lake.dv.RowPosSstIndex;
import org.apache.fluss.lake.dv.index.CompactionIndexPart;
import org.apache.fluss.lake.dv.index.CompactionIndexPartProvider;
import org.apache.fluss.lake.dv.index.KeyPosition;
import org.apache.fluss.lake.dv.index.PositionIndexChangeBuilder;
import org.apache.fluss.lake.dv.index.PositionIndexDelta;
import org.apache.fluss.lake.dv.index.PositionIndexDeltaProvider;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.lake.dv.index.StagedIndexPart;
import org.apache.fluss.lake.dv.index.StagedIndexPartProvider;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.DataDeltaPlan;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.RowPosResult;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.rpc.messages.PbDvKeyPosition;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.json.TableBucketOffsets;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Commits the tiering rounds of tables with deletion vectors.
 *
 * <p>A round looks up the lake positions of the rows it supersedes in the base's index, on the
 * buckets' leaders. It publishes its index change and commits data and deletes in one lake commit,
 * so every lake snapshot is consistent at its tiered offsets. The round's own compaction hands the
 * index the new positions of the rows it rewrote ({@link CompactionIndexPart}), so its rewrite
 * needs no re-index. Files rewritten since the base by another engine, or by a compaction that
 * could not hand its positions over, are re-indexed first. The round is then held ({@link
 * Outcome#held}) until the tiering readers report the re-index; {@link #resumeRound} resumes it.
 *
 * <p>The base fence lets the lake commit land only on the base, or on snapshots after it that
 * changed no data file. A rewrite committed while the round runs makes the lake reject the commit
 * ({@link LakeBaseChangedException}); the round then runs again and re-indexes the rewrite like one
 * before the round. A registered change from the base thus always covers its snapshot, also when a
 * crashed round's snapshot is registered later.
 *
 * <p>A lake that merges rows by primary key itself ({@link
 * LakeTieringFactory#mergesByPrimaryKey()}, Paimon) takes no deletes. A round commits its data as
 * usual and the lake committer reports the readable snapshot, which may lag the commit. The index
 * follows the readable snapshots: a newly readable one is indexed from the files that became
 * readable since the last indexed one, while the round is held. It is registered in Fluss only once
 * its index change is in the chain. See {@link #commitMergingRound}.
 *
 * <p>Rounds run on the commit threads of the {@link TieringCommitOperator}. Rounds of different
 * tables run concurrently, at most one round of a table at a time. The state kept across rounds is
 * per table and thread-safe; a held round's state travels in its {@link Reindex}.
 */
final class DerivedDvTieringCommitter<WriteResult, Committable> {

    private static final Logger LOG = LoggerFactory.getLogger(DerivedDvTieringCommitter.class);

    /** Snapshot property: the id of the position index change a round published. */
    static final String DERIVED_INDEX_ID_PROPERTY = "fluss-dv-index-id";
    /** Snapshot property: the tiered offset of every bucket of the snapshot. */
    static final String DERIVED_OFFSETS_PROPERTY = "fluss-dv-offsets";
    /** Snapshot property: the snapshot a round's index change applies to (its parent). */
    static final String DERIVED_BASE_PROPERTY = LakeCommitter.FLUSS_LAKE_SNAP_BASE_PROPERTY;

    @VisibleForTesting static final int LOOKUP_CHUNK = 50_000;
    // Lookup chunks in flight at once across all buckets of a round; bounds the keys held.
    @VisibleForTesting static final int LOOKUPS_IN_FLIGHT = 8;
    // Buckets whose staged parts are published at once.
    @VisibleForTesting static final int PUBLISH_THREADS = 8;
    // The most re-index splits one request event carries; keeps an event at a few hundred KB.
    @VisibleForTesting static final int SPLITS_PER_REQUEST = 1_000;

    // A bucket catches up to a newly registered snapshot within seconds.
    private static final int MAX_STALE_SNAPSHOT_RETRIES = 480;
    private static final long STALE_SNAPSHOT_BACKOFF_MS = 250;
    // A leader election or a restarted server's registration takes seconds: about 40 s in all.
    @VisibleForTesting static final int MAX_LEADER_RETRIES = 12;
    // A round runs again at once when another engine commits during it. On a table that keeps
    // moving, the round fails after this many attempts and the next round tries again.
    private static final int MAX_BASE_CHANGES = 5;
    private static final long LEADER_RETRY_INITIAL_BACKOFF_MS = 200;
    private static final long LEADER_RETRY_MAX_BACKOFF_MS = 5_000;
    // A leader rebuilding its deletion vectors answers "not ready" until its log replay ends,
    // minutes for a large backlog. The round waits that long.
    @VisibleForTesting static final long MAX_NOT_READY_WAIT_MS = 10 * 60_000;
    private static final long NOT_READY_INITIAL_BACKOFF_MS = 250;
    private static final long NOT_READY_MAX_BACKOFF_MS = 5_000;

    private final Admin admin;
    private final Configuration flussConfig;
    private final Configuration lakeTieringConfig;
    private final LakeTieringFactory<WriteResult, Committable> lakeTieringFactory;
    private final FlussTableLakeSnapshotCommitter flussTableLakeSnapshotCommitter;
    // Directory of the index stores' temporary files; null for the JVM's temporary directory.
    @Nullable private final File localTmpDir;

    // One store per table, since it owns the table's file-id counter.
    private final Map<Long, PositionIndexStore> indexStores = new ConcurrentHashMap<>();
    private final Map<Long, Integer> roundsSinceTrim = new ConcurrentHashMap<>();
    // Per table: the oldest snapshot Fluss keeps registrations from; known to be registered.
    private final Map<Long, Long> retainedFrom = new ConcurrentHashMap<>();
    // Tables whose rollback or write by another engine is already logged, so it is logged once.
    private final Set<Long> rolledBackTables = ConcurrentHashMap.newKeySet();

    private final Counter rounds;
    private final Counter changedKeys;
    private final Counter reindexes;
    private final Counter reindexMs;
    private final Counter compactionHandOffs;
    private final Counter chainEntriesTrimmed;
    private final Counter orphanChangesRemoved;
    private volatile long lastLookupMs;
    private volatile long lastLakeCommitMs;
    private volatile long lastReindexMs;

    DerivedDvTieringCommitter(
            Admin admin,
            Configuration flussConfig,
            Configuration lakeTieringConfig,
            LakeTieringFactory<WriteResult, Committable> lakeTieringFactory,
            FlussTableLakeSnapshotCommitter flussTableLakeSnapshotCommitter,
            MetricGroup metricGroup) {
        this(
                admin,
                flussConfig,
                lakeTieringConfig,
                lakeTieringFactory,
                flussTableLakeSnapshotCommitter,
                metricGroup,
                null);
    }

    DerivedDvTieringCommitter(
            Admin admin,
            Configuration flussConfig,
            Configuration lakeTieringConfig,
            LakeTieringFactory<WriteResult, Committable> lakeTieringFactory,
            FlussTableLakeSnapshotCommitter flussTableLakeSnapshotCommitter,
            MetricGroup metricGroup,
            @Nullable File localTmpDir) {
        this.admin = admin;
        this.localTmpDir = localTmpDir;
        this.flussConfig = flussConfig;
        this.lakeTieringConfig = lakeTieringConfig;
        this.lakeTieringFactory = lakeTieringFactory;
        this.flussTableLakeSnapshotCommitter = flussTableLakeSnapshotCommitter;
        MetricGroup dv = metricGroup.addGroup("derivedDeletionVectors");
        this.rounds = dv.counter("rounds", new ConcurrentCounter());
        this.changedKeys = dv.counter("changedKeys", new ConcurrentCounter());
        this.reindexes = dv.counter("reindexes", new ConcurrentCounter());
        this.reindexMs = dv.counter("reindexMs", new ConcurrentCounter());
        this.compactionHandOffs = dv.counter("compactionHandOffs", new ConcurrentCounter());
        this.chainEntriesTrimmed = dv.counter("chainEntriesTrimmed", new ConcurrentCounter());
        this.orphanChangesRemoved = dv.counter("orphanChangesRemoved", new ConcurrentCounter());
        dv.gauge("lastLookupMs", (Gauge<Long>) () -> lastLookupMs);
        dv.gauge("lastLakeCommitMs", (Gauge<Long>) () -> lastLakeCommitMs);
        dv.gauge("lastReindexMs", (Gauge<Long>) () -> lastReindexMs);
    }

    /** Commits a round of a table with deletion vectors, or holds it for a re-index. */
    Outcome<Committable> commitRound(
            long tableId,
            TablePath tablePath,
            TableInfo tableInfo,
            List<TableBucketWriteResult<WriteResult>> writeResults)
            throws Exception {
        return reportingRollback(
                tableId,
                () ->
                        lakeTieringFactory.mergesByPrimaryKey()
                                ? commitMergingRound(tableId, tablePath, tableInfo, writeResults)
                                : commitRound(tableId, tablePath, tableInfo, writeResults, false));
    }

    /**
     * Publishes the re-index a held round waited for from the readers' results. Then commits the
     * round on the re-indexed snapshot, or holds it again if the table was rewritten meanwhile.
     */
    Outcome<Committable> resumeRound(
            long tableId,
            Reindex reindex,
            List<RowPosResult> rowPosResults,
            List<TableBucketWriteResult<WriteResult>> writeResults)
            throws Exception {
        return reportingRollback(
                tableId, () -> resume(tableId, reindex, rowPosResults, writeResults));
    }

    private Outcome<Committable> resume(
            long tableId,
            Reindex reindex,
            List<RowPosResult> rowPosResults,
            List<TableBucketWriteResult<WriteResult>> writeResults)
            throws Exception {
        @SuppressWarnings("unchecked")
        Registration<Committable> registration = (Registration<Committable>) reindex.registration;
        // The readers skip the splits of a table dropped during the re-index. The change then
        // misses their parts, so it is not published.
        TableInfo tableInfo = admin.getTableInfo(reindex.tablePath).get();
        if (tableInfo.getTableId() != tableId) {
            throw new IllegalStateException(
                    String.format(
                            "Table %s (id %d) was dropped while its round waited for a re-index.",
                            reindex.tablePath, tableId));
        }
        publishReindex(
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
                    register(tableId, reindex.tablePath, registration);
            if (registration.committable == null) {
                // A lost commit of an earlier round was registered instead of this round's.
                throw lostCommitRegistered(
                        reindex.tablePath, registration.result.getCommittedSnapshotId());
            }
            return Outcome.committed(result);
        }
        // The round committed its own compaction before it was held.
        return commitRound(tableId, reindex.tablePath, tableInfo, writeResults, true);
    }

    /**
     * Runs a round. A rollback of the lake table, or rows another engine wrote to it, is logged
     * once as an error that names what to do. Rounds fail until the change is undone.
     */
    private Outcome<Committable> reportingRollback(long tableId, RoundCommit<Committable> round)
            throws Exception {
        try {
            Outcome<Committable> outcome = round.run();
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

    /**
     * Runs a round until it commits, fails or is held. The writers' staged index parts stay while
     * the round is held and are deleted when it ends either way. The change the round published
     * holds what it needs of them.
     */
    private Outcome<Committable> commitRound(
            long tableId,
            TablePath tablePath,
            TableInfo tableInfo,
            List<TableBucketWriteResult<WriteResult>> writeResults,
            boolean resumed)
            throws Exception {
        Map<TableBucket, StagedIndexPart> legacyParts =
                stageLegacyDeltas(tableId, tablePath, writeResults);
        boolean held = false;
        try {
            Outcome<Committable> outcome =
                    commitRound(tableId, tablePath, tableInfo, writeResults, resumed, legacyParts);
            held = outcome.reindex != null;
            return outcome;
        } finally {
            deleteStagedParts(tableId, tablePath, writeResults, legacyParts, held);
        }
    }

    private Outcome<Committable> commitRound(
            long tableId,
            TablePath tablePath,
            TableInfo tableInfo,
            List<TableBucketWriteResult<WriteResult>> writeResults,
            boolean resumed,
            Map<TableBucket, StagedIndexPart> legacyParts)
            throws Exception {
        boolean resuming = resumed;
        int baseChanges = 0;
        while (true) {
            try {
                return Outcome.committed(
                        commitOnce(
                                tableId,
                                tablePath,
                                tableInfo,
                                writeResults,
                                legacyParts,
                                resuming,
                                baseChanges < MAX_BASE_CHANGES));
            } catch (ReindexPending pending) {
                if (pending.reindex != null) {
                    return Outcome.held(pending.reindex);
                }
                // Run the round again at once in two cases. Files were only removed: the
                // re-index is published, and the lookups must not see the removed files. Or the
                // lake table moved past the base during the round: the next attempt re-indexes
                // the rewrite on the new snapshot.
                if (pending.baseChanged) {
                    baseChanges++;
                }
                resuming = true;
            }
        }
    }

    private TieringCommitResult<Committable> commitOnce(
            long tableId,
            TablePath tablePath,
            TableInfo tableInfo,
            List<TableBucketWriteResult<WriteResult>> committableWriteResults,
            Map<TableBucket, StagedIndexPart> legacyParts,
            boolean resuming,
            boolean rerunOnBaseChange)
            throws Exception {
        // Check if the table was dropped and recreated during tiering.
        // If the current table id differs from the committable's table id, fail this commit
        // to avoid dirty commit to a newly created table.
        if (tableInfo.getTableId() != tableId) {
            throw new IllegalStateException(
                    String.format(
                            "The current table id %s for table path %s is different from the table id %s in the committable. "
                                    + "This usually happens when a table was dropped and recreated during tiering. "
                                    + "Aborting commit to prevent dirty commit.",
                            tableInfo.getTableId(), tablePath, tableId));
        }

        // The base is read before the partitions. Every registration after a partition's drop
        // leaves out its buckets, so a partition the base lacks is not listed either. The round
        // then commits nothing there that it cannot look up in the base.
        LakeSnapshot flussCurrentLakeSnapshot = getLatestLakeSnapshot(tablePath);
        List<TableBucketWriteResult<WriteResult>> results =
                withoutResultsOfDroppedPartitions(tablePath, tableInfo, committableWriteResults);

        // filter down to buckets that actually produced data
        List<TableBucketWriteResult<WriteResult>> nonEmptyResults =
                results.stream().filter(r -> r.writeResult() != null).collect(Collectors.toList());

        // All buckets were empty, so nothing is committed. Rewrites since the latest snapshot are
        // still re-indexed, else an idle table keeps reading the snapshot before the rewrite.
        if (nonEmptyResults.isEmpty()) {
            reindexIdleTable(tableId, tablePath, tableInfo);
            return new TieringCommitResult<>(null, null);
        }

        try (LakeCommitter<WriteResult, Committable> lakeCommitter =
                lakeTieringFactory.createLakeCommitter(
                        new TieringCommitterInitContext(
                                tablePath, tableInfo, lakeTieringConfig, flussConfig))) {
            List<WriteResult> writeResults =
                    nonEmptyResults.stream()
                            .map(TableBucketWriteResult::writeResult)
                            .collect(Collectors.toList());

            // collect tiered offsets from all buckets, including those finished without writing
            // data, otherwise their splits would be regenerated forever; buckets with unknown
            // progress (e.g. splits skipped in this round) are excluded
            Map<TableBucket, Long> logEndOffsets = new HashMap<>();
            Map<TableBucket, Long> logMaxTieredTimestamps = new HashMap<>();
            for (TableBucketWriteResult<WriteResult> writeResult : results) {
                if (writeResult.logEndOffset() < 0) {
                    continue;
                }
                TableBucket tableBucket = writeResult.tableBucket();
                logEndOffsets.put(tableBucket, writeResult.logEndOffset());
                if (writeResult.maxTimestamp() >= 0) {
                    logMaxTieredTimestamps.put(tableBucket, writeResult.maxTimestamp());
                }
            }

            Committable committable = lakeCommitter.toCommittable(writeResults);
            RoundFiles round = new RoundFiles();
            try {
                if (flussCurrentLakeSnapshot != null) {
                    // Nothing is committed on top of a lake table rolled back outside Fluss.
                    lakeCommitter.checkCurrentSnapshotDescendsFrom(
                            flussCurrentLakeSnapshot.getSnapshotId());
                }
                // before commit to lake, check fluss not missing any lake snapshot committed by
                // fluss
                checkFlussNotMissingLakeSnapshot(
                        tablePath,
                        tableId,
                        lakeCommitter,
                        flussCurrentLakeSnapshot == null
                                ? null
                                : flussCurrentLakeSnapshot.getSnapshotId());
                if (lakeTieringConfig.get(ConfigOptions.LAKE_TIERING_PARTITION_MARK_DONE_ENABLED)
                        && lakeTieringFactory.supportsPartitionMarkDone()) {
                    preparePartitionMarkDone(lakeCommitter, committable);
                }
                return commitWithDeletes(
                        tableId,
                        tablePath,
                        tableInfo,
                        lakeCommitter,
                        committable,
                        nonEmptyResults,
                        legacyParts,
                        flussCurrentLakeSnapshot,
                        logEndOffsets,
                        logMaxTieredTimestamps,
                        resuming,
                        rerunOnBaseChange,
                        round);
            } catch (ReindexPending pending) {
                // The round is held or runs again, and keeps the files it wrote.
                throw pending;
            } catch (Exception e) {
                cleanUpFailedRound(tableId, tablePath, lakeCommitter, committable, round);
                throw e;
            }
        }
    }

    /**
     * Returns the round's results without those of partitions the Fluss table dropped, and deletes
     * their data files. A dropped partition's buckets are gone from the tablet servers, so a round
     * with deletes cannot look up and delete the old versions of its rows there; committing the new
     * ones would leave both live in the lake. A registration also leaves the partition's offsets
     * out, which the round's own offsets would put back. Its compactions are left out too, and its
     * staged index parts go with the round's staging change. Nothing reads the partition's log
     * again, so its rows stay as the last registered snapshot has them.
     */
    private List<TableBucketWriteResult<WriteResult>> withoutResultsOfDroppedPartitions(
            TablePath tablePath,
            TableInfo tableInfo,
            List<TableBucketWriteResult<WriteResult>> results)
            throws Exception {
        if (!tableInfo.isPartitioned()) {
            return results;
        }
        Set<Long> partitionIds = new HashSet<>();
        // Includes system partitions such as the historical partition, which are tiered too.
        for (PartitionInfo partition : admin.listPartitionInfos(tablePath, true).get()) {
            partitionIds.add(partition.getPartitionId());
        }
        List<TableBucketWriteResult<WriteResult>> kept = new ArrayList<>();
        List<WriteResult> droppedData = new ArrayList<>();
        Set<String> dropped = new TreeSet<>();
        for (TableBucketWriteResult<WriteResult> r : results) {
            Long partitionId = r.tableBucket().getPartitionId();
            if (partitionId == null || partitionIds.contains(partitionId)) {
                kept.add(r);
                continue;
            }
            dropped.add(
                    r.partitionName() == null ? String.valueOf(partitionId) : r.partitionName());
            if (r.writeResult() != null) {
                droppedData.add(r.writeResult());
            }
        }
        if (dropped.isEmpty()) {
            return results;
        }
        LOG.info(
                "Derived DV: partitions {} of {} were dropped; the round commits none of their {}"
                        + " written buckets.",
                dropped,
                tablePath,
                droppedData.size());
        if (!droppedData.isEmpty()) {
            try (LakeCommitter<WriteResult, Committable> lakeCommitter =
                    lakeTieringFactory.createLakeCommitter(
                            new TieringCommitterInitContext(
                                    tablePath, tableInfo, lakeTieringConfig, flussConfig))) {
                lakeCommitter.abort(lakeCommitter.toCommittable(droppedData));
            } catch (Exception e) {
                LOG.warn(
                        "Derived DV: could not delete the data files of the dropped partitions"
                                + " {} of {}.",
                        dropped,
                        tablePath,
                        e);
            }
        }
        return kept;
    }

    /**
     * What a round wrote besides the committable's files, and how far its lake commit got. A failed
     * round deletes its files only when no lake snapshot can reference them.
     */
    private static final class RoundFiles {
        @Nullable String offsetsFile;
        @Nullable Long indexChangeId;
        boolean lakeCommitAttempted;
        boolean lakeCommitRejected;
    }

    /**
     * Deletes the files of a failed round: its data and delete files, its offsets file and its
     * index change. Keeps them if the lake commit was attempted and not definitely rejected. The
     * lake snapshot may then exist and reference them, and the next round repairs it.
     */
    private void cleanUpFailedRound(
            long tableId,
            TablePath tablePath,
            LakeCommitter<WriteResult, Committable> lakeCommitter,
            Committable committable,
            RoundFiles round) {
        if (round.lakeCommitAttempted && !round.lakeCommitRejected) {
            LOG.warn(
                    "Derived DV: the failed round of {} reached its lake commit; keeping its"
                            + " files, as a lake snapshot may reference them.",
                    tablePath);
            return;
        }
        try {
            lakeCommitter.abort(committable);
        } catch (Exception e) {
            LOG.warn(
                    "Derived DV: could not delete the data files of a failed round of {}.",
                    tablePath,
                    e);
        }
        discardAttempt(tableId, tablePath, round);
        LOG.info(
                "Derived DV: deleted the files of the failed round of {} (offsets file {}, index"
                        + " change {}).",
                tablePath,
                round.offsetsFile,
                round.indexChangeId);
    }

    /**
     * Deletes the offsets file and the index change of a round's attempt that the lake rejected.
     * The round's data files stay for its next attempt.
     */
    private void discardAttempt(long tableId, TablePath tablePath, RoundFiles round) {
        if (round.offsetsFile != null) {
            try {
                FsPath offsetsFile = new FsPath(round.offsetsFile);
                offsetsFile.getFileSystem().delete(offsetsFile, false);
            } catch (Exception e) {
                LOG.warn(
                        "Derived DV: could not delete the offsets file {} of a failed round of {}.",
                        round.offsetsFile,
                        tablePath,
                        e);
            }
        }
        if (round.indexChangeId != null) {
            try {
                indexStore(tableId, tablePath).abortChange(round.indexChangeId);
            } catch (Exception e) {
                LOG.warn(
                        "Derived DV: could not delete the index change {} of a failed round of {}.",
                        round.indexChangeId,
                        tablePath,
                        e);
            }
        }
    }

    /**
     * Runs a round without data: re-indexes the lake files another engine rewrote since the latest
     * snapshot, which makes the rewritten snapshot readable. The resumed round comes back here and
     * finds nothing new to re-index, unless the table was rewritten again meanwhile.
     */
    private void reindexIdleTable(long tableId, TablePath tablePath, TableInfo tableInfo)
            throws Exception {
        LakeSnapshot base = getLatestLakeSnapshot(tablePath);
        if (base == null) {
            LOG.info(
                    "Commit tiering write results is empty for table {} ({}).", tablePath, tableId);
            return;
        }
        triggerReindex(tableId, tablePath, tableInfo, base);
        LOG.info(
                "Derived DV: idle round of {} found no rewrite since snapshot {}.",
                tablePath,
                base.getSnapshotId());
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
    private Outcome<Committable> commitMergingRound(
            long tableId,
            TablePath tablePath,
            TableInfo tableInfo,
            List<TableBucketWriteResult<WriteResult>> committableWriteResults)
            throws Exception {
        if (tableInfo.getTableId() != tableId) {
            throw new IllegalStateException(
                    String.format(
                            "The current table id %s for table path %s is different from the table id %s in the committable. "
                                    + "This usually happens when a table was dropped and recreated during tiering. "
                                    + "Aborting commit to prevent dirty commit.",
                            tableInfo.getTableId(), tablePath, tableId));
        }
        List<TableBucketWriteResult<WriteResult>> results =
                withoutResultsOfDroppedPartitions(tablePath, tableInfo, committableWriteResults);
        List<TableBucketWriteResult<WriteResult>> nonEmptyResults =
                results.stream().filter(r -> r.writeResult() != null).collect(Collectors.toList());
        try (LakeCommitter<WriteResult, Committable> lakeCommitter =
                lakeTieringFactory.createLakeCommitter(
                        new TieringCommitterInitContext(
                                tablePath, tableInfo, lakeTieringConfig, flussConfig))) {
            if (nonEmptyResults.isEmpty()) {
                // The readable snapshot is computed on commits. After another engine compacted an
                // idle table, a round commits the registered offsets again, without data, so the
                // compaction becomes readable and is indexed without waiting for a write.
                LakeSnapshot registered = getLatestLakeSnapshot(tablePath);
                if (registered == null
                        || !lakeCommitter.readableSnapshotMayAdvanceSince(
                                registered.getSnapshotId())) {
                    LOG.info(
                            "Commit tiering write results is empty for table {} ({}).",
                            tablePath,
                            tableId);
                    return Outcome.committed(new TieringCommitResult<>(null, null));
                }
                LOG.info(
                        "Derived DV: idle round of {} found a compaction since snapshot {}; it"
                                + " commits no data to make the compaction readable.",
                        tablePath,
                        registered.getSnapshotId());
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
            Committable committable =
                    lakeCommitter.toCommittable(
                            nonEmptyResults.stream()
                                    .map(TableBucketWriteResult::writeResult)
                                    .collect(Collectors.toList()));
            RoundFiles round = new RoundFiles();
            String offsetsFile;
            Map<TableBucket, Long> tieredLogEndOffsets = new HashMap<>();
            LakeSnapshot flussCurrentLakeSnapshot;
            Reindex lostCommitReindex;
            try {
                flussCurrentLakeSnapshot = getLatestLakeSnapshot(tablePath);
                if (flussCurrentLakeSnapshot != null) {
                    lakeCommitter.checkCurrentSnapshotDescendsFrom(
                            flussCurrentLakeSnapshot.getSnapshotId());
                }
                lostCommitReindex =
                        registerLostCommit(
                                tableId,
                                tablePath,
                                tableInfo,
                                lakeCommitter,
                                flussCurrentLakeSnapshot == null
                                        ? null
                                        : flussCurrentLakeSnapshot.getSnapshotId());
            } catch (Exception e) {
                cleanUpFailedRound(tableId, tablePath, lakeCommitter, committable, round);
                throw e;
            }
            if (lostCommitReindex != null) {
                // This round runs again on top of the lost commit once that is registered.
                cleanUpFailedRound(tableId, tablePath, lakeCommitter, committable, round);
                discardHandedOverIndex(indexStore(tableId, tablePath), nonEmptyResults);
                return Outcome.held(lostCommitReindex);
            }
            try {
                if (lakeTieringConfig.get(ConfigOptions.LAKE_TIERING_PARTITION_MARK_DONE_ENABLED)
                        && lakeTieringFactory.supportsPartitionMarkDone()) {
                    preparePartitionMarkDone(lakeCommitter, committable);
                }
                if (flussCurrentLakeSnapshot != null) {
                    tieredLogEndOffsets.putAll(
                            withoutDroppedPartitions(
                                    tablePath, flussCurrentLakeSnapshot.getTableBucketsOffset()));
                }
                tieredLogEndOffsets.putAll(logEndOffsets);
                // The offsets file lists every bucket, so dropped partitions' offsets end here.
                offsetsFile =
                        flussTableLakeSnapshotCommitter.prepareCompleteLakeSnapshot(
                                tableId, tablePath, tieredLogEndOffsets);
                round.offsetsFile = offsetsFile;
                // No base fence guards a merging commit. A round of a table taken from this
                // service would commit its older rows on top of the new owner's round.
                flussTableLakeSnapshotCommitter.checkTieringEpoch(tableId);
            } catch (Exception e) {
                cleanUpFailedRound(tableId, tablePath, lakeCommitter, committable, round);
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
            // The outcome of a failed commit is unknown, so its files are kept.
            LakeCommitResult result =
                    lakeCommitter.commit(
                            committable,
                            new LakeCommitter.CommitContext(
                                    snapshotProperties, tieredLogEndOffsets));
            lastLakeCommitMs = System.currentTimeMillis() - t0;
            if (flussCurrentLakeSnapshot != null) {
                // A rollback during the round leaves the commit on the rolled-back table. The
                // commit is not registered, and the next rounds stop until the rollback is undone.
                lakeCommitter.checkCurrentSnapshotDescendsFrom(
                        flussCurrentLakeSnapshot.getSnapshotId());
            }
            rounds.inc();
            Registration<Committable> registration =
                    new Registration<>(
                            result,
                            offsetsFile,
                            logEndOffsets,
                            logMaxTieredTimestamps,
                            committable);
            Reindex reindex =
                    indexReadableSnapshot(
                            tableId, tablePath, tableInfo, registration, nonEmptyResults);
            if (reindex != null) {
                return Outcome.held(reindex);
            }
            return Outcome.committed(register(tableId, tablePath, registration));
        }
    }

    /**
     * Brings the chain up to the readable snapshot of a merging round's commit. Returns the
     * re-index the round waits for, or null when the round can register: the chain has the readable
     * snapshot now, or there is none.
     */
    @Nullable
    @SuppressWarnings("unchecked")
    private Reindex indexReadableSnapshot(
            long tableId,
            TablePath tablePath,
            TableInfo tableInfo,
            Registration<Committable> registration,
            List<TableBucketWriteResult<WriteResult>> results)
            throws Exception {
        LakeCommitResult result = registration.result;
        Long readable =
                result.committedIsReadable()
                        ? Long.valueOf(result.getCommittedSnapshotId())
                        : result.getReadableSnapshot() == null
                                ? null
                                : result.getReadableSnapshot().getReadableSnapshotId();
        PositionIndexStore store = indexStore(tableId, tablePath);
        if (readable == null) {
            discardHandedOverIndex(store, results);
            return null;
        }
        List<PositionIndexStore.ChainEntry> chain = store.chain();
        for (PositionIndexStore.ChainEntry e : chain) {
            if (e.snapshotId == readable) {
                // nothing newly readable, so the compactions' positions have no change to make
                discardHandedOverIndex(store, results);
                return null;
            }
        }
        long base = chain.isEmpty() ? -1L : chain.get(chain.size() - 1).snapshotId;
        LakeSource<?> lakeSource =
                lakeTieringFactory.createLakeSource(tableInfo.getLakeTablePath());
        checkState(
                lakeSource != null,
                "Lake tiering factory %s merges by primary key but creates no lake source.",
                lakeTieringFactory.getClass().getName());
        return planIndexChange(
                tableId,
                tablePath,
                (LakeSource<LakeSplit>) lakeSource,
                store,
                base,
                readable,
                registration,
                results);
    }

    @Nullable
    private <Split extends LakeSplit> Reindex planIndexChange(
            long tableId,
            TablePath tablePath,
            LakeSource<Split> lakeSource,
            PositionIndexStore store,
            long base,
            long readable,
            Registration<Committable> registration,
            List<TableBucketWriteResult<WriteResult>> results)
            throws Exception {
        long t0 = System.currentTimeMillis();
        DataDeltaPlan<Split> plan = lakeSource.planRewrite(base, readable);
        Map<String, Long> partitionIds = Collections.emptyMap();
        if (plan != null) {
            partitionIds = partitionIds(tablePath, plan);
            plan = withoutFilesOfDroppedPartitions(tablePath, plan, partitionIds);
        }
        long changeId = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        if (plan == null || plan.getSplits().isEmpty()) {
            // Nothing to read, e.g. a compaction whose output has no rows: publish at once.
            discardHandedOverIndex(store, results);
            publishReindex(
                    tableId,
                    tablePath,
                    base,
                    readable,
                    changeId,
                    Collections.emptyList(),
                    plan == null ? null : plan.getDeletedFiles(),
                    t0,
                    false);
            return null;
        }
        checkState(
                plan.getCompactSnapshotId() == readable,
                "Planned the index of snapshot %s instead of the readable snapshot %s.",
                plan.getCompactSnapshotId(),
                readable);
        if (publishHandedOverIndex(tablePath, store, plan, base, readable, results)) {
            return null;
        }
        int firstFileId = store.allocateFileIds(plan.getSplits().size());
        FsPath lakeDir =
                FlussPaths.remoteLakeTableSnapshotDir(
                        FlussConfigUtils.getDefaultRemoteDataDir(flussConfig), tablePath, tableId);
        Reindex reindex =
                planReindexScan(
                        tableId,
                        tablePath,
                        lakeSource,
                        plan,
                        partitionIds,
                        firstFileId,
                        lakeDir,
                        base,
                        changeId,
                        registration);
        LOG.info(
                "Derived DV: readable snapshot {} of {} has {} new readable files since snapshot {}"
                        + " ({} left); indexing them on the tiering readers.",
                readable,
                tablePath,
                plan.getSplits().size(),
                base,
                plan.getDeletedFiles().values().stream()
                        .flatMap(m -> m.values().stream())
                        .mapToInt(List::size)
                        .sum());
        return reindex;
    }

    /**
     * Publishes the index change of a merging round's newly readable snapshot from the positions
     * the round's compactions in the tiering writers handed over ({@link CompactionIndexPart}),
     * instead of re-indexing the snapshot's new files. In every bucket, the compactions must have
     * written exactly the files that became readable since the chain's end and replaced exactly
     * those that left it. Otherwise returns false and the files are re-indexed, e.g. for another
     * engine's compaction, one without a hand-off, or an earlier round's files not yet indexed.
     */
    private <Split extends LakeSplit> boolean publishHandedOverIndex(
            TablePath tablePath,
            PositionIndexStore store,
            DataDeltaPlan<Split> plan,
            long base,
            long readable,
            List<TableBucketWriteResult<WriteResult>> results) {
        Map<TableBucket, CompactionIndexPart> parts = new HashMap<>();
        // partition name (null if unpartitioned) -> bucket id -> bucket
        Map<String, Map<Integer, TableBucket>> buckets = new HashMap<>();
        Set<Long> changeIds = new HashSet<>();
        String reason = null;
        for (TableBucketWriteResult<WriteResult> r : results) {
            Object wr = r.writeResult();
            if (!(wr instanceof CompactionIndexPartProvider)
                    || !((CompactionIndexPartProvider) wr).compacted()) {
                continue;
            }
            CompactionIndexPart part = ((CompactionIndexPartProvider) wr).compactionIndexPart();
            if (part == null) {
                reason = "a compacted bucket handed no positions over";
                continue;
            }
            parts.put(r.tableBucket(), part);
            changeIds.add(part.changeId());
            buckets.computeIfAbsent(r.partitionName(), k -> new HashMap<>())
                    .put(r.tableBucket().getBucket(), r.tableBucket());
        }
        if (parts.isEmpty() && reason == null) {
            return false; // no compaction of this round
        }
        if (reason == null && changeIds.size() > 1) {
            reason = "the compactions handed over different changes";
        }
        if (reason == null) {
            Map<TableBucket, Set<String>> added = new HashMap<>();
            for (Split split : plan.getSplits()) {
                String partitionName = partitionName(split);
                TableBucket tb =
                        buckets.getOrDefault(partitionName, Collections.emptyMap())
                                .get(split.bucket());
                if (tb == null) {
                    reason =
                            "files of bucket "
                                    + split.bucket()
                                    + " of partition "
                                    + partitionName
                                    + " became readable that no compaction of the round wrote";
                    break;
                }
                added.computeIfAbsent(tb, k -> new HashSet<>()).add(split.fileName());
            }
            Map<TableBucket, Set<String>> removed = new HashMap<>();
            for (Map.Entry<String, Map<Integer, List<String>>> pe :
                    plan.getDeletedFiles().entrySet()) {
                for (Map.Entry<Integer, List<String>> be : pe.getValue().entrySet()) {
                    TableBucket tb =
                            buckets.getOrDefault(pe.getKey(), Collections.emptyMap())
                                    .get(be.getKey());
                    if (tb == null) {
                        reason =
                                "files of bucket "
                                        + be.getKey()
                                        + " of partition "
                                        + pe.getKey()
                                        + " left the readable snapshot that no compaction of"
                                        + " the round replaced";
                        break;
                    }
                    removed.computeIfAbsent(tb, k -> new HashSet<>()).addAll(be.getValue());
                }
            }
            for (Map.Entry<TableBucket, CompactionIndexPart> e : parts.entrySet()) {
                if (reason != null) {
                    break;
                }
                TableBucket tb = e.getKey();
                if (!new HashSet<>(e.getValue().addedFiles().values())
                                .equals(added.getOrDefault(tb, Collections.emptySet()))
                        || !new HashSet<>(e.getValue().removedFiles())
                                .equals(removed.getOrDefault(tb, Collections.emptySet()))) {
                    reason =
                            "the files the compaction of "
                                    + tb
                                    + " wrote or replaced are not those of readable snapshot "
                                    + readable;
                }
            }
        }
        if (reason == null) {
            long changeId = changeIds.iterator().next();
            try {
                long t0 = System.currentTimeMillis();
                String identity = snapshotIdentity(tablePath, readable);
                store.commitChange(changeId, assignFileIds(store, changeId, parts));
                store.register(base, readable, changeId, identity);
                compactionHandOffs.inc();
                LOG.info(
                        "Derived DV: the compactions of {} in the tiering writers handed the"
                                + " positions of readable snapshot {} to index change {} ({}"
                                + " buckets, no re-index), published in {} ms.",
                        tablePath,
                        readable,
                        changeId,
                        parts.size(),
                        System.currentTimeMillis() - t0);
                return true;
            } catch (Exception e) {
                reason = "publishing failed: " + e;
            }
        }
        LOG.info(
                "Derived DV: readable snapshot {} of {} is re-indexed from the lake: {}.",
                readable,
                tablePath,
                reason);
        for (long changeId : changeIds) {
            try {
                store.abortChange(changeId);
            } catch (Exception e) {
                LOG.debug("Derived DV: could not delete index change {}.", changeId, e);
            }
        }
        return false;
    }

    /** Deletes the positions the round's compactions handed over, which no change publishes. */
    private void discardHandedOverIndex(
            PositionIndexStore store, List<TableBucketWriteResult<WriteResult>> results) {
        Set<Long> changeIds = new HashSet<>();
        for (TableBucketWriteResult<WriteResult> r : results) {
            Object wr = r.writeResult();
            if (wr instanceof CompactionIndexPartProvider
                    && ((CompactionIndexPartProvider) wr).compactionIndexPart() != null) {
                changeIds.add(((CompactionIndexPartProvider) wr).compactionIndexPart().changeId());
            }
        }
        for (long changeId : changeIds) {
            try {
                store.abortChange(changeId);
            } catch (Exception e) {
                LOG.debug("Derived DV: could not delete index change {}.", changeId, e);
            }
        }
    }

    /**
     * Resolves the changed keys' positions in the base snapshot and commits data and deletion
     * vectors in one lake commit, so every lake snapshot is consistent at its tiered offsets.
     */
    private TieringCommitResult<Committable> commitWithDeletes(
            long tableId,
            TablePath tablePath,
            TableInfo tableInfo,
            LakeCommitter<WriteResult, Committable> lakeCommitter,
            Committable committable,
            List<TableBucketWriteResult<WriteResult>> nonEmptyResults,
            Map<TableBucket, StagedIndexPart> legacyParts,
            @Nullable LakeSnapshot flussCurrentLakeSnapshot,
            Map<TableBucket, Long> logEndOffsets,
            Map<TableBucket, Long> logMaxTieredTimestamps,
            boolean resuming,
            boolean rerunOnBaseChange,
            RoundFiles round)
            throws Exception {
        long base =
                flussCurrentLakeSnapshot == null ? -1L : flussCurrentLakeSnapshot.getSnapshotId();
        long t0 = System.currentTimeMillis();
        // A compaction since the base, by Fluss or another engine, is re-indexed and made
        // readable before the round resolves its keys.
        if (base > 0) {
            // The round's own compaction is committed first, so the round's deletes never
            // invalidate it. A resumed round did so before it was held.
            if (!resuming) {
                Long rewrite = lakeCommitter.commitPendingRewrite(committable);
                if (publishCompactionIndex(
                        tableId, tablePath, flussCurrentLakeSnapshot, rewrite, nonEmptyResults)) {
                    // The rewrite is in the index chain and registered: run the round on it.
                    throw new ReindexPending(null);
                }
            }
            // Holds the round while the tiering readers re-index the rewritten files.
            triggerReindex(tableId, tablePath, tableInfo, flussCurrentLakeSnapshot);
        }
        // 1. The writers' parts of the index change, staged in the store when they completed.
        PositionIndexStore store = indexStore(tableId, tablePath);
        Map<TableBucket, StagedIndexPart> parts =
                stagedParts(tablePath, nonEmptyResults, legacyParts);
        long t1 = System.currentTimeMillis();
        // 2. The snapshot's offsets: this round's, and the base's for the other buckets. Dropped
        //    partitions are left out, since no tablet server has their buckets. The offsets file
        //    holds all the buckets, so the dropped ones are not merged back in either.
        Map<TableBucket, Long> readableOffsets = new HashMap<>();
        if (flussCurrentLakeSnapshot != null) {
            readableOffsets.putAll(
                    withoutDroppedPartitions(
                            tablePath, flussCurrentLakeSnapshot.getTableBucketsOffset()));
        }
        readableOffsets.putAll(logEndOffsets);
        Set<TableBucket> buckets = readableOffsets.keySet();
        String lakeBucketTieredOffsetsFile =
                flussTableLakeSnapshotCommitter.prepareCompleteLakeSnapshot(
                        tableId, tablePath, readableOffsets);
        round.offsetsFile = lakeBucketTieredOffsetsFile;
        Map<String, String> snapshotProperties = new HashMap<>();
        snapshotProperties.put(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, lakeBucketTieredOffsetsFile);
        // 3. Publish the writers' staged parts as the round's index change before the lake commit,
        //    under a new id the snapshot records. A crash after the lake commit is repaired by
        //    repairSnapshot; one before it leaves an orphan. Publishing streams every changed key
        //    and looks it up in the base on the way. The found positions are the rows the round
        //    supersedes, kept as a bitmap per data file.
        long indexId = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        round.indexChangeId = indexId;
        PublishedParts published =
                publishParts(
                        store,
                        indexId,
                        flussCurrentLakeSnapshot,
                        parts,
                        (tb, snapshotId, keys) ->
                                lookupWithRetry(
                                        admin, tablePath, tb, snapshotId, keys, Thread::sleep));
        // Only the buckets the round changed are part of the change. The buckets of idle
        // partitions get no manifest, and trimming can tell which changes touch which buckets.
        store.commitChange(indexId, published.changes);
        // The lake commit makes the change permanent: repairSnapshot registers a crashed round's
        // change. A change the chain would refuse fails this round here, before the commit,
        // instead of every round after it.
        store.checkRegistrable(base, indexId);
        long t2 = System.currentTimeMillis();
        // 4. One lake commit of data and deletion vectors, validated against the base.
        snapshotProperties.put(DERIVED_INDEX_ID_PROPERTY, String.valueOf(indexId));
        snapshotProperties.put(DERIVED_BASE_PROPERTY, String.valueOf(base));
        snapshotProperties.put(DERIVED_OFFSETS_PROPERTY, encodeOffsets(readableOffsets));
        FormatVersions.putSnapshotVersion(snapshotProperties);
        // The base is this service's only while its tiering epoch is current. A round of a table
        // taken from this service would commit its older rows on top of the new owner's round.
        flussTableLakeSnapshotCommitter.checkTieringEpoch(tableId);
        round.lakeCommitAttempted = true;
        LakeCommitResult result;
        try {
            result =
                    lakeCommitter.commitWithDeletes(
                            committable, snapshotProperties, published.deleted, base);
        } catch (LakeCommitRejectedException e) {
            round.lakeCommitRejected = true;
            if (e instanceof LakeBaseChangedException && rerunOnBaseChange) {
                // Another engine committed during the round, e.g. a rewrite of files this round
                // does not touch. The deletes and the index change are relative to the base, so
                // the round runs again on the new snapshot and re-indexes the rewrite first. If
                // the new owner of a table taken from this service committed, the round stops.
                flussTableLakeSnapshotCommitter.checkTieringEpoch(tableId);
                LOG.info(
                        "Derived DV: {} moved past base snapshot {} during the round ({}); the"
                                + " round runs again on its current snapshot.",
                        tablePath,
                        base,
                        e.getCause() == null ? e.getMessage() : e.getCause().getMessage());
                discardAttempt(tableId, tablePath, round);
                throw ReindexPending.baseChanged();
            }
            throw e;
        }
        long snapshotId = result.getCommittedSnapshotId();
        // Links the base to snapshotId. The store refuses a chain that does not end at the base.
        // The base fence put the lake commit on the base or on snapshots after it that changed no
        // data file, so the change from the base covers the snapshot.
        store.register(base, snapshotId, indexId);
        maybeTrimIndexChain(tableId, tablePath, store, readableOffsets);
        long t3 = System.currentTimeMillis();
        // 5. Register in Fluss. The snapshot is readable at its tiered offsets, since its deletes
        //    are in the same commit, and tablet servers catch up to it.
        flussTableLakeSnapshotCommitter.commit(
                tableId,
                tablePath,
                readableAtTieredOffsets(
                        snapshotId,
                        readableOffsets,
                        result.getTieringStats(),
                        earliestSnapshotToKeep(tableId, tablePath)),
                lakeBucketTieredOffsetsFile,
                logEndOffsets,
                logMaxTieredTimestamps);
        // Old lake snapshots are expired if the table expires them, except those of the chain.
        lakeCommitter.expireSnapshots(earliestSnapshotToKeep(tableId, tablePath));
        long t4 = System.currentTimeMillis();
        rounds.inc();
        changedKeys.inc(published.keys);
        lastLookupMs = t2 - t1;
        lastLakeCommitMs = t3 - t2;
        LOG.info(
                "Derived DV round {}: base {} -> snapshot {} (index {}); buckets {}; changed keys {}"
                        + " ({} deleted, {} in base); preparing {} ms; index publish with lookups {}"
                        + " ms; lake commit (data+DV) {} ms; fluss report {} ms.",
                tablePath,
                base,
                snapshotId,
                indexId,
                buckets.size(),
                published.keys,
                published.deletes,
                published.found,
                t1 - t0,
                t2 - t1,
                t3 - t2,
                t4 - t3);
        return new TieringCommitResult<>(committable, result.getTieringStats());
    }

    /**
     * Returns the staged index part of every bucket with a write result: the writer's, or, for a
     * write result of an older version that carries an index delta, the one this committer staged.
     */
    private Map<TableBucket, StagedIndexPart> stagedParts(
            TablePath tablePath,
            List<TableBucketWriteResult<WriteResult>> nonEmptyResults,
            Map<TableBucket, StagedIndexPart> legacyParts) {
        Map<TableBucket, StagedIndexPart> parts = new HashMap<>();
        for (TableBucketWriteResult<WriteResult> r : nonEmptyResults) {
            Object wr = r.writeResult();
            StagedIndexPart part =
                    wr instanceof StagedIndexPartProvider
                            ? ((StagedIndexPartProvider) wr).stagedIndexPart()
                            : null;
            if (part == null) {
                part = legacyParts.get(r.tableBucket());
            }
            if (part != null) {
                parts.put(r.tableBucket(), part);
            } else if (wr instanceof StagedIndexPartProvider
                    || wr instanceof PositionIndexDeltaProvider) {
                // committing its rows without their index entries and deletes would leave
                // superseded rows live
                throw new IllegalStateException(
                        String.format(
                                "The write result of bucket %s of %s carries no position index"
                                        + " part; was its writer given the position index"
                                        + " directory?",
                                r.tableBucket(), tablePath));
            }
        }
        return parts;
    }

    /**
     * Stages the in-memory index deltas of write results from an older version. They are then
     * published like the parts writers stage, under a staging change of their own.
     */
    private Map<TableBucket, StagedIndexPart> stageLegacyDeltas(
            long tableId, TablePath tablePath, List<TableBucketWriteResult<WriteResult>> results)
            throws IOException {
        Map<TableBucket, StagedIndexPart> parts = new HashMap<>();
        long stagingId = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        for (TableBucketWriteResult<WriteResult> r : results) {
            Object wr = r.writeResult();
            if (!(wr instanceof PositionIndexDeltaProvider)
                    || (wr instanceof StagedIndexPartProvider
                            && ((StagedIndexPartProvider) wr).stagedIndexPart() != null)) {
                continue;
            }
            PositionIndexDelta delta = ((PositionIndexDeltaProvider) wr).positionIndexDelta();
            if (delta != null) {
                parts.put(
                        r.tableBucket(),
                        stageDelta(
                                indexStore(tableId, tablePath), stagingId, r.tableBucket(), delta));
            }
        }
        return parts;
    }

    @VisibleForTesting
    static StagedIndexPart stageDelta(
            PositionIndexStore store, long stagingId, TableBucket bucket, PositionIndexDelta delta)
            throws IOException {
        TreeMap<byte[], FilePos> entries =
                new TreeMap<>(PositionIndexChangeBuilder::compareUnsigned);
        // A changed key without a new row in the puts stays a tombstone.
        for (byte[] key : delta.changedKeys()) {
            entries.put(key, null);
        }
        for (byte[] key : delta.deletes()) {
            entries.put(key, null);
        }
        Map<String, Integer> ids = new HashMap<>();
        Map<Integer, String> files = new HashMap<>();
        for (KeyPosition kp : delta.puts()) {
            int id =
                    ids.computeIfAbsent(
                            kp.file(),
                            f -> {
                                files.put(files.size(), f);
                                return files.size() - 1;
                            });
            entries.put(kp.key(), new FilePos(id, kp.pos()));
        }
        long deletes = 0;
        try (PositionIndexStore.StagedPartWriter writer =
                store.stageBucket(stagingId, bucket, "legacy_", null)) {
            for (Map.Entry<byte[], FilePos> e : entries.entrySet()) {
                if (e.getValue() == null) {
                    writer.delete(e.getKey());
                    deletes++;
                } else {
                    writer.put(e.getKey(), e.getValue());
                }
            }
            return StagedIndexPart.of(stagingId, writer.finish(), files, entries.size(), deletes);
        }
    }

    /**
     * Deletes the staging changes of a round's parts. The writers' ones are deleted once the round
     * ends. The ones this committer staged for older versions' write results are always deleted,
     * since a resumed round stages them again.
     */
    private void deleteStagedParts(
            long tableId,
            TablePath tablePath,
            List<TableBucketWriteResult<WriteResult>> results,
            Map<TableBucket, StagedIndexPart> legacyParts,
            boolean held) {
        Set<Long> stagingIds = new HashSet<>();
        for (StagedIndexPart part : legacyParts.values()) {
            stagingIds.add(part.stagingId());
        }
        if (!held) {
            for (TableBucketWriteResult<WriteResult> r : results) {
                Object wr = r.writeResult();
                if (wr instanceof StagedIndexPartProvider) {
                    StagedIndexPart part = ((StagedIndexPartProvider) wr).stagedIndexPart();
                    if (part != null) {
                        stagingIds.add(part.stagingId());
                    }
                }
            }
        }
        for (long stagingId : stagingIds) {
            try {
                indexStore(tableId, tablePath).abortChange(stagingId);
            } catch (Exception e) {
                // removed as an orphan later
                LOG.warn(
                        "Derived DV: could not delete the staged index parts {} of {}.",
                        stagingId,
                        tablePath,
                        e);
            }
        }
    }

    /** What publishing the writers' parts produced. */
    @VisibleForTesting
    static final class PublishedParts {
        final Map<TableBucket, PositionIndexStore.BucketChange> changes = new HashMap<>();
        final DeletedPositions deleted = new DeletedPositions();
        long keys;
        long deletes;
        long found;
    }

    /**
     * Publishes every bucket's staged part under change {@code indexId}, buckets in parallel, with
     * file ids from the table's counter. While it streams a part's keys, it looks up their base
     * positions in chunks of {@link #LOOKUP_CHUNK} keys, at most {@link #LOOKUPS_IN_FLIGHT} chunks
     * at a time. Each found position goes into its data file's bitmap. A bucket not in the base has
     * nothing to look up.
     */
    @VisibleForTesting
    static PublishedParts publishParts(
            PositionIndexStore store,
            long indexId,
            @Nullable LakeSnapshot baseSnapshot,
            Map<TableBucket, StagedIndexPart> parts,
            PositionLookup lookup)
            throws Exception {
        PublishedParts out = new PublishedParts();
        if (parts.isEmpty()) {
            return out;
        }
        List<TableBucket> order = new ArrayList<>(parts.keySet());
        order.sort(BUCKET_ORDER);
        // File ids come from the table's persisted counter; a colliding id masks the wrong file.
        int files = 0;
        for (StagedIndexPart part : parts.values()) {
            files += part.files().size();
        }
        int nextId = files == 0 ? 0 : store.allocateFileIds(files);
        Map<TableBucket, Map<Integer, Integer>> newIds = new HashMap<>();
        Map<TableBucket, Map<Integer, String>> addedFiles = new HashMap<>();
        for (TableBucket tb : order) {
            Map<Integer, Integer> ids = new HashMap<>();
            Map<Integer, String> added = new HashMap<>();
            for (Map.Entry<Integer, String> f : new TreeMap<>(parts.get(tb).files()).entrySet()) {
                ids.put(f.getKey(), nextId);
                added.put(nextId, f.getValue());
                nextId++;
            }
            newIds.put(tb, ids);
            addedFiles.put(tb, added);
        }
        long base = baseSnapshot == null ? -1L : baseSnapshot.getSnapshotId();
        Map<TableBucket, Long> inBase =
                base > 0 ? baseSnapshot.getTableBucketsOffset() : Collections.emptyMap();
        ExecutorService publishers =
                Executors.newFixedThreadPool(Math.min(PUBLISH_THREADS, order.size()));
        ExecutorService lookups = Executors.newFixedThreadPool(LOOKUPS_IN_FLIGHT);
        Semaphore inFlight = new Semaphore(LOOKUPS_IN_FLIGHT);
        try {
            Map<TableBucket, Future<BucketLookups>> futures = new HashMap<>();
            for (TableBucket tb : order) {
                StagedIndexPart part = parts.get(tb);
                BucketLookups bucketLookups =
                        new BucketLookups(
                                tb, base, inBase.containsKey(tb), lookup, lookups, inFlight);
                futures.put(
                        tb,
                        publishers.submit(
                                () -> {
                                    bucketLookups.published =
                                            store.publishStaged(
                                                    part.stagingId(),
                                                    indexId,
                                                    tb,
                                                    part.part(),
                                                    newIds.get(tb),
                                                    bucketLookups::accept);
                                    bucketLookups.finish();
                                    return bucketLookups;
                                }));
            }
            for (TableBucket tb : order) {
                BucketLookups done = futures.get(tb).get();
                StagedIndexPart part = parts.get(tb);
                out.changes.put(
                        tb,
                        new PositionIndexStore.BucketChange(
                                done.published, addedFiles.get(tb), Collections.emptyList()));
                out.deleted.addAll(done.deleted);
                out.keys += part.keys();
                out.deletes += part.deletes();
                out.found += done.found;
            }
        } finally {
            publishers.shutdownNow();
            lookups.shutdownNow();
        }
        return out;
    }

    /**
     * Looks up the base positions of a bucket's keys as publishing streams them, in chunks, and
     * collects the found ones per data file.
     */
    private static final class BucketLookups {
        private final TableBucket bucket;
        private final long base;
        private final boolean lookUp;
        private final PositionLookup lookup;
        private final ExecutorService executor;
        private final Semaphore inFlight;
        private final List<Future<?>> pending = new ArrayList<>();
        private List<byte[]> chunk = new ArrayList<>();
        // written by lookups, read once they are done
        private final DeletedPositions deleted = new DeletedPositions();
        private long found;
        private PositionIndexStore.BucketPart published;

        BucketLookups(
                TableBucket bucket,
                long base,
                boolean lookUp,
                PositionLookup lookup,
                ExecutorService executor,
                Semaphore inFlight) {
            this.bucket = bucket;
            this.base = base;
            this.lookUp = lookUp;
            this.lookup = lookup;
            this.executor = executor;
            this.inFlight = inFlight;
        }

        void accept(byte[] key) throws IOException {
            if (!lookUp) {
                return; // a new bucket has nothing in the base
            }
            chunk.add(key);
            if (chunk.size() == LOOKUP_CHUNK) {
                submit();
            }
        }

        private void submit() throws IOException {
            checkDone(false);
            List<byte[]> keys = chunk;
            chunk = new ArrayList<>();
            try {
                inFlight.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while looking up positions.", e);
            }
            try {
                pending.add(
                        executor.submit(
                                () -> {
                                    try {
                                        GetDvSnapshotResponse response =
                                                lookup.lookUp(bucket, base, keys);
                                        synchronized (deleted) {
                                            for (PbDvKeyPosition kp :
                                                    response.getKeyPositionsList()) {
                                                deleted.add(kp.getFilePath(), kp.getPos());
                                                found++;
                                            }
                                        }
                                    } finally {
                                        inFlight.release();
                                    }
                                    return null;
                                }));
            } catch (RuntimeException e) {
                inFlight.release();
                throw e;
            }
        }

        /** Waits for the bucket's lookups, after its last key. */
        void finish() throws IOException {
            if (!chunk.isEmpty()) {
                submit();
            }
            checkDone(true);
        }

        /** Fails on a failed lookup; with {@code all}, waits for all of them. */
        private void checkDone(boolean all) throws IOException {
            for (Iterator<Future<?>> it = pending.iterator(); it.hasNext(); ) {
                Future<?> f = it.next();
                if (!all && !f.isDone()) {
                    continue;
                }
                try {
                    f.get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while looking up positions.", e);
                } catch (ExecutionException e) {
                    throw new IOException(
                            "Looking up the positions of bucket " + bucket + " failed.",
                            e.getCause());
                }
                // a done lookup's writes are visible once its future returned
                it.remove();
            }
        }
    }

    private static final Comparator<TableBucket> BUCKET_ORDER =
            Comparator.comparingLong(
                            (TableBucket tb) ->
                                    tb.getPartitionId() == null ? -1L : tb.getPartitionId())
                    .thenComparingInt(TableBucket::getBucket);

    /**
     * Publishes the index change of this round's own compaction from the positions its writers
     * handed over ({@link CompactionIndexPart}), so the round needs no re-index. The change is
     * registered as the step from the base to the rewrite, and the rewrite in Fluss. Returns false
     * if a compacted bucket handed nothing over, the compaction read a snapshot other than the
     * base, or publishing fails; the rewrite is then re-indexed like another engine's.
     *
     * <p>The rewrite commits only on the snapshot its compaction read (see {@link
     * LakeCommitter#commitPendingRewrite}), so a change from the base covers it exactly.
     */
    private boolean publishCompactionIndex(
            long tableId,
            TablePath tablePath,
            LakeSnapshot base,
            @Nullable Long rewriteSnapshotId,
            List<TableBucketWriteResult<WriteResult>> results) {
        Map<TableBucket, CompactionIndexPart> parts = new HashMap<>();
        boolean complete = true;
        for (TableBucketWriteResult<WriteResult> r : results) {
            Object wr = r.writeResult();
            if (wr instanceof CompactionIndexPartProvider
                    && ((CompactionIndexPartProvider) wr).compacted()) {
                CompactionIndexPart part = ((CompactionIndexPartProvider) wr).compactionIndexPart();
                if (part == null) {
                    complete = false;
                } else {
                    parts.put(r.tableBucket(), part);
                }
            }
        }
        if (parts.isEmpty()) {
            return false;
        }
        PositionIndexStore store = indexStore(tableId, tablePath);
        Set<Long> changeIds = new HashSet<>();
        Set<Long> readSnapshots = new HashSet<>();
        for (CompactionIndexPart part : parts.values()) {
            changeIds.add(part.changeId());
            readSnapshots.add(part.readSnapshotId());
        }
        String reason =
                rewriteSnapshotId == null
                        ? "the rewrite was not committed"
                        : !complete
                                ? "a compacted bucket handed no positions over"
                                : changeIds.size() > 1 || readSnapshots.size() > 1
                                        ? "the compactions read different snapshots"
                                        : readSnapshots.iterator().next() != base.getSnapshotId()
                                                ? "the compactions read snapshot "
                                                        + readSnapshots.iterator().next()
                                                        + ", not the base"
                                                : null;
        if (reason == null) {
            long changeId = changeIds.iterator().next();
            try {
                long t0 = System.currentTimeMillis();
                Map<TableBucket, PositionIndexStore.BucketChange> change =
                        assignFileIds(store, changeId, parts);
                store.commitChange(changeId, change);
                store.register(base.getSnapshotId(), rewriteSnapshotId, changeId);
                registerSnapshot(tableId, tablePath, rewriteSnapshotId, base);
                compactionHandOffs.inc();
                LOG.info(
                        "Derived DV: Fluss compaction of {} committed as snapshot {}; {} buckets"
                                + " handed their positions to index change {} (no re-index),"
                                + " published in {} ms.",
                        tablePath,
                        rewriteSnapshotId,
                        parts.size(),
                        changeId,
                        System.currentTimeMillis() - t0);
                return true;
            } catch (Exception e) {
                reason = "publishing failed: " + e;
            }
        }
        LOG.info(
                "Derived DV: the compaction of {} is re-indexed from the lake: {}.",
                tablePath,
                reason);
        for (long changeId : changeIds) {
            try {
                store.abortChange(changeId);
            } catch (Exception e) {
                LOG.debug("Derived DV: could not delete index change {}.", changeId, e);
            }
        }
        return false;
    }

    /**
     * Gives the files of the compactions' parts their ids from the table's counter. Compactions of
     * different buckets run concurrently and may outlive their round, so their writers number the
     * files of their part from 0. This committer is the single allocator of a table's ids. The
     * parts' SSTs are rewritten with the ids, in parallel.
     */
    private static Map<TableBucket, PositionIndexStore.BucketChange> assignFileIds(
            PositionIndexStore store, long changeId, Map<TableBucket, CompactionIndexPart> parts)
            throws Exception {
        int files = 0;
        for (CompactionIndexPart part : parts.values()) {
            files += part.addedFiles().size();
        }
        int nextId = files == 0 ? 0 : store.allocateFileIds(files);
        Map<TableBucket, Map<Integer, Integer>> newIds = new HashMap<>();
        Map<TableBucket, Map<Integer, String>> addedFiles = new HashMap<>();
        for (Map.Entry<TableBucket, CompactionIndexPart> e : parts.entrySet()) {
            Map<Integer, Integer> ids = new HashMap<>();
            Map<Integer, String> added = new HashMap<>();
            for (Map.Entry<Integer, String> f :
                    new TreeMap<>(e.getValue().addedFiles()).entrySet()) {
                ids.put(f.getKey(), nextId);
                added.put(nextId, f.getValue());
                nextId++;
            }
            newIds.put(e.getKey(), ids);
            addedFiles.put(e.getKey(), added);
        }
        Map<TableBucket, PositionIndexStore.BucketChange> change = new HashMap<>();
        ExecutorService pool = Executors.newFixedThreadPool(Math.min(8, parts.size()));
        try {
            Map<TableBucket, Future<PositionIndexStore.BucketPart>> remapped = new HashMap<>();
            for (Map.Entry<TableBucket, CompactionIndexPart> e : parts.entrySet()) {
                CompactionIndexPart part = e.getValue();
                List<RowPosSstIndex.SstFileEntry> ssts = new ArrayList<>();
                for (int i = 0; i < part.sstFileNames().size(); i++) {
                    ssts.add(
                            new RowPosSstIndex.SstFileEntry(
                                    part.sstFileNames().get(i), part.sstFileSizes().get(i)));
                }
                PositionIndexStore.BucketPart stored =
                        ssts.isEmpty()
                                ? PositionIndexStore.BucketPart.NONE
                                : new PositionIndexStore.BucketPart(ssts);
                TableBucket tb = e.getKey();
                remapped.put(
                        tb,
                        pool.submit(
                                () -> store.remapFileIds(changeId, tb, stored, newIds.get(tb))));
            }
            for (Map.Entry<TableBucket, CompactionIndexPart> e : parts.entrySet()) {
                change.put(
                        e.getKey(),
                        new PositionIndexStore.BucketChange(
                                remapped.get(e.getKey()).get(),
                                addedFiles.get(e.getKey()),
                                new ArrayList<>(e.getValue().removedFiles())));
            }
        } finally {
            pool.shutdownNow();
        }
        return change;
    }

    private void maybeTrimIndexChain(
            long tableId,
            TablePath tablePath,
            PositionIndexStore store,
            Map<TableBucket, Long> readableOffsets) {
        int roundsOfTable = roundsSinceTrim.merge(tableId, 1, Integer::sum);
        if (roundsOfTable
                < flussConfig.get(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_TRIM_INTERVAL)) {
            return;
        }
        roundsSinceTrim.put(tableId, 0);
        try {
            List<TableBucket> withData = new ArrayList<>();
            for (Map.Entry<TableBucket, Long> e : readableOffsets.entrySet()) {
                if (e.getValue() != null && e.getValue() > 0) {
                    withData.add(e.getKey());
                }
            }
            long t0 = System.currentTimeMillis();
            int trimmed =
                    store.trim(
                            withData,
                            flussConfig.get(
                                    ConfigOptions.LAKE_TIERING_DELETION_VECTORS_TRIM_RETAIN));
            // Only failed attempts' changes are orphans here. This round's change is registered,
            // and repairSnapshot registers a crashed round's change before any later commit.
            int orphans =
                    store.deleteOrphanChanges(
                            flussConfig
                                    .get(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_ORPHAN_MIN_AGE)
                                    .toMillis());
            chainEntriesTrimmed.inc(trimmed);
            orphanChangesRemoved.inc(orphans);
            if (orphans > 0) {
                LOG.info("Derived DV: removed {} orphan index changes of {}.", orphans, tablePath);
            }
            LOG.info(
                    "Derived DV: index chain of {} trimmed by {} entries in {} ms ({} buckets with data).",
                    tablePath,
                    trimmed,
                    System.currentTimeMillis() - t0,
                    withData.size());
        } catch (Exception e) {
            LOG.warn("Derived DV: trimming the index chain of {} failed.", tablePath, e);
        }
    }

    /**
     * Returns the offsets without those of partitions that were dropped from the table: no tablet
     * server has their buckets any more, so a snapshot must not carry them on.
     */
    private Map<TableBucket, Long> withoutDroppedPartitions(
            TablePath tablePath, Map<TableBucket, Long> offsets) throws Exception {
        if (offsets.keySet().stream().allMatch(tb -> tb.getPartitionId() == null)) {
            return offsets;
        }
        Set<Long> partitionIds = new HashSet<>();
        // Includes system partitions such as the historical partition, which are tiered too.
        for (PartitionInfo partition : admin.listPartitionInfos(tablePath, true).get()) {
            partitionIds.add(partition.getPartitionId());
        }
        Map<TableBucket, Long> kept = new HashMap<>();
        Set<Long> dropped = new HashSet<>();
        for (Map.Entry<TableBucket, Long> e : offsets.entrySet()) {
            Long partitionId = e.getKey().getPartitionId();
            if (partitionId == null || partitionIds.contains(partitionId)) {
                kept.put(e.getKey(), e.getValue());
            } else {
                dropped.add(partitionId);
            }
        }
        if (!dropped.isEmpty()) {
            LOG.info(
                    "Derived DV: dropped partitions {} of {} leave the lake snapshot's offsets.",
                    dropped,
                    tablePath);
        }
        return kept;
    }

    private static int chainIndexOf(List<PositionIndexStore.ChainEntry> chain, long snapshotId) {
        for (int i = 0; i < chain.size(); i++) {
            if (chain.get(i).snapshotId == snapshotId) {
                return i;
            }
        }
        return -1;
    }

    /** Returns the table's index store; one per table, since it owns the file-id counter. */
    private PositionIndexStore indexStore(long tableId, TablePath tablePath) {
        return indexStores.computeIfAbsent(
                tableId,
                id ->
                        PositionIndexStore.forTable(
                                FlussConfigUtils.getDefaultRemoteDataDir(flussConfig),
                                tablePath,
                                tableId,
                                localTmpDir));
    }

    /** Encodes "partitionId|bucket=offset,...", with an empty partitionId if unpartitioned. */
    static String encodeOffsets(Map<TableBucket, Long> offsets) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<TableBucket, Long> e : offsets.entrySet()) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            Long pid = e.getKey().getPartitionId();
            sb.append(pid == null ? "" : pid)
                    .append('|')
                    .append(e.getKey().getBucket())
                    .append('=')
                    .append(e.getValue());
        }
        return sb.toString();
    }

    static Map<TableBucket, Long> decodeOffsets(long tableId, String encoded) {
        Map<TableBucket, Long> out = new HashMap<>();
        if (encoded == null || encoded.isEmpty()) {
            return out;
        }
        for (String part : encoded.split(",")) {
            String[] kv = part.split("=");
            String[] pb = kv[0].split("\\|", -1);
            Long pid = pb[0].isEmpty() ? null : Long.parseLong(pb[0]);
            out.put(new TableBucket(tableId, pid, Integer.parseInt(pb[1])), Long.parseLong(kv[1]));
        }
        return out;
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
        int buckets = decodeOffsets(tableId, props.get(DERIVED_OFFSETS_PROPERTY)).size();
        // Without a recorded base, the parent is the chain's end, which was the base at commit.
        PositionIndexStore store = indexStore(tableId, tablePath);
        String baseProp = props.get(DERIVED_BASE_PROPERTY);
        long parent;
        if (baseProp != null) {
            parent = Long.parseLong(baseProp);
        } else {
            List<PositionIndexStore.ChainEntry> chain = store.chain();
            parent = chain.isEmpty() ? -1L : chain.get(chain.size() - 1).snapshotId;
        }
        store.register(parent, missing.getLakeSnapshotId(), indexId);
        flussTableLakeSnapshotCommitter.commit(
                tableId,
                tablePath,
                readableAtTieredOffsets(
                        missing.getLakeSnapshotId(),
                        decodeOffsets(tableId, props.get(DERIVED_OFFSETS_PROPERTY)),
                        null,
                        earliestSnapshotToKeep(tableId, tablePath)),
                offsetsPath,
                Collections.emptyMap(),
                Collections.emptyMap());
        LOG.warn(
                "Derived DV: repaired unregistered lake snapshot {} of {} (index {}, {} buckets).",
                missing.getLakeSnapshotId(),
                tablePath,
                indexId,
                buckets);
    }

    /**
     * Publishes and registers the re-index of a rewritten snapshot, then registers the snapshot in
     * Fluss so tablet servers catch up to it.
     */
    private void publishReindex(
            long tableId,
            TablePath tablePath,
            long baseSnapshotId,
            long compactSnapshotId,
            long changeId,
            List<RowPosResult> results,
            @Nullable Map<String, Map<Integer, List<String>>> replacedFiles,
            long startedAtMs,
            boolean registerInFluss)
            throws Exception {
        long t0 = System.currentTimeMillis();
        // Per bucket: the SSTs and new file ids of its splits, and the files that left the
        // snapshot.
        Map<TableBucket, List<RowPosSstIndex.SstFileEntry>> ssts = new HashMap<>();
        Map<TableBucket, Map<Integer, String>> fileIds = new HashMap<>();
        for (RowPosResult r : results) {
            TableBucket tb = new TableBucket(tableId, r.getPartitionId(), r.getBucketId());
            List<RowPosSstIndex.SstFileEntry> entries =
                    ssts.computeIfAbsent(tb, k -> new ArrayList<>());
            for (RowPosResult.SstMeta m : r.getSstMetas()) {
                entries.add(new RowPosSstIndex.SstFileEntry(m.getFileName(), m.getFileSize()));
            }
            fileIds.computeIfAbsent(tb, k -> new HashMap<>()).putAll(r.getNewFileDictEntries());
        }
        Map<TableBucket, PositionIndexStore.BucketChange> published = new HashMap<>();
        for (Map.Entry<TableBucket, List<RowPosSstIndex.SstFileEntry>> e : ssts.entrySet()) {
            published.put(
                    e.getKey(),
                    new PositionIndexStore.BucketChange(
                            new PositionIndexStore.BucketPart(e.getValue()),
                            fileIds.get(e.getKey()),
                            new ArrayList<>()));
        }
        int replaced = 0;
        if (replacedFiles != null && !replacedFiles.isEmpty()) {
            Map<String, Long> partitionIds = new HashMap<>();
            if (admin.getTableInfo(tablePath).get().isPartitioned()) {
                for (PartitionInfo pi : admin.listPartitionInfos(tablePath, true).get()) {
                    partitionIds.put(pi.getPartitionName(), pi.getPartitionId());
                }
            }
            for (Map.Entry<String, Map<Integer, List<String>>> pe : replacedFiles.entrySet()) {
                Long pid = pe.getKey() == null ? null : partitionIds.get(pe.getKey());
                if (pe.getKey() != null && pid == null) {
                    continue; // the partition is gone, so nothing references these files
                }
                for (Map.Entry<Integer, List<String>> be : pe.getValue().entrySet()) {
                    published
                            .computeIfAbsent(
                                    new TableBucket(tableId, pid, be.getKey()),
                                    k ->
                                            new PositionIndexStore.BucketChange(
                                                    PositionIndexStore.BucketPart.NONE,
                                                    Collections.emptyMap(),
                                                    new ArrayList<>()))
                            .removedFiles
                            .addAll(be.getValue());
                    replaced += be.getValue().size();
                }
            }
        }
        PositionIndexStore store = indexStore(tableId, tablePath);
        store.commitChange(changeId, published);
        store.register(
                baseSnapshotId,
                compactSnapshotId,
                changeId,
                snapshotIdentity(tablePath, compactSnapshotId));
        if (registerInFluss) {
            registerSnapshot(
                    tableId, tablePath, compactSnapshotId, getLatestLakeSnapshot(tablePath));
        }
        reindexes.inc();
        lastReindexMs = System.currentTimeMillis() - startedAtMs;
        reindexMs.inc(lastReindexMs);
        LOG.info(
                "Derived DV: rewritten snapshot {} of {} re-indexed by {} parallel scans, {} files"
                        + " dropped, in {} ms since planned; published in {} ms.",
                compactSnapshotId,
                tablePath,
                results.size(),
                replaced,
                lastReindexMs,
                System.currentTimeMillis() - t0);
    }

    /**
     * Returns the identity of the lake commit that is snapshot {@code snapshotId} for a lake that
     * reuses snapshot ids after a rollback (Paimon), or null for other lakes. Union reads use it to
     * tell a reused id from the snapshot the chain covers.
     */
    @Nullable
    private String snapshotIdentity(TablePath tablePath, long snapshotId) throws Exception {
        if (!lakeTieringFactory.mergesByPrimaryKey()) {
            return null;
        }
        LakeSource<?> lakeSource =
                lakeTieringFactory.createLakeSource(
                        admin.getTableInfo(tablePath).get().getLakeTablePath());
        return lakeSource == null ? null : lakeSource.snapshotIdentity(snapshotId);
    }

    /**
     * Registers a rewritten snapshot in Fluss, readable at the base's tiered offsets. It is
     * protected from expiration first, like the snapshots rounds commit: union reads plan on it,
     * and the next round looks it up as its base.
     */
    private void registerSnapshot(
            long tableId, TablePath tablePath, long snapshotId, LakeSnapshot baseSnapshot)
            throws Exception {
        protectReadableSnapshot(tablePath, snapshotId);
        Map<TableBucket, Long> offsets =
                withoutDroppedPartitions(tablePath, baseSnapshot.getTableBucketsOffset());
        String offsetsFile =
                flussTableLakeSnapshotCommitter.prepareCompleteLakeSnapshot(
                        tableId, tablePath, offsets);
        flussTableLakeSnapshotCommitter.commit(
                tableId,
                tablePath,
                readableAtTieredOffsets(
                        snapshotId, offsets, null, earliestSnapshotToKeep(tableId, tablePath)),
                offsetsFile,
                Collections.emptyMap(),
                Collections.emptyMap());
    }

    /** Best effort: a snapshot left unprotected is re-indexed once it expires. */
    private void protectReadableSnapshot(TablePath tablePath, long snapshotId) {
        try {
            TableInfo tableInfo = admin.getTableInfo(tablePath).get();
            try (LakeCommitter<WriteResult, Committable> lakeCommitter =
                    lakeTieringFactory.createLakeCommitter(
                            new TieringCommitterInitContext(
                                    tablePath, tableInfo, lakeTieringConfig, flussConfig))) {
                lakeCommitter.protectReadableSnapshot(snapshotId);
            }
        } catch (Exception e) {
            LOG.warn(
                    "Derived DV: could not protect readable snapshot {} of {} from expiration.",
                    snapshotId,
                    tablePath,
                    e);
        }
    }

    /**
     * The result of a commit of a lake that takes deletes: the deletes are in the same commit, so
     * it is readable at its tiered offsets. Fluss keeps the registered snapshots from {@code
     * earliestToKeep} on (see {@link #earliestSnapshotToKeep}).
     */
    private static LakeCommitResult readableAtTieredOffsets(
            long snapshotId,
            Map<TableBucket, Long> offsets,
            @Nullable TieringStats stats,
            @Nullable Long earliestToKeep) {
        return LakeCommitResult.withReadableSnapshot(
                snapshotId, snapshotId, offsets, offsets, earliestToKeep, stats);
    }

    /**
     * Returns the earliest registered lake snapshot Fluss must keep, with its offsets files: the
     * snapshot of the chain's oldest entry. Tablet servers catch up only to snapshots in the chain,
     * and a server rebuilding a bucket without a checkpoint of a trimmed chain reads the bucket's
     * offset in the chain's oldest snapshot. Nothing reads the registrations before it. Trimming
     * the chain thus moves the boundary on: Fluss discards the older registrations and their
     * offsets files, so the table's lake snapshot list stays as long as the chain.
     *
     * <p>Fluss keeps everything if the boundary is not registered, so this takes the first chain
     * snapshot Fluss has registered. Returns null (keep the new snapshot only) if there is none,
     * e.g. on the first round. Registrations before the boundary never come back, so a boundary
     * that is still in the chain is not checked again.
     */
    @Nullable
    private Long earliestSnapshotToKeep(long tableId, TablePath tablePath) throws Exception {
        List<PositionIndexStore.ChainEntry> chain = indexStore(tableId, tablePath).chain();
        Long known = retainedFrom.get(tableId);
        if (known != null && chainIndexOf(chain, known) >= 0) {
            return known;
        }
        for (PositionIndexStore.ChainEntry e : chain) {
            if (isRegistered(tablePath, e.snapshotId)) {
                retainedFrom.put(tableId, e.snapshotId);
                return e.snapshotId;
            }
        }
        return LakeCommitResult.KEEP_LATEST;
    }

    /** Whether Fluss has the lake snapshot registered. */
    private boolean isRegistered(TablePath tablePath, long snapshotId) throws Exception {
        try {
            admin.getLakeSnapshot(tablePath, snapshotId).get();
            return true;
        } catch (Exception e) {
            if (ExceptionUtils.findThrowable(e, LakeTableSnapshotNotExistException.class)
                    .isPresent()) {
                return false;
            }
            throw e;
        }
    }

    /**
     * Returns the result of a merging round's lake commit, keeping the registered snapshots the
     * chain needs as well as those the lake committer asked to keep. Lakes that merge by key number
     * their snapshots in commit order, so the smaller id is the earlier one.
     */
    private LakeCommitResult keepingIndexedSnapshots(
            long tableId, TablePath tablePath, LakeCommitResult result) throws Exception {
        LakeCommitResult.ReadableSnapshot readable = result.getReadableSnapshot();
        Long lakeKeeps = result.getEarliestSnapshotIDToKeep();
        if (readable == null
                || result.committedIsReadable()
                || LakeCommitResult.KEEP_ALL_PREVIOUS.equals(lakeKeeps)) {
            return result;
        }
        Long indexKeeps = earliestSnapshotToKeep(tableId, tablePath);
        // Both are null on the first readable snapshot of a table; a conditional expression of
        // Long and long would unbox them.
        Long keep;
        if (indexKeeps == null) {
            keep = lakeKeeps;
        } else if (lakeKeeps == null) {
            keep = indexKeeps;
        } else {
            keep = Math.min(lakeKeeps, indexKeeps);
        }
        if (Objects.equals(keep, lakeKeeps)) {
            return result;
        }
        return LakeCommitResult.withReadableSnapshot(
                result.getCommittedSnapshotId(),
                readable.getReadableSnapshotId(),
                readable.getTieredLogEndOffsets(),
                readable.getReadableLogEndOffsets(),
                keep,
                result.getTieringStats());
    }

    /**
     * Plans the re-index of files rewritten since the base and returns if nothing was rewritten.
     * Otherwise throws {@link ReindexPending}: with the {@link Reindex} the round waits for, or
     * without one if the re-index is already published.
     */
    @SuppressWarnings("unchecked")
    private void triggerReindex(
            long tableId, TablePath tablePath, TableInfo tableInfo, LakeSnapshot base)
            throws Exception {
        // the lake table may live under a custom lake table path
        LakeSource<?> lakeSource =
                lakeTieringFactory.createLakeSource(tableInfo.getLakeTablePath());
        if (lakeSource == null || base == null) {
            return;
        }
        triggerReindex(tableId, tablePath, (LakeSource<LakeSplit>) lakeSource, base);
    }

    private <Split extends LakeSplit> void triggerReindex(
            long tableId, TablePath tablePath, LakeSource<Split> lakeSource, LakeSnapshot base)
            throws Exception {
        DataDeltaPlan<Split> plan = lakeSource.planRewrite(base.getSnapshotId(), null);
        if (plan == null) {
            return;
        }
        long compactSnapshotId = plan.getCompactSnapshotId();
        PositionIndexStore store = indexStore(tableId, tablePath);
        List<PositionIndexStore.ChainEntry> chain = store.chain();
        int compactIndex = chainIndexOf(chain, compactSnapshotId);
        int baseIndex = chainIndexOf(chain, base.getSnapshotId());
        if (compactIndex < 0 && baseIndex >= 0 && baseIndex < chain.size() - 1) {
            // A rewrite reached the chain but not Fluss (its service lost the coordinator), then
            // the lake moved on: the chain's end is registered first. Fails on a rollback past it.
            lakeSource.planRewrite(chain.get(chain.size() - 1).snapshotId, compactSnapshotId);
            compactIndex = chain.size() - 1;
            compactSnapshotId = chain.get(compactIndex).snapshotId;
        }
        if (compactIndex >= 0) {
            if (baseIndex >= 0 && compactIndex <= baseIndex) {
                // The lake's current snapshot is the base or older: a rollback, not a rewrite.
                // Registering it with the base's offsets would lose the rows in between.
                throw LakeTableRolledBackException.of(
                        tablePath.toString(), compactSnapshotId, base.getSnapshotId());
            }
            // The change is already in the chain; only the Fluss registration is missing.
            registerSnapshot(tableId, tablePath, compactSnapshotId, base);
            throw new ReindexPending(null);
        }
        Map<String, Long> partitionIds = partitionIds(tablePath, plan);
        plan = withoutFilesOfDroppedPartitions(tablePath, plan, partitionIds);
        // A new change id per attempt, so two attempts never share files.
        long changeId = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        if (plan.getSplits().isEmpty()) {
            // Files were only removed: publish at once and run the round again. Its lookups must
            // not see the removed files.
            publishReindex(
                    tableId,
                    tablePath,
                    base.getSnapshotId(),
                    compactSnapshotId,
                    changeId,
                    Collections.emptyList(),
                    plan.getDeletedFiles(),
                    System.currentTimeMillis(),
                    true);
            throw new ReindexPending(null);
        }
        int firstFileId = store.allocateFileIds(plan.getSplits().size());
        FsPath lakeDir =
                FlussPaths.remoteLakeTableSnapshotDir(
                        FlussConfigUtils.getDefaultRemoteDataDir(flussConfig), tablePath, tableId);
        Reindex reindex =
                planReindexScan(
                        tableId,
                        tablePath,
                        lakeSource,
                        plan,
                        partitionIds,
                        firstFileId,
                        lakeDir,
                        base.getSnapshotId(),
                        changeId,
                        null);
        LOG.info(
                "Derived DV: {} files of {} rewritten since snapshot {}; re-indexing snapshot {} on the tiering readers.",
                plan.getSplits().size(),
                tablePath,
                base.getSnapshotId(),
                compactSnapshotId);
        throw new ReindexPending(reindex);
    }

    /**
     * Builds the requests that have the tiering readers re-index the plan's files. A split holds at
     * most {@code lake.tiering.deletion-vectors.reindex-files-per-split} files of one bucket and,
     * as far as the files' metadata tells, at most {@code
     * lake.tiering.deletion-vectors.reindex-rows-per-split} rows; a larger file is a split of its
     * own. The lake splits are stored with the change, so a request carries only references and
     * holds a bounded number of splits. The results come back through the operator, one per split.
     */
    @VisibleForTesting
    <Split extends LakeSplit> Reindex planReindexScan(
            long tableId,
            TablePath tablePath,
            LakeSource<Split> lakeSource,
            DataDeltaPlan<Split> plan,
            Map<String, Long> partitionIds,
            int firstFileId,
            FsPath lakeDir,
            long baseSnapshotId,
            long changeId,
            @Nullable Registration<Committable> registration)
            throws IOException {
        SimpleVersionedSerializer<Split> splitSerializer = lakeSource.getSplitSerializer();
        int filesPerSplit =
                flussConfig.get(
                        ConfigOptions.LAKE_TIERING_DELETION_VECTORS_REINDEX_FILES_PER_SPLIT);
        checkState(
                filesPerSplit > 0,
                "%s must be positive.",
                ConfigOptions.LAKE_TIERING_DELETION_VECTORS_REINDEX_FILES_PER_SPLIT.key());
        long rowsPerSplit =
                flussConfig.get(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_REINDEX_ROWS_PER_SPLIT);
        checkState(
                rowsPerSplit > 0,
                "%s must be positive.",
                ConfigOptions.LAKE_TIERING_DELETION_VECTORS_REINDEX_ROWS_PER_SPLIT.key());
        Map<String, Map<Integer, List<Split>>> filesByPartitionAndBucket = new LinkedHashMap<>();
        for (Split split : plan.getSplits()) {
            filesByPartitionAndBucket
                    .computeIfAbsent(partitionName(split), k -> new LinkedHashMap<>())
                    .computeIfAbsent(split.bucket(), k -> new ArrayList<>())
                    .add(split);
        }
        PositionIndexStore store = indexStore(tableId, tablePath);
        List<RowPosRequestEvent.Split> splits = new ArrayList<>();
        int nextFileId = firstFileId;
        for (Map.Entry<String, Map<Integer, List<Split>>> pe :
                filesByPartitionAndBucket.entrySet()) {
            Long partitionId = null;
            if (pe.getKey() != null) {
                partitionId = partitionIds.get(pe.getKey());
                checkState(
                        partitionId != null,
                        "Planned the re-index of files of partition %s, which %s does not have.",
                        pe.getKey(),
                        tablePath);
            }
            for (Map.Entry<Integer, List<Split>> be : pe.getValue().entrySet()) {
                for (List<Split> part : bySize(be.getValue(), filesPerSplit, rowsPerSplit)) {
                    int[] fileIds = new int[part.size()];
                    List<byte[]> serialized = new ArrayList<>(part.size());
                    for (int i = 0; i < part.size(); i++) {
                        fileIds[i] = nextFileId++;
                        serialized.add(splitSerializer.serialize(part.get(i)));
                    }
                    int index = splits.size();
                    String reference =
                            store.writeReindexPlan(
                                    changeId, index, splitSerializer.getVersion(), serialized);
                    splits.add(
                            new RowPosRequestEvent.Split(
                                    pe.getKey(),
                                    partitionId,
                                    be.getKey(),
                                    index,
                                    fileIds,
                                    reference));
                }
            }
        }
        List<RowPosRequestEvent> requests = new ArrayList<>();
        for (int from = 0; from < splits.size(); from += SPLITS_PER_REQUEST) {
            requests.add(
                    new RowPosRequestEvent(
                            tableId,
                            tablePath,
                            changeId,
                            lakeDir.toString(),
                            new ArrayList<>(
                                    splits.subList(
                                            from,
                                            Math.min(splits.size(), from + SPLITS_PER_REQUEST))),
                            splits.size()));
        }
        return new Reindex(
                tablePath,
                baseSnapshotId,
                plan.getCompactSnapshotId(),
                changeId,
                plan.getDeletedFiles(),
                requests,
                splits.size(),
                System.currentTimeMillis(),
                registration);
    }

    /**
     * The ids of the partitions of the Fluss table by name, if the plan has files of a partitioned
     * table; empty otherwise.
     */
    private Map<String, Long> partitionIds(TablePath tablePath, DataDeltaPlan<?> plan)
            throws Exception {
        if (plan.getSplits().stream().allMatch(split -> partitionName(split) == null)) {
            return Collections.emptyMap();
        }
        Map<String, Long> ids = new HashMap<>();
        // Includes system partitions such as the historical partition, which are tiered too.
        for (PartitionInfo partition : admin.listPartitionInfos(tablePath, true).get()) {
            ids.put(partition.getPartitionName(), partition.getPartitionId());
        }
        return ids;
    }

    /**
     * Returns the plan without the files of partitions the Fluss table does not have. A partition
     * dropped in Fluss stays in the lake, where a compaction may still make its files readable. No
     * tablet server serves its buckets, so nothing looks up their positions.
     */
    @VisibleForTesting
    static <Split extends LakeSplit> DataDeltaPlan<Split> withoutFilesOfDroppedPartitions(
            TablePath tablePath, DataDeltaPlan<Split> plan, Map<String, Long> partitionIds) {
        List<Split> kept = new ArrayList<>();
        Set<String> dropped = new TreeSet<>();
        for (Split split : plan.getSplits()) {
            String partitionName = partitionName(split);
            if (partitionName == null || partitionIds.containsKey(partitionName)) {
                kept.add(split);
            } else {
                dropped.add(partitionName);
            }
        }
        if (dropped.isEmpty()) {
            return plan;
        }
        LOG.info(
                "Derived DV: {} newly readable files of {} in dropped partitions {} are not"
                        + " indexed.",
                plan.getSplits().size() - kept.size(),
                tablePath,
                dropped);
        return new DataDeltaPlan<>(plan.getCompactSnapshotId(), kept, plan.getDeletedFiles());
    }

    /** The name of the split's partition, or null if the table is not partitioned. */
    @Nullable
    private static String partitionName(LakeSplit split) {
        List<String> values = split.partition();
        return values == null || values.isEmpty()
                ? null
                : String.join(ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR, values);
    }

    /**
     * Groups files, in order, into parts of at most {@code maxFiles} files and {@code maxRows} rows
     * (a file of unknown size counts no rows); a file of more rows is a part of its own.
     */
    private static <Split extends LakeSplit> List<List<Split>> bySize(
            List<Split> files, int maxFiles, long maxRows) {
        List<List<Split>> parts = new ArrayList<>();
        List<Split> part = new ArrayList<>();
        long rows = 0;
        for (Split file : files) {
            long fileRows = Math.max(0, file.rowCount());
            if (!part.isEmpty() && (part.size() >= maxFiles || rows + fileRows > maxRows)) {
                parts.add(part);
                part = new ArrayList<>();
                rows = 0;
            }
            part.add(file);
            rows += fileRows;
        }
        if (!part.isEmpty()) {
            parts.add(part);
        }
        return parts;
    }

    /**
     * Looks up the positions of the keys in the bucket's index of the snapshot, on the bucket's
     * leader. Retries while the leader catches up to the snapshot, and with backoff while the
     * leader moves or restarts. The admin then drops the cached leader, so the retry resolves the
     * leader and the servers again. Both retries are bounded; other errors fail at once.
     */
    @VisibleForTesting
    static GetDvSnapshotResponse lookupWithRetry(
            Admin admin,
            TablePath tablePath,
            TableBucket tb,
            long snapshotId,
            List<byte[]> keys,
            Sleeper sleeper)
            throws Exception {
        int staleRetries = 0;
        int leaderRetries = 0;
        long leaderBackoffMs = LEADER_RETRY_INITIAL_BACKOFF_MS;
        long notReadyWaitedMs = 0;
        long notReadyBackoffMs = NOT_READY_INITIAL_BACKOFF_MS;
        while (true) {
            try {
                return admin.lookupDvPositions(
                                tablePath,
                                tb.getTableId(),
                                tb.getPartitionId(),
                                tb.getBucket(),
                                snapshotId,
                                keys)
                        .get();
            } catch (Exception e) {
                // The bucket has not ingested the base snapshot's index yet.
                if (staleRetries < MAX_STALE_SNAPSHOT_RETRIES
                        && ExceptionUtils.stringifyException(e).contains("Stale snapshot")) {
                    staleRetries++;
                    sleeper.sleep(STALE_SNAPSHOT_BACKOFF_MS);
                    continue;
                }
                // The leader catches up to the snapshot or rebuilds in the background.
                if (ExceptionUtils.findThrowable(e, DeletionVectorsNotReadyException.class)
                        .isPresent()) {
                    if (notReadyWaitedMs >= MAX_NOT_READY_WAIT_MS) {
                        throw e;
                    }
                    LOG.info(
                            "Derived DV: positions of bucket {} of {} not ready ({}), retry in {}"
                                    + " ms.",
                            tb,
                            tablePath,
                            ExceptionUtils.stripExecutionException(e).getMessage(),
                            notReadyBackoffMs);
                    sleeper.sleep(notReadyBackoffMs);
                    notReadyWaitedMs += notReadyBackoffMs;
                    notReadyBackoffMs = Math.min(2 * notReadyBackoffMs, NOT_READY_MAX_BACKOFF_MS);
                    continue;
                }
                if (leaderRetries < MAX_LEADER_RETRIES && isLeaderUnavailable(e)) {
                    leaderRetries++;
                    LOG.info(
                            "Derived DV: leader of bucket {} of {} unavailable for a position"
                                    + " lookup ({}), retry {}/{} in {} ms.",
                            tb,
                            tablePath,
                            ExceptionUtils.stripExecutionException(e).getMessage(),
                            leaderRetries,
                            MAX_LEADER_RETRIES,
                            leaderBackoffMs);
                    sleeper.sleep(leaderBackoffMs);
                    leaderBackoffMs = Math.min(2 * leaderBackoffMs, LEADER_RETRY_MAX_BACKOFF_MS);
                    continue;
                }
                throw e;
            }
        }
    }

    /**
     * Whether the bucket's leader is not reachable for now: it moved, is being elected, or its
     * server restarted and is not in the client's metadata yet.
     */
    private static boolean isLeaderUnavailable(Throwable e) {
        if (ExceptionUtils.findThrowable(e, RetriableException.class).isPresent()) {
            // InvalidMetadataException (not leader, leader not available, server not in the
            // metadata cache, network error) and other retriable errors such as timeouts
            return true;
        }
        // MetadataUpdater#leaderFor when no leader is elected yet
        return ExceptionUtils.findThrowableWithMessage(e, "Leader not found").isPresent();
    }

    /** Looks up the positions of keys in a bucket's index of a snapshot; replaced in tests. */
    @FunctionalInterface
    interface PositionLookup {
        GetDvSnapshotResponse lookUp(TableBucket bucket, long snapshotId, List<byte[]> keys)
                throws Exception;
    }

    /** Sleeps between lookup retries; replaced in tests. */
    @FunctionalInterface
    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    /** Runs a round. */
    @FunctionalInterface
    private interface RoundCommit<Committable> {
        Outcome<Committable> run() throws Exception;
    }

    @Nullable
    private LakeSnapshot getLatestLakeSnapshot(TablePath tablePath) throws Exception {
        try {
            return admin.getLatestLakeSnapshot(tablePath).get();
        } catch (Exception e) {
            if (e.getCause() instanceof LakeTableSnapshotNotExistException) {
                return null;
            }
            throw e;
        }
    }

    /**
     * Registers a lake snapshot a previous attempt committed but did not register, and fails the
     * round so it runs again on top of it. The caller deletes the round's files. Used for lakes
     * that take deletes; a merging round registers a lost commit with {@link #registerLostCommit}.
     */
    private void checkFlussNotMissingLakeSnapshot(
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
        String lakeSnapshotOffsetPath =
                missingCommittedSnapshot
                        .getSnapshotProperties()
                        .get(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY);
        if (missingCommittedSnapshot
                .getSnapshotProperties()
                .containsKey(DERIVED_INDEX_ID_PROPERTY)) {
            // A snapshot the lake table was rolled back past is not repaired: registering it
            // would only make the next round find the rollback again.
            lakeCommitter.checkCurrentSnapshotDescendsFrom(
                    missingCommittedSnapshot.getLakeSnapshotId());
            repairSnapshot(tableId, tablePath, missingCommittedSnapshot, lakeSnapshotOffsetPath);
            throw new IllegalStateException(
                    "Derived DV: repaired a lake snapshot left unregistered by a previous attempt; "
                            + "this round is retried on top of it.");
        }
        // commit this missing snapshot to fluss
        flussTableLakeSnapshotCommitter.commit(
                tableId,
                missingCommittedSnapshot.getLakeSnapshotId(),
                lakeSnapshotOffsetPath,
                // don't care readable snapshot and offsets,
                null,
                // use empty log offsets, log max timestamp, since we can't know that
                // in last tiering, it doesn't matter for they are just used to
                // report metrics
                Collections.emptyMap(),
                Collections.emptyMap(),
                earliestSnapshotToKeep(tableId, tablePath));
        throw new IllegalStateException(
                String.format(
                        "The current Fluss's lake snapshot %d is less than"
                                + " lake actual snapshot %d committed by Fluss for table: {tablePath=%s, tableId=%d},"
                                + " missing snapshot: %s.",
                        flussCurrentLakeSnapshot,
                        missingCommittedSnapshot.getLakeSnapshotId(),
                        tablePath,
                        tableId,
                        missingCommittedSnapshot));
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
    private Reindex registerLostCommit(
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
        lakeCommitter.checkCurrentSnapshotDescendsFrom(lost.getLakeSnapshotId());
        String offsetsFile =
                lost.getSnapshotProperties().get(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY);
        LakeCommitResult result =
                lakeCommitter.getCommitResult(
                        lost.getLakeSnapshotId(), readOffsets(new FsPath(offsetsFile)));
        if (result == null) {
            result = LakeCommitResult.unknownReadableSnapshot(lost.getLakeSnapshotId(), null);
        }
        Registration<Committable> registration =
                new Registration<>(
                        result, offsetsFile, Collections.emptyMap(), Collections.emptyMap(), null);
        Reindex reindex =
                indexReadableSnapshot(
                        tableId, tablePath, tableInfo, registration, Collections.emptyList());
        if (reindex != null) {
            LOG.warn(
                    "Derived DV: lake snapshot {} of {} was committed by a round lost before it"
                            + " registered it; this round registers it once its readable"
                            + " snapshot {} is indexed, and runs again after.",
                    lost.getLakeSnapshotId(),
                    tablePath,
                    reindex.compactSnapshotId);
            return reindex;
        }
        register(tableId, tablePath, registration);
        throw lostCommitRegistered(tablePath, lost.getLakeSnapshotId());
    }

    private static IllegalStateException lostCommitRegistered(
            TablePath tablePath, long snapshotId) {
        return new IllegalStateException(
                String.format(
                        "Derived DV: registered lake snapshot %d of %s, which a round lost before"
                                + " registering it had committed; this round runs again on top"
                                + " of it.",
                        snapshotId, tablePath));
    }

    private static Map<TableBucket, Long> readOffsets(FsPath offsetsFile) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (FSDataInputStream in = offsetsFile.getFileSystem().open(offsetsFile)) {
            IOUtils.copyBytes(in, bytes, false);
        }
        return TableBucketOffsets.fromJsonBytes(bytes.toByteArray()).getOffsets();
    }

    @SuppressWarnings("unchecked")
    private void preparePartitionMarkDone(
            LakeCommitter<WriteResult, Committable> lakeCommitter, Committable committable)
            throws IOException {
        checkState(
                lakeCommitter instanceof PartitionMarkDoneCommitter,
                "Lake tiering factory %s declares partition mark-done support but its committer does not.",
                lakeTieringFactory.getClass().getName());
        ((PartitionMarkDoneCommitter<WriteResult, Committable>) lakeCommitter)
                .preparePartitionMarkDone(committable);
    }

    /**
     * Registers a merging round's lake commit in Fluss; its readable snapshot is in the index chain
     * by now.
     */
    private TieringCommitResult<Committable> register(
            long tableId, TablePath tablePath, Registration<Committable> registration)
            throws Exception {
        LakeCommitResult result = registration.result;
        if (result.committedIsReadable()) {
            protectReadableSnapshot(tablePath, result.getCommittedSnapshotId());
        } else if (result.getReadableSnapshot() != null) {
            protectReadableSnapshot(
                    tablePath, result.getReadableSnapshot().getReadableSnapshotId());
        }
        flussTableLakeSnapshotCommitter.commit(
                tableId,
                tablePath,
                keepingIndexedSnapshots(tableId, tablePath, result),
                registration.offsetsFile,
                registration.logEndOffsets,
                registration.logMaxTieredTimestamps);
        LakeCommitResult.ReadableSnapshot readable = result.getReadableSnapshot();
        if (readable != null) {
            maybeTrimIndexChain(
                    tableId,
                    tablePath,
                    indexStore(tableId, tablePath),
                    withoutDroppedPartitions(tablePath, readable.getReadableLogEndOffsets()));
        }
        LOG.info(
                "Derived DV merging round {}: lake snapshot {}, readable snapshot {}.",
                tablePath,
                result.getCommittedSnapshotId(),
                result.committedIsReadable()
                        ? String.valueOf(result.getCommittedSnapshotId())
                        : readable == null
                                ? "unchanged"
                                : String.valueOf(readable.getReadableSnapshotId()));
        return new TieringCommitResult<>(registration.committable, result.getTieringStats());
    }

    /**
     * A merging round committed to the lake, registered in Fluss once its readable snapshot is
     * indexed.
     */
    static final class Registration<Committable> {
        final LakeCommitResult result;
        final String offsetsFile;
        final Map<TableBucket, Long> logEndOffsets;
        final Map<TableBucket, Long> logMaxTieredTimestamps;
        // null for the lost commit of an earlier round
        @Nullable final Committable committable;

        private Registration(
                LakeCommitResult result,
                String offsetsFile,
                Map<TableBucket, Long> logEndOffsets,
                Map<TableBucket, Long> logMaxTieredTimestamps,
                @Nullable Committable committable) {
            this.result = result;
            this.offsetsFile = offsetsFile;
            this.logEndOffsets = logEndOffsets;
            this.logMaxTieredTimestamps = logMaxTieredTimestamps;
            this.committable = committable;
        }
    }

    /** The outcome of a round: committed, or held until a re-index is done. */
    static final class Outcome<Committable> {
        @Nullable private final TieringCommitResult<Committable> result;
        @Nullable private final Reindex reindex;

        private Outcome(
                @Nullable TieringCommitResult<Committable> result, @Nullable Reindex reindex) {
            this.result = result;
            this.reindex = reindex;
        }

        static <Committable> Outcome<Committable> committed(
                TieringCommitResult<Committable> result) {
            return new Outcome<>(result, null);
        }

        static <Committable> Outcome<Committable> held(Reindex reindex) {
            return new Outcome<>(null, reindex);
        }

        /** The committed round's result, or null if the round is held. */
        @Nullable
        TieringCommitResult<Committable> result() {
            return result;
        }

        /** The re-index a held round waits for, or null if the round is committed. */
        @Nullable
        Reindex reindex() {
            return reindex;
        }
    }

    /** A re-index of rewritten lake files that a held round waits for. */
    static final class Reindex {
        final TablePath tablePath;
        final long baseSnapshotId;
        final long compactSnapshotId;
        final long changeId;
        @Nullable final Map<String, Map<Integer, List<String>>> removedFiles;
        // the events that request the re-index's splits
        final List<RowPosRequestEvent> requests;
        // one result per split
        final int expectedResults;
        final long startedAtMs;
        // The registration of a round that committed to the lake before it was held, or null if
        // the round commits once the re-index is published.
        @Nullable final Registration<?> registration;

        private Reindex(
                TablePath tablePath,
                long baseSnapshotId,
                long compactSnapshotId,
                long changeId,
                @Nullable Map<String, Map<Integer, List<String>>> removedFiles,
                List<RowPosRequestEvent> requests,
                int expectedResults,
                long startedAtMs,
                @Nullable Registration<?> registration) {
            this.tablePath = tablePath;
            this.baseSnapshotId = baseSnapshotId;
            this.compactSnapshotId = compactSnapshotId;
            this.changeId = changeId;
            this.removedFiles = removedFiles;
            this.requests = requests;
            this.expectedResults = expectedResults;
            this.startedAtMs = startedAtMs;
            this.registration = registration;
        }
    }

    /**
     * Unwinds a round that waits for a re-index, or must run again: on a published re-index, or on
     * the snapshot the lake table moved to during the round.
     */
    private static final class ReindexPending extends Exception {
        private static final long serialVersionUID = 1L;

        // null if the re-index is already published, so the round runs again at once
        @Nullable private final transient Reindex reindex;
        // whether the round runs again because the lake rejected it on a moved base
        private final boolean baseChanged;

        private ReindexPending(@Nullable Reindex reindex) {
            this(reindex, false);
        }

        private ReindexPending(@Nullable Reindex reindex, boolean baseChanged) {
            super(null, null, false, false);
            this.reindex = reindex;
            this.baseChanged = baseChanged;
        }

        static ReindexPending baseChanged() {
            return new ReindexPending(null, true);
        }
    }

    /** A counter that commit threads of different tables increment concurrently. */
    private static final class ConcurrentCounter implements Counter {
        private final LongAdder count = new LongAdder();

        @Override
        public void inc() {
            count.increment();
        }

        @Override
        public void inc(long n) {
            count.add(n);
        }

        @Override
        public void dec() {
            count.decrement();
        }

        @Override
        public void dec(long n) {
            count.add(-n);
        }

        @Override
        public long getCount() {
            return count.sum();
        }
    }
}
