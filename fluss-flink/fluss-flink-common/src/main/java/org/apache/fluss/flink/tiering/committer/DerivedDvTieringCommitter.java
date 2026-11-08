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
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.exception.RetriableException;
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
import org.apache.fluss.lake.dv.index.KeyPosition;
import org.apache.fluss.lake.dv.index.PositionIndexChangeBuilder;
import org.apache.fluss.lake.dv.index.PositionIndexDelta;
import org.apache.fluss.lake.dv.index.PositionIndexDeltaProvider;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.lake.dv.index.StagedIndexPart;
import org.apache.fluss.lake.dv.index.StagedIndexPartProvider;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.rpc.messages.PbDvKeyPosition;
import org.apache.fluss.utils.ExceptionUtils;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
 * so every lake snapshot is consistent at its tiered offsets.
 *
 * <p>The base fence lets the lake commit land only on the base, or on snapshots after it that
 * changed no data file. A rewrite committed since the base makes the lake reject the commit ({@link
 * LakeBaseChangedException}), and the round fails. A registered change from the base thus always
 * covers its snapshot, also when a crashed round's snapshot is registered later.
 *
 * <p>Rounds run on the commit threads of the {@link TieringCommitOperator}. Rounds of different
 * tables run concurrently, at most one round of a table at a time. The state kept across rounds is
 * per table and thread-safe.
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

    // A bucket catches up to a newly registered snapshot within seconds.
    private static final int MAX_STALE_SNAPSHOT_RETRIES = 480;
    private static final long STALE_SNAPSHOT_BACKOFF_MS = 250;
    // A leader election or a restarted server's registration takes seconds: about 40 s in all.
    @VisibleForTesting static final int MAX_LEADER_RETRIES = 12;
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
    private final Counter chainEntriesTrimmed;
    private final Counter orphanChangesRemoved;
    private volatile long lastLookupMs;
    private volatile long lastLakeCommitMs;

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
        this.chainEntriesTrimmed = dv.counter("chainEntriesTrimmed", new ConcurrentCounter());
        this.orphanChangesRemoved = dv.counter("orphanChangesRemoved", new ConcurrentCounter());
        dv.gauge("lastLookupMs", (Gauge<Long>) () -> lastLookupMs);
        dv.gauge("lastLakeCommitMs", (Gauge<Long>) () -> lastLakeCommitMs);
    }

    /** Commits a round of a table with deletion vectors. */
    Outcome<Committable> commitRound(
            long tableId,
            TablePath tablePath,
            TableInfo tableInfo,
            List<TableBucketWriteResult<WriteResult>> writeResults)
            throws Exception {
        return reportingRollback(
                tableId, () -> commitStagedRound(tableId, tablePath, tableInfo, writeResults));
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
     * Runs a round until it commits or fails. The writers' staged index parts are deleted when it
     * ends either way. The change the round published holds what it needs of them.
     */
    private Outcome<Committable> commitStagedRound(
            long tableId,
            TablePath tablePath,
            TableInfo tableInfo,
            List<TableBucketWriteResult<WriteResult>> writeResults)
            throws Exception {
        Map<TableBucket, StagedIndexPart> legacyParts =
                stageLegacyDeltas(tableId, tablePath, writeResults);
        try {
            return Outcome.committed(
                    commitOnce(tableId, tablePath, tableInfo, writeResults, legacyParts));
        } finally {
            deleteStagedParts(tableId, tablePath, writeResults, legacyParts);
        }
    }

    private TieringCommitResult<Committable> commitOnce(
            long tableId,
            TablePath tablePath,
            TableInfo tableInfo,
            List<TableBucketWriteResult<WriteResult>> committableWriteResults,
            Map<TableBucket, StagedIndexPart> legacyParts)
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

        // All buckets were empty, so nothing is committed.
        if (nonEmptyResults.isEmpty()) {
            LOG.info(
                    "Commit tiering write results is empty for table {} ({}).", tablePath, tableId);
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
                        round);
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
            RoundFiles round)
            throws Exception {
        long base =
                flussCurrentLakeSnapshot == null ? -1L : flussCurrentLakeSnapshot.getSnapshotId();
        long t0 = System.currentTimeMillis();
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
     * Deletes the staging changes of a round's parts once the round ends: the writers' ones and the
     * ones this committer staged for older versions' write results.
     */
    private void deleteStagedParts(
            long tableId,
            TablePath tablePath,
            List<TableBucketWriteResult<WriteResult>> results,
            Map<TableBucket, StagedIndexPart> legacyParts) {
        Set<Long> stagingIds = new HashSet<>();
        for (StagedIndexPart part : legacyParts.values()) {
            stagingIds.add(part.stagingId());
        }
        for (TableBucketWriteResult<WriteResult> r : results) {
            Object wr = r.writeResult();
            if (wr instanceof StagedIndexPartProvider) {
                StagedIndexPart part = ((StagedIndexPartProvider) wr).stagedIndexPart();
                if (part != null) {
                    stagingIds.add(part.stagingId());
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
     * round so it runs again on top of it. The caller deletes the round's files.
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

    /** The outcome of a round: committed. */
    static final class Outcome<Committable> {
        private final TieringCommitResult<Committable> result;

        private Outcome(TieringCommitResult<Committable> result) {
            this.result = result;
        }

        static <Committable> Outcome<Committable> committed(
                TieringCommitResult<Committable> result) {
            return new Outcome<>(result);
        }

        /** The committed round's result. */
        TieringCommitResult<Committable> result() {
            return result;
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
