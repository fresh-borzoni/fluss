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
import org.apache.fluss.lake.committer.LakeCommitRejectedException;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.dv.FormatVersions;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.lake.dv.index.StagedIndexPart;
import org.apache.fluss.lake.dv.index.StagedIndexPartProvider;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.apache.fluss.flink.tiering.committer.DerivedDvContext.DERIVED_BASE_PROPERTY;
import static org.apache.fluss.flink.tiering.committer.DerivedDvContext.DERIVED_INDEX_ID_PROPERTY;
import static org.apache.fluss.flink.tiering.committer.DerivedDvContext.DERIVED_OFFSETS_PROPERTY;
import static org.apache.fluss.flink.tiering.committer.DerivedDvContext.checkSameTable;
import static org.apache.fluss.flink.tiering.committer.DerivedDvContext.dvCommit;
import static org.apache.fluss.flink.tiering.committer.DerivedDvContext.positionDeletes;
import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;

/**
 * Runs a round of a lake that takes deletes (Iceberg): it looks up the lake positions of the rows
 * it supersedes in the base's index, publishes its index change, and commits data and deletes in
 * one lake commit, which is the round's commit point.
 *
 * <p>B is the snapshot registered in Fluss when the round starts, its base. An attempt of the round
 * ({@link #commitOnce}) takes these steps; the {@link Phase} it reached tells what it leaves
 * behind.
 *
 * <ol>
 *   <li>{@link Phase#STARTED}: the table is still the round's, the results of dropped partitions
 *       are left out, the lake still descends from B, and a commit an earlier attempt left
 *       unregistered is registered first ({@link
 *       DerivedDvRecovery#checkFlussNotMissingLakeSnapshot}), after which the round fails and runs
 *       again.
 *   <li>{@link Phase#OFFSETS_WRITTEN}: the offsets file of the snapshot.
 *   <li>{@link Phase#INDEX_PUBLISHED}: the writers' staged parts are published as index change I,
 *       committed in the store and not registered, with the base positions of the changed keys
 *       looked up on the leaders; the chain would take I from B.
 *   <li>{@link Phase#LAKE_COMMITTING}: after another epoch check, one lake commit of data and
 *       deletes, fenced on B, whose snapshot S records I, B and the offsets. {@link
 *       Phase#LAKE_REJECTED} if the lake surely refused it; {@link Phase#LAKE_COMMITTED} once it
 *       returned.
 *   <li>{@link Phase#CHAIN_REGISTERED}: I is registered as B → S.
 *   <li>{@link Phase#FLUSS_REGISTERED}: S is registered in Fluss, under the round's epoch. Then the
 *       chain may be trimmed and old lake snapshots expired ({@link DerivedDvRegistrations}).
 * </ol>
 *
 * <p>Transitions and crash rules:
 *
 * <ul>
 *   <li>A failure before {@code LAKE_COMMITTING}, or at {@code LAKE_REJECTED}, deletes the
 *       attempt's data files, offsets file and index change ({@link
 *       DerivedDvRecovery#cleanUpFailedRound}); a crash leaves them as orphans, and the round is
 *       tiered again from B's offsets.
 *   <li>A failure or crash at {@code LAKE_COMMITTING} or later keeps every file, as S may exist,
 *       also when the lake does not show S yet: a commit with an unknown outcome may still land
 *       after the call failed, e.g. on a REST catalog. This is Iceberg's own rule; the files of a
 *       commit that never lands are left to orphan-file removal. The next round finds S by its
 *       properties and registers it ({@link DerivedDvRecovery#checkFlussNotMissingLakeSnapshot});
 *       registering I again is a no-op.
 *   <li>The writers' staged parts are deleted when the round ends.
 * </ul>
 */
final class DerivedDvRound<WriteResult, Committable> {

    private static final Logger LOG = DerivedDvContext.LOG;

    private final DerivedDvContext<WriteResult, Committable> context;

    /**
     * The phases of an attempt of a round, in order. Each names the last durable step the attempt
     * took; see the class javadoc for what it leaves behind.
     */
    enum Phase {
        /** Only the writers' data files and staged index parts exist. */
        STARTED(false),
        /** The offsets file of the snapshot is written. */
        OFFSETS_WRITTEN(false, STARTED),
        /** The round's index change is committed in the store, not registered. */
        INDEX_PUBLISHED(false, OFFSETS_WRITTEN),
        /** The lake commit is called; until it returns, its outcome is unknown. */
        LAKE_COMMITTING(true, OFFSETS_WRITTEN, INDEX_PUBLISHED),
        /**
         * The lake surely refused the commit ({@link LakeCommitRejectedException}), so no lake
         * snapshot references the files. Never inferred from the lake's state after a failed call:
         * a commit with an unknown outcome may still land, or sit under another committer's.
         */
        LAKE_REJECTED(false, LAKE_COMMITTING),
        /** The lake commit landed. */
        LAKE_COMMITTED(true, LAKE_COMMITTING),
        /** The round's index change is registered in the chain. */
        CHAIN_REGISTERED(true, LAKE_COMMITTED),
        /** The round's snapshot is registered in Fluss. */
        FLUSS_REGISTERED(true, CHAIN_REGISTERED);

        /**
         * Whether a lake snapshot may reference the attempt's files, so a failed round keeps them
         * for the next round to register.
         */
        final boolean lakeMayReferenceFiles;

        private final Set<Phase> previous;

        Phase(boolean lakeMayReferenceFiles, Phase... previous) {
            this.lakeMayReferenceFiles = lakeMayReferenceFiles;
            this.previous = new HashSet<>(Arrays.asList(previous));
        }

        /** Whether an attempt in phase {@code phase} may move on to this phase. */
        boolean follows(Phase phase) {
            return previous.contains(phase);
        }
    }

    private final DerivedDvRecovery<WriteResult, Committable> recovery;
    private final DerivedDvRegistrations<WriteResult, Committable> registrations;

    DerivedDvRound(
            DerivedDvContext<WriteResult, Committable> context,
            DerivedDvRecovery<WriteResult, Committable> recovery,
            DerivedDvRegistrations<WriteResult, Committable> registrations) {
        this.context = context;
        this.recovery = recovery;
        this.registrations = registrations;
    }

    /**
     * Runs a round until it commits or fails. The writers' staged index parts are deleted when it
     * ends either way. The change the round published holds what it needs of them.
     */
    RoundOutcome<Committable> commitRound(
            long tableId,
            TablePath tablePath,
            TableInfo tableInfo,
            List<TableBucketWriteResult<WriteResult>> writeResults)
            throws Exception {
        try {
            return RoundOutcome.committed(commitOnce(tableId, tablePath, tableInfo, writeResults));
        } finally {
            deleteStagedParts(tableId, tablePath, writeResults);
        }
    }

    private TieringCommitResult<Committable> commitOnce(
            long tableId,
            TablePath tablePath,
            TableInfo tableInfo,
            List<TableBucketWriteResult<WriteResult>> committableWriteResults)
            throws Exception {
        checkSameTable(tableId, tablePath, tableInfo);

        // The base is read before the partitions. Every registration after a partition's drop
        // leaves out its buckets, so a partition the base lacks is not listed either. The round
        // then commits nothing there that it cannot look up in the base.
        LakeSnapshot flussCurrentLakeSnapshot = context.getLatestLakeSnapshot(tablePath);
        List<TableBucketWriteResult<WriteResult>> results =
                context.withoutResultsOfDroppedPartitions(
                        tablePath, tableInfo, committableWriteResults);

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
                context.createDvLakeCommitter(tablePath, tableInfo)) {
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
                    dvCommit(lakeCommitter)
                            .checkCurrentSnapshotDescendsFrom(
                                    flussCurrentLakeSnapshot.getSnapshotId());
                }
                // before commit to lake, check fluss not missing any lake snapshot committed by
                // fluss
                recovery.checkFlussNotMissingLakeSnapshot(
                        tablePath,
                        tableId,
                        lakeCommitter,
                        flussCurrentLakeSnapshot == null
                                ? null
                                : flussCurrentLakeSnapshot.getSnapshotId());
                if (context.lakeTieringConfig.get(
                                ConfigOptions.LAKE_TIERING_PARTITION_MARK_DONE_ENABLED)
                        && context.lakeTieringFactory.supportsPartitionMarkDone()) {
                    context.preparePartitionMarkDone(lakeCommitter, committable);
                }
                return commitWithDeletes(
                        tableId,
                        tablePath,
                        tableInfo,
                        lakeCommitter,
                        committable,
                        nonEmptyResults,
                        flussCurrentLakeSnapshot,
                        logEndOffsets,
                        logMaxTieredTimestamps,
                        round);
            } catch (Exception e) {
                recovery.cleanUpFailedRound(tableId, tablePath, lakeCommitter, committable, round);
                throw e;
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
            @Nullable LakeSnapshot flussCurrentLakeSnapshot,
            Map<TableBucket, Long> logEndOffsets,
            Map<TableBucket, Long> logMaxTieredTimestamps,
            RoundFiles round)
            throws Exception {
        long base =
                flussCurrentLakeSnapshot == null ? -1L : flussCurrentLakeSnapshot.getSnapshotId();
        long t0 = System.currentTimeMillis();
        // 1. The writers' parts of the index change, staged in the store when they completed.
        PositionIndexStore store = context.indexStore(tableId, tablePath);
        Map<TableBucket, StagedIndexPart> parts = stagedParts(tablePath, nonEmptyResults);
        long t1 = System.currentTimeMillis();
        // 2. The snapshot's offsets: this round's, and the base's for the other buckets. Dropped
        //    partitions are left out, since no tablet server has their buckets. The offsets file
        //    holds all the buckets, so the dropped ones are not merged back in either.
        Map<TableBucket, Long> readableOffsets = new HashMap<>();
        if (flussCurrentLakeSnapshot != null) {
            readableOffsets.putAll(
                    context.withoutDroppedPartitions(
                            tablePath, flussCurrentLakeSnapshot.getTableBucketsOffset()));
        }
        readableOffsets.putAll(logEndOffsets);
        Set<TableBucket> buckets = readableOffsets.keySet();
        String lakeBucketTieredOffsetsFile =
                context.flussTableLakeSnapshotCommitter.prepareCompleteLakeSnapshot(
                        tableId, tablePath, readableOffsets);
        round.offsetsFile = lakeBucketTieredOffsetsFile;
        round.advance(Phase.OFFSETS_WRITTEN);
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
                PublishedParts.publish(
                        store,
                        indexId,
                        flussCurrentLakeSnapshot,
                        parts,
                        (tb, snapshotId, keys) ->
                                DerivedDvLookups.lookupWithRetry(
                                        context.admin,
                                        tablePath,
                                        tb,
                                        snapshotId,
                                        keys,
                                        Thread::sleep));
        // Only the buckets the round changed are part of the change. The buckets of idle
        // partitions get no manifest, and trimming can tell which changes touch which buckets.
        store.commitChange(indexId, published.changes);
        // The lake commit makes the change permanent: repairSnapshot registers a crashed round's
        // change. A change the chain would refuse fails this round here, before the commit,
        // instead of every round after it.
        store.checkRegistrable(base, indexId);
        round.advance(Phase.INDEX_PUBLISHED);
        long t2 = System.currentTimeMillis();
        // 4. One lake commit of data and deletion vectors, validated against the base.
        snapshotProperties.put(DERIVED_INDEX_ID_PROPERTY, String.valueOf(indexId));
        snapshotProperties.put(DERIVED_BASE_PROPERTY, String.valueOf(base));
        snapshotProperties.put(DERIVED_OFFSETS_PROPERTY, DvSnapshotOffsets.encode(readableOffsets));
        FormatVersions.putSnapshotVersion(snapshotProperties);
        // The base is this service's only while its tiering epoch is current. A round of a table
        // taken from this service would commit its older rows on top of the new owner's round.
        context.flussTableLakeSnapshotCommitter.checkTieringEpoch(tableId);
        round.advance(Phase.LAKE_COMMITTING);
        LakeCommitResult result;
        try {
            result =
                    positionDeletes(lakeCommitter)
                            .commitWithDeletes(
                                    committable, snapshotProperties, published.deleted, base);
        } catch (LakeCommitRejectedException e) {
            round.advance(Phase.LAKE_REJECTED);
            throw e;
        }
        round.advance(Phase.LAKE_COMMITTED);
        long snapshotId = result.getCommittedSnapshotId();
        // Links the base to snapshotId. The store refuses a chain that does not end at the base.
        // The base fence put the lake commit on the base or on snapshots after it that changed no
        // data file, so the change from the base covers the snapshot.
        store.register(base, snapshotId, indexId);
        round.advance(Phase.CHAIN_REGISTERED);
        long t3 = System.currentTimeMillis();
        // 5. Register in Fluss. The snapshot is readable at its tiered offsets, since its deletes
        //    are in the same commit, and tablet servers catch up to it.
        context.flussTableLakeSnapshotCommitter.commit(
                tableId,
                tablePath,
                DerivedDvRegistrations.readableAtTieredOffsets(
                        snapshotId,
                        readableOffsets,
                        result.getTieringStats(),
                        registrations.earliestSnapshotToKeep(tableId, tablePath)),
                lakeBucketTieredOffsetsFile,
                logEndOffsets,
                logMaxTieredTimestamps);
        round.advance(Phase.FLUSS_REGISTERED);
        long t4 = System.currentTimeMillis();
        context.metrics.rounds.inc();
        context.metrics.changedKeys.inc(published.keys);
        context.metrics.lastLookupMs = t2 - t1;
        context.metrics.lastLakeCommitMs = t3 - t2;
        LOG.info(
                "Committed the round of {}: base {} -> snapshot {} (index {}); buckets {};"
                        + " changed keys {} ({} deleted, {} in base); preparing {} ms; index publish"
                        + " with lookups {} ms; lake commit (data+DV) {} ms; fluss report {} ms.",
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

    /** Returns the index part each bucket's writer staged when it completed. */
    private Map<TableBucket, StagedIndexPart> stagedParts(
            TablePath tablePath, List<TableBucketWriteResult<WriteResult>> nonEmptyResults) {
        Map<TableBucket, StagedIndexPart> parts = new HashMap<>();
        for (TableBucketWriteResult<WriteResult> r : nonEmptyResults) {
            Object wr = r.writeResult();
            StagedIndexPart part =
                    wr instanceof StagedIndexPartProvider
                            ? ((StagedIndexPartProvider) wr).stagedIndexPart()
                            : null;
            if (part != null) {
                parts.put(r.tableBucket(), part);
            } else if (wr instanceof StagedIndexPartProvider) {
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

    /** Deletes the staging changes of the writers' parts once their round ends. */
    void deleteStagedParts(
            long tableId, TablePath tablePath, List<TableBucketWriteResult<WriteResult>> results) {
        Set<Long> stagingIds = new HashSet<>();
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
                context.indexStore(tableId, tablePath).abortChange(stagingId);
            } catch (Exception e) {
                // removed as an orphan later
                LOG.warn(
                        "Could not delete the staged position index parts {} of {}.",
                        stagingId,
                        tablePath,
                        e);
            }
        }
    }
}
