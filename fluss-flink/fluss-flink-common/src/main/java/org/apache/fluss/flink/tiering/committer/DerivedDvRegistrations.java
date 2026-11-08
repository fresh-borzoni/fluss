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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.committer.TieringStats;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.flink.tiering.committer.DerivedDvContext.dvCommit;

/**
 * Registers lake snapshots in Fluss and keeps what the index chain needs.
 *
 * <p>A snapshot is registered in Fluss only once its index change is in the chain, so a tablet
 * server can always cover it. Each registration keeps the registered snapshots from the chain's
 * oldest one on ({@link #earliestSnapshotToKeep}). After a round registered its snapshot, every
 * {@code lake.tiering.deletion-vectors.trim-interval} changes the chain is trimmed and orphan index
 * changes are removed ({@link #maybeTrimIndexChain}); only then, as a tablet server rebuilds from
 * the registered snapshot. Snapshots Fluss registers but did not commit itself are protected from
 * expiry first ({@link #protectReadableSnapshot}).
 *
 * <p>Crash rule: every step here is repeatable. A registration the chain already has is registered
 * again as a no-op, and a trim cut short is finished by the next one.
 */
final class DerivedDvRegistrations<WriteResult, Committable> {

    private static final Logger LOG = DerivedDvContext.LOG;

    private final DerivedDvContext<WriteResult, Committable> context;

    // Per table: the oldest snapshot Fluss keeps registrations from; known to be registered.
    private final Map<Long, Long> retainedFrom = new ConcurrentHashMap<>();

    DerivedDvRegistrations(DerivedDvContext<WriteResult, Committable> context) {
        this.context = context;
    }

    /**
     * Registers a rewritten snapshot in Fluss, readable at the base's tiered offsets. It is
     * protected from expiration first, like the snapshots rounds commit: union reads plan on it,
     * and the next round looks it up as its base.
     */
    void registerSnapshot(
            long tableId, TablePath tablePath, long snapshotId, LakeSnapshot baseSnapshot)
            throws Exception {
        protectReadableSnapshot(tablePath, snapshotId);
        Map<TableBucket, Long> offsets =
                context.withoutDroppedPartitions(tablePath, baseSnapshot.getTableBucketsOffset());
        String offsetsFile =
                context.flussTableLakeSnapshotCommitter.prepareCompleteLakeSnapshot(
                        tableId, tablePath, offsets);
        context.flussTableLakeSnapshotCommitter.commit(
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
            TableInfo tableInfo = context.admin.getTableInfo(tablePath).get();
            try (LakeCommitter<WriteResult, Committable> lakeCommitter =
                    context.createDvLakeCommitter(tablePath, tableInfo)) {
                dvCommit(lakeCommitter).protectReadableSnapshot(snapshotId);
            }
        } catch (Exception e) {
            LOG.warn(
                    "Could not protect readable snapshot {} of {} from expiration.",
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
    static LakeCommitResult readableAtTieredOffsets(
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
    Long earliestSnapshotToKeep(long tableId, TablePath tablePath) throws Exception {
        List<PositionIndexStore.ChainEntry> chain = context.indexStore(tableId, tablePath).chain();
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
            context.admin.getLakeSnapshot(tablePath, snapshotId).get();
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
     * The buckets with data in Fluss's latest registration of the table (or else in {@code
     * fallback}), without those of dropped partitions.
     */
    private List<TableBucket> bucketsWithData(
            long tableId, TablePath tablePath, Map<TableBucket, Long> fallback) throws IOException {
        try {
            LakeSnapshot registered = context.getLatestLakeSnapshot(tablePath);
            Map<TableBucket, Long> offsets =
                    context.withoutDroppedPartitions(
                            tablePath,
                            registered == null ? fallback : registered.getTableBucketsOffset());
            List<TableBucket> withData = new ArrayList<>();
            for (Map.Entry<TableBucket, Long> e : offsets.entrySet()) {
                if (e.getValue() != null && e.getValue() > 0) {
                    withData.add(e.getKey());
                }
            }
            return withData;
        } catch (Exception e) {
            throw new IOException(
                    "Cannot read the buckets with data of " + tablePath + " (" + tableId + ")", e);
        }
    }

    /**
     * The partitions of the table Fluss dropped, read once when the trim first asks, after it
     * listed the chain: a partition whose changes are on the chain then is either live or dropped
     * for good, as partition ids are never reused.
     */
    private PositionIndexStore.DroppedPartitions droppedPartitions(TablePath tablePath) {
        AtomicReference<Set<Long>> live = new AtomicReference<>();
        return partitionId -> {
            if (live.get() == null) {
                Set<Long> ids = new HashSet<>();
                try {
                    // Includes system partitions such as the historical partition.
                    for (PartitionInfo partition :
                            context.admin.listPartitionInfos(tablePath, true).get()) {
                        ids.add(partition.getPartitionId());
                    }
                } catch (Exception e) {
                    throw new IOException("Cannot list the partitions of " + tablePath, e);
                }
                live.set(ids);
            }
            return !live.get().contains(partitionId);
        };
    }

    void maybeTrimIndexChain(
            long tableId,
            TablePath tablePath,
            PositionIndexStore store,
            Map<TableBucket, Long> readableOffsets) {
        try {
            // Counted in the index store rather than here, so a tiering service that fails over
            // more often than the interval still trims.
            if (store.changesSinceTrim()
                    < context.lakeTieringConfig.get(
                            ConfigOptions.LAKE_TIERING_DELETION_VECTORS_TRIM_INTERVAL)) {
                return;
            }
            store.startTrim();
        } catch (Exception e) {
            LOG.warn(
                    "Could not tell whether the position index chain of {} is due for a" + " trim.",
                    tablePath,
                    e);
            return;
        }
        try {
            // Right before the trim: a service that lost the table meanwhile (paused) trims
            // nothing. The buckets come from Fluss's latest registration, read only once the
            // trim listed the chain, so it is no older than that listing; the store keeps the
            // buckets of entries added since.
            context.flussTableLakeSnapshotCommitter.checkTieringEpoch(tableId);
            List<TableBucket> withData = new ArrayList<>();
            long t0 = System.currentTimeMillis();
            PositionIndexStore.TrimResult result =
                    store.trim(
                            () -> {
                                withData.clear();
                                withData.addAll(
                                        bucketsWithData(tableId, tablePath, readableOffsets));
                                return withData;
                            },
                            context.lakeTieringConfig.get(
                                    ConfigOptions.LAKE_TIERING_DELETION_VECTORS_TRIM_RETAIN),
                            droppedPartitions(tablePath));
            int trimmed = result.trimmed;
            context.metrics.trimmed(tableId, result.chainLength, result.holdingBuckets);
            context.metrics.checkpointBuildsFailed.inc(result.failedBuilds);
            // Only failed attempts' changes are orphans here. This round's change is registered,
            // and repairSnapshot registers a crashed round's change before any later commit.
            int orphans =
                    store.deleteOrphanChanges(
                            context.lakeTieringConfig
                                    .get(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_ORPHAN_MIN_AGE)
                                    .toMillis());
            context.metrics.chainEntriesTrimmed.inc(trimmed);
            context.metrics.orphanChangesRemoved.inc(orphans);
            if (orphans > 0) {
                LOG.info("Removed {} orphan position index changes of {}.", orphans, tablePath);
            }
            LOG.info(
                    "Trimmed the position index chain of {} by {} entries in {} ms ({}"
                            + " buckets with data).",
                    tablePath,
                    trimmed,
                    System.currentTimeMillis() - t0,
                    withData.size());
        } catch (Exception e) {
            context.metrics.indexTrimsFailed.inc();
            LOG.warn("Trimming the position index chain of {} failed.", tablePath, e);
        }
    }

    static int chainIndexOf(List<PositionIndexStore.ChainEntry> chain, long snapshotId) {
        for (int i = 0; i < chain.size(); i++) {
            if (chain.get(i).snapshotId == snapshotId) {
                return i;
            }
        }
        return -1;
    }
}
