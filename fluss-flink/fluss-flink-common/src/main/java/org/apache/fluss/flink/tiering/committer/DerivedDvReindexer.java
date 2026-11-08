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
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.tiering.TableBucketWriteResult;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.LakeTableRolledBackException;
import org.apache.fluss.flink.tiering.event.RowPosRequestEvent;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.dv.DeletionVectorCapabilities;
import org.apache.fluss.lake.dv.RowPosSstIndex;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.DataDeltaPlan;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.RowPosResult;
import org.apache.fluss.lake.source.SupportsRewritePlanning;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.fluss.flink.tiering.committer.DerivedDvContext.partitionName;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Re-indexes lake files that another engine rewrote, or that a compaction rewrote without handing
 * their positions over, on the tiering readers.
 *
 * <p>Before a round with deletes resolves its keys, {@link #triggerReindex} compares the base with
 * the lake's current snapshot. A rewrite since the base is re-indexed first: if it is already in
 * the chain only its Fluss registration is made, if it only removed files its change is published
 * at once, and otherwise the round is held ({@link RoundOutcome#held}) while the tiering readers
 * read the rewritten files. {@link #publishReindex} publishes and registers the change once they
 * reported. A round without data does the same ({@link #reindexIdleTable}). For a lake that merges
 * by key, {@link #indexReadableSnapshot} indexes a newly readable snapshot after the round's
 * commit.
 *
 * <p>Crash rule: a re-index is registered in the chain before it is registered in Fluss, and a
 * change in the chain but not in Fluss is registered in Fluss by the next round. A re-index that
 * was planned but not published is planned again under a new change id; the old one is an orphan.
 */
final class DerivedDvReindexer<WriteResult, Committable> {

    private static final Logger LOG = DerivedDvContext.LOG;

    private final DerivedDvContext<WriteResult, Committable> context;

    // The most re-index splits one request event carries; keeps an event at a few hundred KB.
    @VisibleForTesting static final int SPLITS_PER_REQUEST = 1_000;

    private final DerivedDvRegistrations<WriteResult, Committable> registrations;
    private final DerivedDvCompactionPublisher<WriteResult, Committable> compaction;

    DerivedDvReindexer(
            DerivedDvContext<WriteResult, Committable> context,
            DerivedDvRegistrations<WriteResult, Committable> registrations,
            DerivedDvCompactionPublisher<WriteResult, Committable> compaction) {
        this.context = context;
        this.registrations = registrations;
        this.compaction = compaction;
    }

    /**
     * Runs a round without data: re-indexes the lake files another engine rewrote since the latest
     * snapshot, which makes the rewritten snapshot readable. The resumed round comes back here and
     * finds nothing new to re-index, unless the table was rewritten again meanwhile.
     */
    void reindexIdleTable(long tableId, TablePath tablePath, TableInfo tableInfo) throws Exception {
        LakeSnapshot base = context.getLatestLakeSnapshot(tablePath);
        if (base == null) {
            LOG.info(
                    "Commit tiering write results is empty for table {} ({}).", tablePath, tableId);
            return;
        }
        triggerReindex(tableId, tablePath, tableInfo, base);
        LOG.info(
                "Idle round of {} found no rewrite since snapshot {}.",
                tablePath,
                base.getSnapshotId());
    }

    /**
     * Plans the re-index of files rewritten since the base and returns if nothing was rewritten.
     * Otherwise throws {@link ReindexPending}: with the {@link Reindex} the round waits for, or
     * without one if the re-index is already published.
     */
    @SuppressWarnings("unchecked")
    void triggerReindex(long tableId, TablePath tablePath, TableInfo tableInfo, LakeSnapshot base)
            throws Exception {
        if (base == null) {
            return;
        }
        // the lake table may live under a custom lake table path
        LakeSource<?> lakeSource =
                context.dvTiering(tablePath).createLakeSource(tableInfo.getLakeTablePath());
        triggerReindex(tableId, tablePath, (LakeSource<LakeSplit>) lakeSource, base);
    }

    private <Split extends LakeSplit> void triggerReindex(
            long tableId, TablePath tablePath, LakeSource<Split> lakeSource, LakeSnapshot base)
            throws Exception {
        SupportsRewritePlanning<Split> planning =
                DeletionVectorCapabilities.rewritePlanning(lakeSource, tablePath);
        DataDeltaPlan<Split> plan = planning.planRewrite(base.getSnapshotId(), null);
        if (plan == null) {
            return;
        }
        long compactSnapshotId = plan.getCompactSnapshotId();
        PositionIndexStore store = context.indexStore(tableId, tablePath);
        // A fork a crashed registration left keeps the branch Fluss registered.
        store.repairFork(base.getSnapshotId());
        List<PositionIndexStore.ChainEntry> chain = store.chain();
        int compactIndex = DerivedDvRegistrations.chainIndexOf(chain, compactSnapshotId);
        int baseIndex = DerivedDvRegistrations.chainIndexOf(chain, base.getSnapshotId());
        if (compactIndex < 0 && baseIndex >= 0 && baseIndex < chain.size() - 1) {
            // A rewrite reached the chain but not Fluss (its service lost the coordinator), then
            // the lake moved on: the chain's end is registered first. Fails on a rollback past it.
            planning.planRewrite(chain.get(chain.size() - 1).snapshotId, compactSnapshotId);
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
            registrations.registerSnapshot(tableId, tablePath, compactSnapshotId, base);
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
                    Collections.emptyMap(),
                    plan.getDeletedFiles(),
                    System.currentTimeMillis(),
                    true);
            throw new ReindexPending(null);
        }
        int firstFileId = store.allocateFileIds(plan.getSplits().size());
        FsPath lakeDir = context.lakeTableDir(tableId, tablePath);
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
                "{} files of {} were rewritten since snapshot {}; snapshot {} is"
                        + " re-indexed on the tiering readers.",
                plan.getSplits().size(),
                tablePath,
                base.getSnapshotId(),
                compactSnapshotId);
        throw new ReindexPending(reindex);
    }

    /**
     * Publishes and registers the re-index of a rewritten snapshot, then registers the snapshot in
     * Fluss so tablet servers catch up to it.
     */
    void publishReindex(
            long tableId,
            TablePath tablePath,
            long baseSnapshotId,
            long compactSnapshotId,
            long changeId,
            Map<TableBucket, List<RowPosResult>> results,
            @Nullable Map<String, Map<Integer, List<String>>> replacedFiles,
            long startedAtMs,
            boolean registerInFluss)
            throws Exception {
        long t0 = System.currentTimeMillis();
        // Per bucket: the SSTs and new file ids of its splits, and the files that left the
        // snapshot.
        Map<TableBucket, List<RowPosSstIndex.SstFileEntry>> ssts = new HashMap<>();
        Map<TableBucket, Map<Integer, String>> fileIds = new HashMap<>();
        for (Map.Entry<TableBucket, List<RowPosResult>> b : results.entrySet()) {
            TableBucket tb = b.getKey();
            List<RowPosSstIndex.SstFileEntry> entries =
                    ssts.computeIfAbsent(tb, k -> new ArrayList<>());
            for (RowPosResult r : b.getValue()) {
                for (RowPosResult.SstMeta m : r.getSstMetas()) {
                    entries.add(new RowPosSstIndex.SstFileEntry(m.getFileName(), m.getFileSize()));
                }
                fileIds.computeIfAbsent(tb, k -> new HashMap<>()).putAll(r.getNewFileDictEntries());
            }
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
            if (context.admin.getTableInfo(tablePath).get().isPartitioned()) {
                for (PartitionInfo pi : context.admin.listPartitionInfos(tablePath, true).get()) {
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
        PositionIndexStore store = context.indexStore(tableId, tablePath);
        // Each split sorts its own keys. Two splits of a bucket may still hold one key, when the
        // rewritten files have two live rows of it: the table has duplicates, and either row
        // would be left unmasked. The re-index fails instead of choosing one.
        for (Map.Entry<TableBucket, List<RowPosResult>> b : results.entrySet()) {
            if (b.getValue().size() > 1) {
                store.checkDistinctKeys(
                        changeId, b.getKey(), ssts.get(b.getKey()), fileIds.get(b.getKey()));
            }
        }
        store.commitChange(changeId, published);
        store.register(
                baseSnapshotId,
                compactSnapshotId,
                changeId,
                context.snapshotIdentity(tablePath, compactSnapshotId));
        if (registerInFluss) {
            registrations.registerSnapshot(
                    tableId,
                    tablePath,
                    compactSnapshotId,
                    context.getLatestLakeSnapshot(tablePath));
        }
        context.metrics.reindexes.inc();
        context.metrics.lastReindexMs = System.currentTimeMillis() - startedAtMs;
        context.metrics.reindexMs.inc(context.metrics.lastReindexMs);
        LOG.info(
                "Re-indexed rewritten snapshot {} of {} with {} parallel scans, {}"
                        + " files dropped, in {} ms since planned; published in {} ms.",
                compactSnapshotId,
                tablePath,
                results.size(),
                replaced,
                context.metrics.lastReindexMs,
                System.currentTimeMillis() - t0);
    }

    /**
     * Brings the chain up to the readable snapshot of a merging round's commit. Returns the
     * re-index the round waits for, or null when the round can register: the chain has the readable
     * snapshot now, or there is none.
     */
    @Nullable
    @SuppressWarnings("unchecked")
    Reindex indexReadableSnapshot(
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
        PositionIndexStore store = context.indexStore(tableId, tablePath);
        if (readable == null) {
            compaction.discardHandedOverIndex(store, results);
            return null;
        }
        // A fork a crashed registration left keeps the branch Fluss registered, or else the one
        // this round is about to register.
        LakeSnapshot registered = context.getLatestLakeSnapshot(tablePath);
        if (registered != null) {
            store.repairFork(registered.getSnapshotId());
        }
        store.repairFork(readable);
        List<PositionIndexStore.ChainEntry> chain = store.chain();
        for (PositionIndexStore.ChainEntry e : chain) {
            if (e.snapshotId == readable) {
                // nothing newly readable, so the compactions' positions have no change to make
                compaction.discardHandedOverIndex(store, results);
                return null;
            }
        }
        long base = chain.isEmpty() ? -1L : chain.get(chain.size() - 1).snapshotId;
        LakeSource<?> lakeSource =
                context.dvTiering(tablePath).createLakeSource(tableInfo.getLakeTablePath());
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
        DataDeltaPlan<Split> plan =
                DeletionVectorCapabilities.rewritePlanning(lakeSource, tablePath)
                        .planRewrite(base, readable);
        Map<String, Long> partitionIds = Collections.emptyMap();
        if (plan != null) {
            partitionIds = partitionIds(tablePath, plan);
            plan = withoutFilesOfDroppedPartitions(tablePath, plan, partitionIds);
        }
        long changeId = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        if (plan == null || plan.getSplits().isEmpty()) {
            // Nothing to read, e.g. a compaction whose output has no rows: publish at once.
            compaction.discardHandedOverIndex(store, results);
            publishReindex(
                    tableId,
                    tablePath,
                    base,
                    readable,
                    changeId,
                    Collections.emptyMap(),
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
        if (compaction.publishHandedOverIndex(tablePath, store, plan, base, readable, results)) {
            return null;
        }
        int firstFileId = store.allocateFileIds(plan.getSplits().size());
        FsPath lakeDir = context.lakeTableDir(tableId, tablePath);
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
                "Readable snapshot {} of {} has {} new readable files since snapshot"
                        + " {} ({} left); they are indexed on the tiering readers.",
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
                context.lakeTieringConfig.get(
                        ConfigOptions.LAKE_TIERING_DELETION_VECTORS_REINDEX_FILES_PER_SPLIT);
        checkState(
                filesPerSplit > 0,
                "%s must be positive.",
                ConfigOptions.LAKE_TIERING_DELETION_VECTORS_REINDEX_FILES_PER_SPLIT.key());
        long rowsPerSplit =
                context.lakeTieringConfig.get(
                        ConfigOptions.LAKE_TIERING_DELETION_VECTORS_REINDEX_ROWS_PER_SPLIT);
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
        PositionIndexStore store = context.indexStore(tableId, lakeDir);
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
        for (PartitionInfo partition : context.admin.listPartitionInfos(tablePath, true).get()) {
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
                "{} newly readable files of {} in dropped partitions {} are not" + " indexed.",
                plan.getSplits().size() - kept.size(),
                tablePath,
                dropped);
        return new DataDeltaPlan<>(plan.getCompactSnapshotId(), kept, plan.getDeletedFiles());
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
            long fileRows = Math.max(0, DeletionVectorCapabilities.dataFiles(file).rowCount());
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
}
