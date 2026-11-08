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

package org.apache.fluss.flink.lake;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.initializer.NoStoppingOffsetsInitializer;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketPart;
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketPlanner;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.flink.lake.split.DvBucketSplit;
import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static org.apache.fluss.flink.source.split.LogSplit.NO_STOPPING_OFFSET;
import static org.apache.fluss.metadata.ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR;

/** A generator for lake splits. */
public class LakeSplitGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(LakeSplitGenerator.class);

    private final TableInfo tableInfo;
    private final Admin flussAdmin;
    private final OffsetsInitializer.BucketOffsetsRetriever bucketOffsetsRetriever;
    private final OffsetsInitializer stoppingOffsetInitializer;
    private final int bucketCount;
    private final Supplier<Set<PartitionInfo>> listPartitionSupplier;
    private final boolean dvEnabled;

    private final LakeSource<LakeSplit> lakeSource;

    // The most rows of a bucket's lake files one split reads when a pinned cut lets several splits
    // read the bucket.
    private long fileGroupRows = DvBucketPlanner.DEFAULT_FILE_GROUP_ROWS;

    public LakeSplitGenerator(
            TableInfo tableInfo,
            Admin flussAdmin,
            LakeSource<LakeSplit> lakeSource,
            OffsetsInitializer.BucketOffsetsRetriever bucketOffsetsRetriever,
            OffsetsInitializer stoppingOffsetInitializer,
            int bucketCount,
            Supplier<Set<PartitionInfo>> listPartitionSupplier) {
        this.tableInfo = tableInfo;
        this.flussAdmin = flussAdmin;
        this.lakeSource = lakeSource;
        this.bucketOffsetsRetriever = bucketOffsetsRetriever;
        this.stoppingOffsetInitializer = stoppingOffsetInitializer;
        this.bucketCount = bucketCount;
        this.listPartitionSupplier = listPartitionSupplier;
        this.dvEnabled = tableInfo.getTableConfig().isDeletionVectorsEnabled();
    }

    /**
     * Return A list of hybrid lake snapshot {@link LakeSnapshotSplit}, {@link
     * LakeSnapshotAndFlussLogSplit} and the corresponding Fluss {@link LogSplit} based on the lake
     * snapshot. Return null if no lake snapshot exists.
     *
     * <p>A bounded read of a primary key table with deletion vectors gets {@link DvBucketSplit}s,
     * planned on the coordinator's readable snapshot, or on the empty lake if nothing is tiered
     * yet. Planning asks no tablet server for masks. Each split's reader fetches its masks when the
     * split starts.
     */
    @Nullable
    public List<SourceSplitBase> generateHybridLakeFlussSplits() throws Exception {
        if (dvEnabled
                && tableInfo.hasPrimaryKey()
                && stoppingOffsetInitializer instanceof NoStoppingOffsetsInitializer) {
            // Masked reads stop at the masks' cut, so only bounded reads can use them. A
            // streaming read needs the ordered changelog of the KV snapshot and log splits.
            LOG.info(
                    "Streaming read of deletion-vector table {}: reading Fluss KV snapshot and"
                            + " changelog instead of lake splits with masks.",
                    tableInfo.getTablePath());
            return null;
        }
        boolean dvBatchRead = isBatchReadWithDeletionVectors();
        LakeSnapshot lakeSnapshotInfo;
        try {
            lakeSnapshotInfo = flussAdmin.getReadableLakeSnapshot(tableInfo.getTablePath()).get();
        } catch (Exception exception) {
            if (!(ExceptionUtils.stripExecutionException(exception)
                    instanceof LakeTableSnapshotNotExistException)) {
                throw exception;
            }
            if (!dvBatchRead) {
                return null;
            }
            // Nothing tiered yet reads as an empty lake at offset 0, with the whole log masked.
            lakeSnapshotInfo = null;
        }
        boolean emptyLake = lakeSnapshotInfo == null;
        long snapshotId = emptyLake ? -1L : lakeSnapshotInfo.getSnapshotId();

        boolean isLogTable = !tableInfo.hasPrimaryKey();
        boolean isPartitioned = tableInfo.isPartitioned();

        Map<String, Map<Integer, List<LakeSplit>>> lakeSplits =
                emptyLake
                        ? Collections.emptyMap()
                        : groupLakeSplits(
                                lakeSource
                                        .createPlanner((LakeSource.PlannerContext) () -> snapshotId)
                                        .plan());

        Map<TableBucket, Long> tableBucketsOffset =
                emptyLake ? Collections.emptyMap() : lakeSnapshotInfo.getTableBucketsOffset();

        Set<PartitionInfo> partitionInfos = null;
        Map<TableBucket, Long> allStoppingOffsets = new HashMap<>();

        if (isPartitioned) {
            partitionInfos = listPartitionSupplier.get();
            for (PartitionInfo partitionInfo : partitionInfos) {
                // partitions may have their own bucket count
                Map<Integer, Long> offsets =
                        stoppingOffsetInitializer.getBucketOffsets(
                                partitionInfo.getPartitionName(),
                                IntStream.range(0, partitionInfo.getBucketCount())
                                        .boxed()
                                        .collect(Collectors.toList()),
                                bucketOffsetsRetriever);
                for (Map.Entry<Integer, Long> offsetEntry : offsets.entrySet()) {
                    allStoppingOffsets.put(
                            new TableBucket(
                                    tableInfo.getTableId(),
                                    partitionInfo.getPartitionId(),
                                    offsetEntry.getKey()),
                            offsetEntry.getValue());
                }
            }
        } else {
            Map<Integer, Long> offsets =
                    stoppingOffsetInitializer.getBucketOffsets(
                            null,
                            IntStream.range(0, bucketCount).boxed().collect(Collectors.toList()),
                            bucketOffsetsRetriever);
            for (Map.Entry<Integer, Long> offsetEntry : offsets.entrySet()) {
                allStoppingOffsets.put(
                        new TableBucket(tableInfo.getTableId(), null, offsetEntry.getKey()),
                        offsetEntry.getValue());
            }
        }

        List<SourceSplitBase> splits;
        if (isPartitioned) {
            splits =
                    generatePartitionTableSplit(
                            lakeSplits,
                            isLogTable,
                            tableBucketsOffset,
                            partitionInfos,
                            allStoppingOffsets,
                            dvBatchRead,
                            snapshotId);
        } else {
            Map<Integer, List<LakeSplit>> nonPartitionLakeSplits =
                    lakeSplits.isEmpty() ? null : lakeSplits.values().iterator().next();
            // non-partitioned table
            splits =
                    generateNoPartitionedTableSplit(
                            nonPartitionLakeSplits,
                            isLogTable,
                            tableBucketsOffset,
                            allStoppingOffsets,
                            dvBatchRead,
                            snapshotId);
        }
        return dvBatchRead ? pinBuckets(splits, snapshotId) : splits;
    }

    /**
     * Sets the rows of lake files one split reads at most when a bucket is read by several splits.
     */
    public void setFileGroupRows(long fileGroupRows) {
        this.fileGroupRows = fileGroupRows;
    }

    /**
     * Pins one cut per bucket, so that several splits read the bucket with the same masks: groups
     * of its lake files, and its log. A bucket whose leader cannot pin keeps one split that fetches
     * its masks when it starts. So do all buckets if the server does not pin cuts.
     */
    private List<SourceSplitBase> pinBuckets(List<SourceSplitBase> splits, long snapshotId)
            throws Exception {
        List<DvBucketPart> buckets = new ArrayList<>();
        for (SourceSplitBase split : splits) {
            if (split instanceof DvBucketSplit) {
                buckets.add(((DvBucketSplit) split).getPart());
            }
        }
        Map<TableBucket, List<DvBucketPart>> parts =
                new DvBucketPlanner(flussAdmin, tableInfo.getTablePath(), lakeSource)
                        .withFileGroupRows(fileGroupRows)
                        .pin(snapshotId, buckets);
        if (parts.isEmpty()) {
            return splits;
        }
        List<SourceSplitBase> pinned = new ArrayList<>(splits.size());
        for (SourceSplitBase split : splits) {
            List<DvBucketPart> bucketParts =
                    split instanceof DvBucketSplit ? parts.get(split.getTableBucket()) : null;
            if (bucketParts == null) {
                pinned.add(split);
            } else {
                for (DvBucketPart part : bucketParts) {
                    pinned.add(new DvBucketSplit(part));
                }
            }
        }
        return pinned;
    }

    private Map<String, Map<Integer, List<LakeSplit>>> groupLakeSplits(List<LakeSplit> lakeSplits) {
        Map<String, Map<Integer, List<LakeSplit>>> result = new HashMap<>();
        for (LakeSplit split : lakeSplits) {
            String partition = String.join(PARTITION_SPEC_SEPARATOR, split.partition());
            int bucket = split.bucket();
            // Get or create the partition group
            Map<Integer, List<LakeSplit>> bucketMap =
                    result.computeIfAbsent(partition, k -> new HashMap<>());
            List<LakeSplit> splitList = bucketMap.computeIfAbsent(bucket, k -> new ArrayList<>());
            splitList.add(split);
        }
        return result;
    }

    /** Returns whether this is a bounded read of a primary key table with deletion vectors. */
    private boolean isBatchReadWithDeletionVectors() {
        return dvEnabled
                && tableInfo.hasPrimaryKey()
                && !(stoppingOffsetInitializer instanceof NoStoppingOffsetsInitializer);
    }

    private List<SourceSplitBase> generatePartitionTableSplit(
            Map<String, Map<Integer, List<LakeSplit>>> lakeSplits,
            boolean isLogTable,
            Map<TableBucket, Long> tableBucketSnapshotLogOffset,
            Set<PartitionInfo> partitionInfos,
            Map<TableBucket, Long> allStoppingOffsets,
            boolean dvBatchRead,
            long snapshotId) {
        List<SourceSplitBase> splits = new ArrayList<>();
        Map<String, PartitionInfo> flussPartitionByName =
                partitionInfos.stream()
                        .collect(
                                Collectors.toMap(
                                        PartitionInfo::getPartitionName,
                                        partitionInfo -> partitionInfo,
                                        (existing, replacement) -> existing,
                                        LinkedHashMap::new));
        long lakeSplitPartitionId = -1L;

        // iterate lake splits
        for (Map.Entry<String, Map<Integer, List<LakeSplit>>> lakeSplitEntry :
                lakeSplits.entrySet()) {
            String partitionName = lakeSplitEntry.getKey();
            Map<Integer, List<LakeSplit>> lakeSplitsOfPartition = lakeSplitEntry.getValue();
            PartitionInfo flussPartition = flussPartitionByName.remove(partitionName);
            if (flussPartition != null) {
                // mean the partition also exist in fluss partition
                splits.addAll(
                        generateSplit(
                                lakeSplitsOfPartition,
                                flussPartition.getPartitionId(),
                                partitionName,
                                flussPartition.getBucketCount(),
                                isLogTable,
                                tableBucketSnapshotLogOffset,
                                allStoppingOffsets,
                                dvBatchRead,
                                snapshotId));

            } else {
                // only lake data
                splits.addAll(
                        toLakeSnapshotSplits(
                                lakeSplitsOfPartition,
                                partitionName,
                                // now, we can't get partition id for the partition only
                                // in lake, set them to a arbitrary partition id, but
                                // make sure different partition have different partition id
                                // to enable different partition can be distributed to different
                                // tasks
                                lakeSplitPartitionId--));
            }
        }

        // iterate remain fluss splits
        for (PartitionInfo flussPartition : flussPartitionByName.values()) {
            splits.addAll(
                    generateSplit(
                            null,
                            flussPartition.getPartitionId(),
                            flussPartition.getPartitionName(),
                            flussPartition.getBucketCount(),
                            isLogTable,
                            // Lake splits may be pruned while the partition still has tiered
                            // offsets.
                            tableBucketSnapshotLogOffset,
                            allStoppingOffsets,
                            dvBatchRead,
                            snapshotId));
        }
        return splits;
    }

    private List<SourceSplitBase> generateSplit(
            @Nullable Map<Integer, List<LakeSplit>> lakeSplits,
            @Nullable Long partitionId,
            @Nullable String partitionName,
            int numBuckets,
            boolean isLogTable,
            Map<TableBucket, Long> tableBucketSnapshotLogOffset,
            Map<TableBucket, Long> allStoppingOffsets,
            boolean dvBatchRead,
            long snapshotId) {
        List<SourceSplitBase> splits = new ArrayList<>();
        if (isLogTable) {
            if (lakeSplits != null) {
                splits.addAll(toLakeSnapshotSplits(lakeSplits, partitionName, partitionId));
            }
            for (int bucket = 0; bucket < numBuckets; bucket++) {
                TableBucket tableBucket =
                        new TableBucket(tableInfo.getTableId(), partitionId, bucket);
                Long snapshotLogOffset = tableBucketSnapshotLogOffset.get(tableBucket);
                Long stoppingOffset = allStoppingOffsets.get(tableBucket);
                if (stoppingOffset == null) {
                    stoppingOffset = NO_STOPPING_OFFSET;
                }
                if (snapshotLogOffset == null) {
                    // no data committed to lake for this bucket, scan from fluss log
                    if (stoppingOffset == NO_STOPPING_OFFSET || stoppingOffset > 0) {
                        splits.add(
                                new LogSplit(
                                        tableBucket,
                                        partitionName,
                                        EARLIEST_OFFSET,
                                        stoppingOffset));
                    }
                } else {
                    // need to read remain fluss log
                    if (stoppingOffset == NO_STOPPING_OFFSET
                            || snapshotLogOffset < stoppingOffset) {
                        splits.add(
                                new LogSplit(
                                        tableBucket,
                                        partitionName,
                                        snapshotLogOffset,
                                        stoppingOffset));
                    }
                }
            }
        } else {
            // it's primary key table
            if (lakeSplits != null) {
                // Pairing below only visits buckets in [0, numBuckets), so any lake split with a
                // bucket id outside that range would otherwise be lost from the union read.
                List<Integer> outOfRangeBuckets =
                        lakeSplits.keySet().stream()
                                .filter(bucket -> bucket >= numBuckets)
                                .sorted()
                                .collect(Collectors.toList());
                if (!outOfRangeBuckets.isEmpty()) {
                    throw new IllegalStateException(
                            String.format(
                                    "Lake snapshot of table %s partition %s contains buckets %s "
                                            + "outside the enumerated range [0, %d); refusing to "
                                            + "generate union-read splits that would silently "
                                            + "drop lake data.",
                                    tableInfo.getTablePath(),
                                    partitionName,
                                    outOfRangeBuckets,
                                    numBuckets));
                }
            }
            for (int bucket = 0; bucket < numBuckets; bucket++) {
                TableBucket tableBucket =
                        new TableBucket(tableInfo.getTableId(), partitionId, bucket);
                Long snapshotLogOffset = tableBucketSnapshotLogOffset.get(tableBucket);
                Long stoppingOffset = allStoppingOffsets.get(tableBucket);
                if (stoppingOffset == null) {
                    stoppingOffset = NO_STOPPING_OFFSET;
                }
                if (dvBatchRead) {
                    DvBucketSplit dvSplit =
                            generateDvSplitForBucket(
                                    lakeSplits != null ? lakeSplits.get(bucket) : null,
                                    tableBucket,
                                    partitionName,
                                    snapshotLogOffset,
                                    stoppingOffset,
                                    snapshotId);
                    if (dvSplit != null) {
                        splits.add(dvSplit);
                    }
                } else {
                    splits.add(
                            generateSplitForPrimaryKeyTableBucket(
                                    lakeSplits != null ? lakeSplits.get(bucket) : null,
                                    tableBucket,
                                    partitionName,
                                    snapshotLogOffset,
                                    stoppingOffset));
                }
            }
        }

        return splits;
    }

    private List<SourceSplitBase> toLakeSnapshotSplits(
            Map<Integer, List<LakeSplit>> lakeSplits,
            @Nullable String partitionName,
            @Nullable Long partitionId) {
        List<SourceSplitBase> splits = new ArrayList<>();
        // we may have multiple table buckets; so we need to
        // introduce an index to make split unique
        int index = 0;
        for (LakeSplit lakeSplit :
                lakeSplits.values().stream().flatMap(List::stream).collect(Collectors.toList())) {
            TableBucket tableBucket =
                    new TableBucket(tableInfo.getTableId(), partitionId, lakeSplit.bucket());
            splits.add(new LakeSnapshotSplit(tableBucket, partitionName, lakeSplit, index++));
        }
        return splits;
    }

    private SourceSplitBase generateSplitForPrimaryKeyTableBucket(
            @Nullable List<LakeSplit> lakeSplits,
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable Long snapshotLogOffset,
            long stoppingOffset) {
        // no snapshot data for this bucket or no a corresponding log offset in this bucket,
        // can only scan from change log
        if (snapshotLogOffset == null || snapshotLogOffset < 0) {
            return new LakeSnapshotAndFlussLogSplit(
                    tableBucket, partitionName, null, EARLIEST_OFFSET, stoppingOffset);
        }

        return new LakeSnapshotAndFlussLogSplit(
                tableBucket, partitionName, lakeSplits, snapshotLogOffset, stoppingOffset);
    }

    /**
     * Builds the split of one bucket of a table with deletion vectors: its files in the snapshot
     * and its log after the snapshot's readable offset, read with masks the split's reader fetches.
     * Returns null for a bucket with nothing tiered and no log.
     */
    @Nullable
    private DvBucketSplit generateDvSplitForBucket(
            @Nullable List<LakeSplit> lakeSplits,
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable Long snapshotLogOffset,
            long stoppingOffset,
            long snapshotId) {
        boolean hasTieredData = snapshotLogOffset != null && snapshotLogOffset >= 0;
        // An empty bucket contributes nothing to a bounded read; a split for it would subscribe
        // the reader to a bucket with no records.
        if (!hasTieredData && stoppingOffset != NO_STOPPING_OFFSET && stoppingOffset <= 0) {
            return null;
        }
        // The lake files of a bucket with nothing tiered in the snapshot are not read: all its
        // rows are still in the log.
        return new DvBucketSplit(
                tableBucket,
                partitionName,
                snapshotId,
                hasTieredData ? snapshotLogOffset : -1L,
                stoppingOffset,
                hasTieredData ? lakeSplits : null);
    }

    private List<SourceSplitBase> generateNoPartitionedTableSplit(
            @Nullable Map<Integer, List<LakeSplit>> lakeSplits,
            boolean isLogTable,
            Map<TableBucket, Long> tableBucketSnapshotLogOffset,
            Map<TableBucket, Long> allStoppingOffsets,
            boolean dvBatchRead,
            long snapshotId) {
        return generateSplit(
                lakeSplits,
                null,
                null,
                bucketCount,
                isLogTable,
                tableBucketSnapshotLogOffset,
                allStoppingOffsets,
                dvBatchRead,
                snapshotId);
    }
}
