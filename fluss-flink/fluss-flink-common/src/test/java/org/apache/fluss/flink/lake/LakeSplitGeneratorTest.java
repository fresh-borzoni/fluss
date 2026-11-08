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

import org.apache.fluss.client.admin.DvAdmin;
import org.apache.fluss.client.initializer.NoStoppingOffsetsInitializer;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.table.scanner.batch.dv.DvCutPinner;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.flink.lake.split.DvBucketSplit;
import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
import org.apache.fluss.flink.sink.testutils.TestAdminAdapter;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.TestingLakeSource;
import org.apache.fluss.lake.source.TestingLakeSplit;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.GetDvSnapshotRequest;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests lake and log split planning of {@link LakeSplitGenerator} for partitioned tables and for
 * tables with deletion vectors.
 */
class LakeSplitGeneratorTest {

    /** Table-level bucket count, kept different from the per-partition counts used below. */
    private static final int TABLE_LEVEL_BUCKET_COUNT = 3;

    /**
     * Builds a {@link LakeSplitGenerator} for a partitioned primary-key table (schema: a INT, b
     * STRING, c STRING; PK a+c) whose single partition "p" has {@code partitionBucketCount}
     * enumerated buckets and a single lake split landing in {@code lakeSplitBucket}.
     */
    private static LakeSplitGenerator createGenerator(int partitionBucketCount, int lakeSplitBucket)
            throws Exception {
        TablePath tablePath = TablePath.of("db", "pk_table");
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("b", DataTypes.STRING())
                                        .column("c", DataTypes.STRING())
                                        .primaryKey("a", "c")
                                        .build())
                        .distributedBy(TABLE_LEVEL_BUCKET_COUNT, "a")
                        .partitionedBy("c")
                        .build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, null, 1L, 1L);

        LakeSplit lakeSplit = new TestingLakeSplit(lakeSplitBucket, Collections.singletonList("p"));
        LakeSource<LakeSplit> lakeSource =
                TestingLakeSource.fromSplits(Collections.singletonList(lakeSplit));
        TestAdminAdapter admin =
                new TestAdminAdapter() {
                    @Override
                    public CompletableFuture<LakeSnapshot> getReadableLakeSnapshot(
                            TablePath ignored) {
                        return CompletableFuture.completedFuture(
                                new LakeSnapshot(1L, new HashMap<>()));
                    }
                };
        OffsetsInitializer.BucketOffsetsRetriever retriever = new ZeroOffsetsRetriever();

        // the partition "p" carries its own bucket count (so an out-of-range lake bucket can be
        // detected against the enumerated range [0, partitionBucketCount))
        PartitionInfo partitionInfo =
                new PartitionInfo(
                        7L,
                        ResolvedPartitionSpec.fromPartitionName(tableInfo.getPartitionKeys(), "p"),
                        null,
                        partitionBucketCount);

        return new LakeSplitGenerator(
                tableInfo,
                admin,
                lakeSource,
                retriever,
                OffsetsInitializer.latest(),
                partitionBucketCount,
                () -> Collections.singleton(partitionInfo));
    }

    private static class ZeroOffsetsRetriever implements OffsetsInitializer.BucketOffsetsRetriever {

        @Override
        public Map<Integer, Long> latestOffsets(String partitionName, Collection<Integer> buckets) {
            return zeroOffsets(buckets);
        }

        @Override
        public Map<Integer, Long> earliestOffsets(
                String partitionName, Collection<Integer> buckets) {
            return zeroOffsets(buckets);
        }

        @Override
        public Map<Integer, Long> offsetsFromTimestamp(
                String partitionName, Collection<Integer> buckets, long timestamp) {
            return zeroOffsets(buckets);
        }

        private static Map<Integer, Long> zeroOffsets(Collection<Integer> buckets) {
            Map<Integer, Long> offsets = new HashMap<>();
            for (Integer bucket : buckets) {
                offsets.put(bucket, 0L);
            }
            return offsets;
        }
    }

    @Test
    void testPrimaryKeyOutOfRangeLakeBucketFailsLoud() throws Exception {
        // lake split lands in bucket 5, which is outside [0, 2)
        LakeSplitGenerator generator = createGenerator(2, 5);
        assertThatThrownBy(generator::generateHybridLakeFlussSplits)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outside the enumerated range")
                .hasMessageContaining("refusing to generate union-read splits");
    }

    @Test
    void testPrimaryKeyInRangeLakeBucketSucceeds() throws Exception {
        // lake split lands in bucket 1, which is within [0, 4)
        int partitionBucketCount = 4;
        LakeSplitGenerator generator = createGenerator(partitionBucketCount, 1);

        // no out-of-range bucket: generation succeeds and produces one hybrid lake+log split per
        // bucket of the partition's enumerated range [0, partitionBucketCount)
        List<SourceSplitBase> splits = generator.generateHybridLakeFlussSplits();
        assertThat(partitionBucketCount).isNotEqualTo(TABLE_LEVEL_BUCKET_COUNT);
        assertThat(splits).isNotNull().hasSize(partitionBucketCount);
        for (int bucket = 0; bucket < partitionBucketCount; bucket++) {
            SourceSplitBase split = splits.get(bucket);
            assertThat(split).isInstanceOf(LakeSnapshotAndFlussLogSplit.class);
            assertThat(split.getPartitionName()).isEqualTo("p");
            assertThat(split.getTableBucket().getPartitionId()).isEqualTo(7L);
            assertThat(split.getTableBucket().getBucket()).isEqualTo(bucket);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testPrunedLakePartitionRetainsCommittedLogOffsets(boolean primaryKey) throws Exception {
        TablePath path = TablePath.of("db", "pruned_partition");
        Schema.Builder schema =
                Schema.newBuilder().column("id", DataTypes.INT()).column("day", DataTypes.STRING());
        if (primaryKey) {
            schema.primaryKey("id", "day");
        }
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema.build())
                        .partitionedBy("day")
                        .distributedBy(2, "id")
                        .build();
        TableInfo info = TableInfo.of(path, 1L, 1, descriptor, null, 1L, 1L);
        PartitionInfo partition =
                new PartitionInfo(
                        7L,
                        ResolvedPartitionSpec.fromPartitionName(info.getPartitionKeys(), "p"),
                        null,
                        2);
        TestAdminAdapter admin =
                new TestAdminAdapter() {
                    @Override
                    public CompletableFuture<LakeSnapshot> getReadableLakeSnapshot(
                            TablePath ignored) {
                        // Bucket 0 is tiered through offset 100. Bucket 1 has not been tiered.
                        return CompletableFuture.completedFuture(
                                new LakeSnapshot(
                                        1L,
                                        Collections.singletonMap(
                                                new TableBucket(1L, 7L, 0), 100L)));
                    }
                };
        OffsetsInitializer.BucketOffsetsRetriever retriever =
                new ZeroOffsetsRetriever() {
                    @Override
                    public Map<Integer, Long> latestOffsets(
                            String partitionName, Collection<Integer> buckets) {
                        Map<Integer, Long> offsets = new HashMap<>();
                        offsets.put(0, 110L);
                        offsets.put(1, 20L);
                        return offsets;
                    }
                };
        LakeSplitGenerator generator =
                new LakeSplitGenerator(
                        info,
                        admin,
                        // A pushed-down predicate has pruned every lake split of this partition.
                        TestingLakeSource.fromSplits(Collections.emptyList()),
                        retriever,
                        OffsetsInitializer.latest(),
                        2,
                        () -> Collections.singleton(partition));

        List<SourceSplitBase> splits = generator.generateHybridLakeFlussSplits();

        assertThat(splits).hasSize(2);
        assertThat(splits.get(0).getTableBucket()).isEqualTo(new TableBucket(1L, 7L, 0));
        assertThat(splits.get(1).getTableBucket()).isEqualTo(new TableBucket(1L, 7L, 1));
        assertThat(splits)
                .extracting(
                        split ->
                                primaryKey
                                        ? ((LakeSnapshotAndFlussLogSplit) split).getStartingOffset()
                                        : split.asLogSplit().getStartingOffset())
                .containsExactly(100L, EARLIEST_OFFSET);
        assertThat(splits)
                .extracting(
                        split ->
                                primaryKey
                                        ? ((LakeSnapshotAndFlussLogSplit) split)
                                                .getStoppingOffset()
                                                .get()
                                        : split.asLogSplit().getStoppingOffset().get())
                .containsExactly(110L, 20L);
    }

    private static final long TABLE_ID = 100L;
    private static final TablePath TABLE_PATH = TablePath.of("test_db", "test_table");
    private static final int BUCKET_COUNT = 3;

    // ---- Non-partitioned PK table with DV enabled ----

    /** Planning pins one cut per bucket without asking for masks. The splits carry only the cut. */
    @Test
    void testPlanningPinsOneCutPerBucketWithoutMasks() throws Exception {
        Map<TableBucket, Long> lakeOffsets = new HashMap<>();
        Map<Integer, Long> stoppingOffsets = new HashMap<>();
        for (int b = 0; b < BUCKET_COUNT; b++) {
            lakeOffsets.put(new TableBucket(TABLE_ID, b), 100L * (b + 1));
            stoppingOffsets.put(b, 100L * (b + 1) + 50);
        }
        TestAdmin admin = new TestAdmin(lakeOffsets, stoppingOffsets);

        List<SourceSplitBase> splits =
                createGenerator(true, false, admin, stoppingOffsets)
                        .generateHybridLakeFlussSplits();

        assertThat(admin.dvRequests)
                .hasSize(BUCKET_COUNT)
                .allSatisfy(
                        request -> {
                            assertThat(request.hasPinRequestId()).isTrue();
                            assertThat(request.hasLogEndOffset()).isFalse();
                            assertThat(request.getLakeDvFileNamesCount()).isZero();
                            assertThat(request.hasLogDvFromOffset()).isFalse();
                        });
        assertThat(splits).hasSize(BUCKET_COUNT);
        for (int b = 0; b < BUCKET_COUNT; b++) {
            DvBucketSplit split = findDvSplitForBucket(splits, b);
            assertThat(split.isPinned()).isTrue();
            assertThat(split.sharesBucket()).isFalse();
            assertThat(split.getMaskLogEndOffset()).isEqualTo(TestAdmin.cutOf(b));
            assertThat(split.getPinnedLogStartOffset()).isEqualTo(100L * (b + 1));
            assertThat(split.getLakeSplits()).hasSize(1);
            assertThat(split.readsLog()).isTrue();
            assertThat(split.getMaskState()).isNull();
        }
    }

    /** Pins go out in parallel, at most {@link DvCutPinner#MAX_CONCURRENT_PINS} at a time. */
    @Test
    void testPlanningPinsBucketsWithBoundedConcurrency() throws Exception {
        int buckets = 256;
        Map<TableBucket, Long> lakeOffsets = new HashMap<>();
        Map<Integer, Long> stoppingOffsets = new HashMap<>();
        for (int b = 0; b < buckets; b++) {
            lakeOffsets.put(new TableBucket(TABLE_ID, b), 1_000L);
            stoppingOffsets.put(b, 5_000_000L);
        }
        TestAdmin admin = new TestAdmin(lakeOffsets, stoppingOffsets);
        admin.responseDelayMs = 20;
        TableInfo tableInfo = createTableInfo(true, false);
        List<PartitionInfo> partitions =
                Collections.singletonList(
                        new PartitionInfo(
                                0L,
                                new ResolvedPartitionSpec(
                                        Collections.emptyList(), Collections.emptyList()),
                                null,
                                buckets));
        LakeSplitGenerator generator =
                new LakeSplitGenerator(
                        tableInfo,
                        admin,
                        new TestingLakeSource(buckets, partitions),
                        new ZeroOffsetsRetriever(),
                        (partitionName, bucketIds, retriever) -> stoppingOffsets,
                        buckets,
                        Collections::emptySet);

        List<SourceSplitBase> splits = generator.generateHybridLakeFlussSplits();

        assertThat(splits)
                .hasSize(buckets)
                .allMatch(split -> split instanceof DvBucketSplit)
                .allMatch(split -> ((DvBucketSplit) split).isPinned());
        assertThat(admin.dvRequests).hasSize(buckets);
        assertThat(admin.maxInFlight.get())
                .isGreaterThan(1)
                .isLessThanOrEqualTo(DvCutPinner.MAX_CONCURRENT_PINS);
        assertThat(admin.lakeSnapshotRequests).isEqualTo(1);
    }

    /**
     * A pinned bucket with more rows than a split reads is split into groups of its lake files and
     * a log split, all with the bucket's cut.
     */
    @Test
    void testPinnedBucketSplitsIntoFileGroupsAndLog() throws Exception {
        Map<TableBucket, Long> lakeOffsets = new HashMap<>();
        lakeOffsets.put(new TableBucket(TABLE_ID, 0), 100L);
        lakeOffsets.put(new TableBucket(TABLE_ID, 1), 200L);
        Map<Integer, Long> stoppingOffsets = new HashMap<>();
        stoppingOffsets.put(0, 150L);
        stoppingOffsets.put(1, 250L);
        stoppingOffsets.put(2, 0L);
        TestAdmin admin = new TestAdmin(lakeOffsets, stoppingOffsets);
        List<LakeSplit> lakeSplits = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            lakeSplits.add(rowsSplit(0, 3));
        }
        lakeSplits.add(rowsSplit(1, 3));
        LakeSplitGenerator generator =
                new LakeSplitGenerator(
                        createTableInfo(true, false),
                        admin,
                        TestingLakeSource.fromSplits(lakeSplits),
                        new ZeroOffsetsRetriever(),
                        (partitionName, bucketIds, retriever) -> stoppingOffsets,
                        BUCKET_COUNT,
                        Collections::emptySet);
        generator.setFileGroupRows(6);

        List<SourceSplitBase> splits = generator.generateHybridLakeFlussSplits();

        List<DvBucketSplit> bucket0 = new ArrayList<>();
        for (SourceSplitBase split : splits) {
            if (split.getTableBucket().getBucket() == 0) {
                bucket0.add((DvBucketSplit) split);
            }
        }
        // 5 files of 3 rows in groups of 6 rows, and the log
        assertThat(bucket0).hasSize(4);
        assertThat(bucket0)
                .allSatisfy(
                        split -> {
                            assertThat(split.sharesBucket()).isTrue();
                            assertThat(split.getMaskLogEndOffset()).isEqualTo(TestAdmin.cutOf(0));
                        });
        assertThat(bucket0).extracting(DvBucketSplit::lakeSplitCount).containsExactly(2, 2, 1, 0);
        assertThat(bucket0)
                .extracting(DvBucketSplit::readsLog)
                .containsExactly(false, false, false, true);
        assertThat(bucket0).extracting(SourceSplitBase::splitId).doesNotHaveDuplicates();
        DvBucketSplit bucket1 = findDvSplitForBucket(splits, 1);
        assertThat(bucket1.sharesBucket()).isFalse();
        assertThat(bucket1.lakeSplitCount()).isEqualTo(1);
        assertThat(splits).noneMatch(split -> split.getTableBucket().getBucket() == 2);
    }

    /**
     * A bucket's cut takes a hold per split, which the enumerator releases as each split is read:
     * the file groups and the log of a split bucket, one for a bucket read by one split.
     */
    @Test
    void testPinnedCutTakesAHoldPerSplit() throws Exception {
        Map<TableBucket, Long> lakeOffsets = new HashMap<>();
        lakeOffsets.put(new TableBucket(TABLE_ID, 0), 100L);
        lakeOffsets.put(new TableBucket(TABLE_ID, 1), 200L);
        Map<Integer, Long> stoppingOffsets = new HashMap<>();
        stoppingOffsets.put(0, 150L);
        stoppingOffsets.put(1, 250L);
        stoppingOffsets.put(2, 0L);
        TestAdmin admin = new TestAdmin(lakeOffsets, stoppingOffsets);
        List<LakeSplit> lakeSplits = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            lakeSplits.add(rowsSplit(0, 3));
        }
        lakeSplits.add(rowsSplit(1, 3));
        LakeSplitGenerator generator =
                new LakeSplitGenerator(
                        createTableInfo(true, false),
                        admin,
                        TestingLakeSource.fromSplits(lakeSplits),
                        new ZeroOffsetsRetriever(),
                        (partitionName, bucketIds, retriever) -> stoppingOffsets,
                        BUCKET_COUNT,
                        Collections::emptySet);
        generator.setFileGroupRows(6);

        List<SourceSplitBase> splits = generator.generateHybridLakeFlussSplits();

        assertThat(splits.stream().filter(split -> split.getTableBucket().getBucket() == 0))
                .hasSize(4);
        assertThat(admin.dvRequests)
                .filteredOn(request -> request.getBucketId() == 0)
                .singleElement()
                .satisfies(pin -> assertThat(pin.getHolds()).isEqualTo(4));
        assertThat(admin.dvRequests)
                .filteredOn(request -> request.getBucketId() == 1)
                .singleElement()
                .satisfies(pin -> assertThat(pin.hasHolds()).isFalse());
    }

    /**
     * A bucket with more lake files than one split reads, whose cut could not be pinned, is read by
     * one split that carries none of its files, so its assignment stays small: its reader plans
     * them when it starts.
     */
    @Test
    void testUnpinnedBucketOfManyFilesCarriesNone() throws Exception {
        Map<TableBucket, Long> lakeOffsets = new HashMap<>();
        lakeOffsets.put(new TableBucket(TABLE_ID, 0), 100L);
        lakeOffsets.put(new TableBucket(TABLE_ID, 1), 200L);
        Map<Integer, Long> stoppingOffsets = new HashMap<>();
        stoppingOffsets.put(0, 150L);
        stoppingOffsets.put(1, 250L);
        stoppingOffsets.put(2, 0L);
        TestAdmin admin = new TestAdmin(lakeOffsets, stoppingOffsets);
        admin.stalePins.add(0);
        admin.stalePins.add(1);
        List<LakeSplit> lakeSplits = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            lakeSplits.add(rowsSplit(0, 3));
        }
        lakeSplits.add(rowsSplit(1, 3));
        LakeSplitGenerator generator =
                new LakeSplitGenerator(
                        createTableInfo(true, false),
                        admin,
                        TestingLakeSource.fromSplits(lakeSplits),
                        new ZeroOffsetsRetriever(),
                        (partitionName, bucketIds, retriever) -> stoppingOffsets,
                        BUCKET_COUNT,
                        Collections::emptySet);
        generator.setFileGroupRows(6);

        List<SourceSplitBase> splits = generator.generateHybridLakeFlussSplits();

        DvBucketSplit many = findDvSplitForBucket(splits, 0);
        assertThat(splits.stream().filter(split -> split.getTableBucket().getBucket() == 0))
                .hasSize(1);
        assertThat(many.isPinned()).isFalse();
        assertThat(many.getLakeSplits()).isNull();
        assertThat(many.isLakeSplitsPlannedAtStart()).isTrue();
        assertThat(many.getSnapshotLogOffset()).isEqualTo(100L);
        // a bucket of one split's files keeps them
        DvBucketSplit few = findDvSplitForBucket(splits, 1);
        assertThat(few.isPinned()).isFalse();
        assertThat(few.getLakeSplits()).hasSize(1);
    }

    private static LakeSplit rowsSplit(int bucket, long rows) {
        return new TestingLakeSplit(bucket, Collections.emptyList()) {
            @Override
            public long rowCount() {
                return rows;
            }
        };
    }

    /**
     * A bucket with nothing tiered reads its log from offset 0 without lake files. A bucket with no
     * log either has no split.
     */
    @Test
    void testDvBucketsWithoutTieredData() throws Exception {
        Map<TableBucket, Long> lakeOffsets = new HashMap<>();
        lakeOffsets.put(new TableBucket(TABLE_ID, 0), 100L);
        Map<Integer, Long> stoppingOffsets = new HashMap<>();
        stoppingOffsets.put(0, 100L);
        stoppingOffsets.put(1, 30L);
        stoppingOffsets.put(2, 0L);
        TestAdmin admin = new TestAdmin(lakeOffsets, stoppingOffsets);

        List<SourceSplitBase> splits =
                createGenerator(true, false, admin, stoppingOffsets)
                        .generateHybridLakeFlussSplits();

        assertThat(splits).hasSize(2);
        DvBucketSplit tiered = findDvSplitForBucket(splits, 0);
        assertThat(tiered.getLakeSplits()).hasSize(1);
        assertThat(tiered.getSnapshotLogOffset()).isEqualTo(100L);
        DvBucketSplit logOnly = findDvSplitForBucket(splits, 1);
        assertThat(logOnly.getLakeSplits()).isNull();
        assertThat(logOnly.getSnapshotLogOffset()).isEqualTo(-1L);
        assertThat(logOnly.getStoppingOffset()).isEqualTo(30L);
        assertThat(splits).noneMatch(split -> split.getTableBucket().getBucket() == 2);
        // planning pins cuts, but asks for no masks
        assertThat(admin.dvRequests)
                .allSatisfy(request -> assertThat(request.getLakeDvFileNamesCount()).isZero());
    }

    /** Nothing tiered yet: the read is planned on the empty lake, its whole log masked. */
    @Test
    void testDvReadOfEmptyLake() throws Exception {
        Map<Integer, Long> stoppingOffsets = new HashMap<>();
        stoppingOffsets.put(0, 10L);
        stoppingOffsets.put(1, 0L);
        stoppingOffsets.put(2, 20L);
        TestAdmin admin = new TestAdmin(Collections.emptyMap(), stoppingOffsets);
        admin.noLakeSnapshot = true;

        List<SourceSplitBase> splits =
                createGenerator(true, false, admin, stoppingOffsets)
                        .generateHybridLakeFlussSplits();

        assertThat(splits).hasSize(2).allMatch(split -> split instanceof DvBucketSplit);
        for (SourceSplitBase split : splits) {
            DvBucketSplit dvSplit = (DvBucketSplit) split;
            assertThat(dvSplit.getSnapshotId()).isEqualTo(-1L);
            assertThat(dvSplit.getLakeSplits()).isNull();
        }
        // planning pins cuts, but asks for no masks
        assertThat(admin.dvRequests)
                .allSatisfy(request -> assertThat(request.getLakeDvFileNamesCount()).isZero());
    }

    @Test
    void testStreamingReadOfDvTableFallsBackToFlussSplits() throws Exception {
        Map<TableBucket, Long> lakeOffsets = new HashMap<>();
        lakeOffsets.put(new TableBucket(TABLE_ID, 0), 100L);
        TestAdmin admin = new TestAdmin(lakeOffsets, Collections.emptyMap());

        LakeSplitGenerator generator =
                createGenerator(true, admin, new NoStoppingOffsetsInitializer());

        // Masks serve bounded reads only; a streaming read uses the KV snapshot and changelog.
        assertThat(generator.generateHybridLakeFlussSplits()).isNull();
        assertThat(admin.lakeSnapshotRequests).isZero();
        assertThat(admin.dvRequests).isEmpty();

        // Without deletion vectors a streaming read still gets hybrid lake and log splits.
        LakeSplitGenerator noDv = createGenerator(false, admin, new NoStoppingOffsetsInitializer());
        assertThat(noDv.generateHybridLakeFlussSplits()).isNotNull();
    }

    @Test
    void testDvDisabled() throws Exception {
        Map<TableBucket, Long> lakeOffsets = new HashMap<>();
        lakeOffsets.put(new TableBucket(TABLE_ID, 0), 100L);

        Map<Integer, Long> stoppingOffsets = new HashMap<>();
        stoppingOffsets.put(0, 200L);
        stoppingOffsets.put(1, 200L);
        stoppingOffsets.put(2, 200L);

        TestAdmin admin = new TestAdmin(lakeOffsets, stoppingOffsets);

        LakeSplitGenerator generator = createGenerator(false, false, admin, stoppingOffsets);
        List<SourceSplitBase> splits = generator.generateHybridLakeFlussSplits();

        assertThat(splits).isNotNull();
        assertThat(splits).noneMatch(split -> split instanceof DvBucketSplit);
        assertThat(admin.dvRequests).isEmpty();
    }

    // ---- Partitioned PK table with DV enabled ----

    @Test
    void testDvEnabledPartitionedTable() throws Exception {
        long partition1Id = 1001L;
        long partition2Id = 1002L;

        Map<TableBucket, Long> lakeOffsets = new HashMap<>();
        lakeOffsets.put(new TableBucket(TABLE_ID, partition1Id, 0), 100L);
        lakeOffsets.put(new TableBucket(TABLE_ID, partition1Id, 1), 200L);
        lakeOffsets.put(new TableBucket(TABLE_ID, partition2Id, 0), 50L);
        lakeOffsets.put(new TableBucket(TABLE_ID, partition2Id, 1), 60L);

        Map<Integer, Long> stoppingOffsetsP1 = new HashMap<>();
        stoppingOffsetsP1.put(0, 150L); // gap
        stoppingOffsetsP1.put(1, 200L); // no gap
        stoppingOffsetsP1.put(2, 0L); // no lake data

        Map<Integer, Long> stoppingOffsetsP2 = new HashMap<>();
        stoppingOffsetsP2.put(0, 100L); // gap
        stoppingOffsetsP2.put(1, 60L); // no gap
        stoppingOffsetsP2.put(2, 0L); // no lake data

        List<PartitionInfo> partitionInfos = new ArrayList<>();
        partitionInfos.add(
                new PartitionInfo(
                        partition1Id,
                        new ResolvedPartitionSpec(
                                Collections.singletonList("dt"),
                                Collections.singletonList("2025-01-01")),
                        null,
                        BUCKET_COUNT));
        partitionInfos.add(
                new PartitionInfo(
                        partition2Id,
                        new ResolvedPartitionSpec(
                                Collections.singletonList("dt"),
                                Collections.singletonList("2025-01-02")),
                        null,
                        BUCKET_COUNT));

        TestAdmin admin =
                new TestAdmin(lakeOffsets, null) {
                    @Override
                    protected Map<Integer, Long> getStoppingOffsetsForPartition(
                            String partitionName) {
                        if ("2025-01-01".equals(partitionName)) {
                            return stoppingOffsetsP1;
                        } else if ("2025-01-02".equals(partitionName)) {
                            return stoppingOffsetsP2;
                        }
                        return Collections.emptyMap();
                    }
                };

        LakeSplitGenerator generator =
                createPartitionedGenerator(true, admin, partitionInfos, BUCKET_COUNT);
        List<SourceSplitBase> splits = generator.generateHybridLakeFlussSplits();

        // planning pins cuts, but asks for no masks
        assertThat(admin.dvRequests)
                .allSatisfy(request -> assertThat(request.getLakeDvFileNamesCount()).isZero());
        // a split per bucket with tiered data; bucket 2 of each partition has nothing to read
        assertThat(splits).hasSize(4).allMatch(split -> split instanceof DvBucketSplit);
        DvBucketSplit p1b0 = (DvBucketSplit) only(splitsOf(splits, partition1Id, 0));
        assertThat(p1b0.getPartitionName()).isEqualTo("2025-01-01");
        assertThat(p1b0.getSnapshotLogOffset()).isEqualTo(100L);
        assertThat(p1b0.getStoppingOffset()).isEqualTo(150L);
        assertThat(p1b0.getLakeSplits())
                .singleElement()
                .satisfies(
                        lakeSplit ->
                                assertThat(lakeSplit.partition()).containsExactly("2025-01-01"));
        DvBucketSplit p2b1 = (DvBucketSplit) only(splitsOf(splits, partition2Id, 1));
        assertThat(p2b1.getSnapshotLogOffset()).isEqualTo(60L);
        assertThat(p2b1.getStoppingOffset()).isEqualTo(60L);
        assertThat(splitsOf(splits, partition1Id, 2)).isEmpty();
        assertThat(splitsOf(splits, partition2Id, 2)).isEmpty();
    }

    /**
     * The lake files of a partition dropped from Fluss but still in the readable snapshot are read
     * as lake-only splits. The live partition reads with masks.
     */
    @Test
    void testDvEnabledPartitionedTableWithDroppedPartition() throws Exception {
        long droppedId = 1000L;
        long liveId = 1001L;
        Map<TableBucket, Long> lakeOffsets = new HashMap<>();
        lakeOffsets.put(new TableBucket(TABLE_ID, droppedId, 0), 100L);
        lakeOffsets.put(new TableBucket(TABLE_ID, droppedId, 1), 80L);
        lakeOffsets.put(new TableBucket(TABLE_ID, liveId, 0), 50L);
        lakeOffsets.put(new TableBucket(TABLE_ID, liveId, 1), 60L);
        Map<Integer, Long> liveStoppingOffsets = new HashMap<>();
        liveStoppingOffsets.put(0, 100L); // gap
        liveStoppingOffsets.put(1, 60L); // no gap
        liveStoppingOffsets.put(2, 0L); // no lake data
        PartitionInfo dropped =
                new PartitionInfo(
                        droppedId,
                        new ResolvedPartitionSpec(
                                Collections.singletonList("dt"),
                                Collections.singletonList("2025-01-01")),
                        null,
                        BUCKET_COUNT);
        PartitionInfo live =
                new PartitionInfo(
                        liveId,
                        new ResolvedPartitionSpec(
                                Collections.singletonList("dt"),
                                Collections.singletonList("2025-01-02")),
                        null,
                        BUCKET_COUNT);
        TestAdmin admin =
                new TestAdmin(lakeOffsets, null) {
                    @Override
                    protected Map<Integer, Long> getStoppingOffsetsForPartition(
                            String partitionName) {
                        return "2025-01-02".equals(partitionName)
                                ? liveStoppingOffsets
                                : Collections.emptyMap();
                    }
                };
        OffsetsInitializer stoppingInitializer =
                (partitionName, buckets, retriever) ->
                        admin.getStoppingOffsetsForPartition(partitionName);
        // The lake still has both partitions; Fluss only the live one.
        LakeSplitGenerator generator =
                new LakeSplitGenerator(
                        createTableInfo(true, true),
                        admin,
                        new TestingLakeSource(BUCKET_COUNT, Arrays.asList(dropped, live)),
                        new ZeroOffsetsRetriever(),
                        stoppingInitializer,
                        BUCKET_COUNT,
                        () -> Collections.singleton(live));

        List<SourceSplitBase> splits = generator.generateHybridLakeFlussSplits();

        // planning pins cuts, but asks for no masks
        assertThat(admin.dvRequests)
                .allSatisfy(request -> assertThat(request.getLakeDvFileNamesCount()).isZero());
        List<SourceSplitBase> droppedSplits = new ArrayList<>();
        for (SourceSplitBase split : splits) {
            if ("2025-01-01".equals(split.getPartitionName())) {
                droppedSplits.add(split);
            }
        }
        assertThat(droppedSplits)
                .isNotEmpty()
                .allSatisfy(split -> assertThat(split).isInstanceOf(LakeSnapshotSplit.class));
        DvBucketSplit live0 = (DvBucketSplit) only(splitsOf(splits, liveId, 0));
        assertThat(live0.getSnapshotLogOffset()).isEqualTo(50L);
        assertThat(live0.getStoppingOffset()).isEqualTo(100L);
        assertThat(live0.getLakeSplits()).hasSize(1);
        assertThat(splitsOf(splits, liveId, 1)).singleElement().isInstanceOf(DvBucketSplit.class);
        assertThat(splitsOf(splits, liveId, 2)).isEmpty();
    }

    // ---- Helper methods ----

    private static List<SourceSplitBase> splitsOf(
            List<SourceSplitBase> splits, long partitionId, int bucketId) {
        List<SourceSplitBase> out = new ArrayList<>();
        for (SourceSplitBase split : splits) {
            TableBucket tb = split.getTableBucket();
            if (tb.getBucket() == bucketId
                    && tb.getPartitionId() != null
                    && tb.getPartitionId() == partitionId) {
                out.add(split);
            }
        }
        return out;
    }

    private static SourceSplitBase only(List<SourceSplitBase> splits) {
        assertThat(splits).hasSize(1);
        return splits.get(0);
    }

    private static DvBucketSplit findDvSplitForBucket(List<SourceSplitBase> splits, int bucketId) {
        for (SourceSplitBase split : splits) {
            if (split.getTableBucket().getBucket() == bucketId && split instanceof DvBucketSplit) {
                return (DvBucketSplit) split;
            }
        }
        throw new IllegalStateException("No DvBucketSplit found for bucket " + bucketId);
    }

    private LakeSplitGenerator createGenerator(
            boolean dvEnabled,
            boolean isPartitioned,
            TestAdmin admin,
            Map<Integer, Long> stoppingOffsets) {
        return createGenerator(
                dvEnabled,
                isPartitioned,
                admin,
                stoppingOffsets,
                (partitionName, buckets, retriever) -> stoppingOffsets);
    }

    private LakeSplitGenerator createGenerator(
            boolean dvEnabled, TestAdmin admin, OffsetsInitializer stoppingInitializer) {
        return createGenerator(
                dvEnabled, false, admin, Collections.emptyMap(), stoppingInitializer);
    }

    private LakeSplitGenerator createGenerator(
            boolean dvEnabled,
            boolean isPartitioned,
            TestAdmin admin,
            Map<Integer, Long> stoppingOffsets,
            OffsetsInitializer stoppingInitializer) {
        TableInfo tableInfo = createTableInfo(dvEnabled, isPartitioned);
        List<PartitionInfo> partitions =
                Collections.singletonList(
                        new PartitionInfo(
                                0L,
                                new ResolvedPartitionSpec(
                                        Collections.emptyList(), Collections.emptyList()),
                                null,
                                BUCKET_COUNT));
        TestingLakeSource lakeSource = new TestingLakeSource(BUCKET_COUNT, partitions);

        OffsetsInitializer.BucketOffsetsRetriever retriever =
                new OffsetsInitializer.BucketOffsetsRetriever() {
                    @Override
                    public Map<Integer, Long> latestOffsets(
                            String partitionName, Collection<Integer> buckets) {
                        return stoppingOffsets;
                    }

                    @Override
                    public Map<Integer, Long> earliestOffsets(
                            String partitionName, Collection<Integer> buckets) {
                        return Collections.emptyMap();
                    }

                    @Override
                    public Map<Integer, Long> offsetsFromTimestamp(
                            String partitionName, Collection<Integer> buckets, long timestamp) {
                        return Collections.emptyMap();
                    }
                };

        return new LakeSplitGenerator(
                tableInfo,
                admin,
                lakeSource,
                retriever,
                stoppingInitializer,
                BUCKET_COUNT,
                () -> Collections.emptySet());
    }

    private LakeSplitGenerator createPartitionedGenerator(
            boolean dvEnabled,
            TestAdmin admin,
            List<PartitionInfo> partitionInfos,
            int bucketCount) {
        TableInfo tableInfo = createTableInfo(dvEnabled, true);
        TestingLakeSource lakeSource = new TestingLakeSource(bucketCount, partitionInfos);

        OffsetsInitializer stoppingInitializer =
                (partitionName, buckets, retriever) ->
                        admin.getStoppingOffsetsForPartition(partitionName);

        OffsetsInitializer.BucketOffsetsRetriever retriever =
                new OffsetsInitializer.BucketOffsetsRetriever() {
                    @Override
                    public Map<Integer, Long> latestOffsets(
                            String partitionName, Collection<Integer> buckets) {
                        return admin.getStoppingOffsetsForPartition(partitionName);
                    }

                    @Override
                    public Map<Integer, Long> earliestOffsets(
                            String partitionName, Collection<Integer> buckets) {
                        return Collections.emptyMap();
                    }

                    @Override
                    public Map<Integer, Long> offsetsFromTimestamp(
                            String partitionName, Collection<Integer> buckets, long timestamp) {
                        return Collections.emptyMap();
                    }
                };

        return new LakeSplitGenerator(
                tableInfo,
                admin,
                lakeSource,
                retriever,
                stoppingInitializer,
                bucketCount,
                () -> new HashSet<>(partitionInfos));
    }

    private static TableInfo createTableInfo(boolean dvEnabled, boolean isPartitioned) {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING());
        if (isPartitioned) {
            schemaBuilder.column("dt", DataTypes.STRING()).primaryKey("id", "dt");
        } else {
            schemaBuilder.primaryKey("id");
        }

        TableDescriptor.Builder descriptorBuilder =
                TableDescriptor.builder()
                        .schema(schemaBuilder.build())
                        .distributedBy(BUCKET_COUNT, "id")
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED, dvEnabled);
        if (isPartitioned) {
            descriptorBuilder.partitionedBy("dt");
        }

        return TableInfo.of(
                TABLE_PATH,
                TABLE_ID,
                0,
                descriptorBuilder.build(),
                null,
                System.currentTimeMillis(),
                System.currentTimeMillis());
    }

    /** A mock Admin that answers mask requests as a server that pins cuts or does not. */
    private static class TestAdmin extends TestAdminAdapter implements DvAdmin {
        private final Map<TableBucket, Long> lakeOffsets;
        @Nullable private final Map<Integer, Long> defaultStoppingOffsets;
        final List<GetDvSnapshotRequest> dvRequests =
                java.util.Collections.synchronizedList(new ArrayList<>());
        int lakeSnapshotRequests;
        // nothing tiered yet
        boolean noLakeSnapshot;
        // the buckets whose leaders cover another snapshot, so their cuts cannot be pinned
        final java.util.Set<Integer> stalePins = new java.util.HashSet<>();
        // a server that pins cuts; otherwise it answers every request with all masks
        long responseDelayMs;
        final java.util.concurrent.atomic.AtomicInteger inFlight =
                new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger maxInFlight =
                new java.util.concurrent.atomic.AtomicInteger();

        TestAdmin(
                Map<TableBucket, Long> lakeOffsets,
                @Nullable Map<Integer, Long> defaultStoppingOffsets) {
            this.lakeOffsets = lakeOffsets;
            this.defaultStoppingOffsets = defaultStoppingOffsets;
        }

        static long cutOf(int bucket) {
            return 10_000L + bucket;
        }

        protected Map<Integer, Long> getStoppingOffsetsForPartition(String partitionName) {
            return defaultStoppingOffsets != null ? defaultStoppingOffsets : Collections.emptyMap();
        }

        @Override
        public CompletableFuture<LakeSnapshot> getReadableLakeSnapshot(TablePath tablePath) {
            lakeSnapshotRequests++;
            if (noLakeSnapshot) {
                CompletableFuture<LakeSnapshot> none = new CompletableFuture<>();
                none.completeExceptionally(new LakeTableSnapshotNotExistException("none"));
                return none;
            }
            return CompletableFuture.completedFuture(new LakeSnapshot(1L, lakeOffsets));
        }

        @Override
        public CompletableFuture<GetDvSnapshotResponse> getDvSnapshot(
                TablePath tablePath, GetDvSnapshotRequest request) {
            dvRequests.add(request);
            if (stalePins.contains(request.getBucketId())) {
                CompletableFuture<GetDvSnapshotResponse> stale = new CompletableFuture<>();
                stale.completeExceptionally(
                        new StaleSnapshotException(2L, request.getReadableSnapshotId()));
                return stale;
            }
            TableBucket bucket =
                    new TableBucket(
                            request.getTableId(),
                            request.hasPartitionId() ? request.getPartitionId() : null,
                            request.getBucketId());
            GetDvSnapshotResponse response =
                    new GetDvSnapshotResponse()
                            .setLogEndOffset(cutOf(request.getBucketId()))
                            .setSnapshotStartOffset(lakeOffsets.getOrDefault(bucket, 0L));
            int now = inFlight.incrementAndGet();
            maxInFlight.accumulateAndGet(now, Math::max);
            return CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            Thread.sleep(responseDelayMs);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        inFlight.decrementAndGet();
                        return response;
                    });
        }

        @Override
        public CompletableFuture<GetDvSnapshotResponse> lookupDvPositions(
                TablePath tablePath,
                long tableId,
                @Nullable Long partitionId,
                int bucketId,
                long snapshotId,
                List<byte[]> keys) {
            throw new UnsupportedOperationException("lookupDvPositions");
        }
    }
}
