/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.tiering.committer;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.tiering.event.RowPosRequestEvent;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.DataDeltaPlan;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.utils.InstantiationUtils;

import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the re-index requests of {@link DerivedDvTieringCommitter}: a rewrite of many files with
 * large lake splits must not make a request event or a split grow past what an RPC frame carries.
 */
class DerivedDvReindexRequestTest {

    private static final TablePath TABLE = TablePath.of("db", "t");
    private static final long TABLE_ID = 7L;
    // a lake split carries the table's schema and partition spec, tens of KB for a wide table
    private static final int LAKE_SPLIT_BYTES = 64 * 1024;

    @TempDir Path tempDir;

    @Test
    void testRequestsStaySmallAndSplitsBounded() throws Exception {
        Configuration flussConfig = new Configuration();
        flussConfig.set(ConfigOptions.REMOTE_DATA_DIR, tempDir.toUri().toString());
        DerivedDvTieringCommitter<Object, Object> committer = committer(flussConfig);
        List<FakeSplit> files = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            files.add(new FakeSplit(null, 0, "b0-" + i));
        }
        for (int i = 0; i < 5; i++) {
            files.add(new FakeSplit(null, 1, "b1-" + i));
        }

        DerivedDvTieringCommitter.Reindex reindex = plan(committer, files, 1000);

        // 300 files of 64 KB would be 19 MB in one event, past a 10 MB RPC frame
        assertThat(reindex.requests).hasSize(1);
        assertThat(InstantiationUtils.serializeObject(reindex.requests.get(0)).length)
                .isLessThan(64 * 1024);
        List<RowPosRequestEvent.Split> splits = reindex.requests.get(0).getSplits();
        int filesPerSplit =
                ConfigOptions.LAKE_TIERING_DELETION_VECTORS_REINDEX_FILES_PER_SPLIT.defaultValue();
        assertThat(splits).hasSize(300 / filesPerSplit + 1 + 1);
        assertThat(reindex.expectedResults).isEqualTo(splits.size());
        Set<Integer> fileIds = new HashSet<>();
        Map<String, Integer> fileIdOfFile = new HashMap<>();
        for (int s = 0; s < splits.size(); s++) {
            RowPosRequestEvent.Split split = splits.get(s);
            assertThat(split.getSplit()).isEqualTo(s);
            assertThat(split.getFileIds().length).isBetween(1, filesPerSplit);
            PositionIndexStore.ReindexPlan plan =
                    PositionIndexStore.readReindexPlan(split.getPlanReference());
            assertThat(plan.lakeSplits).hasSize(split.getFileIds().length);
            for (int i = 0; i < plan.lakeSplits.size(); i++) {
                FakeSplit file =
                        new FakeSplitSerializer()
                                .deserialize(plan.splitSerializerVersion, plan.lakeSplits.get(i));
                assertThat(file.bucket()).isEqualTo(split.getBucket());
                assertThat(fileIds.add(split.getFileIds()[i])).isTrue();
                fileIdOfFile.put(file.fileName(), split.getFileIds()[i]);
            }
        }
        // every file once, with the ids from the first one on
        assertThat(fileIdOfFile).hasSize(305);
        assertThat(fileIds).hasSize(305);
        assertThat(Collections.min(fileIds)).isEqualTo(1000);
        assertThat(Collections.max(fileIds)).isEqualTo(1304);
    }

    @Test
    void testManySplitsAreRequestedInSeveralEvents() throws Exception {
        Configuration flussConfig = new Configuration();
        flussConfig.set(ConfigOptions.REMOTE_DATA_DIR, tempDir.toUri().toString());
        flussConfig.set(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_REINDEX_FILES_PER_SPLIT, 1);
        List<FakeSplit> files = new ArrayList<>();
        int count = DerivedDvTieringCommitter.SPLITS_PER_REQUEST * 2 + 7;
        for (int i = 0; i < count; i++) {
            files.add(new FakeSplit("p" + (i % 13), i % 4, "f" + i));
        }

        DerivedDvTieringCommitter.Reindex reindex = plan(committer(flussConfig), files, 0);

        assertThat(reindex.requests).hasSize(3);
        assertThat(reindex.expectedResults).isEqualTo(count);
        Set<Integer> splitIndexes = new HashSet<>();
        for (RowPosRequestEvent request : reindex.requests) {
            assertThat(request.getTotalSplits()).isEqualTo(count);
            assertThat(request.getSplits().size())
                    .isLessThanOrEqualTo(DerivedDvTieringCommitter.SPLITS_PER_REQUEST);
            for (RowPosRequestEvent.Split split : request.getSplits()) {
                assertThat(split.getPartitionName()).startsWith("p");
                assertThat(split.getPartitionId())
                        .isEqualTo(1000L + Integer.parseInt(split.getPartitionName().substring(1)));
                splitIndexes.add(split.getSplit());
            }
        }
        assertThat(splitIndexes).hasSize(count);
    }

    /**
     * The files of a partition the Fluss table dropped are not re-indexed: no tablet server serves
     * the partition, and the enumerator could not name its buckets.
     */
    @Test
    void testFilesOfDroppedPartitionsAreNotReindexed() {
        List<FakeSplit> files =
                Arrays.asList(
                        new FakeSplit("p1", 0, "kept-0"),
                        new FakeSplit("p2", 0, "dropped-0"),
                        new FakeSplit("p1", 1, "kept-1"),
                        new FakeSplit("p2", 1, "dropped-1"));
        DataDeltaPlan<FakeSplit> plan = new DataDeltaPlan<>(42L, files, Collections.emptyMap());

        DataDeltaPlan<FakeSplit> kept =
                DerivedDvTieringCommitter.withoutFilesOfDroppedPartitions(
                        TABLE, plan, Collections.singletonMap("p1", 1001L));

        assertThat(kept.getCompactSnapshotId()).isEqualTo(42L);
        assertThat(kept.getSplits())
                .extracting(FakeSplit::fileName)
                .containsExactly("kept-0", "kept-1");
        Map<String, Long> all = new HashMap<>();
        all.put("p1", 1001L);
        all.put("p2", 1002L);
        assertThat(DerivedDvTieringCommitter.withoutFilesOfDroppedPartitions(TABLE, plan, all))
                .isSameAs(plan);
    }

    /**
     * Tests that a bucket's files are re-indexed in splits of at most the configured number of
     * rows, as far as their metadata tells, and that a larger file is a split of its own. A rewrite
     * into a few large files thus does not make one split read more rows than a reader should index
     * at once.
     */
    @Test
    void testSplitsAreSizedByRows() throws Exception {
        Configuration flussConfig = new Configuration();
        flussConfig.set(ConfigOptions.REMOTE_DATA_DIR, tempDir.toUri().toString());
        flussConfig.set(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_REINDEX_ROWS_PER_SPLIT, 450L);
        List<FakeSplit> files = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            files.add(new FakeSplit(null, 0, "small-" + i, 100));
        }
        for (int i = 0; i < 3; i++) {
            files.add(new FakeSplit(null, 1, "large-" + i, 1_000));
        }

        DerivedDvTieringCommitter.Reindex reindex = plan(committer(flussConfig), files, 0);

        List<Integer> filesPerSplit = new ArrayList<>();
        for (RowPosRequestEvent request : reindex.requests) {
            for (RowPosRequestEvent.Split split : request.getSplits()) {
                filesPerSplit.add(split.getFileIds().length);
            }
        }
        // bucket 0: 4 + 4 + 2 files of 100 rows; bucket 1: one file of 1000 rows per split
        assertThat(filesPerSplit).containsExactly(4, 4, 2, 1, 1, 1);
    }

    private DerivedDvTieringCommitter.Reindex plan(
            DerivedDvTieringCommitter<Object, Object> committer,
            List<FakeSplit> files,
            int firstFileId)
            throws IOException {
        Map<String, Long> partitionIds = new HashMap<>();
        for (FakeSplit file : files) {
            if (file.partition != null) {
                partitionIds.put(
                        file.partition, 1000L + Integer.parseInt(file.partition.substring(1)));
            }
        }
        return committer.planReindexScan(
                TABLE_ID,
                TABLE,
                new FakeLakeSource(),
                new DataDeltaPlan<>(42L, files, Collections.emptyMap()),
                partitionIds,
                firstFileId,
                new FsPath(tempDir.toUri().toString()),
                41L,
                99L);
    }

    private static DerivedDvTieringCommitter<Object, Object> committer(Configuration flussConfig) {
        return new DerivedDvTieringCommitter<>(
                null,
                flussConfig,
                new Configuration(),
                null,
                null,
                UnregisteredMetricsGroup.createOperatorMetricGroup());
    }

    /** A lake split of one file, serialized with a large payload like a real one. */
    private static final class FakeSplit implements LakeSplit {
        private final String partition;
        private final int bucket;
        private final String file;
        private final long rows;

        private FakeSplit(String partition, int bucket, String file) {
            this(partition, bucket, file, -1);
        }

        private FakeSplit(String partition, int bucket, String file, long rows) {
            this.partition = partition;
            this.bucket = bucket;
            this.file = file;
            this.rows = rows;
        }

        @Override
        public long rowCount() {
            return rows;
        }

        @Override
        public int bucket() {
            return bucket;
        }

        @Override
        public List<String> partition() {
            return partition == null
                    ? Collections.emptyList()
                    : Collections.singletonList(partition);
        }

        @Override
        public String fileName() {
            return file;
        }
    }

    private static final class FakeSplitSerializer implements SimpleVersionedSerializer<FakeSplit> {
        @Override
        public int getVersion() {
            return 3;
        }

        @Override
        public byte[] serialize(FakeSplit split) {
            byte[] partition =
                    (split.partition == null ? "" : split.partition)
                            .getBytes(StandardCharsets.UTF_8);
            byte[] file = split.file.getBytes(StandardCharsets.UTF_8);
            ByteBuffer out =
                    ByteBuffer.allocate(12 + partition.length + file.length + LAKE_SPLIT_BYTES);
            out.putInt(split.bucket).putInt(partition.length).put(partition);
            out.putInt(file.length).put(file);
            return out.array();
        }

        @Override
        public FakeSplit deserialize(int version, byte[] serialized) {
            assertThat(version).isEqualTo(3);
            ByteBuffer in = ByteBuffer.wrap(serialized);
            int bucket = in.getInt();
            byte[] partition = new byte[in.getInt()];
            in.get(partition);
            byte[] file = new byte[in.getInt()];
            in.get(file);
            String p = new String(partition, StandardCharsets.UTF_8);
            return new FakeSplit(
                    p.isEmpty() ? null : p, bucket, new String(file, StandardCharsets.UTF_8));
        }
    }

    private static final class FakeLakeSource implements LakeSource<FakeSplit> {
        @Override
        public void withProject(int[][] project) {}

        @Override
        public void withLimit(int limit) {}

        @Override
        public FilterPushDownResult withFilters(List<Predicate> predicates) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Planner<FakeSplit> createPlanner(PlannerContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RecordReader createRecordReader(ReaderContext<FakeSplit> context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SimpleVersionedSerializer<FakeSplit> getSplitSerializer() {
            return new FakeSplitSerializer();
        }

        @Override
        public LakeSource<FakeSplit> copy() {
            return this;
        }
    }
}
