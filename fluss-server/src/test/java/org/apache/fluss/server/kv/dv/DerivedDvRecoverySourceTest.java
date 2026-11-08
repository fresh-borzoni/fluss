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

package org.apache.fluss.server.kv.dv;

import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.remote.RemoteLogSegment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static org.apache.fluss.testutils.DataTestUtils.genLogRecordsWithBaseOffsetAndTimestamp;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests {@link DerivedDvRecoverySource}. */
class DerivedDvRecoverySourceTest {

    @TempDir Path tempDir;

    /** Downloads a previous leadership of the bucket left behind, e.g. on a crash, are removed. */
    @Test
    void testRemovesTheDownloadsOfAPreviousLeadership() throws Exception {
        File localTmp = tempDir.resolve("log").resolve("derived-dv-recovery").toFile();
        Path leftover = localTmp.toPath().resolve("5-12345").resolve("sst_0.sst");
        Files.createDirectories(leftover.getParent());
        Files.write(leftover, new byte[] {1, 2, 3});

        new DerivedDvRecoverySource(
                tempDir.resolve("remote").toUri().toString(),
                DATA1_TABLE_INFO_PK,
                new TableBucket(DATA1_TABLE_INFO_PK.getTableId(), 0),
                null,
                null,
                null,
                null,
                localTmp,
                1,
                1,
                new DerivedDvMetadataCache());

        assertThat(leftover).doesNotExist();
        assertThat(localTmp).doesNotExist();
    }

    /** The directory a change was downloaded into is removed once its files are ingested. */
    @Test
    void testRemovesTheDownloadDirectoriesOfIngestedChanges() throws Exception {
        String remoteDataDir = tempDir.resolve("remote").toUri().toString();
        TableBucket bucket = new TableBucket(DATA1_TABLE_INFO_PK.getTableId(), 0);
        PositionIndexStore writer =
                PositionIndexStore.forTable(
                        remoteDataDir,
                        DATA1_TABLE_INFO_PK.getTablePath(),
                        DATA1_TABLE_INFO_PK.getTableId(),
                        null);
        for (int change = 1; change <= 3; change++) {
            byte[] key = ("k" + change).getBytes(StandardCharsets.UTF_8);
            writer.commitChange(
                    change,
                    Collections.singletonMap(
                            bucket,
                            new PositionIndexStore.BucketChange(
                                    writer.writeBucket(
                                            change,
                                            bucket,
                                            Collections.<Map.Entry<byte[], FilePos>>singletonList(
                                                            new AbstractMap.SimpleEntry<>(
                                                                    key, new FilePos(change, 0)))
                                                    .iterator()),
                                    Collections.singletonMap(change, "f" + change),
                                    Collections.emptyList())));
            writer.register(change == 1 ? -1 : 100 * (change - 1), 100 * change, change);
        }
        File localTmp = tempDir.resolve("log").resolve("derived-dv-recovery").toFile();
        DerivedDvRecoverySource source =
                new DerivedDvRecoverySource(
                        remoteDataDir,
                        DATA1_TABLE_INFO_PK,
                        bucket,
                        null,
                        null,
                        null,
                        null,
                        localTmp,
                        1,
                        1,
                        new DerivedDvMetadataCache());
        List<PositionIndexStore.ChainEntry> chain = source.chain();

        PositionIndexStore.BucketData first = source.readChange(chain.get(0));
        File firstDir = new File(first.localSsts.get(0)).getParentFile();
        PositionIndexStore.BucketData second = source.readChange(chain.get(1));
        File secondDir = new File(second.localSsts.get(0)).getParentFile();
        // the first change is ingested (its files moved), the second one is not yet
        Files.delete(Paths.get(first.localSsts.get(0)));

        PositionIndexStore.BucketData third = source.readChange(chain.get(2));
        assertThat(firstDir).doesNotExist();
        assertThat(secondDir).isDirectory();
        assertThat(new File(third.localSsts.get(0))).isFile();
    }

    /**
     * The chain listing is cached per readable registration. After a trim drops changes a bucket
     * that was idle still needs to read, a read of one fails; the next plan lists the chain afresh,
     * rather than retrying with the listing from before the trim until the next registration.
     */
    @Test
    void testAFailedReadListsTheChainAfresh() throws Exception {
        String remoteDataDir = tempDir.resolve("remote").toUri().toString();
        TableBucket bucket = new TableBucket(DATA1_TABLE_INFO_PK.getTableId(), 0);
        PositionIndexStore writer =
                PositionIndexStore.forTable(
                        remoteDataDir,
                        DATA1_TABLE_INFO_PK.getTablePath(),
                        DATA1_TABLE_INFO_PK.getTableId(),
                        null);
        for (int change = 1; change <= 3; change++) {
            byte[] key = ("k" + change).getBytes(StandardCharsets.UTF_8);
            writer.commitChange(
                    change,
                    Collections.singletonMap(
                            bucket,
                            new PositionIndexStore.BucketChange(
                                    writer.writeBucket(
                                            change,
                                            bucket,
                                            Collections.<Map.Entry<byte[], FilePos>>singletonList(
                                                            new AbstractMap.SimpleEntry<>(
                                                                    key, new FilePos(change, 0)))
                                                    .iterator()),
                                    Collections.singletonMap(change, "f" + change),
                                    Collections.emptyList())));
            writer.register(change == 1 ? -1 : 100 * (change - 1), 100 * change, change);
        }
        DerivedDvMetadataCache cache = new DerivedDvMetadataCache();
        DerivedDvRecoverySource source =
                new DerivedDvRecoverySource(
                        remoteDataDir,
                        DATA1_TABLE_INFO_PK,
                        bucket,
                        null,
                        null,
                        null,
                        null,
                        tempDir.resolve("log").resolve("derived-dv-recovery").toFile(),
                        1,
                        1,
                        cache);
        // the registration of snapshot 300 was read, so the listing is cached under it
        source.useRegistration("offsets-300");
        List<PositionIndexStore.ChainEntry> before = source.chain();
        assertThat(before).hasSize(3);

        // the tiering service trims the chain: the bucket's changes before the last are dropped
        assertThat(writer.trim(Collections.singletonList(bucket), 0)).isPositive();
        List<PositionIndexStore.ChainEntry> trimmed = writer.chain();
        assertThat(trimmed).isNotEqualTo(before);
        assertThat(source.chain()).isEqualTo(before);
        assertThatThrownBy(() -> source.readChange(before.get(0))).isInstanceOf(IOException.class);

        // the next plan lists the chain as it is now
        assertThat(source.chain()).isEqualTo(trimmed);
    }

    /**
     * Below the local log, the write time is bounded from remote log metadata: the max timestamp of
     * the latest segment that ends at or before the offset, by its physical end, as a clipped
     * suffix may hold later batches. With no such segment, no bound is known and the read fails.
     */
    @Test
    void testTheRemoteWriteTimeBoundIsTheLastSegmentEndingBeforeTheOffset() {
        TableBucket bucket = new TableBucket(DATA1_TABLE_INFO_PK.getTableId(), 0);
        List<RemoteLogSegment> segments =
                Arrays.asList(
                        segment(bucket, 0, 100, 1_000),
                        segment(bucket, 100, 200, 2_000),
                        // overlaps the next one: physically up to 300, logically up to 250
                        segment(bucket, 200, 300, 3_000).withLogicalRange(200, 250),
                        segment(bucket, 250, 400, 4_000));

        assertThat(DerivedDvRecoverySource.remoteWriteTimeBound(segments, bucket, 100))
                .isEqualTo(1_000L);
        assertThat(DerivedDvRecoverySource.remoteWriteTimeBound(segments, bucket, 199))
                .isEqualTo(1_000L);
        assertThat(DerivedDvRecoverySource.remoteWriteTimeBound(segments, bucket, 260))
                .isEqualTo(2_000L);
        assertThat(DerivedDvRecoverySource.remoteWriteTimeBound(segments, bucket, 300))
                .isEqualTo(3_000L);
        assertThatThrownBy(() -> DerivedDvRecoverySource.remoteWriteTimeBound(segments, bucket, 50))
                .isInstanceOf(IllegalStateException.class);
    }

    /**
     * In the local log, the write time is the commit time of the batch holding the offset. No batch
     * holding it, or a negative time, fails rather than read as "nothing written yet".
     */
    @Test
    void testTheLocalWriteTimeIsTheCommitTimeOfTheBatchHoldingTheOffset() throws Exception {
        TableBucket bucket = new TableBucket(DATA1_TABLE_INFO_PK.getTableId(), 0);
        int n = DATA1.size();
        List<LogRecordBatch> batches = Arrays.asList(batch(0, 1_000), batch(n, 2_000));

        assertThat(DerivedDvRecoverySource.writeTimeOf(batches, bucket, 0)).isEqualTo(1_000L);
        assertThat(DerivedDvRecoverySource.writeTimeOf(batches, bucket, n - 1)).isEqualTo(1_000L);
        assertThat(DerivedDvRecoverySource.writeTimeOf(batches, bucket, n)).isEqualTo(2_000L);
        assertThatThrownBy(() -> DerivedDvRecoverySource.writeTimeOf(batches, bucket, 2L * n))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(
                        () ->
                                DerivedDvRecoverySource.writeTimeOf(
                                        Collections.singletonList(batch(0, -5)), bucket, 0))
                .isInstanceOf(IllegalStateException.class);
    }

    private static RemoteLogSegment segment(TableBucket bucket, long start, long end, long time) {
        return RemoteLogSegment.Builder.builder()
                .remoteLogSegmentId(UUID.randomUUID())
                .physicalTablePath(PhysicalTablePath.of(DATA1_TABLE_INFO_PK.getTablePath()))
                .tableBucket(bucket)
                .remoteLogStartOffset(start)
                .remoteLogEndOffset(end)
                .maxTimestamp(time)
                .segmentSizeInBytes(1)
                .build();
    }

    private static LogRecordBatch batch(long baseOffset, long commitTime) throws Exception {
        return genLogRecordsWithBaseOffsetAndTimestamp(baseOffset, commitTime, DATA1)
                .batches()
                .iterator()
                .next();
    }
}
