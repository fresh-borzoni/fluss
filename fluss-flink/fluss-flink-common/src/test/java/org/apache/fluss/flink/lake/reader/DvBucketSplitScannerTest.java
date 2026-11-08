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

package org.apache.fluss.flink.lake.reader;

import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketMaskState;
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketMasks;
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketPart;
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketPart.ReadMode;
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketPartReader;
import org.apache.fluss.client.table.scanner.batch.dv.DvMaskFetcher;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.exception.DeletionVectorsNotReadyException;
import org.apache.fluss.exception.LakeTableRolledBackException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.flink.lake.LakeRecordRecordEmitter;
import org.apache.fluss.flink.lake.split.DvBucketSplit;
import org.apache.fluss.flink.lake.state.DvBucketSplitState;
import org.apache.fluss.flink.sink.testutils.TestAdminAdapter;
import org.apache.fluss.flink.source.reader.RecordAndPos;
import org.apache.fluss.flink.source.split.SourceSplitSerializer;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.RowWithPosResult;
import org.apache.fluss.lake.source.SortedRecordReader;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.messages.GetDvSnapshotRequest;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests of {@link DvBucketSplitScanner}, which fetches a bucket's masks when its split starts. */
class DvBucketSplitScannerTest {

    private static final TablePath TABLE_PATH = TablePath.of("db", "t");
    private static final long TABLE_ID = 1L;
    private static final TableBucket BUCKET = new TableBucket(TABLE_ID, 0);
    private static final long SNAPSHOT = 5L;

    private final MaskServer server = new MaskServer();
    private final FileLakeSource lake = new FileLakeSource();
    private final FakeLogScanner log = new FakeLogScanner();
    private final TestContext context = new TestContext();

    /**
     * The bucket of every test: two lake files of keys 0..5 tiered up to offset 100, and a log tail
     * that updates key 1, deletes key 4, inserts key 6 and updates key 6 again.
     */
    private void standardBucket() {
        lake.plan(
                SNAPSHOT,
                new FileSplit("f0.parquet", new int[] {0, 1, 2}, "a"),
                new FileSplit("f1.parquet", new int[] {3, 4, 5}, "a"));
        log.append(100, ChangeType.UPDATE_BEFORE, 1, "a");
        log.append(101, ChangeType.UPDATE_AFTER, 1, "b");
        log.append(102, ChangeType.DELETE, 4, "a");
        log.append(103, ChangeType.INSERT, 6, "a");
        log.append(104, ChangeType.UPDATE_BEFORE, 6, "a");
        log.append(105, ChangeType.UPDATE_AFTER, 6, "b");
        // written after the masks were taken
        log.append(106, ChangeType.UPDATE_AFTER, 0, "late");
        server.answer = request -> masks(SNAPSHOT, 100, 106);
    }

    private static final List<String> EXACT =
            Arrays.asList("0=a", "1=b", "2=a", "3=a", "5=a", "6=b");

    /** The masks of the standard bucket: keys 1 and 4 changed, offset 103 superseded. */
    private static GetDvSnapshotResponse masks(long snapshotId, long start, long cut) {
        GetDvSnapshotResponse response =
                new GetDvSnapshotResponse().setLogEndOffset(cut).setSnapshotStartOffset(start);
        response.addLakeDvEntry()
                .setFilePath("s3://bucket/data/f0.parquet")
                .setDeletedPositionsBitmap(bitmap(1));
        response.addLakeDvEntry().setFilePath("f1.parquet").setDeletedPositionsBitmap(bitmap(1));
        response.setLogDvBitmap(bitmap(103));
        return response;
    }

    @Test
    void testMaskedReadIsExactAndStopsAtTheCut() throws Exception {
        standardBucket();

        Read read = read(split(), Integer.MAX_VALUE);

        assertThat(read.rows).containsExactlyInAnyOrderElementsOf(EXACT);
        assertThat(read.state.getReadMode()).isEqualTo(ReadMode.MASKED);
        DvBucketSplit checkpointed = (DvBucketSplit) read.state.toSourceSplit();
        assertThat(checkpointed.getMaskLogEndOffset()).isEqualTo(106L);
        assertThat(checkpointed.getNextLogOffset()).isEqualTo(106L);
        // one request: the masks of a server that does not page come whole
        assertThat(server.requests).hasSize(1);
        assertThat(server.requests.get(0).isPaged()).isTrue();
        assertThat(server.requests.get(0).hasLogEndOffset()).isFalse();
        assertThat(log.subscribedFrom).isEqualTo(100L);
    }

    /** Masks larger than an assignment RPC can carry are fetched by the reader and applied. */
    @Test
    void testMasksOverEightMegabytes() throws Exception {
        standardBucket();
        GetDvSnapshotResponse large = masks(SNAPSHOT, 100, 106);
        // other files of the bucket, each half masked
        Roaring64Bitmap halfMasked = new Roaring64Bitmap();
        for (long pos = 0; pos < 65536; pos += 2) {
            halfMasked.addLong(pos);
        }
        byte[] halfMaskedBytes = serialize(halfMasked);
        int files = (int) ((9L << 20) / halfMaskedBytes.length) + 1;
        for (int i = 0; i < files; i++) {
            large.addLakeDvEntry()
                    .setFilePath("other-" + i + ".parquet")
                    .setDeletedPositionsBitmap(halfMaskedBytes);
        }
        assertThat(large.totalSize()).isGreaterThan(8 << 20);
        server.answer = request -> large;

        Read read = read(split(), Integer.MAX_VALUE);

        assertThat(read.rows).containsExactlyInAnyOrderElementsOf(EXACT);
        // only the masks of the split's own files are kept
        DvBucketSplit checkpointed = (DvBucketSplit) read(split(), 2).state.toSourceSplit();
        assertThat(checkpointed.getMaskState().getLakeMasks())
                .containsOnlyKeys("f0.parquet", "f1.parquet");
    }

    /**
     * A started split keeps masks over 8 MB in its state and restores exactly from it. The state
     * goes to checkpoint storage, not through an assignment RPC.
     */
    @Test
    void testStartedSplitKeepsLargeMasksInItsState() throws Exception {
        standardBucket();
        Roaring64Bitmap logMask = new Roaring64Bitmap();
        logMask.addLong(103);
        // superseded offsets far past the cut, never read
        for (long offset = 1L << 32; offset < (1L << 32) + (1L << 27); offset += 2) {
            logMask.addLong(offset);
        }
        GetDvSnapshotResponse large = masks(SNAPSHOT, 100, 106).setLogDvBitmap(serialize(logMask));
        server.answer = request -> large;

        Read first = read(split(), 2);
        DvBucketSplit checkpointed = (DvBucketSplit) first.state.toSourceSplit();
        assertThat(serializedSize(checkpointed)).isGreaterThan(8 << 20);
        server.requests.clear();
        Read rest = read(roundTrip(checkpointed), Integer.MAX_VALUE);

        List<String> all = new ArrayList<>(first.rows);
        all.addAll(rest.rows);
        assertThat(all).containsExactlyInAnyOrderElementsOf(EXACT);
        assertThat(server.requests).isEmpty();
    }

    /** A planned split carries no masks, and a checkpoint only those the rest of the read needs. */
    @Test
    void testSplitStateSizeIsBounded() throws Exception {
        standardBucket();
        DvBucketSplit planned = split();
        assertThat(planned.getMaskState()).isNull();

        // resolved but nothing emitted: no masks kept, a restore fetches them again
        DvBucketSplit resolvedOnly = (DvBucketSplit) resolveOnly(planned).toSourceSplit();
        assertThat(resolvedOnly.isStarted()).isFalse();
        assertThat(resolvedOnly.getMaskState()).isNull();
        assertThat(serializedSize(resolvedOnly) - serializedSize(planned)).isLessThan(64);

        // in the lake phase: the masks of the files still to read and the log mask
        DvBucketSplit inLake = (DvBucketSplit) read(planned, 3).state.toSourceSplit();
        assertThat(inLake.getCurrentLakeSplitIndex()).isEqualTo(1);
        assertThat(inLake.getMaskState().getLakeMasks()).containsOnlyKeys("f1.parquet");
        assertThat(inLake.getMaskState().getLogMask()).isNotNull();
        assertThat((long) serializedSize(inLake) - serializedSize(planned))
                .isLessThan(256 + inLake.getMaskState().maskBytes());

        // in the log phase: the log mask only
        DvBucketSplit inLog = (DvBucketSplit) read(planned, 5).state.toSourceSplit();
        assertThat(inLog.getNextLogOffset()).isEqualTo(102L);
        assertThat(inLog.getMaskState().getLakeMasks()).isEmpty();
        assertThat(inLog.getMaskState().maskBytes())
                .isEqualTo(inLog.getMaskState().getLogMask().length);
    }

    /**
     * A split restored mid-read uses the masks its state kept, although the server now serves
     * another cut. Without them it could not resume exactly.
     */
    @Test
    void testRestoreReadsWithKeptMasksWhenTheServerMovedOn() throws Exception {
        standardBucket();
        Read first = read(split(), 2);
        DvBucketSplit checkpointed = (DvBucketSplit) first.state.toSourceSplit();
        assertThat(checkpointed.getMaskState().getLogEndOffset()).isEqualTo(106L);
        checkpointed = roundTrip(checkpointed);

        // new writes moved the high watermark, and the server now refuses cut 106
        log.append(107, ChangeType.UPDATE_AFTER, 2, "later");
        server.answer = request -> masks(SNAPSHOT, 100, 108);
        server.failures.add(new StaleSnapshotException(SNAPSHOT + 1, SNAPSHOT));
        server.requests.clear();
        Read rest = read(checkpointed, Integer.MAX_VALUE);

        List<String> all = new ArrayList<>(first.rows);
        all.addAll(rest.rows);
        assertThat(all).containsExactlyInAnyOrderElementsOf(EXACT);
        assertThat(server.requests).isEmpty();
    }

    /** A split restored before it emitted anything fetches its masks again, at a new cut. */
    @Test
    void testRestoreOfUnstartedSplitRefetches() throws Exception {
        standardBucket();
        DvBucketSplit checkpointed = (DvBucketSplit) resolveOnly(split()).toSourceSplit();
        log.append(107, ChangeType.UPDATE_AFTER, 2, "later");
        // the masks of cut 108: keys 0, 1 and 2 changed in the lake's first file
        server.answer =
                request -> {
                    GetDvSnapshotResponse response = masks(SNAPSHOT, 100, 108);
                    response.clearLakeDvEntries();
                    response.addLakeDvEntry()
                            .setFilePath("f0.parquet")
                            .setDeletedPositionsBitmap(bitmap(0, 1, 2));
                    response.addLakeDvEntry()
                            .setFilePath("f1.parquet")
                            .setDeletedPositionsBitmap(bitmap(1));
                    return response;
                };
        server.requests.clear();

        Read read = read(roundTrip(checkpointed), Integer.MAX_VALUE);

        assertThat(server.requests).hasSize(1);
        assertThat(server.requests.get(0).hasLogEndOffset()).isFalse();
        assertThat(read.rows)
                .containsExactlyInAnyOrder("0=late", "1=b", "2=later", "3=a", "5=a", "6=b");
    }

    /** After a restore, kept masks are not applied to a snapshot id that a rollback reused. */
    @Test
    void testRestoreChecksTheKeptSnapshotIdentity() throws Exception {
        standardBucket();
        Function<GetDvSnapshotRequest, GetDvSnapshotResponse> base = server.answer;
        server.answer = request -> base.apply(request).setLakeSnapshotIdentity("commit-a");
        lake.identities.put(SNAPSHOT, "commit-a");
        DvBucketSplit checkpointed = (DvBucketSplit) read(split(), 2).state.toSourceSplit();
        assertThat(checkpointed.getMaskState().getLakeSnapshotIdentity()).isEqualTo("commit-a");

        lake.identities.put(SNAPSHOT, "commit-b");
        assertThatThrownBy(() -> read(roundTrip(checkpointed), Integer.MAX_VALUE))
                .isInstanceOf(LakeTableRolledBackException.class)
                .hasMessageContaining("commit-b");
    }

    /** A reader that fails mid-read resumes with masks of the same cut and stays exact. */
    @Test
    void testRestoreWithoutKeptMasksPinsTheCut() throws Exception {
        standardBucket();
        Read first = read(split(), 2);
        // as restored from a state without masks
        DvBucketSplit checkpointed =
                ((DvBucketSplit) first.state.toSourceSplit()).withMaskState(null);
        assertThat(checkpointed.getCurrentLakeSplitIndex()).isZero();
        assertThat(checkpointed.getRecordsToSkip()).isEqualTo(2L);

        server.requests.clear();
        Read rest = read(checkpointed, Integer.MAX_VALUE);

        List<String> all = new ArrayList<>(first.rows);
        all.addAll(rest.rows);
        assertThat(all).containsExactlyInAnyOrderElementsOf(EXACT);
        assertThat(server.requests.get(0).getLogEndOffset()).isEqualTo(106L);
    }

    /** A restore in the log phase goes on after the last emitted log row. */
    @Test
    void testRestoreInTheLogPhase() throws Exception {
        standardBucket();
        // the lake rows and key 1 from offset 101
        Read first = read(split(), 5);
        DvBucketSplit checkpointed = (DvBucketSplit) first.state.toSourceSplit();
        assertThat(checkpointed.getNextLogOffset()).isEqualTo(102L);

        server.requests.clear();
        Read rest = read(roundTrip(checkpointed), Integer.MAX_VALUE);

        List<String> all = new ArrayList<>(first.rows);
        all.addAll(rest.rows);
        assertThat(all).containsExactlyInAnyOrderElementsOf(EXACT);
        assertThat(log.subscribedFrom).isEqualTo(102L);
        assertThat(server.requests).isEmpty();
    }

    /** A restored read fails rather than lose or repeat rows with the masks of another cut. */
    @Test
    void testRestoreWithoutKeptMasksRefusesAnotherCut() throws Exception {
        standardBucket();
        DvBucketSplit checkpointed =
                ((DvBucketSplit) read(split(), 2).state.toSourceSplit()).withMaskState(null);
        // the leader moved on and does not serve the old cut
        server.answer = request -> masks(SNAPSHOT, 100, 107);

        assertThatThrownBy(() -> read(checkpointed, Integer.MAX_VALUE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot resume the union read")
                .hasMessageContaining("up to log offset 106");

        server.answer = request -> masks(SNAPSHOT, 100, 106);
        server.failures.add(new StaleSnapshotException(SNAPSHOT + 1, SNAPSHOT));
        assertThatThrownBy(() -> read(checkpointed, Integer.MAX_VALUE))
                .hasMessageContaining("Cannot resume the union read");
    }

    /** The split start waits while a catching-up leader answers not ready. */
    @Test
    void testNotReadyAtSplitStartIsWaitedFor() throws Exception {
        standardBucket();
        for (int i = 0; i < 6; i++) {
            server.failures.add(new DeletionVectorsNotReadyException("catching up"));
        }

        Read read = read(split(), Integer.MAX_VALUE);

        assertThat(read.rows).containsExactlyInAnyOrderElementsOf(EXACT);
        assertThat(server.requests).hasSize(7);
        assertThat(context.sleeps).hasSize(6);
    }

    /** A leader that covers a newer snapshot has the bucket read on that snapshot. */
    @Test
    void testStaleSnapshotAtSplitStartReplansTheBucket() throws Exception {
        standardBucket();
        long newer = 9L;
        // tiering wrote key 1 = b into a new file of snapshot 9, readable from offset 102
        lake.plan(
                newer,
                new FileSplit("f0.parquet", new int[] {0, 1, 2}, "a"),
                new FileSplit("f1.parquet", new int[] {3, 4, 5}, "a"),
                new FileSplit("f2.parquet", new int[] {1}, "b"));
        server.lakeSnapshots.put(
                newer, new LakeSnapshot(newer, Collections.singletonMap(BUCKET, 102L)));
        server.failures.add(new StaleSnapshotException(newer, SNAPSHOT));
        server.answer =
                request -> {
                    assertThat(request.getReadableSnapshotId()).isEqualTo(newer);
                    GetDvSnapshotResponse response = masks(newer, 102, 106);
                    // key 4 is deleted in the log, key 1 is live in f2
                    response.clearLakeDvEntries();
                    response.addLakeDvEntry()
                            .setFilePath("f0.parquet")
                            .setDeletedPositionsBitmap(bitmap(1));
                    response.addLakeDvEntry()
                            .setFilePath("f1.parquet")
                            .setDeletedPositionsBitmap(bitmap(1));
                    return response;
                };

        Read read = read(split(), Integer.MAX_VALUE);

        assertThat(read.rows).containsExactlyInAnyOrderElementsOf(EXACT);
        DvBucketSplit resolved = (DvBucketSplit) read.state.toSourceSplit();
        assertThat(resolved.getSnapshotId()).isEqualTo(newer);
        assertThat(resolved.getSnapshotLogOffset()).isEqualTo(102L);
        assertThat(resolved.getLakeSplits()).hasSize(3);
        assertThat(log.subscribedFrom).isEqualTo(102L);
    }

    /**
     * Tiering keeps registering snapshots while the read waits for a bucket that catches up. The
     * bucket is read on the snapshot its leader covers when it answers, whatever the other buckets
     * cover: a union read is consistent per bucket, so no bucket waits for another.
     */
    @Test
    void testBucketThatKeepsMovingIsReadOnTheSnapshotItCovers() throws Exception {
        standardBucket();
        for (long newer : new long[] {9L, 10L}) {
            // tiering wrote key 1 = b into a new file, readable from offset 102
            lake.plan(
                    newer,
                    new FileSplit("f0.parquet", new int[] {0, 1, 2}, "a"),
                    new FileSplit("f1.parquet", new int[] {3, 4, 5}, "a"),
                    new FileSplit("f2.parquet", new int[] {1}, "b"));
            server.lakeSnapshots.put(
                    newer, new LakeSnapshot(newer, Collections.singletonMap(BUCKET, 102L)));
        }
        server.failures.add(new DeletionVectorsNotReadyException("catching up"));
        server.failures.add(new DeletionVectorsNotReadyException("catching up"));
        // the leader moved past the planned snapshot, and once more after the replan
        server.failures.add(new StaleSnapshotException(9L, SNAPSHOT));
        server.failures.add(new StaleSnapshotException(10L, 9L));
        server.answer =
                request -> {
                    assertThat(request.getReadableSnapshotId()).isEqualTo(10L);
                    GetDvSnapshotResponse response = masks(10L, 102, 106);
                    response.clearLakeDvEntries();
                    response.addLakeDvEntry()
                            .setFilePath("f0.parquet")
                            .setDeletedPositionsBitmap(bitmap(1));
                    response.addLakeDvEntry()
                            .setFilePath("f1.parquet")
                            .setDeletedPositionsBitmap(bitmap(1));
                    return response;
                };

        Read read = read(split(), Integer.MAX_VALUE);

        assertThat(read.rows).containsExactlyInAnyOrderElementsOf(EXACT);
        DvBucketSplit resolved = (DvBucketSplit) read.state.toSourceSplit();
        assertThat(resolved.getSnapshotId()).isEqualTo(10L);
        assertThat(log.subscribedFrom).isEqualTo(102L);
        assertThat(server.requests).hasSize(5);
    }

    /** When the masks stay unavailable, a lake that reads in key order is merged with the log. */
    @Test
    void testUnavailableMasksFallBackToMerge() throws Exception {
        standardBucket();
        server.readable = new LakeSnapshot(SNAPSHOT, Collections.singletonMap(BUCKET, 100L));
        for (int i = 0; i < 10; i++) {
            server.failures.add(new IllegalStateException("Failed to get DV snapshot: overflow"));
        }

        assertThatThrownBy(() -> read(split(), Integer.MAX_VALUE))
                .hasStackTraceContaining("Failed to get DV snapshot");

        server.failures.clear();
        for (int i = 0; i < 10; i++) {
            server.failures.add(new IllegalStateException("Failed to get DV snapshot: overflow"));
        }
        lake.sorted = true;
        Read read = read(split(), Integer.MAX_VALUE);
        assertThat(read.state.getReadMode()).isEqualTo(ReadMode.MERGED);
        assertThat(context.merged).hasSize(1);
        assertThat(context.merged.get(0).getSnapshotLogOffset()).isEqualTo(100L);
        assertThat(context.merged.get(0).getStoppingOffset()).isEqualTo(106L);
    }

    /** A partition dropped before its split starts has its lake files read unmasked. */
    @Test
    void testDroppedPartitionAtSplitStartReadsTheLakeOnly() throws Exception {
        standardBucket();
        server.failures.add(new PartitionNotExistException("dropped"));

        Read read = read(split(), Integer.MAX_VALUE);

        assertThat(read.state.getReadMode()).isEqualTo(ReadMode.LAKE_ONLY);
        assertThat(read.rows).containsExactlyInAnyOrder("0=a", "1=a", "2=a", "3=a", "4=a", "5=a");
        assertThat(log.subscribedFrom).isNull();
    }

    /** The read fails rather than lose rows when the log that masked the lake rows is gone. */
    @Test
    void testPartitionDroppedDuringTheLogFails() throws Exception {
        standardBucket();
        TableBucket partitioned = new TableBucket(TABLE_ID, 7L, 0);
        log.bucket = partitioned;
        log.dropped = true;
        context.partitionDropped = true;
        DvBucketSplit split =
                new DvBucketSplit(partitioned, "p", SNAPSHOT, 100L, 106L, lake.plans.get(SNAPSHOT));

        assertThatThrownBy(() -> read(split, Integer.MAX_VALUE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("was dropped while a union read was reading the log");
    }

    /** A log-only bucket whose partition is gone before any row was read ends without rows. */
    @Test
    void testPartitionDroppedBeforeAnyRowEndsTheSplit() throws Exception {
        standardBucket();
        log.subscribeFails = true;
        DvBucketSplit logOnly = new DvBucketSplit(BUCKET, null, SNAPSHOT, -1L, 106L, null);

        Read read = read(logOnly, Integer.MAX_VALUE);

        assertThat(read.rows).isEmpty();
    }

    /** Masks for a reused snapshot id do not apply to the commit it names now. */
    @Test
    void testReusedSnapshotIdFailsTheRead() throws Exception {
        standardBucket();
        Function<GetDvSnapshotRequest, GetDvSnapshotResponse> base = server.answer;
        server.answer = request -> base.apply(request).setLakeSnapshotIdentity("commit-a");

        lake.identities.put(SNAPSHOT, "commit-a");
        assertThat(read(split(), Integer.MAX_VALUE).rows)
                .containsExactlyInAnyOrderElementsOf(EXACT);

        lake.identities.put(SNAPSHOT, "commit-b");
        assertThatThrownBy(() -> read(split(), Integer.MAX_VALUE))
                .isInstanceOf(LakeTableRolledBackException.class)
                .hasMessageContaining("commit-b");
    }

    /** A server that pages answers each part of the masks of one pinned cut. */
    @Test
    void testPagedMasks() throws Exception {
        standardBucket();
        GetDvSnapshotResponse all = masks(SNAPSHOT, 100, 106);
        server.answer = request -> page(all, request);

        Read read = read(split(), Integer.MAX_VALUE);

        assertThat(read.rows).containsExactlyInAnyOrderElementsOf(EXACT);
        // the first request takes the cut; the next ones pin it
        assertThat(server.requests.get(0).hasLogEndOffset()).isFalse();
        assertThat(server.requests.subList(1, server.requests.size()))
                .isNotEmpty()
                .allSatisfy(request -> assertThat(request.getLogEndOffset()).isEqualTo(106L));
        // all masks of the cut at once, the log in pages of at most two offsets: the leader may
        // drop the cut while the split is read
        assertThat(server.requests.get(0).getLakeDvFileNamesList()).containsExactly("f0.parquet");
        assertThat(server.requests.get(1).getLakeDvFileNamesList())
                .containsExactly("f0.parquet", "f1.parquet");
        assertThat(server.requests)
                .filteredOn(GetDvSnapshotRequest::hasLogDvFromOffset)
                .extracting(GetDvSnapshotRequest::getLogDvFromOffset)
                .containsExactly(100L, 102L, 104L);
        assertThat(((DvBucketSplit) read.state.toSourceSplit()).getMaskState()).isNotNull();
    }

    /**
     * The splits of a bucket that share a pinned cut read the masks of that cut while writes move
     * the high watermark. Together they read the bucket as of the cut.
     */
    @Test
    void testSplitsOfAPinnedBucketReadOneCutWhileWritesContinue() throws Exception {
        standardBucket();
        PinningServer pins = new PinningServer();
        long cut = pins.pin();
        assertThat(cut).isEqualTo(107L);
        List<LakeSplit> files = lake.plans.get(SNAPSHOT);
        DvBucketSplit group0 = pinnedSplit(files.subList(0, 1), 0, cut);
        DvBucketSplit group1 = pinnedSplit(files.subList(1, 2), 1, cut);
        DvBucketSplit logSplit = pinnedSplit(null, DvBucketSplit.BUCKET_LOG, cut);

        List<String> rows = new ArrayList<>(read(group0, Integer.MAX_VALUE).rows);
        // keys 3 and 5 of the second group change after the cut
        log.append(107, ChangeType.UPDATE_AFTER, 3, "later");
        log.append(108, ChangeType.DELETE, 5, "x");
        rows.addAll(read(group1, Integer.MAX_VALUE).rows);
        log.append(109, ChangeType.UPDATE_AFTER, 1, "later");
        rows.addAll(read(logSplit, Integer.MAX_VALUE).rows);

        assertThat(rows).containsExactlyInAnyOrder("0=late", "1=b", "2=a", "3=a", "5=a", "6=b");
        assertThat(server.requests)
                .allSatisfy(request -> assertThat(request.getLogEndOffset()).isEqualTo(cut));
        // the file groups asked for their own files' masks only, the log split for log pages
        assertThat(server.requests)
                .filteredOn(request -> request.getLakeDvFileNamesCount() > 0)
                .extracting(GetDvSnapshotRequest::getLakeDvFileNamesList)
                .containsExactly(
                        Collections.singletonList("f0.parquet"),
                        Collections.singletonList("f1.parquet"));
        assertThat(server.requests)
                .filteredOn(GetDvSnapshotRequest::hasLogDvFromOffset)
                .isNotEmpty();
    }

    /**
     * Splits whose pinned cut the leader lost, for example on a leader change, derive the masks of
     * that cut from the keys. Together with the splits that fetched the masks earlier, they read
     * the bucket as of the cut.
     */
    @Test
    void testSplitsDeriveTheMasksOfALostCut() throws Exception {
        standardBucket();
        PinningServer pins = new PinningServer();
        long cut = pins.pin();
        List<LakeSplit> files = lake.plans.get(SNAPSHOT);
        DvBucketSplit group0 = pinnedSplit(files.subList(0, 1), 0, cut);
        DvBucketSplit group1 = pinnedSplit(files.subList(1, 2), 1, cut);
        DvBucketSplit logSplit = pinnedSplit(null, DvBucketSplit.BUCKET_LOG, cut);

        Read fetched = read(group0, Integer.MAX_VALUE);
        assertThat(context.keyReads).isEmpty();
        assertThat(context.sleeps).isEmpty();
        pins.cuts.clear();
        log.append(107, ChangeType.UPDATE_AFTER, 3, "later");
        log.append(108, ChangeType.DELETE, 2, "x");
        Read derivedGroup = read(group1, Integer.MAX_VALUE);
        Read derivedLog = read(logSplit, Integer.MAX_VALUE);

        List<String> rows = new ArrayList<>(fetched.rows);
        rows.addAll(derivedGroup.rows);
        rows.addAll(derivedLog.rows);
        assertThat(rows).containsExactlyInAnyOrder("0=late", "1=b", "2=a", "3=a", "5=a", "6=b");
        assertThat(context.keyReads).containsExactly("log", "f1.parquet", "log");
        // a lost cut is not asked for again
        assertThat(context.sleeps).isEmpty();
        // the derived masks are those the leader served for the cut, kept for a restore
        GetDvSnapshotResponse served = pins.masksAt(cut);
        DvBucketMaskState group1Masks =
                ((DvBucketSplit) derivedGroup.state.toSourceSplit()).getMaskState();
        assertThat(group1Masks.getLogEndOffset()).isEqualTo(cut);
        assertThat(bitmapOf(group1Masks.getLakeMasks().get("f1.parquet")))
                .isEqualTo(bitmapOf(DvBucketMasks.lakeMasksOf(served).get("f1.parquet")));
        DvBucketMaskState logMasks =
                ((DvBucketSplit) derivedLog.state.toSourceSplit()).getMaskState();
        assertThat(bitmapOf(logMasks.getLogMask())).isEqualTo(bitmapOf(served.getLogDvBitmap()));
    }

    /** A split that cannot derive the masks of its lost cut fails rather than read another. */
    @Test
    void testLostCutFailsASplitThatCannotDeriveIt() throws Exception {
        standardBucket();
        PinningServer pins = new PinningServer();
        long cut = pins.pin();
        pins.cuts.clear();
        log.subscribeFails = true;

        DvBucketSplit group0 = pinnedSplit(lake.plans.get(SNAPSHOT).subList(0, 1), 0, cut);
        assertThatThrownBy(() -> read(group0, Integer.MAX_VALUE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("is no longer served")
                .hasMessageContaining("could not derive its masks");
    }

    private static Roaring64Bitmap bitmapOf(byte[] bytes) throws IOException {
        return DvBucketMasks.deserialize(bytes, "a test mask");
    }

    /** A split that reads its whole bucket resolves afresh when its pinned cut is lost. */
    @Test
    void testLostCutOfAWholeBucketSplitResolvesAfresh() throws Exception {
        standardBucket();
        PinningServer pins = new PinningServer();
        long cut = pins.pin();
        pins.cuts.clear();
        log.append(107, ChangeType.UPDATE_AFTER, 3, "later");

        Read read =
                read(
                        pinnedSplit(lake.plans.get(SNAPSHOT), DvBucketSplit.WHOLE_BUCKET, cut),
                        Integer.MAX_VALUE);

        // as of the new cut, after both late writes
        assertThat(read.rows)
                .containsExactlyInAnyOrder("0=late", "1=b", "2=a", "3=later", "5=a", "6=b");
        assertThat(((DvBucketSplit) read.state.toSourceSplit()).getMaskLogEndOffset())
                .isEqualTo(108L);
    }

    /** A started split of a pinned bucket restores from the masks its state kept. */
    @Test
    void testRestoreOfAPinnedSplitUsesItsKeptMasks() throws Exception {
        standardBucket();
        PinningServer pins = new PinningServer();
        long cut = pins.pin();
        DvBucketSplit group1 = pinnedSplit(lake.plans.get(SNAPSHOT).subList(1, 2), 1, cut);
        Read first = read(group1, 1);
        DvBucketSplit checkpointed = roundTrip((DvBucketSplit) first.state.toSourceSplit());
        assertThat(checkpointed.getMaskState().getLakeMasks()).containsOnlyKeys("f1.parquet");
        pins.cuts.clear();
        server.requests.clear();
        log.append(107, ChangeType.DELETE, 5, "x");

        Read rest = read(checkpointed, Integer.MAX_VALUE);

        List<String> all = new ArrayList<>(first.rows);
        all.addAll(rest.rows);
        assertThat(all).containsExactlyInAnyOrder("3=a", "5=a");
        assertThat(server.requests).isEmpty();
    }

    private DvBucketSplit pinnedSplit(@Nullable List<LakeSplit> files, int fileGroup, long cut) {
        return DvBucketSplit.pinned(BUCKET, null, SNAPSHOT, 100L, cut, files, fileGroup, cut, 100L);
    }

    /**
     * A leader that pins cuts. A paged request without a cut pins the high watermark. Later
     * requests get parts of the masks of a pinned cut, or a stale error once the cut is gone.
     */
    private final class PinningServer {
        final java.util.Set<Long> cuts = new java.util.HashSet<>();

        PinningServer() {
            server.answer = this::answer;
        }

        long pin() {
            return answer(
                            new GetDvSnapshotRequest()
                                    .setTableId(TABLE_ID)
                                    .setBucketId(0)
                                    .setReadableSnapshotId(SNAPSHOT)
                                    .setPaged(true))
                    .getLogEndOffset();
        }

        GetDvSnapshotResponse answer(GetDvSnapshotRequest request) {
            long cut;
            if (request.hasLogEndOffset()) {
                cut = request.getLogEndOffset();
                if (!cuts.contains(cut)) {
                    throw new java.util.concurrent.CompletionException(
                            new StaleSnapshotException(SNAPSHOT, SNAPSHOT));
                }
            } else {
                cut = log.highWatermark();
                cuts.add(cut);
            }
            GetDvSnapshotResponse all = masksAt(cut);
            if (!request.hasPaged() || !request.isPaged()) {
                return all;
            }
            return page(all, request);
        }

        /** The masks of the standard bucket's lake files and its log up to {@code cut}. */
        GetDvSnapshotResponse masksAt(long cut) {
            Map<Integer, Long> live = new HashMap<>();
            java.util.Set<Integer> changed = new java.util.HashSet<>();
            Roaring64Bitmap dead = new Roaring64Bitmap();
            for (ScanRecord record : log.records) {
                long offset = record.logOffset();
                if (offset < 100 || offset >= cut) {
                    continue;
                }
                ChangeType type = record.getChangeType();
                if (type == ChangeType.UPDATE_BEFORE) {
                    continue;
                }
                int key = record.getRow().getInt(0);
                changed.add(key);
                Long previous = live.remove(key);
                if (previous != null) {
                    dead.addLong(previous);
                }
                if (type != ChangeType.DELETE) {
                    live.put(key, offset);
                }
            }
            GetDvSnapshotResponse response =
                    new GetDvSnapshotResponse().setLogEndOffset(cut).setSnapshotStartOffset(100);
            for (LakeSplit split : lake.plans.get(SNAPSHOT)) {
                FileSplit file = (FileSplit) split;
                Roaring64Bitmap mask = new Roaring64Bitmap();
                for (int pos = 0; pos < file.keys.length; pos++) {
                    if (changed.contains(file.keys[pos])) {
                        mask.addLong(pos);
                    }
                }
                response.addLakeDvEntry()
                        .setFilePath(file.fileName)
                        .setDeletedPositionsBitmap(serialize(mask));
            }
            return response.setLogDvBitmap(serialize(dead));
        }
    }

    // ------------------------------------------------------------------------------------------

    /** The paged answer of a server holding {@code all}, with log pages of two offsets. */
    private static GetDvSnapshotResponse page(
            GetDvSnapshotResponse all, GetDvSnapshotRequest request) {
        GetDvSnapshotResponse page =
                new GetDvSnapshotResponse()
                        .setLogEndOffset(all.getLogEndOffset())
                        .setSnapshotStartOffset(all.getSnapshotStartOffset())
                        .setPaged(true);
        for (String name : request.getLakeDvFileNamesList()) {
            for (int i = 0; i < all.getLakeDvEntriesCount(); i++) {
                String path = all.getLakeDvEntryAt(i).getFilePath();
                if (path.endsWith(name)) {
                    page.addLakeDvEntry()
                            .setFilePath(path)
                            .setDeletedPositionsBitmap(
                                    all.getLakeDvEntryAt(i).getDeletedPositionsBitmap());
                }
            }
        }
        if (request.hasLogDvFromOffset()) {
            long from = request.getLogDvFromOffset();
            long to = Math.min(from + 2, all.getLogEndOffset());
            Roaring64Bitmap logMask = deserialize(all.getLogDvBitmap());
            Roaring64Bitmap range = new Roaring64Bitmap();
            logMask.forEach(
                    offset -> {
                        if (offset >= from && offset < to) {
                            range.addLong(offset);
                        }
                    });
            page.setLogDvBitmap(serialize(range)).setLogDvToOffset(to);
        }
        return page;
    }

    private DvBucketSplit split() {
        return new DvBucketSplit(BUCKET, null, SNAPSHOT, 100L, 106L, lake.plans.get(SNAPSHOT));
    }

    /** The rows a reader emits for a split, and its state, stopping after {@code maxRows}. */
    private Read read(DvBucketSplit split, int maxRows) throws Exception {
        DvBucketSplitState state = new DvBucketSplitState(split);
        List<String> rows = new ArrayList<>();
        LakeRecordRecordEmitter<Void> emitter =
                new LakeRecordRecordEmitter<>(
                        (record, output) -> rows.add(rowString(record.getRow())));
        try (DvBucketSplitReader reader =
                new DvBucketSplitReader(new DvBucketSplitScanner(split, context), split)) {
            while (rows.size() < maxRows) {
                CloseableIterator<RecordAndPos> batch = reader.readBatch();
                if (batch == null) {
                    break;
                }
                while (rows.size() < maxRows && batch.hasNext()) {
                    emitter.emitRecord(state, null, batch.next());
                }
                batch.close();
            }
        }
        return new Read(rows, state);
    }

    /** The state of a split whose reader resolved it and emitted no row yet. */
    private DvBucketSplitState resolveOnly(DvBucketSplit split) throws Exception {
        DvBucketSplitState state = new DvBucketSplitState(split);
        LakeRecordRecordEmitter<Void> emitter = new LakeRecordRecordEmitter<>((r, o) -> {});
        try (DvBucketSplitReader reader =
                new DvBucketSplitReader(new DvBucketSplitScanner(split, context), split)) {
            CloseableIterator<RecordAndPos> marker = reader.readBatch();
            emitter.emitRecord(state, null, marker.next());
            assertThat(marker.hasNext()).isFalse();
            marker.close();
        }
        assertThat(state.getReadMode()).isEqualTo(ReadMode.MASKED);
        return state;
    }

    private static final class Read {
        final List<String> rows;
        final DvBucketSplitState state;

        Read(List<String> rows, DvBucketSplitState state) {
            this.rows = rows;
            this.state = state;
        }
    }

    private int serializedSize(DvBucketSplit split) throws IOException {
        return new SourceSplitSerializer(lake).serialize(split).length;
    }

    /** The split as a checkpoint stores and restores it. */
    private DvBucketSplit roundTrip(DvBucketSplit split) throws IOException {
        SourceSplitSerializer serializer = new SourceSplitSerializer(lake);
        return (DvBucketSplit)
                serializer.deserialize(serializer.getVersion(), serializer.serialize(split));
    }

    private static String rowString(InternalRow row) {
        return row.getInt(0) + "=" + row.getString(1);
    }

    private static InternalRow row(int key, String value) {
        return GenericRow.of(key, BinaryString.fromString(value));
    }

    private static byte[] bitmap(long... values) {
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        for (long value : values) {
            bitmap.addLong(value);
        }
        return serialize(bitmap);
    }

    private static byte[] serialize(Roaring64Bitmap bitmap) {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bytes)) {
            bitmap.serialize(out);
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Roaring64Bitmap deserialize(byte[] bytes) {
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        try {
            bitmap.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return bitmap;
    }

    // ------------------------------------------------------------------------------------------

    /** The bucket's leader. */
    private static final class MaskServer extends TestAdminAdapter {
        Function<GetDvSnapshotRequest, GetDvSnapshotResponse> answer;
        final List<GetDvSnapshotRequest> requests = new ArrayList<>();
        final Deque<Exception> failures = new ArrayDeque<>();
        final Map<Long, LakeSnapshot> lakeSnapshots = new HashMap<>();
        @Nullable LakeSnapshot readable;

        @Override
        public CompletableFuture<GetDvSnapshotResponse> getDvSnapshot(
                TablePath tablePath, GetDvSnapshotRequest request) {
            requests.add(request);
            CompletableFuture<GetDvSnapshotResponse> response = new CompletableFuture<>();
            if (!failures.isEmpty()) {
                response.completeExceptionally(failures.poll());
            } else {
                try {
                    response.complete(answer.apply(request));
                } catch (RuntimeException e) {
                    response.completeExceptionally(e.getCause() != null ? e.getCause() : e);
                }
            }
            return response;
        }

        @Override
        public CompletableFuture<LakeSnapshot> getLakeSnapshot(
                TablePath tablePath, long snapshotId) {
            return CompletableFuture.completedFuture(lakeSnapshots.get(snapshotId));
        }

        @Override
        public CompletableFuture<LakeSnapshot> getReadableLakeSnapshot(TablePath tablePath) {
            return CompletableFuture.completedFuture(readable);
        }
    }

    private final class TestContext implements DvBucketPartReader.Context {
        final List<Long> sleeps = new ArrayList<>();
        final List<DvBucketPart> merged = new ArrayList<>();
        final List<String> keyReads = new ArrayList<>();
        boolean partitionDropped;

        @Override
        public TablePath tablePath() {
            return TABLE_PATH;
        }

        @Override
        public DvMaskFetcher maskFetcher() {
            return new DvMaskFetcher(server, TABLE_PATH, sleeps::add);
        }

        @Override
        public LakeSource<LakeSplit> lakeSource() {
            return lake;
        }

        @Nullable
        @Override
        public List<LakeSplit> planLakeSplits(
                long snapshotId, TableBucket bucket, @Nullable String partitionName) {
            return lake.plans.get(snapshotId);
        }

        @Override
        public LakeSnapshot lakeSnapshot(@Nullable Long snapshotId) {
            return snapshotId == null ? server.readable : server.lakeSnapshots.get(snapshotId);
        }

        @Override
        public boolean lakeReadsInKeyOrder() {
            return lake.sorted;
        }

        @Nullable
        @Override
        public String snapshotIdentity(long snapshotId) {
            return lake.identities.get(snapshotId);
        }

        @Override
        public LogScanner createLogScanner() {
            return log;
        }

        @Override
        public BatchScanner createMergeScanner(DvBucketPart part) {
            merged.add(part);
            return new BatchScanner() {
                @Nullable
                @Override
                public CloseableIterator<InternalRow> pollBatch(Duration timeout) {
                    return null;
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public DvBucketPartReader.KeyReader keyReader() {
            return new DvBucketPartReader.KeyReader() {
                @Override
                public CloseableIterator<RowWithPosResult> readLakeKeys(LakeSplit lakeSplit)
                        throws IOException {
                    keyReads.add(lakeSplit.fileName());
                    return lake.createRecordReader(() -> lakeSplit).readWithPos();
                }

                @Override
                public LogScanner createLogScanner() {
                    keyReads.add("log");
                    return log;
                }

                @Override
                public byte[] encode(InternalRow keyRow) {
                    return Integer.toString(keyRow.getInt(0)).getBytes(StandardCharsets.UTF_8);
                }
            };
        }

        @Override
        public boolean isPartitionDropped(long partitionId) {
            return partitionDropped;
        }

        @Override
        public void sleep(long millis) {
            sleeps.add(millis);
        }
    }

    /** A lake data file of the bucket. */
    private static final class FileSplit implements LakeSplit {
        private final String fileName;
        private final int[] keys;
        private final String value;

        FileSplit(String fileName, int[] keys, String value) {
            this.fileName = fileName;
            this.keys = keys;
            this.value = value;
        }

        @Override
        public int bucket() {
            return BUCKET.getBucket();
        }

        @Override
        public List<String> partition() {
            return Collections.emptyList();
        }

        @Override
        public String fileName() {
            return fileName;
        }
    }

    /** A lake whose files hold rows at their positions. */
    private static final class FileLakeSource implements LakeSource<LakeSplit> {
        private static final long serialVersionUID = 1L;

        final Map<Long, List<LakeSplit>> plans = new HashMap<>();
        final Map<Long, String> identities = new HashMap<>();
        boolean sorted;

        void plan(long snapshotId, LakeSplit... splits) {
            plans.put(snapshotId, Arrays.asList(splits));
        }

        @Override
        public LakeSource<LakeSplit> copy() {
            return this;
        }

        @Override
        public void withProject(int[][] project) {}

        @Override
        public void withLimit(int limit) {}

        @Override
        public FilterPushDownResult withFilters(List<Predicate> predicates) {
            return FilterPushDownResult.of(Collections.emptyList(), predicates);
        }

        @Override
        public Planner<LakeSplit> createPlanner(PlannerContext context) {
            return () -> plans.getOrDefault(context.snapshotId(), Collections.emptyList());
        }

        @Override
        public RecordReader createRecordReader(ReaderContext<LakeSplit> context) {
            FileSplit split = (FileSplit) context.lakeSplit();
            if (split == null) {
                if (!sorted) {
                    return CloseableIterator::emptyIterator;
                }
                return new SortedRecordReader() {
                    @Override
                    public Comparator<InternalRow> order() {
                        return Comparator.comparingInt(row -> row.getInt(0));
                    }

                    @Override
                    public CloseableIterator<LogRecord> read() {
                        return CloseableIterator.emptyIterator();
                    }
                };
            }
            return new RecordReader() {
                @Override
                public CloseableIterator<LogRecord> read() {
                    List<LogRecord> records = new ArrayList<>();
                    for (int key : split.keys) {
                        records.add(
                                new GenericRecord(
                                        -1, -1, ChangeType.INSERT, row(key, split.value)));
                    }
                    return CloseableIterator.wrap(records.iterator());
                }

                @Override
                public CloseableIterator<RowWithPosResult> readWithPos() {
                    return new CloseableIterator<RowWithPosResult>() {
                        int pos = 0;

                        @Override
                        public boolean hasNext() {
                            return pos < split.keys.length;
                        }

                        @Override
                        public RowWithPosResult next() {
                            RowWithPosResult result =
                                    new RowWithPosResult()
                                            .set(
                                                    row(split.keys[pos], split.value),
                                                    pos,
                                                    split.fileName);
                            pos++;
                            return result;
                        }

                        @Override
                        public void close() {}
                    };
                }
            };
        }

        @Override
        public SimpleVersionedSerializer<LakeSplit> getSplitSerializer() {
            return new SimpleVersionedSerializer<LakeSplit>() {
                @Override
                public int getVersion() {
                    return 0;
                }

                @Override
                public byte[] serialize(LakeSplit split) {
                    return ((FileSplit) split).fileName.getBytes();
                }

                @Override
                public LakeSplit deserialize(int version, byte[] serialized) {
                    String fileName = new String(serialized);
                    for (List<LakeSplit> splits : plans.values()) {
                        for (LakeSplit split : splits) {
                            if (((FileSplit) split).fileName.equals(fileName)) {
                                return split;
                            }
                        }
                    }
                    throw new IllegalArgumentException(fileName);
                }
            };
        }

        @Nullable
        @Override
        public String snapshotIdentity(long snapshotId) {
            return identities.get(snapshotId);
        }
    }

    /** The bucket's log. */
    private static final class FakeLogScanner implements LogScanner {
        final List<ScanRecord> records = new ArrayList<>();
        TableBucket bucket = BUCKET;

        long highWatermark() {
            return records.isEmpty() ? 0 : records.get(records.size() - 1).logOffset() + 1;
        }

        @Nullable Long subscribedFrom;
        private long position = -1;
        // the partition is gone: polls return nothing, as a real scanner's do
        boolean dropped;
        boolean subscribeFails;

        void append(long offset, ChangeType changeType, int key, String value) {
            records.add(new ScanRecord(offset, offset * 10, changeType, row(key, value)));
        }

        @Override
        public ScanRecords poll(Duration timeout) {
            if (dropped || position < 0) {
                return ScanRecords.EMPTY;
            }
            List<ScanRecord> batch = new ArrayList<>();
            for (ScanRecord record : records) {
                if (record.logOffset() >= position && batch.size() < 2) {
                    batch.add(record);
                }
            }
            if (batch.isEmpty()) {
                return ScanRecords.EMPTY;
            }
            position = batch.get(batch.size() - 1).logOffset() + 1;
            return new ScanRecords(
                    Collections.singletonMap(bucket, batch),
                    Collections.singletonMap(bucket, position));
        }

        @Override
        public void subscribe(int bucket, long offset) {
            if (subscribeFails) {
                throw new PartitionNotExistException("dropped");
            }
            subscribedFrom = offset;
            position = offset;
        }

        @Override
        public void subscribe(long partitionId, int bucket, long offset) {
            subscribe(bucket, offset);
        }

        @Override
        public void unsubscribe(long partitionId, int bucket) {}

        @Override
        public void unsubscribe(int bucket) {}

        @Override
        public void wakeup() {}

        @Override
        public void close() {
            position = -1;
        }
    }
}
