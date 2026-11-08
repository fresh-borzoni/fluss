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

package org.apache.fluss.client.table.scanner.batch.dv;

import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketPart.ReadMode;
import org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.FileSplit;
import org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.PinningServer;
import org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.Read;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.exception.DeletionVectorsNotReadyException;
import org.apache.fluss.exception.LakeTableRolledBackException;
import org.apache.fluss.exception.LeaderNotAvailableException;
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.messages.GetDvSnapshotRequest;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.BUCKET;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.EXACT;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.SNAPSHOT;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.TABLE_ID;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.bitmap;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.covered;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.masks;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests of {@link DvBucketPartReader}, which fetches a bucket's masks when its part starts. */
class DvBucketPartReaderTest {

    // the rows of the standard bucket as of cut 107, after offset 106 updated key 0
    private static final List<String> AT_107 =
            Arrays.asList("0=late", "1=b", "2=a", "3=a", "5=a", "6=b");

    private final DvTestBucket bucket = new DvTestBucket();
    private final DvTestBucket.MaskServer server = bucket.server;
    private final DvTestBucket.FileLakeSource lake = bucket.lake;
    private final DvTestBucket.FakeLogScanner log = bucket.log;
    private final DvTestBucket.Context context = bucket.context;

    @Test
    void testMaskedReadIsExactAndStopsAtTheCut() throws Exception {
        bucket.standardBucket();

        Read read = bucket.read(bucket.bucketPart(), Integer.MAX_VALUE);

        assertThat(read.rows).containsExactlyInAnyOrderElementsOf(EXACT);
        assertThat(read.resolved.getReadMode()).isEqualTo(ReadMode.MASKED);
        assertThat(read.resolved.getMaskLogEndOffset()).isEqualTo(106L);
        // one pin; the other requests fetch the masks of its cut
        assertThat(bucket.pins()).hasSize(1);
        assertThat(bucket.pins().get(0).hasPinRequestId()).isTrue();
        assertThat(log.subscribedFrom).isEqualTo(100L);
        // the masked lake files are read with positions; every reader is closed
        assertThat(bucket.lake.readersOpened).hasPositiveValue();
        assertThat(bucket.lake.readersClosed).hasValue(bucket.lake.readersOpened.get());
    }

    /** The part start waits while a catching-up leader answers not ready. */
    @Test
    void testNotReadyAtStartIsWaitedFor() throws Exception {
        bucket.standardBucket();
        for (int i = 0; i < 6; i++) {
            server.failures.add(new DeletionVectorsNotReadyException("catching up"));
        }

        Read read = bucket.read(bucket.bucketPart(), Integer.MAX_VALUE);

        assertThat(read.rows).containsExactlyInAnyOrderElementsOf(EXACT);
        assertThat(bucket.pins()).hasSize(7);
        assertThat(context.sleeps).hasSize(6);
        // retries of one pin name it alike, so a leader that took one pins no more
        assertThat(bucket.pins())
                .extracting(GetDvSnapshotRequest::getPinRequestId)
                .containsOnly(bucket.pins().get(0).getPinRequestId());
    }

    /** A leader that covers a newer snapshot has the bucket read on that snapshot. */
    @Test
    void testStaleSnapshotAtStartReplansTheBucket() throws Exception {
        bucket.standardBucket();
        long newer = 9L;
        // tiering wrote key 1 = b into a new file of snapshot 9, readable from offset 102
        lake.plan(
                newer,
                new FileSplit("f0.parquet", new int[] {0, 1, 2}, "a"),
                new FileSplit("f1.parquet", new int[] {3, 4, 5}, "a"),
                new FileSplit("f2.parquet", new int[] {1}, "b"));
        // the leader's error names no readable offset: the bucket is planned again on the
        // coordinator's readable snapshot
        server.readable = new LakeSnapshot(newer, Collections.singletonMap(BUCKET, 102L));
        server.failures.add(new StaleSnapshotException(newer, SNAPSHOT));
        server.answer =
                request -> {
                    assertThat(request.getReadableSnapshotId()).isEqualTo(newer);
                    return newerMasks(newer);
                };

        Read read = bucket.read(bucket.bucketPart(), Integer.MAX_VALUE);

        assertThat(read.rows).containsExactlyInAnyOrderElementsOf(EXACT);
        assertThat(read.resolved.getSnapshotId()).isEqualTo(newer);
        assertThat(read.resolved.getSnapshotLogOffset()).isEqualTo(102L);
        assertThat(read.resolved.getLakeSplits()).hasSize(3);
        assertThat(log.subscribedFrom).isEqualTo(102L);
    }

    /** The masks of a newer snapshot with key 1 live in f2: key 4 is deleted in the log. */
    private static GetDvSnapshotResponse newerMasks(long snapshotId) {
        GetDvSnapshotResponse response = masks(snapshotId, 102, 106);
        response.clearLakeDvEntries();
        response.addLakeDvEntry().setFilePath("f0.parquet").setDeletedPositionsBitmap(bitmap(1));
        response.addLakeDvEntry().setFilePath("f1.parquet").setDeletedPositionsBitmap(bitmap(1));
        return response;
    }

    /**
     * Tiering keeps registering snapshots while the read waits for a bucket that switches. The
     * bucket is planned again on the coordinator's readable snapshot, which its leader switches to,
     * whatever the other buckets cover.
     */
    @Test
    void testBucketThatKeepsSwitchingIsReadOnTheReadableSnapshot() throws Exception {
        bucket.standardBucket();
        for (long newer : new long[] {9L, 10L}) {
            lake.plan(
                    newer,
                    new FileSplit("f0.parquet", new int[] {0, 1, 2}, "a"),
                    new FileSplit("f1.parquet", new int[] {3, 4, 5}, "a"),
                    new FileSplit("f2.parquet", new int[] {1}, "b"));
        }
        server.readable = new LakeSnapshot(10L, Collections.singletonMap(BUCKET, 102L));
        server.failures.add(new DeletionVectorsNotReadyException("catching up"));
        server.failures.add(new DeletionVectorsNotReadyException("catching up"));
        // the leader moved past the planned snapshot, and once more after the replan
        server.failures.add(new StaleSnapshotException(9L, SNAPSHOT));
        server.failures.add(new StaleSnapshotException(10L, 9L));
        server.answer =
                request -> {
                    assertThat(request.getReadableSnapshotId()).isEqualTo(10L);
                    return newerMasks(10L);
                };

        Read read = bucket.read(bucket.bucketPart(), Integer.MAX_VALUE);

        assertThat(read.rows).containsExactlyInAnyOrderElementsOf(EXACT);
        assertThat(read.resolved.getSnapshotId()).isEqualTo(10L);
        assertThat(log.subscribedFrom).isEqualTo(102L);
        assertThat(bucket.pins()).hasSize(5);
    }

    /**
     * A read that does not wait: the leader still switches to the planned snapshot and covers the
     * one registered before. The bucket is read on that snapshot at once, without the replan's
     * backoff, and as exactly, the log from the readable offset the leader names.
     */
    @Test
    void testBucketWhoseLeaderSwitchesIsReadOnTheSnapshotBefore() throws Exception {
        long older = SNAPSHOT - 1;
        // snapshot 4 tiered keys 0..2 up to offset 97; keys 3..5 were inserted after it
        log.append(97, ChangeType.INSERT, 3, "a");
        log.append(98, ChangeType.INSERT, 4, "a");
        log.append(99, ChangeType.INSERT, 5, "a");
        bucket.standardBucket();
        context.waitForCatchUp = false;
        lake.plan(older, new FileSplit("f0.parquet", new int[] {0, 1, 2}, "a"));
        server.answers.add(covered(older, true, 97));
        server.answer =
                request -> {
                    assertThat(request.getReadableSnapshotId()).isEqualTo(older);
                    // key 1 changed after the snapshot; key 4's insert and key 6's first version
                    // are superseded in the log
                    GetDvSnapshotResponse response =
                            new GetDvSnapshotResponse()
                                    .setLogEndOffset(106)
                                    .setSnapshotStartOffset(97);
                    response.addLakeDvEntry()
                            .setFilePath("f0.parquet")
                            .setDeletedPositionsBitmap(bitmap(1));
                    response.setLogDvBitmap(bitmap(98, 103));
                    return response;
                };

        Read read = bucket.read(bucket.bucketPart(), Integer.MAX_VALUE);

        assertThat(read.rows).containsExactlyInAnyOrderElementsOf(EXACT);
        assertThat(read.resolved.getSnapshotId()).isEqualTo(older);
        assertThat(read.resolved.getSnapshotLogOffset()).isEqualTo(97L);
        assertThat(log.subscribedFrom).isEqualTo(97L);
        assertThat(bucket.pins()).hasSize(2);
        for (GetDvSnapshotRequest request : server.requests()) {
            if (!request.hasReleaseCut()) {
                assertThat(request.hasWaitForCatchUp() && !request.isWaitForCatchUp()).isTrue();
            }
        }
        // no backoff: the leader serves the snapshot before right away
        assertThat(context.sleeps).isEmpty();
    }

    /**
     * A read that does not wait while the leader's lookups are pending gets masks up to a cut below
     * the high watermark. It reads the log only up to that cut, as of which it is exact.
     */
    @Test
    void testReadWithMasksBelowTheHighWatermarkStopsAtTheirCut() throws Exception {
        bucket.standardBucket();
        context.waitForCatchUp = false;
        server.answer =
                request -> {
                    // as of offset 104: keys 1 and 4 changed, key 6 inserted, nothing superseded
                    GetDvSnapshotResponse response =
                            new GetDvSnapshotResponse()
                                    .setLogEndOffset(104)
                                    .setSnapshotStartOffset(100);
                    response.addLakeDvEntry()
                            .setFilePath("f0.parquet")
                            .setDeletedPositionsBitmap(bitmap(1));
                    response.addLakeDvEntry()
                            .setFilePath("f1.parquet")
                            .setDeletedPositionsBitmap(bitmap(1));
                    return response;
                };

        Read read = bucket.read(bucket.bucketPart(), Integer.MAX_VALUE);

        assertThat(read.rows).containsExactlyInAnyOrder("0=a", "1=b", "2=a", "3=a", "5=a", "6=a");
        assertThat(read.resolved.getMaskLogEndOffset()).isEqualTo(104L);
        assertThat(server.requests().get(0).isWaitForCatchUp()).isFalse();
    }

    /**
     * When the masks stay unavailable, the bucket is planned again on the coordinator's readable
     * snapshot and read by key up to the planned stopping offset. Here the leader keeps covering
     * another snapshot than the readable one, or fails.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testUnavailableMasksFallBackToAReadByKey(boolean staleLeader) throws Exception {
        bucket.standardBucket();
        server.readable = new LakeSnapshot(SNAPSHOT, Collections.singletonMap(BUCKET, 100L));
        for (int i = 0; i < 10; i++) {
            server.failures.add(
                    staleLeader
                            ? new StaleSnapshotException(SNAPSHOT + 1, SNAPSHOT)
                            : new IllegalStateException("the leader failed"));
        }

        Read read = bucket.read(bucket.bucketPart(), Integer.MAX_VALUE);

        assertThat(read.rows).containsExactlyInAnyOrderElementsOf(EXACT);
        assertThat(read.resolved.getReadMode()).isEqualTo(ReadMode.FOLDED);
        assertThat(read.resolved.getSnapshotId()).isEqualTo(SNAPSHOT);
        assertThat(read.resolved.getSnapshotLogOffset()).isEqualTo(100L);
        assertThat(read.resolved.getMaskLogEndOffset()).isEqualTo(106L);
        assertThat(context.keyReads).containsExactly("log", "f0.parquet", "f1.parquet");
        // the readers of the lake files and of their keys are all closed
        assertThat(bucket.lake.readersClosed).hasValue(bucket.lake.readersOpened.get());
        assertThat(bucket.pins()).hasSize(staleLeader ? 3 : 1);
    }

    /** A partition dropped before its part starts has its lake files read unmasked. */
    @Test
    void testDroppedPartitionAtStartReadsTheLakeOnly() throws Exception {
        bucket.standardBucket();
        server.failures.add(new PartitionNotExistException("dropped"));

        Read read = bucket.read(bucket.bucketPart(), Integer.MAX_VALUE);

        assertThat(read.resolved.getReadMode()).isEqualTo(ReadMode.LAKE_ONLY);
        assertThat(read.rows).containsExactlyInAnyOrder("0=a", "1=a", "2=a", "3=a", "4=a", "5=a");
        assertThat(log.subscribedFrom).isNull();
    }

    /**
     * A lake-only part restored after it emitted rows reports the end of its read once, as any
     * other part does: the read's log line and the engine's metrics.
     */
    @Test
    void testRestoredLakeOnlyPartReportsTheEndOfItsRead() throws Exception {
        bucket.standardBucket();
        server.failures.add(new PartitionNotExistException("dropped"));
        DvBucketPart lakeOnly = bucket.read(bucket.bucketPart(), Integer.MAX_VALUE).resolved;
        assertThat(lakeOnly.getReadMode()).isEqualTo(ReadMode.LAKE_ONLY);
        // restored after the rows of its first lake split
        DvBucketPart started = lakeOnly.atPosition(1, 0, -1L);

        List<DvBucketPartReader.ReadStats> ends = new ArrayList<>();
        List<String> rows = new ArrayList<>();
        try (DvBucketPartReader reader = new DvBucketPartReader(started, context)) {
            reader.onReadEnd(ends::add);
            assertThat(reader.resolve()).isSameAs(started);
            CloseableIterator<InternalRow> batch;
            while ((batch = reader.pollBatch(Duration.ofMillis(10))) != null) {
                while (batch.hasNext()) {
                    rows.add(DvTestBucket.rowString(batch.next()));
                }
                batch.close();
            }
            assertThat(reader.resolve()).isSameAs(started);
        }

        assertThat(rows).containsExactlyInAnyOrder("3=a", "4=a", "5=a");
        assertThat(ends).hasSize(1);
    }

    /** The read fails rather than lose rows when the log that masked the lake rows is gone. */
    @Test
    void testPartitionDroppedDuringTheLogFails() throws Exception {
        bucket.standardBucket();
        TableBucket partitioned = new TableBucket(TABLE_ID, 7L, 0);
        log.bucket = partitioned;
        log.dropped = true;
        context.partitionDropped = true;
        DvBucketPart part =
                DvBucketPart.ofBucket(
                        partitioned, "p", SNAPSHOT, 100L, 106L, lake.plans.get(SNAPSHOT));

        assertThatThrownBy(() -> bucket.read(part, Integer.MAX_VALUE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("was dropped while a union read was reading the log");
    }

    /** A log-only bucket whose partition is gone before any row was read ends without rows. */
    @Test
    void testPartitionDroppedBeforeAnyRowEndsThePart() throws Exception {
        bucket.standardBucket();
        log.subscribeFails = true;
        DvBucketPart logOnly = DvBucketPart.ofBucket(BUCKET, null, SNAPSHOT, -1L, 106L, null);

        Read read = bucket.read(logOnly, Integer.MAX_VALUE);

        assertThat(read.rows).isEmpty();
    }

    /**
     * The log part of a bucket read in several parts fails when its partition is dropped before it
     * read a row: the other parts of the bucket return lake rows masked by changes in that log, so
     * ending it without rows would lose the keys those changes wrote.
     */
    @Test
    void testPartitionDroppedBeforeTheLogOfASplitBucketFails() throws Exception {
        bucket.standardBucket();
        TableBucket partitioned = new TableBucket(TABLE_ID, 7L, 0);
        log.bucket = partitioned;
        log.subscribeFails = true;
        context.partitionDropped = true;
        server.answer = request -> masks(SNAPSHOT, 100, 106);
        DvBucketPart logPart =
                DvBucketPart.pinned(
                        partitioned,
                        "p",
                        SNAPSHOT,
                        100L,
                        106L,
                        null,
                        DvBucketPart.BUCKET_LOG,
                        106L,
                        100L);
        assertThat(logPart.sharesBucket()).isTrue();

        assertThatThrownBy(() -> bucket.read(logPart, Integer.MAX_VALUE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Partition 'p'")
                .hasMessageContaining("was dropped");
    }

    /** Masks for a reused snapshot id do not apply to the commit it names now. */
    @Test
    void testReusedSnapshotIdFailsTheRead() throws Exception {
        bucket.standardBucket();
        Function<GetDvSnapshotRequest, GetDvSnapshotResponse> base = server.answer;
        server.answer = request -> base.apply(request).setLakeSnapshotIdentity("commit-a");

        lake.identities.put(SNAPSHOT, "commit-a");
        assertThat(bucket.read(bucket.bucketPart(), Integer.MAX_VALUE).rows)
                .containsExactlyInAnyOrderElementsOf(EXACT);

        lake.identities.put(SNAPSHOT, "commit-b");
        server.clearRequests();
        assertThatThrownBy(() -> bucket.read(bucket.bucketPart(), Integer.MAX_VALUE))
                .isInstanceOf(LakeTableRolledBackException.class)
                .hasMessageContaining("commit-b");
        // the part releases the cut it pinned before it found the snapshot reused
        assertThat(bucket.releases()).hasSize(1);
    }

    /**
     * The leader answers each part of the masks of the cut the part pinned: the pin carries the
     * masks of the first lake split, the next request those of the others, and the log mask comes
     * in pages. Each mask is fetched once.
     */
    @Test
    void testPagedMasks() throws Exception {
        bucket.standardBucket();

        Read read = bucket.read(bucket.bucketPart(), Integer.MAX_VALUE);

        assertThat(read.rows).containsExactlyInAnyOrderElementsOf(EXACT);
        List<GetDvSnapshotRequest> requests = server.requests();
        // the first request takes the cut; the next ones name it
        assertThat(requests.get(0).hasLogEndOffset()).isFalse();
        assertThat(requests.subList(1, requests.size()))
                .isNotEmpty()
                .allSatisfy(request -> assertThat(request.getLogEndOffset()).isEqualTo(106L));
        assertThat(requests.get(0).getLakeDvFileNamesList()).containsExactly("f0.parquet");
        assertThat(requests.get(1).getLakeDvFileNamesList()).containsExactly("f1.parquet");
        assertThat(requests)
                .filteredOn(GetDvSnapshotRequest::hasLogDvFromOffset)
                .extracting(GetDvSnapshotRequest::getLogDvFromOffset)
                .containsExactly(100L, 102L, 104L);
        assertThat(read.resolved.getMaskState()).isNotNull();
        assertThat(read.resolved.getMaskState().getLakeMasks())
                .containsOnlyKeys("f0.parquet", "f1.parquet");
    }

    /** A bucket with one lake split fetches no lake masks after the pin, which carried them. */
    @Test
    void testMasksOfASingleSplitComeWithThePin() throws Exception {
        bucket.standardBucket();
        lake.plan(SNAPSHOT, new FileSplit("f0.parquet", new int[] {0, 1, 2}, "a"));

        Read read = bucket.read(bucket.bucketPart(), Integer.MAX_VALUE);

        assertThat(read.rows).containsExactlyInAnyOrder("0=a", "2=a", "1=b", "6=b");
        assertThat(server.requests())
                .filteredOn(request -> request.getLakeDvFileNamesCount() > 0)
                .singleElement()
                .satisfies(request -> assertThat(request.hasLogEndOffset()).isFalse());
    }

    /**
     * A part's own pin is lost before its masks are fetched: the leader evicted the cut, or a new
     * leader holds no such cut. The part releases its pin and pins again, and reads as exactly.
     */
    @ParameterizedTest
    @EnumSource(Loss.class)
    void testOwnPinLostBeforeItsMasksAreFetchedIsPinnedAgain(Loss loss) throws Exception {
        bucket.standardBucket();
        PinningServer pins = bucket.pinningServer();
        AtomicInteger pinned = new AtomicInteger();
        server.answer =
                request -> {
                    GetDvSnapshotResponse response = pins.answer(request);
                    if (!request.hasLogEndOffset() && pinned.incrementAndGet() == 1) {
                        // the first pin is lost right after the leader answered it
                        pins.cuts.clear();
                        loss.prepare(server);
                    }
                    return response;
                };
        context.waitForCatchUp = loss.waitForCatchUp;

        Read read = bucket.read(bucket.bucketPart(), Integer.MAX_VALUE);

        assertThat(read.rows).containsExactlyInAnyOrderElementsOf(AT_107);
        assertThat(read.resolved.getReadMode()).isEqualTo(ReadMode.MASKED);
        assertThat(read.resolved.getMaskLogEndOffset()).isEqualTo(107L);
        assertThat(bucket.pins()).hasSize(2);
        assertThat(bucket.pins().get(0).getPinRequestId())
                .isNotEqualTo(bucket.pins().get(1).getPinRequestId());
        // the lost pin and the second one are both released
        assertThat(bucket.releases()).hasSize(2);
        assertThat(context.keyReads).isEmpty();
    }

    /** How a part's own pin is lost while its masks are fetched. */
    enum Loss {
        /** The cut is gone: the leader answers stale. */
        STALE(true) {
            @Override
            void prepare(DvTestBucket.MaskServer server) {}
        },
        /** A new leader still recovers and answers not ready until the read stops waiting. */
        NOT_READY(true) {
            @Override
            void prepare(DvTestBucket.MaskServer server) {
                // every attempt of one fetch, up to the fetch's budget
                long waitedMs = 0;
                long backoffMs = DvMaskFetcher.INITIAL_BACKOFF_MS;
                server.failures.add(new DeletionVectorsNotReadyException("recovering"));
                while (waitedMs < DvMaskFetcher.MAX_NOT_READY_WAIT_MS) {
                    waitedMs += backoffMs;
                    backoffMs = Math.min(backoffMs * 2, DvMaskFetcher.MAX_NOT_READY_BACKOFF_MS);
                    server.failures.add(new DeletionVectorsNotReadyException("recovering"));
                }
            }
        },
        /** No leader is found until the fetch stops retrying. */
        NO_LEADER(true) {
            @Override
            void prepare(DvTestBucket.MaskServer server) {
                for (int i = 0; i <= DvMaskFetcher.MAX_LEADER_RETRIES; i++) {
                    server.failures.add(new NotLeaderOrFollowerException("no leader"));
                }
            }
        },
        /** A read that does not wait: the new leader rebuilds and names a cut to fold at. */
        REBUILDING(false) {
            @Override
            void prepare(DvTestBucket.MaskServer server) {
                server.answers.add(new GetDvSnapshotResponse().setFoldCut(200));
            }
        };

        final boolean waitForCatchUp;

        Loss(boolean waitForCatchUp) {
            this.waitForCatchUp = waitForCatchUp;
        }

        abstract void prepare(DvTestBucket.MaskServer server);
    }

    /**
     * A part whose own pin is lost again and again reads the bucket by key as of the last cut it
     * pinned, which is exact for a part that reads the whole bucket.
     */
    @Test
    void testOwnPinLostOverAndOverIsReadByKeyAtItsCut() throws Exception {
        bucket.standardBucket();
        PinningServer pins = bucket.pinningServer();
        server.answer =
                request -> {
                    GetDvSnapshotResponse response = pins.answer(request);
                    if (!request.hasLogEndOffset()) {
                        pins.cuts.clear();
                    }
                    return response;
                };

        Read read = bucket.read(bucket.bucketPart(), Integer.MAX_VALUE);

        assertThat(read.rows).containsExactlyInAnyOrderElementsOf(AT_107);
        assertThat(read.resolved.getReadMode()).isEqualTo(ReadMode.FOLDED);
        assertThat(read.resolved.getSnapshotId()).isEqualTo(SNAPSHOT);
        assertThat(read.resolved.getMaskLogEndOffset()).isEqualTo(107L);
        assertThat(context.keyReads).containsExactly("log", "f0.parquet", "f1.parquet");
        assertThat(bucket.pins()).hasSizeGreaterThan(1);
        // every pin is released: none is held for the folded read
        assertThat(bucket.releases()).hasSameSizeAs(bucket.pins());
    }

    /**
     * A part pinned at planning whose cut is gone resolves afresh on the snapshot its leader covers
     * now, which reads the bucket's log from another offset. Restored before it returned a row, it
     * reads the log from that offset again, not from the one planning pinned.
     */
    @Test
    void testRestoreOfAPartPinnedAgainReadsTheLogFromItsNewStart() throws Exception {
        // snapshot 6 holds none of the bucket's files; its log is read from offset 150
        long newer = 6L;
        List<Long> held = new ArrayList<>();
        server.answer =
                request -> {
                    long snapshotId = request.getReadableSnapshotId();
                    if (!request.hasLogEndOffset()) {
                        if (snapshotId == SNAPSHOT) {
                            return covered(newer, false, 150);
                        }
                        held.add(180L);
                        return new GetDvSnapshotResponse()
                                .setLogEndOffset(180)
                                .setSnapshotStartOffset(150);
                    }
                    if (snapshotId != newer || !held.contains(request.getLogEndOffset())) {
                        throw new CompletionException(
                                new StaleSnapshotException(newer, snapshotId));
                    }
                    return new GetDvSnapshotResponse()
                            .setLogEndOffset(request.getLogEndOffset())
                            .setSnapshotStartOffset(150);
                };
        context.waitForCatchUp = false;
        DvBucketPart planned =
                DvBucketPart.pinned(
                        BUCKET,
                        null,
                        SNAPSHOT,
                        100L,
                        200L,
                        null,
                        DvBucketPart.WHOLE_BUCKET,
                        106L,
                        100L);

        DvBucketPart resolved = new DvBucketPartReader(planned, context).resolve();

        assertThat(resolved.getSnapshotId()).isEqualTo(newer);
        assertThat(resolved.getMaskLogEndOffset()).isEqualTo(180L);
        assertThat(resolved.getMaskState().getLogStartOffset()).isEqualTo(150L);
        // not the planning pin's: its cut is gone
        assertThat(resolved.isPinned() && resolved.getPinnedLogStartOffset() != 150L).isFalse();

        // restored before any row: the masks are not kept
        server.clearRequests();
        DvBucketPart restored =
                new DvBucketPartReader(resolved.withMaskState(null), context).resolve();

        assertThat(restored.getMaskState().getLogStartOffset()).isEqualTo(150L);
        assertThat(restored.getMaskState().getLogEndOffset()).isEqualTo(180L);
        assertThat(server.requests())
                .filteredOn(GetDvSnapshotRequest::hasLogDvFromOffset)
                .extracting(GetDvSnapshotRequest::getLogDvFromOffset)
                .doesNotContain(100L);
    }

    /**
     * The leader's answer names the offset the masks start at. A fetch that expected another fails
     * rather than read the log from the wrong offset; a part that reads its whole bucket then
     * resolves afresh.
     */
    @Test
    void testMasksOfAnotherStartAreNotReadFromThePlannedStart() throws Exception {
        bucket.standardBucket();
        // the bucket's log is read from offset 102 in the snapshot, not from 100 as planned
        server.answer =
                request -> {
                    GetDvSnapshotResponse response = masks(SNAPSHOT, 102, 106);
                    response.clearLakeDvEntries();
                    return response;
                };
        DvBucketPart planned =
                DvBucketPart.pinned(
                        BUCKET,
                        null,
                        SNAPSHOT,
                        100L,
                        106L,
                        null,
                        DvBucketPart.WHOLE_BUCKET,
                        106L,
                        100L);

        Read read = bucket.read(planned, Integer.MAX_VALUE);

        assertThat(read.resolved.getMaskState().getLogStartOffset()).isEqualTo(102L);
        assertThat(log.subscribedFrom).isEqualTo(102L);
        assertThat(read.rows).containsExactlyInAnyOrder("6=b");
    }

    /**
     * The parts of a bucket that share a pinned cut read the masks of that cut while writes move
     * the high watermark. Together they read the bucket as of the cut.
     */
    @Test
    void testPartsOfAPinnedBucketReadOneCutWhileWritesContinue() throws Exception {
        bucket.standardBucket();
        PinningServer pins = bucket.pinningServer();
        long cut = pins.pin();
        assertThat(cut).isEqualTo(107L);
        List<LakeSplit> files = lake.plans.get(SNAPSHOT);
        DvBucketPart group0 = bucket.pinnedPart(files.subList(0, 1), 0, cut);
        DvBucketPart group1 = bucket.pinnedPart(files.subList(1, 2), 1, cut);
        DvBucketPart logPart = bucket.pinnedPart(null, DvBucketPart.BUCKET_LOG, cut);

        List<String> rows = new ArrayList<>(bucket.read(group0, Integer.MAX_VALUE).rows);
        // keys 3 and 5 of the second group change after the cut
        log.append(107, ChangeType.UPDATE_AFTER, 3, "later");
        log.append(108, ChangeType.DELETE, 5, "x");
        rows.addAll(bucket.read(group1, Integer.MAX_VALUE).rows);
        log.append(109, ChangeType.UPDATE_AFTER, 1, "later");
        rows.addAll(bucket.read(logPart, Integer.MAX_VALUE).rows);

        assertThat(rows).containsExactlyInAnyOrderElementsOf(AT_107);
        assertThat(server.requests())
                .allSatisfy(request -> assertThat(request.getLogEndOffset()).isEqualTo(cut));
        // the file groups asked for their own files' masks only, the log part for log pages
        assertThat(server.requests())
                .filteredOn(request -> request.getLakeDvFileNamesCount() > 0)
                .extracting(GetDvSnapshotRequest::getLakeDvFileNamesList)
                .containsExactly(
                        Collections.singletonList("f0.parquet"),
                        Collections.singletonList("f1.parquet"));
        assertThat(server.requests())
                .filteredOn(GetDvSnapshotRequest::hasLogDvFromOffset)
                .isNotEmpty();
        // the cut was pinned at planning, whose engine releases it
        assertThat(bucket.releases()).isEmpty();
    }

    /**
     * A part that pinned its own cut when it started, unpinned at planning, releases the cut when
     * it ends: read to the end, or closed before, as when the read is cancelled.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testPartReleasesTheCutItPinned(boolean readToTheEnd) throws Exception {
        bucket.standardBucket();

        Read read = bucket.read(bucket.bucketPart(), readToTheEnd ? Integer.MAX_VALUE : 1);

        assertThat(read.rows).hasSize(readToTheEnd ? EXACT.size() : 1);
        assertThat(bucket.releases())
                .singleElement()
                .satisfies(
                        release -> {
                            assertThat(release.getReadableSnapshotId()).isEqualTo(SNAPSHOT);
                            assertThat(release.getLogEndOffset()).isEqualTo(106L);
                            assertThat(release.getLakeDvFileNamesCount()).isZero();
                            assertThat(release.hasLogDvFromOffset()).isFalse();
                        });
        // the release is the part's last request
        List<GetDvSnapshotRequest> requests = server.requests();
        assertThat(requests.get(requests.size() - 1)).isSameAs(bucket.releases().get(0));
    }

    /**
     * Parts whose pinned cut the leader lost, for example on a leader change, derive the masks of
     * that cut from the keys. Together with the parts that fetched the masks earlier, they read the
     * bucket as of the cut.
     */
    @Test
    void testPartsDeriveTheMasksOfALostCut() throws Exception {
        bucket.standardBucket();
        PinningServer pins = bucket.pinningServer();
        long cut = pins.pin();
        List<LakeSplit> files = lake.plans.get(SNAPSHOT);
        DvBucketPart group0 = bucket.pinnedPart(files.subList(0, 1), 0, cut);
        DvBucketPart group1 = bucket.pinnedPart(files.subList(1, 2), 1, cut);
        DvBucketPart logPart = bucket.pinnedPart(null, DvBucketPart.BUCKET_LOG, cut);

        Read fetched = bucket.read(group0, Integer.MAX_VALUE);
        assertThat(context.keyReads).isEmpty();
        assertThat(context.sleeps).isEmpty();
        pins.cuts.clear();
        log.append(107, ChangeType.UPDATE_AFTER, 3, "later");
        log.append(108, ChangeType.DELETE, 2, "x");
        Read derivedGroup = bucket.read(group1, Integer.MAX_VALUE);
        Read derivedLog = bucket.read(logPart, Integer.MAX_VALUE);

        List<String> rows = new ArrayList<>(fetched.rows);
        rows.addAll(derivedGroup.rows);
        rows.addAll(derivedLog.rows);
        assertThat(rows).containsExactlyInAnyOrderElementsOf(AT_107);
        assertThat(context.keyReads).containsExactly("log", "f1.parquet", "log");
        // a lost cut is not asked for again
        assertThat(context.sleeps).isEmpty();
        // the derived masks are those the leader served for the cut
        GetDvSnapshotResponse served = pins.masksAt(cut);
        DvBucketMaskState group1Masks = derivedGroup.resolved.getMaskState();
        assertThat(group1Masks.getLogEndOffset()).isEqualTo(cut);
        assertThat(bitmapOf(group1Masks.getLakeMasks().get("f1.parquet")))
                .isEqualTo(bitmapOf(DvBucketMasks.lakeMasksOf(served).get("f1.parquet")));
        DvBucketMaskState logMasks = derivedLog.resolved.getMaskState();
        assertThat(bitmapOf(logMasks.getLogMask())).isEqualTo(bitmapOf(served.getLogDvBitmap()));
    }

    /**
     * Parts that derive the masks of a lost cut fold the log from the offset the leader's masks of
     * the cut start at, which may be before the planned readable offset: a fold from the planned
     * offset would miss the changes in between.
     */
    @Test
    void testPartsDeriveTheMasksOfALostCutFromItsStart() throws Exception {
        bucket.standardBucket();
        PinningServer pins = bucket.pinningServer();
        long cut = pins.pin();
        pins.cuts.clear();
        List<LakeSplit> files = lake.plans.get(SNAPSHOT);

        List<String> rows = new ArrayList<>();
        List<Long> maskStarts = new ArrayList<>();
        for (int fileGroup : new int[] {0, 1, DvBucketPart.BUCKET_LOG}) {
            List<LakeSplit> groupFiles =
                    fileGroup == DvBucketPart.BUCKET_LOG
                            ? null
                            : files.subList(fileGroup, fileGroup + 1);
            // planned at readable offset 102, while the cut's masks start at 100: offsets 100
            // and 101 update key 1
            DvBucketPart part =
                    DvBucketPart.pinned(
                            BUCKET, null, SNAPSHOT, 102L, cut, groupFiles, fileGroup, cut, 100L);
            Read read = bucket.read(part, Integer.MAX_VALUE);
            rows.addAll(read.rows);
            maskStarts.add(read.resolved.getMaskState().getLogStartOffset());
        }

        assertThat(rows).containsExactlyInAnyOrderElementsOf(AT_107);
        assertThat(maskStarts).containsOnly(100L);
        assertThat(log.subscribedFrom).isEqualTo(100L);
    }

    /** A part that cannot derive the masks of its lost cut fails rather than read another. */
    @Test
    void testLostCutFailsAPartThatCannotDeriveIt() throws Exception {
        bucket.standardBucket();
        PinningServer pins = bucket.pinningServer();
        long cut = pins.pin();
        pins.cuts.clear();
        log.subscribeFails = true;

        DvBucketPart group0 = bucket.pinnedPart(lake.plans.get(SNAPSHOT).subList(0, 1), 0, cut);
        assertThatThrownBy(() -> bucket.read(group0, Integer.MAX_VALUE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("is no longer served")
                .hasMessageContaining("could not derive its masks");
    }

    /** A part that reads its whole bucket resolves afresh when its pinned cut is lost. */
    @Test
    void testLostCutOfAWholeBucketPartResolvesAfresh() throws Exception {
        bucket.standardBucket();
        PinningServer pins = bucket.pinningServer();
        long cut = pins.pin();
        pins.cuts.clear();
        log.append(107, ChangeType.UPDATE_AFTER, 3, "later");

        Read read =
                bucket.read(
                        bucket.pinnedPart(lake.plans.get(SNAPSHOT), DvBucketPart.WHOLE_BUCKET, cut),
                        Integer.MAX_VALUE);

        // as of the new cut, after both late writes
        assertThat(read.rows)
                .containsExactlyInAnyOrder("0=late", "1=b", "2=a", "3=later", "5=a", "6=b");
        assertThat(read.resolved.getMaskLogEndOffset()).isEqualTo(108L);
    }

    /**
     * A read that does not wait reads a bucket whose leader has no masks to serve yet by key, at
     * once. A read that waits waits for the masks and reads the same rows.
     */
    @Test
    void testBucketWithoutMasksIsReadByKeyWithoutWaiting() throws Exception {
        bucket.standardBucket();
        AtomicInteger notReady = new AtomicInteger();
        server.answer =
                request -> {
                    if (request.hasWaitForCatchUp() && !request.isWaitForCatchUp()) {
                        return new GetDvSnapshotResponse().setFoldCut(106);
                    }
                    if (notReady.incrementAndGet() <= 2) {
                        throw new CompletionException(
                                new DeletionVectorsNotReadyException("rebuilding"));
                    }
                    return masks(SNAPSHOT, 100, 106);
                };
        context.waitForCatchUp = false;

        Read folded = bucket.read(bucket.bucketPart(), Integer.MAX_VALUE);

        assertThat(folded.rows).containsExactlyInAnyOrderElementsOf(EXACT);
        assertThat(folded.resolved.getReadMode()).isEqualTo(ReadMode.FOLDED);
        assertThat(folded.resolved.getMaskLogEndOffset()).isEqualTo(106L);
        // one request, answered at once, then the keys of the log and of both lake files
        assertThat(server.requests()).hasSize(1);
        assertThat(context.sleeps).isEmpty();
        assertThat(context.keyReads).containsExactly("log", "f0.parquet", "f1.parquet");

        context.waitForCatchUp = true;
        context.keyReads.clear();
        Read waited = bucket.read(bucket.bucketPart(), Integer.MAX_VALUE);

        assertThat(waited.rows).containsExactlyInAnyOrderElementsOf(folded.rows);
        assertThat(waited.resolved.getReadMode()).isEqualTo(ReadMode.MASKED);
        assertThat(context.sleeps).hasSize(2);
        assertThat(context.keyReads).isEmpty();
    }

    /**
     * A read that does not wait is sent to the snapshot before, and the leader of that snapshot has
     * no masks to serve yet: the bucket is read by key from the readable offset the leader named
     * for that snapshot.
     */
    @Test
    void testBucketSentToTheSnapshotBeforeIsReadByKeyFromItsReadableOffset() throws Exception {
        long older = SNAPSHOT - 1;
        // snapshot 4 holds keys 0..2, readable from offset 97; keys 3..5 were inserted after it
        log.append(97, ChangeType.INSERT, 3, "a");
        log.append(98, ChangeType.INSERT, 4, "a");
        log.append(99, ChangeType.INSERT, 5, "a");
        bucket.standardBucket();
        context.waitForCatchUp = false;
        lake.plan(older, new FileSplit("f0.parquet", new int[] {0, 1, 2}, "a"));
        server.readable = new LakeSnapshot(older, Collections.singletonMap(BUCKET, 99L));
        server.answers.add(covered(older, true, 97));
        server.answers.add(new GetDvSnapshotResponse().setFoldCut(106));

        Read read = bucket.read(bucket.bucketPart(), Integer.MAX_VALUE);

        assertThat(read.rows).containsExactlyInAnyOrderElementsOf(EXACT);
        assertThat(read.resolved.getReadMode()).isEqualTo(ReadMode.FOLDED);
        assertThat(read.resolved.getSnapshotId()).isEqualTo(older);
        assertThat(read.resolved.getSnapshotLogOffset()).isEqualTo(97L);
        assertThat(read.resolved.getMaskLogEndOffset()).isEqualTo(106L);
        assertThat(log.subscribedFrom).isEqualTo(97L);
    }

    /**
     * A bucket with nothing tiered, read by key, reads all of its log: from the earliest offset, as
     * the snapshot has no readable offset for it. So does such a part restored before it read a
     * row.
     */
    @Test
    void testBucketWithNothingTieredIsReadByKeyFromTheLogStart() throws Exception {
        log.append(0, ChangeType.INSERT, 7, "a");
        log.append(1, ChangeType.UPDATE_BEFORE, 7, "a");
        log.append(2, ChangeType.UPDATE_AFTER, 7, "b");
        log.append(3, ChangeType.INSERT, 8, "a");
        server.answer = request -> new GetDvSnapshotResponse().setFoldCut(4);
        context.waitForCatchUp = false;
        DvBucketPart emptyLake = DvBucketPart.ofBucket(BUCKET, null, -1L, -1L, 4L, null);

        Read read = bucket.read(emptyLake, Integer.MAX_VALUE);

        assertThat(read.rows).containsExactlyInAnyOrder("7=b", "8=a");
        assertThat(read.resolved.getReadMode()).isEqualTo(ReadMode.FOLDED);
        assertThat(log.subscribedFrom).isEqualTo(LogScanner.EARLIEST_OFFSET);

        log.subscribedFrom = null;
        Read restored = bucket.read(read.resolved.withMaskState(null), Integer.MAX_VALUE);

        assertThat(restored.rows).containsExactlyInAnyOrder("7=b", "8=a");
        assertThat(log.subscribedFrom).isEqualTo(LogScanner.EARLIEST_OFFSET);
    }

    /**
     * A part planned without its lake files, as its bucket has too many to carry, plans them when
     * it starts and reads as exactly.
     */
    @Test
    void testPartWithoutPlannedLakeFilesPlansThemAtStart() throws Exception {
        bucket.standardBucket();
        DvBucketPart unplanned = bucket.bucketPart().withLakeSplitsPlannedAtStart();
        assertThat(unplanned.getLakeSplits()).isNull();

        Read read = bucket.read(unplanned, Integer.MAX_VALUE);

        assertThat(read.rows).containsExactlyInAnyOrderElementsOf(EXACT);
        assertThat(context.plannedSnapshots).containsExactly(SNAPSHOT);
        assertThat(read.resolved.getLakeSplits()).hasSize(2);
    }

    /** A part whose leader moved when it starts follows the leader. */
    @Test
    void testInvalidMetadataAtStartIsRetried() throws Exception {
        bucket.standardBucket();
        server.failures.add(new LeaderNotAvailableException("leader moved"));

        Read read = bucket.read(bucket.bucketPart(), Integer.MAX_VALUE);

        assertThat(read.rows).containsExactlyInAnyOrderElementsOf(EXACT);
        assertThat(context.sleeps).hasSize(1);
    }

    /**
     * A part interrupted while it fetches the masks of the cut planning pinned fails, as the engine
     * cancels the read: it neither pins a cut of its own nor derives the masks.
     */
    @Test
    void testInterruptedFetchOfAPlanningCutFailsTheRead() {
        bucket.standardBucket();
        server.failures.add(new DeletionVectorsNotReadyException("switching"));
        context.interruptSleeps = true;
        DvBucketPart part =
                bucket.pinnedPart(lake.plans.get(SNAPSHOT), DvBucketPart.WHOLE_BUCKET, 106L);

        try {
            assertThatThrownBy(() -> bucket.read(part, Integer.MAX_VALUE))
                    .isInstanceOf(IOException.class)
                    .hasRootCauseInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
        assertThat(bucket.pins()).isEmpty();
        assertThat(context.keyReads).isEmpty();
    }

    private static Roaring64Bitmap bitmapOf(byte[] bytes) throws IOException {
        return DvBucketMasks.deserialize(bytes, "a test mask");
    }
}
