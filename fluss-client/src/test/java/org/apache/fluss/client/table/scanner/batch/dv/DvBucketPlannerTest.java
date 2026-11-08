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

import org.apache.fluss.client.admin.DvAdmin;
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketPart.ReadMode;
import org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.FileSplit;
import org.apache.fluss.exception.LakeTableRolledBackException;
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.SupportsRewritePlanning;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.rpc.messages.GetDvSnapshotRequest;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.BUCKET;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.SNAPSHOT;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.TABLE_ID;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.TABLE_PATH;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/** Tests of {@link DvBucketPlanner}, which pins one cut per bucket and splits the buckets. */
class DvBucketPlannerTest {

    private final DvTestBucket bucket = new DvTestBucket();
    private final DvTestBucket.MaskServer server = bucket.server;
    private final DvTestBucket.FileLakeSource lake = bucket.lake;

    /**
     * A bucket of two file groups and its log takes three holds of its cut, one per part, and each
     * part releases one of them.
     */
    /** A lake source without deletion vectors is refused when the planner is created. */
    @Test
    void testLakeWithoutDeletionVectorsIsRefused() {
        @SuppressWarnings("unchecked")
        LakeSource<LakeSplit> plain = mock(LakeSource.class);
        assertThatThrownBy(() -> new DvBucketPlanner(server, TABLE_PATH, plain))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(SupportsRewritePlanning.class.getName())
                .hasMessageContaining(TABLE_PATH.toString());
    }

    @Test
    void testPinsAHoldPerPart() throws Exception {
        bucket.standardBucket();
        bucket.pinningServer();
        List<DvBucketPart> parts =
                new DvBucketPlanner(server, TABLE_PATH, lake)
                        .withFileGroupRows(1)
                        .pin(SNAPSHOT, Collections.singletonList(bucket.bucketPart()))
                        .get(BUCKET);

        assertThat(parts)
                .extracting(DvBucketPart::getFileGroup)
                .containsExactly(0, 1, DvBucketPart.BUCKET_LOG);
        assertThat(bucket.pins())
                .singleElement()
                .satisfies(pin -> assertThat(pin.getHolds()).isEqualTo(3));
        assertThat(bucket.releases()).isEmpty();
        for (DvBucketPart part : parts) {
            DvBucketPlanner.releasePinnedCuts(
                    server, TABLE_PATH, Collections.singleton(part), Duration.ofSeconds(10));
        }
        assertThat(bucket.releases())
                .hasSize(3)
                .allSatisfy(release -> assertThat(release.hasHolds()).isFalse());
    }

    /** A bucket read by one part takes one hold. */
    @Test
    void testPinsOneHoldForABucketReadByOnePart() throws Exception {
        bucket.standardBucket();
        bucket.pinningServer();

        List<DvBucketPart> parts =
                new DvBucketPlanner(server, TABLE_PATH, lake)
                        .pin(SNAPSHOT, Collections.singletonList(bucket.bucketPart()))
                        .get(BUCKET);

        assertThat(parts)
                .singleElement()
                .satisfies(
                        part ->
                                assertThat(part.getFileGroup())
                                        .isEqualTo(DvBucketPart.WHOLE_BUCKET));
        assertThat(bucket.pins())
                .singleElement()
                .satisfies(pin -> assertThat(pin.hasHolds()).isFalse());
    }

    /**
     * Releasing parts releases one hold for each of them, in one release per bucket and cut. Parts
     * not pinned at planning, and parts read by key, hold none.
     */
    @Test
    void testReleasesOneHoldOfEachPart() {
        List<LakeSplit> files = Collections.emptyList();
        DvBucketPart group0 = bucket.pinnedPart(files, 0, 107L);
        DvBucketPart logPart = bucket.pinnedPart(null, DvBucketPart.BUCKET_LOG, 107L);
        DvBucketPart folded =
                DvBucketPart.pinnedFolded(
                        new TableBucket(TABLE_ID, 1),
                        null,
                        SNAPSHOT,
                        100L,
                        107L,
                        null,
                        DvBucketPart.WHOLE_BUCKET,
                        107L);

        DvBucketPlanner.releasePinnedCuts(
                server,
                TABLE_PATH,
                Arrays.asList(group0, logPart, bucket.bucketPart(), folded),
                Duration.ofSeconds(10));

        assertThat(bucket.releases())
                .singleElement()
                .satisfies(
                        release -> {
                            assertThat(release.getBucketId()).isEqualTo(BUCKET.getBucket());
                            assertThat(release.getReadableSnapshotId()).isEqualTo(SNAPSHOT);
                            assertThat(release.getLogEndOffset()).isEqualTo(107L);
                            assertThat(release.getHolds()).isEqualTo(2);
                        });
    }

    /**
     * A bucket split into file groups whose log has nothing after the snapshot's readable offset
     * has no log part: its unused hold is released at planning, once more if the release fails.
     */
    @Test
    void testUnusedHoldIsReleasedAgainIfTheReleaseFails() throws Exception {
        bucket.standardBucket();
        server.answer =
                request ->
                        new GetDvSnapshotResponse()
                                .setLogEndOffset(100)
                                .setSnapshotStartOffset(100);
        server.releaseFailures.add(new NotLeaderOrFollowerException("moved"));

        List<DvBucketPart> parts =
                new DvBucketPlanner(server, TABLE_PATH, lake)
                        .withFileGroupRows(1)
                        .pin(SNAPSHOT, Collections.singletonList(bucket.bucketPart()))
                        .get(BUCKET);

        assertThat(parts).extracting(DvBucketPart::getFileGroup).containsExactly(0, 1);
        waitUntil(
                () -> bucket.releases().size() == 2,
                Duration.ofSeconds(10),
                "the release of the unused hold is sent again");
        assertThat(bucket.releases())
                .allSatisfy(
                        release -> {
                            assertThat(release.getLogEndOffset()).isEqualTo(100L);
                            // one hold, the default
                            assertThat(release.hasHolds()).isFalse();
                        });
    }

    /**
     * A lake that reused the snapshot's id after a rollback fails the planning, which releases all
     * the holds it took.
     */
    @Test
    void testRolledBackSnapshotReleasesThePins() {
        bucket.standardBucket();
        server.answer =
                request ->
                        DvTestBucket.masks(SNAPSHOT, 100, 106).setLakeSnapshotIdentity("commit-a");
        lake.identities.put(SNAPSHOT, "commit-b");
        TableBucket other = new TableBucket(TABLE_ID, 1);
        DvBucketPart otherBucket =
                DvBucketPart.ofBucket(other, null, SNAPSHOT, 100L, 106L, lake.plans.get(SNAPSHOT));

        assertThatThrownBy(
                        () ->
                                new DvBucketPlanner(server, TABLE_PATH, lake)
                                        .withFileGroupRows(1)
                                        .pin(
                                                SNAPSHOT,
                                                Arrays.asList(bucket.bucketPart(), otherBucket)))
                .isInstanceOf(LakeTableRolledBackException.class);

        assertThat(bucket.pins()).hasSize(2);
        assertThat(bucket.releases())
                .hasSize(2)
                .allSatisfy(
                        release -> {
                            assertThat(release.getLogEndOffset()).isEqualTo(106L);
                            assertThat(release.getHolds()).isEqualTo(3);
                        });
    }

    /**
     * Planning interrupted while it pins releases the cuts pinned so far, and those of pins still
     * in flight once their leaders answer.
     */
    @Test
    void testInterruptedPlanningReleasesItsPins() throws Exception {
        bucket.standardBucket();
        CompletableFuture<GetDvSnapshotResponse> slowPin = new CompletableFuture<>();
        CountDownLatch slowPinSent = new CountDownLatch(1);
        List<GetDvSnapshotRequest> requests = new CopyOnWriteArrayList<>();
        DvAdmin admin =
                new DvAdmin() {
                    @Override
                    public CompletableFuture<GetDvSnapshotResponse> getDvSnapshot(
                            TablePath tablePath, GetDvSnapshotRequest request) {
                        requests.add(request);
                        if (request.hasReleaseCut()) {
                            return CompletableFuture.completedFuture(new GetDvSnapshotResponse());
                        }
                        if (request.getBucketId() == 1) {
                            slowPinSent.countDown();
                            return slowPin;
                        }
                        return CompletableFuture.completedFuture(
                                new GetDvSnapshotResponse()
                                        .setLogEndOffset(106)
                                        .setSnapshotStartOffset(100));
                    }

                    @Override
                    public CompletableFuture<GetDvSnapshotResponse> lookupDvPositions(
                            TablePath tablePath,
                            long tableId,
                            @Nullable Long partitionId,
                            int bucketId,
                            long snapshotId,
                            List<byte[]> keys) {
                        throw new UnsupportedOperationException();
                    }
                };
        DvBucketPart slowBucket =
                DvBucketPart.ofBucket(
                        new TableBucket(TABLE_ID, 1),
                        null,
                        SNAPSHOT,
                        100L,
                        106L,
                        lake.plans.get(SNAPSHOT));
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread planning =
                new Thread(
                        () -> {
                            try {
                                new DvBucketPlanner(admin, TABLE_PATH, lake)
                                        .pin(
                                                SNAPSHOT,
                                                Arrays.asList(bucket.bucketPart(), slowBucket));
                            } catch (Throwable t) {
                                failure.set(t);
                            }
                        });
        planning.start();
        assertThat(slowPinSent.await(10, TimeUnit.SECONDS)).isTrue();
        waitUntil(
                () -> requests.stream().anyMatch(r -> r.getBucketId() == 0),
                Duration.ofSeconds(10),
                "bucket 0 is pinned");

        planning.interrupt();
        planning.join(10_000);

        assertThat(failure.get()).isInstanceOf(InterruptedException.class);
        waitUntil(
                () -> releasesOf(requests, 0) == 1,
                Duration.ofSeconds(10),
                "the pin taken is released");
        // the pin in flight is released once its leader answers
        slowPin.complete(
                new GetDvSnapshotResponse().setLogEndOffset(106).setSnapshotStartOffset(100));
        waitUntil(
                () -> releasesOf(requests, 1) == 1,
                Duration.ofSeconds(10),
                "the pin in flight is released");
    }

    private static long releasesOf(List<GetDvSnapshotRequest> requests, int bucketId) {
        return requests.stream()
                .filter(r -> r.hasReleaseCut() && r.getBucketId() == bucketId)
                .count();
    }

    /**
     * Parts of a bucket whose leader has no masks when a read that does not wait is planned all
     * derive the masks of the cut the leader named at planning, so together they read the bucket as
     * of that cut while writes continue. The leader holds no cut of them, so none is released.
     */
    @Test
    void testPartsOfABucketReadByKeyReadOneCut() throws Exception {
        bucket.standardBucket();
        server.answer =
                request -> new GetDvSnapshotResponse().setFoldCut(bucket.log.highWatermark());
        List<DvBucketPart> parts =
                new DvBucketPlanner(server, TABLE_PATH, lake)
                        .withFileGroupRows(1)
                        .withWaitForCatchUp(false)
                        .pin(SNAPSHOT, Collections.singletonList(bucket.bucketPart()))
                        .get(BUCKET);
        assertThat(parts)
                .extracting(DvBucketPart::getFileGroup)
                .containsExactly(0, 1, DvBucketPart.BUCKET_LOG);
        assertThat(parts)
                .allSatisfy(
                        part -> {
                            assertThat(part.getReadMode()).isEqualTo(ReadMode.FOLDED);
                            assertThat(part.getMaskLogEndOffset()).isEqualTo(107L);
                        });
        bucket.context.waitForCatchUp = false;
        server.clearRequests();

        List<String> rows = new ArrayList<>(bucket.read(parts.get(0), 100).rows);
        bucket.log.append(107, ChangeType.UPDATE_AFTER, 3, "later");
        bucket.log.append(108, ChangeType.DELETE, 5, "x");
        rows.addAll(bucket.read(parts.get(1), 100).rows);
        bucket.log.append(109, ChangeType.UPDATE_AFTER, 1, "later");
        rows.addAll(bucket.read(parts.get(2), 100).rows);

        assertThat(rows).containsExactlyInAnyOrder("0=late", "1=b", "2=a", "3=a", "5=a", "6=b");
        assertThat(bucket.context.keyReads)
                .containsExactly("log", "f0.parquet", "log", "f1.parquet", "log");
        assertThat(server.requests()).isEmpty();
        DvBucketPlanner.releasePinnedCuts(server, TABLE_PATH, parts, Duration.ofSeconds(10));
        assertThat(server.requests()).isEmpty();
    }

    /**
     * A bucket with more lake files than one part reads, whose cut could not be pinned, is read by
     * one part that carries none of its files: its reader plans them when it starts. A part with
     * all of them could be more than an engine can assign in one message.
     */
    @Test
    void testUnpinnedBucketOfManyFilesCarriesNone() throws Exception {
        bucket.standardBucket();
        server.answer =
                request -> {
                    throw new CompletionException(
                            new StaleSnapshotException(SNAPSHOT + 1, SNAPSHOT));
                };
        TableBucket small = new TableBucket(TABLE_ID, 1);
        lake.plan(SNAPSHOT + 1, new FileSplit("g0.parquet", new int[] {0}, "a"));
        DvBucketPart smallBucket =
                DvBucketPart.ofBucket(
                        small, null, SNAPSHOT, 100L, 106L, lake.plans.get(SNAPSHOT + 1));

        Map<TableBucket, List<DvBucketPart>> parts =
                new DvBucketPlanner(server, TABLE_PATH, lake)
                        .withFileGroupRows(1)
                        .pin(SNAPSHOT, Arrays.asList(bucket.bucketPart(), smallBucket));

        // a bucket of one file group keeps its given part, with its files
        assertThat(parts).containsOnlyKeys(BUCKET);
        assertThat(parts.get(BUCKET))
                .singleElement()
                .satisfies(
                        part -> {
                            assertThat(part.getReadMode()).isEqualTo(ReadMode.PENDING);
                            assertThat(part.isPinned()).isFalse();
                            assertThat(part.getFileGroup()).isEqualTo(DvBucketPart.WHOLE_BUCKET);
                            assertThat(part.getLakeSplits()).isNull();
                            assertThat(part.getSnapshotLogOffset()).isEqualTo(100L);
                        });
    }

    /** Only buckets read in several parts are pinned when the planner pins split buckets only. */
    @Test
    void testPinsSplitBucketsOnly() throws Exception {
        bucket.standardBucket();
        bucket.pinningServer();
        lake.plan(SNAPSHOT + 1, new FileSplit("g0.parquet", new int[] {0}, "a"));
        DvBucketPart single =
                DvBucketPart.ofBucket(
                        new TableBucket(TABLE_ID, 1),
                        null,
                        SNAPSHOT,
                        100L,
                        106L,
                        lake.plans.get(SNAPSHOT + 1));

        Map<TableBucket, List<DvBucketPart>> parts =
                new DvBucketPlanner(server, TABLE_PATH, lake)
                        .withFileGroupRows(1)
                        .withPinSplitBucketsOnly(true)
                        .pin(SNAPSHOT, Arrays.asList(bucket.bucketPart(), single));

        assertThat(parts).containsOnlyKeys(BUCKET);
        assertThat(bucket.pins()).singleElement();
    }
}
