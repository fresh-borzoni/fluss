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

package org.apache.fluss.server.kv.dv;

import org.apache.fluss.exception.DeletionVectorsNotReadyException;
import org.apache.fluss.exception.InvalidRequestException;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.index.PositionIndexChangeBuilder;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.utils.RoaringBitmapUtils;

import io.github.fluss_contrib.rocksdb.EnvOptions;
import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.ReadOptions;
import io.github.fluss_contrib.rocksdb.RocksDBException;
import io.github.fluss_contrib.rocksdb.SstFileReader;
import io.github.fluss_contrib.rocksdb.SstFileReaderIterator;
import io.github.fluss_contrib.rocksdb.SstFileWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;

import static org.apache.fluss.server.kv.dv.DvTestManagers.hooks;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Tests for {@link DerivedDvManager}. */
class DerivedDvManagerTest {

    private static final String F1 = "s3://lake/t/data/f1.parquet";
    private static final String F2 = "s3://lake/t/data/f2.parquet";
    private static final String F3 = "s3://lake/t/data/f3.parquet";

    @TempDir Path tempDir;

    private DvRocksDB db;
    private AtomicLong highWatermark;
    private DerivedDvMetrics metrics;
    private DerivedDvManager manager;
    private FakeSource src;
    // for tasks that block on a paused replay; the common pool can be too small for them
    private ExecutorService async;

    @BeforeEach
    void setUp() throws Exception {
        db = DvRocksDB.open(tempDir.resolve("dv").toString());
        highWatermark = new AtomicLong(0);
        metrics = DerivedDvMetrics.noop();
        manager =
                DvTestManagers.newManager(
                        db, highWatermark::get, DvTestManagers.CHECKPOINT_INTERVAL, metrics);
        // most tests read again right after a change; the cache has its own test
        manager.setMaskCacheMillis(0);
        manager.setCheckpointJitterMillis(0);
        src = new FakeSource(Files.createDirectories(tempDir.resolve("remote")));
        async = Executors.newCachedThreadPool();
    }

    @AfterEach
    void tearDown() {
        async.shutdownNow();
        manager.close();
        db.close();
    }

    // ------------------------------------------------------------------------------------------
    // empty lake (no readable snapshot yet)
    // ------------------------------------------------------------------------------------------

    @Test
    void testEmptyLakeReplaysWholeLogAndMasksSupersededVersions() throws Exception {
        src.append("k1", 0, 0); // +I k1
        src.append("k2", 1, 1); // +I k2
        src.append("k1", 3, 3); // -U k1 @2, +U k1 @3
        src.append("k3", 4, 4); // +I k3
        src.append("k2", 5, -1); // -D k2
        src.append("k1", 7, 7); // -U k1 @6, +U k1 @7
        src.append("k2", 8, 8); // +I k2 again
        highWatermark.set(9);

        manager.recover(src);
        assertThat(manager.getReadableSnapshotId()).isEqualTo(-1L);
        assertThat(manager.logRetainOffset()).isEqualTo(0L);
        // the log is replayed as the bucket becomes leader, before any union read
        awaitCondition(() -> src.replayedFrom.size() == 1, "the empty lake was not replayed");

        DvSnapshot snapshot = unionRead(-1);

        assertThat(src.replayedFrom).containsExactly(0L);
        assertThat(snapshot.getLakeDvEntries()).isEmpty();
        // k1@0 and k1@3 superseded by later updates, k2@1 deleted; k2@8, k3@4, k1@7 live
        assertThat(offsets(snapshot.getLogDvBitmap())).containsExactly(0L, 1L, 3L);
        assertThat(snapshot.getLogEndOffset()).isEqualTo(9L);
        assertThat(snapshot.getSnapshotStartOffset()).isEqualTo(0L);

        // a second read does not replay again
        unionRead(-1);
        assertThat(src.replayedFrom).containsExactly(0L);
    }

    @Test
    void testEmptyLakeMasksOnlyChangesBelowHighWatermark() throws Exception {
        src.append("k1", 0, 0);
        src.append("k1", 2, 2);
        highWatermark.set(3);
        // the leader takes over at log end 3: no read goes below it
        manager.recover(src);
        for (long[] c : new long[][] {{1, 4}, {2, 5}, {2, 6}}) {
            src.append("k" + c[0], c[1], c[1]);
            DvTestManagers.handOver(
                    manager, Collections.singletonList(src.written("k" + c[0], c[1], c[1])));
        }

        DvSnapshot snapshot = unionRead(-1);
        // k1@4 is beyond the high watermark, so it must not mask k1@2
        assertThat(offsets(snapshot.getLogDvBitmap())).containsExactly(0L);
        assertThat(snapshot.getLogEndOffset()).isEqualTo(3L);

        highWatermark.set(5);
        snapshot = unionRead(-1);
        assertThat(offsets(snapshot.getLogDvBitmap())).containsExactly(0L, 2L);
        assertThat(snapshot.getLogEndOffset()).isEqualTo(5L);

        highWatermark.set(7);
        snapshot = unionRead(-1);
        assertThat(offsets(snapshot.getLogDvBitmap())).containsExactly(0L, 2L, 5L);
        assertThat(snapshot.getLogEndOffset()).isEqualTo(7L);
        // replayed once: a final read from where the replay got to may follow its first one
        assertThat(src.replayedFrom).startsWith(0L);
        assertThat(src.replayedFrom.subList(1, src.replayedFrom.size())).doesNotContain(0L);
    }

    @Test
    void testDuplicateDeliveryFromReplayAndWritePathDoesNotMaskLiveVersion() throws Exception {
        src.append("k1", 0, 0);
        src.append("k1", 2, 2);
        highWatermark.set(3);
        manager.recover(src);
        assertThat(offsets(unionRead(-1).getLogDvBitmap())).containsExactly(0L);

        // the write path delivers changes the replay has already applied
        DvTestManagers.handOver(
                manager, Arrays.asList(src.written("k1", 0, 0), src.written("k1", 2, 2)));
        assertThat(offsets(unionRead(-1).getLogDvBitmap())).containsExactly(0L);

        // a new change still masks the version it supersedes
        src.append("k1", 4, 4);
        DvTestManagers.handOver(manager, Collections.singletonList(src.written("k1", 4, 4)));
        highWatermark.set(5);
        assertThat(offsets(unionRead(-1).getLogDvBitmap())).containsExactly(0L, 2L);
    }

    @Test
    void testDuplicateQueuedBeyondHighWatermarkIsAppliedOnce() throws Exception {
        src.append("k1", 0, 0);
        src.append("k1", 2, 2);
        highWatermark.set(1);
        manager.recover(src);
        // k1@2 stays queued (beyond the high watermark); no read is served below the log end the
        // leader took over at
        waitUntil(() -> manager.inspect().changeListPresent);
        assertThat(manager.inspect().queued).isEqualTo(1);

        // the write path delivers k1@2 again while the replayed copy is still queued
        DvTestManagers.handOver(manager, Collections.singletonList(src.written("k1", 2, 2)));
        highWatermark.set(3);
        assertThat(offsets(unionRead(-1).getLogDvBitmap())).containsExactly(0L);
    }

    @Test
    void testWritePathIgnoredUntilEmptyLakeIsTracked() throws Exception {
        // the changes are in the log and are also delivered by the write path while the replay of
        // the empty lake's change list runs
        src.append("k1", 0, 0);
        src.append("k1", 1, 1);
        CountDownLatch gate = pauseReplayAfter(0);
        manager.recover(src);
        try {
            assertThat(src.replayPaused.await(10, TimeUnit.SECONDS)).isTrue();
            DvTestManagers.handOver(
                    manager, Arrays.asList(src.written("k1", 0, 0), src.written("k1", 1, 1)));
            assertThat(manager.inspect().changeCount).isZero();
            assertThat(manager.inspect().queued).isZero();
        } finally {
            gate.countDown();
        }

        highWatermark.set(2);
        assertThat(offsets(unionRead(-1).getLogDvBitmap())).containsExactly(0L);
        assertThat(src.replayedFrom).containsExactly(0L);
    }

    @Test
    void testDeleteMasksPreviousVersionAndReinsertMasksNothing() throws Exception {
        // in the log before the leader takes over, so its replay has them: changes appended later
        // come through the write path
        src.append("k1", 0, 0);
        src.append("k1", 1, -1);
        src.append("k1", 2, 2);
        src.append("k1", 3, -1);
        highWatermark.set(4);
        manager.recover(src);

        DvSnapshot snapshot = unionRead(-1);

        // the deletes have no live version, so nothing beyond the two inserts is masked
        assertThat(offsets(snapshot.getLogDvBitmap())).containsExactly(0L, 2L);
    }

    @Test
    void testStaleSnapshotRequestsOnEmptyLake() throws Exception {
        manager.recover(src);
        List<byte[]> keys = Collections.singletonList(key("k1"));

        // snapshot 5 is not registered: no catch-up, the reader is told to re-plan
        assertThatThrownBy(() -> unionRead(5))
                .isInstanceOfSatisfying(
                        StaleSnapshotException.class,
                        e -> {
                            assertThat(e.getCurrentSnapshotId()).isEqualTo(-1L);
                            assertThat(e.getRequestedSnapshotId()).isEqualTo(5L);
                        });
        assertThatThrownBy(() -> lookupWhenReady(5, keys))
                .isInstanceOf(StaleSnapshotException.class);
        assertThat(manager.getReadableSnapshotId()).isEqualTo(-1L);
    }

    // ------------------------------------------------------------------------------------------
    // covered lake snapshot
    // ------------------------------------------------------------------------------------------

    @Test
    void testRecoverFromIndexChainMasksLakeAndTailVersions() throws Exception {
        recoverAtSnapshot100();

        assertThat(src.replayedFrom).containsExactly(10L);
        assertThat(src.downloadedIndexIds).containsExactly(1L);
        assertThat(manager.getReadableSnapshotId()).isEqualTo(100L);
        assertThat(manager.logRetainOffset()).isEqualTo(10L);

        DvSnapshot snapshot = unionRead(100);
        // k1 (updated) and k2 (deleted) changed in [10, 14): their lake rows are masked; k3's
        // change @15 is beyond the high watermark, so its lake row stays visible
        assertThat(lakeMasks(snapshot)).containsOnly(entry(F1, set(0L, 1L)));
        assertThat(offsets(snapshot.getLogDvBitmap())).containsExactly(10L);
        assertThat(snapshot.getLogEndOffset()).isEqualTo(14L);
        assertThat(snapshot.getSnapshotStartOffset()).isEqualTo(10L);

        highWatermark.set(16);
        snapshot = unionRead(100);
        assertThat(lakeMasks(snapshot)).containsOnly(entry(F1, set(0L, 1L, 2L)));
        assertThat(offsets(snapshot.getLogDvBitmap())).containsExactly(10L);
        assertThat(snapshot.getLogEndOffset()).isEqualTo(16L);
    }

    @Test
    void testFailedTailReplayFailsUnionReadsUntilReplaySucceeds() throws Exception {
        src.replayFailure = new IllegalStateException("log gone");
        recoverAtSnapshot100();

        // the index is in place, so lookups still work
        assertThat(asStrings(lookupWhenReady(100, Collections.singletonList(key("k1")))))
                .containsExactly("k1@" + F1 + "#0");
        // masks without the tail would miss k1 and k2
        assertThatThrownBy(() -> unionRead(100))
                .isInstanceOf(IOException.class)
                .hasRootCauseMessage("log gone");
        assertThatThrownBy(() -> unionRead(100)).isInstanceOf(IOException.class);

        src.replayFailure = null;
        DvSnapshot snapshot = unionRead(100);
        assertThat(lakeMasks(snapshot)).containsOnly(entry(F1, set(0L, 1L)));
        assertThat(offsets(snapshot.getLogDvBitmap())).containsExactly(10L);
        assertThat(snapshot.getSnapshotStartOffset()).isEqualTo(10L);
    }

    @Test
    void testFailedHandOffReplaysTheTailFromTheLog() throws Exception {
        recoverAtSnapshot100();
        assertThat(lakeMasks(unionRead(100))).containsOnly(entry(F1, set(0L, 1L)));

        // the write path appended k3's change but could not hand it over
        highWatermark.set(16);
        manager.handOffFailed(new IllegalStateException("hand-off failed"));
        // a later change is not taken until the replay, which reads it from the log as well
        src.append("k4", 16, 16);
        DvTestManagers.handOver(manager, Collections.singletonList(src.written("k4", 16, 16)));
        highWatermark.set(17);

        DvSnapshot snapshot = unionRead(100);
        assertThat(src.replayedFrom).containsExactly(10L, 10L);
        assertThat(lakeMasks(snapshot)).containsOnly(entry(F1, set(0L, 1L, 2L)));
        // k1's version @10 and k4's version @11 are superseded
        assertThat(offsets(snapshot.getLogDvBitmap())).containsExactly(10L, 11L);
        assertThat(snapshot.getLogEndOffset()).isEqualTo(17L);
    }

    @Test
    void testRecoverOfLaggingLakeQueuesOnlyThePartAboveTheHighWatermark() throws Exception {
        // The lake lags far behind the log (a tiering backlog or a bulk load), so the tail spans
        // many more changes than keys. A rebuild applies the replayed changes as the high
        // watermark passes them, so only the rest waits in the queue. The applied updates stay,
        // one per change, in the bucket's RocksDB.
        int keys = 100;
        int changes = 50_000;
        long start = 10;
        src.publish(100, 1, start, dict(1, F1), new Index().put("k0", 1, 0).build());
        src.readable = new long[] {100, start};
        appendChanges(start, keys, changes);
        long hw = start + changes - 5; // the last 5 changes are not replicated yet
        highWatermark.set(hw);
        src.queueProbe = manager::queuedChanges;

        manager.recover(src);

        assertThat(src.maxQueuedDuringReplay).isLessThan(changes / 10);
        // only the changes above the high watermark wait
        assertThat(manager.queuedChanges()).isEqualTo(5);
        assertThat(manager.inspect().changeCount).isEqualTo(changes - 5);
        // no read is served below the log end the leader took over at, so masks are read once the
        // high watermark reaches it: every change but the last of each key is superseded
        highWatermark.set(start + changes);
        DvSnapshot snapshot = unionRead(100);
        assertThat(lakeMasks(snapshot)).containsOnly(entry(F1, set(0L)));
        Set<Long> masked = offsets(snapshot.getLogDvBitmap());
        assertThat(masked).hasSize(changes - keys).contains(start);
    }

    @Test
    void testEmptyLakeReplayHoldsOneEntryPerKeyNotPerChange() throws Exception {
        int keys = 10;
        int changes = 50_000;
        appendChanges(0, keys, changes);
        highWatermark.set(changes);
        manager.recover(src);
        src.queueProbe = manager::queuedChanges;

        DvSnapshot snapshot = unionRead(-1);

        assertThat(src.maxQueuedDuringReplay).isLessThan(changes / 10);
        assertThat(offsets(snapshot.getLogDvBitmap())).hasSize(changes - keys);
    }

    @Test
    void testReplayFailingMidwayLeavesNoPartialTail() throws Exception {
        int keys = 100;
        int changes = 50_000;
        long start = 10;
        src.publish(100, 1, start, dict(1, F1), new Index().put("k0", 1, 0).build());
        src.readable = new long[] {100, start};
        appendChanges(start, keys, changes);
        highWatermark.set(start + changes);
        src.replayFailure = new IllegalStateException("remote segment gone");
        src.failAfterChanges = changes / 2;

        manager.recover(src);

        // a partial tail would serve masks that miss the unreplayed changes
        DvInspection state = manager.inspect();
        assertThat(state.changeCount).as("%s", state).isZero();
        assertThat(state.queued).as("%s", state).isZero();
        assertThat(state.deadVersions).as("%s", state).isZero();
        assertThatThrownBy(() -> unionRead(100))
                .isInstanceOf(IOException.class)
                .hasRootCauseMessage("remote segment gone");

        src.replayFailure = null;
        DvSnapshot snapshot = unionRead(100);
        assertThat(lakeMasks(snapshot)).containsOnly(entry(F1, set(0L)));
        assertThat(offsets(snapshot.getLogDvBitmap())).hasSize(changes - keys);
    }

    @Test
    void testLookupReturnsPositionsInCoveredSnapshot() throws Exception {
        recoverAtSnapshot100();

        List<Object[]> found =
                lookupWhenReady(100, Arrays.asList(key("k1"), key("k2"), key("k3"), key("k9")));

        assertThat(asStrings(found))
                .containsExactly("k1@" + F1 + "#0", "k2@" + F1 + "#1", "k3@" + F1 + "#2");
        assertThatThrownBy(() -> lookupWhenReady(99, Collections.singletonList(key("k1"))))
                .isInstanceOf(StaleSnapshotException.class);
        assertThatThrownBy(() -> unionRead(-1)).isInstanceOf(StaleSnapshotException.class);
    }

    @Test
    void testUnionReadOfNewerSnapshotCatchesUpIncrementally() throws Exception {
        recoverAtSnapshot100();
        highWatermark.set(16);
        unionRead(100);

        // round 2 tiers [10, 14): k1@12 -> f2#0, k4@11 -> f2#1, k2 deleted
        src.publish(
                200,
                2,
                14,
                dict(2, F2),
                new Index().put("k1", 2, 0).put("k4", 2, 1).delete("k2").build());

        DvSnapshot snapshot = unionRead(200);

        assertThat(manager.getReadableSnapshotId()).isEqualTo(200L);
        assertThat(src.downloadedIndexIds).containsExactly(1L, 2L);
        // No log replay on an incremental advance, only the recovery's.
        assertThat(src.replayedFrom).containsExactly(10L);
        assertThat(snapshot.getSnapshotStartOffset()).isEqualTo(14L);
        // only k3 changed since 14: its lake row f1#2 is masked; tail masks below 14 are dropped
        assertThat(lakeMasks(snapshot)).containsOnly(entry(F1, set(2L)));
        assertThat(snapshot.getLogDvBitmap()).isNull();

        // a new change of k4 masks its row in the new snapshot
        src.append("k4", 16, 16);
        DvTestManagers.handOver(manager, Collections.singletonList(src.written("k4", 16, 16)));
        highWatermark.set(17);
        snapshot = unionRead(200);
        assertThat(lakeMasks(snapshot)).containsOnly(entry(F1, set(2L)), entry(F2, set(1L)));

        assertThat(
                        asStrings(
                                lookupWhenReady(
                                        200,
                                        Arrays.asList(key("k1"), key("k2"), key("k3"), key("k4")))))
                .containsExactly("k1@" + F2 + "#0", "k3@" + F1 + "#2", "k4@" + F2 + "#1");

        // the older snapshot is gone for good
        assertThatThrownBy(() -> unionRead(100)).isInstanceOf(StaleSnapshotException.class);
        assertThatThrownBy(() -> lookupWhenReady(100, Collections.singletonList(key("k1"))))
                .isInstanceOf(StaleSnapshotException.class);
        assertThat(manager.ensureCovered(100)).isFalse();
        assertThat(manager.getReadableSnapshotId()).isEqualTo(200L);
    }

    @Test
    void testCatchUpToLowerReadableOffsetReplaysTheLog() throws Exception {
        // A lake whose readable offsets lag its commits (Paimon) can register a snapshot read
        // from below the covered snapshot's offset. The tail from there is not held, so it is
        // replayed.
        src.append("k5", 8, 8);
        recoverAtSnapshot100();
        highWatermark.set(16);
        unionRead(100);
        assertThat(src.replayedFrom).containsExactly(10L);

        src.publish(200, 2, 8, dict(2, F2), new Index().put("k5", 2, 0).build());
        DvSnapshot snapshot = unionRead(200);

        assertThat(manager.logRetainOffset()).isEqualTo(8L);
        assertThat(src.replayedFrom).containsExactly(10L, 8L);
        assertThat(snapshot.getSnapshotStartOffset()).isEqualTo(8L);
        // k5 changed at 8, so its row in the new snapshot is masked
        assertThat(lakeMasks(snapshot)).containsKey(F2);
        assertThat(lakeMasks(snapshot).get(F2)).isEqualTo(set(0L));
    }

    @Test
    void testAdvanceAsyncCatchesUpInBackground() throws Exception {
        recoverAtSnapshot100();
        src.publish(200, 2, 14, dict(2, F2), new Index().put("k1", 2, 0).build());

        manager.advanceAsync(200);

        long deadline = System.currentTimeMillis() + 10_000;
        while (manager.getReadableSnapshotId() != 200 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertThat(manager.getReadableSnapshotId()).isEqualTo(200L);
        assertThat(manager.logRetainOffset()).isEqualTo(14L);
    }

    @Test
    void testEnsureCoveredWaitsForRegistrationAndChainMarker() throws Exception {
        recoverAtSnapshot100();

        // not registered in ZooKeeper yet
        assertThat(manager.ensureCovered(300)).isFalse();
        // registered, but its chain marker is not visible yet
        src.snapshotOffsets.put(300L, 20L);
        assertThat(manager.ensureCovered(300)).isFalse();
        assertThat(manager.getReadableSnapshotId()).isEqualTo(100L);
        assertThat(manager.ensureCovered(100)).isTrue();
    }

    @Test
    void testRecoverWithNothingTieredForBucketSkipsIndexDownloads() throws Exception {
        src.publish(100, 1, 0, dict(1, F1), new Index().put("k1", 1, 0).build());
        src.readable = new long[] {100, 0};
        src.append("k1", 0, 0);
        src.append("k1", 1, 1);
        highWatermark.set(2);

        manager.recover(src);

        assertThat(src.downloadedIndexIds).isEmpty();
        DvSnapshot snapshot = unionRead(100);
        assertThat(snapshot.getLakeDvEntries()).isEmpty();
        assertThat(offsets(snapshot.getLogDvBitmap())).containsExactly(0L);
    }

    @Test
    void testRecoverRejectsSnapshotMissingFromChain() {
        src.readable = new long[] {100, 10};
        assertThatThrownBy(() -> manager.recover(src))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not in the published index chain");
    }

    @Test
    void testRecoverRejectsTrimmedChainWithoutCheckpoint() {
        src.publish(100, 1, 10, dict(1, F1), new Index().put("k1", 1, 0).build());
        src.readable = new long[] {100, 10};
        src.trimmed = true;
        assertThatThrownBy(() -> manager.recover(src))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("trimmed");
        assertThat(src.downloadedIndexIds).isEmpty();
    }

    @Test
    void testRecoverFromCheckpointReplaysOnlyLaterChainEntries() throws Exception {
        src.publish(100, 1, 10, dict(1, F1), new Index().put("k1", 1, 0).put("k2", 1, 1).build());
        src.publish(200, 2, 20, dict(2, F2), new Index().put("k2", 2, 0).build());
        src.publish(300, 3, 30, dict(3, F3), new Index().put("k3", 3, 0).delete("k1").build());
        src.trimmed = true;
        src.checkpoint = new PositionIndexStore.Checkpoint(1, 200); // chain index 1 = snapshot 200
        src.checkpointIndex = new Index().put("k1", 1, 0).put("k2", 2, 0).build();
        Map<Integer, String> ckptDict = new HashMap<>();
        ckptDict.put(1, F1);
        ckptDict.put(2, F2);
        src.checkpointDict = ckptDict;
        src.readable = new long[] {300, 30};

        manager.recover(src);

        assertThat(src.downloadedCheckpoints).containsExactly(200L);
        assertThat(src.downloadedIndexIds).containsExactly(3L);
        assertThat(asStrings(lookupWhenReady(300, Arrays.asList(key("k1"), key("k2"), key("k3")))))
                .containsExactly("k2@" + F2 + "#0", "k3@" + F3 + "#0");
    }

    @Test
    void testCheckpointExportsExactlyTheCoveredIndex() throws Exception {
        int every = publishLongChain();

        manager.recover(src);

        assertThat(src.uploaded.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(src.uploadedSnapshotId).isEqualTo(every);
        assertThat(src.uploadedDict).containsOnly(entry(1, F1), entry(2, F2));
        assertThat(src.uploadedSst).isNotNull();
        // exported on the tablet's disk, next to the DV RocksDB, and deleted after the upload
        assertThat(src.uploadedFrom).startsWith(tempDir.resolve("dv-tmp").toString());
        waitUntil(() -> !Files.exists(Paths.get(src.uploadedFrom)));
        // k3 was deleted by the last index change: the checkpoint holds the live entries only
        assertThat(readSst(src.uploadedSst))
                .containsExactly(entry("k1", new FilePos(1, 0)), entry("k2", new FilePos(2, 0)));

        // a rebuild from the checkpoint answers like the chain replay
        Path copy = tempDir.resolve("ingest-copy.sst");
        Files.copy(src.uploadedSst, copy);
        try (DvRocksDB other = DvRocksDB.open(tempDir.resolve("other").toString())) {
            other.rowPosIndex().ingestExternalFile(Collections.singletonList(copy.toString()));
            List<byte[]> keys = Arrays.asList(key("k1"), key("k2"), key("k3"));
            assertThat(other.rowPosIndex().multiGetByKeys(keys))
                    .isEqualTo(db.rowPosIndex().multiGetByKeys(keys))
                    .containsExactly(new FilePos(1, 0), new FilePos(2, 0), null);
        }
    }

    /**
     * A checkpoint releases its exported RocksDB snapshot before the upload. An upload can outlast
     * the wait of a closing tablet, on an object store in particular, so closing does not wait for
     * it and the RocksDB is never closed under the snapshot.
     */
    @Test
    void testCheckpointReleasesTheIndexBeforeItsUpload() throws Exception {
        publishLongChain();
        CountDownLatch gate = new CountDownLatch(1);
        CountDownLatch uploading = new CountDownLatch(1);
        AtomicLong snapshotsAtUpload = new AtomicLong(-1);
        src.uploadGate = gate;
        src.onUpload =
                () -> {
                    snapshotsAtUpload.set(openRocksDbSnapshots(db));
                    uploading.countDown();
                };
        try {
            manager.recover(src);
            assertThat(uploading.await(30, TimeUnit.SECONDS)).isTrue();
            // only the covered version's own
            assertThat(snapshotsAtUpload).hasValue(1L);

            long start = System.nanoTime();
            manager.close();
            assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)).isLessThan(5_000L);
        } finally {
            // the upload ends before the tablet's RocksDB is closed below
            gate.countDown();
            assertThat(src.uploaded.await(60, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(src.uploadedSnapshotId).isEqualTo(DvTestManagers.CHECKPOINT_INTERVAL);
    }

    /**
     * A checkpoint whose task the executor rejects, e.g. while it shuts down, releases the RocksDB
     * snapshot and its use of the RocksDB, so the bucket still closes its RocksDB.
     */
    @Test
    void testRejectedCheckpointReleasesTheIndex() throws Exception {
        int every = publishLongChain();
        ScheduledExecutorService rejecting = Executors.newSingleThreadScheduledExecutor();
        rejecting.shutdown();
        manager.useCheckpointTimer(rejecting);
        assertThatCode(() -> manager.recover(src)).doesNotThrowAnyException();
        assertThat(manager.getReadableSnapshotId()).isEqualTo((long) every);
        // only the covered version's own
        assertThat(openRocksDbSnapshots(db)).isEqualTo(1L);

        long start = System.nanoTime();
        manager.close();
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)).isLessThan(5_000L);
        // closed: the RocksDB opens again
        try (DvRocksDB reopened = DvRocksDB.open(tempDir.resolve("dv").toString())) {
            assertThat(reopened.fileDict().getFilePath(2)).isEqualTo(F2);
        }
    }

    /**
     * A switch while the checkpoint of the snapshot before still runs skips its own checkpoint. It
     * is checkpointed once that one ends, without another switch.
     */
    @Test
    void testASwitchDuringACheckpointIsCheckpointedWhenItEnds() throws Exception {
        List<Runnable> starts = new CopyOnWriteArrayList<>();
        ScheduledExecutorService holding =
                new ScheduledThreadPoolExecutor(1) {
                    @Override
                    public ScheduledFuture<?> schedule(
                            Runnable command, long delay, TimeUnit unit) {
                        starts.add(command);
                        return null;
                    }
                };
        // a checkpoint after every index change
        DerivedDvManager bucket =
                DvTestManagers.newManager(
                        DvRocksDB.open(tempDir.resolve("dv-every").toString()),
                        highWatermark::get,
                        1,
                        DerivedDvMetrics.noop());
        CountDownLatch uploading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            bucket.useCheckpointTimer(holding);
            src.publish(100, 1, 10, dict(1, F1), new Index().put("k1", 1, 0).build());
            src.readable = new long[] {100, 10};
            highWatermark.set(10);
            bucket.recover(src);
            // the checkpoint of 100 runs, held in its upload
            src.onUpload = uploading::countDown;
            src.uploadGate = release;
            awaitCondition(() -> starts.size() == 1, "the checkpoint of 100 did not start");
            starts.remove(0).run();
            assertThat(uploading.await(30, TimeUnit.SECONDS)).isTrue();

            // the switch to 101 swaps while it runs
            src.publish(101, 2, 12, dict(2, F2), new Index().put("k1", 2, 0).build());
            src.readable = new long[] {101, 12};
            assertThat(bucket.ensureCovered(101)).isTrue();
            assertThat(starts).isEmpty();

            // the checkpoint of 100 ends, and 101 starts its own
            src.onUpload = null;
            src.uploadGate = null;
            release.countDown();
            awaitCondition(() -> starts.size() == 1, "the checkpoint of 101 did not start");
            starts.remove(0).run();
            awaitCondition(() -> src.uploadedSnapshotId == 101, "101 was not checkpointed");
        } finally {
            release.countDown();
            holding.shutdown();
            bucket.setCloseWaitMillis(1000);
            bucket.close();
        }
    }

    /**
     * A checkpoint starts after a random delay. A bucket that advanced meanwhile exports the index
     * it covers when the checkpoint starts, so a bucket that advances more often than the delay
     * still checkpoints.
     */
    @Test
    void testDelayedCheckpointExportsTheIndexCoveredWhenItStarts() throws Exception {
        int every = publishLongChain();
        List<Runnable> delayed = new CopyOnWriteArrayList<>();
        ScheduledExecutorService holding =
                new ScheduledThreadPoolExecutor(1) {
                    @Override
                    public ScheduledFuture<?> schedule(
                            Runnable command, long delay, TimeUnit unit) {
                        delayed.add(command);
                        return null;
                    }
                };
        try {
            manager.useCheckpointTimer(holding);
            manager.recover(src);
            assertThat(delayed).hasSize(1);

            src.publish(
                    every + 1,
                    1000 + every + 1,
                    every + 1,
                    dict(3, F3),
                    new Index().put("k4", 3, 0).build());
            assertThat(manager.ensureCovered(every + 1)).isTrue();
            delayed.get(0).run();

            assertThat(src.uploaded.await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(src.uploadedSnapshotId).isEqualTo(every + 1);
            assertThat(src.uploadedDict).containsOnly(entry(1, F1), entry(2, F2), entry(3, F3));
            assertThat(readSst(src.uploadedSst))
                    .containsExactly(
                            entry("k1", new FilePos(1, 0)),
                            entry("k2", new FilePos(2, 0)),
                            entry("k4", new FilePos(3, 0)));
        } finally {
            holding.shutdownNow();
        }
    }

    /**
     * A background resolve of a long tail looks keys up in bounded chunks and stops at the next
     * chunk once the bucket closes. Closing does not wait for a lookup of every key, and the
     * RocksDB closes at once with nothing reading it afterwards.
     */
    @Test
    void testCloseStopsALargeBackgroundResolve() throws Exception {
        int keys = 2_000_000;
        TreeMap<byte[], FilePos> index = new TreeMap<>(DerivedDvManagerTest::compareUnsigned);
        for (int i = 0; i < keys; i++) {
            index.put(key(String.format("k%07d", i)), new FilePos(1, i));
        }
        src.publish(100, 1, 10, dict(1, F1), index);
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        manager.recover(src);
        index.clear();

        List<DvEntry> changes = new ArrayList<>(keys);
        for (int i = 0; i < keys; i++) {
            changes.add(new DvEntry(key(String.format("k%07d", i)), 10 + i, 10 + i, false, -1));
        }
        highWatermark.set(10L + keys);
        // the hand-off owns the list from here on
        DvTestManagers.handOver(manager, changes);
        // The changes are applied at once; the resolver starts looking their keys up a moment
        // later, on its own thread.
        waitUntil(manager::resolving);

        long start = System.nanoTime();
        manager.close();
        long closeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertThat(db.isClosed()).isTrue();
        assertThat(closeMs).isLessThan(500L);
    }

    /** A lookup of more than 100,000 keys is refused. The tiering service sends 50,000 at most. */
    @Test
    void testLookupOfTooManyKeysIsRefused() throws Exception {
        recoverAtSnapshot100();
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0; i <= 100_000; i++) {
            keys.add(key("k" + i));
        }
        assertThatThrownBy(() -> manager.lookup(100, keys))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining("100001 keys");
        assertThat(asStrings(manager.lookup(100, keys.subList(0, 100_000)))).hasSize(3);
    }

    /**
     * A union read waits for a background resolve at most a moment, then is answered as not ready
     * instead of holding its request thread until the resolve ends.
     */
    @Test
    void testUnionReadDoesNotWaitOutALargeBackgroundResolve() throws Exception {
        int keys = 2_000_000;
        TreeMap<byte[], FilePos> index = new TreeMap<>(DerivedDvManagerTest::compareUnsigned);
        for (int i = 0; i < keys; i++) {
            index.put(key(String.format("k%07d", i)), new FilePos(1, i));
        }
        src.publish(100, 1, 10, dict(1, F1), index);
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        manager.recover(src);
        index.clear();
        DvTestReads.unionRead(manager, 100);

        List<DvEntry> changes = new ArrayList<>(keys);
        for (int i = 0; i < keys; i++) {
            changes.add(new DvEntry(key(String.format("k%07d", i)), 10 + i, 10 + i, false, -1));
        }
        highWatermark.set(10L + keys);
        // the hand-off owns the list from here on
        DvTestManagers.handOver(manager, changes);
        // The changes are applied at once; the resolver starts looking their keys up a moment
        // later, on its own thread, and takes much longer than a union read waits.
        waitUntil(manager::resolving);

        assertThatThrownBy(() -> DvTestReads.unionRead(manager, 100))
                .isInstanceOf(DeletionVectorsNotReadyException.class);
        // answered while the resolve still runs, not once it ended
        assertThat(manager.resolving()).isTrue();
        assertThat(lakeMasks(unionRead(100)).get(F1)).hasSize(keys);
    }

    /** The number of RocksDB snapshots of the DV RocksDB that are not released. */
    private static long openRocksDbSnapshots(DvRocksDB dvRocksDB) {
        return dvRocksDB.openSnapshots();
    }

    /**
     * A background resolve that fails with an unexpected exception still ends its accounting: union
     * reads do not wait for it, its keys are looked up again, and the bucket closes at once.
     */
    @Test
    void testResolveFailingUnexpectedlyStillEnds() throws Exception {
        src.publish(100, 1, 10, dict(1, F1), new Index().put("k1", 1, 0).put("k2", 1, 1).build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        manager.recover(src);
        assertThat(lakeMasks(unionRead(100))).isEmpty();

        AtomicInteger failures = new AtomicInteger(1);
        hooks(manager)
                .setBeforeResolveLookup(
                        () -> {
                            if (failures.getAndDecrement() > 0) {
                                throw new IllegalStateException("injected");
                            }
                        });
        // an update of k1 supersedes its row in the lake, which the resolver looks up
        DvTestManagers.handOver(
                manager, Collections.singletonList(new DvEntry(key("k1"), 10, 10, false, -1)));
        highWatermark.set(11);
        waitUntil(() -> failures.get() < 0);

        assertThat(lakeMasks(unionRead(100)).get(F1)).containsExactly(0L);
        long start = System.nanoTime();
        manager.close();
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)).isLessThan(1000L);
        assertThat(manager.dbClosed()).isDone();
    }

    @Test
    void testCloseReturnsPromptlyWhenIdle() throws Exception {
        src.append("k1", 0, 0);
        src.append("k1", 1, 1);
        highWatermark.set(2);
        manager.recover(src);
        unionRead(-1);
        // a change beyond the high watermark keeps the background resolver rescheduling
        src.append("k1", 5, 5);
        DvTestManagers.handOver(manager, Collections.singletonList(src.written("k1", 5, 5)));
        Thread.sleep(20);

        long start = System.nanoTime();
        manager.close();
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)).isLessThan(1000L);
    }

    /**
     * A union read of a snapshot the bucket must rebuild to is answered at once as not ready, and
     * retriable, while the rebuild runs in the background. A rebuild on the request thread would
     * hold a handler thread for as long as the log replay takes.
     */
    @Test
    void testUnionReadDoesNotRebuildOnTheRequestThread() throws Exception {
        recoverEmptyLake();
        src.publish(100, 1, 10, dict(1, F1), new Index().put("k1", 1, 0).put("k2", 1, 1).build());
        src.readable = new long[] {100, 10};
        src.append("k1", 10, 10);
        src.append("k1", 11, 11);
        highWatermark.set(12);
        CountDownLatch gate = pauseReplayAfter(1);
        try {
            CompletableFuture<DvSnapshot> read =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return DvTestReads.unionRead(manager, 100);
                                } catch (Exception e) {
                                    throw new CompletionException(e);
                                }
                            },
                            async);
            assertThatThrownBy(() -> read.get(3, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(RetriableException.class);
            // the rebuild runs in the background meanwhile
            assertThat(src.replayPaused.await(10, TimeUnit.SECONDS)).isTrue();
        } finally {
            gate.countDown();
        }
        DvSnapshot snapshot = unionRead(100);
        assertThat(lakeMasks(snapshot)).containsOnly(entry(F1, set(0L)));
        assertThat(offsets(snapshot.getLogDvBitmap())).containsExactly(10L);
    }

    /**
     * A background catch-up that fails, e.g. on a storage error listing the chain, leaves the
     * request retriable and runs again. The snapshot is not answered as stale until a newer one is
     * covered.
     */
    @Test
    void testFailedCatchUpIsRetriedNotStale() throws Exception {
        recoverAtSnapshot100();
        src.publish(200, 2, 14, dict(2, F2), new Index().put("k1", 2, 0).build());
        src.chainFailure = new IOException("listing the chain failed");

        assertThatThrownBy(() -> DvTestReads.unionRead(manager, 200))
                .isInstanceOf(RetriableException.class)
                .hasMessageContaining("listing the chain failed");
        assertThat(manager.getReadableSnapshotId()).isEqualTo(100L);

        DvSnapshot snapshot = unionRead(200);
        assertThat(manager.getReadableSnapshotId()).isEqualTo(200L);
        assertThat(snapshot.getSnapshotStartOffset()).isEqualTo(14L);
        assertThat(asStrings(lookupWhenReady(200, Collections.singletonList(key("k1")))))
                .containsExactly("k1@" + F2 + "#0");
    }

    /** Neither does a tiering lookup catch up on the request thread. */
    @Test
    void testLookupDoesNotCatchUpOnTheRequestThread() throws Exception {
        recoverEmptyLake();
        src.publish(100, 1, 10, dict(1, F1), new Index().put("k1", 1, 0).build());
        src.readable = new long[] {100, 10};
        src.append("k1", 10, 10);
        highWatermark.set(11);
        CountDownLatch gate = pauseReplayAfter(0);
        try {
            CompletableFuture<List<Object[]>> lookup =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return manager.lookup(
                                            100, Collections.singletonList(key("k1")));
                                } catch (Exception e) {
                                    throw new CompletionException(e);
                                }
                            },
                            async);
            assertThatThrownBy(() -> lookup.get(3, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(RetriableException.class);
        } finally {
            gate.countDown();
        }
        assertThat(asStrings(lookupWhenReady(100, Collections.singletonList(key("k1")))))
                .containsExactly("k1@" + F1 + "#0");
    }

    /**
     * Catch-up threads never wait for a bucket's rebuild: notifications for a bucket that is
     * rebuilding do not hold up the catch-up of another bucket of the tablet server.
     */
    @Test
    void testCatchUpsOfABusyBucketDoNotHoldUpOthers() throws Exception {
        src.publish(100, 1, 10, dict(1, F1), new Index().put("k1", 1, 0).build());
        src.readable = new long[] {100, 10};
        src.append("k1", 10, 10);
        highWatermark.set(11);
        CountDownLatch gate = pauseReplayAfter(0);
        CompletableFuture<Void> recovery =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                manager.recover(src);
                            } catch (Exception e) {
                                throw new CompletionException(e);
                            }
                        },
                        async);
        FakeSource srcB = new FakeSource(Files.createDirectories(tempDir.resolve("remote-b")));
        srcB.publish(100, 1, 10, dict(1, F1), new Index().put("k1", 1, 0).build());
        srcB.readable = new long[] {100, 10};
        try (DvRocksDB dbB = DvRocksDB.open(tempDir.resolve("dv-b").toString())) {
            DerivedDvManager managerB =
                    DvTestManagers.newManager(
                            dbB, () -> 20L, DvTestManagers.CHECKPOINT_INTERVAL, metrics);
            try {
                assertThat(src.replayPaused.await(10, TimeUnit.SECONDS)).isTrue();
                // notifications for the rebuilding bucket, more than there are catch-up threads
                for (int i = 0; i < 4; i++) {
                    manager.advanceAsync(200 + i);
                }
                managerB.recover(srcB);
                srcB.publish(200, 2, 12, dict(2, F2), new Index().put("k2", 2, 0).build());
                managerB.advanceAsync(200);
                long deadline = System.currentTimeMillis() + 5_000;
                while (managerB.getReadableSnapshotId() != 200L
                        && System.currentTimeMillis() < deadline) {
                    Thread.sleep(10);
                }
                assertThat(managerB.getReadableSnapshotId()).isEqualTo(200L);
            } finally {
                gate.countDown();
                managerB.close();
            }
        }
        recovery.get(30, TimeUnit.SECONDS);
        assertThat(manager.getReadableSnapshotId()).isEqualTo(100L);
    }

    @Test
    void testFirstSnapshotIsReachedByRebuildFromDurableState() throws Exception {
        recoverEmptyLake();
        src.publish(100, 1, 10, dict(1, F1), new Index().put("k1", 1, 0).put("k2", 1, 1).build());
        src.readable = new long[] {100, 10};
        src.append("k1", 10, 10);
        highWatermark.set(11);

        assertThat(manager.ensureCovered(100)).isTrue();

        assertThat(manager.getReadableSnapshotId()).isEqualTo(100L);
        assertThat(lakeMasks(unionRead(100))).containsOnly(entry(F1, set(0L)));
        // covering the same snapshot again is a no-op; the empty lake's log was replayed first
        assertThat(manager.ensureCovered(100)).isTrue();
        assertThat(src.replayedFrom).containsExactly(0L, 10L);
    }

    @Test
    void testRecoverAndCatchUpThroughTheIndexStore() throws Exception {
        // the tiering-side builder writes into the real store and the manager reads it
        PositionIndexStore store =
                PositionIndexStore.forTable(new FsPath(tempDir.resolve("lake").toUri()), null);
        TableBucket bucket = new TableBucket(1L, 0);
        src.useStore(store, bucket);
        new PositionIndexChangeBuilder(store)
                .add(bucket, Collections.singletonMap("f1.parquet", 1))
                .put(bucket, key("k2"), "f1.parquet", 1)
                .put(bucket, key("k1"), "f1.parquet", 0)
                .commit(7);
        store.register(-1, 100, 7);
        src.snapshotOffsets.put(100L, 10L);
        src.readable = new long[] {100, 10};
        src.append("k1", 0, 0);
        src.append("k2", 1, 1);
        src.append("k1", 10, 10);
        highWatermark.set(11);

        manager.recover(src);

        // k1 changed at 10 (after the snapshot): its lake row is masked
        assertThat(lakeMasks(unionRead(100))).containsOnly(entry("f1.parquet", set(0L)));

        // round 2 tiers [10, 11): k1 moves to f2#0; the bucket catches up to it from the chain
        new PositionIndexChangeBuilder(store)
                .add(bucket, Collections.singletonMap("f2.parquet", 2))
                .put(bucket, key("k1"), "f2.parquet", 0)
                .commit(8);
        store.register(100, 200, 8);
        src.snapshotOffsets.put(200L, 11L);
        assertThat(manager.ensureCovered(200)).isTrue();

        DvSnapshot snapshot = unionRead(200);
        assertThat(snapshot.getSnapshotStartOffset()).isEqualTo(11L);
        assertThat(lakeMasks(snapshot)).isEmpty();
        assertThat(asStrings(lookupWhenReady(200, Arrays.asList(key("k1"), key("k2")))))
                .containsExactly("k1@f2.parquet#0", "k2@f1.parquet#1");
    }

    @Test
    void testCatchUpDropsFilesTheIndexChangeRemoves() throws Exception {
        recoverAtSnapshot100();
        src.publish(200, 2, 10, Collections.emptyMap(), new Index().build());
        src.replaced.put(2L, Collections.singletonList(F1));

        assertThat(manager.ensureCovered(200)).isTrue();

        assertThat(db.fileDict().getFilePath(1)).isNull();
        assertThat(lookupWhenReady(200, Arrays.asList(key("k1"), key("k3")))).isEmpty();
    }

    /**
     * An older rewrite applied after a newer round, e.g. out of order or twice, keeps the newer
     * position: the round's file was added after every file of the rewrite.
     */
    @Test
    void testRewriteAppliedAfterANewerRoundKeepsTheNewerPosition() throws Exception {
        recoverAtSnapshot100();
        // round 200 moves k1 into f3, the newest file
        src.publish(200, 3, 14, dict(3, F3), new Index().put("k1", 3, 0).build());
        // a rewrite of f1 into f2, whose file ids were allocated before the round's
        src.publish(
                300,
                2,
                14,
                dict(2, F2),
                new Index().put("k1", 2, 0).put("k2", 2, 1).put("k3", 2, 2).build());
        src.replaced.put(2L, Collections.singletonList(F1));

        assertThat(manager.ensureCovered(300)).isTrue();

        assertThat(asStrings(lookupWhenReady(300, Arrays.asList(key("k1"), key("k3")))))
                .containsExactly("k1@" + F3 + "#0", "k3@" + F2 + "#2");
    }

    /**
     * A rewrite also moves keys that point outside its removed files, or nowhere: a Paimon
     * compaction merges level-0 files, which are not indexed, into its output.
     */
    @Test
    void testRewriteMovesKeysOfFilesItDoesNotRemove() throws Exception {
        src.publish(
                100,
                1,
                10,
                dict(1, F1),
                new Index().put("k1", 1, 0).put("k2", 1, 1).put("k3", 1, 2).build());
        src.publish(200, 2, 10, dict(2, F2), new Index().put("k4", 2, 0).build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        manager.recover(src);
        assertThat(manager.ensureCovered(200)).isTrue();
        // compacts f2 and level-0 rows of k1 and k5 into f3; f1 stays
        src.publish(
                300,
                3,
                10,
                dict(3, F3),
                new Index().put("k1", 3, 0).put("k4", 3, 1).put("k5", 3, 2).build());
        src.replaced.put(3L, Collections.singletonList(F2));

        assertThat(manager.ensureCovered(300)).isTrue();

        assertThat(
                        asStrings(
                                lookupWhenReady(
                                        300,
                                        Arrays.asList(key("k1"), key("k2"), key("k4"), key("k5")))))
                .containsExactly(
                        "k1@" + F3 + "#0", "k2@" + F1 + "#1", "k4@" + F3 + "#1", "k5@" + F3 + "#2");
    }

    /**
     * A read that does not wait for catch-ups and asks for the snapshot registered right after the
     * covered one is told at once that the leader still catches up, and reads the covered snapshot
     * with the masks a waiting read gets. A snapshot further ahead is still waited for, as the lake
     * may no longer keep the covered snapshot's files, and so is the first snapshot of a leader
     * that covers none yet.
     */
    @Test
    void testReadThatDoesNotWaitForCatchUpReadsTheSnapshotBefore() throws Exception {
        // the leader covers no snapshot yet: the read waits for the rebuild to the first one
        manager.recover(src);
        src.publish(100, 1, 10, dict(1, F1), new Index().put("k1", 1, 0).put("k2", 1, 1).build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        assertThat(lakeMasks(unionRead(100, false))).isEmpty();

        DvTestManagers.handOver(manager, Collections.singletonList(src.written("k1", 10, 10)));
        src.append("k1", 10, 10);
        DvTestManagers.handOver(manager, Collections.singletonList(src.written("k2", 11, 11)));
        src.append("k2", 11, 11);
        highWatermark.set(12);
        Map<String, Set<Long>> before = lakeMasks(unionRead(100));
        assertThat(before)
                .isEqualTo(Collections.singletonMap(F1, new TreeSet<>(Arrays.asList(0L, 1L))));

        // 101 is registered; the switch to it stops while it downloads 101's change
        src.publish(101, 2, 11, dict(2, "f2"), new Index().put("k1", 2, 0).build());
        src.readable = new long[] {101, 11};
        src.changeGate = new CountDownLatch(1);
        manager.advanceAsync(101);
        assertThat(src.changeEntered.await(10, TimeUnit.SECONDS)).isTrue();

        // the newest registered snapshot, right after the covered one: the covered one, at once
        assertThatThrownBy(() -> DvTestReads.unionRead(manager, 101, false))
                .isInstanceOfSatisfying(
                        StaleSnapshotException.class,
                        e -> {
                            assertThat(e.isBehind()).isTrue();
                            assertThat(e.getCurrentSnapshotId()).isEqualTo(100L);
                            assertThat(e.getRequestedSnapshotId()).isEqualTo(101L);
                        });
        assertThat(lakeMasks(DvTestReads.unionRead(manager, 100, false))).isEqualTo(before);
        // a read that waits waits for the switch
        assertThatThrownBy(() -> DvTestReads.unionRead(manager, 101))
                .isInstanceOf(DeletionVectorsNotReadyException.class);
        // once a newer snapshot is registered, the covered one is two registrations back: a read
        // that does not wait waits too
        manager.noteRegistration(102);
        assertThatThrownBy(() -> DvTestReads.unionRead(manager, 101, false))
                .isInstanceOf(DeletionVectorsNotReadyException.class);

        src.changeGate.countDown();
        // 101 holds the change of k1; k2 changed after it
        assertThat(lakeMasks(unionRead(101, false)))
                .isEqualTo(
                        Collections.singletonMap(F1, new TreeSet<>(Collections.singletonList(1L))));
        assertThatThrownBy(() -> DvTestReads.unionRead(manager, 100, false))
                .isInstanceOfSatisfying(
                        StaleSnapshotException.class, e -> assertThat(e.isBehind()).isFalse());
    }

    /**
     * A read that waits for the lake-position lookups of its keys reports that wait in its timing.
     */
    @Test
    void testAReadReportsItsWaitForLookups() throws Exception {
        recoverWithTenLakeKeys();
        CountDownLatch release = new CountDownLatch(1);
        hooks(manager).setBeforeResolveLookup(waitFor(release));
        handOver("k1", 10, false);
        highWatermark.set(11);
        awaitCondition(manager::resolving, "no lookup in flight");
        long start = System.nanoTime();
        Future<DvSnapshot> read = async.submit(() -> unionRead(100));
        Thread.sleep(200);
        hooks(manager).setBeforeResolveLookup(null);
        release.countDown();
        assertThat(lakeMasks(read.get(30, TimeUnit.SECONDS)))
                .isEqualTo(Collections.singletonMap(F1, positions(1)));
        long readMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertThat(metrics.maskResolveWaitMaxMs()).isBetween(100L, readMs);
    }

    /**
     * A read that does not wait gets the ready masks while lookups are pending: the masks of the
     * last cut built while none was, exact up to that cut and below the high watermark. A read that
     * waits still waits. A pin takes the same cut for all parts. Once the lookups drain, the read
     * is served up to the high watermark again.
     */
    @Test
    void testReadThatDoesNotWaitGetsTheReadyMasksWhileLookupsArePending() throws Exception {
        recoverWithTenLakeKeys();
        handOver("k0", 10, false);
        highWatermark.set(11);
        assertThat(unionRead(100).getLogEndOffset()).isEqualTo(11L);
        DvSnapshot at11 = DvTestReads.unionRead(manager, 100, false);
        assertThat(at11.getLogEndOffset()).isEqualTo(11L);
        assertThat(lakeMasks(at11)).isEqualTo(Collections.singletonMap(F1, positions(0)));

        // a burst whose lookups stay pending
        CountDownLatch release = new CountDownLatch(1);
        hooks(manager).setBeforeResolveLookup(waitFor(release));
        for (int i = 1; i <= 5; i++) {
            handOver("k" + i, 10 + i, false);
        }
        highWatermark.set(16);
        awaitCondition(manager::resolving, "no lookup in flight");
        DvSnapshot frontier = DvTestReads.unionRead(manager, 100, false);
        assertThat(frontier.getLogEndOffset()).isEqualTo(11L);
        assertThat(frontier.getHighWatermark()).isEqualTo(16L);
        assertThat(lakeMasks(frontier)).isEqualTo(lakeMasks(at11));
        assertThat(frontier.getLogDvBitmap()).isNull();
        assertThat(metrics.frontierReads()).isEqualTo(1L);

        // a read that waits still waits for the lookups
        assertThatThrownBy(() -> DvTestReads.unionRead(manager, 100))
                .isInstanceOf(DeletionVectorsNotReadyException.class);
        // the parts of a read that pins take one cut
        DvPinnedCut pinned = manager.pinCut(100, false, null, 1);
        assertThat(pinned.logEndOffset()).isEqualTo(11L);
        assertThat(manager.pinnedCut(100, 11L)).isSameAs(pinned);

        // the lookups drain: up to the high watermark again
        hooks(manager).setBeforeResolveLookup(null);
        release.countDown();
        awaitCondition(() -> !manager.resolving(), "the lookups did not drain");
        DvSnapshot at16 = DvTestReads.unionRead(manager, 100, false);
        assertThat(at16.getLogEndOffset()).isEqualTo(16L);
        assertThat(lakeMasks(at16)).isEqualTo(Collections.singletonMap(F1, range(0, 5)));
        assertThat(lakeMasks(at16)).isEqualTo(lakeMasks(unionRead(100)));
    }

    /**
     * The ready masks advance as lookups drain, while reads that do not wait come: once a burst's
     * lookups are done, a read during the next burst's lookups gets the first burst's end.
     */
    @Test
    void testReadyMasksAdvanceAsLookupsDrain() throws Exception {
        manager.setReadyRefreshMillis(0);
        recoverWithTenLakeKeys();
        handOver("k0", 10, false);
        highWatermark.set(11);
        unionRead(100);
        assertThat(DvTestReads.unionRead(manager, 100, false).getLogEndOffset()).isEqualTo(11L);

        AtomicInteger lookups = new AtomicInteger();
        CountDownLatch first = new CountDownLatch(1);
        CountDownLatch second = new CountDownLatch(1);
        hooks(manager)
                .setBeforeResolveLookup(
                        () -> waitFor(lookups.incrementAndGet() == 1 ? first : second).run());
        for (int i = 1; i <= 3; i++) {
            handOver("k" + i, 10 + i, false);
        }
        highWatermark.set(14);
        awaitCondition(() -> lookups.get() == 1, "the first burst's lookups did not start");
        assertThat(DvTestReads.unionRead(manager, 100, false).getLogEndOffset()).isEqualTo(11L);

        // the second burst is handed over while the first one's lookups are pending
        hooks(manager).setResolvesPaused(true);
        handOver("k4", 14, false);
        handOver("k5", 15, false);
        highWatermark.set(16);
        first.countDown();
        awaitCondition(() -> !manager.resolving(), "the first burst's lookups did not drain");
        hooks(manager).setResolvesPaused(false);
        awaitCondition(() -> lookups.get() == 2, "the second burst's lookups did not start");

        DvSnapshot during = DvTestReads.unionRead(manager, 100, false);
        assertThat(during.getLogEndOffset()).isEqualTo(14L);
        assertThat(lakeMasks(during)).isEqualTo(Collections.singletonMap(F1, range(0, 3)));
        second.countDown();
        hooks(manager).setBeforeResolveLookup(null);
        awaitCondition(() -> !manager.resolving(), "the second burst's lookups did not drain");
        assertThat(DvTestReads.unionRead(manager, 100, false).getLogEndOffset()).isEqualTo(16L);
    }

    /**
     * With no lookup pending, a read that does not wait applies the queued changes up to the first
     * one that needs a lookup, and gets the masks up to it. A read that waits applies them all and
     * looks the key up.
     */
    @Test
    void testReadThatDoesNotWaitStopsAtTheFirstChangeThatNeedsALookup() throws Exception {
        recoverWithTenLakeKeys();
        assertThat(DvTestReads.unionRead(manager, 100, false).getLogEndOffset()).isEqualTo(10L);
        hooks(manager).setResolvesPaused(true);
        // an insert, an update of a key in the lake, an update of the inserted key
        handOver("k20", 10, true);
        handOver("k1", 11, false);
        handOver("k20", 12, false);
        highWatermark.set(13);

        DvSnapshot partial = DvTestReads.unionRead(manager, 100, false);
        assertThat(partial.getLogEndOffset()).isEqualTo(11L);
        assertThat(partial.getHighWatermark()).isEqualTo(13L);
        assertThat(lakeMasks(partial)).isEmpty();
        assertThat(partial.getLogDvBitmap()).isNull();

        // the lookup a read that waits needs runs on the resolver threads, not on its own
        hooks(manager).setResolvesPaused(false);
        DvSnapshot all = DvTestReads.unionRead(manager, 100);
        assertThat(all.getLogEndOffset()).isEqualTo(13L);
        assertThat(lakeMasks(all)).isEqualTo(Collections.singletonMap(F1, positions(1)));
        assertThat(offsets(all.getLogDvBitmap())).containsExactly(10L);
    }

    /**
     * Reads that do not wait never go back in time across a move. Before the move a read gets the
     * masks up to 14. Right after it, snapshot 101 is readable from 12 only and its lookups are
     * pending: the read is told the leader is behind, with snapshot 100, whose kept masks up to 14
     * it then gets. Once the lookups drain, snapshot 101 is served from 14 on.
     */
    @Test
    void testReadsThatDoNotWaitStayMonotonicAcrossAMove() throws Exception {
        moveWithLookupsHeld(true);
        List<Long> cuts = new ArrayList<>();
        cuts.add(14L);
        assertThat(manager.previousReadyCut()).isEqualTo(14L);

        assertThatThrownBy(() -> DvTestReads.unionRead(manager, 101, false))
                .isInstanceOfSatisfying(
                        StaleSnapshotException.class,
                        e -> {
                            assertThat(e.isBehind()).isTrue();
                            assertThat(e.getCurrentSnapshotId()).isEqualTo(100L);
                            assertThat(e.getRequestedSnapshotId()).isEqualTo(101L);
                        });
        DvSnapshot during = DvTestReads.unionRead(manager, 100, false);
        cuts.add(during.getLogEndOffset());
        assertThat(during.getLogEndOffset()).isEqualTo(14L);
        assertThat(during.getSnapshotStartOffset()).isEqualTo(10L);
        assertThat(lakeMasks(during)).isEqualTo(Collections.singletonMap(F1, range(0, 3)));
        // a read that waits waits for the lookups
        assertThatThrownBy(() -> DvTestReads.unionRead(manager, 101))
                .isInstanceOf(DeletionVectorsNotReadyException.class);

        releaseLookups();
        DvSnapshot after = DvTestReads.unionRead(manager, 101, false);
        cuts.add(after.getLogEndOffset());
        assertThat(lakeMasks(after)).isEqualTo(Collections.singletonMap(F1, range(2, 3)));
        assertThat(lakeMasks(after)).isEqualTo(lakeMasks(unionRead(101)));
        // snapshot 101 has ready masks now: the kept ones are gone, and 100 is passed
        assertThat(manager.previousReadyCut()).isEqualTo(-1L);
        assertThatThrownBy(() -> DvTestReads.unionRead(manager, 100, false))
                .isInstanceOfSatisfying(
                        StaleSnapshotException.class, e -> assertThat(e.isBehind()).isFalse());
        assertThat(cuts).isSorted();
    }

    /**
     * The masks kept across a move count in the tablet server's deletion vector memory and are
     * dropped when it is exceeded. Without them, snapshot 101 alone, readable from 12, would go
     * back before the 14 served: the read waits for the lookups instead.
     */
    @Test
    void testKeptMasksAreEvictedForMemory() throws Exception {
        moveWithLookupsHeld(true);
        long withKeptMasks = manager.tailMemoryBytes();
        assertThat(manager.previousReadyCut()).isEqualTo(14L);

        assertThat(manager.shedForBudget()).isTrue();
        assertThat(manager.previousReadyCut()).isEqualTo(-1L);
        assertThat(manager.tailMemoryBytes()).isLessThan(withKeptMasks);
        assertThatThrownBy(() -> DvTestReads.unionRead(manager, 101, false))
                .isInstanceOf(DeletionVectorsNotReadyException.class);
        assertThatThrownBy(() -> DvTestReads.unionRead(manager, 100, false))
                .isInstanceOfSatisfying(
                        StaleSnapshotException.class, e -> assertThat(e.isBehind()).isFalse());

        releaseLookups();
        assertThat(DvTestReads.unionRead(manager, 101, false).getLogEndOffset()).isEqualTo(14L);
    }

    /**
     * The first move after the leader rebuilt its state, with no read that did not wait before: no
     * masks are kept. A read that does not wait gets the new snapshot alone if its readable offset
     * is no older than the highest cut served, 14 to a read that waited. Otherwise it waits for the
     * move's lookups.
     */
    @Test
    void testFirstMoveAfterARebuildKeepsNoMasks() throws Exception {
        // readable from 12: older than what was served, so the read waits
        moveWithLookupsHeld(false, 12);
        assertThat(manager.previousReadyCut()).isEqualTo(-1L);
        assertThatThrownBy(() -> DvTestReads.unionRead(manager, 101, false))
                .isInstanceOf(DeletionVectorsNotReadyException.class);
        releaseLookups();
        assertThat(DvTestReads.unionRead(manager, 101, false).getLogEndOffset()).isEqualTo(17L);
    }

    /** As above, with the new snapshot readable from 14: the snapshot alone is served. */
    @Test
    void testFirstMoveAfterARebuildReadsTheSnapshotAloneIfNoOlder() throws Exception {
        moveWithLookupsHeld(false, 14);
        DvSnapshot lakeOnly = DvTestReads.unionRead(manager, 101, false);
        assertThat(lakeOnly.getLogEndOffset()).isEqualTo(14L);
        assertThat(lakeOnly.getSnapshotStartOffset()).isEqualTo(14L);
        assertThat(lakeOnly.getHighWatermark()).isEqualTo(17L);
        assertThat(lakeOnly.getLakeDvEntries()).isEmpty();
        assertThat(lakeOnly.getLogDvBitmap()).isNull();
        releaseLookups();
        DvSnapshot after = DvTestReads.unionRead(manager, 101, false);
        assertThat(after.getLogEndOffset()).isEqualTo(17L);
        assertThat(lakeMasks(after)).isEqualTo(lakeMasks(unionRead(101)));
    }

    /**
     * A leader change between two reads that do not wait. The new leader rebuilds the bucket from
     * the same log and index, its lookups held. The lake snapshot alone, from 10, would go back
     * before the cut 14 the old leader served, so the new leader's first read waits for its masks
     * and then gets them up to at least that cut.
     */
    @Test
    void testNewLeaderNeverServesALowerCutThanTheOldOne() throws Exception {
        recoverWithTenLakeKeys();
        for (int i = 0; i < 4; i++) {
            handOver("k" + i, 10 + i, false);
        }
        highWatermark.set(14);
        unionRead(100);
        long served = DvTestReads.unionRead(manager, 100, false).getLogEndOffset();
        assertThat(served).isEqualTo(14L);

        CountDownLatch release = new CountDownLatch(1);
        try (DvRocksDB newDb = DvRocksDB.open(tempDir.resolve("dv-new-leader").toString())) {
            DerivedDvManager newLeader =
                    DvTestManagers.newManager(
                            newDb, highWatermark::get, DvTestManagers.CHECKPOINT_INTERVAL, metrics);
            try {
                newLeader.setMaskCacheMillis(0);
                // the rebuilt tail's lake keys are looked up through the hook
                hooks(newLeader).setBeforeResolveLookup(waitFor(release));
                newLeader.recover(src);
                awaitCondition(newLeader::resolving, "the new leader's lookups did not start");
                assertThatThrownBy(() -> DvTestReads.unionRead(newLeader, 100, false))
                        .isInstanceOf(DeletionVectorsNotReadyException.class);

                hooks(newLeader).setBeforeResolveLookup(null);
                release.countDown();
                awaitCondition(() -> !newLeader.resolving(), "the lookups did not drain");
                DvSnapshot first = DvTestReads.unionRead(newLeader, 100, false);
                assertThat(first.getLogEndOffset()).isGreaterThanOrEqualTo(served);
                assertThat(lakeMasks(first))
                        .isEqualTo(lakeMasks(DvTestReads.unionRead(manager, 100, false)));
            } finally {
                release.countDown();
                newLeader.close();
            }
        }
    }

    // held by moveWithLookupsHeld until releaseLookups
    private CountDownLatch heldLookups;

    /**
     * A switch whose ingest failed after it ingested part of a newer snapshot leaves the index
     * dirty up to there. A switch to an older snapshot cannot clean it: it is refused, and the
     * bucket keeps covering the snapshot it covered, with exact lookups and masks.
     */
    @Test
    void testFailedIngestThenSwitchToAnOlderSnapshotIsRefused() throws Exception {
        recoverWithTenLakeKeys();
        handOver("k0", 10, false);
        highWatermark.set(11);
        src.publish(101, 2, 11, dict(2, F2), new Index().put("k0", 2, 0).build());
        // 102 tiers nothing new; it moves k9 to file 3, as a compaction does
        src.publish(102, 3, 11, dict(3, F3), new Index().put("k9", 3, 9).build());
        src.readable = new long[] {102, 11};
        AtomicInteger ingests = new AtomicInteger();
        hooks(manager)
                .setAfterIngest(
                        () -> {
                            if (ingests.incrementAndGet() == 2) {
                                throw new IllegalStateException("the ingest failed");
                            }
                        });
        assertThatThrownBy(() -> manager.ensureCovered(102))
                .hasMessageContaining("the ingest failed");
        hooks(manager).setAfterIngest(null);

        // a read planned on 101 does not switch the bucket to 101 over 102's positions
        assertThat(manager.ensureCovered(101)).isFalse();
        assertThat(manager.getReadableSnapshotId()).isNotEqualTo(101L);
        // a switch to 102 cleans the index again
        assertThat(manager.ensureCovered(102)).isTrue();
        handOver("k9", 11, false);
        highWatermark.set(12);
        assertThat(asStrings(lookupWhenReady(102, Collections.singletonList(key("k9")))))
                .containsExactly("k9@" + F3 + "#9");
        assertThat(lakeMasks(unionRead(102))).isEqualTo(Collections.singletonMap(F3, positions(9)));
    }

    /**
     * No read of a new leader, waiting or not, is served masks below the highest cut served: here
     * the log end the new leader took over at, above its high watermark. The last leader may have
     * acknowledged writes up to there, so a read that waits waits for the high watermark to reach
     * it, and then gets masks that a read that does not wait may take again.
     */
    @Test
    void testNoReadOfANewLeaderGoesBelowTheTakeoverCut() throws Exception {
        manager.setMaskCacheMillis(10_000);
        Index initial = new Index();
        for (int i = 0; i < 10; i++) {
            initial.put("k" + i, 1, i);
        }
        src.publish(100, 1, 10, dict(1, F1), initial.build());
        src.readable = new long[] {100, 10};
        // the new leader's log runs to 14, its high watermark lags at 12
        for (int i = 0; i < 4; i++) {
            src.append("k" + i, 10 + i, 10 + i);
        }
        highWatermark.set(12);
        manager.recover(src);
        assertThatThrownBy(() -> DvTestReads.unionRead(manager, 100))
                .isInstanceOf(DeletionVectorsNotReadyException.class);

        long served;
        try {
            served = DvTestReads.unionRead(manager, 100, false).getLogEndOffset();
        } catch (DeletionVectorsNotReadyException e) {
            // answered as not ready: no masks below the cut
            served = Long.MAX_VALUE;
        }
        assertThat(served).as("no-wait read below the takeover cut 14").isGreaterThanOrEqualTo(14L);

        highWatermark.set(14);
        assertThat(unionRead(100).getLogEndOffset()).isEqualTo(14L);
        assertThat(DvTestReads.unionRead(manager, 100, false).getLogEndOffset()).isEqualTo(14L);
    }

    /**
     * Log retention keeps the log that reads still read: a cut pinned on the snapshot before a
     * switch, and the masks kept across a switch for reads that do not wait, read it from that
     * snapshot's readable offset, below the covered snapshot's.
     */
    @Test
    void testLogRetentionKeepsTheLogOfPinnedCutsAndKeptMasks() throws Exception {
        recoverWithTenLakeKeys();
        for (int i = 0; i < 4; i++) {
            handOver("k" + i, 10 + i, false);
        }
        highWatermark.set(14);
        DvPinnedCut pinned = manager.pinCut(100, true, null, 1);
        Index tiered = new Index();
        for (int i = 0; i < 2; i++) {
            tiered.put("k" + i, 2, i);
        }
        src.publish(101, 2, 12, dict(2, F2), tiered.build());
        src.readable = new long[] {101, 12};
        assertThat(manager.ensureCovered(101)).isTrue();
        // the pinned cut of 100 reads the log from 10
        assertThat(manager.logRetainOffset()).isEqualTo(10L);
        manager.releaseCut(100, pinned.logEndOffset(), 1);
        assertThat(manager.logRetainOffset()).isEqualTo(12L);
    }

    /** As above, for the masks kept across a switch, until they are dropped for memory. */
    @Test
    void testLogRetentionKeepsTheLogOfKeptMasks() throws Exception {
        moveWithLookupsHeld(true);
        // covers 101, readable from 12; the masks kept of 100 read from 10
        assertThat(manager.previousReadyCut()).isEqualTo(14L);
        assertThat(manager.logRetainOffset()).isEqualTo(10L);
        assertThat(manager.shedForBudget()).isTrue();
        // released once the owner published the state without them
        waitUntil(() -> manager.logRetainOffset() == 12L);
        releaseLookups();
    }

    /**
     * A read that does not wait is sent back at most to the snapshot right before the newest
     * registered one. Once a newer snapshot is registered, a read planned on the covered snapshot
     * is not sent further back to the masks kept of the snapshot before it.
     */
    @Test
    void testReadIsNotSentBackTwoSnapshots() throws Exception {
        moveWithLookupsHeld(true);
        assertThatThrownBy(() -> DvTestReads.unionRead(manager, 101, false))
                .isInstanceOfSatisfying(
                        StaleSnapshotException.class, e -> assertThat(e.isBehind()).isTrue());

        // tiering registers 102 while the bucket still switches to 101
        manager.noteRegistration(102);
        assertThatThrownBy(() -> DvTestReads.unionRead(manager, 101, false))
                .satisfies(
                        e ->
                                assertThat(
                                                e instanceof StaleSnapshotException
                                                        && ((StaleSnapshotException) e).isBehind())
                                        .as("sent back to 100 from %s", e)
                                        .isFalse());
        releaseLookups();
    }

    /**
     * A leader with nothing to serve names a fold cut to a read that does not wait, but counts it
     * as served only once the answer is handed out: an answer the server never sends does not keep
     * later reads from the masks below it.
     */
    @Test
    void testFoldCutCountsAsServedOnlyOnceHandedOut() throws Exception {
        manager.setMaskCacheMillis(0);
        recoverWithTenLakeKeys();
        for (int i = 0; i < 4; i++) {
            handOver("k" + i, 10 + i, false);
        }
        highWatermark.set(14);
        assertThat(unionRead(100).getLogEndOffset()).isEqualTo(14L);
        assertThat(DvTestReads.unionRead(manager, 100, false).getLogEndOffset()).isEqualTo(14L);
        assertThat(manager.inspect().servedCut).isEqualTo(14L);
        assertThat(manager.foldCut()).isEqualTo(14L);
        highWatermark.set(20);
        assertThat(manager.foldCut()).isEqualTo(20L);
        assertThat(manager.inspect().servedCut).isEqualTo(14L);
        manager.foldCutServed(20);
        assertThat(manager.inspect().servedCut).isEqualTo(20L);
    }

    /** As {@link #moveWithLookupsHeld(boolean, long)}, to a snapshot readable from 12. */
    private void moveWithLookupsHeld(boolean readsThatDoNotWait) throws Exception {
        moveWithLookupsHeld(readsThatDoNotWait, 12);
    }

    /**
     * Covers snapshot 100 by a rebuild, writes k0..k3 at 10..13 and serves masks up to 14, to a
     * read that does not wait if {@code readsThatDoNotWait}, else to one that waits. Without such
     * reads it writes k4..k6 at 14..16 too. Then moves to snapshot 101, which tiers the changes
     * below {@code readableOffset}, and holds the move's lookups of the other lake keys.
     */
    private void moveWithLookupsHeld(boolean readsThatDoNotWait, long readableOffset)
            throws Exception {
        // the move looks up every lake key of the tail again
        manager.setTargetedMoves(false);
        recoverWithTenLakeKeys();
        for (int i = 0; i < 4; i++) {
            handOver("k" + i, 10 + i, false);
        }
        highWatermark.set(14);
        assertThat(unionRead(100).getLogEndOffset()).isEqualTo(14L);
        if (readsThatDoNotWait) {
            assertThat(DvTestReads.unionRead(manager, 100, false).getLogEndOffset()).isEqualTo(14L);
        } else {
            for (int i = 4; i < 7; i++) {
                handOver("k" + i, 10 + i, false);
            }
            highWatermark.set(17);
        }
        heldLookups = new CountDownLatch(1);
        hooks(manager).setBeforeResolveLookup(waitFor(heldLookups));
        Index tiered = new Index();
        for (int i = 0; i < readableOffset - 10; i++) {
            tiered.put("k" + i, 2, i);
        }
        src.publish(101, 2, readableOffset, dict(2, "f2"), tiered.build());
        src.readable = new long[] {101, readableOffset};
        assertThat(manager.ensureCovered(101)).isTrue();
        awaitCondition(manager::resolving, "the move's lookups did not start");
    }

    private void releaseLookups() throws InterruptedException {
        hooks(manager).setBeforeResolveLookup(null);
        heldLookups.countDown();
        awaitCondition(() -> !manager.resolving(), "the move's lookups did not drain");
    }

    /**
     * A read that does not wait while the leader still catches up and lookups are pending: the
     * leader answers that it covers the snapshot before, and the read of that snapshot gets the
     * masks ready below the high watermark.
     */
    @Test
    void testReadThatDoesNotWaitWhileBehindAndLookupsArePending() throws Exception {
        recoverWithTenLakeKeys();
        handOver("k0", 10, false);
        highWatermark.set(11);
        unionRead(100);
        // The owner answers the waiting read in its step and publishes the frontier that served
        // it when the step ends: a read that does not wait reads the published state.
        awaitCondition(
                () -> manager.state().latest != null && manager.state().latest.cut() >= 11,
                "the frontier of the waiting read published");
        assertThat(DvTestReads.unionRead(manager, 100, false).getLogEndOffset()).isEqualTo(11L);

        // 101 is registered; its catch-up stops while it downloads the change
        src.publish(101, 2, 11, dict(2, "f2"), new Index().put("k0", 2, 0).build());
        src.readable = new long[] {101, 11};
        src.changeGate = new CountDownLatch(1);
        manager.advanceAsync(101);
        assertThat(src.changeEntered.await(10, TimeUnit.SECONDS)).isTrue();
        // a burst whose lookups stay pending
        CountDownLatch release = new CountDownLatch(1);
        hooks(manager).setBeforeResolveLookup(waitFor(release));
        handOver("k1", 11, false);
        handOver("k2", 12, false);
        highWatermark.set(13);
        awaitCondition(manager::resolving, "no lookup in flight");

        assertThatThrownBy(() -> DvTestReads.unionRead(manager, 101, false))
                .isInstanceOfSatisfying(
                        StaleSnapshotException.class, e -> assertThat(e.isBehind()).isTrue());
        DvSnapshot ready = DvTestReads.unionRead(manager, 100, false);
        assertThat(ready.getLogEndOffset()).isEqualTo(11L);
        assertThat(ready.getHighWatermark()).isEqualTo(13L);
        assertThat(lakeMasks(ready)).isEqualTo(Collections.singletonMap(F1, positions(0)));

        hooks(manager).setBeforeResolveLookup(null);
        release.countDown();
        src.changeGate.countDown();
        // 101 holds k0's change: the masks of k1 and k2 remain
        assertThat(lakeMasks(unionRead(101)))
                .isEqualTo(Collections.singletonMap(F1, positions(1, 2)));
    }

    /**
     * A switch ingests the new snapshot's index off the owner. Meanwhile the covered snapshot is
     * served right away, and its lookups, of a change written during the ingest and of tiering, see
     * the positions it holds, not the ones being ingested. A read that does not wait for the new
     * snapshot is told the leader is behind; a read that waits gets it after the swap.
     */
    @Test
    void testRequestsDuringAMoveIngestAreServedFromTheCoveredSnapshot() throws Exception {
        recoverWithTenLakeKeys();
        for (int i = 0; i < 4; i++) {
            handOver("k" + i, 10 + i, false);
        }
        highWatermark.set(14);
        assertThat(lakeMasks(unionRead(100))).isEqualTo(Collections.singletonMap(F1, range(0, 3)));

        // 101 tiers k0 and k1, and puts k8 in file 2 too
        src.publish(
                101,
                2,
                12,
                dict(2, F2),
                new Index().put("k0", 2, 0).put("k1", 2, 1).put("k8", 2, 8).build());
        src.readable = new long[] {101, 12};
        CountDownLatch ingesting = new CountDownLatch(1);
        CountDownLatch ingestGate = new CountDownLatch(1);
        hooks(manager)
                .setAfterIngest(
                        () -> {
                            ingesting.countDown();
                            waitFor(ingestGate).run();
                        });
        CompletableFuture<Boolean> move =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return manager.ensureCovered(101);
                            } catch (Exception e) {
                                throw new CompletionException(e);
                            }
                        },
                        async);
        // the index already holds k8 in file 2
        assertThat(ingesting.await(10, TimeUnit.SECONDS)).isTrue();
        CompletableFuture<DvSnapshot> waiting;
        try {
            // served at once from the covered snapshot
            assertThat(lakeMasks(unionReadAsync(100).get(5, TimeUnit.SECONDS)))
                    .isEqualTo(Collections.singletonMap(F1, range(0, 3)));
            // a change of k8 is looked up as of the covered snapshot: file 1, not file 2
            handOver("k8", 14, false);
            highWatermark.set(15);
            assertThat(lakeMasks(unionReadAsync(100).get(5, TimeUnit.SECONDS)))
                    .isEqualTo(Collections.singletonMap(F1, positions(0, 1, 2, 3, 8)));
            assertThat(asStrings(lookupWhenReady(100, Collections.singletonList(key("k8")))))
                    .containsExactly("k8@" + F1 + "#8");
            assertThatThrownBy(() -> DvTestReads.unionRead(manager, 101, false))
                    .isInstanceOfSatisfying(
                            StaleSnapshotException.class,
                            e -> {
                                assertThat(e.isBehind()).isTrue();
                                assertThat(e.getCurrentSnapshotId()).isEqualTo(100L);
                            });
            waiting = unionReadAsync(101);
            assertThat(manager.indexAhead()).isTrue();
        } finally {
            hooks(manager).setAfterIngest(null);
            ingestGate.countDown();
        }
        assertThat(move.get(10, TimeUnit.SECONDS)).isTrue();
        assertThat(manager.indexAhead()).isFalse();

        // 101 holds k0 and k1; k2 and k3 mask their rows in file 1, k8 its row in file 2
        Map<String, Set<Long>> expected = new TreeMap<>();
        expected.put(F1, range(2, 3));
        expected.put(F2, positions(8));
        assertThat(new TreeMap<>(lakeMasks(waiting.get(30, TimeUnit.SECONDS)))).isEqualTo(expected);
        assertThat(new TreeMap<>(lakeMasks(unionRead(101)))).isEqualTo(expected);
    }

    /**
     * A move whose ingest fails part way leaves the covered snapshot serving: its lookups keep
     * reading the index as it was, though the index holds part of the new changes. The next move
     * applies them all again and covers the new snapshot.
     */
    @Test
    void testFailedIngestLeavesTheCoveredSnapshotServing() throws Exception {
        recoverWithTenLakeKeys();
        handOver("k0", 10, false);
        handOver("k1", 11, false);
        highWatermark.set(12);
        assertThat(lakeMasks(unionRead(100))).isEqualTo(Collections.singletonMap(F1, range(0, 1)));

        // 101 moves k8 to file 2, 102 moves k9 to file 3
        src.publish(101, 2, 11, dict(2, F2), new Index().put("k0", 2, 0).put("k8", 2, 8).build());
        src.publish(102, 3, 12, dict(3, F3), new Index().put("k1", 3, 1).put("k9", 3, 9).build());
        src.readable = new long[] {102, 12};
        AtomicInteger ingests = new AtomicInteger();
        hooks(manager)
                .setAfterIngest(
                        () -> {
                            if (ingests.incrementAndGet() == 1) {
                                throw new IllegalStateException("the ingest failed");
                            }
                        });
        assertThatThrownBy(() -> manager.ensureCovered(102))
                .hasMessageContaining("the ingest failed");

        // 101's change is in the index, yet the covered snapshot is read as it was
        assertThat(manager.getReadableSnapshotId()).isEqualTo(100L);
        assertThat(manager.indexAhead()).isTrue();
        handOver("k8", 12, false);
        highWatermark.set(13);
        assertThat(lakeMasks(unionRead(100)))
                .isEqualTo(Collections.singletonMap(F1, positions(0, 1, 8)));
        assertThat(asStrings(lookupWhenReady(100, Collections.singletonList(key("k8")))))
                .containsExactly("k8@" + F1 + "#8");

        hooks(manager).setAfterIngest(null);
        assertThat(manager.ensureCovered(102)).isTrue();
        assertThat(manager.indexAhead()).isFalse();
        // 102 holds k0 and k1; k8 masks its row in file 2
        assertThat(lakeMasks(unionRead(102))).isEqualTo(Collections.singletonMap(F2, positions(8)));
    }

    /**
     * A read that does not wait, of a bucket whose new leader still rebuilds, is answered at once:
     * the leader has no masks to serve and names a cut to read by key up to. The cut is the log end
     * the leader had when it took over, as its high watermark lags it. A read that waits still
     * waits, and gets the masks once rebuilt; a read that does not wait then gets masks of that cut
     * or a later one.
     */
    @Test
    void testNoWaitReadOfARebuildingLeaderIsAnsweredAtOnce() throws Exception {
        int changes = 20_000;
        long start = 10;
        src.publish(
                100, 1, start, dict(1, F1), new Index().put("k0", 1, 0).put("k1", 1, 1).build());
        src.readable = new long[] {100, start};
        appendChanges(start, 100, changes);
        long logEnd = start + changes;
        highWatermark.set(logEnd - 5);
        CountDownLatch gate = pauseReplayAfter(changes / 2);
        manager.recoverInBackground(src, new DerivedDvRebuildLimiter(1));
        CompletableFuture<DvSnapshot> waiting;
        try {
            assertThat(src.replayPaused.await(10, TimeUnit.SECONDS)).isTrue();
            long askedAt = System.nanoTime();
            assertThatThrownBy(() -> DvTestReads.unionRead(manager, 100, false))
                    .isInstanceOfSatisfying(
                            DeletionVectorsNotReadyException.class,
                            e -> {
                                assertThat(e.isRebuilding()).isTrue();
                                assertThat(e.getRebuildingCut()).isEqualTo(logEnd);
                                assertThat(e.getMessage()).contains(": rebuild;");
                            });
            // a pin of a read that does not wait too, and both without the wait of a read that
            // waits, half a second
            assertThatThrownBy(() -> manager.pinCut(100, false, null, 1))
                    .isInstanceOfSatisfying(
                            DeletionVectorsNotReadyException.class,
                            e -> assertThat(e.getRebuildingCut()).isEqualTo(logEnd));
            assertThat(System.nanoTime() - askedAt).isLessThan(TimeUnit.MILLISECONDS.toNanos(400));
            highWatermark.set(logEnd);
            waiting = unionReadAsync(100);
            Thread.sleep(300);
            assertThat(waiting).isNotDone();
        } finally {
            gate.countDown();
        }
        assertThat(waiting.get(30, TimeUnit.SECONDS).getLogEndOffset()).isEqualTo(logEnd);
        assertThat(unionRead(100, false).getLogEndOffset()).isGreaterThanOrEqualTo(logEnd);
        assertThat(manager.pinnedCutCount()).isZero();
    }

    /**
     * Once rebuilt, a leader looks up the lake positions of its tail's keys. Its masks would go
     * back from the cut the last leader served, so meanwhile a read that does not wait reads by key
     * too. Once the lookups are done, it gets the masks of that cut.
     */
    @Test
    void testNoWaitReadDoesNotWaitForTheLookupsOfARebuild() throws Exception {
        Index initial = new Index();
        for (int i = 0; i < 10; i++) {
            initial.put("k" + i, 1, i);
        }
        src.publish(100, 1, 10, dict(1, F1), initial.build());
        src.readable = new long[] {100, 10};
        for (int i = 0; i < 4; i++) {
            src.append("k" + i, 10 + i, 10 + i);
        }
        highWatermark.set(14);
        // the rebuild's lookups wait while the resolves are paused
        hooks(manager).setResolvesPaused(true);
        manager.recover(src);
        assertThat(manager.getReadableSnapshotId()).isEqualTo(100L);

        assertThatThrownBy(() -> DvTestReads.unionRead(manager, 100, false))
                .isInstanceOfSatisfying(
                        DeletionVectorsNotReadyException.class,
                        e -> {
                            assertThat(e.getRebuildingCut()).isEqualTo(14L);
                            assertThat(e.getMessage()).contains(": lookups;");
                        });

        hooks(manager).setResolvesPaused(false);
        DvSnapshot masks = unionRead(100, false);
        assertThat(masks.getLogEndOffset()).isEqualTo(14L);
        assertThat(lakeMasks(masks)).isEqualTo(Collections.singletonMap(F1, range(0, 3)));
    }

    /**
     * A read that does not wait, of a bucket whose tail is replayed, here the empty lake's whole
     * log, which the leader replays as it starts, is answered at once. Once replayed, the masks are
     * served.
     */
    @Test
    void testNoWaitReadOfAReplayingTailIsAnsweredAtOnce() throws Exception {
        src.append("k1", 0, 0);
        src.append("k1", 1, 1);
        src.append("k2", 2, 2);
        highWatermark.set(3);
        CountDownLatch gate = pauseReplayAfter(1);
        manager.recover(src);
        try {
            // the replay started with the leader, and is paused
            assertThat(src.replayPaused.await(10, TimeUnit.SECONDS)).isTrue();
            for (int i = 0; i < 2; i++) {
                assertThatThrownBy(() -> DvTestReads.unionRead(manager, -1, false))
                        .isInstanceOfSatisfying(
                                DeletionVectorsNotReadyException.class,
                                e -> {
                                    assertThat(e.getRebuildingCut()).isEqualTo(3L);
                                    assertThat(e.getMessage()).contains(": replay;");
                                });
            }
        } finally {
            gate.countDown();
        }
        assertThat(offsets(unionRead(-1, false).getLogDvBitmap())).containsExactly(0L);
    }

    /**
     * A read that does not wait, of a snapshot a bucket that covers none has not started to rebuild
     * to, is answered at once, and starts the rebuild.
     */
    @Test
    void testNoWaitReadOfABucketThatCoversNoSnapshotIsAnsweredAtOnce() throws Exception {
        src.append("k1", 0, 0);
        highWatermark.set(1);
        manager.recover(src);
        src.publish(100, 1, 1, dict(1, F1), new Index().put("k1", 1, 0).build());
        src.readable = new long[] {100, 1};

        assertThatThrownBy(() -> DvTestReads.unionRead(manager, 100, false))
                .isInstanceOfSatisfying(
                        DeletionVectorsNotReadyException.class,
                        e -> {
                            assertThat(e.getRebuildingCut()).isEqualTo(1L);
                            assertThat(e.getMessage()).contains(": none;");
                        });
        assertThat(lakeMasks(unionRead(100))).isEmpty();
        assertThat(manager.getReadableSnapshotId()).isEqualTo(100L);
    }

    /** Covers snapshot 100 of keys k0..k9 in file 1, readable from offset 10. */
    private void recoverWithTenLakeKeys() throws Exception {
        Index initial = new Index();
        for (int i = 0; i < 10; i++) {
            initial.put("k" + i, 1, i);
        }
        src.publish(100, 1, 10, dict(1, F1), initial.build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        manager.recover(src);
    }

    /** Hands a change of key {@code k} at {@code offset} over, as the write path does. */
    private void handOver(String k, long offset, boolean insert) {
        DvEntry entry = src.written(k, offset, offset, insert);
        src.append(k, offset, offset, insert);
        DvTestManagers.handOver(manager, Collections.singletonList(entry));
    }

    /** The positions {@code from} to {@code to}, both included. */
    private static Set<Long> range(long from, long to) {
        Set<Long> positions = new TreeSet<>();
        for (long p = from; p <= to; p++) {
            positions.add(p);
        }
        return positions;
    }

    private static Set<Long> positions(long... values) {
        Set<Long> positions = new TreeSet<>();
        for (long p : values) {
            positions.add(p);
        }
        return positions;
    }

    private static Runnable waitFor(CountDownLatch latch) {
        return () -> {
            try {
                latch.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
    }

    private static void awaitCondition(java.util.function.BooleanSupplier condition, String what)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (!condition.getAsBoolean()) {
            assertThat(System.currentTimeMillis()).as(what).isLessThan(deadline);
            Thread.sleep(5);
        }
    }

    @Test
    void testCloseFencesInFlightCatchUp() throws Exception {
        // the empty lake: the first snapshot is reached by a full rebuild
        recoverEmptyLake();
        src.publish(100, 1, 10, dict(1, F1), new Index().put("k1", 1, 0).build());
        // the catch-up blocks in a slow remote listing of the chain, off the owner
        src.chainGate = new CountDownLatch(1);
        CompletableFuture<Boolean> catchUp =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return manager.ensureCovered(100);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        assertThat(src.chainEntered.await(10, TimeUnit.SECONDS)).isTrue();

        // KvTablet#close: DerivedDvManager#close, which closes the DV RocksDB
        CompletableFuture<Void> closing = CompletableFuture.runAsync(manager::close);
        boolean closedWhileCatchUpInFlight;
        try {
            closing.get(1, TimeUnit.SECONDS);
            closedWhileCatchUpInFlight = true;
        } catch (java.util.concurrent.TimeoutException e) {
            closedWhileCatchUpInFlight = false; // close() waits for the catch-up: also fine
        }
        src.chainGate.countDown();
        catchUp.get(10, TimeUnit.SECONDS);
        closing.get(10, TimeUnit.SECONDS);

        if (closedWhileCatchUpInFlight) {
            // the DV RocksDB is closed by now: nothing may be written into it
            assertThat(src.downloadedIndexIds).isEmpty();
            assertThat(manager.getReadableSnapshotId()).isEqualTo(-1L);
            db.close();
            try (DvRocksDB reopened = DvRocksDB.open(tempDir.resolve("dv").toString())) {
                assertThat(reopened.fileDict().getFilePath(1)).isNull();
            }
        }
    }

    @Test
    void testBucketTieredOnlyAfterTrimRecoversFromKeptChain() throws Exception {
        // the kept chain starts at snapshot 300, where this bucket had nothing tiered yet, so it
        // has no checkpoint
        src.publish(300, 3, 0, Collections.emptyMap(), new Index().build());
        src.publish(400, 4, 10, dict(1, F1), new Index().put("k1", 1, 0).build());
        src.trimmed = true;
        src.readable = new long[] {400, 10};
        src.append("k1", 0, 0);
        src.append("k1", 10, 10);
        highWatermark.set(11);

        manager.recover(src);

        assertThat(asStrings(lookupWhenReady(400, Collections.singletonList(key("k1")))))
                .containsExactly("k1@" + F1 + "#0");
        assertThat(lakeMasks(unionRead(400))).containsOnly(entry(F1, set(0L)));
    }

    // ------------------------------------------------------------------------------------------
    // rebuilds: writes go on while the log is replayed, a few rebuilds at a time
    // ------------------------------------------------------------------------------------------

    @Test
    void testWriteProceedsWhileRecoveryReplaysTheLog() throws Exception {
        int keys = 100;
        int changes = 50_000;
        long start = 10;
        long end = start + changes;
        src.publish(100, 1, start, dict(1, F1), new Index().put("k0", 1, 0).build());
        src.readable = new long[] {100, start};
        appendChanges(start, keys, changes);
        highWatermark.set(end);
        CountDownLatch gate = pauseReplayAfter(changes / 2);

        CompletableFuture<Void> recovery =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                manager.recover(src);
                            } catch (Exception e) {
                                throw new CompletionException(e);
                            }
                        },
                        async);
        CompletableFuture<DvSnapshot> read;
        try {
            assertThat(src.replayPaused.await(10, TimeUnit.SECONDS)).isTrue();
            read = unionReadAsync(100);
            writeDuringReplay("k0", end);
            // a union read is answered from the complete replay only
            Thread.sleep(100);
            assertThat(read).isNotDone();
        } finally {
            gate.countDown();
        }
        recovery.get(30, TimeUnit.SECONDS);

        // the first pass ended at the old log end; the final pass replayed the write
        assertThat(src.replayedFrom).containsExactly(start, end);
        // the read's high watermark is below the write
        assertThat(offsets(read.get(30, TimeUnit.SECONDS).getLogDvBitmap()))
                .hasSize(changes - keys);
        highWatermark.set(end + 1);
        DvSnapshot snapshot = unionRead(100);
        assertThat(lakeMasks(snapshot)).containsOnly(entry(F1, set(0L)));
        // the write supersedes the last replayed version of k0
        Set<Long> masked = offsets(snapshot.getLogDvBitmap());
        assertThat(masked).hasSize(changes + 1 - keys).contains(end - keys).doesNotContain(end);
    }

    @Test
    void testWriteProceedsWhileFirstUnionReadReplaysTheLog() throws Exception {
        int keys = 100;
        int changes = 50_000;
        appendChanges(0, keys, changes);
        highWatermark.set(changes);
        manager.recover(src);
        CountDownLatch gate = pauseReplayAfter(changes / 2);

        CompletableFuture<DvSnapshot> read = unionReadAsync(-1);
        try {
            assertThat(src.replayPaused.await(10, TimeUnit.SECONDS)).isTrue();
            writeDuringReplay("k0", changes);
        } finally {
            gate.countDown();
        }

        // the write is beyond the high watermark of that read
        assertThat(offsets(read.get(30, TimeUnit.SECONDS).getLogDvBitmap()))
                .hasSize(changes - keys);
        assertThat(src.replayedFrom).containsExactly(0L, (long) changes);
        highWatermark.set(changes + 1);
        Set<Long> masked = offsets(unionRead(-1).getLogDvBitmap());
        assertThat(masked)
                .hasSize(changes + 1 - keys)
                .contains((long) changes - keys)
                .doesNotContain((long) changes);
    }

    @Test
    void testRebuildMetrics() throws Exception {
        int changes = 50_000;
        long start = 10;
        src.publish(100, 1, start, dict(1, F1), new Index().put("k0", 1, 0).build());
        src.readable = new long[] {100, start};
        appendChanges(start, 100, changes);
        highWatermark.set(start + changes);
        CountDownLatch gate = pauseReplayAfter(changes / 2);

        manager.recoverInBackground(src, new DerivedDvRebuildLimiter(1));
        try {
            assertThat(src.replayPaused.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(metrics.rebuildsInProgress()).isEqualTo(1);
            // reported as the replay goes: below the whole tail, above what is replayed
            assertThat(metrics.replayBacklog()).isGreaterThan(0).isLessThan(changes);
            Thread.sleep(100);
        } finally {
            gate.countDown();
        }
        // the slot is given back right after the state is installed
        waitUntil(() -> metrics.rebuildsInProgress() == 0);

        assertThat(manager.getReadableSnapshotId()).isEqualTo(100L);
        assertThat(metrics.replayBacklog()).isZero();
        assertThat(metrics.rebuildCount()).isEqualTo(1);
        assertThat(metrics.lastRebuildMs()).isGreaterThanOrEqualTo(100);
    }

    @Test
    void testRebuildsOfAServerWaitForAFreeSlot() throws Exception {
        DerivedDvRebuildLimiter limiter = new DerivedDvRebuildLimiter(1);
        // bucket A: a new leader whose rebuild replays a long log
        int changes = 50_000;
        long start = 10;
        src.publish(100, 1, start, dict(1, F1), new Index().put("k0", 1, 0).build());
        src.readable = new long[] {100, start};
        appendChanges(start, 100, changes);
        highWatermark.set(start + changes);
        CountDownLatch gate = pauseReplayAfter(changes / 2);
        manager.recoverInBackground(src, limiter);

        // bucket B on the same server: its first union read of the empty lake replays its log
        FakeSource srcB = new FakeSource(Files.createDirectories(tempDir.resolve("remote-b")));
        srcB.append("k1", 0, 0);
        srcB.append("k1", 1, 1);
        try (DvRocksDB dbB = DvRocksDB.open(tempDir.resolve("dv-b").toString())) {
            DerivedDvManager managerB =
                    DvTestManagers.newManager(
                            dbB, () -> 2L, DvTestManagers.CHECKPOINT_INTERVAL, metrics);
            try {
                CompletableFuture<DvSnapshot> readB;
                try {
                    assertThat(src.replayPaused.await(10, TimeUnit.SECONDS)).isTrue();
                    managerB.recoverInBackground(srcB, limiter);
                    readB =
                            CompletableFuture.supplyAsync(
                                    () -> {
                                        try {
                                            // retried while not ready, as clients do
                                            long deadline = System.currentTimeMillis() + 30_000;
                                            while (true) {
                                                try {
                                                    return DvTestReads.unionRead(managerB, -1);
                                                } catch (RetriableException e) {
                                                    if (System.currentTimeMillis() > deadline) {
                                                        throw e;
                                                    }
                                                    Thread.sleep(20);
                                                }
                                            }
                                        } catch (Exception e) {
                                            throw new CompletionException(e);
                                        }
                                    },
                                    async);
                    Thread.sleep(300);
                    // A holds the only slot
                    assertThat(readB).isNotDone();
                    assertThat(srcB.replayedFrom).isEmpty();
                    assertThat(metrics.rebuildsInProgress()).isEqualTo(1);
                } finally {
                    gate.countDown();
                }

                assertThat(offsets(readB.get(30, TimeUnit.SECONDS).getLogDvBitmap()))
                        .containsExactly(0L);
                waitUntil(() -> metrics.rebuildsInProgress() == 0);
                assertThat(manager.getReadableSnapshotId()).isEqualTo(100L);
                assertThat(srcB.replayedFrom).containsExactly(0L);
                assertThat(metrics.rebuildCount()).isEqualTo(2);
            } finally {
                managerB.close();
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------------------------

    /**
     * Snapshot 100 covers [0, 10) with k1 -> f1#0, k2 -> f1#1, k3 -> f1#2; the tail is k1@10,
     * k4@11, k1@12, -D k2@13, k3@15; the high watermark is 14.
     */
    private void recoverAtSnapshot100() throws Exception {
        src.publish(
                100,
                1,
                10,
                dict(1, F1),
                new Index().put("k1", 1, 0).put("k2", 1, 1).put("k3", 1, 2).build());
        src.readable = new long[] {100, 10};
        src.append("k1", 0, 0);
        src.append("k2", 1, 1);
        src.append("k3", 2, 2);
        src.append("k1", 10, 10);
        src.append("k4", 11, 11);
        src.append("k1", 12, 12);
        src.append("k2", 13, -1);
        highWatermark.set(14);
        // the leader takes over at log end 14, the high watermark: no read goes below a cut the
        // last leader may have served
        manager.recover(src);
        // a write above the high watermark
        src.append("k3", 15, 15);
        DvTestManagers.handOver(manager, Collections.singletonList(src.written("k3", 15, 15)));
    }

    /** A chain long enough to trigger an index checkpoint; returns the covered snapshot id. */
    private int publishLongChain() {
        int every = DvTestManagers.CHECKPOINT_INTERVAL;
        src.publish(
                1,
                1001,
                1,
                dict(1, F1),
                new Index().put("k1", 1, 0).put("k2", 1, 1).put("k3", 1, 2).build());
        for (int i = 2; i < every; i++) {
            src.publish(i, 1000 + i, i, Collections.emptyMap(), new Index().build());
        }
        src.publish(
                every,
                1000 + every,
                every,
                dict(2, F2),
                new Index().put("k2", 2, 0).delete("k3").build());
        src.readable = new long[] {every, every};
        return every;
    }

    /** Pauses the next log replay after {@code changes} changes until the returned gate opens. */
    /**
     * Recovers the bucket with no readable snapshot, and waits until the empty lake's log, which a
     * new leader replays in the background, is replayed.
     */
    private void recoverEmptyLake() throws Exception {
        manager.recover(src);
        unionRead(-1);
    }

    private CountDownLatch pauseReplayAfter(int changes) {
        CountDownLatch gate = new CountDownLatch(1);
        src.gateAfterChanges = changes;
        src.replayGate = gate;
        return gate;
    }

    /**
     * Writes a change of {@code k} at {@code offset} as the write path does (appends it to the log,
     * then hands the key change to the manager) and requires it not to wait for a replay.
     */
    private void writeDuringReplay(String k, long offset) throws Exception {
        src.append(k, offset, offset);
        CompletableFuture.runAsync(
                        () ->
                                DvTestManagers.handOver(
                                        manager,
                                        Collections.singletonList(src.written(k, offset, offset))),
                        async)
                .get(5, TimeUnit.SECONDS);
    }

    /** A union read, retried while the bucket is not ready; a stale snapshot fails it. */
    private DvSnapshot unionRead(long snapshotId, boolean waitForCatchUp) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (true) {
            try {
                return DvTestReads.unionRead(manager, snapshotId, waitForCatchUp);
            } catch (RetriableException e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(20);
            }
        }
    }

    /** A union read, retried while the bucket is not ready, as clients retry. */
    private DvSnapshot unionRead(long snapshotId) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (true) {
            try {
                return DvTestReads.unionRead(manager, snapshotId);
            } catch (RetriableException e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(20);
            }
        }
    }

    /** A tiering lookup, retried while the bucket is not ready, as the tiering service retries. */
    private List<Object[]> lookupWhenReady(long snapshotId, List<byte[]> keys) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (true) {
            try {
                return manager.lookup(snapshotId, keys);
            } catch (RetriableException e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(20);
            }
        }
    }

    private CompletableFuture<DvSnapshot> unionReadAsync(long snapshotId) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return unionRead(snapshotId);
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                },
                async);
    }

    private static void waitUntil(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }

    /** Appends {@code changes} upserts from {@code start}, cycling over {@code keys} keys. */
    private void appendChanges(long start, int keys, int changes) {
        List<Change> batch = new ArrayList<>(changes);
        for (int i = 0; i < changes; i++) {
            batch.add(new Change(key("k" + (i % keys)), start + i, start + i));
        }
        src.log.addAll(batch); // one copy of the copy-on-write log
    }

    static byte[] key(String k) {
        return k.getBytes(StandardCharsets.UTF_8);
    }

    static Map<Integer, String> dict(int fileId, String path) {
        return Collections.singletonMap(fileId, path);
    }

    static Set<Long> set(Long... values) {
        return new TreeSet<>(Arrays.asList(values));
    }

    static Set<Long> offsets(@Nullable byte[] serialized) throws IOException {
        Set<Long> out = new TreeSet<>();
        if (serialized == null) {
            return out;
        }
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        RoaringBitmapUtils.deserializeRoaringBitmap64(bitmap, serialized);
        LongIterator it = bitmap.getLongIterator();
        while (it.hasNext()) {
            out.add(it.next());
        }
        return out;
    }

    static Map<String, Set<Long>> lakeMasks(DvSnapshot snapshot) throws IOException {
        Map<String, Set<Long>> out = new HashMap<>();
        for (Map.Entry<String, byte[]> e : snapshot.getLakeDvEntries().entrySet()) {
            out.put(e.getKey(), offsets(e.getValue()));
        }
        return out;
    }

    private static List<String> asStrings(List<Object[]> lookups) {
        List<String> out = new ArrayList<>();
        for (Object[] r : lookups) {
            out.add(new String((byte[]) r[0], StandardCharsets.UTF_8) + "@" + r[1] + "#" + r[2]);
        }
        return out;
    }

    private static Map<String, FilePos> readSst(Path sst) throws RocksDBException {
        Map<String, FilePos> out = new LinkedHashMap<>();
        try (Options options = new Options();
                SstFileReader reader = new SstFileReader(options);
                ReadOptions readOptions = new ReadOptions()) {
            reader.open(sst.toString());
            try (SstFileReaderIterator it = reader.newIterator(readOptions)) {
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    out.put(
                            new String(it.key(), StandardCharsets.UTF_8),
                            FilePos.decode(it.value()));
                }
            }
        }
        return out;
    }

    /** RocksDB's default bytewise comparator. */
    private static int compareUnsigned(byte[] a, byte[] b) {
        int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            int c = (a[i] & 0xff) - (b[i] & 0xff);
            if (c != 0) {
                return c;
            }
        }
        return a.length - b.length;
    }

    /** One index change: key -> position, null = tombstone. */
    static final class Index {
        private final TreeMap<byte[], FilePos> entries =
                new TreeMap<>(DerivedDvManagerTest::compareUnsigned);

        Index put(String k, int fileId, long pos) {
            return put(key(k), fileId, pos);
        }

        Index put(byte[] k, int fileId, long pos) {
            entries.put(k, new FilePos(fileId, pos));
            return this;
        }

        Index delete(String k) {
            entries.put(key(k), null);
            return this;
        }

        TreeMap<byte[], FilePos> build() {
            return entries;
        }
    }

    /** One change of the local log, as replayed. */
    static final class Change {
        final byte[] key;
        final long recordOffset;
        final long versionOffset;
        // the change inserted the key, which had no live version
        final boolean insert;

        Change(byte[] key, long recordOffset, long versionOffset) {
            this(key, recordOffset, versionOffset, false);
        }

        Change(byte[] key, long recordOffset, long versionOffset, boolean insert) {
            this.key = key;
            this.recordOffset = recordOffset;
            this.versionOffset = versionOffset;
            this.insert = insert;
        }
    }

    /** In-memory durable state of a bucket: the log, the published index chain, checkpoints. */
    /** What the bucket holds of its change list and masks. */
    private static String stateSummary(DerivedDvManager m) {
        return m.inspect().toString();
    }

    static final class FakeSource implements DerivedDvManager.RecoverySource {
        private final Path dir;
        private final AtomicInteger files = new AtomicInteger();

        volatile long[] readable;
        final Map<Long, Long> snapshotOffsets = new ConcurrentHashMap<>();
        final List<PositionIndexStore.ChainEntry> chain = new CopyOnWriteArrayList<>();
        final Map<Long, Map<Integer, String>> fileDicts = new ConcurrentHashMap<>();
        final Map<Long, TreeMap<byte[], FilePos>> indexChanges = new ConcurrentHashMap<>();
        final Map<Long, List<String>> replaced = new ConcurrentHashMap<>();
        final List<Change> log = new CopyOnWriteArrayList<>();
        final List<Long> replayedFrom = new CopyOnWriteArrayList<>();
        final List<Long> downloadedIndexIds = new CopyOnWriteArrayList<>();

        volatile Exception replayFailure;
        // the replay fails with replayFailure after delivering this many changes (-1: at once)
        volatile int failAfterChanges = -1;
        // sampled after every replayed change, for the peak number of queued changes
        volatile IntSupplier queueProbe;
        volatile int maxQueuedDuringReplay;
        volatile CountDownLatch chainGate;
        // the next replay waits for replayGate after delivering gateAfterChanges changes
        volatile CountDownLatch replayGate;
        volatile int gateAfterChanges;
        final CountDownLatch replayPaused = new CountDownLatch(1);
        // when set, chain() and readChange() read a real index store (for one bucket)
        volatile PositionIndexStore store;
        volatile TableBucket storeBucket;

        void useStore(PositionIndexStore store, TableBucket bucket) {
            this.store = store;
            this.storeBucket = bucket;
        }

        final CountDownLatch chainEntered = new CountDownLatch(1);
        // when set, readChange() waits for it once it counted down changeEntered
        volatile CountDownLatch changeGate;
        final CountDownLatch changeEntered = new CountDownLatch(1);
        // the next chain() fails with this, once
        volatile IOException chainFailure;

        volatile boolean trimmed;
        // snapshotIdentity() fails with this
        volatile Exception identityFailure;
        // readChange() adds a file that is no SST to each download
        volatile boolean corruptDownloads;
        volatile PositionIndexStore.Checkpoint checkpoint;
        volatile TreeMap<byte[], FilePos> checkpointIndex;
        volatile Map<Integer, String> checkpointDict = Collections.emptyMap();
        final List<Long> downloadedCheckpoints = new CopyOnWriteArrayList<>();

        final CountDownLatch uploaded = new CountDownLatch(1);
        volatile Runnable onUpload;
        volatile CountDownLatch uploadGate;
        volatile long uploadedSnapshotId = -1;
        volatile Path uploadedSst;
        // the local file the manager uploaded
        @Nullable volatile String uploadedFrom;
        volatile Map<Integer, String> uploadedDict;

        FakeSource(Path dir) {
            this.dir = dir;
        }

        void publish(
                long snapshotId,
                long indexId,
                long tieredOffset,
                Map<Integer, String> fileDict,
                TreeMap<byte[], FilePos> change) {
            indexChanges.put(indexId, change);
            fileDicts.put(indexId, fileDict);
            snapshotOffsets.put(snapshotId, tieredOffset);
            long parent = chain.isEmpty() ? -1L : chain.get(chain.size() - 1).snapshotId;
            chain.add(
                    new PositionIndexStore.ChainEntry(
                            chain.size() + 1L, parent, snapshotId, indexId));
        }

        void append(String k, long recordOffset, long versionOffset) {
            append(k, recordOffset, versionOffset, false);
        }

        void append(String k, long recordOffset, long versionOffset, boolean insert) {
            log.add(new Change(key(k), recordOffset, versionOffset, insert));
        }

        /**
         * The change of {@code k} at {@code recordOffset} as the write path hands it over: it names
         * the version it supersedes, the tag of the key's KV value. That is the key's last version
         * in the log before the change, or -1 if the log has none (the version is in the lake).
         */
        DvEntry written(String k, long recordOffset, long versionOffset) {
            return written(k, recordOffset, versionOffset, false);
        }

        DvEntry written(String k, long recordOffset, long versionOffset, boolean insert) {
            byte[] key = key(k);
            long previous = -1;
            long previousOffset = -1;
            for (Change c : log) {
                if (c.recordOffset < recordOffset
                        && c.recordOffset > previousOffset
                        && Arrays.equals(c.key, key)) {
                    previous = c.versionOffset;
                    previousOffset = c.recordOffset;
                }
            }
            return new DvEntry(key, recordOffset, versionOffset, insert, previous);
        }

        @Nullable
        @Override
        public long[] readableSnapshot() {
            return readable;
        }

        @Nullable
        @Override
        public Long snapshotOffset(long snapshotId) {
            return snapshotOffsets.get(snapshotId);
        }

        @Override
        public List<PositionIndexStore.ChainEntry> chain()
                throws InterruptedException, IOException {
            IOException failure = chainFailure;
            if (failure != null) {
                chainFailure = null;
                throw failure;
            }
            CountDownLatch gate = chainGate;
            if (gate != null) {
                chainEntered.countDown();
                gate.await(30, TimeUnit.SECONDS);
            }
            PositionIndexStore s = store;
            if (s != null) {
                try {
                    return s.chain();
                } catch (java.io.IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            }
            return new ArrayList<>(chain);
        }

        @Override
        public PositionIndexStore.BucketData readChange(PositionIndexStore.ChainEntry entry)
                throws Exception {
            CountDownLatch gate = changeGate;
            if (gate != null) {
                changeEntered.countDown();
                gate.await(30, TimeUnit.SECONDS);
            }
            long indexId = entry.changeId;
            downloadedIndexIds.add(indexId);
            PositionIndexStore s = store;
            if (s != null) {
                return s.readChange(
                        entry, storeBucket, Files.createTempDirectory(dir, "dl").toFile());
            }
            TreeMap<byte[], FilePos> change = indexChanges.get(indexId);
            List<String> ssts = new ArrayList<>();
            if (change != null && !change.isEmpty()) {
                ssts.add(writeSst(change));
            }
            if (corruptDownloads) {
                Path corrupt = dir.resolve("sst-" + files.incrementAndGet() + ".sst");
                Files.write(corrupt, new byte[] {1, 2, 3});
                ssts.add(corrupt.toString());
            }
            return new PositionIndexStore.BucketData(
                    ssts,
                    fileDicts.getOrDefault(indexId, Collections.emptyMap()),
                    replaced.getOrDefault(indexId, Collections.emptyList()));
        }

        @Nullable
        @Override
        public String snapshotIdentity(PositionIndexStore.ChainEntry entry) throws Exception {
            Exception failure = identityFailure;
            if (failure != null) {
                throw failure;
            }
            return null;
        }

        // the log end of the changes scanned so far; changes are only appended
        private int scannedChanges;
        private long scannedEnd;

        // the next logEndOffset() fails with this, once
        volatile RuntimeException logEndFailure;

        @Override
        public synchronized long logEndOffset() {
            RuntimeException failure = logEndFailure;
            if (failure != null) {
                logEndFailure = null;
                throw failure;
            }
            int size = log.size();
            if (size < scannedChanges) {
                scannedChanges = 0;
                scannedEnd = 0;
            }
            for (int i = scannedChanges; i < size; i++) {
                scannedEnd = Math.max(scannedEnd, log.get(i).recordOffset + 1);
            }
            scannedChanges = size;
            return scannedEnd;
        }

        @Override
        public long replayLog(
                long fromOffset, long toOffset, DerivedDvManager.ChangeConsumer consumer)
                throws Exception {
            replayedFrom.add(fromOffset);
            Exception failure = replayFailure;
            if (failure != null && failAfterChanges < 0) {
                throw failure;
            }
            long next = fromOffset;
            int delivered = 0;
            for (Change c : log) {
                if (c.recordOffset >= fromOffset && c.recordOffset < toOffset) {
                    if (failure != null && delivered == failAfterChanges) {
                        throw failure;
                    }
                    CountDownLatch gate = replayGate;
                    if (gate != null && delivered == gateAfterChanges) {
                        replayGate = null;
                        replayPaused.countDown();
                        gate.await(30, TimeUnit.SECONDS);
                    }
                    consumer.accept(c.key.clone(), c.recordOffset, c.versionOffset, c.insert);
                    next = c.recordOffset + 1;
                    delivered++;
                    IntSupplier probe = queueProbe;
                    if (probe != null) {
                        maxQueuedDuringReplay = Math.max(maxQueuedDuringReplay, probe.getAsInt());
                    }
                }
            }
            return next;
        }

        @Override
        public boolean chainTrimmed() {
            return trimmed;
        }

        @Nullable
        @Override
        public PositionIndexStore.Checkpoint latestCheckpoint(
                List<PositionIndexStore.ChainEntry> chain, int upTo) {
            return checkpoint;
        }

        @Override
        public PositionIndexStore.BucketData readCheckpoint(PositionIndexStore.Checkpoint cp)
                throws Exception {
            downloadedCheckpoints.add(cp.snapshotId);
            TreeMap<byte[], FilePos> index = checkpointIndex;
            return new PositionIndexStore.BucketData(
                    index == null || index.isEmpty()
                            ? Collections.emptyList()
                            : Collections.singletonList(writeSst(index)),
                    checkpointDict,
                    Collections.emptyList());
        }

        @Override
        public void uploadCheckpoint(
                long snapshotId, @Nullable String localSst, Map<Integer, String> fileDict)
                throws Exception {
            Runnable probe = onUpload;
            if (probe != null) {
                probe.run();
            }
            CountDownLatch gate = uploadGate;
            if (gate != null) {
                gate.await(60, TimeUnit.SECONDS);
            }
            uploadedFrom = localSst;
            if (localSst != null) {
                // the manager deletes its temp file after the upload
                Path copy = dir.resolve("uploaded-" + files.incrementAndGet() + ".sst");
                Files.copy(Paths.get(localSst), copy);
                uploadedSst = copy;
            }
            uploadedDict = new HashMap<>(fileDict);
            uploadedSnapshotId = snapshotId;
            uploaded.countDown();
        }

        /** A fresh SST per call: ingestion moves the file away. */
        private String writeSst(TreeMap<byte[], FilePos> entries) throws RocksDBException {
            String path = dir.resolve("sst-" + files.incrementAndGet() + ".sst").toString();
            try (EnvOptions env = new EnvOptions();
                    Options options = new Options();
                    SstFileWriter writer = new SstFileWriter(env, options)) {
                writer.open(path);
                for (Map.Entry<byte[], FilePos> e : entries.entrySet()) {
                    if (e.getValue() == null) {
                        writer.delete(e.getKey());
                    } else {
                        writer.put(e.getKey(), e.getValue().encode());
                    }
                }
                writer.finish();
            }
            return path;
        }
    }
}
