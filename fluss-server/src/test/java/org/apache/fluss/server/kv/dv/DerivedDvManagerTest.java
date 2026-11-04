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

import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.index.KeyPosition;
import org.apache.fluss.lake.dv.index.PositionIndexChangeBuilder;
import org.apache.fluss.lake.dv.index.PositionIndexDelta;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.entity.DvPositionReportData;
import org.apache.fluss.server.utils.RoaringBitmapUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileReader;
import org.rocksdb.SstFileReaderIterator;
import org.rocksdb.SstFileWriter;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
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
    private DerivedDvManager manager;
    private FakeSource src;

    @BeforeEach
    void setUp() throws Exception {
        db = DvRocksDB.open(tempDir.resolve("dv").toString());
        highWatermark = new AtomicLong(0);
        manager = new DerivedDvManager(db, new DvRWLock(), highWatermark::get);
        src = new FakeSource(Files.createDirectories(tempDir.resolve("remote")));
    }

    @AfterEach
    void tearDown() {
        manager.close();
        db.close();
    }

    // ------------------------------------------------------------------------------------------
    // empty lake (no readable snapshot yet)
    // ------------------------------------------------------------------------------------------

    @Test
    void testEmptyLakeUnionReadReplaysWholeLogAndMasksSupersededVersions() throws Exception {
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
        assertThat(manager.getSnapshotStartLogOffset()).isEqualTo(0L);
        // the log is replayed lazily, on the first union read of the empty lake
        assertThat(src.replayedFrom).isEmpty();

        DvSnapshot snapshot = manager.getDvForUnionRead(-1, 0);

        assertThat(src.replayedFrom).containsExactly(0L);
        assertThat(snapshot.getLakeDvEntries()).isEmpty();
        // k1@0 and k1@3 superseded by later updates, k2@1 deleted; k2@8, k3@4, k1@7 live
        assertThat(offsets(snapshot.getLogDvBitmap())).containsExactly(0L, 1L, 3L);
        assertThat(snapshot.getLogEndOffset()).isEqualTo(9L);
        assertThat(snapshot.getSnapshotStartOffset()).isEqualTo(0L);

        // a second read does not replay again
        manager.getDvForUnionRead(-1, 0);
        assertThat(src.replayedFrom).containsExactly(0L);
    }

    @Test
    void testEmptyLakeMasksOnlyChangesBelowHighWatermark() throws Exception {
        src.append("k1", 0, 0);
        src.append("k1", 2, 2);
        src.append("k1", 4, 4);
        src.append("k2", 5, 5);
        src.append("k2", 6, 6);
        manager.recover(src);

        highWatermark.set(3);
        DvSnapshot snapshot = manager.getDvForUnionRead(-1, 0);
        // k1@4 is beyond the high watermark, so it must not mask k1@2
        assertThat(offsets(snapshot.getLogDvBitmap())).containsExactly(0L);
        assertThat(snapshot.getLogEndOffset()).isEqualTo(3L);

        highWatermark.set(5);
        snapshot = manager.getDvForUnionRead(-1, 0);
        assertThat(offsets(snapshot.getLogDvBitmap())).containsExactly(0L, 2L);
        assertThat(snapshot.getLogEndOffset()).isEqualTo(5L);

        highWatermark.set(7);
        snapshot = manager.getDvForUnionRead(-1, 0);
        assertThat(offsets(snapshot.getLogDvBitmap())).containsExactly(0L, 2L, 5L);
        assertThat(snapshot.getLogEndOffset()).isEqualTo(7L);
        assertThat(src.replayedFrom).containsExactly(0L);
    }

    @Test
    void testDuplicateDeliveryFromReplayAndWritePathDoesNotMaskLiveVersion() throws Exception {
        src.append("k1", 0, 0);
        src.append("k1", 2, 2);
        highWatermark.set(3);
        manager.recover(src);
        assertThat(offsets(manager.getDvForUnionRead(-1, 0).getLogDvBitmap())).containsExactly(0L);

        // the write path delivers changes the replay has already applied
        manager.handleChangelogSynced(
                Arrays.asList(DvEntry.derived(key("k1"), 0, 0), DvEntry.derived(key("k1"), 2, 2)));
        assertThat(offsets(manager.getDvForUnionRead(-1, 0).getLogDvBitmap())).containsExactly(0L);

        // a genuinely new change still masks the version it supersedes
        src.append("k1", 4, 4);
        manager.handleChangelogSynced(Collections.singletonList(DvEntry.derived(key("k1"), 4, 4)));
        highWatermark.set(5);
        assertThat(offsets(manager.getDvForUnionRead(-1, 0).getLogDvBitmap()))
                .containsExactly(0L, 2L);
    }

    @Test
    void testDuplicateQueuedBeyondHighWatermarkIsAppliedOnce() throws Exception {
        src.append("k1", 0, 0);
        src.append("k1", 2, 2);
        highWatermark.set(1);
        manager.recover(src);
        // k1@2 stays queued (beyond the high watermark)
        assertThat(manager.getDvForUnionRead(-1, 0).getLogDvBitmap()).isNull();

        // the write path delivers k1@2 again while the replayed copy is still queued
        manager.handleChangelogSynced(Collections.singletonList(DvEntry.derived(key("k1"), 2, 2)));
        highWatermark.set(3);
        assertThat(offsets(manager.getDvForUnionRead(-1, 0).getLogDvBitmap())).containsExactly(0L);
    }

    @Test
    void testWritePathIgnoredUntilEmptyLakeIsTracked() throws Exception {
        manager.recover(src);
        // the change is in the log and is also delivered by the write path before any union read
        src.append("k1", 0, 0);
        src.append("k1", 1, 1);
        manager.handleChangelogSynced(
                Arrays.asList(DvEntry.derived(key("k1"), 0, 0), DvEntry.derived(key("k1"), 1, 1)));
        assertThat(manager.stateSummary()).contains("tailKeys=0").contains("queued=0");

        highWatermark.set(2);
        assertThat(offsets(manager.getDvForUnionRead(-1, 0).getLogDvBitmap())).containsExactly(0L);
        assertThat(src.replayedFrom).containsExactly(0L);
    }

    @Test
    void testDeleteMasksPreviousVersionAndReinsertMasksNothing() throws Exception {
        manager.recover(src);
        src.append("k1", 0, 0);
        src.append("k1", 1, -1);
        src.append("k1", 2, 2);
        src.append("k1", 3, -1);
        highWatermark.set(4);

        DvSnapshot snapshot = manager.getDvForUnionRead(-1, 0);

        // the deletes have no live version, so nothing beyond the two inserts is masked
        assertThat(offsets(snapshot.getLogDvBitmap())).containsExactly(0L, 2L);
    }

    @Test
    void testStaleSnapshotRequestsOnEmptyLake() throws Exception {
        manager.recover(src);
        List<byte[]> keys = Collections.singletonList(key("k1"));

        // snapshot 5 is not registered: no catch-up, the reader is told to re-plan
        assertThatThrownBy(() -> manager.getDvForUnionRead(5, 0))
                .isInstanceOfSatisfying(
                        StaleSnapshotException.class,
                        e -> {
                            assertThat(e.getCurrentSnapshotId()).isEqualTo(-1L);
                            assertThat(e.getRequestedSnapshotId()).isEqualTo(5L);
                        });
        assertThatThrownBy(() -> manager.lookup(5, keys))
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
        assertThat(manager.getSnapshotStartLogOffset()).isEqualTo(10L);

        DvSnapshot snapshot = manager.getDvForUnionRead(100, 0);
        // k1 (updated) and k2 (deleted) changed in [10, 14): their lake rows are masked; k3's
        // change @15 is beyond the high watermark, so its lake row stays visible
        assertThat(lakeMasks(snapshot)).containsOnly(entry(F1, set(0L, 1L)));
        assertThat(offsets(snapshot.getLogDvBitmap())).containsExactly(10L);
        assertThat(snapshot.getLogEndOffset()).isEqualTo(14L);
        assertThat(snapshot.getSnapshotStartOffset()).isEqualTo(10L);

        highWatermark.set(16);
        snapshot = manager.getDvForUnionRead(100, 0);
        assertThat(lakeMasks(snapshot)).containsOnly(entry(F1, set(0L, 1L, 2L)));
        assertThat(offsets(snapshot.getLogDvBitmap())).containsExactly(10L);
        assertThat(snapshot.getLogEndOffset()).isEqualTo(16L);
    }

    @Test
    void testFailedTailReplayFailsUnionReadsUntilReplaySucceeds() throws Exception {
        src.replayFailure = new IllegalStateException("log gone");
        recoverAtSnapshot100();

        // the index is in place, so lookups still work
        assertThat(asStrings(manager.lookup(100, Collections.singletonList(key("k1")))))
                .containsExactly("k1@" + F1 + "#0");
        // masks without the tail would miss k1 and k2
        assertThatThrownBy(() -> manager.getDvForUnionRead(100, 0))
                .isInstanceOf(IOException.class)
                .hasRootCauseMessage("log gone");
        assertThatThrownBy(() -> manager.getDvForUnionRead(100, 0)).isInstanceOf(IOException.class);

        src.replayFailure = null;
        DvSnapshot snapshot = manager.getDvForUnionRead(100, 0);
        assertThat(lakeMasks(snapshot)).containsOnly(entry(F1, set(0L, 1L)));
        assertThat(offsets(snapshot.getLogDvBitmap())).containsExactly(10L);
        assertThat(snapshot.getSnapshotStartOffset()).isEqualTo(10L);
    }

    @Test
    void testLookupReturnsPositionsInCoveredSnapshot() throws Exception {
        recoverAtSnapshot100();

        List<Object[]> found =
                manager.lookup(100, Arrays.asList(key("k1"), key("k2"), key("k3"), key("k9")));

        assertThat(asStrings(found))
                .containsExactly("k1@" + F1 + "#0", "k2@" + F1 + "#1", "k3@" + F1 + "#2");
        assertThatThrownBy(() -> manager.lookup(99, Collections.singletonList(key("k1"))))
                .isInstanceOf(StaleSnapshotException.class);
        assertThatThrownBy(() -> manager.getDvForUnionRead(-1, 0))
                .isInstanceOf(StaleSnapshotException.class);
    }

    @Test
    void testUnionReadOfNewerSnapshotCatchesUpIncrementally() throws Exception {
        recoverAtSnapshot100();
        highWatermark.set(16);
        manager.getDvForUnionRead(100, 0);

        // round 2 tiers [10, 14): k1@12 -> f2#0, k4@11 -> f2#1, k2 deleted
        src.publish(
                200,
                2,
                14,
                dict(2, F2),
                new Index().put("k1", 2, 0).put("k4", 2, 1).delete("k2").build());

        DvSnapshot snapshot = manager.getDvForUnionRead(200, 0);

        assertThat(manager.getReadableSnapshotId()).isEqualTo(200L);
        assertThat(src.downloadedIndexIds).containsExactly(1L, 2L);
        // no log replay on an incremental advance
        assertThat(src.replayedFrom).containsExactly(10L);
        assertThat(snapshot.getSnapshotStartOffset()).isEqualTo(14L);
        // only k3 changed since 14: its lake row f1#2 is masked; tail masks below 14 are dropped
        assertThat(lakeMasks(snapshot)).containsOnly(entry(F1, set(2L)));
        assertThat(snapshot.getLogDvBitmap()).isNull();

        // a new change of k4 masks its row in the new snapshot
        src.append("k4", 16, 16);
        manager.handleChangelogSynced(
                Collections.singletonList(DvEntry.derived(key("k4"), 16, 16)));
        highWatermark.set(17);
        snapshot = manager.getDvForUnionRead(200, 0);
        assertThat(lakeMasks(snapshot)).containsOnly(entry(F1, set(2L)), entry(F2, set(1L)));

        assertThat(
                        asStrings(
                                manager.lookup(
                                        200,
                                        Arrays.asList(key("k1"), key("k2"), key("k3"), key("k4")))))
                .containsExactly("k1@" + F2 + "#0", "k3@" + F1 + "#2", "k4@" + F2 + "#1");

        // the older snapshot is gone for good
        assertThatThrownBy(() -> manager.getDvForUnionRead(100, 0))
                .isInstanceOf(StaleSnapshotException.class);
        assertThatThrownBy(() -> manager.lookup(100, Collections.singletonList(key("k1"))))
                .isInstanceOf(StaleSnapshotException.class);
        assertThat(manager.ensureCovered(100)).isFalse();
        assertThat(manager.getReadableSnapshotId()).isEqualTo(200L);
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
        assertThat(manager.getSnapshotStartLogOffset()).isEqualTo(14L);
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
        DvSnapshot snapshot = manager.getDvForUnionRead(100, 0);
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
        assertThat(asStrings(manager.lookup(300, Arrays.asList(key("k1"), key("k2"), key("k3")))))
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

    @Test
    void testCloseWaitsForInFlightCheckpoint() throws Exception {
        publishLongChain();
        src.uploadGate = new CountDownLatch(1);
        manager.recover(src);

        CompletableFuture<Void> closing = CompletableFuture.runAsync(manager::close);
        Thread.sleep(300);
        assertThat(closing).isNotDone();

        src.uploadGate.countDown();
        closing.get(10, TimeUnit.SECONDS);
        assertThat(src.uploaded.getCount()).isZero();
    }

    @Test
    void testCloseReturnsPromptlyWhenIdle() throws Exception {
        src.append("k1", 0, 0);
        src.append("k1", 1, 1);
        src.append("k1", 5, 5);
        highWatermark.set(2);
        manager.recover(src);
        manager.getDvForUnionRead(-1, 0);
        // a change beyond the high watermark keeps the background resolver rescheduling
        manager.handleChangelogSynced(Collections.singletonList(DvEntry.derived(key("k1"), 5, 5)));
        Thread.sleep(20);

        long start = System.nanoTime();
        manager.close();
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)).isLessThan(1000L);
    }

    @Test
    void testReadableSwitchWithoutPrepareRebuildsFromDurableState() throws Exception {
        manager.recover(src);
        src.publish(100, 1, 10, dict(1, F1), new Index().put("k1", 1, 0).put("k2", 1, 1).build());
        src.readable = new long[] {100, 10};
        src.append("k1", 10, 10);
        highWatermark.set(11);

        manager.handleReadableSwitch(0, 100, 10);

        assertThat(manager.getReadableSnapshotId()).isEqualTo(100L);
        assertThat(lakeMasks(manager.getDvForUnionRead(100, 0))).containsOnly(entry(F1, set(0L)));
        // a repeated switch is a no-op
        manager.handleReadableSwitch(0, 100, 10);
        assertThat(src.replayedFrom).containsExactly(10L);
    }

    @Test
    void testRecoverAndPreparedSwitchThroughTheIndexStore() throws Exception {
        // the tiering-side builder writes into the real store and the manager reads it
        PositionIndexStore store =
                PositionIndexStore.forTable(new FsPath(tempDir.resolve("lake").toUri()));
        TableBucket bucket = new TableBucket(1L, 0);
        src.useStore(store, bucket);
        PositionIndexChangeBuilder round1 = new PositionIndexChangeBuilder(store);
        round1.add(
                bucket,
                new PositionIndexDelta(
                        Arrays.asList(key("k1"), key("k2")),
                        Arrays.asList(
                                new KeyPosition(key("k2"), "f1.parquet", 1),
                                new KeyPosition(key("k1"), "f1.parquet", 0)),
                        Collections.emptyList()),
                Collections.singletonMap("f1.parquet", 1));
        round1.commit(7);
        store.register(-1, 100, 7);
        src.snapshotOffsets.put(100L, 10L);
        src.readable = new long[] {100, 10};
        src.append("k1", 0, 0);
        src.append("k2", 1, 1);
        src.append("k1", 10, 10);
        highWatermark.set(11);

        manager.recover(src);

        // k1 changed at 10 (after the snapshot): its lake row is masked
        assertThat(lakeMasks(manager.getDvForUnionRead(100, 0)))
                .containsOnly(entry("f1.parquet", set(0L)));

        // round 2 tiers [10, 11): k1 moves to f2#0; the coordinator prepares, then switches
        PositionIndexChangeBuilder round2 = new PositionIndexChangeBuilder(store);
        round2.add(
                bucket,
                new PositionIndexDelta(
                        Collections.singletonList(key("k1")),
                        Collections.singletonList(new KeyPosition(key("k1"), "f2.parquet", 0)),
                        Collections.emptyList()),
                Collections.singletonMap("f2.parquet", 2));
        round2.commit(8);
        store.register(100, 200, 8);
        src.snapshotOffsets.put(200L, 11L);
        manager.handlePrepare(
                0,
                null,
                new DvPositionReportData.DvBucketOffset(
                        11, Collections.singletonMap(2, "f2.parquet"), Collections.emptyList()),
                null,
                8,
                tempDir.resolve("download").toString());
        manager.setPreparedReadable(200);
        manager.handleReadableSwitch(0, 200, 11);

        DvSnapshot snapshot = manager.getDvForUnionRead(200, 0);
        assertThat(snapshot.getSnapshotStartOffset()).isEqualTo(11L);
        assertThat(lakeMasks(snapshot)).isEmpty();
        assertThat(asStrings(manager.lookup(200, Arrays.asList(key("k1"), key("k2")))))
                .containsExactly("k1@f2.parquet#0", "k2@f1.parquet#1");
    }

    @Test
    void testPreparedSwitchDropsFilesTheIndexChangeRemoves() throws Exception {
        recoverAtSnapshot100();
        // snapshot 200 removes F1 without replacement (nothing tiered since 10)
        src.publish(200, 2, 10, Collections.emptyMap(), new Index().build());
        src.replaced.put(2L, Collections.singletonList(F1));

        // prepare reads the change from the index store; the switch applies its removed files too
        manager.handlePrepare(
                0,
                null,
                new DvPositionReportData.DvBucketOffset(
                        10, Collections.emptyMap(), Collections.emptyList()),
                null,
                2,
                tempDir.resolve("download").toString());
        manager.setPreparedReadable(200);
        manager.handleReadableSwitch(0, 200, 10);

        assertThat(manager.getReadableSnapshotId()).isEqualTo(200L);
        assertThat(db.fileDict().getFilePath(1)).isNull();
        // no lookup resolves into the removed file any more
        assertThat(manager.lookup(200, Arrays.asList(key("k1"), key("k3")))).isEmpty();
    }

    @Test
    void testCatchUpDropsFilesTheIndexChangeRemoves() throws Exception {
        recoverAtSnapshot100();
        src.publish(200, 2, 10, Collections.emptyMap(), new Index().build());
        src.replaced.put(2L, Collections.singletonList(F1));

        assertThat(manager.ensureCovered(200)).isTrue();

        assertThat(db.fileDict().getFilePath(1)).isNull();
        assertThat(manager.lookup(200, Arrays.asList(key("k1"), key("k3")))).isEmpty();
    }

    @Test
    void testCloseFencesInFlightCatchUp() throws Exception {
        manager.recover(src); // empty lake: the first snapshot is reached by a full rebuild
        src.publish(100, 1, 10, dict(1, F1), new Index().put("k1", 1, 0).build());
        // the catch-up blocks in a slow remote listing of the chain (outside the manager lock)
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

        // KvTablet#close: DvManager#close, then DvRocksDB#close
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
            // the tablet would have closed the DV RocksDB by now: nothing may be written into it
            assertThat(src.downloadedIndexIds).isEmpty();
            assertThat(db.fileDict().getFilePath(1)).isNull();
            assertThat(manager.getReadableSnapshotId()).isEqualTo(-1L);
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

        assertThat(asStrings(manager.lookup(400, Collections.singletonList(key("k1")))))
                .containsExactly("k1@" + F1 + "#0");
        assertThat(lakeMasks(manager.getDvForUnionRead(400, 0))).containsOnly(entry(F1, set(0L)));
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
        src.append("k3", 15, 15);
        highWatermark.set(14);
        manager.recover(src);
    }

    /** A chain long enough to trigger an index checkpoint; returns the covered snapshot id. */
    private int publishLongChain() {
        int every = DerivedDvManager.DEFAULT_CHECKPOINT_INTERVAL;
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

    private static byte[] key(String k) {
        return k.getBytes(StandardCharsets.UTF_8);
    }

    private static Map<Integer, String> dict(int fileId, String path) {
        return Collections.singletonMap(fileId, path);
    }

    private static Set<Long> set(Long... values) {
        return new TreeSet<>(Arrays.asList(values));
    }

    private static Set<Long> offsets(@Nullable byte[] serialized) throws IOException {
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

    private static Map<String, Set<Long>> lakeMasks(DvSnapshot snapshot) throws IOException {
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
    private static final class Index {
        private final TreeMap<byte[], FilePos> entries =
                new TreeMap<>(DerivedDvManagerTest::compareUnsigned);

        Index put(String k, int fileId, long pos) {
            entries.put(key(k), new FilePos(fileId, pos));
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
    private static final class Change {
        final byte[] key;
        final long recordOffset;
        final long versionOffset;

        Change(byte[] key, long recordOffset, long versionOffset) {
            this.key = key;
            this.recordOffset = recordOffset;
            this.versionOffset = versionOffset;
        }
    }

    /** In-memory durable state of a bucket: the log, the published index chain, checkpoints. */
    private static final class FakeSource implements DerivedDvManager.RecoverySource {
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
        volatile CountDownLatch chainGate;
        // when set, chain() and readChange() read a real index store (for one bucket)
        volatile PositionIndexStore store;
        volatile TableBucket storeBucket;

        void useStore(PositionIndexStore store, TableBucket bucket) {
            this.store = store;
            this.storeBucket = bucket;
        }

        final CountDownLatch chainEntered = new CountDownLatch(1);

        volatile boolean trimmed;
        volatile PositionIndexStore.Checkpoint checkpoint;
        volatile TreeMap<byte[], FilePos> checkpointIndex;
        volatile Map<Integer, String> checkpointDict = Collections.emptyMap();
        final List<Long> downloadedCheckpoints = new CopyOnWriteArrayList<>();

        final CountDownLatch uploaded = new CountDownLatch(1);
        volatile CountDownLatch uploadGate;
        volatile long uploadedSnapshotId = -1;
        volatile Path uploadedSst;
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
            log.add(new Change(key(k), recordOffset, versionOffset));
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
        public List<PositionIndexStore.ChainEntry> chain() throws InterruptedException {
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
            long indexId = entry.changeId;
            downloadedIndexIds.add(indexId);
            PositionIndexStore s = store;
            if (s != null) {
                return s.readChange(
                        entry, storeBucket, Files.createTempDirectory(dir, "dl").toFile());
            }
            TreeMap<byte[], FilePos> change = indexChanges.get(indexId);
            return new PositionIndexStore.BucketData(
                    change == null || change.isEmpty()
                            ? Collections.emptyList()
                            : Collections.singletonList(writeSst(change)),
                    fileDicts.getOrDefault(indexId, Collections.emptyMap()),
                    replaced.getOrDefault(indexId, Collections.emptyList()));
        }

        @Override
        public void replayLog(long fromOffset, DerivedDvManager.ChangeConsumer consumer)
                throws Exception {
            replayedFrom.add(fromOffset);
            Exception failure = replayFailure;
            if (failure != null) {
                throw failure;
            }
            for (Change c : log) {
                if (c.recordOffset >= fromOffset) {
                    consumer.accept(c.key.clone(), c.recordOffset, c.versionOffset);
                }
            }
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
            CountDownLatch gate = uploadGate;
            if (gate != null) {
                gate.await(30, TimeUnit.SECONDS);
            }
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
