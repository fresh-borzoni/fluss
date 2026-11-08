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

import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.lake.dv.FilePos;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

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

    // ------------------------------------------------------------------------------------------
    // covered lake snapshot
    // ------------------------------------------------------------------------------------------

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

    /** The number of RocksDB snapshots of the DV RocksDB that are not released. */
    private static long openRocksDbSnapshots(DvRocksDB dvRocksDB) {
        return dvRocksDB.openSnapshots();
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

    // held by moveWithLookupsHeld until releaseLookups
    private CountDownLatch heldLookups;

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

    // ------------------------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------------------------

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

    private static void waitUntil(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertThat(condition.getAsBoolean()).isTrue();
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

    static final class FakeSource implements DerivedDvManager.RecoverySource {
        private final Path dir;
        private final AtomicInteger files = new AtomicInteger();

        volatile long[] readable;
        final Map<Long, Long> snapshotOffsets = new ConcurrentHashMap<>();
        final List<PositionIndexStore.ChainEntry> chain = new CopyOnWriteArrayList<>();
        final Map<Long, Map<Integer, String>> fileDicts = new ConcurrentHashMap<>();
        final Map<Long, TreeMap<byte[], FilePos>> indexChanges = new ConcurrentHashMap<>();
        final Map<Long, List<String>> replaced = new ConcurrentHashMap<>();
        final List<Long> downloadedIndexIds = new CopyOnWriteArrayList<>();
        volatile CountDownLatch chainGate;

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
