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
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.server.kv.dv.DerivedDvManagerTest.FakeSource;
import org.apache.fluss.server.kv.dv.DerivedDvManagerTest.Index;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.lakeMasks;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.offsets;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the cuts union reads pin ({@link DerivedDvPinnedCuts}, {@link DvPinnedCut}). */
class DerivedDvPinnedCutsTest {

    private static final String F1 = "s3://lake/t/data/f1.parquet";
    private static final String F2 = "s3://lake/t/data/f2.parquet";
    private static final Duration IDLE = Duration.ofMinutes(30);

    @TempDir Path tempDir;

    private final AtomicLong highWatermark = new AtomicLong();
    private final AtomicLong clock = new AtomicLong();
    private final List<DerivedDvManager> managers = new ArrayList<>();
    // recovered buckets, which the write path hands its changes to
    private final List<DerivedDvManager> live = new ArrayList<>();
    private final Random random = new Random(42);
    // {offset, key, version} of every write
    private final List<long[]> changes = new ArrayList<>();
    private FakeSource src;
    private DerivedDvTailBudget budget;
    private DerivedDvMetrics metrics;
    private DerivedDvManager manager;
    private long logEnd;
    private int nextFile = 3;

    @BeforeEach
    void setUp() throws IOException {
        src = new FakeSource(Files.createDirectories(tempDir.resolve("remote")));
        budget = new DerivedDvTailBudget(Long.MAX_VALUE, IDLE, clock::get);
        metrics = DerivedDvMetrics.noop();
        manager = newManager("bucket", budget, metrics);
    }

    @AfterEach
    void tearDown() {
        for (DerivedDvManager m : managers) {
            m.close();
        }
    }

    /**
     * Workers fetch their parts of a pinned cut at different times while writes continue, the
     * bucket spills its tail and advances to newer snapshots. Every part equals that of a bucket
     * that never moved past the cut.
     */
    @Test
    void testWorkersReadThePinnedCutWhileTheBucketMovesOn() throws Exception {
        recoverAtSnapshot100(manager);
        write(190);
        highWatermark.set(logEnd);
        long hw = logEnd;

        DvPinnedCut cut = pin(manager, 100);
        assertThat(cut.logEndOffset()).isEqualTo(hw);
        DvSnapshot reference = referenceMasks(100, hw);
        assertThat(lakeMasks(reference)).isNotEmpty();
        assertThat(offsets(reference.getLogDvBitmap())).isNotEmpty();
        assertThat(describe(cut.toDvSnapshot())).isEqualTo(describe(reference));
        DvPinnedCut referenceCut = DvPinnedCut.freeze(100, reference);

        AtomicBoolean stop = new AtomicBoolean();
        AtomicInteger reads = new AtomicInteger();
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        ExecutorService workers = Executors.newFixedThreadPool(4);
        List<CompletableFuture<Void>> running = new ArrayList<>();
        for (int w = 0; w < 4; w++) {
            Random parts = new Random(w);
            running.add(
                    CompletableFuture.runAsync(
                            () -> {
                                try {
                                    while (!stop.get()) {
                                        readParts(parts, hw, referenceCut);
                                        reads.incrementAndGet();
                                    }
                                } catch (Throwable t) {
                                    failures.add(t);
                                }
                            },
                            workers));
        }
        try {
            // the bucket moves on: writes, a spill, two advances that patch the masks
            write(150);
            highWatermark.set(logEnd);
            assertThat(manager.spillTail()).isTrue();
            tier(manager, 200, 150);
            write(100);
            highWatermark.set(logEnd);
            DvPinnedCut later = pin(manager, 200);
            assertThat(later.logEndOffset()).isEqualTo(logEnd);
            tier(manager, 300, 300);
            manager.spillTail();
            write(50);
            highWatermark.set(logEnd);
            waitUntil(() -> reads.get() > 200 || !failures.isEmpty());
        } finally {
            stop.set(true);
            for (CompletableFuture<Void> r : running) {
                r.get(60, TimeUnit.SECONDS);
            }
            workers.shutdownNow();
        }
        assertThat(failures).isEmpty();
        assertThat(manager.getReadableSnapshotId()).isEqualTo(300L);
        assertThat(manager.tailSpilled()).isTrue();
        // the bucket's own masks moved on
        assertThat(describe(unionRead(manager, 300))).isNotEqualTo(describe(reference));
        // and the cut still serves the masks of the high watermark it was pinned at
        assertThat(describe(manager.pinnedCut(100, hw).toDvSnapshot()))
                .isEqualTo(describe(reference));
    }

    /** One worker's parts: the masks of some lake files and of the log from some offset. */
    private void readParts(Random parts, long hw, DvPinnedCut reference) throws Exception {
        DvPinnedCut cut = manager.pinnedCut(100, hw);
        assertThat(cut.logEndOffset()).isEqualTo(hw);
        assertThat(cut.snapshotStartOffset()).isEqualTo(10L);
        List<String> files =
                parts.nextBoolean()
                        ? Arrays.asList("f1.parquet")
                        : Arrays.asList("f1.parquet", "f2.parquet");
        assertThat(toSets(cut.lakeMasksOf(files))).isEqualTo(toSets(reference.lakeMasksOf(files)));
        long from = 10 + parts.nextInt((int) (hw - 10) + 1);
        int maxBytes = parts.nextBoolean() ? 1 : 1 << 20;
        DvPinnedCut.LogPage page = cut.logPage(from, maxBytes);
        DvPinnedCut.LogPage expected = reference.logPage(from, maxBytes);
        assertThat(page.to()).isEqualTo(expected.to());
        assertThat(offsets(page.bitmap())).isEqualTo(offsets(expected.bitmap()));
    }

    @Test
    void testIdenticalPinsShareTheCut() throws Exception {
        recoverAtSnapshot100(manager);
        write(50);
        highWatermark.set(logEnd);

        DvPinnedCut first = pin(manager, 100);
        DvPinnedCut second = pin(manager, 100);
        assertThat(second).isSameAs(first);
        assertThat(budget.pinnedCuts().size()).isEqualTo(1);
        assertThat(metrics.pinnedCuts()).isEqualTo(1);
        assertThat(metrics.pinnedCutBytes()).isEqualTo(first.bytes());
        assertThat(budget.totalBytes()).isGreaterThanOrEqualTo(first.bytes());

        // a pin at another high watermark is another cut; both are served
        write(10);
        highWatermark.set(logEnd);
        DvPinnedCut third = pin(manager, 100);
        assertThat(third.logEndOffset()).isEqualTo(logEnd).isNotEqualTo(first.logEndOffset());
        assertThat(manager.pinnedCut(100, first.logEndOffset())).isSameAs(first);
        assertThat(manager.pinnedCut(100, third.logEndOffset())).isSameAs(third);
        assertThat(budget.pinnedCuts().size()).isEqualTo(2);
        assertThat(metrics.pinnedCuts()).isEqualTo(2);
    }

    @Test
    void testCutsExpireOnceNoRequestAsksForThem() throws Exception {
        recoverAtSnapshot100(manager);
        write(50);
        highWatermark.set(logEnd);
        DvPinnedCut cut = pin(manager, 100);
        long hw = cut.logEndOffset();
        write(10);
        highWatermark.set(logEnd);
        DvPinnedCut idle = pin(manager, 100);
        write(10);
        highWatermark.set(logEnd);

        // each request keeps the cut for another idle timeout
        long almost = IDLE.toNanos() - 1;
        for (int i = 0; i < 3; i++) {
            clock.addAndGet(almost);
            assertThat(manager.pinnedCut(100, hw)).isSameAs(cut);
        }
        // the other cut was not asked for: the sweep drops it
        budget.pinnedCuts().expireIdle();
        assertThat(budget.pinnedCuts().size()).isEqualTo(1);
        assertThat(metrics.pinnedCutExpirations()).isEqualTo(1);

        clock.addAndGet(IDLE.toNanos());
        assertThatThrownBy(() -> manager.pinnedCut(100, hw))
                .isInstanceOfSatisfying(
                        StaleSnapshotException.class,
                        e -> {
                            assertThat(e.getCurrentSnapshotId()).isEqualTo(100L);
                            assertThat(e.getRequestedSnapshotId()).isEqualTo(100L);
                        });
        assertThatThrownBy(() -> manager.pinnedCut(100, idle.logEndOffset()))
                .isInstanceOf(StaleSnapshotException.class);
        assertThat(budget.pinnedCuts().size()).isZero();
        assertThat(budget.pinnedCuts().bytes()).isZero();
        assertThat(metrics.pinnedCutExpirations()).isEqualTo(2);
        assertThat(metrics.pinnedCuts()).isZero();
        assertThat(metrics.pinnedCutBytes()).isZero();
    }

    @Test
    void testCutAtTheHighWatermarkIsBuiltAgain() throws Exception {
        recoverAtSnapshot100(manager);
        write(50);
        highWatermark.set(logEnd);
        DvPinnedCut cut = pin(manager, 100);
        String masks = describe(cut.toDvSnapshot());
        clock.addAndGet(IDLE.toNanos());
        budget.pinnedCuts().expireIdle();
        assertThat(budget.pinnedCuts().size()).isZero();

        // the same masks: the bucket is still at the cut
        DvPinnedCut again = manager.pinnedCut(100, cut.logEndOffset());
        assertThat(again).isNotSameAs(cut);
        assertThat(describe(again.toDvSnapshot())).isEqualTo(masks);

        // not once the high watermark moved past it
        clock.addAndGet(IDLE.toNanos());
        write(1);
        highWatermark.set(logEnd);
        assertThatThrownBy(() -> manager.pinnedCut(100, cut.logEndOffset()))
                .isInstanceOf(StaleSnapshotException.class);
        // nor for a snapshot the bucket does not cover
        assertThatThrownBy(() -> manager.pinnedCut(99, logEnd))
                .isInstanceOfSatisfying(
                        StaleSnapshotException.class,
                        e -> {
                            assertThat(e.getCurrentSnapshotId()).isEqualTo(100L);
                            assertThat(e.getRequestedSnapshotId()).isEqualTo(99L);
                        });
    }

    @Test
    void testLeastRecentlyUsedCutsAreDroppedBeyondTheirShareOfTheBudget() throws Exception {
        // the size of one bucket's cut, from a bucket of its own
        DerivedDvManager probe = newManager("probe", budget, DerivedDvMetrics.noop());
        recoverAtSnapshot100(probe);
        write(50);
        highWatermark.set(logEnd);
        long cutBytes = pin(probe, 100).bytes();

        // room for two cuts of three buckets
        DerivedDvMetrics serverMetrics = DerivedDvMetrics.noop();
        DerivedDvTailBudget small =
                new DerivedDvTailBudget((long) (5 * cutBytes / 2 / 0.5), IDLE, clock::get);
        List<DerivedDvManager> buckets = new ArrayList<>();
        List<DvPinnedCut> cuts = new ArrayList<>();
        for (int b = 0; b < 3; b++) {
            DerivedDvManager m = newManager("bucket-" + b, small, serverMetrics);
            recover(m);
            buckets.add(m);
        }
        cuts.add(pin(buckets.get(0), 100));
        cuts.add(pin(buckets.get(1), 100));
        assertThat(cuts.get(0).bytes()).isEqualTo(cutBytes);
        // bucket 0's cut is used again, so bucket 1's is the least recently used
        assertThat(buckets.get(0).pinnedCut(100, cuts.get(0).logEndOffset())).isSameAs(cuts.get(0));
        cuts.add(pin(buckets.get(2), 100));
        assertThat(small.pinnedCuts().size()).isEqualTo(2);
        assertThat(small.pinnedCuts().bytes()).isLessThanOrEqualTo(small.pinnedCuts().limitBytes());
        assertThat(small.totalBytes()).isGreaterThanOrEqualTo(small.pinnedCuts().bytes());
        assertThat(serverMetrics.pinnedCutEvictions()).isEqualTo(1);
        assertThat(serverMetrics.pinnedCuts()).isEqualTo(2);

        write(1);
        highWatermark.set(logEnd);
        assertThatThrownBy(() -> buckets.get(1).pinnedCut(100, cuts.get(1).logEndOffset()))
                .isInstanceOf(StaleSnapshotException.class);
        assertThat(buckets.get(0).pinnedCut(100, cuts.get(0).logEndOffset())).isSameAs(cuts.get(0));
        assertThat(buckets.get(2).pinnedCut(100, cuts.get(2).logEndOffset())).isSameAs(cuts.get(2));

        // a cut larger than the share is kept alone, so its read can go on
        DerivedDvTailBudget tiny = new DerivedDvTailBudget(1, IDLE, clock::get);
        DerivedDvManager first = newManager("tiny-0", tiny, DerivedDvMetrics.noop());
        DerivedDvManager second = newManager("tiny-1", tiny, DerivedDvMetrics.noop());
        recover(first);
        recover(second);
        pin(first, 100);
        DvPinnedCut kept = pin(second, 100);
        assertThat(tiny.pinnedCuts().size()).isEqualTo(1);
        assertThat(second.pinnedCut(100, kept.logEndOffset())).isSameAs(kept);
    }

    @Test
    void testClosingTheBucketDropsItsCuts() throws Exception {
        DerivedDvManager other = newManager("other", budget, metrics);
        recoverAtSnapshot100(manager);
        recover(other);
        write(20);
        highWatermark.set(logEnd);
        DvPinnedCut cut = pin(manager, 100);
        DvPinnedCut kept = pin(other, 100);
        assertThat(budget.pinnedCuts().size()).isEqualTo(2);

        manager.close();
        assertThat(budget.pinnedCuts().size()).isEqualTo(1);
        assertThat(budget.pinnedCuts().bytes()).isEqualTo(kept.bytes());
        assertThat(metrics.pinnedCuts()).isEqualTo(1);
        assertThatThrownBy(() -> manager.pinnedCut(100, cut.logEndOffset()))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> manager.pinCut(100)).isInstanceOf(IOException.class);
        assertThat(other.pinnedCut(100, kept.logEndOffset())).isSameAs(kept);

        // a bucket moving to another budget leaves its cuts behind
        other.useTailBudget(new DerivedDvTailBudget(Long.MAX_VALUE));
        assertThat(budget.pinnedCuts().size()).isZero();
        assertThat(budget.pinnedCuts().bytes()).isZero();
        assertThat(metrics.pinnedCuts()).isZero();
        assertThat(metrics.pinnedCutBytes()).isZero();
    }

    // ------------------------------------------------------------------------------------------

    /** Snapshot 100 tiers [0, 10): k0..k19 are rows of F1, k20..k39 of F2. */
    private void recoverAtSnapshot100(DerivedDvManager m) throws Exception {
        if (src.readable == null) {
            Index initial = new Index();
            for (int i = 0; i < 40; i++) {
                initial.put("k" + i, i < 20 ? 1 : 2, i % 20);
            }
            Map<Integer, String> files = new HashMap<>();
            files.put(1, F1);
            files.put(2, F2);
            src.publish(100, 1, 10, files, initial.build());
            src.readable = new long[] {100, 10};
            logEnd = 10;
            highWatermark.set(10);
        }
        recover(m);
    }

    private void recover(DerivedDvManager m) throws Exception {
        m.recover(src);
        live.add(m);
    }

    /**
     * Writes {@code n} changes of random keys, some of them deletes, and hands them to the buckets
     * as the write path does.
     */
    private void write(int n) {
        for (int i = 0; i < n; i++) {
            int k = random.nextInt(60);
            long offset = logEnd++;
            long version = random.nextInt(6) == 0 ? -1 : offset;
            src.append("k" + k, offset, version);
            changes.add(new long[] {offset, k, version});
            // names the version it supersedes, as the KV value's tag does
            List<DvEntry> entry = Collections.singletonList(src.written("k" + k, offset, version));
            for (DerivedDvManager m : live) {
                if (!m.isClosed()) {
                    m.handleChangelogSynced(entry);
                }
            }
        }
    }

    /** Registers snapshot {@code snapshotId} tiering up to {@code upTo} and advances to it. */
    private void tier(DerivedDvManager m, long snapshotId, long upTo) throws Exception {
        long from = src.readable[1];
        Map<Long, long[]> last = new TreeMap<>();
        for (long[] c : changes) {
            if (c[0] >= from && c[0] < upTo) {
                last.put(c[1], c);
            }
        }
        Index change = new Index();
        int position = 0;
        for (long[] c : last.values()) {
            if (c[2] < 0) {
                change.delete("k" + c[1]);
            } else {
                change.put("k" + c[1], nextFile, position++);
            }
        }
        src.publish(
                snapshotId,
                nextFile,
                upTo,
                Collections.singletonMap(nextFile, "s3://lake/t/data/f" + nextFile + ".parquet"),
                change.build());
        src.readable = new long[] {snapshotId, upTo};
        nextFile++;
        assertThat(m.ensureCovered(snapshotId)).isTrue();
    }

    /** The masks of a bucket that recovers at {@code snapshotId} and stays at {@code hw}. */
    private DvSnapshot referenceMasks(long snapshotId, long hw) throws Exception {
        AtomicLong fixed = new AtomicLong(hw);
        DerivedDvManager reference =
                new DerivedDvManager(
                        DvRocksDB.open(tempDir.resolve("reference").toString()),
                        fixed::get,
                        DerivedDvManager.DEFAULT_CHECKPOINT_INTERVAL,
                        DerivedDvMetrics.noop(),
                        new DerivedDvTailBudget(Long.MAX_VALUE));
        try {
            reference.setMaskCacheMillis(0);
            reference.recover(src);
            return unionRead(reference, snapshotId);
        } finally {
            reference.close();
        }
    }

    private DerivedDvManager newManager(
            String dir, DerivedDvTailBudget budget, DerivedDvMetrics metrics) throws IOException {
        DerivedDvManager m =
                new DerivedDvManager(
                        DvRocksDB.open(tempDir.resolve(dir).toString()),
                        highWatermark::get,
                        DerivedDvManager.DEFAULT_CHECKPOINT_INTERVAL,
                        metrics,
                        budget);
        m.setMaskCacheMillis(0);
        m.setCheckpointJitterMillis(0);
        managers.add(m);
        return m;
    }

    /** Pins a cut, retried while the bucket is not ready, as clients retry. */
    private static DvPinnedCut pin(DerivedDvManager m, long snapshotId) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        while (true) {
            try {
                return m.pinCut(snapshotId);
            } catch (RetriableException e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(10);
            }
        }
    }

    private static DvSnapshot unionRead(DerivedDvManager m, long snapshotId) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        while (true) {
            try {
                return m.getDvForUnionRead(snapshotId);
            } catch (RetriableException e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(10);
            }
        }
    }

    private static String describe(DvSnapshot s) throws IOException {
        Map<String, Object> d = new TreeMap<>();
        d.put("lake", new TreeMap<>(lakeMasks(s)));
        d.put("log", offsets(s.getLogDvBitmap()));
        d.put("end", s.getLogEndOffset());
        d.put("start", s.getSnapshotStartOffset());
        return d.toString();
    }

    private static Map<String, Object> toSets(Map<String, byte[]> masks) throws IOException {
        Map<String, Object> out = new TreeMap<>();
        for (Map.Entry<String, byte[]> e : masks.entrySet()) {
            out.put(e.getKey(), offsets(e.getValue()));
        }
        return out;
    }

    private static void waitUntil(java.util.function.BooleanSupplier condition)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 60_000;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }
}
