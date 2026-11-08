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
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.server.kv.dv.DerivedDvManagerTest.Index;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;

import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.dict;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.key;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.lakeMasks;
import static org.apache.fluss.server.kv.dv.DvTestManagers.hooks;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests of the owner's state table ({@link DvOwner}): a bucket in each state, what it serves there,
 * and how failures and a close move it on. The owner's steps run on the test thread, a step at a
 * time, so each state can be looked at while a load is held in it.
 */
class DvOwnerTest extends DerivedDvManagerTestBase {

    private final ManualExecutor steps = new ManualExecutor();
    private DvRocksDB ownedDb;
    private DerivedDvManager m;
    // the bucket's own budget, so a test sees what it counts
    private final DerivedDvHeapBudget budget =
            new DerivedDvHeapBudget(Long.MAX_VALUE, DvTestManagers.executors());

    @BeforeEach
    void setUpOwned() throws Exception {
        ownedDb = DvRocksDB.open(tempDir.resolve("owned").toString());
        m =
                new DerivedDvManager(
                        ownedDb,
                        highWatermark::get,
                        DvTestManagers.CHECKPOINT_INTERVAL,
                        metrics,
                        budget,
                        DvTestManagers.executors(),
                        steps,
                        new TestingDvHooks());
        m.setMaskCacheMillis(0);
        m.setCheckpointJitterMillis(0);
    }

    @AfterEach
    void tearDownOwned() throws Exception {
        if (!m.isClosed()) {
            CompletableFuture<Void> closing = async(() -> m.close());
            pump(closing);
        }
        ownedDb.close();
    }

    /** A new leader that has not taken over serves nothing: reads and lookups are not ready. */
    @Test
    void testUnrecoveredServesNothing() throws Exception {
        assertThat(m.state().kind).isEqualTo(DvState.Kind.UNRECOVERED);
        assertNotReady(read(-1, true));
        assertNotReady(read(-1, false));
        assertNotReady(async(() -> m.lookup(-1, Collections.singletonList(key("k1")))));
        assertThat(m.getReadableSnapshotId()).isEqualTo(-1L);
    }

    /**
     * A new leader takes over, rebuilds to the readable snapshot, and covers it. While it rebuilds,
     * a read that does not wait reads by key, and nothing else is served.
     */
    @Test
    void testRecoveryRebuildsAndThenCovers() throws Exception {
        publish100();
        CountDownLatch gate = pauseReplayAfter(0);
        CompletableFuture<Void> recovered = async(() -> m.recover(src));
        pumpUntil(() -> src.replayPaused.getCount() == 0);
        assertThat(m.state().kind).isEqualTo(DvState.Kind.REBUILDING);
        assertThatThrownBy(() -> pump(read(100, false)))
                .isInstanceOf(DeletionVectorsNotReadyException.class)
                .satisfies(
                        e ->
                                assertThat(((DeletionVectorsNotReadyException) e).isRebuilding())
                                        .isTrue());

        gate.countDown();
        pump(recovered);
        assertThat(m.state().kind).isEqualTo(DvState.Kind.COVERING);
        assertThat(m.state().version.snapshotId).isEqualTo(100L);
        assertThat(lakeMasks(pump(read(100, true)))).containsKey(F1);
    }

    /**
     * A switch serves the version it leaves until its swap: lookups and masks of the covered
     * snapshot answer while the next snapshot's changes are ingested.
     */
    @Test
    void testSwitchServesTheVersionItLeavesUntilTheSwap() throws Exception {
        coverSnapshot100();
        src.publish(101, 2, 12, dict(2, F2), new Index().put("k1", 2, 0).build());
        src.readable = new long[] {101, 12};
        CountDownLatch ingesting = new CountDownLatch(1);
        CountDownLatch ingest = new CountDownLatch(1);
        hooks(m).setAfterIngest(
                        () -> {
                            ingesting.countDown();
                            waitFor(ingest).run();
                        });
        CompletableFuture<Boolean> covered = async(() -> m.ensureCovered(101));
        pumpUntil(() -> ingesting.getCount() == 0);

        assertThat(m.state().kind).isEqualTo(DvState.Kind.SWITCHING);
        assertThat(m.state().version.snapshotId).isEqualTo(100L);
        // a lookup of the covered version needs no step of the owner
        assertThat(asStrings(m.lookup(100, Collections.singletonList(key("k1")))))
                .containsExactly("k1@" + F1 + "#0");
        assertThat(lakeMasks(pump(read(100, true)))).containsKey(F1);

        hooks(m).setAfterIngest(null);
        ingest.countDown();
        assertThat(pump(covered)).isTrue();
        assertThat(m.state().kind).isEqualTo(DvState.Kind.COVERING);
        assertThat(m.state().version.snapshotId).isEqualTo(101L);
        assertThat(asStrings(m.lookup(101, Collections.singletonList(key("k1")))))
                .containsExactly("k1@" + F2 + "#0");
    }

    /**
     * A bucket whose change list is missing replays it: its lookups are served through its version
     * meanwhile, and its masks once the replay is installed.
     */
    @Test
    void testReplayServesLookupsThroughTheVersion() throws Exception {
        coverSnapshot100();
        m.handOffFailed(new IllegalStateException("hand-off failed"));
        CountDownLatch gate = pauseReplayAfter(0);
        CompletableFuture<?> waiting = read(100, true);
        pumpUntil(() -> src.replayPaused.getCount() == 0);
        assertThat(m.state().kind).isEqualTo(DvState.Kind.REPLAYING);
        assertThat(m.state().changeListPresent).isFalse();
        assertThat(asStrings(m.lookup(100, Collections.singletonList(key("k2")))))
                .containsExactly("k2@" + F1 + "#1");
        gate.countDown();
        pumpUntil(waiting::isDone);
        assertThat(lakeMasks(pump(read(100, true)))).containsKey(F1);
        assertThat(m.state().kind).isEqualTo(DvState.Kind.COVERING);
    }

    /** One load runs at a time: a second target waits for the first, and both are reached. */
    @Test
    void testTargetsWaitForTheLoadThatRuns() throws Exception {
        coverSnapshot100();
        src.publish(101, 2, 12, dict(2, F2), new Index().put("k1", 2, 0).build());
        src.publish(102, 3, 13, dict(3, F3), new Index().put("k2", 3, 0).build());
        src.readable = new long[] {102, 13};
        CountDownLatch ingesting = new CountDownLatch(1);
        CountDownLatch ingest = new CountDownLatch(1);
        hooks(m).setAfterIngest(
                        () -> {
                            if (ingesting.getCount() > 0) {
                                ingesting.countDown();
                                waitFor(ingest).run();
                            }
                        });
        CompletableFuture<Boolean> to101 = async(() -> m.ensureCovered(101));
        pumpUntil(() -> ingesting.getCount() == 0);
        CompletableFuture<Boolean> to102 = async(() -> m.ensureCovered(102));
        steps.runAll();
        assertThat(to102).isNotDone();
        assertThat(m.state().kind).isEqualTo(DvState.Kind.SWITCHING);

        ingest.countDown();
        assertThat(pump(to101)).isTrue();
        assertThat(pump(to102)).isTrue();
        assertThat(m.getReadableSnapshotId()).isEqualTo(102L);
    }

    /**
     * A step that fails unexpectedly does not fail the server: it is logged and counted, the change
     * list is dropped, and the bucket replays it before it serves masks again.
     */
    @Test
    void testFailedStepReplaysTheChangeList() throws Exception {
        coverSnapshot100();
        DvSnapshot before = pump(read(100, true));
        hooks(m).setFaultInNextDrain(
                        () -> {
                            throw new IllegalStateException("injected");
                        });
        src.append("k3", 14, 14);
        DvTestManagers.handOver(manager(), Collections.singletonList(src.written("k3", 14, 14)));
        steps.runAll();
        assertThat(metrics.ownerFaults()).isEqualTo(1L);
        assertThat(m.state().changeListPresent).isFalse();
        highWatermark.set(15);
        DvSnapshot after = pump(read(100, true));
        assertThat(lakeMasks(after).get(F1)).containsAll(lakeMasks(before).get(F1)).contains(2L);
    }

    /**
     * A read the published state answers never hops to the owner: with the owner paused, a read
     * that waits gets the latest frontier, whose cut reaches its floor, and so does a read that
     * does not wait, and a lookup reads the published version.
     */
    @Test
    void testReadyReadsNeverHopToTheOwner() throws Exception {
        coverSnapshot100();
        m.setMaskCacheMillis(60_000);
        DvSnapshot built = pump(read(100, true));
        assertThat(m.state().latest).isNotNull();

        // no step of the owner runs from here
        DvSnapshot waited = read(100, true).get(10, TimeUnit.SECONDS);
        assertThat(waited.getLogEndOffset()).isEqualTo(built.getLogEndOffset());
        assertThat(lakeMasks(waited)).isEqualTo(lakeMasks(built));
        DvSnapshot notWaited = read(100, false).get(10, TimeUnit.SECONDS);
        assertThat(notWaited.getLogEndOffset()).isEqualTo(built.getLogEndOffset());
        assertThat(lakeMasks(notWaited)).isEqualTo(lakeMasks(built));
        assertThat(asStrings(m.lookup(100, Collections.singletonList(key("k3")))))
                .containsExactly("k3@" + F1 + "#2");

        // a read whose floor is above the latest frontier needs new masks: it hops
        src.append("k3", 12, 12);
        DvTestManagers.handOver(manager(), Collections.singletonList(src.written("k3", 12, 12)));
        highWatermark.set(13);
        CompletableFuture<DvSnapshot> newer = read(100, true);
        Thread.sleep(50);
        assertThat(newer).isNotDone();
        assertThat(pump(newer).getLogEndOffset()).isEqualTo(13L);
    }

    /**
     * Past 100,000 changes the owner has not taken, the writer waits for it: after a second the
     * stall is counted (and logged at WARN), and the write returns normally once the owner drains,
     * never failing.
     */
    @Test
    void testWriterWaitsAtTheBackstopAndNeverFails() throws Exception {
        coverSnapshot100();
        List<DvEntry> batch = new ArrayList<>();
        for (int i = 0; i <= 100_000; i++) {
            batch.add(new DvEntry(key("w" + i), 20 + i, 20 + i, true, -1));
        }
        // the owner is paused: the batch stays in the hand-off
        CompletableFuture<Void> write =
                async(() -> m.handleChangelogSynced(batch, 20L + batch.size()));
        long deadline = System.currentTimeMillis() + 30_000;
        while (metrics.handOffStalls() == 0) {
            assertThat(System.currentTimeMillis()).isLessThan(deadline);
            Thread.sleep(50);
        }
        assertThat(write).isNotDone();
        assertThat(metrics.handOffStalls()).isEqualTo(1L);

        pump(write);
        assertThat(write).isCompletedWithValue(null);
        assertThat(metrics.handOffStalls()).isEqualTo(1L);
        assertThat(metrics.ownerFaults()).isZero();
        assertThat(m.state().changeListPresent).isTrue();
    }

    /**
     * A fault inside a load's swap, an error no step expects, does not fail the server: it is
     * counted, the load releases what it held, the covered version keeps serving, and the change
     * list is replayed before masks are served again. The switch succeeds when tried again.
     */
    @Test
    void testFaultInsideALoadsSwap() throws Exception {
        coverSnapshot100();
        int leasesCovering = m.leasesHeld();
        src.publish(101, 2, 12, dict(2, F2), new Index().put("k1", 2, 0).build());
        src.replaced.put(2L, Collections.singletonList(F1));
        src.readable = new long[] {101, 12};
        hooks(m).setBeforeFileDrop(
                        name -> {
                            throw new AssertionError("injected fault");
                        });
        assertThatThrownBy(() -> pump(async(() -> m.ensureCovered(101))))
                .hasMessageContaining("injected fault");
        assertThat(metrics.ownerFaults()).isEqualTo(1L);
        assertThat(m.state().kind).isEqualTo(DvState.Kind.COVERING);
        assertThat(m.state().version.snapshotId).isEqualTo(100L);
        assertThat(m.state().changeListPresent).isFalse();
        assertThat(m.leasesHeld()).isEqualTo(leasesCovering);
        assertThat(asStrings(m.lookup(100, Collections.singletonList(key("k1")))))
                .containsExactly("k1@" + F1 + "#0");

        hooks(m).setBeforeFileDrop(null);
        long deadline = System.currentTimeMillis() + 30_000;
        while (true) {
            try {
                if (pump(async(() -> m.ensureCovered(101)))) {
                    break;
                }
            } catch (DeletionVectorsNotReadyException e) {
                // the failed switch is tried again after a backoff
            }
            assertThat(System.currentTimeMillis()).isLessThan(deadline);
            Thread.sleep(50);
        }
        assertThat(m.state().version.snapshotId).isEqualTo(101L);
        // the change list is replayed before the masks are served
        assertThat(pump(read(101, true)).getLogEndOffset()).isEqualTo(12L);
        assertThat(m.state().changeListPresent).isTrue();
    }

    /**
     * The owner is paused and the writes fill the hand-off: a write waits at most the bound, then
     * returns, and later writes return at once. The change list is {@code Missing}: a read that
     * does not wait reads by key, and one that waits is not ready. Once the owner runs again, a
     * replay after the backoff restores the masks, which equal a fold of the log.
     */
    @Test
    void testHandOffOverflowDropsTheChangeListUntilAReplay() throws Exception {
        coverSnapshot100();
        m.setHandOffBounds(10, 200, 300, 2_400, 60_000);
        List<long[]> log = new ArrayList<>();
        log.add(new long[] {10, 1, 10});
        log.add(new long[] {11, 2, 11});
        // k1@12 updates a lake row; inserts of new keys follow
        src.append("k1", 12, 12);
        List<DvEntry> batch = new ArrayList<>();
        batch.add(src.written("k1", 12, 12));
        log.add(new long[] {12, 1, 12});
        for (int i = 0; i < 10; i++) {
            src.append("w" + i, 13 + i, 13 + i);
            batch.add(new DvEntry(key("w" + i), 13 + i, 13 + i, true, -1));
            log.add(new long[] {13 + i, 100 + i, 13 + i});
        }
        highWatermark.set(23);

        // no step of the owner runs: the write waits the bound, then returns
        long start = System.currentTimeMillis();
        CompletableFuture<Void> write = async(() -> m.handleChangelogSynced(batch, 23));
        write.get(10, TimeUnit.SECONDS);
        assertThat(System.currentTimeMillis() - start).isLessThan(2_000);
        // a later write returns at once: the hand-off drops it
        src.append("k2", 23, 23);
        log.add(new long[] {23, 2, 23});
        start = System.currentTimeMillis();
        m.handleChangelogSynced(Collections.singletonList(src.written("k2", 23, 23)), 24);
        assertThat(System.currentTimeMillis() - start).isLessThan(100);
        highWatermark.set(24);
        assertThat(metrics.handOffsDropped()).isEqualTo(1L);

        // the owner releases the queued batch and drops the change list
        steps.runAll();
        assertThat(metrics.handOffsDropped()).isEqualTo(2L);
        assertThat(m.state().changeListPresent).isFalse();
        assertThatThrownBy(() -> read(100, false).get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(DeletionVectorsNotReadyException.class)
                .satisfies(
                        e ->
                                assertThat(
                                                ((DeletionVectorsNotReadyException) e.getCause())
                                                        .isRebuilding())
                                        .isTrue());
        assertNotReady(read(100, true));

        // the replay after the backoff restores the masks
        pumpUntil(() -> m.state().changeListPresent);
        DvSnapshot masks = pump(read(100, true));
        Map<Integer, int[]> lake = new HashMap<>();
        lake.put(1, new int[] {1, 0});
        lake.put(2, new int[] {1, 1});
        lake.put(3, new int[] {1, 2});
        assertThat(DerivedDvChangeListTest.describe(masks))
                .isEqualTo(DerivedDvChangeListTest.model(log, lake, 10, 24));
        assertThat(pump(async(() -> m.overflowRetries()))[1]).isEqualTo(300L);
    }

    /**
     * After an overflow, the replay waits while the owner is behind: the backoff doubles instead of
     * replaying into the next overflow. Once the owner keeps up, the replay runs and the backoff
     * starts over.
     */
    @Test
    void testOverflowReplayBacksOffWhileTheOwnerIsBehind() throws Exception {
        coverSnapshot100();
        m.setHandOffBounds(10, 100, 50, 400, 100);
        List<DvEntry> batch = new ArrayList<>();
        for (int i = 0; i < 11; i++) {
            src.append("w" + i, 12 + i, 12 + i);
            batch.add(new DvEntry(key("w" + i), 12 + i, 12 + i, true, -1));
        }
        highWatermark.set(23);
        async(() -> m.handleChangelogSynced(batch, 23)).get(10, TimeUnit.SECONDS);
        steps.runAll();
        assertThat(m.state().changeListPresent).isFalse();
        int replays = src.replayedFrom.size();

        // the owner's steps wait longer than it takes to be behind: the replay is put off
        Thread.sleep(400);
        steps.runAll();
        long[] retries = pump(async(() -> m.overflowRetries()));
        assertThat(retries[0]).isEqualTo(1L);
        assertThat(retries[1]).isEqualTo(200L);
        assertThat(src.replayedFrom).hasSize(replays);
        assertThat(m.state().changeListPresent).isFalse();

        // the owner keeps up: the replay runs, and the backoff starts over
        pumpUntil(() -> m.state().changeListPresent);
        assertThat(src.replayedFrom.size()).isGreaterThan(replays);
        assertThat(pump(async(() -> m.overflowRetries()))[1]).isEqualTo(50L);
        assertThat(pump(read(100, true)).getLogEndOffset()).isEqualTo(23L);
    }

    /**
     * A waiting read that came before the takeover has a floor below the cut the takeover serves
     * (the old leader's log end). While the high watermark is below that cut, the read is not
     * ready, with no fault of the owner; once the high watermark reaches it, the read is served
     * there.
     */
    @Test
    void testWaitingReadFromBeforeTheTakeoverWaitsForTheServedCut() throws Exception {
        publish100();
        src.append("k3", 12, 12);
        src.append("k1", 13, 13);
        // the new leader's high watermark lags its log end, as right after an election
        highWatermark.set(12);
        CompletableFuture<Void> recovered = async(() -> m.recover(src));
        // the read takes its floor before the takeover step runs: no step runs until pumped
        Thread.sleep(100);
        CompletableFuture<DvSnapshot> read = read(100, true);
        Thread.sleep(100);
        assertNotReady(read);
        pump(recovered);
        assertThat(metrics.ownerFaults()).isZero();
        assertThat(m.state().changeListPresent).isTrue();
        assertNotReady(read(100, true));

        highWatermark.set(14);
        assertThat(pump(read(100, true)).getLogEndOffset()).isEqualTo(14L);
        assertThat(metrics.ownerFaults()).isZero();
    }

    /**
     * A read that does not wait was told to read by key up to the raw high watermark, above the
     * changes handed over so far. A waiting read that came before is not served below that cut: it
     * is not ready until the changes are handed over, with no fault of the owner.
     */
    @Test
    void testWaitingReadAfterAFoldCutAboveTheHandOff() throws Exception {
        coverSnapshot100();
        src.append("k3", 12, 12);
        DvTestManagers.handOver(manager(), Collections.singletonList(src.written("k3", 12, 12)));
        highWatermark.set(13);
        CompletableFuture<DvSnapshot> read = read(100, true);
        Thread.sleep(100);
        // [13, 20) is appended and replicated, not handed over yet; a fold up to 20 is handed out
        highWatermark.set(20);
        m.foldCutServed(20);
        assertNotReady(read);
        assertThat(metrics.ownerFaults()).isZero();
        assertThat(m.state().changeListPresent).isTrue();

        for (long offset = 13; offset < 20; offset++) {
            src.append("w" + offset, offset, offset);
            DvTestManagers.handOver(
                    manager(),
                    Collections.singletonList(
                            new DvEntry(key("w" + offset), offset, offset, true, -1)));
        }
        assertThat(pump(read(100, true)).getLogEndOffset()).isEqualTo(20L);
        assertThat(metrics.ownerFaults()).isZero();
    }

    /**
     * The write path fails to hand a batch over after appending it, and the write is acknowledged.
     * Until the owner has dropped the change list, a waiting read is not served the latest
     * frontier, which is below that write, on the request thread: it goes to the owner, which
     * replays the change list and serves the write.
     */
    @Test
    void testFailedHandOffKeepsReadsFromTheFrontierBelowIt() throws Exception {
        coverSnapshot100();
        m.setMaskCacheMillis(60_000);
        src.append("k3", 12, 12);
        DvTestManagers.handOver(manager(), Collections.singletonList(src.written("k3", 12, 12)));
        highWatermark.set(13);
        assertThat(pump(read(100, true)).getLogEndOffset()).isEqualTo(13L);

        // the owner does not run: the batch at 13 is appended and its hand-off fails
        src.append("k1", 13, 13);
        m.handOffFailed(new IllegalStateException("hand-off failed"));
        // the write is acknowledged
        highWatermark.set(14);
        CompletableFuture<DvSnapshot> read = read(100, true);
        Thread.sleep(100);
        assertThat(read).isNotDone();
        assertThat(pump(read).getLogEndOffset()).isEqualTo(14L);
    }

    /**
     * A targeted switch's derivation holds the hashes of the keys it derives again and their old
     * positions from its prepare on: the budget counts them until the derivation ends, and then
     * holds the bucket's own bytes only.
     */
    @Test
    void testTheBudgetCountsATargetedSwitchsDerivation() throws Exception {
        coverSnapshot100();
        long before = budget.totalBytes();
        src.publish(101, 2, 12, dict(2, F2), new Index().put("k1", 2, 0).put("k2", 2, 1).build());
        src.readable = new long[] {101, 12};
        CountDownLatch ingesting = new CountDownLatch(1);
        CountDownLatch ingest = new CountDownLatch(1);
        hooks(m).setAfterIngest(
                        () -> {
                            ingesting.countDown();
                            waitFor(ingest).run();
                        });
        CompletableFuture<Boolean> covered = async(() -> m.ensureCovered(101));
        pumpUntil(() -> ingesting.getCount() == 0);
        // prepared: at least its hash table, 16 longs, is counted
        assertThat(budget.totalBytes() - before).isGreaterThanOrEqualTo(16 * 8L);

        ingest.countDown();
        hooks(m).setAfterIngest(null);
        assertThat(pump(covered)).isTrue();
        pumpUntil(() -> m.state().lookupsDrained);
        long bucket = pump(async(() -> m.tailMemoryBytes()));
        assertThat(budget.totalBytes()).isEqualTo(bucket);
    }

    /**
     * A targeted switch's derivation is counted in the budget as it grows, not when it ends: by the
     * time a chunk of keys and their old positions is added, the hash array and the positions are
     * counted.
     */
    @Test
    void testTheBudgetCountsADerivationWhileItGrows() throws Exception {
        coverSnapshot100();
        long before = budget.totalBytes();
        src.publish(101, 2, 12, dict(2, F2), new Index().put("k1", 2, 0).put("k2", 2, 1).build());
        src.readable = new long[] {101, 12};
        AtomicLong counted = new AtomicLong(-1);
        hooks(m).setAfterDerivationChunk(
                        () -> counted.compareAndSet(-1, budget.totalBytes() - before));
        assertThat(pump(async(() -> m.ensureCovered(101)))).isTrue();
        // the hash array of 16 longs, and the old positions of k1 and k2
        assertThat(counted.get()).isGreaterThan(16 * 8L);
    }

    /**
     * The tablet server's budget drops the frontier kept across a switch on its own thread. Until
     * the owner publishes a state without it, reads that do not wait may still be served it, so log
     * retention keeps the log it reads.
     */
    @Test
    void testLogRetentionKeepsThePublishedKeptFrontierTheBudgetDropped() throws Exception {
        coverSnapshot100();
        // the rebuild's lookups are done, so a read that does not wait does not read by key
        pumpUntil(() -> m.state().lookupsDrained);
        // a read that does not wait comes, and builds a frontier of 100
        assertThat(pump(read(100, false)).getLogEndOffset()).isEqualTo(12L);
        src.append("k3", 12, 12);
        DvTestManagers.handOver(manager(), Collections.singletonList(src.written("k3", 12, 12)));
        highWatermark.set(13);
        // the switch looks up the lake keys of the change list again, and those lookups wait: 101
        // builds no frontier of its own, so 100's is kept
        m.setTargetedMoves(false);
        hooks(m).setResolvesPaused(true);
        src.publish(101, 2, 12, dict(2, F2), new Index().put("k1", 2, 0).build());
        src.readable = new long[] {101, 12};
        assertThat(pump(async(() -> m.ensureCovered(101)))).isTrue();
        assertThat(m.state().kept).isNotNull();
        assertThat(m.logRetainOffset()).isEqualTo(10L);

        // the budget frees the kept frontier; no step of the owner has run since
        assertThat(m.shedForBudget()).isTrue();
        assertThat(m.state().kept).isNotNull();
        assertThat(m.logRetainOffset()).isEqualTo(10L);
        steps.runAll();
        assertThat(m.state().kept).isNull();
        assertThat(m.logRetainOffset()).isEqualTo(12L);
        hooks(m).setResolvesPaused(false);
    }

    /**
     * A read served on its own thread is served no lower than the highest cut served: if a higher
     * cut is handed out after the read checked the latest frontier, the read goes to the owner,
     * which serves it no lower than that cut either.
     */
    @Test
    void testAReadyReadIsNotServedBelowACutServedMeanwhile() throws Exception {
        coverSnapshot100();
        m.setMaskCacheMillis(60_000);
        assertThat(pump(read(100, true)).getLogEndOffset()).isEqualTo(12L);
        hooks(m).setBeforeReadyServe(
                        () -> {
                            highWatermark.set(13);
                            m.foldCutServed(13);
                        });
        CompletableFuture<DvSnapshot> read = read(100, true);
        Thread.sleep(100);
        hooks(m).setBeforeReadyServe(null);
        assertThat(read).isNotDone();
        // the changes up to 13 are not handed over
        assertNotReady(read);
    }

    /**
     * Decided change: a read that waits is served no lower than a cut served before it came, also
     * on the owner, where it is evaluated after the cut was handed out.
     */
    @Test
    void testAWaitingReadIsNotServedBelowACutServedBeforeIt() throws Exception {
        coverSnapshot100();
        assertThat(pump(read(100, true)).getLogEndOffset()).isEqualTo(12L);
        // a read that does not wait was told to fold up to 13, not handed over yet
        highWatermark.set(13);
        m.foldCutServed(13);
        assertNotReady(read(100, true));
        src.append("k3", 12, 12);
        DvTestManagers.handOver(manager(), Collections.singletonList(src.written("k3", 12, 12)));
        assertThat(pump(read(100, true)).getLogEndOffset()).isEqualTo(13L);
    }

    /**
     * A step faults while a switch load runs. The load stops at its next unit and swaps nothing in;
     * the work ends with the fault only once the load released what it holds, so a request for a
     * newer snapshot starts its load after that, never next to it. No version holds another
     * snapshot's changes.
     */
    @Test
    void testAFaultStopsTheLoadInFlightBeforeTheNextStarts() throws Exception {
        coverSnapshot100();
        int idleLeases = m.leasesHeld();
        src.publish(101, 2, 12, dict(2, F2), new Index().put("k1", 2, 0).build());
        src.publish(102, 3, 12, dict(3, F3), new Index().put("k2", 3, 0).build());
        src.readable = new long[] {102, 12};
        AtomicInteger ingests = new AtomicInteger();
        CountDownLatch firstHeld = new CountDownLatch(1);
        CountDownLatch firstGo = new CountDownLatch(1);
        hooks(m).setAfterIngest(
                        () -> {
                            if (ingests.incrementAndGet() == 1) {
                                firstHeld.countDown();
                                waitFor(firstGo).run();
                            }
                        });
        CompletableFuture<Boolean> to101 = async(() -> m.ensureCovered(101));
        pumpUntil(() -> firstHeld.getCount() == 0);

        // an unexpected exception in an unrelated step while the switch ingests
        hooks(m).setFaultInNextDrain(
                        () -> {
                            throw new IllegalStateException("injected");
                        });
        src.append("k3", 12, 12);
        DvTestManagers.handOver(manager(), Collections.singletonList(src.written("k3", 12, 12)));
        pumpUntil(() -> metrics.ownerFaults() == 1);
        // a request for 102 waits for the stopped load: no second load ingests next to it
        CompletableFuture<Boolean> to102 = async(() -> m.ensureCovered(102));
        long deadline = System.currentTimeMillis() + 300;
        while (System.currentTimeMillis() < deadline) {
            steps.runAll();
            Thread.sleep(5);
        }
        assertThat(ingests.get()).isEqualTo(1);
        assertThat(to101).isNotDone();

        // the stopped load ends: it swaps nothing in, and the work ends with the fault
        firstGo.countDown();
        assertThatThrownBy(() -> pump(to101)).hasMessageContaining("injected");
        assertThat(pump(to102)).isTrue();
        assertThat(m.state().version.snapshotId).isEqualTo(102L);
        assertThat(asStrings(m.lookup(102, Collections.singletonList(key("k2")))))
                .containsExactly("k2@" + F3 + "#0");
        assertThat(asStrings(m.lookup(102, Collections.singletonList(key("k1")))))
                .containsExactly("k1@" + F2 + "#0");
        assertThat(m.leasesHeld()).isEqualTo(idleLeases);
    }

    /**
     * As above, with a targeted switch: the stopped switch releases the views its derivation took
     * (a lease and the version it leaves), so leases return to idle and the RocksDB closes.
     */
    @Test
    void testAFaultedTargetedSwitchReleasesItsViews() throws Exception {
        coverSnapshot100();
        int idleLeases = m.leasesHeld();
        src.publish(101, 2, 12, dict(2, F2), new Index().put("k1", 2, 0).build());
        src.readable = new long[] {101, 12};
        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger ingests = new AtomicInteger();
        hooks(m).setAfterIngest(
                        () -> {
                            if (ingests.incrementAndGet() == 1) {
                                held.countDown();
                                waitFor(go).run();
                            }
                        });
        CompletableFuture<Boolean> to101 = async(() -> m.ensureCovered(101));
        pumpUntil(() -> held.getCount() == 0);
        hooks(m).setFaultInNextDrain(
                        () -> {
                            throw new IllegalStateException("injected");
                        });
        src.append("k3", 12, 12);
        DvTestManagers.handOver(manager(), Collections.singletonList(src.written("k3", 12, 12)));
        pumpUntil(() -> metrics.ownerFaults() == 1);
        highWatermark.set(13);
        go.countDown();
        assertThatThrownBy(() -> pump(to101)).hasMessageContaining("injected");
        assertThat(m.state().version.snapshotId).isEqualTo(100L);
        assertThat(m.leasesHeld()).isEqualTo(idleLeases);
        // the change list is replayed, and masks of 100 are served
        assertThat(pump(read(100, true)).getLogEndOffset()).isEqualTo(13L);

        m.setCloseWaitMillis(100);
        pump(async(() -> m.close()));
        pumpUntil(() -> m.dbClosed().isDone());
        assertThat(m.leasesHeld()).isZero();
    }

    /**
     * An error, not an exception, on a switch's job thread ends its work with the error, logged and
     * counted as a fault; the bucket switches again once the error is gone.
     */
    @Test
    void testAnErrorInASwitchJobEndsItsWork() throws Exception {
        coverSnapshot100();
        src.publish(101, 2, 12, dict(2, F2), new Index().put("k1", 2, 0).build());
        src.readable = new long[] {101, 12};
        hooks(m).setAfterIngest(
                        () -> {
                            throw new AssertionError("injected error on the job thread");
                        });
        assertThatThrownBy(() -> pump(async(() -> m.ensureCovered(101))))
                .hasMessageContaining("injected error");
        assertThat(metrics.ownerFaults()).isEqualTo(1L);
        assertThat(m.state().kind).isEqualTo(DvState.Kind.COVERING);

        hooks(m).setAfterIngest(null);
        long deadline = System.currentTimeMillis() + 30_000;
        while (true) {
            try {
                if (pump(async(() -> m.ensureCovered(101)))) {
                    break;
                }
            } catch (DeletionVectorsNotReadyException e) {
                // the failed switch is tried again after a backoff
            }
            assertThat(System.currentTimeMillis()).isLessThan(deadline);
            Thread.sleep(50);
        }
        assertThat(m.state().version.snapshotId).isEqualTo(101L);
    }

    /**
     * An error on a replay's job thread ends the replay: its rebuild slot, one of the server's, is
     * released, and a later read replays the change list again.
     */
    @Test
    void testAnErrorInAReplayJobReleasesItsRebuildSlot() throws Exception {
        coverSnapshot100();
        DerivedDvRebuildLimiter limiter = DvTestManagers.executors().rebuilds();
        int free = limiter.availableSlots();
        m.handOffFailed(new IllegalStateException("hand-off failed"));
        src.queueProbe =
                () -> {
                    throw new AssertionError("injected error on the replay thread");
                };
        assertThatThrownBy(() -> pump(read(100, true))).isInstanceOf(Exception.class);
        pumpUntil(() -> m.state().kind == DvState.Kind.COVERING);
        assertThat(limiter.availableSlots()).isEqualTo(free);
        assertThat(metrics.ownerFaults()).isEqualTo(1L);

        src.queueProbe = null;
        long deadline = System.currentTimeMillis() + 30_000;
        while (true) {
            try {
                assertThat(pump(read(100, true)).getLogEndOffset()).isEqualTo(12L);
                break;
            } catch (Exception e) {
                // the replay is tried again
            }
            assertThat(System.currentTimeMillis()).isLessThan(deadline);
            Thread.sleep(50);
        }
        assertThat(limiter.availableSlots()).isEqualTo(free);
    }

    /**
     * Once the hand-off overflowed, no write waits again while the episode lasts: also a write
     * whose batch has no key changes returns at once, while the owner has not released the queue.
     */
    @Test
    void testNoWriteWaitsAgainAfterAnOverflow() throws Exception {
        coverSnapshot100();
        m.setHandOffBounds(10, 300, 60_000, 60_000, 60_000);
        List<DvEntry> batch = new ArrayList<>();
        for (int i = 0; i < 11; i++) {
            src.append("w" + i, 12 + i, 12 + i);
            batch.add(new DvEntry(key("w" + i), 12 + i, 12 + i, true, -1));
        }
        highWatermark.set(23);
        // no step of the owner runs: the write overflows the hand-off after the bound
        async(() -> m.handleChangelogSynced(batch, 23)).get(10, TimeUnit.SECONDS);
        for (int i = 0; i < 5; i++) {
            long start = System.currentTimeMillis();
            m.handleChangelogSynced(Collections.emptyList(), 24 + i);
            assertThat(System.currentTimeMillis() - start).isLessThan(100L);
        }
        steps.runAll();
        assertThat(m.state().changeListPresent).isFalse();
    }

    /**
     * A close waits for the owner's close step and for the RocksDB's leases at most the close wait
     * in all, not once for each.
     */
    @Test
    void testACloseWaitsAtMostTheCloseWaitInAll() throws Exception {
        coverSnapshot100();
        m.setCloseWaitMillis(400);
        // no step of the owner runs: the close step and the leases both wait
        long start = System.currentTimeMillis();
        m.close();
        assertThat(System.currentTimeMillis() - start).isLessThan(700L);
        pumpUntil(() -> m.dbClosed().isDone());
    }

    /**
     * A takeover step that fails is tried again by the owner after a backoff, on the path a leader
     * transition takes, which starts the recovery once: it returns at the failure, not after the
     * takeover wait, and the bucket is taken over and recovered by the retry.
     */
    @Test
    void testAFailedTakeoverIsTriedAgain() throws Exception {
        publish100();
        src.logEndFailure = new IllegalStateException("injected takeover failure");
        long start = System.currentTimeMillis();
        pump(async(() -> m.recoverInBackground(src, DvTestManagers.executors().rebuilds())));
        assertThat(System.currentTimeMillis() - start).isLessThan(DvOwnerWork.TAKEOVER_WAIT_MS);
        assertThat(m.state().kind).isEqualTo(DvState.Kind.UNRECOVERED);
        assertThat(metrics.ownerFaults()).isEqualTo(1L);

        pumpUntil(() -> m.state().kind == DvState.Kind.COVERING);
        assertThat(m.getReadableSnapshotId()).isEqualTo(100L);
        assertThat(asStrings(m.lookup(100, Collections.singletonList(key("k1")))))
                .containsExactly("k1@" + F1 + "#0");
        assertThat(metrics.ownerFaults()).isEqualTo(1L);
    }

    /**
     * A close while a failed takeover waits to be tried again ends the recovery, and the takeover
     * is not tried again: nothing is recovered after the close.
     */
    @Test
    void testACloseEndsATakeoverWaitingToBeTriedAgain() throws Exception {
        publish100();
        src.logEndFailure = new IllegalStateException("injected takeover failure");
        CompletableFuture<Void> recovered = async(() -> m.recover(src));
        pumpUntil(() -> metrics.ownerFaults() == 1);
        assertThat(recovered).isNotDone();
        pump(async(() -> m.close()));
        // the blocking recovery returns, covering nothing
        pump(recovered);
        // past the backoff: the retry finds the bucket closed
        pumpFor(DvOwnerPlanner.ADVANCE_RETRY_INITIAL_BACKOFF_MS + 500);
        assertThat(m.state().kind).isEqualTo(DvState.Kind.CLOSED);
        assertThat(src.replayedFrom).isEmpty();
        assertThat(m.dbClosed()).isDone();
    }

    /**
     * A lookup of a published version it cannot retain gives up at its deadline with NOT_READY,
     * rather than spinning until the owner publishes another version.
     */
    @Test
    void testALookupOfAVersionItCannotRetainGivesUp() throws Exception {
        coverSnapshot100();
        // as if the owner released the version and could not publish the next one
        AtomicInteger refs = refs(m.state().version);
        refs.set(0);
        try {
            CompletableFuture<List<Object[]>> lookup =
                    async(() -> m.lookup(100, Collections.singletonList(key("k1"))));
            assertThatThrownBy(() -> lookup.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(DeletionVectorsNotReadyException.class);
        } finally {
            refs.set(1);
        }
    }

    private static AtomicInteger refs(DvVersion v) throws Exception {
        Field f = DvVersion.class.getDeclaredField("refs");
        f.setAccessible(true);
        return (AtomicInteger) f.get(v);
    }

    /**
     * A change list lost in a replay's final pass (a hand-off failed after the final read began) is
     * not installed, as the batches after the final read may be lost with it: the list stays
     * missing until a later replay, which installs the fold of the log. In a recovery's replay
     * (full) and in a replay of the change list alone (tail).
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testAChangeListLostInTheFinalPassIsNotInstalled(boolean full) throws Exception {
        AtomicInteger replayed = new AtomicInteger();
        IntSupplier probe =
                () -> {
                    int n = replayed.incrementAndGet();
                    if (n == 1) {
                        // written during the first pass, so the final read replays it
                        src.append("k3", 12, 12);
                        highWatermark.set(13);
                    } else if (n == 3) {
                        // the final read replays k3@12; a hand-off fails meanwhile
                        m.handOffFailed(new IllegalStateException("lost in the final pass"));
                    }
                    return 0;
                };
        if (full) {
            publish100();
            src.queueProbe = probe;
            pump(async(() -> m.recover(src)));
        } else {
            coverSnapshot100();
            m.handOffFailed(new IllegalStateException("hand-off failed"));
            steps.runAll();
            src.replayedFrom.clear();
            src.queueProbe = probe;
            CompletableFuture<DvSnapshot> first = read(100, true);
            pumpUntil(first::isDone);
        }
        assertThat(replayed.get()).isGreaterThanOrEqualTo(3);
        DvSnapshot after = pump(read(100, true));
        // the lost replay (a first pass from 10, the final read from 12), then one from 10
        assertThat(src.replayedFrom).containsExactly(10L, 12L, 10L);
        assertThat(after.getLogEndOffset()).isEqualTo(13L);
        assertThat(lakeMasks(after).get(F1)).containsExactlyInAnyOrder(0L, 1L, 2L);
        assertThat(metrics.ownerFaults()).isZero();
    }

    /**
     * A batch handed over in a replay's final pass, after the final read took the log end, is
     * buffered and installed with the replayed list: the masks include it, as the fold does. In a
     * recovery's replay (full) and in a replay of the change list alone (tail).
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testABatchHandedOverInTheFinalPassIsInstalled(boolean full) throws Exception {
        AtomicInteger replayed = new AtomicInteger();
        IntSupplier probe =
                () -> {
                    int n = replayed.incrementAndGet();
                    if (n == 1) {
                        // written during the first pass, so the final read replays it
                        src.append("k2", 12, 12);
                    } else if (n == 3) {
                        // the final read took the log end 13: k3@13 comes after it, handed over
                        src.append("k3", 13, 13);
                        DvTestManagers.handOver(
                                manager(), Collections.singletonList(src.written("k3", 13, 13)));
                        highWatermark.set(14);
                    }
                    return 0;
                };
        if (full) {
            publish100();
            src.queueProbe = probe;
            pump(async(() -> m.recover(src)));
        } else {
            coverSnapshot100();
            m.handOffFailed(new IllegalStateException("hand-off failed"));
            steps.runAll();
            src.replayedFrom.clear();
            src.queueProbe = probe;
            pump(read(100, true));
        }
        assertThat(replayed.get()).isEqualTo(3);
        DvSnapshot after = pump(read(100, true));
        // one replay: a first pass from 10 and the final read from 12
        assertThat(src.replayedFrom).containsExactly(10L, 12L);
        assertThat(after.getLogEndOffset()).isEqualTo(14L);
        assertThat(lakeMasks(after).get(F1)).containsExactlyInAnyOrder(0L, 1L, 2L);
    }

    /**
     * A load whose job cannot start (the rebuild pool cannot create a thread) ends there: the fault
     * is counted, the work ends with it and releases what the load holds, and the error then goes
     * to the pool's handler. The bucket plans again, so a later catch-up covers the snapshot.
     */
    @Test
    void testAnErrorStartingALoadJobEndsItsWork() throws Exception {
        publish100();
        DerivedDvRebuildLimiter limiter = new DerivedDvRebuildLimiter(4);
        AtomicInteger executes = new AtomicInteger();
        ThreadPoolExecutor failing =
                new ThreadPoolExecutor(2, 2, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>()) {
                    @Override
                    public void execute(Runnable command) {
                        // the second job is the recovery's full load
                        if (executes.incrementAndGet() == 2) {
                            throw new OutOfMemoryError("unable to create native thread");
                        }
                        super.execute(command);
                    }
                };
        Field f = DerivedDvRebuildLimiter.class.getDeclaredField("executor");
        f.setAccessible(true);
        f.set(limiter, new BoundedConcurrencyExecutor(failing, 4));
        try {
            pump(async(() -> m.recoverInBackground(src, limiter)));
            pumpUntil(() -> metrics.ownerFaults() == 1);
            // handled as the step's fault, then handed to the pool's uncaught exception handler
            assertThat(steps.escaped).hasSize(1);
            assertThat(steps.escaped.get(0)).isInstanceOf(OutOfMemoryError.class);
            assertThat(executes.get()).isEqualTo(2);
            assertThat(m.state().kind).isNotEqualTo(DvState.Kind.REBUILDING);
            assertThat(limiter.availableSlots()).isEqualTo(4);

            assertThat(pump(async(() -> m.ensureCovered(100)))).isTrue();
            assertThat(m.getReadableSnapshotId()).isEqualTo(100L);
            assertThat(metrics.ownerFaults()).isEqualTo(1L);
        } finally {
            failing.shutdownNow();
        }
    }

    /**
     * An overflow whose drain signal a running drain took, as the writer's wait ran out before the
     * drain reset its signal, is taken by that same drain once it polled: the change list goes
     * Missing and a replay is armed without another drain.
     */
    @Test
    void testAnOverflowDuringADrainIsTakenWithoutAnotherSignal() throws Exception {
        coverSnapshot100();
        DvHandOff h = handOff();
        AtomicBoolean overflowed = new AtomicBoolean();
        setQueue(
                h,
                new ConcurrentLinkedQueue<DvHandOff.Batch>() {
                    @Override
                    public DvHandOff.Batch poll() {
                        DvHandOff.Batch b = super.poll();
                        if (b != null && overflowed.compareAndSet(false, true)) {
                            // the writer's wait runs out while the drain's own signal is still
                            // set, so its overflow posts no drain
                            AtomicBoolean posted = drainPosted(h);
                            posted.set(true);
                            assertThat(
                                            h.awaitDrain(
                                                    0,
                                                    1,
                                                    () -> false,
                                                    DerivedDvMetrics.noop(),
                                                    "test"))
                                    .isTrue();
                            posted.set(false);
                        }
                        return b;
                    }
                });
        src.append("k3", 12, 12);
        DvTestManagers.handOver(manager(), Collections.singletonList(src.written("k3", 12, 12)));
        steps.runAll();
        assertThat(overflowed).isTrue();
        assertThat(m.state().changeListPresent).isFalse();
    }

    private DvHandOff handOff() throws Exception {
        Field f = DerivedDvManager.class.getDeclaredField("owner");
        f.setAccessible(true);
        return ((DvOwner) f.get(m)).handOff;
    }

    private static void setQueue(DvHandOff h, ConcurrentLinkedQueue<DvHandOff.Batch> queue)
            throws Exception {
        Field f = DvHandOff.class.getDeclaredField("queue");
        f.setAccessible(true);
        f.set(h, queue);
    }

    private static AtomicBoolean drainPosted(DvHandOff h) {
        try {
            Field f = DvHandOff.class.getDeclaredField("drainPosted");
            f.setAccessible(true);
            return (AtomicBoolean) f.get(h);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    /** A bucket closes in each state: requests go to the new leader, and nothing is left open. */
    @ParameterizedTest
    @EnumSource(
            value = DvState.Kind.class,
            names = {"UNRECOVERED", "REBUILDING", "COVERING", "SWITCHING", "REPLAYING"})
    void testCloseInEveryState(DvState.Kind kind) throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture<?> pending = CompletableFuture.completedFuture(null);
        switch (kind) {
            case UNRECOVERED:
                break;
            case REBUILDING:
                publish100();
                pauseReplayAfter(0);
                src.replayGate = release;
                pending = async(() -> m.recover(src));
                pumpUntil(() -> src.replayPaused.getCount() == 0);
                break;
            case COVERING:
                coverSnapshot100();
                break;
            case SWITCHING:
                coverSnapshot100();
                src.publish(101, 2, 12, dict(2, F2), new Index().put("k1", 2, 0).build());
                src.readable = new long[] {101, 12};
                CountDownLatch ingesting = new CountDownLatch(1);
                hooks(m).setAfterIngest(
                                () -> {
                                    ingesting.countDown();
                                    waitFor(release).run();
                                });
                pending = async(() -> m.ensureCovered(101));
                pumpUntil(() -> ingesting.getCount() == 0);
                break;
            case REPLAYING:
                coverSnapshot100();
                m.handOffFailed(new IllegalStateException("hand-off failed"));
                pauseReplayAfter(0);
                src.replayGate = release;
                pending = read(100, true);
                pumpUntil(() -> src.replayPaused.getCount() == 0);
                break;
            default:
                throw new IllegalArgumentException(kind.toString());
        }
        assertThat(m.state().kind).isEqualTo(kind);

        m.setCloseWaitMillis(10);
        pump(async(() -> m.close()));
        assertThat(m.state().kind).isEqualTo(DvState.Kind.CLOSED);
        assertThatThrownBy(() -> pump(read(100, true)))
                .isInstanceOf(NotLeaderOrFollowerException.class);
        assertThatThrownBy(() -> m.lookup(100, Collections.singletonList(key("k1"))))
                .isInstanceOf(NotLeaderOrFollowerException.class);

        // the held load ends at its next unit and releases what it held
        release.countDown();
        pumpUntil(pending::isDone);
        pumpUntil(() -> m.dbClosed().isDone());
        assertThat(m.leasesHeld()).isZero();
    }

    // ------------------------------------------------------------------------------------------

    /** Snapshot 100 covers k1, k2, k3 at f1#0..2; k1 and k2 changed at 10 and 11. */
    private void publish100() {
        src.publish(
                100,
                1,
                10,
                dict(1, F1),
                new Index().put("k1", 1, 0).put("k2", 1, 1).put("k3", 1, 2).build());
        src.readable = new long[] {100, 10};
        src.append("k1", 10, 10);
        src.append("k2", 11, 11);
        highWatermark.set(12);
    }

    private void coverSnapshot100() throws Exception {
        publish100();
        pump(async(() -> m.recover(src)));
        assertThat(m.state().kind).isEqualTo(DvState.Kind.COVERING);
    }

    private DerivedDvManager manager() {
        return m;
    }

    private CompletableFuture<DvSnapshot> read(long snapshotId, boolean waitForCatchUp) {
        return async(() -> DvTestReads.unionRead(m, snapshotId, waitForCatchUp));
    }

    private <T> CompletableFuture<T> async(Callable<T> call) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return call.call();
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                },
                async);
    }

    private CompletableFuture<Void> async(ThrowingRunnable run) {
        return async(
                () -> {
                    run.run();
                    return null;
                });
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private void assertNotReady(CompletableFuture<?> f) {
        assertThatThrownBy(() -> pump(f)).isInstanceOf(DeletionVectorsNotReadyException.class);
    }

    /** Runs the owner's steps on this thread until {@code f} completes; returns its value. */
    private <T> T pump(CompletableFuture<T> f) throws Exception {
        pumpUntil(f::isDone);
        try {
            return f.join();
        } catch (CompletionException e) {
            throw e.getCause() instanceof Exception ? (Exception) e.getCause() : e;
        }
    }

    private void pumpUntil(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (!condition.getAsBoolean()) {
            assertThat(System.currentTimeMillis()).isLessThan(deadline);
            if (!steps.runAll()) {
                Thread.sleep(1);
            }
        }
        steps.runAll();
    }

    /** Runs the owner's steps on this thread for {@code millis}. */
    private void pumpFor(long millis) throws InterruptedException {
        long end = System.currentTimeMillis() + millis;
        while (System.currentTimeMillis() < end) {
            if (!steps.runAll()) {
                Thread.sleep(2);
            }
        }
    }

    /** Runs the owner's steps when the test says so. */
    private static final class ManualExecutor implements Executor {
        private final ConcurrentLinkedQueue<Runnable> tasks = new ConcurrentLinkedQueue<>();
        // what escaped a step, as the pool's uncaught exception handler would get it
        final List<Throwable> escaped = new CopyOnWriteArrayList<>();

        @Override
        public void execute(Runnable task) {
            tasks.add(task);
        }

        /** Runs the queued tasks and those they queue; returns whether any ran. */
        boolean runAll() {
            boolean ran = false;
            Runnable task;
            while ((task = tasks.poll()) != null) {
                try {
                    task.run();
                } catch (VirtualMachineError e) {
                    escaped.add(e);
                }
                ran = true;
            }
            return ran;
        }
    }
}
