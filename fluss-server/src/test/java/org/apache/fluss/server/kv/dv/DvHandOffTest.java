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
import org.apache.fluss.server.kv.dv.DerivedDvManagerTest.Index;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.dict;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.key;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests of the hand-off of a bucket's key changes from its write path ({@link DvHandOff}): writes
 * keep coming while the change list is replayed and installed again and again, and the masks at the
 * end equal a fold of the log. A change drained before a replay's final pass is in the replay; one
 * drained during it is buffered, or repeats one the replay has; none is lost or applied twice.
 */
class DvHandOffTest extends DerivedDvManagerTestBase {

    // The writes run at most this many changes ahead of the replays the test forces, so the last of
    // the 3,000 writes waits for the fourth replay, however the threads are scheduled.
    private static final int WRITES_PER_REPLAY = 600;

    @ParameterizedTest
    @CsvSource({"false, false", "true, false", "false, true", "true, true"})
    void testWritesDuringReplaysMatchAFoldOfTheLog(boolean lakeSnapshot, boolean overflow)
            throws Exception {
        if (overflow) {
            // the owner falls behind for a moment again and again: the hand-off overflows
            manager.setHandOffBounds(20, 20, 10, 80, 10_000);
        }
        // key -> {file id, row position} in the lake
        Map<Integer, int[]> lake = new TreeMap<>();
        long start = 0;
        if (lakeSnapshot) {
            Index initial = new Index();
            for (int i = 0; i < 20; i++) {
                initial.put("k" + i, 1, i);
                lake.put(i, new int[] {1, i});
            }
            start = 10;
            src.publish(100, 1, start, dict(1, F1), initial.build());
            src.readable = new long[] {100, start};
        }
        highWatermark.set(start);
        manager.recover(src);
        long snapshot = lakeSnapshot ? 100 : -1;
        awaitServed(snapshot);

        Random random = new Random(lakeSnapshot ? 7 : 3);
        List<long[]> changes = Collections.synchronizedList(new ArrayList<>());
        AtomicBoolean writing = new AtomicBoolean(true);
        AtomicInteger replays = new AtomicInteger();
        long first = start;
        CompletableFuture<Void> writer =
                CompletableFuture.runAsync(
                        () -> {
                            // keys with a live version in KV, to its offset
                            Map<Integer, Long> live = new HashMap<>();
                            if (lakeSnapshot) {
                                for (int i = 0; i < 20; i++) {
                                    live.put(i, -1L);
                                }
                            }
                            Random r = new Random(11);
                            long offset = first;
                            for (int i = 0; i < 3_000; i++) {
                                awaitReplays(replays, i / WRITES_PER_REPLAY);
                                int k = r.nextInt(30);
                                long version = r.nextInt(6) == 0 ? -1 : offset;
                                Long previous = live.get(k);
                                boolean insert = version >= 0 && previous == null;
                                if (version < 0) {
                                    live.remove(k);
                                } else {
                                    live.put(k, version);
                                }
                                // in the log first, then handed over, as the write path does
                                src.append("k" + k, offset, version, insert);
                                changes.add(new long[] {offset, k, version});
                                DvTestManagers.handOver(
                                        manager,
                                        Collections.singletonList(
                                                new DvEntry(
                                                        key("k" + k),
                                                        offset,
                                                        version,
                                                        insert,
                                                        previous == null ? -1 : previous)));
                                offset++;
                                highWatermark.set(offset);
                                if (i % 10 == 9) {
                                    // writes keep coming while replays install, not all at once
                                    sleepQuietly(1);
                                }
                            }
                            writing.set(false);
                        },
                        async);
        // the change list is dropped and replayed again and again while the writes come
        while (writing.get()) {
            // counted first, so the writes come while the owner is paused or replays
            replays.incrementAndGet();
            if (overflow) {
                pauseOwner(30 + random.nextInt(30));
            } else {
                manager.handOffFailed(new IllegalStateException("hand-off failed"));
            }
            awaitServed(snapshot);
            Thread.sleep(random.nextInt(5));
        }
        writer.get(60, TimeUnit.SECONDS);
        assertThat(replays.get()).isGreaterThanOrEqualTo((3_000 - 1) / WRITES_PER_REPLAY);
        if (overflow) {
            assertThat(metrics.handOffsDropped()).isPositive();
        }

        long end = highWatermark.get();
        DvSnapshot masks = awaitServed(snapshot);
        List<long[]> log;
        synchronized (changes) {
            log = new ArrayList<>(changes);
        }
        assertThat(masks.getLogEndOffset()).isEqualTo(end);
        assertThat(DerivedDvChangeListTest.describe(masks))
                .isEqualTo(DerivedDvChangeListTest.model(log, lake, start, end));
    }

    /** Waits until the test has forced {@code count} replays. */
    private static void awaitReplays(AtomicInteger replays, int count) {
        long deadline = System.currentTimeMillis() + 60_000;
        while (replays.get() < count) {
            assertThat(System.currentTimeMillis()).isLessThan(deadline);
            sleepQuietly(1);
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Reads masks that wait until the bucket serves them. */
    /**
     * A batch dropped once the hand-off overflowed is in the log, not in the queue: a drain never
     * returns an end past it, so no masks are built over it while the change list is still present,
     * whenever the owner takes the overflow.
     */
    @Test
    void testADroppedBatchIsNotCountedAsHandedOver() {
        AtomicInteger posted = new AtomicInteger();
        DvHandOff h = new DvHandOff(posted::incrementAndGet);
        h.add(Collections.singletonList(new DvEntry(key("a"), 10, 10, true, -1)), 11);
        // the owner has not taken the overflow yet when the writer's wait runs out
        assertThat(h.takeOverflow()).isFalse();
        assertThat(h.awaitDrain(0, 1, () -> false, DerivedDvMetrics.noop(), "test")).isTrue();
        assertThat(h.add(Collections.singletonList(new DvEntry(key("b"), 11, 11, false, 0)), 12))
                .isFalse();
        // reads take their floor past the dropped batch
        assertThat(h.appendedEnd()).isEqualTo(12L);
        List<DvHandOff.Batch> drained = new ArrayList<>();
        assertThat(h.drain(drained::add)).isEqualTo(11L);
        assertThat(drained).hasSize(1);
        assertThat(h.takeOverflow()).isTrue();
    }

    /**
     * A drain returns the end it read before it polled: a batch queued once the drain found the
     * queue empty is above that end, so masks built up to it never pass a batch not drained.
     */
    @Test
    void testADrainNeverReturnsTheEndOfABatchItDidNotTake() throws Exception {
        DvHandOff h = new DvHandOff(() -> {});
        AtomicBoolean written = new AtomicBoolean();
        setQueue(
                h,
                new ConcurrentLinkedQueue<DvHandOff.Batch>() {
                    @Override
                    public DvHandOff.Batch poll() {
                        DvHandOff.Batch b = super.poll();
                        if (b == null && written.compareAndSet(false, true)) {
                            // a write lands right after the drain found the queue empty
                            h.add(
                                    Collections.singletonList(
                                            new DvEntry(key("b"), 11, 11, false, -1)),
                                    12);
                        }
                        return b;
                    }
                });
        h.add(Collections.singletonList(new DvEntry(key("a"), 10, 10, false, -1)), 11);
        List<DvHandOff.Batch> drained = new ArrayList<>();
        assertThat(h.drain(drained::add)).isEqualTo(11L);
        assertThat(drained).hasSize(1);
        assertThat(written).isTrue();
        // the next drain takes it
        assertThat(h.drain(drained::add)).isEqualTo(12L);
        assertThat(drained).hasSize(2);
    }

    private static void setQueue(DvHandOff h, ConcurrentLinkedQueue<DvHandOff.Batch> queue)
            throws Exception {
        Field f = DvHandOff.class.getDeclaredField("queue");
        f.setAccessible(true);
        f.set(h, queue);
    }

    /** Holds the bucket's owner in a step for {@code millis}, as a slow step would. */
    private void pauseOwner(long millis) throws Exception {
        CountDownLatch held = new CountDownLatch(1);
        CompletableFuture<Void> paused =
                CompletableFuture.runAsync(
                        () ->
                                manager.onOwner(
                                        () -> {
                                            held.countDown();
                                            sleepQuietly(millis);
                                        }),
                        async);
        held.await(10, TimeUnit.SECONDS);
        paused.get(10, TimeUnit.SECONDS);
    }

    private DvSnapshot awaitServed(long snapshotId) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        while (true) {
            try {
                return DvTestReads.unionRead(manager, snapshotId);
            } catch (RetriableException e) {
                assertThat(System.currentTimeMillis()).isLessThan(deadline);
                Thread.sleep(5);
            }
        }
    }
}
