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

import org.apache.fluss.exception.DeletionVectorsNotReadyException;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.server.kv.dv.DerivedDvManagerTest.FakeSource;
import org.apache.fluss.server.kv.dv.DerivedDvManagerTest.Index;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.dict;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.key;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.lakeMasks;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.offsets;
import static org.apache.fluss.server.kv.dv.DvTestManagers.hooks;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the log tail, whose updates and deletes the bucket keeps in its RocksDB, and the tablet
 * server's heap budget for the rest of the deletion vector state.
 */
class DerivedDvChangeListTest {

    private static final String F1 = "s3://lake/t/data/f1.parquet";

    @TempDir Path tempDir;

    private final AtomicLong highWatermark = new AtomicLong();
    private final List<DerivedDvManager> managers = new ArrayList<>();
    private FakeSource src;

    @BeforeEach
    void setUp() throws IOException {
        src = new FakeSource(Files.createDirectories(tempDir.resolve("remote")));
    }

    @AfterEach
    void tearDown() {
        for (DerivedDvManager m : managers) {
            m.close();
        }
    }

    /**
     * The same changes, high watermark moves, snapshot advances and rebuilds, applied to a bucket,
     * to one whose budget is always exceeded and to one whose replays keep their map of last
     * versions on disk, give the same masks: those of a model of the log and the lake. A change
     * names the offset of the version it supersedes, as the KV value's tag gives it. Changes of
     * keys without a live version are inserts, as on the KV write path, so their positions are not
     * looked up.
     */
    @ParameterizedTest
    @ValueSource(longs = {1, 7, 42})
    void testTailServesTheMasksOfTheModel(long seed) throws Exception {
        DerivedDvManager plain = newManager("plain", unbounded(), DerivedDvMetrics.noop());
        DerivedDvManager tight =
                newManager(
                        "tight",
                        new DerivedDvHeapBudget(1, DvTestManagers.executors()),
                        DerivedDvMetrics.noop());
        // its replays keep the map of last versions on disk
        DerivedDvManager spilledReplay =
                newManager("spilled-replay", unbounded(), DerivedDvMetrics.noop());
        spilledReplay.setSpillReplayVersions(true);
        List<DerivedDvManager> all = java.util.Arrays.asList(plain, tight, spilledReplay);
        Index initial = new Index();
        // key -> {file id, row position} in the lake
        Map<Integer, int[]> lake = new TreeMap<>();
        // keys with a live version in KV, to the offset of that version (the KV value's tag)
        Map<Integer, Long> live = new HashMap<>();
        for (int i = 0; i < 40; i++) {
            initial.put("k" + i, 1, i);
            lake.put(i, new int[] {1, i});
            live.put(i, 0L);
        }
        src.publish(100, 1, 10, dict(1, F1), initial.build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        for (DerivedDvManager m : all) {
            m.recover(src);
        }

        Random random = new Random(seed);
        List<long[]> changes = new ArrayList<>(); // {offset, key, version}
        long logEnd = 10;
        long tiered = 10;
        long snapshot = 100;
        int nextFile = 2;
        int compares = 0;
        for (int step = 0; step < 700; step++) {
            int op = random.nextInt(100);
            if (op < 60) {
                int k = random.nextInt(60);
                long offset = logEnd++;
                long version = random.nextInt(5) == 0 ? -1 : offset;
                Long previous = live.get(k);
                boolean insert = version >= 0 && previous == null;
                if (version < 0) {
                    live.remove(k);
                } else {
                    live.put(k, version);
                }
                src.append("k" + k, offset, version, insert);
                changes.add(new long[] {offset, k, version});
                for (DerivedDvManager m : all) {
                    // one entry per bucket, as a writer marks its own
                    DvTestManagers.handOver(
                            m,
                            Collections.singletonList(
                                    new DvEntry(
                                            key("k" + k),
                                            offset,
                                            version,
                                            insert,
                                            previous == null ? -1 : previous)));
                }
            } else if (op < 70) {
                highWatermark.addAndGet(random.nextInt((int) (logEnd - highWatermark.get()) + 1));
            } else if (op < 77 && highWatermark.get() > tiered) {
                // tiering registers a snapshot at an offset up to the high watermark
                long upTo = tiered + 1 + random.nextInt((int) (highWatermark.get() - tiered));
                Map<Long, long[]> last = new TreeMap<>();
                for (long[] c : changes) {
                    if (c[0] >= tiered && c[0] < upTo) {
                        last.put(c[1], c);
                    }
                }
                Index change = new Index();
                int position = 0;
                for (long[] c : last.values()) {
                    if (c[2] < 0) {
                        change.delete("k" + c[1]);
                        lake.remove((int) c[1]);
                    } else {
                        change.put("k" + c[1], nextFile, position);
                        lake.put((int) c[1], new int[] {nextFile, position++});
                    }
                }
                snapshot = 100 + nextFile;
                src.publish(
                        snapshot, nextFile, upTo, dict(nextFile, "f" + nextFile), change.build());
                src.readable = new long[] {snapshot, upTo};
                nextFile++;
                tiered = upTo;
                for (DerivedDvManager m : all) {
                    assertThat(m.ensureCovered(snapshot)).isTrue();
                }
            } else if (op < 80 && !lake.isEmpty()) {
                // a rewrite moves the rows of one lake file into a new one, at the same offset
                int file = lake.values().iterator().next()[0];
                Index change = new Index();
                int position = 0;
                for (Map.Entry<Integer, int[]> e : lake.entrySet()) {
                    if (e.getValue()[0] == file) {
                        change.put("k" + e.getKey(), nextFile, position);
                        e.setValue(new int[] {nextFile, position++});
                    }
                }
                snapshot = 100 + nextFile;
                src.replaced.put(
                        (long) nextFile, Collections.singletonList(file == 1 ? F1 : "f" + file));
                src.publish(
                        snapshot, nextFile, tiered, dict(nextFile, "f" + nextFile), change.build());
                src.readable = new long[] {snapshot, tiered};
                nextFile++;
                for (DerivedDvManager m : all) {
                    assertThat(m.ensureCovered(snapshot)).isTrue();
                }
            } else if (op < 84) {
                for (DerivedDvManager m : all) {
                    m.handOffFailed(new IllegalStateException("rebuild"));
                }
            } else {
                String expected = model(changes, lake, tiered, highWatermark.get());
                for (DerivedDvManager m : all) {
                    assertThat(describe(unionRead(m, snapshot))).isEqualTo(expected);
                }
                compares++;
            }
        }
        highWatermark.set(logEnd);
        String expected = model(changes, lake, tiered, logEnd);
        for (DerivedDvManager m : all) {
            assertThat(describe(unionRead(m, snapshot))).isEqualTo(expected);
        }
        assertThat(compares).isGreaterThan(20);
    }

    /**
     * The history of {@link #testTailServesTheMasksOfTheModel}, with reads racing it: readers on
     * their own threads keep reading the masks, waiting or not, of the snapshot the history last
     * registered and of the one before, while the writes, switches, replays and hand-off failures
     * go on, and switches are started by registrations rather than waited for. Every answer equals
     * the model of the log and of the lake of the snapshot it was served for, at its own cut, and a
     * reader never gets a lower cut than it got before. With small steps, the owner applies a few
     * changes a step and builds frontiers a file a step, and ready reads are answered on their own
     * threads.
     */
    @ParameterizedTest
    @CsvSource({"3, false", "19, false", "3, true", "19, true"})
    void testConcurrentReadsServeTheMasksOfTheModel(long seed, boolean smallSteps)
            throws Exception {
        DerivedDvManager m = newManager("concurrent", unbounded(), DerivedDvMetrics.noop());
        if (smallSteps) {
            m.setStepChunks(3, 1);
            m.setMaskCacheMillis(100);
        }
        Index initial = new Index();
        Map<Integer, int[]> lake = new TreeMap<>();
        Map<Integer, Long> live = new HashMap<>();
        for (int i = 0; i < 40; i++) {
            initial.put("k" + i, 1, i);
            lake.put(i, new int[] {1, i});
            live.put(i, 0L);
        }
        src.publish(100, 1, 10, dict(1, F1), initial.build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        m.recover(src);
        // per snapshot: its lake rows and readable offset, as the model reads them
        Map<Long, Object[]> lakes = new ConcurrentHashMap<>();
        lakes.put(100L, new Object[] {new TreeMap<>(lake), 10L});
        List<long[]> changes = Collections.synchronizedList(new ArrayList<>());
        java.util.concurrent.atomic.AtomicLong newest =
                new java.util.concurrent.atomic.AtomicLong(100);
        AtomicBoolean running = new AtomicBoolean(true);
        List<String> mismatches = Collections.synchronizedList(new ArrayList<>());
        java.util.concurrent.atomic.AtomicInteger checked =
                new java.util.concurrent.atomic.AtomicInteger();
        ExecutorService readers = Executors.newFixedThreadPool(3);
        List<Future<?>> readerTasks = new ArrayList<>();
        for (int t = 0; t < 3; t++) {
            boolean wait = t != 1;
            Random r = new Random(seed * 31 + t);
            readerTasks.add(
                    readers.submit(
                            () -> {
                                long lastCut = -1;
                                while (running.get()) {
                                    long snapshot = newest.get() - (r.nextInt(3) == 0 ? 1 : 0);
                                    DvSnapshot masks;
                                    try {
                                        masks = DvTestReads.unionRead(m, snapshot, wait);
                                    } catch (Exception e) {
                                        continue; // not ready, stale or behind: asked again
                                    }
                                    Object[] l = lakes.get(snapshot);
                                    if (l == null) {
                                        mismatches.add("masks of unknown snapshot " + snapshot);
                                        continue;
                                    }
                                    List<long[]> log;
                                    synchronized (changes) {
                                        log = new ArrayList<>(changes);
                                    }
                                    @SuppressWarnings("unchecked")
                                    Map<Integer, int[]> rows = (Map<Integer, int[]>) l[0];
                                    String expected =
                                            model(log, rows, (Long) l[1], masks.getLogEndOffset());
                                    String actual;
                                    try {
                                        actual = describe(masks);
                                    } catch (IOException e) {
                                        throw new UncheckedIOException(e);
                                    }
                                    if (!expected.equals(actual)) {
                                        mismatches.add(
                                                snapshot + ": " + actual + " != " + expected);
                                    }
                                    if (masks.getLogEndOffset() < lastCut) {
                                        mismatches.add(
                                                "cut went back from "
                                                        + lastCut
                                                        + " to "
                                                        + masks.getLogEndOffset());
                                    }
                                    lastCut = Math.max(lastCut, masks.getLogEndOffset());
                                    checked.incrementAndGet();
                                }
                                return null;
                            }));
        }
        Random random = new Random(seed);
        long logEnd = 10;
        long tiered = 10;
        int nextFile = 2;
        try {
            for (int step = 0; step < 600; step++) {
                int op = random.nextInt(100);
                if (op < 65) {
                    int k = random.nextInt(60);
                    long offset = logEnd++;
                    long version = random.nextInt(5) == 0 ? -1 : offset;
                    Long previous = live.get(k);
                    boolean insert = version >= 0 && previous == null;
                    if (version < 0) {
                        live.remove(k);
                    } else {
                        live.put(k, version);
                    }
                    src.append("k" + k, offset, version, insert);
                    changes.add(new long[] {offset, k, version});
                    DvTestManagers.handOver(
                            m,
                            Collections.singletonList(
                                    new DvEntry(
                                            key("k" + k),
                                            offset,
                                            version,
                                            insert,
                                            previous == null ? -1 : previous)));
                } else if (op < 80) {
                    highWatermark.addAndGet(
                            random.nextInt((int) (logEnd - highWatermark.get()) + 1));
                } else if (op < 88 && highWatermark.get() > tiered) {
                    // tiering registers a snapshot; the bucket switches to it on its own
                    long upTo = tiered + 1 + random.nextInt((int) (highWatermark.get() - tiered));
                    Map<Long, long[]> last = new TreeMap<>();
                    synchronized (changes) {
                        for (long[] c : changes) {
                            if (c[0] >= tiered && c[0] < upTo) {
                                last.put(c[1], c);
                            }
                        }
                    }
                    Index change = new Index();
                    int position = 0;
                    for (long[] c : last.values()) {
                        if (c[2] < 0) {
                            change.delete("k" + c[1]);
                            lake.remove((int) c[1]);
                        } else {
                            change.put("k" + c[1], nextFile, position);
                            lake.put((int) c[1], new int[] {nextFile, position++});
                        }
                    }
                    long snapshot = 100 + nextFile;
                    lakes.put(snapshot, new Object[] {new TreeMap<>(lake), upTo});
                    src.publish(
                            snapshot,
                            nextFile,
                            upTo,
                            dict(nextFile, "f" + nextFile),
                            change.build());
                    src.readable = new long[] {snapshot, upTo};
                    nextFile++;
                    tiered = upTo;
                    newest.set(snapshot);
                    if (random.nextBoolean()) {
                        m.advanceAsync(snapshot);
                    } else {
                        assertThat(m.ensureCovered(snapshot)).isTrue();
                    }
                } else if (op < 91) {
                    m.handOffFailed(new IllegalStateException("rebuild"));
                } else {
                    Thread.sleep(random.nextInt(3));
                }
            }
            highWatermark.set(logEnd);
            assertThat(m.ensureCovered(newest.get())).isTrue();
            DvSnapshot last = unionRead(m, newest.get());
            Object[] l = lakes.get(newest.get());
            @SuppressWarnings("unchecked")
            Map<Integer, int[]> rows = (Map<Integer, int[]>) l[0];
            assertThat(describe(last)).isEqualTo(model(changes, rows, (Long) l[1], logEnd));
        } finally {
            running.set(false);
            for (Future<?> f : readerTasks) {
                f.get(60, TimeUnit.SECONDS);
            }
            readers.shutdownNow();
        }
        assertThat(mismatches).isEmpty();
        assertThat(checked.get()).isGreaterThan(50);
    }

    /**
     * A move to a newer snapshot keeps the masks of the keys its index changes do not name and
     * derives the others again. Random histories of updates, deletes and re-inserts, with catch-ups
     * over several snapshots at once, rewrites of whole files, compactions that move rows of some
     * keys without removing their file, and snapshots that make nothing new readable: after every
     * move, the masks of a bucket, one whose budget is always exceeded and one whose replays spill
     * their map equal those of a model of the log and the lake, and those of a bucket that looks up
     * every lake key again on each move.
     */
    @ParameterizedTest
    @ValueSource(longs = {3, 11, 29, 57, 101})
    void testTargetedMovesServeTheMasksOfTheModel(long seed) throws Exception {
        DerivedDvManager plain = newManager("t-plain", unbounded(), DerivedDvMetrics.noop());
        DerivedDvManager tight =
                newManager(
                        "t-tight",
                        new DerivedDvHeapBudget(1, DvTestManagers.executors()),
                        DerivedDvMetrics.noop());
        DerivedDvManager spilledReplay =
                newManager("t-spilled-replay", unbounded(), DerivedDvMetrics.noop());
        spilledReplay.setSpillReplayVersions(true);
        DerivedDvManager full = newManager("t-full", unbounded(), DerivedDvMetrics.noop());
        full.setTargetedMoves(false);
        List<DerivedDvManager> all = java.util.Arrays.asList(plain, tight, spilledReplay, full);
        Index initial = new Index();
        // key -> {file id, row position} in the lake as of the newest snapshot
        Map<Integer, int[]> lake = new TreeMap<>();
        Map<Integer, Long> live = new HashMap<>();
        for (int i = 0; i < 60; i++) {
            initial.put("k" + i, 1, i);
            lake.put(i, new int[] {1, i});
            live.put(i, 0L);
        }
        src.publish(100, 1, 10, dict(1, F1), initial.build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        for (DerivedDvManager m : all) {
            m.recover(src);
        }
        // the lake rows and readable offset of each snapshot, for the model
        Map<Long, Map<Integer, int[]>> lakeAt = new HashMap<>();
        Map<Long, Long> startAt = new HashMap<>();
        lakeAt.put(100L, new TreeMap<>(lake));
        startAt.put(100L, 10L);

        Random random = new Random(seed);
        List<long[]> changes = new ArrayList<>(); // {offset, key, version}
        long logEnd = 10;
        long tiered = 10;
        long newest = 100;
        long covered = 100;
        int nextFile = 2;
        int moves = 0;
        for (int step = 0; step < 600; step++) {
            int op = random.nextInt(100);
            if (op < 55) {
                int k = random.nextInt(80);
                long offset = logEnd++;
                long version = random.nextInt(5) == 0 ? -1 : offset;
                Long previous = live.get(k);
                boolean insert = version >= 0 && previous == null;
                if (version < 0) {
                    live.remove(k);
                } else {
                    live.put(k, version);
                }
                src.append("k" + k, offset, version, insert);
                changes.add(new long[] {offset, k, version});
                for (DerivedDvManager m : all) {
                    DvTestManagers.handOver(
                            m,
                            Collections.singletonList(
                                    new DvEntry(
                                            key("k" + k),
                                            offset,
                                            version,
                                            insert,
                                            previous == null ? -1 : previous)));
                }
                continue;
            }
            Index change = null;
            List<String> removed = Collections.emptyList();
            long readableFrom = tiered;
            if (op < 62) {
                highWatermark.addAndGet(random.nextInt((int) (logEnd - highWatermark.get()) + 1));
            } else if (op < 69 && highWatermark.get() > tiered) {
                // a round: the last version of each key it tiers, a delete removes the key
                long upTo = tiered + 1 + random.nextInt((int) (highWatermark.get() - tiered));
                Map<Long, long[]> last = new TreeMap<>();
                for (long[] c : changes) {
                    if (c[0] >= tiered && c[0] < upTo) {
                        last.put(c[1], c);
                    }
                }
                change = new Index();
                int position = 0;
                for (long[] c : last.values()) {
                    if (c[2] < 0) {
                        change.delete("k" + c[1]);
                        lake.remove((int) c[1]);
                    } else {
                        change.put("k" + c[1], nextFile, position);
                        lake.put((int) c[1], new int[] {nextFile, position++});
                    }
                }
                readableFrom = upTo;
            } else if (op < 73 && !lake.isEmpty()) {
                // a compaction moves the rows of some keys and keeps their file (Paimon level 0)
                change = new Index();
                int position = 0;
                for (Map.Entry<Integer, int[]> e : lake.entrySet()) {
                    if (random.nextInt(4) == 0) {
                        change.put("k" + e.getKey(), nextFile, position);
                        e.setValue(new int[] {nextFile, position++});
                    }
                }
            } else if (op < 76 && !lake.isEmpty()) {
                // a rewrite moves every row of one file into a new one and removes the file
                int file = lake.values().iterator().next()[0];
                change = new Index();
                int position = 0;
                for (Map.Entry<Integer, int[]> e : lake.entrySet()) {
                    if (e.getValue()[0] == file) {
                        change.put("k" + e.getKey(), nextFile, position);
                        e.setValue(new int[] {nextFile, position++});
                    }
                }
                removed = Collections.singletonList(file == 1 ? F1 : "f" + file);
            } else if (op < 78) {
                // a snapshot that makes nothing new readable
                change = new Index();
            } else if (op < 88 && newest != covered) {
                // a catch-up over every snapshot published since the last one
                for (DerivedDvManager m : all) {
                    assertThat(m.ensureCovered(newest)).isTrue();
                }
                covered = newest;
                moves++;
                String expected =
                        model(
                                changes,
                                lakeAt.get(covered),
                                startAt.get(covered),
                                highWatermark.get());
                for (DerivedDvManager m : all) {
                    assertThat(describe(unionRead(m, covered))).isEqualTo(expected);
                }
            } else if (op < 94) {
                for (DerivedDvManager m : all) {
                    m.handOffFailed(new IllegalStateException("rebuild"));
                }
            } else {
                String expected =
                        model(
                                changes,
                                lakeAt.get(covered),
                                startAt.get(covered),
                                highWatermark.get());
                for (DerivedDvManager m : all) {
                    assertThat(describe(unionRead(m, covered))).isEqualTo(expected);
                }
            }
            if (change != null) {
                newest = 100 + nextFile;
                if (!removed.isEmpty()) {
                    src.replaced.put((long) nextFile, removed);
                }
                src.publish(
                        newest,
                        nextFile,
                        readableFrom,
                        dict(nextFile, "f" + nextFile),
                        change.build());
                src.readable = new long[] {newest, readableFrom};
                lakeAt.put(newest, new TreeMap<>(lake));
                startAt.put(newest, readableFrom);
                tiered = readableFrom;
                nextFile++;
            }
        }
        for (DerivedDvManager m : all) {
            assertThat(m.ensureCovered(newest)).isTrue();
        }
        highWatermark.set(logEnd);
        String expected = model(changes, lakeAt.get(newest), startAt.get(newest), logEnd);
        for (DerivedDvManager m : all) {
            assertThat(describe(unionRead(m, newest))).isEqualTo(expected);
        }
        assertThat(moves).isGreaterThan(15);
        // most moves kept the masks; a move right after a rebuild looks every key up
        assertThat(plain.targetedMoves()[0]).isGreaterThan(moves / 3);
        assertThat(tight.targetedMoves()[0]).isGreaterThan(moves / 3);
        assertThat(full.targetedMoves()[0]).isZero();
        for (DerivedDvManager m : all) {
            assertThat(m.targetedMoves()[1]).isZero();
        }
    }

    /**
     * A backlog of many keys caught up over many moves: each move looks up the keys its changes
     * name and the lake keys among them, not every lake key of the backlog, and serves the same
     * masks as a bucket that looks them all up again.
     */
    @Test
    void testTargetedMovesLookUpWhatTheyChange() throws Exception {
        DerivedDvManager targeted =
                newManager("cost-targeted", unbounded(), DerivedDvMetrics.noop());
        DerivedDvManager full = newManager("cost-full", unbounded(), DerivedDvMetrics.noop());
        full.setTargetedMoves(false);
        int keys = 100_000;
        Index initial = new Index();
        for (int i = 0; i < keys; i++) {
            initial.put(keyName(i), 1, i);
        }
        src.publish(100, 1, 10, dict(1, F1), initial.build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        targeted.recover(src);
        full.recover(src);
        // every key changes once, a fifth of them twice: the backlog of a stalled tiering
        Map<Integer, Long> last = new HashMap<>();
        List<long[]> changes = new ArrayList<>(); // {offset, key, version}
        long offset = 10;
        List<DvEntry> batch = new ArrayList<>();
        for (int pass = 0; pass < 2; pass++) {
            for (int i = 0; i < keys; i++) {
                if (pass == 1 && i % 5 != 0) {
                    continue;
                }
                Long previous = last.put(i, offset);
                batch.add(
                        new DvEntry(
                                key(keyName(i)),
                                offset,
                                offset,
                                false,
                                previous == null ? -1 : previous));
                changes.add(new long[] {offset, i, offset});
                offset++;
                if (batch.size() == 1000) {
                    DvTestManagers.handOver(targeted, batch);
                    DvTestManagers.handOver(full, batch);
                    batch = new ArrayList<>();
                }
            }
        }
        DvTestManagers.handOver(targeted, batch);
        DvTestManagers.handOver(full, batch);
        highWatermark.set(offset);
        unionRead(targeted, 100);
        unionRead(full, 100);

        // the lake catches up in ten rounds
        long tiered = 10;
        long step = (offset - 10) / 10 + 1;
        int nextFile = 2;
        long snapshot = 100;
        for (int round = 0; round < 10 && tiered < offset; round++) {
            long upTo = Math.min(offset, tiered + step);
            Map<Long, long[]> lastInRound = new TreeMap<>();
            for (long[] c : changes) {
                if (c[0] >= tiered && c[0] < upTo) {
                    lastInRound.put(c[1], c);
                }
            }
            Index change = new Index();
            int position = 0;
            for (long[] c : lastInRound.values()) {
                change.put(keyName((int) c[1]), nextFile, position++);
            }
            snapshot = 100 + nextFile;
            src.publish(snapshot, nextFile, upTo, dict(nextFile, "f" + nextFile), change.build());
            src.readable = new long[] {snapshot, upTo};
            nextFile++;
            tiered = upTo;

            long before = targeted.lookedUpKeys();
            long fullBefore = full.lookedUpKeys();
            assertThat(targeted.ensureCovered(snapshot)).isTrue();
            assertThat(full.ensureCovered(snapshot)).isTrue();
            String expected = describe(unionRead(full, snapshot));
            assertThat(describe(unionRead(targeted, snapshot))).isEqualTo(expected);
            long lookups = targeted.lookedUpKeys() - before;
            long fullLookups = full.lookedUpKeys() - fullBefore;
            // the keys the change names, the deleted ones, and the lake keys among them
            assertThat(lookups)
                    .as(
                            "round %d: %d keys named, %d lookups, %d for all",
                            round, lastInRound.size(), lookups, fullLookups)
                    .isLessThanOrEqualTo(2L * lastInRound.size());
            if (round < 4) {
                assertThat(lookups).isLessThan(fullLookups / 2);
            }
        }
        // a move whose change names more than half as many keys as the tail keeps looks every key
        // up instead: the last rounds, when little of the backlog is left
        assertThat(targeted.targetedMoves()[0]).isGreaterThanOrEqualTo(7);
        assertThat(targeted.targetedMoves()[1]).isZero();
    }

    /**
     * A catch-up that tiers everything the tail holds keeps nothing, so it looks every lake key up
     * again, which is none, instead of looking up the keys its change names.
     */
    @Test
    void testCatchUpOverTheWholeTailLooksUpNothing() throws Exception {
        int keys = 3 * DerivedDvManager.RESOLVE_CHUNK;
        Index initial = new Index();
        for (int i = 0; i < keys; i++) {
            initial.put(keyName(i), 1, i);
        }
        src.publish(100, 1, 10, dict(1, F1), initial.build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        DerivedDvManager m = newManager("catch-up-all", unbounded(), DerivedDvMetrics.noop());
        m.recover(src);
        unionRead(m, 100);
        // every key is updated once: its lake row is masked
        List<DvEntry> batch = new ArrayList<>();
        Index change = new Index();
        long offset = 10;
        for (int i = 0; i < keys; i++) {
            batch.add(new DvEntry(key(keyName(i)), offset, offset, false, -1));
            change.put(keyName(i), 2, i);
            offset++;
        }
        DvTestManagers.handOver(m, batch);
        highWatermark.set(offset);
        assertThat(lakeMasks(unionRead(m, 100)).get(F1)).hasSize(keys);
        // the next snapshot tiers all of them
        long before = m.lookedUpKeys();
        src.publish(101, 2, offset, dict(2, "f2"), change.build());
        src.readable = new long[] {101, offset};
        assertThat(m.ensureCovered(101)).isTrue();
        assertThat(lakeMasks(unionRead(m, 101))).isEmpty();
        assertThat(m.lookedUpKeys() - before).isZero();
        assertThat(m.targetedMoves()[0]).isZero();
    }

    /**
     * A change that inserted its key is not looked up in the lake, which has no row of the key.
     * Here the index has one anyway, so a lookup would show as a mask. A replay of the log marks
     * the insert the same way.
     */
    @Test
    void testInsertedKeysAreNotLookedUp() throws Exception {
        src.publish(100, 1, 10, dict(1, F1), new Index().put("a", 1, 0).put("b", 1, 1).build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        DerivedDvManager m = newManager("inserts", unbounded(), DerivedDvMetrics.noop());
        m.recover(src);
        change(m, "a", 11, true);
        change(m, "b", 12, false);
        highWatermark.set(13);
        assertThat(lakeMasks(unionRead(m, 100)))
                .isEqualTo(Collections.singletonMap(F1, Collections.singleton(1L)));

        m.handOffFailed(new IllegalStateException("replayed"));
        assertThat(lakeMasks(unionRead(m, 100)))
                .isEqualTo(Collections.singletonMap(F1, Collections.singleton(1L)));
    }

    /**
     * Buckets sharing a tablet server's budget keep their updates and deletes on disk. The budget
     * and the metrics count their heap only, which does not grow with the changes on disk, and the
     * metrics count the tails on disk and their bytes.
     */
    @Test
    void testBudgetCountsTheHeapNotTheTailOnDisk() throws Exception {
        DerivedDvMetrics metrics = DerivedDvMetrics.noop();
        DerivedDvHeapBudget budget = unbounded();
        List<DerivedDvManager> buckets = new ArrayList<>();
        List<FakeSource> sources = new ArrayList<>();
        for (int b = 0; b < 6; b++) {
            FakeSource s = new FakeSource(Files.createDirectories(tempDir.resolve("remote-" + b)));
            s.publish(100, 1, 10, dict(1, F1), new Index().put("a0", 1, 0).build());
            s.readable = new long[] {100, 10};
            DerivedDvManager m = newManager("bucket-" + b, budget, metrics);
            highWatermark.set(10);
            m.recover(s);
            buckets.add(m);
            sources.add(s);
        }
        highWatermark.set(10_000);
        for (int b = 0; b < 6; b++) {
            int keys = 100 * (b + 1);
            // in batches, as the write path hands them over, each applied before the next
            for (int from = 0; from < keys; from += 20) {
                List<DvEntry> entries = new ArrayList<>();
                for (int i = from; i < from + 20; i++) {
                    sources.get(b).append("a" + i, 10 + i, 10 + i);
                    entries.add(new DvEntry(key("a" + i), 10 + i, 10 + i, false, -1));
                }
                DvTestManagers.handOver(buckets.get(b), entries);
                unionRead(buckets.get(b), 100);
            }
        }
        for (int b = 0; b < 6; b++) {
            DvSnapshot masks = unionRead(buckets.get(b), 100);
            // a0's lake row is masked, and nothing of the log is superseded
            assertThat(lakeMasks(masks)).containsOnlyKeys(F1);
            assertThat(masks.getLogDvBitmap()).isNull();
        }
        // six times the changes on disk, the same heap
        assertThat(buckets.get(5).tailMemoryBytes()).isEqualTo(buckets.get(0).tailMemoryBytes());
        assertThat(metrics.changeListBytes()).isPositive();
        long sum = 0;
        for (DerivedDvManager m : buckets) {
            sum += m.tailMemoryBytes();
        }
        assertThat(metrics.heapBytes()).isEqualTo(sum).isEqualTo(budget.totalBytes());
    }

    /**
     * A tail advances without a replay: it drops the changes the next snapshot tiered with one
     * range delete and looks up the rest from disk. Its masks are those of a tail rebuilt on the
     * new snapshot.
     */
    @Test
    void testAdvanceWithoutAReplay() throws Exception {
        DerivedDvManager advanced = newManager("advanced", unbounded(), DerivedDvMetrics.noop());
        src.publish(100, 1, 10, dict(1, F1), new Index().put("k1", 1, 0).put("k2", 1, 1).build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        advanced.recover(src);
        for (long offset = 10; offset < 20; offset++) {
            write(advanced, "k" + (offset % 3), offset);
        }
        highWatermark.set(20);
        unionRead(advanced, 100);

        src.publish(200, 2, 15, dict(2, "f2"), new Index().put("k1", 2, 0).build());
        src.readable = new long[] {200, 15};
        assertThat(advanced.ensureCovered(200)).isTrue();
        DvSnapshot masks = unionRead(advanced, 200);
        // only the recovery replayed the log
        assertThat(src.replayedFrom).containsExactly(10L);

        DerivedDvManager rebuilt = newManager("rebuilt", unbounded(), DerivedDvMetrics.noop());
        rebuilt.recover(src);
        assertThat(describe(masks)).isEqualTo(describe(unionRead(rebuilt, 200)));
        assertThat(src.replayedFrom).containsExactly(10L, 15L);
    }

    /**
     * A tail whose masks are new, after a rebuild or a move that does not keep them, reads from
     * disk only the changes applied by then. The keys of later changes reach the masks as they are
     * applied, so each key is looked up once, and masks are served without reading the whole tail
     * again. Here the rebuild installs an empty tail, and the move leaves half of a round on disk
     * while the next round is written.
     */
    @Test
    void testTailLooksUpEachKeyOnce() throws Exception {
        int keys = 3 * DerivedDvManager.RESOLVE_CHUNK;
        int half = keys / 2;
        DerivedDvManager m = newManager("once", unbounded(), DerivedDvMetrics.noop());
        // a move looks up every lake key of the tail again, from disk
        m.setTargetedMoves(false);
        Index initial = new Index();
        for (int i = 0; i < keys; i++) {
            initial.put(keyName(i), 1, i);
        }
        src.publish(100, 1, 10, dict(1, F1), initial.build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        m.recover(src);

        // round 1 updates every key, which masks its lake row; the resolvers apply it and look up
        // its keys in the background before the first read, as during a burst of writes
        List<DvEntry> round1 = new ArrayList<>();
        for (int i = 0; i < keys; i++) {
            round1.add(new DvEntry(key(keyName(i)), 10 + i, 10 + i, false, -1));
        }
        highWatermark.set(10 + keys);
        DvTestManagers.handOver(m, round1);
        awaitResolved(m);
        assertThat(lakeMasks(unionRead(m, 100)).get(F1)).hasSize(keys);
        assertThat(m.lookedUpKeys()).isEqualTo(keys);

        // snapshot 101 tiers the first half of round 1; the second half is looked up again
        Index change = new Index();
        for (int i = 0; i < half; i++) {
            change.put(keyName(i), 2, i);
        }
        src.publish(101, 2, 10 + half, dict(2, "f2"), change.build());
        src.readable = new long[] {101, 10 + half};
        assertThat(m.ensureCovered(101)).isTrue();
        // round 2 updates every key again: the first half masks rows of file 2, the second half
        // versions in the log
        List<DvEntry> round2 = new ArrayList<>();
        for (int i = 0; i < keys; i++) {
            long offset = 10 + keys + i;
            round2.add(new DvEntry(key(keyName(i)), offset, offset, false, 10 + i));
        }
        highWatermark.set(10 + 2L * keys);
        DvTestManagers.handOver(m, round2);
        awaitResolved(m);
        assertThat(lakeMasks(unionRead(m, 101)).get("f2")).hasSize(half);
        assertThat(lakeMasks(unionRead(m, 101)).get(F1)).hasSize(keys - half);
        assertThat(m.lookedUpKeys()).isEqualTo(2L * keys);
    }

    /**
     * An advance keeps the changes after the new snapshot's offset and looks up again the keys
     * whose superseded version it holds, at their positions in it. A change whose superseded
     * version the new snapshot tiered masks its lake row; one whose superseded version is after the
     * snapshot masks that version in the log.
     */
    @Test
    void testAdvanceDerivesTheMasksOfTheRemainingChanges() throws Exception {
        DerivedDvManager m = newManager("advance", unbounded(), DerivedDvMetrics.noop());
        Index initial = new Index();
        for (int i = 0; i < 40; i++) {
            initial.put("k" + i, 1, i);
        }
        src.publish(100, 1, 10, dict(1, F1), initial.build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        m.recover(src);
        for (int i = 0; i < 30; i++) {
            write(m, "k" + i, 10 + i);
        }
        highWatermark.set(40);
        Set<Long> all = new TreeSet<>();
        for (long i = 0; i < 30; i++) {
            all.add(i);
        }
        assertThat(lakeMasks(unionRead(m, 100))).isEqualTo(Collections.singletonMap(F1, all));

        // snapshot 200 tiers [10, 30): k0..k19 move to file 2
        Index change = new Index();
        for (int i = 0; i < 20; i++) {
            change.put("k" + i, 2, i);
        }
        src.publish(200, 2, 30, dict(2, "f2"), change.build());
        src.readable = new long[] {200, 30};
        assertThat(m.ensureCovered(200)).isTrue();
        // k3 supersedes its version in the tiered log, k25 one after it, k35 one in file 1
        write(m, "k3", 40);
        write(m, "k25", 41);
        write(m, "k35", 42);
        highWatermark.set(43);

        DvSnapshot masks = unionRead(m, 200);
        Map<String, Set<Long>> expected = new TreeMap<>();
        Set<Long> f1 = new TreeSet<>();
        for (long i = 20; i < 30; i++) {
            f1.add(i);
        }
        f1.add(35L);
        expected.put(F1, f1);
        expected.put("f2", Collections.singleton(3L));
        assertThat(new TreeMap<>(lakeMasks(masks))).isEqualTo(expected);
        assertThat(offsets(masks.getLogDvBitmap())).containsExactly(35L);
        assertThat(masks.getSnapshotStartOffset()).isEqualTo(30L);
        assertThat(src.replayedFrom).containsExactly(10L);
    }

    /**
     * A replay that outgrows the budget moves its map of last versions to disk while it replays;
     * its masks are the same.
     */
    @Test
    void testReplayMapSpillsDuringReplay() throws Exception {
        DerivedDvManager reference = newManager("reference", unbounded(), DerivedDvMetrics.noop());
        DerivedDvMetrics metrics = DerivedDvMetrics.noop();
        DerivedDvManager spilled =
                newManager(
                        "spilled", new DerivedDvHeapBudget(1, DvTestManagers.executors()), metrics);
        src.publish(100, 1, 10, dict(1, F1), new Index().put("k5", 1, 0).build());
        src.readable = new long[] {100, 10};
        int changes = 30_000;
        for (int i = 0; i < changes; i++) {
            src.log.add(new DerivedDvManagerTest.Change(key("k" + (i % 5_000)), 10 + i, 10 + i));
        }
        highWatermark.set(10 + changes);
        // pause the replay half-way, so the budget asks it to spill
        java.util.concurrent.CountDownLatch gate = new java.util.concurrent.CountDownLatch(1);
        src.gateAfterChanges = changes / 2;
        src.replayGate = gate;
        java.util.concurrent.CompletableFuture<Void> recovery =
                java.util.concurrent.CompletableFuture.runAsync(
                        () -> {
                            try {
                                spilled.recover(src);
                            } catch (Exception e) {
                                throw new java.util.concurrent.CompletionException(e);
                            }
                        });
        assertThat(src.replayPaused.await(30, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        Thread.sleep(300);
        gate.countDown();
        recovery.get(60, java.util.concurrent.TimeUnit.SECONDS);
        reference.recover(src);

        // the installed tail holds its changes on disk
        assertThat(describe(unionRead(spilled, 100)))
                .isEqualTo(describe(unionRead(reference, 100)));
    }

    /**
     * A tail writes its updates and deletes to the bucket's RocksDB as they are applied, also a
     * tail a rebuild replays from the log: they take no heap however many there are. Its masks are
     * those of a rebuilt tail, before and after a move to a newer snapshot.
     */
    @Test
    void testTailKeepsItsChangesOffTheHeap() throws Exception {
        DerivedDvMetrics metrics = DerivedDvMetrics.noop();
        DerivedDvManager m = newManager("disk", unbounded(), metrics);
        Index initial = new Index();
        for (int i = 0; i < 100; i++) {
            initial.put("k" + i, 1, i);
        }
        src.publish(100, 1, 10, dict(1, F1), initial.build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        m.recover(src);
        unionRead(m, 100);
        long empty = m.tailMemoryBytes();
        // more changes than a replay applies at once, most keys several times
        int changes = 10_000;
        for (int i = 0; i < changes; i++) {
            write(m, "k" + (i % 100), 10 + i);
        }
        highWatermark.set(10 + changes);
        String expected = describe(unionRead(m, 100));
        // the lake mask of 100 keys and the log mask of the superseded versions, not the changes
        assertThat(m.tailMemoryBytes() - empty).isLessThan(64L * 1024);
        assertThat(metrics.changeListBytes()).isGreaterThan(changes * 16L);

        // a rebuild replays the log into a tail on disk
        DerivedDvManager rebuilt = newManager("rebuilt", unbounded(), DerivedDvMetrics.noop());
        rebuilt.recover(src);
        assertThat(describe(unionRead(rebuilt, 100))).isEqualTo(expected);

        // snapshot 200 tiers the first half of the changes: one range delete, the rest read back
        long half = 10 + changes / 2;
        Index change = new Index();
        for (int i = 0; i < 100; i++) {
            change.put("k" + i, 2, i);
        }
        src.publish(200, 2, half, dict(2, "f2"), change.build());
        src.readable = new long[] {200, half};
        assertThat(m.ensureCovered(200)).isTrue();
        assertThat(rebuilt.ensureCovered(200)).isTrue();
        String advanced = describe(unionRead(m, 200));
        assertThat(describe(unionRead(rebuilt, 200))).isEqualTo(advanced);
        DerivedDvManager rebuiltAt200 =
                newManager("rebuilt-200", unbounded(), DerivedDvMetrics.noop());
        rebuiltAt200.recover(src);
        assertThat(describe(unionRead(rebuiltAt200, 200))).isEqualTo(advanced);
    }

    /** Closing a bucket whose tail holds changes leaves nothing in the budget or the metrics. */
    @Test
    void testCloseLeavesNothingBehind() throws Exception {
        DerivedDvMetrics metrics = DerivedDvMetrics.noop();
        DerivedDvHeapBudget budget = unbounded();
        DerivedDvManager bucket = newManager("bucket", budget, metrics);
        src.publish(100, 1, 10, dict(1, F1), new Index().put("k1", 1, 0).build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        bucket.recover(src);
        for (long offset = 10; offset < 1010; offset++) {
            src.append("k" + offset, offset, offset);
        }
        List<DvEntry> entries = new ArrayList<>();
        for (long offset = 10; offset < 1010; offset++) {
            entries.add(new DvEntry(key("k" + offset), offset, offset, false, -1));
        }
        highWatermark.set(1010);
        DvTestManagers.handOver(bucket, entries);
        unionRead(bucket, 100);

        long start = System.nanoTime();
        bucket.close();
        assertThat((System.nanoTime() - start) / 1_000_000).isLessThan(5_000L);
        assertThat(budget.totalBytes()).isZero();
        assertThat(metrics.heapBytes()).isZero();
        assertThat(metrics.changeListBytes()).isZero();
        // the tail's changes are gone with the RocksDB, which clears them when it opens again
        try (DvRocksDB reopened = DvRocksDB.open(tempDir.resolve("bucket").toString());
                DvChangeListStore.PendingCursor cursor =
                        reopened.changeListStore().pendingCursor(1, null, 0, Long.MAX_VALUE)) {
            assertThat(cursor.nextLakeKeys(10, Long.MAX_VALUE, new ArrayList<>())).isFalse();
        }
    }

    /**
     * A tail whose disk fails is dropped. Union reads are not ready, and retriable, until the tail
     * is replayed from the log, and then serve the same masks.
     */
    @Test
    void testDiskFailureOfTheTail() throws Exception {
        DerivedDvManager reference = newManager("reference", unbounded(), DerivedDvMetrics.noop());
        DvRocksDB bucketDb = DvRocksDB.open(tempDir.resolve("bucket").toString());
        DerivedDvManager bucket =
                new DerivedDvManager(
                        bucketDb,
                        highWatermark::get,
                        DvTestManagers.CHECKPOINT_INTERVAL,
                        DerivedDvMetrics.noop(),
                        unbounded(),
                        DvTestManagers.executors(),
                        DvTestManagers.executors().bucketState(),
                        new TestingDvHooks());
        bucket.setMaskCacheMillis(0);
        managers.add(bucket);
        src.publish(100, 1, 10, dict(1, F1), new Index().put("k1", 1, 0).build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        reference.recover(src);
        bucket.recover(src);
        write(reference, bucket, "k1", 10);
        highWatermark.set(11);
        unionRead(bucket, 100);

        hooks(bucket).failChangeListWrites(new IOException("disk full"));
        write(reference, bucket, "k2", 11);
        highWatermark.set(12);
        assertThatThrownBy(() -> DvTestReads.unionRead(bucket, 100))
                .isInstanceOf(DeletionVectorsNotReadyException.class)
                .hasMessageContaining("disk full");

        hooks(bucket).failChangeListWrites(null);
        assertThat(describe(unionRead(bucket, 100))).isEqualTo(describe(unionRead(reference, 100)));
    }

    /**
     * The estimated bytes of the changes waiting for the high watermark, which stay on the heap,
     * match what the heap holds for them.
     */
    @Test
    void testQueuedChangesEstimateMatchesTheHeap() throws Exception {
        int keys = 400_000;
        src.publish(100, 1, 10, dict(1, F1), new Index().put("x", 1, 0).build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        DerivedDvManager m = newManager("heap", unbounded(), DerivedDvMetrics.noop());
        m.recover(src);
        unionRead(m, 100);
        long empty = m.tailMemoryBytes();
        long before = usedHeapAfterGc();
        List<DvEntry> entries = new ArrayList<>(keys);
        for (int i = 0; i < keys; i++) {
            // primary keys of 12 to 20 bytes
            entries.add(
                    new DvEntry(
                            key(String.format("key-%08d-%d", i, i % 1000)),
                            10 + i,
                            10 + i,
                            false,
                            -1));
        }
        // the high watermark does not pass them: they wait in the queue
        DvTestManagers.handOver(m, entries);
        entries = null;
        long heap = usedHeapAfterGc() - before;
        long estimate = m.tailMemoryBytes() - empty;
        assertThat(m.queuedChanges()).isEqualTo(keys);
        assertThat((double) estimate)
                .as("estimated %s bytes, heap %s bytes", estimate, heap)
                .isBetween(heap * 0.75, heap * 1.25);
    }

    private static String keyName(int i) {
        return String.format("key-%08d", i);
    }

    // ------------------------------------------------------------------------------------------

    private void change(DerivedDvManager m, String k, long offset, boolean insert) {
        src.append(k, offset, offset, insert);
        DvTestManagers.handOver(
                m, Collections.singletonList(new DvEntry(key(k), offset, offset, insert, -1)));
    }

    /** Waits until the background resolvers applied every queued change and looked it up. */
    private static void awaitResolved(DerivedDvManager m) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 60_000;
        int quiet = 0;
        // quiet twice in a row: a resolve takes its keys in the owner step that applies them
        while (quiet < 2) {
            assertThat(System.currentTimeMillis()).as("%s", m.inspect()).isLessThan(deadline);
            quiet = m.queuedChanges() == 0 && !m.resolving() ? quiet + 1 : 0;
            Thread.sleep(20);
        }
    }

    private void write(DerivedDvManager m, String k, long offset) {
        List<DvEntry> e = Collections.singletonList(src.written(k, offset, offset));
        src.append(k, offset, offset);
        DvTestManagers.handOver(m, e);
    }

    private void write(DerivedDvManager a, DerivedDvManager b, String k, long offset) {
        List<DvEntry> e = Collections.singletonList(src.written(k, offset, offset));
        src.append(k, offset, offset);
        DvTestManagers.handOver(a, e);
        DvTestManagers.handOver(b, e);
    }

    private DerivedDvManager newManager(
            String dir, DerivedDvHeapBudget budget, DerivedDvMetrics metrics) throws IOException {
        DerivedDvManager m =
                new DerivedDvManager(
                        DvRocksDB.open(tempDir.resolve(dir).toString()),
                        highWatermark::get,
                        DvTestManagers.CHECKPOINT_INTERVAL,
                        metrics,
                        budget,
                        DvTestManagers.executors());
        m.setMaskCacheMillis(0);
        managers.add(m);
        return m;
    }

    private static DerivedDvHeapBudget unbounded() {
        return new DerivedDvHeapBudget(Long.MAX_VALUE, DvTestManagers.executors());
    }

    private static DvSnapshot unionRead(DerivedDvManager m, long snapshotId) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        while (true) {
            try {
                return DvTestReads.unionRead(m, snapshotId);
            } catch (RetriableException e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(10);
            }
        }
    }

    /**
     * The masks of the lake snapshot readable from {@code start}, up to {@code hw}, from a model:
     * {@code changes} as {offset, key, version or -1} in log order, and the lake rows of the
     * snapshot as key to {file id, row position}.
     */
    static String model(List<long[]> changes, Map<Integer, int[]> lake, long start, long hw) {
        Map<String, Set<Long>> lakeMask = new TreeMap<>();
        Set<Long> logMask = new TreeSet<>();
        Map<Long, Long> liveVersion = new HashMap<>();
        for (long[] c : changes) {
            if (c[0] < start || c[0] >= hw) {
                continue;
            }
            if (!liveVersion.containsKey(c[1])) {
                int[] row = lake.get((int) c[1]);
                if (row != null) {
                    lakeMask.computeIfAbsent(row[0] == 1 ? F1 : "f" + row[0], f -> new TreeSet<>())
                            .add((long) row[1]);
                }
            }
            Long previous = liveVersion.put(c[1], c[2]);
            if (previous != null && previous >= 0) {
                logMask.add(previous);
            }
        }
        Map<String, Object> d = new TreeMap<>();
        d.put("lake", lakeMask);
        d.put("log", logMask);
        d.put("end", hw);
        d.put("start", start);
        return d.toString();
    }

    static String describe(DvSnapshot s) throws IOException {
        Map<String, Object> d = new TreeMap<>();
        d.put("lake", new TreeMap<>(lakeMasks(s)));
        d.put("log", offsets(s.getLogDvBitmap()));
        d.put("end", s.getLogEndOffset());
        d.put("start", s.getSnapshotStartOffset());
        return d.toString();
    }

    private static long usedHeapAfterGc() throws InterruptedException {
        MemoryMXBean bean = ManagementFactory.getMemoryMXBean();
        for (int i = 0; i < 4; i++) {
            System.gc();
            Thread.sleep(50);
        }
        return bean.getHeapMemoryUsage().getUsed();
    }

    private static void waitUntil(java.util.function.BooleanSupplier condition)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }
}
