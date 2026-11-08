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
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
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
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.dict;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.key;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.lakeMasks;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.offsets;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Tests the tablet server's heap budget for log tails and the tails spilled to disk once it is
 * exceeded. A spilled tail serves the same masks as an in-memory one.
 */
class DerivedDvTailSpillTest {

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
     * The same changes, high watermark moves, snapshot advances and rebuilds, applied to a tail
     * held in memory, to one spilled to disk again and again, to one spilled at random and to one
     * whose replays keep their map of last versions on disk, give the same masks: those of a model
     * of the log and the lake. A change names the offset of the version it supersedes, as the KV
     * value's tag gives it. Changes of keys without a live version are inserts, as on the KV write
     * path, so their positions are not looked up.
     */
    @ParameterizedTest
    @ValueSource(longs = {1, 7, 42})
    void testSpilledTailServesTheSameMasks(long seed) throws Exception {
        DerivedDvManager memory = newManager("memory", unbounded(), DerivedDvMetrics.noop());
        DerivedDvManager spilled = newManager("spilled", unbounded(), DerivedDvMetrics.noop());
        DerivedDvManager spilledToo =
                newManager("spilled-too", unbounded(), DerivedDvMetrics.noop());
        // its replays keep the map of last versions on disk
        DerivedDvManager spilledReplay =
                newManager("spilled-replay", unbounded(), DerivedDvMetrics.noop());
        spilledReplay.setSpillReplayVersions(true);
        List<DerivedDvManager> all =
                java.util.Arrays.asList(memory, spilled, spilledToo, spilledReplay);
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
        int spilledCompares = 0;
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
                    m.handleChangelogSynced(
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
            } else if (op < 92) {
                spilled.spillTail();
            } else {
                if (random.nextBoolean()) {
                    spilled.spillTail();
                }
                if (random.nextBoolean()) {
                    spilledToo.spillTail();
                }
                boolean wasSpilled = spilled.tailSpilled();
                String expected = describe(unionRead(memory, snapshot));
                assertThat(expected).isEqualTo(model(changes, lake, tiered, highWatermark.get()));
                assertThat(describe(unionRead(spilled, snapshot))).isEqualTo(expected);
                assertThat(describe(unionRead(spilledToo, snapshot))).isEqualTo(expected);
                assertThat(describe(unionRead(spilledReplay, snapshot))).isEqualTo(expected);
                compares++;
                if (wasSpilled) {
                    spilledCompares++;
                }
            }
        }
        spilled.spillTail();
        highWatermark.set(logEnd);
        String expected = describe(unionRead(memory, snapshot));
        assertThat(expected).isEqualTo(model(changes, lake, tiered, logEnd));
        assertThat(describe(unionRead(spilled, snapshot))).isEqualTo(expected);
        assertThat(describe(unionRead(spilledToo, snapshot))).isEqualTo(expected);
        assertThat(describe(unionRead(spilledReplay, snapshot))).isEqualTo(expected);
        assertThat(compares).isGreaterThan(20);
        assertThat(spilledCompares).isGreaterThan(5);
    }

    /**
     * A move to a newer snapshot keeps the masks of the keys its index changes do not name and
     * derives the others again. Random histories of updates, deletes and re-inserts, with catch-ups
     * over several snapshots at once, rewrites of whole files, compactions that move rows of some
     * keys without removing their file, and snapshots that make nothing new readable: after every
     * move, the masks of a tail in memory, one spilled again and again and one whose replays spill
     * their map equal those of a model of the log and the lake, and those of a bucket that looks up
     * every lake key again on each move.
     */
    @ParameterizedTest
    @ValueSource(longs = {3, 11, 29, 57, 101})
    void testTargetedMovesServeTheMasksOfTheModel(long seed) throws Exception {
        DerivedDvManager memory = newManager("t-memory", unbounded(), DerivedDvMetrics.noop());
        DerivedDvManager spilled = newManager("t-spilled", unbounded(), DerivedDvMetrics.noop());
        DerivedDvManager spilledReplay =
                newManager("t-spilled-replay", unbounded(), DerivedDvMetrics.noop());
        spilledReplay.setSpillReplayVersions(true);
        DerivedDvManager full = newManager("t-full", unbounded(), DerivedDvMetrics.noop());
        full.setTargetedMoves(false);
        List<DerivedDvManager> all = java.util.Arrays.asList(memory, spilled, spilledReplay, full);
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
                    m.handleChangelogSynced(
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
            } else if (op < 92) {
                spilled.spillTail();
                if (random.nextBoolean()) {
                    spilledReplay.spillTail();
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
        // most moves kept the masks; a move right after a rebuild or a spill looks every key up
        assertThat(memory.targetedMoves()[0]).isGreaterThan(moves / 3);
        assertThat(spilled.targetedMoves()[0]).isGreaterThan(3);
        assertThat(full.targetedMoves()[0]).isZero();
        for (DerivedDvManager m : all) {
            assertThat(m.targetedMoves()[1]).isZero();
        }
    }

    /**
     * A spilled backlog of many keys caught up over many moves: each move looks up the keys its
     * changes name and the lake keys among them, not every lake key of the backlog, and serves the
     * same masks as a bucket that looks them all up again.
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
            initial.put(benchKey(i), 1, i);
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
                                key(benchKey(i)),
                                offset,
                                offset,
                                false,
                                previous == null ? -1 : previous));
                changes.add(new long[] {offset, i, offset});
                offset++;
                if (batch.size() == 1000) {
                    targeted.handleChangelogSynced(batch);
                    full.handleChangelogSynced(batch);
                    batch = new ArrayList<>();
                }
            }
        }
        targeted.handleChangelogSynced(batch);
        full.handleChangelogSynced(batch);
        highWatermark.set(offset);
        unionRead(targeted, 100);
        unionRead(full, 100);
        assertThat(targeted.spillTail()).isTrue();
        assertThat(full.spillTail()).isTrue();

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
                change.put(benchKey((int) c[1]), nextFile, position++);
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
        assertThat(targeted.tailSpilled()).isTrue();
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
            initial.put(benchKey(i), 1, i);
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
            batch.add(new DvEntry(key(benchKey(i)), offset, offset, false, -1));
            change.put(benchKey(i), 2, i);
            offset++;
        }
        m.handleChangelogSynced(batch);
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
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testInsertedKeysAreNotLookedUp(boolean spill) throws Exception {
        src.publish(100, 1, 10, dict(1, F1), new Index().put("a", 1, 0).put("b", 1, 1).build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        DerivedDvManager m = newManager("inserts-" + spill, unbounded(), DerivedDvMetrics.noop());
        m.recover(src);
        if (spill) {
            // an update, as inserts leave nothing to spill
            change(m, "c", 10, false);
            highWatermark.set(11);
            unionRead(m, 100);
            assertThat(m.spillTail()).isTrue();
        }
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
     * Buckets sharing a tablet server's budget: the largest tails spill until the rest fits, and
     * every bucket keeps serving its masks.
     */
    @Test
    void testBudgetSpillsTheLargestTails() throws Exception {
        DerivedDvMetrics metrics = DerivedDvMetrics.noop();
        DerivedDvTailBudget roomy = unbounded();
        // fits about 350 of the 6 buckets' 2,100 changes
        DerivedDvTailBudget budget = new DerivedDvTailBudget(150 * 160L);
        List<DerivedDvManager> buckets = new ArrayList<>();
        List<FakeSource> sources = new ArrayList<>();
        for (int b = 0; b < 6; b++) {
            FakeSource s = new FakeSource(Files.createDirectories(tempDir.resolve("remote-" + b)));
            s.publish(100, 1, 10, dict(1, F1), new Index().put("a0", 1, 0).build());
            s.readable = new long[] {100, 10};
            DerivedDvManager m = newManager("bucket-" + b, roomy, metrics);
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
                    entries.add(new DvEntry(key("a" + i), 10 + i, 10 + i));
                }
                buckets.get(b).handleChangelogSynced(entries);
                unionRead(buckets.get(b), 100);
            }
        }
        assertThat(roomy.totalBytes()).isGreaterThan(budget.limitBytes());
        // the tablet server's budget shrinks below the tails: the largest spill
        for (DerivedDvManager m : buckets) {
            m.useTailBudget(budget);
        }
        long deadline = System.currentTimeMillis() + 30_000;
        while (budget.totalBytes() > budget.limitBytes() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        StringBuilder state = new StringBuilder();
        for (DerivedDvManager m : buckets) {
            state.append(m.tailMemoryBytes()).append(m.tailSpilled() ? "s " : "m ");
        }
        assertThat(budget.totalBytes())
                .as(state.toString())
                .isLessThanOrEqualTo(budget.limitBytes());

        // the largest spilled, the smallest kept in memory
        assertThat(buckets.get(5).tailSpilled()).isTrue();
        assertThat(buckets.get(4).tailSpilled()).isTrue();
        assertThat(buckets.get(0).tailSpilled()).as(state.toString()).isFalse();
        for (int b = 0; b < 6; b++) {
            DvSnapshot masks = unionRead(buckets.get(b), 100);
            // a0's lake row is masked, and nothing of the log is superseded
            assertThat(lakeMasks(masks)).containsOnlyKeys(F1);
            assertThat(masks.getLogDvBitmap()).isNull();
        }
        assertThat(metrics.spilledTails()).isGreaterThanOrEqualTo(2);
        assertThat(metrics.spilledTailBytes()).isPositive();
        long sum = 0;
        for (DerivedDvManager m : buckets) {
            sum += m.tailMemoryBytes();
        }
        assertThat(metrics.tailMemoryBytes()).isEqualTo(sum).isEqualTo(budget.totalBytes());
        assertThat(budget.totalBytes()).isLessThanOrEqualTo(budget.limitBytes());
    }

    /**
     * A spilled tail advances without a replay: it drops the changes the next snapshot tiered with
     * one range delete, looks up the rest from disk, and stays on disk.
     */
    @Test
    void testAdvanceAfterSpill() throws Exception {
        DerivedDvManager memory = newManager("memory", unbounded(), DerivedDvMetrics.noop());
        DerivedDvManager spilled = newManager("spilled", unbounded(), DerivedDvMetrics.noop());
        src.publish(100, 1, 10, dict(1, F1), new Index().put("k1", 1, 0).put("k2", 1, 1).build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        memory.recover(src);
        spilled.recover(src);
        for (long offset = 10; offset < 20; offset++) {
            String k = "k" + (offset % 3);
            List<DvEntry> e = Collections.singletonList(src.written(k, offset, offset));
            src.append(k, offset, offset);
            memory.handleChangelogSynced(e);
            spilled.handleChangelogSynced(e);
        }
        highWatermark.set(20);
        assertThat(spilled.spillTail()).isTrue();
        assertThat(describe(unionRead(spilled, 100))).isEqualTo(describe(unionRead(memory, 100)));

        src.publish(200, 2, 15, dict(2, "f2"), new Index().put("k1", 2, 0).build());
        src.readable = new long[] {200, 15};
        assertThat(spilled.ensureCovered(200)).isTrue();
        assertThat(memory.ensureCovered(200)).isTrue();
        assertThat(describe(unionRead(spilled, 200))).isEqualTo(describe(unionRead(memory, 200)));
        assertThat(spilled.tailSpilled()).isTrue();
        // only the recoveries replayed the log
        assertThat(src.replayedFrom).containsExactly(10L, 10L);
        // the changes [15, 20) are left
        assertThat(memory.stateSummary()).contains("tailChanges=5");
    }

    /**
     * An advance keeps the changes after the new snapshot's offset and looks up again the keys
     * whose superseded version it holds, at their positions in it. A change whose superseded
     * version the new snapshot tiered masks its lake row; one whose superseded version is after the
     * snapshot masks that version in the log. The same for a spilled tail.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testAdvanceDerivesTheMasksOfTheRemainingChanges(boolean spill) throws Exception {
        DerivedDvManager m = newManager("advance-" + spill, unbounded(), DerivedDvMetrics.noop());
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
        if (spill) {
            assertThat(m.spillTail()).isTrue();
        }

        // snapshot 200 tiers [10, 30): k0..k19 move to file 2
        Index change = new Index();
        for (int i = 0; i < 20; i++) {
            change.put("k" + i, 2, i);
        }
        src.publish(200, 2, 30, dict(2, "f2"), change.build());
        src.readable = new long[] {200, 30};
        assertThat(m.ensureCovered(200)).isTrue();
        if (!spill) {
            // a spilled tail counts the changes it dropped by whole runs
            assertThat(m.stateSummary()).contains("tailChanges=10");
        }
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
        assertThat(m.tailSpilled()).isEqualTo(spill);
        assertThat(src.replayedFrom).containsExactly(10L);
    }

    /** A replay that outgrows the budget spills while it replays; its masks are the same. */
    @Test
    void testSpillDuringReplay() throws Exception {
        DerivedDvManager memory = newManager("memory", unbounded(), DerivedDvMetrics.noop());
        DerivedDvMetrics metrics = DerivedDvMetrics.noop();
        DerivedDvManager spilled = newManager("spilled", new DerivedDvTailBudget(1), metrics);
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
        memory.recover(src);

        // spilled while replaying: installed spilled
        assertThat(spilled.tailSpilled()).isTrue();
        assertThat(metrics.spilledTails()).isEqualTo(1);
        assertThat(describe(unionRead(spilled, 100))).isEqualTo(describe(unionRead(memory, 100)));
    }

    /** Closing a bucket whose tail is spilled leaves nothing in the budget or the metrics. */
    @Test
    void testCloseWhileSpilled() throws Exception {
        DerivedDvMetrics metrics = DerivedDvMetrics.noop();
        DerivedDvTailBudget budget = unbounded();
        DerivedDvManager spilled = newManager("spilled", budget, metrics);
        src.publish(100, 1, 10, dict(1, F1), new Index().put("k1", 1, 0).build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        spilled.recover(src);
        for (long offset = 10; offset < 1010; offset++) {
            src.append("k" + offset, offset, offset);
        }
        List<DvEntry> entries = new ArrayList<>();
        for (long offset = 10; offset < 1010; offset++) {
            entries.add(new DvEntry(key("k" + offset), offset, offset));
        }
        highWatermark.set(1010);
        spilled.handleChangelogSynced(entries);
        unionRead(spilled, 100);
        assertThat(spilled.spillTail()).isTrue();
        assertThat(metrics.spilledTails()).isEqualTo(1);

        long start = System.nanoTime();
        spilled.close();
        assertThat((System.nanoTime() - start) / 1_000_000).isLessThan(5_000L);
        assertThat(budget.totalBytes()).isZero();
        assertThat(metrics.tailMemoryBytes()).isZero();
        assertThat(metrics.spilledTails()).isZero();
        assertThat(metrics.spilledTailBytes()).isZero();
        // the spilled changes are gone with the RocksDB, which clears them when it opens again
        try (DvRocksDB reopened = DvRocksDB.open(tempDir.resolve("spilled").toString());
                TailStore.PendingCursor cursor = reopened.tailStore().pendingCursor(1)) {
            assertThat(cursor.nextLakeKeys(10, Long.MAX_VALUE, new ArrayList<>())).isFalse();
        }
    }

    /**
     * A spilled tail whose disk fails is dropped. Union reads are not ready, and retriable, until
     * the tail is replayed from the log, and then serve the same masks.
     */
    @Test
    void testDiskFailureOfASpilledTail() throws Exception {
        DerivedDvManager memory = newManager("memory", unbounded(), DerivedDvMetrics.noop());
        DvRocksDB spilledDb = DvRocksDB.open(tempDir.resolve("spilled").toString());
        DerivedDvManager spilled =
                new DerivedDvManager(
                        spilledDb,
                        highWatermark::get,
                        DerivedDvManager.DEFAULT_CHECKPOINT_INTERVAL,
                        DerivedDvMetrics.noop(),
                        unbounded());
        spilled.setMaskCacheMillis(0);
        managers.add(spilled);
        src.publish(100, 1, 10, dict(1, F1), new Index().put("k1", 1, 0).build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        memory.recover(src);
        spilled.recover(src);
        write(memory, spilled, "k1", 10);
        highWatermark.set(11);
        assertThat(spilled.spillTail()).isTrue();
        unionRead(spilled, 100);

        spilledDb.tailStore().failWrites(new IOException("disk full"));
        write(memory, spilled, "k2", 11);
        highWatermark.set(12);
        assertThatThrownBy(() -> spilled.getDvForUnionRead(100))
                .isInstanceOf(DeletionVectorsNotReadyException.class)
                .hasMessageContaining("disk full");
        assertThat(spilled.tailSpilled()).isFalse();

        spilledDb.tailStore().failWrites(null);
        assertThat(describe(unionRead(spilled, 100))).isEqualTo(describe(unionRead(memory, 100)));
    }

    /** The estimated bytes of an in-memory tail match what the heap holds for it. */
    @Test
    void testTailMemoryEstimateMatchesTheHeap() throws Exception {
        int keys = 400_000;
        src.publish(100, 1, 10, dict(1, F1), new Index().put("x", 1, 0).build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        DerivedDvManager m = newManager("heap", unbounded(), DerivedDvMetrics.noop());
        m.recover(src);
        long before = usedHeapAfterGc();
        List<DvEntry> entries = new ArrayList<>(keys);
        for (int i = 0; i < keys; i++) {
            // primary keys of 12 to 20 bytes
            entries.add(
                    new DvEntry(key(String.format("key-%08d-%d", i, i % 1000)), 10 + i, 10 + i));
        }
        highWatermark.set(10L + keys);
        m.handleChangelogSynced(entries);
        entries = null;
        unionRead(m, 100); // applied and resolved, nothing queued
        long heap = usedHeapAfterGc() - before;
        long estimate = m.tailMemoryBytes();
        System.out.printf(
                "Tail of %d keys: estimated %d bytes (%d per key), heap %d bytes (%d per key)%n",
                keys, estimate, estimate / keys, heap, heap / keys);
        assertThat((double) estimate)
                .as("estimated %s bytes, heap %s bytes", estimate, heap)
                .isBetween(heap * 0.75, heap * 1.25);
    }

    /**
     * Measures the heap of a 5M-key tail held in memory and spilled. Run with
     * -Dfluss.dv.tail-benchmark=true.
     */
    @Test
    void testFiveMillionKeyTailMemory() throws Exception {
        assumeThat(System.getProperties().getProperty("fluss.dv.tail-benchmark")).isEqualTo("true");
        int keys = 5_000_000;
        src.publish(100, 1, 10, dict(1, F1), new Index().put("x", 1, 0).build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        for (boolean spill : new boolean[] {false, true}) {
            DerivedDvManager m =
                    newManager(
                            spill ? "bench-spilled" : "bench-memory",
                            unbounded(),
                            DerivedDvMetrics.noop());
            m.recover(src);
            if (spill) {
                // a tail spilled before it grows: the budget's case once it is exceeded
                m.handleChangelogSynced(
                        Collections.singletonList(new DvEntry(key("seed"), 10, 10)));
                highWatermark.set(11);
                unionRead(m, 100);
                assertThat(m.spillTail()).isTrue();
            }
            long before = usedHeapAfterGc();
            long t0 = System.nanoTime();
            int batch = 100_000;
            for (int from = 0; from < keys; from += batch) {
                List<DvEntry> entries = new ArrayList<>(batch);
                for (int i = from; i < from + batch; i++) {
                    entries.add(
                            new DvEntry(
                                    key(String.format("key-%08d-%d", i, i % 1000)),
                                    11 + i,
                                    11 + i));
                }
                highWatermark.set(11L + from + batch);
                m.handleChangelogSynced(entries);
            }
            unionRead(m, 100);
            long ms = (System.nanoTime() - t0) / 1_000_000;
            long heap = usedHeapAfterGc() - before;
            System.out.printf(
                    "%s tail of %d keys: heap %d MB (estimate %d MB), built in %d ms%n",
                    spill ? "spilled" : "in-memory",
                    keys,
                    heap >> 20,
                    m.tailMemoryBytes() >> 20,
                    ms);
            m.close();
            managers.remove(m);
        }
    }

    /**
     * Advances a spilled tail of 5M keys 20 times, by 5% of the log each, as bounded tiering rounds
     * do. Measures how long union reads are not ready per advance and the total replay time. Run
     * with -Dfluss.dv.tail-benchmark=true.
     */
    @Test
    void testSpilledTailAdvancedInSteps() throws Exception {
        assumeThat(System.getProperties().getProperty("fluss.dv.tail-benchmark")).isEqualTo("true");
        int changes = 5_000_000;
        int steps = 20;
        // the lake has a fifth of the keys
        Index index = new Index();
        for (int i = 0; i < changes; i += 5) {
            index.put(benchKey(i), 1, i);
        }
        src.publish(100, 1, 10, dict(1, F1), index.build());
        src.readable = new long[] {100, 10};
        List<DerivedDvManagerTest.Change> log = new ArrayList<>(changes);
        for (int i = 0; i < changes; i++) {
            log.add(new DerivedDvManagerTest.Change(key(benchKey(i)), 10 + i, 10 + i));
        }
        src.log.addAll(log);
        log.clear();
        highWatermark.set(10L + changes);
        DerivedDvMetrics metrics = DerivedDvMetrics.noop();
        // a tail of 5M keys exceeds the budget, so it spills while it is replayed
        DerivedDvManager m = newManager("bench-steps", new DerivedDvTailBudget(64L << 20), metrics);
        long t0 = System.nanoTime();
        m.recover(src);
        serve(m, 100);
        long firstMs = (System.nanoTime() - t0) / 1_000_000;
        System.out.printf(
                "recovered: %d keys spilled=%s, served after %d ms (replay %d ms)%n",
                changes, m.tailSpilled(), firstMs, metrics.lastRebuildMs());
        long totalMs = 0;
        long totalReplayMs = 0;
        long worstMs = 0;
        for (int s = 1; s <= steps; s++) {
            long offset = 10 + (long) changes * s / steps;
            long rebuilds = metrics.rebuildCount();
            // the round's index change: a tenth of the keys it tiered (the rest were deletes)
            Index change = new Index();
            for (int i = changes * (s - 1) / steps; i < changes * s / steps; i += 10) {
                change.put(benchKey(i), 1 + s, i);
            }
            src.publish(100 + s, 1 + s, offset, dict(1 + s, "f" + s), change.build());
            src.readable = new long[] {100 + s, offset};
            long t = System.nanoTime();
            serve(m, 100 + s);
            long ms = (System.nanoTime() - t) / 1_000_000;
            long replayMs = metrics.rebuildCount() > rebuilds ? metrics.lastRebuildMs() : 0;
            totalMs += ms;
            totalReplayMs += replayMs;
            worstMs = Math.max(worstMs, ms);
            System.out.printf(
                    "advance %2d to offset %8d: not ready for %6d ms (replay %6d ms), %s%n",
                    s, offset, ms, replayMs, m.stateSummary());
        }
        System.out.printf(
                "%d advances: not ready %d ms in all, worst %d ms; replays %d ms in all, one full"
                        + " replay %d ms%n",
                steps, totalMs, worstMs, totalReplayMs, firstMs);
    }

    /**
     * Writes a burst that changes every key of a spilled tail again, in batches the size of a KV
     * write, as an update round of a large table does. Measures how long the write path waits per
     * batch and how long until the masks are served. Run with -Dfluss.dv.tail-benchmark=true; the
     * number of keys is -Dfluss.dv.tail-benchmark.keys (default 2M).
     */
    @Test
    void testWritesDuringASpilledBurst() throws Exception {
        assumeThat(System.getProperties().getProperty("fluss.dv.tail-benchmark")).isEqualTo("true");
        int keys =
                Integer.parseInt(
                        System.getProperties()
                                .getProperty("fluss.dv.tail-benchmark.keys", "2000000"));
        int batch = 1_000;
        Index index = new Index();
        for (int i = 0; i < keys; i++) {
            index.put(benchKey(i), 1, i);
        }
        src.publish(100, 1, 10, dict(1, F1), index.build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        DerivedDvManager m = newManager("bench-burst", unbounded(), DerivedDvMetrics.noop());
        m.recover(src);
        long offset = 10;
        long snapshot = 100;
        // the offset of each key's live version, as its KV value's tag gives it; the lake's
        // versions are before the snapshot's offset
        long[] versions = new long[keys];
        // the second round inserts new keys instead of updating the first round's
        boolean inserts =
                Boolean.parseBoolean(
                        System.getProperties().getProperty("fluss.dv.tail-benchmark.inserts"));
        for (int round = 1; round <= 2; round++) {
            if (round == 2) {
                // tiering registers the first round, which moves every key to a new file; the
                // bucket catches up while the second round is written
                Index moved = new Index();
                for (int i = 0; i < keys; i++) {
                    moved.put(benchKey(i), 2, i);
                }
                src.publish(101, 2, offset, dict(2, "f2"), moved.build());
                src.readable = new long[] {101, offset};
                snapshot = 101;
                m.advanceAsync(101);
            }
            // the round's log, appended at once as the fake log copies on every append
            List<List<DvEntry>> batches = new ArrayList<>(keys / batch);
            List<DerivedDvManagerTest.Change> log = new ArrayList<>(keys);
            for (int b = 0; b < keys / batch; b++) {
                List<DvEntry> entries = new ArrayList<>(batch);
                for (int i = b * batch; i < (b + 1) * batch; i++) {
                    // an update writes -U and +U; the +U is the change's last record
                    offset += 2;
                    if (round == 2 && inserts) {
                        byte[] k = key(benchKey(keys + i));
                        entries.add(new DvEntry(k, offset - 1, offset - 1, true, -1));
                        log.add(new DerivedDvManagerTest.Change(k, offset - 1, offset - 1, true));
                        continue;
                    }
                    byte[] k = key(benchKey(i));
                    entries.add(new DvEntry(k, offset - 1, offset - 1, false, versions[i]));
                    versions[i] = offset - 1;
                    log.add(new DerivedDvManagerTest.Change(k, offset - 1, offset - 1));
                }
                batches.add(entries);
            }
            src.log.addAll(log);
            log = null;
            long[] waits = new long[keys / batch];
            long t0 = System.nanoTime();
            long hw = highWatermark.get();
            for (int b = 0; b < batches.size(); b++) {
                long t = System.nanoTime();
                m.handleChangelogSynced(batches.get(b));
                waits[b] = System.nanoTime() - t;
                hw += 2L * batch;
                highWatermark.set(hw);
            }
            batches = null;
            long writeMs = (System.nanoTime() - t0) / 1_000_000;
            long t1 = System.nanoTime();
            serve(m, snapshot);
            long serveMs = (System.nanoTime() - t1) / 1_000_000;
            java.util.Arrays.sort(waits);
            System.out.printf(
                    "round %d (%s tail): %d keys written in %d ms; write path waits per batch"
                            + " of %d: p50 %.2f ms, p99 %.2f ms, max %.1f ms; masks served %d ms"
                            + " after the last write; tail memory %d MB; %s%n",
                    round,
                    m.tailSpilled() ? "spilled" : "in-memory",
                    keys,
                    writeMs,
                    batch,
                    waits[waits.length / 2] / 1e6,
                    waits[waits.length * 99 / 100] / 1e6,
                    waits[waits.length - 1] / 1e6,
                    serveMs,
                    m.tailMemoryBytes() >> 20,
                    m.stateSummary());
            if (round == 1) {
                assertThat(m.spillTail()).isTrue();
            }
        }
        // every key changed after the snapshot, which holds the first round's versions; new keys
        // have no lake rows
        DvSnapshot masks = m.getDvForUnionRead(101);
        if (inserts) {
            assertThat(lakeMasks(masks)).isEmpty();
        } else {
            assertThat(lakeMasks(masks).get("f2")).hasSize(keys);
        }
        assertThat(offsets(masks.getLogDvBitmap())).isEmpty();
    }

    private static String benchKey(int i) {
        return String.format("key-%08d", i);
    }

    /** Waits up to ten minutes, not one, until the bucket serves the snapshot's masks. */
    private static void serve(DerivedDvManager m, long snapshotId) throws Exception {
        long deadline = System.currentTimeMillis() + 600_000;
        while (true) {
            try {
                m.getDvForUnionRead(snapshotId);
                return;
            } catch (RetriableException e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(5);
            }
        }
    }

    // ------------------------------------------------------------------------------------------

    private void change(DerivedDvManager m, String k, long offset, boolean insert) {
        src.append(k, offset, offset, insert);
        m.handleChangelogSynced(
                Collections.singletonList(new DvEntry(key(k), offset, offset, insert)));
    }

    private void write(DerivedDvManager m, String k, long offset) {
        List<DvEntry> e = Collections.singletonList(src.written(k, offset, offset));
        src.append(k, offset, offset);
        m.handleChangelogSynced(e);
    }

    private void write(DerivedDvManager a, DerivedDvManager b, String k, long offset) {
        List<DvEntry> e = Collections.singletonList(src.written(k, offset, offset));
        src.append(k, offset, offset);
        a.handleChangelogSynced(e);
        b.handleChangelogSynced(e);
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
        managers.add(m);
        return m;
    }

    private static DerivedDvTailBudget unbounded() {
        return new DerivedDvTailBudget(Long.MAX_VALUE);
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

    /**
     * The masks of the lake snapshot readable from {@code start}, up to {@code hw}, from a model:
     * {@code changes} as {offset, key, version or -1} in log order, and the lake rows of the
     * snapshot as key to {file id, row position}.
     */
    private static String model(
            List<long[]> changes, Map<Integer, int[]> lake, long start, long hw) {
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

    private static String describe(DvSnapshot s) throws IOException {
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
