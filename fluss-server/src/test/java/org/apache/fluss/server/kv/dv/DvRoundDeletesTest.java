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
import org.apache.fluss.server.kv.dv.DerivedDvManagerTest.Index;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.dict;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.key;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.lakeMasks;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.offsets;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The deletes of a tiering round from the change list ({@link DvRoundReads}). A round on a snapshot
 * readable from o that ends at e deletes the rows of the snapshot whose keys changed in [o, e).
 */
class DvRoundDeletesTest extends DerivedDvManagerTestBase {

    private static final Duration TTL = Duration.ofDays(1);
    // keys 0..LAKE_KEYS-1 are in the first snapshot; the others are inserted later
    private static final int LAKE_KEYS = 40;
    private static final int KEYS = 60;

    private final ManualClock clock = new ManualClock(System.currentTimeMillis());
    private final List<DerivedDvManager> buckets = new ArrayList<>();
    private int dirs;

    @AfterEach
    @Override
    void tearDown() {
        for (DerivedDvManager m : buckets) {
            m.close();
        }
        super.tearDown();
    }

    /**
     * A random history of inserts, updates (-U then +U), deletes and re-inserts of keys, some never
     * in the lake, with high watermark moves, registrations of snapshots tiered up to any offset,
     * rewrites, lost change lists, restarts and union reads. Under row TTL, any version may expire
     * from the KV, also one written after the readable offset, so the key's next write is an
     * insert. At random points, for many ends e of a round on the newest snapshot, its deletes are
     * the rows of the snapshot whose keys changed in [o, e). Three buckets take the history: one as
     * is, one whose rounds take small parts and whose owner applies two changes a step, and one
     * whose replays keep their map of last versions on disk.
     */
    @ParameterizedTest(name = "seed {0}, row TTL {1}")
    @CsvSource({"1, false", "7, false", "42, false", "5, true", "23, true", "34, true"})
    void testRoundDeletesAreTheRowsOfTheChangedKeys(long seed, boolean rowTtl) throws Exception {
        Index initial = new Index();
        // key -> {file id, row position} in the newest snapshot
        Map<Integer, int[]> lake = new TreeMap<>();
        // keys with a live version in KV, to the offset of that version (the KV value's tag)
        Map<Integer, Long> live = new HashMap<>();
        for (int i = 0; i < LAKE_KEYS; i++) {
            initial.put("k" + i, 1, i);
            lake.put(i, new int[] {1, i});
            live.put(i, 0L);
        }
        src.publish(100, 1, 10, dict(1, F1), initial.build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        for (int i = 0; i < 3; i++) {
            buckets.add(newBucket(i, rowTtl));
        }

        Random random = new Random(seed);
        // {offset, key, version or -1} of every change, in log order
        List<long[]> changes = new ArrayList<>();
        long logEnd = 10;
        long tiered = 10;
        long snapshot = 100;
        int nextFile = 2;
        int compares = 0;
        // keys whose version written after the readable offset expired, and their re-inserts
        Set<Integer> expiredUntiered = new TreeSet<>();
        int reinsertsAfterExpiry = 0;
        for (int step = 0; step < 600; step++) {
            int op = random.nextInt(100);
            if (op < 55) {
                int k = random.nextInt(KEYS);
                Long previous = live.get(k);
                DvEntry entry;
                if (previous == null) {
                    if (expiredUntiered.remove(k)) {
                        reinsertsAfterExpiry++;
                    }
                    // with row TTL it may follow an expired version, so it names its lake row
                    entry = new DvEntry(key("k" + k), logEnd, logEnd, !rowTtl, -1);
                    src.append("k" + k, logEnd, logEnd, true);
                    live.put(k, logEnd);
                    changes.add(new long[] {logEnd, k, logEnd});
                    logEnd++;
                } else if (random.nextInt(4) == 0) {
                    entry = new DvEntry(key("k" + k), logEnd, -1, false, previous);
                    src.append("k" + k, logEnd, -1, false);
                    live.remove(k);
                    expiredUntiered.remove(k);
                    changes.add(new long[] {logEnd, k, -1});
                    logEnd++;
                } else {
                    // -U at logEnd, +U at logEnd + 1: the change is the +U's
                    long after = logEnd + 1;
                    entry = new DvEntry(key("k" + k), after, after, false, previous);
                    src.append("k" + k, after, after, false);
                    live.put(k, after);
                    changes.add(new long[] {after, k, after});
                    logEnd += 2;
                }
                for (DerivedDvManager m : buckets) {
                    DvTestManagers.handOver(m, Collections.singletonList(entry));
                }
            } else if (op < 65) {
                highWatermark.addAndGet(random.nextInt((int) (logEnd - highWatermark.get()) + 1));
            } else if (op < 72 && highWatermark.get() > tiered) {
                // tiering registers a snapshot up to any offset below the high watermark, also
                // between a -U and its +U
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
                    int k = (int) c[1];
                    if (c[2] < 0) {
                        change.delete("k" + k);
                        lake.remove(k);
                    } else {
                        change.put("k" + k, nextFile, position);
                        lake.put(k, new int[] {nextFile, position++});
                    }
                }
                snapshot = 100 + nextFile;
                src.publish(
                        snapshot, nextFile, upTo, dict(nextFile, path(nextFile)), change.build());
                src.readable = new long[] {snapshot, upTo};
                nextFile++;
                tiered = upTo;
                for (DerivedDvManager m : buckets) {
                    if (random.nextBoolean()) {
                        assertThat(m.ensureCovered(snapshot)).isTrue();
                    } else {
                        // the round's request finds the bucket switching
                        m.advanceAsync(snapshot);
                    }
                }
            } else if (op < 75 && !lake.isEmpty()) {
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
                src.replaced.put((long) nextFile, Collections.singletonList(path(file)));
                src.publish(
                        snapshot, nextFile, tiered, dict(nextFile, path(nextFile)), change.build());
                src.readable = new long[] {snapshot, tiered};
                nextFile++;
                for (DerivedDvManager m : buckets) {
                    assertThat(m.ensureCovered(snapshot)).isTrue();
                }
            } else if (op < 78) {
                // the change list is lost and replayed
                buckets.get(random.nextInt(buckets.size()))
                        .handOffFailed(new IllegalStateException("replayed"));
            } else if (op < 80) {
                // a new leader rebuilds from the lake and the log; it serves nothing below the log
                // end it took over at, which the high watermark reaches
                highWatermark.set(logEnd);
                int i = random.nextInt(buckets.size());
                buckets.get(i).close();
                buckets.set(i, newBucket(i, rowTtl));
            } else if (op < 85 && rowTtl && !live.isEmpty()) {
                // KV compaction removes an expired value, also one not tiered yet; nothing is
                // logged
                List<Integer> keys = new ArrayList<>(live.keySet());
                int k = keys.get(random.nextInt(keys.size()));
                if (live.remove(k) >= tiered) {
                    expiredUntiered.add(k);
                }
            } else if (op < 88) {
                // a union read builds the masks at the high watermark
                for (DerivedDvManager m : buckets) {
                    long cut = unionRead(m, snapshot, true).getLogEndOffset();
                    assertRoundDeletes(m, snapshot, lake, changes, tiered, cut);
                }
                compares++;
            } else {
                long hw = highWatermark.get();
                List<Long> ends = new ArrayList<>();
                ends.add(tiered);
                ends.add(hw);
                for (int i = 0; i < 4; i++) {
                    ends.add(tiered + random.nextInt((int) (hw - tiered) + 1));
                }
                for (DerivedDvManager m : buckets) {
                    for (long end : ends) {
                        assertRoundDeletes(m, snapshot, lake, changes, tiered, end);
                    }
                }
                compares++;
            }
        }
        highWatermark.set(logEnd);
        for (DerivedDvManager m : buckets) {
            for (long end = tiered; end <= logEnd; end++) {
                assertRoundDeletes(m, snapshot, lake, changes, tiered, end);
            }
        }
        assertThat(compares).isGreaterThan(20);
        if (rowTtl) {
            assertThat(reinsertsAfterExpiry).isPositive();
        }
    }

    /**
     * A round ending at the cut of the latest masks deletes the lake rows those masks hold; one
     * ending below it fewer.
     */
    @Test
    void testRoundEndingAtTheLatestCut() throws Exception {
        recoverAtSnapshot100();
        DvSnapshot read = unionRead(100);
        assertThat(read.getLogEndOffset()).isEqualTo(14L);

        assertThat(roundDeletes(manager, 100, 14))
                .isEqualTo(deletes(F1, 0L, 1L))
                .isEqualTo(lakeMasks(read));
        // k1@10 masks k1's row, k4@11 is not in the lake, k1@12 supersedes k1@10
        assertThat(roundDeletes(manager, 100, 10)).isEmpty();
        assertThat(roundDeletes(manager, 100, 11)).isEqualTo(deletes(F1, 0L));
        assertThat(roundDeletes(manager, 100, 13)).isEqualTo(deletes(F1, 0L));
    }

    /**
     * A round whose end the changes handed over do not reach yet is not ready; once they are
     * applied, it is answered. A round ending before the readable offset, or a part starting there,
     * is invalid; a round on a snapshot the bucket moved past is stale.
     */
    @Test
    void testReadinessAndInvalidRounds() throws Exception {
        recoverAtSnapshot100();
        // k3@15 is handed over, above the high watermark 14
        assertThat(roundDeletes(manager, 100, 15)).isEqualTo(deletes(F1, 0L, 1L));
        assertThatThrownBy(() -> manager.roundDeletes(100, null, 16))
                .isInstanceOf(DeletionVectorsNotReadyException.class);
        assertThatThrownBy(() -> manager.roundDeletes(100, null, 17))
                .isInstanceOf(DeletionVectorsNotReadyException.class);
        highWatermark.set(16);
        assertThat(roundDeletes(manager, 100, 16)).isEqualTo(deletes(F1, 0L, 1L, 2L));

        assertThatThrownBy(() -> manager.roundDeletes(100, null, 9))
                .isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> manager.roundDeletes(100, 9L, 14))
                .isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> manager.roundDeletes(100, 15L, 14))
                .isInstanceOf(InvalidRequestException.class);

        src.publish(
                101,
                2,
                16,
                dict(2, F2),
                new Index().put("k1", 2, 0).put("k4", 2, 1).put("k3", 2, 2).delete("k2").build());
        src.readable = new long[] {101, 16};
        assertThat(manager.ensureCovered(101)).isTrue();
        assertThatThrownBy(() -> manager.roundDeletes(100, null, 16))
                .isInstanceOf(StaleSnapshotException.class);
        assertThat(roundDeletes(manager, 101, 16)).isEmpty();
    }

    /**
     * Under row TTL, a key's next write after its version expired is an insert that names nothing
     * in the log. The round still deletes the key's lake row: an insert names it.
     */
    @Test
    void testRoundsOfARowTtlBucketDeleteTheLakeRowsOfInserts() throws Exception {
        manager.useRowTtl(TTL, clock);
        src.publish(100, 1, 10, dict(1, F1), new Index().put("k1", 1, 0).put("k2", 1, 1).build());
        src.readable = new long[] {100, 10};
        src.append("k1", 0, 0);
        src.append("k2", 1, 1);
        highWatermark.set(10);
        manager.recover(src);
        // k1 updated at 11, which expires; k1 inserted again at 12, and k2 at 13 after its lake
        // version expired
        List<DvEntry> batch = new ArrayList<>();
        batch.add(new DvEntry(key("k1"), 11, 11, false, 0));
        src.append("k1", 11, 11);
        batch.add(new DvEntry(key("k1"), 12, 12, false, -1));
        src.append("k1", 12, 12, true);
        batch.add(new DvEntry(key("k2"), 13, 13, false, -1));
        src.append("k2", 13, 13, true);
        DvTestManagers.handOver(manager, batch);
        highWatermark.set(14);
        waitUntil(() -> manager.rowTtlFilterClock().cappedFrom() == 10);
        assertThat(roundDeletes(manager, 100, 14)).isEqualTo(deletes(F1, 0L, 1L));
        assertThat(roundDeletes(manager, 100, 12)).isEqualTo(deletes(F1, 0L));
    }

    /**
     * A round comes in parts, each starting where the one before stopped, bounded by the lake keys
     * it looks up and by the changes it reads; together they are the round's deletes.
     */
    @ParameterizedTest(name = "{0} lake keys, {1} changes a part")
    @CsvSource({"3, 1000, 4", "1000, 4, 4", "2, 3, 7"})
    void testRoundInParts(int lakeKeys, int changes, int parts) throws Exception {
        recoverWithTenLakeKeys();
        manager.setRoundDeletePartSize(lakeKeys, changes);
        long offset = 10;
        List<DvEntry> batch = new ArrayList<>();
        Map<Integer, Long> versions = new HashMap<>();
        // ten changes of lake keys, then five of keys changed before
        for (int pass = 0; pass < 2; pass++) {
            for (int i = 0; i < 10; i += 1 + pass) {
                Long previous = versions.put(i, offset);
                batch.add(
                        new DvEntry(
                                key("k" + i),
                                offset,
                                offset,
                                false,
                                previous == null ? 0 : previous));
                offset++;
            }
        }
        DvTestManagers.handOver(manager, batch);
        highWatermark.set(offset);

        List<Long> starts = new ArrayList<>();
        Map<String, Set<Long>> all = new TreeMap<>();
        Long from = null;
        while (true) {
            DvRoundDeletes part = manager.roundDeletes(100, from, offset);
            starts.add(from == null ? part.startOffset() : from);
            add(all, part);
            if (part.nextOffset() == offset) {
                break;
            }
            assertThat(part.nextOffset()).isGreaterThan(starts.get(starts.size() - 1));
            from = part.nextOffset();
        }
        assertThat(starts).hasSize(parts);
        assertThat(all).isEqualTo(deletes(F1, 0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L));
        assertThat(roundDeletes(manager, 100, 12)).isEqualTo(deletes(F1, 0L, 1L));
    }

    /** Many changes of a few lake keys take one part: a part is bounded by the keys it looks up. */
    @Test
    void testHotKeysTakeOnePart() throws Exception {
        recoverWithTenLakeKeys();
        manager.setRoundDeletePartSize(3, 100_000);
        long offset = 10;
        long[] versions = new long[2];
        List<DvEntry> batch = new ArrayList<>();
        for (int i = 0; i < 20_000; i++) {
            int k = i % 2;
            batch.add(new DvEntry(key("k" + k), offset, offset, false, versions[k]));
            versions[k] = offset;
            offset++;
        }
        DvTestManagers.handOver(manager, batch);
        highWatermark.set(offset);

        DvRoundDeletes part = retry(() -> manager.roundDeletes(100, null, highWatermark.get()));
        assertThat(part.nextOffset()).isEqualTo(offset);
        Map<String, Set<Long>> all = new TreeMap<>();
        add(all, part);
        assertThat(all).isEqualTo(deletes(F1, 0L, 1L));
    }

    /**
     * While a writer hands changes over above the round's end, raises the high watermark and loses
     * the change list now and then, so the bucket replays it, rounds read in small parts always
     * delete the rows of the keys changed below their end.
     */
    @Test
    void testRoundsInPartsWhileTheBucketWritesAndReplays() throws Exception {
        recoverWithTenLakeKeys();
        manager.setRoundDeletePartSize(2, 3);
        Random random = new Random(7);
        Map<Integer, Long> versions = new HashMap<>();
        Set<Long> changedLakeRows = new TreeSet<>();
        long offset = 10;
        for (int i = 0; i < 60; i++) {
            // lake keys 5..9 change only above the round's end, in the writer below; keys from 10
            // on are new
            int k = random.nextInt(15);
            k = k < 5 ? k : k + 5;
            offset = change(versions, k, offset);
            if (k < 10) {
                changedLakeRows.add((long) k);
            }
        }
        long end = offset;
        highWatermark.set(end);
        Map<String, Set<Long>> expected = new TreeMap<>();
        expected.put(F1, changedLakeRows);

        AtomicBoolean stop = new AtomicBoolean();
        AtomicInteger replays = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        long writerStart = offset;
        Thread writer =
                new Thread(
                        () -> {
                            Random r = new Random(11);
                            long o = writerStart;
                            try {
                                while (!stop.get() && o < writerStart + 20_000) {
                                    o = change(versions, r.nextInt(20), o);
                                    if (r.nextInt(4) == 0) {
                                        highWatermark.set(o);
                                    }
                                    if (r.nextInt(150) == 0) {
                                        manager.handOffFailed(new IllegalStateException("lost"));
                                        replays.incrementAndGet();
                                    }
                                    Thread.sleep(1);
                                }
                            } catch (Throwable t) {
                                failure.set(t);
                            }
                        });
        writer.start();
        try {
            for (int i = 0; i < 60 || replays.get() < 5; i++) {
                assertThat(roundDeletes(manager, 100, end)).as("round %d", i).isEqualTo(expected);
                assertThat(failure.get()).isNull();
                assertThat(writer.isAlive()).as("the writer runs").isTrue();
            }
        } finally {
            stop.set(true);
            writer.join();
        }
        assertThat(failure.get()).isNull();
    }

    // ---- helpers --------------------------------------------------------------------------------

    /**
     * Writes key {@code k} at {@code offset}, naming its version before, as the write path hands it
     * over; returns the next offset.
     */
    private long change(Map<Integer, Long> versions, int k, long offset) {
        Long previous = versions.put(k, offset);
        // keys from 10 on are not in the lake: their first write is an insert
        boolean insert = k >= 10 && previous == null;
        src.append("k" + k, offset, offset, insert);
        DvTestManagers.handOver(
                manager,
                Collections.singletonList(
                        new DvEntry(
                                key("k" + k),
                                offset,
                                offset,
                                insert,
                                previous == null ? -1 : previous)));
        return offset + 1;
    }

    private DerivedDvManager newBucket(int kind, boolean rowTtl) throws Exception {
        DerivedDvManager m =
                DvTestManagers.newManager(
                        DvRocksDB.open(tempDir.resolve("bucket-" + dirs++).toString()),
                        highWatermark::get,
                        DvTestManagers.CHECKPOINT_INTERVAL,
                        DerivedDvMetrics.noop());
        m.setMaskCacheMillis(0);
        if (kind == 1) {
            m.setRoundDeletePartSize(2, 3);
            m.setStepChunks(2, 1);
        } else if (kind == 2) {
            m.setSpillReplayVersions(true);
        }
        if (rowTtl) {
            m.useRowTtl(TTL, clock);
        }
        m.recover(src);
        return m;
    }

    /**
     * Asserts that the round on {@code snapshot} from {@code start} to {@code end} deletes the rows
     * of {@code lake} whose keys changed in between.
     */
    private static void assertRoundDeletes(
            DerivedDvManager m,
            long snapshot,
            Map<Integer, int[]> lake,
            List<long[]> changes,
            long start,
            long end)
            throws Exception {
        Map<String, Set<Long>> expected = new TreeMap<>();
        for (long[] c : changes) {
            int[] row = lake.get((int) c[1]);
            if (c[0] >= start && c[0] < end && row != null) {
                expected.computeIfAbsent(path(row[0]), f -> new TreeSet<>()).add((long) row[1]);
            }
        }
        assertThat(roundDeletes(m, snapshot, end))
                .as("round deletes of [%d, %d)", start, end)
                .isEqualTo(expected);
    }

    /** All parts of the round's deletes, retried while the bucket is not ready. */
    private static Map<String, Set<Long>> roundDeletes(DerivedDvManager m, long snapshot, long end)
            throws Exception {
        Map<String, Set<Long>> out = new TreeMap<>();
        Long from = null;
        while (true) {
            Long partFrom = from;
            DvRoundDeletes part = retry(() -> m.roundDeletes(snapshot, partFrom, end));
            add(out, part);
            if (part.nextOffset() == end) {
                return out;
            }
            assertThat(part.nextOffset())
                    .isGreaterThan(from == null ? part.startOffset() : from)
                    .isLessThan(end);
            from = part.nextOffset();
        }
    }

    private static <T> T retry(Call<T> call) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        while (true) {
            try {
                return call.call();
            } catch (RetriableException e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(10);
            }
        }
    }

    private interface Call<T> {
        T call() throws Exception;
    }

    private static void add(Map<String, Set<Long>> out, DvRoundDeletes part) throws Exception {
        for (Map.Entry<String, byte[]> e : part.deletes().entrySet()) {
            out.computeIfAbsent(e.getKey(), f -> new TreeSet<>()).addAll(offsets(e.getValue()));
        }
    }

    private static Map<String, Set<Long>> deletes(String file, Long... positions) {
        Map<String, Set<Long>> out = new TreeMap<>();
        out.put(file, new TreeSet<>(Arrays.asList(positions)));
        return out;
    }

    private static String path(int fileId) {
        return fileId == 1 ? F1 : "f" + fileId;
    }
}
