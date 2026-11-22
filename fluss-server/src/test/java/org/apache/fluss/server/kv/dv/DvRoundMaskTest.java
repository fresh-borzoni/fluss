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
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.offsets;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The log mask of a tiering round from the change list ({@link DvRoundReads}). A round on a
 * snapshot readable from o that ends at e masks the offsets in [o, e) whose version a change below
 * e supersedes. A version superseded only at or after e is the latest of its key at e and stays.
 * Masking a live version would lose its key from the lake, so the mask must never hold more.
 */
class DvRoundMaskTest extends DerivedDvManagerTestBase {

    private static final Duration TTL = Duration.ofDays(1);
    // keys 0..LAKE_KEYS-1 are in the first snapshot; the others are inserted later
    private static final int LAKE_KEYS = 40;
    private static final int KEYS = 60;

    private final ManualClock clock = new ManualClock(System.currentTimeMillis());
    private final List<DerivedDvManager> buckets = new ArrayList<>();
    // the changes of change(), by offset: {key, version superseded or -1}
    private final Map<Long, long[]> written = new TreeMap<>();
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
     * rewrites, lost change lists, new leaders and union reads. Under row TTL a version may expire
     * from the KV before its key is written again, also one written after the readable offset, so
     * the key's next write is an insert that names nothing. At random points, for many ends e of a
     * round on the newest snapshot, the mask is compared with the model:
     *
     * <ul>
     *   <li>Without row TTL it is exactly the versions in [o, e) a later change of their key below
     *       e supersedes.
     *   <li>With row TTL it holds every version a change below e named, and nothing a later change
     *       of its key below e does not supersede. A replayed change list names an expired version
     *       from its map of last versions; a write after expiry cannot, so such a version may stay
     *       unmasked.
     * </ul>
     *
     * <p>Three buckets take the history: one as is, one whose rounds take parts of a few changes
     * and whose owner applies two changes a step, and one whose replays keep their map of last
     * versions on disk.
     */
    @ParameterizedTest(name = "seed {0}, row TTL {1}")
    @CsvSource({
        "1, false",
        "2, false",
        "3, false",
        "7, false",
        "42, false",
        "99, false",
        "1234, false",
        "5, true",
        "23, true",
        "34, true",
        "77, true",
        "4321, true"
    })
    void testRoundMaskIsTheVersionsSupersededBelowItsEnd(long seed, boolean rowTtl)
            throws Exception {
        Index initial = new Index();
        // keys with a live version in KV, to the offset of that version (the KV value's tag)
        Map<Integer, Long> live = new HashMap<>();
        // key -> {file id, row position} in the newest snapshot
        Map<Integer, int[]> lake = new TreeMap<>();
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
        List<Change> changes = new ArrayList<>();
        long logEnd = 10;
        long tiered = 10;
        long snapshot = 100;
        int nextFile = 2;
        int compares = 0;
        int expiredBeforeWrite = 0;
        Totals totals = new Totals();
        for (int step = 0; step < 700; step++) {
            int op = random.nextInt(100);
            if (op < 55) {
                int k = random.nextInt(KEYS);
                Long previous = live.get(k);
                DvEntry entry;
                if (previous == null) {
                    // a new key, one deleted before, or under row TTL one whose version expired
                    entry = new DvEntry(key("k" + k), logEnd, logEnd, !rowTtl, -1);
                    src.append("k" + k, logEnd, logEnd, true);
                    live.put(k, logEnd);
                    changes.add(new Change(logEnd, k, logEnd, -1));
                    logEnd++;
                } else if (random.nextInt(4) == 0) {
                    entry = new DvEntry(key("k" + k), logEnd, -1, false, previous);
                    src.append("k" + k, logEnd, -1, false);
                    live.remove(k);
                    changes.add(new Change(logEnd, k, -1, previous));
                    logEnd++;
                } else {
                    // -U at logEnd, +U at logEnd + 1: the change is the +U's
                    long after = logEnd + 1;
                    entry = new DvEntry(key("k" + k), after, after, false, previous);
                    src.append("k" + k, after, after, false);
                    live.put(k, after);
                    changes.add(new Change(after, k, after, previous));
                    logEnd += 2;
                }
                for (DerivedDvManager m : buckets) {
                    DvTestManagers.handOver(m, Collections.singletonList(entry));
                }
            } else if (op < 65) {
                highWatermark.addAndGet(random.nextInt((int) (logEnd - highWatermark.get()) + 1));
            } else if (op < 71 && highWatermark.get() > tiered) {
                // tiering registers a snapshot up to any offset below the high watermark, also
                // between a -U and its +U
                long upTo = tiered + 1 + random.nextInt((int) (highWatermark.get() - tiered));
                Map<Integer, Change> last = new TreeMap<>();
                for (Change c : changes) {
                    if (c.offset >= tiered && c.offset < upTo) {
                        last.put(c.key, c);
                    }
                }
                Index change = new Index();
                int position = 0;
                for (Change c : last.values()) {
                    if (c.version < 0) {
                        change.delete("k" + c.key);
                        lake.remove(c.key);
                    } else {
                        change.put("k" + c.key, nextFile, position);
                        lake.put(c.key, new int[] {nextFile, position++});
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
            } else if (op < 73 && !lake.isEmpty()) {
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
            } else if (op < 77) {
                // the change list is lost and replayed
                buckets.get(random.nextInt(buckets.size()))
                        .handOffFailed(new IllegalStateException("replayed"));
            } else if (op < 79) {
                // a new leader rebuilds from the lake and the log; it serves nothing below the log
                // end it took over at, which the high watermark reaches
                highWatermark.set(logEnd);
                int i = random.nextInt(buckets.size());
                buckets.get(i).close();
                buckets.set(i, newBucket(i, rowTtl));
            } else if (op < 85 && rowTtl && !live.isEmpty()) {
                // KV compaction removes an expired value, also one not tiered yet; nothing is
                // logged, and the key's next write is an insert
                List<Integer> keys = new ArrayList<>(live.keySet());
                int k = keys.get(random.nextInt(keys.size()));
                if (live.remove(k) >= tiered) {
                    expiredBeforeWrite++;
                }
            } else if (op < 87) {
                // a union read builds the masks at the high watermark
                for (DerivedDvManager m : buckets) {
                    long cut = unionRead(m, snapshot, true).getLogEndOffset();
                    assertMask(m, snapshot, changes, tiered, cut, rowTtl, totals);
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
                        assertMask(m, snapshot, changes, tiered, end, rowTtl, totals);
                    }
                }
                compares++;
            }
        }
        highWatermark.set(logEnd);
        for (DerivedDvManager m : buckets) {
            for (long end = tiered; end <= logEnd; end++) {
                assertMask(m, snapshot, changes, tiered, end, rowTtl, totals);
            }
        }
        assertThat(compares).isGreaterThan(20);
        // the rounds saw versions superseded after their end, the shape a mask fixed too late
        // breaks
        assertThat(totals.supersededAfterEnd).isPositive();
        if (rowTtl) {
            assertThat(expiredBeforeWrite).isPositive();
        }
    }

    /**
     * Snapshot 100 is readable from 10; k1@10 is superseded by k1@12, and -D k2@13 and k3@15 change
     * lake keys. Only k1@10 is ever masked, and only by a round that ends after 12.
     */
    @Test
    void testRoundMaskAtEachEnd() throws Exception {
        recoverAtSnapshot100();
        assertThat(mask(manager, 100, 10)).isEmpty();
        assertThat(mask(manager, 100, 11)).isEmpty();
        assertThat(mask(manager, 100, 12)).isEmpty();
        assertThat(mask(manager, 100, 13)).containsExactly(10L);
        assertThat(mask(manager, 100, 14)).containsExactly(10L);
        highWatermark.set(16);
        assertThat(mask(manager, 100, 16)).containsExactly(10L);
    }

    /**
     * The shape of a round that stops before a key's next write: k is written at 10 and written
     * again at 20. A round ending at 20 or before keeps k@10, its latest version there; only a
     * round past 20 masks it.
     */
    @Test
    void testAChangeAtOrAfterTheEndMasksNothing() throws Exception {
        recoverWithTenLakeKeys();
        long offset = 10;
        handOver("k20", offset++, true);
        for (int i = 0; i < 9; i++) {
            handOver("k" + (30 + i), offset++, true);
        }
        handOver("k20", offset++, false);
        highWatermark.set(offset);
        for (long end = 10; end <= 20; end++) {
            assertThat(mask(manager, 100, end)).as("round [10, %d)", end).isEmpty();
        }
        assertThat(mask(manager, 100, 21)).containsExactly(10L);
    }

    /**
     * A round whose end the changes handed over do not reach yet is not ready; once they are
     * applied, it is answered. A part starting outside the round is invalid; a round on a snapshot
     * the bucket moved past is stale.
     */
    @Test
    void testReadinessAndInvalidRounds() throws Exception {
        recoverAtSnapshot100();
        highWatermark.set(15);
        // nothing at 15 is handed over yet
        handOver("k1", 16, false);
        assertThatThrownBy(() -> manager.roundMask(100, null, 17))
                .isInstanceOf(DeletionVectorsNotReadyException.class);
        highWatermark.set(17);
        assertThat(mask(manager, 100, 17)).containsExactly(10L, 12L);

        assertThatThrownBy(() -> manager.roundMask(100, null, 9))
                .isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> manager.roundMask(100, 9L, 14))
                .isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> manager.roundMask(100, 15L, 14))
                .isInstanceOf(InvalidRequestException.class);

        src.publish(
                101,
                2,
                16,
                dict(2, F2),
                new Index().put("k1", 2, 0).put("k4", 2, 1).put("k3", 2, 2).delete("k2").build());
        src.readable = new long[] {101, 16};
        assertThat(manager.ensureCovered(101)).isTrue();
        assertThatThrownBy(() -> manager.roundMask(100, null, 16))
                .isInstanceOf(StaleSnapshotException.class);
        // k1@16 supersedes k1@12, which snapshot 101 holds: a lake row, not a log version
        assertThat(mask(manager, 101, 17)).isEmpty();
    }

    /**
     * The empty lake, before the first snapshot, is readable from 0: a round from 0 masks the
     * versions its own changes supersede.
     */
    @Test
    void testRoundOnTheEmptyLake() throws Exception {
        manager.recover(src);
        long offset = 0;
        for (int i = 0; i < 5; i++) {
            handOver("k" + i, offset++, true);
        }
        handOver("k1", offset++, false);
        handOver("k3", offset++, false);
        handOver("k1", offset++, false);
        highWatermark.set(offset);
        assertThat(mask(manager, -1, offset)).containsExactly(1L, 3L, 5L);
        assertThat(mask(manager, -1, 6)).containsExactly(1L);
    }

    /**
     * Under row TTL a version may expire before its key is written again, as on Paimon while the
     * readable offset is below the TTL filter's cap. The write is then an insert that names
     * nothing, so the mask keeps the expired version. A replay of the change list names it from its
     * map of last versions. Either way the mask holds no live version.
     */
    @Test
    void testExpiredVersionsUnderRowTtl() throws Exception {
        manager.useRowTtl(TTL, clock);
        src.publish(100, 1, 10, dict(1, F1), new Index().put("k1", 1, 0).build());
        src.readable = new long[] {100, 10};
        src.append("k1", 0, 0);
        highWatermark.set(10);
        manager.recover(src);
        // k2 inserted at 10, expires, inserted again at 11; k3 at 12, updated at 13
        List<DvEntry> batch = new ArrayList<>();
        batch.add(new DvEntry(key("k2"), 10, 10, false, -1));
        src.append("k2", 10, 10, true);
        batch.add(new DvEntry(key("k2"), 11, 11, false, -1));
        src.append("k2", 11, 11, true);
        batch.add(new DvEntry(key("k3"), 12, 12, false, -1));
        src.append("k3", 12, 12, true);
        batch.add(new DvEntry(key("k3"), 13, 13, false, 12));
        src.append("k3", 13, 13, false);
        DvTestManagers.handOver(manager, batch);
        highWatermark.set(14);
        assertThat(mask(manager, 100, 14)).containsExactly(12L);

        manager.handOffFailed(new IllegalStateException("replayed"));
        assertThat(mask(manager, 100, 14)).containsExactly(10L, 12L);
        assertThat(mask(manager, 100, 11)).isEmpty();
    }

    /**
     * A mask comes in parts, each starting where the one before stopped and reading at most so many
     * changes; together they are the round's mask. Inserts are not in the change list.
     */
    @ParameterizedTest(name = "{0} changes a part")
    @CsvSource({"1000000, 1", "4, 9", "3, 11"})
    void testMaskInParts(int changes, int parts) throws Exception {
        recoverWithTenLakeKeys();
        manager.setRoundPartSize(DvRoundReads.PART_LAKE_KEYS, changes, Integer.MAX_VALUE);
        assertParts(40, parts);
    }

    /**
     * A part of a mask stops once its bitmap serializes to the size a part may take; it reads at
     * least one chunk of changes.
     */
    @ParameterizedTest(name = "{0} bytes a part")
    @CsvSource({"1048576, 1", "1, 3"})
    void testMaskPartsStopAtTheirSize(int maskBytes, int parts) throws Exception {
        recoverWithTenLakeKeys();
        manager.setRoundPartSize(DvRoundReads.PART_LAKE_KEYS, Integer.MAX_VALUE, maskBytes);
        assertParts(2 * DvOwner.RESOLVE_CHUNK + 25, parts);
    }

    /**
     * Writes {@code n} changes of seven new keys and asserts that the mask comes in {@code parts}
     * parts that hold every version but the last of each key.
     */
    private void assertParts(int n, int parts) throws Exception {
        Map<Integer, Long> versions = new HashMap<>();
        long offset = 10;
        for (int i = 0; i < n; i++) {
            offset = change(versions, 10 + (i % 7), offset);
        }
        highWatermark.set(offset);

        List<Long> starts = new ArrayList<>();
        Set<Long> all = new TreeSet<>();
        Long from = null;
        while (true) {
            DvRoundMask part = manager.roundMask(100, from, offset);
            assertThat(part.startOffset()).isEqualTo(10L);
            starts.add(from == null ? part.startOffset() : from);
            all.addAll(offsets(part.bitmap()));
            if (part.nextOffset() == offset) {
                break;
            }
            assertThat(part.nextOffset()).isGreaterThan(starts.get(starts.size() - 1));
            from = part.nextOffset();
        }
        assertThat(all).isEqualTo(expectedOf(offset)).hasSize(n - 7);
        assertThat(starts).hasSize(parts);
    }

    /**
     * While a writer hands changes over above the round's end, raises the high watermark and loses
     * the change list now and then, so the bucket replays it, masks read in small parts stay the
     * versions superseded below their end.
     */
    @Test
    void testMasksInPartsWhileTheBucketWritesAndReplays() throws Exception {
        recoverWithTenLakeKeys();
        manager.setRoundPartSize(2, 3, 1);
        Random random = new Random(7);
        Map<Integer, Long> versions = new HashMap<>();
        long offset = 10;
        for (int i = 0; i < 80; i++) {
            offset = change(versions, random.nextInt(15), offset);
        }
        long end = offset;
        highWatermark.set(end);
        Set<Long> expected = expectedOf(end);
        assertThat(expected).isNotEmpty();

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
                                    // every key changes again above the round's end
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
                assertThat(mask(manager, 100, end)).as("round %d", i).isEqualTo(expected);
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
     * over; returns the next offset. Keys from 10 on are not in the lake: their first write is an
     * insert.
     */
    private long change(Map<Integer, Long> versions, int k, long offset) {
        Long previous;
        synchronized (written) {
            previous = versions.put(k, offset);
            written.put(offset, new long[] {k, previous == null ? -1 : previous});
        }
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

    /** The versions at or after 10 that {@link #change} superseded below {@code end}. */
    private Set<Long> expectedOf(long end) {
        Set<Long> out = new TreeSet<>();
        synchronized (written) {
            for (Map.Entry<Long, long[]> e : written.entrySet()) {
                if (e.getKey() < end && e.getValue()[1] >= 10) {
                    out.add(e.getValue()[1]);
                }
            }
        }
        return out;
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
            m.setRoundPartSize(2, 3, 1);
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
     * What a change of the model wrote: its offset, key, version (-1 deletes) and the one named.
     */
    private static final class Change {
        final long offset;
        final int key;
        final long version;
        final long named;

        Change(long offset, int key, long version, long named) {
            this.offset = offset;
            this.key = key;
            this.version = version;
            this.named = named;
        }
    }

    /** Counts over the comparisons of a history. */
    private static final class Totals {
        // versions in a round that a change at or after its end superseded
        long supersededAfterEnd;
    }

    /**
     * Asserts the mask of the round on {@code snapshot} from {@code start} to {@code end} against
     * the model's changes.
     */
    private static void assertMask(
            DerivedDvManager m,
            long snapshot,
            List<Change> changes,
            long start,
            long end,
            boolean rowTtl,
            Totals totals)
            throws Exception {
        // the versions a later write of their key below the end supersedes
        Set<Long> exact = new TreeSet<>();
        // the versions a change below the end names
        Set<Long> named = new TreeSet<>();
        // the latest version of each key, -1 after a delete
        Map<Integer, Long> latest = new HashMap<>();
        // the records in the round that supersede a version: +U and -D, and +I under row TTL
        long counted = 0;
        for (Change c : changes) {
            Long before = latest.get(c.key);
            if (c.offset < end) {
                if (before != null && before >= start) {
                    exact.add(before);
                }
                if (c.named >= start) {
                    named.add(c.named);
                }
                boolean insert = c.named < 0 && c.version >= 0;
                if (c.offset >= start && (rowTtl || !insert)) {
                    counted++;
                }
            } else if (before != null && before >= start && before < end) {
                totals.supersededAfterEnd++;
            }
            latest.put(c.key, c.version);
        }
        Set<Long> mask = mask(m, snapshot, end);
        assertThat(changes(m, snapshot, end, rowTtl))
                .as("changes of [%d, %d)", start, end)
                .isEqualTo(counted);
        if (rowTtl) {
            assertThat(mask).as("mask of [%d, %d) under row TTL", start, end).containsAll(named);
            assertThat(exact).as("mask of [%d, %d) under row TTL", start, end).containsAll(mask);
        } else {
            assertThat(named).isEqualTo(exact);
            assertThat(mask).as("mask of [%d, %d)", start, end).isEqualTo(exact);
        }
    }

    /** All parts of the round's mask, retried while the bucket is not ready. */
    private static Set<Long> mask(DerivedDvManager m, long snapshot, long end) throws Exception {
        Set<Long> out = new TreeSet<>();
        Long from = null;
        while (true) {
            Long partFrom = from;
            DvRoundMask part = retry(() -> m.roundMask(snapshot, partFrom, end));
            Set<Long> offsets = offsets(part.bitmap());
            if (part.bitmap() != null) {
                assertThat(offsets).isNotEmpty();
            }
            for (long offset : offsets) {
                assertThat(offset).isGreaterThanOrEqualTo(part.startOffset()).isLessThan(end);
            }
            out.addAll(offsets);
            if (part.nextOffset() == end) {
                return out;
            }
            assertThat(part.nextOffset())
                    .isGreaterThan(from == null ? part.startOffset() : from)
                    .isLessThan(end);
            from = part.nextOffset();
        }
    }

    /** The changes the round's mask parts count, all parts summed. */
    private static long changes(DerivedDvManager m, long snapshot, long end, boolean rowTtl)
            throws Exception {
        long changes = 0;
        Long from = null;
        while (true) {
            Long partFrom = from;
            DvRoundMask part = retry(() -> m.roundMask(snapshot, partFrom, end));
            assertThat(part.changesWithInserts()).isEqualTo(rowTtl);
            changes += part.changes();
            if (part.nextOffset() == end) {
                return changes;
            }
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

    private static String path(int fileId) {
        return fileId == 1 ? F1 : "f" + fileId;
    }
}
