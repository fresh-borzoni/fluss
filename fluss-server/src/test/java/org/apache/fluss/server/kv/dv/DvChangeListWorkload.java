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

import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.server.kv.RowTtlFilterClock;
import org.apache.fluss.server.kv.snapshot.RowTtlFilterState;
import org.apache.fluss.utils.clock.ManualClock;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * A primary key bucket's log as the write path produces it, for tests of the change list: inserts
 * (+I), updates (-U then +U, the +U holding the new version) and deletes (-D) of a few keys. It
 * records the changes the write path hands to the deletion vectors, with the version each one
 * supersedes from the KV value's tag, and the KV's version offsets at every batch boundary, where a
 * KV snapshot may be taken.
 *
 * <p>{@link Model} names superseded versions the ways the server does: the write path, a full
 * replay of the log, and a change-list checkpoint at X followed by a replay from X that names a
 * key's first superseded version after X from the KV at X. Their change lists, classified against a
 * readable offset, must be equal.
 *
 * <p>With row TTL ({@link #randomWithRowTtl}), each batch is written a time step after the one
 * before, and KV compaction may remove a value whose TTL has passed, with nothing in the log: the
 * key's next write is then an insert, which the write path hands over too, superseding nothing it
 * knows (its key is looked up in the lake). A replay names the expired version if it replayed it.
 */
final class DvChangeListWorkload {

    /** One log record; a -U record carries the key and no version. */
    static final class Record {
        final long offset;
        final String key;
        final Kind kind;

        Record(long offset, String key, Kind kind) {
            this.offset = offset;
            this.key = key;
            this.kind = kind;
        }

        /** The key's version after the record: its offset, or -1 for a delete. */
        long version() {
            return kind == Kind.DELETE ? -1 : offset;
        }

        byte[] keyBytes() {
            return key.getBytes(StandardCharsets.UTF_8);
        }
    }

    /** The kind of a log record. */
    enum Kind {
        INSERT,
        UPDATE_BEFORE,
        UPDATE_AFTER,
        DELETE
    }

    /** An update or delete in a change list, with the version it superseded or -1. */
    static final class Entry {
        final long offset;
        final String key;
        final long superseded;

        Entry(long offset, String key, long superseded) {
            this.offset = offset;
            this.key = key;
            this.superseded = superseded;
        }

        /** As a change list of readable offset {@code o} sees it: a version before o is -1. */
        Entry classified(long o) {
            return new Entry(offset, key, superseded >= o ? superseded : -1);
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Entry)) {
                return false;
            }
            Entry e = (Entry) other;
            return offset == e.offset && superseded == e.superseded && key.equals(e.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(offset, key, superseded);
        }

        @Override
        public String toString() {
            return offset + ":" + key + "<-" + superseded;
        }
    }

    final List<Record> log = new ArrayList<>();
    // the write path's updates and deletes, with the KV tag of the value each one superseded; with
    // row TTL its inserts too, superseding -1
    final List<Entry> handedOver = new ArrayList<>();
    // batch boundaries: a KV snapshot's offset, with each live key's version offset there
    final TreeMap<Long, Map<String, Long>> kvAt = new TreeMap<>();
    private final Map<String, Long> kv = new HashMap<>();
    private long next;
    // row TTL: the time of each batch, from its first offset; the write time of each live value;
    // the version offsets KV compaction removed as expired
    final boolean rowTtl;
    final long ttlMs;
    final TreeMap<Long, Long> batchTimes = new TreeMap<>();
    private final Map<String, Long> writtenAt = new HashMap<>();
    final List<Long> expiredVersions = new ArrayList<>();
    private long now;
    private long batchStart;
    // With a capped filter: the leader's filter, the clock it reads, and the readable offset the
    // bucket covers, which tiering moves forward and a rebuild sometimes moves back.
    @Nullable private RowTtlFilterClock filter;
    private final ManualClock clock = new ManualClock(0);
    private long tiered;
    // the TTL time of the next write; NEVER for a row that never expires
    static final long NEVER = KvValueLayout.NEVER_EXPIRE_TIMESTAMP;
    private boolean ttlTimes;
    private long nextTtlTime;

    /** The time between two batches, with row TTL. */
    static final long STEP_MS = 1_000;

    DvChangeListWorkload() {
        this(-1);
    }

    private DvChangeListWorkload(long ttlMs) {
        this.rowTtl = ttlMs > 0;
        this.ttlMs = ttlMs;
        kvAt.put(0L, Collections.emptyMap());
    }

    /** A random workload of {@code batches} batches over {@code keys} keys. */
    static DvChangeListWorkload random(Random random, int keys, int batches, int maxBatch) {
        DvChangeListWorkload w = new DvChangeListWorkload();
        for (int b = 0; b < batches; b++) {
            int size = 1 + random.nextInt(maxBatch);
            for (int i = 0; i < size; i++) {
                String key = "k" + random.nextInt(keys);
                if (random.nextInt(5) == 0) {
                    w.delete(key);
                } else {
                    w.upsert(key);
                }
            }
            w.endBatch();
        }
        return w;
    }

    /**
     * A random workload with row TTL of {@code ttlSteps} batches: at each batch boundary, KV
     * compaction removes some of the values whose TTL has passed.
     */
    static DvChangeListWorkload randomWithRowTtl(
            Random random, int keys, int batches, int maxBatch, int ttlSteps) {
        DvChangeListWorkload w = new DvChangeListWorkload(ttlSteps * STEP_MS);
        for (int b = 0; b < batches; b++) {
            int size = 1 + random.nextInt(maxBatch);
            for (int i = 0; i < size; i++) {
                String key = "k" + random.nextInt(keys);
                if (random.nextInt(5) == 0) {
                    w.delete(key);
                } else {
                    w.upsert(key);
                }
            }
            w.endBatch();
            for (String key : new ArrayList<>(w.kv.keySet())) {
                if (random.nextInt(3) == 0) {
                    w.expire(key);
                }
            }
        }
        return w;
    }

    /**
     * A random workload with row TTL whose KV filter is the leader's capped one ({@link
     * RowTtlFilterClock}), fed and capped as the leader does it: the write path tracks each
     * version's TTL time, and the bucket covers the readable offset o each time tiering moves it.
     * With {@code eventTimes}, writes carry TTL times as by event time: most the time now, some
     * late, some backfilled several TTLs back, some none; else the time now, as by processing time.
     * Tiering stalls for several TTLs at times; a rebuild sometimes moves o back to an earlier
     * boundary; and a new leader sometimes takes the KV over from a snapshot at the log end, with
     * the filter's state the old leader saved.
     */
    static DvChangeListWorkload randomWithCappedRowTtl(
            Random random, int keys, int batches, int maxBatch, int ttlSteps, boolean eventTimes) {
        DvChangeListWorkload w = new DvChangeListWorkload(ttlSteps * STEP_MS);
        w.ttlTimes = eventTimes;
        w.filter = new RowTtlFilterClock(w.clock, w.ttlMs);
        // the leader covers the empty lake
        w.filter.coveredAt(0);
        for (int b = 0; b < batches; b++) {
            int size = 1 + random.nextInt(maxBatch);
            for (int i = 0; i < size; i++) {
                String key = "k" + random.nextInt(keys);
                if (random.nextInt(5) == 0) {
                    w.delete(key);
                } else {
                    int kind = eventTimes ? random.nextInt(10) : 9;
                    w.nextTtlTime =
                            kind == 0
                                    ? w.now - random.nextInt(3 * ttlSteps + 1) * STEP_MS
                                    : kind == 1 ? w.now - STEP_MS : kind == 2 ? NEVER : w.now;
                    w.upsert(key);
                }
            }
            w.endBatch();
            int step = random.nextInt(16);
            if (step < 4) {
                // a round commits: o moves to a boundary at or before the log end
                List<Long> later =
                        new ArrayList<>(w.kvAt.subMap(w.tiered, true, w.next, true).keySet());
                w.tiered = later.get(random.nextInt(later.size()));
                w.filter.coveredAt(w.tiered);
            } else if (step == 4) {
                // a rebuild covers an older snapshot
                List<Long> earlier = new ArrayList<>(w.kvAt.headMap(w.tiered, true).keySet());
                w.tiered = earlier.get(random.nextInt(earlier.size()));
                w.filter.coveredAt(w.tiered);
            } else if (step == 5) {
                // a new leader restores a KV snapshot at the log end
                RowTtlFilterState saved = w.filter.saved(w.next);
                w.filter = new RowTtlFilterClock(w.clock, w.ttlMs);
                w.filter.restored(w.next, saved);
                w.filter.coveredAt(w.tiered);
            }
            for (String key : new ArrayList<>(w.kv.keySet())) {
                if (random.nextInt(3) == 0) {
                    w.expire(key);
                }
            }
        }
        return w;
    }

    /** The time KV compaction compares TTL timestamps with. */
    private long filterTime() {
        return filter == null ? now : filter.getAsLong();
    }

    /** P: the highest readable offset the leader's filter was capped for. */
    long cappedFrom() {
        return filter.cappedFrom();
    }

    /** Writes {@code key}: an insert if it has no value, else an update. */
    DvChangeListWorkload upsert(String key) {
        Long current = kv.get(key);
        batchTimes.putIfAbsent(batchStart, now);
        long time = ttlTimes ? nextTtlTime : now;
        writtenAt.put(key, time);
        if (filter != null) {
            // the write path tracks it before the KV write, at its version's offset
            filter.track(current == null ? next : next + 1, time);
        }
        if (current == null) {
            log.add(new Record(next, key, Kind.INSERT));
            if (rowTtl) {
                // it may follow an expired value: the write path looks its key up in the lake
                handedOver.add(new Entry(next, key, -1));
            }
            kv.put(key, next);
            next++;
        } else {
            log.add(new Record(next, key, Kind.UPDATE_BEFORE));
            log.add(new Record(next + 1, key, Kind.UPDATE_AFTER));
            handedOver.add(new Entry(next + 1, key, current));
            kv.put(key, next + 1);
            next += 2;
        }
        return this;
    }

    /** Deletes {@code key}; nothing if it has no value, as the write path writes nothing then. */
    DvChangeListWorkload delete(String key) {
        Long current = kv.remove(key);
        if (current != null) {
            batchTimes.putIfAbsent(batchStart, now);
            writtenAt.remove(key);
            log.add(new Record(next, key, Kind.DELETE));
            handedOver.add(new Entry(next, key, current));
            next++;
        }
        return this;
    }

    /** Ends a batch: the KV may be snapshotted here. With row TTL, time moves a step on. */
    DvChangeListWorkload endBatch() {
        kvAt.put(next, new HashMap<>(kv));
        batchStart = next;
        now += STEP_MS;
        clock.advanceTime(STEP_MS, TimeUnit.MILLISECONDS);
        return this;
    }

    /**
     * KV compaction removes the value of {@code key} if its TTL has passed, with nothing in the
     * log; nothing otherwise. The KV at the next boundaries lacks it.
     */
    DvChangeListWorkload expire(String key) {
        Long version = kv.get(key);
        if (rowTtl && version != null && writtenAt.get(key) + ttlMs <= filterTime()) {
            kv.remove(key);
            writtenAt.remove(key);
            expiredVersions.add(version);
            // as compaction runs between batches, the KV at this boundary already lacks it
            kvAt.put(next, new HashMap<>(kv));
        }
        return this;
    }

    long end() {
        return next;
    }

    /** The time now, after the last batch. */
    long now() {
        return now;
    }

    /**
     * The commit time of the first batch holding an offset at or after {@code o}, as a leader reads
     * it from the log; -1 if nothing was written there.
     */
    long firstWriteTime(long o) {
        // batches are contiguous: the one holding o starts at or before it
        return o < end() ? batchTimes.floorEntry(o).getValue() : -1;
    }

    /**
     * Whether no version written at or after {@code o} has expired: the bucket is TTL-safe for a
     * readable offset of o.
     */
    boolean safeFrom(long o) {
        for (long version : expiredVersions) {
            if (version >= o) {
                return false;
            }
        }
        return true;
    }

    /**
     * The keys the lake holds at {@code o}: the last change of each key in the log before it, not a
     * delete. With row TTL it may hold keys the KV no longer does.
     */
    Map<String, Long> lakeAt(long o) {
        Map<String, Long> lake = new HashMap<>();
        for (Record r : log) {
            if (r.offset >= o || r.kind == Kind.UPDATE_BEFORE) {
                continue;
            }
            if (r.kind == Kind.DELETE) {
                lake.remove(r.key);
            } else {
                lake.put(r.key, r.offset);
            }
        }
        return lake;
    }

    /** The superseded versions of the server's three ways, as reference implementations. */
    static final class Model {

        private Model() {}

        /**
         * A replay of the log from {@code from}, as the deletion vector replay does: a key's
         * superseded version is the last one replayed, else {@code base}'s, else -1.
         */
        static List<Entry> replay(
                DvChangeListWorkload w, long from, long to, Map<String, Long> base) {
            Map<String, Long> last = new HashMap<>();
            List<Entry> out = new ArrayList<>();
            for (Record r : w.log) {
                if (r.offset < from || r.offset >= to || r.kind == Kind.UPDATE_BEFORE) {
                    continue;
                }
                Long previous = last.containsKey(r.key) ? last.get(r.key) : base.get(r.key);
                last.put(r.key, r.version());
                // with row TTL an insert may follow an expired version, which it supersedes
                if (r.kind != Kind.INSERT || w.rowTtl) {
                    out.add(new Entry(r.offset, r.key, previous == null ? -1 : previous));
                }
            }
            return out;
        }

        /** Today's rebuild: a full replay from the readable offset, with no base. */
        static List<Entry> fullReplay(DvChangeListWorkload w, long o) {
            return classify(replay(w, o, w.end(), Collections.emptyMap()), o);
        }

        /** The live leader's change list: what the write path handed over from o on. */
        static List<Entry> writePath(DvChangeListWorkload w, long o) {
            List<Entry> out = new ArrayList<>();
            for (Entry e : w.handedOver) {
                if (e.offset >= o) {
                    out.add(e);
                }
            }
            return classify(out, o);
        }

        /**
         * A rebuild from a checkpoint: the old leader's change list of readable offset {@code
         * exported} at KV snapshot offset {@code x}, kept from {@code o} on, then a replay from x
         * naming a key's first superseded version from the KV at x.
         */
        static List<Entry> fromCheckpoint(
                DvChangeListWorkload w, List<Entry> checkpoint, long x, long o) {
            List<Entry> out = new ArrayList<>();
            for (Entry e : checkpoint) {
                if (e.offset >= o && e.offset < x) {
                    out.add(e);
                }
            }
            out.addAll(replay(w, x, w.end(), w.kvAt.get(x)));
            return classify(out, o);
        }

        static List<Entry> classify(List<Entry> entries, long o) {
            List<Entry> out = new ArrayList<>(entries.size());
            for (Entry e : entries) {
                out.add(e.classified(o));
            }
            return out;
        }
    }
}
