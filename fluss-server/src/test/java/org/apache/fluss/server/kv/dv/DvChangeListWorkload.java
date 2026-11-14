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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.TreeMap;

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
    // the write path's updates and deletes, with the KV tag of the value each one superseded
    final List<Entry> handedOver = new ArrayList<>();
    // batch boundaries: a KV snapshot's offset, with each live key's version offset there
    final TreeMap<Long, Map<String, Long>> kvAt = new TreeMap<>();
    private final Map<String, Long> kv = new HashMap<>();
    private long next;

    DvChangeListWorkload() {
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

    /** Writes {@code key}: an insert if it has no value, else an update. */
    DvChangeListWorkload upsert(String key) {
        Long current = kv.get(key);
        if (current == null) {
            log.add(new Record(next, key, Kind.INSERT));
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
            log.add(new Record(next, key, Kind.DELETE));
            handedOver.add(new Entry(next, key, current));
            next++;
        }
        return this;
    }

    /** Ends a batch: the KV may be snapshotted here. */
    DvChangeListWorkload endBatch() {
        kvAt.put(next, new HashMap<>(kv));
        return this;
    }

    long end() {
        return next;
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
                if (r.kind != Kind.INSERT) {
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
