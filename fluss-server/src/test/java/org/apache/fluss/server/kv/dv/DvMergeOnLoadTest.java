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

import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.index.PositionIndexStore.BucketData;

import io.github.fluss_contrib.rocksdb.ColumnFamilyHandle;
import io.github.fluss_contrib.rocksdb.EnvOptions;
import io.github.fluss_contrib.rocksdb.MutableColumnFamilyOptions;
import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.SstFileWriter;
import io.github.fluss_contrib.rocksdb.WriteOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests of a full load's ingest of its checkpoint and index changes in one call ({@link
 * DvOwnerIndex#ingestInOrder}) and its compaction ({@link RowPosIndex#compact}): for any chain the
 * tiering service registers, the index and dictionary equal what ingesting the changes one by one
 * gives, before and after the compaction.
 */
class DvMergeOnLoadTest {

    private static final int KEYS = 60;
    // change SSTs of a full load: more than RocksDB's default level-0 trigger to stop writes
    private static final int OVERLAPPING_SSTS = 40;

    @TempDir Path tempDir;

    /**
     * Random chains: a checkpoint or none, then plain changes with puts and tombstones, rewrites
     * with and without removed files, and Paimon-style moves of keys whose rows were in unindexed
     * level-0 files. Keys move again and again; a change may have more than one SST, also
     * overlapping. One call gives every key the position, and the dictionary the files, the change
     * by change ingest gives, compacted or not, and both equal applying the changes in order.
     */
    @Test
    void testOneCallGivesTheIndexOfIngestingOneByOne() throws Exception {
        for (int seed = 0; seed < 50; seed++) {
            ChainModel chain = ChainModel.random(new Random(seed));
            assertThat(DvOwnerIndex.mergesInOrder(chain.downloads(write(seed, "probe"))))
                    .as("seed %s", seed)
                    .isTrue();

            Path dir = Files.createDirectories(tempDir.resolve("seed-" + seed));
            Map<String, FilePos> oneByOne;
            Map<Integer, String> oneByOneDict;
            Map<String, FilePos> oneCall;
            Map<String, FilePos> compacted;
            Map<Integer, String> oneCallDict;
            try (DerivedDvManager a =
                            DvTestManagers.newManager(
                                    DvRocksDB.open(dir.resolve("one-by-one").toString()),
                                    () -> 0L);
                    DerivedDvManager b =
                            DvTestManagers.newManager(
                                    DvRocksDB.open(dir.resolve("one-call").toString()), () -> 0L)) {
                DvOwner ownerA = owner(a);
                List<BucketData> downloadsA = chain.downloads(write(seed, "a"));
                ownerA.db.rowPosIndex().clear();
                for (BucketData d : downloadsA) {
                    ownerA.index.ingest(d);
                }
                oneByOne = positions(ownerA.db);
                oneByOneDict = ownerA.db.fileDict().idToPathEntries(null);

                DvOwner ownerB = owner(b);
                List<BucketData> downloadsB = chain.downloads(write(seed, "b"));
                ownerB.db.rowPosIndex().clear();
                ownerB.index.ingestInOrder(downloadsB);
                oneCall = positions(ownerB.db);
                oneCallDict = ownerB.db.fileDict().idToPathEntries(null);
                try (RowPosIndex.Compaction c = new RowPosIndex.Compaction()) {
                    assertThat(ownerB.db.rowPosIndex().compact(c)).isTrue();
                }
                compacted = positions(ownerB.db);
            }
            assertThat(oneCall).as("seed %s", seed).isEqualTo(oneByOne);
            assertThat(compacted).as("seed %s", seed).isEqualTo(oneByOne);
            assertThat(oneCallDict).as("seed %s", seed).isEqualTo(oneByOneDict);
            assertThat(oneCall).as("seed %s", seed).isEqualTo(chain.expected());
        }
    }

    /**
     * A rewrite whose newest file is not newer than every file of the entries before it, as a chain
     * applied out of order would have, is not ingested in one call: one by one, its filter may keep
     * positions one call would overwrite.
     */
    @Test
    void testARewriteOlderThanAnEntryBeforeItIsNotIngestedInOneCall() {
        BucketData checkpoint =
                new BucketData(
                        Collections.singletonList("c.sst"), files(1, 5), Collections.emptyList());
        BucketData olderRewrite =
                new BucketData(
                        Collections.singletonList("r.sst"),
                        files(3),
                        Collections.singletonList(name(1)));
        BucketData newerRewrite =
                new BucketData(
                        Collections.singletonList("r.sst"),
                        files(6),
                        Collections.singletonList(name(1)));
        BucketData olderPlain =
                new BucketData(
                        Collections.singletonList("p.sst"), files(2), Collections.emptyList());
        assertThat(DvOwnerIndex.mergesInOrder(Arrays.asList(checkpoint, olderRewrite))).isFalse();
        assertThat(DvOwnerIndex.mergesInOrder(Arrays.asList(checkpoint, newerRewrite))).isTrue();
        // only a rewrite's filter depends on the order
        assertThat(DvOwnerIndex.mergesInOrder(Arrays.asList(checkpoint, olderPlain))).isTrue();
    }

    /**
     * A compaction cancelled while it runs ends early and leaves the index as it was; one cancelled
     * before it starts does not run. Either way the index answers as before, and compacts later.
     */
    @Test
    void testACancelledCompactionEndsEarlyAndKeepsTheIndex() throws Exception {
        int keys = 1_000_000;
        List<String> ssts = new ArrayList<>();
        ssts.add(writeIntSst(tempDir.resolve("bottom.sst"), keys, 0, 1, 1));
        // three overlapping changes, each moving every third key
        for (int j = 0; j < 3; j++) {
            ssts.add(writeIntSst(tempDir.resolve("change-" + j + ".sst"), keys, j, 3, 2 + j));
        }
        try (DvRocksDB db = DvRocksDB.open(tempDir.resolve("db").toString())) {
            RowPosIndex index = db.rowPosIndex();
            RocksDB rocksDb = field(index, "db");
            // only the compactions under test run
            rocksDb.setOptions(
                    field(index, "cfHandle"),
                    MutableColumnFamilyOptions.builder().setDisableAutoCompactions(true).build());
            for (String sst : ssts) {
                index.ingestExternalFile(Collections.singletonList(sst));
            }
            try (RowPosIndex.Compaction before = new RowPosIndex.Compaction()) {
                before.cancel();
                assertThat(index.compact(before)).isFalse();
            }
            assertMovedEveryThirdKey(index, keys);

            ExecutorService thread = Executors.newSingleThreadExecutor();
            try (RowPosIndex.Compaction running = new RowPosIndex.Compaction()) {
                Future<Boolean> compacted = thread.submit(() -> index.compact(running));
                long deadline = System.currentTimeMillis() + 30_000;
                while (rocksDb.getLongProperty("rocksdb.num-running-compactions") == 0) {
                    assertThat(compacted).isNotDone();
                    assertThat(System.currentTimeMillis()).isLessThan(deadline);
                    Thread.sleep(1);
                }
                running.cancel();
                assertThat(compacted.get(10, TimeUnit.SECONDS)).isFalse();
            } finally {
                thread.shutdownNow();
            }
            assertMovedEveryThirdKey(index, keys);

            try (RowPosIndex.Compaction later = new RowPosIndex.Compaction()) {
                assertThat(index.compact(later)).isTrue();
            }
            assertMovedEveryThirdKey(index, keys);
        }
    }

    /**
     * A full load's one call puts each change SST in level 0. However many there are, the other
     * column families of the bucket's RocksDB keep taking writes: a write of the change list
     * neither slows down nor stops while the compaction catches up.
     */
    @Test
    void testOneCallOfManyChangesDoesNotStallTheChangeList() throws Exception {
        List<String> ssts = overlappingSsts("stall", OVERLAPPING_SSTS);
        try (DvRocksDB db = DvRocksDB.open(tempDir.resolve("db").toString())) {
            RowPosIndex index = db.rowPosIndex();
            RocksDB rocksDb = field(index, "db");
            index.ingestExternalFile(ssts);
            // more level-0 files than RocksDB's default trigger to stop writes
            assertThat(
                            Long.parseLong(
                                    rocksDb.getProperty(
                                            field(index, "cfHandle"),
                                            "rocksdb.num-files-at-level0")))
                    .isGreaterThanOrEqualTo(36L);
            assertThat(rocksDb.getLongProperty("rocksdb.is-write-stopped")).isZero();
            assertThat(rocksDb.getLongProperty("rocksdb.actual-delayed-write-rate")).isZero();
            List<ColumnFamilyHandle> handles = field(db, "cfHandles");
            try (WriteOptions noStall =
                    new WriteOptions().setDisableWAL(true).setNoSlowdown(true)) {
                // fails rather than wait if the write would stall
                rocksDb.put(handles.get(3), noStall, intKey(1), intKey(1));
            }
        }
    }

    /**
     * Right after a full load's one call, an automatic compaction of level 0 usually runs, and the
     * load's compaction waits behind it. A close cancels the waiting compaction at once rather than
     * when the automatic one ends: it takes a fraction of the automatic compaction's time.
     */
    @Test
    void testACloseEndsACompactionWaitingBehindAnAutomaticOne() throws Exception {
        List<String> ssts = overlappingSsts("control", OVERLAPPING_SSTS);
        long automaticMs;
        try (DvRocksDB db = DvRocksDB.open(tempDir.resolve("control").toString())) {
            RowPosIndex index = db.rowPosIndex();
            long start = System.nanoTime();
            index.ingestExternalFile(ssts);
            awaitCompactions(index, true);
            awaitCompactions(index, false);
            automaticMs = (System.nanoTime() - start) / 1_000_000;
        }

        ssts = overlappingSsts("close", OVERLAPPING_SSTS);
        DvRocksDB db = DvRocksDB.open(tempDir.resolve("db").toString());
        DvDbLeases leases = new DvDbLeases(db);
        RowPosIndex index = db.rowPosIndex();
        index.ingestExternalFile(ssts);
        awaitCompactions(index, true);
        ExecutorService thread = Executors.newSingleThreadExecutor();
        try (RowPosIndex.Compaction c = new RowPosIndex.Compaction()) {
            Runnable cancel = c::cancel;
            assertThat(leases.tryAcquire(cancel)).isTrue();
            Future<Boolean> compacted =
                    thread.submit(
                            () -> {
                                try {
                                    return index.compact(c);
                                } finally {
                                    leases.release(cancel);
                                }
                            });
            // the compaction starts and waits for the automatic one
            Thread.sleep(20);
            long start = System.nanoTime();
            leases.close(60_000);
            long closeMs = (System.nanoTime() - start) / 1_000_000;
            assertThat(closeMs)
                    .as("automatic compaction %s ms", automaticMs)
                    .isLessThan(automaticMs / 2);
            // false, or true if nothing was left to compact when it woke
            compacted.get(30, TimeUnit.SECONDS);
            assertThat(leases.dbClosed().get(30, TimeUnit.SECONDS)).isNull();
        } finally {
            thread.shutdownNow();
        }
    }

    // ------------------------------------------------------------------------------------------

    /** Writes the SSTs of a chain under a directory of their own. */
    private SstWriter write(int seed, String copy) throws Exception {
        Path dir = Files.createDirectories(tempDir.resolve("ssts-" + seed + "-" + copy));
        int[] n = new int[1];
        return entries -> writeSst(dir.resolve("sst-" + n[0]++ + ".sst"), entries);
    }

    private static DvOwner owner(DerivedDvManager manager) throws Exception {
        Field f = DerivedDvManager.class.getDeclaredField("owner");
        f.setAccessible(true);
        return (DvOwner) f.get(manager);
    }

    /** Writes keys {@code from}, {@code from + step}, ... below {@code keys}, each in a file. */
    private static String writeIntSst(Path path, int keys, int from, int step, int fileId)
            throws Exception {
        try (EnvOptions env = new EnvOptions();
                Options options = new Options();
                SstFileWriter writer = new SstFileWriter(env, options)) {
            writer.open(path.toString());
            for (int k = from; k < keys; k += step) {
                writer.put(intKey(k), new FilePos(fileId, k).encode());
            }
            writer.finish();
        }
        return path.toString();
    }

    /** SSTs that each name the same 50K keys, in a file of their own. */
    private List<String> overlappingSsts(String tag, int n) throws Exception {
        Path dir = Files.createDirectories(tempDir.resolve("overlapping-" + tag));
        List<String> out = new ArrayList<>();
        for (int f = 0; f < n; f++) {
            out.add(writeIntSst(dir.resolve("c-" + f + ".sst"), 50_000, 0, 1, f + 1));
        }
        return out;
    }

    /** Waits until a compaction of the index runs, or until none runs or is pending. */
    private static void awaitCompactions(RowPosIndex index, boolean running) throws Exception {
        RocksDB db = field(index, "db");
        ColumnFamilyHandle cf = field(index, "cfHandle");
        long deadline = System.currentTimeMillis() + 60_000;
        while (true) {
            boolean busy =
                    db.getLongProperty("rocksdb.num-running-compactions") > 0
                            || (!running
                                    && db.getLongProperty(cf, "rocksdb.compaction-pending") > 0);
            if (busy == running) {
                return;
            }
            assertThat(System.currentTimeMillis()).isLessThan(deadline);
            Thread.sleep(1);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T field(Object o, String name) throws Exception {
        Field f = o.getClass().getDeclaredField(name);
        f.setAccessible(true);
        return (T) f.get(o);
    }

    /** Checks a sample of the keys: each change moved every third key from the bottom SST. */
    private static void assertMovedEveryThirdKey(RowPosIndex index, int keys) throws Exception {
        List<byte[]> sample = new ArrayList<>();
        for (int k = 0; k < keys; k += 997) {
            sample.add(intKey(k));
        }
        List<FilePos> found = index.multiGetByKeys(sample);
        for (int i = 0; i < sample.size(); i++) {
            int k = i * 997;
            assertThat(found.get(i)).as("key %s", k).isEqualTo(new FilePos(2 + k % 3, k));
        }
    }

    /** Every key of the universe the index has a position of. */
    private static Map<String, FilePos> positions(DvRocksDB db) throws Exception {
        List<byte[]> keys = new ArrayList<>();
        for (int k = 0; k < KEYS; k++) {
            keys.add(key(k));
        }
        List<FilePos> found = db.rowPosIndex().multiGetByKeys(keys);
        Map<String, FilePos> out = new TreeMap<>();
        for (int k = 0; k < KEYS; k++) {
            if (found.get(k) != null) {
                out.put("k" + k, found.get(k));
            }
        }
        return out;
    }

    private static String writeSst(Path path, TreeMap<byte[], FilePos> entries) throws Exception {
        try (EnvOptions env = new EnvOptions();
                Options options = new Options();
                SstFileWriter writer = new SstFileWriter(env, options)) {
            writer.open(path.toString());
            for (Map.Entry<byte[], FilePos> e : entries.entrySet()) {
                if (e.getValue() == null) {
                    writer.delete(e.getKey());
                } else {
                    writer.put(e.getKey(), e.getValue().encode());
                }
            }
            writer.finish();
        }
        return path.toString();
    }

    private static Map<Integer, String> files(int... ids) {
        Map<Integer, String> out = new LinkedHashMap<>();
        for (int id : ids) {
            out.put(id, name(id));
        }
        return out;
    }

    private static String name(int fileId) {
        return "data-" + fileId + ".parquet";
    }

    private static byte[] key(int k) {
        return ("k" + k).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] intKey(int k) {
        return ByteBuffer.allocate(4).putInt(k).array();
    }

    private static int compare(byte[] a, byte[] b) {
        for (int i = 0; i < Math.min(a.length, b.length); i++) {
            int c = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (c != 0) {
                return c;
            }
        }
        return a.length - b.length;
    }

    /** Writes one SST of an entry's entries, a null position being a tombstone. */
    private interface SstWriter {
        String write(TreeMap<byte[], FilePos> entries) throws Exception;
    }

    /**
     * A random chain as the tiering service registers it: file ids grow with every entry, a rewrite
     * moves the rows of the files it removes, and of unindexed level-0 rows, into the files it
     * adds. Keeps the index applying the chain in order gives.
     */
    private static final class ChainModel {
        // the entries in chain order: each SST's entries, its files and the files it removes
        private final List<List<TreeMap<byte[], FilePos>>> ssts = new ArrayList<>();
        private final List<Map<Integer, String>> added = new ArrayList<>();
        private final List<List<String>> removed = new ArrayList<>();
        private final Map<String, FilePos> index = new TreeMap<>();
        private final List<Integer> liveFiles = new ArrayList<>();
        private int nextFileId = 1;

        static ChainModel random(Random r) {
            ChainModel chain = new ChainModel();
            if (r.nextInt(10) < 7) {
                chain.checkpoint(r);
            }
            int changes = 1 + r.nextInt(12);
            for (int i = 0; i < changes; i++) {
                int kind = r.nextInt(10);
                if (kind < 4) {
                    chain.plain(r);
                } else if (kind < 9 && !chain.liveFiles.isEmpty()) {
                    chain.rewrite(r, true);
                } else {
                    chain.rewrite(r, false);
                }
            }
            return chain;
        }

        List<BucketData> downloads(SstWriter writer) throws Exception {
            List<BucketData> out = new ArrayList<>();
            for (int i = 0; i < ssts.size(); i++) {
                List<String> paths = new ArrayList<>();
                for (TreeMap<byte[], FilePos> entries : ssts.get(i)) {
                    paths.add(writer.write(entries));
                }
                out.add(new BucketData(paths, added.get(i), removed.get(i)));
            }
            return out;
        }

        Map<String, FilePos> expected() {
            return index;
        }

        private void checkpoint(Random r) {
            int[] ids = newFiles(1 + r.nextInt(3));
            TreeMap<byte[], FilePos> entries = entries();
            for (int k = 0; k < KEYS; k++) {
                if (r.nextInt(3) > 0) {
                    entries.put(key(k), new FilePos(ids[r.nextInt(ids.length)], k));
                }
            }
            add(Collections.singletonList(entries), ids, Collections.emptyList());
        }

        /** New rows of some keys, updates of others, and deletes. */
        private void plain(Random r) {
            int[] ids = newFiles(1 + r.nextInt(2));
            TreeMap<byte[], FilePos> entries = entries();
            for (int k = 0; k < KEYS; k++) {
                int what = r.nextInt(10);
                if (what < 3) {
                    entries.put(key(k), new FilePos(ids[r.nextInt(ids.length)], r.nextInt(1000)));
                } else if (what < 4 && index.containsKey("k" + k)) {
                    entries.put(key(k), null);
                }
            }
            add(split(r, entries), ids, Collections.emptyList());
        }

        /**
         * A compaction: the rows of the files it removes move to the files it adds, some of them
         * deleted on the way, and with them rows of keys from unindexed level-0 files, whose
         * indexed position, if any, is in a file it does not remove. Without removed files, it only
         * moves level-0 rows.
         */
        private void rewrite(Random r, boolean removesFiles) {
            List<Integer> inputs = new ArrayList<>();
            if (removesFiles) {
                int n = 1 + r.nextInt(Math.min(2, liveFiles.size()));
                List<Integer> live = new ArrayList<>(liveFiles);
                Collections.shuffle(live, r);
                inputs.addAll(live.subList(0, n));
            }
            int[] ids = newFiles(1 + r.nextInt(2));
            TreeMap<byte[], FilePos> moves = entries();
            TreeMap<byte[], FilePos> deletes = entries();
            for (int k = 0; k < KEYS; k++) {
                FilePos at = index.get("k" + k);
                boolean input = at != null && inputs.contains(at.fileId());
                if (input && r.nextInt(8) == 0) {
                    deletes.put(key(k), null);
                } else if (input || r.nextInt(8) == 0) {
                    moves.put(key(k), new FilePos(ids[r.nextInt(ids.length)], r.nextInt(1000)));
                }
            }
            List<TreeMap<byte[], FilePos>> parts = new ArrayList<>(split(r, moves));
            if (!deletes.isEmpty()) {
                // tombstones of a rewrite come in an SST of their own, or with its moves
                if (r.nextBoolean()) {
                    parts.add(deletes);
                } else {
                    parts.get(parts.size() - 1).putAll(deletes);
                }
            }
            List<String> names = new ArrayList<>();
            for (int id : inputs) {
                names.add(name(id));
                liveFiles.remove(Integer.valueOf(id));
            }
            add(parts, ids, names);
        }

        /** Splits a change's entries into one or two SSTs, which may both name a key. */
        private static List<TreeMap<byte[], FilePos>> split(
                Random r, TreeMap<byte[], FilePos> entries) {
            if (entries.size() < 2 || r.nextBoolean()) {
                return new ArrayList<>(Collections.singletonList(entries));
            }
            TreeMap<byte[], FilePos> first = entries();
            TreeMap<byte[], FilePos> second = entries();
            for (Map.Entry<byte[], FilePos> e : entries.entrySet()) {
                (r.nextBoolean() ? first : second).put(e.getKey(), e.getValue());
            }
            if (!first.isEmpty() && r.nextInt(4) == 0) {
                // the first SST also names a key of the second, which the second overrides
                byte[] k = second.isEmpty() ? first.firstKey() : second.firstKey();
                first.put(k, new FilePos(0, 0));
                if (second.isEmpty()) {
                    second.put(k, entries.get(k));
                }
            }
            List<TreeMap<byte[], FilePos>> out = new ArrayList<>();
            if (!first.isEmpty()) {
                out.add(first);
            }
            if (!second.isEmpty()) {
                out.add(second);
            }
            return out;
        }

        private void add(List<TreeMap<byte[], FilePos>> parts, int[] ids, List<String> names) {
            for (TreeMap<byte[], FilePos> part : parts) {
                for (Map.Entry<byte[], FilePos> e : part.entrySet()) {
                    String k = new String(e.getKey(), StandardCharsets.UTF_8);
                    if (e.getValue() == null) {
                        index.remove(k);
                    } else {
                        index.put(k, e.getValue());
                    }
                }
            }
            ssts.add(parts);
            Map<Integer, String> dict = new HashMap<>();
            for (int id : ids) {
                dict.put(id, name(id));
                liveFiles.add(id);
            }
            added.add(dict);
            removed.add(names);
        }

        private int[] newFiles(int n) {
            int[] ids = new int[n];
            for (int i = 0; i < n; i++) {
                ids[i] = nextFileId++;
            }
            return ids;
        }

        private static TreeMap<byte[], FilePos> entries() {
            return new TreeMap<>(DvMergeOnLoadTest::compare);
        }
    }
}
