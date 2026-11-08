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
import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.SstFileWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

/**
 * Times a full load's ingest of a checkpoint of 1M keys and 14 index changes, half of them
 * rewrites, in the ways of {@link Variant}, and a lookup of 100K keys right after, once the
 * background compaction settled, and after a {@code compactRange}. Reports, asserts nothing; run
 * with {@code -Ddv.mergeBenchmark=true}.
 */
@EnabledIfSystemProperty(named = "dv.mergeBenchmark", matches = "true")
class DvMergeOnLoadBenchmark {

    private static final int KEYS = 1_000_000;
    private static final int CHANGES = 14;
    // keys a plain change updates, and deletes
    private static final int UPDATES = 50_000;
    private static final int DELETES = 5_000;
    // files of the checkpoint; a rewrite compacts two of the live files
    private static final int CHECKPOINT_FILES = 20;
    private static final int LOOKUPS = 100_000;

    @TempDir Path tempDir;

    @Test
    void benchmark() throws Exception {
        for (int round = 0; round < 3; round++) {
            for (Variant v : Variant.values()) {
                System.out.println(
                        "DV merge-on-load benchmark round " + round + " " + run(v, round));
            }
        }
    }

    /**
     * How a full load ingests: one by one with the rewrite filter (before), or all SSTs in one
     * call, later ones over earlier ones, left to the background compaction or compacted in the
     * load.
     */
    private enum Variant {
        ONE_BY_ONE,
        ONE_CALL,
        ONE_CALL_COMPACTED
    }

    private String run(Variant variant, int round) throws Exception {
        Path dir = Files.createDirectories(tempDir.resolve(variant + "-" + round));
        List<BucketData> downloads = chain(Files.createDirectories(dir.resolve("ssts")));
        long downloadBytes = 0;
        for (String sst : ssts(downloads)) {
            downloadBytes += Files.size(Paths.get(sst));
        }
        Path dbDir = dir.resolve("db");
        try (DerivedDvManager manager =
                DvTestManagers.newManager(DvRocksDB.open(dbDir.toString()), () -> 0L)) {
            DvOwner owner = owner(manager);
            RocksDB db = field(owner.db.rowPosIndex(), "db");
            ColumnFamilyHandle cf = field(owner.db.rowPosIndex(), "cfHandle");
            long loadCompactMs = -1;
            long start = System.nanoTime();
            switch (variant) {
                case ONE_BY_ONE:
                    owner.db.rowPosIndex().clear();
                    for (BucketData d : downloads) {
                        owner.index.ingest(d);
                    }
                    break;
                default:
                    owner.db.rowPosIndex().clear();
                    owner.index.ingestInOrder(downloads);
            }
            long ingestMs = ms(start);
            long level0Files = Long.parseLong(db.getProperty(cf, "rocksdb.num-files-at-level0"));
            long afterIngestBytes = size(dbDir);
            if (variant == Variant.ONE_CALL_COMPACTED) {
                start = System.nanoTime();
                try (RowPosIndex.Compaction c = new RowPosIndex.Compaction()) {
                    owner.db.rowPosIndex().compact(c);
                }
                loadCompactMs = ms(start);
            }
            long lookupMs = lookups(owner, false);

            start = System.nanoTime();
            awaitCompactions(db, cf);
            long settleMs = ms(start);
            long settledLookupMs = lookups(owner, false);
            long settledMissMs = lookups(owner, true);
            long settledBytes = size(dbDir);

            start = System.nanoTime();
            db.compactRange(cf);
            long compactMs = ms(start);
            long compactedLookupMs = lookups(owner, false);
            long compactedBytes = size(dbDir);
            return String.format(
                    "%s: ingest %d ms (%d level-0 files), compactRange in the load %d ms, lookups"
                            + " %d ms; background compaction settles in %d ms, lookups %d ms,"
                            + " absent keys %d ms; compactRange %d ms, lookups %d ms; downloads %d"
                            + " MB, db after ingest %d MB, after settling %d MB,"
                            + " after compactRange %d MB",
                    variant,
                    ingestMs,
                    level0Files,
                    loadCompactMs,
                    lookupMs,
                    settleMs,
                    settledLookupMs,
                    settledMissMs,
                    compactMs,
                    compactedLookupMs,
                    downloadBytes >> 20,
                    afterIngestBytes >> 20,
                    settledBytes >> 20,
                    compactedBytes >> 20);
        }
    }

    /**
     * Looks up 100K random keys in batches of 10K, or as many absent keys between them; returns the
     * ms it took.
     */
    private static long lookups(DvOwner owner, boolean absent) throws Exception {
        Random r = new Random(7);
        List<byte[]> keys = new ArrayList<>(LOOKUPS);
        for (int i = 0; i < LOOKUPS; i++) {
            byte[] key = key(r.nextInt(KEYS));
            // one byte longer: sorts between this key and the next
            keys.add(absent ? Arrays.copyOf(key, key.length + 1) : key);
        }
        long start = System.nanoTime();
        for (int from = 0; from < LOOKUPS; from += 10_000) {
            owner.db.rowPosIndex().multiGetByKeys(keys.subList(from, from + 10_000));
        }
        return ms(start);
    }

    /** Waits until the column family has no compaction pending or running. */
    private static void awaitCompactions(RocksDB db, ColumnFamilyHandle cf) throws Exception {
        long deadline = System.currentTimeMillis() + 120_000;
        int quiet = 0;
        while (quiet < 3 && System.currentTimeMillis() < deadline) {
            boolean busy =
                    db.getLongProperty(cf, "rocksdb.compaction-pending") > 0
                            || db.getLongProperty("rocksdb.num-running-compactions") > 0;
            quiet = busy ? 0 : quiet + 1;
            Thread.sleep(50);
        }
    }

    private static long size(Path dir) throws Exception {
        try (Stream<Path> files = Files.walk(dir)) {
            return files.filter(Files::isRegularFile)
                    .mapToLong(
                            p -> {
                                try {
                                    return Files.size(p);
                                } catch (Exception e) {
                                    return 0;
                                }
                            })
                    .sum();
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T field(Object o, String name) throws Exception {
        Field f = o.getClass().getDeclaredField(name);
        f.setAccessible(true);
        return (T) f.get(o);
    }

    private static long ms(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }

    /**
     * A checkpoint of every key spread over 20 files, then 14 changes: plain ones update and delete
     * keys into a file of their own, rewrites compact two live files into one.
     */
    private static List<BucketData> chain(Path dir) throws Exception {
        Random r = new Random(11);
        int nextFileId = 1;
        // key -> file id
        int[] fileOf = new int[KEYS];
        List<Integer> live = new ArrayList<>();
        Map<Integer, String> checkpointFiles = new HashMap<>();
        for (int i = 0; i < CHECKPOINT_FILES; i++) {
            live.add(nextFileId);
            checkpointFiles.put(nextFileId, name(nextFileId));
            nextFileId++;
        }
        List<BucketData> out = new ArrayList<>();
        try (Writer w = new Writer(dir.resolve("checkpoint.sst"))) {
            for (int k = 0; k < KEYS; k++) {
                fileOf[k] = live.get(k % CHECKPOINT_FILES);
                w.put(k, fileOf[k]);
            }
            out.add(new BucketData(w.paths(), checkpointFiles, Collections.emptyList()));
        }
        for (int c = 0; c < CHANGES; c++) {
            int file = nextFileId++;
            Map<Integer, String> added = Collections.singletonMap(file, name(file));
            if (c % 2 == 0) {
                boolean[] deleted = new boolean[KEYS];
                boolean[] updated = new boolean[KEYS];
                for (int i = 0; i < DELETES; i++) {
                    deleted[r.nextInt(KEYS)] = true;
                }
                for (int i = 0; i < UPDATES; i++) {
                    updated[r.nextInt(KEYS)] = true;
                }
                try (Writer w = new Writer(dir.resolve("change-" + c + ".sst"))) {
                    for (int k = 0; k < KEYS; k++) {
                        if (deleted[k]) {
                            w.delete(k);
                            fileOf[k] = 0;
                        } else if (updated[k]) {
                            w.put(k, file);
                            fileOf[k] = file;
                        }
                    }
                    out.add(new BucketData(w.paths(), added, Collections.emptyList()));
                }
                live.add(file);
            } else {
                Collections.shuffle(live, r);
                List<Integer> inputs = new ArrayList<>(live.subList(0, 2));
                live.removeAll(inputs);
                live.add(file);
                List<String> removed = new ArrayList<>();
                for (int input : inputs) {
                    removed.add(name(input));
                }
                try (Writer w = new Writer(dir.resolve("change-" + c + ".sst"))) {
                    for (int k = 0; k < KEYS; k++) {
                        if (inputs.contains(fileOf[k])) {
                            w.put(k, file);
                            fileOf[k] = file;
                        }
                    }
                    out.add(new BucketData(w.paths(), added, removed));
                }
            }
        }
        return out;
    }

    private static DvOwner owner(DerivedDvManager manager) throws Exception {
        Field f = DerivedDvManager.class.getDeclaredField("owner");
        f.setAccessible(true);
        return (DvOwner) f.get(manager);
    }

    private static List<String> ssts(List<BucketData> downloads) {
        List<String> out = new ArrayList<>();
        for (BucketData d : downloads) {
            out.addAll(d.localSsts);
        }
        return out;
    }

    private static String name(int fileId) {
        return "data-" + fileId + ".parquet";
    }

    /** Keys in increasing byte order: big-endian ints. */
    private static byte[] key(int k) {
        return ByteBuffer.allocate(4).putInt(k).array();
    }

    /** Writes one SST in key order. */
    private static final class Writer implements AutoCloseable {
        private final EnvOptions env = new EnvOptions();
        private final Options options = new Options();
        private final SstFileWriter writer = new SstFileWriter(env, options);
        private final String path;
        private boolean empty = true;
        private long position;

        Writer(Path path) throws Exception {
            this.path = path.toString();
            writer.open(this.path);
        }

        void put(int k, int fileId) throws Exception {
            writer.put(key(k), new FilePos(fileId, position++).encode());
            empty = false;
        }

        void delete(int k) throws Exception {
            writer.delete(key(k));
            empty = false;
        }

        List<String> paths() {
            return Collections.singletonList(path);
        }

        @Override
        public void close() throws Exception {
            if (!empty) {
                writer.finish();
            }
            writer.close();
            options.close();
            env.close();
        }
    }
}
