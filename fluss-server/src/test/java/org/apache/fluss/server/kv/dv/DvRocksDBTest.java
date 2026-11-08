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

import io.github.fluss_contrib.rocksdb.Snapshot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.apache.fluss.server.kv.dv.RowPosIndexTest.key;
import static org.apache.fluss.server.kv.dv.RowPosIndexTest.readSst;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/** Test for {@link DvRocksDB}. */
class DvRocksDBTest {

    @TempDir Path tempDir;

    @Test
    void testOpenWriteAndReopen() throws Exception {
        String path = tempDir.resolve("db").toString();
        try (DvRocksDB dvRocksDB = DvRocksDB.open(path)) {
            dvRocksDB.rowPosIndex().put(key("k1"), new FilePos(10, 100L));
            dvRocksDB.fileDict().put(10, "/data/file10.parquet");
            assertThat(dvRocksDB.rowPosIndex().getByKey(key("k1")))
                    .isEqualTo(new FilePos(10, 100L));
            assertThat(dvRocksDB.fileDict().getFilePath(10)).isEqualTo("/data/file10.parquet");
        }
        // the column families are opened again on an existing database
        try (DvRocksDB dvRocksDB = DvRocksDB.open(path)) {
            assertThat(dvRocksDB.fileDict().getFileId("/data/file10.parquet")).isEqualTo(10);
        }
    }

    /**
     * A tablet server opens one DV RocksDB per leader of a table with deletion vectors, so its
     * memory and open files are bounded: no 32 MB block cache and 64 MB write buffers of its own,
     * no unlimited open files.
     */
    @Test
    void testOpensWithBoundedResources() throws Exception {
        java.io.File dir = tempDir.resolve("bounded").toFile();
        try (DvRocksDB ignored = DvRocksDB.open(dir.getPath())) {
            java.util.Map<String, String> options = latestOptions(dir);
            assertThat(Integer.parseInt(options.get("max_open_files"))).isBetween(1, 256);
            assertThat(Long.parseLong(options.get("write_buffer_size")))
                    .isLessThanOrEqualTo(8L << 20);
            assertThat(Long.parseLong(options.get("max_write_buffer_number")))
                    .isLessThanOrEqualTo(2L);
        }
    }

    /** With the KV tablets' block cache and write buffer manager, it uses theirs. */
    @Test
    void testUsesTheSharedBlockCache() throws Exception {
        // the cache is created before any RocksDB, which would load the native library
        io.github.fluss_contrib.rocksdb.RocksDB.loadLibrary();
        try (io.github.fluss_contrib.rocksdb.Cache shared =
                        new io.github.fluss_contrib.rocksdb.LRUCache(64L << 20);
                io.github.fluss_contrib.rocksdb.WriteBufferManager writeBuffers =
                        new io.github.fluss_contrib.rocksdb.WriteBufferManager(32L << 20, shared);
                DvRocksDB dvRocksDB =
                        DvRocksDB.open(
                                tempDir.resolve("shared").toString(), shared, writeBuffers)) {
            assertThat(dvRocksDB.blockCacheCapacity()).isEqualTo(64L << 20);
            dvRocksDB.rowPosIndex().put(key("k1"), new FilePos(10, 100L));
            assertThat(dvRocksDB.rowPosIndex().getByKey(key("k1")))
                    .isEqualTo(new FilePos(10, 100L));
        }
        try (DvRocksDB own = DvRocksDB.open(tempDir.resolve("own").toString())) {
            assertThat(own.blockCacheCapacity()).isEqualTo(8L << 20);
        }
    }

    /** The options last written to the RocksDB's OPTIONS file, the last value of each name. */
    private static java.util.Map<String, String> latestOptions(java.io.File dir)
            throws java.io.IOException {
        java.io.File latest = null;
        for (java.io.File f : dir.listFiles()) {
            if (f.getName().startsWith("OPTIONS-")
                    && (latest == null || f.getName().compareTo(latest.getName()) > 0)) {
                latest = f;
            }
        }
        java.util.Map<String, String> options = new java.util.HashMap<>();
        for (String line : java.nio.file.Files.readAllLines(latest.toPath())) {
            int eq = line.indexOf('=');
            if (eq > 0) {
                options.put(line.substring(0, eq).trim(), line.substring(eq + 1).trim());
            }
        }
        return options;
    }

    @Test
    void testSnapshotIsPointInTimeView() throws Exception {
        try (DvRocksDB dvRocksDB = DvRocksDB.open(tempDir.resolve("db").toString())) {
            dvRocksDB.rowPosIndex().put(key("k1"), new FilePos(10, 100L));
            dvRocksDB.rowPosIndex().put(key("k3"), new FilePos(10, 300L));
            dvRocksDB.fileDict().put(10, "/data/file10.parquet");

            Snapshot snapshot = dvRocksDB.snapshot();
            try {
                // writes after the snapshot: an insert, an overwrite, a delete, a new file
                dvRocksDB.rowPosIndex().put(key("k2"), new FilePos(20, 200L));
                dvRocksDB.rowPosIndex().put(key("k1"), new FilePos(20, 111L));
                dvRocksDB.rowPosIndex().delete(key("k3"));
                dvRocksDB.fileDict().put(20, "/data/file20.parquet");

                String atSnapshot = tempDir.resolve("at-snapshot.sst").toString();
                assertThat(dvRocksDB.rowPosIndex().exportTo(atSnapshot, snapshot, null))
                        .isEqualTo(2L);
                assertThat(readSst(atSnapshot))
                        .containsExactly(
                                entry("k1", new FilePos(10, 100L)),
                                entry("k3", new FilePos(10, 300L)));
                assertThat(dvRocksDB.fileDict().idToPathEntries(snapshot))
                        .containsOnly(entry(10, "/data/file10.parquet"));

                // without a snapshot the latest state is visible
                String latest = tempDir.resolve("latest.sst").toString();
                assertThat(dvRocksDB.rowPosIndex().exportTo(latest, null, null)).isEqualTo(2L);
                assertThat(readSst(latest))
                        .containsExactly(
                                entry("k1", new FilePos(20, 111L)),
                                entry("k2", new FilePos(20, 200L)));
                assertThat(dvRocksDB.fileDict().idToPathEntries(null))
                        .containsOnly(
                                entry(10, "/data/file10.parquet"),
                                entry(20, "/data/file20.parquet"));
            } finally {
                dvRocksDB.releaseSnapshot(snapshot);
            }
        }
    }
}
