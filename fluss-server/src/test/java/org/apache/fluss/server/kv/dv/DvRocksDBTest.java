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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.Snapshot;
import org.rocksdb.SstFileReader;
import org.rocksdb.SstFileReaderIterator;
import org.rocksdb.WriteBatch;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/** Test for {@link DvRocksDB}. */
class DvRocksDBTest {

    @TempDir Path tempDir;

    @Test
    void testOpenAndClose() throws Exception {
        DvRocksDB dvRocksDB = DvRocksDB.open(tempDir.resolve("db").toString());
        // Verify sub-components are accessible
        assertThat(dvRocksDB.rowPosIndex()).isNotNull();
        assertThat(dvRocksDB.logDv()).isNotNull();
        assertThat(dvRocksDB.lakeDv()).isNotNull();
        assertThat(dvRocksDB.fileDict()).isNotNull();
        assertThat(dvRocksDB.pendingDeletes()).isNotNull();
        dvRocksDB.close();
    }

    @Test
    void testAllColumnFamiliesWritable() throws Exception {
        try (DvRocksDB dvRocksDB = DvRocksDB.open(tempDir.resolve("db").toString())) {
            // RowPosIndex
            dvRocksDB.rowPosIndex().put(1L, new FilePos(10, 100L));
            assertThat(dvRocksDB.rowPosIndex().get(1L)).isEqualTo(new FilePos(10, 100L));

            // LakeDv
            Roaring64Bitmap bitmap = new Roaring64Bitmap();
            bitmap.add(42L);
            dvRocksDB.lakeDv().put(1, bitmap);
            assertThat(dvRocksDB.lakeDv().get(1).contains(42L)).isTrue();

            // LogDv
            dvRocksDB.logDv().markDeleted(500L);
            assertThat(dvRocksDB.logDv().isDeleted(500L)).isTrue();

            // FileDict
            dvRocksDB.fileDict().put(1, "/data/file1.parquet");
            assertThat(dvRocksDB.fileDict().getFilePath(1)).isEqualTo("/data/file1.parquet");

            // PendingDeletes
            dvRocksDB.pendingDeletes().putPending(99L);
            assertThat(dvRocksDB.pendingDeletes().get(99L).isPending()).isTrue();
        }
    }

    @Test
    void testCheckpointAndRestore() throws Exception {
        String dbPath = tempDir.resolve("db").toString();
        String checkpointPath = tempDir.resolve("checkpoint").toString();

        // Write data and checkpoint
        try (DvRocksDB dvRocksDB = DvRocksDB.open(dbPath)) {
            dvRocksDB.rowPosIndex().put(1L, new FilePos(10, 100L));
            dvRocksDB.rowPosIndex().put(2L, new FilePos(20, 200L));
            dvRocksDB.fileDict().put(10, "/data/file10.parquet");
            dvRocksDB.logDv().markDeleted(777L);

            dvRocksDB.checkpoint(checkpointPath);
        }

        // Restore and verify
        try (DvRocksDB restored = DvRocksDB.restore(checkpointPath)) {
            assertThat(restored.rowPosIndex().get(1L)).isEqualTo(new FilePos(10, 100L));
            assertThat(restored.rowPosIndex().get(2L)).isEqualTo(new FilePos(20, 200L));
            assertThat(restored.fileDict().getFilePath(10)).isEqualTo("/data/file10.parquet");
            assertThat(restored.logDv().isDeleted(777L)).isTrue();
        }
    }

    @Test
    void testWriteBatch() throws Exception {
        try (DvRocksDB dvRocksDB = DvRocksDB.open(tempDir.resolve("db").toString())) {
            try (WriteBatch batch = new WriteBatch()) {
                dvRocksDB.rowPosIndex().put(batch, 1L, new FilePos(10, 100L));
                dvRocksDB.rowPosIndex().put(batch, 2L, new FilePos(20, 200L));
                dvRocksDB.fileDict().put(batch, 10, "/data/file10.parquet");
                dvRocksDB.write(batch);
            }

            assertThat(dvRocksDB.rowPosIndex().get(1L)).isEqualTo(new FilePos(10, 100L));
            assertThat(dvRocksDB.rowPosIndex().get(2L)).isEqualTo(new FilePos(20, 200L));
            assertThat(dvRocksDB.fileDict().getFilePath(10)).isEqualTo("/data/file10.parquet");
        }
    }

    @Test
    void testSnapshotIsPointInTimeView() throws Exception {
        try (DvRocksDB dvRocksDB = DvRocksDB.open(tempDir.resolve("db").toString())) {
            dvRocksDB.rowPosIndex().put(1L, new FilePos(10, 100L));
            dvRocksDB.rowPosIndex().put(3L, new FilePos(10, 300L));
            dvRocksDB.fileDict().put(10, "/data/file10.parquet");

            Snapshot snapshot = dvRocksDB.snapshot();
            try {
                // writes after the snapshot: an insert, an overwrite, a delete, a new file
                dvRocksDB.rowPosIndex().put(2L, new FilePos(20, 200L));
                dvRocksDB.rowPosIndex().put(1L, new FilePos(20, 111L));
                dvRocksDB.rowPosIndex().delete(3L);
                dvRocksDB.fileDict().put(20, "/data/file20.parquet");

                String atSnapshot = tempDir.resolve("at-snapshot.sst").toString();
                assertThat(dvRocksDB.rowPosIndex().exportTo(atSnapshot, snapshot)).isEqualTo(2L);
                assertThat(readSst(atSnapshot))
                        .containsExactly(
                                entry(1L, new FilePos(10, 100L)), entry(3L, new FilePos(10, 300L)));
                assertThat(dvRocksDB.fileDict().idToPathEntries(snapshot))
                        .containsOnly(entry(10, "/data/file10.parquet"));

                // without a snapshot the latest state is visible
                String latest = tempDir.resolve("latest.sst").toString();
                assertThat(dvRocksDB.rowPosIndex().exportTo(latest, null)).isEqualTo(2L);
                assertThat(readSst(latest))
                        .containsExactly(
                                entry(1L, new FilePos(20, 111L)), entry(2L, new FilePos(20, 200L)));
                assertThat(dvRocksDB.fileDict().idToPathEntries(null))
                        .containsOnly(
                                entry(10, "/data/file10.parquet"),
                                entry(20, "/data/file20.parquet"));
            } finally {
                dvRocksDB.releaseSnapshot(snapshot);
            }
        }
    }

    private static Map<Long, FilePos> readSst(String path) throws Exception {
        Map<Long, FilePos> out = new LinkedHashMap<>();
        try (Options options = new Options();
                SstFileReader reader = new SstFileReader(options);
                ReadOptions readOptions = new ReadOptions()) {
            reader.open(path);
            try (SstFileReaderIterator it = reader.newIterator(readOptions)) {
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    out.put(RowPosIndex.decodeRowId(it.key()), FilePos.decode(it.value()));
                }
            }
        }
        return out;
    }
}
