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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileReader;
import org.rocksdb.SstFileReaderIterator;
import org.rocksdb.SstFileWriter;
import org.rocksdb.WriteBatch;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/** Test for {@link RowPosIndex}. */
class RowPosIndexTest {

    @TempDir Path tempDir;
    private DvRocksDB dvRocksDB;

    @BeforeEach
    void setup() throws Exception {
        dvRocksDB = DvRocksDB.open(tempDir.resolve("db").toString());
    }

    @AfterEach
    void teardown() {
        if (dvRocksDB != null) {
            dvRocksDB.close();
        }
    }

    @Test
    void testPutAndGet() throws Exception {
        RowPosIndex index = dvRocksDB.rowPosIndex();
        FilePos pos = new FilePos(10, 100L);

        index.put(1L, pos);
        assertThat(index.get(1L)).isEqualTo(pos);
    }

    @Test
    void testGetNonExistent() throws Exception {
        assertThat(dvRocksDB.rowPosIndex().get(999L)).isNull();
    }

    @Test
    void testPutOverwrite() throws Exception {
        RowPosIndex index = dvRocksDB.rowPosIndex();

        index.put(1L, new FilePos(10, 100L));
        index.put(1L, new FilePos(20, 200L));
        assertThat(index.get(1L)).isEqualTo(new FilePos(20, 200L));
    }

    @Test
    void testDelete() throws Exception {
        RowPosIndex index = dvRocksDB.rowPosIndex();

        index.put(1L, new FilePos(10, 100L));
        index.delete(1L);
        assertThat(index.get(1L)).isNull();
    }

    @Test
    void testWriteBatch() throws Exception {
        RowPosIndex index = dvRocksDB.rowPosIndex();

        try (WriteBatch batch = new WriteBatch()) {
            index.put(batch, 1L, new FilePos(10, 100L));
            index.put(batch, 2L, new FilePos(20, 200L));
            index.put(batch, 3L, new FilePos(30, 300L));
            dvRocksDB.write(batch);
        }

        assertThat(index.get(1L)).isEqualTo(new FilePos(10, 100L));
        assertThat(index.get(2L)).isEqualTo(new FilePos(20, 200L));
        assertThat(index.get(3L)).isEqualTo(new FilePos(30, 300L));
    }

    @Test
    void testWriteBatchDelete() throws Exception {
        RowPosIndex index = dvRocksDB.rowPosIndex();

        index.put(1L, new FilePos(10, 100L));
        index.put(2L, new FilePos(20, 200L));

        try (WriteBatch batch = new WriteBatch()) {
            index.delete(batch, 1L);
            dvRocksDB.write(batch);
        }

        assertThat(index.get(1L)).isNull();
        assertThat(index.get(2L)).isEqualTo(new FilePos(20, 200L));
    }

    @Test
    void testIngestExternalFile() throws Exception {
        RowPosIndex index = dvRocksDB.rowPosIndex();
        String sstPath = tempDir.resolve("test.sst").toString();

        // Create SST file with sorted entries
        try (EnvOptions envOptions = new EnvOptions();
                Options options = new Options();
                SstFileWriter writer = new SstFileWriter(envOptions, options)) {
            writer.open(sstPath);
            // Keys must be in sorted order (BigEndian encoding ensures this)
            writer.put(RowPosIndex.encodeRowId(1L), new FilePos(10, 100L).encode());
            writer.put(RowPosIndex.encodeRowId(2L), new FilePos(10, 200L).encode());
            writer.put(RowPosIndex.encodeRowId(3L), new FilePos(20, 300L).encode());
            writer.finish();
        }

        index.ingestExternalFile(Collections.singletonList(sstPath));

        assertThat(index.get(1L)).isEqualTo(new FilePos(10, 100L));
        assertThat(index.get(2L)).isEqualTo(new FilePos(10, 200L));
        assertThat(index.get(3L)).isEqualTo(new FilePos(20, 300L));
    }

    @Test
    void testEncodeDecodeRowId() {
        assertThat(RowPosIndex.decodeRowId(RowPosIndex.encodeRowId(0L))).isEqualTo(0L);
        assertThat(RowPosIndex.decodeRowId(RowPosIndex.encodeRowId(1L))).isEqualTo(1L);
        assertThat(RowPosIndex.decodeRowId(RowPosIndex.encodeRowId(Long.MAX_VALUE)))
                .isEqualTo(Long.MAX_VALUE);
        assertThat(RowPosIndex.decodeRowId(RowPosIndex.encodeRowId(-1L))).isEqualTo(-1L);
    }

    @Test
    void testExportToAndIngestRoundTrip() throws Exception {
        RowPosIndex index = dvRocksDB.rowPosIndex();
        index.put(1L, new FilePos(10, 100L));
        index.put(2L, new FilePos(10, 200L));
        index.put(3L, new FilePos(20, 300L));
        index.put(1L << 40, new FilePos(Integer.MAX_VALUE, Long.MAX_VALUE));
        index.delete(2L); // deleted entries are not exported
        String sst = tempDir.resolve("export.sst").toString();

        assertThat(index.exportTo(sst, null)).isEqualTo(3L);

        Map<Long, FilePos> exported = readSst(sst);
        assertThat(exported)
                .containsExactly(
                        entry(1L, new FilePos(10, 100L)),
                        entry(3L, new FilePos(20, 300L)),
                        entry(1L << 40, new FilePos(Integer.MAX_VALUE, Long.MAX_VALUE)));

        try (DvRocksDB other = DvRocksDB.open(tempDir.resolve("other").toString())) {
            other.rowPosIndex().ingestExternalFile(Collections.singletonList(sst));
            for (Map.Entry<Long, FilePos> e : exported.entrySet()) {
                assertThat(other.rowPosIndex().get(e.getKey())).isEqualTo(e.getValue());
            }
            assertThat(other.rowPosIndex().get(2L)).isNull();
            assertThat(other.rowPosIndex().multiGet(Arrays.asList(1L, 2L, 3L, 1L << 40)))
                    .isEqualTo(index.multiGet(Arrays.asList(1L, 2L, 3L, 1L << 40)));

            // re-exporting the ingested copy yields the same entries
            String again = tempDir.resolve("again.sst").toString();
            assertThat(other.rowPosIndex().exportTo(again, null)).isEqualTo(3L);
            assertThat(readSst(again)).isEqualTo(exported);
        }
    }

    @Test
    void testExportToEmptyIndexWritesNoFile() throws Exception {
        RowPosIndex index = dvRocksDB.rowPosIndex();
        File sst = tempDir.resolve("empty.sst").toFile();

        assertThat(index.exportTo(sst.getPath(), null)).isZero();
        assertThat(sst).doesNotExist();

        // an index whose entries were all deleted is empty as well
        index.put(1L, new FilePos(10, 100L));
        index.delete(1L);
        assertThat(index.exportTo(sst.getPath(), null)).isZero();
        assertThat(sst).doesNotExist();
    }

    private static Map<Long, FilePos> readSst(String path) throws RocksDBException {
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
