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

import io.github.fluss_contrib.rocksdb.EnvOptions;
import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.ReadOptions;
import io.github.fluss_contrib.rocksdb.RocksDBException;
import io.github.fluss_contrib.rocksdb.SstFileReader;
import io.github.fluss_contrib.rocksdb.SstFileReaderIterator;
import io.github.fluss_contrib.rocksdb.SstFileWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
    void testPutGetAndOverwrite() throws Exception {
        RowPosIndex index = dvRocksDB.rowPosIndex();
        assertThat(index.getByKey(key("k1"))).isNull();

        index.put(key("k1"), new FilePos(10, 100L));
        assertThat(index.getByKey(key("k1"))).isEqualTo(new FilePos(10, 100L));

        index.put(key("k1"), new FilePos(20, 5L));
        assertThat(index.getByKey(key("k1"))).isEqualTo(new FilePos(20, 5L));

        index.delete(key("k1"));
        assertThat(index.getByKey(key("k1"))).isNull();
    }

    @Test
    void testMultiGetByKeysReturnsNullForMissingKeys() throws Exception {
        RowPosIndex index = dvRocksDB.rowPosIndex();
        index.put(key("k1"), new FilePos(10, 100L));
        index.put(key("k3"), new FilePos(30, 300L));

        assertThat(index.multiGetByKeys(Arrays.asList(key("k1"), key("k2"), key("k3"))))
                .containsExactly(new FilePos(10, 100L), null, new FilePos(30, 300L));
        assertThat(index.multiGetByKeys(Collections.emptyList())).isEmpty();
    }

    @Test
    void testIngestExternalFile() throws Exception {
        RowPosIndex index = dvRocksDB.rowPosIndex();
        String sstPath = tempDir.resolve("test.sst").toString();

        // keys must be written in ascending order
        try (EnvOptions envOptions = new EnvOptions();
                Options options = new Options();
                SstFileWriter writer = new SstFileWriter(envOptions, options)) {
            writer.open(sstPath);
            writer.put(key("k1"), new FilePos(10, 100L).encode());
            writer.put(key("k2"), new FilePos(10, 200L).encode());
            writer.put(key("k3"), new FilePos(20, 300L).encode());
            writer.finish();
        }

        index.ingestExternalFile(Collections.singletonList(sstPath));

        assertThat(index.getByKey(key("k1"))).isEqualTo(new FilePos(10, 100L));
        assertThat(index.getByKey(key("k2"))).isEqualTo(new FilePos(10, 200L));
        assertThat(index.getByKey(key("k3"))).isEqualTo(new FilePos(20, 300L));
    }

    /**
     * Reads as of a snapshot taken before an ingest do not see the ingested files: neither a file
     * that overlaps the existing keys, nor one whose keys are all new and beyond them. A move
     * ingests while the covered snapshot's lookups read such a snapshot.
     */
    @Test
    void testIngestedFilesAreNotSeenAsOfAnEarlierSnapshot() throws Exception {
        RowPosIndex index = dvRocksDB.rowPosIndex();
        index.ingestExternalFile(
                Collections.singletonList(
                        sst("base.sst", "k1", new FilePos(10, 1L), "k3", new FilePos(10, 3L))));
        index.put(key("k5"), new FilePos(10, 5L));

        io.github.fluss_contrib.rocksdb.Snapshot before = dvRocksDB.snapshot();
        try {
            // overlaps the keys there, and a file of new keys beyond them
            index.ingestExternalFile(
                    Collections.singletonList(
                            sst(
                                    "overlap.sst",
                                    "k1",
                                    new FilePos(20, 1L),
                                    "k2",
                                    new FilePos(20, 2L))));
            index.ingestExternalFile(
                    Collections.singletonList(
                            sst(
                                    "beyond.sst",
                                    "z1",
                                    new FilePos(20, 7L),
                                    "z2",
                                    new FilePos(20, 8L))));

            assertThat(
                            index.multiGetByKeys(
                                    Arrays.asList(
                                            key("k1"), key("k2"), key("k3"), key("k5"), key("z1"),
                                            key("z2")),
                                    before))
                    .containsExactly(
                            new FilePos(10, 1L),
                            null,
                            new FilePos(10, 3L),
                            new FilePos(10, 5L),
                            null,
                            null);
            assertThat(
                            index.multiGetByKeys(
                                    Arrays.asList(key("k1"), key("k2"), key("z1"), key("z2"))))
                    .containsExactly(
                            new FilePos(20, 1L),
                            new FilePos(20, 2L),
                            new FilePos(20, 7L),
                            new FilePos(20, 8L));
        } finally {
            dvRocksDB.releaseSnapshot(before);
        }
    }

    /** Writes an SST of two keys, in order, with their positions. */
    private String sst(String name, String k1, FilePos p1, String k2, FilePos p2) throws Exception {
        String path = tempDir.resolve(name).toString();
        try (EnvOptions envOptions = new EnvOptions();
                Options options = new Options();
                SstFileWriter writer = new SstFileWriter(envOptions, options)) {
            writer.open(path);
            writer.put(key(k1), p1.encode());
            writer.put(key(k2), p2.encode());
            writer.finish();
        }
        return path;
    }

    /**
     * The SSTs of a bucket's re-index splits cover interleaved key ranges, as each split indexes
     * different files of the bucket. One ingestion takes them all.
     */
    @Test
    void testIngestOverlappingFilesAtOnce() throws Exception {
        RowPosIndex index = dvRocksDB.rowPosIndex();
        String first = tempDir.resolve("split0.sst").toString();
        String second = tempDir.resolve("split1.sst").toString();
        try (EnvOptions envOptions = new EnvOptions();
                Options options = new Options();
                SstFileWriter writer = new SstFileWriter(envOptions, options)) {
            writer.open(first);
            writer.put(key("k1"), new FilePos(10, 1L).encode());
            writer.put(key("k5"), new FilePos(10, 5L).encode());
            writer.finish();
        }
        try (EnvOptions envOptions = new EnvOptions();
                Options options = new Options();
                SstFileWriter writer = new SstFileWriter(envOptions, options)) {
            writer.open(second);
            writer.put(key("k2"), new FilePos(11, 2L).encode());
            writer.put(key("k9"), new FilePos(11, 9L).encode());
            writer.finish();
        }

        index.ingestExternalFile(Arrays.asList(first, second));

        assertThat(index.multiGetByKeys(Arrays.asList(key("k1"), key("k2"), key("k5"), key("k9"))))
                .containsExactly(
                        new FilePos(10, 1L),
                        new FilePos(11, 2L),
                        new FilePos(10, 5L),
                        new FilePos(11, 9L));
    }

    @Test
    void testExportToAndIngestRoundTrip() throws Exception {
        RowPosIndex index = dvRocksDB.rowPosIndex();
        index.put(key("k1"), new FilePos(10, 100L));
        index.put(key("k2"), new FilePos(10, 200L));
        index.put(key("k3"), new FilePos(20, 300L));
        index.put(key("k4"), new FilePos(Integer.MAX_VALUE, Long.MAX_VALUE));
        index.delete(key("k2")); // deleted entries are not exported
        String sst = tempDir.resolve("export.sst").toString();

        assertThat(index.exportTo(sst, null, null)).isEqualTo(3L);

        Map<String, FilePos> exported = readSst(sst);
        assertThat(exported)
                .containsExactly(
                        entry("k1", new FilePos(10, 100L)),
                        entry("k3", new FilePos(20, 300L)),
                        entry("k4", new FilePos(Integer.MAX_VALUE, Long.MAX_VALUE)));

        try (DvRocksDB other = DvRocksDB.open(tempDir.resolve("other").toString())) {
            other.rowPosIndex().ingestExternalFile(Collections.singletonList(sst));
            assertThat(
                            other.rowPosIndex()
                                    .multiGetByKeys(
                                            Arrays.asList(
                                                    key("k1"), key("k2"), key("k3"), key("k4"))))
                    .isEqualTo(
                            index.multiGetByKeys(
                                    Arrays.asList(key("k1"), key("k2"), key("k3"), key("k4"))));

            // re-exporting the ingested copy yields the same entries
            String again = tempDir.resolve("again.sst").toString();
            assertThat(other.rowPosIndex().exportTo(again, null, null)).isEqualTo(3L);
            assertThat(readSst(again)).isEqualTo(exported);
        }
    }

    @Test
    void testExportToKeepsOnlyEntriesOfKeptFiles() throws Exception {
        RowPosIndex index = dvRocksDB.rowPosIndex();
        index.put(key("k1"), new FilePos(10, 100L));
        index.put(key("k2"), new FilePos(20, 200L));
        String sst = tempDir.resolve("filtered.sst").toString();

        assertThat(index.exportTo(sst, null, fileId -> fileId == 20)).isEqualTo(1L);
        assertThat(readSst(sst)).containsExactly(entry("k2", new FilePos(20, 200L)));
    }

    @Test
    void testAbortedExportStops() throws Exception {
        RowPosIndex index = dvRocksDB.rowPosIndex();
        for (int i = 0; i < 5000; i++) {
            index.put(key(String.format("k%05d", i)), new FilePos(10, i));
        }
        String sst = tempDir.resolve("aborted.sst").toString();
        AtomicInteger checks = new AtomicInteger();

        assertThatThrownBy(
                        () -> index.exportTo(sst, null, null, () -> checks.incrementAndGet() > 2))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("aborted");
        assertThat(checks).hasValue(3);
        assertThat(index.exportTo(sst, null, null, () -> false)).isEqualTo(5000L);
    }

    @Test
    void testExportToEmptyIndexWritesNoFile() throws Exception {
        RowPosIndex index = dvRocksDB.rowPosIndex();
        File sst = tempDir.resolve("empty.sst").toFile();

        assertThat(index.exportTo(sst.getPath(), null, null)).isZero();
        assertThat(sst).doesNotExist();

        // an index whose entries were all deleted is empty as well
        index.put(key("k1"), new FilePos(10, 100L));
        index.delete(key("k1"));
        assertThat(index.exportTo(sst.getPath(), null, null)).isZero();
        assertThat(sst).doesNotExist();
    }

    static byte[] key(String k) {
        return k.getBytes(StandardCharsets.UTF_8);
    }

    static Map<String, FilePos> readSst(String path) throws RocksDBException {
        Map<String, FilePos> out = new LinkedHashMap<>();
        try (Options options = new Options();
                SstFileReader reader = new SstFileReader(options);
                ReadOptions readOptions = new ReadOptions()) {
            reader.open(path);
            try (SstFileReaderIterator it = reader.newIterator(readOptions)) {
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    out.put(
                            new String(it.key(), StandardCharsets.UTF_8),
                            FilePos.decode(it.value()));
                }
            }
        }
        return out;
    }
}
