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
import org.rocksdb.WriteBatch;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/** Test for {@link FileDict}. */
class FileDictTest {

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
    void testPutAndBidirectionalLookup() throws Exception {
        FileDict fileDict = dvRocksDB.fileDict();

        fileDict.put(1, "/data/file1.parquet");

        assertThat(fileDict.getFileId("/data/file1.parquet")).isEqualTo(1);
        assertThat(fileDict.getFilePath(1)).isEqualTo("/data/file1.parquet");
    }

    @Test
    void testGetFileIdNonExistent() throws Exception {
        assertThat(dvRocksDB.fileDict().getFileId("/nonexistent")).isEqualTo(-1);
    }

    @Test
    void testGetFilePathNonExistent() throws Exception {
        assertThat(dvRocksDB.fileDict().getFilePath(999)).isNull();
    }

    @Test
    void testMultipleEntries() throws Exception {
        FileDict fileDict = dvRocksDB.fileDict();

        fileDict.put(1, "/data/file1.parquet");
        fileDict.put(2, "/data/file2.parquet");
        fileDict.put(3, "/data/file3.parquet");

        assertThat(fileDict.getFileId("/data/file1.parquet")).isEqualTo(1);
        assertThat(fileDict.getFileId("/data/file2.parquet")).isEqualTo(2);
        assertThat(fileDict.getFileId("/data/file3.parquet")).isEqualTo(3);
        assertThat(fileDict.getFilePath(1)).isEqualTo("/data/file1.parquet");
        assertThat(fileDict.getFilePath(2)).isEqualTo("/data/file2.parquet");
        assertThat(fileDict.getFilePath(3)).isEqualTo("/data/file3.parquet");
    }

    @Test
    void testWriteBatch() throws Exception {
        FileDict fileDict = dvRocksDB.fileDict();

        try (WriteBatch batch = new WriteBatch()) {
            fileDict.put(batch, 1, "/data/file1.parquet");
            fileDict.put(batch, 2, "/data/file2.parquet");
            dvRocksDB.write(batch);
        }

        assertThat(fileDict.getFileId("/data/file1.parquet")).isEqualTo(1);
        assertThat(fileDict.getFilePath(2)).isEqualTo("/data/file2.parquet");
    }

    @Test
    void testPutAll() throws Exception {
        FileDict fileDict = dvRocksDB.fileDict();
        Map<Integer, String> entries = new HashMap<>();
        entries.put(10, "/data/file10.parquet");
        entries.put(20, "/data/file20.parquet");
        entries.put(30, "/data/file30.parquet");

        try (WriteBatch batch = new WriteBatch()) {
            fileDict.putAll(batch, entries);
            dvRocksDB.write(batch);
        }

        assertThat(fileDict.getFileId("/data/file10.parquet")).isEqualTo(10);
        assertThat(fileDict.getFileId("/data/file20.parquet")).isEqualTo(20);
        assertThat(fileDict.getFileId("/data/file30.parquet")).isEqualTo(30);
        assertThat(fileDict.getFilePath(10)).isEqualTo("/data/file10.parquet");
        assertThat(fileDict.getFilePath(20)).isEqualTo("/data/file20.parquet");
        assertThat(fileDict.getFilePath(30)).isEqualTo("/data/file30.parquet");
    }

    @Test
    void testUnicodeFilePath() throws Exception {
        FileDict fileDict = dvRocksDB.fileDict();

        fileDict.put(1, "/data/日本語/ファイル.parquet");

        assertThat(fileDict.getFileId("/data/日本語/ファイル.parquet")).isEqualTo(1);
        assertThat(fileDict.getFilePath(1)).isEqualTo("/data/日本語/ファイル.parquet");
    }

    @Test
    void testIdToPathEntriesReturnsOnlyIdToPathMappings() throws Exception {
        FileDict fileDict = dvRocksDB.fileDict();
        assertThat(fileDict.idToPathEntries(null)).isEmpty();

        fileDict.put(1, "/data/file1.parquet");
        fileDict.put(2, "/data/file2.parquet");
        fileDict.put(256, "x");
        fileDict.put(Integer.MAX_VALUE, "/data/max.parquet");
        // path bytes that look like an id->path key must not leak into the result
        fileDict.put(3, "\u0001\u0000\u0000\u0000");

        assertThat(fileDict.idToPathEntries(null))
                .containsOnly(
                        entry(1, "/data/file1.parquet"),
                        entry(2, "/data/file2.parquet"),
                        entry(3, "\u0001\u0000\u0000\u0000"),
                        entry(256, "x"),
                        entry(Integer.MAX_VALUE, "/data/max.parquet"));

        fileDict.delete(2);
        assertThat(fileDict.idToPathEntries(null)).containsOnlyKeys(1, 3, 256, Integer.MAX_VALUE);
    }
}
