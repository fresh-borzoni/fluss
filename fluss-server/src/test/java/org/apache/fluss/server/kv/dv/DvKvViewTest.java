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

import io.github.fluss_contrib.rocksdb.FlushOptions;
import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.RocksDB;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the view of a restored KV that a change-list checkpoint names versions from. */
class DvKvViewTest {

    @Test
    void testViewKeepsTheRestoredKvWhileTheKvMovesOn(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("db");
        Path viewDir = dir.resolve("view");
        // the downloaded snapshot: three keys, at version offsets 10, 11 and 12
        try (Options options = new Options().setCreateIfMissing(true);
                RocksDB kv = RocksDB.open(options, db.toString())) {
            kv.put(key("k1"), value(10));
            kv.put(key("k2"), value(11));
            kv.put(key("k3"), value(12));
            try (FlushOptions flush = new FlushOptions().setWaitForFlush(true)) {
                kv.flush(flush);
            }
        }
        List<Path> ssts = ssts(db);
        assertThat(ssts).isNotEmpty();

        long bytes = DvKvView.link(db, viewDir);

        assertThat(bytes).isPositive();
        for (Path sst : ssts) {
            // hard links, no copies
            assertThat(Files.isSameFile(sst, viewDir.resolve(sst.getFileName()))).isTrue();
        }

        // the KV opens the files and moves on: overwrites, deletes, compacts the old files away
        try (Options options = new Options();
                RocksDB kv = RocksDB.open(options, db.toString())) {
            kv.put(key("k1"), value(20));
            kv.delete(key("k2"));
            kv.put(key("k9"), value(21));
            try (FlushOptions flush = new FlushOptions().setWaitForFlush(true)) {
                kv.flush(flush);
            }
            kv.compactRange();
        }
        for (Path sst : ssts) {
            assertThat(sst).doesNotExist();
        }

        DvKvView view = DvKvView.of(viewDir, bytes, System.currentTimeMillis(), DvKvViewTest::tag);
        try {
            assertThat(view.versionsAt(Arrays.asList(key("k1"), key("k2"), key("k3"), key("k9"))))
                    .containsExactly(10, 11, 12, -1);
            assertThat(view.versionsAt(java.util.Collections.emptyList())).isEmpty();
            assertThat(view.linkedBytes()).isEqualTo(bytes);
        } finally {
            view.close();
        }

        assertThat(viewDir).doesNotExist();
        assertThatThrownBy(() -> view.versionsAt(Arrays.asList(key("k1"))))
                .hasMessageContaining("closed");
    }

    @Test
    void testLinkReplacesAnOldView(@TempDir Path dir) throws Exception {
        Path db = Files.createDirectories(dir.resolve("db"));
        Files.write(db.resolve("000001.sst"), new byte[] {1, 2});
        Files.write(db.resolve("CURRENT"), "MANIFEST-000002\n".getBytes(StandardCharsets.UTF_8));
        Path viewDir = Files.createDirectories(dir.resolve("view"));
        Files.write(viewDir.resolve("000099.sst"), new byte[] {9});

        assertThat(DvKvView.link(db, viewDir)).isEqualTo(2);

        try (Stream<Path> files = Files.list(viewDir)) {
            assertThat(files.map(p -> p.getFileName().toString()).collect(Collectors.toList()))
                    .containsExactlyInAnyOrder("000001.sst", "CURRENT");
        }
        // CURRENT is a copy: the KV rewrites it when it opens
        assertThat(Files.isSameFile(db.resolve("CURRENT"), viewDir.resolve("CURRENT"))).isFalse();
    }

    @Test
    void testFailedLinkLeavesNothing(@TempDir Path dir) {
        Path viewDir = dir.resolve("view");

        assertThat(DvKvView.link(dir.resolve("no-such-db"), viewDir)).isEqualTo(-1);

        assertThat(viewDir).doesNotExist();
    }

    // ---------------------------------------------------------------------------------------------

    private static List<Path> ssts(Path db) throws Exception {
        try (Stream<Path> files = Files.list(db)) {
            return files.filter(p -> p.toString().endsWith(".sst")).collect(Collectors.toList());
        }
    }

    private static byte[] key(String k) {
        return k.getBytes(StandardCharsets.UTF_8);
    }

    // a value whose first 8 bytes are its version offset, as the KV's tagged layouts have
    private static byte[] value(long versionOffset) {
        return ByteBuffer.allocate(12).putLong(versionOffset).putInt(7).array();
    }

    private static long tag(byte[] value) {
        return ByteBuffer.wrap(value).getLong();
    }
}
