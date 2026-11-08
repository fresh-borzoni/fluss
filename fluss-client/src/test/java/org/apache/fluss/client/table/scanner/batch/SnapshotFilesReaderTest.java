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

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.row.encode.ValueEncoder;

import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.RocksDB;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SnapshotFilesReader}. */
class SnapshotFilesReaderTest {

    private static final short SCHEMA_ID = 1;

    /**
     * The KV snapshot of a table with deletion vectors and row TTL holds values in the versioned
     * layout, with the version offset and the TTL timestamp before the schema id. The reader takes
     * the layout from the table's persisted layout version and decodes the rows.
     */
    @Test
    void testReadsSnapshotOfVersionedValues(@TempDir Path dir) throws Exception {
        RocksDB.loadLibrary();
        Path dbPath = dir.resolve("db");
        Configuration tableConf = new Configuration();
        tableConf.set(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED, true);
        tableConf.set(
                ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION, KvValueLayout.VERSIONED.version());
        tableConf.setString(ConfigOptions.TABLE_KV_TTL.key(), "1 h");
        KvValueLayout layout = KvValueLayout.fromTableConfig(new TableConfig(tableConf));
        assertThat(layout).isSameAs(KvValueLayout.VERSIONED);

        try (Options options = new Options().setCreateIfMissing(true);
                RocksDB db = RocksDB.open(options, dbPath.toString())) {
            for (int i = 1; i <= 3; i++) {
                long ttlTimestamp = 1000L * i;
                byte[] value =
                        ValueEncoder.forLayout(layout, row -> ttlTimestamp)
                                .encodeValue(
                                        new BinaryValue(
                                                SCHEMA_ID,
                                                compactedRow(
                                                        DATA1_SCHEMA_PK.getRowType(),
                                                        new Object[] {i, "v" + i})),
                                        100L + i);
                assertThat(layout.readVersionOffset(MemorySegment.wrap(value), 0))
                        .isEqualTo(100L + i);
                assertThat(layout.readTtlTimestamp(MemorySegment.wrap(value), 0))
                        .isEqualTo(ttlTimestamp);
                db.put(("k" + i).getBytes(StandardCharsets.UTF_8), value);
            }
        }

        List<String> rows = new ArrayList<>();
        SnapshotFilesReader reader =
                new SnapshotFilesReader(
                        KvFormat.COMPACTED,
                        dbPath,
                        new int[] {1},
                        SCHEMA_ID,
                        DATA1_SCHEMA_PK,
                        new TestingSchemaGetter(SCHEMA_ID, DATA1_SCHEMA_PK),
                        layout);
        try {
            while (reader.hasNext()) {
                rows.add(reader.next().getString(0).toString());
            }
        } finally {
            reader.close();
        }
        assertThat(rows).containsExactly("v1", "v2", "v3");
    }
}
