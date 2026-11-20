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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.lake.paimon.tiering.mergetree.MergeTreeWriter;
import org.apache.fluss.lake.paimon.tiering.mergetree.SharedWriteBuffers;
import org.apache.fluss.lake.writer.WriterMemoryPool;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.RowType;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** The primary key writers of one tiering task share one write buffer, bounded by its pool. */
class SharedWriteBuffersTest {

    private static final int BUCKETS = 4;
    private static final int ROWS_PER_BUCKET = 20_000;
    // 16 pages of 64 KB; each bucket below buffers about 2 MB, so 4 own buffers would hold 8 MB
    private static final long TASK_MEMORY = 1024 * 1024;

    private @TempDir File warehouse;

    @Test
    void testWritersOfATaskShareOneBoundedBuffer() throws Exception {
        FileStoreTable table = createTable();
        WriterMemoryPool taskMemory = new WriterMemoryPool(TASK_MEMORY);
        List<RecordWriter<?>> writers = new ArrayList<>();
        for (int bucket = 0; bucket < BUCKETS; bucket++) {
            writers.add(writer(table, bucket, taskMemory));
        }
        String payload = String.join("", Collections.nCopies(80, "x"));
        long maxUsed = 0;
        // the buckets' rows interleave, as a tiering task reads its buckets' logs
        for (int i = 0; i < ROWS_PER_BUCKET; i++) {
            for (int bucket = 0; bucket < BUCKETS; bucket++) {
                GenericRow row = new GenericRow(2);
                row.setField(0, bucket * ROWS_PER_BUCKET + i);
                row.setField(1, BinaryString.fromString(payload));
                writers.get(bucket).write(new GenericRecord(i, 0L, ChangeType.INSERT, row));
            }
            maxUsed = Math.max(maxUsed, SharedWriteBuffers.usedBytes(taskMemory));
        }
        assertThat(maxUsed).isPositive().isLessThanOrEqualTo(TASK_MEMORY);

        long rows = 0;
        int files = 0;
        for (RecordWriter<?> writer : writers) {
            for (CommitMessage message : writer.complete()) {
                for (DataFileMeta file :
                        ((CommitMessageImpl) message).newFilesIncrement().newFiles()) {
                    rows += file.rowCount();
                    files++;
                }
            }
            assertThat(SharedWriteBuffers.usedBytes(taskMemory)).isLessThanOrEqualTo(TASK_MEMORY);
        }
        // the writers flushed before they completed, as their rows did not fit in the task's buffer
        assertThat(files).isGreaterThan(BUCKETS);
        // a writer flushed by another one's need for memory keeps every row
        assertThat(rows).isEqualTo((long) BUCKETS * ROWS_PER_BUCKET);

        for (RecordWriter<?> writer : writers) {
            assertThat(SharedWriteBuffers.hasBuffer(taskMemory)).isTrue();
            writer.close();
        }
        // dropped with the task's last writer, so the next round's writers start a new one
        assertThat(SharedWriteBuffers.hasBuffer(taskMemory)).isFalse();
    }

    @Test
    void testWritersWithoutTaskMemoryKeepOwnBuffers() throws Exception {
        FileStoreTable table = createTable();
        try (RecordWriter<?> writer = writer(table, 0, null)) {
            GenericRow row = new GenericRow(2);
            row.setField(0, 1);
            row.setField(1, BinaryString.fromString("a"));
            writer.write(new GenericRecord(0, 0L, ChangeType.INSERT, row));
            assertThat(writer.complete()).hasSize(1);
        }
    }

    private RecordWriter<?> writer(
            FileStoreTable table, int bucket, @Nullable WriterMemoryPool taskMemory) {
        return new MergeTreeWriter(
                table,
                new TableBucket(1L, bucket),
                null,
                Collections.emptyList(),
                RowType.of(
                        org.apache.fluss.types.DataTypes.INT(),
                        org.apache.fluss.types.DataTypes.STRING()),
                new String[] {new File(warehouse, "io").getPath()},
                taskMemory,
                false,
                false);
    }

    private FileStoreTable createTable() throws Exception {
        Options options = new Options();
        options.set("warehouse", warehouse.getPath());
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase("db", true);
        Identifier id = Identifier.create("db", "t");
        catalog.createTable(
                id,
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .primaryKey("c1")
                        .option(CoreOptions.BUCKET.key(), String.valueOf(BUCKETS))
                        // a full buffer is written out, not spilled, so rows land in data files
                        .option(CoreOptions.WRITE_BUFFER_SPILLABLE.key(), "false")
                        .build(),
                true);
        return (FileStoreTable) catalog.getTable(id);
    }
}
