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

package org.apache.fluss.flink.sink.writer;

import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ReadYourWrites}. */
class ReadYourWritesTest {

    private static final TablePath TABLE = TablePath.of("db", "sale_items");

    // sale_id, item_id, name with primary key (sale_id, item_id), looked up by sale_id too
    private static final RowType ROW_TYPE =
            RowType.of(DataTypes.INT(), DataTypes.INT(), DataTypes.STRING());
    private static final int[] PRIMARY_KEY = {0, 1};
    private static final int[] SALE = {0};

    private ReadYourWrites.Writer writer;

    @BeforeEach
    void register() {
        writer = ReadYourWrites.register(TABLE, ROW_TYPE, PRIMARY_KEY, () -> {});
    }

    @AfterEach
    void close() {
        writer.close();
    }

    @Test
    void testNoWriterInThread() {
        writer.close();
        assertThat(ReadYourWrites.view(TABLE, SALE, GenericRow.of(7))).isNull();
    }

    @Test
    void testUnacknowledgedRowsReplaceRowsRead() {
        writer.written(row(7, 1, "new"), false, new CompletableFuture<>());
        writer.written(row(7, 2, "gone"), true, new CompletableFuture<>());
        writer.written(row(9, 3, "other sale"), false, new CompletableFuture<>());

        assertThat(lookup(SALE, GenericRow.of(7), row(7, 1, "old"), row(7, 2, "gone")))
                .containsExactlyInAnyOrder(row(7, 1, "new"));
        assertThat(lookup(PRIMARY_KEY, GenericRow.of(7, 2), row(7, 2, "gone"))).isEmpty();
        assertThat(lookup(PRIMARY_KEY, GenericRow.of(7, 4), row(7, 4, "kept")))
                .containsExactly(row(7, 4, "kept"));
    }

    /**
     * Pages of a scan: every page drops the rows that unacknowledged writes replace or delete, and
     * only the first page adds the written rows.
     */
    @Test
    void testUnacknowledgedRowsReplaceRowsOfPages() {
        writer.written(row(7, 1, "new"), false, new CompletableFuture<>());
        writer.written(row(7, 2, "gone"), true, new CompletableFuture<>());
        writer.written(row(7, 5, "added"), false, new CompletableFuture<>());
        ReadYourWrites.View view = ReadYourWrites.view(TABLE, SALE, GenericRow.of(7));

        assertThat(view.applyToPage(Arrays.asList(row(7, 1, "old"), row(7, 3, "kept")), true))
                .containsExactlyInAnyOrder(row(7, 1, "new"), row(7, 5, "added"), row(7, 3, "kept"));
        assertThat(view.applyToPage(Arrays.asList(row(7, 2, "gone"), row(7, 4, "kept")), false))
                .containsExactly(row(7, 4, "kept"));
    }

    @Test
    void testAcknowledgedRowsAreReadFromTable() {
        CompletableFuture<Void> first = new CompletableFuture<>();
        writer.written(row(7, 1, "a"), false, first);
        assertThat(lookup(SALE, GenericRow.of(7))).containsExactly(row(7, 1, "a"));

        first.complete(null);
        assertThat(lookup(SALE, GenericRow.of(7), row(7, 1, "b"))).containsExactly(row(7, 1, "b"));
        assertThat(lookup(PRIMARY_KEY, GenericRow.of(7, 1))).isEmpty();
    }

    @Test
    void testLaterWriteOutlivesAcknowledgementOfEarlierOne() {
        CompletableFuture<Void> first = new CompletableFuture<>();
        writer.written(row(7, 1, "a"), false, first);
        writer.written(row(7, 1, "b"), false, new CompletableFuture<>());
        first.complete(null);

        assertThat(lookup(SALE, GenericRow.of(7), row(7, 1, "a"))).containsExactly(row(7, 1, "b"));
        assertThat(lookup(PRIMARY_KEY, GenericRow.of(7, 1), row(7, 1, "a")))
                .containsExactly(row(7, 1, "b"));
    }

    @Test
    void testFailedWriteStays() {
        CompletableFuture<Void> write = new CompletableFuture<>();
        writer.written(row(7, 1, "a"), false, write);
        write.completeExceptionally(new RuntimeException("expected"));

        assertThat(lookup(SALE, GenericRow.of(7))).containsExactly(row(7, 1, "a"));
    }

    /**
     * In the flush mode, the writer keeps no rows: a lookup waits until the writer flushed what it
     * received, and then reads the table as it is, in pages too.
     */
    @Test
    void testFlushModeWaitsForTheWriterToFlush() throws Exception {
        TablePath table = TablePath.of("db", "flushed_sale_items");
        AtomicInteger flushes = new AtomicInteger();
        CompletableFuture<Void> flushDone = new CompletableFuture<>();
        ReadYourWrites.Writer flushing;
        System.setProperty("fluss.read-your-writes.mode", "flush");
        try {
            flushing =
                    ReadYourWrites.register(
                            table,
                            ROW_TYPE,
                            PRIMARY_KEY,
                            () -> {
                                flushes.incrementAndGet();
                                flushDone.join();
                            });
        } finally {
            System.clearProperty("fluss.read-your-writes.mode");
        }
        try {
            flushing.written(row(7, 1, "new"), false, new CompletableFuture<>());
            ReadYourWrites.View view = ReadYourWrites.view(table, SALE, GenericRow.of(7));
            assertThat(view).isNotNull();
            assertThat(view.ready()).as("the flush is running").isNotDone();

            flushDone.complete(null);
            view.ready().get(10, TimeUnit.SECONDS);
            assertThat(flushes).hasValue(1);
            List<InternalRow> read = Collections.singletonList(row(7, 1, "flushed"));
            assertThat(view.apply(read)).containsExactly(row(7, 1, "flushed"));
            assertThat(view.applyToPage(read, true)).containsExactly(row(7, 1, "flushed"));
        } finally {
            flushing.close();
        }
    }

    /** The operator before the sink may reuse its records: the writer keeps values of its own. */
    @Test
    void testRowsAreKeptWhenTheRecordIsReused() {
        TablePath table = TablePath.of("db", "sale_tags");
        RowType type =
                RowType.of(DataTypes.INT(), DataTypes.ARRAY(DataTypes.INT()), DataTypes.BYTES());
        ReadYourWrites.Writer tags = ReadYourWrites.register(table, type, new int[] {0}, () -> {});
        try {
            int[] values = {1, 2};
            byte[] bytes = {3};
            tags.written(
                    GenericRow.of(7, new GenericArray(values), bytes),
                    false,
                    new CompletableFuture<>());
            values[0] = 9;
            bytes[0] = 9;

            ReadYourWrites.View view = ReadYourWrites.view(table, new int[] {0}, GenericRow.of(7));
            assertThat(view).isNotNull();
            List<InternalRow> rows = view.apply(Collections.emptyList());
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getArray(1).toIntArray()).containsExactly(1, 2);
            assertThat(rows.get(0).getBytes(2)).containsExactly(3);
        } finally {
            tags.close();
        }
    }

    private static List<InternalRow> lookup(
            int[] keyColumns, InternalRow key, InternalRow... read) {
        ReadYourWrites.View view = ReadYourWrites.view(TABLE, keyColumns, key);
        assertThat(view).isNotNull();
        assertThat(view.ready()).isDone();
        return view.apply(Arrays.asList(read));
    }

    private static GenericRow row(int sale, int item, String name) {
        return GenericRow.of(sale, item, BinaryString.fromString(name));
    }
}
