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

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.iceberg.tiering.writer.TaskWriterFactory;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.RowWithPosResult;
import org.apache.fluss.lake.source.SupportsRowPositions;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that a reader of a lake split closes the catalog it was given, with the catalog's FileIO
 * and its connection pool, once its rows are read.
 */
class IcebergRecordReaderCatalogTest {

    @TempDir private File warehouse;

    private Table table;
    private FileScanTask task;

    @BeforeEach
    void beforeEach() throws Exception {
        ClosingCatalog catalog = newCatalog();
        table =
                catalog.createTable(
                        TableIdentifier.of("db", "t"),
                        new Schema(
                                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                                Types.NestedField.optional(2, "name", Types.StringType.get())),
                        PartitionSpec.unpartitioned());
        appendFile(0);
        List<FileScanTask> tasks = new ArrayList<>();
        try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
            planned.forEach(tasks::add);
        }
        assertThat(tasks).hasSize(1);
        task = tasks.get(0);
    }

    /** A union read reads each split with a catalog of its own. */
    @Test
    void testReadClosesTheCatalogOfItsSplit() throws IOException {
        ClosingCatalog catalog = newCatalog();
        IcebergRecordReader reader = new IcebergRecordReader(task, table, null, catalog);
        int rows = 0;
        try (CloseableIterator<LogRecord> it = reader.read()) {
            while (it.hasNext()) {
                it.next();
                rows++;
            }
        }
        assertThat(rows).isEqualTo(3);
        assertThat(catalog.closed).isTrue();
    }

    /**
     * A reader that is closed without being read, e.g. because its split turned out to need no
     * rows, closes its catalog too.
     */
    @Test
    void testCloseClosesTheCatalogOfAReaderNeverRead() throws IOException {
        ClosingCatalog catalog = newCatalog();
        IcebergRecordReader reader = new IcebergRecordReader(task, table, null, catalog);
        assertThat(catalog.closes).isZero();
        reader.close();
        assertThat(catalog.closes).isEqualTo(1);
    }

    /** The iterator and the reader both close; the catalog closes once. */
    @Test
    void testIteratorAndReaderCloseTheCatalogOnce() throws IOException {
        ClosingCatalog catalog = newCatalog();
        try (IcebergRecordReader reader = new IcebergRecordReader(task, table, null, catalog);
                CloseableIterator<RowWithPosResult> it = reader.readWithPos()) {
            while (it.hasNext()) {
                it.next();
            }
        }
        assertThat(catalog.closes).isEqualTo(1);
    }

    /** A re-index reads many splits, each with a catalog of its own. */
    @Test
    void testReadWithPosClosesTheCatalogOfItsSplit() throws IOException {
        ClosingCatalog catalog = newCatalog();
        IcebergRecordReader reader = new IcebergRecordReader(task, table, null, catalog);
        int rows = 0;
        try (CloseableIterator<RowWithPosResult> it = reader.readWithPos()) {
            while (it.hasNext()) {
                assertThat(it.next().getPos()).isEqualTo(rows);
                rows++;
            }
        }
        assertThat(rows).isEqualTo(3);
        assertThat(catalog.closed).isTrue();
    }

    /**
     * The lake half of a union read of a table with deletion vectors: every split is read by a
     * reader with a catalog of its own, with or without positions. Each catalog is closed once its
     * rows are read; a JDBC catalog holds database connections until then.
     */
    @Test
    void testUnionReadClosesEveryCatalogItOpens() throws Exception {
        appendFile(3);
        appendFile(6);
        Configuration conf = new Configuration();
        conf.setString("catalog-impl", CountingCatalog.class.getName());
        conf.setString("warehouse", "file://" + warehouse);
        IcebergLakeSource source = new IcebergLakeSource(conf, TablePath.of("db", "t"));
        List<IcebergSplit> splits =
                source.createPlanner(() -> table.currentSnapshot().snapshotId()).plan();
        assertThat(splits).hasSize(3);

        CountingCatalog.OPENED.set(0);
        CountingCatalog.CLOSED.set(0);
        int rows = 0;
        int positions = 0;
        for (IcebergSplit split : splits) {
            RecordReader reader = source.createRecordReader(() -> split);
            try (CloseableIterator<LogRecord> it = reader.read()) {
                while (it.hasNext()) {
                    it.next();
                    rows++;
                }
            }
            reader = source.createRecordReader(() -> split);
            try (CloseableIterator<RowWithPosResult> it =
                    ((SupportsRowPositions) reader).readWithPos()) {
                while (it.hasNext()) {
                    it.next();
                    positions++;
                }
            }
        }
        assertThat(rows).isEqualTo(9);
        assertThat(positions).isEqualTo(9);
        assertThat(CountingCatalog.OPENED.get()).isEqualTo(6);
        assertThat(CountingCatalog.CLOSED.get()).isEqualTo(6);
    }

    private void appendFile(int firstId) throws IOException {
        AppendFiles append = table.newAppend();
        try (TaskWriter<Record> writer = TaskWriterFactory.createTaskWriter(table, null, 0, true)) {
            for (int i = firstId; i < firstId + 3; i++) {
                Record record = GenericRecord.create(table.schema());
                record.setField("id", i);
                record.setField("name", "v" + i);
                writer.write(record);
            }
            for (DataFile file : writer.complete().dataFiles()) {
                append.appendFile(file);
            }
        }
        append.commit();
        table.refresh();
    }

    private ClosingCatalog newCatalog() {
        ClosingCatalog catalog = new ClosingCatalog();
        catalog.setConf(new org.apache.hadoop.conf.Configuration());
        HashMap<String, String> properties = new HashMap<>();
        properties.put("warehouse", "file://" + warehouse);
        catalog.initialize("test", properties);
        return catalog;
    }

    /** A Hadoop catalog, loaded by name, that counts the instances opened and closed. */
    public static final class CountingCatalog extends HadoopCatalog {
        static final AtomicInteger OPENED = new AtomicInteger();
        static final AtomicInteger CLOSED = new AtomicInteger();

        public CountingCatalog() {}

        @Override
        public void initialize(String name, Map<String, String> properties) {
            super.initialize(name, properties);
            OPENED.incrementAndGet();
        }

        @Override
        public void close() throws IOException {
            CLOSED.incrementAndGet();
            super.close();
        }
    }

    /** A Hadoop catalog that records that, and how often, it was closed. */
    private static final class ClosingCatalog extends HadoopCatalog {
        private boolean closed;
        private int closes;

        @Override
        public void close() throws IOException {
            closed = true;
            closes++;
            super.close();
        }
    }
}
