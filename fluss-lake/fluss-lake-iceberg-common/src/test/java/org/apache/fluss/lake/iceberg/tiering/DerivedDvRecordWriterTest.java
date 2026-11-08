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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.index.FlussPositionIndexStore;
import org.apache.fluss.lake.dv.index.PositionIndexChangeBuilder;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.lake.dv.index.StagedIndexPart;
import org.apache.fluss.lake.iceberg.IcebergLakeCatalog;
import org.apache.fluss.lake.iceberg.tiering.writer.DerivedDvRecordWriter;
import org.apache.fluss.lake.iceberg.tiering.writer.TaskWriterFactory;
import org.apache.fluss.lake.lakestorage.TestingLakeCatalogContext;
import org.apache.fluss.lake.writer.DeletionVectorWriterInitContext;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.lake.writer.WriterMemoryPool;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataTypes;

import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.ReadOptions;
import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.SstFileReader;
import io.github.fluss_contrib.rocksdb.SstFileReaderIterator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.parquet.Parquet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the tiering writer of tables with deletion vectors.
 *
 * <p>A round larger than the memory its writers share spills to local disk and is written as the
 * last version of every key, in key order. The writer stages the bucket's index part in the store.
 * The rows of a KV snapshot are written as they come.
 */
class DerivedDvRecordWriterTest {

    private static final long ROUND = 42L;

    @TempDir File tmp;

    private Configuration configuration;
    private IcebergLakeTieringFactory tieringFactory;
    private IcebergCatalogProvider catalogProvider;
    private File spillDir;
    private File indexDir;
    private FlussPositionIndexStore store;
    private TablePath tablePath;
    private TableInfo tableInfo;

    @BeforeEach
    void beforeEach() {
        configuration = new Configuration();
        configuration.setString("warehouse", "file://" + new File(tmp, "warehouse"));
        configuration.setString("type", "hadoop");
        configuration.setString("name", "test");
        tieringFactory = new IcebergLakeTieringFactory(configuration);
        catalogProvider = new IcebergCatalogProvider(configuration);
        spillDir = new File(tmp, "spill");
        assertThat(spillDir.mkdirs()).isTrue();
        indexDir = new File(tmp, "index");
        store = new FlussPositionIndexStore(new FsPath(indexDir.toURI()));
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testARoundLargerThanItsBufferWritesTheLastVersionOfEveryKey(int formatVersion)
            throws Exception {
        Table table = createTable("spilled_v" + formatVersion, formatVersion);
        // a few hundred records fit
        WriterMemoryPool pool = new WriterMemoryPool(64 * 1024);
        Random random = new Random(formatVersion);
        // key -> its last value, null if deleted
        Map<Integer, String> expected = new HashMap<>();
        IcebergWriteResult result;
        try (LakeWriter<IcebergWriteResult> writer = createWriter(pool)) {
            long offset = 0;
            for (int i = 0; i < 30_000; i++) {
                // hot keys get most of the versions
                int key = random.nextInt(4) == 0 ? random.nextInt(10) : random.nextInt(3_000);
                String previous = expected.get(key);
                int op = random.nextInt(10);
                if (op == 0) {
                    writer.write(record(offset++, ChangeType.DELETE, key, previous));
                    expected.put(key, null);
                } else {
                    String value = "v" + key + "_" + i;
                    if (previous != null && op < 5) {
                        writer.write(record(offset++, ChangeType.UPDATE_BEFORE, key, previous));
                        writer.write(record(offset++, ChangeType.UPDATE_AFTER, key, value));
                    } else {
                        writer.write(record(offset++, ChangeType.INSERT, key, value));
                    }
                    expected.put(key, value);
                }
            }
            // the round did not fit: its sorted runs are on local disk
            assertThat(spillDirs()).hasSize(1);
            result = writer.complete();
            assertThat(spillDirs()).isEmpty();
        }
        assertThat(pool.used()).isZero();

        // one row per live key, its last version, the data files sorted by key
        List<DataFile> files = Arrays.asList(result.getWriteResult().dataFiles());
        List<byte[]> writtenKeys = new ArrayList<>();
        Map<Integer, String> lake = new HashMap<>();
        Map<String, String> positions = new TreeMap<>();
        for (DataFile file : files) {
            long pos = 0;
            for (Record row : read(table, file)) {
                int key = (Integer) row.getField("id");
                assertThat(lake.put(key, (String) row.getField("v"))).isNull();
                byte[] encoded = encode(key);
                writtenKeys.add(encoded);
                positions.put(Arrays.toString(encoded), fileName(file) + "@" + pos++);
            }
        }
        Map<Integer, String> live = new HashMap<>(expected);
        live.values().removeIf(v -> v == null);
        assertThat(lake).isEqualTo(live);
        for (int i = 1; i < writtenKeys.size(); i++) {
            assertThat(
                            PositionIndexChangeBuilder.compareUnsigned(
                                    writtenKeys.get(i - 1), writtenKeys.get(i)))
                    .isNegative();
        }

        // the staged part: every changed key, at its row or deleted
        StagedIndexPart part = result.stagedIndexPart();
        assertThat(part).isNotNull();
        assertThat(part.stagingId())
                .isEqualTo(StagedIndexPart.stagingIdOf(tableInfo.getTableId(), ROUND));
        assertThat(part.keys()).isEqualTo(expected.size());
        assertThat(part.deletes()).isEqualTo(expected.size() - live.size());
        assertThat(part.files().values())
                .containsExactlyInAnyOrderElementsOf(
                        files.stream()
                                .map(DerivedDvRecordWriterTest::fileName)
                                .collect(Collectors.toList()));
        Published published = publish(part);
        assertThat(published.keys).hasSize(expected.size());
        assertThat(published.index).isEqualTo(positions);
        assertThat(published.deletions).isEqualTo(part.deletes());
    }

    /**
     * A writer that fails over in the middle of its round leaves no spill files; the round runs
     * again with a new writer.
     */
    @Test
    void testAWriterClosedInTheMiddleOfItsRoundLeavesNoSpillFiles() throws Exception {
        Table table = createTable("failover", 3);
        WriterMemoryPool pool = new WriterMemoryPool(16 * 1024);
        LakeWriter<IcebergWriteResult> writer = createWriter(pool);
        for (int i = 0; i < 5_000; i++) {
            writer.write(record(i, ChangeType.INSERT, i % 1_000, "v" + i));
        }
        assertThat(spillDirs()).hasSize(1);
        writer.close();
        assertThat(spillDirs()).isEmpty();
        assertThat(pool.used()).isZero();

        // the round runs again from its first record, with a new writer
        IcebergWriteResult result;
        try (LakeWriter<IcebergWriteResult> again = createWriter(pool)) {
            for (int i = 0; i < 5_000; i++) {
                again.write(record(i, ChangeType.INSERT, i % 1_000, "v" + i));
            }
            result = again.complete();
        }
        Map<Integer, String> lake = new HashMap<>();
        Map<String, String> positions = new TreeMap<>();
        for (DataFile file : result.getWriteResult().dataFiles()) {
            long pos = 0;
            for (Record row : read(table, file)) {
                int key = (Integer) row.getField("id");
                assertThat(lake.put(key, (String) row.getField("v"))).isNull();
                positions.put(Arrays.toString(encode(key)), fileName(file) + "@" + pos++);
            }
        }
        assertThat(lake).hasSize(1_000);
        for (int key = 0; key < 1_000; key++) {
            assertThat(lake.get(key)).isEqualTo("v" + (4_000 + key));
        }
        // the closed writer staged nothing, so the part is the new writer's alone
        Published published = publish(result.stagedIndexPart());
        assertThat(published.index).isEqualTo(positions);
    }

    /**
     * The heap a round holds is bounded by the memory its writers share, whatever the size of the
     * round. A copy of every changed row would take about 375 bytes per row of this table, 376 MB
     * for 1M rows.
     */
    @Test
    void testTheHeapOfARoundIsBoundedByTheSharedMemory() throws Exception {
        createTable("heap", 3);
        long budget = 32L << 20;
        for (int rows : new int[] {250_000, 500_000, 1_000_000}) {
            WriterMemoryPool pool = new WriterMemoryPool(budget);
            long before = usedHeapAfterGc();
            try (LakeWriter<IcebergWriteResult> writer = createWriter(pool)) {
                StringBuilder pad = new StringBuilder();
                for (int i = 0; i < 100; i++) {
                    pad.append('x');
                }
                for (int i = 0; i < rows; i++) {
                    writer.write(record(i, ChangeType.INSERT, i, pad + Integer.toString(i)));
                }
                long held = usedHeapAfterGc() - before;
                IcebergWriteResult result = writer.complete();
                assertThat(held).isLessThan(budget + (16L << 20));
                assertThat(result.stagedIndexPart().keys()).isEqualTo(rows);
            }
        }
    }

    /**
     * A round that fails once its data files are written, here because they do not hold the rows
     * written to them, leaves no staged index part on local disk: the part's SST writer is closed
     * and its directory deleted.
     */
    @Test
    void testAFailedRoundLeavesNoStagedPartOnLocalDisk() throws Exception {
        Table table = createTable("failed", 3);
        DerivedDvRecordWriter writer =
                new DerivedDvRecordWriter(
                        table,
                        initContext(new WriterMemoryPool(16 * 1024)),
                        TaskWriterFactory.createTaskWriter(table, null, 0, true)) {
                    @Override
                    protected Map<Integer, String> checkFiles(WriteResult result) {
                        throw new IllegalStateException("the data files do not match");
                    }
                };
        try {
            for (int i = 0; i < 100; i++) {
                writer.write(record(i, ChangeType.INSERT, i, "v" + i));
            }
            assertThatThrownBy(writer::complete).hasMessage("the data files do not match");
        } finally {
            writer.close();
        }
        assertThat(stagedPartDirs()).isEmpty();
    }

    // ------------------------------------------------------------------------------------------

    private Table createTable(String name, int formatVersion) throws Exception {
        tablePath = TablePath.of("iceberg", name);
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("v", DataTypes.STRING())
                                        .primaryKey("id")
                                        .build())
                        .distributedBy(1, "id")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED, true)
                        .customProperty("iceberg.format-version", String.valueOf(formatVersion))
                        .build();
        try (IcebergLakeCatalog catalog = new IcebergLakeCatalog(configuration)) {
            catalog.createTable(tablePath, descriptor, new TestingLakeCatalogContext());
        }
        tableInfo = TableInfo.of(tablePath, 7L, 1, descriptor, DEFAULT_REMOTE_DATA_DIR, 1L, 1L);
        return catalogProvider.get().loadTable(toIceberg(tablePath));
    }

    private LakeWriter<IcebergWriteResult> createWriter(WriterMemoryPool pool) throws Exception {
        return tieringFactory.createLakeWriter(initContext(pool));
    }

    private WriterInitContext initContext(WriterMemoryPool pool) {
        TablePath path = tablePath;
        TableInfo info = tableInfo;
        return new DeletionVectorWriterInitContext() {
            @Override
            public TablePath tablePath() {
                return path;
            }

            @Override
            public TableBucket tableBucket() {
                return new TableBucket(info.getTableId(), 0);
            }

            @Nullable
            @Override
            public String partition() {
                return null;
            }

            @Override
            public TableInfo tableInfo() {
                return info;
            }

            @Override
            public long tieringRoundTimestamp() {
                return ROUND;
            }

            @Override
            public String[] ioTmpDirs() {
                return new String[] {spillDir.getPath()};
            }

            @Override
            public String positionIndexDir() {
                return indexDir.toURI().toString();
            }

            @Override
            public WriterMemoryPool writerMemoryPool() {
                return pool;
            }

            @Override
            public int bucketCount() {
                return 1;
            }
        };
    }

    private File[] stagedPartDirs() {
        File dir = new File(spillDir, "dv");
        File[] dirs = dir.listFiles((d, name) -> name.startsWith("fluss-pkidx-stage-"));
        return dirs == null ? new File[0] : dirs;
    }

    private File[] spillDirs() {
        File[] dirs = spillDir.listFiles((dir, name) -> name.startsWith("fluss-sort-"));
        return dirs == null ? new File[0] : dirs;
    }

    private static LogRecord record(long offset, ChangeType type, int key, String value) {
        GenericRow row = new GenericRow(2);
        row.setField(0, key);
        row.setField(1, value == null ? null : BinaryString.fromString(value));
        return new GenericRecord(offset, 1_000L, type, row);
    }

    private byte[] encode(int key) {
        KeyEncoder encoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        tableInfo.getRowType(),
                        tableInfo.getPhysicalPrimaryKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());
        GenericRow row = new GenericRow(2);
        row.setField(0, key);
        byte[] encoded = encoder.encodeKey(row);
        return Arrays.copyOf(encoded, encoded.length);
    }

    private static List<Record> read(Table table, DataFile file) throws Exception {
        List<Record> rows = new ArrayList<>();
        try (CloseableIterable<Record> records =
                Parquet.read(table.io().newInputFile(file.location()))
                        .project(table.schema())
                        .createReaderFunc(
                                type -> GenericParquetReaders.buildReader(table.schema(), type))
                        .build()) {
            for (Record r : records) {
                rows.add(r.copy());
            }
        }
        return rows;
    }

    private static String fileName(DataFile file) {
        String location = file.location();
        return location.substring(location.lastIndexOf('/') + 1);
    }

    private static final class Published {
        final List<byte[]> keys = new ArrayList<>();
        final Map<String, String> index = new TreeMap<>();
        long deletions;
    }

    /** Publishes a staged part as a tablet server reads it: key -> "file@position". */
    private Published publish(StagedIndexPart part) throws Exception {
        TableBucket bucket = new TableBucket(tableInfo.getTableId(), 0);
        Map<Integer, Integer> newIds = new HashMap<>();
        for (int id : part.files().keySet()) {
            newIds.put(id, 100 + id);
        }
        Published published = new Published();
        long changeId = 4242L;
        PositionIndexStore.BucketPart publishedPart =
                store.publishStaged(
                        part.stagingId(),
                        changeId,
                        bucket,
                        part.part(),
                        newIds,
                        published.keys::add);
        Map<Integer, String> added = new HashMap<>();
        for (Map.Entry<Integer, String> f : part.files().entrySet()) {
            added.put(100 + f.getKey(), f.getValue());
        }
        store.commitChange(
                changeId,
                Collections.singletonMap(
                        bucket,
                        new PositionIndexStore.BucketChange(
                                publishedPart, added, Collections.emptyList())));
        store.register(-1, 1, changeId);
        PositionIndexStore.BucketData data =
                store.readChange(store.chain().get(0), bucket, new File(tmp, "download"));
        RocksDB.loadLibrary();
        for (String sst : data.localSsts) {
            try (Options options = new Options();
                    SstFileReader reader = new SstFileReader(options);
                    ReadOptions readOptions = new ReadOptions()) {
                reader.open(sst);
                published.deletions += reader.getTableProperties().getNumDeletions();
                try (SstFileReaderIterator it = reader.newIterator(readOptions)) {
                    for (it.seekToFirst(); it.isValid(); it.next()) {
                        FilePos pos = FilePos.decode(it.value());
                        published.index.put(
                                Arrays.toString(it.key()),
                                data.fileId2Name.get(pos.fileId()) + "@" + pos.rowPosition());
                    }
                }
            }
        }
        return published;
    }

    private static long usedHeapAfterGc() throws InterruptedException {
        for (int i = 0; i < 3; i++) {
            System.gc();
            Thread.sleep(100);
        }
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }
}
