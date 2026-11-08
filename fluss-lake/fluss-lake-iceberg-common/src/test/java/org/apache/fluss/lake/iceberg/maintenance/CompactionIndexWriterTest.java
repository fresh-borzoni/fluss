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

package org.apache.fluss.lake.iceberg.maintenance;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.RowPosSstIndex;
import org.apache.fluss.lake.dv.index.CompactionIndexPart;
import org.apache.fluss.lake.dv.index.FlussPositionIndexStore;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataTypes;

import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.ReadOptions;
import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.SstFileReader;
import io.github.fluss_contrib.rocksdb.SstFileReaderIterator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests the sorted runs of {@link CompactionIndexWriter} on every way a hand-off ends. */
class CompactionIndexWriterTest {

    private static final org.apache.iceberg.Schema ICEBERG_SCHEMA =
            new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "c1", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "c2", Types.StringType.get()));

    @TempDir private File tempDir;

    private TableInfo tableInfo;
    private TableBucket bucket;
    private FlussPositionIndexStore store;
    private File sortDir;

    @BeforeEach
    void beforeEach() {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("c1", DataTypes.INT())
                                        .column("c2", DataTypes.STRING())
                                        .primaryKey("c1")
                                        .build())
                        .distributedBy(1, "c1")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED, true)
                        .build();
        tableInfo =
                TableInfo.of(
                        TablePath.of("iceberg", "t"),
                        7,
                        1,
                        descriptor,
                        DEFAULT_REMOTE_DATA_DIR,
                        1L,
                        1L);
        bucket = new TableBucket(7, 0);
        store = new FlussPositionIndexStore(new FsPath(new File(tempDir, "index").toURI()));
        sortDir = new File(tempDir, "sort");
    }

    /**
     * The rows of a file group dropped after they were spilled stay out of the part; the kept rows,
     * empty files among them, land at their files and positions.
     */
    @Test
    void testDiscardedFileGroupIsLeftOutOfTheSortedRuns() throws Exception {
        CompactionIndexWriter writer = writer(1234L);
        // even keys are kept; the odd ones interleave with them in the sorted runs and are
        // discarded
        addKeys(writer, 0, 2, 4, 6, 8, 10, 12, 14, 16, 18);
        writer.groupWritten(
                new DataFile[] {dataFile("a", 6), dataFile("empty", 0), dataFile("b", 4)});
        addKeys(writer, 1, 3, 5, 7, 9, 11, 13);
        assertThat(filesUnder(sortDir)).hasSizeGreaterThan(2);
        writer.groupDiscarded();
        addKeys(writer, 20, 22);
        writer.groupWritten(new DataFile[] {dataFile("c", 2)});

        CompactionIndexPart part = writer.complete(5L, Collections.singletonList("old"));
        assertThat(part).isNotNull();
        assertThat(filesUnder(sortDir)).isEmpty();
        assertThat(part.addedFiles().values()).containsExactlyInAnyOrder("a", "empty", "b", "c");

        Map<String, String> expected = new TreeMap<>();
        int[] kept = {0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22};
        for (int i = 0; i < kept.length; i++) {
            String at = i < 6 ? "a@" + i : i < 10 ? "b@" + (i - 6) : "c@" + (i - 10);
            expected.put(key(kept[i]), at);
        }
        assertThat(handedOver(part)).isEqualTo(expected);
    }

    /** A hand-off that fails, finds a key twice or never completes leaves no sorted run. */
    @Test
    void testFailedOrAbortedHandOffDeletesItsSortedRuns() throws Exception {
        // the files hold fewer rows than were written
        CompactionIndexWriter miscounted = writer(1234L);
        addKeys(miscounted, 0, 1, 2, 3, 4, 5, 6);
        assertThat(filesUnder(sortDir)).isNotEmpty();
        miscounted.groupWritten(new DataFile[] {dataFile("a", 6)});
        assertThat(filesUnder(sortDir)).isEmpty();
        assertThat(miscounted.complete(5L, Collections.emptyList())).isNull();

        // a key in two runs, found only when they are merged
        CompactionIndexWriter duplicated = writer(1234L);
        addKeys(duplicated, 0, 1, 2, 3, 4, 5, 0);
        duplicated.groupWritten(new DataFile[] {dataFile("a", 7)});
        assertThat(filesUnder(sortDir)).isNotEmpty();
        assertThat(duplicated.complete(5L, Collections.emptyList())).isNull();
        assertThat(filesUnder(sortDir)).isEmpty();

        // the round is unknown
        CompactionIndexWriter unknownRound = writer(-1L);
        addKeys(unknownRound, 0, 1, 2, 3, 4, 5);
        unknownRound.groupWritten(new DataFile[] {dataFile("a", 6)});
        assertThat(unknownRound.complete(5L, Collections.emptyList())).isNull();
        assertThat(filesUnder(sortDir)).isEmpty();

        // the compaction stopped or failed before the hand-off completed
        CompactionIndexWriter aborted = writer(1234L);
        addKeys(aborted, 0, 1, 2, 3, 4, 5);
        assertThat(filesUnder(sortDir)).isNotEmpty();
        aborted.close();
        assertThat(filesUnder(sortDir)).isEmpty();
    }

    private CompactionIndexWriter writer(long roundTimestamp) {
        return new CompactionIndexWriter(store, tableInfo, bucket, roundTimestamp, sortDir, 3);
    }

    private static void addKeys(CompactionIndexWriter writer, int... keys) {
        for (int key : keys) {
            Record record = GenericRecord.create(ICEBERG_SCHEMA);
            record.setField("c1", key);
            record.setField("c2", "v" + key);
            writer.add(record);
        }
    }

    private static DataFile dataFile(String name, long rows) {
        return DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("file:/warehouse/data/" + name)
                .withFormat("parquet")
                .withFileSizeInBytes(1024)
                .withRecordCount(rows)
                .build();
    }

    private String key(int key) {
        KeyEncoder encoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        tableInfo.getRowType().project(new int[] {0}),
                        tableInfo.getPhysicalPrimaryKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());
        return Arrays.toString(encoder.encodeKey(GenericRow.of(key)));
    }

    /** The part as a tablet server reads it once registered: key to file@position. */
    private Map<String, String> handedOver(CompactionIndexPart part) throws Exception {
        List<RowPosSstIndex.SstFileEntry> ssts =
                Stream.iterate(0, i -> i + 1)
                        .limit(part.sstFileNames().size())
                        .map(
                                i ->
                                        new RowPosSstIndex.SstFileEntry(
                                                part.sstFileNames().get(i),
                                                part.sstFileSizes().get(i)))
                        .collect(Collectors.toList());
        store.commitChange(
                part.changeId(),
                Collections.singletonMap(
                        bucket,
                        new PositionIndexStore.BucketChange(
                                new PositionIndexStore.BucketPart(ssts),
                                part.addedFiles(),
                                part.removedFiles())));
        store.register(-1, 100, part.changeId());
        PositionIndexStore.BucketData data =
                store.readChange(store.chain().get(0), bucket, new File(tempDir, "download"));
        RocksDB.loadLibrary();
        Map<String, String> out = new TreeMap<>();
        for (String sst : data.localSsts) {
            try (Options options = new Options();
                    SstFileReader reader = new SstFileReader(options);
                    ReadOptions readOptions = new ReadOptions()) {
                reader.open(sst);
                try (SstFileReaderIterator it = reader.newIterator(readOptions)) {
                    for (it.seekToFirst(); it.isValid(); it.next()) {
                        FilePos pos = FilePos.decode(it.value());
                        out.put(
                                Arrays.toString(it.key()),
                                data.fileId2Name.get(pos.fileId()) + "@" + pos.rowPosition());
                    }
                }
            }
        }
        return out;
    }

    private static List<Path> filesUnder(File dir) {
        if (!dir.exists()) {
            return Collections.emptyList();
        }
        try (Stream<Path> files = Files.walk(dir.toPath())) {
            return files.filter(Files::isRegularFile).collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
