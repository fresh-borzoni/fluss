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

package org.apache.fluss.lake.iceberg.tiering.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link IcebergPositionDeleteFileWriter} against a local format-version 2 table. */
class IcebergPositionDeleteFileWriterTest {

    private static final Schema SCHEMA =
            new Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "data", Types.StringType.get()));

    @TempDir File tempDir;

    private Table table;

    @BeforeEach
    void setUp() {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, "2");
        table =
                new HadoopTables(new Configuration())
                        .create(
                                SCHEMA,
                                PartitionSpec.unpartitioned(),
                                props,
                                new File(tempDir, "t").toURI().toString());
    }

    @Test
    void testDeletesAreMaskedOnRead() throws Exception {
        DataFile dataFile = appendDataFile(0, 5);

        DeleteWriteResult result;
        try (IcebergPositionDeleteFileWriter writer =
                new IcebergPositionDeleteFileWriter(
                        table, 0, path -> null, path -> Collections.emptyList())) {
            writer.delete(dataFile, 3L, 1L);
            writer.delete(dataFile, 3L); // the same position twice in one round
            result = writer.complete();
        }

        assertThat(result.deleteFiles()).hasSize(1);
        DeleteFile deleteFile = result.deleteFiles().get(0);
        assertThat(deleteFile.content()).isEqualTo(FileContent.POSITION_DELETES);
        assertThat(deleteFile.format()).isEqualTo(FileFormat.PARQUET);
        assertThat(deleteFile.recordCount()).isEqualTo(2L);
        assertThat(deleteFile.referencedDataFile()).isEqualTo(dataFile.location());
        assertThat(ContentFileUtil.isFileScoped(deleteFile)).isTrue();
        assertThat(result.referencedDataFiles()).hasSize(1);
        assertThat(result.referencedDataFiles().iterator().next().toString())
                .isEqualTo(dataFile.location());
        assertThat(result.rewrittenDeleteFiles()).isEmpty();
        assertThat(positions(Collections.singletonList(deleteFile), dataFile.location()))
                .containsExactly(1L, 3L);

        commit(result);
        assertThat(readIds()).containsExactly(0, 2, 4);
    }

    @Test
    void testSecondRoundMergesPreviousDeletesIntoOneFile() throws Exception {
        DataFile dataFile = appendDataFile(0, 6);
        DeleteFile first;
        try (IcebergPositionDeleteFileWriter writer =
                new IcebergPositionDeleteFileWriter(
                        table, 0, path -> null, path -> Collections.emptyList())) {
            writer.delete(dataFile, 1L, 3L);
            DeleteWriteResult result = writer.complete();
            commit(result);
            first = result.deleteFiles().get(0);
        }
        assertThat(first.referencedDataFile()).isEqualTo(dataFile.location());
        assertThat(readIds()).containsExactly(0, 2, 4, 5);

        // round 2: the previous deletes come from the table, like the committer loads them
        Map<String, List<DeleteFile>> previous = deletesByDataFile();
        assertThat(previous.get(dataFile.location()))
                .extracting(ContentFile::location)
                .containsExactly(first.location());
        DeleteLoader loader = new BaseDeleteLoader(df -> table.io().newInputFile(df.location()));
        Function<String, PositionDeleteIndex> loadPrevious =
                path -> loader.loadPositionDeletes(previous.get(path), path);

        DeleteWriteResult result;
        try (IcebergPositionDeleteFileWriter writer =
                new IcebergPositionDeleteFileWriter(table, 0, loadPrevious, previous::get)) {
            writer.delete(dataFile, 0L, 3L); // 3 is deleted already
            result = writer.complete();
        }

        assertThat(result.deleteFiles()).hasSize(1);
        DeleteFile merged = result.deleteFiles().get(0);
        assertThat(merged.location()).isNotEqualTo(first.location());
        assertThat(merged.recordCount()).isEqualTo(3L);
        assertThat(merged.referencedDataFile()).isEqualTo(dataFile.location());
        assertThat(ContentFileUtil.isFileScoped(merged)).isTrue();
        assertThat(positions(Collections.singletonList(merged), dataFile.location()))
                .containsExactly(0L, 1L, 3L);
        assertThat(result.rewrittenDeleteFiles())
                .extracting(ContentFile::location)
                .containsExactly(first.location());

        commit(result);
        assertThat(readIds()).containsExactly(2, 4, 5);
        // the replaced file is gone from the table: deletes do not pile up per data file
        assertThat(deletesByDataFile().get(dataFile.location()))
                .extracting(ContentFile::location)
                .containsExactly(merged.location());
    }

    @Test
    void testOneDeleteFilePerDataFileAndSharedDeleteFileIsKept() throws Exception {
        DataFile f1 = appendDataFile(0, 3);
        DataFile f2 = appendDataFile(10, 3);
        // another engine wrote one position-delete file covering both data files
        DeleteFile shared = writeSharedDeleteFile(f1, 0L, f2, 0L);
        table.newRowDelta().addDeletes(shared).commit();
        assertThat(ContentFileUtil.isFileScoped(shared)).isFalse();
        assertThat(readIds()).containsExactly(1, 2, 11, 12);

        Map<String, List<DeleteFile>> previous = deletesByDataFile();
        DeleteLoader loader = new BaseDeleteLoader(df -> table.io().newInputFile(df.location()));
        DeleteWriteResult result;
        try (IcebergPositionDeleteFileWriter writer =
                new IcebergPositionDeleteFileWriter(
                        table,
                        0,
                        path -> loader.loadPositionDeletes(previous.get(path), path),
                        previous::get)) {
            writer.delete(f1, 1L);
            writer.delete(f2, 2L);
            result = writer.complete();
        }

        assertThat(result.deleteFiles()).hasSize(2);
        assertThat(result.deleteFiles())
                .extracting(DeleteFile::referencedDataFile)
                .containsExactlyInAnyOrder(f1.location(), f2.location());
        for (DeleteFile d : result.deleteFiles()) {
            assertThat(ContentFileUtil.isFileScoped(d)).isTrue();
        }
        Set<String> referenced = new TreeSet<>();
        result.referencedDataFiles().forEach(p -> referenced.add(p.toString()));
        assertThat(referenced).containsExactlyInAnyOrder(f1.location(), f2.location());
        // the shared file is not file-scoped: it is kept, its positions are re-stated
        assertThat(result.rewrittenDeleteFiles()).isEmpty();
        assertThat(positions(result.deleteFiles(), f1.location())).containsExactly(0L, 1L);
        assertThat(positions(result.deleteFiles(), f2.location())).containsExactly(0L, 2L);

        commit(result);
        assertThat(readIds()).containsExactly(2, 11);
    }

    // ------------------------------------------------------------------------------------------

    private DataFile appendDataFile(int firstId, int rows) throws IOException {
        OutputFileFactory files =
                OutputFileFactory.builderFor(table, 1, firstId).format(FileFormat.PARQUET).build();
        GenericAppenderFactory appenders = new GenericAppenderFactory(SCHEMA);
        DataWriter<Record> writer =
                appenders.newDataWriter(files.newOutputFile(), FileFormat.PARQUET, null);
        try {
            for (int i = 0; i < rows; i++) {
                GenericRecord record = GenericRecord.create(SCHEMA);
                record.setField("id", firstId + i);
                record.setField("data", "v" + (firstId + i));
                writer.write(record);
            }
        } finally {
            writer.close();
        }
        DataFile dataFile = writer.toDataFile();
        table.newAppend().appendFile(dataFile).commit();
        return dataFile;
    }

    private DeleteFile writeSharedDeleteFile(DataFile a, long posA, DataFile b, long posB)
            throws IOException {
        OutputFileFactory files =
                OutputFileFactory.builderFor(table, 2, 0).format(FileFormat.PARQUET).build();
        PositionDeleteWriter<Record> writer =
                new GenericAppenderFactory(SCHEMA, PartitionSpec.unpartitioned())
                        .newPosDeleteWriter(files.newOutputFile(), FileFormat.PARQUET, null);
        List<String[]> sorted = new ArrayList<>();
        sorted.add(new String[] {a.location(), String.valueOf(posA)});
        sorted.add(new String[] {b.location(), String.valueOf(posB)});
        sorted.sort((x, y) -> x[0].compareTo(y[0]));
        PositionDelete<Record> delete = PositionDelete.create();
        try {
            for (String[] d : sorted) {
                writer.write(delete.set(d[0], Long.parseLong(d[1]), null));
            }
        } finally {
            writer.close();
        }
        return writer.toDeleteFile();
    }

    private void commit(DeleteWriteResult result) {
        RowDelta rowDelta = table.newRowDelta();
        result.deleteFiles().forEach(rowDelta::addDeletes);
        result.rewrittenDeleteFiles().forEach(rowDelta::removeDeletes);
        rowDelta.commit();
        table.refresh();
    }

    private Map<String, List<DeleteFile>> deletesByDataFile() throws IOException {
        table.refresh();
        Map<String, List<DeleteFile>> out = new HashMap<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                out.put(task.file().location(), new ArrayList<>(task.deletes()));
            }
        }
        return out;
    }

    private Set<Long> positions(List<DeleteFile> deleteFiles, String dataFilePath) {
        DeleteLoader loader = new BaseDeleteLoader(df -> table.io().newInputFile(df.location()));
        PositionDeleteIndex index = loader.loadPositionDeletes(deleteFiles, dataFilePath);
        Set<Long> out = new TreeSet<>();
        index.forEach(out::add);
        return out;
    }

    private List<Integer> readIds() throws IOException {
        table.refresh();
        List<Integer> ids = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record r : records) {
                ids.add((Integer) r.getField("id"));
            }
        }
        Collections.sort(ids);
        return ids;
    }
}
