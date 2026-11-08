/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.tiering.writer;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.ContentFileUtil;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Counterpart of {@link IcebergDvFileWriter} for format version 2 tables. It writes one merged,
 * file-scoped position delete file per data file, which replaces the data file's previous ones.
 */
public class IcebergPositionDeleteFileWriter implements Closeable {

    private final Table table;
    private final WrittenDeleteFiles writtenFiles;
    private final OutputFileFactory outputFileFactory;
    private final Function<String, PositionDeleteIndex> loadPrevious;
    private final Function<String, List<DeleteFile>> previousFiles;
    private final Map<String, DataFile> dataFiles = new LinkedHashMap<>();
    private final Map<String, List<long[]>> newPositions = new HashMap<>();
    private boolean completed;

    public IcebergPositionDeleteFileWriter(
            Table table,
            int partitionId,
            Function<String, PositionDeleteIndex> loadPrevious,
            Function<String, List<DeleteFile>> previousFiles) {
        this.table = table;
        this.writtenFiles = new WrittenDeleteFiles(table.io());
        this.outputFileFactory =
                OutputFileFactory.builderFor(table, partitionId, 0)
                        .format(FileFormat.PARQUET)
                        .ioSupplier(() -> writtenFiles)
                        .build();
        this.loadPrevious = loadPrevious;
        this.previousFiles = previousFiles;
    }

    /** Marks the given row positions as deleted in the given data file. */
    public void delete(DataFile dataFile, long... positions) {
        String path = dataFile.location();
        dataFiles.putIfAbsent(path, dataFile);
        newPositions.computeIfAbsent(path, k -> new ArrayList<>()).add(positions);
    }

    /** Writes the delete files and reports the previous file-scoped ones they replace. */
    public DeleteWriteResult complete() throws IOException {
        List<DeleteFile> written = new ArrayList<>();
        List<DeleteFile> rewritten = new ArrayList<>();
        CharSequenceSet referenced = CharSequenceSet.empty();
        for (Map.Entry<String, DataFile> e : dataFiles.entrySet()) {
            String path = e.getKey();
            DataFile dataFile = e.getValue();
            long[] all = mergedPositions(path);
            PartitionSpec spec = table.specs().get(dataFile.specId());
            EncryptedOutputFile out = outputFileFactory.newOutputFile(spec, dataFile.partition());
            // Path/position-only deletes, written with APIs of both Iceberg 1.10 and 1.12 (1.12
            // removed GenericAppenderFactory and position deletes with rows).
            PositionDeleteWriter<Record> writer =
                    Parquet.writeDeletes(out)
                            .withSpec(spec)
                            .withPartition(dataFile.partition())
                            .withKeyMetadata(out.keyMetadata())
                            .overwrite()
                            .buildPositionWriter();
            PositionDelete<Record> delete = PositionDelete.create();
            try {
                for (long pos : all) {
                    writer.write(delete.set(path, pos));
                }
            } finally {
                writer.close();
            }
            // The referenced data file makes it file-scoped: the next round can replace it, and
            // readers of other data files skip it without opening it.
            written.add(
                    org.apache.iceberg.FileMetadata.deleteFileBuilder(spec)
                            .copy(writer.toDeleteFile())
                            .withReferencedDataFile(path)
                            .build());
            referenced.add(path);
            // Shared delete files are kept; re-stating their positions here is harmless.
            for (DeleteFile previous : previousFiles.apply(path)) {
                if (previous.content() == FileContent.POSITION_DELETES
                        && ContentFileUtil.isFileScoped(previous)) {
                    rewritten.add(previous);
                }
            }
        }
        completed = true;
        return new DeleteWriteResult(written, referenced, rewritten);
    }

    /** Returns the previous and new deleted positions of the data file, sorted and distinct. */
    private long[] mergedPositions(String path) {
        int n = 0;
        List<long[]> batches = newPositions.getOrDefault(path, Collections.emptyList());
        for (long[] b : batches) {
            n += b.length;
        }
        PositionDeleteIndex previous = loadPrevious.apply(path);
        long prevCount = previous == null ? 0 : previous.cardinality();
        long[] all = new long[(int) (n + prevCount)];
        int i = 0;
        for (long[] b : batches) {
            System.arraycopy(b, 0, all, i, b.length);
            i += b.length;
        }
        if (previous != null) {
            final int[] idx = {i};
            previous.forEach(p -> all[idx[0]++] = p);
            i = idx[0];
        }
        long[] filled = i == all.length ? all : Arrays.copyOf(all, i);
        Arrays.sort(filled);
        int w = 0;
        for (int r = 0; r < filled.length; r++) {
            if (w == 0 || filled[r] != filled[w - 1]) {
                filled[w++] = filled[r];
            }
        }
        return w == filled.length ? filled : Arrays.copyOf(filled, w);
    }

    /**
     * Deletes the files the writer wrote unless {@link #complete()} returned them, e.g. those of a
     * write that failed in the middle.
     */
    @Override
    public void close() {
        if (!completed) {
            writtenFiles.deleteAll();
        }
    }
}
