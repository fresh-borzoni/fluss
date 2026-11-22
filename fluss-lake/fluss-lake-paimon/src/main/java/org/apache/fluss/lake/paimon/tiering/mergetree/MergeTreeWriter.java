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

package org.apache.fluss.lake.paimon.tiering.mergetree;

import org.apache.fluss.lake.paimon.tiering.RecordWriter;
import org.apache.fluss.lake.writer.WriterMemoryPool;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IOUtils;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.RowKeyExtractor;
import org.apache.paimon.table.sink.TableWriteImpl;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.fluss.lake.paimon.tiering.PaimonLakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toRowKind;

/** A {@link RecordWriter} to write to Paimon's primary-key table. */
public class MergeTreeWriter extends RecordWriter<KeyValue> {

    private final KeyValue keyValue = new KeyValue();

    private final RowKeyExtractor rowKeyExtractor;

    private final IOManager ioManager;

    // the buffer this writer shares with the other writers of its tiering task; null if it has
    // its own
    @Nullable private final SharedWriteBuffers.Lease writeBuffer;

    /**
     * A writer that buffers in the write buffer the writers of its tiering task share, of the
     * capacity of {@code taskMemory}, or in its own of the table's 'write-buffer-size' if null.
     */
    public MergeTreeWriter(
            FileStoreTable fileStoreTable,
            TableBucket tableBucket,
            @Nullable String partition,
            List<String> partitionKeys,
            RowType flussRowType,
            @Nullable String[] ioTmpDirs,
            @Nullable WriterMemoryPool taskMemory,
            boolean paimonIncludingSystemColumns,
            boolean historicalPartition) {
        this(
                fileStoreTable,
                createIOManager(ioTmpDirs),
                taskMemory == null
                        ? null
                        : SharedWriteBuffers.acquire(
                                taskMemory, fileStoreTable.coreOptions().pageSize()),
                tableBucket,
                partition,
                partitionKeys,
                flussRowType,
                paimonIncludingSystemColumns,
                historicalPartition);
    }

    private MergeTreeWriter(
            FileStoreTable fileStoreTable,
            IOManager ioManager,
            @Nullable SharedWriteBuffers.Lease writeBuffer,
            TableBucket tableBucket,
            @Nullable String partition,
            List<String> partitionKeys,
            RowType flussRowType,
            boolean paimonIncludingSystemColumns,
            boolean historicalPartition) {
        super(
                createTableWrite(fileStoreTable, ioManager, writeBuffer),
                fileStoreTable.rowType(),
                tableBucket,
                partition,
                partitionKeys,
                flussRowType,
                paimonIncludingSystemColumns,
                historicalPartition);
        this.rowKeyExtractor = fileStoreTable.createRowKeyExtractor();
        this.ioManager = ioManager;
        this.writeBuffer = writeBuffer;
    }

    private static IOManager createIOManager(@Nullable String[] ioTmpDirs) {
        return IOManager.create(getIoManagerTmpDirs(ioTmpDirs));
    }

    static String[] getIoManagerTmpDirs(@Nullable String[] ioTmpDirs) {
        if (ioTmpDirs != null && ioTmpDirs.length > 0) {
            return ioTmpDirs;
        }
        return new String[] {System.getProperty("java.io.tmpdir")};
    }

    private static TableWriteImpl<KeyValue> createTableWrite(
            FileStoreTable fileStoreTable,
            IOManager ioManager,
            @Nullable SharedWriteBuffers.Lease writeBuffer) {
        //noinspection unchecked
        TableWriteImpl<KeyValue> write =
                (TableWriteImpl<KeyValue>)
                        fileStoreTable
                                .newWrite(FLUSS_LAKE_TIERING_COMMIT_USER)
                                .withIOManager(ioManager);
        return writeBuffer == null ? write : write.withMemoryPoolFactory(writeBuffer.factory());
    }

    @Override
    public void close() throws Exception {
        IOUtils.closeAll(super::close, ioManager, writeBuffer);
    }

    @Override
    public void write(LogRecord record) throws Exception {
        BinaryRow targetPartition = prepareRecordAndGetPartition(record);

        rowKeyExtractor.setRecord(flussRecordAsPaimonRow);
        keyValue.replace(
                rowKeyExtractor.trimmedPrimaryKey(),
                KeyValue.UNKNOWN_SEQUENCE,
                toRowKind(record.getChangeType()),
                flussRecordAsPaimonRow);
        // hacky, call internal method tableWrite.getWrite() to support
        // to write to given partition, otherwise, it'll always extract a partition from Paimon row
        // which may be costly
        tableWrite.getWrite().write(targetPartition, bucket, keyValue);
    }
}
