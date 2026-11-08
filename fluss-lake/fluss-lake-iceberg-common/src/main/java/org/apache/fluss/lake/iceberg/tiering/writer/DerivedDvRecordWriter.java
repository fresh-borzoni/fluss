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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.lake.dv.index.StagedIndexPart;
import org.apache.fluss.lake.iceberg.source.FlussRowAsIcebergRecord;
import org.apache.fluss.lake.iceberg.tiering.RecordWriter;
import org.apache.fluss.lake.writer.SpillingKeySortBuffer;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.lake.writer.WriterMemoryPool;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.compacted.CompactedRowDeserializer;
import org.apache.fluss.row.compacted.CompactedRowReader;
import org.apache.fluss.row.encode.CompactedRowEncoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * The record writer of tables with deletion vectors. It writes only the last version of each
 * primary key changed in the round, as data files without deletes, and stages the bucket's index
 * part ({@link StagedIndexPart}): the data file and row position of every written row, and the keys
 * the round deleted. The committer publishes the part, looks up the rows the round supersedes on
 * the way, and commits their deletes with the data.
 *
 * <p>The round's records go serialized into a {@link SpillingKeySortBuffer}, which spills sorted
 * runs to local disk when the memory the task's writers share is used up. Completing the round
 * merges the sorted runs and writes the last version of every key in key order. So the data files
 * are sorted by key, and the part's positions come in key order too: the part is written as it
 * goes, not held in memory. The writer holds at most its share of the task's memory, whatever the
 * size of the round.
 *
 * <p>The records of a KV snapshot ({@link WriterInitContext#readsKvSnapshot()}) have one version
 * per key and need no merge. They are written as they come, and only their keys and positions are
 * sorted for the part.
 */
public class DerivedDvRecordWriter extends RecordWriter {

    private static final Logger LOG = LoggerFactory.getLogger(DerivedDvRecordWriter.class);

    // a position in the buffer of a KV snapshot's writer: file id and row position
    private static final int POSITION_BYTES = 12;

    private final TableInfo tableInfo;
    private final TableBucket tableBucket;
    private final GenericRecordAppendOnlyWriter dataWriter;
    private final KeyEncoder keyEncoder;
    private final CompactedRowEncoder rowEncoder;
    private final InternalRow.FieldGetter[] fieldGetters;
    // decodes the buffered rows; a CompactedRow cannot be pointed at another row once read
    private final CompactedRowReader rowReader;
    private final CompactedRowDeserializer rowDeserializer;
    private final GenericRow mergedRow;
    private final FlussRowAsIcebergRecord icebergRecord;
    private final boolean kvSnapshot;
    private final SpillingKeySortBuffer buffer;
    @Nullable private final PositionIndexStore indexStore;
    private final long stagingId;
    private final String stagingPrefix;
    @Nullable private final File localTmpDir;

    // the data files rows were written to, by their ids in the staged part
    private final Map<String, Integer> fileIds = new HashMap<>();
    private final List<String> filePaths = new ArrayList<>();
    private final List<long[]> fileRows = new ArrayList<>();
    private final byte[] position = new byte[POSITION_BYTES];
    // the row position writeRow wrote its row at
    private long lastPosition;
    private long records;
    private long ordinal;
    @Nullable private StagedIndexPart stagedIndexPart;

    public DerivedDvRecordWriter(
            Table icebergTable,
            WriterInitContext writerInitContext,
            TaskWriter<Record> taskWriter) {
        super(
                taskWriter,
                icebergTable.schema(),
                writerInitContext.tableInfo().getRowType(),
                writerInitContext.tableBucket());
        checkArgument(
                taskWriter instanceof GenericRecordAppendOnlyWriter,
                "A table with deletion vectors is written by an append-only task writer, not %s.",
                taskWriter.getClass().getName());
        this.dataWriter = (GenericRecordAppendOnlyWriter) taskWriter;
        this.tableInfo = writerInitContext.tableInfo();
        this.tableBucket = writerInitContext.tableBucket();
        RowType rowType = tableInfo.getRowType();
        this.keyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        rowType,
                        tableInfo.getPhysicalPrimaryKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());
        DataType[] types = rowType.getChildren().toArray(new DataType[0]);
        this.rowEncoder = new CompactedRowEncoder(types);
        this.fieldGetters = new InternalRow.FieldGetter[types.length];
        for (int i = 0; i < types.length; i++) {
            fieldGetters[i] = InternalRow.createFieldGetter(types[i], i);
        }
        this.rowReader = new CompactedRowReader(types.length);
        this.rowDeserializer = new CompactedRowDeserializer(types);
        this.mergedRow = new GenericRow(types.length);
        this.icebergRecord = new FlussRowAsIcebergRecord(icebergTable.schema().asStruct(), rowType);
        this.kvSnapshot = writerInitContext.readsKvSnapshot();
        WriterMemoryPool pool = writerInitContext.writerMemoryPool();
        if (pool == null) {
            pool =
                    new WriterMemoryPool(
                            ConfigOptions.LAKE_TIERING_DELETION_VECTORS_WRITE_BUFFER_SIZE
                                    .defaultValue()
                                    .getBytes());
        }
        String[] tmpDirs = writerInitContext.ioTmpDirs();
        this.buffer = new SpillingKeySortBuffer(pool, tmpDirs, kvSnapshot);
        this.localTmpDir = tmpDirs == null || tmpDirs.length == 0 ? null : new File(tmpDirs[0]);
        String indexDir = writerInitContext.positionIndexDir();
        this.indexStore =
                indexDir == null
                        ? null
                        : PositionIndexStore.forTable(new FsPath(indexDir), localTmpDir);
        long roundTimestamp = writerInitContext.tieringRoundTimestamp();
        this.stagingId =
                roundTimestamp == WriterInitContext.UNKNOWN_TIERING_ROUND_TIMESTAMP
                        ? ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE)
                        : StagedIndexPart.stagingIdOf(tableInfo.getTableId(), roundTimestamp);
        // the writers of a bucket in different attempts of the round stage different files
        this.stagingPrefix = "w" + UUID.randomUUID().toString().replace("-", "") + "_";
    }

    @Override
    public void write(LogRecord record) throws Exception {
        ChangeType changeType = record.getChangeType();
        records++;
        if (kvSnapshot) {
            checkState(
                    changeType == ChangeType.INSERT || changeType == ChangeType.UPDATE_AFTER,
                    "A KV snapshot has no %s records.",
                    changeType);
            byte[] key = keyEncoder.encodeKey(record.getRow());
            int fileId = writeRow(record.getRow());
            putInt(position, 0, fileId);
            putLong(position, 4, lastPosition);
            buffer.add(key, 0, key.length, ordinal++, position, 0, POSITION_BYTES);
            return;
        }
        if (changeType == ChangeType.UPDATE_BEFORE) {
            return; // the following +U carries the key and the new version
        }
        byte[] key = keyEncoder.encodeKey(record.getRow());
        if (changeType == ChangeType.DELETE) {
            buffer.add(key, 0, key.length, record.logOffset(), null, 0, 0);
            return;
        }
        InternalRow row = record.getRow();
        rowEncoder.startNewRow();
        for (int i = 0; i < fieldGetters.length; i++) {
            rowEncoder.encodeField(i, fieldGetters[i].getFieldOrNull(row));
        }
        CompactedRow encoded = rowEncoder.finishRow();
        MemorySegment segment = encoded.getSegment();
        buffer.add(
                key,
                0,
                key.length,
                record.logOffset(),
                segment.getHeapMemory(),
                encoded.getOffset(),
                encoded.getSizeInBytes());
    }

    /** Writes a row and returns the id of its data file; lastPosition is set to its position. */
    private int writeRow(InternalRow row) throws IOException {
        String path = dataWriter.currentPath().toString();
        Integer fileId = fileIds.get(path);
        if (fileId == null) {
            fileId = filePaths.size();
            fileIds.put(path, fileId);
            filePaths.add(path);
            fileRows.add(new long[1]);
        }
        lastPosition = dataWriter.currentRows();
        dataWriter.write(icebergRecord.replace(row));
        fileRows.get(fileId)[0]++;
        return fileId;
    }

    /** Returns an upper bound on the number of keys the round changed so far. */
    public long bufferedKeys() {
        return buffer.distinctKeysUpperBound();
    }

    /** The bytes of the round's records the writer holds in memory and on local disk. */
    public long bufferedBytes() {
        return buffer.bytes();
    }

    @Override
    public WriteResult complete() throws Exception {
        long start = System.currentTimeMillis();
        PositionIndexStore.StagedPartWriter staged =
                indexStore == null
                        ? null
                        : indexStore.stageBucket(
                                stagingId, tableBucket, stagingPrefix, localTmpDir);
        long keys = 0;
        long deletes = 0;
        WriteResult result;
        try {
            if (!kvSnapshot) {
                // the last version of every key, in key order, written in that order
                try (CloseableIterator<SpillingKeySortBuffer.Entry> versions =
                        buffer.lastVersions()) {
                    while (versions.hasNext()) {
                        SpillingKeySortBuffer.Entry e = versions.next();
                        keys++;
                        if (e.isDelete()) {
                            deletes++;
                            if (staged != null) {
                                staged.delete(e.key());
                            }
                            continue;
                        }
                        rowReader.pointTo(
                                MemorySegment.wrap(e.valueArray()),
                                e.valueOffset(),
                                e.valueLength());
                        rowDeserializer.deserialize(rowReader, mergedRow);
                        int fileId = writeRow(mergedRow);
                        if (staged != null) {
                            staged.put(e.key(), new FilePos(fileId, lastPosition));
                        }
                    }
                }
                result = dataWriter.complete();
            } else {
                result = dataWriter.complete();
                try (CloseableIterator<SpillingKeySortBuffer.Entry> sorted =
                        buffer.lastVersions()) {
                    while (sorted.hasNext()) {
                        SpillingKeySortBuffer.Entry e = sorted.next();
                        keys++;
                        if (staged != null) {
                            byte[] v = e.valueArray();
                            int at = e.valueOffset();
                            staged.put(e.key(), new FilePos(getInt(v, at), getLong(v, at + 4)));
                        }
                    }
                }
            }
        } catch (UncheckedIOException e) {
            if (staged != null) {
                staged.close();
            }
            throw e.getCause();
        } catch (Exception e) {
            if (staged != null) {
                staged.close();
            }
            throw e;
        }
        Map<Integer, String> files = checkFiles(result);
        if (staged != null) {
            stagedIndexPart = StagedIndexPart.of(stagingId, staged.finish(), files, keys, deletes);
        } else {
            LOG.warn(
                    "Derived DV writer of {} has no position index directory; it stages no index"
                            + " part, so its round cannot commit.",
                    tableBucket);
        }
        LOG.info(
                "Derived DV writer of {}: {} records, {} keys ({} deleted) in {} data files; {}"
                        + " spills of {} bytes; completed in {} ms.",
                tableBucket,
                records,
                keys,
                deletes,
                files.size(),
                buffer.spills(),
                buffer.bytes(),
                System.currentTimeMillis() - start);
        buffer.close();
        return result;
    }

    /**
     * Checks that the data files hold exactly the rows written to them and returns their names by
     * the ids the staged part uses.
     */
    private Map<Integer, String> checkFiles(WriteResult result) {
        Map<Integer, String> files = new HashMap<>();
        for (DataFile f : result.dataFiles()) {
            String location = f.location();
            Integer id = fileIds.get(location);
            long written = id == null ? 0 : fileRows.get(id)[0];
            if (written != f.recordCount()) {
                throw new IllegalStateException(
                        String.format(
                                "Derived DV writer of %s wrote %d rows to %s, but it holds %d.",
                                tableBucket, written, location, f.recordCount()));
            }
            files.put(id, location.substring(location.lastIndexOf('/') + 1));
        }
        for (int id = 0; id < filePaths.size(); id++) {
            if (fileRows.get(id)[0] > 0 && !files.containsKey(id)) {
                throw new IllegalStateException(
                        String.format(
                                "Derived DV writer of %s wrote %d rows to %s, which is not among"
                                        + " the data files.",
                                tableBucket, fileRows.get(id)[0], filePaths.get(id)));
            }
        }
        return files;
    }

    @Nullable
    public StagedIndexPart stagedIndexPart() {
        return stagedIndexPart;
    }

    @Override
    public void close() throws Exception {
        // deletes the spilled runs, also of a round that never completed
        buffer.close();
        super.close();
    }

    private static void putInt(byte[] b, int at, int v) {
        b[at] = (byte) (v >>> 24);
        b[at + 1] = (byte) (v >>> 16);
        b[at + 2] = (byte) (v >>> 8);
        b[at + 3] = (byte) v;
    }

    private static int getInt(byte[] b, int at) {
        return ((b[at] & 0xff) << 24)
                | ((b[at + 1] & 0xff) << 16)
                | ((b[at + 2] & 0xff) << 8)
                | (b[at + 3] & 0xff);
    }

    private static void putLong(byte[] b, int at, long v) {
        putInt(b, at, (int) (v >>> 32));
        putInt(b, at + 4, (int) v);
    }

    private static long getLong(byte[] b, int at) {
        return ((long) getInt(b, at) << 32) | (getInt(b, at + 4) & 0xffffffffL);
    }
}
