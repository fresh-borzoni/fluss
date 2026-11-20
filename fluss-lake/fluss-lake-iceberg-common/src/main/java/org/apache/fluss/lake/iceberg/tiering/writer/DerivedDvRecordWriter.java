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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.lake.dv.index.StagedIndexPart;
import org.apache.fluss.lake.iceberg.source.FlussRowAsIcebergRecord;
import org.apache.fluss.lake.iceberg.tiering.RecordWriter;
import org.apache.fluss.lake.iceberg.utils.IcebergConversions;
import org.apache.fluss.lake.writer.DeletionVectorWriterInitContext;
import org.apache.fluss.lake.writer.RoundMaskMismatchException;
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
import org.apache.fluss.utils.FileUtils;

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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * The record writer of tables with deletion vectors. It writes one live row of each primary key
 * changed in the round, its last version, and stages the bucket's index part ({@link
 * StagedIndexPart}): the data file and row position of every key's row, and the keys the round
 * deleted. The committer publishes the part, takes the rows the round supersedes from the buckets'
 * leaders, and commits their deletes with the data.
 *
 * <p>A round read through its leader's log mask ({@link
 * DeletionVectorWriterInitContext#roundMasked()}) comes as the versions still live at the round's
 * end, in log order. The writer writes each row as it comes and buffers only its index entry: the
 * key, the version's offset and the row's position, or a delete. Completing the round sorts the
 * entries by key and offset. A row of a key that a later entry of the key follows is a version the
 * mask missed: the writer fails the round with a {@link RoundMaskMismatchException}, and the round
 * is written again in sort mode, so no row of the round is superseded within it.
 *
 * <p>In sort mode, for a round read without the mask, the writer gets every record of the round.
 * They go serialized into the buffer, and completing the round writes the last version of every key
 * in key order, so no row is superseded within the round.
 *
 * <p>The buffer is a {@link SpillingKeySortBuffer}, which spills sorted runs to local disk when the
 * memory the task's writers share is used up, so the writer holds at most its share of the task's
 * memory, whatever the size of the round. The part's entries come out in key order, so the part is
 * written as it goes, not held in memory.
 *
 * <p>A table with deletion vectors is tiered from its log only, never from a KV snapshot.
 */
public class DerivedDvRecordWriter extends RecordWriter {

    private static final Logger LOG = LoggerFactory.getLogger(DerivedDvRecordWriter.class);

    private static final int ROW_POSITION_BYTES = Integer.BYTES + Long.BYTES;

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
    private final SpillingKeySortBuffer buffer;
    // the round comes through its leader's log mask: rows are written as they come
    private final boolean roundMasked;
    // an index entry's position of its row: the data file's id and the row's position in it
    private final byte[] rowPosition = new byte[ROW_POSITION_BYTES];
    @Nullable private final PositionIndexStore indexStore;
    private final long stagingId;
    private final String stagingPrefix;
    @Nullable private final File localTmpDir;

    // the data files rows were written to, by their ids in the staged part
    private final Map<String, Integer> fileIds = new HashMap<>();
    private final List<String> filePaths = new ArrayList<>();
    private final List<long[]> fileRows = new ArrayList<>();
    // the row position writeRow wrote its row at
    private long lastPosition;
    private long records;
    // the round completed: its files are the write result's, not the writer's to drop
    private boolean completed;
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
        // Fluss gives the writers of a table with deletion vectors their index directory and the
        // memory they share
        DeletionVectorWriterInitContext dvContext =
                writerInitContext instanceof DeletionVectorWriterInitContext
                        ? (DeletionVectorWriterInitContext) writerInitContext
                        : null;
        WriterMemoryPool pool = dvContext == null ? null : dvContext.writerMemoryPool();
        if (pool == null) {
            pool =
                    new WriterMemoryPool(
                            ConfigOptions.LAKE_TIERING_DELETION_VECTORS_WRITE_BUFFER_SIZE
                                    .defaultValue()
                                    .getBytes());
        }
        this.roundMasked = dvContext != null && dvContext.roundMasked();
        String[] tmpDirs = writerInitContext.ioTmpDirs();
        this.buffer = new SpillingKeySortBuffer(pool, tmpDirs);
        this.localTmpDir = FileUtils.randomTmpDir(tmpDirs, "dv");
        String indexDir = dvContext == null ? null : dvContext.positionIndexDir();
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
        if (changeType == ChangeType.UPDATE_BEFORE) {
            return; // the following +U carries the key and the new version
        }
        byte[] key = keyEncoder.encodeKey(record.getRow());
        if (roundMasked) {
            writeIndexEntry(key, record);
            return;
        }
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

    /**
     * Writes a version of a masked round as it comes and buffers its index entry. The entry's key
     * is the record's key and offset, so every version stays in the buffer, in key and offset
     * order, and a key that comes again after a row shows when the round completes.
     */
    private void writeIndexEntry(byte[] key, LogRecord record) throws IOException {
        byte[] entryKey = RoundIndexEntries.entryKey(key, record.logOffset());
        if (record.getChangeType() == ChangeType.DELETE) {
            buffer.add(entryKey, 0, entryKey.length, record.logOffset(), null, 0, 0);
            return;
        }
        int fileId = writeRow(record.getRow());
        ByteBuffer.wrap(rowPosition).putInt(fileId).putLong(lastPosition);
        buffer.add(
                entryKey,
                0,
                entryKey.length,
                record.logOffset(),
                rowPosition,
                0,
                ROW_POSITION_BYTES);
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
        Map<Integer, String> files;
        try {
            if (roundMasked) {
                result = dataWriter.complete();
                files = checkFiles(result);
                MaskedRoundIndex index = new MaskedRoundIndex(staged);
                try (CloseableIterator<SpillingKeySortBuffer.Entry> entries =
                        buffer.lastVersions()) {
                    while (entries.hasNext()) {
                        index.add(entries.next());
                    }
                    index.finish();
                }
                keys = index.keys;
                deletes = index.deletes;
            } else {
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
                // The staged part's SST writer, native options and local files go with a failure.
                files = checkFiles(result);
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
        if (staged != null) {
            stagedIndexPart = StagedIndexPart.of(stagingId, staged.finish(), files, keys, deletes);
        } else {
            LOG.warn(
                    "The deletion vector writer of {} has no position index directory; it"
                            + " stages no index part, so its round cannot commit.",
                    tableBucket);
        }
        LOG.info(
                "The deletion vector writer of {} wrote {} records{}, {} keys ({} deleted) in {}"
                        + " data files; {} spills of {} bytes; completed in {} ms.",
                tableBucket,
                records,
                roundMasked ? " through the round's log mask" : " in sort mode",
                keys,
                deletes,
                files.size(),
                buffer.spills(),
                buffer.bytes(),
                System.currentTimeMillis() - start);
        buffer.close();
        completed = true;
        return result;
    }

    /**
     * Checks that the data files hold exactly the rows written to them and returns their names by
     * the ids the staged part uses.
     */
    @VisibleForTesting
    protected Map<Integer, String> checkFiles(WriteResult result) {
        Map<Integer, String> files = new HashMap<>();
        for (DataFile f : result.dataFiles()) {
            String location = f.location();
            Integer id = fileIds.get(location);
            long written = id == null ? 0 : fileRows.get(id)[0];
            if (written != f.recordCount()) {
                throw new IllegalStateException(
                        String.format(
                                "The deletion vector writer of %s wrote %d rows to %s, but it holds %d.",
                                tableBucket, written, location, f.recordCount()));
            }
            files.put(id, IcebergConversions.fileName(location));
        }
        for (int id = 0; id < filePaths.size(); id++) {
            if (fileRows.get(id)[0] > 0 && !files.containsKey(id)) {
                throw new IllegalStateException(
                        String.format(
                                "The deletion vector writer of %s wrote %d rows to %s, which is not"
                                        + " among the data files.",
                                tableBucket, fileRows.get(id)[0], filePaths.get(id)));
            }
        }
        return files;
    }

    /**
     * Stages the index entries of a masked round, which come in key order and, within a key, in
     * offset order. A key that comes again after a delete was written again; one that comes again
     * after a row is a mismatch, as a later record of the key superseded a version the mask kept.
     */
    private final class MaskedRoundIndex {
        @Nullable private final PositionIndexStore.StagedPartWriter staged;
        // the key's last entry so far: its offset, and its row's file id and position, or a delete
        @Nullable private byte[] key;
        private long offset;
        private boolean delete;
        private int fileId;
        private long position;
        long keys;
        long deletes;

        MaskedRoundIndex(@Nullable PositionIndexStore.StagedPartWriter staged) {
            this.staged = staged;
        }

        void add(SpillingKeySortBuffer.Entry entry) throws IOException {
            byte[] entryKey = RoundIndexEntries.keyOf(entry.key());
            long entryOffset = RoundIndexEntries.offsetOf(entry.key());
            if (key != null && Arrays.equals(key, entryKey)) {
                if (!delete) {
                    throw new RoundMaskMismatchException(
                            String.format(
                                    "The log mask of the round of bucket %s does not match the"
                                            + " round: the key of the row at offset %d comes again"
                                            + " at offset %d.",
                                    tableBucket, offset, entryOffset));
                }
            } else {
                stage();
                key = entryKey;
            }
            offset = entryOffset;
            delete = entry.isDelete();
            if (!delete) {
                ByteBuffer value =
                        ByteBuffer.wrap(
                                entry.valueArray(), entry.valueOffset(), entry.valueLength());
                fileId = value.getInt();
                position = value.getLong();
            }
        }

        void finish() throws IOException {
            stage();
            key = null;
        }

        private void stage() throws IOException {
            if (key == null) {
                return;
            }
            keys++;
            if (delete) {
                deletes++;
                if (staged != null) {
                    staged.delete(key);
                }
            } else if (staged != null) {
                staged.put(key, new FilePos(fileId, position));
            }
        }
    }

    @Nullable
    public StagedIndexPart stagedIndexPart() {
        return stagedIndexPart;
    }

    @Override
    public void close() throws Exception {
        // deletes the spilled runs, also of a round that never completed
        buffer.close();
        if (completed) {
            super.close();
            return;
        }
        // A writer closed before its round completed, as the one of a round the reader reads
        // again, never has its files committed: they go with it.
        try {
            dataWriter.abort();
        } catch (IOException e) {
            LOG.warn(
                    "The deletion vector writer of {} could not delete the data files of a round"
                            + " that did not complete.",
                    tableBucket,
                    e);
        }
    }
}
