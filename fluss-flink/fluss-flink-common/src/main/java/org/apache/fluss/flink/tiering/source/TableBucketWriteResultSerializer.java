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

package org.apache.fluss.flink.tiering.source;

import org.apache.fluss.client.tiering.RoundDeletes;
import org.apache.fluss.client.tiering.TableBucketWriteResult;
import org.apache.fluss.client.tiering.TieringSplit;
import org.apache.fluss.lake.source.RowPosResult;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The serializer for {@link TableBucketWriteResult}.
 *
 * <h3>Version Evolution:</h3>
 *
 * <ul>
 *   <li><b>Version 1:</b> Initial version. The version of the lake write result serializer was not
 *       persisted; the write result was deserialized with this serializer's version instead.
 *   <li><b>Version 2:</b> Persists the version of the lake write result serializer before the write
 *       result bytes and passes it to the lake serializer on deserialization, so that lake formats
 *       can evolve their write result format independently.
 *   <li><b>Version 3 (Current):</b> Appends, after the number of write results and in the order
 *       they were added, the tiering epoch of the round ({@link
 *       TableBucketWriteResult#tieringEpoch()}), whether the bucket's round ended below its high
 *       watermark ({@link TableBucketWriteResult#endedEarly()}), the offset its log split started
 *       at ({@link TableBucketWriteResult#logStartOffset()}), the optional deletion-vector {@link
 *       RowPosResult} (index change id, re-index split, new file dictionary entries, SST metas and
 *       the failure of the split, if any) and the optional {@link RoundDeletes} of the bucket's
 *       round ({@link TableBucketWriteResult#roundDeletes()}): its snapshot id, start and end
 *       offsets, and the serialized positions per data file.
 * </ul>
 */
public class TableBucketWriteResultSerializer<WriteResult>
        implements SimpleVersionedSerializer<TableBucketWriteResult<WriteResult>> {

    private static final int INITIAL_BUFFER_SIZE = 64;

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(INITIAL_BUFFER_SIZE));

    private static final int VERSION_1 = 1;
    private static final int VERSION_2 = 2;
    private static final int VERSION_3 = 3;

    private static final int CURRENT_VERSION = VERSION_3;

    // a failure message is cut to fit a UTF string of at most 64 KB
    private static final int MAX_FAILURE_CHARS = 16_000;

    private final org.apache.fluss.lake.serializer.SimpleVersionedSerializer<WriteResult>
            writeResultSerializer;

    public TableBucketWriteResultSerializer(
            org.apache.fluss.lake.serializer.SimpleVersionedSerializer<WriteResult>
                    writeResultSerializer) {
        this.writeResultSerializer = writeResultSerializer;
    }

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(TableBucketWriteResult<WriteResult> tableBucketWriteResult)
            throws IOException {
        RoundDeletes roundDeletes = tableBucketWriteResult.roundDeletes();
        // round deletes take up to megabytes, so they do not grow the buffer the thread keeps
        final DataOutputSerializer out =
                roundDeletes == null
                        ? SERIALIZER_CACHE.get()
                        : new DataOutputSerializer(
                                (int)
                                        Math.min(
                                                Integer.MAX_VALUE,
                                                INITIAL_BUFFER_SIZE + roundDeletes.bytes()));
        // serialize table path
        TablePath tablePath = tableBucketWriteResult.tablePath();
        out.writeUTF(tablePath.getDatabaseName());
        out.writeUTF(tablePath.getTableName());

        // serialize bucket
        TableBucket tableBucket = tableBucketWriteResult.tableBucket();
        out.writeLong(tableBucket.getTableId());
        // write partition
        if (tableBucket.getPartitionId() != null) {
            out.writeBoolean(true);
            out.writeLong(tableBucket.getPartitionId());
            out.writeUTF(tableBucketWriteResult.partitionName());
        } else {
            out.writeBoolean(false);
        }
        out.writeInt(tableBucket.getBucket());

        // serialize write result
        WriteResult writeResult = tableBucketWriteResult.writeResult();
        if (writeResult == null) {
            // write -1 to mark write result as null
            out.writeInt(-1);
        } else {
            byte[] serializeBytes = writeResultSerializer.serialize(writeResult);
            out.writeInt(serializeBytes.length);
            // since VERSION_2
            out.writeInt(writeResultSerializer.getVersion());
            out.write(serializeBytes);
        }

        // serialize log end offset
        out.writeLong(tableBucketWriteResult.logEndOffset());

        // serialize max timestamp
        out.writeLong(tableBucketWriteResult.maxTimestamp());

        // serialize number of write results
        out.writeInt(tableBucketWriteResult.numberOfWriteResults());

        // since VERSION_3
        out.writeLong(tableBucketWriteResult.tieringEpoch());
        out.writeBoolean(tableBucketWriteResult.endedEarly());
        out.writeLong(tableBucketWriteResult.logStartOffset());

        // serialize the optional RowPosResult, since VERSION_3
        RowPosResult rowPosResult = tableBucketWriteResult.getRowPosResult();
        if (rowPosResult != null) {
            out.writeBoolean(true);
            out.writeLong(rowPosResult.getChangeId());
            out.writeInt(rowPosResult.getSplit());
            // serialize newFileDictEntries
            Map<Integer, String> newFileDictEntries = rowPosResult.getNewFileDictEntries();
            out.writeInt(newFileDictEntries.size());
            for (Map.Entry<Integer, String> entry : newFileDictEntries.entrySet()) {
                out.writeInt(entry.getKey());
                out.writeUTF(entry.getValue());
            }
            // serialize sstMetas
            List<RowPosResult.SstMeta> sstMetas = rowPosResult.getSstMetas();
            out.writeInt(sstMetas.size());
            for (RowPosResult.SstMeta meta : sstMetas) {
                out.writeUTF(meta.getFileName());
                out.writeLong(meta.getFileSize());
            }
            String failure = rowPosResult.getFailure();
            out.writeBoolean(failure != null);
            if (failure != null) {
                out.writeUTF(
                        failure.length() > MAX_FAILURE_CHARS
                                ? failure.substring(0, MAX_FAILURE_CHARS)
                                : failure);
            }
        } else {
            out.writeBoolean(false);
        }

        // since VERSION_3
        writeRoundDeletes(out, roundDeletes);

        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    @Override
    public TableBucketWriteResult<WriteResult> deserialize(int version, byte[] serialized)
            throws IOException {
        if (version < VERSION_1 || version > CURRENT_VERSION) {
            throw new IOException("Unknown version " + version);
        }
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        // deserialize table path
        String databaseName = in.readUTF();
        String tableName = in.readUTF();
        TablePath tablePath = new TablePath(databaseName, tableName);

        // deserialize bucket
        long tableId = in.readLong();
        Long partitionId = null;
        String partitionName = null;
        if (in.readBoolean()) {
            partitionId = in.readLong();
            partitionName = in.readUTF();
        }
        int bucketId = in.readInt();
        TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);

        // deserialize write result
        int writeResultLength = in.readInt();
        WriteResult writeResult;
        if (writeResultLength >= 0) {
            // VERSION_1 did not persist the lake serializer version and passed its own version
            // to the lake serializer, keep that behavior for data written with VERSION_1
            int writeResultVersion = version >= VERSION_2 ? in.readInt() : VERSION_1;
            byte[] writeResultBytes = new byte[writeResultLength];
            in.readFully(writeResultBytes);
            writeResult = writeResultSerializer.deserialize(writeResultVersion, writeResultBytes);
        } else {
            writeResult = null;
        }

        // deserialize log end offset
        long logEndOffset = in.readLong();
        // deserialize max timestamp
        long maxTimestamp = in.readLong();
        // deserialize number of write results
        int numberOfWriteResults = in.readInt();

        long tieringEpoch =
                version >= VERSION_3 ? in.readLong() : TieringSplit.UNKNOWN_TIERING_EPOCH;
        boolean endedEarly = version >= VERSION_3 && in.readBoolean();
        long logStartOffset =
                version >= VERSION_3
                        ? in.readLong()
                        : TableBucketWriteResult.UNKNOWN_LOG_START_OFFSET;

        // deserialize the optional RowPosResult, present since VERSION_3
        RowPosResult rowPosResult = null;
        if (version >= VERSION_3 && in.readBoolean()) {
            long changeId = in.readLong();
            int split = in.readInt();
            // deserialize newFileDictEntries
            int dictSize = in.readInt();
            Map<Integer, String> newFileDictEntries = new HashMap<>(dictSize);
            for (int d = 0; d < dictSize; d++) {
                int fileId = in.readInt();
                String fileName = in.readUTF();
                newFileDictEntries.put(fileId, fileName);
            }
            // deserialize sstMetas
            int sstMetaCount = in.readInt();
            List<RowPosResult.SstMeta> sstMetas = new ArrayList<>(sstMetaCount);
            for (int s = 0; s < sstMetaCount; s++) {
                String sstFileName = in.readUTF();
                long sstFileSize = in.readLong();
                sstMetas.add(new RowPosResult.SstMeta(sstFileName, sstFileSize));
            }
            String failure = in.readBoolean() ? in.readUTF() : null;
            rowPosResult = new RowPosResult(changeId, split, newFileDictEntries, sstMetas, failure);
        }

        RoundDeletes roundDeletes = version >= VERSION_3 ? readRoundDeletes(in) : null;

        return new TableBucketWriteResult<>(
                tablePath,
                tableBucket,
                partitionName,
                writeResult,
                logStartOffset,
                logEndOffset,
                maxTimestamp,
                numberOfWriteResults,
                rowPosResult,
                endedEarly,
                tieringEpoch,
                roundDeletes);
    }

    private static void writeRoundDeletes(
            DataOutputSerializer out, @Nullable RoundDeletes roundDeletes) throws IOException {
        if (roundDeletes == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        out.writeLong(roundDeletes.snapshotId());
        out.writeLong(roundDeletes.startOffset());
        out.writeLong(roundDeletes.endOffset());
        List<RoundDeletes.File> files = roundDeletes.files();
        out.writeInt(files.size());
        for (RoundDeletes.File file : files) {
            out.writeUTF(file.path());
            out.writeInt(file.positions().length);
            out.write(file.positions());
        }
    }

    @Nullable
    private static RoundDeletes readRoundDeletes(DataInputDeserializer in) throws IOException {
        if (!in.readBoolean()) {
            return null;
        }
        long snapshotId = in.readLong();
        long startOffset = in.readLong();
        long endOffset = in.readLong();
        int count = in.readInt();
        List<RoundDeletes.File> files = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String path = in.readUTF();
            byte[] positions = new byte[in.readInt()];
            in.readFully(positions);
            files.add(new RoundDeletes.File(path, positions));
        }
        return new RoundDeletes(snapshotId, startOffset, endOffset, files);
    }
}
