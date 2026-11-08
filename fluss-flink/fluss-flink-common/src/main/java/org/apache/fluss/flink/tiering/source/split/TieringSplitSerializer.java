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

package org.apache.fluss.flink.tiering.source.split;

import org.apache.fluss.client.tiering.TieringLogSplit;
import org.apache.fluss.client.tiering.TieringRowPosSplit;
import org.apache.fluss.client.tiering.TieringSnapshotSplit;
import org.apache.fluss.client.tiering.TieringSplit;
import org.apache.fluss.flink.tiering.source.TieringSource;
import org.apache.fluss.flink.tiering.source.enumerator.TieringSourceEnumerator;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;

import static org.apache.fluss.client.tiering.TieringRowPosSplit.TIERING_ROW_POS_SPLIT_FLAG;
import static org.apache.fluss.client.tiering.TieringSplit.TIERING_LOG_SPLIT_FLAG;
import static org.apache.fluss.client.tiering.TieringSplit.TIERING_SNAPSHOT_SPLIT_FLAG;

/**
 * A serializer for the {@link FlinkTieringSplit}.
 *
 * <p>This serializer is mainly used to send splits from {@link TieringSourceEnumerator} to {@link
 * TieringSource}.
 *
 * <h3>Version Evolution:</h3>
 *
 * <ul>
 *   <li><b>Version 0:</b> Initial version.
 *   <li><b>Version 1 (Current):</b> Adds the tiering epoch of the split's round after the round's
 *       timestamp, and the row position split of a re-index. A split of version 0 has no epoch, so
 *       its round's commits are not fenced.
 * </ul>
 */
public class TieringSplitSerializer implements SimpleVersionedSerializer<FlinkTieringSplit> {

    public static final TieringSplitSerializer INSTANCE = new TieringSplitSerializer();

    private static final int VERSION_0 = 0;
    private static final int VERSION_1 = 1;

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final int CURRENT_VERSION = VERSION_1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(FlinkTieringSplit flinkSplit) throws IOException {
        final DataOutputSerializer out = SERIALIZER_CACHE.get();
        TieringSplit split = flinkSplit.unwrap();

        byte splitKind = split.splitKind();
        out.writeByte(splitKind);

        // write table path
        TablePath tablePath = split.getTablePath();
        out.writeUTF(tablePath.getDatabaseName());
        out.writeUTF(tablePath.getTableName());

        // write table id
        TableBucket tableBucket = split.getTableBucket();
        out.writeLong(tableBucket.getTableId());

        // write bucket
        out.writeInt(tableBucket.getBucket());

        // write partition
        if (split.getTableBucket().getPartitionId() != null) {
            out.writeBoolean(true);
            out.writeLong(split.getTableBucket().getPartitionId());
            out.writeUTF(split.getPartitionName());
        } else {
            out.writeBoolean(false);
        }

        // write number of splits
        out.writeInt(split.getNumberOfSplits());
        // write skipCurrentRound
        out.writeBoolean(split.shouldSkipCurrentRound());
        // write split metadata for this tiering round
        out.writeInt(split.getSplitIndex());
        out.writeLong(split.getTieringRoundTimestamp());
        // since VERSION_1
        out.writeLong(split.getTieringEpoch());
        if (split.isTieringSnapshotSplit()) {
            // Snapshot split
            TieringSnapshotSplit tieringSnapshotSplit = split.asTieringSnapshotSplit();
            // write snapshot id
            out.writeLong(tieringSnapshotSplit.getSnapshotId());
            // write log offset of snapshot
            out.writeLong(tieringSnapshotSplit.getLogOffsetOfSnapshot());
        } else if (split.isTieringRowPosSplit()) {
            // Row position split: some files of one bucket to re-index, by reference
            TieringRowPosSplit rowPosSplit = split.asTieringRowPosSplit();
            int[] fileIds = rowPosSplit.getFileIds();
            out.writeInt(fileIds.length);
            for (int fileId : fileIds) {
                out.writeInt(fileId);
            }
            out.writeUTF(rowPosSplit.getPlanReference());
            out.writeInt(rowPosSplit.getSplit());
            out.writeLong(rowPosSplit.getChangeId());
            out.writeUTF(rowPosSplit.getRemoteUploadBasePath());
        } else {
            // Log split
            TieringLogSplit tieringLogSplit = split.asTieringLogSplit();
            // write starting offset
            out.writeLong(tieringLogSplit.getStartingOffset());
            // write stopping offset
            out.writeLong(tieringLogSplit.getStoppingOffset());
        }

        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    @Override
    public FlinkTieringSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION_0 && version != VERSION_1) {
            throw new IOException("Unknown version " + version);
        }
        final DataInputDeserializer in = new DataInputDeserializer(serialized);

        byte splitKind = in.readByte();

        // deserialize table path
        String databaseName = in.readUTF();
        String tableName = in.readUTF();
        TablePath tablePath = new TablePath(databaseName, tableName);

        // deserialize table id
        long tableId = in.readLong();

        // deserialize table bucket
        int bucketId = in.readInt();

        // deserialize table partition
        Long partitionId = null;
        String partitionName = null;
        if (in.readBoolean()) {
            partitionId = in.readLong();
            partitionName = in.readUTF();
        }
        TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);

        // deserialize number of splits
        int numberOfSplits = in.readInt();
        boolean skipCurrentRound = in.readBoolean();
        int splitIndex = in.readInt();
        long tieringRoundTimestamp = in.readLong();
        long tieringEpoch =
                version >= VERSION_1 ? in.readLong() : TieringSplit.UNKNOWN_TIERING_EPOCH;

        TieringSplit tieringSplit;
        if (splitKind == TIERING_SNAPSHOT_SPLIT_FLAG) {
            // deserialize snapshot id
            long snapshotId = in.readLong();
            // deserialize log offset of snapshot
            long logOffsetOfSnapshot = in.readLong();
            tieringSplit =
                    new TieringSnapshotSplit(
                            tablePath,
                            tableBucket,
                            partitionName,
                            snapshotId,
                            logOffsetOfSnapshot,
                            numberOfSplits,
                            skipCurrentRound,
                            splitIndex,
                            tieringRoundTimestamp);
        } else if (splitKind == TIERING_ROW_POS_SPLIT_FLAG) {
            int fileCount = in.readInt();
            int[] fileIds = new int[fileCount];
            for (int i = 0; i < fileCount; i++) {
                fileIds[i] = in.readInt();
            }
            String planReference = in.readUTF();
            int rowPosSplitIndex = in.readInt();
            long changeId = in.readLong();
            String remoteUploadBasePath = in.readUTF();
            tieringSplit =
                    new TieringRowPosSplit(
                            tablePath,
                            tableBucket,
                            partitionName,
                            fileIds,
                            planReference,
                            rowPosSplitIndex,
                            numberOfSplits,
                            changeId,
                            remoteUploadBasePath,
                            splitIndex,
                            tieringRoundTimestamp);
        } else if (splitKind == TIERING_LOG_SPLIT_FLAG) {
            // deserialize starting offset
            long startingOffset = in.readLong();
            // deserialize starting offset
            long stoppingOffset = in.readLong();
            tieringSplit =
                    new TieringLogSplit(
                            tablePath,
                            tableBucket,
                            partitionName,
                            startingOffset,
                            stoppingOffset,
                            numberOfSplits,
                            skipCurrentRound,
                            splitIndex,
                            tieringRoundTimestamp);
        } else {
            throw new IOException("Unknown split kind " + splitKind);
        }
        if (tieringEpoch != TieringSplit.UNKNOWN_TIERING_EPOCH) {
            tieringSplit =
                    tieringSplit.copy(
                            numberOfSplits, splitIndex, tieringRoundTimestamp, tieringEpoch);
        }
        return new FlinkTieringSplit(tieringSplit);
    }
}
