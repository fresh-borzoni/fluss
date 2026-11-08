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

package org.apache.fluss.flink.lake;

import org.apache.fluss.client.table.scanner.batch.dv.DvBucketMaskState;
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketPart;
import org.apache.fluss.flink.lake.split.DvBucketSplit;
import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.split.SourceSplitSerializer;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TableBucket;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.flink.lake.split.DvBucketSplit.DV_BUCKET_SPLIT_KIND;
import static org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit.LAKE_SNAPSHOT_FLUSS_LOG_SPLIT_KIND;
import static org.apache.fluss.flink.lake.split.LakeSnapshotSplit.LAKE_SNAPSHOT_SPLIT_KIND;

/**
 * A serializer for lake split.
 *
 * <p>The layout of the lake split section is versioned by the enclosing {@link
 * SourceSplitSerializer}, see its version evolution for details.
 */
public class LakeSplitSerializer {

    private final SimpleVersionedSerializer<LakeSplit> sourceSplitSerializer;

    public LakeSplitSerializer(SimpleVersionedSerializer<LakeSplit> sourceSplitSerializer) {
        this.sourceSplitSerializer = sourceSplitSerializer;
    }

    public void serialize(DataOutputSerializer out, SourceSplitBase split) throws IOException {
        out.writeInt(sourceSplitSerializer.getVersion());
        if (split instanceof LakeSnapshotSplit) {
            LakeSnapshotSplit lakeSplit = (LakeSnapshotSplit) split;
            out.writeInt(lakeSplit.getSplitIndex());
            byte[] serializeBytes = sourceSplitSerializer.serialize(lakeSplit.getLakeSplit());
            out.writeInt(serializeBytes.length);
            out.write(serializeBytes);
            out.writeLong(lakeSplit.getRecordsToSkip());
        } else if (split instanceof LakeSnapshotAndFlussLogSplit) {
            // writing file store source split
            LakeSnapshotAndFlussLogSplit lakeSnapshotAndFlussLogSplit =
                    ((LakeSnapshotAndFlussLogSplit) split);
            List<LakeSplit> lakeSplits = lakeSnapshotAndFlussLogSplit.getLakeSplits();
            if (lakeSplits == null) {
                // no snapshot data for the bucket
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeInt(lakeSplits.size());
                for (LakeSplit lakeSplit : lakeSplits) {
                    byte[] serializeBytes = sourceSplitSerializer.serialize(lakeSplit);
                    out.writeInt(serializeBytes.length);
                    out.write(serializeBytes);
                }
            }
            // writing starting/stopping offset
            out.writeLong(lakeSnapshotAndFlussLogSplit.getStartingOffset());
            out.writeLong(
                    lakeSnapshotAndFlussLogSplit
                            .getStoppingOffset()
                            .orElse(LogSplit.NO_STOPPING_OFFSET));
            out.writeLong(lakeSnapshotAndFlussLogSplit.getRecordsToSkip());
            out.writeInt(lakeSnapshotAndFlussLogSplit.getCurrentLakeSplitIndex());
            out.writeBoolean(lakeSnapshotAndFlussLogSplit.isLakeSplitFinished());
        } else if (split instanceof DvBucketSplit) {
            // since VERSION_3 of the enclosing SourceSplitSerializer
            DvBucketSplit dvSplit = (DvBucketSplit) split;
            out.writeLong(dvSplit.getSnapshotId());
            out.writeLong(dvSplit.getSnapshotLogOffset());
            out.writeLong(dvSplit.getStoppingOffset());
            writeLakeSplits(out, dvSplit.getLakeSplits());
            out.writeByte(dvSplit.getReadMode().code());
            out.writeLong(dvSplit.getMaskLogEndOffset());
            out.writeInt(dvSplit.getCurrentLakeSplitIndex());
            out.writeLong(dvSplit.getRecordsToSkip());
            out.writeLong(dvSplit.getNextLogOffset());
            writeMaskState(out, dvSplit.getMaskState());
            out.writeBoolean(dvSplit.isPinned());
            out.writeInt(dvSplit.getFileGroup());
            out.writeBoolean(dvSplit.readsLog());
            out.writeLong(dvSplit.getPinnedLogStartOffset());
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported split type: " + split.getClass().getName());
        }
    }

    /**
     * Deserializes a lake split.
     *
     * @param splitKind the kind of the split
     * @param tableBucket the table bucket of the split
     * @param partition the partition name of the split, null for non-partitioned table
     * @param input the input to read the lake split section from
     * @param readRecordsToSkip whether the records to skip of a {@link LakeSnapshotSplit} are
     *     persisted, which depends on the version of the enclosing {@link SourceSplitSerializer}.
     *     If not, the split is read from the beginning.
     */
    public SourceSplitBase deserialize(
            byte splitKind,
            TableBucket tableBucket,
            @Nullable String partition,
            DataInputDeserializer input,
            boolean readRecordsToSkip)
            throws IOException {
        int version = input.readInt();
        if (splitKind == LAKE_SNAPSHOT_SPLIT_KIND) {
            int splitIndex = input.readInt();
            byte[] serializeBytes = new byte[input.readInt()];
            input.readFully(serializeBytes);
            LakeSplit lakeSplit = sourceSplitSerializer.deserialize(version, serializeBytes);
            long recordsToSkip = readRecordsToSkip ? input.readLong() : 0;
            return new LakeSnapshotSplit(
                    tableBucket, partition, lakeSplit, splitIndex, recordsToSkip);
        } else if (splitKind == LAKE_SNAPSHOT_FLUSS_LOG_SPLIT_KIND) {
            List<LakeSplit> lakeSplits = null;
            if (input.readBoolean()) {
                int lakeSplitSize = input.readInt();
                lakeSplits = new ArrayList<>(lakeSplitSize);
                for (int i = 0; i < lakeSplitSize; i++) {
                    byte[] serializeBytes = new byte[input.readInt()];
                    input.readFully(serializeBytes);
                    lakeSplits.add(sourceSplitSerializer.deserialize(version, serializeBytes));
                }
            }
            long startingOffset = input.readLong();
            long stoppingOffset = input.readLong();
            long recordsToSkip = input.readLong();
            int splitIndex = input.readInt();
            boolean isLakeSplitFinished = input.readBoolean();
            return new LakeSnapshotAndFlussLogSplit(
                    tableBucket,
                    partition,
                    lakeSplits,
                    startingOffset,
                    stoppingOffset,
                    recordsToSkip,
                    splitIndex,
                    isLakeSplitFinished);
        } else if (splitKind == DV_BUCKET_SPLIT_KIND) {
            long snapshotId = input.readLong();
            long snapshotLogOffset = input.readLong();
            long stoppingOffset = input.readLong();
            List<LakeSplit> lakeSplits = readLakeSplits(input, version);
            DvBucketPart.ReadMode readMode = DvBucketPart.ReadMode.of(input.readByte());
            long maskLogEndOffset = input.readLong();
            int currentLakeSplitIndex = input.readInt();
            long recordsToSkip = input.readLong();
            long nextLogOffset = input.readLong();
            DvBucketMaskState maskState = readMaskState(input);
            boolean pinned = input.readBoolean();
            int fileGroup = input.readInt();
            boolean readsLog = input.readBoolean();
            long pinnedLogStartOffset = input.readLong();
            return new DvBucketSplit(
                    tableBucket,
                    partition,
                    snapshotId,
                    snapshotLogOffset,
                    stoppingOffset,
                    lakeSplits,
                    readMode,
                    maskLogEndOffset,
                    currentLakeSplitIndex,
                    recordsToSkip,
                    nextLogOffset,
                    maskState,
                    pinned,
                    fileGroup,
                    readsLog,
                    pinnedLogStartOffset);
        } else {
            throw new UnsupportedOperationException("Unsupported split kind: " + splitKind);
        }
    }

    private void writeLakeSplits(DataOutputSerializer out, @Nullable List<LakeSplit> lakeSplits)
            throws IOException {
        if (lakeSplits == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        out.writeInt(lakeSplits.size());
        for (LakeSplit lakeSplit : lakeSplits) {
            byte[] serializeBytes = sourceSplitSerializer.serialize(lakeSplit);
            out.writeInt(serializeBytes.length);
            out.write(serializeBytes);
        }
    }

    @Nullable
    private List<LakeSplit> readLakeSplits(DataInputDeserializer input, int version)
            throws IOException {
        if (!input.readBoolean()) {
            return null;
        }
        int size = input.readInt();
        List<LakeSplit> lakeSplits = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            byte[] serializeBytes = new byte[input.readInt()];
            input.readFully(serializeBytes);
            lakeSplits.add(sourceSplitSerializer.deserialize(version, serializeBytes));
        }
        return lakeSplits;
    }

    private static void writeMaskState(
            DataOutputSerializer out, @Nullable DvBucketMaskState maskState) throws IOException {
        if (maskState == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        out.writeLong(maskState.getLogStartOffset());
        out.writeLong(maskState.getLogEndOffset());
        String identity = maskState.getLakeSnapshotIdentity();
        out.writeBoolean(identity != null);
        if (identity != null) {
            out.writeUTF(identity);
        }
        writeBytes(out, maskState.getLogMask());
        out.writeInt(maskState.getLakeMasks().size());
        for (Map.Entry<String, byte[]> e : maskState.getLakeMasks().entrySet()) {
            out.writeUTF(e.getKey());
            writeBytes(out, e.getValue());
        }
    }

    @Nullable
    private static DvBucketMaskState readMaskState(DataInputDeserializer input) throws IOException {
        if (!input.readBoolean()) {
            return null;
        }
        long logStartOffset = input.readLong();
        long logEndOffset = input.readLong();
        String identity = input.readBoolean() ? input.readUTF() : null;
        byte[] logMask = readBytes(input);
        int size = input.readInt();
        Map<String, byte[]> lakeMasks = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            lakeMasks.put(input.readUTF(), readBytes(input));
        }
        return new DvBucketMaskState(logStartOffset, logEndOffset, identity, lakeMasks, logMask);
    }

    private static void writeBytes(DataOutputSerializer out, @Nullable byte[] bytes)
            throws IOException {
        if (bytes == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(bytes.length);
            out.write(bytes);
        }
    }

    @Nullable
    private static byte[] readBytes(DataInputDeserializer input) throws IOException {
        int length = input.readInt();
        if (length < 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return bytes;
    }
}
