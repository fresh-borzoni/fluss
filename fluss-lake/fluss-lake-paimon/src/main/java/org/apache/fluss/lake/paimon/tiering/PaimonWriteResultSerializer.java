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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.lake.dv.index.CompactionIndexPart;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;

import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The {@link SimpleVersionedSerializer} for {@link PaimonWriteResult}. */
public class PaimonWriteResultSerializer implements SimpleVersionedSerializer<PaimonWriteResult> {

    // Version 2 appends whether the writer compacted and the index part of its compaction.
    private static final int VERSION_1 = 1;
    private static final int CURRENT_VERSION = 2;

    private final CommitMessageSerializer messageSer = new CommitMessageSerializer();

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(PaimonWriteResult paimonWriteResult) throws IOException {
        DataOutputSerializer output = new DataOutputSerializer(64);
        messageSer.serializeList(paimonWriteResult.commitMessages(), output);
        output.writeBoolean(paimonWriteResult.compacted());
        CompactionIndexPart part = paimonWriteResult.compactionIndexPart();
        output.writeBoolean(part != null);
        if (part != null) {
            output.writeLong(part.changeId());
            output.writeLong(part.readSnapshotId());
            output.writeInt(part.sstFileNames().size());
            for (int i = 0; i < part.sstFileNames().size(); i++) {
                output.writeUTF(part.sstFileNames().get(i));
                output.writeLong(part.sstFileSizes().get(i));
            }
            output.writeInt(part.addedFiles().size());
            for (Map.Entry<Integer, String> e : part.addedFiles().entrySet()) {
                output.writeInt(e.getKey());
                output.writeUTF(e.getValue());
            }
            output.writeInt(part.removedFiles().size());
            for (String file : part.removedFiles()) {
                output.writeUTF(file);
            }
        }
        return output.getCopyOfBuffer();
    }

    @Override
    public PaimonWriteResult deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION_1 && version != CURRENT_VERSION) {
            throw new UnsupportedOperationException(
                    "Expecting PaimonWriteResult version to be "
                            + CURRENT_VERSION
                            + ", but found "
                            + version
                            + ".");
        }
        try (DataInputViewStreamWrapper input =
                new DataInputViewStreamWrapper(new ByteArrayInputStream(serialized))) {
            List<CommitMessage> commitMessages =
                    messageSer.deserializeList(messageSer.getVersion(), input);
            if (version == VERSION_1) {
                return new PaimonWriteResult(commitMessages);
            }
            boolean compacted = input.readBoolean();
            CompactionIndexPart part = null;
            if (input.readBoolean()) {
                long changeId = input.readLong();
                long readSnapshotId = input.readLong();
                int ssts = input.readInt();
                List<String> sstNames = new ArrayList<>(ssts);
                List<Long> sstSizes = new ArrayList<>(ssts);
                for (int i = 0; i < ssts; i++) {
                    sstNames.add(input.readUTF());
                    sstSizes.add(input.readLong());
                }
                int added = input.readInt();
                Map<Integer, String> addedFiles = new LinkedHashMap<>();
                for (int i = 0; i < added; i++) {
                    addedFiles.put(input.readInt(), input.readUTF());
                }
                int removed = input.readInt();
                List<String> removedFiles = new ArrayList<>(removed);
                for (int i = 0; i < removed; i++) {
                    removedFiles.add(input.readUTF());
                }
                part =
                        new CompactionIndexPart(
                                changeId,
                                readSnapshotId,
                                sstNames,
                                sstSizes,
                                addedFiles,
                                removedFiles);
            }
            return new PaimonWriteResult(commitMessages, compacted, part);
        }
    }
}
