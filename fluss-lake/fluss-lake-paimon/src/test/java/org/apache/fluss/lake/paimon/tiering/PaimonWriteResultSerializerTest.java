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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link PaimonWriteResultSerializer}. */
class PaimonWriteResultSerializerTest {

    private static final List<CommitMessage> MESSAGES =
            Collections.singletonList(
                    new CommitMessageImpl(
                            BinaryRow.EMPTY_ROW,
                            3,
                            4,
                            DataIncrement.emptyIncrement(),
                            new CompactIncrement(
                                    Collections.emptyList(),
                                    Collections.emptyList(),
                                    Collections.emptyList())));

    private final PaimonWriteResultSerializer serializer = new PaimonWriteResultSerializer();

    @Test
    void testDeserializeVersionOne() throws Exception {
        // the layout of version 1: the commit messages only
        DataOutputSerializer out = new DataOutputSerializer(64);
        new CommitMessageSerializer().serializeList(MESSAGES, out);

        PaimonWriteResult result = serializer.deserialize(1, out.getCopyOfBuffer());

        assertThat(result.commitMessages()).isEqualTo(MESSAGES);
        assertThat(result.compacted()).isFalse();
        assertThat(result.compactionIndexPart()).isNull();
    }

    @Test
    void testSerializeAndDeserializeCompactionIndexPart() throws Exception {
        CompactionIndexPart part =
                new CompactionIndexPart(
                        7L,
                        3L,
                        Arrays.asList("sst_0.sst", "sst_1.sst"),
                        Arrays.asList(10L, 20L),
                        Collections.singletonMap(4, "data-4.parquet"),
                        Collections.singletonList("data-1.parquet"));
        PaimonWriteResult result =
                serializer.deserialize(
                        serializer.getVersion(),
                        serializer.serialize(new PaimonWriteResult(MESSAGES, true, part)));

        assertThat(serializer.getVersion()).isEqualTo(2);
        assertThat(result.commitMessages()).isEqualTo(MESSAGES);
        assertThat(result.compacted()).isTrue();
        CompactionIndexPart restored = result.compactionIndexPart();
        assertThat(restored).isNotNull();
        assertThat(restored.changeId()).isEqualTo(7L);
        assertThat(restored.readSnapshotId()).isEqualTo(3L);
        assertThat(restored.sstFileNames()).containsExactly("sst_0.sst", "sst_1.sst");
        assertThat(restored.sstFileSizes()).containsExactly(10L, 20L);
        assertThat(restored.addedFiles()).containsOnlyKeys(4).containsEntry(4, "data-4.parquet");
        assertThat(restored.removedFiles()).containsExactly("data-1.parquet");
    }

    @Test
    void testDeserializeUnknownVersion() {
        assertThatThrownBy(() -> serializer.deserialize(3, new byte[0]))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
