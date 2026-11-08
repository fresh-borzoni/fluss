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

import org.apache.fluss.client.tiering.TableBucketWriteResult;
import org.apache.fluss.client.tiering.TieringSplit;
import org.apache.fluss.flink.tiering.TestingWriteResult;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.RowPosResult;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link TableBucketWriteResultSerializer}. */
class TableBucketWriteResultSerializerTest {

    private static final TableBucketWriteResultSerializer<TestingWriteResult>
            tableBucketWriteResultSerializer =
                    new TableBucketWriteResultSerializer<>(new TestingWriteResultSerializer());

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSerializeAndDeserialize(boolean isPartitioned) throws Exception {
        // verify when writeResult is not null
        TestingWriteResult testingWriteResult = new TestingWriteResult(2);
        TablePath tablePath = TablePath.of("db1", "tb1");
        TableBucket tableBucket =
                isPartitioned ? new TableBucket(1, 1000L, 2) : new TableBucket(1, 2);
        String partitionName = isPartitioned ? "partition1" : null;
        TableBucketWriteResult<TestingWriteResult> tableBucketWriteResult =
                new TableBucketWriteResult<>(
                        tablePath, tableBucket, partitionName, testingWriteResult, 10, 30L, 20);

        // test serialize and deserialize
        byte[] serialized = tableBucketWriteResultSerializer.serialize(tableBucketWriteResult);
        TableBucketWriteResult<TestingWriteResult> deserialized =
                tableBucketWriteResultSerializer.deserialize(
                        tableBucketWriteResultSerializer.getVersion(), serialized);

        assertThat(deserialized.tablePath()).isEqualTo(tablePath);
        assertThat(deserialized.tableBucket()).isEqualTo(tableBucket);
        assertThat(deserialized.partitionName()).isEqualTo(partitionName);
        TestingWriteResult deserializedWriteResult = deserialized.writeResult();
        assertThat(deserializedWriteResult).isNotNull();
        assertThat(deserializedWriteResult.getWriteResult())
                .isEqualTo(testingWriteResult.getWriteResult());
        assertThat(deserialized.numberOfWriteResults()).isEqualTo(20);

        // verify when writeResult is null
        tableBucketWriteResult =
                new TableBucketWriteResult<>(
                        tablePath, tableBucket, partitionName, null, 20, 30L, 30);
        serialized = tableBucketWriteResultSerializer.serialize(tableBucketWriteResult);
        deserialized =
                tableBucketWriteResultSerializer.deserialize(
                        tableBucketWriteResultSerializer.getVersion(), serialized);
        assertThat(deserialized.tablePath()).isEqualTo(tablePath);
        assertThat(deserialized.tableBucket()).isEqualTo(tableBucket);
        assertThat(deserialized.partitionName()).isEqualTo(partitionName);
        assertThat(deserialized.writeResult()).isNull();
        assertThat(deserialized.numberOfWriteResults()).isEqualTo(30);
    }

    @Test
    void testLakeWriteResultSerializerVersionIsPersisted() throws Exception {
        VersionRecordingWriteResultSerializer lakeSerializer =
                new VersionRecordingWriteResultSerializer(7);
        TableBucketWriteResultSerializer<TestingWriteResult> serializer =
                new TableBucketWriteResultSerializer<>(lakeSerializer);

        TableBucketWriteResult<TestingWriteResult> deserialized =
                serializer.deserialize(
                        serializer.getVersion(),
                        serializer.serialize(
                                new TableBucketWriteResult<>(
                                        TablePath.of("db1", "tb1"),
                                        new TableBucket(1, 2),
                                        null,
                                        new TestingWriteResult(5),
                                        10,
                                        30L,
                                        1)));

        assertThat(lakeSerializer.deserializedVersions).containsExactly(7);
        assertThat(deserialized.writeResult().getWriteResult()).isEqualTo(5);
        assertThat(deserialized.logEndOffset()).isEqualTo(10);
        assertThat(deserialized.maxTimestamp()).isEqualTo(30L);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDeserializeVersionOne(boolean withWriteResult) throws Exception {
        VersionRecordingWriteResultSerializer lakeSerializer =
                new VersionRecordingWriteResultSerializer(7);
        TableBucketWriteResultSerializer<TestingWriteResult> serializer =
                new TableBucketWriteResultSerializer<>(lakeSerializer);

        // the layout of VERSION_1, which doesn't persist the lake write result serializer version
        DataOutputSerializer out = new DataOutputSerializer(64);
        out.writeUTF("db1");
        out.writeUTF("tb1");
        out.writeLong(1L);
        out.writeBoolean(true);
        out.writeLong(1000L);
        out.writeUTF("partition1");
        out.writeInt(2);
        if (withWriteResult) {
            byte[] writeResultBytes = lakeSerializer.serialize(new TestingWriteResult(5));
            out.writeInt(writeResultBytes.length);
            out.write(writeResultBytes);
        } else {
            out.writeInt(-1);
        }
        out.writeLong(10L);
        out.writeLong(30L);
        out.writeInt(3);

        TableBucketWriteResult<TestingWriteResult> deserialized =
                serializer.deserialize(1, out.getCopyOfBuffer());

        assertThat(deserialized.tablePath()).isEqualTo(TablePath.of("db1", "tb1"));
        assertThat(deserialized.tableBucket()).isEqualTo(new TableBucket(1L, 1000L, 2));
        assertThat(deserialized.partitionName()).isEqualTo("partition1");
        assertThat(deserialized.logEndOffset()).isEqualTo(10L);
        assertThat(deserialized.maxTimestamp()).isEqualTo(30L);
        assertThat(deserialized.numberOfWriteResults()).isEqualTo(3);
        if (withWriteResult) {
            // VERSION_1 passed its own version to the lake serializer
            assertThat(lakeSerializer.deserializedVersions).containsExactly(1);
            assertThat(deserialized.writeResult().getWriteResult()).isEqualTo(5);
        } else {
            assertThat(lakeSerializer.deserializedVersions).isEmpty();
            assertThat(deserialized.writeResult()).isNull();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testEndedEarlyRoundTrips(boolean endedEarly) throws Exception {
        TableBucketWriteResult<TestingWriteResult> result =
                new TableBucketWriteResult<>(
                        TablePath.of("db1", "tb1"),
                        new TableBucket(1, 2),
                        null,
                        new TestingWriteResult(3),
                        7,
                        30L,
                        2,
                        null,
                        endedEarly);
        TableBucketWriteResult<TestingWriteResult> deserialized =
                tableBucketWriteResultSerializer.deserialize(
                        tableBucketWriteResultSerializer.getVersion(),
                        tableBucketWriteResultSerializer.serialize(result));
        assertThat(deserialized.endedEarly()).isEqualTo(endedEarly);
        assertThat(deserialized.logEndOffset()).isEqualTo(7);
    }

    @ParameterizedTest
    @ValueSource(longs = {TieringSplit.UNKNOWN_TIERING_EPOCH, 0, 42})
    void testTieringEpochRoundTrips(long tieringEpoch) throws Exception {
        TableBucketWriteResult<TestingWriteResult> result =
                new TableBucketWriteResult<>(
                                TablePath.of("db1", "tb1"),
                                new TableBucket(1, 2),
                                null,
                                new TestingWriteResult(3),
                                7,
                                30L,
                                2)
                        .withTieringEpoch(tieringEpoch);
        TableBucketWriteResult<TestingWriteResult> deserialized =
                tableBucketWriteResultSerializer.deserialize(
                        tableBucketWriteResultSerializer.getVersion(),
                        tableBucketWriteResultSerializer.serialize(result));
        assertThat(deserialized.tieringEpoch()).isEqualTo(tieringEpoch);
        assertThat(deserialized.logEndOffset()).isEqualTo(7);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 4})
    void testDeserializeUnknownVersion(int version) {
        assertThatThrownBy(() -> tableBucketWriteResultSerializer.deserialize(version, new byte[0]))
                .isInstanceOf(IOException.class)
                .hasMessage("Unknown version " + version);
    }

    @Test
    void testDeserializeVersionTwo() throws Exception {
        VersionRecordingWriteResultSerializer lakeSerializer =
                new VersionRecordingWriteResultSerializer(7);
        TableBucketWriteResultSerializer<TestingWriteResult> serializer =
                new TableBucketWriteResultSerializer<>(lakeSerializer);

        // the layout of VERSION_2, which doesn't carry the RowPosResult
        DataOutputSerializer out = new DataOutputSerializer(64);
        out.writeUTF("db1");
        out.writeUTF("tb1");
        out.writeLong(1L);
        out.writeBoolean(false);
        out.writeInt(2);
        byte[] writeResultBytes = lakeSerializer.serialize(new TestingWriteResult(5));
        out.writeInt(writeResultBytes.length);
        out.writeInt(7);
        out.write(writeResultBytes);
        out.writeLong(10L);
        out.writeLong(30L);
        out.writeInt(3);

        TableBucketWriteResult<TestingWriteResult> deserialized =
                serializer.deserialize(2, out.getCopyOfBuffer());

        assertThat(deserialized.tableBucket()).isEqualTo(new TableBucket(1L, 2));
        assertThat(lakeSerializer.deserializedVersions).containsExactly(7);
        assertThat(deserialized.writeResult().getWriteResult()).isEqualTo(5);
        assertThat(deserialized.logEndOffset()).isEqualTo(10L);
        assertThat(deserialized.maxTimestamp()).isEqualTo(30L);
        assertThat(deserialized.numberOfWriteResults()).isEqualTo(3);
        assertThat(deserialized.isRowPosResult()).isFalse();
    }

    @Test
    void testDeserializeVersionThree() throws Exception {
        VersionRecordingWriteResultSerializer lakeSerializer =
                new VersionRecordingWriteResultSerializer(7);
        TableBucketWriteResultSerializer<TestingWriteResult> serializer =
                new TableBucketWriteResultSerializer<>(lakeSerializer);

        // the layout of VERSION_3: VERSION_2, then the fields in the order they were added: the
        // tiering epoch, endedEarly and the RowPosResult
        DataOutputSerializer out = new DataOutputSerializer(64);
        out.writeUTF("db1");
        out.writeUTF("tb1");
        out.writeLong(1L);
        out.writeBoolean(true);
        out.writeLong(1000L);
        out.writeUTF("partition1");
        out.writeInt(2);
        byte[] writeResultBytes = lakeSerializer.serialize(new TestingWriteResult(5));
        out.writeInt(writeResultBytes.length);
        out.writeInt(7);
        out.write(writeResultBytes);
        out.writeLong(10L);
        out.writeLong(30L);
        out.writeInt(3);
        // tiering epoch and endedEarly
        out.writeLong(42L);
        out.writeBoolean(true);
        // RowPosResult
        out.writeBoolean(true);
        out.writeLong(77L);
        out.writeInt(4);
        out.writeInt(1);
        out.writeInt(10);
        out.writeUTF("file1.parquet");
        out.writeInt(1);
        out.writeUTF("sst_0.sst");
        out.writeLong(12345L);
        // the split did not fail
        out.writeBoolean(false);

        TableBucketWriteResult<TestingWriteResult> deserialized =
                serializer.deserialize(3, out.getCopyOfBuffer());

        assertThat(deserialized.tableBucket()).isEqualTo(new TableBucket(1L, 1000L, 2));
        assertThat(deserialized.partitionName()).isEqualTo("partition1");
        assertThat(lakeSerializer.deserializedVersions).containsExactly(7);
        assertThat(deserialized.writeResult().getWriteResult()).isEqualTo(5);
        assertThat(deserialized.logEndOffset()).isEqualTo(10L);
        assertThat(deserialized.maxTimestamp()).isEqualTo(30L);
        assertThat(deserialized.numberOfWriteResults()).isEqualTo(3);
        RowPosResult rowPosResult = deserialized.getRowPosResult();
        assertThat(rowPosResult).isNotNull();
        assertThat(rowPosResult.getChangeId()).isEqualTo(77L);
        assertThat(rowPosResult.getSplit()).isEqualTo(4);
        assertThat(rowPosResult.getNewFileDictEntries())
                .hasSize(1)
                .containsEntry(10, "file1.parquet");
        assertThat(rowPosResult.getSstMetas()).hasSize(1);
        assertThat(rowPosResult.getSstMetas().get(0).getFileName()).isEqualTo("sst_0.sst");
        assertThat(rowPosResult.getSstMetas().get(0).getFileSize()).isEqualTo(12345L);
        assertThat(deserialized.endedEarly()).isTrue();
        assertThat(deserialized.tieringEpoch()).isEqualTo(42L);

        // and the current version writes exactly that layout
        assertThat(serializer.getVersion()).isEqualTo(3);
        assertThat(serializer.serialize(deserialized)).isEqualTo(out.getCopyOfBuffer());
    }

    /** The failure of a re-index split round-trips. */
    @Test
    void testFailedRowPosResultRoundTrips() throws Exception {
        TableBucketWriteResult<TestingWriteResult> result =
                new TableBucketWriteResult<>(
                        new TablePath("db1", "tb1"),
                        new TableBucket(1L, 2),
                        null,
                        null,
                        -1,
                        -1,
                        3,
                        RowPosResult.failed(77L, 4, "two live rows of one key"));

        TableBucketWriteResult<TestingWriteResult> deserialized =
                tableBucketWriteResultSerializer.deserialize(
                        tableBucketWriteResultSerializer.getVersion(),
                        tableBucketWriteResultSerializer.serialize(result));

        assertThat(deserialized.getRowPosResult().isFailed()).isTrue();
        assertThat(deserialized.getRowPosResult().getFailure())
                .isEqualTo("two live rows of one key");
        assertThat(deserialized.getRowPosResult().getChangeId()).isEqualTo(77L);
    }

    @Test
    void testDeserializeVersionTwoHasNoRoundFields() throws Exception {
        // VERSION_2 predates endedEarly and the tiering epoch: the round is not fenced
        DataOutputSerializer out = new DataOutputSerializer(64);
        out.writeUTF("db1");
        out.writeUTF("tb1");
        out.writeLong(1L);
        out.writeBoolean(false);
        out.writeInt(2);
        out.writeInt(-1);
        out.writeLong(10L);
        out.writeLong(30L);
        out.writeInt(3);

        TableBucketWriteResult<TestingWriteResult> deserialized =
                tableBucketWriteResultSerializer.deserialize(2, out.getCopyOfBuffer());
        assertThat(deserialized.writeResult()).isNull();
        assertThat(deserialized.endedEarly()).isFalse();
        assertThat(deserialized.tieringEpoch()).isEqualTo(TieringSplit.UNKNOWN_TIERING_EPOCH);
    }

    @Test
    void testSerializeAndDeserializeWithRowPosResult() throws Exception {
        TablePath tablePath = TablePath.of("db1", "tb1");
        TableBucket tableBucket = new TableBucket(1, 100L, 2);

        // RowPosResult with multiple SST files
        Map<Integer, String> fileDictEntries = new HashMap<>();
        fileDictEntries.put(10, "file1.parquet");
        fileDictEntries.put(11, "file2.parquet");
        RowPosResult rowPosResult =
                new RowPosResult(
                        77L,
                        5,
                        fileDictEntries,
                        Arrays.asList(
                                new RowPosResult.SstMeta("sst_0.sst", 12345),
                                new RowPosResult.SstMeta("sst_1.sst", 67890)));

        TableBucketWriteResult<TestingWriteResult> tableBucketWriteResult =
                new TableBucketWriteResult<>(
                        tablePath, tableBucket, "partition1", null, -1, -1, 3, rowPosResult);

        byte[] serialized = tableBucketWriteResultSerializer.serialize(tableBucketWriteResult);
        TableBucketWriteResult<TestingWriteResult> deserialized =
                tableBucketWriteResultSerializer.deserialize(
                        tableBucketWriteResultSerializer.getVersion(), serialized);

        assertThat(deserialized.isRowPosResult()).isTrue();
        RowPosResult deserializedRowPos = deserialized.getRowPosResult();
        assertThat(deserializedRowPos).isNotNull();
        assertThat(deserializedRowPos.getChangeId()).isEqualTo(77L);
        assertThat(deserializedRowPos.getSplit()).isEqualTo(5);
        assertThat(deserializedRowPos.getNewFileDictEntries()).hasSize(2);
        assertThat(deserializedRowPos.getNewFileDictEntries().get(10)).isEqualTo("file1.parquet");
        assertThat(deserializedRowPos.getNewFileDictEntries().get(11)).isEqualTo("file2.parquet");
        assertThat(deserializedRowPos.getSstMetas()).hasSize(2);
        assertThat(deserializedRowPos.getSstMetas().get(0).getFileName()).isEqualTo("sst_0.sst");
        assertThat(deserializedRowPos.getSstMetas().get(0).getFileSize()).isEqualTo(12345);
        assertThat(deserializedRowPos.getSstMetas().get(1).getFileName()).isEqualTo("sst_1.sst");
        assertThat(deserializedRowPos.getSstMetas().get(1).getFileSize()).isEqualTo(67890);
    }

    @Test
    void testSerializeAndDeserializeWithSingleSstRowPosResult() throws Exception {
        TablePath tablePath = TablePath.of("db1", "tb1");
        TableBucket tableBucket = new TableBucket(1, 2);

        // single-SST RowPosResult (non-partitioned)
        Map<Integer, String> fileDictEntries = new HashMap<>();
        fileDictEntries.put(5, "output.parquet");
        RowPosResult rowPosResult =
                new RowPosResult(
                        3L,
                        0,
                        fileDictEntries,
                        Collections.singletonList(new RowPosResult.SstMeta("sst_0.sst", 1000)));

        TableBucketWriteResult<TestingWriteResult> tableBucketWriteResult =
                new TableBucketWriteResult<>(
                        tablePath, tableBucket, null, null, -1, -1, 1, rowPosResult);

        byte[] serialized = tableBucketWriteResultSerializer.serialize(tableBucketWriteResult);
        TableBucketWriteResult<TestingWriteResult> deserialized =
                tableBucketWriteResultSerializer.deserialize(
                        tableBucketWriteResultSerializer.getVersion(), serialized);

        assertThat(deserialized.isRowPosResult()).isTrue();
        RowPosResult deserializedRowPos = deserialized.getRowPosResult();
        assertThat(deserializedRowPos).isNotNull();
        assertThat(deserializedRowPos.getChangeId()).isEqualTo(3L);
        assertThat(deserializedRowPos.getNewFileDictEntries()).hasSize(1);
        assertThat(deserializedRowPos.getNewFileDictEntries().get(5)).isEqualTo("output.parquet");
        assertThat(deserializedRowPos.getSstMetas()).hasSize(1);
        assertThat(deserializedRowPos.getSstMetas().get(0).getFileName()).isEqualTo("sst_0.sst");
        assertThat(deserializedRowPos.getSstMetas().get(0).getFileSize()).isEqualTo(1000);
    }

    /** A lake write result serializer which records the versions passed for deserialization. */
    private static class VersionRecordingWriteResultSerializer
            implements SimpleVersionedSerializer<TestingWriteResult> {

        private final int version;
        private final List<Integer> deserializedVersions = new ArrayList<>();

        private VersionRecordingWriteResultSerializer(int version) {
            this.version = version;
        }

        @Override
        public int getVersion() {
            return version;
        }

        @Override
        public byte[] serialize(TestingWriteResult writeResult) throws IOException {
            DataOutputSerializer out = new DataOutputSerializer(4);
            out.writeInt(writeResult.getWriteResult());
            return out.getCopyOfBuffer();
        }

        @Override
        public TestingWriteResult deserialize(int version, byte[] serialized) throws IOException {
            deserializedVersions.add(version);
            return new TestingWriteResult(new DataInputDeserializer(serialized).readInt());
        }
    }
}
