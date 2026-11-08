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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for serialization and deserialization of {@link TieringSnapshotSplit} and {@link
 * TieringLogSplit}.
 */
class TieringSplitSerializerTest {

    private static final TieringSplitSerializer serializer = TieringSplitSerializer.INSTANCE;
    private static final TableBucket tableBucket = new TableBucket(1, 2);
    private static final TablePath tablePath = TablePath.of("test_db", "test_table");
    private static final TableBucket partitionedTableBucket = new TableBucket(1, 100L, 2);
    private static final TablePath partitionedTablePath =
            TablePath.of("test_db", "test_partitioned_table");

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTieringSnapshotSplitSerde(Boolean isPartitionedTable) throws Exception {
        TableBucket bucket = isPartitionedTable ? partitionedTableBucket : tableBucket;
        TablePath path = isPartitionedTable ? partitionedTablePath : tablePath;
        String partitionName = isPartitionedTable ? "1024" : null;
        TieringSnapshotSplit tieringSplit =
                new TieringSnapshotSplit(path, bucket, partitionName, 0L, 200L, 10);

        byte[] serialized = serializer.serialize(new FlinkTieringSplit(tieringSplit));
        FlinkTieringSplit deserializedFlinkSplit =
                serializer.deserialize(serializer.getVersion(), serialized);
        TieringSnapshotSplit deserializedSplit =
                deserializedFlinkSplit.unwrap().asTieringSnapshotSplit();
        assertThat(deserializedSplit).isEqualTo(tieringSplit);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTieringSnapshotSplitStringExpression(Boolean isPartitionedTable) throws Exception {
        TableBucket bucket = isPartitionedTable ? partitionedTableBucket : tableBucket;
        TablePath path = isPartitionedTable ? partitionedTablePath : tablePath;
        String partitionName = isPartitionedTable ? "1024" : null;
        String expectedSplitId =
                isPartitionedTable
                        ? "tiering-snapshot-split-1-p100-2"
                        : "tiering-snapshot-split-1-2";
        assertThat(new TieringSnapshotSplit(path, bucket, partitionName, 0L, 200L, 20).splitId())
                .isEqualTo(expectedSplitId);

        String expectedSplitString =
                isPartitionedTable
                        ? "TieringSnapshotSplit{tablePath=test_db.test_partitioned_table, tableBucket=TableBucket{tableId=1, partitionId=100, bucket=2}, partitionName='1024', numberOfSplits=30, skipCurrentRound=false, snapshotId=0, logOffsetOfSnapshot=200, splitIndex=-1, tieringRoundTimestamp=-1}"
                        : "TieringSnapshotSplit{tablePath=test_db.test_table, tableBucket=TableBucket{tableId=1, bucket=2}, partitionName='null', numberOfSplits=30, skipCurrentRound=false, snapshotId=0, logOffsetOfSnapshot=200, splitIndex=-1, tieringRoundTimestamp=-1}";
        assertThat(new TieringSnapshotSplit(path, bucket, partitionName, 0L, 200L, 30).toString())
                .isEqualTo(expectedSplitString);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTieringLogSplitSerde(Boolean isPartitionedTable) throws Exception {
        TableBucket bucket = isPartitionedTable ? partitionedTableBucket : tableBucket;
        TablePath path = isPartitionedTable ? partitionedTablePath : tablePath;
        String partitionName = isPartitionedTable ? "1024" : null;
        TieringLogSplit tieringSplit =
                new TieringLogSplit(path, bucket, partitionName, 100, 200, 40);

        byte[] serialized = serializer.serialize(new FlinkTieringSplit(tieringSplit));
        FlinkTieringSplit deserializedFlinkSplit =
                serializer.deserialize(serializer.getVersion(), serialized);
        TieringLogSplit deserializedSplit = deserializedFlinkSplit.unwrap().asTieringLogSplit();
        assertThat(deserializedSplit).isEqualTo(tieringSplit);
    }

    @Test
    void testDeserializeUnknownSplitKindFailsFast() throws Exception {
        TieringLogSplit tieringSplit =
                new TieringLogSplit(tablePath, tableBucket, null, 100, 200, 40);
        byte[] serialized = serializer.serialize(new FlinkTieringSplit(tieringSplit));
        // the split kind is written as the first byte, tamper it with an unknown kind so that
        // deserialization must fail fast instead of silently falling back to a log split
        serialized[0] = 99;

        assertThatThrownBy(() -> serializer.deserialize(serializer.getVersion(), serialized))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unknown split kind 99");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTieringLogSplitStringExpression(Boolean isPartitionedTable) throws Exception {
        TableBucket bucket = isPartitionedTable ? partitionedTableBucket : tableBucket;
        TablePath path = isPartitionedTable ? partitionedTablePath : tablePath;
        String partitionName = isPartitionedTable ? "1024" : null;
        String expectedSplitId =
                isPartitionedTable ? "tiering-log-split-1-p100-2" : "tiering-log-split-1-2";
        assertThat(new TieringLogSplit(path, bucket, partitionName, 100, 200, 3).splitId())
                .isEqualTo(expectedSplitId);

        String expectedSplitString =
                isPartitionedTable
                        ? "TieringLogSplit{tablePath=test_db.test_partitioned_table, tableBucket=TableBucket{tableId=1, partitionId=100, bucket=2}, partitionName='1024', numberOfSplits=2, skipCurrentRound=false, startingOffset=100, stoppingOffset=200, splitIndex=-1, tieringRoundTimestamp=-1}"
                        : "TieringLogSplit{tablePath=test_db.test_table, tableBucket=TableBucket{tableId=1, bucket=2}, partitionName='null', numberOfSplits=2, skipCurrentRound=false, startingOffset=100, stoppingOffset=200, splitIndex=-1, tieringRoundTimestamp=-1}";
        assertThat(new TieringLogSplit(path, bucket, partitionName, 100, 200, 2).toString())
                .isEqualTo(expectedSplitString);
    }

    @Test
    void testSkipCurrentRoundSerde() throws Exception {
        // Test TieringSnapshotSplit with skipCurrentRound set at creation
        TieringSnapshotSplit snapshotSplitWithSkipCurrentRound =
                new TieringSnapshotSplit(tablePath, tableBucket, null, 0L, 200L, 10, true);
        byte[] serialized =
                serializer.serialize(new FlinkTieringSplit(snapshotSplitWithSkipCurrentRound));
        TieringSnapshotSplit deserializedSnapshotSplit =
                serializer
                        .deserialize(serializer.getVersion(), serialized)
                        .unwrap()
                        .asTieringSnapshotSplit();
        assertThat(deserializedSnapshotSplit).isEqualTo(snapshotSplitWithSkipCurrentRound);

        // Test TieringLogSplit with skipCurrentRound set at creation
        TieringLogSplit logSplitWithSkipCurrentRound =
                new TieringLogSplit(tablePath, tableBucket, null, 100, 200, 40, true);
        serialized = serializer.serialize(new FlinkTieringSplit(logSplitWithSkipCurrentRound));
        TieringLogSplit deserializedLogSplit =
                serializer
                        .deserialize(serializer.getVersion(), serialized)
                        .unwrap()
                        .asTieringLogSplit();
        assertThat(deserializedLogSplit).isEqualTo(logSplitWithSkipCurrentRound);

        // Test TieringSnapshotSplit with skipCurrentRound set after creation
        TieringSnapshotSplit snapshotSplit =
                new TieringSnapshotSplit(tablePath, tableBucket, null, 0L, 200L, 10, false);
        assertThat(snapshotSplit.shouldSkipCurrentRound()).isFalse();
        snapshotSplit.skipCurrentRound();
        assertThat(snapshotSplit.shouldSkipCurrentRound()).isTrue();

        serialized = serializer.serialize(new FlinkTieringSplit(snapshotSplit));
        deserializedSnapshotSplit =
                serializer
                        .deserialize(serializer.getVersion(), serialized)
                        .unwrap()
                        .asTieringSnapshotSplit();
        assertThat(deserializedSnapshotSplit).isEqualTo(snapshotSplit);

        // Test TieringLogSplit with skipCurrentRound set after creation
        TieringLogSplit logSplit =
                new TieringLogSplit(tablePath, tableBucket, null, 100, 200, 40, false);
        assertThat(logSplit.shouldSkipCurrentRound()).isFalse();
        logSplit.skipCurrentRound();
        assertThat(logSplit.shouldSkipCurrentRound()).isTrue();

        serialized = serializer.serialize(new FlinkTieringSplit(logSplit));
        deserializedLogSplit =
                serializer
                        .deserialize(serializer.getVersion(), serialized)
                        .unwrap()
                        .asTieringLogSplit();
        assertThat(deserializedLogSplit).isEqualTo(logSplit);
    }

    @Test
    void testTieringRoundTimestampSerde() throws Exception {
        TieringSnapshotSplit snapshotSplit =
                new TieringSnapshotSplit(tablePath, tableBucket, null, 0L, 200L, 10, 0, 1000L);
        byte[] serialized = serializer.serialize(new FlinkTieringSplit(snapshotSplit));
        TieringSnapshotSplit deserializedSnapshotSplit =
                serializer
                        .deserialize(serializer.getVersion(), serialized)
                        .unwrap()
                        .asTieringSnapshotSplit();
        assertThat(deserializedSnapshotSplit.getSplitIndex()).isZero();
        assertThat(deserializedSnapshotSplit.isFirstSplit()).isTrue();
        assertThat(deserializedSnapshotSplit.getTieringRoundTimestamp()).isEqualTo(1000L);

        TieringLogSplit logSplit =
                new TieringLogSplit(tablePath, tableBucket, null, 100, 200, 40, 2, 2000L);
        serialized = serializer.serialize(new FlinkTieringSplit(logSplit));
        TieringLogSplit deserializedLogSplit =
                serializer
                        .deserialize(serializer.getVersion(), serialized)
                        .unwrap()
                        .asTieringLogSplit();
        assertThat(deserializedLogSplit.getSplitIndex()).isEqualTo(2);
        assertThat(deserializedLogSplit.isFirstSplit()).isFalse();
        assertThat(deserializedLogSplit.getTieringRoundTimestamp()).isEqualTo(2000L);
    }

    @Test
    void testTieringEpochSerde() throws Exception {
        TieringSplit logSplit =
                new TieringLogSplit(tablePath, tableBucket, null, 100, 200, 40, 2, 2000L)
                        .copy(40, 2, 2000L, 7L);
        TieringSplit deserialized =
                serializer
                        .deserialize(
                                serializer.getVersion(),
                                serializer.serialize(new FlinkTieringSplit(logSplit)))
                        .unwrap();
        assertThat(deserialized).isEqualTo(logSplit);
        assertThat(deserialized.getTieringEpoch()).isEqualTo(7L);
        // a split handed back to the enumerator stays in its round
        assertThat(deserialized.copy(40).getTieringEpoch()).isEqualTo(7L);

        TieringSplit unknown = new TieringSnapshotSplit(tablePath, tableBucket, null, 0L, 2L, 1);
        assertThat(
                        serializer
                                .deserialize(
                                        serializer.getVersion(),
                                        serializer.serialize(new FlinkTieringSplit(unknown)))
                                .unwrap()
                                .getTieringEpoch())
                .isEqualTo(TieringSplit.UNKNOWN_TIERING_EPOCH);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRestoreVersion0Splits(boolean isPartitionedTable) throws Exception {
        TableBucket bucket = isPartitionedTable ? partitionedTableBucket : tableBucket;
        TablePath path = isPartitionedTable ? partitionedTablePath : tablePath;
        String partitionName = isPartitionedTable ? "1024" : null;

        TieringSplit snapshotSplit =
                new TieringSnapshotSplit(path, bucket, partitionName, 3L, 200L, 10, true, 1, 1000L);
        TieringSplit restoredSnapshotSplit =
                serializer
                        .deserialize(
                                0,
                                version0Bytes(
                                        TieringSplit.TIERING_SNAPSHOT_SPLIT_FLAG,
                                        path,
                                        bucket,
                                        partitionName,
                                        10,
                                        true,
                                        1,
                                        1000L,
                                        3L,
                                        200L))
                        .unwrap();
        assertThat(restoredSnapshotSplit).isEqualTo(snapshotSplit);
        assertThat(restoredSnapshotSplit.getTieringEpoch())
                .isEqualTo(TieringSplit.UNKNOWN_TIERING_EPOCH);

        TieringSplit logSplit =
                new TieringLogSplit(path, bucket, partitionName, 100L, 200L, 40, 2, 2000L);
        TieringSplit restoredLogSplit =
                serializer
                        .deserialize(
                                0,
                                version0Bytes(
                                        TieringSplit.TIERING_LOG_SPLIT_FLAG,
                                        path,
                                        bucket,
                                        partitionName,
                                        40,
                                        false,
                                        2,
                                        2000L,
                                        100L,
                                        200L))
                        .unwrap();
        assertThat(restoredLogSplit).isEqualTo(logSplit);
        assertThat(restoredLogSplit.getTieringEpoch())
                .isEqualTo(TieringSplit.UNKNOWN_TIERING_EPOCH);
    }

    @Test
    void testDeserializeUnknownVersion() throws Exception {
        byte[] serialized =
                serializer.serialize(
                        new FlinkTieringSplit(
                                new TieringLogSplit(tablePath, tableBucket, null, 0, 100, 3)));
        assertThatThrownBy(() -> serializer.deserialize(2, serialized))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unknown version 2");
    }

    /** The layout of {@link TieringSplitSerializer} version 0, as released. */
    private static byte[] version0Bytes(
            byte splitKind,
            TablePath path,
            TableBucket bucket,
            String partitionName,
            int numberOfSplits,
            boolean skipCurrentRound,
            int splitIndex,
            long tieringRoundTimestamp,
            long first,
            long second)
            throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(64);
        out.writeByte(splitKind);
        out.writeUTF(path.getDatabaseName());
        out.writeUTF(path.getTableName());
        out.writeLong(bucket.getTableId());
        out.writeInt(bucket.getBucket());
        if (bucket.getPartitionId() != null) {
            out.writeBoolean(true);
            out.writeLong(bucket.getPartitionId());
            out.writeUTF(partitionName);
        } else {
            out.writeBoolean(false);
        }
        out.writeInt(numberOfSplits);
        out.writeBoolean(skipCurrentRound);
        out.writeInt(splitIndex);
        out.writeLong(tieringRoundTimestamp);
        // snapshot id and log offset of snapshot, or starting and stopping offset
        out.writeLong(first);
        out.writeLong(second);
        return out.getCopyOfBuffer();
    }

    @Test
    void testFlinkTieringSplitAdapter() {
        TieringLogSplit logSplit = new TieringLogSplit(tablePath, tableBucket, null, 10, 200, 5);
        FlinkTieringSplit flinkSplit = new FlinkTieringSplit(logSplit);
        assertThat(flinkSplit.splitId()).isEqualTo(logSplit.splitId());
        assertThat(flinkSplit.unwrap()).isSameAs(logSplit);

        TieringSnapshotSplit snapshotSplit =
                new TieringSnapshotSplit(
                        tablePath, tableBucket, null, 1L, 100L, 3, false, 0, 5000L);
        FlinkTieringSplit flinkSnapshotSplit = new FlinkTieringSplit(snapshotSplit);
        assertThat(flinkSnapshotSplit.splitId()).isEqualTo(snapshotSplit.splitId());
        assertThat(flinkSnapshotSplit.unwrap()).isSameAs(snapshotSplit);
    }

    @Test
    void testFlinkTieringSplitEqualsHashCodeToString() {
        TieringLogSplit logSplit = new TieringLogSplit(tablePath, tableBucket, null, 0, 100, 3);
        FlinkTieringSplit split1 = new FlinkTieringSplit(logSplit);
        FlinkTieringSplit split2 = new FlinkTieringSplit(logSplit);

        assertThat(split1).isEqualTo(split2);
        assertThat(split1.hashCode()).isEqualTo(split2.hashCode());
        assertThat(split1).isNotEqualTo(null);
        assertThat(split1).isNotEqualTo("not a split");

        String str = split1.toString();
        assertThat(str).startsWith("FlinkTieringSplit{");
        assertThat(str).contains("TieringLogSplit");
    }

    @Test
    void testFlinkTieringSplitPartitioned() {
        FlinkTieringSplit flinkSplit =
                new FlinkTieringSplit(
                        new TieringLogSplit(
                                partitionedTablePath, partitionedTableBucket, "p1", 0, 50, 2));

        assertThat(flinkSplit.unwrap().getPartitionName()).isEqualTo("p1");
        assertThat(flinkSplit.unwrap().getTableBucket().getPartitionId()).isEqualTo(100L);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTieringRowPosSplitSerde(Boolean isPartitionedTable) throws Exception {
        TableBucket bucket = isPartitionedTable ? partitionedTableBucket : tableBucket;
        TablePath path = isPartitionedTable ? partitionedTablePath : tablePath;
        String partitionName = isPartitionedTable ? "1024" : null;

        int[] fileIds = {10, 11, 12};
        TieringRowPosSplit tieringSplit =
                new TieringRowPosSplit(
                        path,
                        bucket,
                        partitionName,
                        fileIds,
                        "/remote/lake/table/snapshot/rowPos/100/_plan/3",
                        3,
                        5,
                        100L,
                        "/remote/lake/table/snapshot");

        byte[] serialized = serializer.serialize(new FlinkTieringSplit(tieringSplit));
        TieringRowPosSplit deserializedSplit =
                serializer
                        .deserialize(serializer.getVersion(), serialized)
                        .unwrap()
                        .asTieringRowPosSplit();
        assertThat(deserializedSplit).isEqualTo(tieringSplit);
    }

    @Test
    void testTieringRowPosSplitSingleFileSerde() throws Exception {
        int[] fileIds = {42};
        TieringRowPosSplit tieringSplit =
                new TieringRowPosSplit(
                        tablePath,
                        tableBucket,
                        null,
                        fileIds,
                        "/remote/lake/table/snapshot2/rowPos/200/_plan/0",
                        0,
                        1,
                        200L,
                        "/remote/lake/table/snapshot2");

        byte[] serialized = serializer.serialize(new FlinkTieringSplit(tieringSplit));
        TieringRowPosSplit deserializedSplit =
                serializer
                        .deserialize(serializer.getVersion(), serialized)
                        .unwrap()
                        .asTieringRowPosSplit();
        assertThat(deserializedSplit).isEqualTo(tieringSplit);
    }
}
