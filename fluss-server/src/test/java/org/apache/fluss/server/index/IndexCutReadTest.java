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

package org.apache.fluss.server.index;

import org.apache.fluss.exception.CorruptRecordException;
import org.apache.fluss.exception.LookupPositionExpiredException;
import org.apache.fluss.exception.LookupPositionNotYetReadableException;
import org.apache.fluss.metadata.BucketReadPosition;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.server.entity.PutIndexDataForBucket;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaTestBase;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ByteArraySlice;
import org.apache.fluss.utils.IndexTableUtils;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_LEADER_EPOCH;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.apache.fluss.utils.IndexTableUtils.DATA_RECORD_KIND;
import static org.apache.fluss.utils.IndexTableUtils.PROGRESS_RECORD_KIND;
import static org.apache.fluss.utils.IndexTableUtils.REMOVED_RECORD_KIND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests how an Index Table bucket applies windows with removed pointers, purges them below the
 * history floor of their source bucket, and serves reads at a cut of the main table.
 */
class IndexCutReadTest extends ReplicaTestBase {

    private static final byte[] ROUTING_KEY = {7};
    private static final String VALUE = "x";

    private TableBucket indexBucket;
    private Replica replica;
    private RowType rowType;
    private RowType keyType;
    private IndexTableRows rows;

    @Test
    void readsLiveAndStillCurrentRemovedPointersAtACut() throws Exception {
        createIndexTable("cut_read", 800101L);
        put(
                0,
                10,
                -1,
                data(1),
                removed(0, 2, 5),
                removed(0, 7, 3),
                // a pointer added after a removed one of the same row
                data(7));
        put(1, 4, -1, removed(1, 3, 2));
        awaitApplied(0, 10);
        awaitApplied(1, 4);

        assertThat(read(cut(0, 5L, 1, 2L))).containsExactlyInAnyOrder(1, 2, 3, 7);
        assertThat(read(cut(0, 6L, 1, 2L))).containsExactlyInAnyOrder(1, 3, 7);
        assertThat(read(cut(0, 6L, 1, 3L))).containsExactlyInAnyOrder(1, 7);
        // the removed pointers of buckets outside the cut are left out
        assertThat(read(cut(0, 5L))).containsExactlyInAnyOrder(1, 2, 7);
        // latest reads see the data pointers alone
        assertThat(decode(replica.prefixLookup(dataPrefix()))).containsExactlyInAnyOrder(1, 7);
        // the candidate limit covers both kinds of pointers
        assertThat(replica.indexLookupAt(prefixes(), cut(0, 5L, 1, 2L), 3, false).get(0))
                .hasSize(3);
        assertThatThrownBy(() -> replica.indexLookupAt(prefixes(), cut(0, 5L, 1, 2L), 3, true))
                .hasMessageContaining("candidate limit of 3");
    }

    @Test
    void waitsUntilEverySourceBucketIsAppliedUpToTheCut() throws Exception {
        createIndexTable("cut_gate", 800102L);
        put(0, 10, -1, data(1));
        awaitApplied(0, 10);

        assertThatThrownBy(() -> read(cut(0, 11L)))
                .isInstanceOfSatisfying(
                        LookupPositionNotYetReadableException.class,
                        e ->
                                assertThat(e.getLatestPosition())
                                        .isEqualTo(
                                                BucketReadPosition.of(
                                                        new TableBucket(DATA1_TABLE_ID_PK, 0),
                                                        10)));
        // nothing of bucket 2 was applied: only its empty prefix is readable
        assertThat(read(cut(0, 10L, 2, 0L))).containsExactly(1);
        assertThatThrownBy(() -> read(cut(0, 10L, 2, 1L)))
                .isInstanceOf(LookupPositionNotYetReadableException.class);

        put(2, 3, -1, data(2));
        awaitApplied(2, 3);
        assertThat(read(cut(0, 10L, 2, 1L))).containsExactlyInAnyOrder(1, 2);
    }

    @Test
    void purgesRemovedPointersBelowTheHistoryFloorOfTheirSourceBucket() throws Exception {
        createIndexTable("cut_purge", 800103L);
        put(0, 10, -1, removed(0, 2, 3), removed(0, 4, 7), removed(0, 5, 2));
        put(1, 4, -1, removed(1, 3, 1));
        awaitApplied(0, 10);
        awaitApplied(1, 4);
        assertThat(removedPointers()).containsExactlyInAnyOrder(2, 4, 5, 3);

        put(0, 12, 5, data(9));
        awaitApplied(0, 12);
        awaitRemovedPointers(4, 3);
        assertThat(read(cut(0, 5L, 1, 1L))).containsExactlyInAnyOrder(9, 4, 3);
        assertThatThrownBy(() -> read(cut(0, 4L, 1, 1L)))
                .isInstanceOfSatisfying(
                        LookupPositionExpiredException.class,
                        e ->
                                assertThat(e.getEarliestPosition())
                                        .isEqualTo(
                                                BucketReadPosition.of(
                                                        new TableBucket(DATA1_TABLE_ID_PK, 0), 5)));

        // a lower floor does not lower the purge floor
        put(0, 14, 3, data(10));
        awaitApplied(0, 14);
        assertThatThrownBy(() -> read(cut(0, 4L, 1, 1L)))
                .isInstanceOf(LookupPositionExpiredException.class);

        // the same window removes pointer 4 again: its stored removal is below the floor, but the
        // latest one, still unflushed while the window purges, is not
        put(0, 17, 10, removed(0, 4, 16), removed(0, 6, 15));
        awaitApplied(0, 17);
        awaitRemovedPointers(4, 6, 3);
        assertThat(read(cut(0, 16L, 1, 1L))).containsExactlyInAnyOrder(9, 10, 4, 3);
        assertThat(read(cut(0, 17L, 1, 2L))).containsExactlyInAnyOrder(9, 10);
    }

    @Test
    void rejectsRemovedPointersWhoseValueIsOfAnotherKind() throws Exception {
        createIndexTable("cut_validate", 800104L);
        byte[] rowKey = IndexTableUtils.removedRowKey(0, new byte[] {1});
        Tuple2<Object[], Object[]> mismatch =
                Tuple2.of(
                        new Object[] {REMOVED_RECORD_KIND, ROUTING_KEY, rowKey},
                        new Object[] {VALUE, 1, DATA_RECORD_KIND, ROUTING_KEY, rowKey, null});
        assertThatThrownBy(() -> put(0, 10, -1, mismatch))
                .isInstanceOf(CorruptRecordException.class);
    }

    // ------------------------------------------------------------------------------------------

    private void createIndexTable(String name, long tableId) throws Exception {
        TablePath path = TablePath.of("test_db_1", IndexTableUtils.indexTableName(name, "idx_b"));
        Schema schema =
                Schema.newBuilder()
                        .fromColumns(DATA1_SCHEMA_PK.getColumns())
                        .primaryKey("a")
                        .index("idx_b", IndexType.SECONDARY, Collections.singletonList("b"))
                        .build();
        TableDescriptor descriptor =
                IndexTableDescriptorFactory.derive(
                        DATA1_TABLE_DESCRIPTOR_PK.withReplicationFactor(1),
                        DATA1_TABLE_ID_PK,
                        schema.getIndexes().get(0));
        zkClient.registerTable(
                path, TableRegistration.newTable(tableId, DEFAULT_REMOTE_DATA_DIR, descriptor));
        zkClient.registerFirstSchema(path, descriptor.getSchema());
        indexBucket = new TableBucket(tableId, 0);
        makeKvTableAsLeader(indexBucket, path, INITIAL_LEADER_EPOCH, false);
        replica = replicaManager.getReplicaOrException(indexBucket);
        rowType = replica.getTableInfo().getRowType();
        keyType = rowType.project(descriptor.getSchema().getPrimaryKeyIndexes());
        rows = new IndexTableRows(replica.getTableInfo(), replica.getSchemaGetter());
    }

    private static Tuple2<Object[], Object[]> data(int a) {
        byte[] rowKey = {(byte) a};
        return Tuple2.of(
                new Object[] {DATA_RECORD_KIND, ROUTING_KEY, rowKey},
                new Object[] {VALUE, a, DATA_RECORD_KIND, ROUTING_KEY, rowKey, null});
    }

    private static Tuple2<Object[], Object[]> removed(int sourceBucket, int a, long removedAt) {
        byte[] rowKey = IndexTableUtils.removedRowKey(sourceBucket, new byte[] {(byte) a});
        return Tuple2.of(
                new Object[] {REMOVED_RECORD_KIND, ROUTING_KEY, rowKey},
                new Object[] {VALUE, a, REMOVED_RECORD_KIND, ROUTING_KEY, rowKey, removedAt});
    }

    /** Applies a window of a source bucket, ending in its progress, as the replicator sends it. */
    @SafeVarargs
    private final void put(
            int sourceBucket, long end, long historyFloor, Tuple2<Object[], Object[]>... records)
            throws Exception {
        byte[] sourceKey = IndexTableUtils.sourceBucketRowKey(sourceBucket);
        Object[] progressKey = new Object[] {PROGRESS_RECORD_KIND, new byte[0], sourceKey};
        List<Tuple2<Object[], Object[]>> batch = new ArrayList<>(Arrays.asList(records));
        batch.add(
                Tuple2.of(
                        progressKey,
                        new Object[] {
                            null, null, PROGRESS_RECORD_KIND, new byte[0], sourceKey, end
                        }));
        KvRecordBatch kvRecords = genKvRecordBatch(keyType, rowType, batch);
        CompletableFuture<List<PutKvResultForBucket>> result = new CompletableFuture<>();
        replicaManager.putIndexRecords(
                300000,
                -1,
                Collections.singletonList(
                        new PutIndexDataForBucket(
                                indexBucket,
                                new TableBucket(DATA1_TABLE_ID_PK, sourceBucket),
                                end,
                                new CompactedKeyEncoder(keyType).encodeKey(row(progressKey)),
                                kvRecords,
                                historyFloor)),
                result::complete);
        PutKvResultForBucket put = result.get(10, TimeUnit.SECONDS).get(0);
        if (put.failed()) {
            throw put.getError().exception();
        }
    }

    /** Waits until the flushed state of the bucket shows the window of a source bucket. */
    private void awaitApplied(int sourceBucket, long end) {
        waitUntil(
                () -> {
                    try {
                        read(cut(sourceBucket, end));
                        return true;
                    } catch (LookupPositionNotYetReadableException e) {
                        return false;
                    }
                },
                Duration.ofSeconds(30),
                "The index bucket did not apply the window.");
    }

    /** The main rows of the pointers of {@link #VALUE} at a cut. */
    private Set<Integer> read(Map<TableBucket, Long> cut) {
        List<Integer> result = decode(replica.indexLookupAt(prefixes(), cut, 0, false).get(0));
        Set<Integer> distinct = new HashSet<>(result);
        assertThat(distinct).hasSameSizeAs(result);
        return distinct;
    }

    /** Waits until the flushed state of the bucket keeps exactly these removed pointers. */
    private void awaitRemovedPointers(Integer... mainKeys) {
        Set<Integer> expected = new HashSet<>(Arrays.asList(mainKeys));
        waitUntil(
                () -> removedPointers().equals(expected),
                Duration.ofSeconds(30),
                "The index bucket did not purge its removed pointers.");
    }

    private Set<Integer> removedPointers() {
        return new HashSet<>(
                decode(replica.prefixLookup(IndexTableRows.removedPrefix(dataPrefix()))));
    }

    private List<Integer> decode(List<ByteArraySlice> values) {
        List<Integer> mainKeys = new ArrayList<>();
        for (ByteArraySlice value : values) {
            InternalRow row = rows.decode(value);
            mainKeys.add(row.getInt(1));
        }
        return mainKeys;
    }

    private static List<byte[]> prefixes() {
        return Collections.singletonList(dataPrefix());
    }

    private static byte[] dataPrefix() {
        return new CompactedKeyEncoder(RowType.of(DataTypes.TINYINT(), DataTypes.BYTES()))
                .encodeKey(row(DATA_RECORD_KIND, ROUTING_KEY));
    }

    private static Map<TableBucket, Long> cut(Object... bucketsAndPositions) {
        Map<TableBucket, Long> cut = new HashMap<>();
        for (int i = 0; i < bucketsAndPositions.length; i += 2) {
            cut.put(
                    new TableBucket(DATA1_TABLE_ID_PK, (Integer) bucketsAndPositions[i]),
                    (Long) bucketsAndPositions[i + 1]);
        }
        return cut;
    }
}
