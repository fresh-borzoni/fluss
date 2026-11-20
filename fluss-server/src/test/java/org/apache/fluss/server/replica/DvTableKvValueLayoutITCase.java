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

package org.apache.fluss.server.replica;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.DeletionVectorsNotReadyException;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.GetDvSnapshotRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.utils.RoaringBitmapUtils;
import org.apache.fluss.utils.ExceptionUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.server.testutils.KvTestUtils.assertLookupResponse;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newLookupRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A table with deletion vectors takes the versioned KV value layout, with row TTL or without. A
 * server that knows only the tagged layout writes a time into the tag, where deletion vectors read
 * the log offset of a value's version; it does not know the versioned layout and refuses the table
 * instead. Without row TTL the leaders keep each value's version offset through writes, lookups, a
 * KV snapshot and a leader change, and no value expires.
 */
class DvTableKvValueLayoutITCase {

    private static final String DB = "test_db_dv_layout";

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(clusterConf())
                    .build();

    private static Configuration clusterConf() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.DATALAKE_FORMAT.key(), DataLakeFormat.PAIMON.toString());
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // KV snapshots are taken by the test only
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1));
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(5));
        return conf;
    }

    /**
     * The coordinator gives a table with deletion vectors the versioned layout, with row TTL or
     * without, whatever version the client asked for. Row TTL alone keeps the tagged layout.
     */
    @Test
    void testCreateTakesTheVersionedLayoutForDeletionVectors() throws Exception {
        assertThat(createdLayout("dv", dvTable())).isEqualTo(KvValueLayout.VERSIONED.version());
        assertThat(
                        createdLayout(
                                "dv_row_ttl",
                                dvTable().property(ConfigOptions.TABLE_KV_TTL, Duration.ofDays(1))))
                .isEqualTo(KvValueLayout.VERSIONED.version());
        assertThat(
                        createdLayout(
                                "dv_asks_tagged",
                                dvTable()
                                        .property(
                                                ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION,
                                                KvValueLayout.TAGGED.version())))
                .isEqualTo(KvValueLayout.VERSIONED.version());
        assertThat(
                        createdLayout(
                                "row_ttl",
                                pkTable().property(ConfigOptions.TABLE_KV_TTL, Duration.ofDays(1))))
                .isEqualTo(KvValueLayout.TAGGED.version());
        assertThat(createdLayout("plain", pkTable())).isEqualTo(KvValueLayout.PLAIN.version());
    }

    /**
     * A table with deletion vectors and without row TTL: each value carries the log offset of its
     * version and the TTL timestamp that never expires. A new leader restores them from the KV
     * snapshot and the log after it, and a change on it names the version it supersedes from the
     * restored value, so the masks hide exactly the superseded versions.
     */
    @Test
    void testVersionOffsetsSurviveALeaderChangeWithoutRowTtl() throws Exception {
        TablePath path = TablePath.of(DB, "dv_failover");
        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, path, dvTable().build());
        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);
        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway gateway = FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        // k1, k2, k3 inserted at 0, 1, 2; k1 updated (-U at 3, +U at 4); k2 deleted at 5
        put(gateway, tableId, upsert(1, "a"), upsert(2, "b"), upsert(3, "c"));
        put(gateway, tableId, upsert(1, "a2"), delete(2));
        Replica replica = leaderReplica(tb);
        KvTablet kv = replica.getKvTablet();
        assertThat(kv.getKvValueLayout()).isSameAs(KvValueLayout.VERSIONED);
        assertThat(kv.getDvManager().rowTtlFilterClock()).isNull();
        assertVersions(replica, 4L, null, 2L, null);
        assertThat(logMask(gateway, tableId)).containsExactly(0L, 1L);
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tb);

        // only in the log after the snapshot: k3 updated (-U at 6, +U at 7), k4 inserted at 8
        put(gateway, tableId, upsert(3, "c2"), upsert(4, "d"));
        assertVersions(replica, 4L, null, 7L, 8L);
        assertThat(logMask(gateway, tableId)).containsExactly(0L, 1L, 2L);

        FLUSS_CLUSTER_EXTENSION.stopTabletServer(leader);
        try {
            waitUntil(
                    () -> FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb) != leader,
                    Duration.ofMinutes(2),
                    "No new leader after stopping tablet server " + leader);
            int newLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
            TabletServerGateway newGateway =
                    FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(newLeader);
            Replica newReplica = leaderReplica(tb);
            assertThat(newReplica.getLeaderId()).isEqualTo(newLeader);
            KvTablet restored = newReplica.getKvTablet();
            assertThat(restored).isNotSameAs(kv);
            assertThat(restored.getKvValueLayout()).isSameAs(KvValueLayout.VERSIONED);
            assertVersions(newReplica, 4L, null, 7L, 8L);
            assertThat(logMask(newGateway, tableId)).containsExactly(0L, 1L, 2L);

            // k1 updated (-U at 9, +U at 10): it supersedes its version 4 from the KV snapshot
            put(newGateway, tableId, upsert(1, "a3"));
            assertVersions(newReplica, 10L, null, 7L, 8L);
            assertThat(logMask(newGateway, tableId)).containsExactly(0L, 1L, 2L, 4L);
            assertLookups(newGateway, tableId, "a3", null, "c2", "d");

            // without row TTL a full compaction removes nothing
            restored.getRocksDBKv().getDb().compactRange();
            assertVersions(newReplica, 10L, null, 7L, 8L);
            assertLookups(newGateway, tableId, "a3", null, "c2", "d");
        } finally {
            FLUSS_CLUSTER_EXTENSION.startTabletServer(leader);
        }
    }

    /**
     * Each key's value in RocksDB, once the leader flushed its writes, carries the expected version
     * offset, or the key has no value, and the TTL timestamp that never expires.
     */
    private static void assertVersions(Replica replica, @Nullable Long... versionOffsets)
            throws Exception {
        KvTablet kv = replica.getKvTablet();
        long logEnd = replica.getLocalLogEndOffset();
        waitUntil(
                () -> kv.getFlushedLogOffset() >= logEnd,
                Duration.ofMinutes(1),
                "The KV did not flush up to " + logEnd);
        for (int k = 1; k <= versionOffsets.length; k++) {
            byte[] raw = kv.getRocksDBKv().get(key(k));
            Long expected = versionOffsets[k - 1];
            if (expected == null) {
                assertThat(raw).as("k%s", k).isNull();
                assertThat(kv.versionOffset(key(k))).isNull();
                continue;
            }
            assertThat(raw).as("k%s", k).isNotNull();
            MemorySegment value = MemorySegment.wrap(raw);
            assertThat(KvValueLayout.VERSIONED.readVersionOffset(value, 0))
                    .as("k%s", k)
                    .isEqualTo(expected);
            assertThat(KvValueLayout.VERSIONED.readTtlTimestamp(value, 0))
                    .as("k%s", k)
                    .isEqualTo(KvValueLayout.NEVER_EXPIRE_TIMESTAMP);
            assertThat(kv.versionOffset(key(k))).isEqualTo(expected);
        }
    }

    /** A lookup of each key returns its value, or nothing. */
    private static void assertLookups(
            TabletServerGateway gateway, long tableId, @Nullable String... values)
            throws Exception {
        for (int k = 1; k <= values.length; k++) {
            assertLookupResponse(
                    gateway.lookup(newLookupRequest(tableId, 0, key(k))).get(),
                    values[k - 1] == null
                            ? null
                            : ValueEncoder.encodeValue(
                                    DEFAULT_SCHEMA_ID,
                                    compactedRow(DATA1_ROW_TYPE, new Object[] {k, values[k - 1]})));
        }
    }

    /** The log mask of a union read of the empty lake, once the leader serves masks. */
    private static List<Long> logMask(TabletServerGateway gateway, long tableId) throws Exception {
        GetDvSnapshotRequest request =
                new GetDvSnapshotRequest()
                        .setTableId(tableId)
                        .setBucketId(0)
                        .setReadableSnapshotId(-1L)
                        .setLogDvFromOffset(0);
        long deadline = System.currentTimeMillis() + 60_000;
        while (true) {
            try {
                byte[] bitmap = gateway.getDvSnapshot(request).get().getLogDvBitmap();
                Roaring64Bitmap offsets = new Roaring64Bitmap();
                if (bitmap != null) {
                    RoaringBitmapUtils.deserializeRoaringBitmap64(offsets, bitmap);
                }
                List<Long> out = new ArrayList<>();
                offsets.forEach(out::add);
                return out;
            } catch (ExecutionException e) {
                if (!ExceptionUtils.findThrowable(e, DeletionVectorsNotReadyException.class)
                                .isPresent()
                        || System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(50);
            }
        }
    }

    private static Replica leaderReplica(TableBucket tb) {
        Replica[] replica = new Replica[1];
        waitUntil(
                () -> {
                    replica[0] = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tb);
                    return replica[0].getKvTablet() != null;
                },
                Duration.ofMinutes(1),
                "The leader of " + tb + " has no KV");
        return replica[0];
    }

    private static int createdLayout(String name, TableDescriptor.Builder table) throws Exception {
        TablePath path = TablePath.of(DB, name);
        createTable(FLUSS_CLUSTER_EXTENSION, path, table.build());
        return Integer.parseInt(
                FLUSS_CLUSTER_EXTENSION
                        .getZooKeeperClient()
                        .getTable(path)
                        .get()
                        .properties
                        .get(ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION.key()));
    }

    private static TableDescriptor.Builder pkTable() {
        return TableDescriptor.builder().schema(DATA1_SCHEMA_PK).distributedBy(1, "a");
    }

    private static TableDescriptor.Builder dvTable() {
        return pkTable()
                .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                .property(ConfigOptions.TABLE_DATALAKE_DELETION_VECTORS_ENABLED, true);
    }

    // keys encoded for the Paimon lake, as clients do and the deletion vectors expect
    private static byte[] key(int k) {
        return KeyEncoder.of(DATA1_ROW_TYPE, Collections.singletonList("a"), DataLakeFormat.PAIMON)
                .encodeKey(row(k, ""));
    }

    private static KvRecord upsert(int k, String value) {
        return KvRecordTestUtils.KvRecordFactory.of(DATA1_ROW_TYPE)
                .ofRecord(key(k), new Object[] {k, value});
    }

    private static KvRecord delete(int k) {
        return KvRecordTestUtils.KvRecordFactory.of(DATA1_ROW_TYPE).ofRecord(key(k), null);
    }

    private static void put(TabletServerGateway gateway, long tableId, KvRecord... records)
            throws Exception {
        List<KvRecord> batch = new ArrayList<>();
        Collections.addAll(batch, records);
        PutKvResponse response =
                gateway.putKv(
                                newPutKvRequest(
                                        tableId,
                                        0,
                                        -1,
                                        KvRecordTestUtils.KvRecordBatchFactory.of(DEFAULT_SCHEMA_ID)
                                                .ofRecords(batch)))
                        .get();
        assertThat(response.getBucketsRespsCount()).isEqualTo(1);
        assertThat(response.getBucketsRespsList().get(0).hasErrorCode()).isFalse();
    }
}
