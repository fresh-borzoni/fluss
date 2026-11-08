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

package org.apache.fluss.server.tablet;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.DeletionVectorsNotReadyException;
import org.apache.fluss.exception.InvalidRequestException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.GetDvSnapshotRequest;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.rpc.messages.PbPutKvRespForBucket;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.utils.RoaringBitmapUtils;
import org.apache.fluss.utils.ExceptionUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The union read requests of a table with deletion vectors that pin a cut, over the RPC. */
class TabletServiceDvPinnedCutITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(
                            Configuration.fromMap(
                                    Collections.singletonMap(
                                            ConfigOptions.DATALAKE_FORMAT.key(),
                                            DataLakeFormat.PAIMON.toString())))
                    .build();

    @Test
    void testWorkersFetchThePartsOfAPinnedCut() throws Exception {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA_PK)
                        .distributedBy(1, "a")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED, true)
                        .build();
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        TablePath.of("test_db_1", "dv_pinned_cut"),
                        descriptor);
        // the values of a table with deletion vectors are tagged with their version's offset
        assertThat(
                        FLUSS_CLUSTER_EXTENSION
                                .getZooKeeperClient()
                                .getTable(TablePath.of("test_db_1", "dv_pinned_cut"))
                                .get()
                                .properties)
                .containsEntry(
                        ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION.key(),
                        String.valueOf(KvValueLayout.TAGGED.version()));
        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);
        TabletServerGateway leader =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(
                        FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb));

        // keys 0..29 written three times: two versions of each are superseded
        for (int round = 0; round < 3; round++) {
            put(leader, tableId, "v" + round);
        }
        // a pin with the whole log mask in one page
        GetDvSnapshotResponse all = getDv(leader, request(tableId).setLogDvFromOffset(0));
        assertThat(all.getLogDvToOffset()).isEqualTo(all.getLogEndOffset());
        List<Long> pinned = offsets(all.getLogDvBitmap());
        assertThat(pinned).hasSize(60);

        // the planner pins the cut
        GetDvSnapshotResponse pin = getDv(leader, request(tableId));
        assertThat(pin.getLogEndOffset()).isEqualTo(all.getLogEndOffset());
        assertThat(pin.hasLogDvBitmap()).isFalse();
        long cut = pin.getLogEndOffset();

        // writes go on, and the bucket's own masks with them
        put(leader, tableId, "v3");
        GetDvSnapshotResponse now = getDv(leader, request(tableId).setLogDvFromOffset(0));
        long deadline = System.currentTimeMillis() + 60_000;
        // masks are served again for a moment once built
        while (now.getLogEndOffset() == cut && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
            now = getDv(leader, request(tableId).setLogDvFromOffset(0));
        }
        assertThat(now.getLogEndOffset()).isGreaterThan(cut);
        assertThat(offsets(now.getLogDvBitmap()))
                .as("masks up to %s, pinned up to %s", now.getLogEndOffset(), cut)
                .hasSize(90);

        // workers fetch the log mask of the cut in pages
        List<Long> pages = new ArrayList<>();
        long from = 0;
        while (from < cut) {
            GetDvSnapshotResponse page =
                    getDv(
                            leader,
                            request(tableId)
                                    .setLogEndOffset(cut)
                                    .setLogDvFromOffset(from)
                                    .setMaxLogDvBytes(1));
            assertThat(page.getLogEndOffset()).isEqualTo(cut);
            assertThat(page.getLogDvToOffset()).isGreaterThan(from);
            pages.addAll(offsets(page.hasLogDvBitmap() ? page.getLogDvBitmap() : null));
            from = page.getLogDvToOffset();
        }
        assertThat(pages).isEqualTo(pinned);
        // a cut no request pinned is stale
        assertThatThrownBy(() -> getDv(leader, request(tableId).setLogEndOffset(1)))
                .hasMessageContaining("Stale snapshot");
    }

    /**
     * A read that does not wait pins a cut and fetches its parts as one that waits does. A fetch of
     * a cut the server no longer holds fails as stale, as for a read that waits: the leader still
     * covers the snapshot, so it is not answered as covering another one.
     */
    @Test
    void testReadThatDoesNotWaitGetsAStaleErrorForALostCut() throws Exception {
        long tableId = createDvTable("dv_no_wait_lost_cut");
        TabletServerGateway leader = leaderOf(tableId);
        put(leader, tableId, "v0");
        put(leader, tableId, "v1");
        // a read that waits until the bucket replayed its log, and ends
        long waited = getDv(leader, request(tableId)).getLogEndOffset();
        leader.getDvSnapshot(request(tableId).setLogEndOffset(waited).setReleaseCut(true)).get();

        GetDvSnapshotResponse pin =
                getDv(leader, request(tableId).setWaitForCatchUp(false).setHolds(2));
        assertThat(pin.hasFoldCut()).isFalse();
        assertThat(pin.hasCoveredSnapshotId()).isFalse();
        long cut = pin.getLogEndOffset();
        GetDvSnapshotResponse part =
                getDv(
                        leader,
                        request(tableId)
                                .setWaitForCatchUp(false)
                                .setLogEndOffset(cut)
                                .setLogDvFromOffset(0));
        assertThat(part.getLogEndOffset()).isEqualTo(cut);
        assertThat(offsets(part.getLogDvBitmap())).hasSize(30);

        // the read ends, and writes move the high watermark past the cut
        leader.getDvSnapshot(request(tableId).setLogEndOffset(cut).setReleaseCut(true).setHolds(2))
                .get();
        put(leader, tableId, "v2");
        assertThatThrownBy(
                        () ->
                                leader.getDvSnapshot(
                                                request(tableId)
                                                        .setWaitForCatchUp(false)
                                                        .setLogEndOffset(cut)
                                                        .setLogDvFromOffset(0))
                                        .get())
                .satisfies(
                        e -> {
                            StaleSnapshotException stale =
                                    ExceptionUtils.findThrowable(e, StaleSnapshotException.class)
                                            .orElseThrow(() -> new AssertionError(e));
                            assertThat(stale.getRequestedSnapshotId()).isEqualTo(-1L);
                        })
                .hasMessageContaining("cut " + cut);
    }

    /**
     * Requests the server cannot serve as asked are refused as invalid, and requests of a table
     * without deletion vectors as of an invalid table, never as an unknown server error.
     */
    @Test
    void testInvalidRequestsAreRefusedAsSuch() throws Exception {
        long tableId = createDvTable("dv_invalid_requests");
        TabletServerGateway leader = leaderOf(tableId);

        assertRefused(leader, request(tableId).setHolds(0), InvalidRequestException.class);
        assertRefused(leader, request(tableId).setHolds(4097), InvalidRequestException.class);
        assertRefused(
                leader,
                request(tableId).setReleaseCut(true).setHolds(-1),
                InvalidRequestException.class);
        assertRefused(leader, request(tableId).setReleaseCut(true), InvalidRequestException.class);
        GetDvSnapshotRequest lookup = request(tableId);
        for (int i = 0; i <= 100_000; i++) {
            lookup.addLookupKey(new byte[] {(byte) i, (byte) (i >> 8), (byte) (i >> 16)});
        }
        assertRefused(leader, lookup, InvalidRequestException.class);

        TablePath plainPath = TablePath.of("test_db_1", "dv_plain_table");
        long plainId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        plainPath,
                        TableDescriptor.builder()
                                .schema(DATA1_SCHEMA_PK)
                                .distributedBy(1, "a")
                                .build());
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(new TableBucket(plainId, 0));
        assertRefused(leaderOf(plainId), request(plainId), InvalidTableException.class);
    }

    private static void assertRefused(
            TabletServerGateway gateway,
            GetDvSnapshotRequest request,
            Class<? extends Throwable> error) {
        assertThatThrownBy(() -> gateway.getDvSnapshot(request).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(error);
    }

    private static long createDvTable(String name) throws Exception {
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        TablePath.of("test_db_1", name),
                        TableDescriptor.builder()
                                .schema(DATA1_SCHEMA_PK)
                                .distributedBy(1, "a")
                                .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                                .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED, true)
                                .build());
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(new TableBucket(tableId, 0));
        return tableId;
    }

    private static TabletServerGateway leaderOf(long tableId) {
        return FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(
                FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(new TableBucket(tableId, 0)));
    }

    private static GetDvSnapshotRequest request(long tableId) {
        return new GetDvSnapshotRequest()
                .setTableId(tableId)
                .setBucketId(0)
                .setReadableSnapshotId(-1L);
    }

    /** Sends the request until the bucket has replayed its log for union reads. */
    private static GetDvSnapshotResponse getDv(
            TabletServerGateway gateway, GetDvSnapshotRequest request) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        while (true) {
            try {
                return gateway.getDvSnapshot(request).get();
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

    private static void put(TabletServerGateway gateway, long tableId, String value)
            throws Exception {
        // keys encoded for the Paimon lake, as clients do and the deletion vectors expect
        KeyEncoder keyEncoder =
                KeyEncoder.of(
                        DATA1_ROW_TYPE, Collections.singletonList("a"), DataLakeFormat.PAIMON);
        KvRecordTestUtils.KvRecordFactory records =
                KvRecordTestUtils.KvRecordFactory.of(DATA1_ROW_TYPE);
        List<KvRecord> batch = new ArrayList<>();
        for (int k = 0; k < 30; k++) {
            Object[] row = new Object[] {k, value};
            batch.add(records.ofRecord(keyEncoder.encodeKey(row(row)), row));
        }
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
        PbPutKvRespForBucket bucket = response.getBucketsRespsList().get(0);
        assertThat(bucket.hasErrorCode()).isFalse();
    }

    private static List<Long> offsets(@Nullable byte[] bytes) throws Exception {
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        if (bytes != null) {
            RoaringBitmapUtils.deserializeRoaringBitmap64(bitmap, bytes);
        }
        List<Long> out = new ArrayList<>();
        for (LongIterator it = bitmap.getLongIterator(); it.hasNext(); ) {
            out.add(it.next());
        }
        return out;
    }
}
