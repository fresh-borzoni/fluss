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
import org.apache.fluss.exception.InvalidRequestException;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.kv.dv.DerivedDvManager;
import org.apache.fluss.server.kv.dv.DvMaskRequest;
import org.apache.fluss.server.utils.RoaringBitmapUtils;
import org.apache.fluss.utils.ExceptionUtils;

import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.server.kv.dv.DvPinnedCut.DEFAULT_LOG_PAGE_BYTES;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_LEADER_EPOCH;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Union reads of a bucket with deletion vectors that pin a cut and fetch its parts. */
class ReplicaManagerDvPinnedCutTest extends ReplicaTestBase {

    private static final TablePath DV_TABLE_PATH = TablePath.of("test_db_1", "dv_pinned_cut");
    private static final long DV_TABLE_ID = 150099L;
    private static final short PUT_KV_VERSION = 1;
    // short, so the tests see idle cuts expire, yet far longer than the requests of one read
    private static final Duration IDLE = Duration.ofSeconds(3);
    // the timeout and the sweep after it, with room for a slow machine
    private static final Duration EXPIRY_WAIT = IDLE.multipliedBy(5);

    @Override
    protected Configuration getServerConf() {
        Configuration conf = super.getServerConf();
        conf.set(ConfigOptions.KV_DELETION_VECTORS_PINNED_CUT_IDLE_TIMEOUT, IDLE);
        return conf;
    }

    @Test
    void testPagedRequestsReadThePinnedCut() throws Exception {
        TableBucket tb = makeDvLeader(INITIAL_LEADER_EPOCH);
        // keys 0..19, each written three times: the first two versions of each are superseded
        for (int round = 0; round < 3; round++) {
            put(tb, 0, 20, "v" + round);
        }
        long hw = replicaManager.getReplicaOrException(tb).getLogHighWatermark();

        // a pin with the whole log mask in one page
        GetDvSnapshotResponse all = getDvWhenReady(tb, paged(null, 0L, DEFAULT_LOG_PAGE_BYTES));
        assertThat(all.getLogEndOffset()).isEqualTo(hw);
        assertThat(all.getLogDvToOffset()).isEqualTo(hw);
        Roaring64Bitmap masked = bitmap(all.getLogDvBitmap());
        assertThat(masked.getLongCardinality()).isEqualTo(40L);

        GetDvSnapshotResponse pin = getDvWhenReady(tb, paged(null, null, DEFAULT_LOG_PAGE_BYTES));
        assertThat(pin.getLogEndOffset()).isEqualTo(hw);
        assertThat(pin.getSnapshotStartOffset()).isEqualTo(0L);
        assertThat(pin.hasLogDvBitmap()).isFalse();
        assertThat(pin.hasLogDvToOffset()).isFalse();
        assertThat(pin.getLakeDvEntriesCount()).isZero();
        long cut = pin.getLogEndOffset();

        // writes go on; the bucket's own masks move on
        put(tb, 0, 20, "v3");
        long later = replicaManager.getReplicaOrException(tb).getLogHighWatermark();
        assertThat(later).isGreaterThan(cut);
        GetDvSnapshotResponse now = masksAt(tb, later);
        assertThat(bitmap(now.getLogDvBitmap()).getLongCardinality()).isEqualTo(60L);

        // the parts of the cut: the log mask page by page, of exactly the masks pinned
        Roaring64Bitmap pages = new Roaring64Bitmap();
        long from = 0;
        int pageCount = 0;
        while (from < cut) {
            GetDvSnapshotResponse page = getDvWhenReady(tb, paged(cut, from, 1));
            assertThat(page.getLogEndOffset()).isEqualTo(cut);
            assertThat(page.getLogDvToOffset()).isGreaterThan(from).isLessThanOrEqualTo(cut);
            pages.or(bitmap(page.hasLogDvBitmap() ? page.getLogDvBitmap() : null));
            from = page.getLogDvToOffset();
            pageCount++;
        }
        assertThat(pageCount).isGreaterThanOrEqualTo(1);
        assertThat(offsets(pages)).isEqualTo(offsets(masked));
        // from the cut on, nothing
        GetDvSnapshotResponse end = getDvWhenReady(tb, paged(cut, cut, 1));
        assertThat(end.getLogDvToOffset()).isEqualTo(cut);
        assertThat(end.hasLogDvBitmap()).isFalse();

        // a pin now is another cut, at the new high watermark
        GetDvSnapshotResponse second = getDvWhenReady(tb, paged(null, 0L, DEFAULT_LOG_PAGE_BYTES));
        assertThat(second.getLogEndOffset()).isEqualTo(later);
        assertThat(second.getLogDvToOffset()).isEqualTo(later);
        assertThat(bitmap(second.getLogDvBitmap()).getLongCardinality()).isEqualTo(60L);
    }

    @Test
    void testLeaderChangeDropsTheCuts() throws Exception {
        TableBucket tb = makeDvLeader(INITIAL_LEADER_EPOCH);
        put(tb, 0, 10, "a");
        put(tb, 0, 10, "b");
        long cut = getDvWhenReady(tb, paged(null, null, DEFAULT_LOG_PAGE_BYTES)).getLogEndOffset();
        put(tb, 0, 10, "c");
        assertThat(getDvWhenReady(tb, paged(cut, 0L, DEFAULT_LOG_PAGE_BYTES)).getLogEndOffset())
                .isEqualTo(cut);

        // a new leader epoch opens the bucket's state anew, without the cut
        makeKvTableAsLeader(tb, DV_TABLE_PATH, INITIAL_LEADER_EPOCH + 1, false);
        assertThatThrownBy(() -> getDvWhenReady(tb, paged(cut, 0L, DEFAULT_LOG_PAGE_BYTES)))
                .satisfies(
                        e -> {
                            StaleSnapshotException stale =
                                    ExceptionUtils.findThrowable(e, StaleSnapshotException.class)
                                            .orElseThrow(() -> new AssertionError(e));
                            assertThat(stale.getCurrentSnapshotId()).isEqualTo(-1L);
                            assertThat(stale.getRequestedSnapshotId()).isEqualTo(-1L);
                        })
                .hasMessageContaining("cut " + cut);
        // a pin on the new leader works
        GetDvSnapshotResponse pin = getDvWhenReady(tb, paged(null, 0L, DEFAULT_LOG_PAGE_BYTES));
        assertThat(pin.getLogEndOffset())
                .isEqualTo(replicaManager.getReplicaOrException(tb).getLogHighWatermark());
    }

    /**
     * A cut no request asks for again, as that of a read that ended, was cancelled or lost its
     * client, is dropped once it idled for the timeout.
     */
    @Test
    void testCutNoRequestAsksForExpires() throws Exception {
        TableBucket tb = makeDvLeader(INITIAL_LEADER_EPOCH);
        put(tb, 0, 10, "a");
        long cut = getDvWhenReady(tb, paged(null, null, DEFAULT_LOG_PAGE_BYTES)).getLogEndOffset();
        DerivedDvManager dv = dvManager(tb);
        assertThat(dv.pinnedCutCount()).isEqualTo(1);

        waitUntil(() -> dv.pinnedCutCount() == 0, EXPIRY_WAIT, "the idle cut was not dropped");

        // gone for good once the high watermark moved past it
        put(tb, 10, 20, "b");
        assertThatThrownBy(() -> getDvWhenReady(tb, paged(cut, 0L, DEFAULT_LOG_PAGE_BYTES)))
                .satisfies(
                        e ->
                                assertThat(
                                                ExceptionUtils.findThrowable(
                                                        e, StaleSnapshotException.class))
                                        .isPresent());
    }

    /**
     * A pin request retried after the server took it, e.g. as its response was lost, gets the cut
     * it pinned without another pin, also once the high watermark moved.
     */
    @Test
    void testRetriedPinRequestPinsOnce() throws Exception {
        TableBucket tb = makeDvLeader(INITIAL_LEADER_EPOCH);
        put(tb, 0, 10, "a");
        long cut = getDvWhenReady(tb, pin(42L)).getLogEndOffset();
        // the retry reaches the server after more writes
        put(tb, 10, 20, "b");
        assertThat(getDvWhenReady(tb, pin(42L)).getLogEndOffset()).isEqualTo(cut);
        assertThat(dvManager(tb).pinnedCutCount()).isEqualTo(1);
    }

    @Test
    void testInvalidPageRequestsAreRefused() throws Exception {
        TableBucket tb = makeDvLeader(INITIAL_LEADER_EPOCH);
        put(tb, 0, 5, "a");
        long cut = getDvWhenReady(tb, paged(null, null, DEFAULT_LOG_PAGE_BYTES)).getLogEndOffset();
        assertThatThrownBy(() -> getDvWhenReady(tb, paged(cut, cut + 1, DEFAULT_LOG_PAGE_BYTES)))
                .isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> paged(cut, 0L, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    // ------------------------------------------------------------------------------------------

    private TableBucket makeDvLeader(int leaderEpoch) throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true");
        properties.put(ConfigOptions.TABLE_DATALAKE_DELETION_VECTORS_ENABLED.key(), "true");
        registerTableInZkClient(
                DV_TABLE_PATH,
                DATA1_SCHEMA_PK,
                DV_TABLE_ID,
                Collections.singletonList("a"),
                properties);
        TableBucket tb = new TableBucket(DV_TABLE_ID, 0);
        makeKvTableAsLeader(tb, DV_TABLE_PATH, leaderEpoch, false);
        assertThat(replicaManager.getReplicaOrException(tb).getKvTablet().isDvEnabled()).isTrue();
        return tb;
    }

    /** Writes keys {@code [from, to)} with value {@code value} and waits for the commit. */
    private void put(TableBucket tb, int from, int to, String value) throws Exception {
        List<Object[]> rows = new ArrayList<>();
        for (int k = from; k < to; k++) {
            rows.add(new Object[] {k, value});
        }
        CompletableFuture<List<PutKvResultForBucket>> future = new CompletableFuture<>();
        replicaManager.putRecordsToKv(
                20000,
                -1,
                Collections.singletonMap(tb, genKvRecordBatch(rows.toArray(new Object[0][]))),
                null,
                MergeMode.DEFAULT,
                PUT_KV_VERSION,
                future::complete);
        PutKvResultForBucket result = future.get(30, TimeUnit.SECONDS).get(0);
        assertThat(result.failed()).as(String.valueOf(result.getErrorMessage())).isFalse();
    }

    /** The whole log mask of a pin, once it is of high watermark {@code hw}. */
    private GetDvSnapshotResponse masksAt(TableBucket tb, long hw) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        while (true) {
            // built masks are served again for a moment, so they may be of an earlier hw
            GetDvSnapshotResponse masks =
                    getDvWhenReady(tb, paged(null, 0L, DEFAULT_LOG_PAGE_BYTES));
            if (masks.getLogEndOffset() == hw || System.currentTimeMillis() > deadline) {
                assertThat(masks.getLogEndOffset()).isEqualTo(hw);
                return masks;
            }
            Thread.sleep(20);
        }
    }

    private static DvMaskRequest paged(Long cut, Long logFrom, int maxBytes) {
        return new DvMaskRequest(cut, Collections.emptyList(), logFrom, maxBytes, true, null);
    }

    /** A pin of the bucket's cut by request {@code pinRequestId}. */
    private static DvMaskRequest pin(long pinRequestId) {
        return new DvMaskRequest(
                null, Collections.emptyList(), null, DEFAULT_LOG_PAGE_BYTES, true, pinRequestId);
    }

    private DerivedDvManager dvManager(TableBucket tb) {
        return replicaManager.getReplicaOrException(tb).getKvTablet().getDvManager();
    }

    /** A union read of the empty lake, retried while the bucket replays its log. */
    private GetDvSnapshotResponse getDvWhenReady(TableBucket tb, DvMaskRequest masks)
            throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        while (true) {
            try {
                return replicaManager.getDvSnapshot(
                        tb.getTableId(), tb.getPartitionId(), tb.getBucket(), -1L, masks);
            } catch (RetriableException e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(20);
            }
        }
    }

    private static Roaring64Bitmap bitmap(byte[] bytes) throws Exception {
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        if (bytes != null) {
            RoaringBitmapUtils.deserializeRoaringBitmap64(bitmap, bytes);
        }
        return bitmap;
    }

    private static List<Long> offsets(Roaring64Bitmap bitmap) {
        List<Long> out = new ArrayList<>();
        for (LongIterator it = bitmap.getLongIterator(); it.hasNext(); ) {
            out.add(it.next());
        }
        return out;
    }
}
