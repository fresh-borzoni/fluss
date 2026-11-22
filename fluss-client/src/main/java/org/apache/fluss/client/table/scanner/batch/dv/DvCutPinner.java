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

package org.apache.fluss.client.table.scanner.batch.dv;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.admin.DvAdmin;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.GetDvSnapshotRequest;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Pins one cut per bucket on the buckets' leaders when a union read is planned, so the parts of a
 * bucket read with the same masks. A pin carries no masks, only the cut, the offset the log mask
 * starts at, and the snapshot's identity. A leader with nothing to serve yet answers a read that
 * does not wait with a cut instead; the bucket's parts all derive their masks of that cut from the
 * keys.
 */
@Internal
public class DvCutPinner {

    private static final Logger LOG = LoggerFactory.getLogger(DvCutPinner.class);

    public static final int MAX_CONCURRENT_PINS = 16;

    /** The cut pinned for a bucket. */
    static final class PinnedCut {
        final long cut;
        final long logStartOffset;
        @Nullable final String lakeSnapshotIdentity;
        // the leader had nothing to serve: the bucket is read by key up to the cut, which the
        // leader keeps no masks of
        final boolean folded;

        PinnedCut(long cut, long logStartOffset, @Nullable String lakeSnapshotIdentity) {
            this(cut, logStartOffset, lakeSnapshotIdentity, false);
        }

        private PinnedCut(
                long cut,
                long logStartOffset,
                @Nullable String lakeSnapshotIdentity,
                boolean folded) {
            this.cut = cut;
            this.logStartOffset = logStartOffset;
            this.lakeSnapshotIdentity = lakeSnapshotIdentity;
            this.folded = folded;
        }

        /** The cut a leader with nothing to serve named; the log starts at the readable offset. */
        static PinnedCut folded(long cut) {
            return new PinnedCut(cut, -1L, null, true);
        }
    }

    private final DvAdmin admin;
    private final TablePath tablePath;
    private final DvMaskFetcher.Sleeper sleeper;
    // false: a bucket whose leader still switches to the snapshot is not waited for; its part
    // reads the snapshot the leader covers
    private final boolean waitForCatchUp;
    private final long requestTimeoutMs;

    DvCutPinner(
            DvAdmin admin,
            TablePath tablePath,
            DvMaskFetcher.Sleeper sleeper,
            boolean waitForCatchUp,
            long requestTimeoutMs) {
        this.admin = admin;
        this.tablePath = tablePath;
        this.sleeper = sleeper;
        this.waitForCatchUp = waitForCatchUp;
        this.requestTimeoutMs = requestTimeoutMs;
    }

    /**
     * Pins a cut of lake snapshot {@code snapshotId} for each bucket, at most {@link
     * #MAX_CONCURRENT_PINS} at a time. A bucket that cannot be pinned is absent; its reader fetches
     * masks when its part starts.
     *
     * <p>If pinning fails or the calling thread is interrupted, no cut is returned and pins not
     * sent yet are not sent. The cuts pinned so far idle out on their leaders.
     */
    Map<TableBucket, PinnedCut> pin(long snapshotId, List<TableBucket> buckets) throws Exception {
        if (buckets.isEmpty()) {
            return new HashMap<>();
        }
        ExecutorService executor =
                Executors.newFixedThreadPool(Math.min(MAX_CONCURRENT_PINS, buckets.size()));
        Map<TableBucket, Future<PinnedCut>> pinning = new LinkedHashMap<>();
        try {
            for (TableBucket bucket : buckets) {
                pinning.put(bucket, executor.submit(() -> pinOne(snapshotId, bucket)));
            }
            Map<TableBucket, PinnedCut> pins = new HashMap<>();
            for (Map.Entry<TableBucket, Future<PinnedCut>> pin : pinning.entrySet()) {
                PinnedCut cut = pin.getValue().get();
                if (cut != null) {
                    pins.put(pin.getKey(), cut);
                }
            }
            return pins;
        } finally {
            // stops the pins still in flight if planning failed: no read waits for their cuts
            for (Future<PinnedCut> pin : pinning.values()) {
                pin.cancel(true);
            }
            executor.shutdown();
        }
    }

    /** Pins the bucket's cut; null if the bucket cannot be pinned now. */
    @Nullable
    private PinnedCut pinOne(long snapshotId, TableBucket bucket) throws Exception {
        // the same in every retry of this pin, so a retry the leader took pins nothing more
        long pinRequestId = ThreadLocalRandom.current().nextLong();
        try {
            GetDvSnapshotResponse response =
                    new DvMaskFetcher(admin, tablePath, sleeper, requestTimeoutMs)
                            .fetch(
                                    () -> {
                                        GetDvSnapshotRequest request =
                                                new GetDvSnapshotRequest()
                                                        .setTableId(bucket.getTableId())
                                                        .setBucketId(bucket.getBucket())
                                                        .setReadableSnapshotId(snapshotId)
                                                        .setPinRequestId(pinRequestId);
                                        if (bucket.getPartitionId() != null) {
                                            request.setPartitionId(bucket.getPartitionId());
                                        }
                                        if (!waitForCatchUp) {
                                            request.setWaitForCatchUp(false);
                                        }
                                        return request;
                                    });
            return toPin(response);
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            long foldCut = DvMaskFetcher.rebuildingCut(e);
            if (foldCut >= 0) {
                LOG.info(
                        "The leader of {} of {} has no masks of snapshot {} to serve yet; its"
                                + " parts read it by key up to log offset {}.",
                        bucket,
                        tablePath,
                        snapshotId,
                        foldCut);
                return PinnedCut.folded(foldCut);
            }
            // e.g. the leader covers another snapshot or the partition was dropped: the reader
            // resolves the bucket when its part starts
            LOG.info(
                    "Cannot pin the mask cut of {} of {} for snapshot {} ({}); its part fetches"
                            + " masks when it starts.",
                    bucket,
                    tablePath,
                    snapshotId,
                    ExceptionUtils.stripExecutionException(e).getMessage());
            return null;
        }
    }

    private static PinnedCut toPin(GetDvSnapshotResponse response) {
        return new PinnedCut(
                response.getLogEndOffset(),
                response.getSnapshotStartOffset(),
                response.hasLakeSnapshotIdentity() ? response.getLakeSnapshotIdentity() : null);
    }
}
