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
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.GetDvSnapshotRequest;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Pins one cut per bucket on the buckets' leaders when a union read is planned, so the parts of a
 * bucket read with the same masks. A pin carries no masks, only the cut, the offset the log mask
 * starts at, and the snapshot's identity.
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

        PinnedCut(long cut, long logStartOffset, @Nullable String lakeSnapshotIdentity) {
            this.cut = cut;
            this.logStartOffset = logStartOffset;
            this.lakeSnapshotIdentity = lakeSnapshotIdentity;
        }
    }

    private final Admin admin;
    private final TablePath tablePath;
    private final DvMaskFetcher.Sleeper sleeper;

    DvCutPinner(Admin admin, TablePath tablePath, DvMaskFetcher.Sleeper sleeper) {
        this.admin = admin;
        this.tablePath = tablePath;
        this.sleeper = sleeper;
    }

    /**
     * Pins a cut of lake snapshot {@code snapshotId} for each bucket, at most {@link
     * #MAX_CONCURRENT_PINS} at a time. A bucket that cannot be pinned is absent; its reader fetches
     * masks when its part starts. Returns null if the server does not pin cuts, which it shows by
     * answering the first pin with all masks.
     */
    @Nullable
    Map<TableBucket, PinnedCut> pin(long snapshotId, List<TableBucket> buckets) throws Exception {
        Map<TableBucket, PinnedCut> pins = new HashMap<>();
        if (buckets.isEmpty()) {
            return pins;
        }
        // Pin one bucket at a time until one answers: a server that does not pin answers with all
        // masks, which must not be requested for every bucket.
        Iterator<TableBucket> remaining = buckets.iterator();
        while (remaining.hasNext()) {
            TableBucket bucket = remaining.next();
            GetDvSnapshotResponse response = pinOne(snapshotId, bucket);
            if (response == null) {
                continue;
            }
            if (!(response.hasPaged() && response.isPaged())) {
                LOG.info(
                        "The leaders of {} do not pin mask cuts; each bucket is read by one"
                                + " part.",
                        tablePath);
                return null;
            }
            pins.put(bucket, toPin(response));
            break;
        }
        List<TableBucket> rest = new ArrayList<>();
        remaining.forEachRemaining(rest::add);
        if (rest.isEmpty()) {
            return pins;
        }
        ExecutorService executor =
                Executors.newFixedThreadPool(Math.min(MAX_CONCURRENT_PINS, rest.size()));
        try {
            List<Future<GetDvSnapshotResponse>> futures = new ArrayList<>(rest.size());
            for (TableBucket bucket : rest) {
                futures.add(executor.submit(() -> pinOne(snapshotId, bucket)));
            }
            for (int i = 0; i < rest.size(); i++) {
                GetDvSnapshotResponse response = futures.get(i).get();
                if (response != null && response.hasPaged() && response.isPaged()) {
                    pins.put(rest.get(i), toPin(response));
                }
            }
        } finally {
            executor.shutdownNow();
        }
        return pins;
    }

    /** Pins the bucket's cut; null if the bucket cannot be pinned now. */
    @Nullable
    private GetDvSnapshotResponse pinOne(long snapshotId, TableBucket bucket) throws Exception {
        try {
            return new DvMaskFetcher(admin, tablePath, sleeper)
                    .fetch(
                            () -> {
                                GetDvSnapshotRequest request =
                                        new GetDvSnapshotRequest()
                                                .setTableId(bucket.getTableId())
                                                .setBucketId(bucket.getBucket())
                                                .setReadableSnapshotId(snapshotId)
                                                .setPaged(true);
                                if (bucket.getPartitionId() != null) {
                                    request.setPartitionId(bucket.getPartitionId());
                                }
                                return request;
                            });
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
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
