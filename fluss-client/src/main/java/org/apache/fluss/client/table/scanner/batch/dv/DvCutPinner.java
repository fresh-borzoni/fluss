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
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.HashMap;
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
        // leader holds no masks of
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

    /** A leader's answer to a pin: the pinned cut's response, or a cut to read by key. */
    private static final class Answer {
        @Nullable final GetDvSnapshotResponse response;
        final long foldCut;

        Answer(@Nullable GetDvSnapshotResponse response, long foldCut) {
            this.response = response;
            this.foldCut = foldCut;
        }
    }

    private final DvAdmin admin;
    private final TablePath tablePath;
    private final DvMaskFetcher.Sleeper sleeper;
    // false: a bucket whose leader still switches to the snapshot is not waited for; its part
    // reads the snapshot the leader covers
    private final boolean waitForCatchUp;

    DvCutPinner(
            DvAdmin admin,
            TablePath tablePath,
            DvMaskFetcher.Sleeper sleeper,
            boolean waitForCatchUp) {
        this.admin = admin;
        this.tablePath = tablePath;
        this.sleeper = sleeper;
        this.waitForCatchUp = waitForCatchUp;
    }

    /**
     * Pins a cut of lake snapshot {@code snapshotId} for each bucket, at most {@link
     * #MAX_CONCURRENT_PINS} at a time, with the bucket's number of {@code holds} (1 if absent). A
     * bucket that cannot be pinned is absent; its reader fetches masks when its part starts.
     *
     * <p>If pinning fails or the calling thread is interrupted, no cut is returned, so none is
     * read: the cuts pinned so far are released, those of pins in flight once their leaders answer,
     * and pins not sent yet are not sent.
     */
    Map<TableBucket, PinnedCut> pin(
            long snapshotId, List<TableBucket> buckets, Map<TableBucket, Integer> holds)
            throws Exception {
        if (buckets.isEmpty()) {
            return new HashMap<>();
        }
        Pins pins = new Pins(snapshotId, holds);
        ExecutorService executor =
                Executors.newFixedThreadPool(Math.min(MAX_CONCURRENT_PINS, buckets.size()));
        List<Future<?>> futures = new ArrayList<>(buckets.size());
        boolean pinned = false;
        try {
            for (TableBucket bucket : buckets) {
                futures.add(executor.submit(() -> pins.pin(bucket)));
            }
            for (Future<?> future : futures) {
                future.get();
            }
            pinned = true;
            return pins.taken();
        } finally {
            if (!pinned) {
                for (Future<?> future : futures) {
                    future.cancel(false);
                }
                pins.abandon();
            }
            // pins in flight finish, so their cuts are released
            executor.shutdown();
        }
    }

    /** The cuts pinned for the buckets of one planning. */
    private final class Pins {
        private final long snapshotId;
        private final Map<TableBucket, Integer> holds;

        @GuardedBy("this")
        private final Map<TableBucket, PinnedCut> taken = new HashMap<>();

        // the planning ended without the cuts: the pins are released
        @GuardedBy("this")
        private boolean abandoned;

        Pins(long snapshotId, Map<TableBucket, Integer> holds) {
            this.snapshotId = snapshotId;
            this.holds = holds;
        }

        @Nullable
        Void pin(TableBucket bucket) throws Exception {
            Answer answer = pinOne(snapshotId, bucket, holdsOf(bucket));
            if (answer == null) {
                return null;
            }
            PinnedCut cut =
                    answer.response == null
                            ? PinnedCut.folded(answer.foldCut)
                            : toPin(answer.response);
            synchronized (this) {
                if (!abandoned) {
                    taken.put(bucket, cut);
                    return null;
                }
            }
            release(bucket, cut);
            return null;
        }

        synchronized Map<TableBucket, PinnedCut> taken() {
            return new HashMap<>(taken);
        }

        void abandon() {
            Map<TableBucket, PinnedCut> released;
            synchronized (this) {
                abandoned = true;
                released = new HashMap<>(taken);
                taken.clear();
            }
            for (Map.Entry<TableBucket, PinnedCut> e : released.entrySet()) {
                release(e.getKey(), e.getValue());
            }
        }

        private int holdsOf(TableBucket bucket) {
            return holds.getOrDefault(bucket, 1);
        }

        private void release(TableBucket bucket, PinnedCut cut) {
            // a folded bucket's leader holds no cut
            if (!cut.folded) {
                new DvMaskFetcher(admin, tablePath, sleeper)
                        .release(
                                DvMaskFetcher.releaseRequest(
                                        bucket, snapshotId, cut.cut, holdsOf(bucket)));
            }
        }
    }

    /** Pins the bucket's cut; null if the bucket cannot be pinned now. */
    @Nullable
    private Answer pinOne(long snapshotId, TableBucket bucket, int holds) throws Exception {
        // the same in every retry of this pin, so a retry the leader took pins nothing more
        long pinRequestId = ThreadLocalRandom.current().nextLong();
        try {
            GetDvSnapshotResponse response =
                    new DvMaskFetcher(admin, tablePath, sleeper)
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
                                        if (holds > 1) {
                                            request.setHolds(holds);
                                        }
                                        return request;
                                    });
            return new Answer(response, -1L);
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
                return new Answer(null, foldCut);
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
