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

package org.apache.fluss.client.tiering;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.DvAdmin;
import org.apache.fluss.client.admin.DvRequestRetries;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.GetDvSnapshotRequest;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Sends the requests of the tiering rounds of a table with deletion vectors to the leaders of their
 * buckets: the plan of a round's end, and the parts of its log mask and of its deletes. Retries
 * while a leader is not ready, as long as the caller waits, and with backoff while the leader moves
 * or restarts, a bounded number of times. Each attempt waits for its answer at most the request
 * timeout; see {@link DvRequestRetries}.
 */
@Internal
public final class DvRoundRequests {

    private static final Logger LOG = LoggerFactory.getLogger(DvRoundRequests.class);

    // A leader election or a restarted server's registration takes seconds: about 40 s in all.
    @VisibleForTesting static final int MAX_LEADER_RETRIES = 12;
    private static final long LEADER_RETRY_INITIAL_BACKOFF_MS = 200;
    private static final long LEADER_RETRY_MAX_BACKOFF_MS = 5_000;
    // A leader answers "not ready" until it holds every change below the round's end: also while
    // it rebuilds its deletion vectors or replays a lost change list from the readable offset,
    // minutes for a large backlog. A part of a round's deletes or log mask waits that long; then
    // the round fails and runs again.
    public static final long MAX_NOT_READY_WAIT_MS = 10 * 60_000;
    private static final long NOT_READY_INITIAL_BACKOFF_MS = 250;
    private static final long NOT_READY_MAX_BACKOFF_MS = 5_000;

    private final Admin admin;
    private final TablePath tablePath;
    private final Sleeper sleeper;
    // a round that fails is tiered again from its start, so an unanswered request is retried
    private final DvRequestRetries retries;
    // the buckets whose wait for a leader that is not ready is logged already
    private final Set<TableBucket> waitsLogged = ConcurrentHashMap.newKeySet();

    /** Requests whose attempts wait for an answer at most the default request timeout. */
    public DvRoundRequests(Admin admin, TablePath tablePath, Sleeper sleeper) {
        this(
                admin,
                tablePath,
                sleeper,
                ConfigOptions.CLIENT_REQUEST_TIMEOUT.defaultValue().toMillis());
    }

    /** Requests whose attempts wait for an answer at most {@code requestTimeoutMs}. */
    public DvRoundRequests(
            Admin admin, TablePath tablePath, Sleeper sleeper, long requestTimeoutMs) {
        this.admin = admin;
        this.tablePath = tablePath;
        this.sleeper = sleeper;
        this.retries =
                new DvRequestRetries(
                        requestTimeoutMs,
                        MAX_LEADER_RETRIES,
                        LEADER_RETRY_INITIAL_BACKOFF_MS,
                        LEADER_RETRY_MAX_BACKOFF_MS,
                        NOT_READY_INITIAL_BACKOFF_MS,
                        NOT_READY_MAX_BACKOFF_MS,
                        true);
    }

    /**
     * Plans the end of the bucket's round on the snapshot from {@code startOffset}, with at most
     * {@code maxRecords} records and {@code maxBytes} bytes. A plan reads the leader's log only, so
     * the leader is always ready for it.
     */
    public GetDvSnapshotResponse plan(
            TableBucket tb, long snapshotId, long startOffset, long maxRecords, long maxBytes)
            throws Exception {
        return send(
                tb,
                snapshotId,
                planRequest(tb, snapshotId, startOffset, maxRecords, maxBytes),
                "the plan",
                0);
    }

    /**
     * Plans the rounds of the buckets from their start offsets on the snapshot, within the caps:
     * the requests of all buckets go out at once, and a bucket whose request fails is planned again
     * with the retries of {@link #plan}. Returns each bucket's answer, whose round ends within the
     * record cap.
     */
    public Map<TableBucket, GetDvSnapshotResponse> planRounds(
            long snapshotId, Map<TableBucket, Long> startOffsets, long maxRecords, long maxBytes)
            throws Exception {
        checkArgument(
                maxRecords > 0 && maxBytes > 0,
                "A round's caps must be positive: %s records, %s bytes.",
                maxRecords,
                maxBytes);
        Map<TableBucket, CompletableFuture<GetDvSnapshotResponse>> sent = new LinkedHashMap<>();
        for (Map.Entry<TableBucket, Long> start : startOffsets.entrySet()) {
            sent.put(
                    start.getKey(),
                    DvAdmin.of(admin)
                            .getDvSnapshot(
                                    tablePath,
                                    planRequest(
                                            start.getKey(),
                                            snapshotId,
                                            start.getValue(),
                                            maxRecords,
                                            maxBytes)));
        }
        Map<TableBucket, GetDvSnapshotResponse> plans = new HashMap<>();
        for (Map.Entry<TableBucket, CompletableFuture<GetDvSnapshotResponse>> e : sent.entrySet()) {
            TableBucket tb = e.getKey();
            long start = startOffsets.get(tb);
            GetDvSnapshotResponse response;
            try {
                // an unanswered plan is abandoned, so its retry connects again
                response =
                        retries.await(DvAdmin.of(admin), tablePath, tb.getBucket(), e.getValue());
            } catch (Exception failed) {
                response = plan(tb, snapshotId, start, maxRecords, maxBytes);
            }
            checkState(
                    response.hasRoundEndOffset()
                            && response.getRoundEndOffset() >= start
                            && response.getRoundEndOffset() - start <= maxRecords,
                    "The leader of bucket %s of %s planned its round from offset %s to %s.",
                    tb,
                    tablePath,
                    start,
                    response.hasRoundEndOffset() ? response.getRoundEndOffset() : null);
            plans.put(tb, response);
        }
        return plans;
    }

    /** The request of the plan of a bucket's round; see {@link #plan}. */
    static GetDvSnapshotRequest planRequest(
            TableBucket tb, long snapshotId, long startOffset, long maxRecords, long maxBytes) {
        return request(tb, snapshotId)
                .setRoundStartOffset(startOffset)
                .setRoundMaxRecords(maxRecords)
                .setRoundMaxBytes(maxBytes);
    }

    /**
     * Fetches the part from {@code fromOffset} on, or from the start if null, of the deletes of the
     * bucket's round on the snapshot that ends at {@code endOffset}. Waits for a leader that is not
     * ready, e.g. it switches to the snapshot, rebuilds or replays its change list, as long as the
     * round can wait. Other errors, also a leader that covers another snapshot, fail at once.
     */
    public GetDvSnapshotResponse fetch(
            TableBucket tb, long snapshotId, @Nullable Long fromOffset, long endOffset)
            throws Exception {
        return fetch(tb, snapshotId, fromOffset, endOffset, MAX_NOT_READY_WAIT_MS);
    }

    /**
     * Like {@link #fetch(TableBucket, long, Long, long)}, but waits for a leader that is not ready
     * at most {@code maxNotReadyWaitMs}, then fails with its last answer.
     */
    public GetDvSnapshotResponse fetch(
            TableBucket tb,
            long snapshotId,
            @Nullable Long fromOffset,
            long endOffset,
            long maxNotReadyWaitMs)
            throws Exception {
        return send(
                tb,
                snapshotId,
                part(tb, snapshotId, fromOffset, endOffset),
                "the deletes",
                maxNotReadyWaitMs);
    }

    /**
     * Fetches the part from {@code fromOffset} on, or from the start if null, of the log mask of
     * the bucket's round on the snapshot that ends at {@code endOffset}. Waits for a leader that is
     * not ready at most {@code maxNotReadyWaitMs}, then fails with its last answer.
     */
    public GetDvSnapshotResponse fetchMask(
            TableBucket tb,
            long snapshotId,
            @Nullable Long fromOffset,
            long endOffset,
            long maxNotReadyWaitMs)
            throws Exception {
        return send(
                tb,
                snapshotId,
                part(tb, snapshotId, fromOffset, endOffset).setRoundLogMask(true),
                "the log mask",
                maxNotReadyWaitMs);
    }

    /**
     * Sends a request of the bucket's round to its leader with the retries of {@link
     * DvRequestRetries}, waiting for a leader that is not ready at most {@code maxNotReadyWaitMs}.
     * The first wait of a bucket is logged, and when it ends.
     */
    private GetDvSnapshotResponse send(
            TableBucket tb,
            long snapshotId,
            GetDvSnapshotRequest request,
            String what,
            long maxNotReadyWaitMs)
            throws Exception {
        long startMs = System.currentTimeMillis();
        boolean[] waitLogged = {false};
        DvRequestRetries.Backoff backoff =
                (req, cause, backoffMs) -> {
                    if (!DvRequestRetries.isNotReady(cause)) {
                        LOG.info(
                                "The leader of bucket {} of {} is unavailable for {} of a round"
                                        + " ({}); retry in {} ms.",
                                tb,
                                tablePath,
                                what,
                                cause.getMessage(),
                                backoffMs);
                    } else if (waitsLogged.add(tb)) {
                        waitLogged[0] = true;
                        LOG.info(
                                "{} of the round of bucket {} of {} on snapshot {} waits for its"
                                        + " leader: {}",
                                capitalized(what),
                                tb,
                                tablePath,
                                snapshotId,
                                cause.getMessage());
                    } else {
                        LOG.debug(
                                "The leader of bucket {} of {} is not ready ({}); retrying in {}"
                                        + " ms.",
                                tb,
                                tablePath,
                                cause.getMessage(),
                                backoffMs);
                    }
                    sleeper.sleep(backoffMs);
                };
        GetDvSnapshotResponse response =
                retries.send(
                        DvAdmin.of(admin), tablePath, () -> request, maxNotReadyWaitMs, backoff);
        if (waitLogged[0]) {
            LOG.info(
                    "The leader of bucket {} of {} served {} of the round on snapshot {}"
                            + " after {} ms.",
                    tb,
                    tablePath,
                    what,
                    snapshotId,
                    System.currentTimeMillis() - startMs);
        }
        return response;
    }

    private static GetDvSnapshotRequest request(TableBucket tb, long snapshotId) {
        GetDvSnapshotRequest request =
                new GetDvSnapshotRequest()
                        .setTableId(tb.getTableId())
                        .setBucketId(tb.getBucket())
                        .setReadableSnapshotId(snapshotId);
        if (tb.getPartitionId() != null) {
            request.setPartitionId(tb.getPartitionId());
        }
        return request;
    }

    private static GetDvSnapshotRequest part(
            TableBucket tb, long snapshotId, @Nullable Long fromOffset, long endOffset) {
        GetDvSnapshotRequest request = request(tb, snapshotId).setRoundEndOffset(endOffset);
        if (fromOffset != null) {
            request.setRoundFromOffset(fromOffset);
        }
        return request;
    }

    private static String capitalized(String what) {
        return Character.toUpperCase(what.charAt(0)) + what.substring(1);
    }

    /**
     * Fetches a part of the deletes of a bucket's round on a snapshot, from {@code fromOffset} on
     * or from the start if null; replaced in tests.
     */
    @FunctionalInterface
    public interface Fetcher {
        GetDvSnapshotResponse fetch(
                TableBucket bucket, long snapshotId, @Nullable Long fromOffset, long endOffset)
                throws Exception;
    }

    /** Sleeps between retries; replaced in tests. */
    @FunctionalInterface
    public interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }
}
