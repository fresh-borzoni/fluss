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

package org.apache.fluss.client.admin;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.DeletionVectorsNotReadyException;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.GetDvSnapshotRequest;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.utils.ExceptionUtils;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * The retries of the deletion vector requests of union reads and tiering rounds. An attempt waits
 * at most the request timeout for its answer, as the RPC layer bounds no request; an unanswered
 * request is abandoned, which drops the connection to its server ({@link DvAdmin#await}).
 *
 * <ul>
 *   <li>Not ready: the leader switches snapshots, rebuilds its deletion vectors or replays its
 *       change list. Retried with backoff as long as the caller waits for that.
 *   <li>Leader unavailable ({@link #isLeaderUnavailable}): retried with backoff a bounded number of
 *       times, on the leader the admin resolves again; so is an unanswered request, on a new
 *       connection, if the caller retries it.
 *   <li>Any other error fails at once.
 * </ul>
 */
@Internal
public final class DvRequestRetries {

    /** Logs a retry of {@code request} after {@code cause}, then waits {@code backoffMs}. */
    @FunctionalInterface
    public interface Backoff {
        void retry(GetDvSnapshotRequest request, Throwable cause, long backoffMs)
                throws InterruptedException;
    }

    private final long requestTimeoutMs;
    private final int maxLeaderRetries;
    private final long leaderBackoffMs;
    private final long maxLeaderBackoffMs;
    private final long notReadyBackoffMs;
    private final long maxNotReadyBackoffMs;
    // whether an unanswered request is retried, as a leader that is unavailable
    private final boolean retriesUnanswered;

    public DvRequestRetries(
            long requestTimeoutMs,
            int maxLeaderRetries,
            long leaderBackoffMs,
            long maxLeaderBackoffMs,
            long notReadyBackoffMs,
            long maxNotReadyBackoffMs,
            boolean retriesUnanswered) {
        this.requestTimeoutMs = requestTimeoutMs;
        this.maxLeaderRetries = maxLeaderRetries;
        this.leaderBackoffMs = leaderBackoffMs;
        this.maxLeaderBackoffMs = maxLeaderBackoffMs;
        this.notReadyBackoffMs = notReadyBackoffMs;
        this.maxNotReadyBackoffMs = maxNotReadyBackoffMs;
        this.retriesUnanswered = retriesUnanswered;
    }

    /**
     * Sends the request built by {@code request}, once per attempt, until the leader answers it.
     * Waits for a leader that is not ready at most {@code maxNotReadyWaitMs} in all, then fails
     * with its last answer.
     */
    public GetDvSnapshotResponse send(
            DvAdmin admin,
            TablePath tablePath,
            Supplier<GetDvSnapshotRequest> request,
            long maxNotReadyWaitMs,
            Backoff backoff)
            throws Exception {
        int leaderRetries = 0;
        long nextLeaderBackoffMs = leaderBackoffMs;
        long notReadyWaitedMs = 0;
        long nextNotReadyBackoffMs = notReadyBackoffMs;
        while (true) {
            GetDvSnapshotRequest req = request.get();
            try {
                return await(
                        admin, tablePath, req.getBucketId(), admin.getDvSnapshot(tablePath, req));
            } catch (Exception e) {
                boolean unanswered =
                        ExceptionUtils.findThrowable(e, TimeoutException.class).isPresent();
                long backoffMs;
                if (isNotReady(e)) {
                    if (notReadyWaitedMs >= maxNotReadyWaitMs) {
                        throw e;
                    }
                    backoffMs =
                            Math.min(nextNotReadyBackoffMs, maxNotReadyWaitMs - notReadyWaitedMs);
                    notReadyWaitedMs += backoffMs;
                    nextNotReadyBackoffMs =
                            Math.min(2 * nextNotReadyBackoffMs, maxNotReadyBackoffMs);
                } else if (leaderRetries < maxLeaderRetries
                        && (unanswered ? retriesUnanswered : isLeaderUnavailable(e))) {
                    leaderRetries++;
                    backoffMs = nextLeaderBackoffMs;
                    nextLeaderBackoffMs = Math.min(2 * nextLeaderBackoffMs, maxLeaderBackoffMs);
                } else {
                    throw e;
                }
                backoff.retry(req, ExceptionUtils.stripExecutionException(e), backoffMs);
            }
        }
    }

    /**
     * Waits at most the request timeout for {@code answer}, the answer of a request of bucket
     * {@code bucket}, and abandons it if the answer does not come ({@link DvAdmin#await}).
     */
    public <T> T await(DvAdmin admin, TablePath tablePath, int bucket, CompletableFuture<T> answer)
            throws Exception {
        return admin.await(
                answer,
                requestTimeoutMs,
                () ->
                        String.format(
                                "The leader of bucket %d of %s did not answer a deletion vector"
                                        + " request within %d ms.",
                                bucket, tablePath, requestTimeoutMs));
    }

    /** Whether the leader was not ready to serve a request. */
    public static boolean isNotReady(Throwable e) {
        return ExceptionUtils.findThrowable(e, DeletionVectorsNotReadyException.class).isPresent();
    }

    /**
     * Whether the bucket's leader is not reachable for now: it moved, is being elected, or its
     * server restarted and is not in the client's metadata yet.
     */
    public static boolean isLeaderUnavailable(Throwable e) {
        if (ExceptionUtils.findThrowable(e, RetriableException.class).isPresent()) {
            // InvalidMetadataException (not leader, leader not available, server not in the
            // metadata cache, network error) and other retriable errors such as timeouts
            return true;
        }
        // MetadataUpdater#leaderFor when no leader is elected yet; the client throws it untyped
        return ExceptionUtils.findThrowableWithMessage(e, "Leader not found").isPresent();
    }
}
