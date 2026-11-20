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
import org.apache.fluss.client.admin.DvRequestRetries;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.DeletionVectorsNotReadyException;
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.GetDvSnapshotRequest;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.function.Supplier;

/**
 * Fetches deletion vector masks of a bucket from its leader for a union read. Waits while the
 * leader switches snapshots or rebuilds, and follows a leader that moved.
 *
 * <ul>
 *   <li>Not ready: the leader switches to the snapshot, rebuilds its masks or replays its change
 *       list in the background, which takes as long as replaying its log. The fetch waits that
 *       long.
 *   <li>Leader moved or unavailable ({@link DvRequestRetries#isLeaderUnavailable}): the admin
 *       dropped the stale leader, so a retry reaches the new one. An election takes seconds, so it
 *       has a separate budget.
 *   <li>Stale: the leader covers another snapshot, or no longer keeps the requested cut. The fetch
 *       fails at once, and the caller plans the bucket again or derives the masks.
 *   <li>A read that does not wait gets the leader's answer at once, in the response: the snapshot
 *       it covers instead, as a {@link StaleSnapshotException}, or the cut to read by key up to, as
 *       a {@link DeletionVectorsNotReadyException#isRebuilding() rebuilding} one. The fetch fails
 *       with either at once.
 * </ul>
 *
 * <p>Any other error fails the fetch at once, and so does a request the leader does not answer
 * within the client's request timeout: the RPC layer has no timeout of its own, and a server that
 * never reads the request leaves it unanswered until it closes the idle connection. The admin then
 * drops its connection to the leader, so that the next request, also one of the read by key that
 * follows, connects again instead of waiting behind it. The retries are those of {@link
 * DvRequestRetries}, which tiering rounds share.
 */
@Internal
public class DvMaskFetcher {

    private static final Logger LOG = LoggerFactory.getLogger(DvMaskFetcher.class);

    static final long INITIAL_BACKOFF_MS = 300;
    // A leader election after a tablet-server failure takes seconds; this allows about 20s.
    static final int MAX_LEADER_RETRIES = 8;
    static final long MAX_LEADER_BACKOFF_MS = 4000;
    static final long MAX_NOT_READY_WAIT_MS = 120_000;
    static final long MAX_NOT_READY_BACKOFF_MS = 2000;

    /** Waits between attempts; tests replace it. */
    public interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    private final DvAdmin admin;
    private final TablePath tablePath;
    private final Sleeper sleeper;
    private final DvRequestRetries retries;

    public DvMaskFetcher(DvAdmin admin, TablePath tablePath, Sleeper sleeper) {
        this(
                admin,
                tablePath,
                sleeper,
                ConfigOptions.CLIENT_REQUEST_TIMEOUT.defaultValue().toMillis());
    }

    public DvMaskFetcher(
            DvAdmin admin, TablePath tablePath, Sleeper sleeper, long requestTimeoutMs) {
        this.admin = admin;
        this.tablePath = tablePath;
        this.sleeper = sleeper;
        // an unanswered request fails the fetch at once: the part reads the bucket by key, on a
        // new connection
        this.retries =
                new DvRequestRetries(
                        requestTimeoutMs,
                        MAX_LEADER_RETRIES,
                        INITIAL_BACKOFF_MS,
                        MAX_LEADER_BACKOFF_MS,
                        INITIAL_BACKOFF_MS,
                        MAX_NOT_READY_BACKOFF_MS,
                        false);
    }

    /**
     * Sends the request built by {@code request} until the leader answers it with masks or the
     * retries run out. {@code request} is called once per attempt.
     */
    public GetDvSnapshotResponse fetch(Supplier<GetDvSnapshotRequest> request) throws Exception {
        GetDvSnapshotRequest[] sent = new GetDvSnapshotRequest[1];
        GetDvSnapshotResponse response =
                retries.send(
                        admin,
                        tablePath,
                        () -> sent[0] = request.get(),
                        MAX_NOT_READY_WAIT_MS,
                        this::retry);
        throwIfNoMasks(response, sent[0].getReadableSnapshotId());
        return response;
    }

    private void retry(GetDvSnapshotRequest request, Throwable cause, long backoffMs)
            throws InterruptedException {
        LOG.info(
                "The leader of bucket {} of {} {} for snapshot {} ({}); retry in {} ms.",
                request.getBucketId(),
                tablePath,
                DvRequestRetries.isNotReady(cause) ? "is not ready" : "moved or is unavailable",
                request.getReadableSnapshotId(),
                cause.getMessage(),
                backoffMs);
        sleeper.sleep(backoffMs);
    }

    /**
     * Fails with the answer of a leader that served no masks: the snapshot it covers instead, or
     * the cut to read the bucket by key up to.
     */
    private static void throwIfNoMasks(GetDvSnapshotResponse response, long requested) {
        if (response.hasFoldCut()) {
            throw DeletionVectorsNotReadyException.rebuilding(
                    String.format("No masks of lake snapshot %d", requested),
                    "it answered with a cut",
                    response.getFoldCut());
        }
        if (response.hasCoveredSnapshotId()) {
            throw new StaleSnapshotException(
                    response.getCoveredSnapshotId(),
                    requested,
                    response.hasBehind() && response.isBehind(),
                    response.hasSnapshotStartOffset() ? response.getSnapshotStartOffset() : -1L);
        }
    }

    /** Returns the {@link StaleSnapshotException} in {@code e}, or null. */
    @Nullable
    public static StaleSnapshotException stale(Throwable e) {
        return ExceptionUtils.findThrowable(e, StaleSnapshotException.class).orElse(null);
    }

    /**
     * The cut a leader with nothing to serve yet named for a read that does not wait ({@link
     * DeletionVectorsNotReadyException#isRebuilding()}); the reader reads the bucket by key up to
     * it. Returns -1 for any other failure.
     */
    public static long rebuildingCut(Throwable e) {
        return ExceptionUtils.findThrowable(e, DeletionVectorsNotReadyException.class)
                .map(DeletionVectorsNotReadyException::getRebuildingCut)
                .orElse(-1L);
    }
}
