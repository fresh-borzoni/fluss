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
import org.apache.fluss.exception.DeletionVectorsNotReadyException;
import org.apache.fluss.exception.InvalidMetadataException;
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.GetDvSnapshotRequest;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Fetches deletion vector masks of a bucket from its leader for a union read. Waits while the
 * leader switches snapshots or rebuilds, and follows a leader that moved.
 *
 * <ul>
 *   <li>Not ready: the leader switches to the snapshot, rebuilds its masks or replays its change
 *       list in the background, which takes as long as replaying its log. The fetch waits that
 *       long.
 *   <li>Leader moved: the admin dropped the stale leader, so a retry reaches the new one. An
 *       election takes seconds, so it has a separate budget.
 *   <li>Stale: the leader covers another snapshot, or no longer holds the requested cut. The fetch
 *       fails at once, and the caller plans the bucket again or derives the masks.
 *   <li>A read that does not wait gets the leader's answer at once, in the response: the snapshot
 *       it covers instead, as a {@link StaleSnapshotException}, or the cut to read by key up to, as
 *       a {@link DeletionVectorsNotReadyException#isRebuilding() rebuilding} one. The fetch fails
 *       with either at once.
 * </ul>
 *
 * <p>Any other error fails the fetch at once.
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

    public DvMaskFetcher(DvAdmin admin, TablePath tablePath, Sleeper sleeper) {
        this.admin = admin;
        this.tablePath = tablePath;
        this.sleeper = sleeper;
    }

    /**
     * Sends the request built by {@code request} until the leader answers it with masks or the
     * retries run out. {@code request} is called once per attempt.
     */
    public GetDvSnapshotResponse fetch(Supplier<GetDvSnapshotRequest> request) throws Exception {
        long leaderBackoffMs = INITIAL_BACKOFF_MS;
        int leaderRetries = 0;
        long notReadyBackoffMs = INITIAL_BACKOFF_MS;
        long notReadyWaitedMs = 0;
        while (true) {
            GetDvSnapshotRequest req = request.get();
            long snapshotId = req.getReadableSnapshotId();
            GetDvSnapshotResponse response;
            try {
                response = admin.getDvSnapshot(tablePath, req).get();
            } catch (Exception e) {
                if (ExceptionUtils.findThrowable(e, DeletionVectorsNotReadyException.class)
                                .isPresent()
                        && notReadyWaitedMs < MAX_NOT_READY_WAIT_MS) {
                    LOG.info(
                            "Deletion vectors of bucket {} of {} not ready for snapshot {} ({}),"
                                    + " retry in {} ms.",
                            req.getBucketId(),
                            tablePath,
                            snapshotId,
                            ExceptionUtils.stripExecutionException(e).getMessage(),
                            notReadyBackoffMs);
                    sleeper.sleep(notReadyBackoffMs);
                    notReadyWaitedMs += notReadyBackoffMs;
                    notReadyBackoffMs = Math.min(notReadyBackoffMs * 2, MAX_NOT_READY_BACKOFF_MS);
                    continue;
                }
                if (ExceptionUtils.findThrowable(e, InvalidMetadataException.class).isPresent()
                        && leaderRetries < MAX_LEADER_RETRIES) {
                    leaderRetries++;
                    LOG.info(
                            "Leader of bucket {} of {} moved while fetching its masks ({}),"
                                    + " retry {}/{}.",
                            req.getBucketId(),
                            tablePath,
                            e.getMessage(),
                            leaderRetries,
                            MAX_LEADER_RETRIES);
                    sleeper.sleep(leaderBackoffMs);
                    leaderBackoffMs = Math.min(leaderBackoffMs * 2, MAX_LEADER_BACKOFF_MS);
                    continue;
                }
                throw e;
            }
            throwIfNoMasks(response, snapshotId);
            return response;
        }
    }

    /**
     * Fails with the answer of a leader that served a read that does not wait no masks: the
     * snapshot it covers instead, or the cut to read the bucket by key up to.
     */
    private static void throwIfNoMasks(GetDvSnapshotResponse response, long requested) {
        if (response.hasFoldCut()) {
            throw DeletionVectorsNotReadyException.rebuilding(
                    String.format("No masks of lake snapshot %d", requested),
                    "the leader rebuilds or replays them",
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

    /**
     * Sends {@code request}, a release of a pinned cut, without waiting for it, and sends it once
     * more if it fails, as when the leader moved. A cut that is not released is dropped once no
     * request asks for it within the idle timeout. The returned future completes with the answer to
     * the last attempt.
     */
    public CompletableFuture<GetDvSnapshotResponse> release(GetDvSnapshotRequest request) {
        CompletableFuture<GetDvSnapshotResponse> released = new CompletableFuture<>();
        send(request)
                .whenComplete(
                        (response, e) -> {
                            if (e == null) {
                                released.complete(response);
                                return;
                            }
                            LOG.debug(
                                    "Could not release the mask cut {} of bucket {} of {}; sending"
                                            + " the release again.",
                                    request.getLogEndOffset(),
                                    request.getBucketId(),
                                    tablePath,
                                    e);
                            send(new GetDvSnapshotRequest().copyFrom(request))
                                    .whenComplete(
                                            (retried, again) -> {
                                                if (again == null) {
                                                    released.complete(retried);
                                                    return;
                                                }
                                                LOG.warn(
                                                        "Could not release the mask cut {} of"
                                                                + " bucket {} of {} ({}); the"
                                                                + " leader drops it once it"
                                                                + " idles.",
                                                        request.getLogEndOffset(),
                                                        request.getBucketId(),
                                                        tablePath,
                                                        ExceptionUtils.stripExecutionException(
                                                                        again)
                                                                .toString());
                                                released.completeExceptionally(again);
                                            });
                        });
        return released;
    }

    private CompletableFuture<GetDvSnapshotResponse> send(GetDvSnapshotRequest request) {
        try {
            return admin.getDvSnapshot(tablePath, request);
        } catch (RuntimeException e) {
            CompletableFuture<GetDvSnapshotResponse> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }
    }

    /**
     * The release of {@code holds} holds of the cut {@code cut} of lake snapshot {@code snapshotId}
     * on the leader of {@code bucket}.
     */
    public static GetDvSnapshotRequest releaseRequest(
            TableBucket bucket, long snapshotId, long cut, int holds) {
        GetDvSnapshotRequest release =
                new GetDvSnapshotRequest()
                        .setTableId(bucket.getTableId())
                        .setBucketId(bucket.getBucket())
                        .setReadableSnapshotId(snapshotId)
                        .setLogEndOffset(cut)
                        .setReleaseCut(true);
        if (bucket.getPartitionId() != null) {
            release.setPartitionId(bucket.getPartitionId());
        }
        if (holds > 1) {
            release.setHolds(holds);
        }
        return release;
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
