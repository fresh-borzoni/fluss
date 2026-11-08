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
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.exception.DeletionVectorsNotReadyException;
import org.apache.fluss.exception.InvalidMetadataException;
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
 * leader catches up or rebuilds, and follows a leader that moved.
 *
 * <ul>
 *   <li>Not ready: the bucket catches up to the snapshot or rebuilds its deletion vectors in the
 *       background, which takes as long as replaying its log. The fetch waits that long.
 *   <li>Leader moved: the admin dropped the stale leader, so a retry reaches the new one. An
 *       election takes seconds, so it has a separate, longer budget.
 *   <li>Stale: a bucket still catching up (snapshot -1, or the requested one) is asked again a few
 *       times. One that covers another snapshot fails the fetch at once, and the caller reads that
 *       snapshot instead. A request for a cut the leader dropped fails at once too: asking again
 *       does not bring the cut back.
 * </ul>
 */
@Internal
public class DvMaskFetcher {

    private static final Logger LOG = LoggerFactory.getLogger(DvMaskFetcher.class);

    // A bucket catches up to a newly registered snapshot within about a second. A longer budget
    // would turn a bucket that cannot catch up into a hang.
    static final int MAX_STALE_RETRIES = 3;
    static final long INITIAL_BACKOFF_MS = 300;
    static final long MAX_BACKOFF_MS = 800;
    // A leader election after a tablet-server failure takes seconds; this allows about 20s.
    static final int MAX_LEADER_RETRIES = 8;
    static final long MAX_LEADER_BACKOFF_MS = 4000;
    static final long MAX_NOT_READY_WAIT_MS = 120_000;
    static final long MAX_NOT_READY_BACKOFF_MS = 2000;

    /** Waits between attempts; tests replace it. */
    public interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    private final Admin admin;
    private final TablePath tablePath;
    private final Sleeper sleeper;

    public DvMaskFetcher(Admin admin, TablePath tablePath, Sleeper sleeper) {
        this.admin = admin;
        this.tablePath = tablePath;
        this.sleeper = sleeper;
    }

    /**
     * Sends the request built by {@code request} until the leader answers it or the retries run
     * out. Fails at once with the {@link StaleSnapshotException} of a bucket that covers another
     * snapshot than the requested one.
     */
    public GetDvSnapshotResponse fetch(Supplier<GetDvSnapshotRequest> request) throws Exception {
        long backoffMs = INITIAL_BACKOFF_MS;
        long leaderBackoffMs = INITIAL_BACKOFF_MS;
        int leaderRetries = 0;
        long notReadyBackoffMs = INITIAL_BACKOFF_MS;
        long notReadyWaitedMs = 0;
        Exception last = null;
        for (int attempt = 0; attempt < MAX_STALE_RETRIES; ) {
            GetDvSnapshotRequest req = request.get();
            long snapshotId = req.getReadableSnapshotId();
            try {
                return admin.getDvSnapshot(tablePath, req).get();
            } catch (Exception e) {
                last = e;
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
                attempt++;
                if (!isTransientlyUnavailable(e)
                        || staleOnAnotherSnapshot(e, snapshotId) != null
                        || (req.hasLogEndOffset() && isStale(e))) {
                    throw e;
                }
                LOG.info(
                        "Masks of bucket {} of {} not ready for snapshot {}, retry {}/{}.",
                        req.getBucketId(),
                        tablePath,
                        snapshotId,
                        attempt,
                        MAX_STALE_RETRIES);
                sleeper.sleep(backoffMs);
                backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
            }
        }
        throw last;
    }

    /**
     * Returns the {@link StaleSnapshotException} of a leader that covers another snapshot than
     * {@code requested}, on which a read can be planned. Returns null otherwise, also for a leader
     * still catching up, which reports -1 or the requested snapshot.
     */
    @Nullable
    public static StaleSnapshotException staleOnAnotherSnapshot(Throwable e, long requested) {
        StaleSnapshotException stale =
                ExceptionUtils.findThrowable(e, StaleSnapshotException.class).orElse(null);
        return stale != null
                        && stale.getCurrentSnapshotId() >= 0
                        && stale.getCurrentSnapshotId() != requested
                ? stale
                : null;
    }

    private static boolean isStale(Throwable e) {
        return ExceptionUtils.findThrowable(e, StaleSnapshotException.class).isPresent()
                || ExceptionUtils.stringifyException(e).contains("Stale snapshot");
    }

    /**
     * Returns whether the exception means the bucket does not cover the requested snapshot yet, or
     * not any more. Handles both a direct {@link StaleSnapshotException} and the wrapped form the
     * RPC delivers, whose message contains the server's DV-fetch or stale-snapshot text.
     */
    @VisibleForTesting
    public static boolean isTransientlyUnavailable(Throwable e) {
        if (ExceptionUtils.findThrowable(e, StaleSnapshotException.class).isPresent()) {
            return true;
        }
        Throwable t = e;
        while (t != null) {
            String msg = t.getMessage();
            if (msg != null
                    && (msg.contains("Stale snapshot")
                            || msg.contains("Failed to get DV snapshot"))) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }
}
