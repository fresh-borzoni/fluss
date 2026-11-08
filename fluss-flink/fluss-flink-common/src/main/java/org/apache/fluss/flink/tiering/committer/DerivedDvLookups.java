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

package org.apache.fluss.flink.tiering.committer;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.DvAdmin;
import org.apache.fluss.exception.DeletionVectorsNotReadyException;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;

import java.util.List;

/**
 * Looks up the base positions of a round's changed keys on the buckets' leaders, and retries while
 * a leader is not ready or not reachable.
 */
final class DerivedDvLookups {

    private static final Logger LOG = DerivedDvContext.LOG;

    // A leader election or a restarted server's registration takes seconds: about 40 s in all.
    @VisibleForTesting static final int MAX_LEADER_RETRIES = 12;
    private static final long LEADER_RETRY_INITIAL_BACKOFF_MS = 200;
    private static final long LEADER_RETRY_MAX_BACKOFF_MS = 5_000;
    // A leader rebuilding its deletion vectors answers "not ready" until its log replay ends,
    // minutes for a large backlog. The round waits that long.
    @VisibleForTesting static final long MAX_NOT_READY_WAIT_MS = 10 * 60_000;
    private static final long NOT_READY_INITIAL_BACKOFF_MS = 250;
    private static final long NOT_READY_MAX_BACKOFF_MS = 5_000;

    private DerivedDvLookups() {}

    /**
     * Looks up the positions of the keys in the bucket's index of the snapshot, on the bucket's
     * leader. Retries while the leader switches to the snapshot or rebuilds, also while a new
     * leader has not recovered its index yet, and with backoff while the leader moves or restarts.
     * The admin then drops the cached leader, so the retry resolves the leader and the servers
     * again. All retries are bounded; other errors, also a leader that covers another snapshot,
     * fail at once.
     */
    static GetDvSnapshotResponse lookupWithRetry(
            Admin admin,
            TablePath tablePath,
            TableBucket tb,
            long snapshotId,
            List<byte[]> keys,
            Sleeper sleeper)
            throws Exception {
        int leaderRetries = 0;
        long leaderBackoffMs = LEADER_RETRY_INITIAL_BACKOFF_MS;
        long notReadyWaitedMs = 0;
        long notReadyBackoffMs = NOT_READY_INITIAL_BACKOFF_MS;
        while (true) {
            try {
                return DvAdmin.of(admin)
                        .lookupDvPositions(
                                tablePath,
                                tb.getTableId(),
                                tb.getPartitionId(),
                                tb.getBucket(),
                                snapshotId,
                                keys)
                        .get();
            } catch (Exception e) {
                // The leader switches to the snapshot, rebuilds in the background, or has not
                // recovered its index yet after it became leader.
                if (ExceptionUtils.findThrowable(e, DeletionVectorsNotReadyException.class)
                        .isPresent()) {
                    if (notReadyWaitedMs >= MAX_NOT_READY_WAIT_MS) {
                        throw e;
                    }
                    LOG.info(
                            "The deletion vectors of bucket {} of {} are not ready for a position"
                                    + " lookup ({}); retrying in {} ms.",
                            tb,
                            tablePath,
                            ExceptionUtils.stripExecutionException(e).getMessage(),
                            notReadyBackoffMs);
                    sleeper.sleep(notReadyBackoffMs);
                    notReadyWaitedMs += notReadyBackoffMs;
                    notReadyBackoffMs = Math.min(2 * notReadyBackoffMs, NOT_READY_MAX_BACKOFF_MS);
                    continue;
                }
                if (leaderRetries < MAX_LEADER_RETRIES && isLeaderUnavailable(e)) {
                    leaderRetries++;
                    LOG.info(
                            "The leader of bucket {} of {} is unavailable for a position lookup"
                                    + " ({}); retry {}/{} in {} ms.",
                            tb,
                            tablePath,
                            ExceptionUtils.stripExecutionException(e).getMessage(),
                            leaderRetries,
                            MAX_LEADER_RETRIES,
                            leaderBackoffMs);
                    sleeper.sleep(leaderBackoffMs);
                    leaderBackoffMs = Math.min(2 * leaderBackoffMs, LEADER_RETRY_MAX_BACKOFF_MS);
                    continue;
                }
                throw e;
            }
        }
    }

    /**
     * Whether the bucket's leader is not reachable for now: it moved, is being elected, or its
     * server restarted and is not in the client's metadata yet.
     */
    private static boolean isLeaderUnavailable(Throwable e) {
        if (ExceptionUtils.findThrowable(e, RetriableException.class).isPresent()) {
            // InvalidMetadataException (not leader, leader not available, server not in the
            // metadata cache, network error) and other retriable errors such as timeouts
            return true;
        }
        // MetadataUpdater#leaderFor when no leader is elected yet; the client throws it untyped
        return ExceptionUtils.findThrowableWithMessage(e, "Leader not found").isPresent();
    }

    /** Looks up the positions of keys in a bucket's index of a snapshot; replaced in tests. */
    @FunctionalInterface
    interface PositionLookup {
        GetDvSnapshotResponse lookUp(TableBucket bucket, long snapshotId, List<byte[]> keys)
                throws Exception;
    }

    /** Sleeps between lookup retries; replaced in tests. */
    @FunctionalInterface
    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }
}
