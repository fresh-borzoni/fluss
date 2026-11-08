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

package org.apache.fluss.server.kv.dv;

import org.apache.fluss.server.kv.dv.DvOwner.HandOffOverflowException;
import org.apache.fluss.server.kv.dv.DvOwner.TailDiskException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Schedules the rebuilds and replays of one bucket, for its {@link DvOwner}, on the tablet server's
 * {@link DerivedDvRebuildLimiter}: full loads on a new leader or when a catch-up cannot apply index
 * changes on top of the covered snapshot, and tail loads when union reads need the change list
 * again ({@link DvLoad}). Keeps their backoffs and failures for the reads that wait for them. Its
 * methods run as owner steps unless their javadoc names another thread.
 */
final class DvRebuild {

    private static final Logger LOG = LoggerFactory.getLogger(DvRebuild.class);

    // How long a replay waits after a disk failure.
    private static final long REPLAY_RETRY_INITIAL_BACKOFF_MS = 1_000;
    private static final long REPLAY_RETRY_MAX_BACKOFF_MS = 30_000;
    // How long the replay after a hand-off overflow waits: doubling per overflow, from the start
    // again once a replay is installed.
    static final long OVERFLOW_RETRY_INITIAL_MS = 1_000;
    static final long OVERFLOW_RETRY_MAX_MS = 60_000;

    private final DvOwner m;

    // The tablet server's rebuild slots and threads; volatile as the caller of a background
    // recovery sets them.
    volatile DerivedDvRebuildLimiter limiter;
    // Recoveries and full loads started or queued; while any is, the state is REBUILDING and a
    // read that does not wait reads by key. This field and the ones below are the owner's.
    int pending;
    // when a replay retried after a disk failure may start, in epoch milliseconds
    long replayRetryAtMs = 0;
    private long replayBackoffMs = REPLAY_RETRY_INITIAL_BACKOFF_MS;
    private boolean replayScheduled = false;
    // background replays are numbered; a request reports the failure of the one it waited for
    private long replays = 0;
    long lastFailedReplay = -1;
    @Nullable Exception lastReplayFailure;
    // the replay in progress, for tests to see its queue; its job sets and clears it
    @Nullable volatile DvReplay replaying;
    // replays keep their map of last versions on disk from the start; a test sets it, replay
    // jobs read it
    volatile boolean spillReplayVersions = false;
    // The backoff of the replay after a hand-off overflow, and whether its timer is set. The
    // bounds are volatile as tests set them.
    volatile long overflowRetryInitialMs = OVERFLOW_RETRY_INITIAL_MS;
    volatile long overflowRetryMaxMs = OVERFLOW_RETRY_MAX_MS;
    private long overflowBackoffMs = -1;
    private boolean overflowRetryArmed;
    // replays after an overflow put off as the owner was behind, for tests
    long overflowRetriesPutOff;

    DvRebuild(DvOwner owner) {
        this.m = owner;
    }

    /**
     * Replays the change list of the covered snapshot in the background unless a replay is
     * scheduled already, and returns the replay's number.
     */
    long scheduleReplay(long snapshotId) {
        if (replayScheduled) {
            return replays;
        }
        replayScheduled = true;
        long replay = ++replays;
        m.work.replay(snapshotId, replay);
        return replay;
    }

    /** Records how replay number {@code replay} ended. */
    void replayEnded(long replay, @Nullable Exception failure) {
        replayScheduled = false;
        if (failure != null) {
            lastReplayFailure = failure;
            lastFailedReplay = replay;
        }
    }

    /** Retries the replay of a tail whose disk failed after a backoff. */
    void retryAfterDiskFailure() {
        replayRetryAtMs = System.currentTimeMillis() + replayBackoffMs;
        replayBackoffMs = Math.min(2 * replayBackoffMs, REPLAY_RETRY_MAX_BACKOFF_MS);
    }

    /** Whether the tail's disk failed less than a backoff ago. */
    boolean diskRetryPending() {
        return m.maskState.tailFailure instanceof TailDiskException
                && System.currentTimeMillis() < replayRetryAtMs;
    }

    /**
     * Whether the change list waits for a replay that backs off, after a disk failure or a hand-off
     * overflow: reads do not start it.
     */
    boolean retryPending() {
        return diskRetryPending() || overflowRetryPending();
    }

    /** Whether the change list waits for the replay after a hand-off overflow. */
    boolean overflowRetryPending() {
        return m.maskState.tailFailure instanceof HandOffOverflowException && !replayScheduled;
    }

    /**
     * Replays the change list after a hand-off overflow, after a backoff that doubles with each
     * overflow, up to a maximum.
     */
    void retryAfterOverflow() {
        long initial = overflowRetryInitialMs;
        long delay = overflowBackoffMs < 0 ? initial : overflowBackoffMs;
        overflowBackoffMs = Math.min(2 * delay, Math.max(initial, overflowRetryMaxMs));
        armOverflowRetry(delay);
    }

    /** The backoff the next overflow waits, in ms. */
    long nextOverflowBackoffMs() {
        return overflowBackoffMs < 0 ? overflowRetryInitialMs : overflowBackoffMs;
    }

    private void armOverflowRetry(long delayMs) {
        if (overflowRetryArmed) {
            return;
        }
        overflowRetryArmed = true;
        try {
            m.runtime
                    .scheduler()
                    .schedule(() -> m.post(this::overflowRetry), delayMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            // the server shuts down
            overflowRetryArmed = false;
        }
    }

    /**
     * The backoff after an overflow passed: starts the replay, unless the owner is behind, which
     * backs off again rather than replay into another overflow.
     */
    void overflowRetry() {
        overflowRetryArmed = false;
        if (m.closed || !(m.maskState.tailFailure instanceof HandOffOverflowException)) {
            return;
        }
        if (replayScheduled) {
            // a replay runs, one the overflow made install nothing: this one comes after it
            armOverflowRetry(nextOverflowBackoffMs());
            return;
        }
        if (m.drain.behind()) {
            overflowRetriesPutOff++;
            LOG.info(
                    "The deletion vectors of {} are behind; the replay of the change list"
                            + " waits {} ms more.",
                    m.logName,
                    nextOverflowBackoffMs());
            retryAfterOverflow();
            return;
        }
        scheduleReplay(m.coveredSnapshotId);
    }

    /** A replay was installed: the next disk failure or overflow backs off from the start. */
    void replayInstalled() {
        lastReplayFailure = null;
        replayBackoffMs = REPLAY_RETRY_INITIAL_BACKOFF_MS;
        overflowBackoffMs = -1;
    }

    /** The bucket moved to a newer snapshot: an earlier replay failure is moot. */
    void clearReplayFailure() {
        lastReplayFailure = null;
    }
}
