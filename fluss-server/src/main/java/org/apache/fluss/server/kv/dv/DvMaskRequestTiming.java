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

import org.apache.fluss.exception.DeletionVectorsNotReadyException;
import org.apache.fluss.exception.StaleSnapshotException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * The time a union read's mask request spends in its steps on the bucket leader, for the server's
 * histograms and its log line. Used by the request's thread only.
 *
 * <p>A request that took longer than {@link #INFO_NANOS} is logged at INFO, as reads then waited
 * for the bucket. Shorter requests that were answered below the high watermark, as behind, or to
 * read by key, are logged at DEBUG.
 */
final class DvMaskRequestTiming {

    private static final Logger LOG = LoggerFactory.getLogger(DvMaskRequestTiming.class);

    // a request that takes longer on the server is logged at INFO
    private static final long INFO_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    // a shorter one is logged at DEBUG if it takes longer than this, or ends unusually
    private static final long DEBUG_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

    static final long UNKNOWN = Long.MIN_VALUE;
    // what a request waits for: the bucket to switch to the requested snapshot, the masks a switch
    // or rebuild derives again, a replay of the change list
    static final int MOVE = 0;
    static final int DERIVE = 1;
    static final int REPLAY = 2;

    final long startNanos = System.nanoTime();
    // all: the masks in one response; pin: pinning a cut; cut: building a cut not held again
    final String kind;
    final long requestedSnapshotId;
    long coveredBefore = UNKNOWN;
    // the offset the masks go up to and the high watermark then, -1 if none were built
    long cut = -1;
    long highWatermark = -1;
    // how a read that does not wait got masks below the high watermark: partial (applied up to a
    // change that needs a lookup), ready (the last ready masks), lake (the snapshot alone),
    // previous (the masks kept from the snapshot before); null otherwise
    @Nullable String frontier;
    // how long ago the ready masks were built
    long cutAgeNanos;
    // why a read that does not wait was told to read by key, or null
    @Nullable String rebuildCause;
    boolean cached;
    long waitMoveNanos;
    long waitDeriveNanos;
    long waitReplayNanos;
    long applyNanos;
    long resolveWaitNanos;
    long buildNanos;
    long pinNanos;

    DvMaskRequestTiming(String kind, long requestedSnapshotId) {
        this.kind = kind;
        this.requestedSnapshotId = requestedSnapshotId;
    }

    void waited(int waitingFor, long nanos) {
        if (waitingFor == MOVE) {
            waitMoveNanos += nanos;
        } else if (waitingFor == DERIVE) {
            waitDeriveNanos += nanos;
        } else {
            waitReplayNanos += nanos;
        }
    }

    /**
     * Records the request in the server's histograms, and logs it for bucket {@code logName}, with
     * the snapshot it covers now.
     */
    void done(
            DerivedDvMetrics metrics,
            String logName,
            LongSupplier coveredSnapshotId,
            @Nullable Throwable failure) {
        long total = System.nanoTime() - startNanos;
        metrics.maskRequest(
                TimeUnit.NANOSECONDS.toMillis(total),
                TimeUnit.NANOSECONDS.toMillis(waitMoveNanos),
                TimeUnit.NANOSECONDS.toMillis(waitDeriveNanos),
                TimeUnit.NANOSECONDS.toMillis(waitReplayNanos),
                TimeUnit.NANOSECONDS.toMillis(applyNanos),
                TimeUnit.NANOSECONDS.toMillis(resolveWaitNanos),
                TimeUnit.NANOSECONDS.toMillis(buildNanos));
        boolean behind =
                failure instanceof StaleSnapshotException
                        && ((StaleSnapshotException) failure).isBehind();
        boolean rebuilding =
                failure instanceof DeletionVectorsNotReadyException && rebuildCause != null;
        // masks below the high watermark: the write path had not handed over the changes above
        long cutLag = failure == null && cut >= 0 ? highWatermark - cut : 0;
        boolean info = total > INFO_NANOS;
        if (!info
                && (!LOG.isDebugEnabled()
                        || (total <= DEBUG_NANOS && !behind && !rebuilding && cutLag <= 0))) {
            return;
        }
        String result;
        if (failure == null) {
            if (cutLag <= 0) {
                result = "ok";
            } else {
                result = frontier != null ? "ok_frontier" : "ok_below_hw";
            }
        } else if (behind) {
            result = "behind";
        } else if (rebuilding) {
            result = "rebuilding";
        } else if (failure instanceof StaleSnapshotException) {
            result = "stale";
        } else if (failure instanceof DeletionVectorsNotReadyException) {
            result = "not_ready";
        } else {
            result = "error";
        }
        String format =
                "Served the deletion vector masks of {}: requested_snapshot={}"
                        + " covered_snapshot={} wait_switch_ms={} apply_ms={} resolve_wait_ms={}"
                        + " build_ms={} total_ms={} kind={} result={} covered_before={} cut={}"
                        + " hw={} cut_lag={} frontier={} cut_age_ms={} wait_derive_ms={}"
                        + " wait_replay_ms={} pin_ms={} cached={} cause={}";
        Object[] args = {
            logName,
            requestedSnapshotId,
            coveredSnapshotId.getAsLong(),
            ms(waitMoveNanos),
            ms(applyNanos),
            ms(resolveWaitNanos),
            ms(buildNanos),
            ms(total),
            kind,
            result,
            coveredBefore == UNKNOWN ? "-" : coveredBefore,
            cut,
            highWatermark,
            cutLag,
            frontier == null ? "-" : frontier,
            ms(cutAgeNanos),
            ms(waitDeriveNanos),
            ms(waitReplayNanos),
            ms(pinNanos),
            cached,
            rebuildCause == null ? "-" : rebuildCause
        };
        if (info) {
            LOG.info(format, args);
        } else {
            LOG.debug(format, args);
        }
    }

    /** Nanoseconds as milliseconds with one decimal, for the log lines. */
    static String ms(long nanos) {
        return String.format(Locale.ROOT, "%.1f", nanos / 1_000_000.0);
    }
}
