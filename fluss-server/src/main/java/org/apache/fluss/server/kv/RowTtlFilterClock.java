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

package org.apache.fluss.server.kv;

import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.server.kv.snapshot.RowTtlFilterState;
import org.apache.fluss.utils.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

/**
 * The time the row TTL filter of a table with deletion vectors expires values by.
 *
 * <p>A change names the version it supersedes from the key's KV value. A version written at or
 * after the covered snapshot's readable offset o that expires before it is tiered cannot be named,
 * and the masks would keep it next to the key's next version. So the filter gets min(now, B(o) +
 * ttl - 1), where B(o) is the oldest TTL time of a version at or after o, by processing or by event
 * time. {@link RowTtlEventTimes} keeps it from the TTL slot of each value before the value reaches
 * the KV, the field the filter compares, so the bound is exact and needs no clock of its own. Until
 * the bucket covers a snapshot whose versions the tracker all knows, the filter gets a time by
 * which nothing expires. While tiering stalls, versions the lake does not hold outlive their TTL;
 * they expire once a covered snapshot holds them. A row whose TTL time is already old when it is
 * written, as a backfill by event time, holds its bucket's cap low until it is tiered.
 *
 * <p>P is the highest o the filter was capped for. It only moves forward, so every version at or
 * after P was protected since it was written, and a bucket is safe while its o is at least P. A KV
 * restored from a snapshot takes P and the tracked times below the snapshot's offset X from the
 * snapshot; without them, which only a corrupt snapshot lacks, P is X, as a filter this leader did
 * not cap compacted the files below it, and the filter expires nothing until o passes X.
 *
 * <p>The filter calls {@link #getAsLong} on compaction threads, at a compaction's first entry and
 * every 1,000 entries, never on a flush; the write path calls {@link #track}; the bucket's owner
 * calls {@link #coveredAt}; request threads ask {@link #safe}.
 */
public final class RowTtlFilterClock implements LongSupplier {

    private static final Logger LOG = LoggerFactory.getLogger(RowTtlFilterClock.class);

    /** A time by which no value expires. */
    public static final long EXPIRES_NOTHING = Long.MIN_VALUE;

    // the tracker reports its heap every this many slices
    private static final int HEAP_REPORT_SLICES = 64;

    private final Clock clock;
    private final long ttlMs;

    // guarded by this
    private final RowTtlEventTimes times;
    // the readable offset the cap is for, -1 before the first
    private long capFrom = -1;
    // P: the highest offset the filter was capped for
    private long cappedFrom = 0;
    // since when the filter expires nothing; Long.MIN_VALUE while it is capped
    private long expiresNothingSince;
    private boolean warned;
    private String logName = "";
    @Nullable private LongConsumer heapListener;
    private int reportedSlices;

    public RowTtlFilterClock(Clock clock, long ttlMs) {
        this.clock = clock;
        this.ttlMs = ttlMs;
        this.times = new RowTtlEventTimes(clock, 0);
        this.expiresNothingSince = clock.milliseconds();
    }

    /** Names the bucket in the log lines. */
    public synchronized void setLogName(String logName) {
        this.logName = logName;
    }

    /** Receives the tracker's heap bytes as it grows and shrinks, for the heap budget. */
    public synchronized void setHeapListener(LongConsumer heapListener) {
        this.heapListener = heapListener;
        reportedSlices = Integer.MIN_VALUE / 2;
        reportHeap();
    }

    /**
     * The filter's time: the clock, at most the cap. Logs once when it lags the clock by more than
     * the TTL, and once when it no longer does.
     */
    @Override
    public synchronized long getAsLong() {
        long now = clock.milliseconds();
        long time = time(now);
        long lag = lag(now, time);
        if (lag > ttlMs) {
            if (!warned) {
                warned = true;
                LOG.warn(
                        "Row TTL expires nothing that expired in the last {} ms in {}: {}. Rows"
                                + " written after the readable lake snapshot stay until they are"
                                + " tiered, and a row whose TTL time is far in the past holds the"
                                + " cap until it is.",
                        lag,
                        logName,
                        describe(now));
            }
        } else if (warned) {
            warned = false;
            LOG.info("Row TTL expires rows at their TTL again in {}: {}.", logName, describe(now));
        }
        return time;
    }

    /**
     * Tracks the TTL time {@code ttlTime} of the version at {@code offset}, read from its value
     * before the value reaches the KV. A value that never expires is not tracked.
     */
    public synchronized void track(long offset, long ttlTime) {
        if (ttlTime != KvValueLayout.NEVER_EXPIRE_TIMESTAMP) {
            times.add(offset, ttlTime);
            reportHeap();
        }
    }

    /**
     * Caps the filter for the versions at or after {@code readableOffset}, the readable offset of
     * the snapshot the bucket covers now. P moves only if the tracker knows them all.
     */
    public synchronized void coveredAt(long readableOffset) {
        times.trim(readableOffset);
        capFrom = readableOffset;
        if (readableOffset >= times.from()) {
            cappedFrom = Math.max(cappedFrom, readableOffset);
        }
        reportHeap();
    }

    /**
     * Starts a KV restored from a snapshot at {@code snapshotOffset}: P and the tracked times its
     * leader saved, or without them P = {@code snapshotOffset} and nothing tracked below it. Saved
     * times whose slices do not follow each other are not used. Every KV snapshot of a table with
     * row TTL saves them, so their lack is corruption, which the caller reports.
     */
    public synchronized void restored(long snapshotOffset, @Nullable RowTtlFilterState saved) {
        if (saved == null || !saved.slicesFollowEachOther()) {
            cappedFrom = Math.max(cappedFrom, snapshotOffset);
            times.restore(snapshotOffset, Collections.emptyList());
        } else {
            cappedFrom = Math.max(cappedFrom, saved.cappedFrom());
            times.restore(saved.from(), saved.slices());
        }
        reportHeap();
    }

    /**
     * P and the tracked times below {@code snapshotOffset}, for a KV snapshot whose files are
     * fixed: every compaction that wrote one took its time by now.
     */
    public synchronized RowTtlFilterState saved(long snapshotOffset) {
        return new RowTtlFilterState(cappedFrom, times.from(), times.below(snapshotOffset));
    }

    /**
     * Whether the filter cannot have removed a version at or after {@code readableOffset}: each cap
     * since such a version was written was for an offset at or below it.
     */
    public synchronized boolean safe(long readableOffset) {
        return readableOffset >= cappedFrom;
    }

    /** P. */
    public synchronized long cappedFrom() {
        return cappedFrom;
    }

    /**
     * How far the filter's time lags the clock: 0 while the cap does not bind, and the time since
     * the filter expires nothing while it does.
     */
    public synchronized long capLagMs() {
        long now = clock.milliseconds();
        return lag(now, time(now));
    }

    @Override
    public synchronized String toString() {
        return describe(clock.milliseconds());
    }

    private long time(long now) {
        if (capFrom < 0 || capFrom < times.from()) {
            // versions at or after the readable offset that the tracker does not know
            if (expiresNothingSince == Long.MIN_VALUE) {
                expiresNothingSince = now;
            }
            return EXPIRES_NOTHING;
        }
        expiresNothingSince = Long.MIN_VALUE;
        long oldest = times.min();
        return oldest == RowTtlEventTimes.NONE
                ? now
                : Math.min(now, saturatedAdd(oldest, ttlMs - 1));
    }

    private long lag(long now, long time) {
        return time == EXPIRES_NOTHING ? now - expiresNothingSince : now - time;
    }

    private String describe(long now) {
        long oldest = times.min();
        return String.format(
                "cap_from=%d tracked_from=%d oldest=%s slices=%d capped_from=%d ttl_ms=%d now=%d",
                capFrom,
                times.from(),
                oldest == RowTtlEventTimes.NONE ? "none" : String.valueOf(oldest),
                times.size(),
                cappedFrom,
                ttlMs,
                now);
    }

    private void reportHeap() {
        LongConsumer listener = heapListener;
        int slices = times.size();
        if (listener != null
                && (Math.abs(slices - reportedSlices) >= HEAP_REPORT_SLICES
                        || (slices == 0 && reportedSlices != 0))) {
            reportedSlices = slices;
            listener.accept(times.heapBytes());
        }
    }

    private static long saturatedAdd(long a, long b) {
        long sum = a + b;
        // overflow only when both have the same sign and the sum has the other
        if (((a ^ sum) & (b ^ sum)) < 0) {
            return a < 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
        }
        return sum;
    }
}
