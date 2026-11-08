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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.server.kv.dv.DerivedDvManager.RecoverySource;
import org.apache.fluss.utils.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.concurrent.RejectedExecutionException;

/**
 * Whether the masks of a bucket of a table with processing-time row TTL are exact.
 *
 * <p>Row TTL removes a value in KV compaction without a change in the log. A version written at or
 * after the covered snapshot's readable offset that expires before it is tiered cannot be named by
 * the key's next write, which then looks like an insert: the masks keep both versions of the log
 * until the bucket switches to a snapshot that holds the first. Masks are exact while no such
 * version can have expired. Every version the covered snapshot does not hold was written at or
 * after the commit time of the log's first batch at or after the readable offset, the write time:
 * until the write time plus the TTL, less a margin for clocks, none can have expired. After that
 * the bucket is unsafe, and a union read reads it by key at its cut, which is exact whatever
 * expired (see {@code ttl_unsafe} of {@code GetDvSnapshotResponse}). A read that waits gets the
 * same answer: waiting brings no tiering.
 *
 * <p>The write time is read when the covered snapshot changes, on the server's IO pool, and kept
 * per bucket: from the header of a local batch, or below the local log, from remote log metadata,
 * which bounds it from below without a download ({@link RecoverySource#firstWriteTime}). Until it
 * is read, or while it cannot be, the bucket is unsafe: the guard fails closed, and a failed read
 * is retried with a backoff. A switch to a later readable offset keeps the earlier write time
 * meanwhile, also if the read fails, as the log's commit times grow with its offsets and an earlier
 * time is a safe bound; an earlier one only folds sooner.
 *
 * <p>{@link #covered} runs as an owner step; {@link #foldsByKey} on request threads.
 */
final class DvRowTtlGuard {

    private static final Logger LOG = LoggerFactory.getLogger(DvRowTtlGuard.class);

    // The most the margin takes. The TTL timestamp in a KV value and a batch's commit time are both
    // the leader's clock at the write, the commit time a little later, and the compaction filter
    // compares the TTL timestamp with the clock of the server that compacts. A version can expire
    // earlier than its batch's commit time says by the skew between leaders' clocks (commit times
    // never go back, TTL timestamps can), and the compacting server's clock can run ahead of this
    // one's: the margin covers both, plus the time from the KV write to the log append, so the
    // servers' clocks must stay within it (NTP). It is a tenth of the TTL, at least 5 s for clock
    // steps and at most 60 s, but never more than half the TTL, which keeps short TTLs useful.
    static final long MIN_MARGIN_MS = 5_000;
    static final long MAX_MARGIN_MS = 60_000;
    // a failed read of the write time is retried after 1 s, doubling to 60 s
    static final long FIRST_RETRY_MS = 1_000;
    static final long MAX_RETRY_MS = 60_000;

    // a write time not known: the guard fails closed
    private static final long UNKNOWN = Long.MIN_VALUE;

    private final DvOwner o;
    // -1: the table has no row TTL; set before the first recovery
    private volatile long ttlMs = -1;
    @Nullable private volatile Clock clock;
    // guarded by this
    private long generation;
    // The write time of the oldest version the covered snapshot does not hold, and the readable
    // offset it was read at; it bounds the versions of any snapshot readable at or after that
    // offset. Unknown until read.
    private volatile Bound bound = new Bound(Long.MAX_VALUE, UNKNOWN);
    // whether the last answer was unsafe, for the log lines of the changes
    private boolean unsafeLogged;

    DvRowTtlGuard(DvOwner o) {
        this.o = o;
    }

    /**
     * Marks the table as one with processing-time row TTL of {@code ttlMs}, compared with {@code
     * clock}; before recovery. A negative TTL leaves the guard off.
     */
    void useRowTtl(long ttlMs, @Nullable Clock clock) {
        this.clock = clock;
        this.ttlMs = ttlMs;
    }

    /**
     * Reads the write time again for the covered snapshot readable from {@code readableOffset}, in
     * the background; an owner step.
     */
    void covered(long readableOffset) {
        if (ttlMs < 0) {
            return;
        }
        long gen;
        synchronized (this) {
            Bound b = bound;
            if (b.writeTime == UNKNOWN || readableOffset < b.readableOffset) {
                // a rebuild back to an earlier offset: the kept time bounds nothing below it
                bound = new Bound(readableOffset, UNKNOWN);
            }
            gen = ++generation;
        }
        RecoverySource src = o.recoverySource;
        if (src == null) {
            return;
        }
        schedule(src, readableOffset, gen, 0);
    }

    /**
     * Reads the write time after {@code delayMs} on the server's IO pool: a batch header or remote
     * log metadata, brief. Not on the change-list share, whose checkpoints must not be late, nor on
     * the key-lookup threads, which serve masks: a registration switches every bucket at once.
     */
    private void schedule(RecoverySource src, long readableOffset, long gen, long delayMs) {
        Runnable stopped =
                () -> LOG.debug("Not reading the write time of {}: the server stops.", o.logName);
        try {
            o.runtime.executeAfter(
                    delayMs,
                    o.runtime.io(),
                    () -> read(src, readableOffset, gen, delayMs),
                    stopped);
        } catch (RejectedExecutionException e) {
            // the bucket stays unsafe
            stopped.run();
        }
    }

    private void read(RecoverySource src, long readableOffset, long gen, long delayMs) {
        synchronized (this) {
            if (o.closed || gen != generation) {
                return;
            }
        }
        long writeTime;
        try {
            long first = src.firstWriteTime(readableOffset);
            if (first == RecoverySource.NO_WRITE) {
                // no batch at or after the offset yet: every later one is written after now
                writeTime = clock.milliseconds();
            } else if (first < 0) {
                throw new IllegalStateException("The log names a negative write time " + first);
            } else {
                writeTime = first;
            }
        } catch (Exception e) {
            // the bound stays: an earlier one still valid at this offset, or unknown
            long retryMs = delayMs <= 0 ? FIRST_RETRY_MS : Math.min(2 * delayMs, MAX_RETRY_MS);
            if (delayMs <= 0) {
                LOG.warn(
                        "Cannot read the write time of the log under row TTL: union reads read the"
                                + " bucket by key unless an earlier write time still bounds it,"
                                + " and the read is retried. {} readable_offset={} retry_ms={}",
                        o.logName,
                        readableOffset,
                        retryMs,
                        e);
            } else {
                LOG.debug(
                        "Cannot read the write time of the log under row TTL again. {}"
                                + " readable_offset={} retry_ms={}",
                        o.logName,
                        readableOffset,
                        retryMs,
                        e);
            }
            schedule(src, readableOffset, gen, retryMs);
            return;
        }
        synchronized (this) {
            if (gen == generation) {
                bound = new Bound(readableOffset, writeTime);
            }
        }
    }

    /**
     * Whether a union read of the cut {@code cut} of a snapshot readable from {@code
     * snapshotStartOffset} reads the bucket by key, as row TTL may have removed a version of the
     * log the masks then miss; counts such answers. On a request thread.
     */
    boolean foldsByKey(long snapshotStartOffset, long cut) {
        long ttl = ttlMs;
        if (ttl < 0 || cut <= snapshotStartOffset || o.rebuild.limiter.ignoresRowTtlExpiry()) {
            // no row TTL, or no log to read
            return false;
        }
        Bound b = bound;
        boolean unsafe =
                b.writeTime == UNKNOWN
                        || b.readableOffset > snapshotStartOffset
                        || clock.milliseconds() >= b.writeTime + ttl - margin(ttl);
        if (unsafe) {
            o.metrics.rowTtlFold();
        }
        boolean changed;
        synchronized (this) {
            changed = unsafeLogged != unsafe;
            unsafeLogged = unsafe;
        }
        if (changed && unsafe) {
            LOG.info(
                    "Row TTL may have removed a version the lake does not hold: union reads read"
                            + " the bucket by key. {} readable_offset={} write_time={} ttl_ms={}",
                    o.logName,
                    snapshotStartOffset,
                    b.writeTime == UNKNOWN ? -1 : b.writeTime,
                    ttl);
        } else if (changed) {
            LOG.info(
                    "No version the lake does not hold can have expired under row TTL: union reads"
                            + " use masks again. {} readable_offset={} write_time={} ttl_ms={}",
                    o.logName,
                    snapshotStartOffset,
                    b.writeTime,
                    ttl);
        }
        return unsafe;
    }

    static long margin(long ttlMs) {
        return Math.min(Math.max(Math.min(ttlMs / 10, MAX_MARGIN_MS), MIN_MARGIN_MS), ttlMs / 2);
    }

    /** The write time known now, or -1. */
    @VisibleForTesting
    long writeTime() {
        Bound b = bound;
        return b.writeTime == UNKNOWN ? -1 : b.writeTime;
    }

    private static final class Bound {
        final long readableOffset;
        final long writeTime;

        Bound(long readableOffset, long writeTime) {
            this.readableOffset = readableOffset;
            this.writeTime = writeTime;
        }
    }
}
