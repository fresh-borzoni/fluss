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

import org.apache.fluss.server.kv.RowTtlFilterClock;
import org.apache.fluss.server.kv.snapshot.RowTtlFilterState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * The cap of the row TTL filter of a bucket of a table with row TTL, and whether its masks are
 * exact ({@link RowTtlFilterClock}).
 *
 * <p>Row TTL removes a value in KV compaction without a change in the log. A version written at or
 * after the covered snapshot's readable offset o that expires before it is tiered cannot be named
 * by the key's next write, and the masks would keep both versions. So each time the bucket covers a
 * snapshot, the filter is capped below the TTL of the oldest version at or after its o.
 *
 * <p>The masks are exact while o is at least P, the highest o the filter was capped for. Every KV
 * snapshot saves P and the filter's tracked times, and a restored KV takes them over. On Iceberg o
 * only moves forward, and a rollback past the registered snapshot stops tiering, so o is always at
 * least P. Saved state that is missing or out of order, or a cover below P, means the state is
 * corrupt: the leader logs it at ERROR and counts it, and a bucket covered below P drops its change
 * list as a rebuild whose replay failed does. It serves no masks, union reads read it by key, and
 * its tiering rounds wait; no replay serves masks again until it covers a snapshot at or after P.
 *
 * <p>On Paimon the readable snapshot is the latest compacted one, so o can move back below P. A
 * union read of such a bucket reads it by key at its cut, which is exact whatever expired, until o
 * passes P again.
 *
 * <p>{@link #restored} runs before the bucket's recovery, {@link #covered} as an owner step, the
 * others on any thread.
 */
final class DvRowTtlGuard {

    private static final Logger LOG = LoggerFactory.getLogger(DvRowTtlGuard.class);

    private final DvOwner o;
    // -1: the table has no row TTL; set before the first recovery
    private volatile long ttlMs = -1;
    // the time of the bucket's TTL filter; null without row TTL
    @Nullable private volatile RowTtlFilterClock filterClock;
    // the lake's readable offset can move back below P (Paimon); else it only moves forward
    private volatile boolean readableOffsetMovesBack;
    // whether the last answer was unsafe, for the log lines of the changes; guarded by this
    private boolean unsafeLogged;

    DvRowTtlGuard(DvOwner o) {
        this.o = o;
    }

    /**
     * Marks the table as one with row TTL of {@code ttlMs}, whose KV filter takes its time from
     * {@code filterClock}, on a lake whose readable offset can move back or not; before recovery. A
     * negative TTL leaves the guard off.
     */
    void useRowTtl(
            long ttlMs, @Nullable RowTtlFilterClock filterClock, boolean readableOffsetMovesBack) {
        checkArgument(ttlMs < 0 || filterClock != null, "Row TTL needs the filter's clock.");
        this.filterClock = filterClock;
        this.readableOffsetMovesBack = readableOffsetMovesBack;
        this.ttlMs = ttlMs;
    }

    @Nullable
    RowTtlFilterClock filterClock() {
        return filterClock;
    }

    /**
     * Starts the filter of a KV restored from a snapshot at {@code snapshotOffset} from what the
     * snapshot saved of it ({@link RowTtlFilterClock#restored}). Saved state that is missing or
     * whose slices do not follow each other is corrupt: logged and counted, and P is the snapshot's
     * offset. Before the bucket's recovery.
     */
    void restored(long snapshotOffset, @Nullable RowTtlFilterState saved) {
        RowTtlFilterClock fc = filterClock;
        if (ttlMs < 0 || fc == null) {
            return;
        }
        if (saved == null || !saved.slicesFollowEachOther()) {
            o.metrics.rowTtlCorrupt();
            LOG.error(
                    "The KV snapshot of {} at offset {} saved {} of its row TTL filter: the"
                            + " bucket's TTL state is corrupt. The filter expires nothing below"
                            + " offset {}, and union reads use masks only of lake snapshots"
                            + " readable from there.",
                    o.logName,
                    snapshotOffset,
                    saved == null ? "no state" : "slices out of order, " + saved + ",",
                    snapshotOffset);
        }
        fc.restored(snapshotOffset, saved);
    }

    /**
     * Caps the filter for the covered snapshot readable from {@code readableOffset}; an owner step.
     * Returns the failure the bucket's change list is dropped with if its TTL state is corrupt
     * ({@link #corruption}), logged and counted; null otherwise.
     */
    @Nullable
    IOException covered(long readableOffset) {
        RowTtlFilterClock fc = filterClock;
        if (ttlMs < 0 || fc == null) {
            return null;
        }
        fc.coveredAt(readableOffset);
        String corruption = corruption(readableOffset);
        if (corruption == null) {
            return null;
        }
        o.metrics.rowTtlCorrupt();
        LOG.error(
                "{} The bucket serves no masks: union reads read it by key, and its tiering"
                        + " rounds wait. filter={}",
                corruption,
                fc);
        return new IOException(corruption);
    }

    /**
     * Why the masks of the bucket covered from {@code readableOffset} cannot be exact, on a lake
     * whose readable offset only moves forward: the offset is below P, so the bucket's saved TTL
     * state is corrupt, and a version after it may have expired before it was tiered. Null if they
     * can, and always on a lake whose readable offset can move back. Any thread.
     */
    @Nullable
    String corruption(long readableOffset) {
        RowTtlFilterClock fc = filterClock;
        if (ttlMs < 0 || fc == null || readableOffsetMovesBack || fc.safe(readableOffset)) {
            return null;
        }
        return String.format(
                "The deletion vectors of %s cover a lake snapshot readable from offset %d, below"
                        + " offset %d their row TTL filter was capped for: the bucket's TTL state"
                        + " is corrupt, and a version after the readable offset may have expired"
                        + " before it was tiered.",
                o.logName, readableOffset, fc.cappedFrom());
    }

    /**
     * Whether a union read of the cut {@code cut} of a snapshot readable from {@code
     * snapshotStartOffset} reads the bucket by key, on a lake whose readable offset can move back:
     * row TTL may have removed a version of the log the masks then miss. Counts such answers. On a
     * request thread.
     */
    boolean foldsByKey(long snapshotStartOffset, long cut) {
        long ttl = ttlMs;
        if (ttl < 0
                || !readableOffsetMovesBack
                || cut <= snapshotStartOffset
                || o.rebuild.limiter.ignoresRowTtlExpiry()) {
            // no row TTL, a readable offset that only moves forward, or no log to read
            return false;
        }
        RowTtlFilterClock fc = filterClock;
        boolean unsafe = !fc.safe(snapshotStartOffset);
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
                            + " the bucket by key. {} readable_offset={} filter={}",
                    o.logName,
                    snapshotStartOffset,
                    fc);
        } else if (changed) {
            LOG.info(
                    "No version the lake does not hold can have expired under row TTL: union reads"
                            + " use masks again. {} readable_offset={} filter={}",
                    o.logName,
                    snapshotStartOffset,
                    fc);
        }
        return unsafe;
    }
}
