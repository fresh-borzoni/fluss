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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

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
 * <p>The masks are exact while o is at least P, the highest o the filter was capped for. A bucket
 * whose o moved back, a rebuild to an older snapshot, or whose KV was restored from a snapshot
 * without P below the snapshot's offset, is unsafe: a union read reads it by key at its cut, which
 * is exact whatever expired (see {@code ttl_unsafe} of {@code GetDvSnapshotResponse}).
 *
 * <p>{@link #covered} runs as an owner step; {@link #foldsByKey} on request threads.
 */
final class DvRowTtlGuard {

    private static final Logger LOG = LoggerFactory.getLogger(DvRowTtlGuard.class);

    private final DvOwner o;
    // -1: the table has no row TTL; set before the first recovery
    private volatile long ttlMs = -1;
    // the time of the bucket's TTL filter; null without row TTL
    @Nullable private volatile RowTtlFilterClock filterClock;
    // whether the last answer was unsafe, for the log lines of the changes; guarded by this
    private boolean unsafeLogged;

    DvRowTtlGuard(DvOwner o) {
        this.o = o;
    }

    /**
     * Marks the table as one with row TTL of {@code ttlMs}, whose KV filter takes its time from
     * {@code filterClock}; before recovery. A negative TTL leaves the guard off.
     */
    void useRowTtl(long ttlMs, @Nullable RowTtlFilterClock filterClock) {
        checkArgument(ttlMs < 0 || filterClock != null, "Row TTL needs the filter's clock.");
        this.filterClock = filterClock;
        this.ttlMs = ttlMs;
    }

    @Nullable
    RowTtlFilterClock filterClock() {
        return filterClock;
    }

    /**
     * Caps the filter for the covered snapshot readable from {@code readableOffset}; an owner step.
     */
    void covered(long readableOffset) {
        if (ttlMs >= 0) {
            filterClock.coveredAt(readableOffset);
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
