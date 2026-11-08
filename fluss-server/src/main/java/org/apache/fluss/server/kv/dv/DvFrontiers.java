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

import javax.annotation.Nullable;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The frontiers a bucket keeps for union reads: the latest one of the covered state, and the one
 * kept from the state before across a switch or rebuild. Only the bucket's owner changes them;
 * readers take the immutable frontiers, and note on their own threads that they came.
 *
 * <ul>
 *   <li>{@link #latest}: the last masks built for the covered state, exact up to their cut. A read
 *       that waits takes them if their cut is no lower than its floor; one that does not wait takes
 *       them while lookups are pending, or for a moment after they were built.
 *   <li>{@link #kept}: the latest frontier of the state before, of the snapshot it covered. Reads
 *       that do not wait get it until the current state has a frontier of its own. It is kept only
 *       for the snapshot right before the covered one, or for the covered one itself, and only
 *       while such reads come.
 * </ul>
 *
 * <p>The latest frontier is dropped a minute after the last union read, the kept one a minute after
 * the last read that does not wait, or when the tablet server's deletion vector memory is exceeded.
 */
final class DvFrontiers {

    /** How a new state keeps the latest frontier of the one it replaces. */
    enum KeepReady {
        // the covered snapshot stays: its frontier stays exact
        SAME_SNAPSHOT,
        // the bucket switches to the snapshot registered right after the covered one
        NEXT_SNAPSHOT,
        // nothing is kept: the lake may not keep the snapshot the frontier is of
        NONE
    }

    // How long the frontiers are kept after the last read that does not wait.
    private static final long NO_WAIT_DEMAND_NANOS = TimeUnit.SECONDS.toNanos(60);

    @Nullable private volatile DvFrontier latest;
    // The frontier kept from the state before. The owner sets it; the budget's thread may take it
    // (getAndSet), and log retention reads where it starts from it, so the frontier and its start
    // never disagree.
    private final AtomicReference<DvFrontier> kept = new AtomicReference<>();
    // When reads came last. Volatile: reads the published state answers note themselves on the
    // request thread; the owner reads them to keep or drop the frontiers.
    private volatile boolean readSeen;
    private volatile long lastReadNanos;
    private volatile boolean anyReadSeen;
    private volatile long lastAnyReadNanos;

    /** Notes a read that does not wait, which keeps the frontiers for a while; thread-safe. */
    void readCame(long nowNanos) {
        readSeen = true;
        lastReadNanos = nowNanos;
        waitingReadCame(nowNanos);
    }

    /** Notes a union read that waits, which keeps the latest frontier for a while; thread-safe. */
    void waitingReadCame(long nowNanos) {
        anyReadSeen = true;
        lastAnyReadNanos = nowNanos;
    }

    /** Whether a union read came within the last minute. */
    boolean readsCameWithin(long nowNanos) {
        return anyReadSeen && nowNanos - lastAnyReadNanos < NO_WAIT_DEMAND_NANOS;
    }

    /** Whether a read that does not wait came within the last minute. */
    boolean readsCome(long nowNanos) {
        return readSeen && nowNanos - lastReadNanos < NO_WAIT_DEMAND_NANOS;
    }

    /** The latest frontier of the covered state, or null. */
    @Nullable
    DvFrontier latest() {
        return latest;
    }

    /** The frontier kept from the state before, or null. */
    @Nullable
    DvFrontier kept() {
        return kept.get();
    }

    /** Where the kept frontier reads the log from, or {@link Long#MAX_VALUE} without one. */
    long keptStartOffset() {
        DvFrontier k = kept.get();
        return k == null ? Long.MAX_VALUE : k.readableOffset();
    }

    /**
     * Makes {@code frontier}, just built for the covered state, the latest one. The covered state
     * has a frontier of its own now, so the kept one goes.
     */
    void built(DvFrontier frontier) {
        latest = frontier;
        dropKept();
    }

    /**
     * Keeps the latest frontier of the covered state, as {@code keep} allows, before a new state of
     * the bucket starts; {@code covered} is the snapshot the bucket covers until then.
     */
    void newState(KeepReady keep, long covered, long nowNanos) {
        DvFrontier current = latest;
        latest = null;
        if (keep == KeepReady.NONE) {
            dropKept();
        } else if (current != null && readsCome(nowNanos)) {
            kept.set(current);
        } else if (keep == KeepReady.NEXT_SNAPSHOT) {
            DvFrontier k = kept.get();
            if (k != null && k.snapshotId != covered) {
                // older masks would be two snapshots back, which the lake may no longer keep
                dropKept();
            }
        }
    }

    /** Drops the kept frontier once the read that would get it has a higher cut served. */
    void dropKeptBelow(long servedCut) {
        DvFrontier k = kept.get();
        if (k != null && k.cut() < servedCut) {
            kept.compareAndSet(k, null);
        }
    }

    /** Drops the kept frontier, from any thread; returns whether there was one. */
    boolean dropKept() {
        return kept.getAndSet(null) != null;
    }

    /**
     * Drops the kept frontier once no read that does not wait comes, the latest one once no union
     * read comes, and both on close.
     */
    void dropIfUnused(boolean closed, long nowNanos) {
        if (kept.get() != null && (closed || !readsCome(nowNanos))) {
            dropKept();
        }
        if (latest != null
                && (closed
                        || !anyReadSeen
                        || nowNanos - lastAnyReadNanos >= NO_WAIT_DEMAND_NANOS)) {
            latest = null;
        }
    }

    /** Estimated heap bytes of the frontiers. */
    long heapBytes() {
        DvFrontier l = latest;
        return (l == null ? 0 : l.bytes) + keptBytes();
    }

    /** Estimated heap bytes of the kept frontier, which can be dropped. */
    long keptBytes() {
        DvFrontier k = kept.get();
        return k == null ? 0 : k.bytes;
    }
}
