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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * The pinned cuts ({@link DvPinnedCut}) of union reads of a tablet server's buckets. A cut is held
 * from the request that pins it until no request asks for it for {@code
 * kv.deletion-vectors.pinned-cut-idle-timeout}. A bucket's cuts are dropped when its leader closes.
 *
 * <p>Cuts count towards the server's deletion vector memory ({@link DerivedDvTailBudget}), so the
 * log tails spill sooner, and take at most half of it. Beyond that the least recently used cuts are
 * dropped, but never the one pinned last, so a read can always start. A request for a dropped cut
 * fails as stale.
 */
@Internal
@ThreadSafe
public final class DerivedDvPinnedCuts implements DerivedDvTailBudget.Owner {

    private static final Logger LOG = LoggerFactory.getLogger(DerivedDvPinnedCuts.class);

    /** The share of the server's deletion vector memory pinned cuts may take. */
    static final double MEMORY_SHARE = 0.5;

    private static final long MIN_SWEEP_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    private static final long MAX_SWEEP_NANOS = TimeUnit.MINUTES.toNanos(1);

    // Drops idle cuts; scheduled only while cuts are held.
    private static final ScheduledExecutorService EXPIRER =
            Executors.newSingleThreadScheduledExecutor(
                    r -> {
                        Thread t = new Thread(r, "derived-dv-pinned-cuts");
                        t.setDaemon(true);
                        return t;
                    });

    private final DerivedDvTailBudget budget;
    private final long limitBytes;
    private final long idleNanos;
    private final LongSupplier nanoClock;

    // in access order: the least recently used first
    @GuardedBy("this")
    private final LinkedHashMap<Key, Entry> cuts = new LinkedHashMap<>(16, 0.75f, true);

    @GuardedBy("this")
    private long bytes;

    @GuardedBy("this")
    private boolean sweepScheduled;

    @GuardedBy("this")
    private long lastWarnNanos = Long.MIN_VALUE;

    DerivedDvPinnedCuts(DerivedDvTailBudget budget, Duration idleTimeout, LongSupplier nanoClock) {
        checkArgument(
                !idleTimeout.isNegative() && !idleTimeout.isZero(),
                "%s must be positive, but is %s",
                ConfigOptions.KV_DELETION_VECTORS_PINNED_CUT_IDLE_TIMEOUT.key(),
                idleTimeout);
        this.budget = budget;
        this.limitBytes = Math.max(1L, (long) (budget.limitBytes() * MEMORY_SHARE));
        this.idleNanos = idleTimeout.toNanos();
        this.nanoClock = nanoClock;
    }

    /** Returns and refreshes the cut of {@code owner}'s snapshot {@code snapshotId}, or null. */
    @Nullable
    synchronized DvPinnedCut get(DerivedDvManager owner, long snapshotId, long cut) {
        Key key = new Key(owner, snapshotId, cut);
        Entry entry = cuts.get(key);
        if (entry == null) {
            return null;
        }
        long now = nanoClock.getAsLong();
        if (now - entry.lastAccessNanos >= idleNanos) {
            remove(key, Removal.EXPIRED);
            report();
            return null;
        }
        entry.lastAccessNanos = now;
        return entry.cut;
    }

    /**
     * Holds the masks of a union read of {@code owner}'s snapshot {@code snapshotId} as a cut. Pins
     * at the same high watermark share one cut, so a cut held for it already is returned.
     */
    DvPinnedCut pin(
            DerivedDvManager owner, long snapshotId, DvSnapshot masks, DerivedDvMetrics metrics)
            throws IOException {
        DvPinnedCut held = get(owner, snapshotId, masks.getLogEndOffset());
        if (held != null && held.sameSnapshotAs(masks)) {
            return held;
        }
        // outside the lock: it reads the whole log mask
        DvPinnedCut frozen = DvPinnedCut.freeze(snapshotId, masks);
        synchronized (this) {
            if (owner.isClosed()) {
                // its cuts were dropped, and one added now would never be dropped
                throw new IOException("Derived DV manager is closed");
            }
            Key key = new Key(owner, snapshotId, frozen.logEndOffset());
            long now = nanoClock.getAsLong();
            Entry entry = cuts.get(key);
            if (entry != null && entry.cut.sameSnapshotAs(masks)) {
                entry.lastAccessNanos = now;
                return entry.cut;
            }
            if (entry != null) {
                // the snapshot's id names another commit now
                remove(key, Removal.DROPPED);
            }
            cuts.put(key, new Entry(frozen, metrics, now));
            bytes += frozen.bytes();
            metrics.pinnedCutAdded(frozen.bytes());
            evictBeyondLimit();
            report();
            scheduleSweep();
            return frozen;
        }
    }

    /** Drops the cuts of {@code owner}, e.g. as its bucket's leader closes. */
    synchronized void dropAll(DerivedDvManager owner) {
        Iterator<Map.Entry<Key, Entry>> it = cuts.entrySet().iterator();
        boolean dropped = false;
        while (it.hasNext()) {
            Map.Entry<Key, Entry> e = it.next();
            if (e.getKey().owner == owner) {
                it.remove();
                released(e.getValue(), Removal.DROPPED);
                dropped = true;
            }
        }
        if (dropped) {
            report();
        }
    }

    /** Drops the cuts no request asked for within the idle timeout. */
    @VisibleForTesting
    synchronized void expireIdle() {
        long now = nanoClock.getAsLong();
        Iterator<Map.Entry<Key, Entry>> it = cuts.entrySet().iterator();
        boolean expired = false;
        while (it.hasNext()) {
            Entry entry = it.next().getValue();
            if (now - entry.lastAccessNanos < idleNanos) {
                break; // the rest were used later
            }
            it.remove();
            released(entry, Removal.EXPIRED);
            expired = true;
        }
        if (expired) {
            report();
        }
    }

    /** The number of cuts held. */
    public synchronized int size() {
        return cuts.size();
    }

    /** The estimated heap bytes of the cuts held. */
    public synchronized long bytes() {
        return bytes;
    }

    /** The bytes beyond which the least recently used cuts are dropped. */
    public long limitBytes() {
        return limitBytes;
    }

    /** Cuts are not moved to disk; the tails spill to make room for them instead. */
    @Override
    public boolean spill() {
        return false;
    }

    @GuardedBy("this")
    private void evictBeyondLimit() {
        int evicted = 0;
        long evictedBytes = 0;
        Iterator<Map.Entry<Key, Entry>> it = cuts.entrySet().iterator();
        // never the newest, the last in access order
        while (bytes > limitBytes && cuts.size() > 1) {
            Entry entry = it.next().getValue();
            it.remove();
            released(entry, Removal.EVICTED);
            evicted++;
            evictedBytes += entry.cut.bytes();
        }
        if (evicted > 0) {
            long now = nanoClock.getAsLong();
            if (lastWarnNanos == Long.MIN_VALUE || now - lastWarnNanos > MAX_SWEEP_NANOS) {
                lastWarnNanos = now;
                LOG.warn(
                        "Derived DV: dropped {} pinned cuts of union reads ({} bytes) as the"
                                + " pinned cuts of this server take more than {} bytes, half of"
                                + " {}; reads that still need them fail and plan again.",
                        evicted,
                        evictedBytes,
                        limitBytes,
                        ConfigOptions.KV_DELETION_VECTORS_TAIL_MEMORY.key());
            }
        }
    }

    @GuardedBy("this")
    private void remove(Key key, Removal removal) {
        Entry entry = cuts.remove(key);
        if (entry != null) {
            released(entry, removal);
        }
    }

    @GuardedBy("this")
    private void released(Entry entry, Removal removal) {
        bytes -= entry.cut.bytes();
        entry.metrics.pinnedCutRemoved(entry.cut.bytes());
        if (removal == Removal.EVICTED) {
            entry.metrics.pinnedCutEvicted();
        } else if (removal == Removal.EXPIRED) {
            entry.metrics.pinnedCutExpired();
        }
    }

    @GuardedBy("this")
    private void report() {
        // never calls back
        budget.update(this, bytes, 0);
    }

    @GuardedBy("this")
    private void scheduleSweep() {
        if (sweepScheduled || cuts.isEmpty()) {
            return;
        }
        long delay = Math.max(MIN_SWEEP_NANOS, Math.min(MAX_SWEEP_NANOS, idleNanos / 4));
        try {
            EXPIRER.schedule(this::sweep, delay, TimeUnit.NANOSECONDS);
            sweepScheduled = true;
        } catch (RejectedExecutionException e) {
            // idle cuts expire on their next request instead
            LOG.warn("Derived DV: no sweep of idle pinned cuts scheduled ({}).", e.toString());
        }
    }

    private synchronized void sweep() {
        sweepScheduled = false;
        expireIdle();
        scheduleSweep();
    }

    private enum Removal {
        EVICTED,
        EXPIRED,
        DROPPED
    }

    /** A bucket leader's cut: compared by the manager's identity, not its state. */
    private static final class Key {
        final DerivedDvManager owner;
        final long snapshotId;
        final long cut;

        Key(DerivedDvManager owner, long snapshotId, long cut) {
            this.owner = owner;
            this.snapshotId = snapshotId;
            this.cut = cut;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Key)) {
                return false;
            }
            Key k = (Key) o;
            return owner == k.owner && snapshotId == k.snapshotId && cut == k.cut;
        }

        @Override
        public int hashCode() {
            int h = System.identityHashCode(owner);
            h = 31 * h + Long.hashCode(snapshotId);
            return 31 * h + Long.hashCode(cut);
        }
    }

    private static final class Entry {
        final DvPinnedCut cut;
        // the pinning bucket's, which all report to the server's metrics
        final DerivedDvMetrics metrics;
        long lastAccessNanos;

        Entry(DvPinnedCut cut, DerivedDvMetrics metrics, long lastAccessNanos) {
            this.cut = cut;
            this.metrics = metrics;
            this.lastAccessNanos = lastAccessNanos;
        }
    }
}
