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
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
 * <p>Cuts count towards the server's deletion vector memory ({@link DerivedDvHeapBudget}), so the
 * other state makes room sooner, and take at most half of it. Beyond that the least recently used
 * cuts are dropped, but never the one pinned last, so a read can always start. A request for a
 * dropped cut fails as stale: a part that shares its bucket's cut with other parts then derives its
 * masks from the keys, and one that reads its whole bucket pins a new cut.
 *
 * <p>A pin may name the request that took it. A retry of that request gets the same cut without
 * another pin, so a retry never leaves a pin that no read releases. A cut remembers its latest pin
 * requests only, which count towards its memory.
 */
@Internal
@ThreadSafe
public final class DerivedDvPinnedCuts implements DerivedDvHeapBudget.Owner {

    private static final Logger LOG = LoggerFactory.getLogger(DerivedDvPinnedCuts.class);

    /** The share of the server's deletion vector memory pinned cuts may take. */
    static final double MEMORY_SHARE = 0.5;

    /**
     * The pin requests a cut remembers, the latest ones, so a retry of one gets the cut again: a
     * retry comes soon after its request, while a cut that stays held may be pinned by many reads.
     */
    static final int MAX_PIN_REQUESTS_PER_CUT = 1_024;

    // the heap of a remembered pin request: its map entry, its key and its id in the cut's list
    static final long PIN_REQUEST_BYTES = 112;

    private static final long MIN_SWEEP_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    private static final long MAX_SWEEP_NANOS = TimeUnit.MINUTES.toNanos(1);

    private final DerivedDvHeapBudget budget;
    private final long limitBytes;
    private final long idleNanos;
    private final LongSupplier nanoClock;
    // the server's thread that drops idle cuts; scheduled only while cuts are held
    private final ScheduledExecutorService expirer;

    // in access order: the least recently used first
    @GuardedBy("this")
    private final LinkedHashMap<Key, Entry> cuts = new LinkedHashMap<>(16, 0.75f, true);

    // the cut each remembered pin request took, so a retry of it gets that cut again
    @GuardedBy("this")
    private final Map<PinRequest, Entry> pinRequests = new HashMap<>();

    // the lowest snapshot start offset of each owner's cuts, read without the lock by log retention
    private final Map<DerivedDvManager, Long> minStartOffsets = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private long bytes;

    @GuardedBy("this")
    private boolean sweepScheduled;

    @GuardedBy("this")
    private long lastWarnNanos = Long.MIN_VALUE;

    DerivedDvPinnedCuts(
            DerivedDvHeapBudget budget,
            Duration idleTimeout,
            LongSupplier nanoClock,
            ScheduledExecutorService expirer) {
        checkArgument(
                !idleTimeout.isNegative() && !idleTimeout.isZero(),
                "%s must be positive, but is %s",
                ConfigOptions.KV_DELETION_VECTORS_PINNED_CUT_IDLE_TIMEOUT.key(),
                idleTimeout);
        this.budget = budget;
        this.limitBytes = Math.max(1L, (long) (budget.limitBytes() * MEMORY_SHARE));
        this.idleNanos = idleTimeout.toNanos();
        this.nanoClock = nanoClock;
        this.expirer = expirer;
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
     * Returns the cut of snapshot {@code snapshotId} that pin request {@code pinRequestId} of
     * {@code owner} took, if it is still held, as a retry of that request gets it; null otherwise.
     * A request id that took a cut of another snapshot is no retry of a pin of this one.
     */
    @Nullable
    synchronized DvPinnedCut pinnedBy(DerivedDvManager owner, long pinRequestId, long snapshotId) {
        Entry entry = pinRequests.get(new PinRequest(owner, pinRequestId));
        return entry == null || entry.key.snapshotId != snapshotId
                ? null
                : get(owner, snapshotId, entry.key.cut);
    }

    /**
     * Holds the masks of a union read of {@code owner}'s snapshot {@code snapshotId} as a cut. Pins
     * at the same high watermark share one cut, so a cut held for it already is returned. Each pin
     * takes {@code holds} holds, which count until they are released ({@link #release}). A pin that
     * names a request ({@code pinRequestId}) that took a cut already gets that cut, without another
     * pin.
     */
    DvPinnedCut pin(
            DerivedDvManager owner,
            long snapshotId,
            DvSnapshot masks,
            DerivedDvMetrics metrics,
            @Nullable Long pinRequestId,
            int holds)
            throws IOException {
        DvPinnedCut held = holdAgain(owner, snapshotId, masks, pinRequestId, holds);
        if (held != null) {
            return held;
        }
        // outside the lock: it reads the whole log mask
        DvPinnedCut frozen = DvPinnedCut.freeze(snapshotId, masks);
        synchronized (this) {
            if (owner.isClosed()) {
                // its cuts were dropped, and one added now would never be dropped
                throw DerivedDvManager.closedError();
            }
            DvPinnedCut retried = retriedPin(owner, pinRequestId, snapshotId);
            if (retried != null) {
                return retried;
            }
            Key key = new Key(owner, snapshotId, frozen.logEndOffset());
            long now = nanoClock.getAsLong();
            Entry entry = cuts.get(key);
            if (entry != null && entry.cut.sameSnapshotAs(masks)) {
                entry.lastAccessNanos = now;
                entry.pins += holds;
                recordPinRequest(entry, pinRequestId);
                evictBeyondLimit();
                report();
                return entry.cut;
            }
            if (entry != null) {
                // the snapshot's id names another commit now
                remove(key, Removal.DROPPED);
            }
            Entry added = new Entry(key, frozen, metrics, now, holds);
            cuts.put(key, added);
            bytes += frozen.bytes();
            metrics.pinnedCutAdded(frozen.bytes());
            recordPinRequest(added, pinRequestId);
            evictBeyondLimit();
            updateMinStartOffset(owner);
            report();
            scheduleSweep();
            return frozen;
        }
    }

    /**
     * Counts another pin of the cut held for {@code masks}, if one is, and returns it; returns the
     * cut a retried request took already without counting a pin; null otherwise.
     */
    @Nullable
    private synchronized DvPinnedCut holdAgain(
            DerivedDvManager owner,
            long snapshotId,
            DvSnapshot masks,
            @Nullable Long pinRequestId,
            int holds) {
        DvPinnedCut retried = retriedPin(owner, pinRequestId, snapshotId);
        if (retried != null) {
            return retried;
        }
        DvPinnedCut held = get(owner, snapshotId, masks.getLogEndOffset());
        if (held == null || !held.sameSnapshotAs(masks)) {
            return null;
        }
        Key key = new Key(owner, snapshotId, masks.getLogEndOffset());
        Entry entry = cuts.get(key);
        entry.pins += holds;
        recordPinRequest(entry, pinRequestId);
        evictBeyondLimit();
        report();
        return held;
    }

    @GuardedBy("this")
    @Nullable
    private DvPinnedCut retriedPin(
            DerivedDvManager owner, @Nullable Long pinRequestId, long snapshotId) {
        return pinRequestId == null ? null : pinnedBy(owner, pinRequestId, snapshotId);
    }

    /**
     * Remembers that pin request {@code pinRequestId} took the cut of {@code entry}, so a retry of
     * it gets the cut again. The cut remembers its latest {@link #MAX_PIN_REQUESTS_PER_CUT}
     * requests; a retry of an older one pins the cut again, with holds that its idle timeout ends.
     */
    @GuardedBy("this")
    private void recordPinRequest(Entry entry, @Nullable Long pinRequestId) {
        if (pinRequestId == null) {
            return;
        }
        Entry previous = pinRequests.put(new PinRequest(entry.key.owner, pinRequestId), entry);
        if (previous == entry) {
            return;
        }
        if (previous != null) {
            // the request took a cut of another snapshot before
            previous.pinRequestIds.remove(pinRequestId);
            requestBytesChanged(previous, -PIN_REQUEST_BYTES);
        }
        entry.pinRequestIds.addLast(pinRequestId);
        requestBytesChanged(entry, PIN_REQUEST_BYTES);
        while (entry.pinRequestIds.size() > MAX_PIN_REQUESTS_PER_CUT) {
            pinRequests.remove(
                    new PinRequest(entry.key.owner, entry.pinRequestIds.pollFirst()), entry);
            requestBytesChanged(entry, -PIN_REQUEST_BYTES);
        }
    }

    @GuardedBy("this")
    private void requestBytesChanged(Entry entry, long delta) {
        bytes += delta;
        entry.metrics.pinnedCutBytesChanged(delta);
    }

    /**
     * The lowest snapshot start offset of {@code owner}'s cuts, where the oldest log a cut's reads
     * read starts; {@link Long#MAX_VALUE} without cuts. Takes no lock.
     */
    long minStartOffset(DerivedDvManager owner) {
        return minStartOffsets.getOrDefault(owner, Long.MAX_VALUE);
    }

    @GuardedBy("this")
    private void updateMinStartOffset(DerivedDvManager owner) {
        long min = Long.MAX_VALUE;
        for (Map.Entry<Key, Entry> e : cuts.entrySet()) {
            if (e.getKey().owner == owner) {
                min = Math.min(min, e.getValue().cut.snapshotStartOffset());
            }
        }
        if (min == Long.MAX_VALUE) {
            minStartOffsets.remove(owner);
        } else {
            minStartOffsets.put(owner, min);
        }
    }

    /**
     * Releases {@code holds} holds of {@code owner}'s cut of snapshot {@code snapshotId} at {@code
     * cut}, as the union read that pinned it, or some of its parts, end, and drops the cut once all
     * its holds are released. Returns whether the cut was dropped. A cut not held is ignored.
     */
    synchronized boolean release(DerivedDvManager owner, long snapshotId, long cut, int holds) {
        Key key = new Key(owner, snapshotId, cut);
        Entry entry = cuts.get(key);
        if (entry == null) {
            return false;
        }
        if ((entry.pins -= holds) > 0) {
            // a use of the cut: the read's other parts still need it
            entry.lastAccessNanos = nanoClock.getAsLong();
            return false;
        }
        remove(key, Removal.RELEASED);
        report();
        return true;
    }

    /** The cuts {@code owner} holds. */
    @VisibleForTesting
    synchronized int count(DerivedDvManager owner) {
        int n = 0;
        for (Key key : cuts.keySet()) {
            if (key.owner == owner) {
                n++;
            }
        }
        return n;
    }

    /** Drops the cuts of {@code owner}, e.g. as its bucket's leader closes. */
    synchronized void dropAll(DerivedDvManager owner) {
        Iterator<Map.Entry<Key, Entry>> it = cuts.entrySet().iterator();
        boolean dropped = false;
        while (it.hasNext()) {
            Map.Entry<Key, Entry> e = it.next();
            if (e.getKey().owner == owner) {
                it.remove();
                released(e.getKey(), e.getValue(), Removal.DROPPED);
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
            Map.Entry<Key, Entry> e = it.next();
            if (now - e.getValue().lastAccessNanos < idleNanos) {
                break; // the rest were used later
            }
            it.remove();
            released(e.getKey(), e.getValue(), Removal.EXPIRED);
            expired = true;
        }
        if (expired) {
            report();
        }
    }

    /** The pin requests whose cut a retry of them gets again. */
    @VisibleForTesting
    synchronized int pinRequestCount() {
        return pinRequests.size();
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

    /** Cuts are not freed for the budget; they are dropped beyond their own limit instead. */
    @Override
    public boolean free() {
        return false;
    }

    @GuardedBy("this")
    private void evictBeyondLimit() {
        int evicted = 0;
        long evictedBytes = 0;
        Iterator<Map.Entry<Key, Entry>> it = cuts.entrySet().iterator();
        // never the newest, the last in access order
        while (bytes > limitBytes && cuts.size() > 1) {
            Map.Entry<Key, Entry> e = it.next();
            Entry entry = e.getValue();
            it.remove();
            released(e.getKey(), entry, Removal.EVICTED);
            evicted++;
            evictedBytes += entry.bytes();
        }
        if (evicted > 0) {
            long now = nanoClock.getAsLong();
            if (lastWarnNanos == Long.MIN_VALUE || now - lastWarnNanos > MAX_SWEEP_NANOS) {
                lastWarnNanos = now;
                LOG.warn(
                        "Dropped {} pinned cuts of union reads ({} bytes) as the pinned cuts"
                                + " of this server take more than {} bytes, half of {}; reads that still"
                                + " need them derive their masks from the keys or pin a new cut.",
                        evicted,
                        evictedBytes,
                        limitBytes,
                        ConfigOptions.KV_DELETION_VECTORS_HEAP_MEMORY.key());
            }
        }
    }

    @GuardedBy("this")
    private void remove(Key key, Removal removal) {
        Entry entry = cuts.remove(key);
        if (entry != null) {
            released(key, entry, removal);
        }
    }

    @GuardedBy("this")
    private void released(Key key, Entry entry, Removal removal) {
        for (long pinRequestId : entry.pinRequestIds) {
            // a request remembered here may have taken a cut of another snapshot since
            pinRequests.remove(new PinRequest(key.owner, pinRequestId), entry);
        }
        updateMinStartOffset(key.owner);
        bytes -= entry.bytes();
        entry.metrics.pinnedCutRemoved(entry.bytes());
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
            expirer.schedule(this::sweep, delay, TimeUnit.NANOSECONDS);
            sweepScheduled = true;
        } catch (RejectedExecutionException e) {
            // idle cuts expire on their next request instead
            LOG.warn(
                    "No sweep of idle pinned deletion vector cuts was scheduled ({}).",
                    e.toString());
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
        DROPPED,
        // every pin was released, as the reads that pinned it ended
        RELEASED
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

    /** A pin request of a bucket leader, by the manager's identity. */
    private static final class PinRequest {
        final DerivedDvManager owner;
        final long id;

        PinRequest(DerivedDvManager owner, long id) {
            this.owner = owner;
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof PinRequest)) {
                return false;
            }
            PinRequest r = (PinRequest) o;
            return owner == r.owner && id == r.id;
        }

        @Override
        public int hashCode() {
            return 31 * System.identityHashCode(owner) + Long.hashCode(id);
        }
    }

    private static final class Entry {
        final Key key;
        final DvPinnedCut cut;
        // the latest requests that pinned the cut and named themselves, oldest first
        final ArrayDeque<Long> pinRequestIds = new ArrayDeque<>(1);
        // the pinning bucket's, which all report to the server's metrics
        final DerivedDvMetrics metrics;
        long lastAccessNanos;
        // the holds not released yet; one that is never released keeps the cut until it idles
        int pins;

        Entry(Key key, DvPinnedCut cut, DerivedDvMetrics metrics, long lastAccessNanos, int pins) {
            this.key = key;
            this.cut = cut;
            this.metrics = metrics;
            this.lastAccessNanos = lastAccessNanos;
            this.pins = pins;
        }

        /** The heap of the cut and of the pin requests it remembers. */
        long bytes() {
            return cut.bytes() + pinRequestIds.size() * PIN_REQUEST_BYTES;
        }
    }
}
