/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * The heap budget for the log tails of all buckets on a tablet server ({@code
 * kv.deletion-vectors.tail-memory}).
 *
 * <p>Each bucket reports the estimated bytes of its tail: changes waiting for the high watermark,
 * the updates and deletes since its covered snapshot, keys waiting for lake positions, and mask
 * bitmaps. A rebuild also reports its map of each key's last version. When the total exceeds the
 * budget, the largest tails and maps spill to RocksDB in the background until it fits. Pinned cuts
 * ({@link DerivedDvPinnedCuts}) count towards the budget too, up to a share of it.
 */
@Internal
public final class DerivedDvTailBudget {

    private static final Logger LOG = LoggerFactory.getLogger(DerivedDvTailBudget.class);

    /** Share of the maximum heap the tails may take unless the option is set. */
    static final double DEFAULT_HEAP_FRACTION = 0.12;

    /** For servers and tests without a budget of their own. */
    public static final DerivedDvTailBudget DEFAULT = new DerivedDvTailBudget(defaultBytes());

    private static final ExecutorService ENFORCER =
            Executors.newSingleThreadExecutor(
                    r -> {
                        Thread t = new Thread(r, "derived-dv-tail-budget");
                        t.setDaemon(true);
                        return t;
                    });

    /** A tail, or a rebuild's map of last versions, that takes memory and can spill to disk. */
    interface Owner {
        /**
         * Moves the tail's changes or the map to disk. Returns false if already spilled or closing.
         * The spill may take effect later, at the next batch of a replay.
         */
        boolean spill();
    }

    private final long limitBytes;
    private final DerivedDvPinnedCuts pinnedCuts;
    // Per tail: {heap bytes, bytes a spill moves to disk}.
    private final Map<Owner, long[]> bytes = new ConcurrentHashMap<>();
    private final AtomicLong total = new AtomicLong();
    private final AtomicBoolean enforcing = new AtomicBoolean();
    // Set by every update over the budget. A pass works from the total it read at its start, so a
    // tail that joins or grows during the pass needs another pass.
    private final AtomicBoolean enforceRequested = new AtomicBoolean();
    private volatile long lastWarnMs;

    public DerivedDvTailBudget(long limitBytes) {
        this(limitBytes, ConfigOptions.KV_DELETION_VECTORS_PINNED_CUT_IDLE_TIMEOUT.defaultValue());
    }

    public DerivedDvTailBudget(long limitBytes, Duration pinnedCutIdleTimeout) {
        this(limitBytes, pinnedCutIdleTimeout, System::nanoTime);
    }

    @VisibleForTesting
    DerivedDvTailBudget(long limitBytes, Duration pinnedCutIdleTimeout, LongSupplier nanoClock) {
        checkArgument(
                limitBytes > 0,
                "%s must be positive, but is %s",
                ConfigOptions.KV_DELETION_VECTORS_TAIL_MEMORY.key(),
                limitBytes);
        this.limitBytes = limitBytes;
        this.pinnedCuts = new DerivedDvPinnedCuts(this, pinnedCutIdleTimeout, nanoClock);
    }

    /** The budget of {@code conf}: the option, or a share of the maximum heap. */
    public static DerivedDvTailBudget of(Configuration conf) {
        return new DerivedDvTailBudget(
                conf.getOptional(ConfigOptions.KV_DELETION_VECTORS_TAIL_MEMORY)
                        .map(MemorySize::getBytes)
                        .orElseGet(DerivedDvTailBudget::defaultBytes),
                conf.get(ConfigOptions.KV_DELETION_VECTORS_PINNED_CUT_IDLE_TIMEOUT));
    }

    static long defaultBytes() {
        return Math.max(1L, (long) (Runtime.getRuntime().maxMemory() * DEFAULT_HEAP_FRACTION));
    }

    public long limitBytes() {
        return limitBytes;
    }

    /** The cuts the union reads of the server's buckets pinned. */
    public DerivedDvPinnedCuts pinnedCuts() {
        return pinnedCuts;
    }

    /** The bytes all tails take now, as reported. */
    public long totalBytes() {
        return total.get();
    }

    /**
     * Records the bytes {@code owner}'s tail takes now; 0 leaves the budget. Over the budget, the
     * largest tails spill in the background. Never calls the owner.
     */
    void update(Owner owner, long ownerBytes, long spillableBytes) {
        long[] previous =
                ownerBytes <= 0
                        ? bytes.remove(owner)
                        : bytes.put(owner, new long[] {ownerBytes, spillableBytes});
        long now = total.addAndGet(Math.max(0, ownerBytes) - (previous == null ? 0L : previous[0]));
        if (now > limitBytes) {
            enforceAsync();
        }
    }

    /** Leaves the budget. */
    void remove(Owner owner) {
        update(owner, 0, 0);
    }

    private void enforceAsync() {
        enforceRequested.set(true);
        if (enforcing.compareAndSet(false, true)) {
            try {
                ENFORCER.execute(this::enforce);
            } catch (RejectedExecutionException e) {
                enforcing.set(false);
            }
        }
    }

    /** Spills the largest tails until the total fits; runs without holding any bucket lock. */
    private void enforce() {
        try {
            while (enforceRequested.getAndSet(false)) {
                enforceOnce();
            }
        } finally {
            enforcing.set(false);
        }
        // A request that came after the last pass saw this pass running and did not start one.
        if (enforceRequested.get()) {
            enforceAsync();
        }
    }

    private void enforceOnce() {
        Set<Owner> asked = new HashSet<>();
        long projected = total.get();
        while (projected > limitBytes) {
            // the tail whose spill frees the most
            Owner largest = null;
            long largestBytes = 0;
            for (Map.Entry<Owner, long[]> e : bytes.entrySet()) {
                if (!asked.contains(e.getKey()) && e.getValue()[1] > largestBytes) {
                    largest = e.getKey();
                    largestBytes = e.getValue()[1];
                }
            }
            if (largest == null) {
                break;
            }
            asked.add(largest);
            if (largest.spill()) {
                projected -= largestBytes;
            }
        }
        if (projected > limitBytes) {
            long now = System.currentTimeMillis();
            if (now - lastWarnMs > 60_000) {
                lastWarnMs = now;
                LOG.warn(
                        "Derived DV: the log tails and pinned cuts of this server take {}"
                                + " bytes after spilling every tail, more than {} ({}): their"
                                + " mask bitmaps grow while tiering lags.",
                        total.get(),
                        limitBytes,
                        ConfigOptions.KV_DELETION_VECTORS_TAIL_MEMORY.key());
            }
        }
    }
}
