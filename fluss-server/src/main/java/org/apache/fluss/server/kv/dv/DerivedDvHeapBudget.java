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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * The heap budget: the heap the deletion vector state of all buckets on a tablet server may take
 * ({@code kv.deletion-vectors.heap-memory}).
 *
 * <p>Each bucket reports the estimated heap bytes of its state: changes waiting for the high
 * watermark, keys waiting for lake positions, mask bitmaps, and the masks it keeps for reads that
 * do not wait. Its updates and deletes are in its RocksDB and do not count. A rebuild also reports
 * its map of each key's last version. When the total exceeds the budget, the owners that free the
 * most are asked to in the background until it fits: a rebuild moves its map to RocksDB, and a
 * bucket drops the masks it keeps across a switch. Pinned cuts ({@link DerivedDvPinnedCuts}) count
 * towards the budget too, up to a share of it.
 */
@Internal
public final class DerivedDvHeapBudget {

    private static final Logger LOG = LoggerFactory.getLogger(DerivedDvHeapBudget.class);

    /** Share of the maximum heap the deletion vector state may take unless the option is set. */
    static final double DEFAULT_HEAP_FRACTION = 0.12;

    /** A bucket's state, or a rebuild's map of last versions, that takes heap. */
    interface Owner {
        /**
         * Frees what the owner can: a rebuild moves its map to disk, a bucket drops the masks it
         * keeps across a switch. Returns false if there is nothing to free or the owner is closing.
         * It may take effect later, at the next batch of a replay.
         */
        boolean free();
    }

    private final long limitBytes;
    private final DerivedDvRuntime runtime;
    private final DerivedDvPinnedCuts pinnedCuts;
    // Per owner: {heap bytes, bytes a free() frees}.
    private final Map<Owner, long[]> bytes = new ConcurrentHashMap<>();
    private final AtomicLong total = new AtomicLong();
    private final AtomicBoolean enforcing = new AtomicBoolean();
    // Set by every update over the budget. A pass works from the total it read at its start, so an
    // owner that joins or grows during the pass needs another pass.
    private final AtomicBoolean enforceRequested = new AtomicBoolean();
    private volatile long lastWarnMs;

    /** A budget whose pinned cuts and freeing run on the tablet server's {@code runtime}. */
    public DerivedDvHeapBudget(long limitBytes, DerivedDvRuntime runtime) {
        this(
                limitBytes,
                ConfigOptions.KV_DELETION_VECTORS_PINNED_CUT_IDLE_TIMEOUT.defaultValue(),
                runtime);
    }

    public DerivedDvHeapBudget(
            long limitBytes, Duration pinnedCutIdleTimeout, DerivedDvRuntime runtime) {
        this(limitBytes, pinnedCutIdleTimeout, System::nanoTime, runtime);
    }

    @VisibleForTesting
    DerivedDvHeapBudget(
            long limitBytes,
            Duration pinnedCutIdleTimeout,
            LongSupplier nanoClock,
            DerivedDvRuntime runtime) {
        checkArgument(
                limitBytes > 0,
                "%s must be positive, but is %s",
                ConfigOptions.KV_DELETION_VECTORS_HEAP_MEMORY.key(),
                limitBytes);
        this.limitBytes = limitBytes;
        this.runtime = runtime;
        this.pinnedCuts =
                new DerivedDvPinnedCuts(this, pinnedCutIdleTimeout, nanoClock, runtime.scheduler());
    }

    /** The budget of {@code conf}: the option, or a share of the maximum heap. */
    public static DerivedDvHeapBudget of(Configuration conf, DerivedDvRuntime runtime) {
        return new DerivedDvHeapBudget(
                conf.getOptional(ConfigOptions.KV_DELETION_VECTORS_HEAP_MEMORY)
                        .map(MemorySize::getBytes)
                        .orElseGet(DerivedDvHeapBudget::defaultBytes),
                conf.get(ConfigOptions.KV_DELETION_VECTORS_PINNED_CUT_IDLE_TIMEOUT),
                runtime);
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

    /** The bytes all owners take now, as reported. */
    public long totalBytes() {
        return total.get();
    }

    /**
     * Records the bytes {@code owner} takes now, of which {@link Owner#free} frees {@code
     * freeableBytes}; 0 leaves the budget. Over the budget, the owners that free the most free in
     * the background. Never calls the owner.
     */
    void update(Owner owner, long ownerBytes, long freeableBytes) {
        long[] previous =
                ownerBytes <= 0
                        ? bytes.remove(owner)
                        : bytes.put(owner, new long[] {ownerBytes, freeableBytes});
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
                runtime.scheduler().execute(this::enforce);
            } catch (RejectedExecutionException e) {
                // the server shuts down
                enforcing.set(false);
                LOG.warn(
                        "No freeing over the deletion vector heap budget was scheduled ({}).",
                        e.toString());
            }
        }
    }

    /** Asks the owners that free the most to free until the total fits; it waits for no step. */
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
            // the owner that frees the most
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
            if (largest.free()) {
                projected -= largestBytes;
            }
        }
        if (projected > limitBytes) {
            long now = System.currentTimeMillis();
            if (now - lastWarnMs > 60_000) {
                lastWarnMs = now;
                LOG.warn(
                        "The deletion vector state and pinned cuts of this server take {}"
                                + " bytes after freeing what can be freed, more than {} ({}): their mask"
                                + " bitmaps grow while tiering lags.",
                        total.get(),
                        limitBytes,
                        ConfigOptions.KV_DELETION_VECTORS_HEAP_MEMORY.key());
            }
        }
    }
}
