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
import org.apache.fluss.utils.FatalExitExceptionHandler;

import java.io.InterruptedIOException;
import java.util.concurrent.Semaphore;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Bounds how many deletion vector rebuilds run at once on a tablet server. A rebuild downloads a
 * bucket's index after a leader change. That costs I/O, so a failover that moves many leaders to
 * one server rebuilds them a few at a time.
 */
@Internal
public final class DerivedDvRebuildLimiter {

    /**
     * For snapshot contexts without a tablet server's runtime, as tests have; a server's buckets
     * use {@link DerivedDvRuntime#rebuilds}.
     */
    public static final DerivedDvRebuildLimiter DEFAULT =
            new DerivedDvRebuildLimiter(
                    ConfigOptions.KV_DELETION_VECTORS_MAX_CONCURRENT_REBUILDS.defaultValue());

    private final Semaphore slots;
    private volatile DvTestHooks testHooks = DvTestHooks.NONE;
    // Runs the rebuilds of new leaders, so a leader change does not wait for them: the rebuilds'
    // share of the runtime's index-load threads ({@link DerivedDvRuntime}), or threads of this
    // limiter's own.
    private final BoundedConcurrencyExecutor executor;

    /** A limiter with threads of its own, for a snapshot context without a server's runtime. */
    public DerivedDvRebuildLimiter(int maxConcurrentRebuilds) {
        this(
                checkSlots(maxConcurrentRebuilds),
                new BoundedConcurrencyExecutor(
                        DerivedDvRuntime.idleEnding(
                                maxConcurrentRebuilds,
                                "kv-deletion-vectors-index-load",
                                FatalExitExceptionHandler.INSTANCE),
                        maxConcurrentRebuilds));
    }

    /** A limiter whose rebuilds run on {@code executor}, a share of a runtime's threads. */
    DerivedDvRebuildLimiter(int maxConcurrentRebuilds, BoundedConcurrencyExecutor executor) {
        this.slots = new Semaphore(checkSlots(maxConcurrentRebuilds), true);
        this.executor = executor;
    }

    private static int checkSlots(int maxConcurrentRebuilds) {
        checkArgument(
                maxConcurrentRebuilds > 0,
                "%s must be positive, but is %s",
                ConfigOptions.KV_DELETION_VECTORS_MAX_CONCURRENT_REBUILDS.key(),
                maxConcurrentRebuilds);
        return maxConcurrentRebuilds;
    }

    /** The executor the rebuilds run on, for the runtime's metrics and shutdown. */
    BoundedConcurrencyExecutor lane() {
        return executor;
    }

    @VisibleForTesting
    int availableSlots() {
        return slots.availablePermits();
    }

    /** Waits for a free rebuild slot. */
    void acquire() throws InterruptedException {
        slots.acquire();
    }

    void release() {
        slots.release();
    }

    /** Makes the rebuilds and switches of this server's leaders run {@code testHooks}. */
    @VisibleForTesting
    public void setTestHooks(DvTestHooks testHooks) {
        this.testHooks = testHooks;
    }

    /** Runs before a rebuild starts; see {@link DvTestHooks#beforeRebuild}. */
    void beforeRebuild() throws InterruptedIOException {
        testHooks.beforeRebuild();
    }

    /**
     * Whether this server's leaders put off their switches to newer snapshots. A held switch is
     * tried again later, so it holds no switch thread.
     */
    boolean switchesHeld() {
        return testHooks.holdsSwitches();
    }

    /** Runs {@code rebuild} in the background; it takes a slot when it starts loading. */
    void execute(Runnable rebuild) {
        executor.execute(rebuild);
    }
}
