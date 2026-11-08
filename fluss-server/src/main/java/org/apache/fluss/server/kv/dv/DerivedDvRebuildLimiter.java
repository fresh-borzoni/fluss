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
import org.apache.fluss.config.ConfigOptions;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Bounds how many derived deletion vector rebuilds run at once on a tablet server. A rebuild
 * replays a bucket's log since its covered snapshot, and after a leader change also downloads the
 * bucket's index. That costs I/O and memory for the replayed keys, so a failover that moves many
 * leaders to one server rebuilds them a few at a time.
 */
@Internal
public final class DerivedDvRebuildLimiter {

    /** For servers and tests without the option set. */
    public static final DerivedDvRebuildLimiter DEFAULT =
            new DerivedDvRebuildLimiter(
                    ConfigOptions.KV_DELETION_VECTORS_MAX_CONCURRENT_REBUILDS.defaultValue());

    private final Semaphore slots;
    // Runs the rebuilds of new leaders, so a leader change does not wait for them.
    private final ThreadPoolExecutor executor;

    public DerivedDvRebuildLimiter(int maxConcurrentRebuilds) {
        checkArgument(
                maxConcurrentRebuilds > 0,
                "%s must be positive, but is %s",
                ConfigOptions.KV_DELETION_VECTORS_MAX_CONCURRENT_REBUILDS.key(),
                maxConcurrentRebuilds);
        this.slots = new Semaphore(maxConcurrentRebuilds, true);
        AtomicInteger threads = new AtomicInteger();
        this.executor =
                new ThreadPoolExecutor(
                        maxConcurrentRebuilds,
                        maxConcurrentRebuilds,
                        60,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(),
                        r -> {
                            Thread t =
                                    new Thread(
                                            r, "derived-dv-rebuild-" + threads.incrementAndGet());
                            t.setDaemon(true);
                            return t;
                        });
        this.executor.allowCoreThreadTimeOut(true);
    }

    /** Waits for a free rebuild slot. */
    void acquire() throws InterruptedException {
        slots.acquire();
    }

    void release() {
        slots.release();
    }

    /** Runs {@code rebuild} in the background; it takes a slot when it starts replaying. */
    void execute(Runnable rebuild) {
        executor.execute(rebuild);
    }
}
