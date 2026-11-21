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

package org.apache.fluss.flink.source.epochs;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;

/**
 * The epochs of a domain as the coordinators of a job decide them, when epochs end after their
 * changes rather than by the wall clock (see {@link EpochSettings#getCloseAfterMs}).
 *
 * <p>Every source coordinator (split enumerator) of a job runs in the JobManager, so the
 * enumerators of all scans of a domain share one {@link Domain}. A reader that emits the first
 * change of the open epoch tells its enumerator; the domain closes the epoch {@code closeAfterMs}
 * after it opened and opens the next one, whose number is the time it opened in milliseconds; and
 * every enumerator tells its readers, which end every epoch before the open one at the positions
 * they reached. The ends are recorded as before, so a restore replays them whatever opens epochs.
 */
public final class CoordinatedEpochs {

    private static final Map<String, Domain> DOMAINS = new HashMap<>();

    private static final ScheduledExecutorService TIMER =
            Executors.newSingleThreadScheduledExecutor(
                    r -> {
                        Thread thread = new Thread(r, "fluss-epoch-coordinator");
                        thread.setDaemon(true);
                        return thread;
                    });

    private CoordinatedEpochs() {}

    /** Joins the domain; the listener receives every epoch it opens from now on. */
    public static synchronized Domain join(
            String domain, long closeAfterMs, LongConsumer listener) {
        Domain shared = DOMAINS.computeIfAbsent(domain, d -> new Domain(closeAfterMs));
        shared.listeners.add(listener);
        return shared;
    }

    public static synchronized void leave(String domain, Domain shared, LongConsumer listener) {
        shared.listeners.remove(listener);
        if (shared.listeners.isEmpty()) {
            DOMAINS.remove(domain, shared);
        }
    }

    /** The open epoch of a domain, closed soon after a change in it. */
    public static final class Domain {
        private final long closeAfterMs;
        private final Set<LongConsumer> listeners = new CopyOnWriteArraySet<>();
        private long open = System.currentTimeMillis();
        private long openedAt = open;
        private boolean closing;

        private Domain(long closeAfterMs) {
            this.closeAfterMs = closeAfterMs;
        }

        public synchronized long open() {
            return open;
        }

        /** A reader emitted a change: the open epoch ends {@code closeAfterMs} after it opened. */
        public void noteChange() {
            long delay;
            synchronized (this) {
                if (closing) {
                    return;
                }
                closing = true;
                delay = Math.max(0L, openedAt + closeAfterMs - System.currentTimeMillis());
            }
            TIMER.schedule(this::close, delay, TimeUnit.MILLISECONDS);
        }

        private void close() {
            long opened;
            synchronized (this) {
                long now = System.currentTimeMillis();
                open = Math.max(now, open + 1);
                openedAt = now;
                closing = false;
                opened = open;
            }
            for (LongConsumer listener : listeners) {
                listener.accept(opened);
            }
        }
    }
}
