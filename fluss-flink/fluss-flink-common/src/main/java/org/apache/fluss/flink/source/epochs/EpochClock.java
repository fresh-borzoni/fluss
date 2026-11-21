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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

/**
 * Tells the readers of a domain which epoch is open. A reader ends every epoch before the open one
 * at the current positions of its splits. Epoch numbers only grow, also across restarts, but need
 * not be consecutive: a change belongs to the epoch after the last watermark, and a read as of the
 * epoch before reads the end of the previous one.
 */
public interface EpochClock {

    /** The open epoch. */
    long current();

    /** Completes once {@link #current} may have advanced past the given epoch. */
    CompletableFuture<Void> advancedPast(long epoch);

    /** A reader emitted a change in the open epoch. */
    default void noteChange() {}

    /** Epochs of fixed length by the wall clock, see {@link EpochSettings#epochAt}. */
    static EpochClock wall(EpochSettings settings, LongSupplier clock) {
        return new Wall(settings, clock);
    }

    /**
     * The epochs that the job's coordinators open, see {@link CoordinatedEpochs}: the reader tells
     * its coordinator about the first change it emits in the open epoch, and learns of every epoch
     * the coordinators open through {@link Coordinated#opened}.
     */
    static Coordinated coordinated(LongConsumer changeInEpoch) {
        return new Coordinated(changeInEpoch);
    }

    /** See {@link #wall}. */
    final class Wall implements EpochClock {

        /** Wakes the readers of all wall-clock epochs in the JVM. */
        private static final ScheduledExecutorService TIMER =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread thread = new Thread(r, "fluss-epoch-clock");
                            thread.setDaemon(true);
                            return thread;
                        });

        private final EpochSettings settings;
        private final LongSupplier clock;

        private Wall(EpochSettings settings, LongSupplier clock) {
            this.settings = settings;
            this.clock = clock;
        }

        @Override
        public long current() {
            return settings.epochAt(clock.getAsLong());
        }

        @Override
        public CompletableFuture<Void> advancedPast(long epoch) {
            long delay = Math.max(1L, (epoch + 1) * settings.getIntervalMs() - clock.getAsLong());
            CompletableFuture<Void> next = new CompletableFuture<>();
            TIMER.schedule(() -> next.complete(null), delay, TimeUnit.MILLISECONDS);
            return next;
        }
    }

    /** See {@link #coordinated}. Used in the task thread only. */
    final class Coordinated implements EpochClock {

        private final LongConsumer changeInEpoch;

        /** The open epoch, 0 until the coordinator told it. */
        private long open;

        /** The epoch whose change the coordinator was told of, -1 for none. */
        private long told = -1;

        private CompletableFuture<Void> advanced = new CompletableFuture<>();

        private Coordinated(LongConsumer changeInEpoch) {
            this.changeInEpoch = changeInEpoch;
        }

        @Override
        public long current() {
            return open;
        }

        @Override
        public CompletableFuture<Void> advancedPast(long epoch) {
            return open > epoch ? CompletableFuture.completedFuture(null) : advanced;
        }

        @Override
        public void noteChange() {
            if (told != open) {
                told = open;
                changeInEpoch.accept(open);
            }
        }

        /** The coordinator opened the given epoch. */
        public void opened(long epoch) {
            if (epoch <= open) {
                return;
            }
            open = epoch;
            CompletableFuture<Void> done = advanced;
            advanced = new CompletableFuture<>();
            done.complete(null);
        }
    }
}
