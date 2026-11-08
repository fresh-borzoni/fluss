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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The mailbox of a bucket's owner ({@link DvOwner}): steps are posted from any thread and run one
 * at a time, in the order posted, on the tablet server's {@code kv-deletion-vectors-bucket-state}
 * pool. A run takes at most {@link #STEPS_PER_RUN} steps before it yields its thread to other
 * buckets. Each step's wait in the mailbox and its run time are measured.
 */
final class DvOwnerMailbox {

    private final DvOwner o;

    DvOwnerMailbox(DvOwner o, Executor stateExecutor) {
        this.o = o;
        this.stateExecutor = stateExecutor;
    }

    // steps one run of the mailbox takes before it yields its thread to other buckets
    static final int STEPS_PER_RUN = 64;

    final Executor stateExecutor;
    // the steps posted and not run yet
    final ConcurrentLinkedQueue<Posted> posted = new ConcurrentLinkedQueue<>();
    final AtomicBoolean scheduled = new AtomicBoolean();
    // how long the last step waited in the mailbox; the owner is behind while it is long
    long lastMailboxDelayNanos;

    /** A step in the mailbox, and when it was posted. */
    private static final class Posted {
        final Runnable step;
        final long postedNanos;

        Posted(Runnable step, long postedNanos) {
            this.step = step;
            this.postedNanos = postedNanos;
        }
    }

    /** Posts a step; it runs after the steps posted before it. */
    void post(Runnable step) {
        posted.add(new Posted(step, System.nanoTime()));
        schedule();
    }

    void schedule() {
        if (!scheduled.compareAndSet(false, true)) {
            return;
        }
        try {
            stateExecutor.execute(this::runSteps);
        } catch (RejectedExecutionException e) {
            // the server shuts down: the steps run here, one thread at a time all the same
            runSteps();
        }
    }

    void runSteps() {
        Throwable fatal = null;
        for (int i = 0; i < STEPS_PER_RUN && fatal == null; i++) {
            Posted p = posted.poll();
            if (p == null) {
                break;
            }
            long start = System.nanoTime();
            lastMailboxDelayNanos = start - p.postedNanos;
            o.metrics.ownerMailboxDelay(lastMailboxDelayNanos);
            try {
                p.step.run();
                o.afterStep();
            } catch (Throwable t) {
                o.faults.fault(t);
                if (t instanceof VirtualMachineError) {
                    fatal = t;
                }
            }
            o.metrics.ownerStep(System.nanoTime() - start);
        }
        scheduled.set(false);
        if (!posted.isEmpty()) {
            schedule();
        }
        if (fatal != null) {
            // after the fault is handled, as on every job's thread: it reaches the pool's handler
            DvOwnerFaults.rethrowIfFatal(fatal);
        }
    }
}
