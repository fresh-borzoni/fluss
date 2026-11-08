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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * The hand-off of a bucket's key changes from its write path to its owner. The write path appends a
 * batch to the log, then hands its changes over here; the owner drains them in order.
 *
 * <p>There is one producer, as the KV lock serializes a bucket's writes, and one consumer, the
 * owner. A batch goes into the queue before {@link #queuedEnd} moves past it, and the owner reads
 * {@link #queuedEnd} before it polls, so every batch below the end it read is drained by the time
 * it is done: masks never go past the changes drained.
 *
 * <p>The queue is bounded. Past a threshold of queued changes the write path waits for the owner,
 * but at most a fixed bound ({@link #awaitDrain}). If the owner has not drained by then, the
 * hand-off overflows: the write returns, the hand-off drops every batch from then on ({@link
 * #dropping}), and the owner, at its next step, releases the batches queued and drops the change
 * list. The batches are in the log, so the replay that follows reads them. The hand-off takes
 * batches again once that replay enters its final pass ({@link #resume}).
 */
final class DvHandOff {

    private static final Logger LOG = LoggerFactory.getLogger(DvHandOff.class);

    /** The changes of one appended batch, or a failure to hand them over. */
    static final class Batch {
        final List<DvEntry> entries;
        // the log end after the batch, or -1 if not known
        final long logEnd;
        // the write path failed to hand the batch's changes over
        @Nullable final Throwable failure;

        private Batch(List<DvEntry> entries, long logEnd, @Nullable Throwable failure) {
            this.entries = entries;
            this.logEnd = logEnd;
            this.failure = failure;
        }
    }

    private final ConcurrentLinkedQueue<Batch> queue = new ConcurrentLinkedQueue<>();
    // the changes queued, for the write path's backstop and the heap budget
    private final AtomicLong queuedEntries = new AtomicLong();
    private final AtomicLong queuedBytes = new AtomicLong();
    // The log end after the last batch handed over, -1 if none: reads take their floor from it.
    // It moves on while batches are dropped, as they are in the log.
    private volatile long appendedEnd = -1;
    // The log end after the last batch queued: every batch below it is in the queue or drained,
    // so a drain returns it. Dropped batches do not move it.
    private volatile long queuedEnd = -1;
    // a drain is posted to the owner and has not started yet
    private final AtomicBoolean drainPosted = new AtomicBoolean();
    // the write path waits for a drain
    private volatile boolean writerWaiting;
    // Set by the write path when the hand-off overflows: batches are dropped until the owner's
    // replay takes them again. Volatile: the write path sets and reads it, the owner clears it.
    private volatile boolean dropping;
    // the hand-off overflowed and the owner has not acted on it yet
    private final AtomicBoolean overflowed = new AtomicBoolean();
    // Failures to hand a batch over, counted by the write path before it returns, and those the
    // owner has taken from the queue. Until a published state has taken every failure counted, a
    // read is not answered from it on the request thread: its frontier may be below a write
    // whose hand-off failed.
    private final AtomicLong failures = new AtomicLong();
    private volatile long failuresTaken;
    private final Runnable postDrain;

    DvHandOff(Runnable postDrain) {
        this.postDrain = postDrain;
    }

    /**
     * Queues the changes of an appended batch, which moved the log end to {@code logEnd}. Returns
     * false if the batch was dropped, as the hand-off overflowed: the replay reads it from the log.
     */
    boolean add(List<DvEntry> entries, long logEnd) {
        if (dropping) {
            if (logEnd > appendedEnd) {
                appendedEnd = logEnd;
            }
            return entries.isEmpty();
        }
        if (!entries.isEmpty()) {
            long bytes = 0;
            for (DvEntry e : entries) {
                bytes += DvChangeList.queuedBytes(e.getKey());
            }
            queue.add(new Batch(entries, logEnd, null));
            queuedEntries.addAndGet(entries.size());
            queuedBytes.addAndGet(bytes);
        }
        if (logEnd > queuedEnd) {
            queuedEnd = logEnd;
        }
        if (logEnd > appendedEnd) {
            appendedEnd = logEnd;
        }
        signal();
        return true;
    }

    /** Queues a failure to hand a batch over: the owner drops the change list. */
    void fail(Throwable cause) {
        failures.incrementAndGet();
        queue.add(new Batch(Collections.emptyList(), -1, cause));
        signal();
    }

    private void signal() {
        if (drainPosted.compareAndSet(false, true)) {
            postDrain.run();
        }
    }

    /**
     * Drains the queue into {@code sink}, an owner step. Returns the log end below which every
     * batch is drained now, or -1 if none was ever queued.
     */
    long drain(Consumer<Batch> sink) {
        drainPosted.set(false);
        // read before polling: every batch below it is queued by now
        long end = queuedEnd;
        Batch b;
        while ((b = queue.poll()) != null) {
            if (b.failure != null) {
                failuresTaken++;
            }
            queuedEntries.addAndGet(-b.entries.size());
            long bytes = 0;
            for (DvEntry e : b.entries) {
                bytes += DvChangeList.queuedBytes(e.getKey());
            }
            queuedBytes.addAndGet(-bytes);
            sink.accept(b);
        }
        if (writerWaiting) {
            synchronized (this) {
                notifyAll();
            }
        }
        return end;
    }

    /**
     * Waits until the owner drained the queue to {@code limit} changes or fewer, on the write path,
     * at most {@code waitMs}: writes slow down rather than the heap grow. It never fails the write.
     * A wait of more than a second is logged and counted. If the owner did not drain by then, the
     * hand-off overflows ({@link #overflow}) and the write returns. Returns whether it overflowed.
     */
    boolean awaitDrain(
            long limit,
            long waitMs,
            BooleanSupplier closed,
            DerivedDvMetrics metrics,
            String logName) {
        long start = System.currentTimeMillis();
        boolean warned = false;
        boolean interrupted = false;
        boolean overflow = false;
        synchronized (this) {
            writerWaiting = true;
            try {
                while (queuedEntries.get() > limit && !closed.getAsBoolean()) {
                    long left = start + waitMs - System.currentTimeMillis();
                    if (left <= 0) {
                        overflow = true;
                        break;
                    }
                    try {
                        wait(Math.min(100, left));
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                    if (!warned && System.currentTimeMillis() - start > 1_000) {
                        warned = true;
                        metrics.handOffStalled();
                        LOG.warn(
                                "The deletion vectors of {} have more than {} changes handed over and"
                                        + " not taken yet; writes wait for them.",
                                logName,
                                limit);
                    }
                }
            } finally {
                writerWaiting = false;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        if (overflow) {
            overflow(logName);
        }
        return overflow;
    }

    /**
     * Overflows the hand-off, on the write path: batches are dropped from now on, and the owner is
     * signalled to release the ones queued and to drop the change list. Logged once per episode.
     */
    private void overflow(String logName) {
        if (dropping) {
            return;
        }
        dropping = true;
        overflowed.set(true);
        LOG.warn(
                "The deletion vectors of {} did not take {} queued changes within the"
                        + " hand-off wait; the change list is dropped and replayed from the log,"
                        + " and changes are not handed over until then.",
                logName,
                queuedEntries.get());
        signal();
    }

    /** Whether the hand-off overflowed since the owner last asked; an owner step. */
    boolean takeOverflow() {
        return overflowed.getAndSet(false);
    }

    /**
     * Releases the batches queued, as the hand-off overflowed; an owner step. Returns how many
     * batches of changes were released. A failure queued among them goes too: the change list is
     * dropped all the same.
     */
    int release() {
        int released = 0;
        Batch b;
        while ((b = queue.poll()) != null) {
            if (b.failure != null) {
                failuresTaken++;
            }
            queuedEntries.addAndGet(-b.entries.size());
            long bytes = 0;
            for (DvEntry e : b.entries) {
                bytes += DvChangeList.queuedBytes(e.getKey());
            }
            queuedBytes.addAndGet(-bytes);
            if (b.failure == null) {
                released++;
            }
        }
        return released;
    }

    /** The failures to hand a batch over the write path counted. */
    long failures() {
        return failures.get();
    }

    /** The failures the owner took from the queue; an owner step reads it to publish it. */
    long failuresTaken() {
        return failuresTaken;
    }

    /** Whether batches are dropped, as the hand-off overflowed. */
    boolean dropping() {
        return dropping;
    }

    /**
     * Takes batches again, as a replay enters its final pass; an owner step. A batch dropped before
     * is in the log before the replay's final read, so the replay has it.
     */
    void resume() {
        dropping = false;
    }

    /** The log end after the last batch handed over, dropped or not, or -1. */
    long appendedEnd() {
        return appendedEnd;
    }

    /** The changes queued and not drained yet. */
    long queuedEntries() {
        return queuedEntries.get();
    }

    /** Estimated heap bytes of the changes queued and not drained yet. */
    long queuedBytes() {
        return queuedBytes.get();
    }
}
