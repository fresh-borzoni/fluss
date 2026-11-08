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

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The owner's side of the write path's hand-off ({@link DvHandOff}): the write path hands a batch
 * over and waits, bounded, past the threshold; the owner drains the batches into the change list,
 * and acts on an overflow ({@link DvHandOff}). Its methods run as owner steps unless their javadoc
 * names another thread.
 */
final class DvOwnerHandOff {

    private final DvOwner o;

    DvOwnerHandOff(DvOwner o) {
        this.o = o;
    }

    static final int APPLY_THRESHOLD = 100_000;
    // Past APPLY_THRESHOLD queued changes a write waits at most this long for the owner; then the
    // hand-off overflows and the change list is replayed. Tests make them small.
    static final long HAND_OFF_WAIT_MS = 2_000;
    // the owner is behind, and starts no replay after an overflow, while its steps wait this long
    static final long BEHIND_NANOS = TimeUnit.MILLISECONDS.toNanos(500);
    // volatile: the write path reads them, and tests set them
    volatile long handOffThreshold = APPLY_THRESHOLD;
    volatile long handOffWaitMs = HAND_OFF_WAIT_MS;
    volatile long behindNanos = BEHIND_NANOS;

    /** Hands a batch's changes over to the owner, on the write path. */
    void handOver(List<DvEntry> entries, long logEnd) {
        if (o.closed) {
            return; // the RocksDB may be closed; a new leader replays the log
        }
        if (!o.handOff.add(entries, logEnd)) {
            // the hand-off overflowed: the replay reads the batch from the log
            o.metrics.handOffsDropped(1);
            return;
        }
        if (o.handOff.dropping()) {
            // an overflow episode: no write waits again until the replay takes batches again
            return;
        }
        long threshold = handOffThreshold;
        if (o.handOff.queuedEntries() > threshold) {
            // the owner falls behind: writes slow down rather than the heap grow, for a while
            o.handOff.awaitDrain(threshold, handOffWaitMs, () -> o.closed, o.metrics, o.logName);
        }
    }

    /**
     * The hand-off overflowed: the batches queued are released, and the change list is missing
     * until a replay, which starts after a backoff once the owner is not behind.
     */
    void handOffOverflowed() {
        int released = o.handOff.release();
        o.metrics.handOffsDropped(released);
        o.faults.changeListLost(
                new DvOwner.HandOffOverflowException(
                        String.format(
                                "The deletion vectors of %s fell behind the writes; %d batches were"
                                        + " dropped and the change list is replayed from the log.",
                                o.logName, released)));
        o.rebuild.retryAfterOverflow();
    }

    /** Whether the owner is behind: its steps wait long, or the hand-off fills up. */
    boolean behind() {
        return o.steps.lastMailboxDelayNanos > behindNanos
                || o.handOff.queuedEntries() > handOffThreshold / 2;
    }

    void postDrain() {
        o.post(this::drainHandOff);
    }

    /**
     * Takes the changes the write path handed over into the change list. While the change list is
     * missing they are dropped: the replay reads them from the log, as they were appended before
     * they were handed over. A replay in its final pass also gets them, and installs those from
     * where it ended ({@link DvReplay}).
     */
    void drainHandOff() {
        o.testHooks.onDrain();
        if (o.handOff.takeOverflow() && !o.closed) {
            // the batches queued are released, not drained into the change list
            handOffOverflowed();
        }
        long end =
                o.handOff.drain(
                        batch -> {
                            if (o.closed) {
                                return;
                            }
                            if (batch.failure != null) {
                                o.faults.changeListLost(batch.failure);
                                return;
                            }
                            DvReplay pass = o.finalPass;
                            if (pass != null) {
                                // the replay in its final pass installs them, from where it ends
                                pass.buffer(batch.entries);
                            }
                            // while the change list is missing, its replay reads them from the
                            // log; before the takeover, the recovery replaces the change list
                            if (!(o.recoverySource != null && o.maskState.tailMissing())) {
                                o.maskState.tail.enqueueAll(batch.entries);
                            }
                        });
        // Taken again after the drain reset its signal and read its end: an overflow while the
        // drain ran is handled now rather than wait for a drain nobody posted, and a batch it
        // dropped is above the end the drain read.
        if (o.handOff.takeOverflow() && !o.closed) {
            handOffOverflowed();
        }
        if (o.closed) {
            return;
        }
        if (end >= 0) {
            o.handedOffEnd = Math.max(o.handedOffEnd, end);
        }
        if (!o.maskState.tail.queue.isEmpty()) {
            if (o.maskState.tail.queue.size() > APPLY_THRESHOLD) {
                o.maskState.applyUpTo(o.highWatermark.getAsLong());
            }
            o.maskState.reportTail(false);
            o.maskState.scheduleResolve();
        }
    }

    /** The write path failed to hand a batch over: the change list goes Missing in a step. */
    void handOffFailed(Throwable cause) {
        if (!o.closed) {
            o.handOff.fail(cause);
        }
    }
}
