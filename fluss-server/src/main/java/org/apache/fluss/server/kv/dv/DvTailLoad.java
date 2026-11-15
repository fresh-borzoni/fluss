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

import org.apache.fluss.server.kv.dv.DerivedDvManager.RecoverySource;
import org.apache.fluss.server.kv.dv.DvFrontiers.KeepReady;
import org.apache.fluss.server.kv.dv.DvOwner.TailDiskException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.fluss.server.kv.dv.DvMaskRequestTiming.ms;
import static org.apache.fluss.server.kv.dv.DvOwner.isDiskFailure;

/**
 * Replays the change list of the covered snapshot if union reads need it: for the empty lake (the
 * whole log), or again after a failed replay or hand-off (the index is in place). The replay runs
 * on a job thread and is installed only once complete, so a replay that fails part way leaves
 * nothing behind.
 */
final class DvTailLoad extends DvLoad {

    private static final Logger LOG = LoggerFactory.getLogger(DvTailLoad.class);

    @Nullable private DerivedDvRebuildLimiter slots;
    @Nullable private Exception failure;
    // how the replay starts: from the change list the new leader restored, or not
    private final DvCheckpointReplay replayStart;
    private long slotWaitNanos;
    private long replayEarlyNanos;
    private long replayFinalNanos;
    private long t0;

    DvTailLoad(DvOwner m, RecoverySource src, long snapshotId, long startOffset) {
        super(m, src, Kind.TAIL, snapshotId, startOffset);
        this.replayStart = new DvCheckpointReplay(m);
    }

    /** The tail load the covered snapshot needs, or null if it needs none; an owner step. */
    @Nullable
    static DvTailLoad ifMissing(DvOwner m, long snapshotId) {
        if (m.closed
                || m.coveredSnapshotId != snapshotId
                || m.recoverySource == null
                || !m.maskState.tailMissing()) {
            return null; // the union read answers
        }
        return new DvTailLoad(m, m.recoverySource, snapshotId, m.snapshotStartOffset);
    }

    @Override
    boolean load() throws Exception {
        long slotStart = System.nanoTime();
        slots = startSlot(m);
        slotWaitNanos = System.nanoTime() - slotStart;
        slots.beforeRebuild();
        t0 = System.currentTimeMillis();
        replay = replayStart.start(src, readableOffset, this::stopped);
        if (replay == null) {
            return false;
        }
        long replayStartNanos = System.nanoTime();
        failure = replay.replayEarlyPasses();
        replayEarlyNanos = System.nanoTime() - replayStartNanos;
        if (failure == null) {
            long finalStart = System.nanoTime();
            if (!m.work.enterFinalPass(replay)) {
                return false;
            }
            failure = replay.finalRead();
            replayFinalNanos = System.nanoTime() - finalStart;
        }
        return true;
    }

    @Override
    boolean swap() throws Exception {
        long swapStart = System.nanoTime();
        if (m.closed) {
            return false;
        }
        if (failure != null) {
            if (isDiskFailure(failure)) {
                m.maskState.tailFailure = new TailDiskException(failure);
                m.rebuild.retryAfterDiskFailure();
                throw new IOException(failure.getMessage(), m.maskState.tailFailure);
            }
            if (m.coveredSnapshotId < 0) {
                throw new IOException(
                        "No lake snapshot yet, and the log cannot be replayed from offset 0"
                                + " for the deletion vectors",
                        failure);
            }
            m.maskState.tailFailure = failure;
            throw new IOException(
                    String.format(
                            "Cannot replay the log of snapshot %d from offset %d, so no deletion"
                                    + " vector masks are served",
                            m.coveredSnapshotId, readableOffset),
                    failure);
        }
        if (replay.lost) {
            LOG.info(
                    "The deletion vector replay of {} from offset {} lost the change list"
                            + " during its final pass and installs nothing; it is replayed again.",
                    m.logName,
                    readableOffset);
            return true;
        }
        m.maskState.renew(KeepReady.SAME_SNAPSHOT);
        // its lookups are a rebuild's
        m.maskState.fromRebuild = true;
        DvReplay r = replay;
        long installStart = System.nanoTime();
        r.install();
        long installNanos = System.nanoTime() - installStart;
        if (m.coveredSnapshotId < 0) {
            m.emptyLakeTracked = true;
        }
        m.metrics.rebuilt(System.currentTimeMillis() - t0);
        long end = System.nanoTime();
        // a replay of the change list; snapshot -1 is the empty lake
        LOG.info(
                "Rebuilt the deletion vectors of {}: kind=changes snapshot={}"
                        + " readable_offset={} log_end={} changes_replayed={} load_index_ms={}"
                        + " replay_ms={} total_ms={} versions_spilled={} tail_changes={}"
                        + " queued={} replay_locked_ms={} install_ms={} lock_ms={} slot_wait_ms={} {}",
                m.logName,
                m.coveredSnapshotId,
                readableOffset,
                r.next,
                r.changes,
                ms(0),
                ms(replayEarlyNanos + replayFinalNanos),
                ms(end - startNanos),
                r.versionsId >= 0,
                m.maskState.tail.changeCount(),
                m.maskState.tail.queue.size(),
                ms(replayFinalNanos),
                ms(installNanos),
                ms(end - swapStart),
                ms(slotWaitNanos),
                replayStart.logFields(r));
        return true;
    }

    @Override
    public void close() {
        super.close();
        if (slots != null) {
            endSlot(m, slots);
            slots = null;
        }
    }
}
