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

import org.apache.fluss.exception.DeletionVectorsNotReadyException;
import org.apache.fluss.exception.StaleSnapshotException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The union reads of a bucket. A read the published state ({@link DvState}) answers is answered on
 * the request thread. Any other read posts itself to the bucket's owner and waits for the answer,
 * at most {@link #NOT_READY_WAIT_MS}; the owner answers it as soon as it can, from its own steps.
 * Catch-ups, rebuilds, replays and lookups run in the background, never on the request thread.
 *
 * <ul>
 *   <li>A read that waits gets masks up to at least its floor: the high watermark and the changes
 *       handed over when it came, and the highest cut served ({@link ServedCuts}). If the latest
 *       frontier reaches the floor, the request thread serves it. Otherwise the owner drains the
 *       hand-off, applies, and looks keys up; once the masks are exact up to a cut at or above the
 *       floor, it builds them there.
 *   <li>A read that does not wait is answered by {@link DvReadPolicy}, on the request thread from
 *       the published state. Only a read the policy sends to the owner (to build a frontier, or to
 *       wait for a switch or lookups) posts itself.
 * </ul>
 *
 * <p>A read the owner cannot answer within the wait is answered as not ready ({@link
 * DeletionVectorsNotReadyException}), and the client retries.
 */
final class DvMaskReads {

    private static final Logger LOG = LoggerFactory.getLogger(DvMaskReads.class);

    /** How long a union read waits for its bucket before it is answered as not ready. */
    static final long NOT_READY_WAIT_MS = 500;
    // a read that waits for the high watermark is evaluated again this often
    private static final long HW_RECHECK_MS = 20;

    private final DvOwner m;
    // owner-confined: the reads waiting for an answer
    private final List<MaskRead> waiting = new ArrayList<>();
    private boolean recheckScheduled;

    DvMaskReads(DvOwner m) {
        this.m = m;
    }

    /** A read waiting for its masks. */
    static final class MaskRead {
        final long requested;
        final boolean useCache;
        final boolean waitForCatchUp;
        // the high watermark and the changes handed over when the read came, and the highest cut
        // served then: a read that waits gets no masks below it
        final long floor;
        final CompletableFuture<Answer> answer = new CompletableFuture<>();
        // the catch-up and the replay the read waits for, numbered
        long advance = -1;
        long replay = -1;
        // what the read waited for last, for its timing, and the answer if it waits too long
        volatile int waitingFor = -1;
        volatile String notReady = "Deletion vectors of the bucket are not ready.";
        // a read told to read by key: why, and the cut and high watermark it was told
        @Nullable volatile String foldCause;
        volatile long foldCut = -1;
        volatile long foldHighWatermark = -1;
        long applyNanos;
        long buildNanos;
        // The time the read waited for lake-position lookups: from an evaluation that found lookups
        // pending until one that found none, or until it was answered. Owner-confined until the
        // answer completes; the request thread reads it after.
        long resolveWaitNanos;
        long lookupWaitStart = -1;

        MaskRead(long requested, boolean useCache, boolean waitForCatchUp, long floor) {
            this.requested = requested;
            this.useCache = useCache;
            this.waitForCatchUp = waitForCatchUp;
            this.floor = floor;
        }
    }

    /** The answer to a read: its masks and how they were found, for its timing. */
    static final class Answer {
        final DvSnapshot masks;
        @Nullable final String frontier;
        final long cutAgeNanos;
        final boolean cached;
        final long applyNanos;
        final long buildNanos;

        Answer(
                DvSnapshot masks,
                @Nullable String frontier,
                long cutAgeNanos,
                boolean cached,
                long applyNanos,
                long buildNanos) {
            this.masks = masks;
            this.frontier = frontier;
            this.cutAgeNanos = cutAgeNanos;
            this.cached = cached;
            this.applyNanos = applyNanos;
            this.buildNanos = buildNanos;
        }
    }

    // ---- the request thread ---------------------------------------------------------------------

    /**
     * The masks of a union read of {@code requestedSnapshotId}, on the request thread. A read the
     * published state answers is answered here, without a hop to the owner: one that waits and
     * whose floor the latest frontier reaches, and one that does not wait and that {@link
     * DvReadPolicy} serves, sends back, or tells to read by key. Only a read that needs new masks,
     * or must wait, posts itself to the owner.
     */
    DvSnapshot read(
            long requestedSnapshotId,
            boolean useCache,
            DvMaskRequestTiming timing,
            boolean waitForCatchUp)
            throws IOException, StaleSnapshotException {
        long deadline = System.currentTimeMillis() + NOT_READY_WAIT_MS;
        DvState s = m.state;
        if (timing.coveredBefore == DvMaskRequestTiming.UNKNOWN) {
            timing.coveredBefore = s.coveredSnapshotId;
        }
        if (m.isClosed()) {
            throw DerivedDvManager.closedError();
        }
        long hw = m.highWatermark.getAsLong();
        long handedOver = m.handOff.appendedEnd();
        long floor = Math.max(handedOver >= 0 ? Math.min(hw, handedOver) : hw, m.servedCuts.get());
        MaskRead r = new MaskRead(requestedSnapshotId, useCache, waitForCatchUp, floor);
        Answer a;
        try {
            a = waitForCatchUp ? readyNow(s, r) : decideNow(s, r, hw);
            if (a == null) {
                a = ask(r, deadline, timing);
            }
        } finally {
            if (r.foldCause != null) {
                timing.rebuildCause = r.foldCause;
                timing.cut = r.foldCut;
                timing.highWatermark = r.foldHighWatermark;
            }
        }
        timing.applyNanos += a.applyNanos;
        timing.resolveWaitNanos += r.resolveWaitNanos;
        timing.buildNanos += a.buildNanos;
        timing.cached = a.cached;
        timing.frontier = a.frontier;
        timing.cutAgeNanos = a.cutAgeNanos;
        timing.cut = a.masks.getLogEndOffset();
        timing.highWatermark = a.masks.getHighWatermark();
        return a.masks;
    }

    /**
     * Serves a read that waits from the published latest frontier, on the request thread: the
     * frontier is of the requested snapshot and exact up to a cut no lower than the read's floor.
     * Null if it is not, or the cache is off.
     */
    @Nullable
    private Answer readyNow(DvState s, MaskRead r) {
        if (!r.useCache
                || m.maskCacheNanos <= 0
                || !DvReadPolicy.servesWaitingRead(s, r.requested, r.floor)) {
            return null;
        }
        if (s.handOffFailuresTaken != m.handOff.failures()) {
            // a hand-off failed after the state was published: its frontier may be below that
            // write, which the owner answers once it dropped the change list
            return null;
        }
        m.frontiers.waitingReadCame(System.nanoTime());
        DvSnapshot masks = s.latest.masks;
        m.testHooks.beforeReadyServe();
        if (!m.servedCuts.tryServe(masks.getLogEndOffset())) {
            return null; // a read was served a higher cut meanwhile
        }
        return new Answer(masks, null, 0, true, 0, 0);
    }

    /**
     * Answers a read that does not wait from the published state, on the request thread, as {@link
     * DvReadPolicy} decides: serves a frontier, sends the reader back to the covered snapshot, or
     * tells it to read by key. What the answer starts is posted to the owner. Null if the owner
     * must build masks, or the read must wait for a switch or lookups.
     */
    @Nullable
    private Answer decideNow(DvState s, MaskRead r, long hw) throws StaleSnapshotException {
        long now = System.nanoTime();
        m.frontiers.readCame(now);
        long servedCut = m.servedCuts.get();
        DvReadPolicy.Answer answer =
                DvReadPolicy.noWait(
                        s, r.requested, servedCut, hw, now, m.maskCacheNanos, m::passed);
        switch (answer.action) {
            case SERVE:
                DvSnapshot masks = answer.masks;
                if (!m.servedCuts.tryServe(masks.getLogEndOffset())) {
                    return null; // a read was served a higher cut meanwhile: the owner decides
                }
                start(s, answer, r.requested, servedCut);
                if (masks.getLogEndOffset() < masks.getHighWatermark()) {
                    m.metrics.frontierServed();
                }
                return new Answer(masks, answer.frontier, now - answer.builtAtNanos, false, 0, 0);
            case BEHIND:
                start(s, answer, r.requested, servedCut);
                throw new StaleSnapshotException(
                        answer.behindSnapshotId, r.requested, true, answer.behindStartOffset);
            case STALE:
                throw m.stale(r.requested, s);
            case FOLD:
                start(s, answer, r.requested, servedCut);
                throw fold(r, answer.foldCause, s.coveredSnapshotId, hw);
            case CLOSED:
                throw DerivedDvManager.closedError();
            default:
                // BUILD, WAIT_FOR_LOOKUPS, WAIT_FOR_SWITCH, NOT_READY: the owner answers
                return null;
        }
    }

    /**
     * Posts what an answer decided on the request thread starts (a switch, a replay, a resolve),
     * and the drop of a kept frontier below the served cut. Nothing to post, nothing posted.
     */
    private void start(DvState s, DvReadPolicy.Answer a, long requested, long servedCut) {
        boolean dropKept = s.kept != null && s.kept.cut() < servedCut;
        if (!a.startSwitch && !a.startReplay && !a.startResolve && !dropKept) {
            return;
        }
        try {
            m.post(
                    () -> {
                        m.frontiers.dropKeptBelow(servedCut);
                        started(a, requested);
                    });
        } catch (RejectedExecutionException e) {
            // the bucket closes: nothing to start
        }
    }

    /** Starts what a read's answer asks to start; an owner step. */
    private void started(DvReadPolicy.Answer a, long requested) {
        if (m.isClosed()) {
            return;
        }
        if (a.startSwitch) {
            m.planner.scheduleAdvance(requested);
        }
        if (a.startReplay) {
            m.rebuild.scheduleReplay(requested);
        }
        if (a.startResolve) {
            m.maskState.scheduleResolve();
        }
    }

    /**
     * The answer to a read of a bucket that has nothing to serve yet, for {@code cause}: a rebuild,
     * a replay of the change list, a rebuild's lookups, or none (no snapshot covered and no rebuild
     * started yet). The reader reads the bucket's log by key up to the cut ({@link
     * DvReadPolicy#foldCut}). The cut counts as served once the answer is handed out ({@link
     * DerivedDvManager#foldCutServed}), so no later read goes back from it.
     */
    private DeletionVectorsNotReadyException fold(
            MaskRead r, String cause, long coveredSnapshotId, long hw) {
        long cut = DvReadPolicy.foldCut(hw, m.servedCuts.get());
        r.foldCause = cause;
        r.foldCut = cut;
        r.foldHighWatermark = hw;
        return DeletionVectorsNotReadyException.rebuilding(
                String.format(
                        "Deletion vectors of the bucket cover lake snapshot %d and serve no masks"
                                + " yet",
                        coveredSnapshotId),
                cause,
                cut);
    }

    /** Posts {@code r} to the owner and waits for its answer, at most until {@code deadline}. */
    private Answer ask(MaskRead r, long deadline, DvMaskRequestTiming timing)
            throws IOException, StaleSnapshotException {
        long waitStart = System.nanoTime();
        try {
            m.post(() -> arrive(r));
        } catch (RejectedExecutionException e) {
            throw DerivedDvManager.closedError();
        }
        try {
            try {
                return r.answer.get(
                        Math.max(1, deadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
                throw rethrow(e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                r.answer.cancel(false);
                throw new InterruptedIOException("Interrupted while waiting for deletion vectors");
            } catch (TimeoutException e) {
                r.answer.completeExceptionally(new DeletionVectorsNotReadyException(r.notReady));
                return join(r);
            }
        } finally {
            if (r.waitingFor >= 0) {
                timing.waited(r.waitingFor, System.nanoTime() - waitStart);
            }
        }
    }

    private static Answer join(MaskRead r) throws IOException, StaleSnapshotException {
        try {
            return r.answer.get();
        } catch (ExecutionException e) {
            throw rethrow(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            r.answer.cancel(false);
            throw new InterruptedIOException("Interrupted while waiting for deletion vectors");
        }
    }

    private static IOException rethrow(Throwable cause) throws StaleSnapshotException {
        if (cause instanceof StaleSnapshotException) {
            throw (StaleSnapshotException) cause;
        }
        if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        if (cause instanceof IOException) {
            return (IOException) cause;
        }
        return new IOException(cause);
    }

    // ---- pins, on the request thread ------------------------------------------------------------

    /** Pins the cut of a union read for {@code bucket}; see {@link DerivedDvManager#pinCut}. */
    DvPinnedCut pin(
            DerivedDvManager bucket,
            long requestedSnapshotId,
            boolean waitForCatchUp,
            @Nullable Long pinRequestId,
            int holds)
            throws IOException, StaleSnapshotException {
        if (pinRequestId != null) {
            DvPinnedCut retried =
                    m.heapBudget.pinnedCuts().pinnedBy(bucket, pinRequestId, requestedSnapshotId);
            if (retried != null) {
                return retried;
            }
        }
        DvMaskRequestTiming timing = new DvMaskRequestTiming("pin", requestedSnapshotId);
        Throwable failure = null;
        try {
            DvSnapshot masks = read(requestedSnapshotId, true, timing, waitForCatchUp);
            long pinStart = System.nanoTime();
            try {
                return m.heapBudget
                        .pinnedCuts()
                        .pin(bucket, requestedSnapshotId, masks, m.metrics, pinRequestId, holds);
            } finally {
                timing.pinNanos = System.nanoTime() - pinStart;
            }
        } catch (Throwable t) {
            failure = t;
            throw t;
        } finally {
            timing.done(m.metrics, m.logName, () -> m.state.coveredSnapshotId, failure);
        }
    }

    /** A cut {@code bucket} pinned; see {@link DerivedDvManager#pinnedCut}. */
    DvPinnedCut pinned(DerivedDvManager bucket, long snapshotId, long cut)
            throws IOException, StaleSnapshotException {
        DerivedDvPinnedCuts pinnedCuts = m.heapBudget.pinnedCuts();
        DvPinnedCut held = pinnedCuts.get(bucket, snapshotId, cut);
        if (held != null) {
            return held;
        }
        DvState s = m.state;
        if (m.closed) {
            throw DerivedDvManager.closedError();
        }
        if (s.coveredSnapshotId != snapshotId || m.servableHighWatermark() != cut) {
            LOG.debug(
                    "The deletion vector cut of snapshot {} at offset {} is not held; the"
                            + " bucket covers snapshot {} up to offset {}.",
                    snapshotId,
                    cut,
                    s.coveredSnapshotId,
                    m.servableHighWatermark());
            throw m.stale(snapshotId, s);
        }
        DvMaskRequestTiming timing = new DvMaskRequestTiming("cut", snapshotId);
        Throwable failure = null;
        try {
            // the cached masks may be of an earlier high watermark
            DvSnapshot masks = read(snapshotId, false, timing, true);
            if (masks.getLogEndOffset() != cut) {
                // the high watermark moved meanwhile
                throw m.stale(snapshotId, m.state);
            }
            long pinStart = System.nanoTime();
            try {
                return pinnedCuts.pin(bucket, snapshotId, masks, m.metrics, null, 1);
            } finally {
                timing.pinNanos = System.nanoTime() - pinStart;
            }
        } catch (Throwable t) {
            failure = t;
            throw t;
        } finally {
            timing.done(m.metrics, m.logName, () -> m.state.coveredSnapshotId, failure);
        }
    }

    // ---- the owner ------------------------------------------------------------------------------

    /** A read came; an owner step. */
    private void arrive(MaskRead r) {
        m.drain.drainHandOff();
        if (!evaluate(r)) {
            waiting.add(r);
        }
    }

    /** Evaluates the waiting reads again, after a step that changed the state; an owner step. */
    void evaluateWaiting() {
        if (waiting.isEmpty()) {
            return;
        }
        m.drain.drainHandOff();
        for (Iterator<MaskRead> it = waiting.iterator(); it.hasNext(); ) {
            if (evaluate(it.next())) {
                it.remove();
            }
        }
    }

    /** Fails the waiting reads as the bucket closes; an owner step. */
    void closed() {
        for (MaskRead r : waiting) {
            r.answer.completeExceptionally(DerivedDvManager.closedError());
        }
        waiting.clear();
    }

    /** Answers {@code r} if it can be answered now; an owner step. */
    private boolean evaluate(MaskRead r) {
        if (r.answer.isDone()) {
            return true;
        }
        try {
            Answer a = r.waitForCatchUp ? waiting(r) : notWaiting(r);
            if (a == null) {
                return false;
            }
            endLookupWait(r);
            r.answer.complete(a);
        } catch (Exception e) {
            r.answer.completeExceptionally(e);
        }
        return true;
    }

    // what untilServable answers when the bucket can serve the read now
    private static final Answer SERVABLE =
            new Answer(new DvSnapshot(Collections.emptyMap(), null, -1, -1), null, 0, false, 0, 0);

    /** Keeps {@code r} waiting for the lake-position lookups pending; returns null. */
    @Nullable
    private Answer waitForLookups(MaskRead r) {
        if (r.lookupWaitStart < 0) {
            r.lookupWaitStart = System.nanoTime();
        }
        return waitFor(r, DvMaskRequestTiming.DERIVE, lookingUp());
    }

    /** Ends the wait of {@code r} for lookups, if it waits for them. */
    private static void endLookupWait(MaskRead r) {
        if (r.lookupWaitStart >= 0) {
            r.resolveWaitNanos += System.nanoTime() - r.lookupWaitStart;
            r.lookupWaitStart = -1;
        }
    }

    /** Keeps {@code r} waiting for {@code what}; returns null. */
    @Nullable
    private static Answer waitFor(MaskRead r, int what, String notReady) {
        r.waitingFor = what;
        r.notReady = notReady;
        return null;
    }

    /** One evaluation of a read that waits; an owner step. Returns null to keep it waiting. */
    @Nullable
    private Answer waiting(MaskRead r) throws IOException, StaleSnapshotException {
        m.frontiers.waitingReadCame(System.nanoTime());
        Answer servable = untilServable(r);
        if (servable != SERVABLE) {
            return servable;
        }
        DvState state = m.buildState();
        if (r.useCache
                && m.maskCacheNanos > 0
                && DvReadPolicy.servesWaitingRead(state, r.requested, r.floor)) {
            DvSnapshot masks = state.latest.masks;
            if (m.servedCuts.tryServe(masks.getLogEndOffset())) {
                return new Answer(masks, null, 0, true, r.applyNanos, r.buildNanos);
            }
        }
        if (m.inFlight > 0) {
            // a lookup runs; its positions reach the masks in a step
            return waitForLookups(r);
        }
        long hw = m.servableHighWatermark();
        r.applyNanos += m.maskState.applyUpTo(hw);
        if (m.maskState.tailFailure != null) {
            // the change list's disk failed while applying
            return waiting(r);
        }
        if (m.maskState.tail.hasBelow(hw)) {
            // applied a chunk a step, or after the build in progress
            return waitFor(r, DvMaskRequestTiming.DERIVE, lookingUp());
        }
        if (!m.maskState.unresolved.isEmpty()) {
            m.maskState.scheduleResolve(0);
            return waitForLookups(r);
        }
        endLookupWait(r);
        // The masks go no lower than the read's floor, nor than a cut served since it came: a
        // takeover or a fold handed out meanwhile may have raised it above the changes handed over.
        long floor = Math.max(r.floor, m.servedCuts.get());
        if (hw < floor) {
            return belowServedCut(r, floor, hw);
        }
        DvFrontier latest = m.frontiers.latest();
        if (latest != null && latest.cut() >= hw && m.servedCuts.tryServe(latest.cut())) {
            // built at this cut already, as by a build that took more than one step
            return new Answer(latest.masks, null, 0, false, r.applyNanos, r.buildNanos);
        }
        long buildStart = System.nanoTime();
        DvSnapshot masks = m.maskState.buildMasks(hw);
        r.buildNanos += System.nanoTime() - buildStart;
        if (masks == null) {
            // the build goes on in the steps after this one
            return waitFor(r, DvMaskRequestTiming.DERIVE, lookingUp());
        }
        if (!m.servedCuts.tryServe(hw)) {
            // a cut above was served meanwhile: the read waits for masks up to it
            return belowServedCut(r, m.servedCuts.get(), hw);
        }
        return new Answer(masks, null, 0, false, r.applyNanos, r.buildNanos);
    }

    /**
     * Keeps a read that waits waiting for masks up to {@code floor} while the changes handed over
     * end below it, at {@code hw}, as right after a takeover or a fold above the hand-off. The read
     * is evaluated again a moment later, and answered as not ready if it waits too long.
     */
    @Nullable
    private Answer belowServedCut(MaskRead r, long floor, long hw) {
        scheduleRecheck();
        return waitFor(
                r,
                DvMaskRequestTiming.DERIVE,
                String.format(
                        "Deletion vectors of lake snapshot %d served masks up to offset %d;"
                                + " the changes are handed over up to offset %d.",
                        m.coveredSnapshotId, floor, hw));
    }

    /**
     * Whether the bucket can answer {@code r} now: returns {@link #SERVABLE} if it can, else keeps
     * the read waiting (null) or throws its answer. Starts the catch-up or replay the read needs.
     * An owner step.
     */
    @Nullable
    private Answer untilServable(MaskRead r) throws IOException, StaleSnapshotException {
        long requested = r.requested;
        if (m.closed) {
            throw DerivedDvManager.closedError();
        }
        if (m.recoverySource == null) {
            // A new leader that has not started recovering yet covers nothing so far, not even
            // the empty lake: its change list does not hold the log before its takeover.
            throw new DeletionVectorsNotReadyException(
                    String.format(
                            "Deletion vectors of this bucket are not recovered yet; lake"
                                    + " snapshot %d is requested.",
                            requested));
        }
        if (m.coveredSnapshotId != requested) {
            if (requested < 0 || m.passed(requested) || m.unreachable == requested) {
                throw m.stale(requested);
            }
            if (r.advance >= 0 && m.planner.lastFailedAdvance >= r.advance) {
                // the catch-up this read waited for failed
                throw m.planner.advanceFailed(requested);
            }
            long attempt = m.planner.scheduleAdvance(requested);
            if (attempt < 0) {
                // the last catch-up to it failed; it is tried again after a backoff
                throw m.planner.advanceFailed(requested);
            }
            if (r.advance < 0) {
                r.advance = attempt;
            }
            return waitFor(
                    r,
                    DvMaskRequestTiming.MOVE,
                    String.format(
                            "Deletion vectors cover lake snapshot %d and are switching to"
                                    + " snapshot %d.",
                            m.coveredSnapshotId, requested));
        }
        if (!r.waitForCatchUp) {
            return SERVABLE;
        }
        if (m.maskState.tailMissing()) {
            if (m.rebuild.overflowRetryPending()) {
                // the owner fell behind its writes: replayed after a backoff
                throw new DeletionVectorsNotReadyException(
                        String.format(
                                "Deletion vectors of lake snapshot %d fell behind the writes and"
                                        + " dropped their change list; it is replayed from offset"
                                        + " %d after a backoff.",
                                m.coveredSnapshotId, m.snapshotStartOffset));
            }
            if (m.rebuild.diskRetryPending()) {
                // the local disk failed: retried after a backoff
                throw new DeletionVectorsNotReadyException(
                        String.format(
                                "Deletion vectors of lake snapshot %d could not keep their"
                                        + " change list on disk (%s); it is replayed again.",
                                m.coveredSnapshotId, m.maskState.tailFailure.getCause()));
            }
            Exception lastReplayFailure = m.rebuild.lastReplayFailure;
            if (r.replay >= 0
                    && m.rebuild.lastFailedReplay >= r.replay
                    && lastReplayFailure != null) {
                if (DvOwner.isDiskFailure(lastReplayFailure)) {
                    throw new DeletionVectorsNotReadyException(
                            String.format(
                                    "Deletion vectors of lake snapshot %d could not keep their"
                                            + " change list on disk (%s); it is replayed again.",
                                    m.coveredSnapshotId, lastReplayFailure));
                }
                // the replay this read waited for failed
                throw new IOException(
                        String.format(
                                "The log of snapshot %d from offset %d is not replayed, so no deletion"
                                        + " vector masks are served",
                                m.coveredSnapshotId, m.snapshotStartOffset),
                        lastReplayFailure);
            }
            if (r.replay < 0) {
                r.replay = m.rebuild.scheduleReplay(requested);
            }
            return waitFor(
                    r,
                    DvMaskRequestTiming.REPLAY,
                    String.format(
                            "Deletion vectors of lake snapshot %d are replaying the log from"
                                    + " offset %d.",
                            m.coveredSnapshotId, m.snapshotStartOffset));
        }
        if (m.maskState.fullResolvePending) {
            m.maskState.scheduleFullResolve();
            return waitFor(r, DvMaskRequestTiming.DERIVE, lookingUp());
        }
        if (m.maskState.targetedPending) {
            return waitFor(r, DvMaskRequestTiming.DERIVE, lookingUp());
        }
        return SERVABLE;
    }

    private String lookingUp() {
        return String.format(
                "Deletion vectors of lake snapshot %d are applying %d changes and looking up the"
                        + " lake positions of %d changed keys from offset %d.",
                m.coveredSnapshotId,
                m.maskState.tail.queue.size(),
                m.maskState.unresolved.size(),
                m.snapshotStartOffset);
    }

    /** One evaluation of a read that does not wait; an owner step. Null keeps it waiting. */
    @Nullable
    private Answer notWaiting(MaskRead r) throws IOException, StaleSnapshotException {
        long now = System.nanoTime();
        m.frontiers.readCame(now);
        long servedCut = m.servedCuts.get();
        m.frontiers.dropKeptBelow(servedCut);
        long hw = m.highWatermark.getAsLong();
        DvReadPolicy.Answer answer =
                DvReadPolicy.noWait(
                        m.buildState(),
                        r.requested,
                        servedCut,
                        hw,
                        now,
                        m.maskCacheNanos,
                        m::passed);
        if (answer.startSwitch) {
            m.planner.scheduleAdvance(r.requested);
        }
        if (answer.startReplay) {
            m.rebuild.scheduleReplay(r.requested);
        }
        if (answer.startResolve) {
            m.maskState.scheduleResolve();
        }
        switch (answer.action) {
            case SERVE:
                DvSnapshot masks = answer.masks;
                if (!m.servedCuts.tryServe(masks.getLogEndOffset())) {
                    return notWaiting(r); // a read served a higher cut meanwhile
                }
                if (masks.getLogEndOffset() < masks.getHighWatermark()) {
                    m.metrics.frontierServed();
                }
                return new Answer(
                        masks,
                        answer.frontier,
                        now - answer.builtAtNanos,
                        false,
                        r.applyNanos,
                        r.buildNanos);
            case BEHIND:
                throw new StaleSnapshotException(
                        answer.behindSnapshotId, r.requested, true, answer.behindStartOffset);
            case STALE:
                throw m.stale(r.requested);
            case FOLD:
                throw fold(r, answer.foldCause, m.coveredSnapshotId, hw);
            case NOT_READY:
            case CLOSED:
            case WAIT_FOR_SWITCH:
                // answered, or kept waiting, as a read that waits would be
                Answer servable = untilServable(r);
                return servable == SERVABLE ? notWaiting(r) : servable;
            case BUILD:
                Answer built = buildFrontier(r, servedCut);
                if (built != null) {
                    return built;
                }
                // the next lookup must be done first; it will be soon, as nothing is pending
                m.maskState.scheduleResolve(0);
                return waitFor(r, DvMaskRequestTiming.DERIVE, lookingUp());
            case WAIT_FOR_LOOKUPS:
                return waitFor(r, DvMaskRequestTiming.DERIVE, lookingUp());
            default:
                throw new IllegalStateException("Unknown deletion vector read answer " + answer);
        }
    }

    /**
     * Builds the masks of a read that does not wait while no lookup is pending: applies the queued
     * changes up to the first one that needs a lookup, and builds the masks there, up to the high
     * watermark if none does. Returns null if that cut is below the highest cut served, or if
     * applying failed. An owner step.
     */
    @Nullable
    private Answer buildFrontier(MaskRead r, long servedCut) throws IOException {
        long hw = m.servableHighWatermark();
        long cut = hw;
        if (m.coveredSnapshotId >= 0) {
            // the queue holds no change below the applied cut
            cut =
                    Math.max(
                            m.maskState.tail.firstLakeChangeBelow(hw, m.snapshotStartOffset),
                            m.maskState.appliedCut);
            cut = Math.max(Math.min(cut, hw), m.snapshotStartOffset);
        }
        if (cut < servedCut) {
            return null;
        }
        DvFrontier latest = m.frontiers.latest();
        if (latest != null && latest.cut() >= cut && m.servedCuts.tryServe(latest.cut())) {
            // built at this cut already, as by a build that took more than one step
            return new Answer(latest.at(hw), "ready", 0, false, r.applyNanos, r.buildNanos);
        }
        r.applyNanos += m.maskState.applyUpTo(cut);
        if (m.maskState.tailFailure != null || m.maskState.tail.hasBelow(cut)) {
            return null;
        }
        long buildStart = System.nanoTime();
        DvSnapshot masks = m.maskState.buildMasks(cut);
        r.buildNanos += System.nanoTime() - buildStart;
        if (masks == null || !m.servedCuts.tryServe(cut)) {
            return null;
        }
        String frontier = cut < hw ? "partial" : null;
        if (frontier != null && masks.getLogEndOffset() < masks.getHighWatermark()) {
            m.metrics.frontierServed();
        }
        return new Answer(masks, frontier, 0, false, r.applyNanos, r.buildNanos);
    }

    /** Evaluates the waiting reads again a moment later, as the high watermark moves. */
    private void scheduleRecheck() {
        if (recheckScheduled) {
            return;
        }
        recheckScheduled = true;
        try {
            m.runtime
                    .scheduler()
                    .schedule(
                            () -> m.post(() -> recheckScheduled = false),
                            HW_RECHECK_MS,
                            TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            recheckScheduled = false;
        }
    }
}
