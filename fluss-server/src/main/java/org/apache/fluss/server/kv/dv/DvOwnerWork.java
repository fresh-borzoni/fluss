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

import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.server.kv.dv.DerivedDvManager.RecoverySource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The work queue of a bucket's owner ({@link DvOwner}): recovery and catch-ups to a target
 * snapshot, one at a time. A work plans its load, the load runs off the owner, and its swap is an
 * owner step; targets wanted meanwhile wait in the queue. Its methods run as owner steps unless
 * their javadoc names another thread.
 */
final class DvOwnerWork {

    private static final Logger LOG = LoggerFactory.getLogger(DvOwnerWork.class);

    private final DvOwner o;

    DvOwnerWork(DvOwner o) {
        this.o = o;
    }

    // a switch a test holds is tried again after this backoff, doubling up to the maximum
    static final long HELD_RETRY_INITIAL_MS = 100;
    static final long HELD_RETRY_MAX_MS = 2_000;
    // how long a leader transition waits for the bucket's owner to take the bucket over
    static final long TAKEOVER_WAIT_MS = 30_000;

    // owner-confined: the work queued and the one running
    final ArrayDeque<Work> queue = new ArrayDeque<>();
    @Nullable Work running;
    // the load in flight, for the published state
    @Nullable DvLoad load;
    // the recovery whose failed takeover is tried again after takeoverBackoffMs, which a close
    // ends
    @Nullable private Recover takeoverRetry;
    private long takeoverBackoffMs;

    /** The load in flight, or null. */
    @Nullable
    DvLoad load() {
        return load;
    }

    // ---- work -----------------------------------------------------------------------------------

    /**
     * Recovers in the background, with full loads limited by the tablet server's {@code limiter}.
     */
    CompletableFuture<Boolean> recover(RecoverySource src, DerivedDvRebuildLimiter limiter) {
        o.rebuild.limiter = limiter;
        return recover(src);
    }

    /**
     * Takes the bucket over with {@code src}: covers its readable snapshot. Returns once the owner
     * took it over, or its takeover failed, or did not run within {@link #TAKEOVER_WAIT_MS}; the
     * future completes once the bucket covers its readable snapshot, or the empty lake. A takeover
     * that fails is tried again by the owner after a backoff until it succeeds or the bucket
     * closes, so the caller starts it once and never twice.
     */
    CompletableFuture<Boolean> recover(RecoverySource src) {
        Recover w = new Recover(src);
        CompletableFuture<Void> takenOver = new CompletableFuture<>();
        o.post(() -> takeOver(w, takenOver));
        try {
            // the leader transition waits for the takeover step, but not forever
            takenOver.get(TAKEOVER_WAIT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            w.done.completeExceptionally(e);
        } catch (ExecutionException e) {
            // the owner tries it again after a backoff
        } catch (TimeoutException e) {
            LOG.warn(
                    "The takeover of the deletion vectors of {} did not run within {} ms;"
                            + " it runs later.",
                    o.logName,
                    TAKEOVER_WAIT_MS);
        }
        return w.done;
    }

    /**
     * Takes the bucket over for the recovery {@code w} and queues it. If the takeover fails, the
     * bucket stays unrecovered, and the takeover is tried again after a backoff that doubles up to
     * the cap of a failed catch-up's, until it succeeds or the bucket closes. {@code takenOver} is
     * the first try's, which the caller waits for; null for a retry.
     */
    private void takeOver(Recover w, @Nullable CompletableFuture<Void> takenOver) {
        if (takenOver == null) {
            if (takeoverRetry != w) {
                return; // the bucket closed, or another recovery took it over meanwhile
            }
            takeoverRetry = null;
        }
        if (o.closed) {
            w.end(false, null);
            if (takenOver != null) {
                takenOver.complete(null);
            }
            return;
        }
        try {
            o.takeOver(w.src);
        } catch (Throwable t) {
            retryTakeOver(w);
            if (takenOver != null) {
                takenOver.completeExceptionally(t);
            }
            // the step's fault handling logs and counts it
            throw t;
        }
        takeoverBackoffMs = 0;
        Recover superseded = takeoverRetry;
        if (superseded != null) {
            // a recovery whose takeover waits to be tried again: this one took the bucket over
            takeoverRetry = null;
            superseded.end(false, null);
        }
        try {
            enqueue(w);
            // reads see the bucket taken over once the caller returns
            o.publish();
        } finally {
            if (takenOver != null) {
                takenOver.complete(null);
            }
        }
    }

    /** Tries the takeover of {@code w} again after a backoff. */
    private void retryTakeOver(Recover w) {
        takeoverBackoffMs =
                takeoverBackoffMs == 0
                        ? DvOwnerPlanner.ADVANCE_RETRY_INITIAL_BACKOFF_MS
                        : Math.min(
                                2 * takeoverBackoffMs, DvOwnerPlanner.ADVANCE_RETRY_MAX_BACKOFF_MS);
        takeoverRetry = w;
        LOG.warn(
                "The takeover of the deletion vectors of {} failed; it is tried again"
                        + " in {} ms.",
                o.logName,
                takeoverBackoffMs);
        try {
            o.runtime
                    .scheduler()
                    .schedule(
                            () -> o.post(() -> takeOver(w, null)),
                            takeoverBackoffMs,
                            TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            // the server shuts down
            takeoverRetry = null;
            w.end(false, e);
        }
    }

    /**
     * Reaches {@code snapshotId}, for a registration ({@code attempt} -1) or the catch-up a request
     * started ({@code attempt} its number). The future completes with whether the bucket covers it.
     * Any thread.
     */
    CompletableFuture<Boolean> reach(long snapshotId, long attempt) {
        CompletableFuture<Boolean> done = new CompletableFuture<>();
        o.post(() -> want(snapshotId, attempt, done));
        return done;
    }

    /**
     * Reaches {@code snapshotId} for a lookup: starts the catch-up unless it failed less than a
     * backoff ago, in which case the future fails at once.
     */
    CompletableFuture<Boolean> reachForLookup(long snapshotId) {
        CompletableFuture<Boolean> done = new CompletableFuture<>();
        o.post(
                () -> {
                    if (o.closed) {
                        done.complete(false);
                        return;
                    }
                    long attempt = o.planner.scheduleAdvance(snapshotId);
                    if (attempt < 0) {
                        done.completeExceptionally(o.planner.advanceFailed(snapshotId));
                        return;
                    }
                    want(snapshotId, attempt, done);
                });
        return done;
    }

    /** Joins the work that reaches {@code snapshotId}, or queues one. */
    void want(long snapshotId, long attempt, @Nullable CompletableFuture<Boolean> done) {
        if (o.closed) {
            if (done != null) {
                done.complete(false);
            }
            return;
        }
        if (running instanceof Reach && ((Reach) running).target == snapshotId) {
            ((Reach) running).join(attempt, done);
            return;
        }
        for (Work w : queue) {
            if (w instanceof Reach && ((Reach) w).target == snapshotId) {
                ((Reach) w).join(attempt, done);
                return;
            }
        }
        Reach reach = new Reach(snapshotId);
        reach.join(attempt, done);
        enqueue(reach);
    }

    void enqueue(Work w) {
        queue.add(w);
        startNext();
    }

    /** Starts the next work unless one runs. */
    void startNext() {
        while (running == null && !queue.isEmpty()) {
            Work w = queue.poll();
            if (o.closed) {
                w.end(false, null);
                continue;
            }
            running = w;
            try {
                w.start();
            } catch (RejectedExecutionException e) {
                // the server shuts down
                running = null;
                w.end(false, e);
            }
        }
    }

    /** Ends the running work {@code w} and starts the next one. */
    void finish(Work w, boolean covered, @Nullable Exception failure) {
        if (running != w) {
            return;
        }
        running = null;
        load = null;
        try {
            w.end(covered, failure);
        } finally {
            startNext();
        }
    }

    /** Ends every work as the bucket closes. */
    void closeWork() {
        Work w = running;
        if (w != null) {
            // its job sees the bucket closed and posts its end, which is ignored
            running = null;
            w.end(false, null);
        }
        while (!queue.isEmpty()) {
            queue.poll().end(false, null);
        }
        Recover retry = takeoverRetry;
        if (retry != null) {
            // its takeover is not tried again
            takeoverRetry = null;
            retry.end(false, null);
        }
    }

    /**
     * Starts a load of the running work {@code w}: what the load reads of the state is taken here,
     * and its long work runs on {@code executor}.
     */
    void startLoad(Work w, DvLoad l, Executor executor) {
        if (running != w || o.closed) {
            l.close();
            if (running == w) {
                finish(w, false, null);
            }
            return;
        }
        load = l;
        boolean begun;
        try {
            begun = l.begin();
            if (begun) {
                executor.execute(() -> runLoad(w, l));
            }
        } catch (RejectedExecutionException e) {
            l.abort();
            l.close();
            finish(w, false, e);
            return;
        } catch (Throwable t) {
            // No job runs, so no swap of the load comes: the load ends here, and the work ends
            // with the fault while it still runs, as after a fault of any step (logged, counted).
            // It backs off as any failed load does.
            endFaulted(l);
            if (t instanceof VirtualMachineError) {
                // the mailbox handles it as a fault of this step, then rethrows it
                throw (VirtualMachineError) t;
            }
            o.faults.fault(t);
            return;
        }
        if (!begun) {
            l.close();
            finish(w, l.notLoaded(), null);
        }
    }

    /** Runs a load's long work on its job thread, and posts its swap. */
    void runLoad(Work w, DvLoad l) {
        boolean loaded;
        Exception failure = null;
        Throwable error = null;
        try {
            loaded = l.load();
        } catch (Exception e) {
            loaded = false;
            failure = e;
        } catch (Throwable t) {
            // the swap step ends the work with it and closes the load, which frees its slot
            loaded = false;
            failure = o.faults.jobError(t, "a " + l.kind + " load");
            error = t;
        }
        boolean ok = loaded;
        Exception f = failure;
        o.post(() -> swap(w, l, ok, f));
        if (error != null) {
            DvOwnerFaults.rethrowIfFatal(error);
        }
    }

    /**
     * Swaps a load in. What runs after the swap runs on the switch share of the index-load threads,
     * and the work ends after it.
     */
    void swap(Work w, DvLoad l, boolean loaded, @Nullable Exception failure) {
        if (l.cancelled() || running != w || load != l) {
            // Not the load the owner runs: a fault stopped it, or its work ended. It swaps in
            // nothing; what it left in the latest index is recorded, and it releases what it holds
            // before the work ends and the next load starts.
            endFaulted(l);
            if (running == w) {
                finish(w, false, w.fault != null ? w.fault : failure);
            }
            return;
        }
        if (failure == null && !o.closed && loaded) {
            boolean ready;
            try {
                ready = l.readyToSwap();
            } catch (Throwable t) {
                endFaulted(l);
                throw t;
            }
            if (!ready) {
                // a part of the swap's work ran; the swap comes again after the steps posted since
                o.post(() -> swap(w, l, true, null));
                return;
            }
        }
        boolean covered = false;
        try {
            if (failure == null && !o.closed && loaded) {
                covered = l.swap();
            } else {
                l.abort();
                covered = failure == null && l.notLoaded();
            }
        } catch (Exception e) {
            failure = e;
        } catch (Throwable t) {
            // a fault: the load ends here, what it left in the latest index is recorded, and the
            // fault handling ends its work
            endFaulted(l);
            throw t;
        }
        if (failure != null || !covered || !loaded || running != w) {
            l.close();
            finish(w, covered, failure);
            return;
        }
        try {
            o.runtime
                    .switches()
                    .execute(
                            () -> {
                                try {
                                    l.afterSwap();
                                } catch (Throwable t) {
                                    o.faults.jobError(t, "the end of a " + l.kind + " load");
                                    DvOwnerFaults.rethrowIfFatal(t);
                                } finally {
                                    l.close();
                                    o.post(() -> finish(w, true, null));
                                }
                            });
        } catch (RejectedExecutionException e) {
            l.close();
            finish(w, true, null);
        }
    }

    /** Ends a load whose swap faulted: records what it left in the latest index, and closes it. */
    private void endFaulted(DvLoad l) {
        if (load == l) {
            // its work ends now, with the fault, as no swap of it comes any more
            load = null;
        }
        try {
            l.abort();
        } catch (Throwable again) {
            LOG.error(
                    "The faulted load of the deletion vectors of {} could not be aborted.",
                    o.logName,
                    again);
        } finally {
            l.close();
        }
    }

    /** What a plan decided. */
    void planned(Work w, DvOwnerPlanner.Plan plan) {
        if (running != w) {
            if (plan.load != null) {
                plan.load.close();
            }
            return;
        }
        switch (plan.kind) {
            case COVERED:
                finish(w, true, null);
                return;
            case NOT_COVERED:
                finish(w, false, null);
                return;
            case HELD:
                // a test holds the switch: tried again later, holding no thread
                w.heldBackoffMs =
                        w.heldBackoffMs == 0
                                ? HELD_RETRY_INITIAL_MS
                                : Math.min(2 * w.heldBackoffMs, HELD_RETRY_MAX_MS);
                o.runtime
                        .scheduler()
                        .schedule(
                                () -> o.post(() -> retry(w)),
                                w.heldBackoffMs,
                                TimeUnit.MILLISECONDS);
                return;
            case SWITCH:
                startLoad(w, plan.load, o.runtime.switches());
                return;
            case FULL:
                // it waits for a rebuild slot, so it runs on the rebuilds' share of the threads
                w.countRebuild();
                startLoad(w, plan.load, o.rebuild.limiter::execute);
                return;
            default:
                throw new IllegalStateException("Unknown deletion vector plan " + plan.kind);
        }
    }

    /** Runs a held work again. */
    void retry(Work w) {
        if (running != w) {
            return;
        }
        if (o.closed) {
            finish(w, false, null);
            return;
        }
        w.start();
    }

    /** Something the owner does, one at a time with the others. */
    abstract class Work {
        long heldBackoffMs;
        // a fault of the owner while the work's load was in flight: the work ends with it once
        // the load's swap step has stopped and closed the load
        @Nullable Exception fault;
        private boolean countedRebuild;

        /** Starts the work's job. */
        abstract void start();

        /** Reports how the work ended. */
        abstract void end(boolean covered, @Nullable Exception failure);

        /** Counts a full load as a rebuild in progress, which reads that do not wait see. */
        void countRebuild() {
            if (!countedRebuild) {
                countedRebuild = true;
                o.rebuild.pending++;
            }
        }

        void uncountRebuild() {
            if (countedRebuild) {
                countedRebuild = false;
                o.rebuild.pending--;
            }
        }
    }

    /** Reaches a snapshot, for the requests and registrations that want it. */
    final class Reach extends Work {
        final long target;
        private final List<Long> attempts = new ArrayList<>();
        private final List<CompletableFuture<Boolean>> waiters = new ArrayList<>();

        Reach(long target) {
            this.target = target;
        }

        void join(long attempt, @Nullable CompletableFuture<Boolean> done) {
            attempts.add(attempt);
            if (done != null) {
                waiters.add(done);
            }
        }

        @Override
        void start() {
            DvOwnerPlanner.PlanInput input = o.planner.planInput(target);
            if (input == null) {
                // covered already, or passed
                finish(this, o.coveredSnapshotId == target, null);
                return;
            }
            o.runtime
                    .switches()
                    .execute(
                            () -> {
                                DvOwnerPlanner.Plan plan;
                                try {
                                    plan = o.planner.plan(input);
                                } catch (Exception e) {
                                    o.post(() -> finish(this, false, e));
                                    return;
                                } catch (Throwable t) {
                                    Exception e = o.faults.jobError(t, "a plan");
                                    o.post(() -> finish(this, false, e));
                                    DvOwnerFaults.rethrowIfFatal(t);
                                    return;
                                }
                                o.post(() -> planned(this, plan));
                            });
        }

        @Override
        void end(boolean covered, @Nullable Exception failure) {
            uncountRebuild();
            if (failure != null) {
                LOG.warn("Switching the deletion vectors to snapshot {} failed.", target, failure);
            }
            for (long attempt : attempts) {
                o.planner.advanceDone(target, attempt, covered, failure);
            }
            // a lookup that waited sees the state as the work left it
            o.publish();
            for (CompletableFuture<Boolean> w : waiters) {
                if (failure != null) {
                    w.completeExceptionally(failure);
                } else {
                    w.complete(covered);
                }
            }
        }
    }

    /** Takes the bucket over: covers its readable snapshot, or the empty lake. */
    final class Recover extends Work {
        private final RecoverySource src;
        final CompletableFuture<Boolean> done = new CompletableFuture<>();

        Recover(RecoverySource src) {
            this.src = src;
        }

        @Override
        void start() {
            countRebuild();
            o.rebuild.limiter.execute(
                    () -> {
                        long[] readable;
                        List<PositionIndexStore.ChainEntry> chain;
                        try {
                            readable = src.readableSnapshot();
                            chain = readable == null ? null : src.chain();
                        } catch (Exception e) {
                            o.post(() -> finish(this, false, e));
                            return;
                        } catch (Throwable t) {
                            Exception e = o.faults.jobError(t, "a recovery");
                            o.post(() -> finish(this, false, e));
                            DvOwnerFaults.rethrowIfFatal(t);
                            return;
                        }
                        o.post(() -> recovered(readable, chain));
                    });
        }

        /** Plans the recovery once the readable snapshot is known. */
        private void recovered(
                @Nullable long[] readable, @Nullable List<PositionIndexStore.ChainEntry> chain) {
            if (running != this) {
                return;
            }
            if (readable == null) {
                o.coverTheEmptyLake();
                finish(this, true, null);
                return;
            }
            if (o.coveredSnapshotId >= 0) {
                // The bucket covers a snapshot already: plan from there.
                uncountRebuild();
                DvOwnerPlanner.PlanInput input = o.planner.planInput(readable[0]);
                if (input == null) {
                    finish(this, o.coveredSnapshotId == readable[0], null);
                    return;
                }
                o.runtime
                        .switches()
                        .execute(
                                () -> {
                                    DvOwnerPlanner.Plan plan;
                                    try {
                                        plan = o.planner.plan(input);
                                    } catch (Exception e) {
                                        o.post(() -> finish(this, false, e));
                                        return;
                                    } catch (Throwable t) {
                                        Exception e = o.faults.jobError(t, "a plan");
                                        o.post(() -> finish(this, false, e));
                                        DvOwnerFaults.rethrowIfFatal(t);
                                        return;
                                    }
                                    o.post(() -> planned(this, plan));
                                });
                return;
            }
            startLoad(
                    this,
                    new DvLoad.FullLoad(o, src, readable[0], readable[1], chain),
                    o.rebuild.limiter::execute);
        }

        @Override
        void end(boolean covered, @Nullable Exception failure) {
            uncountRebuild();
            if (failure != null) {
                LOG.warn(
                        "Recovering the deletion vectors failed; the next union read or lookup"
                                + " of the bucket rebuilds them.",
                        failure);
                done.completeExceptionally(failure);
            } else {
                done.complete(covered);
            }
        }
    }
}
