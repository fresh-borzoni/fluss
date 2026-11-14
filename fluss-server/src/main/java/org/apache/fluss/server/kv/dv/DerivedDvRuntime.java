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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.utils.FatalExitExceptionHandler;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;

/**
 * The threads of a tablet server's deletion vectors: the one place that creates them, names them,
 * sizes them and shuts them down. The server's {@code KvManager} creates one runtime and shuts it
 * down after its buckets closed, so servers that share a JVM do not share threads. Buckets get the
 * runtime, not single executors. Work submitted after the shutdown is rejected, which the callers
 * log or answer as not ready.
 *
 * <table>
 *   <tr><th>Pool</th><th>Threads</th><th>What runs there</th><th>May it block?</th></tr>
 *   <tr><td>{@code kv-deletion-vectors-bucket-state}</td><td>2</td><td>the buckets' owner steps,
 *       one bucket at a time each</td><td>no: a step never waits for a job, a request or another
 *       step</td></tr>
 *   <tr><td>{@code kv-deletion-vectors-request}</td><td>{@link
 *       DerivedDvRequestExecutor#DEFAULT_THREADS} (half the cores, 2 to 16), 1,024
 *       queued</td><td>union read masks, pins, tiering's position lookups</td><td>yes: a read waits
 *       at most 500 ms for its bucket's owner, a lookup reads RocksDB; kept off the RPC
 *       threads</td></tr>
 *   <tr><td>{@code kv-deletion-vectors-key-lookup}</td><td>4</td><td>lookups of the lake
 *       positions of changed keys, 10,000 keys per task, and full-resolve chunks</td><td>briefly:
 *       one RocksDB multi-get per task</td></tr>
 *   <tr><td>{@code kv-deletion-vectors-index-load}</td><td>2 + {@code
 *       kv.deletion-vectors.max-concurrent-rebuilds}</td><td>planning and switch loads, at most 2
 *       at a time; full rebuilds and change list replays, at most one per rebuild slot</td><td>yes:
 *       index downloads, RocksDB ingest and log replays, for seconds to minutes</td></tr>
 *   <tr><td>{@code kv-deletion-vectors-scheduler}</td><td>1</td><td>timers: resolve delays,
 *       retry backoffs, checkpoint delays, idle pinned cuts; asking the buckets over the heap budget
 *       to free</td><td>no: it only posts work to owners and pools</td></tr>
 * </table>
 *
 * <p>Planning and switch loads, and rebuilds, share the index-load pool, but each kind runs on its
 * own share of its threads ({@link BoundedConcurrencyExecutor}): work beyond the share waits
 * without a thread, so a long rebuild never holds up a switch. Exports and uploads of index
 * checkpoints run on the server's IO pool ({@code server.io-pool.size}), at most 2 at a time. The
 * change lists saved with KV snapshots (their segments, the deletes of segments no snapshot
 * references, the read-back of the committed metadata) and the expiry checks of restored KV views
 * run on a share of their own of the IO pool, also at most 2 at a time, so a long index export
 * never makes a KV snapshot's change list late.
 *
 * <p>Errors follow Fluss's other server pools: the threads hand a {@link VirtualMachineError} to
 * {@code FatalExitExceptionHandler}, which stops the tablet server, after the job, step or request
 * reported it; any other failure is posted to its bucket's owner, answered to its request, or
 * logged.
 *
 * <p>Each pool's or share's queued and active tasks are gauges in the server's {@code dvPool}
 * group, by {@code pool} ({@link #registerMetrics}). {@link #shutdown} stops the pools in a fixed
 * order: requests first, so no new work comes in, then the pools that start work for others (index
 * loads, checkpoints, key lookups), then the owners' steps and the scheduler.
 */
@Internal
public final class DerivedDvRuntime {

    private static final Logger LOG = LoggerFactory.getLogger(DerivedDvRuntime.class);

    static final int STATE_THREADS = 2;
    static final int KEY_LOOKUP_THREADS = 4;
    static final int SWITCH_THREADS = 2;
    static final int CHECKPOINT_THREADS = 2;
    static final int CHANGE_LIST_THREADS = 2;
    // the IO pool of a runtime whose server gives it none: a thread for each slot of its lanes
    static final int IO_THREADS = CHECKPOINT_THREADS + CHANGE_LIST_THREADS;

    private final DerivedDvRequestExecutor requests;
    private final ThreadPoolExecutor bucketState;
    private final ThreadPoolExecutor keyLookup;
    private final ThreadPoolExecutor indexLoad;
    private final BoundedConcurrencyExecutor switches;
    private final DerivedDvRebuildLimiter rebuilds;
    private final ScheduledThreadPoolExecutor scheduler;
    private final BoundedConcurrencyExecutor checkpoints;
    private final BoundedConcurrencyExecutor changeLists;
    // the IO pool the runtime created as the server gave it none, which it then shuts down
    @Nullable private final ExecutorService ownIo;

    /**
     * The runtime of a tablet server with {@code conf}: its rebuild slots come from the option, and
     * its checkpoints run on the server's {@code ioExecutor}.
     */
    public DerivedDvRuntime(Configuration conf, ExecutorService ioExecutor) {
        this(
                KEY_LOOKUP_THREADS,
                conf.get(ConfigOptions.KV_DELETION_VECTORS_MAX_CONCURRENT_REBUILDS),
                DerivedDvRequestExecutor.DEFAULT_THREADS,
                DerivedDvRequestExecutor.DEFAULT_QUEUED,
                ioExecutor,
                FatalExitExceptionHandler.INSTANCE);
    }

    /** As above, with an IO pool of the runtime's own, for a server that gives it none. */
    public DerivedDvRuntime(Configuration conf) {
        this(
                KEY_LOOKUP_THREADS,
                conf.get(ConfigOptions.KV_DELETION_VECTORS_MAX_CONCURRENT_REBUILDS),
                DerivedDvRequestExecutor.DEFAULT_THREADS,
                DerivedDvRequestExecutor.DEFAULT_QUEUED,
                null,
                FatalExitExceptionHandler.INSTANCE);
    }

    /** A runtime with the default sizes. */
    @VisibleForTesting
    DerivedDvRuntime() {
        this(KEY_LOOKUP_THREADS);
    }

    /** A runtime with {@code keyLookupThreads} key-lookup threads. */
    @VisibleForTesting
    DerivedDvRuntime(int keyLookupThreads) {
        this(keyLookupThreads, FatalExitExceptionHandler.INSTANCE);
    }

    /**
     * A runtime whose threads hand a {@link VirtualMachineError} to {@code fatalErrorHandler}
     * instead of stopping the process.
     */
    @VisibleForTesting
    DerivedDvRuntime(int keyLookupThreads, Thread.UncaughtExceptionHandler fatalErrorHandler) {
        this(
                keyLookupThreads,
                ConfigOptions.KV_DELETION_VECTORS_MAX_CONCURRENT_REBUILDS.defaultValue(),
                DerivedDvRequestExecutor.DEFAULT_THREADS,
                DerivedDvRequestExecutor.DEFAULT_QUEUED,
                null,
                fatalErrorHandler);
    }

    private DerivedDvRuntime(
            int keyLookupThreads,
            int maxConcurrentRebuilds,
            int requestThreads,
            int queued,
            @Nullable ExecutorService ioExecutor,
            Thread.UncaughtExceptionHandler fatalErrorHandler) {
        this.requests =
                new DerivedDvRequestExecutor(
                        requestPool(requestThreads, queued, fatalErrorHandler));
        this.bucketState =
                fixed(STATE_THREADS, "kv-deletion-vectors-bucket-state", fatalErrorHandler);
        this.keyLookup =
                fixed(keyLookupThreads, "kv-deletion-vectors-key-lookup", fatalErrorHandler);
        this.indexLoad =
                idleEnding(
                        SWITCH_THREADS + maxConcurrentRebuilds,
                        "kv-deletion-vectors-index-load",
                        fatalErrorHandler);
        this.switches = new BoundedConcurrencyExecutor(indexLoad, SWITCH_THREADS);
        this.rebuilds =
                new DerivedDvRebuildLimiter(
                        maxConcurrentRebuilds,
                        new BoundedConcurrencyExecutor(indexLoad, maxConcurrentRebuilds));
        this.scheduler = new Scheduler(fatalErrorHandler);
        this.ownIo =
                ioExecutor == null
                        ? fixed(IO_THREADS, "kv-deletion-vectors-io", fatalErrorHandler)
                        : null;
        Executor io = ioExecutor == null ? ownIo : ioExecutor;
        this.checkpoints = new BoundedConcurrencyExecutor(io, CHECKPOINT_THREADS);
        this.changeLists = new BoundedConcurrencyExecutor(io, CHANGE_LIST_THREADS);
    }

    /** The executor of the server's deletion vector requests. */
    public DerivedDvRequestExecutor requests() {
        return requests;
    }

    /** The server's rebuild slots, and their share of the index-load threads. */
    public DerivedDvRebuildLimiter rebuilds() {
        return rebuilds;
    }

    /** Runs the buckets' owner steps. */
    Executor bucketState() {
        return bucketState;
    }

    /** Looks up the lake positions of changed keys. */
    Executor keyLookup() {
        return keyLookup;
    }

    /** Plans catch-ups and runs switch loads, on their share of the index-load threads. */
    Executor switches() {
        return switches;
    }

    /** Exports and uploads index checkpoints, on the server's IO pool. */
    Executor checkpoints() {
        return checkpoints;
    }

    /**
     * Writes and deletes the segments of the change lists saved with KV snapshots, reads back the
     * committed metadata, and checks the restored KV views for expiry, on a share of the server's
     * IO pool apart from the index checkpoints.
     */
    Executor changeLists() {
        return changeLists;
    }

    /** Runs timers, which only post work to owners and pools. */
    ScheduledExecutorService scheduler() {
        return scheduler;
    }

    /**
     * Runs {@code task} on {@code executor} after {@code delayMs}, posted by the scheduler; at once
     * if there is no delay. If {@code executor} rejects the task once its delay passed, {@code
     * onRejected} runs on the scheduler. Throws {@link RejectedExecutionException} if the delay
     * cannot be scheduled.
     */
    void executeAfter(long delayMs, Executor executor, Runnable task, Runnable onRejected) {
        if (delayMs <= 0) {
            executor.execute(task);
            return;
        }
        scheduler.schedule(
                () -> {
                    try {
                        executor.execute(task);
                    } catch (RejectedExecutionException e) {
                        onRejected.run();
                    }
                },
                delayMs,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Registers each pool's or share's queued and active tasks as gauges {@code queued} and {@code
     * active}, in the group {@code dvPool} of {@code group} with the variable {@code pool}:
     * request, state, resolver (the key lookups), advance (planning and switch loads), rebuild,
     * checkpoint, change-list, and scheduler.
     */
    public void registerMetrics(MetricGroup group) {
        MetricGroup pools = group.addGroup("dvPool");
        ThreadPoolExecutor requestPool = requests.pool();
        register(
                pools, "request", () -> requestPool.getQueue().size(), requestPool::getActiveCount);
        register(pools, "state", () -> bucketState.getQueue().size(), bucketState::getActiveCount);
        register(pools, "resolver", () -> keyLookup.getQueue().size(), keyLookup::getActiveCount);
        register(pools, "advance", switches::queued, switches::active);
        register(pools, "rebuild", rebuilds.lane()::queued, rebuilds.lane()::active);
        register(pools, "checkpoint", checkpoints::queued, checkpoints::active);
        register(pools, "change-list", changeLists::queued, changeLists::active);
        register(pools, "scheduler", () -> scheduler.getQueue().size(), scheduler::getActiveCount);
    }

    private static void register(
            MetricGroup pools, String pool, IntSupplier queued, IntSupplier active) {
        MetricGroup g = pools.addGroup("pool", pool);
        g.gauge("queued", queued::getAsInt);
        g.gauge("active", active::getAsInt);
    }

    /**
     * Stops accepting work, in order: requests, then the pools that start work for others (index
     * loads, checkpoints, key lookups), then the owners' steps and the scheduler. Tasks that run
     * finish, and so do the owners' queued steps; delayed timers and waiting shares are dropped, as
     * every bucket is closed by then. The threads end once idle. The server's IO pool is the
     * server's to shut down.
     */
    public void shutdown() {
        requests.close();
        switches.shutdown();
        rebuilds.lane().shutdown();
        indexLoad.shutdown();
        checkpoints.shutdown();
        changeLists.shutdown();
        if (ownIo != null) {
            ownIo.shutdown();
        }
        keyLookup.shutdown();
        bucketState.shutdown();
        scheduler.shutdown();
    }

    // ---- how the pools are made ----

    /** A pool of {@code threads} request threads that queue {@code queued} tasks at most. */
    static ThreadPoolExecutor requestPool(
            int threads, int queued, Thread.UncaughtExceptionHandler fatalErrorHandler) {
        return new ThreadPoolExecutor(
                threads,
                threads,
                60,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queued),
                new ExecutorThreadFactory("kv-deletion-vectors-request", fatalErrorHandler));
    }

    /** A pool of {@code threads} threads, which end once idle for a minute. */
    static ThreadPoolExecutor idleEnding(
            int threads, String name, Thread.UncaughtExceptionHandler fatalErrorHandler) {
        ThreadPoolExecutor pool =
                new ThreadPoolExecutor(
                        threads,
                        threads,
                        60,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(),
                        new ExecutorThreadFactory(name, fatalErrorHandler));
        pool.allowCoreThreadTimeOut(true);
        return pool;
    }

    private static ThreadPoolExecutor fixed(
            int threads, String name, Thread.UncaughtExceptionHandler fatalErrorHandler) {
        return new ThreadPoolExecutor(
                threads,
                threads,
                0,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new ExecutorThreadFactory(name, fatalErrorHandler));
    }

    /**
     * The scheduler's pool. A scheduled pool keeps what a task throws in the task's future, so this
     * one takes it out: a {@link VirtualMachineError} goes to the thread's uncaught exception
     * handler, as on the other pools, and anything else is logged.
     */
    private static final class Scheduler extends ScheduledThreadPoolExecutor {

        Scheduler(Thread.UncaughtExceptionHandler fatalErrorHandler) {
            super(1, new ExecutorThreadFactory("kv-deletion-vectors-scheduler", fatalErrorHandler));
            setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        }

        @Override
        protected void afterExecute(Runnable task, Throwable thrown) {
            super.afterExecute(task, thrown);
            if (!(task instanceof Future) || !((Future<?>) task).isDone()) {
                return;
            }
            try {
                ((Future<?>) task).get();
            } catch (ExecutionException e) {
                Throwable failure = e.getCause();
                if (failure instanceof VirtualMachineError) {
                    Thread t = Thread.currentThread();
                    t.getUncaughtExceptionHandler().uncaughtException(t, failure);
                } else {
                    LOG.error("A deletion vector timer failed.", failure);
                }
            } catch (CancellationException | InterruptedException e) {
                // cancelled: nothing failed
            }
        }
    }
}
