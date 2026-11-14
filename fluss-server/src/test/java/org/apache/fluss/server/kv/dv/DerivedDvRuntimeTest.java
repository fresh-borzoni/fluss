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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.DeletionVectorsNotReadyException;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DerivedDvRuntime}, the deletion vector threads of a tablet server. */
class DerivedDvRuntimeTest {

    @TempDir Path tempDir;

    /**
     * After the server's executors shut down, a change can no longer schedule its resolve and a
     * budget over its limit can no longer free in the background. Both are logged, not thrown.
     */
    @Test
    void testWorkAfterShutdownIsNotThrown() throws Exception {
        DerivedDvRuntime executors = new DerivedDvRuntime();
        DerivedDvHeapBudget budget = new DerivedDvHeapBudget(1, executors);
        DerivedDvManager manager =
                new DerivedDvManager(
                        DvRocksDB.open(tempDir.resolve("dv").toString()),
                        () -> 0L,
                        DvTestManagers.CHECKPOINT_INTERVAL,
                        DerivedDvMetrics.noop(),
                        budget,
                        executors);
        executors.shutdown();
        try {
            DvEntry change = new DvEntry("k1".getBytes(StandardCharsets.UTF_8), 10, 10, false, -1);
            assertThatCode(
                            () ->
                                    DvTestManagers.handOver(
                                            manager, Collections.singletonList(change)))
                    .doesNotThrowAnyException();
            assertThat(budget.totalBytes()).isGreaterThan(budget.limitBytes());
        } finally {
            manager.close();
        }
    }

    /**
     * The runtime names its threads, registers each pool's or share's queued and active tasks, and
     * stops every pool on shutdown: a request after it is answered as not ready, other work is
     * rejected.
     */
    @Test
    void testPoolsAreNamedMeteredAndShutDown() throws Exception {
        Map<String, Metric> metrics = new ConcurrentHashMap<>();
        MetricRegistry registry =
                new NOPMetricRegistry() {
                    @Override
                    public void register(Metric metric, String name, AbstractMetricGroup group) {
                        metrics.put(
                                String.join(".", group.getScopeComponents()) + "." + name, metric);
                    }
                };
        DerivedDvRuntime runtime = new DerivedDvRuntime(new Configuration());
        runtime.registerMetrics(new TabletServerMetricGroup(registry, "fluss", "host", "rack", 0));
        for (String pool :
                new String[] {
                    "request",
                    "state",
                    "resolver",
                    "advance",
                    "rebuild",
                    "checkpoint",
                    "change-list",
                    "scheduler"
                }) {
            for (String gauge : new String[] {"queued", "active"}) {
                assertThat(metrics.keySet())
                        .anySatisfy(
                                id ->
                                        assertThat(id)
                                                .contains("dvPool")
                                                .contains("." + pool + ".")
                                                .endsWith("." + gauge));
            }
        }

        assertThat(threadOf(runtime.bucketState()))
                .startsWith("kv-deletion-vectors-bucket-state-thread-");
        assertThat(threadOf(runtime.keyLookup()))
                .startsWith("kv-deletion-vectors-key-lookup-thread-");
        assertThat(threadOf(runtime.switches()))
                .startsWith("kv-deletion-vectors-index-load-thread-");
        assertThat(threadOf(runtime.rebuilds()::execute))
                .startsWith("kv-deletion-vectors-index-load-thread-");
        assertThat(threadOf(runtime.scheduler()))
                .startsWith("kv-deletion-vectors-scheduler-thread-");
        assertThat(threadOf(runtime.checkpoints())).startsWith("kv-deletion-vectors-io-thread-");
        assertThat(
                        runtime.requests()
                                .submit(() -> Thread.currentThread().getName())
                                .get(10, TimeUnit.SECONDS))
                .startsWith("kv-deletion-vectors-request-thread-");

        runtime.shutdown();
        assertThatThrownBy(() -> runtime.requests().submit(() -> 1).join())
                .hasCauseInstanceOf(DeletionVectorsNotReadyException.class);
        assertThatThrownBy(() -> runtime.bucketState().execute(() -> {}))
                .isInstanceOf(RejectedExecutionException.class);
        assertThatThrownBy(() -> runtime.rebuilds().execute(() -> {}))
                .isInstanceOf(RejectedExecutionException.class);
        assertThatThrownBy(() -> runtime.switches().execute(() -> {}))
                .isInstanceOf(RejectedExecutionException.class);
        assertThatThrownBy(() -> runtime.checkpoints().execute(() -> {}))
                .isInstanceOf(RejectedExecutionException.class);
    }

    /**
     * Rebuilds and switches share the index-load threads, each on its own share: a switch runs
     * while every rebuild slot is held by a rebuild that blocks, and switches beyond their share
     * wait without a thread.
     */
    @Test
    void testASwitchRunsWhileEveryRebuildSlotIsHeld() throws Exception {
        DerivedDvRuntime runtime = new DerivedDvRuntime(new Configuration());
        int slots = ConfigOptions.KV_DELETION_VECTORS_MAX_CONCURRENT_REBUILDS.defaultValue();
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch rebuilding = new CountDownLatch(slots);
        try {
            DerivedDvRebuildLimiter limiter = runtime.rebuilds();
            // every slot held, and one more rebuild waiting for a slot
            for (int i = 0; i <= slots; i++) {
                limiter.execute(
                        () -> {
                            try {
                                limiter.acquire();
                                try {
                                    rebuilding.countDown();
                                    release.await();
                                } finally {
                                    limiter.release();
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });
            }
            assertThat(rebuilding.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(limiter.availableSlots()).isZero();
            assertThat(limiter.lane().queued()).isEqualTo(1);

            CompletableFuture<Void> switched = new CompletableFuture<>();
            runtime.switches().execute(() -> switched.complete(null));
            switched.get(10, TimeUnit.SECONDS);

            // two switches that block take the share; a third waits without a thread
            CountDownLatch switching = new CountDownLatch(DerivedDvRuntime.SWITCH_THREADS);
            for (int i = 0; i <= DerivedDvRuntime.SWITCH_THREADS; i++) {
                runtime.switches()
                        .execute(
                                () -> {
                                    switching.countDown();
                                    awaitQuietly(release);
                                });
            }
            assertThat(switching.await(10, TimeUnit.SECONDS)).isTrue();
            BoundedConcurrencyExecutor switches = (BoundedConcurrencyExecutor) runtime.switches();
            assertThat(switches.active()).isEqualTo(DerivedDvRuntime.SWITCH_THREADS);
            assertThat(switches.queued()).isEqualTo(1);
        } finally {
            release.countDown();
            runtime.shutdown();
        }
    }

    /** An index checkpoint runs on the IO pool while rebuilds and switches take their threads. */
    @Test
    void testACheckpointRunsWhileRebuildsAndSwitchesRun() throws Exception {
        ExecutorService io = Executors.newFixedThreadPool(2);
        DerivedDvRuntime runtime = new DerivedDvRuntime(new Configuration(), io);
        int slots = ConfigOptions.KV_DELETION_VECTORS_MAX_CONCURRENT_REBUILDS.defaultValue();
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch running = new CountDownLatch(slots + DerivedDvRuntime.SWITCH_THREADS);
        try {
            for (int i = 0; i < slots; i++) {
                runtime.rebuilds()
                        .execute(
                                () -> {
                                    running.countDown();
                                    awaitQuietly(release);
                                });
            }
            for (int i = 0; i < DerivedDvRuntime.SWITCH_THREADS; i++) {
                runtime.switches()
                        .execute(
                                () -> {
                                    running.countDown();
                                    awaitQuietly(release);
                                });
            }
            assertThat(running.await(10, TimeUnit.SECONDS)).isTrue();

            CompletableFuture<Void> checkpointed = new CompletableFuture<>();
            runtime.checkpoints().execute(() -> checkpointed.complete(null));
            checkpointed.get(10, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            runtime.shutdown();
            io.shutdown();
        }
    }

    /**
     * A delayed task runs on its pool, not on the scheduler: a task that blocks there leaves the
     * scheduler free to fire the next timer on time.
     */
    @Test
    void testTimersPostAndNeverBlock() throws Exception {
        DerivedDvRuntime runtime = new DerivedDvRuntime(new Configuration());
        CountDownLatch release = new CountDownLatch(1);
        try {
            CompletableFuture<String> blocked = new CompletableFuture<>();
            runtime.executeAfter(
                    10,
                    runtime.keyLookup(),
                    () -> {
                        blocked.complete(Thread.currentThread().getName());
                        awaitQuietly(release);
                    },
                    () -> {});
            assertThat(blocked.get(10, TimeUnit.SECONDS))
                    .startsWith("kv-deletion-vectors-key-lookup-thread-");

            CompletableFuture<Void> fired = new CompletableFuture<>();
            runtime.scheduler().schedule(() -> fired.complete(null), 10, TimeUnit.MILLISECONDS);
            fired.get(10, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            runtime.shutdown();
        }
    }

    /** A delayed task whose pool shut down meanwhile runs its rejection, on the scheduler. */
    @Test
    void testADelayedTaskThatThePoolRejectsRunsItsRejection() throws Exception {
        DerivedDvRuntime runtime = new DerivedDvRuntime(new Configuration());
        ExecutorService closed = Executors.newSingleThreadExecutor();
        closed.shutdown();
        try {
            CompletableFuture<Void> rejected = new CompletableFuture<>();
            runtime.executeAfter(10, closed, () -> {}, () -> rejected.complete(null));
            rejected.get(10, TimeUnit.SECONDS);
        } finally {
            runtime.shutdown();
        }
    }

    /**
     * A {@link VirtualMachineError} that a task on a plain pool lets through reaches the threads'
     * handler: the key lookups, both shares of the index-load threads, the index checkpoints and
     * the change lists.
     */
    @Test
    void testAVirtualMachineErrorOnAPlainPoolReachesTheHandler() throws Exception {
        RecordingHandler handler = new RecordingHandler();
        DerivedDvRuntime runtime = new DerivedDvRuntime(1, handler);
        try {
            runtime.keyLookup().execute(fail(new OutOfMemoryError("key lookup")));
            runtime.switches().execute(fail(new OutOfMemoryError("switch")));
            runtime.rebuilds().execute(fail(new OutOfMemoryError("rebuild")));
            runtime.checkpoints().execute(fail(new OutOfMemoryError("checkpoint")));
            runtime.changeLists().execute(fail(new OutOfMemoryError("change list")));
            handler.await(5);
            assertThat(handler.messages())
                    .containsExactlyInAnyOrder(
                            "key lookup", "switch", "rebuild", "checkpoint", "change list");
        } finally {
            runtime.shutdown();
        }
    }

    /**
     * The scheduler takes what a timer throws out of the timer's future: a {@link
     * VirtualMachineError} reaches the handler, anything else is logged.
     */
    @Test
    void testOnlyAVirtualMachineErrorOfATimerReachesTheHandler() throws Exception {
        RecordingHandler handler = new RecordingHandler();
        DerivedDvRuntime runtime = new DerivedDvRuntime(1, handler);
        try {
            runtime.scheduler()
                    .schedule(fail(new IllegalStateException("timer")), 0, TimeUnit.MILLISECONDS);
            runtime.scheduler()
                    .schedule(fail(new OutOfMemoryError("fatal timer")), 0, TimeUnit.MILLISECONDS);
            runtime.scheduler().execute(fail(new OutOfMemoryError("fatal task")));
            handler.await(2);
            // a timer after them has run, so the first one reached no handler
            runtime.scheduler().schedule(() -> {}, 0, TimeUnit.MILLISECONDS).get(10, SECONDS);
            assertThat(handler.messages()).containsExactly("fatal timer", "fatal task");
        } finally {
            runtime.shutdown();
        }
    }

    /**
     * A request that fails is answered with its failure; a {@link VirtualMachineError} then also
     * reaches the handler.
     */
    @Test
    void testARequestIsAnsweredBeforeAVirtualMachineErrorReachesTheHandler() throws Exception {
        RecordingHandler handler = new RecordingHandler();
        DerivedDvRuntime runtime = new DerivedDvRuntime(1, handler);
        try {
            CompletableFuture<Object> plain =
                    runtime.requests()
                            .submit(
                                    () -> {
                                        throw new IllegalStateException("plain");
                                    });
            assertThatThrownBy(plain::join).hasCauseInstanceOf(IllegalStateException.class);
            CompletableFuture<Object> fatal =
                    runtime.requests()
                            .submit(
                                    () -> {
                                        throw new OutOfMemoryError("request");
                                    });
            assertThatThrownBy(fatal::join).hasCauseInstanceOf(OutOfMemoryError.class);
            handler.await(1);
            assertThat(handler.messages()).containsExactly("request");
        } finally {
            runtime.shutdown();
        }
    }

    /**
     * An owner step that fails is the bucket's fault; a {@link VirtualMachineError} reaches the
     * handler once the fault is handled.
     */
    @Test
    void testAFailedOwnerStepIsAFaultAndAVirtualMachineErrorReachesTheHandler() throws Exception {
        RecordingHandler handler = new RecordingHandler();
        DerivedDvRuntime runtime = new DerivedDvRuntime(1, handler);
        DerivedDvMetrics metrics = DerivedDvMetrics.noop();
        DerivedDvManager manager =
                new DerivedDvManager(
                        DvRocksDB.open(tempDir.resolve("faults").toString()),
                        () -> 0L,
                        DvTestManagers.CHECKPOINT_INTERVAL,
                        metrics,
                        new DerivedDvHeapBudget(Long.MAX_VALUE, runtime),
                        runtime);
        try {
            DvOwner owner = owner(manager);
            owner.post(fail(new IllegalStateException("step")));
            owner.post(fail(new OutOfMemoryError("fatal step")));
            handler.await(1);
            assertThat(handler.messages()).containsExactly("fatal step");
            assertThat(metrics.ownerFaults()).isEqualTo(2L);
        } finally {
            manager.setCloseWaitMillis(100);
            manager.close();
            runtime.shutdown();
        }
    }

    private static Runnable fail(RuntimeException e) {
        return () -> {
            throw e;
        };
    }

    private static Runnable fail(Error e) {
        return () -> {
            throw e;
        };
    }

    private static DvOwner owner(DerivedDvManager manager) throws Exception {
        Field f = DerivedDvManager.class.getDeclaredField("owner");
        f.setAccessible(true);
        return (DvOwner) f.get(manager);
    }

    /** Records the errors handed to it, in place of the handler that stops the process. */
    private static final class RecordingHandler implements Thread.UncaughtExceptionHandler {
        private final List<Throwable> errors = new CopyOnWriteArrayList<>();

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            errors.add(e);
        }

        void await(int count) throws InterruptedException {
            long deadline = System.currentTimeMillis() + 10_000;
            while (errors.size() < count && System.currentTimeMillis() < deadline) {
                Thread.sleep(5);
            }
            assertThat(errors).hasSize(count);
        }

        List<String> messages() {
            List<String> out = new ArrayList<>();
            for (Throwable e : errors) {
                out.add(e.getMessage());
            }
            return out;
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String threadOf(Executor executor) throws Exception {
        CompletableFuture<String> name = new CompletableFuture<>();
        executor.execute(() -> name.complete(Thread.currentThread().getName()));
        return name.get(10, TimeUnit.SECONDS);
    }
}
