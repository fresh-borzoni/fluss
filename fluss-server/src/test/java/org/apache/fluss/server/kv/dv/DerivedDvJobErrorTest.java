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

import org.apache.fluss.server.kv.dv.DerivedDvManagerTest.Index;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.dict;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.lakeMasks;
import static org.apache.fluss.server.kv.dv.DvTestManagers.hooks;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A fatal error of the virtual machine on a job thread of a bucket, in each job that runs off the
 * owner and is not a load: a resolve chunk, a full resolve, a switch's derivation and an index
 * checkpoint. The job posts its failure, so what it did ends as after any failed job; the error is
 * logged and counted once as an owner fault; then it is rethrown on the job's thread, which the
 * pools of this test record.
 */
class DerivedDvJobErrorTest extends DerivedDvManagerTestBase {

    private final ManualExecutor steps = new ManualExecutor();
    private DerivedDvRuntime runtime;
    private RecordingPool advancer;
    private RecordingPool resolver;
    private RecordingPool checkpointer;
    private DvRocksDB ownedDb;
    private DerivedDvManager m;

    @BeforeEach
    void setUpRuntime() throws Exception {
        runtime = new DerivedDvRuntime();
        advancer = replaceShare("switches", DerivedDvRuntime.SWITCH_THREADS);
        resolver = replacePool("keyLookup");
        checkpointer = replaceShare("checkpoints", DerivedDvRuntime.CHECKPOINT_THREADS);
        ownedDb = DvRocksDB.open(tempDir.resolve("owned").toString());
    }

    @AfterEach
    void tearDownRuntime() throws Exception {
        if (m != null && !m.isClosed()) {
            m.setCloseWaitMillis(10);
            pump(async(() -> m.close()));
        }
        ownedDb.close();
        runtime.shutdown();
    }

    /** A resolve chunk: its keys are looked up again, and the masks get their positions. */
    @Test
    void testAnErrorInAResolveChunk() throws Exception {
        newManager(DvTestManagers.CHECKPOINT_INTERVAL);
        coverSnapshot100();
        pumpUntil(() -> m.state().lookupsDrained);
        hooks(m).setBeforeResolveLookup(failOnce());
        src.append("k3", 12, 12);
        DvTestManagers.handOver(m, Collections.singletonList(src.written("k3", 12, 12)));
        highWatermark.set(13);

        assertEscaped(resolver);
        assertThat(metrics.ownerFaults()).isEqualTo(1L);
        assertThat(lakeMasks(pump(read(100))).get(F1)).containsExactlyInAnyOrder(0L, 1L, 2L);
    }

    /** A full resolve: the change list is dropped, as after any failed full resolve. */
    @Test
    void testAnErrorInAFullResolve() throws Exception {
        newManager(DvTestManagers.CHECKPOINT_INTERVAL);
        // the recovery's install looks up the lake keys of its change list in a full resolve
        hooks(m).setBeforeResolveLookup(failOnce());
        coverSnapshot100();

        assertEscaped(resolver);
        assertThat(metrics.ownerFaults()).isEqualTo(1L);
        pumpUntil(() -> !m.state().changeListPresent);
    }

    /** A switch's derivation: every lake key is looked up again, as after a failed derivation. */
    @Test
    void testAnErrorInADerivation() throws Exception {
        newManager(DvTestManagers.CHECKPOINT_INTERVAL);
        coverSnapshot100();
        pumpUntil(() -> m.state().lookupsDrained);
        // 101 names k1 again; the switch drops k2's change, whose old position the derivation
        // reads in its second chunk
        src.publish(101, 2, 12, dict(2, F2), new Index().put("k1", 2, 0).build());
        src.readable = new long[] {101, 12};
        AtomicInteger chunks = new AtomicInteger();
        hooks(m).setAfterDerivationChunk(
                        () -> {
                            if (chunks.incrementAndGet() == 2) {
                                throw new OutOfMemoryError("injected");
                            }
                        });
        assertThat(pump(async(() -> m.ensureCovered(101)))).isTrue();

        assertEscaped(advancer);
        assertThat(chunks.get()).isEqualTo(2);
        assertThat(metrics.ownerFaults()).isEqualTo(1L);
        // k2 is in the lake of 100 only; 101 masks nothing
        pumpUntil(() -> m.state().lookupsDrained);
        assertThat(lakeMasks(pump(read(101)))).isEmpty();
    }

    /**
     * An index checkpoint: it ends, so the next index change checkpoints again. The test starts
     * each checkpoint itself, so a checkpoint never retains a version while a switch swaps.
     */
    @Test
    void testAnErrorInACheckpoint() throws Exception {
        // a checkpoint after every index change
        newManager(1);
        List<Runnable> starts = new CopyOnWriteArrayList<>();
        ScheduledThreadPoolExecutor timer = holding(starts);
        m.useCheckpointTimer(timer);
        AtomicInteger uploads = new AtomicInteger();
        src.onUpload =
                () -> {
                    if (uploads.incrementAndGet() == 1) {
                        throw new OutOfMemoryError("injected");
                    }
                };
        try {
            // the recovery's install is due a checkpoint of 100, which fails
            coverSnapshot100();
            assertThat(starts).hasSize(1);
            starts.remove(0).run();
            assertEscaped(checkpointer);
            assertThat(metrics.ownerFaults()).isEqualTo(1L);

            // the failed checkpoint ended, so the switch to 101 checkpoints again
            src.publish(101, 2, 12, dict(2, F2), new Index().put("k1", 2, 0).build());
            src.readable = new long[] {101, 12};
            assertThat(pump(async(() -> m.ensureCovered(101)))).isTrue();
            assertThat(starts).hasSize(1);
            starts.remove(0).run();
            pumpUntil(() -> src.uploadedSnapshotId == 101);
            assertThat(uploads.get()).isEqualTo(2);
        } finally {
            timer.shutdown();
        }
    }

    // ------------------------------------------------------------------------------------------

    private void newManager(int checkpointInterval) {
        m =
                new DerivedDvManager(
                        ownedDb,
                        highWatermark::get,
                        checkpointInterval,
                        metrics,
                        new DerivedDvHeapBudget(Long.MAX_VALUE, runtime),
                        runtime,
                        steps,
                        new TestingDvHooks());
        m.setMaskCacheMillis(0);
        m.setCheckpointJitterMillis(0);
    }

    /** Snapshot 100 covers k1, k2, k3 at f1#0..2; k1 and k2 changed at 10 and 11. */
    private void coverSnapshot100() throws Exception {
        src.publish(
                100,
                1,
                10,
                dict(1, F1),
                new Index().put("k1", 1, 0).put("k2", 1, 1).put("k3", 1, 2).build());
        src.readable = new long[] {100, 10};
        src.append("k1", 10, 10);
        src.append("k2", 11, 11);
        highWatermark.set(12);
        pump(async(() -> m.recover(src)));
    }

    /** A timer that keeps what is scheduled on it, for the test to run. */
    private static ScheduledThreadPoolExecutor holding(List<Runnable> scheduled) {
        return new ScheduledThreadPoolExecutor(1) {
            @Override
            public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
                scheduled.add(command);
                return null;
            }
        };
    }

    private static Runnable failOnce() {
        AtomicBoolean thrown = new AtomicBoolean();
        return () -> {
            if (thrown.compareAndSet(false, true)) {
                throw new OutOfMemoryError("injected");
            }
        };
    }

    /** The injected error escaped a task of {@code pool}, once. */
    private void assertEscaped(RecordingPool pool) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (pool.escaped.isEmpty() && System.currentTimeMillis() < deadline) {
            if (!steps.runAll()) {
                Thread.sleep(2);
            }
        }
        assertThat(pool.escaped).hasSize(1);
        assertThat(pool.escaped.get(0)).isInstanceOf(OutOfMemoryError.class).hasMessage("injected");
    }

    /**
     * Puts a recording pool in place of the runtime's {@code name} pool, before a bucket uses it.
     */
    private RecordingPool replacePool(String name) throws Exception {
        Field f = DerivedDvRuntime.class.getDeclaredField(name);
        f.setAccessible(true);
        ThreadPoolExecutor old = (ThreadPoolExecutor) f.get(runtime);
        RecordingPool pool = new RecordingPool(old.getCorePoolSize());
        f.set(runtime, pool);
        old.shutdownNow();
        return pool;
    }

    /**
     * Puts a share of a recording pool in place of the runtime's {@code name} share of a pool,
     * before a bucket uses it.
     */
    private RecordingPool replaceShare(String name, int threads) throws Exception {
        Field f = DerivedDvRuntime.class.getDeclaredField(name);
        f.setAccessible(true);
        RecordingPool pool = new RecordingPool(threads);
        f.set(runtime, new BoundedConcurrencyExecutor(pool, threads));
        return pool;
    }

    /** A pool that records what escaped its tasks: a scheduled pool keeps it in the task. */
    private static final class RecordingPool extends ScheduledThreadPoolExecutor {
        final List<Throwable> escaped = new CopyOnWriteArrayList<>();

        RecordingPool(int threads) {
            super(threads);
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            super.afterExecute(r, t);
            if (t == null && r instanceof Future && ((Future<?>) r).isDone()) {
                try {
                    ((Future<?>) r).get();
                } catch (ExecutionException e) {
                    escaped.add(e.getCause());
                } catch (Exception ignored) {
                    // cancelled or interrupted: nothing escaped
                }
            } else if (t != null) {
                escaped.add(t);
            }
        }
    }

    private CompletableFuture<DvSnapshot> read(long snapshotId) {
        return async(() -> DvTestReads.unionRead(m, snapshotId, true));
    }

    private <T> CompletableFuture<T> async(Callable<T> call) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return call.call();
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                },
                async);
    }

    private CompletableFuture<Void> async(ThrowingRunnable run) {
        return async(
                () -> {
                    run.run();
                    return null;
                });
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private <T> T pump(CompletableFuture<T> f) throws Exception {
        pumpUntil(f::isDone);
        try {
            return f.join();
        } catch (CompletionException e) {
            throw e.getCause() instanceof Exception ? (Exception) e.getCause() : e;
        }
    }

    private void pumpUntil(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (!condition.getAsBoolean()) {
            assertThat(System.currentTimeMillis()).isLessThan(deadline);
            if (!steps.runAll()) {
                Thread.sleep(1);
            }
        }
        steps.runAll();
    }

    /** Runs the owner's steps when the test says so. */
    private static final class ManualExecutor implements Executor {
        private final ConcurrentLinkedQueue<Runnable> tasks = new ConcurrentLinkedQueue<>();
        // what escaped a step, as the pool's uncaught exception handler would get it
        final List<Throwable> escaped = new CopyOnWriteArrayList<>();

        @Override
        public void execute(Runnable task) {
            tasks.add(task);
        }

        boolean runAll() {
            boolean ran = false;
            Runnable task;
            while ((task = tasks.poll()) != null) {
                try {
                    task.run();
                } catch (VirtualMachineError e) {
                    escaped.add(e);
                }
                ran = true;
            }
            return ran;
        }
    }
}
