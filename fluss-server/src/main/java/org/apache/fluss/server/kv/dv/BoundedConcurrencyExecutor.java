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

import java.util.ArrayDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * Runs tasks on a shared pool, at most {@code maxRunning} of them at a time. Tasks beyond that wait
 * in this executor's queue and take no thread of the pool until one of its running tasks ends, so
 * the other users of the pool keep their threads.
 *
 * <p>A task the pool rejects is rejected to the caller of {@link #execute}. A waiting task the pool
 * rejects once it is its turn, as the pool shut down, is dropped with the tasks behind it.
 */
final class BoundedConcurrencyExecutor implements Executor {

    private final Executor pool;
    private final int maxRunning;
    private final ArrayDeque<Runnable> waiting = new ArrayDeque<>();
    private int running;
    private boolean shutdown;

    BoundedConcurrencyExecutor(Executor pool, int maxRunning) {
        this.pool = pool;
        this.maxRunning = maxRunning;
    }

    @Override
    public void execute(Runnable task) {
        synchronized (this) {
            if (shutdown) {
                throw new RejectedExecutionException("The executor is shut down.");
            }
            if (running >= maxRunning) {
                waiting.add(task);
                return;
            }
            running++;
        }
        try {
            pool.execute(() -> run(task));
        } catch (Throwable t) {
            synchronized (this) {
                running--;
            }
            throw t;
        }
    }

    private void run(Runnable task) {
        try {
            task.run();
        } finally {
            startNext();
        }
    }

    private void startNext() {
        Runnable next;
        synchronized (this) {
            next = shutdown ? null : waiting.poll();
            if (next == null) {
                running--;
                return;
            }
        }
        try {
            pool.execute(() -> run(next));
        } catch (RejectedExecutionException e) {
            synchronized (this) {
                running--;
                waiting.clear();
            }
        } catch (Throwable t) {
            // the pool could not start it, e.g. without memory for a thread: it waits again
            synchronized (this) {
                running--;
                waiting.addFirst(next);
            }
            throw t;
        }
    }

    /** Takes no more tasks and drops the waiting ones; the running ones finish. */
    synchronized void shutdown() {
        shutdown = true;
        waiting.clear();
    }

    /** The tasks waiting for their turn. */
    synchronized int queued() {
        return waiting.size();
    }

    /** The tasks running on the pool. */
    synchronized int active() {
        return running;
    }
}
