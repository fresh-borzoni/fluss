/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.kv.dv;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.DeletionVectorsNotReadyException;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runs a tablet server's deletion vector requests (union read masks, tiering's position lookups) on
 * their own threads. A request may wait briefly for background work or read many keys, so it must
 * not hold the RPC threads that serve writes and fetches. At most {@code threads} requests run at
 * once and {@code queued} wait. A request beyond that is answered as not ready; clients retry it.
 */
@Internal
public final class DerivedDvRequestExecutor implements AutoCloseable {

    /** Threads of a tablet server's executor. */
    public static final int DEFAULT_THREADS =
            Math.max(2, Math.min(16, Runtime.getRuntime().availableProcessors() / 2));

    /** Requests a tablet server's executor queues before it answers them as not ready. */
    public static final int DEFAULT_QUEUED = 1024;

    private final ThreadPoolExecutor executor;

    public DerivedDvRequestExecutor() {
        this(DEFAULT_THREADS, DEFAULT_QUEUED);
    }

    public DerivedDvRequestExecutor(int threads, int queued) {
        AtomicInteger ids = new AtomicInteger();
        this.executor =
                new ThreadPoolExecutor(
                        threads,
                        threads,
                        60,
                        TimeUnit.SECONDS,
                        new ArrayBlockingQueue<>(queued),
                        r -> {
                            Thread t = new Thread(r, "derived-dv-request-" + ids.incrementAndGet());
                            t.setDaemon(true);
                            return t;
                        });
    }

    /**
     * Runs {@code request} on the executor. The returned future fails with {@link
     * DeletionVectorsNotReadyException} if the executor is full or shut down.
     */
    public <T> CompletableFuture<T> submit(Callable<T> request) {
        CompletableFuture<T> response = new CompletableFuture<>();
        try {
            executor.execute(
                    () -> {
                        try {
                            response.complete(request.call());
                        } catch (Throwable t) {
                            response.completeExceptionally(t);
                        }
                    });
        } catch (RejectedExecutionException e) {
            response.completeExceptionally(
                    new DeletionVectorsNotReadyException(
                            String.format(
                                    "The tablet server is busy with %d deletion vector requests;"
                                            + " retry later.",
                                    executor.getActiveCount() + executor.getQueue().size())));
        }
        return response;
    }

    /** Stops taking requests; the running ones finish. */
    @Override
    public void close() {
        executor.shutdown();
    }
}
