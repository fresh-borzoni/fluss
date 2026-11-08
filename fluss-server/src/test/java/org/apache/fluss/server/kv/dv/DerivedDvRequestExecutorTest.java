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

import org.apache.fluss.exception.DeletionVectorsNotReadyException;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests {@link DerivedDvRequestExecutor}. */
class DerivedDvRequestExecutorTest {

    /** Requests run on the executor's threads, never on the caller's (an RPC thread). */
    @Test
    void testRunsRequestsOffTheCallersThread() throws Exception {
        try (DerivedDvRequestExecutor executor = new DerivedDvRequestExecutor(2, 4)) {
            Thread caller = Thread.currentThread();
            CompletableFuture<Thread> ran = executor.submit(Thread::currentThread);
            assertThat(ran.get(10, TimeUnit.SECONDS)).isNotSameAs(caller);
            assertThat(ran.get().getName()).startsWith("kv-deletion-vectors-request-thread-");

            CompletableFuture<Object> failed =
                    executor.submit(
                            () -> {
                                throw new IllegalStateException("boom");
                            });
            assertThatThrownBy(() -> failed.get(10, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(IllegalStateException.class);
        }
    }

    /** A request beyond the running and queued ones is answered as not ready at once. */
    @Test
    void testFullExecutorAnswersNotReady() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        try (DerivedDvRequestExecutor executor = new DerivedDvRequestExecutor(1, 1)) {
            CompletableFuture<Boolean> running =
                    executor.submit(() -> release.await(30, TimeUnit.SECONDS));
            CompletableFuture<Boolean> queued = executor.submit(() -> true);
            CompletableFuture<Boolean> refused = executor.submit(() -> true);
            assertThat(refused).isCompletedExceptionally();
            assertThatThrownBy(refused::join)
                    .hasCauseInstanceOf(DeletionVectorsNotReadyException.class)
                    .hasMessageContaining("busy");
            release.countDown();
            assertThat(running.get(10, TimeUnit.SECONDS)).isTrue();
            assertThat(queued.get(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    /** After shutdown, requests are answered as not ready; the client retries another leader. */
    @Test
    void testClosedExecutorAnswersNotReady() {
        DerivedDvRequestExecutor executor = new DerivedDvRequestExecutor(1, 1);
        executor.close();
        assertThat(executor.submit(() -> true)).isCompletedExceptionally();
    }
}
