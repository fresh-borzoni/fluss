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

package org.apache.fluss.lake.iceberg.tiering;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests that a round waits for its bucket's compaction only so long. */
class IcebergLakeWriterCompactionWaitTest {

    @Test
    void testFinishedCompactionIsTakenAtOnce() throws Exception {
        AtomicBoolean stopped = new AtomicBoolean();
        assertThat(
                        IcebergLakeWriter.awaitBounded(
                                CompletableFuture.completedFuture("done"),
                                () -> stopped.set(true),
                                10_000,
                                10_000))
                .isEqualTo("done");
        assertThat(stopped).isFalse();
    }

    @Test
    void testSlowCompactionIsStoppedAndKeepsWhatItFinished() throws Exception {
        CompletableFuture<String> compaction = new CompletableFuture<>();
        long start = System.currentTimeMillis();
        // the compaction completes with the groups it finished once it is asked to stop
        String result =
                IcebergLakeWriter.awaitBounded(
                        compaction, () -> compaction.complete("partial"), 200, 10_000);
        assertThat(result).isEqualTo("partial");
        assertThat(System.currentTimeMillis() - start).isLessThan(5_000);
    }

    @Test
    void testCompactionThatDoesNotStopIsGivenUp() throws Exception {
        CompletableFuture<String> compaction = new CompletableFuture<>();
        long start = System.currentTimeMillis();
        assertThat(IcebergLakeWriter.awaitBounded(compaction, () -> {}, 100, 100)).isNull();
        assertThat(compaction).isCancelled();
        assertThat(System.currentTimeMillis() - start).isLessThan(5_000);
    }
}
