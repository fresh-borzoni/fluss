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

package org.apache.fluss.flink.source.epochs;

import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link EpochResolver}: resolving the epoch of a lookup to a position in a bucket. */
class EpochResolverTest {

    private static final TableBucket B0 = new TableBucket(7L, 0);
    private static final TableBucket B1 = new TableBucket(7L, 1);

    private TestingEpochStore store;
    private ScheduledExecutorService retries;
    private EpochResolver resolver;

    @BeforeEach
    void before() {
        store = new TestingEpochStore();
        retries = Executors.newSingleThreadScheduledExecutor();
        resolver = new EpochResolver(store, retries, 2L);
        // the bucket started at offset 0 and changed in epochs 3 and 7
        store.putEnd(B0, -1L, 0L);
        store.putEnd(B0, 3L, 5L);
        store.putEnd(B0, 7L, 9L);
        store.putProgress(B0, 10L);
    }

    @AfterEach
    void after() {
        retries.shutdownNow();
    }

    @Test
    void epochEndsWhereTheLatestEpochAtOrBeforeItThatChangedTheBucketEnded() throws Exception {
        assertThat(endOf(B0, 3)).isEqualTo(5L);
        assertThat(endOf(B0, 6)).isEqualTo(5L);
        assertThat(endOf(B0, 7)).isEqualTo(9L);
        assertThat(endOf(B0, 10)).isEqualTo(9L);
    }

    @Test
    void epochBeforeTheFirstReadsWhereTheScanStarted() throws Exception {
        assertThat(endOf(B0, Epochs.BEFORE_FIRST)).isEqualTo(0L);
        assertThat(endOf(B0, 2)).isEqualTo(0L);
        // a scan that emits a snapshot started from an empty bucket
        store.putEnd(B1, -1L, Epochs.EMPTY);
        store.putEnd(B1, 0L, 12L);
        store.putProgress(B1, 4L);
        assertThat(endOf(B1, Epochs.BEFORE_FIRST)).isEqualTo(Epochs.EMPTY);
        assertThat(endOf(B1, 4)).isEqualTo(12L);
    }

    @Test
    void epochThatDidNotEndYetWaits() throws Exception {
        CompletableFuture<Long> end = resolver.endOf(B0, 12);
        // concurrent reads of the epoch wait together
        assertThat(resolver.endOf(B0, 12)).isSameAs(end);
        Thread.sleep(50);
        assertThat(end).isNotDone();

        // the scan ends epoch 11 with a change, and 12 without
        store.putEnd(B0, 11L, 14L);
        store.putProgress(B0, 11L);
        Thread.sleep(50);
        assertThat(end).isNotDone();
        store.putProgress(B0, 12L);
        assertThat(end.get(10, TimeUnit.SECONDS)).isEqualTo(14L);
    }

    @Test
    void epochAfterTheLastWaitsForTheEndOfTheScan() throws Exception {
        CompletableFuture<Long> end = resolver.endOf(B0, Epochs.AFTER_LAST);
        Thread.sleep(50);
        assertThat(end).isNotDone();
        store.putEnd(B0, 11L, 20L);
        store.putProgress(B0, Epochs.AFTER_LAST);
        assertThat(end.get(10, TimeUnit.SECONDS)).isEqualTo(20L);
    }

    @Test
    void resolvedEpochsAreNotReadAgain() throws Exception {
        assertThat(endOf(B0, 6)).isEqualTo(5L);
        int reads = store.reads.get();
        // epochs 3 to 6 end at the same position
        assertThat(endOf(B0, 4)).isEqualTo(5L);
        assertThat(endOf(B0, 6)).isEqualTo(5L);
        assertThat(endOf(B0, 3)).isEqualTo(5L);
        assertThat(store.reads.get()).isEqualTo(reads);
        // a later epoch is read
        assertThat(endOf(B0, 8)).isEqualTo(9L);
        assertThat(store.reads.get()).isGreaterThan(reads);
    }

    private long endOf(TableBucket bucket, long epoch) throws Exception {
        return resolver.endOf(bucket, epoch)
                .get(Duration.ofSeconds(10).toMillis(), TimeUnit.MILLISECONDS);
    }
}
