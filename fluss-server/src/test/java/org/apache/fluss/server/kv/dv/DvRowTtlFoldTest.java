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
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.dict;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The fold for row TTL ({@link DvRowTtlGuard}): a bucket's union reads read it by key once a
 * version above the covered snapshot's readable offset can have expired before it was tiered.
 * Snapshot 100 is readable from offset 10; the cut of 14 reads the log [10, 14).
 */
class DvRowTtlFoldTest extends DerivedDvManagerTestBase {

    private static final Duration TTL = Duration.ofMinutes(10);
    // a tenth of the TTL, within [5 s, 60 s]
    private static final long MARGIN_MS = 60_000;
    private static final long WRITTEN = 1_000_000;

    private final ManualClock clock = new ManualClock(WRITTEN);

    /**
     * Masks are served until the TTL, less the margin, has passed since the first write above the
     * readable offset; from then on reads fold, and each such answer counts. A cut at the readable
     * offset reads no log and never folds.
     */
    @Test
    void testABucketFoldsOnceItsOldestUntieredVersionCanHaveExpired() throws Exception {
        src.writeTimes = offset -> offset == 10 ? WRITTEN : -1;
        manager.useRowTtl(TTL, clock);
        recoverAtSnapshot100();
        waitUntil(() -> manager.rowTtlWriteTime() == WRITTEN);
        assertThat(manager.foldsForRowTtl(10, 14)).isFalse();

        clock.advanceTime(TTL.toMillis() - MARGIN_MS - 1, TimeUnit.MILLISECONDS);
        assertThat(manager.foldsForRowTtl(10, 14)).isFalse();
        clock.advanceTime(1, TimeUnit.MILLISECONDS);
        assertThat(manager.foldsForRowTtl(10, 14)).isTrue();
        assertThat(manager.foldsForRowTtl(10, 14)).isTrue();
        assertThat(manager.foldsForRowTtl(10, 10)).isFalse();
        assertThat(metrics.rowTtlFolds()).isEqualTo(2L);
    }

    /**
     * A switch to a snapshot readable from a later offset, whose first write above it is recent,
     * makes masks exact again.
     */
    @Test
    void testASwitchToANewerSnapshotEndsTheFold() throws Exception {
        long rewritten = WRITTEN + TTL.toMillis();
        src.writeTimes = offset -> offset == 10 ? WRITTEN : offset == 14 ? rewritten : -1;
        manager.useRowTtl(TTL, clock);
        recoverAtSnapshot100();
        waitUntil(() -> manager.rowTtlWriteTime() == WRITTEN);
        clock.advanceTime(TTL);
        assertThat(manager.foldsForRowTtl(10, 14)).isTrue();

        src.publish(101, 2, 14, dict(2, F2), new Index().put("k1", 2, 0).build());
        src.readable = new long[] {101, 14};
        assertThat(manager.ensureCovered(101)).isTrue();
        waitUntil(() -> manager.rowTtlWriteTime() == rewritten);
        assertThat(manager.foldsForRowTtl(14, 16)).isFalse();
    }

    /** With nothing written above the readable offset yet, the TTL counts from the read. */
    @Test
    void testNothingAboveTheReadableOffsetCountsFromTheRead() throws Exception {
        src.writeTimes = offset -> -1;
        manager.useRowTtl(TTL, clock);
        recoverAtSnapshot100();
        waitUntil(() -> manager.rowTtlWriteTime() == WRITTEN);

        clock.advanceTime(TTL.toMillis() - MARGIN_MS - 1, TimeUnit.MILLISECONDS);
        assertThat(manager.foldsForRowTtl(10, 14)).isFalse();
        clock.advanceTime(1, TimeUnit.MILLISECONDS);
        assertThat(manager.foldsForRowTtl(10, 14)).isTrue();
    }

    /** A log whose write time cannot be read folds, however recent its writes: it fails closed. */
    @Test
    void testABucketWhoseLogCannotBeReadFolds() throws Exception {
        CountDownLatch read = new CountDownLatch(1);
        src.writeTimes =
                offset -> {
                    read.countDown();
                    throw new UncheckedIOException(new IOException("the segment is gone"));
                };
        manager.useRowTtl(TTL, clock);
        recoverAtSnapshot100();
        assertThat(read.await(30, TimeUnit.SECONDS)).isTrue();

        assertThat(manager.foldsForRowTtl(10, 14)).isTrue();
        assertThat(manager.rowTtlWriteTime()).isEqualTo(-1L);
    }

    /** A table without row TTL reads no write time and never folds for it. */
    @Test
    void testATableWithoutRowTtlNeverFolds() throws Exception {
        AtomicInteger reads = new AtomicInteger();
        src.writeTimes =
                offset -> {
                    reads.incrementAndGet();
                    return 0;
                };
        recoverAtSnapshot100();
        clock.advanceTime(Duration.ofDays(365));

        assertThat(manager.foldsForRowTtl(10, 14)).isFalse();
        assertThat(reads).hasValue(0);
        assertThat(metrics.rowTtlFolds()).isZero();
    }

    /**
     * A failed read of the write time is retried: the bucket folds until a retry reads it, then
     * serves masks again.
     */
    @Test
    void testAFailedReadIsRetried() throws Exception {
        AtomicInteger reads = new AtomicInteger();
        src.writeTimes =
                offset -> {
                    if (reads.incrementAndGet() == 1) {
                        throw new UncheckedIOException(new IOException("the segment is gone"));
                    }
                    return WRITTEN;
                };
        manager.useRowTtl(TTL, clock);
        recoverAtSnapshot100();
        waitUntil(() -> reads.get() >= 1);
        assertThat(manager.foldsForRowTtl(10, 14)).isTrue();

        waitUntil(() -> manager.rowTtlWriteTime() == WRITTEN);
        assertThat(reads).hasValue(2);
        assertThat(manager.foldsForRowTtl(10, 14)).isFalse();
    }

    /**
     * After a switch to a later readable offset, a failed read keeps the earlier write time, which
     * still bounds the later offset's versions; a retry then reads the later one.
     */
    @Test
    void testAFailedReadAfterASwitchKeepsTheEarlierWriteTime() throws Exception {
        long rewritten = WRITTEN + 1_000;
        AtomicInteger failures = new AtomicInteger();
        src.writeTimes =
                offset -> {
                    if (offset == 14 && failures.incrementAndGet() == 1) {
                        throw new UncheckedIOException(new IOException("the segment is gone"));
                    }
                    return offset == 10 ? WRITTEN : rewritten;
                };
        manager.useRowTtl(TTL, clock);
        recoverAtSnapshot100();
        waitUntil(() -> manager.rowTtlWriteTime() == WRITTEN);

        src.publish(101, 2, 14, dict(2, F2), new Index().put("k1", 2, 0).build());
        src.readable = new long[] {101, 14};
        assertThat(manager.ensureCovered(101)).isTrue();
        waitUntil(() -> failures.get() >= 1);
        assertThat(manager.rowTtlWriteTime()).isEqualTo(WRITTEN);
        assertThat(manager.foldsForRowTtl(14, 16)).isFalse();

        waitUntil(() -> manager.rowTtlWriteTime() == rewritten);
    }

    /** A negative write time from the log is no time at all: the bucket folds. */
    @Test
    void testANegativeWriteTimeFolds() throws Exception {
        AtomicInteger reads = new AtomicInteger();
        src.writeTimes =
                offset -> {
                    reads.incrementAndGet();
                    return -5;
                };
        manager.useRowTtl(TTL, clock);
        recoverAtSnapshot100();
        waitUntil(() -> reads.get() >= 1);

        assertThat(manager.foldsForRowTtl(10, 14)).isTrue();
        assertThat(manager.rowTtlWriteTime()).isEqualTo(-1L);
    }

    /**
     * The margin is a tenth of the TTL, at least 5 s and at most 60 s, but at most half the TTL.
     */
    @Test
    void testTheMarginHasAFloorAndACap() {
        assertThat(DvRowTtlGuard.margin(Duration.ofHours(1).toMillis())).isEqualTo(60_000L);
        assertThat(DvRowTtlGuard.margin(TTL.toMillis())).isEqualTo(60_000L);
        assertThat(DvRowTtlGuard.margin(Duration.ofMinutes(2).toMillis())).isEqualTo(12_000L);
        assertThat(DvRowTtlGuard.margin(Duration.ofSeconds(30).toMillis())).isEqualTo(5_000L);
        assertThat(DvRowTtlGuard.margin(Duration.ofSeconds(3).toMillis())).isEqualTo(1_500L);
    }
}
