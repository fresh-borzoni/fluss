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

package org.apache.fluss.server.kv;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.rocksdb.RocksDBHandle;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.utils.clock.ManualClock;

import io.github.fluss_contrib.rocksdb.ColumnFamilyOptions;
import io.github.fluss_contrib.rocksdb.DBOptions;
import io.github.fluss_contrib.rocksdb.FlushOptions;
import io.github.fluss_contrib.rocksdb.FlussTtlCompactionFilter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests row TTL cleanup with native Flink compaction filter. */
class RowTtlCompactionFilterTest {

    @TempDir private Path tempDir;

    @Test
    void testFlussTtlCompactionFilterReadsTimestampFromTaggedValue() throws Exception {
        byte[] expiredKey = "expired-key".getBytes(StandardCharsets.UTF_8);
        byte[] freshKey = "fresh-key".getBytes(StandardCharsets.UTF_8);
        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        long now = 123456789L;

        try (FlussTtlCompactionFilter.FlussTtlCompactionFilterFactory filterFactory =
                        RowTtlCompactionFilterFactory.create(
                                KvValueLayout.TAGGED, Duration.ofHours(1L), new ManualClock(now));
                DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
                ColumnFamilyOptions cfOptions =
                        new ColumnFamilyOptions().setCompactionFilterFactory(filterFactory);
                RocksDBHandle handle = new RocksDBHandle(tempDir.toFile(), dbOptions, cfOptions);
                FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            handle.openDB();
            handle.getDb()
                    .put(expiredKey, encodeTaggedValue(row, now - Duration.ofHours(2L).toMillis()));
            handle.getDb().put(freshKey, encodeTaggedValue(row, now));
            handle.getDb().flush(flushOptions);

            handle.getDb().compactRange();

            assertThat(handle.getDb().get(expiredKey)).isNull();
            assertThat(handle.getDb().get(freshKey)).isNotNull();
        }
    }

    /**
     * In the versioned layout the filter reads the TTL timestamp slot, not the version offset slot
     * before it: a value with a small version offset and a fresh timestamp stays, and one with a
     * large version offset and an old timestamp goes. A value without row TTL never expires.
     */
    @Test
    void testFlussTtlCompactionFilterReadsTheTtlSlotOfVersionedValue() throws Exception {
        byte[] expiredKey = "expired-key".getBytes(StandardCharsets.UTF_8);
        byte[] freshKey = "fresh-key".getBytes(StandardCharsets.UTF_8);
        byte[] neverKey = "never-key".getBytes(StandardCharsets.UTF_8);
        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        long now = 123456789L;
        assertThat(RowTtlCompactionFilterFactory.expiryFieldOffset(KvValueLayout.VERSIONED))
                .isEqualTo(8);
        assertThat(RowTtlCompactionFilterFactory.expiryFieldOffset(KvValueLayout.TAGGED))
                .isEqualTo(0);

        try (FlussTtlCompactionFilter.FlussTtlCompactionFilterFactory filterFactory =
                        RowTtlCompactionFilterFactory.create(
                                KvValueLayout.VERSIONED,
                                Duration.ofHours(1L),
                                new ManualClock(now));
                DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
                ColumnFamilyOptions cfOptions =
                        new ColumnFamilyOptions().setCompactionFilterFactory(filterFactory);
                RocksDBHandle handle = new RocksDBHandle(tempDir.toFile(), dbOptions, cfOptions);
                FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            handle.openDB();
            // an old timestamp behind a large version offset, a fresh one behind offset 0
            handle.getDb()
                    .put(
                            expiredKey,
                            ValueEncoder.forLayout(
                                            KvValueLayout.VERSIONED,
                                            ignored -> now - Duration.ofHours(2L).toMillis())
                                    .encodeValue(new BinaryValue(DEFAULT_SCHEMA_ID, row), now));
            handle.getDb()
                    .put(
                            freshKey,
                            ValueEncoder.forLayout(KvValueLayout.VERSIONED, ignored -> now)
                                    .encodeValue(new BinaryValue(DEFAULT_SCHEMA_ID, row), 0L));
            handle.getDb()
                    .put(
                            neverKey,
                            ValueEncoder.forLayout(KvValueLayout.VERSIONED)
                                    .encodeValue(new BinaryValue(DEFAULT_SCHEMA_ID, row), 0L));
            handle.getDb().flush(flushOptions);

            handle.getDb().compactRange();

            assertThat(handle.getDb().get(expiredKey)).isNull();
            assertThat(handle.getDb().get(freshKey)).isNotNull();
            assertThat(handle.getDb().get(neverKey)).isNotNull();
        }
    }

    /**
     * With deletion vectors the filter takes its time from {@link RowTtlFilterClock}. Long after
     * the TTL, a version at or after the readable offset, written at W, survives the cap; one the
     * covered snapshot holds, written a TTL before W, expires. Before the cap is set, nothing
     * expires, not even a value stamped 0 or before the epoch.
     */
    @Test
    void testTheCappedClockKeepsVersionsTheLakeDoesNotHold() throws Exception {
        byte[] tieredKey = "tiered-key".getBytes(StandardCharsets.UTF_8);
        byte[] untieredKey = "untiered-key".getBytes(StandardCharsets.UTF_8);
        byte[] zeroKey = "zero-key".getBytes(StandardCharsets.UTF_8);
        byte[] negativeKey = "negative-key".getBytes(StandardCharsets.UTF_8);
        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        Duration ttl = Duration.ofHours(1L);
        long w = 123456789L;
        ManualClock clock = new ManualClock(w + Duration.ofDays(7).toMillis());
        RowTtlFilterClock filterClock = new RowTtlFilterClock(clock, ttl.toMillis());

        try (FlussTtlCompactionFilter.FlussTtlCompactionFilterFactory filterFactory =
                        RowTtlCompactionFilterFactory.create(
                                KvValueLayout.VERSIONED, ttl, filterClock);
                DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
                ColumnFamilyOptions cfOptions =
                        new ColumnFamilyOptions().setCompactionFilterFactory(filterFactory);
                RocksDBHandle handle = new RocksDBHandle(tempDir.toFile(), dbOptions, cfOptions);
                FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            handle.openDB();
            handle.getDb().put(tieredKey, versionedValue(row, 5L, w - ttl.toMillis()));
            // the write path tracks a version's TTL time before the KV write
            filterClock.track(12L, w);
            handle.getDb().put(untieredKey, versionedValue(row, 12L, w));
            handle.getDb().put(zeroKey, versionedValue(row, 3L, 0L));
            handle.getDb().put(negativeKey, versionedValue(row, 4L, -ttl.toMillis()));
            handle.getDb().flush(flushOptions);

            handle.getDb().compactRange();
            assertThat(handle.getDb().get(tieredKey)).isNotNull();
            assertThat(handle.getDb().get(zeroKey)).isNotNull();
            assertThat(handle.getDb().get(negativeKey)).isNotNull();

            // the bucket covers a snapshot readable from 10: the version at 12 is the oldest above
            filterClock.coveredAt(10L);
            handle.getDb().compactRange();
            assertThat(handle.getDb().get(tieredKey)).isNull();
            assertThat(handle.getDb().get(zeroKey)).isNull();
            assertThat(handle.getDb().get(untieredKey)).isNotNull();
        }
    }

    /**
     * By event time, a row backfilled after the readable offset with an event time a week old
     * survives compaction under the cap; once a snapshot holds it, it expires.
     */
    @Test
    void testTheCappedClockByEventTimeKeepsABackfilledRow() throws Exception {
        byte[] backfilledKey = "backfilled-key".getBytes(StandardCharsets.UTF_8);
        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        Duration ttl = Duration.ofHours(1L);
        long now = 123456789000L;
        long backfilled = now - Duration.ofDays(7).toMillis();
        RowTtlFilterClock filterClock = new RowTtlFilterClock(new ManualClock(now), ttl.toMillis());

        try (FlussTtlCompactionFilter.FlussTtlCompactionFilterFactory filterFactory =
                        RowTtlCompactionFilterFactory.create(
                                KvValueLayout.VERSIONED, ttl, filterClock);
                DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
                ColumnFamilyOptions cfOptions =
                        new ColumnFamilyOptions().setCompactionFilterFactory(filterFactory);
                RocksDBHandle handle = new RocksDBHandle(tempDir.toFile(), dbOptions, cfOptions);
                FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            handle.openDB();
            filterClock.coveredAt(10L);
            // the write path tracks the row's event time before the KV write
            filterClock.track(12L, backfilled);
            handle.getDb().put(backfilledKey, versionedValue(row, 12L, backfilled));
            handle.getDb().flush(flushOptions);

            handle.getDb().compactRange();
            assertThat(handle.getDb().get(backfilledKey)).isNotNull();

            filterClock.coveredAt(13L);
            handle.getDb().compactRange();
            assertThat(handle.getDb().get(backfilledKey)).isNull();
        }
    }

    /**
     * What the cap relies on in the native filter, so that an upgrade that changes it fails here. A
     * flush never asks for the time; a compaction asks at its first entry and every 1,000 entries
     * after, each compaction anew. The comparison of TTL time plus TTL with the time saturates:
     * with the time at Long.MIN_VALUE nothing expires, whatever the TTL time, and a TTL time near
     * Long.MAX_VALUE never wraps into expiry.
     */
    @Test
    void testTheNativeFilterAsksForTheTimePerCompactionAndSaturates() throws Exception {
        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        Duration ttl = Duration.ofHours(1L);
        AtomicLong time = new AtomicLong(Long.MIN_VALUE);
        AtomicInteger calls = new AtomicInteger();
        long[] stamps = {
            Long.MIN_VALUE,
            Long.MIN_VALUE + 1,
            -ttl.toMillis(),
            0L,
            Long.MAX_VALUE - 10,
            Long.MAX_VALUE
        };

        try (FlussTtlCompactionFilter.FlussTtlCompactionFilterFactory filterFactory =
                        RowTtlCompactionFilterFactory.create(
                                KvValueLayout.VERSIONED,
                                ttl,
                                () -> {
                                    calls.incrementAndGet();
                                    return time.get();
                                });
                DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
                ColumnFamilyOptions cfOptions =
                        new ColumnFamilyOptions().setCompactionFilterFactory(filterFactory);
                RocksDBHandle handle = new RocksDBHandle(tempDir.toFile(), dbOptions, cfOptions);
                FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            handle.openDB();
            for (int i = 0; i < 3_000; i++) {
                handle.getDb()
                        .put(
                                ("key-" + i).getBytes(StandardCharsets.UTF_8),
                                versionedValue(row, i, stamps[i % stamps.length]));
            }
            handle.getDb().flush(flushOptions);
            assertThat(calls).hasValue(0);

            handle.getDb().compactRange();
            assertThat(calls).hasValue(3);
            for (int i = 0; i < 3_000; i++) {
                assertThat(handle.getDb().get(("key-" + i).getBytes(StandardCharsets.UTF_8)))
                        .as("TTL time %d", stamps[i % stamps.length])
                        .isNotNull();
            }

            // a later compaction asks again; near Long.MAX_VALUE nothing wraps into expiry
            time.set(Duration.ofDays(1).toMillis());
            handle.getDb().compactRange();
            assertThat(calls.get()).isGreaterThan(3);
            for (int i = 0; i < 3_000; i++) {
                long stamp = stamps[i % stamps.length];
                byte[] value = handle.getDb().get(("key-" + i).getBytes(StandardCharsets.UTF_8));
                if (stamp > Long.MAX_VALUE - ttl.toMillis() - 1 || stamp == Long.MAX_VALUE - 10) {
                    assertThat(value).as("TTL time %d", stamp).isNotNull();
                } else {
                    assertThat(value).as("TTL time %d", stamp).isNull();
                }
            }
        }
    }

    @Test
    void testCreateRejectsInvalidTtlDuration() {
        assertThatThrownBy(
                        () ->
                                RowTtlCompactionFilterFactory.create(
                                        KvValueLayout.TAGGED, Duration.ZERO, new ManualClock(0L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(ConfigOptions.TABLE_KV_TTL.key());
    }

    private static byte[] versionedValue(BinaryRow row, long versionOffset, long ttlTimestamp) {
        return ValueEncoder.forLayout(KvValueLayout.VERSIONED, ignored -> ttlTimestamp)
                .encodeValue(new BinaryValue(DEFAULT_SCHEMA_ID, row), versionOffset);
    }

    private static byte[] encodeTaggedValue(BinaryRow row, long valueTag) {
        return ValueEncoder.forLayout(KvValueLayout.TAGGED, ignored -> valueTag)
                .encodeValue(new BinaryValue(DEFAULT_SCHEMA_ID, row));
    }
}
