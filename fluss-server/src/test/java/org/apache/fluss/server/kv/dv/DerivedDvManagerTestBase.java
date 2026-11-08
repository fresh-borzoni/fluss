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

import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.server.kv.dv.DerivedDvManagerTest.FakeSource;
import org.apache.fluss.server.kv.dv.DerivedDvManagerTest.Index;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.dict;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A bucket's {@link DerivedDvManager} over its own DV RocksDB and a fake of its durable state, with
 * the helpers its tests share. The high watermark is the test's; the mask cache is off.
 */
abstract class DerivedDvManagerTestBase {

    static final String F1 = "s3://lake/t/data/f1.parquet";
    static final String F2 = "s3://lake/t/data/f2.parquet";
    static final String F3 = "s3://lake/t/data/f3.parquet";

    @TempDir Path tempDir;

    DvRocksDB db;
    AtomicLong highWatermark;
    DerivedDvMetrics metrics;
    DerivedDvManager manager;
    FakeSource src;
    // for tasks that block on a paused replay; the common pool can be too small for them
    ExecutorService async;

    @BeforeEach
    void setUp() throws Exception {
        db = DvRocksDB.open(tempDir.resolve("dv").toString());
        highWatermark = new AtomicLong(0);
        metrics = DerivedDvMetrics.noop();
        manager =
                DvTestManagers.newManager(
                        db, highWatermark::get, DvTestManagers.CHECKPOINT_INTERVAL, metrics);
        manager.setMaskCacheMillis(0);
        manager.setCheckpointJitterMillis(0);
        src = new FakeSource(Files.createDirectories(tempDir.resolve("remote")));
        async = Executors.newCachedThreadPool();
    }

    @AfterEach
    void tearDown() {
        async.shutdownNow();
        manager.close();
        db.close();
    }

    /**
     * Snapshot 100 covers [0, 10) with k1 -> f1#0, k2 -> f1#1, k3 -> f1#2; the change list is
     * k1@10, k4@11, k1@12, -D k2@13, k3@15; the high watermark is 14.
     */
    void recoverAtSnapshot100() throws Exception {
        src.publish(
                100,
                1,
                10,
                dict(1, F1),
                new Index().put("k1", 1, 0).put("k2", 1, 1).put("k3", 1, 2).build());
        src.readable = new long[] {100, 10};
        src.append("k1", 0, 0);
        src.append("k2", 1, 1);
        src.append("k3", 2, 2);
        src.append("k1", 10, 10);
        src.append("k4", 11, 11);
        src.append("k1", 12, 12);
        src.append("k2", 13, -1);
        highWatermark.set(14);
        // the leader takes over at log end 14, the high watermark: no read goes below a cut the
        // last leader may have served
        manager.recover(src);
        // a write above the high watermark
        src.append("k3", 15, 15);
        DvTestManagers.handOver(manager, Collections.singletonList(src.written("k3", 15, 15)));
    }

    /** Snapshot 100 covers [0, 10) with k0..k9 at f1#0..9; nothing changed since. */
    void recoverWithTenLakeKeys() throws Exception {
        Index initial = new Index();
        for (int i = 0; i < 10; i++) {
            initial.put("k" + i, 1, i);
        }
        src.publish(100, 1, 10, dict(1, F1), initial.build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        manager.recover(src);
    }

    /** Hands a change of key {@code k} at {@code offset} over, as the write path does. */
    void handOver(String k, long offset, boolean insert) {
        DvEntry entry = src.written(k, offset, offset, insert);
        src.append(k, offset, offset, insert);
        DvTestManagers.handOver(manager, Collections.singletonList(entry));
    }

    /** Makes the next replay wait for the returned gate after {@code changes} changes. */
    CountDownLatch pauseReplayAfter(int changes) {
        CountDownLatch gate = new CountDownLatch(1);
        src.gateAfterChanges = changes;
        src.replayGate = gate;
        return gate;
    }

    DvSnapshot unionRead(long snapshotId) throws Exception {
        return unionRead(manager, snapshotId, true);
    }

    DvSnapshot unionRead(long snapshotId, boolean waitForCatchUp) throws Exception {
        return unionRead(manager, snapshotId, waitForCatchUp);
    }

    /** A union read, retried while the bucket is not ready, as clients retry. */
    static DvSnapshot unionRead(DerivedDvManager m, long snapshotId, boolean waitForCatchUp)
            throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (true) {
            try {
                return DvTestReads.unionRead(m, snapshotId, waitForCatchUp);
            } catch (RetriableException e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(20);
            }
        }
    }

    /** A tiering lookup, retried while the bucket is not ready, as the tiering service retries. */
    List<Object[]> lookupWhenReady(long snapshotId, List<byte[]> keys) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (true) {
            try {
                return manager.lookup(snapshotId, keys);
            } catch (RetriableException e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(20);
            }
        }
    }

    /** The positions of a lookup, as key@file#position. */
    static List<String> asStrings(List<Object[]> lookups) {
        List<String> out = new ArrayList<>();
        for (Object[] r : lookups) {
            out.add(new String((byte[]) r[0], StandardCharsets.UTF_8) + "@" + r[1] + "#" + r[2]);
        }
        return out;
    }

    /** The downloaded index files left in {@code dir}; ingestion moves the ingested ones away. */
    static List<Path> downloads(Path dir) throws IOException {
        List<Path> out = new ArrayList<>();
        try (Stream<Path> files = Files.list(dir)) {
            files.filter(f -> f.getFileName().toString().startsWith("sst-")).forEach(out::add);
        }
        return out;
    }

    static Runnable waitFor(CountDownLatch latch) {
        return () -> {
            try {
                latch.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
    }

    static void waitUntil(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }
}
