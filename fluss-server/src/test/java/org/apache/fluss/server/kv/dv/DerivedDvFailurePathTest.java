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

import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.server.kv.dv.DerivedDvManagerTest.Index;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.dict;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.key;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.lakeMasks;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.set;
import static org.apache.fluss.server.kv.dv.DvTestManagers.hooks;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/**
 * Tests of a bucket's deletion vector state on its failure and load paths: a switch whose swap
 * fails, a full resolve that shares the resolver threads, a rebuild that ingests while writes go
 * on, and a full rebuild that loads a checkpoint over the index of an older snapshot.
 */
class DerivedDvFailurePathTest extends DerivedDvManagerTestBase {

    /**
     * The swap of a switch fails while it removes the files the new snapshot dropped from the
     * dictionary. Nothing of the switch is applied: the covered snapshot keeps every file and mask,
     * the switch releases what it held, and a later switch succeeds.
     */
    @Test
    void testFailedSwapLeavesTheCoveredSnapshotWhole() throws Exception {
        Map<Integer, String> files = new HashMap<>();
        files.put(1, F1);
        files.put(2, F2);
        src.publish(100, 1, 10, files, new Index().put("k0", 1, 0).put("k1", 2, 0).build());
        src.readable = new long[] {100, 10};
        highWatermark.set(10);
        manager.recover(src);
        handOver("k0", 10, false);
        handOver("k1", 11, false);
        highWatermark.set(12);
        Map<String, Set<Long>> before = lakeMasks(unionRead(100));
        assertThat(before).containsOnly(entry(F1, set(0L)), entry(F2, set(0L)));

        // 200 compacts f1 and f2 into f3
        src.publish(200, 2, 10, dict(3, F3), new Index().put("k0", 3, 0).put("k1", 3, 1).build());
        src.replaced.put(2L, Arrays.asList(F1, F2));
        AtomicInteger drops = new AtomicInteger();
        hooks(manager)
                .setBeforeFileDrop(
                        name -> {
                            if (drops.incrementAndGet() == 2) {
                                throw new java.io.UncheckedIOException(
                                        new IOException("disk failed"));
                            }
                        });
        assertThatThrownBy(() -> manager.ensureCovered(200)).hasMessageContaining("disk failed");

        assertThat(manager.getReadableSnapshotId()).isEqualTo(100L);
        assertThat(db.fileDict().getFilePath(1)).isEqualTo(F1);
        assertThat(db.fileDict().getFilePath(2)).isEqualTo(F2);
        assertThat(lakeMasks(unionRead(100))).isEqualTo(before);
        assertThat(asStrings(lookupWhenReady(100, Arrays.asList(key("k0"), key("k1")))))
                .containsExactly("k0@" + F1 + "#0", "k1@" + F2 + "#0");

        hooks(manager).setBeforeFileDrop(null);
        assertThat(manager.ensureCovered(200)).isTrue();
        assertThat(lakeMasks(unionRead(200))).containsOnly(entry(F3, set(0L, 1L)));
        // nothing the failed switch held keeps the RocksDB open
        manager.setCloseWaitMillis(10_000);
        manager.close();
        assertThat(manager.dbClosed()).isDone();
    }

    /**
     * A full resolve of a long change list looks its keys up a chunk per task, so the resolver
     * threads it shares with the other buckets of the server run their work between its chunks.
     */
    @Test
    void testFullResolveYieldsTheResolverBetweenChunks() throws Exception {
        DerivedDvRuntime executors = new DerivedDvRuntime(1);
        DvRocksDB dbA = DvRocksDB.open(tempDir.resolve("dv-a").toString());
        DerivedDvManager a =
                new DerivedDvManager(
                        dbA,
                        highWatermark::get,
                        DvTestManagers.CHECKPOINT_INTERVAL,
                        metrics,
                        DvTestManagers.budget(),
                        executors,
                        executors.bucketState(),
                        new TestingDvHooks());
        try {
            int n = 3 * DerivedDvManager.RESOLVE_CHUNK - 1;
            Index s100 = new Index();
            for (int i = 0; i < n; i++) {
                s100.put("k" + i, 1, i);
                src.append("k" + i, 10 + i, 10 + i);
            }
            src.publish(100, 1, 10, dict(1, F1), s100.build());
            src.readable = new long[] {100, 10};
            highWatermark.set(10L + n);
            List<String> events = new CopyOnWriteArrayList<>();
            CountDownLatch firstChunk = new CountDownLatch(1);
            CountDownLatch probeQueued = new CountDownLatch(1);
            hooks(a).setBeforeResolveLookup(
                            () -> {
                                events.add("chunk");
                                if (firstChunk.getCount() > 0) {
                                    firstChunk.countDown();
                                    waitFor(probeQueued).run();
                                }
                            });
            a.recover(src);
            assertThat(firstChunk.await(30, TimeUnit.SECONDS)).isTrue();
            executors.keyLookup().execute(() -> events.add("probe"));
            probeQueued.countDown();

            waitUntil(() -> events.size() == 4);
            assertThat(events).containsExactly("chunk", "probe", "chunk", "chunk");
            assertThat(lakeMasks(unionRead(a, 100, true)).get(F1)).hasSize(n);
        } finally {
            a.close();
            dbA.close();
            executors.shutdown();
        }
    }

    /**
     * A rebuild ingests the index off the owner, as a switch does, so writes keep flowing; only the
     * install of the index and the change list is an owner step.
     */
    @Test
    void testRebuildIngestsTheIndexWithoutBlockingWrites() throws Exception {
        src.publish(100, 1, 10, dict(1, F1), new Index().put("k1", 1, 0).put("k2", 1, 1).build());
        src.readable = new long[] {100, 10};
        src.append("k1", 10, 10);
        highWatermark.set(11);
        List<Throwable> blocked = new CopyOnWriteArrayList<>();
        AtomicInteger ingests = new AtomicInteger();
        hooks(manager)
                .setAfterIngest(
                        () -> {
                            if (ingests.incrementAndGet() > 1) {
                                return;
                            }
                            // a write during the ingest
                            DvEntry entry = src.written("k2", 11, 11);
                            src.append("k2", 11, 11);
                            CompletableFuture<Void> write =
                                    CompletableFuture.runAsync(
                                            () ->
                                                    DvTestManagers.handOver(
                                                            manager,
                                                            Collections.singletonList(entry)),
                                            async);
                            try {
                                write.get(5, TimeUnit.SECONDS);
                            } catch (Exception e) {
                                blocked.add(e);
                            }
                        });
        manager.recover(src);
        hooks(manager).setAfterIngest(null);

        assertThat(ingests).hasValue(1);
        assertThat(blocked).isEmpty();
        highWatermark.set(12);
        assertThat(lakeMasks(unionRead(100))).containsOnly(entry(F1, set(0L, 1L)));
    }

    /**
     * A switch that must rebuild in full, as the trimmed chain no longer holds the covered
     * snapshot, loads the newest checkpoint over the index of the covered snapshot. A key the
     * covered snapshot holds and a later snapshot deleted is in neither the checkpoint nor the
     * changes after it, so the load must not keep its old position: a lookup of it finds nothing,
     * and no mask names its row. On a table with row TTL an insert of the key is looked up in the
     * index, as here.
     */
    @Test
    void testFullRebuildFromACheckpointDropsKeysDeletedSinceTheCoveredSnapshot() throws Exception {
        manager.useRowTtl();
        // snapshot 100 covers [0, 10): a -> f1#0, b -> f1#1
        src.publish(100, 1, 10, dict(1, F1), new Index().put("a", 1, 0).put("b", 1, 1).build());
        src.readable = new long[] {100, 10};
        src.append("a", 0, 0, true);
        src.append("b", 1, 1, true);
        highWatermark.set(10);
        manager.recover(src);
        assertThat(asStrings(lookupWhenReady(100, Arrays.asList(key("a"), key("b")))))
                .containsExactly("a@" + F1 + "#0", "b@" + F1 + "#1");

        // b is deleted at 12; snapshot 200 covers [0, 20) and deletes it, and b is inserted again
        src.append("b", 12, -1);
        src.publish(200, 2, 20, Collections.emptyMap(), new Index().delete("b").build());
        src.append("b", 25, 25, true);
        highWatermark.set(26);
        // tiering checkpointed 200 and trimmed the chain before it
        src.chain.remove(0);
        src.trimmed = true;
        src.checkpoint = new PositionIndexStore.Checkpoint(0, 200);
        src.checkpointIndex = new Index().put("a", 1, 0).build();
        src.checkpointDict = dict(1, F1);
        src.readable = new long[] {200, 20};

        assertThat(manager.ensureCovered(200)).isTrue();
        assertThat(src.downloadedCheckpoints).containsExactly(200L);
        assertThat(asStrings(lookupWhenReady(200, Arrays.asList(key("a"), key("b")))))
                .containsExactly("a@" + F1 + "#0");
        DvSnapshot masks = unionRead(200);
        assertThat(lakeMasks(masks).getOrDefault(F1, Collections.emptySet())).doesNotContain(1L);
    }
}
