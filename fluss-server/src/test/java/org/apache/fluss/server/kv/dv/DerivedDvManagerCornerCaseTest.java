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

import org.apache.fluss.exception.DeletionVectorsNotReadyException;
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.server.kv.dv.DerivedDvManagerTest.Index;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.dict;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.key;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.lakeMasks;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.offsets;
import static org.apache.fluss.server.kv.dv.DvTestManagers.hooks;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests of a bucket's {@link DerivedDvManager} at the edges of its life and its bounds: before it
 * recovers, while it closes, on bulk deletes, failed switches and renewals of its mask state, and
 * with the mask cache on.
 */
class DerivedDvManagerCornerCaseTest extends DerivedDvManagerTestBase {

    /**
     * A new leader whose state does not recover yet covers nothing: a read of the empty lake is not
     * ready, as a read of a snapshot is, rather than served masks without the change list.
     */
    @Test
    void testReadOfTheEmptyLakeIsNotReadyBeforeRecoveryStarts() throws Exception {
        handOver("k1", 0, true);
        handOver("k1", 1, false);
        highWatermark.set(2);

        for (boolean wait : new boolean[] {true, false}) {
            assertThatThrownBy(() -> DvTestReads.unionRead(manager, -1, wait))
                    .isInstanceOf(DeletionVectorsNotReadyException.class);
            assertThatThrownBy(() -> DvTestReads.unionRead(manager, 5, wait))
                    .isInstanceOf(DeletionVectorsNotReadyException.class);
        }

        // once it recovers, the change list is the whole log: offset 0 is superseded
        manager.recover(src);
        assertThat(offsets(unionRead(-1).getLogDvBitmap())).containsExactly(0L);
    }

    /**
     * A read that waits sees every write acknowledged before it, so masks built a moment ago serve
     * it again only while the high watermark stays where they were built. A read that does not wait
     * may take them, as it takes any exact masks not older than the ones served before.
     */
    @Test
    void testCachedMasksServeAWaitingReadOnlyAtTheSameHighWatermark() throws Exception {
        manager.setMaskCacheMillis(TimeUnit.HOURS.toMillis(1));
        manager.recover(src);
        handOver("k1", 0, true);
        highWatermark.set(1);
        assertThat(unionRead(-1).getLogEndOffset()).isEqualTo(1L);

        // an update of k1 is acknowledged: the high watermark passes it
        handOver("k1", 1, false);
        highWatermark.set(2);
        DvSnapshot second = unionRead(-1);
        assertThat(second.getLogEndOffset()).isEqualTo(2L);
        assertThat(offsets(second.getLogDvBitmap())).containsExactly(0L);

        handOver("k1", 2, false);
        highWatermark.set(3);
        assertThat(unionRead(-1, false).getLogEndOffset()).isEqualTo(2L);
        DvSnapshot fourth = unionRead(-1);
        assertThat(fourth.getLogEndOffset()).isEqualTo(3L);
        assertThat(offsets(fourth.getLogDvBitmap())).containsExactly(0L, 1L);
    }

    /**
     * A round that deletes many lake keys names them as tombstones, which count towards the keys a
     * switch derives again. A bulk delete therefore looks the remaining lake keys up again, instead
     * of reading every deleted key onto the heap, and the masks stay exact.
     */
    @Test
    void testBulkDeleteSwitchDerivesTheMasksAgain() throws Exception {
        int n = DerivedDvManager.RESOLVE_CHUNK + 2_000;
        Index s100 = new Index();
        for (int i = 0; i < n; i++) {
            s100.put("k" + i, 1, i);
            src.append("k" + i, i, i, true);
        }
        src.publish(100, 1, n, dict(1, F1), s100.build());
        src.readable = new long[] {100, n};
        highWatermark.set(n);
        manager.recover(src);
        List<DvEntry> deletes = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            src.append("k" + i, n + i, -1);
            deletes.add(new DvEntry(key("k" + i), n + i, -1, false, i));
        }
        DvTestManagers.handOver(manager, deletes);
        highWatermark.set(2L * n);
        assertThat(lakeMasks(unionRead(100)).get(F1)).hasSize(n);

        Index s200 = new Index();
        for (int i = 0; i < n; i++) {
            s200.delete("k" + i);
        }
        src.publish(200, 2, 2L * n, Collections.emptyMap(), s200.build());
        long lookedUp = manager.lookedUpKeys();
        assertThat(manager.ensureCovered(200)).isTrue();

        assertThat(manager.targetedMoves()).containsExactly(0L, 0L);
        assertThat(manager.lookedUpKeys() - lookedUp).isLessThan(n);
        DvSnapshot masks = unionRead(200);
        assertThat(masks.getLakeDvEntries()).isEmpty();
        assertThat(masks.getSnapshotStartOffset()).isEqualTo(2L * n);
    }

    /**
     * A switch that fails after it downloaded its index changes, while it reads the snapshot's
     * identity or the keys the changes name, deletes the downloads.
     */
    @Test
    void testFailedSwitchDeletesItsDownloads() throws Exception {
        recoverAtSnapshot100();
        unionRead(100);
        Path remote = tempDir.resolve("remote");
        src.publish(200, 2, 14, dict(2, F2), new Index().put("k1", 2, 0).build());

        src.identityFailure = new IOException("identity unreadable");
        assertThatThrownBy(() -> manager.ensureCovered(200))
                .hasMessageContaining("identity unreadable");
        assertThat(downloads(remote)).isEmpty();

        src.identityFailure = null;
        src.corruptDownloads = true;
        assertThatThrownBy(() -> manager.ensureCovered(200)).isInstanceOf(IOException.class);
        assertThat(downloads(remote)).isEmpty();

        src.corruptDownloads = false;
        assertThat(manager.ensureCovered(200)).isTrue();
    }

    /**
     * The keys waiting for their lake positions count their heap, the key's bytes and its wrapper,
     * towards the server's budget, not only a list slot.
     */
    @Test
    void testKeysWaitingForLookupsCountTheirHeap() throws Exception {
        recoverWithTenLakeKeys();
        unionRead(100);
        hooks(manager).setResolvesPaused(true);
        long before = manager.tailMemoryBytes();
        int n = 1_000;
        List<DvEntry> changes = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            changes.add(new DvEntry(key(String.format("%064d", i)), 10 + i, 10 + i, false, -1));
        }
        DvTestManagers.handOver(manager, changes);
        highWatermark.set(10L + n);
        manager.settle();
        assertThat(manager.inspect().unresolvedKeys).isEqualTo(n);
        assertThat(manager.tailMemoryBytes() - before).isGreaterThanOrEqualTo(n * (64L + 48L));
        hooks(manager).setResolvesPaused(false);
    }

    /**
     * A request that waits for its bucket while the bucket's leader closes is answered as sent to a
     * server that is not the leader, so the client looks the new leader up.
     */
    @Test
    void testRequestWaitingWhileTheLeaderClosesIsAnsweredNotLeader() throws Exception {
        src.publish(100, 1, 10, dict(1, F1), new Index().put("k1", 1, 0).build());
        src.readable = new long[] {100, 10};
        src.append("k1", 10, 10);
        highWatermark.set(11);
        CountDownLatch gate = pauseReplayAfter(0);
        try {
            manager.recoverInBackground(src, DerivedDvRebuildLimiter.DEFAULT);
            assertThat(src.replayPaused.await(10, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<DvSnapshot> read =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return DvTestReads.unionRead(manager, 100);
                                } catch (Exception e) {
                                    throw new CompletionException(e);
                                }
                            },
                            async);
            Thread.sleep(50);
            manager.setCloseWaitMillis(10);
            manager.close();
            assertThatThrownBy(() -> read.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(NotLeaderOrFollowerException.class);
            assertThatThrownBy(() -> DvTestReads.unionRead(manager, 100))
                    .isInstanceOf(NotLeaderOrFollowerException.class);
            assertThatThrownBy(() -> manager.lookup(100, Collections.singletonList(key("k1"))))
                    .isInstanceOf(NotLeaderOrFollowerException.class);
        } finally {
            gate.countDown();
        }
        // the replay ends once the gate opens and closes the RocksDB
        assertThat(manager.dbClosed().get(30, TimeUnit.SECONDS)).isNull();
    }

    /**
     * A tiering lookup during which the bucket's change list was reset, on the covered snapshot,
     * answers with the positions of that snapshot: it reads one version of the index, which a reset
     * does not change.
     */
    @Test
    void testLookupWhileTheChangeListResetsAnswersForItsVersion() throws Exception {
        recoverAtSnapshot100();
        unionRead(100);
        AtomicInteger reads = new AtomicInteger();
        hooks(manager)
                .setAfterLookupRead(
                        () -> {
                            if (reads.incrementAndGet() == 1) {
                                manager.handOffFailed(new IOException("hand-off failed"));
                            }
                        });
        List<byte[]> keys = Arrays.asList(key("k1"), key("k3"));
        assertThat(asStrings(manager.lookup(100, keys)))
                .containsExactly("k1@" + F1 + "#0", "k3@" + F1 + "#2");
        assertThat(asStrings(manager.lookup(100, keys)))
                .containsExactly("k1@" + F1 + "#0", "k3@" + F1 + "#2");
        assertThat(reads).hasValue(2);
    }

    /** A switch that ingests several index changes stops between them once the bucket closes. */
    @Test
    void testSwitchStopsIngestingOnceTheBucketCloses() throws Exception {
        recoverAtSnapshot100();
        unionRead(100);
        src.publish(200, 2, 14, dict(2, F2), new Index().put("k1", 2, 0).build());
        src.publish(300, 3, 14, dict(3, F3), new Index().put("k3", 3, 0).build());
        AtomicInteger ingests = new AtomicInteger();
        CompletableFuture<Void> closing = new CompletableFuture<>();
        hooks(manager)
                .setAfterIngest(
                        () -> {
                            if (ingests.incrementAndGet() == 1) {
                                closing.complete(null);
                                CompletableFuture.runAsync(manager::close, async);
                                try {
                                    waitUntil(manager::isClosed);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        });
        assertThat(manager.ensureCovered(300)).isFalse();
        assertThat(closing).isDone();
        assertThat(ingests).hasValue(1);
        assertThat(manager.dbClosed().get(10, TimeUnit.SECONDS)).isNull();
    }

    /**
     * A replay installs a new change list whose queue starts back at the readable offset. Masks
     * built while changes below the old list's applied cut are still queued would miss them: with
     * one change applied a step, every read after the replay gets the masks of the whole log.
     */
    @Test
    void testAReplayedChangeListIsAppliedBeforeItsMasksAreServed() throws Exception {
        recoverWithTenLakeKeys();
        manager.setStepChunks(1, 1L << 40);
        manager.setReadyRefreshMillis(0);
        manager.setMaskCacheMillis(60_000);
        handOver("k20", 10, true);
        handOver("k20", 11, false);
        handOver("k20", 12, false);
        handOver("k0", 13, false);
        highWatermark.set(14);
        String expected = DerivedDvChangeListTest.describe(unionRead(100));
        assertThat(expected).contains(F1);
        DvTestReads.unionRead(manager, 100, false);

        // the change list is lost and replayed from the log
        manager.handOffFailed(new IllegalStateException("hand-off failed"));
        List<String> wrong = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 1_000;
        while (System.currentTimeMillis() < deadline) {
            try {
                String served =
                        DerivedDvChangeListTest.describe(
                                DvTestReads.unionRead(manager, 100, false));
                if (served.contains("end=14") && !served.equals(expected)) {
                    wrong.add(served);
                }
            } catch (Exception e) {
                // read by key or not ready while the change list is replayed
            }
            Thread.sleep(2);
        }
        assertThat(wrong).isEmpty();
        assertThat(DerivedDvChangeListTest.describe(unionRead(100))).isEqualTo(expected);
    }
}
