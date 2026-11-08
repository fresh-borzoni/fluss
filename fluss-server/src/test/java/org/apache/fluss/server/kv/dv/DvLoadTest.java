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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.dict;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.key;
import static org.apache.fluss.server.kv.dv.DvTestManagers.hooks;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Fault injection in each step of a load ({@link DvLoad}): whatever fails, the version the bucket
 * covered keeps serving, no lease of the RocksDB is left held, and no download is left on disk.
 */
class DvLoadTest extends DerivedDvManagerTestBase {

    // the covered version holds one lease of the RocksDB while the bucket is open
    private static final int IDLE_LEASES = 1;
    // how long a held compaction waits to be cancelled
    private static final long CANCEL_WAIT_MS = 20_000;

    /** A switch whose download or snapshot identity fails applies nothing and leaves nothing. */
    @Test
    void testFailedDownloadLeavesTheVersionServing() throws Exception {
        recoverAtSnapshot100();
        Path remote = tempDir.resolve("remote");
        src.publish(101, 2, 14, dict(2, F2), new Index().put("k1", 2, 0).build());

        src.corruptDownloads = true;
        assertThatThrownBy(() -> manager.ensureCovered(101)).isInstanceOf(IOException.class);
        assertServing100(remote);

        src.corruptDownloads = false;
        src.identityFailure = new IOException("identity unreadable");
        assertThatThrownBy(() -> manager.ensureCovered(101))
                .hasMessageContaining("identity unreadable");
        assertServing100(remote);

        src.identityFailure = null;
        assertThat(manager.ensureCovered(101)).isTrue();
        assertThat(asStrings(manager.lookup(101, Collections.singletonList(key("k1")))))
                .containsExactly("k1@" + F2 + "#0");
        assertIdle(remote);
    }

    /**
     * A switch whose ingest fails part way leaves the latest index ahead of the covered version,
     * which still answers as its snapshot; the next switch applies every change again.
     */
    @Test
    void testIngestFailingPartWayLeavesTheVersionServing() throws Exception {
        recoverAtSnapshot100();
        Path remote = tempDir.resolve("remote");
        src.publish(101, 2, 14, dict(2, F2), new Index().put("k1", 2, 0).build());
        src.publish(102, 3, 14, dict(3, F3), new Index().put("k2", 3, 0).build());
        src.readable = new long[] {102, 14};
        AtomicInteger ingests = new AtomicInteger();
        hooks(manager)
                .setAfterIngest(
                        () -> {
                            if (ingests.incrementAndGet() == 1) {
                                throw new IllegalStateException("the ingest failed");
                            }
                        });
        assertThatThrownBy(() -> manager.ensureCovered(102))
                .hasMessageContaining("the ingest failed");
        assertThat(manager.indexAhead()).isTrue();
        assertServing100(remote);

        hooks(manager).setAfterIngest(null);
        assertThat(manager.ensureCovered(102)).isTrue();
        assertThat(manager.indexAhead()).isFalse();
        assertThat(asStrings(manager.lookup(102, Arrays.asList(key("k1"), key("k2")))))
                .containsExactly("k1@" + F2 + "#0", "k2@" + F3 + "#0");
        assertIdle(remote);
    }

    /** A swap whose dictionary batch fails applies nothing of the switch. */
    @Test
    void testFailedDictionaryBatchLeavesTheVersionServing() throws Exception {
        recoverAtSnapshot100();
        Path remote = tempDir.resolve("remote");
        src.publish(101, 2, 14, dict(2, F2), new Index().put("k1", 2, 0).build());
        src.replaced.put(2L, Collections.singletonList(F1));
        hooks(manager)
                .setBeforeFileDrop(
                        name -> {
                            throw new UncheckedIOException(new IOException("disk failed"));
                        });
        assertThatThrownBy(() -> manager.ensureCovered(101)).hasMessageContaining("disk failed");
        assertServing100(remote);
        assertThat(db.fileDict().getFilePath(1)).isEqualTo(F1);

        hooks(manager).setBeforeFileDrop(null);
        assertThat(manager.ensureCovered(101)).isTrue();
        assertIdle(remote);
    }

    /**
     * A swap whose trim of the change list fails covers the new snapshot all the same, with its
     * change list missing: it is replayed from the log before masks are served.
     */
    @Test
    void testFailedTrimDropsOnlyTheChangeList() throws Exception {
        recoverAtSnapshot100();
        Path remote = tempDir.resolve("remote");
        unionRead(100);
        src.publish(101, 2, 12, dict(2, F2), new Index().put("k1", 2, 0).build());
        src.replaced.put(2L, Collections.emptyList());
        // the change list's disk fails from the swap on
        hooks(manager)
                .setBeforeFileDrop(
                        name ->
                                hooks(manager)
                                        .failChangeListWrites(new IOException("change list disk")));
        hooks(manager)
                .setAfterIngest(
                        () ->
                                hooks(manager)
                                        .failChangeListWrites(new IOException("change list disk")));
        assertThat(manager.ensureCovered(101)).isTrue();
        assertThat(manager.inspect().changeListPresent).isFalse();
        assertThat(asStrings(manager.lookup(101, Collections.singletonList(key("k1")))))
                .containsExactly("k1@" + F2 + "#0");

        hooks(manager).setAfterIngest(null);
        hooks(manager).setBeforeFileDrop(null);
        hooks(manager).failChangeListWrites(null);
        long deadline = System.currentTimeMillis() + 30_000;
        while (true) {
            try {
                unionRead(101);
                break;
            } catch (Exception e) {
                assertThat(System.currentTimeMillis()).isLessThan(deadline);
                Thread.sleep(50);
            }
        }
        assertThat(manager.inspect().changeListPresent).isTrue();
        assertIdle(remote);
    }

    /**
     * A load the bucket closes under ends at its next unit, in whichever step it is held: it
     * releases its leases and deletes its downloads, and the RocksDB closes.
     */
    @ParameterizedTest
    @ValueSource(strings = {"download", "ingest", "replay"})
    void testCloseDuringALoadLeavesNothing(String step) throws Exception {
        recoverAtSnapshot100();
        Path remote = tempDir.resolve("remote");
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch held = new CountDownLatch(1);
        CompletableFuture<?> load;
        if (step.equals("replay")) {
            manager.handOffFailed(new IllegalStateException("hand-off failed"));
            pauseReplayAfter(0);
            src.replayGate = release;
            load = CompletableFuture.runAsync(() -> unionReadQuietly(100), async);
            assertThat(src.replayPaused.await(10, TimeUnit.SECONDS)).isTrue();
        } else {
            src.publish(101, 2, 14, dict(2, F2), new Index().put("k1", 2, 0).build());
            src.publish(102, 3, 14, dict(3, F3), new Index().put("k2", 3, 0).build());
            src.readable = new long[] {102, 14};
            if (step.equals("download")) {
                src.changeGate = release;
            } else {
                hooks(manager)
                        .setAfterIngest(
                                () -> {
                                    held.countDown();
                                    waitFor(release).run();
                                });
            }
            load = CompletableFuture.runAsync(() -> ensureQuietly(102), async);
            CountDownLatch entered = step.equals("download") ? src.changeEntered : held;
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
        }
        manager.setCloseWaitMillis(10);
        manager.close();
        if (step.equals("download")) {
            // no lease is held across remote reads: the RocksDB closes while the download waits
            assertThat(manager.dbClosed().get(10, TimeUnit.SECONDS)).isNull();
        }
        release.countDown();
        load.get(30, TimeUnit.SECONDS);
        assertThat(manager.dbClosed().get(30, TimeUnit.SECONDS)).isNull();
        assertThat(manager.leasesHeld()).isZero();
        // the load releases what it owns once its job ends, after the close
        waitUntil(() -> downloadsGone(remote));
    }

    /**
     * A bucket that closes while a rebuild compacts the index it ingested in one call cancels the
     * compaction rather than wait for it: the close ends at once, and the load leaves nothing.
     */
    @Test
    void testCloseDuringARebuildsCompactionCancelsIt() throws Exception {
        Path remote = tempDir.resolve("remote");
        publishSnapshots100And101();
        CountDownLatch compacting = new CountDownLatch(1);
        AtomicBoolean cancelled = new AtomicBoolean();
        hooks(manager)
                .setBeforeCompaction(
                        c -> {
                            compacting.countDown();
                            cancelled.set(awaitCancel(c));
                        });
        CompletableFuture<?> load = CompletableFuture.runAsync(this::recoverQuietly, async);
        assertThat(compacting.await(10, TimeUnit.SECONDS)).isTrue();

        manager.setCloseWaitMillis(3 * CANCEL_WAIT_MS);
        long start = System.currentTimeMillis();
        manager.close();
        assertThat(System.currentTimeMillis() - start).isLessThan(CANCEL_WAIT_MS);
        assertThat(cancelled).isTrue();
        load.get(30, TimeUnit.SECONDS);
        assertThat(manager.dbClosed().get(30, TimeUnit.SECONDS)).isNull();
        assertThat(manager.leasesHeld()).isZero();
        waitUntil(() -> downloadsGone(remote));
    }

    /**
     * A fault of the owner while a rebuild compacts the index it ingested in one call cancels the
     * compaction: the load ends at once and swaps nothing in, and the next catch-up rebuilds.
     */
    @Test
    void testAFaultDuringARebuildsCompactionCancelsItAndTheNextCatchUpRebuilds() throws Exception {
        Path remote = tempDir.resolve("remote");
        publishSnapshots100And101();
        CountDownLatch compacting = new CountDownLatch(1);
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicInteger compactions = new AtomicInteger();
        hooks(manager)
                .setBeforeCompaction(
                        c -> {
                            if (compactions.incrementAndGet() == 1) {
                                compacting.countDown();
                                cancelled.set(awaitCancel(c));
                            }
                        });
        CompletableFuture<?> load = CompletableFuture.runAsync(this::recoverQuietly, async);
        assertThat(compacting.await(10, TimeUnit.SECONDS)).isTrue();

        // an unexpected exception in an unrelated step while the rebuild compacts
        long start = System.currentTimeMillis();
        hooks(manager)
                .setFaultInNextDrain(
                        () -> {
                            throw new IllegalStateException("injected");
                        });
        src.append("k4", 10, 10);
        DvTestManagers.handOver(manager, Collections.singletonList(src.written("k4", 10, 10)));
        load.get(30, TimeUnit.SECONDS);
        assertThat(System.currentTimeMillis() - start).isLessThan(CANCEL_WAIT_MS);
        assertThat(cancelled).isTrue();

        assertThat(asStrings(lookupWhenReady(101, Arrays.asList(key("k1"), key("k2")))))
                .containsExactly("k1@" + F2 + "#0", "k2@" + F1 + "#1");
        assertThat(compactions.get()).isEqualTo(2);
        assertIdle(remote);
    }

    // ------------------------------------------------------------------------------------------

    /** Snapshot 100 puts k1..k3 in F1, snapshot 101 moves k1 to F2; nothing above. */
    private void publishSnapshots100And101() {
        src.publish(
                100,
                1,
                10,
                dict(1, F1),
                new Index().put("k1", 1, 0).put("k2", 1, 1).put("k3", 1, 2).build());
        src.publish(101, 2, 10, dict(2, F2), new Index().put("k1", 2, 0).build());
        src.readable = new long[] {101, 10};
        highWatermark.set(10);
    }

    /** Waits up to {@link #CANCEL_WAIT_MS} for a compaction to be cancelled; whether it was. */
    private static boolean awaitCancel(BooleanSupplier cancelled) {
        long deadline = System.currentTimeMillis() + CANCEL_WAIT_MS;
        while (!cancelled.getAsBoolean() && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return cancelled.getAsBoolean();
    }

    private void recoverQuietly() {
        try {
            manager.recover(src);
        } catch (Exception e) {
            // the bucket closes under it, or a fault ends it
        }
    }

    private void assertServing100(Path remote) throws Exception {
        assertThat(manager.getReadableSnapshotId()).isEqualTo(100L);
        assertThat(asStrings(manager.lookup(100, Collections.singletonList(key("k1")))))
                .containsExactly("k1@" + F1 + "#0");
        assertIdle(remote);
    }

    private void assertIdle(Path remote) throws Exception {
        assertThat(downloads(remote)).isEmpty();
        waitUntil(() -> manager.leasesHeld() == IDLE_LEASES);
    }

    private static boolean downloadsGone(Path remote) {
        try {
            return downloads(remote).isEmpty();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void unionReadQuietly(long snapshotId) {
        try {
            unionRead(snapshotId);
        } catch (Exception e) {
            // the bucket closes under it
        }
    }

    private void ensureQuietly(long snapshotId) {
        try {
            manager.ensureCovered(snapshotId);
        } catch (Exception e) {
            // the bucket closes under it
        }
    }

    /**
     * A new leader that rebuilds a chain with an older rewrite after a newer round, e.g. applied
     * out of order or twice, ingests its changes one by one instead of merging them: the rewrite
     * keeps the newer round's position, as a switch does.
     */
    @Test
    void testRebuildOfARewriteAfterANewerRoundKeepsTheNewerPosition() throws Exception {
        src.publish(
                100,
                1,
                10,
                dict(1, F1),
                new Index().put("k1", 1, 0).put("k2", 1, 1).put("k3", 1, 2).build());
        src.publish(200, 3, 10, dict(3, F3), new Index().put("k1", 3, 0).build());
        src.publish(
                300,
                2,
                10,
                dict(2, F2),
                new Index().put("k1", 2, 0).put("k2", 2, 1).put("k3", 2, 2).build());
        src.replaced.put(2L, Collections.singletonList(F1));
        src.readable = new long[] {300, 10};
        highWatermark.set(10);

        manager.recover(src);

        assertThat(asStrings(lookupWhenReady(300, Arrays.asList(key("k1"), key("k3")))))
                .containsExactly("k1@" + F3 + "#0", "k3@" + F2 + "#2");
    }
}
