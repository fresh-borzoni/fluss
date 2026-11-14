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

import org.apache.fluss.server.kv.snapshot.DvChangeListCheckpoints;
import org.apache.fluss.server.kv.snapshot.DvChangeListHandle;
import org.apache.fluss.server.kv.snapshot.KvFileHandle;
import org.apache.fluss.server.kv.snapshot.KvFileHandleAndLocalPath;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Change-list checkpoints end to end on the bucket's owner: a live leader saves its change list
 * with a KV snapshot at X, a new leader restores it with the KV at X and replays the log from X,
 * and its masks are those of a new leader that replays the whole log from the readable offset, and
 * of the live leader. Also every reason a new leader replays without the checkpoint, and how the
 * saved segments are reused from one KV snapshot to the next.
 */
class DvChangeListCheckpointTest extends DvCheckpointTestBase {

    // ---- the checkpoint gives the full replay's masks -------------------------------------------

    @Test
    void testInsertBeforeXUpdatedAfterX() throws Exception {
        DvChangeListWorkload w = new DvChangeListWorkload();
        w.upsert("k0").upsert("k1").endBatch(); // 0, 1: tiered
        long o = w.end();
        w.upsert("k2").endBatch(); // 2
        w.upsert("k1").endBatch(); // 3, 4 supersedes the lake row of k1
        long x = w.end();
        w.upsert("k2").endBatch(); // 5, 6 supersedes 2, a log version only the KV at X names
        w.upsert("k0").endBatch(); // 7, 8 supersedes the lake row of k0, named from the KV at X

        Leader b = rebuildFromCheckpoint(w, o, x);

        assertThat(b.manager.lastRebuildCheckpointFields())
                .contains("ckpt=used", "ckpt_end=" + x, "ckpt_changes=1", "replay_from=" + x)
                .contains("view_reads=2");
        assertThat(b.src.replayedFrom).startsWith(x);
        // k2@2 is superseded in the log; k1's and k0's lake rows are masked
        String masks = masks(b);
        assertThat(masks).contains("log=[2]").contains(F1 + "=[0, 1]");
        assertThat(masks).isEqualTo(masks(fullReplay(w, o)));
    }

    @Test
    void testRandomWorkloads() throws Exception {
        Random random = new Random(20261114L);
        for (int run = 0; run < 25; run++) {
            DvChangeListWorkload w =
                    DvChangeListWorkload.random(
                            random, 1 + random.nextInt(30), 2 + random.nextInt(40), 6);
            List<Long> boundaries = new ArrayList<>(w.kvAt.keySet());
            long a = boundaries.get(random.nextInt(boundaries.size()));
            long b = boundaries.get(random.nextInt(boundaries.size()));
            long o = Math.min(a, b);
            long x = Math.max(a, b);

            Leader live = liveLeader(w, o);
            DvChangeListHandle part = export(live, 1, x);
            Leader rebuilt = rebuild(w, o, x, part);
            Leader full = fullReplay(w, o);

            assertThat(rebuilt.manager.lastRebuildCheckpointFields())
                    .as("run %d, o %d, x %d", run, o, x)
                    .contains("ckpt=used", "replay_from=" + x);
            assertThat(masks(rebuilt)).as("run %d, o %d, x %d", run, o, x).isEqualTo(masks(full));
            assertThat(masks(full)).isEqualTo(masks(live));
            closeAll();
        }
    }

    @Test
    void testReadableOffsetInsideTheExportedRange() throws Exception {
        // the old leader covered an older snapshot than the new leader's readable one, which is
        // still before X: the checkpoint is used from the readable offset on
        Random random = new Random(424242L);
        int checked = 0;
        for (int run = 0; run < 30; run++) {
            DvChangeListWorkload w =
                    DvChangeListWorkload.random(
                            random, 1 + random.nextInt(25), 6 + random.nextInt(40), 6);
            List<Long> boundaries = new ArrayList<>(w.kvAt.keySet());
            long[] picks = new long[3];
            for (int i = 0; i < 3; i++) {
                picks[i] = boundaries.get(random.nextInt(boundaries.size()));
            }
            Arrays.sort(picks);
            long exported = picks[0];
            long o = picks[1];
            long x = picks[2];
            if (!(exported < o && o < x)) {
                continue;
            }
            Leader live = liveLeader(w, exported);
            DvChangeListHandle part = export(live, 1, x);
            Leader rebuilt = rebuild(w, o, x, part);
            Leader full = fullReplay(w, o);

            assertThat(part.from()).isEqualTo(exported);
            assertThat(rebuilt.manager.lastRebuildCheckpointFields())
                    .as("run %d", run)
                    .contains("ckpt=used", "replay_from=" + x);
            assertThat(masks(rebuilt))
                    .as("run %d: exported %d, o %d, x %d", run, exported, o, x)
                    .isEqualTo(masks(full));
            checked++;
            closeAll();
        }
        assertThat(checked).isGreaterThan(5);
    }

    @Test
    void testSpilledReplayMapWithACheckpoint() throws Exception {
        // the replay's map of versions on disk: a key the view names as absent at X is kept
        // apart from a key the replay has not seen
        Random random = new Random(20261114L);
        for (int run = 0; run < 25; run++) {
            DvChangeListWorkload w =
                    DvChangeListWorkload.random(
                            random, 1 + random.nextInt(30), 2 + random.nextInt(40), 6);
            List<Long> boundaries = new ArrayList<>(w.kvAt.keySet());
            long a = boundaries.get(random.nextInt(boundaries.size()));
            long c = boundaries.get(random.nextInt(boundaries.size()));
            long o = Math.min(a, c);
            long x = Math.max(a, c);

            Leader live = liveLeader(w, o);
            DvChangeListHandle part = export(live, 1, x);
            Leader rebuilt = leader("rebuilt-spill", w, o, w.end());
            rebuilt.manager.setSpillReplayVersions(true);
            rebuilt.src.restored = restored(part, x, view(w.kvAt.get(x)), false);
            rebuilt.manager.recover(rebuilt.src);
            Leader full = leader("full-spill", w, o, w.end());
            full.manager.setSpillReplayVersions(true);
            full.manager.recover(full.src);

            assertThat(rebuilt.manager.lastRebuildCheckpointFields())
                    .as("run %d", run)
                    .contains("ckpt=used");
            assertThat(masks(rebuilt)).as("run %d, o %d, x %d", run, o, x).isEqualTo(masks(full));
            assertThat(masks(full)).isEqualTo(masks(live));
            closeAll();
        }
    }

    @Test
    void testPaimonEmptyLakeReplaysFromX() throws Exception {
        // nothing readable yet: the change list is the whole log, from 0
        DvChangeListWorkload w = DvChangeListWorkload.random(new Random(7), 8, 12, 5);
        List<Long> boundaries = new ArrayList<>(w.kvAt.keySet());
        long x = boundaries.get(boundaries.size() / 2);

        Leader live = leader("live", w, -1, 0);
        live.manager.recover(live.src);
        // the empty lake's change list is replayed in the background
        emptyLakeMasks(live);
        handOverFrom(live, w, 0);
        DvChangeListHandle part = export(live, 1, x);
        Leader rebuilt = leader("rebuilt", w, -1, w.end());
        rebuilt.src.restored = restored(part, x, view(w.kvAt.get(x)), false);
        rebuilt.manager.recover(rebuilt.src);
        Leader full = leader("full", w, -1, w.end());
        full.manager.recover(full.src);

        assertThat(part.from()).isZero();
        assertThat(part.validFrom()).isZero();
        assertThat(emptyLakeMasks(rebuilt)).isEqualTo(emptyLakeMasks(full));
        assertThat(rebuilt.manager.lastRebuildCheckpointFields())
                .contains("ckpt=used", "replay_from=" + x);
        assertThat(rebuilt.src.replayedFrom).startsWith(x);
    }

    // ---- every reason the new leader replays without it -----------------------------------------

    @Test
    void testCorruptSegmentFallsBackToTheFullReplay() throws Exception {
        Fixture f = fixture();
        File segment = localFile(f.part.segments().get(0));
        byte[] bytes = Files.readAllBytes(segment.toPath());
        bytes[bytes.length / 2] ^= 0x10;
        Files.write(segment.toPath(), bytes);

        assertFallsBack(f, rebuild(f.w, f.o, f.x, f.part), "corrupt");
    }

    @Test
    void testSegmentOfAnotherSizeIsCorrupt() throws Exception {
        Fixture f = fixture();
        DvChangeListHandle.Segment s = f.part.segments().get(0);
        KvFileHandle file = s.file().getKvFileHandle();
        DvChangeListHandle otherSize =
                new DvChangeListHandle(
                        f.part.from(),
                        f.part.end(),
                        f.part.validFrom(),
                        f.part.lakeSnapshotId(),
                        f.part.entries(),
                        Collections.singletonList(
                                new DvChangeListHandle.Segment(
                                        KvFileHandleAndLocalPath.of(
                                                new KvFileHandle(
                                                        file.getFilePath(), file.getSize() + 1),
                                                s.file().getLocalPath()),
                                        s.from(),
                                        s.end(),
                                        s.entries())));

        assertFallsBack(f, rebuild(f.w, f.o, f.x, otherSize), "corrupt");
    }

    @Test
    void testMissingSegmentFallsBackToTheFullReplay() throws Exception {
        // e.g. with kv.snapshot.num-retained = 1, the snapshot that saved it was subsumed
        Fixture f = fixture();
        Files.delete(localFile(f.part.segments().get(0)).toPath());

        assertFallsBack(f, rebuild(f.w, f.o, f.x, f.part), "missing");
    }

    @Test
    void testLakeAheadOfTheKvSnapshot() throws Exception {
        Fixture f = fixture();
        long ahead = f.w.end();

        Leader b = leader("ahead", f.w, ahead, f.w.end());
        b.src.restored = restored(f.part, f.x, view(f.w.kvAt.get(f.x)), false);
        b.manager.recover(b.src);

        assertThat(b.manager.lastRebuildCheckpointFields()).contains("ckpt=lake_ahead");
        assertThat(b.src.replayedFrom).startsWith(ahead);
        assertThat(masks(b)).isEqualTo(masks(fullReplay(f.w, ahead)));
    }

    @Test
    void testReadableOffsetBelowTheCheckpoint() throws Exception {
        // the old leader covered a later snapshot than the one the new leader finds readable
        Fixture f = fixture();
        assertThat(f.o).isPositive();

        Leader b = leader("below", f.w, 0, f.w.end());
        b.src.restored = restored(f.part, f.x, view(f.w.kvAt.get(f.x)), false);
        b.manager.recover(b.src);

        assertThat(b.manager.lastRebuildCheckpointFields()).contains("ckpt=range");
        assertThat(masks(b)).isEqualTo(masks(fullReplay(f.w, 0)));
    }

    @Test
    void testNoViewOrOffOrTtl() throws Exception {
        Fixture f = fixture();

        Leader noView = leader("noview", f.w, f.o, f.w.end());
        noView.src.restored =
                DvRestoredChangeList.restored("b", BUCKET, true, false, 1, f.x, f.part, null, -1);
        noView.manager.recover(noView.src);
        assertFallsBack(f, noView, "kv_view");

        Leader off = leader("off", f.w, f.o, f.w.end());
        off.src.restored =
                DvRestoredChangeList.restored(
                        "b", BUCKET, false, false, 1, f.x, f.part, view(f.w.kvAt.get(f.x)), 1);
        off.manager.recover(off.src);
        assertFallsBack(f, off, "off");
    }

    @Test
    void testViewThatCannotOpenFallsBackAtOnce() throws Exception {
        Fixture f = fixture();
        Leader b = leader("badview", f.w, f.o, f.w.end());
        // a view whose RocksDB does not open, e.g. its files went away
        DvKvView bad =
                DvKvView.of(
                        dir.resolve("no-such-view"),
                        0,
                        System.currentTimeMillis(),
                        v -> ByteBuffer.wrap(v).getLong());
        b.src.restored =
                DvRestoredChangeList.restored("b", BUCKET, true, false, 1, f.x, f.part, bad, 1);
        b.manager.recover(b.src);

        // the same load replays from o: no load retries the view
        assertFallsBack(f, b, "view_failed");
        assertThat(b.src.replayedFrom).containsExactly(f.o);
        assertThat(b.src.restored.releasedAs()).isEqualTo(DvRestoredChangeList.Reason.VIEW_FAILED);
    }

    @Test
    void testViewDoesNotExpireUnderTheLoadThatUsesIt() throws Exception {
        Fixture f = fixture();
        Leader b = leader("expiring", f.w, f.o, f.w.end());
        DvRestoredChangeList restored = restored(f.part, f.x, view(f.w.kvAt.get(f.x)), false);
        b.src.restored = restored;
        CountDownLatch gate = new CountDownLatch(1);
        b.src.gateAfterChanges = 1;
        b.src.replayGate = gate;
        CompletableFuture<Void> recovered =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                b.manager.recover(b.src);
                            } catch (Exception e) {
                                throw new CompletionException(e);
                            }
                        });
        assertThat(b.src.replayPaused.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(b.src.replayedFrom).containsExactly(f.x);

        // the load replays from X with the view when the view's 15 minutes are up: it stays
        assertThat(
                        restored.expireIfDue(
                                System.currentTimeMillis() + DvRestoredChangeList.VIEW_MAX_AGE_MS))
                .isFalse();
        gate.countDown();
        recovered.get(60, TimeUnit.SECONDS);

        assertThat(masks(b)).isEqualTo(masks(fullReplay(f.w, f.o)));
        assertThat(b.manager.lastRebuildCheckpointFields()).contains("ckpt=used");
        assertThat(b.src.replayedFrom).containsExactly(f.x);
    }

    @Test
    void testDiskFailureWhileLoadingTheCheckpointUndoesItsReplay() throws Exception {
        Fixture f = fixture();
        DerivedDvMetrics metrics = DerivedDvMetrics.noop();
        Leader b = new Leader("disk", metrics);
        seed(b, f.w, f.o, f.w.end());
        b.src.restored = restored(f.part, f.x, view(f.w.kvAt.get(f.x)), false);
        TestingDvHooks hooks = DvTestManagers.hooks(b.manager);
        hooks.failChangeListWrites(new IOException("the change list's disk failed"));
        CompletableFuture<Void> recovered =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                b.manager.recover(b.src);
                            } catch (Exception e) {
                                throw new CompletionException(e);
                            }
                        });
        // the load fails while it writes the checkpoint's changes; a read retries it
        retry(Duration.ofMinutes(1), () -> assertThat(hooks.changeListWritesFailed()).isPositive());
        hooks.failChangeListWrites(null);

        assertThat(masks(b)).isEqualTo(masks(fullReplay(f.w, f.o)));
        // the recovery reports the failed load; reads retried it
        assertThat(recovered).failsWithin(60, TimeUnit.SECONDS);
        // the failed load's replay was finished: no backlog stays behind
        retry(Duration.ofMinutes(1), () -> assertThat(metrics.replayBacklog()).isZero());
    }

    @Test
    void testRowTtlTableNeverUsesACheckpointAndStaysExact() throws Exception {
        DvChangeListWorkload w = DvChangeListWorkload.random(new Random(3), 10, 20, 5);
        List<Long> boundaries = new ArrayList<>(w.kvAt.keySet());
        long o = boundaries.get(boundaries.size() / 3);
        long x = boundaries.get(2 * boundaries.size() / 3);
        Leader live = liveLeader(w, o);
        DvChangeListHandle part = export(live, 1, x);

        Leader ttl = leader("ttl", w, o, w.end());
        ttl.manager.useRowTtl();
        ttl.src.restored = restored(part, x, view(w.kvAt.get(x)), true);
        ttl.manager.recover(ttl.src);
        Leader full = leader("ttl-full", w, o, w.end());
        full.manager.useRowTtl();
        full.manager.recover(full.src);

        assertThat(ttl.manager.lastRebuildCheckpointFields()).contains("ckpt=ttl");
        assertThat(ttl.src.replayedFrom).startsWith(o);
        assertThat(masks(ttl)).isEqualTo(masks(full));
        // nor does a TTL table's leader save one
        Leader ttlLive = leader("ttl-live", w, o, w.end());
        ttlLive.manager.useRowTtl();
        ttlLive.manager.recover(ttlLive.src);
        assertThat(export(ttlLive, 2, w.end())).isNull();
    }

    // ---- saving: what each KV snapshot holds ----------------------------------------------------

    @Test
    void testSegmentsAreReusedFromSnapshotToSnapshot() throws Exception {
        DvChangeListWorkload w = DvChangeListWorkload.random(new Random(11), 10, 30, 5);
        List<Long> b = new ArrayList<>(w.kvAt.keySet());
        long o = b.get(b.size() / 5);
        long x1 = b.get(2 * b.size() / 5);
        long x2 = b.get(3 * b.size() / 5);
        long x3 = b.get(4 * b.size() / 5);
        Leader live = liveLeader(w, o);

        DvChangeListHandle p1 = export(live, 1, x1);
        DvChangeListHandle p2 = export(live, 2, x2);
        // a failed snapshot: the next one still builds on the last committed one
        DvChangeListCheckpoints.Checkpoint failed = live.manager.prepare(3, x3, location());
        DvChangeListHandle lost = failed.attach();
        failed.aborted();
        DvChangeListHandle p3 = export(live, 4, x3);

        assertThat(p1.segments()).hasSize(1);
        assertThat(p1.from()).isEqualTo(o);
        assertThat(p2.segments()).hasSize(2);
        assertThat(path(p2.segments().get(0))).isEqualTo(path(p1.segments().get(0)));
        assertThat(p2.segments().get(1).from()).isEqualTo(x1);
        assertThat(lost.segments()).hasSize(3);
        assertThat(p3.segments()).hasSize(3);
        assertThat(path(p3.segments().get(1))).isEqualTo(path(p2.segments().get(1)));
        assertThat(p3.entries()).isEqualTo(count(w, o, x3));
        // what the last one saved restores to the full replay's masks
        assertThat(masks(rebuild(w, o, x3, p3))).isEqualTo(masks(fullReplay(w, o)));
    }

    @Test
    void testChangesNotAppliedInTimeAreLeftOut() throws Exception {
        DvChangeListWorkload w = DvChangeListWorkload.random(new Random(12), 10, 30, 5);
        List<Long> b = new ArrayList<>(w.kvAt.keySet());
        long o = b.get(b.size() / 4);
        Leader live = leader("live", w, o, o);
        live.manager.recover(live.src);
        // handed over, but the high watermark stays at o: nothing after it is applied
        handOverFrom(live, w, o, false);
        live.manager.setChangeListPrepareWaitMillis(50);

        assertThat(export(live, 1, w.end())).isNull();

        live.hw.set(w.end());
        live.manager.setChangeListPrepareWaitMillis(5_000);
        DvChangeListHandle next = export(live, 2, w.end());
        assertThat(next.segments()).hasSize(1);
        assertThat(next.from()).isEqualTo(o);
        assertThat(next.entries()).isEqualTo(count(w, o, w.end()));
    }

    @Test
    void testUncertainOrPartlessCommitMakesTheNextOneWhole() throws Exception {
        DvChangeListWorkload w = DvChangeListWorkload.random(new Random(13), 10, 30, 5);
        List<Long> b = new ArrayList<>(w.kvAt.keySet());
        long o = b.get(b.size() / 4);
        Leader live = liveLeader(w, o);
        export(live, 1, b.get(b.size() / 2));

        // whether snapshot 2 was committed is not known: its segments are not built on
        DvChangeListCheckpoints.Checkpoint uncertain = live.manager.prepare(2, w.end(), location());
        assertThat(uncertain.attach().segments()).hasSize(2);
        uncertain.uncertain();
        live.manager.inspect();
        DvChangeListHandle next = export(live, 3, w.end());

        assertThat(next.segments()).hasSize(1);
        assertThat(next.from()).isEqualTo(o);
    }

    @Test
    void testManySegmentsAreSavedWholeAgain() throws Exception {
        DvChangeListWorkload w = new DvChangeListWorkload();
        w.upsert("k").endBatch();
        long o = w.end();
        for (int i = 0; i < DvOwnerChangeListExport.MAX_SEGMENTS + 2; i++) {
            w.upsert("k").endBatch();
        }
        Leader live = liveLeader(w, o);
        List<Long> b = new ArrayList<>(w.kvAt.tailMap(o, false).keySet());
        DvChangeListHandle last = null;
        int most = 0;
        for (int i = 0; i < b.size(); i++) {
            last = export(live, i + 1, b.get(i));
            most = Math.max(most, last.segments().size());
        }

        assertThat(most).isEqualTo(DvOwnerChangeListExport.MAX_SEGMENTS);
        assertThat(last.segments().size()).isLessThan(DvOwnerChangeListExport.MAX_SEGMENTS);
        assertThat(last.entries()).isEqualTo(count(w, o, w.end()));
    }

    @Test
    void testNewLeaderReusesTheRestoredSegments() throws Exception {
        Fixture f = fixture();
        Leader b = leader("reuse", f.w, f.o, f.w.end());
        b.src.restored = restored(f.part, f.x, view(f.w.kvAt.get(f.x)), false);
        b.manager.recover(b.src);

        DvChangeListHandle next = export(b, 11, f.w.end());

        // the restored KV snapshot is the last one the bucket committed: no subsumption can
        // delete its segments before this part references them
        assertThat(next.segments()).hasSize(2);
        assertThat(path(next.segments().get(0))).isEqualTo(path(f.part.segments().get(0)));
        assertThat(next.from()).isEqualTo(f.o);
        assertThat(next.entries()).isEqualTo(count(f.w, f.o, f.w.end()));
        assertThat(masks(rebuild(f.w, f.o, f.w.end(), next)))
                .isEqualTo(masks(fullReplay(f.w, f.o)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testNewLeaderThatCommitsBeforeItsInstallSavesTheChangeListWhole(boolean known)
            throws Exception {
        Fixture f = fixture();
        Leader b = leader("early", f.w, f.o, f.w.end());
        b.src.restored = restored(f.part, f.x, view(f.w.kvAt.get(f.x)), false);
        // the new leader commits a KV snapshot before its change list is installed, or may have
        // (its outcome is not known): the restored snapshot may be subsumed by it, whatever
        // number the coordinator retains
        DvChangeListCheckpoints.Checkpoint early = b.manager.prepare(10, f.w.end(), location());
        assertThat(early.attach()).isNull();
        if (known) {
            early.committed(() -> true);
        } else {
            early.uncertain();
        }
        b.manager.recover(b.src);

        DvChangeListHandle next = export(b, 11, f.w.end());

        assertThat(b.manager.lastRebuildCheckpointFields()).contains("ckpt=used");
        assertThat(next.segments()).hasSize(1);
        assertThat(path(next.segments().get(0))).isNotEqualTo(path(f.part.segments().get(0)));
        assertThat(next.from()).isEqualTo(f.o);
        assertThat(next.entries()).isEqualTo(count(f.w, f.o, f.w.end()));
    }

    @Test
    void testCloseReleasesAPreparedChangeList() throws Exception {
        Fixture f = fixture();
        DvChangeListCheckpoints.Checkpoint prepared =
                f.live.manager.prepare(9, f.w.end(), location());

        f.live.manager.close();

        assertThat(f.live.manager.dbClosed()).succeedsWithin(java.time.Duration.ofSeconds(30));
        assertThat(prepared.attach()).isNull();
    }
}
