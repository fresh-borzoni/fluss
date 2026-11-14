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
import org.apache.fluss.server.kv.snapshot.PlaceholderKvFileHandler;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Saving the change list with a KV snapshot when the change list goes in the owner's step, the
 * bucket closes under the job, the job is late for its snapshot, or the coordinator drops the part.
 */
class DvChangeListExportTest extends DvCheckpointTestBase {

    // ---- the change list the step saves -------------------------------------------------------

    @Test
    void testChangeListLostInThePrepareStepsDrainIsLeftOut() throws Exception {
        DvChangeListWorkload w = DvChangeListWorkload.random(new Random(5), 12, 30, 5);
        List<Long> b = new ArrayList<>(w.kvAt.keySet());
        long o = b.get(b.size() / 3);
        long x = b.get(2 * b.size() / 3);
        Leader live = liveLeader(w, o);
        live.manager.inspect();
        TestingDvHooks hooks = DvTestManagers.hooks(live.manager);
        AtomicInteger fired = new AtomicInteger();
        Runnable[] fault = new Runnable[1];
        fault[0] =
                () -> {
                    if (inPrepareStep()) {
                        // the write path fails to hand a batch over just after the KV snapshot
                        // posted its step: the step's own drain loses the change list
                        fired.incrementAndGet();
                        live.manager.handOffFailed(new IOException("the hand-off failed"));
                    } else {
                        hooks.setFaultInNextDrain(fault[0]);
                    }
                };
        hooks.setFaultInNextDrain(fault[0]);

        DvChangeListHandle part = export(live, 1, x);

        assertThat(fired.get()).isEqualTo(1);
        assertThat(count(w, o, x)).isPositive();
        // no part rather than an empty one, which a new leader would use
        assertThat(part).isNull();
    }

    @Test
    void testDiskFailureInThePrepareStepsApplyIsLeftOut() throws Exception {
        DvChangeListWorkload w = DvChangeListWorkload.random(new Random(5), 12, 30, 5);
        List<Long> b = new ArrayList<>(w.kvAt.keySet());
        long o = b.get(b.size() / 3);
        long x = b.get(2 * b.size() / 3);
        Leader live = leader("live", w, o, o);
        live.manager.recover(live.src);
        // handed over, but the high watermark stays at o: nothing after it is applied yet
        handOverFast(live, w, o);
        live.manager.inspect();
        TestingDvHooks hooks = DvTestManagers.hooks(live.manager);
        hooks.failChangeListWrites(new IOException("the DV disk is full"));
        live.hw.set(w.end());

        // the step applies the changes below X, which fails and drops the change list
        DvChangeListHandle part = export(live, 1, x);
        hooks.failChangeListWrites(null);

        assertThat(count(w, o, x)).isPositive();
        assertThat(part).isNull();
    }

    // ---- the job ------------------------------------------------------------------------------

    @Test
    void testCloseWaitsForTheJobInsideItsCursor() throws Exception {
        // enough changes that the segment's stream writes while the cursor iterates
        DvChangeListWorkload w = DvChangeListWorkload.random(new Random(9), 400, 3000, 8);
        Leader live = leader("live", w, 0, 0);
        live.manager.recover(live.src);
        handOverFast(live, w, 0);
        live.hw.set(w.end());
        live.manager.inspect();
        assertThat(count(w, 0, w.end())).isGreaterThan(9000);
        BlockingFs fs = new BlockingFs();
        DvChangeListCheckpoints.Checkpoint checkpoint =
                live.manager.prepare(1, w.end(), location(fs));
        assertThat(fs.entered.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(Arrays.toString(fs.stack)).contains("PendingCursor.next");

        // the bucket closes (a leader change) while the job iterates the change list
        DvTestManagers.setCloseWaitMillis(live.manager, 200);
        live.manager.close();

        // the job holds a lease of the DV RocksDB and its snapshot: the RocksDB stays open
        assertThat(live.manager.dbClosed()).isNotDone();
        assertThat(live.manager.leasesHeld()).isPositive();
        fs.release.countDown();
        // the job stops at its next chunk and releases both, and the last release closes it
        assertThat(live.manager.dbClosed()).succeedsWithin(Duration.ofSeconds(30));
        assertThat(live.manager.leasesHeld()).isZero();
        assertThat(checkpoint.attach()).isNull();
        checkpoint.aborted();
    }

    @Test
    void testJobWaitingForAThreadHoldsNoLease() throws Exception {
        DvChangeListWorkload w = DvChangeListWorkload.random(new Random(17), 10, 30, 5);
        List<Long> b = new ArrayList<>(w.kvAt.keySet());
        long o = b.get(b.size() / 3);
        long x = b.get(2 * b.size() / 3);
        Leader live = liveLeader(w, o);
        live.manager.inspect();
        int held = live.manager.leasesHeld();
        DvChangeListCheckpoints.Checkpoint queued;
        CountDownLatch busy = new CountDownLatch(1);
        try {
            occupyTheChangeListLane(busy);
            queued = live.manager.prepare(1, x, location());
            ((DvOwnerChangeListExport.Export) queued).plan.get(30, TimeUnit.SECONDS);

            // planned, and its job waits for a thread: it holds no lease and no snapshot
            assertThat(live.manager.leasesHeld()).isEqualTo(held);
            // so the bucket's close closes the RocksDB at once
            live.manager.close();
            assertThat(live.manager.dbClosed()).succeedsWithin(Duration.ofSeconds(30));
        } finally {
            busy.countDown();
        }
        // the job starts after the close: it takes nothing and saves nothing
        ((DvOwnerChangeListExport.Export) queued).written.get(30, TimeUnit.SECONDS);
        assertThat(live.manager.leasesHeld()).isZero();
        assertThat(queued.attach()).isNull();
        queued.aborted();
    }

    @Test
    void testChangeListLostBeforeTheJobStartsIsLeftOut() throws Exception {
        DvChangeListWorkload w = DvChangeListWorkload.random(new Random(18), 10, 30, 5);
        List<Long> b = new ArrayList<>(w.kvAt.keySet());
        long o = b.get(b.size() / 3);
        long x = b.get(2 * b.size() / 3);
        Leader live = liveLeader(w, o);
        live.manager.inspect();
        int held = live.manager.leasesHeld();
        DvChangeListCheckpoints.Checkpoint queued;
        CountDownLatch busy = new CountDownLatch(1);
        try {
            occupyTheChangeListLane(busy);
            queued = live.manager.prepare(1, x, location());
            ((DvOwnerChangeListExport.Export) queued).plan.get(30, TimeUnit.SECONDS);
            // the change list the plan saves is lost while its job waits for a thread
            live.manager.handOffFailed(new IOException("the hand-off failed"));
            live.manager.inspect();
        } finally {
            busy.countDown();
        }

        // the job reads only a change list that still holds every change the plan saves
        ((DvOwnerChangeListExport.Export) queued).written.get(30, TimeUnit.SECONDS);
        assertThat(queued.attach()).isNull();
        queued.aborted();
        assertThat(live.manager.leasesHeld()).isEqualTo(held);
    }

    @Test
    void testLateChangeListIsCarriedByTheNextSnapshot() throws Exception {
        DvChangeListWorkload w = DvChangeListWorkload.random(new Random(14), 10, 30, 5);
        List<Long> b = new ArrayList<>(w.kvAt.keySet());
        long o = b.get(b.size() / 5);
        long x1 = b.get(2 * b.size() / 5);
        long x2 = b.get(3 * b.size() / 5);
        long x3 = b.get(4 * b.size() / 5);
        Leader live = liveLeader(w, o);
        DvChangeListHandle p1 = export(live, 1, x1);

        // snapshot 2's KV files are uploaded while its segment is still being written
        BlockingFs fs = new BlockingFs();
        live.manager.setChangeListAttachWaitMillis(100);
        DvChangeListCheckpoints.Checkpoint late = live.manager.prepare(2, x2, location(fs));
        assertThat(fs.entered.await(30, TimeUnit.SECONDS)).isTrue();
        DvChangeListHandle carried = late.attach();
        late.committed(() -> true);
        ((DvOwnerChangeListExport.Export) late).readBackPosted.get(30, TimeUnit.SECONDS);

        // the snapshot commits without waiting: its part only keeps snapshot 1's segment
        assertThat(carried.segments()).hasSize(1);
        assertThat(path(carried.segments().get(0))).isEqualTo(path(p1.segments().get(0)));
        assertThat(carried.from()).isEqualTo(o);
        assertThat(carried.end()).isEqualTo(x1);
        // which a new leader that restores snapshot 2 does not use
        Leader restoredLate = rebuild(w, o, x2, carried);
        assertThat(restoredLate.manager.lastRebuildCheckpointFields())
                .contains("ckpt=end_mismatch", "replay_from=" + o);
        assertThat(masks(restoredLate)).isEqualTo(masks(fullReplay(w, o)));

        // the segment is finished after its snapshot: the next snapshot carries it
        fs.release.countDown();
        ((DvOwnerChangeListExport.Export) late).written.get(30, TimeUnit.SECONDS);
        live.manager.setChangeListAttachWaitMillis(DvOwnerChangeListExport.ATTACH_WAIT_MS);
        DvChangeListHandle p3 = export(live, 3, x3);

        assertThat(p3.segments()).hasSize(3);
        assertThat(path(p3.segments().get(0))).isEqualTo(path(p1.segments().get(0)));
        assertThat(p3.segments().get(0).file().getKvFileHandle())
                .isInstanceOf(PlaceholderKvFileHandler.class);
        // no committed snapshot references the late segment: snapshot 3 owns it
        assertThat(p3.segments().get(1).from()).isEqualTo(x1);
        assertThat(p3.segments().get(1).end()).isEqualTo(x2);
        assertThat(p3.segments().get(1).file().getKvFileHandle())
                .isNotInstanceOf(PlaceholderKvFileHandler.class);
        assertThat(p3.segments().get(2).from()).isEqualTo(x2);
        assertThat(p3.entries()).isEqualTo(count(w, o, x3));
        assertThat(masks(rebuild(w, o, x3, p3))).isEqualTo(masks(fullReplay(w, o)));
    }

    @Test
    void testLateSegmentOfAFailedSnapshotIsDeleted() throws Exception {
        DvChangeListWorkload w = DvChangeListWorkload.random(new Random(15), 10, 30, 5);
        List<Long> b = new ArrayList<>(w.kvAt.keySet());
        long o = b.get(b.size() / 3);
        long x = b.get(2 * b.size() / 3);
        Leader live = liveLeader(w, o);
        List<String> before = sharedFiles();

        BlockingFs fs = new BlockingFs();
        live.manager.setChangeListAttachWaitMillis(100);
        DvChangeListCheckpoints.Checkpoint late = live.manager.prepare(1, x, location(fs));
        assertThat(fs.entered.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(late.attach()).isNull();
        late.aborted();
        fs.release.countDown();
        ((DvOwnerChangeListExport.Export) late).written.get(30, TimeUnit.SECONDS);
        live.manager.inspect();

        // no snapshot references the segment the job finished: it is deleted
        retry(Duration.ofMinutes(1), () -> assertThat(sharedFiles()).isEqualTo(before));
        live.manager.setChangeListAttachWaitMillis(DvOwnerChangeListExport.ATTACH_WAIT_MS);
        DvChangeListHandle next = export(live, 2, x);
        assertThat(next.segments()).hasSize(1);
        assertThat(next.entries()).isEqualTo(count(w, o, x));
    }

    // ---- the commit ---------------------------------------------------------------------------

    @Test
    void testSnapshotPlannedWhileACommitIsReadBackCarriesNothing() throws Exception {
        DvChangeListWorkload w = DvChangeListWorkload.random(new Random(19), 10, 36, 5);
        List<Long> b = new ArrayList<>(w.kvAt.keySet());
        long o = b.get(b.size() / 6);
        long x1 = b.get(2 * b.size() / 6);
        long x2 = b.get(3 * b.size() / 6);
        long x3 = b.get(4 * b.size() / 6);
        long x4 = b.get(5 * b.size() / 6);
        Leader live = liveLeader(w, o);
        DvChangeListHandle p1 = export(live, 1, x1);

        // snapshot 2 is committed, and its metadata is slow to read back
        CountDownLatch slow = new CountDownLatch(1);
        DvChangeListCheckpoints.Checkpoint second = live.manager.prepare(2, x2, location());
        DvChangeListHandle p2 = second.attach();
        assertThat(p2.segments()).hasSize(2);
        second.committed(
                () -> {
                    slow.await(60, TimeUnit.SECONDS);
                    return true;
                });
        live.manager.inspect();

        // snapshot 3 is planned meanwhile: which segments snapshot 2 holds is not known yet, and
        // its commit may have subsumed snapshot 1, so it carries nothing
        DvChangeListHandle p3 = export(live, 3, x3);
        assertThat(p3.segments()).hasSize(1);
        assertThat(p3.from()).isEqualTo(o);
        assertThat(path(p3.segments().get(0)))
                .isNotEqualTo(path(p1.segments().get(0)))
                .isNotEqualTo(path(p2.segments().get(1)));

        // snapshot 2's read-back ends after snapshot 3's commit: snapshot 3's part is carried
        slow.countDown();
        ((DvOwnerChangeListExport.Export) second).readBackPosted.get(30, TimeUnit.SECONDS);
        live.manager.inspect();
        DvChangeListHandle p4 = export(live, 4, x4);
        assertThat(p4.segments()).hasSize(2);
        assertThat(path(p4.segments().get(0))).isEqualTo(path(p3.segments().get(0)));
        assertThat(p4.entries()).isEqualTo(count(w, o, x4));
    }

    @Test
    void testCommitWithoutThePartDeletesItsNewSegments() throws Exception {
        DvChangeListWorkload w = DvChangeListWorkload.random(new Random(16), 10, 30, 5);
        List<Long> b = new ArrayList<>(w.kvAt.keySet());
        long o = b.get(b.size() / 5);
        long x1 = b.get(2 * b.size() / 5);
        long x2 = b.get(3 * b.size() / 5);
        long x3 = b.get(4 * b.size() / 5);
        Leader live = liveLeader(w, o);
        DvChangeListHandle p1 = export(live, 1, x1);

        DvChangeListCheckpoints.Checkpoint checkpoint = live.manager.prepare(2, x2, location());
        DvChangeListHandle p2 = checkpoint.attach();
        assertThat(p2.segments()).hasSize(2);
        // a coordinator that does not know the field committed the snapshot without the part
        checkpoint.committed(() -> false);
        ((DvOwnerChangeListExport.Export) checkpoint).readBackPosted.get(30, TimeUnit.SECONDS);
        live.manager.inspect();

        // the part's new segment goes; the one snapshot 1 references stays
        File added = localFile(p2.segments().get(1));
        retry(Duration.ofMinutes(1), () -> assertThat(added).doesNotExist());
        assertThat(localFile(p1.segments().get(0))).exists();
        // snapshot 1 may be subsumed by snapshot 2: the next part carries nothing of it
        DvChangeListHandle p3 = export(live, 3, x3);
        assertThat(p3.segments()).hasSize(1);
        assertThat(path(p3.segments().get(0))).isNotEqualTo(path(p1.segments().get(0)));
        assertThat(p3.from()).isEqualTo(o);
        assertThat(p3.entries()).isEqualTo(count(w, o, x3));
    }

    /** Keeps every thread of the change lists' lane busy until {@code busy} counts down. */
    private static void occupyTheChangeListLane(CountDownLatch busy) throws Exception {
        CountDownLatch running = new CountDownLatch(DerivedDvRuntime.CHANGE_LIST_THREADS);
        for (int i = 0; i < DerivedDvRuntime.CHANGE_LIST_THREADS; i++) {
            DvTestManagers.executors()
                    .changeLists()
                    .execute(
                            () -> {
                                running.countDown();
                                try {
                                    busy.await(60, TimeUnit.SECONDS);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            });
        }
        assertThat(running.await(30, TimeUnit.SECONDS)).isTrue();
    }

    private static boolean inPrepareStep() {
        for (StackTraceElement e : Thread.currentThread().getStackTrace()) {
            if (e.getMethodName().equals("prepareStep")) {
                return true;
            }
        }
        return false;
    }
}
