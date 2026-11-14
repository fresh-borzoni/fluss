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

import org.apache.fluss.server.kv.dv.DvChangeListWorkload.Entry;
import org.apache.fluss.server.kv.dv.DvChangeListWorkload.Model;
import org.apache.fluss.server.kv.snapshot.DvChangeListHandle;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The change list a new leader builds from a checkpoint at a KV snapshot's offset X and a replay
 * from X, naming a key's first superseded version after X from the KV at X, is the one a full
 * replay from the readable offset builds, and the one the write path hands over: the masks are the
 * same (Appendix A, item 3a). Checked on the reference model of {@link DvChangeListWorkload}, and
 * the change lists the owner installs, from the checkpoint and from a full replay, against the
 * model.
 */
class DvChangeListCheckpointEquivalenceTest extends DvCheckpointTestBase {

    @Test
    void testInsertBeforeXUpdatedAfterX() {
        // o = 0; k1 is inserted after o and before X, and updated after X: the update supersedes
        // the insert, a log version, which a change list at X alone does not know
        DvChangeListWorkload w = new DvChangeListWorkload();
        w.upsert("k0").endBatch(); // 0
        long o = w.end();
        w.upsert("k1").endBatch(); // 1
        long x = w.end();
        w.upsert("k1").endBatch(); // 2, 3
        List<Entry> checkpoint = Model.writePath(w, o);

        List<Entry> rebuilt = Model.fromCheckpoint(w, checkpoint, x, o);

        assertThat(rebuilt).containsExactly(new Entry(3, "k1", 1));
        assertThat(rebuilt).isEqualTo(Model.fullReplay(w, o));
        assertThat(rebuilt).isEqualTo(Model.writePath(w, o));
    }

    @Test
    void testDeleteBeforeXReinsertAfterX() {
        DvChangeListWorkload w = new DvChangeListWorkload();
        w.upsert("k1").endBatch(); // 0, tiered
        long o = w.end();
        w.delete("k1").endBatch(); // 1 supersedes the lake row
        long x = w.end();
        w.upsert("k1").endBatch(); // 2, an insert: supersedes nothing
        w.upsert("k1").endBatch(); // 3, 4 supersedes 2
        List<Entry> checkpoint = Model.writePath(w, o);

        List<Entry> rebuilt = Model.fromCheckpoint(w, checkpoint, x, o);

        assertThat(rebuilt).containsExactly(new Entry(1, "k1", -1), new Entry(4, "k1", 2));
        assertThat(rebuilt).isEqualTo(Model.fullReplay(w, o));
    }

    @Test
    void testLakeRowUpdatedOnlyAfterX() {
        DvChangeListWorkload w = new DvChangeListWorkload();
        w.upsert("k1").upsert("k2").endBatch(); // 0, 1, tiered
        long o = w.end();
        w.upsert("k2").endBatch(); // 2, 3
        long x = w.end();
        w.upsert("k1").endBatch(); // 4, 5: k1's version at X is 0, before o, so its lake row

        assertThat(Model.fromCheckpoint(w, Model.writePath(w, o), x, o))
                .containsExactly(new Entry(3, "k2", -1), new Entry(5, "k1", -1))
                .isEqualTo(Model.fullReplay(w, o));
    }

    @Test
    void testRandomWorkloads() {
        Random random = new Random(20261114L);
        int checked = 0;
        for (int run = 0; run < 300; run++) {
            DvChangeListWorkload w =
                    DvChangeListWorkload.random(
                            random, 1 + random.nextInt(40), 1 + random.nextInt(60), 8);
            List<Long> boundaries = new ArrayList<>(w.kvAt.keySet());
            for (int pick = 0; pick < 20; pick++) {
                // the old leader covered exported <= o; the KV snapshot is at x >= o
                long[] three = sortedPicks(random, boundaries);
                long exported = three[0];
                long o = three[1];
                long x = three[2];
                List<Entry> expected = Model.fullReplay(w, o);
                assertThat(Model.writePath(w, o)).isEqualTo(expected);
                // the old leader's change list came from its write path ...
                assertThat(Model.fromCheckpoint(w, Model.writePath(w, exported), x, o))
                        .as("run %d: exported %d, o %d, x %d", run, exported, o, x)
                        .isEqualTo(expected);
                // ... or from a replay from its readable offset, naming earlier versions -1
                assertThat(Model.fromCheckpoint(w, replayed(w, exported), x, o))
                        .as("run %d: replayed from %d, o %d, x %d", run, exported, o, x)
                        .isEqualTo(expected);
                checked++;
            }
        }
        assertThat(checked).isEqualTo(6000);
    }

    @Test
    void testOwnersChangeListsAreTheModels() throws Exception {
        Random random = new Random(20261115L);
        int fromCheckpoint = 0;
        for (int run = 0; run < 30; run++) {
            DvChangeListWorkload w =
                    DvChangeListWorkload.random(
                            random, 1 + random.nextInt(30), 2 + random.nextInt(40), 6);
            List<Long> boundaries = new ArrayList<>(w.kvAt.keySet());
            long[] three = sortedPicks(random, boundaries);
            long exported = three[0];
            long o = three[1];
            long x = three[2];
            // every other run, the replay's map of versions spills to disk
            boolean spill = run % 2 == 1;
            String at = String.format("run %d: exported %d, o %d, x %d", run, exported, o, x);

            Leader live = liveLeader(w, exported);
            DvChangeListHandle part = export(live, 1, x);
            Leader rebuilt = leader("rebuilt", w, o, w.end());
            rebuilt.manager.setSpillReplayVersions(spill);
            rebuilt.src.restored = restored(part, x, view(w.kvAt.get(x)), false);
            rebuilt.manager.recover(rebuilt.src);
            Leader full = leader("full", w, o, w.end());
            full.manager.setSpillReplayVersions(spill);
            full.manager.recover(full.src);

            List<Entry> expected = Model.fullReplay(w, o);
            assertThat(changeList(live, o)).as(at).isEqualTo(Model.writePath(w, o));
            assertThat(changeList(full, o)).as(at).isEqualTo(expected);
            assertThat(rebuilt.manager.lastRebuildCheckpointFields())
                    .as(at)
                    .contains("ckpt=used", "replay_from=" + x);
            assertThat(changeList(rebuilt, o))
                    .as(at)
                    .isEqualTo(Model.fromCheckpoint(w, Model.writePath(w, exported), x, o))
                    .isEqualTo(expected);
            fromCheckpoint++;
            closeAll();
        }
        assertThat(fromCheckpoint).isEqualTo(30);
    }

    @Test
    void testCheckpointBelowItsValidFromIsWrong() {
        // why a load needs o >= valid-from: a replay from 2 names k1's version 1 only as -1
        DvChangeListWorkload w = new DvChangeListWorkload();
        w.upsert("k0").endBatch(); // 0
        long o = w.end();
        w.upsert("k1").endBatch(); // 1
        long validFrom = w.end();
        w.upsert("k1").endBatch(); // 2, 3 supersedes 1
        long x = w.end();
        w.upsert("k9").endBatch();

        assertThat(Model.fromCheckpoint(w, replayed(w, validFrom), x, o))
                .isNotEqualTo(Model.fullReplay(w, o));
    }

    private static List<Entry> replayed(DvChangeListWorkload w, long from) {
        return Model.replay(w, from, w.end(), Collections.emptyMap());
    }

    private static long[] sortedPicks(Random random, List<Long> boundaries) {
        long[] picks = new long[3];
        for (int i = 0; i < 3; i++) {
            picks[i] = boundaries.get(random.nextInt(boundaries.size()));
        }
        java.util.Arrays.sort(picks);
        return picks;
    }
}
