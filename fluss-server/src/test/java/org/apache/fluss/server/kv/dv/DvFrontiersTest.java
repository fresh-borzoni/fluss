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

import org.apache.fluss.server.kv.dv.DvFrontiers.KeepReady;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests of the frontiers a bucket keeps for union reads ({@link DvFrontiers}). */
class DvFrontiersTest {

    /**
     * A switch to the snapshot registered next keeps the latest frontier of the snapshot it leaves,
     * for reads planned on it. If the bucket switches on before the snapshot in between built a
     * frontier of its own, the kept one would be two snapshots back, which the lake may no longer
     * keep: it is dropped.
     */
    @Test
    void testTheKeptFrontierIsNeverTwoSnapshotsBack() {
        DvFrontiers frontiers = new DvFrontiers();
        long now = System.nanoTime();
        frontiers.readCame(now);
        frontiers.built(frontier(99, 12, now));
        // 99 -> 100: 99's frontier is kept
        frontiers.newState(KeepReady.NEXT_SNAPSHOT, 99, now);
        assertThat(frontiers.kept().snapshotId).isEqualTo(99L);
        assertThat(frontiers.keptStartOffset()).isEqualTo(10L);

        // 100 -> 101 before 100 had a frontier of its own
        frontiers.newState(KeepReady.NEXT_SNAPSHOT, 100, now);
        assertThat(frontiers.kept()).isNull();
        assertThat(frontiers.keptStartOffset()).isEqualTo(Long.MAX_VALUE);
    }

    /** A kept frontier of the covered snapshot itself stays across a replay of its change list. */
    @Test
    void testTheKeptFrontierOfTheCoveredSnapshotStays() {
        DvFrontiers frontiers = new DvFrontiers();
        long now = System.nanoTime();
        frontiers.readCame(now);
        frontiers.built(frontier(100, 14, now));
        frontiers.newState(KeepReady.SAME_SNAPSHOT, 100, now);
        assertThat(frontiers.kept().snapshotId).isEqualTo(100L);
        frontiers.newState(KeepReady.SAME_SNAPSHOT, 100, now);
        assertThat(frontiers.kept().snapshotId).isEqualTo(100L);
        assertThat(frontiers.dropKept()).isTrue();
        assertThat(frontiers.keptStartOffset()).isEqualTo(Long.MAX_VALUE);
    }

    private static DvFrontier frontier(long snapshotId, long cut, long builtAtNanos) {
        DvSnapshot masks = new DvSnapshot(Collections.emptyMap(), null, cut, 10, null, cut);
        return new DvFrontier(snapshotId, masks, builtAtNanos);
    }
}
