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

package org.apache.fluss.lake.source;

import org.apache.fluss.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * A {@link LakeSource} that can tell how the data files of a lake table changed between two
 * snapshots, and which commit a snapshot id names. Tables with deletion vectors need it: their
 * position index follows the lake's data files, and union reads apply masks to the snapshot they
 * were made for only. Fluss checks a lake source for this capability before it reads or tiers such
 * a table, and fails with a clear error otherwise.
 *
 * @param <Split> the split type of the lake source
 * @since 1.1
 */
@PublicEvolving
public interface SupportsRewritePlanning<Split extends LakeSplit> {

    /**
     * Plans the change of the position index of a table with deletion vectors from snapshot {@code
     * baseSnapshotId} (-1 for none) to snapshot {@code targetSnapshotId} (null for the current
     * one). It compares the readable data files of the two snapshots, so expired snapshots in
     * between cannot hide a rewrite.
     *
     * <p>Returns one split per data file whose rows the index must learn (read with {@link
     * SupportsRowPositions#readWithPos()}), and the data files that left the snapshot. Returns
     * every readable file of the target if the base is gone, or null if the base's index already
     * fits the target. If a snapshot between the two expired, it returns a plan also when the
     * base's index fits, without files then, so the index moves to the target.
     *
     * <p>Files whose positions the tiering writer staged in the index itself (see {@code
     * StagedIndexPartProvider}) may be left out. A lake that merges rows by primary key itself (see
     * {@code SupportsDeletionVectorTiering#mergesByPrimaryKey()}) reports no positions, so every
     * data file that became readable since the base is planned.
     *
     * <p>Reads only. Throws {@link org.apache.fluss.exception.LakeTableRolledBackException} if the
     * target does not descend from the base.
     */
    @Nullable
    DataDeltaPlan<Split> planRewrite(long baseSnapshotId, @Nullable Long targetSnapshotId);

    /**
     * Describes the commit snapshot {@code snapshotId} names now, so that a reused snapshot id,
     * e.g. of a commit after a rollback, is told apart from the snapshot it named before. Returns
     * "none" if the snapshot does not exist, and null for a lake that never reuses snapshot ids.
     * Fluss records it when it registers a readable snapshot of a table with deletion vectors.
     * Union reads check it before they apply the snapshot's masks. Reads only.
     */
    @Nullable
    String snapshotIdentity(long snapshotId) throws IOException;
}
