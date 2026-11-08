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

package org.apache.fluss.lake.committer;

import org.apache.fluss.annotation.PublicEvolving;

import java.io.IOException;

/**
 * A {@link LakeCommitter} that can commit the tiering rounds of tables with deletion vectors
 * ({@code table.deletion-vectors.enabled}). Fluss checks a committer of such a table for this
 * capability before a round commits, and fails the round with a clear error otherwise.
 *
 * <p>Fluss resolves the rows a round supersedes and commits their deletes with the data ({@link
 * SupportsPositionDeleteCommit}). This interface holds the checks that the lake table still
 * descends from what Fluss registered.
 *
 * @since 1.1
 */
@PublicEvolving
public interface SupportsDeletionVectorCommit {

    /**
     * The snapshot property key that stores the base of a tiering round of a table with deletion
     * vectors: the id of the latest lake snapshot registered in Fluss when the round started, or -1
     * if there was none. The round's commit descends from it.
     */
    String FLUSS_LAKE_SNAP_BASE_PROPERTY = "fluss-dv-base";

    /**
     * Checks that the lake table's current snapshot is {@code snapshotId} or descends from it.
     *
     * <p>Throws {@link org.apache.fluss.exception.LakeTableRolledBackException} if the table was
     * rolled back past it outside Fluss. The check passes if the lake cannot tell or the snapshot
     * is gone. Throws {@link org.apache.fluss.exception.LakeTableExternallyModifiedException} if
     * another engine changed the table's rows after {@code snapshotId}; rewrites that keep the rows
     * pass. Reads only; calling it again gives the same answer for the same lake state.
     *
     * @param snapshotId a lake snapshot registered in Fluss
     * @throws IOException if the lake metadata cannot be read
     */
    void checkCurrentSnapshotDescendsFrom(long snapshotId) throws IOException;

    /**
     * Keeps snapshot {@code snapshotId} from expiring while it is among the most recent readable
     * snapshots, as the lake does for the readable snapshots it commits. Called before Fluss
     * registers as readable a snapshot it did not commit, e.g. a rewrite by another engine.
     * Idempotent and best effort: a failure must not fail the round, and a lake without such
     * protection does nothing.
     *
     * @param snapshotId the lake snapshot Fluss is about to register as readable
     */
    void protectReadableSnapshot(long snapshotId);
}
