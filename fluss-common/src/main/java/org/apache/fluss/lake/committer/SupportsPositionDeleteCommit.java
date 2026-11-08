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
import java.util.Map;

/**
 * A {@link LakeCommitter} of a lake that takes deletes by row position from Fluss: tiering a table
 * with deletion vectors resolves the rows a round supersedes, and the committer commits their
 * deletes atomically with the round's data, so every commit is readable at its tiered offsets. Used
 * when {@link org.apache.fluss.lake.writer.SupportsDeletionVectorTiering#mergesByPrimaryKey()} is
 * false.
 *
 * <p>It extends {@link LakeCommitter} with the same type parameters, so a committer cannot take
 * deletes for a committable of another type than the one it commits.
 *
 * @param <WriteResult> the write result type of the lake committer
 * @param <CommittableT> the committable type of the lake committer
 * @since 1.1
 */
@PublicEvolving
public interface SupportsPositionDeleteCommit<WriteResult, CommittableT>
        extends LakeCommitter<WriteResult, CommittableT>, SupportsDeletionVectorCommit {

    /**
     * Commits {@code committable} together with deletion vectors for the given row positions per
     * data-file basename, atomically on top of {@code baseSnapshotId}. The committed snapshot's
     * parent is the base, or a snapshot after it that changed no data file. Fails if a referenced
     * file is gone or another commit happened since the base, because the deletes and the round's
     * index change are relative to the base.
     *
     * <p>Exceptions say whether the lake took the commit:
     *
     * <ul>
     *   <li>{@link LakeCommitRejectedException}: the lake definitely did not take the commit. The
     *       delete files written for it are already deleted, so the caller can delete the
     *       committable's files too.
     *   <li>{@link LakeBaseChangedException}: the table moved past the base, so the commit was not
     *       taken; the caller plans it again on the new snapshot.
     *   <li>Any other exception: the outcome is unknown, as the commit may still land after the
     *       call failed. Every file is kept, the delete files written for the commit included:
     *       Fluss never deletes the files of a commit whose outcome is unknown, and leaves them to
     *       the lake's orphan-file removal if it never lands.
     * </ul>
     *
     * <p>Not idempotent. Fluss never retries a commit whose outcome is unknown; a later round finds
     * a commit that landed with {@link LakeCommitter#getMissingLakeSnapshot} and registers it.
     *
     * @return the result of the commit
     */
    LakeCommitResult commitWithDeletes(
            CommittableT committable,
            Map<String, String> snapshotProperties,
            Map<String, long[]> deletedPositionsByFile,
            long baseSnapshotId)
            throws IOException;
}
