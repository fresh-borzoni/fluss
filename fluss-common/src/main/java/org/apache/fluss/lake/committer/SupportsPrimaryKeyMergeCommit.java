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
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;

/**
 * A {@link LakeCommitter} of a lake that merges rows by primary key itself, as a merge-tree table
 * does. Tiering a table with deletion vectors commits no deletes then: a round commits its data
 * with {@link LakeCommitter#commit}, and the {@link LakeCommitResult} reports which snapshot is
 * readable and from which log offset per bucket, which may lag the commit. Used when {@link
 * org.apache.fluss.lake.writer.SupportsDeletionVectorTiering#mergesByPrimaryKey()} is true.
 *
 * <p>For a table with deletion vectors, {@link LakeCommitter#commit} of such a committer throws
 * {@link LakeCommitRejectedException} only if the lake definitely did not take the commit; the
 * caller then deletes the commit's files ({@link LakeCommitter#abort}). Any other exception leaves
 * the outcome unknown: the commit may still land after the call failed, so the caller keeps every
 * file. A lake that cannot tell at once never throws {@link LakeCommitRejectedException}.
 *
 * <p>Such a committer may instead keep the files of a failed commit and delete them itself later,
 * e.g. at the next commit of the table, once it knows the commit never landed. That decision must
 * keep the files whenever anything is unsure. It may live in memory only: a failover of the tiering
 * job then loses it, and the files stay for the lake's orphan-file removal. Together with the files
 * the commit wrote, it may delete the Fluss offsets file named in the commit's own snapshot
 * properties ({@link LakeCommitter#FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY}), and only when that
 * commit provably never landed: a snapshot of a commit that landed references the file.
 *
 * @since 1.1
 */
@PublicEvolving
public interface SupportsPrimaryKeyMergeCommit extends SupportsDeletionVectorCommit {

    /**
     * Returns the result {@link LakeCommitter#commit} reported for lake snapshot {@code
     * snapshotId}, which Fluss committed with the tiered offsets {@code tieredLogEndOffsets}. With
     * it, a commit whose round was lost before registering it, e.g. on failover, is registered as
     * the round would have, with its readable snapshot. Returns null if the lake cannot tell; the
     * snapshot is then registered without a readable snapshot. Reads only.
     */
    @Nullable
    LakeCommitResult getCommitResult(long snapshotId, Map<TableBucket, Long> tieredLogEndOffsets)
            throws IOException;

    /**
     * Returns whether the readable snapshot may have moved past {@code snapshotId}, the latest lake
     * snapshot registered in Fluss, without new data, e.g. when another engine compacted the table.
     * A tiering round without data then commits the registered offsets again, so the newer readable
     * snapshot is registered without waiting for a write. Reads only; a false negative only delays
     * the registration until the next write.
     */
    boolean readableSnapshotMayAdvanceSince(long snapshotId) throws IOException;
}
