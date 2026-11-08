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
import java.util.List;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * The LakeCommitter interface for committing write results. It extends the AutoCloseable interface
 * to ensure resources are released after use.
 *
 * @param <WriteResult> the type of the write result
 * @param <CommittableT> the type of the committable object
 * @since 0.7
 */
@PublicEvolving
public interface LakeCommitter<WriteResult, CommittableT> extends AutoCloseable {

    /**
     * The property key used to store the file path of lake table bucket offsets in snapshot
     * properties.
     */
    String FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY = "fluss-offsets";

    /**
     * The snapshot property key that stores the base of a tiering round of a table with deletion
     * vectors: the id of the latest lake snapshot registered in Fluss when the round started, or -1
     * if there was none. The round's commit descends from it.
     */
    String FLUSS_LAKE_SNAP_BASE_PROPERTY = "fluss-dv-base";

    /**
     * Converts a list of write results to a committable object.
     *
     * @param writeResults the list of write results
     * @return the committable object
     * @throws IOException if an I/O error occurs
     */
    CommittableT toCommittable(List<WriteResult> writeResults) throws IOException;

    /**
     * Commits the given committable object.
     *
     * @param committable the committable object
     * @param context the snapshot properties and tiered log end offsets for this commit
     * @return the commit result, which always includes the latest committed snapshot ID and may
     *     optionally contain distinct readable snapshot information if the physical tiered offsets
     *     do not yet represent a consistent readable state (e.g., in Paimon DV tables where the
     *     tiered log records may still in level0 which is not readable).
     * @throws IOException if an I/O error occurs
     */
    LakeCommitResult commit(CommittableT committable, CommitContext context) throws IOException;

    /** Context for committing a lake snapshot. */
    final class CommitContext {
        private final Map<String, String> snapshotProperties;
        private final Map<TableBucket, Long> tieredLogEndOffsets;

        /**
         * Creates a commit context.
         *
         * @param snapshotProperties the properties stored in the lake snapshot
         * @param tieredLogEndOffsets the previous lake snapshot offsets merged with this commit's
         *     log end offsets
         */
        public CommitContext(
                Map<String, String> snapshotProperties,
                Map<TableBucket, Long> tieredLogEndOffsets) {
            this.snapshotProperties = checkNotNull(snapshotProperties);
            this.tieredLogEndOffsets = checkNotNull(tieredLogEndOffsets);
        }

        /** Returns the properties stored in the lake snapshot. */
        public Map<String, String> snapshotProperties() {
            return snapshotProperties;
        }

        /** Returns the tiered log end offsets. */
        public Map<TableBucket, Long> tieredLogEndOffsets() {
            return tieredLogEndOffsets;
        }
    }

    /**
     * Aborts the given committable object.
     *
     * @param committable the committable object
     * @throws IOException if an I/O error occurs
     */
    void abort(CommittableT committable) throws IOException;

    /**
     * Get missing lake snapshot that has been committed to lake but didn't commit to fluss.
     *
     * @param latestLakeSnapshotIdOfFluss the latest lake snapshot id in fluss, used to judge which
     *     lake snapshot is missing.
     * @return the missing lake snapshot, returns null if no any missing snapshot found
     * @throws IOException if an I/O error occurs
     */
    @Nullable
    CommittedLakeSnapshot getMissingLakeSnapshot(@Nullable Long latestLakeSnapshotIdOfFluss)
            throws IOException;

    /**
     * Returns the result {@link #commit} reported for lake snapshot {@code snapshotId}, which Fluss
     * committed with the tiered offsets {@code tieredLogEndOffsets}. With it, a commit whose round
     * was lost before registering it, e.g. on failover, is registered as the round would have, with
     * its readable snapshot. Returns null if the lake cannot tell; the snapshot is then registered
     * without a readable snapshot.
     */
    @Nullable
    default LakeCommitResult getCommitResult(
            long snapshotId, Map<TableBucket, Long> tieredLogEndOffsets) throws IOException {
        return null;
    }

    /**
     * Commits {@code committable} together with deletion vectors for the given row positions per
     * data-file basename, atomically on top of {@code baseSnapshotId}. The committed snapshot's
     * parent is the base, or a snapshot after it that changed no data file. Fails if a referenced
     * file is gone or another commit happened since the base, because the deletes and the round's
     * index change are relative to the base.
     *
     * <p>If the lake definitely did not take the commit, the delete files written for it are
     * deleted. It then throws a {@link LakeCommitRejectedException}, so the caller can delete the
     * committable's files too, or a {@link LakeBaseChangedException} if the table moved past the
     * base, so the caller can plan the commit again on the new snapshot. Any other exception leaves
     * the outcome unknown.
     */
    default LakeCommitResult commitWithDeletes(
            CommittableT committable,
            Map<String, String> snapshotProperties,
            Map<String, long[]> deletedPositionsByFile,
            long baseSnapshotId)
            throws IOException {
        throw new UnsupportedOperationException("commitWithDeletes is not supported");
    }

    /**
     * Commits this round's compaction rewrite, if any, before the round's deletes, so that it
     * validates against the files it read. It commits only on the snapshot the compaction read, or
     * on snapshots after it that changed no data file, so the positions the compaction handed to
     * the index are a change from that snapshot. Returns the rewrite's snapshot id, or null if
     * there is none or it was rejected because the table changed. A rejected rewrite is safe to
     * skip.
     */
    @Nullable
    default Long commitPendingRewrite(CommittableT committable) throws IOException {
        return null;
    }

    /**
     * Checks that the lake table's current snapshot is {@code snapshotId} or descends from it.
     *
     * <p>Throws {@link org.apache.fluss.exception.LakeTableRolledBackException} if the table was
     * rolled back past it outside Fluss. The check passes if the lake cannot tell or the snapshot
     * is gone. Throws {@link org.apache.fluss.exception.LakeTableExternallyModifiedException} if
     * another engine changed the table's rows after {@code snapshotId}; rewrites that keep the rows
     * pass.
     */
    default void checkCurrentSnapshotDescendsFrom(long snapshotId) throws IOException {}

    /**
     * Returns whether the readable snapshot may have moved past {@code snapshotId}, the latest lake
     * snapshot registered in Fluss, without new data. This applies to a lake whose commits are not
     * readable at once (see {@link LakeCommitResult#getReadableSnapshot()}), e.g. when another
     * engine compacted a Paimon table. A tiering round without data then commits the registered
     * offsets again, so the newer readable snapshot is registered without waiting for a write.
     */
    default boolean readableSnapshotMayAdvanceSince(long snapshotId) throws IOException {
        return false;
    }

    /**
     * Keeps snapshot {@code snapshotId} from expiring while it is among the most recent readable
     * snapshots, as the lake does for the readable snapshots it commits. Called before Fluss
     * registers as readable a snapshot it did not commit, e.g. a rewrite by another engine. Best
     * effort; a lake without such protection does nothing.
     */
    default void protectReadableSnapshot(long snapshotId) {}

    /**
     * Expires old snapshots of the lake table if its snapshots expire automatically ({@code
     * table.datalake.auto-expire-snapshot} or {@code lake.tiering.auto-expire-snapshot}). Keeps
     * {@code keepFromSnapshotId} and the snapshots after it, which Fluss still needs. If null,
     * keeps only the snapshots the lake protects. Called after a round's commit of a table with
     * deletion vectors. A lake that expires snapshots on commit, or not at all, does nothing. Best
     * effort.
     */
    default void expireSnapshots(@Nullable Long keepFromSnapshotId) {}
}
