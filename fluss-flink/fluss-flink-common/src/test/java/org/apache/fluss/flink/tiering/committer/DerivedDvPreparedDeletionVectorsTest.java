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

package org.apache.fluss.flink.tiering.committer;

import org.apache.fluss.client.tiering.TableBucketWriteResult;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.committer.SupportsPositionDeleteCommit;
import org.apache.fluss.lake.dv.PreparedDeletionVectors;
import org.apache.fluss.lake.dv.PreparedDeletionVectorsProvider;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests how a round's committer treats the deletion vectors its writers prepared. */
class DerivedDvPreparedDeletionVectorsTest {

    private static final TablePath TABLE = TablePath.of("db", "t");

    /**
     * One deletion vector per data file and commit: buckets whose prepared deletion vectors name
     * the same data file, or a data file of the deletes merged at the committer, clash, and their
     * deletes are merged at the committer instead.
     */
    @Test
    void testPreparedDeletionVectorsOfOneDataFileClash() {
        List<Object> discarded = new ArrayList<>();
        LakeCommitter<Prepared, Object> committer = committer(discarded);
        TableBucket b0 = new TableBucket(1L, 0);
        TableBucket b1 = new TableBucket(1L, 1);
        TableBucket b2 = new TableBucket(1L, 2);
        Map<TableBucket, Prepared> prepared = new HashMap<>();
        prepared.put(b0, new Prepared("a", "b"));
        prepared.put(b1, new Prepared("b", "c"));
        prepared.put(b2, new Prepared("d"));

        assertThat(DerivedDvRound.clashing(committer, prepared, Collections.emptySet()))
                .containsExactlyInAnyOrder(b0, b1);
        assertThat(DerivedDvRound.clashing(committer, prepared, Collections.singleton("d")))
                .containsExactlyInAnyOrder(b0, b1, b2);
        prepared.remove(b1);
        assertThat(DerivedDvRound.clashing(committer, prepared, Collections.singleton("e")))
                .isEmpty();
    }

    /**
     * A round that is not committed deletes the deletion vectors its writers prepared, and nothing
     * of the write results without them.
     */
    @Test
    void testDiscardedRoundDeletesThePreparedDeletionVectors() {
        List<Object> discarded = new ArrayList<>();
        LakeCommitter<Prepared, Object> committer = committer(discarded);
        Prepared prepared = new Prepared("a");
        Prepared unprepared = new Prepared();
        List<TableBucketWriteResult<Prepared>> results =
                Arrays.asList(result(0, prepared), result(1, unprepared), result(2, null));

        DerivedDvRound.discardPreparedDeletionVectors(committer, results);

        assertThat(discarded).containsExactly(prepared);
    }

    private static TableBucketWriteResult<Prepared> result(int bucket, @Nullable Prepared result) {
        return new TableBucketWriteResult<>(
                TABLE, new TableBucket(1L, bucket), null, result, 10L, -1L, 3);
    }

    private static LakeCommitter<Prepared, Object> committer(List<Object> discarded) {
        return new TestingPreparedCommitter(discarded);
    }

    /**
     * A lake committer of deletes by position whose prepared deletion vectors name the data files
     * of {@link Prepared}, and that records the write results whose deletion vectors it deletes. It
     * commits nothing.
     */
    private static final class TestingPreparedCommitter
            implements SupportsPositionDeleteCommit<Prepared, Object> {
        private final List<Object> discarded;

        private TestingPreparedCommitter(List<Object> discarded) {
            this.discarded = discarded;
        }

        @Override
        public Set<String> preparedDataFiles(Prepared prepared) {
            return prepared.files;
        }

        @Override
        public void discardDeletionVectors(Collection<Prepared> prepared) {
            discarded.addAll(prepared);
        }

        @Override
        public LakeCommitResult commitWithDeletes(
                Object committable,
                Map<String, String> snapshotProperties,
                Map<String, long[]> deletedPositionsByFile,
                long baseSnapshotId,
                Collection<Prepared> prepared) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Long commitPendingRewrite(Object committable) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void expireSnapshots(@Nullable Long keepFromSnapshotId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCurrentSnapshotDescendsFrom(long snapshotId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void protectReadableSnapshot(long snapshotId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object toCommittable(List<Prepared> writeResults) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LakeCommitResult commit(Object committable, CommitContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void abort(Object committable) {
            throw new UnsupportedOperationException();
        }

        @Nullable
        @Override
        public CommittedLakeSnapshot getMissingLakeSnapshot(
                @Nullable Long latestLakeSnapshotIdOfFluss) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }

    /** A write result with deletion vectors of the given data files, or none if there are none. */
    private static final class Prepared implements PreparedDeletionVectorsProvider {
        final Set<String> files;

        Prepared(String... files) {
            this.files = new HashSet<>(Arrays.asList(files));
        }

        @Nullable
        @Override
        public PreparedDeletionVectors preparedDeletionVectors() {
            return files.isEmpty() ? null : new PreparedDeletionVectors(7L, 0L, 10L);
        }
    }
}
