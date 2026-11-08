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

package org.apache.fluss.lake.writer;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.lake.committer.CommitterInitContext;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.committer.PartitionMarkDoneCommitter;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

/**
 * The LakeTieringFactory interface defines how to create lake writers and committers. It provides
 * methods to create writers and committers for Fluss's rows to Paimon/Iceberg rows, and to obtain
 * serializers for write results and committable objects.
 *
 * @param <WriteResult> the type of the write result
 * @param <CommittableT> the type of the committable object
 * @since 0.7
 */
@PublicEvolving
public interface LakeTieringFactory<WriteResult, CommittableT> extends Serializable {

    String FLUSS_LAKE_TIERING_COMMIT_USER = "__fluss_lake_tiering";

    /**
     * Creates a lake writer to write Fluss's rows to Paimon/Iceberg rows.
     *
     * @param writerInitContext the context for initializing the writer
     * @return the lake writer
     * @throws IOException if an I/O error occurs
     */
    LakeWriter<WriteResult> createLakeWriter(WriterInitContext writerInitContext)
            throws IOException;

    /**
     * Returns the serializer for write results.
     *
     * @return the serializer for write results
     */
    SimpleVersionedSerializer<WriteResult> getWriteResultSerializer();

    /**
     * Creates a lake committer to commit to Paimon/Iceberg.
     *
     * @return the lake committer
     * @throws IOException if an I/O error occurs
     */
    LakeCommitter<WriteResult, CommittableT> createLakeCommitter(
            CommitterInitContext committerInitContext) throws IOException;

    /**
     * Returns whether this factory creates {@link PartitionMarkDoneCommitter} instances.
     *
     * <p>This check must not access lake resources. The committer checks whether the actual table
     * enables mark-done.
     */
    default boolean supportsPartitionMarkDone() {
        return false;
    }

    /**
     * Returns whether the lake table merges rows by primary key itself, as a merge-tree table does.
     *
     * <p>Tiering a table with deletion vectors then commits no deletes: the lake supersedes old
     * rows on its own. The committer reports in its {@link
     * org.apache.fluss.lake.committer.LakeCommitResult} which snapshot is readable and from which
     * log offset per bucket; this may lag the commit. The position index follows the readable
     * snapshots and learns their files through {@link LakeSource#planRewrite}. Otherwise tiering
     * resolves the superseded rows and commits their deletes with the data ({@link
     * LakeCommitter#commitWithDeletes}), so every commit is readable at its tiered offsets.
     *
     * <p>This check must not access lake resources.
     */
    default boolean mergesByPrimaryKey() {
        return false;
    }

    /**
     * Returns the serializer for committable objects.
     *
     * @return the serializer for committable objects
     */
    SimpleVersionedSerializer<CommittableT> getCommittableSerializer();

    /**
     * Creates a {@link LakeSource} of the lake table using the factory's configuration. Tiering
     * uses it for deletion vectors, to plan the re-index of rewritten data files and to read the
     * positions of their rows.
     *
     * @param tablePath the table path to create the lake source for
     * @return the lake source, or null if not supported
     */
    @Nullable
    default LakeSource<?> createLakeSource(TablePath tablePath) {
        return null;
    }
}
