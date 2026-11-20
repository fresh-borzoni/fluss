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
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Prepares the deletion vectors of a bucket's tiering round next to its data, as the lake's own
 * engines do: merges the round's deletes into the current deletion vectors of the data files they
 * delete rows in, and writes them, so the committer only checks that they still hold on its base
 * and commits them. Created per table by a {@link SupportsDeletionVectorPreparation} factory and
 * used by several threads at once.
 *
 * @param <WriteResult> the write result type of the lake writer
 * @since 1.1
 */
@PublicEvolving
public interface DeletionVectorPreparer<WriteResult> extends AutoCloseable {

    /**
     * Returns {@code writeResult}, the result of the bucket's round from {@code startOffset} to
     * {@code endOffset}, with the bucket's deletion vectors prepared on lake snapshot {@code
     * snapshotId}: the round's deletes there ({@code deletedPositionsByFile}, positions per data
     * file name). Returns null if they are not prepared here, e.g. the round compacted the bucket;
     * the committer then merges the deletes itself.
     */
    @Nullable
    WriteResult prepare(
            WriteResult writeResult,
            TableBucket bucket,
            long snapshotId,
            long startOffset,
            long endOffset,
            Map<String, long[]> deletedPositionsByFile)
            throws IOException;

    /**
     * Called when a tiering reader starts reading a bucket's round on lake snapshot {@code
     * snapshotId}, long before its deletes arrive: the preparer may start reading what {@link
     * #prepare} reads on that snapshot, so that it overlaps the round's read. Best effort, and must
     * not block.
     */
    default void prefetch(long snapshotId) {}

    /**
     * Called when the deletes of a bucket's round on lake snapshot {@code snapshotId} arrive, which
     * is mostly before the round's writer completes: the preparer may start reading the current
     * deletion vectors of the data files they name ({@code dataFiles}, by name), for {@link
     * #prepare} to find them read. Best effort, and must not block.
     */
    default void prefetch(TableBucket bucket, long snapshotId, Collection<String> dataFiles) {}

    /** Deletes the files of deletion vectors {@link #prepare} wrote that will not be committed. */
    void discard(WriteResult prepared) throws IOException;
}
