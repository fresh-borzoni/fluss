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
import org.apache.fluss.lake.committer.SupportsPositionDeleteCommit;
import org.apache.fluss.lake.committer.SupportsPrimaryKeyMergeCommit;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.SupportsDataFiles;
import org.apache.fluss.lake.source.SupportsRewritePlanning;
import org.apache.fluss.lake.source.SupportsRowPositions;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * A {@link LakeTieringFactory} of a lake that can tier tables with deletion vectors ({@code
 * table.deletion-vectors.enabled}). Fluss refuses to tier such a table if the factory lacks this
 * capability.
 *
 * <p>A factory with this capability promises, for tables with deletion vectors:
 *
 * <ul>
 *   <li>its lake committers implement {@link SupportsPositionDeleteCommit} if {@link
 *       #mergesByPrimaryKey()} is false, or {@link SupportsPrimaryKeyMergeCommit} if it is true;
 *   <li>the lake source of {@link #createLakeSource} implements {@link SupportsRewritePlanning};
 *       its record readers implement {@link SupportsRowPositions} and its splits {@link
 *       SupportsDataFiles};
 *   <li>its lake writers take the table's position index directory from the {@link
 *       DeletionVectorWriterInitContext} Fluss gives them.
 * </ul>
 *
 * @since 1.1
 */
@PublicEvolving
public interface SupportsDeletionVectorTiering {

    /**
     * Returns whether the lake table merges rows by primary key itself, as a merge-tree table does.
     *
     * <p>Tiering a table with deletion vectors then commits no deletes: the lake supersedes old
     * rows on its own, and the committer reports which snapshot is readable ({@link
     * SupportsPrimaryKeyMergeCommit}). The position index follows the readable snapshots and learns
     * their files through {@link SupportsRewritePlanning#planRewrite}. Otherwise tiering resolves
     * the superseded rows and commits their deletes with the data ({@link
     * SupportsPositionDeleteCommit}), so every commit is readable at its tiered offsets.
     *
     * <p>This check must not access lake resources.
     */
    boolean mergesByPrimaryKey();

    /**
     * Creates a {@link LakeSource} of the lake table using the factory's configuration. Tiering
     * uses it to plan the re-index of rewritten data files and to read the positions of their rows.
     *
     * @param lakeTablePath the path of the lake table
     * @return the lake source, which implements {@link SupportsRewritePlanning}
     */
    LakeSource<?> createLakeSource(TablePath lakeTablePath);

    /**
     * Validates a table with deletion vectors before a tiering round writes any of its data. Fluss
     * calls it after {@link TieringTableValidator#validateTable}, if the factory implements that
     * too, so it makes only the checks deletion vectors need of the lake table, e.g. that it can
     * take deletes by position.
     *
     * <p>If {@code registeredSnapshotId} is not null, it also checks that the lake table's current
     * snapshot is the latest lake snapshot registered in Fluss or descends from it: a rollback of
     * the lake table outside Fluss breaks the deletion vectors. It then fails with {@link
     * org.apache.fluss.exception.LakeTableRolledBackException}, or with {@link
     * org.apache.fluss.exception.LakeTableExternallyModifiedException} if another engine changed
     * the table's rows since.
     *
     * @param tableInfo the Fluss table metadata
     * @param registeredSnapshotId the latest lake snapshot registered in Fluss, or null if none
     * @throws IOException if the validation fails or cannot be completed
     */
    void validateDeletionVectorTable(TableInfo tableInfo, @Nullable Long registeredSnapshotId)
            throws IOException;
}
