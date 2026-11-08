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

package org.apache.fluss.lake.dv;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.committer.SupportsPositionDeleteCommit;
import org.apache.fluss.lake.committer.SupportsPrimaryKeyMergeCommit;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.SupportsDataFiles;
import org.apache.fluss.lake.source.SupportsRewritePlanning;
import org.apache.fluss.lake.source.SupportsRowPositions;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.lake.writer.SupportsDeletionVectorTiering;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Checks that the lake plugin objects a table with deletion vectors uses have the capabilities it
 * needs, and returns them as those capabilities. Callers check once, where they get the object, and
 * fail with a clear error if the lake lacks a capability.
 */
@Internal
public final class DeletionVectorCapabilities {

    private DeletionVectorCapabilities() {}

    /** The lake tiering factory as one that can tier tables with deletion vectors. */
    public static SupportsDeletionVectorTiering tiering(
            LakeTieringFactory<?, ?> factory, TablePath tablePath) {
        return require(
                factory, SupportsDeletionVectorTiering.class, "lake tiering factory", tablePath);
    }

    /** The lake committer as one that commits deletes by position with a round's data. */
    @SuppressWarnings("unchecked")
    public static <WriteResult, CommittableT>
            SupportsPositionDeleteCommit<WriteResult, CommittableT> positionDeleteCommit(
                    LakeCommitter<WriteResult, CommittableT> committer, TablePath tablePath) {
        return require(committer, SupportsPositionDeleteCommit.class, "lake committer", tablePath);
    }

    /** The lake committer as one of a lake that merges rows by primary key itself. */
    public static SupportsPrimaryKeyMergeCommit primaryKeyMergeCommit(
            LakeCommitter<?, ?> committer, TablePath tablePath) {
        return require(committer, SupportsPrimaryKeyMergeCommit.class, "lake committer", tablePath);
    }

    /**
     * Checks that the lake committer commits the rounds of a table with deletion vectors the way
     * its lake does ({@link SupportsDeletionVectorTiering#mergesByPrimaryKey()}): it takes deletes
     * ({@link SupportsPositionDeleteCommit}), or the lake merges by primary key ({@link
     * SupportsPrimaryKeyMergeCommit}).
     */
    public static void checkCommitter(
            SupportsDeletionVectorTiering tiering,
            LakeCommitter<?, ?> committer,
            TablePath tablePath) {
        if (tiering.mergesByPrimaryKey()) {
            primaryKeyMergeCommit(committer, tablePath);
        } else {
            positionDeleteCommit(committer, tablePath);
        }
    }

    /** The lake source as one that plans rewrites and names the commits of snapshot ids. */
    @SuppressWarnings("unchecked")
    public static <Split extends LakeSplit> SupportsRewritePlanning<Split> rewritePlanning(
            LakeSource<Split> source, TablePath tablePath) {
        return require(source, SupportsRewritePlanning.class, "lake source", tablePath);
    }

    /** The record reader as one that reads rows with their positions. */
    public static SupportsRowPositions rowPositions(RecordReader reader) {
        return require(reader, SupportsRowPositions.class, "lake record reader", null);
    }

    /** The lake split as one that names its data files. */
    public static SupportsDataFiles dataFiles(LakeSplit split) {
        return require(split, SupportsDataFiles.class, "lake split", null);
    }

    /**
     * The name of the one data file of {@code split}, e.g. of a split of {@link
     * SupportsRewritePlanning#planRewrite}, which covers one data file each.
     */
    public static String onlyFileName(LakeSplit split) {
        List<String> fileNames = dataFiles(split).fileNames();
        if (fileNames == null || fileNames.size() != 1) {
            throw new IllegalStateException(
                    String.format(
                            "The lake split %s covers %s data files, not one.",
                            split, fileNames == null ? "unknown" : fileNames.size()));
        }
        return fileNames.get(0);
    }

    private static <T> T require(
            @Nullable Object object,
            Class<T> capability,
            String role,
            @Nullable TablePath tablePath) {
        if (object == null) {
            throw new NullPointerException(
                    String.format(
                            "No %s%s.", role, tablePath == null ? "" : " for table " + tablePath));
        }
        if (capability.isInstance(object)) {
            return capability.cast(object);
        }
        throw new UnsupportedOperationException(
                String.format(
                        "The %s %s does not implement %s, so its lake format does not support"
                                + " '%s'%s.",
                        role,
                        object.getClass().getName(),
                        capability.getName(),
                        ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key(),
                        tablePath == null ? "" : ", which table " + tablePath + " has enabled"));
    }
}
