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

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * The change of the position index from one lake snapshot to another: the data files to index
 * (splits to scan) and the data files that left the snapshot.
 *
 * <p>Each split holds exactly one data file, whose name the reader takes from {@link
 * LakeSplit#fileName()}. The reader reads each split with {@link RecordReader#readWithPos()} and
 * maps the primary key of every live row to its file and row position.
 *
 * <p>Removed files left the snapshot, e.g. replaced by a rewrite. Their entries are dropped from
 * the position index.
 *
 * @param <Split> The type of data split
 * @since 0.8
 */
@PublicEvolving
public class DataDeltaPlan<Split extends LakeSplit> {

    /** The lake snapshot the plan leads to, e.g. the rewrite that produced these files. */
    private final long compactSnapshotId;

    /** Files to re-index, each split contains exactly one file. */
    private final List<Split> splits;

    /**
     * Files removed by the rewrite, indexed by (partitionName, bucketId). For non-partitioned
     * tables, partitionName is null; lake formats know partition names, not partition ids.
     */
    private final Map<String, Map<Integer, List<String>>> deletedFiles;

    public DataDeltaPlan(
            long compactSnapshotId,
            List<Split> splits,
            Map<String, Map<Integer, List<String>>> deletedFiles) {
        this.compactSnapshotId = compactSnapshotId;
        this.splits = splits;
        this.deletedFiles = deletedFiles;
    }

    /** Returns the lake snapshot the plan leads to, e.g. the rewrite that produced these files. */
    public long getCompactSnapshotId() {
        return compactSnapshotId;
    }

    /** Returns the splits to scan. Each split contains exactly one data file. */
    public List<Split> getSplits() {
        return splits;
    }

    /** Returns the files removed by the rewrite, indexed by (partitionName, bucketId). */
    public Map<String, Map<Integer, List<String>>> getDeletedFiles() {
        return deletedFiles;
    }

    /**
     * Returns the deleted file names for a specific partition and bucket.
     *
     * @param partitionName the partition name, or null for non-partitioned tables
     * @param bucketId the bucket id
     * @return the deleted file names, or an empty list if none
     */
    public List<String> getDeletedFiles(String partitionName, int bucketId) {
        Map<Integer, List<String>> bucketFiles = deletedFiles.get(partitionName);
        if (bucketFiles == null) {
            return Collections.emptyList();
        }
        return bucketFiles.getOrDefault(bucketId, Collections.emptyList());
    }
}
