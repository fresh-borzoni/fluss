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

package org.apache.fluss.flink.tiering.event;

import org.apache.fluss.metadata.TablePath;

import org.apache.flink.api.connector.source.SourceEvent;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;

/**
 * A source event the committer sends to the enumerator to request the re-index of rewritten lake
 * data files of a table with deletion vectors.
 *
 * <p>The re-index is planned as splits, each a bounded number of files of one bucket. The lake
 * splits of a split's files are stored with the index change ({@code
 * PositionIndexStore#writeReindexPlan}), so an event carries only a reference per split. It stays
 * small however many files were rewritten and however large the table's schema. A re-index with
 * many splits is requested in several events of a bounded number of splits each. The enumerator
 * creates the splits of each event as it arrives.
 *
 * <p>The files the rewrite removed are not part of this event. The committer keeps them.
 */
public class RowPosRequestEvent implements SourceEvent {

    private static final long serialVersionUID = 2L;

    private final long tableId;
    private final TablePath tablePath;

    /** The id of the index change the readers write their index parts under. */
    private final long compactSnapshotId;

    /** The remote lake directory of the table, holding the position index. */
    private final String remoteUploadBasePath;

    /** Some of the re-index's splits. */
    private final List<Split> splits;

    /** The number of splits of the whole re-index, over all its events. */
    private final int totalSplits;

    public RowPosRequestEvent(
            long tableId,
            TablePath tablePath,
            long compactSnapshotId,
            String remoteUploadBasePath,
            List<Split> splits,
            int totalSplits) {
        this.tableId = tableId;
        this.tablePath = tablePath;
        this.compactSnapshotId = compactSnapshotId;
        this.remoteUploadBasePath = remoteUploadBasePath;
        this.splits = splits;
        this.totalSplits = totalSplits;
    }

    public long getTableId() {
        return tableId;
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public long getCompactSnapshotId() {
        return compactSnapshotId;
    }

    public String getRemoteUploadBasePath() {
        return remoteUploadBasePath;
    }

    public List<Split> getSplits() {
        return splits;
    }

    public int getTotalSplits() {
        return totalSplits;
    }

    /** One split of a re-index: some rewritten files of one bucket. */
    public static final class Split implements Serializable {

        private static final long serialVersionUID = 2L;

        /** Partition name, null for a non-partitioned table. */
        @Nullable private final String partitionName;

        /** Partition id, null for a non-partitioned table. */
        @Nullable private final Long partitionId;

        private final int bucket;

        /** The index of the split among all splits of the re-index. */
        private final int split;

        /** The ids of the split's files, in the order of the stored plan. */
        private final int[] fileIds;

        /** The reference of the stored lake splits of the files. */
        private final String planReference;

        public Split(
                @Nullable String partitionName,
                @Nullable Long partitionId,
                int bucket,
                int split,
                int[] fileIds,
                String planReference) {
            this.partitionName = partitionName;
            this.partitionId = partitionId;
            this.bucket = bucket;
            this.split = split;
            this.fileIds = fileIds;
            this.planReference = planReference;
        }

        @Nullable
        public String getPartitionName() {
            return partitionName;
        }

        @Nullable
        public Long getPartitionId() {
            return partitionId;
        }

        public int getBucket() {
            return bucket;
        }

        public int getSplit() {
            return split;
        }

        public int[] getFileIds() {
            return fileIds;
        }

        public String getPlanReference() {
            return planReference;
        }
    }
}
