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

package org.apache.fluss.client.tiering;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;

/**
 * A split to re-index the lake data files of one bucket for deletion vectors after another engine
 * or a compaction rewrote them.
 *
 * <p>The tiering enumerator creates it from a row position request of the committer. A bucket's
 * files are spread over splits of a bounded number of files each. A split references the lake
 * splits of its files, which the committer stored with the index change ({@code
 * PositionIndexStore#writeReindexPlan}), so neither the request nor the split grows with the
 * table's schema. The reader reads every live row of the files and writes its part of the bucket's
 * index change: the encoded primary key of each row mapped to its file id and row position.
 */
public class TieringRowPosSplit extends TieringSplit {

    private static final long serialVersionUID = 1L;

    public static final byte TIERING_ROW_POS_SPLIT_FLAG = 3;

    private static final String TIERING_ROW_POS_SPLIT_PREFIX = "tiering-row-pos-split-";

    /** File IDs assigned by the committer, one per file of this split, in plan order. */
    private final int[] fileIds;

    /** The reference of the stored lake splits of the files, one per file id. */
    private final String planReference;

    /** The index of this split among the request's splits. */
    private final int split;

    /** The number of results the committer expects for this round. */
    private final int totalExpectedResults;

    /** The id of the index change the bucket's part is written under. */
    private final long compactSnapshotId;

    /** The remote lake directory of the table, holding the position index. */
    private final String remoteUploadBasePath;

    public TieringRowPosSplit(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            int[] fileIds,
            String planReference,
            int split,
            int totalExpectedResults,
            long compactSnapshotId,
            String remoteUploadBasePath) {
        this(
                tablePath,
                tableBucket,
                partitionName,
                fileIds,
                planReference,
                split,
                totalExpectedResults,
                compactSnapshotId,
                remoteUploadBasePath,
                UNKNOWN_SPLIT_INDEX,
                UNKNOWN_TIERING_ROUND_TIMESTAMP);
    }

    public TieringRowPosSplit(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            int[] fileIds,
            String planReference,
            int split,
            int totalExpectedResults,
            long compactSnapshotId,
            String remoteUploadBasePath,
            int splitIndex,
            long tieringRoundTimestamp) {
        super(
                tablePath,
                tableBucket,
                partitionName,
                totalExpectedResults,
                false,
                splitIndex,
                tieringRoundTimestamp);
        this.fileIds = fileIds;
        this.planReference = planReference;
        this.split = split;
        this.totalExpectedResults = totalExpectedResults;
        this.compactSnapshotId = compactSnapshotId;
        this.remoteUploadBasePath = remoteUploadBasePath;
    }

    @Override
    public String splitId() {
        return toSplitId(TIERING_ROW_POS_SPLIT_PREFIX, this.tableBucket) + "-f" + fileIds[0];
    }

    public int[] getFileIds() {
        return fileIds;
    }

    public String getPlanReference() {
        return planReference;
    }

    public int getSplit() {
        return split;
    }

    public int getTotalExpectedResults() {
        return totalExpectedResults;
    }

    public long getCompactSnapshotId() {
        return compactSnapshotId;
    }

    public String getRemoteUploadBasePath() {
        return remoteUploadBasePath;
    }

    @Override
    public TieringRowPosSplit copy(int numberOfSplits, int splitIndex, long tieringRoundTimestamp) {
        // the number of splits of a row position split is its total expected results
        return new TieringRowPosSplit(
                tablePath,
                tableBucket,
                partitionName,
                fileIds,
                planReference,
                split,
                numberOfSplits,
                compactSnapshotId,
                remoteUploadBasePath,
                splitIndex,
                tieringRoundTimestamp);
    }

    @Override
    public String toString() {
        return "TieringRowPosSplit{"
                + "tablePath="
                + tablePath
                + ", tableBucket="
                + tableBucket
                + ", partitionName='"
                + partitionName
                + '\''
                + ", fileIds="
                + Arrays.toString(fileIds)
                + ", planReference='"
                + planReference
                + '\''
                + ", split="
                + split
                + ", totalExpectedResults="
                + totalExpectedResults
                + ", compactSnapshotId="
                + compactSnapshotId
                + ", remoteUploadBasePath='"
                + remoteUploadBasePath
                + '\''
                + ", splitIndex="
                + splitIndex
                + ", tieringRoundTimestamp="
                + tieringRoundTimestamp
                + '}';
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof TieringRowPosSplit)) {
            return false;
        }
        if (!super.equals(object)) {
            return false;
        }
        TieringRowPosSplit that = (TieringRowPosSplit) object;
        return totalExpectedResults == that.totalExpectedResults
                && compactSnapshotId == that.compactSnapshotId
                && split == that.split
                && Arrays.equals(fileIds, that.fileIds)
                && Objects.equals(planReference, that.planReference)
                && Objects.equals(remoteUploadBasePath, that.remoteUploadBasePath);
    }

    @Override
    public int hashCode() {
        int result =
                Objects.hash(
                        super.hashCode(),
                        totalExpectedResults,
                        compactSnapshotId,
                        split,
                        planReference,
                        remoteUploadBasePath);
        result = 31 * result + Arrays.hashCode(fileIds);
        return result;
    }
}
