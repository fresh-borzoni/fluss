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

import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * This class contains the {@link WriteResult} of {@link LakeWriter}, the table path and the bucket
 * that the write result is for, the end log offset of tiering, the total number of write results in
 * one round of tiering. It'll be passed to downstream committer operator to collect all the write
 * results of a table and do commit.
 */
public class TableBucketWriteResult<WriteResult> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TablePath tablePath;

    private final TableBucket tableBucket;

    // null when the bucket is not for a partition
    @Nullable private final String partitionName;

    // will be null when no any data write, such as for tiering an empty log split
    @Nullable private final WriteResult writeResult;

    // the end offset of tiering, should be the last tiered record's offset + 1
    private final long logEndOffset;

    // the max timestamp of tiering, should be the last tiered record's timestamp,
    // will be -1 for empty log splits or snapshot splits
    private final long maxTimestamp;

    // the total number of write results in one round of tiering,
    // used for downstream committer operator to determine when all write results
    // for the round of tiering is finished
    private final int numberOfWriteResults;

    // the tiering epoch the coordinator assigned the round's table under, which fences the round's
    // commits; TieringSplit.UNKNOWN_TIERING_EPOCH if unknown
    private final long tieringEpoch;

    public TableBucketWriteResult(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable WriteResult writeResult,
            long logEndOffset,
            long maxTimestamp,
            int numberOfWriteResults) {
        this(
                tablePath,
                tableBucket,
                partitionName,
                writeResult,
                logEndOffset,
                maxTimestamp,
                numberOfWriteResults,
                TieringSplit.UNKNOWN_TIERING_EPOCH);
    }

    public TableBucketWriteResult(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable WriteResult writeResult,
            long logEndOffset,
            long maxTimestamp,
            int numberOfWriteResults,
            long tieringEpoch) {
        this.tablePath = tablePath;
        this.tableBucket = tableBucket;
        this.partitionName = partitionName;
        this.writeResult = writeResult;
        this.logEndOffset = logEndOffset;
        this.maxTimestamp = maxTimestamp;
        this.numberOfWriteResults = numberOfWriteResults;
        this.tieringEpoch = tieringEpoch;
    }

    /** Returns this write result of a round the coordinator assigned under {@code tieringEpoch}. */
    public TableBucketWriteResult<WriteResult> withTieringEpoch(long tieringEpoch) {
        return tieringEpoch == this.tieringEpoch
                ? this
                : new TableBucketWriteResult<>(
                        tablePath,
                        tableBucket,
                        partitionName,
                        writeResult,
                        logEndOffset,
                        maxTimestamp,
                        numberOfWriteResults,
                        tieringEpoch);
    }

    public TablePath tablePath() {
        return tablePath;
    }

    public TableBucket tableBucket() {
        return tableBucket;
    }

    @Nullable
    public String partitionName() {
        return partitionName;
    }

    @Nullable
    public WriteResult writeResult() {
        return writeResult;
    }

    public int numberOfWriteResults() {
        return numberOfWriteResults;
    }

    public long logEndOffset() {
        return logEndOffset;
    }

    public long maxTimestamp() {
        return maxTimestamp;
    }

    /**
     * Returns the tiering epoch the coordinator assigned the round's table under, or {@link
     * TieringSplit#UNKNOWN_TIERING_EPOCH}.
     */
    public long tieringEpoch() {
        return tieringEpoch;
    }

    /**
     * Returns the tiering epoch of a round from its write results, or {@link
     * TieringSplit#UNKNOWN_TIERING_EPOCH} if none knows it.
     */
    public static long tieringEpochOf(List<? extends TableBucketWriteResult<?>> roundResults) {
        long epoch = TieringSplit.UNKNOWN_TIERING_EPOCH;
        for (TableBucketWriteResult<?> result : roundResults) {
            if (result.tieringEpoch == TieringSplit.UNKNOWN_TIERING_EPOCH) {
                continue;
            }
            checkState(
                    epoch == TieringSplit.UNKNOWN_TIERING_EPOCH || epoch == result.tieringEpoch,
                    "The write results of a round of table %s have tiering epochs %s and %s.",
                    result.tablePath,
                    epoch,
                    result.tieringEpoch);
            epoch = result.tieringEpoch;
        }
        return epoch;
    }
}
