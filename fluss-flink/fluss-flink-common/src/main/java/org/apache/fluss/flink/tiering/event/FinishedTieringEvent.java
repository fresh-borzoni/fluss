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

import org.apache.fluss.client.tiering.TieringSplit;
import org.apache.fluss.lake.committer.TieringStats;

import org.apache.flink.api.connector.source.SourceEvent;

import javax.annotation.Nullable;

/** SourceEvent used to represent a Fluss table has been tiered finished. */
public class FinishedTieringEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final long tableId;

    /**
     * The tiering epoch of the finished round, or {@link TieringSplit#UNKNOWN_TIERING_EPOCH}. The
     * end of a round whose table was handed out again under a newer epoch is stale.
     */
    private final long tieringEpoch;

    /** Statistics collected during this tiering round. */
    @Nullable private final TieringStats stats;

    /**
     * Whether a bucket's round ended before the end of its split, so the table has more to tier at
     * once instead of after its freshness interval.
     */
    private final boolean moreToTier;

    public FinishedTieringEvent(long tableId, @Nullable TieringStats stats) {
        this(tableId, stats, false);
    }

    public FinishedTieringEvent(long tableId, @Nullable TieringStats stats, boolean moreToTier) {
        this(tableId, TieringSplit.UNKNOWN_TIERING_EPOCH, stats, moreToTier);
    }

    public FinishedTieringEvent(
            long tableId, long tieringEpoch, @Nullable TieringStats stats, boolean moreToTier) {
        this.tableId = tableId;
        this.tieringEpoch = tieringEpoch;
        this.stats = stats;
        this.moreToTier = moreToTier;
    }

    public FinishedTieringEvent(long tableId) {
        this(tableId, null);
    }

    public long getTableId() {
        return tableId;
    }

    public long getTieringEpoch() {
        return tieringEpoch;
    }

    @Nullable
    public TieringStats getStats() {
        return stats;
    }

    public boolean hasMoreToTier() {
        return moreToTier;
    }
}
