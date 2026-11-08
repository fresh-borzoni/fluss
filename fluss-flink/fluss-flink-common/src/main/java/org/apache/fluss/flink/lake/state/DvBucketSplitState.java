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

package org.apache.fluss.flink.lake.state;

import org.apache.fluss.client.table.scanner.batch.dv.DvBucketMaskState;
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketPart;
import org.apache.fluss.flink.lake.split.DvBucketSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.split.SourceSplitState;

/**
 * The state of a {@link DvBucketSplit}: how the reader resolved it, and its read position. A
 * started masked split keeps the masks it still needs, so that a restore reads with the same cut.
 */
public class DvBucketSplitState extends SourceSplitState {

    private DvBucketSplit resolved;
    private int currentLakeSplitIndex;
    private long recordsToSkip;
    private long nextLogOffset;

    public DvBucketSplitState(DvBucketSplit split) {
        super(split);
        this.resolved = split;
        this.currentLakeSplitIndex = split.getCurrentLakeSplitIndex();
        this.recordsToSkip = split.getRecordsToSkip();
        this.nextLogOffset = split.getNextLogOffset();
    }

    /**
     * Records how the reader reads the split, before any of its rows: the read mode, the snapshot
     * and files it reads, and the cut of its masks.
     */
    public void resolve(DvBucketSplit resolved) {
        this.resolved = resolved;
        this.currentLakeSplitIndex = resolved.getCurrentLakeSplitIndex();
        this.recordsToSkip = resolved.getRecordsToSkip();
        this.nextLogOffset = resolved.getNextLogOffset();
    }

    /** Records a row emitted from a lake file, or the merge read's count of emitted rows. */
    public void setLakePosition(int currentLakeSplitIndex, long recordsToSkip) {
        this.currentLakeSplitIndex = currentLakeSplitIndex;
        this.recordsToSkip = recordsToSkip;
    }

    /** Records a row emitted from the log, after every lake file was read. */
    public void setNextLogOffset(long nextLogOffset) {
        this.currentLakeSplitIndex = resolved.lakeSplitCount();
        this.recordsToSkip = 0;
        this.nextLogOffset = nextLogOffset;
    }

    public DvBucketPart.ReadMode getReadMode() {
        return resolved.getReadMode();
    }

    @Override
    public SourceSplitBase toSourceSplit() {
        DvBucketSplit split =
                resolved.atPosition(currentLakeSplitIndex, recordsToSkip, nextLogOffset);
        DvBucketMaskState masks = split.getMaskState();
        if (masks == null) {
            return split;
        }
        if (!split.isStarted()) {
            // nothing emitted yet: a restore fetches masks again
            return split.withMaskState(null);
        }
        // the lake files already read need no masks any more
        int fromLakeSplit = nextLogOffset >= 0 ? split.lakeSplitCount() : currentLakeSplitIndex;
        DvBucketMaskState needed =
                masks.withoutLakeSplitsBefore(fromLakeSplit, split.getLakeSplits());
        return needed == masks ? split : split.withMaskState(needed);
    }
}
