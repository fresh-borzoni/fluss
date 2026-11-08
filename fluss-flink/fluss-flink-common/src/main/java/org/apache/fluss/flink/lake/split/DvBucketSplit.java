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

package org.apache.fluss.flink.lake.split;

import org.apache.fluss.client.table.scanner.batch.dv.DvBucketMaskState;
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketPart;
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketPart.ReadMode;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.List;

/**
 * A split of a bounded union read of a primary key table with deletion vectors. It reads one {@link
 * DvBucketPart}: the whole bucket, or a part of it with a cut pinned at planning.
 *
 * <p>An assigned split carries no masks. Its reader fetches them from the bucket's leader when the
 * split starts, so planning and split assignments stay small however many rows changed. A split of
 * a bucket with more lake files than one split reads, whose cut could not be pinned, carries none
 * of them either: its reader plans them when it starts. Once the reader resolves how to read the
 * split, the split records the snapshot, its files, the cut and the read position. A started masked
 * split also keeps the masks, so that a restore reads with the same cut.
 */
public class DvBucketSplit extends SourceSplitBase {

    public static final byte DV_BUCKET_SPLIT_KIND = -3;

    /** Returned by {@link #getNextLogOffset()} while the log is not read yet. */
    public static final long LOG_NOT_STARTED = DvBucketPart.LOG_NOT_STARTED;

    /** Returned by {@link #getMaskLogEndOffset()} for a split not read with masks. */
    public static final long NO_MASK_CUT = DvBucketPart.NO_MASK_CUT;

    /** Returned by {@link #getFileGroup()} for a split that reads all of its bucket. */
    public static final int WHOLE_BUCKET = DvBucketPart.WHOLE_BUCKET;

    /** Returned by {@link #getFileGroup()} for the log split of a bucket read by several splits. */
    public static final int BUCKET_LOG = DvBucketPart.BUCKET_LOG;

    private final DvBucketPart part;

    public DvBucketSplit(DvBucketPart part) {
        super(part.getTableBucket(), part.getPartitionName());
        this.part = part;
    }

    public DvBucketSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            long snapshotId,
            long snapshotLogOffset,
            long stoppingOffset,
            @Nullable List<LakeSplit> lakeSplits) {
        this(
                DvBucketPart.ofBucket(
                        tableBucket,
                        partitionName,
                        snapshotId,
                        snapshotLogOffset,
                        stoppingOffset,
                        lakeSplits));
    }

    public DvBucketSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            long snapshotId,
            long snapshotLogOffset,
            long stoppingOffset,
            @Nullable List<LakeSplit> lakeSplits,
            ReadMode readMode,
            long maskLogEndOffset,
            int currentLakeSplitIndex,
            long recordsToSkip,
            long nextLogOffset,
            @Nullable DvBucketMaskState maskState,
            boolean pinned,
            int fileGroup,
            long pinnedLogStartOffset,
            boolean lakeSplitsPlannedAtStart) {
        this(
                new DvBucketPart(
                        tableBucket,
                        partitionName,
                        snapshotId,
                        snapshotLogOffset,
                        stoppingOffset,
                        lakeSplits,
                        readMode,
                        maskLogEndOffset,
                        currentLakeSplitIndex,
                        recordsToSkip,
                        nextLogOffset,
                        maskState,
                        pinned,
                        fileGroup,
                        pinnedLogStartOffset,
                        lakeSplitsPlannedAtStart));
    }

    /** See {@link DvBucketPart#pinned}. */
    public static DvBucketSplit pinned(
            TableBucket tableBucket,
            @Nullable String partitionName,
            long snapshotId,
            long snapshotLogOffset,
            long stoppingOffset,
            @Nullable List<LakeSplit> lakeSplits,
            int fileGroup,
            long cut,
            long logStartOffset) {
        return new DvBucketSplit(
                DvBucketPart.pinned(
                        tableBucket,
                        partitionName,
                        snapshotId,
                        snapshotLogOffset,
                        stoppingOffset,
                        lakeSplits,
                        fileGroup,
                        cut,
                        logStartOffset));
    }

    /** The part of the bucket the split reads. */
    public DvBucketPart getPart() {
        return part;
    }

    /** Returns this split read as {@code readMode}, from its start. */
    public DvBucketSplit resolved(
            ReadMode readMode,
            long snapshotId,
            long snapshotLogOffset,
            @Nullable List<LakeSplit> lakeSplits,
            long maskLogEndOffset) {
        return new DvBucketSplit(
                part.resolved(
                        readMode, snapshotId, snapshotLogOffset, lakeSplits, maskLogEndOffset));
    }

    /** Returns this split at the given read position. */
    public DvBucketSplit atPosition(
            int currentLakeSplitIndex, long recordsToSkip, long nextLogOffset) {
        return new DvBucketSplit(
                part.atPosition(currentLakeSplitIndex, recordsToSkip, nextLogOffset));
    }

    /** Returns this split with the given masks, or none. */
    public DvBucketSplit withMaskState(@Nullable DvBucketMaskState maskState) {
        return new DvBucketSplit(part.withMaskState(maskState));
    }

    /** Whether the split reads with a cut pinned at planning. */
    public boolean isPinned() {
        return part.isPinned();
    }

    /** Whether other splits read other parts of the bucket with the same pinned cut. */
    public boolean sharesBucket() {
        return part.sharesBucket();
    }

    /** The group of the bucket's lake files the split reads, or {@link #WHOLE_BUCKET}. */
    public int getFileGroup() {
        return part.getFileGroup();
    }

    /** Whether the split reads its bucket's log. */
    public boolean readsLog() {
        return part.readsLog();
    }

    /** Whether the split's reader plans its lake files when it starts. */
    public boolean isLakeSplitsPlannedAtStart() {
        return part.isLakeSplitsPlannedAtStart();
    }

    /** The log offset the masks of a pinned cut start at, or -1 if not pinned. */
    public long getPinnedLogStartOffset() {
        return part.getPinnedLogStartOffset();
    }

    /** The masks the split was read with, kept once it started; null if not kept. */
    @Nullable
    public DvBucketMaskState getMaskState() {
        return part.getMaskState();
    }

    public long getSnapshotId() {
        return part.getSnapshotId();
    }

    public long getSnapshotLogOffset() {
        return part.getSnapshotLogOffset();
    }

    public long getStoppingOffset() {
        return part.getStoppingOffset();
    }

    @Nullable
    public List<LakeSplit> getLakeSplits() {
        return part.getLakeSplits();
    }

    public int lakeSplitCount() {
        return part.lakeSplitCount();
    }

    public ReadMode getReadMode() {
        return part.getReadMode();
    }

    public long getMaskLogEndOffset() {
        return part.getMaskLogEndOffset();
    }

    public int getCurrentLakeSplitIndex() {
        return part.getCurrentLakeSplitIndex();
    }

    public long getRecordsToSkip() {
        return part.getRecordsToSkip();
    }

    public long getNextLogOffset() {
        return part.getNextLogOffset();
    }

    /** Whether rows of the split may have been emitted, so it must resume where it was. */
    public boolean isStarted() {
        return part.isStarted();
    }

    @Override
    public boolean isLakeSplit() {
        return true;
    }

    @Override
    protected byte splitKind() {
        return DV_BUCKET_SPLIT_KIND;
    }

    @Override
    public String splitId() {
        String bucketId = toSplitId("lake-dv-bucket-", tableBucket);
        int fileGroup = part.getFileGroup();
        if (fileGroup == BUCKET_LOG) {
            return bucketId + "-log";
        }
        return fileGroup == WHOLE_BUCKET ? bucketId : bucketId + "-g" + fileGroup;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DvBucketSplit) || !super.equals(o)) {
            return false;
        }
        return part.equals(((DvBucketSplit) o).part);
    }

    @Override
    public int hashCode() {
        return part.hashCode();
    }

    @Override
    public String toString() {
        return "DvBucketSplit{" + part + '}';
    }
}
