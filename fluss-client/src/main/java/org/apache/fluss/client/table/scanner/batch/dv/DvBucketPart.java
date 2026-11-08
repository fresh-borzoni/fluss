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

package org.apache.fluss.client.table.scanner.batch.dv;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * A part of a bounded union read of one bucket of a primary-key table with deletion vectors: the
 * bucket's data files in a lake snapshot, masked by the rows the log changed since, and the log
 * after the snapshot's readable offset, masked by the versions superseded within it.
 *
 * <p>The masks cover the log up to a cut: the leader's high watermark when they were taken. The
 * lake files and the log of a bucket must be read against the same cut. A part planned without a
 * cut reads its whole bucket and fetches the masks when it starts. Such a part of a bucket with
 * more lake files than one part reads may carry none of them and plan them when it starts. A cut
 * pinned at planning is shared by several parts: groups of the bucket's lake files, and one part
 * for its log.
 *
 * <p>Once a reader resolved the {@link ReadMode}, the part records the snapshot it reads, its
 * files, the cut and the read position. An engine that restores reads keeps a started part with its
 * masks, so a restore reads with the same cut.
 */
@Internal
public final class DvBucketPart implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Returned by {@link #getNextLogOffset()} while the log is not read yet. */
    public static final long LOG_NOT_STARTED = -1L;

    /** Returned by {@link #getMaskLogEndOffset()} for a part not read with masks. */
    public static final long NO_MASK_CUT = -1L;

    /** Returned by {@link #getFileGroup()} for a part that reads all of its bucket. */
    public static final int WHOLE_BUCKET = -1;

    /** Returned by {@link #getFileGroup()} for the log part of a bucket read in several parts. */
    public static final int BUCKET_LOG = -2;

    /** How a reader reads the part. */
    public enum ReadMode {
        /** Not resolved yet: the reader fetches the masks when the part starts. */
        PENDING((byte) 0),
        /** The lake files and the log are read with the masks of cut {@code maskLogEndOffset}. */
        MASKED((byte) 1),
        /** The partition was dropped from Fluss: only its lake files are read, unmasked. */
        LAKE_ONLY((byte) 2),
        /**
         * The leader had no masks to serve, or they stayed unavailable: the masks of cut {@code
         * maskLogEndOffset} are derived from the keys of the lake files and of the log from the
         * snapshot's readable offset, the log folded to the latest change of each key. The part is
         * then read as {@link #MASKED} is.
         */
        FOLDED((byte) 3);

        private final byte code;

        ReadMode(byte code) {
            this.code = code;
        }

        public byte code() {
            return code;
        }

        public static ReadMode of(byte code) {
            for (ReadMode mode : values()) {
                if (mode.code == code) {
                    return mode;
                }
            }
            throw new IllegalArgumentException("Unknown read mode " + code);
        }
    }

    private final TableBucket tableBucket;
    @Nullable private final String partitionName;
    // the lake snapshot whose files the part reads, -1 for an empty lake (nothing tiered yet)
    private final long snapshotId;
    // the bucket's readable offset in the snapshot, where the log read starts; -1 if none tiered
    private final long snapshotLogOffset;
    // the stopping offset the read was planned with; a read by key stops there when the masks stay
    // unavailable
    private final long stoppingOffset;
    // the part's data files in the snapshot, null if the lake holds none of them
    @Nullable private final List<LakeSplit> lakeSplits;

    private final ReadMode readMode;
    private final long maskLogEndOffset;
    private final int currentLakeSplitIndex;
    // the rows of lake split currentLakeSplitIndex emitted so far
    private final long recordsToSkip;
    private final long nextLogOffset;
    // the masks of a started masked part, so a restore reads with the same cut
    @Nullable private final DvBucketMaskState maskState;

    private final boolean pinned;
    private final int fileGroup;
    // the log offset the masks of a pinned cut start at
    private final long pinnedLogStartOffset;
    // the part's lake files are planned when it starts, as there are too many to carry in a split
    private final boolean lakeSplitsPlannedAtStart;

    public DvBucketPart(
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
        this.tableBucket = tableBucket;
        this.partitionName = partitionName;
        this.snapshotId = snapshotId;
        this.snapshotLogOffset = snapshotLogOffset;
        this.stoppingOffset = stoppingOffset;
        this.lakeSplits = lakeSplits;
        this.readMode = readMode;
        this.maskLogEndOffset = maskLogEndOffset;
        this.currentLakeSplitIndex = currentLakeSplitIndex;
        this.recordsToSkip = recordsToSkip;
        this.nextLogOffset = nextLogOffset;
        this.maskState = maskState;
        this.pinned = pinned;
        this.fileGroup = fileGroup;
        this.pinnedLogStartOffset = pinnedLogStartOffset;
        this.lakeSplitsPlannedAtStart = lakeSplitsPlannedAtStart;
    }

    /**
     * Creates a part that reads all of a bucket and fetches its masks when it starts.
     *
     * @param snapshotId the lake snapshot read, -1 if nothing was tiered
     * @param snapshotLogOffset the bucket's readable offset in the snapshot, -1 if none
     * @param stoppingOffset the log offset the read stops at when read by key
     * @param lakeSplits the bucket's data files in the snapshot, null if none
     */
    public static DvBucketPart ofBucket(
            TableBucket tableBucket,
            @Nullable String partitionName,
            long snapshotId,
            long snapshotLogOffset,
            long stoppingOffset,
            @Nullable List<LakeSplit> lakeSplits) {
        return new DvBucketPart(
                tableBucket,
                partitionName,
                snapshotId,
                snapshotLogOffset,
                stoppingOffset,
                lakeSplits,
                ReadMode.PENDING,
                NO_MASK_CUT,
                0,
                0,
                LOG_NOT_STARTED,
                null,
                false,
                WHOLE_BUCKET,
                -1L,
                false);
    }

    /**
     * Creates a part read with a cut pinned at planning: a group of the bucket's lake files ({@code
     * fileGroup} from 0), its log ({@link #BUCKET_LOG}), or all of the bucket ({@link
     * #WHOLE_BUCKET}) when one part reads it.
     */
    public static DvBucketPart pinned(
            TableBucket tableBucket,
            @Nullable String partitionName,
            long snapshotId,
            long snapshotLogOffset,
            long stoppingOffset,
            @Nullable List<LakeSplit> lakeSplits,
            int fileGroup,
            long cut,
            long logStartOffset) {
        return new DvBucketPart(
                tableBucket,
                partitionName,
                snapshotId,
                snapshotLogOffset,
                stoppingOffset,
                lakeSplits,
                ReadMode.MASKED,
                cut,
                0,
                0,
                LOG_NOT_STARTED,
                null,
                true,
                fileGroup,
                logStartOffset,
                false);
    }

    /**
     * Creates a part of a bucket whose leader had no masks to serve when the read was planned. Like
     * {@link #pinned}, but every part of the bucket derives its masks of cut {@code cut} from the
     * keys ({@link ReadMode#FOLDED}), with the log from the snapshot's readable offset.
     */
    public static DvBucketPart pinnedFolded(
            TableBucket tableBucket,
            @Nullable String partitionName,
            long snapshotId,
            long snapshotLogOffset,
            long stoppingOffset,
            @Nullable List<LakeSplit> lakeSplits,
            int fileGroup,
            long cut) {
        return new DvBucketPart(
                tableBucket,
                partitionName,
                snapshotId,
                snapshotLogOffset,
                stoppingOffset,
                lakeSplits,
                ReadMode.FOLDED,
                cut,
                0,
                0,
                LOG_NOT_STARTED,
                null,
                true,
                fileGroup,
                snapshotLogOffset,
                false);
    }

    /**
     * Returns this part read as {@code readMode}, from its start, by a reader that resolved it
     * itself: with masks it fetched with a cut it pinned, derived from the keys, or none. The part
     * no longer reads with a cut pinned at planning.
     */
    public DvBucketPart resolved(
            ReadMode readMode,
            long snapshotId,
            long snapshotLogOffset,
            @Nullable List<LakeSplit> lakeSplits,
            long maskLogEndOffset) {
        return new DvBucketPart(
                tableBucket,
                partitionName,
                snapshotId,
                snapshotLogOffset,
                stoppingOffset,
                lakeSplits,
                readMode,
                maskLogEndOffset,
                0,
                0,
                LOG_NOT_STARTED,
                null,
                false,
                fileGroup,
                -1L,
                false);
    }

    /**
     * Returns this part, not resolved yet, without its lake files: its reader plans them when the
     * part starts. A bucket with many lake files whose cut could not be pinned is read by one part,
     * which would otherwise carry all of them.
     */
    public DvBucketPart withLakeSplitsPlannedAtStart() {
        checkState(
                readMode == ReadMode.PENDING && !isStarted(),
                "Only a part not resolved yet plans its lake files when it starts: %s",
                this);
        return new DvBucketPart(
                tableBucket,
                partitionName,
                snapshotId,
                snapshotLogOffset,
                stoppingOffset,
                null,
                readMode,
                maskLogEndOffset,
                currentLakeSplitIndex,
                recordsToSkip,
                nextLogOffset,
                maskState,
                pinned,
                fileGroup,
                pinnedLogStartOffset,
                true);
    }

    /**
     * Returns this part at the given read position: lake split {@code currentLakeSplitIndex} with
     * {@code recordsToSkip} of its rows read, or the log from {@code nextLogOffset} once read.
     */
    public DvBucketPart atPosition(
            int currentLakeSplitIndex, long recordsToSkip, long nextLogOffset) {
        return new DvBucketPart(
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
                lakeSplitsPlannedAtStart);
    }

    /** Returns this part with the given masks, or none. */
    public DvBucketPart withMaskState(@Nullable DvBucketMaskState maskState) {
        return new DvBucketPart(
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
                lakeSplitsPlannedAtStart);
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    @Nullable
    public String getPartitionName() {
        return partitionName;
    }

    /** Whether the part reads with a cut pinned at planning. */
    public boolean isPinned() {
        return pinned;
    }

    /** Whether other parts read the rest of the bucket with the same pinned cut. */
    public boolean sharesBucket() {
        return pinned && fileGroup != WHOLE_BUCKET;
    }

    /** The group of the bucket's lake files the part reads, or {@link #WHOLE_BUCKET}. */
    public int getFileGroup() {
        return fileGroup;
    }

    /** Whether the part reads its bucket's log: it reads all of the bucket, or its log alone. */
    public boolean readsLog() {
        return fileGroup < 0;
    }

    /**
     * Whether the part's lake files are planned when it starts, instead of carried by the part.
     * {@link #getLakeSplits()} is null until then.
     */
    public boolean isLakeSplitsPlannedAtStart() {
        return lakeSplitsPlannedAtStart;
    }

    /** The log offset the masks of a pinned cut start at; -1 if not pinned. */
    public long getPinnedLogStartOffset() {
        return pinnedLogStartOffset;
    }

    /** The masks the part was read with, kept once it started; null if not kept. */
    @Nullable
    public DvBucketMaskState getMaskState() {
        return maskState;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public long getSnapshotLogOffset() {
        return snapshotLogOffset;
    }

    public long getStoppingOffset() {
        return stoppingOffset;
    }

    @Nullable
    public List<LakeSplit> getLakeSplits() {
        return lakeSplits;
    }

    public int lakeSplitCount() {
        return lakeSplits == null ? 0 : lakeSplits.size();
    }

    public ReadMode getReadMode() {
        return readMode;
    }

    /** The cut the part is read with, or {@link #NO_MASK_CUT}. */
    public long getMaskLogEndOffset() {
        return maskLogEndOffset;
    }

    public int getCurrentLakeSplitIndex() {
        return currentLakeSplitIndex;
    }

    public long getRecordsToSkip() {
        return recordsToSkip;
    }

    public long getNextLogOffset() {
        return nextLogOffset;
    }

    /** Whether rows of the part may have been emitted, so it must resume where it was. */
    public boolean isStarted() {
        return currentLakeSplitIndex > 0 || recordsToSkip > 0 || nextLogOffset >= 0;
    }

    /** A short name of the part for messages. */
    public String describe() {
        String part =
                fileGroup == BUCKET_LOG
                        ? "log"
                        : fileGroup == WHOLE_BUCKET ? "all" : "files " + fileGroup;
        return "bucket " + tableBucket + " (" + part + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DvBucketPart)) {
            return false;
        }
        DvBucketPart that = (DvBucketPart) o;
        return snapshotId == that.snapshotId
                && snapshotLogOffset == that.snapshotLogOffset
                && stoppingOffset == that.stoppingOffset
                && maskLogEndOffset == that.maskLogEndOffset
                && currentLakeSplitIndex == that.currentLakeSplitIndex
                && recordsToSkip == that.recordsToSkip
                && nextLogOffset == that.nextLogOffset
                && pinned == that.pinned
                && fileGroup == that.fileGroup
                && pinnedLogStartOffset == that.pinnedLogStartOffset
                && lakeSplitsPlannedAtStart == that.lakeSplitsPlannedAtStart
                && readMode == that.readMode
                && tableBucket.equals(that.tableBucket)
                && Objects.equals(partitionName, that.partitionName)
                && Objects.equals(lakeSplits, that.lakeSplits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                tableBucket,
                partitionName,
                snapshotId,
                snapshotLogOffset,
                stoppingOffset,
                readMode,
                maskLogEndOffset,
                currentLakeSplitIndex,
                recordsToSkip,
                nextLogOffset);
    }

    @Override
    public String toString() {
        return "DvBucketPart{"
                + "tableBucket="
                + tableBucket
                + ", partitionName='"
                + partitionName
                + '\''
                + ", snapshotId="
                + snapshotId
                + ", snapshotLogOffset="
                + snapshotLogOffset
                + ", stoppingOffset="
                + stoppingOffset
                + ", lakeSplits="
                + lakeSplitCount()
                + ", readMode="
                + readMode
                + ", maskLogEndOffset="
                + maskLogEndOffset
                + ", currentLakeSplitIndex="
                + currentLakeSplitIndex
                + ", recordsToSkip="
                + recordsToSkip
                + ", nextLogOffset="
                + nextLogOffset
                + ", maskBytes="
                + (maskState == null ? 0 : maskState.maskBytes())
                + ", pinned="
                + pinned
                + ", fileGroup="
                + fileGroup
                + ", lakeSplitsPlannedAtStart="
                + lakeSplitsPlannedAtStart
                + '}';
    }
}
