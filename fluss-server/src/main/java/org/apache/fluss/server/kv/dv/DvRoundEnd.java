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

package org.apache.fluss.server.kv.dv;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.server.log.LogSegment;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.OffsetIndex;
import org.apache.fluss.server.log.OffsetPosition;
import org.apache.fluss.server.log.remote.RemoteLogManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * The end of a tiering round of a bucket with deletion vectors, which its leader picks when the
 * round is planned. The round's log mask is exact at that end only, so the end never moves once
 * picked: the caps of a round apply here, not while it is written.
 *
 * <p>The end is the largest offset up to the high watermark whose round, from its start, holds at
 * most the most records (log offsets are dense) and at most the most bytes, as the offset indexes
 * of the log's segments place the records. An index entry names the last offset of an append and
 * where the append starts, so a record is placed where the first indexed append at or after it
 * starts: exactly where its own append starts if every append is indexed, else up to an index
 * interval later. The end is past the start whenever the high watermark is, so every round makes
 * progress, also over an append larger than the byte cap. Only the indexes are read, never the
 * records.
 */
@Internal
public final class DvRoundEnd {

    private final long endOffset;
    private final boolean capped;

    private DvRoundEnd(long endOffset, boolean capped) {
        this.endOffset = endOffset;
        this.capped = capped;
    }

    /** The round's end (exclusive). */
    public long endOffset() {
        return endOffset;
    }

    /** Whether the caps put the end below the high watermark, so more is left to tier. */
    public boolean capped() {
        return capped;
    }

    /**
     * Plans the end of a round from {@code startOffset} over the log {@code pieces}, in offset
     * order, that cover it up to {@code highWatermark}. Offsets no piece covers count no bytes.
     */
    public static DvRoundEnd plan(
            List<? extends LogPiece> pieces,
            long startOffset,
            long highWatermark,
            long maxRecords,
            long maxBytes)
            throws IOException {
        checkArgument(maxRecords > 0 && maxBytes > 0, "A round's caps must be positive.");
        if (highWatermark <= startOffset) {
            return new DvRoundEnd(startOffset, false);
        }
        long limit =
                highWatermark - startOffset > maxRecords ? startOffset + maxRecords : highWatermark;
        long used = 0;
        long cursor = startOffset;
        for (LogPiece piece : pieces) {
            long from = Math.max(cursor, piece.startOffset());
            long to = Math.min(limit, piece.endOffset());
            if (from >= to) {
                continue;
            }
            long fromPosition = piece.positionOf(from);
            long bytes = piece.positionOf(to) - fromPosition;
            if (used + bytes <= maxBytes) {
                used += bytes;
                cursor = to;
                continue;
            }
            long end = Math.min(to, piece.endAt(fromPosition + (maxBytes - used)));
            if (end <= from) {
                // The pieces before hold the round. Or a single append is above the byte cap,
                // and the round takes it to make progress.
                end = cursor > startOffset ? cursor : Math.min(to, piece.endOfAppendAt(from));
            }
            return new DvRoundEnd(end, end < highWatermark);
        }
        return new DvRoundEnd(limit, limit < highWatermark);
    }

    /**
     * The segments of a bucket's log from {@code startOffset} up to {@code endOffset}, in offset
     * order: the remote ones below the local log start, then the local ones. Nothing is read yet; a
     * remote segment's index is downloaded once a plan reads it.
     */
    public static List<LogPiece> logPieces(
            TableBucket tb,
            LogTablet log,
            @Nullable RemoteLogManager remoteLogManager,
            long startOffset,
            long endOffset) {
        List<LogPiece> pieces = new ArrayList<>();
        long localStart = log.localLogStartOffset();
        if (startOffset < localStart && remoteLogManager != null) {
            for (RemoteLogSegment segment :
                    remoteLogManager.remoteLogSegmentsFrom(tb, startOffset)) {
                if (segment.logicalStartOffset() >= Math.min(localStart, endOffset)) {
                    break;
                }
                pieces.add(
                        new IndexedPiece(
                                segment.logicalStartOffset(),
                                Math.min(segment.logicalEndOffset(), localStart),
                                segment.remoteLogStartOffset(),
                                segment.remoteLogEndOffset(),
                                segment.segmentSizeInBytes()) {
                            @Override
                            protected OffsetPosition ceiling(long offset) {
                                return remoteLogManager.lookupIndexCeiling(segment, offset);
                            }
                        });
            }
        }
        List<LogSegment> segments = log.logSegments(Math.max(startOffset, localStart), endOffset);
        for (int i = 0; i < segments.size(); i++) {
            LogSegment segment = segments.get(i);
            long base = segment.getBaseOffset();
            // a segment ends where the next one starts, the active one at the log end
            long end =
                    i + 1 < segments.size()
                            ? segments.get(i + 1).getBaseOffset()
                            : log.localLogEndOffset();
            pieces.add(
                    new IndexedPiece(base, end, base, end, segment.getSizeInBytes()) {
                        @Override
                        protected OffsetPosition ceiling(long offset) throws IOException {
                            return segment.offsetIndex().lookupCeiling(offset);
                        }
                    });
        }
        return pieces;
    }

    /**
     * A segment of the log, local or remote, as its offset index places its records. Positions are
     * relative to the segment's start.
     */
    public interface LogPiece {

        /** The first offset the piece serves. */
        long startOffset();

        /** The offset after the last record the piece serves. */
        long endOffset();

        /**
         * Where the record at {@code offset} starts, as the index places it; the segment's size at
         * its physical end. It only grows with the offset.
         */
        long positionOf(long offset) throws IOException;

        /**
         * The largest offset whose round reads the segment only up to {@code position}: no offset
         * after it starts at or below {@code position}. It ends where an indexed append starts, or
         * at the segment's physical end.
         */
        long endAt(long position) throws IOException;

        /**
         * The offset after the first indexed append at or after {@code offset}, or the segment's
         * physical end if there is none.
         */
        long endOfAppendAt(long offset) throws IOException;
    }

    /**
     * A log segment whose offset index covers the physical offsets [base, physical end). It looks
     * entries up by offset only, so a remote segment's index serves it through its cache.
     */
    public abstract static class IndexedPiece implements LogPiece {

        private final long startOffset;
        private final long endOffset;
        private final long baseOffset;
        private final long physicalEndOffset;
        private final long sizeInBytes;

        protected IndexedPiece(
                long startOffset,
                long endOffset,
                long baseOffset,
                long physicalEndOffset,
                long sizeInBytes) {
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.baseOffset = baseOffset;
            this.physicalEndOffset = physicalEndOffset;
            this.sizeInBytes = sizeInBytes;
        }

        /**
         * The first index entry at or above {@code offset}, or null if there is none, as {@link
         * OffsetIndex#lookupCeiling} returns it.
         */
        @Nullable
        protected abstract OffsetPosition ceiling(long offset) throws IOException;

        @Override
        public long startOffset() {
            return startOffset;
        }

        @Override
        public long endOffset() {
            return endOffset;
        }

        @Override
        public long positionOf(long offset) throws IOException {
            if (offset <= baseOffset) {
                return 0;
            }
            if (offset >= physicalEndOffset) {
                return sizeInBytes;
            }
            OffsetPosition entry = ceiling(offset);
            return entry == null ? sizeInBytes : entry.getPosition();
        }

        @Override
        public long endAt(long position) throws IOException {
            if (sizeInBytes <= position) {
                return physicalEndOffset;
            }
            // the last offset placed at or below the position
            long lo = baseOffset;
            long hi = physicalEndOffset - 1;
            while (lo < hi) {
                long mid = lo + (hi - lo + 1) / 2;
                if (positionOf(mid) <= position) {
                    lo = mid;
                } else {
                    hi = mid - 1;
                }
            }
            // the round ends before the first offset placed where that one is
            long placed = positionOf(lo);
            hi = lo;
            lo = baseOffset;
            while (lo < hi) {
                long mid = lo + (hi - lo) / 2;
                if (positionOf(mid) >= placed) {
                    hi = mid;
                } else {
                    lo = mid + 1;
                }
            }
            return lo;
        }

        @Override
        public long endOfAppendAt(long offset) throws IOException {
            OffsetPosition entry = ceiling(offset);
            return entry == null ? physicalEndOffset : entry.getOffset() + 1;
        }
    }
}
