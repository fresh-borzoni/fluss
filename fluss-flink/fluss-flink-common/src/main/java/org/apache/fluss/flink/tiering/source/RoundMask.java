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

package org.apache.fluss.flink.tiering.source;

import org.apache.fluss.client.tiering.TieringLogSplit;
import org.apache.fluss.lake.writer.RoundMaskMismatchException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;

import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The log mask of a bucket's planned round, from its leader, as the tiering reader applies it. The
 * reader skips the masked versions, which a change below the round's end supersedes, and the -U
 * records, and hands the rest to the lake writer in log order: at most the last version of each
 * key, apart from versions the mask missed.
 *
 * <p>The reader trusts the leader's change list for the mask, as it does for the round's deletes,
 * and checks the mask against the round it reads: every part must be of the round's snapshot, start
 * and end, all from one leader epoch, and mask offsets of the round only. A masked offset must be
 * an insert or an update after image; a -U or -D at a masked offset is a mismatch. At the end every
 * masked offset must have been read, and the round's changes, the records that supersede a version,
 * must be as many as the leader counted. That the round was read whole, every offset once, the
 * reader checks apart ({@link PlannedRound#notReadWhole()}).
 *
 * <p>The mask is held besides the writers' sort buffers: a bitmap of offsets of the round, at most
 * one bit per offset of the round, and two bytes per masked offset where they are sparse.
 */
final class RoundMask {

    private final TableBucket bucket;
    private final Fetched fetched;
    private final long maskedOffsets;

    // records of the round masked and skipped, and changes read (+U, -D, and +I if counted)
    private long masked;
    private long changes;

    RoundMask(TableBucket bucket, Fetched fetched) {
        this.bucket = bucket;
        this.fetched = fetched;
        this.maskedOffsets = fetched.offsets.getLongCardinality();
    }

    /** A round's mask as its leader served it: the masked offsets, and the changes it counted. */
    static final class Fetched {
        final Roaring64Bitmap offsets;
        // the round's changes the leader counted, or null if it did not count them
        @Nullable final Long changes;
        final boolean changesWithInserts;

        Fetched(Roaring64Bitmap offsets, @Nullable Long changes, boolean changesWithInserts) {
            this.offsets = offsets;
            this.changes = changes;
            this.changesWithInserts = changesWithInserts;
        }
    }

    /**
     * Fetches the round's mask from the bucket's leader, all of its parts, and checks that they are
     * of the round. Waits for a leader that is not ready at most {@code maxWaitMs} in all.
     *
     * @throws RoundMaskMismatchException if a part is not of the round
     * @throws Exception if the leader does not serve the mask
     */
    static Fetched fetch(PartFetcher parts, TieringLogSplit split, long maxWaitMs)
            throws Exception {
        long snapshotId = split.getRoundSnapshotId();
        TableBucket tb = split.getTableBucket();
        long start = split.getStartingOffset();
        long end = split.getStoppingOffset();
        long deadline = System.currentTimeMillis() + maxWaitMs;
        Roaring64Bitmap mask = new Roaring64Bitmap();
        Integer leaderEpoch = null;
        // null once a part comes without a count, or counts otherwise than the parts before
        Long changes = 0L;
        Boolean withInserts = null;
        Long from = null;
        while (true) {
            GetDvSnapshotResponse part =
                    parts.fetch(from, Math.max(0, deadline - System.currentTimeMillis()));
            if (!part.hasRoundSnapshotId()
                    || part.getRoundSnapshotId() != snapshotId
                    || !part.hasSnapshotStartOffset()
                    || part.getSnapshotStartOffset() != start
                    || !part.hasRoundEndOffset()
                    || part.getRoundEndOffset() != end
                    || !part.hasLeaderEpoch()
                    || !part.hasRoundNextOffset()) {
                throw mismatch(
                        tb,
                        String.format(
                                "a part of it is of snapshot %s from %s to %s, not of snapshot %s"
                                        + " from %s to %s",
                                part.hasRoundSnapshotId() ? part.getRoundSnapshotId() : null,
                                part.hasSnapshotStartOffset()
                                        ? part.getSnapshotStartOffset()
                                        : null,
                                part.hasRoundEndOffset() ? part.getRoundEndOffset() : null,
                                snapshotId,
                                start,
                                end));
            }
            if (leaderEpoch == null) {
                leaderEpoch = part.getLeaderEpoch();
            } else if (leaderEpoch != part.getLeaderEpoch()) {
                throw mismatch(
                        tb,
                        String.format(
                                "its parts come from leader epochs %s and %s",
                                leaderEpoch, part.getLeaderEpoch()));
            }
            long partStart = from == null ? start : from;
            long next = part.getRoundNextOffset();
            if (next > end || (next <= partStart && next != end)) {
                throw mismatch(
                        tb,
                        String.format(
                                "its part from %s names the next part at %s, outside (%s, %s]",
                                partStart, next, partStart, end));
            }
            if (part.hasLogDvBitmap()) {
                Roaring64Bitmap offsets = offsetsOf(tb, partStart, part.getLogDvBitmap());
                if (!offsets.isEmpty() && (offsets.first() < start || offsets.last() >= end)) {
                    throw mismatch(
                            tb,
                            String.format(
                                    "it masks offsets from %s to %s, outside [%s, %s)",
                                    offsets.first(), offsets.last(), start, end));
                }
                mask.or(offsets);
            }
            boolean partWithInserts =
                    part.hasRoundChangesWithInserts() && part.isRoundChangesWithInserts();
            if (changes != null
                    && part.hasRoundChanges()
                    && (withInserts == null || withInserts == partWithInserts)) {
                withInserts = partWithInserts;
                changes += part.getRoundChanges();
            } else {
                changes = null;
            }
            if (next == end) {
                return new Fetched(mask, changes, withInserts != null && withInserts);
            }
            from = next;
        }
    }

    /**
     * The offsets a part masks. Read here, not through the client's helpers: the connector jar
     * relocates the bitmap classes, so no signature between modules carries them.
     */
    private static Roaring64Bitmap offsetsOf(TableBucket tb, long partStart, byte[] bytes)
            throws RoundMaskMismatchException {
        Roaring64Bitmap offsets = new Roaring64Bitmap();
        if (bytes.length > 0) {
            try {
                offsets.deserialize(ByteBuffer.wrap(bytes));
            } catch (IOException | RuntimeException e) {
                throw mismatch(
                        tb,
                        String.format(
                                "its part from %s is not a bitmap of offsets (%s)", partStart, e));
            }
        }
        return offsets;
    }

    /** The masked offsets and the heap they take, for the round's log line. */
    long maskedOffsets() {
        return maskedOffsets;
    }

    long bytes() {
        return fetched.offsets.getLongSizeInBytes();
    }

    /**
     * Whether to hand a record of the round to the lake writer.
     *
     * @throws RoundMaskMismatchException if the mask masks a -U or a -D
     */
    boolean pass(long offset, ChangeType changeType) throws RoundMaskMismatchException {
        boolean isMasked = fetched.offsets.contains(offset);
        if (changeType == ChangeType.UPDATE_BEFORE || changeType == ChangeType.DELETE) {
            if (isMasked) {
                throw mismatch(
                        String.format(
                                "it masks offset %d, a %s record, not a version",
                                offset, changeType.shortString()));
            }
            if (changeType == ChangeType.UPDATE_BEFORE) {
                return false;
            }
        }
        if (changeType != ChangeType.INSERT || fetched.changesWithInserts) {
            changes++;
        }
        if (isMasked) {
            masked++;
            return false;
        }
        return true;
    }

    /**
     * Checks the round once it is read: every masked offset was a version the reader read, and the
     * changes read are those the leader counted.
     *
     * @throws RoundMaskMismatchException if a check fails
     */
    void finish() throws RoundMaskMismatchException {
        if (masked != maskedOffsets) {
            throw mismatch(
                    String.format(
                            "it masks %d offsets, but the round read %d of them",
                            maskedOffsets, masked));
        }
        if (fetched.changes != null && changes != fetched.changes) {
            throw mismatch(
                    String.format(
                            "its leader counts %d changes in the round, but the round read %d",
                            fetched.changes, changes));
        }
    }

    private RoundMaskMismatchException mismatch(String what) {
        return mismatch(bucket, what);
    }

    private static RoundMaskMismatchException mismatch(TableBucket bucket, String what) {
        return new RoundMaskMismatchException(
                String.format(
                        "The log mask of the round of bucket %s does not match the round: %s.",
                        bucket, what));
    }

    /**
     * Fetches the part of the round's mask from {@code from} on, or from the start if null, waiting
     * for a leader that is not ready at most {@code maxWaitMs}.
     */
    @FunctionalInterface
    interface PartFetcher {
        GetDvSnapshotResponse fetch(@Nullable Long from, long maxWaitMs) throws Exception;
    }
}
