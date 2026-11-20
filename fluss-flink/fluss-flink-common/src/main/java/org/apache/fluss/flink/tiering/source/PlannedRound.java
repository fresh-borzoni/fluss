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

import org.apache.fluss.client.tiering.RoundDeletes;
import org.apache.fluss.client.tiering.TieringLogSplit;
import org.apache.fluss.lake.writer.RoundMaskMismatchException;
import org.apache.fluss.utils.ExceptionUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * A bucket's round its leader planned, as the tiering reader reads it. The round's log mask is
 * fetched from the leader in the background once the reader has memory for it; the reader
 * subscribes the bucket once it decided how the round is written: through the mask, or in sort
 * mode, where the writer keeps the last version of each key itself, if the masks are off or the
 * mask does not match the round. So the reader never waits for a mask while it reads other buckets.
 *
 * <p>Once subscribed, the round's deletes are fetched from the leader in the background too, and
 * handed to the committer with the round's write result.
 *
 * <p>A round is completed only if it was read whole: every offset of the round once, as a record or
 * as an offset of a batch without records, and the records in increasing offsets. A round read
 * through its mask that was not is read again from its start in sort mode; one read in sort mode
 * fails. A round whose mask turns out not to match it is read again in sort mode too.
 */
final class PlannedRound {

    // the most heap a mask takes per 64K offsets of its round, a container of 8 KB and its key
    private static final long MASK_BYTES_PER_CHUNK = 8 * 1024 + 128;

    private final TieringLogSplit split;
    // whether the round is read through its mask if it matches, else in sort mode
    private final boolean masked;
    // the mask's fetch, or null if not started or the masks are off
    @Nullable private CompletableFuture<RoundMask.Fetched> fetched;
    private boolean decided;
    private boolean subscribed;
    @Nullable private RoundMask mask;
    @Nullable private String sortModeBecause;
    // offsets of the round read in this pass, as records or as offsets without records
    private long covered;
    // the offset of the last record read in this pass, and whether one came twice or out of order
    private long lastRecordOffset;
    private boolean outOfOrder;
    // the offset the scanner consumed the round's log up to in this pass
    private long consumedUpTo;
    // the last time the round's mask fetch started or ended, or the scanner consumed more of it
    private long lastProgressMs;
    // the fetch of the round's deletes, or null if not started or handed over
    @Nullable private CompletableFuture<RoundDeletes> deletes;

    PlannedRound(TieringLogSplit split, boolean masked, long nowMs) {
        this.split = split;
        this.masked = masked;
        this.lastProgressMs = nowMs;
        this.lastRecordOffset = split.getStartingOffset() - 1;
        this.consumedUpTo = split.getStartingOffset();
    }

    TieringLogSplit split() {
        return split;
    }

    boolean subscribed() {
        return subscribed;
    }

    void subscribed(boolean subscribed) {
        this.subscribed = subscribed;
    }

    /** Whether the round waits for the reader to start the fetch of its mask. */
    boolean waitsToFetch() {
        return masked && fetched == null;
    }

    /** Starts the fetch of the round's mask. */
    void startFetch(CompletableFuture<RoundMask.Fetched> fetched, long nowMs) {
        this.fetched = fetched;
        this.lastProgressMs = nowMs;
    }

    /**
     * The heap the round's mask takes: what the fetched mask takes, or the most it may take while
     * it is fetched; nothing in sort mode.
     */
    long maskBytes() {
        if (fetched == null || (decided && mask == null)) {
            return 0;
        }
        return mask != null ? mask.bytes() : maxMaskBytes(split);
    }

    /** The most heap the mask of a round takes: at most one bit per offset of the round. */
    static long maxMaskBytes(TieringLogSplit split) {
        long offsets = split.getStoppingOffset() - split.getStartingOffset();
        return (offsets / (1 << 16) + 2) * MASK_BYTES_PER_CHUNK;
    }

    /**
     * Decides how the round is written once its mask is fetched: through the mask, or in sort mode
     * if the masks are off or the mask does not match the round. Returns false while the fetch
     * waits or runs and if it failed ({@link #failure()}).
     */
    boolean tryDecide(long nowMs) throws IOException {
        if (decided) {
            return true;
        }
        if (!masked) {
            decided = true;
            return true;
        }
        if (fetched == null || !fetched.isDone() || failure() != null) {
            return false;
        }
        RoundMask.Fetched mask;
        try {
            mask = fetched.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while taking a round's log mask", e);
        } catch (ExecutionException e) {
            // a mismatch: failure() is null for it
            sortModeBecause = ExceptionUtils.stripExecutionException(e).getMessage();
            decided = true;
            lastProgressMs = nowMs;
            return true;
        }
        this.mask = new RoundMask(split.getTableBucket(), mask);
        decided = true;
        lastProgressMs = nowMs;
        return true;
    }

    /** Why the fetch of the round's mask failed, other than a mismatch, or null. */
    @Nullable
    Throwable failure() {
        if (fetched == null || !fetched.isCompletedExceptionally()) {
            return null;
        }
        try {
            fetched.get();
            return null;
        } catch (Exception e) {
            Throwable cause = ExceptionUtils.stripExecutionException(e);
            return cause instanceof RoundMaskMismatchException ? null : cause;
        }
    }

    /** The fetch of the round's mask, or null if not started or the masks are off. */
    @Nullable
    CompletableFuture<RoundMask.Fetched> fetch() {
        return fetched;
    }

    /** Fetches the round's mask again, after a fetch that failed. */
    void fetchAgain(CompletableFuture<RoundMask.Fetched> fetched) {
        this.fetched = fetched;
    }

    /** The mask the round is read through, or null in sort mode. */
    @Nullable
    RoundMask mask() {
        return mask;
    }

    /** Why the round is written in sort mode although the masks are on, or null. */
    @Nullable
    String sortModeBecause() {
        return sortModeBecause;
    }

    /** Counts a record of the round read, at {@code offset}. */
    void read(long offset, long nowMs) {
        if (offset <= lastRecordOffset) {
            outOfOrder = true;
        }
        lastRecordOffset = offset;
        covered++;
        lastProgressMs = nowMs;
    }

    /** Counts the offsets of the round in [from, to) of a batch without records. */
    void readWithoutRecords(long from, long to) {
        long lo = Math.max(from, split.getStartingOffset());
        long hi = Math.min(to, split.getStoppingOffset());
        if (lo < hi) {
            covered += hi - lo;
        }
    }

    /** The scanner consumed the round's log up to {@code offset}, records or not. */
    void consumed(long offset, long nowMs) {
        if (offset > consumedUpTo) {
            consumedUpTo = offset;
            lastProgressMs = nowMs;
        }
    }

    long lastProgressMs() {
        return lastProgressMs;
    }

    /** Why the round was not read whole, once the scanner consumed it to its end, or null. */
    @Nullable
    String notReadWhole() {
        long offsets = split.getStoppingOffset() - split.getStartingOffset();
        if (outOfOrder) {
            return "a record of it came twice or out of order";
        }
        if (covered != offsets) {
            return String.format("it read %d of its %d offsets", covered, offsets);
        }
        return null;
    }

    /** The round is read again from its start and written in sort mode. */
    void readAgainInSortMode(String because) {
        mask = null;
        sortModeBecause = because;
        covered = 0;
        lastRecordOffset = split.getStartingOffset() - 1;
        outOfOrder = false;
        consumedUpTo = split.getStartingOffset();
    }

    /** Starts the fetch of the round's deletes. */
    void startDeletesFetch(CompletableFuture<RoundDeletes> deletes) {
        this.deletes = deletes;
    }

    /** The round's deletes if their fetch completed, else null. */
    @Nullable
    RoundDeletes fetchedDeletes() {
        return deletes != null && deletes.isDone() && !deletes.isCompletedExceptionally()
                ? deletes.join()
                : null;
    }

    /**
     * Drops the fetched deletes of the rounds that do not fit in {@code maxBytes}, keeping those of
     * the rounds that come first, so their write results go without them. Returns the rounds whose
     * deletes were dropped.
     */
    static List<PlannedRound> dropDeletesOverBudget(Iterable<PlannedRound> rounds, long maxBytes) {
        List<PlannedRound> dropped = new ArrayList<>();
        long held = 0;
        for (PlannedRound round : rounds) {
            RoundDeletes fetched = round.fetchedDeletes();
            if (fetched == null) {
                continue;
            }
            if (held + fetched.bytes() <= maxBytes) {
                held += fetched.bytes();
            } else {
                round.deletes = null;
                dropped.add(round);
            }
        }
        return dropped;
    }

    /** Hands the fetch of the round's deletes to its write result; null if none was started. */
    @Nullable
    CompletableFuture<RoundDeletes> takeDeletes() {
        CompletableFuture<RoundDeletes> taken = deletes;
        deletes = null;
        return taken;
    }

    /**
     * Drops the round's mask, and stops the fetch of its deletes unless handed over; a running mask
     * fetch is left to end on its own.
     */
    void release() {
        mask = null;
        decided = true;
        if (fetched != null) {
            fetched.cancel(false);
        }
        if (deletes != null) {
            deletes.cancel(true);
            deletes = null;
        }
    }
}
