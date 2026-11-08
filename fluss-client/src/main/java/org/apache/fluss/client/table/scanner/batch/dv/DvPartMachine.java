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
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketPart.ReadMode;
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketPartReader.ReadStats;
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.client.table.scanner.batch.dv.DvPartState.CLOSED;
import static org.apache.fluss.client.table.scanner.batch.dv.DvPartState.DONE;
import static org.apache.fluss.client.table.scanner.batch.dv.DvPartState.FAILED;
import static org.apache.fluss.client.table.scanner.batch.dv.DvPartState.FOLDING;
import static org.apache.fluss.client.table.scanner.batch.dv.DvPartState.PINNED;
import static org.apache.fluss.client.table.scanner.batch.dv.DvPartState.PINNED_AT_PLANNING;
import static org.apache.fluss.client.table.scanner.batch.dv.DvPartState.PLANNED;
import static org.apache.fluss.client.table.scanner.batch.dv.DvPartState.READING_LAKE;
import static org.apache.fluss.client.table.scanner.batch.dv.DvPartState.READING_LOG;
import static org.apache.fluss.client.table.scanner.batch.dv.DvPartState.READY;
import static org.apache.fluss.client.table.scanner.batch.dv.DvPartState.REPLANNING;
import static org.apache.fluss.client.table.scanner.batch.dv.DvPartState.RESUMING;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * The read of one {@link DvBucketPart} as a state machine: its {@link DvPartState}, what the state
 * works on, and {@link #apply}, the only place the state changes. {@link DvPartResolver} performs
 * the action of each resolving state and {@link DvBucketPartReader} that of each reading state;
 * each action ends in a {@link DvPartEvent} applied here.
 *
 * <p>S is the lake snapshot the part reads, o the bucket's readable offset in S, c a cut. Guards:
 * <i>replans</i> counts COVERED answers to pins, of which the {@link #MAX_REPLANS}th is read by
 * key; <i>repins</i> counts cuts the leader lost before their masks were fetched, at most {@link
 * #MAX_REPINS} of which are pinned again; <i>started</i> is a part restored after it emitted rows;
 * <i>whole</i> a part that reads all of its bucket; <i>returned</i> a part whose reader returned
 * rows. R is the coordinator's readable snapshot.
 *
 * <pre>
 * From                Event                                    To
 * PLANNED             MASKS (the pin's answer: c)              PINNED, holding its own pin of c
 * PLANNED             REBUILDING (cut f)                       FOLDING S from o up to f
 * PLANNED             COVERED (S'), replans left               REPLANNING S' (R if S' has no offset)
 * PLANNED             COVERED (none left), NOT_READY,          REPLANNING R to read it by key up to
 *                     LEADER_LOST, ERROR                         the planned stopping offset
 * PLANNED             PARTITION_DROPPED                        READY, lake only
 * PLANNED             ROLLED_BACK, INTERRUPTED                 FAILED
 * REPLANNING          REPLANNED                                PLANNED, or FOLDING for R
 * REPLANNING          ROLLED_BACK, INTERRUPTED, ERROR          FAILED
 * PINNED              MASKS (all of c)                         READY, masked, keeping its own pin
 * PINNED              COVERED, REBUILDING, NOT_READY,          PLANNED (c is lost), and once
 *                     LEADER_LOST                                MAX_REPINS were lost FOLDING S up to c
 * PINNED              PARTITION_DROPPED                        READY, lake only
 * PINNED              ROLLED_BACK, INTERRUPTED, ERROR          FAILED
 * PINNED_AT_PLANNING  MASKS                                    READY, masked
 * PINNED_AT_PLANNING  ROLLED_BACK, INTERRUPTED                 FAILED
 * PINNED_AT_PLANNING  any other answer, started                FAILED: cannot resume
 * PINNED_AT_PLANNING  any other answer, whole                  PLANNED on S
 * PINNED_AT_PLANNING  any other answer, not whole              FOLDING c from the pinned start
 * FOLDING             DERIVED                                  READY, folded (masked if the part
 *                                                                was pinned at planning)
 * FOLDING             any failure                              FAILED
 * RESUMING            MASKS (the kept ones, checked)           READY
 * RESUMING            any failure                              FAILED
 * READY               POLL                                     READING_LAKE
 * READING_LAKE        LAKE_END                                 READING_LOG if the part reads its
 *                                                                log, else DONE
 * READING_LOG         LOG_END                                  DONE
 * READING_LOG         PARTITION_DROPPED, whole, not returned,  DONE, without rows
 *                     not started
 * READING_LOG         PARTITION_DROPPED otherwise              FAILED
 * READY, READING_*    ROLLED_BACK, INTERRUPTED, ERROR          FAILED
 * any but CLOSED      CLOSE                                    CLOSED
 * </pre>
 *
 * <p>Any other pair is a bug and fails with an {@link IllegalStateException}. A part starts, also
 * when restored, in the state its stored form names ({@link #initial()}):
 *
 * <pre>
 * Stored part                                   Initial state
 * PENDING                                       PLANNED, or REPLANNING its own files if it
 *                                                 carries none
 * MASKED or FOLDED with kept masks              RESUMING
 * MASKED pinned at planning                     PINNED_AT_PLANNING
 * MASKED, started                               FAILED: cannot resume without its masks
 * MASKED or LAKE_ONLY, not started              PLANNED on the snapshot it resolved
 * LAKE_ONLY, started                            READY
 * FOLDED                                        FOLDING its cut again
 * </pre>
 *
 * <p>The part's own pin ({@link DvOwnPin}) is held from the pin's answer, before anything checks
 * it, while the part is PINNED, and in READY and the reading states of a part read with the masks
 * of that pin. Entering any other state releases it: a lost cut, a fold, a lake-only read, and the
 * ends DONE, FAILED and CLOSED.
 */
@Internal
final class DvPartMachine {

    private static final Logger LOG = LoggerFactory.getLogger(DvPartMachine.class);

    // The most re-plans on another snapshot than the planned one, when leaders move to newer ones.
    static final int MAX_REPLANS = 3;
    static final long REPLAN_BACKOFF_MS = 400;
    // The most times a part pins its own cut again after the leader lost it before its masks were
    // fetched; then it reads the bucket by key up to the last cut it pinned.
    static final int MAX_REPINS = 3;

    /**
     * What a part reads: a lake snapshot, the bucket's readable offset in it and its lake files.
     * The offset is the planned one, one a leader named for the snapshot it covers, or the
     * coordinator's readable one; never a tiered offset, which may be past the readable one.
     */
    static final class Target {
        final long snapshotId;
        final long snapshotLogOffset;
        @Nullable final List<LakeSplit> lakeSplits;

        Target(long snapshotId, long snapshotLogOffset, @Nullable List<LakeSplit> lakeSplits) {
            this.snapshotId = snapshotId;
            this.snapshotLogOffset = snapshotLogOffset;
            this.lakeSplits = lakeSplits;
        }

        static Target of(DvBucketPart part) {
            return new Target(
                    part.getSnapshotId(), part.getSnapshotLogOffset(), part.getLakeSplits());
        }
    }

    /** What REPLANNING plans. */
    enum Replan {
        /** The part's own snapshot, for a part that carries no lake files. */
        OWN_FILES,
        /** The snapshot the leader covers, or the readable one; then the part pins again. */
        COVERED,
        /** The readable snapshot, read by key up to the planned stopping offset. */
        READABLE_BY_KEY
    }

    private final DvBucketPart part;
    private final TablePath tablePath;
    private final ReadStats stats;
    private final DvOwnPin ownPin;
    // the states the part went through, for tests and messages
    private final List<DvPartState> history = new ArrayList<>();

    private DvPartState state;
    private Target target;
    private int replans;
    private int repins;

    // REPLANNING
    @Nullable private Replan replan;
    @Nullable private StaleSnapshotException covered;
    // PINNED and PINNED_AT_PLANNING: the cut; PINNED: the leader's answer to the pin
    private long cut = -1L;
    @Nullable private GetDvSnapshotResponse pinAnswer;
    // FOLDING: the part whose masks are derived, from which log offset, up to which cut
    @Nullable private DvBucketPart foldPart;
    private long foldLogStart;
    private long foldCut;
    // FOLDING a lost cut pinned at planning: why the leader no longer serves it; null for a read
    // by key
    @Nullable private Throwable lostCut;

    // READY on: how the part is read, and with which masks
    @Nullable private DvBucketPart resolved;
    @Nullable private DvBucketMasks masks;
    private boolean returnedRows;
    @Nullable private Throwable failure;

    DvPartMachine(DvBucketPart part, TablePath tablePath, ReadStats stats, DvOwnPin ownPin) {
        this.part = part;
        this.tablePath = tablePath;
        this.stats = stats;
        this.ownPin = ownPin;
        this.target = Target.of(part);
        this.state = initial();
        history.add(state);
    }

    /** The state a part starts in, also when restored; see the class javadoc. */
    private DvPartState initial() {
        ReadMode mode = part.getReadMode();
        DvBucketMaskState kept = part.getMaskState();
        if (mode == ReadMode.FOLDED) {
            if (kept != null) {
                stats.folded = true;
                return RESUMING;
            }
            // folded at planning or when it started: derives its masks again
            setFolding(part, part.getSnapshotLogOffset(), part.getMaskLogEndOffset(), null);
            return FOLDING;
        }
        if (part.isPinned() && mode == ReadMode.MASKED) {
            if (kept != null) {
                return RESUMING;
            }
            cut = part.getMaskLogEndOffset();
            return PINNED_AT_PLANNING;
        }
        if (part.isStarted() && mode != ReadMode.PENDING) {
            if (mode != ReadMode.MASKED) {
                // a lake-only read depends on the snapshot and planned offsets only
                resolved = part;
                return READY;
            }
            if (kept != null) {
                return RESUMING;
            }
            failure = cannotResume("it kept no masks of that cut", null);
            return FAILED;
        }
        if (part.isLakeSplitsPlannedAtStart()) {
            replan = Replan.OWN_FILES;
            return REPLANNING;
        }
        return PLANNED;
    }

    /** Applies {@code event} to the part's state: the only place the state changes. */
    void apply(DvPartEvent event) {
        if (event.kind == DvPartEvent.Kind.CLOSE) {
            if (state != CLOSED) {
                enter(CLOSED);
            }
            return;
        }
        switch (state) {
            case PLANNED:
                onPlanned(event);
                return;
            case REPLANNING:
                onReplanning(event);
                return;
            case PINNED:
                onPinned(event);
                return;
            case PINNED_AT_PLANNING:
                onPinnedAtPlanning(event);
                return;
            case FOLDING:
                onFolding(event);
                return;
            case RESUMING:
                onResuming(event);
                return;
            case READY:
            case READING_LAKE:
            case READING_LOG:
                onReading(event);
                return;
            default:
                throw unexpected(event);
        }
    }

    private void onPlanned(DvPartEvent event) {
        switch (event.kind) {
            case MASKS:
                GetDvSnapshotResponse answer = checkNotNull(event.pinAnswer);
                cut = answer.getLogEndOffset();
                // the leader pinned the cut for this part: released when the part leaves it or
                // ends, also if the snapshot turns out to name another commit now
                ownPin.hold(target.snapshotId, cut);
                pinAnswer = answer;
                enter(PINNED);
                return;
            case REBUILDING:
                fold(target, event.foldCut);
                return;
            case COVERED:
                if (++replans < MAX_REPLANS) {
                    stats.replans++;
                    if (checkNotNull(event.covered).isBehind()) {
                        stats.replansBehind++;
                    }
                    covered = event.covered;
                    replan = Replan.COVERED;
                    enter(REPLANNING);
                    return;
                }
                readReadableByKey(event);
                return;
            case NOT_READY:
            case LEADER_LOST:
            case ERROR:
                readReadableByKey(event);
                return;
            case PARTITION_DROPPED:
                lakeOnly();
                return;
            case ROLLED_BACK:
            case INTERRUPTED:
                fail(event.failure);
                return;
            default:
                throw unexpected(event);
        }
    }

    /** The masks stay unavailable: reads the readable snapshot by key. */
    private void readReadableByKey(DvPartEvent event) {
        LOG.warn(
                "Masks of {} for snapshot {} are unavailable; reading the readable snapshot"
                        + " by key up to log offset {} instead.",
                part.describe(),
                target.snapshotId,
                part.getStoppingOffset(),
                event.failure);
        replan = Replan.READABLE_BY_KEY;
        enter(REPLANNING);
    }

    private void onReplanning(DvPartEvent event) {
        switch (event.kind) {
            case REPLANNED:
                target = checkNotNull(event.target);
                if (replan == Replan.READABLE_BY_KEY) {
                    fold(target, part.getStoppingOffset());
                } else {
                    enter(PLANNED);
                }
                return;
            case ROLLED_BACK:
            case INTERRUPTED:
            case ERROR:
                fail(event.failure);
                return;
            default:
                throw unexpected(event);
        }
    }

    private void onPinned(DvPartEvent event) {
        switch (event.kind) {
            case MASKS:
                // The caller keeps these masks to restore the part once it emitted rows.
                ready(
                        part.resolved(
                                        ReadMode.MASKED,
                                        target.snapshotId,
                                        target.snapshotLogOffset,
                                        target.lakeSplits,
                                        cut)
                                .withMaskState(checkNotNull(event.masks)));
                return;
            case COVERED:
            case REBUILDING:
            case NOT_READY:
            case LEADER_LOST:
                // The leader lost the cut before its masks were fetched: it evicted it, or a new
                // leader holds no such cut. Nothing of the part was read, so it pins again.
                String why = whyOf(event.failure);
                if (++repins <= MAX_REPINS) {
                    LOG.info(
                            "The leader of {} lost the cut {} pinned for it before its masks were"
                                    + " fetched ({}); pinning again ({}/{}).",
                            part.describe(),
                            cut,
                            why,
                            repins,
                            MAX_REPINS);
                    enter(PLANNED);
                    return;
                }
                // A part that reads its whole bucket reads it as of any cut, also by key.
                LOG.warn(
                        "The leader of {} keeps losing the cuts pinned for it before their masks"
                                + " are fetched ({}); reading it by key up to log offset {}.",
                        part.describe(),
                        why,
                        cut);
                fold(target, cut);
                return;
            case PARTITION_DROPPED:
                lakeOnly();
                return;
            case ROLLED_BACK:
            case INTERRUPTED:
            case ERROR:
                fail(event.failure);
                return;
            default:
                throw unexpected(event);
        }
    }

    /**
     * The masks of a cut pinned at planning: if the leader dropped the cut, a part that reads its
     * whole bucket and emitted nothing resolves afresh. A part that shares the cut derives its
     * masks from the keys, because other parts of the bucket read with that cut.
     */
    private void onPinnedAtPlanning(DvPartEvent event) {
        switch (event.kind) {
            case MASKS:
                ready(part.withMaskState(checkNotNull(event.masks)));
                return;
            case ROLLED_BACK:
            case INTERRUPTED:
                fail(event.failure);
                return;
            case COVERED:
            case REBUILDING:
            case NOT_READY:
            case LEADER_LOST:
            case PARTITION_DROPPED:
            case ERROR:
                String why = whyOf(event.failure);
                if (part.isStarted()) {
                    fail(
                            cannotResume(
                                    String.format("its leader no longer serves them (%s)", why),
                                    event.failure));
                    return;
                }
                if (!part.sharesBucket()) {
                    LOG.info(
                            "The cut pinned for {} is no longer served ({}); resolving it afresh.",
                            part.describe(),
                            why);
                    target = Target.of(part);
                    enter(PLANNED);
                    return;
                }
                LOG.warn(
                        "The cut pinned for {} up to log offset {} is no longer served ({});"
                                + " deriving its masks from the keys of its lake files and the"
                                + " log.",
                        part.describe(),
                        cut,
                        why);
                setFolding(part, part.getPinnedLogStartOffset(), cut, event.failure);
                enter(FOLDING);
                return;
            default:
                throw unexpected(event);
        }
    }

    private void onFolding(DvPartEvent event) {
        switch (event.kind) {
            case DERIVED:
                ready(checkNotNull(foldPart).withMaskState(checkNotNull(event.masks)));
                return;
            case ROLLED_BACK:
            case INTERRUPTED:
            case ERROR:
                Throwable derivation = checkNotNull(event.failure);
                if (lostCut == null) {
                    fail(derivation);
                    return;
                }
                derivation.addSuppressed(lostCut);
                fail(
                        new IllegalStateException(
                                String.format(
                                        "The cut pinned for bucket %s of %s, lake snapshot %d up to"
                                                + " log offset %d, is no longer served (%s), and %s"
                                                + " could not derive its masks. Other splits of the"
                                                + " bucket read with that cut, so it cannot read"
                                                + " with another one. Run the read again.",
                                        part.getTableBucket(),
                                        tablePath,
                                        part.getSnapshotId(),
                                        foldCut,
                                        whyOf(lostCut),
                                        part.describe()),
                                derivation));
                return;
            default:
                throw unexpected(event);
        }
    }

    private void onResuming(DvPartEvent event) {
        switch (event.kind) {
            case MASKS:
                // reads with the masks it kept, of the cut it was read with
                ready(part);
                return;
            case ROLLED_BACK:
            case INTERRUPTED:
            case ERROR:
                fail(event.failure);
                return;
            default:
                throw unexpected(event);
        }
    }

    private void onReading(DvPartEvent event) {
        DvBucketPart current = checkNotNull(resolved);
        switch (event.kind) {
            case POLL:
                if (state != READY) {
                    throw unexpected(event);
                }
                enter(READING_LAKE);
                return;
            case LAKE_END:
                if (state != READING_LAKE) {
                    throw unexpected(event);
                }
                boolean readsLog =
                        current.getReadMode() != ReadMode.LAKE_ONLY && current.readsLog();
                enter(readsLog ? READING_LOG : DONE);
                return;
            case LOG_END:
                if (state != READING_LOG) {
                    throw unexpected(event);
                }
                enter(DONE);
                return;
            case PARTITION_DROPPED:
                if (state != READING_LOG) {
                    throw unexpected(event);
                }
                partitionDropped();
                return;
            case ROLLED_BACK:
            case INTERRUPTED:
            case ERROR:
                fail(event.failure);
                return;
            default:
                throw unexpected(event);
        }
    }

    /**
     * Handles a partition dropped from Fluss while its log was to be read. Rows emitted so far were
     * masked by versions in that log, so the read cannot end without it and fails. So does the log
     * part of a bucket read in several parts: the other parts return lake rows masked by that log.
     * A part that reads its whole bucket and emitted nothing of it ends, as a read planned after
     * the drop would read nothing of it.
     */
    private void partitionDropped() {
        if (!returnedRows && !part.isStarted() && !part.sharesBucket()) {
            LOG.warn(
                    "The partition of {} was dropped before its log was read; the part ends"
                            + " without rows.",
                    part.describe());
            enter(DONE);
            return;
        }
        fail(
                new IllegalStateException(
                        String.format(
                                "Partition '%s' of %s was dropped while a union read was reading"
                                        + " the log of its bucket %d. The rows read of the bucket"
                                        + " were masked by changes in that log, which is gone, so"
                                        + " the read cannot complete exactly. Run the read again:"
                                        + " it reads the partition's lake data only.",
                                part.getPartitionName(),
                                tablePath,
                                part.getTableBucket().getBucket())));
    }

    /**
     * The part of a partition dropped after planning: the log is gone and nothing changes the lake
     * files any more, so they are read unmasked.
     */
    private void lakeOnly() {
        LOG.info("The partition of {} was dropped; reading its lake files only.", part.describe());
        ready(
                part.resolved(
                        ReadMode.LAKE_ONLY,
                        target.snapshotId,
                        target.snapshotLogOffset,
                        target.lakeSplits,
                        DvBucketPart.NO_MASK_CUT));
    }

    /**
     * Reads the bucket by key up to {@code cut}, as its leader has no masks to serve yet: the lake
     * snapshot as planned, correct alone with its own deletes, and the log from the snapshot's
     * readable offset folded to the latest change of each key. The masks are derived from the keys
     * of the lake files and the log; a restore reads with them, or derives them again.
     *
     * <p>Such a bucket is not checked against a rollback that reused the snapshot's id (Paimon), as
     * masks are: the leader holds no commit identity it could name. A rollback that reuses the id
     * between planning and the read goes unnoticed, and the bucket reads the files the id names
     * then with the log from the offset planned for the earlier commit.
     */
    private void fold(Target target, long cut) {
        LOG.info(
                "The leader of {} has no masks of snapshot {} to serve yet; reading it by key up to"
                        + " log offset {}.",
                part.describe(),
                target.snapshotId,
                cut);
        DvBucketPart folded =
                part.resolved(
                        ReadMode.FOLDED,
                        target.snapshotId,
                        target.snapshotLogOffset,
                        target.lakeSplits,
                        cut);
        setFolding(folded, folded.getSnapshotLogOffset(), folded.getMaskLogEndOffset(), null);
        enter(FOLDING);
    }

    private void setFolding(
            DvBucketPart foldPart, long logStart, long cut, @Nullable Throwable lostCut) {
        this.foldPart = foldPart;
        this.foldLogStart = logStart;
        this.foldCut = cut;
        this.lostCut = lostCut;
    }

    private void ready(DvBucketPart resolved) {
        this.resolved = resolved;
        DvBucketMaskState state = resolved.getMaskState();
        this.masks =
                state == null || resolved.getReadMode() == ReadMode.LAKE_ONLY
                        ? null
                        : DvBucketMasks.fromState(state);
        enter(READY);
    }

    private void fail(@Nullable Throwable failure) {
        this.failure = checkNotNull(failure);
        enter(FAILED);
    }

    /** Enters {@code next}, releasing the part's own pin unless {@code next} reads with it. */
    private void enter(DvPartState next) {
        state = next;
        history.add(next);
        boolean readsWithOwnPin =
                next == PINNED
                        || (next.isReading()
                                && resolved != null
                                && resolved.getReadMode() == ReadMode.MASKED);
        if (!readsWithOwnPin) {
            ownPin.release();
        }
    }

    private IllegalStateException unexpected(DvPartEvent event) {
        return new IllegalStateException(
                String.format(
                        "%s has no transition from %s on %s (states %s).",
                        part.describe(), state, event, history));
    }

    private IllegalStateException cannotResume(String why, @Nullable Throwable cause) {
        return new IllegalStateException(
                String.format(
                        "Cannot resume the union read of %s bucket %s exactly: rows were emitted"
                                + " with the deletion vector masks of lake snapshot %d up to log"
                                + " offset %d, but %s. Masks of another cut would emit rows"
                                + " twice or lose them; run the bounded read again without its"
                                + " state.",
                        tablePath,
                        part.getTableBucket(),
                        part.getSnapshotId(),
                        part.getMaskLogEndOffset(),
                        why),
                cause);
    }

    @Nullable
    private static String whyOf(@Nullable Throwable failure) {
        return failure == null
                ? null
                : ExceptionUtils.stripExecutionException(failure).getMessage();
    }

    // ------------------------------------------------------------------------------------------
    // What the states work on, for the actions of DvPartResolver and DvBucketPartReader.

    DvPartState state() {
        return state;
    }

    /** The states the part went through, from its initial one. */
    List<DvPartState> history() {
        return Collections.unmodifiableList(history);
    }

    DvBucketPart part() {
        return part;
    }

    Target target() {
        return target;
    }

    int replans() {
        return replans;
    }

    Replan replan() {
        return checkNotNull(replan);
    }

    StaleSnapshotException covered() {
        return checkNotNull(covered);
    }

    long cut() {
        return cut;
    }

    GetDvSnapshotResponse pinAnswer() {
        return checkNotNull(pinAnswer);
    }

    DvBucketPart foldPart() {
        return checkNotNull(foldPart);
    }

    long foldLogStart() {
        return foldLogStart;
    }

    long foldCut() {
        return foldCut;
    }

    /** Whether FOLDING reads the bucket by key, rather than deriving a lost planning cut. */
    boolean foldsByKey() {
        return lostCut == null;
    }

    @Nullable
    DvBucketPart resolved() {
        return resolved;
    }

    @Nullable
    DvBucketMasks masks() {
        return masks;
    }

    @Nullable
    Throwable failure() {
        return failure;
    }

    /** Records that the part's reader returned rows. */
    void returnedRows() {
        returnedRows = true;
    }
}
