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

package org.apache.fluss.flink.source.epochs;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.source.emitter.FlinkRecordEmitter;
import org.apache.fluss.flink.source.emitter.FlinkRecordEmitter.DeferredRecord;
import org.apache.fluss.flink.source.reader.RecordAndPos;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplit;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplitState;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.split.SourceSplitState;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Cuts the changes that a source reader emits into epochs, so that lookups of the same table can
 * read it as of an epoch.
 *
 * <p>All splits end an epoch together, each at its own position (see {@link Epochs}), never inside
 * a snapshot or between the two halves of an update. Every change carries its epoch as the record
 * timestamp and belongs to the epoch after the last watermark; watermark {@code e} is emitted only
 * once the ends of epoch {@code e} are durable in the {@link EpochStore}.
 *
 * <p>A restored split replays the ends recorded after its position, holding back its changes until
 * every split reached the end of the open epoch; only past those ends does the clock end epochs
 * again.
 */
public final class ReaderEpochs<OUT> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ReaderEpochs.class);

    /** No watermark yet, or no progress recorded. */
    private static final long NONE = Long.MIN_VALUE;

    private static final int RESTORE_PAGE = 256;
    private static final int MAX_RELEASES_PER_POLL = 1024;

    private final EpochSettings settings;
    private final EpochStore store;
    private final StartOffsets startOffsets;
    private final EpochClock epochClock;
    private final AutoCloseable resources;

    private final Map<String, SplitEpochs<OUT>> splits = new LinkedHashMap<>();
    private final Map<SourceSplitState, SplitEpochs<OUT>> byState = new IdentityHashMap<>();

    /** The last watermark the reader emitted, {@link #NONE} before the first one. */
    private long lastWatermark = NONE;

    private boolean idle;

    /**
     * Whether every split changes in the open epoch and holds no records, so that only the clock
     * ends epochs; until a split is added.
     */
    private boolean live;

    /** Whether the reader recorded the end of its input. */
    private boolean ended;

    /** The ends being recorded, after which {@link #afterWrite} emits their watermark. */
    private @Nullable CompletableFuture<Void> pendingWrite;

    private @Nullable Runnable afterWrite;

    private @Nullable ReaderOutput<OUT> output;

    private final StampingOutput<OUT> stampingOutput = new StampingOutput<>();

    private @Nullable CompletableFuture<Void> tick;
    private long tickEpoch = NONE;

    public ReaderEpochs(
            EpochSettings settings,
            EpochStore store,
            StartOffsets startOffsets,
            LongSupplier clock,
            AutoCloseable resources) {
        this(settings, store, startOffsets, EpochClock.wall(settings, clock), resources);
    }

    public ReaderEpochs(
            EpochSettings settings,
            EpochStore store,
            StartOffsets startOffsets,
            EpochClock epochClock,
            AutoCloseable resources) {
        this.settings = settings;
        this.store = store;
        this.startOffsets = startOffsets;
        this.epochClock = epochClock;
        this.resources = resources;
    }

    /** Opens the epochs of a reader of the table, recorded in the {@link EpochRegistry}. */
    public static <OUT> ReaderEpochs<OUT> open(
            Configuration flussConf, TablePath tablePath, EpochSettings settings) throws Exception {
        return open(flussConf, tablePath, settings, null);
    }

    /**
     * As {@link #open(Configuration, TablePath, EpochSettings)}; with epochs that the coordinators
     * open (see {@link EpochSettings#getCloseAfterMs}), {@code changeInEpoch} tells the coordinator
     * of the first change of the open epoch.
     */
    public static <OUT> ReaderEpochs<OUT> open(
            Configuration flussConf,
            TablePath tablePath,
            EpochSettings settings,
            @Nullable LongConsumer changeInEpoch)
            throws Exception {
        Connection connection = ConnectionFactory.createConnection(flussConf);
        try {
            EpochRegistry registry =
                    EpochRegistry.open(
                            connection, false, tablePath.getDatabaseName(), settings.getDomain());
            Admin admin = connection.getAdmin();
            StartOffsets earliest =
                    bucket ->
                            admin.listOffsets(
                                            tablePath,
                                            Collections.singleton(bucket.getBucket()),
                                            new OffsetSpec.EarliestSpec())
                                    .bucketResult(bucket.getBucket())
                                    .get();
            EpochClock epochClock;
            if (settings.getCloseAfterMs() > 0) {
                checkArgument(
                        changeInEpoch != null,
                        "Epochs that the coordinators open need a way to tell them of changes.");
                epochClock = EpochClock.coordinated(changeInEpoch);
            } else {
                epochClock = EpochClock.wall(settings, System::currentTimeMillis);
            }
            return new ReaderEpochs<>(settings, registry, earliest, epochClock, connection);
        } catch (Exception e) {
            connection.close();
            throw e;
        }
    }

    /** Resolves the earliest offset of a bucket, where a scan from the earliest offset starts. */
    @FunctionalInterface
    public interface StartOffsets {
        long earliest(TableBucket bucket) throws Exception;
    }

    // ------------------------------------------------------------------------------------------
    // Splits
    // ------------------------------------------------------------------------------------------

    /**
     * Prepares splits before the reader adds them: resolves where they start, and loads the ends
     * recorded for their buckets, or records where a bucket that has none starts. Returns the
     * splits to add, which start at a concrete offset.
     */
    public List<SourceSplitBase> addingSplits(List<SourceSplitBase> added) throws Exception {
        List<SourceSplitBase> resolved = new ArrayList<>(added.size());
        for (SourceSplitBase split : added) {
            SourceSplitBase start = resolveStart(split);
            TableBucket bucket = start.getTableBucket();
            long position = initialPosition(start);
            SplitEpochs<OUT> epochs = new SplitEpochs<>(start.splitId(), bucket);
            Long endedThrough = store.endedThrough(bucket).get();
            if (endedThrough == null) {
                // the start must be durable before any progress, which tells lookups it exists
                store.write(
                                Collections.singletonList(
                                        EpochRecord.end(bucket, Epochs.BEFORE_FIRST, position)))
                        .get();
                if (lastWatermark != NONE) {
                    // the epochs that the reader ended already did not change the bucket
                    store.write(
                                    Collections.singletonList(
                                            EpochRecord.progress(bucket, lastWatermark)))
                            .get();
                    epochs.recordedProgress = lastWatermark;
                }
                epochs.lastRecordedEnd = position;
            } else {
                loadEnds(epochs, endedThrough, position);
                EpochEnd next = epochs.pending.peekFirst();
                if (lastWatermark != NONE && next != null && next.getEpoch() <= lastWatermark) {
                    throw new IllegalStateException(
                            String.format(
                                    "Split %s at position %s comes too late to replay its epochs:"
                                            + " %s, but the reader ended epoch %s already.",
                                    start.splitId(),
                                    Epochs.positionToString(position),
                                    next,
                                    lastWatermark));
                }
            }
            LOG.info(
                    "Split {} at position {} cuts epochs of domain '{}': {} ended through {},"
                            + " last end {}, ends to replay {}.",
                    start.splitId(),
                    Epochs.positionToString(position),
                    settings.getDomain(),
                    bucket,
                    epochs.recordedProgress == NONE ? "none" : epochs.recordedProgress,
                    Epochs.positionToString(epochs.lastRecordedEnd),
                    epochs.pending);
            splits.put(start.splitId(), epochs);
            resolved.add(start);
            live = false;
        }
        return resolved;
    }

    /** Binds the state that the reader keeps for a split it added. */
    public void track(SourceSplitBase split, SourceSplitState state) {
        SplitEpochs<OUT> epochs = splits.get(split.splitId());
        if (epochs == null) {
            throw new IllegalStateException(
                    "The epochs of split " + split.splitId() + " were not prepared.");
        }
        if (epochs.state != null) {
            byState.remove(epochs.state);
        }
        epochs.state = state;
        byState.put(state, epochs);
    }

    /** The splits finished; their buckets keep their last position in the epochs to come. */
    public void splitsFinished(Collection<String> splitIds) {
        for (String splitId : splitIds) {
            SplitEpochs<OUT> epochs = splits.get(splitId);
            if (epochs != null) {
                epochs.finished = true;
            }
        }
    }

    /** The finished splits that hold back records, which a checkpoint must keep. */
    public List<SourceSplitBase> finishedSplitsHoldingRecords() {
        List<SourceSplitBase> holding = new ArrayList<>();
        for (SplitEpochs<OUT> epochs : splits.values()) {
            if (epochs.finished && !epochs.held.isEmpty() && epochs.state != null) {
                holding.add(epochs.state.toSourceSplit());
            }
        }
        return holding;
    }

    private SourceSplitBase resolveStart(SourceSplitBase split) throws Exception {
        if (split.isLogSplit()) {
            LogSplit log = split.asLogSplit();
            if (log.getStartingOffset() == LogScanner.EARLIEST_OFFSET) {
                long earliest = startOffsets.earliest(log.getTableBucket());
                return new LogSplit(
                        log.getTableBucket(),
                        log.getPartitionName(),
                        earliest,
                        log.getStoppingOffset().orElse(LogSplit.NO_STOPPING_OFFSET));
            } else if (log.getStartingOffset() < 0) {
                throw new UnsupportedOperationException(
                        "A scan that cuts epochs needs a concrete starting offset, but split "
                                + split
                                + " has none.");
            }
            return split;
        } else if (split.isHybridSnapshotLogSplit()
                && !split.asHybridSnapshotLogSplit().isBatch()) {
            return split;
        }
        throw new UnsupportedOperationException(
                "A scan that cuts epochs reads only log splits and snapshot splits followed by"
                        + " their log, not "
                        + split);
    }

    private static long initialPosition(SourceSplitBase split) {
        if (split.isLogSplit()) {
            return split.asLogSplit().getStartingOffset();
        }
        HybridSnapshotLogSplit hybrid = split.asHybridSnapshotLogSplit();
        if (hybrid.isSnapshotFinished()) {
            return hybrid.getLogStartingOffset();
        }
        return hybrid.recordsToSkip() > 0 ? Epochs.IN_SNAPSHOT : Epochs.EMPTY;
    }

    /**
     * Loads the ends recorded in the bucket after the position, which a restored split replays, and
     * the last end at or before the position.
     */
    private void loadEnds(SplitEpochs<OUT> epochs, long endedThrough, long position)
            throws Exception {
        epochs.recordedProgress = endedThrough;
        EpochEnd before = null;
        long bound = endedThrough;
        while (before == null) {
            List<EpochEnd> page = store.latestEnds(epochs.bucket, bound, RESTORE_PAGE).get();
            for (EpochEnd end : page) {
                if (Epochs.rank(end.getEnd()) > Epochs.rank(position)) {
                    // the ends come latest first
                    epochs.pending.addFirst(end);
                } else {
                    before = end;
                    break;
                }
            }
            if (before == null) {
                if (page.size() < RESTORE_PAGE) {
                    throw new IllegalStateException(
                            String.format(
                                    "The epochs of %s in domain '%s' record no start at or before"
                                            + " position %s.",
                                    epochs.bucket,
                                    settings.getDomain(),
                                    Epochs.positionToString(position)));
                }
                bound = page.get(page.size() - 1).getEpoch() - 1;
            }
        }
        epochs.lastRecordedEnd = before.getEnd();
    }

    // ------------------------------------------------------------------------------------------
    // Records
    // ------------------------------------------------------------------------------------------

    /**
     * Wraps the emitter of the reader: a record that belongs to an epoch after the open one waits
     * until the reader ended the epochs before it.
     */
    public RecordEmitter<RecordAndPos, OUT, SourceSplitState> wrap(
            FlinkRecordEmitter<OUT> emitter) {
        return (record, sourceOutput, splitState) -> {
            SplitEpochs<OUT> epochs = byState.get(splitState);
            if (epochs == null) {
                throw new IllegalStateException(
                        "The epochs do not track the split of a record: "
                                + splitState.toSourceSplit());
            }
            long rank = rankOf(record);
            boolean updateBefore = isUpdateBefore(record);
            if (epochs.held.isEmpty() && mayEmit(epochs, rank)) {
                emitter.emitRecord(record, stamped(sourceOutput), splitState);
                epochClock.noteChange();
                epochs.midUpdate = updateBefore;
                advance(epochs);
            } else {
                epochs.held.addLast(
                        new Held<>(
                                rank,
                                emitter.defer(record, splitState),
                                sourceOutput,
                                updateBefore));
            }
        };
    }

    /**
     * The output of a split, with every record stamped with the open epoch: the epoch of a change
     * is the timestamp of its record. Reused, as the reader emits from one thread.
     */
    private SourceOutput<OUT> stamped(SourceOutput<OUT> output) {
        stampingOutput.target = output;
        stampingOutput.epoch = lastWatermark + 1;
        return stampingOutput;
    }

    /** The rank of the position that a record starts at, see {@link Epochs#rank}. */
    private static long rankOf(RecordAndPos record) {
        if (record.isSnapshotPhaseFinished()) {
            return Epochs.rank(Epochs.IN_SNAPSHOT);
        }
        long offset = record.record().logOffset();
        if (offset < 0) {
            // a row of a snapshot
            return Epochs.rank(Epochs.IN_SNAPSHOT);
        }
        // a skipped update pair starts at its update-before
        return Epochs.rank(record.skipEmission() ? offset - 1 : offset);
    }

    private static boolean isUpdateBefore(RecordAndPos record) {
        if (record.isSnapshotPhaseFinished() || record.skipEmission()) {
            return false;
        }
        ScanRecord scanRecord = record.record();
        return scanRecord.logOffset() >= 0
                && scanRecord.getChangeType() == ChangeType.UPDATE_BEFORE;
    }

    /** Whether a record of the split that starts at the position may be emitted now. */
    private boolean mayEmit(SplitEpochs<OUT> epochs, long rank) {
        if (lastWatermark == NONE) {
            return false;
        }
        EpochEnd next = epochs.pending.peekFirst();
        if (next != null) {
            return next.getEpoch() == lastWatermark + 1 && rank < Epochs.rank(next.getEnd());
        }
        return isFree(epochs);
    }

    /** Whether the split changes in the open epoch: it recorded no end after it. */
    private boolean isFree(SplitEpochs<OUT> epochs) {
        return epochs.pending.isEmpty()
                && (epochs.recordedProgress == NONE
                        || (lastWatermark != NONE && epochs.recordedProgress <= lastWatermark));
    }

    /** Moves past the recorded end of the open epoch once the split reached it. */
    private void advance(SplitEpochs<OUT> epochs) {
        while (lastWatermark != NONE) {
            EpochEnd next = epochs.pending.peekFirst();
            if (next == null
                    || next.getEpoch() != lastWatermark + 1
                    || !reached(epochs, next.getEnd())) {
                return;
            }
            epochs.pending.pollFirst();
            epochs.lastRecordedEnd = next.getEnd();
        }
    }

    private boolean reached(SplitEpochs<OUT> epochs, long end) {
        if (epochs.state == null) {
            return false;
        }
        long rank = Epochs.rank(end);
        if (Epochs.rank(position(epochs)) >= rank) {
            return true;
        }
        // no record of the split lies between its position and the record it holds back
        Held<OUT> first = epochs.held.peekFirst();
        return first != null && first.rank >= rank;
    }

    private static long position(SplitEpochs<?> epochs) {
        SourceSplitState state = epochs.state;
        if (state.isLogSplitState()) {
            return state.asLogSplitState().getNextOffset();
        }
        HybridSnapshotLogSplitState hybrid = state.asHybridSnapshotLogSplitState();
        if (hybrid.isSnapshotFinished()) {
            return hybrid.getNextOffset();
        }
        return hybrid.getRecordsToSkip() > 0 ? Epochs.IN_SNAPSHOT : Epochs.EMPTY;
    }

    /** Emits held records that may be emitted now, at most the given number; returns how many. */
    private int release(int max) {
        int released = 0;
        for (SplitEpochs<OUT> epochs : splits.values()) {
            while (released < max && !epochs.held.isEmpty()) {
                Held<OUT> first = epochs.held.peekFirst();
                if (!mayEmit(epochs, first.rank)) {
                    break;
                }
                epochs.held.pollFirst();
                first.record.emit(stamped(first.output), epochs.state);
                epochClock.noteChange();
                epochs.midUpdate = first.updateBefore;
                advance(epochs);
                released++;
            }
        }
        return released;
    }

    // ------------------------------------------------------------------------------------------
    // Polls
    // ------------------------------------------------------------------------------------------

    /**
     * Called before the reader polls a record: completes a recorded end, releases held records and
     * ends epochs. Returns the status of the poll if it did something, or null to poll a record.
     */
    @Nullable
    public InputStatus beforePoll(ReaderOutput<OUT> readerOutput) {
        this.output = readerOutput;
        if (pendingWrite != null) {
            if (!pendingWrite.isDone()) {
                return InputStatus.NOTHING_AVAILABLE;
            }
            CompletableFuture<Void> written = pendingWrite;
            Runnable then = afterWrite;
            pendingWrite = null;
            afterWrite = null;
            try {
                written.join();
            } catch (CompletionException e) {
                throw new FlinkRuntimeException(
                        "Failed to record the end of epochs of domain '"
                                + settings.getDomain()
                                + "'.",
                        e.getCause());
            }
            then.run();
            return ended ? InputStatus.END_OF_INPUT : InputStatus.MORE_AVAILABLE;
        }
        if (ended) {
            return InputStatus.END_OF_INPUT;
        }
        if (splits.isEmpty()) {
            if (lastWatermark == NONE && !idle) {
                // without splits the reader holds back no epochs
                readerOutput.markIdle();
                idle = true;
            }
            return null;
        }
        if (live && epochClock.current() - 1 <= lastWatermark) {
            return null;
        }
        for (SplitEpochs<OUT> epochs : splits.values()) {
            advance(epochs);
        }
        if (release(MAX_RELEASES_PER_POLL) > 0) {
            return InputStatus.MORE_AVAILABLE;
        }
        return endEpochs();
    }

    /**
     * Ends the epochs up to the next watermark if the splits allow it: the last epoch before a
     * recorded end that a split waits for, or else the epoch before the current one of the clock.
     */
    @Nullable
    private InputStatus endEpochs() {
        long target = Long.MAX_VALUE;
        for (SplitEpochs<OUT> epochs : splits.values()) {
            EpochEnd next = epochs.pending.peekFirst();
            if (next != null) {
                if (lastWatermark != NONE && next.getEpoch() == lastWatermark + 1) {
                    // the split did not reach the end of the open epoch yet
                    return null;
                }
                target = Math.min(target, next.getEpoch() - 1);
            } else if (epochs.recordedProgress != NONE
                    && (lastWatermark == NONE || epochs.recordedProgress > lastWatermark)) {
                // the split changes only after the epochs it ended before
                target = Math.min(target, epochs.recordedProgress);
            }
        }
        long watermark;
        if (target != Long.MAX_VALUE) {
            watermark = target;
        } else if (lastWatermark == NONE) {
            watermark = Epochs.BEFORE_FIRST;
        } else {
            live = splits.values().stream().allMatch(epochs -> epochs.held.isEmpty());
            long current = epochClock.current();
            if (current - 1 <= lastWatermark) {
                return null;
            }
            watermark = current - 1;
        }

        List<EpochRecord> records = new ArrayList<>();
        List<Runnable> effects = new ArrayList<>();
        for (SplitEpochs<OUT> epochs : splits.values()) {
            if (!isFree(epochs)) {
                // its ends up to the watermark are recorded already
                continue;
            }
            if (lastWatermark != NONE) {
                long position = position(epochs);
                if (position != epochs.lastRecordedEnd) {
                    if (position == Epochs.IN_SNAPSHOT || epochs.midUpdate) {
                        // no epoch ends inside a snapshot or an update
                        return null;
                    }
                    records.add(EpochRecord.end(epochs.bucket, lastWatermark + 1, position));
                    effects.add(() -> epochs.lastRecordedEnd = position);
                }
            }
            if (watermark > epochs.recordedProgress) {
                records.add(EpochRecord.progress(epochs.bucket, watermark));
                effects.add(() -> epochs.recordedProgress = watermark);
            }
        }
        Runnable emit =
                () -> {
                    effects.forEach(Runnable::run);
                    emitWatermark(watermark);
                };
        if (records.isEmpty()) {
            emit.run();
            return InputStatus.MORE_AVAILABLE;
        }
        LOG.debug("Recording {} before watermark {}.", records, watermark);
        pendingWrite = store.write(records);
        afterWrite = emit;
        return InputStatus.NOTHING_AVAILABLE;
    }

    private void emitWatermark(long watermark) {
        LOG.debug("Emitting watermark {} of domain '{}'.", watermark, settings.getDomain());
        output.emitWatermark(new Watermark(watermark));
        lastWatermark = watermark;
        idle = false;
    }

    /**
     * Called with the status of a poll: at the end of the input, the reader records the end of the
     * last epoch and that every epoch ended.
     */
    public InputStatus afterPoll(InputStatus status) {
        if (status != InputStatus.END_OF_INPUT || ended || pendingWrite != null) {
            return status;
        }
        if (lastWatermark == NONE) {
            ended = true;
            return status;
        }
        List<EpochRecord> records = new ArrayList<>();
        List<Runnable> effects = new ArrayList<>();
        for (SplitEpochs<OUT> epochs : splits.values()) {
            if (!epochs.held.isEmpty() || !epochs.pending.isEmpty()) {
                throw new IllegalStateException(
                        String.format(
                                "The input of split %s ended before the ends of epochs it"
                                        + " recorded: %s.",
                                epochs.splitId, epochs.pending));
            }
            long position = position(epochs);
            if (position != epochs.lastRecordedEnd) {
                if (position == Epochs.IN_SNAPSHOT || epochs.midUpdate) {
                    throw new IllegalStateException(
                            "The input of split " + epochs.splitId + " ended inside a change.");
                }
                records.add(EpochRecord.end(epochs.bucket, lastWatermark + 1, position));
                effects.add(() -> epochs.lastRecordedEnd = position);
            }
            records.add(EpochRecord.progress(epochs.bucket, Epochs.AFTER_LAST));
            effects.add(() -> epochs.recordedProgress = Epochs.AFTER_LAST);
        }
        pendingWrite = store.write(records);
        afterWrite =
                () -> {
                    effects.forEach(Runnable::run);
                    ended = true;
                };
        return InputStatus.NOTHING_AVAILABLE;
    }

    /**
     * When the reader becomes available again: once the ends are recorded, a record arrived, or the
     * clock passed into the next epoch.
     */
    public CompletableFuture<Void> availability(CompletableFuture<Void> records) {
        if (pendingWrite != null) {
            return pendingWrite.handle((ignored, error) -> null);
        }
        if (ended || splits.isEmpty()) {
            return records;
        }
        if (lastWatermark == NONE) {
            // the first watermark is due
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.anyOf(records, nextTick()).thenApply(ignored -> null);
    }

    private CompletableFuture<Void> nextTick() {
        long epoch = epochClock.current();
        if (tick == null || tickEpoch != epoch) {
            tickEpoch = epoch;
            tick = epochClock.advancedPast(epoch);
        }
        return tick;
    }

    /** The coordinator opened an epoch, see {@link EpochClock#coordinated}. */
    public void epochOpened(long epoch) {
        if (epochClock instanceof EpochClock.Coordinated) {
            ((EpochClock.Coordinated) epochClock).opened(epoch);
        }
    }

    /** The last watermark the reader emitted, or {@link Long#MIN_VALUE} before the first. */
    public long getLastWatermark() {
        return lastWatermark;
    }

    @Override
    public void close() throws Exception {
        try {
            store.close();
        } finally {
            resources.close();
        }
    }

    // ------------------------------------------------------------------------------------------

    /** The epochs of one split. */
    private static final class SplitEpochs<OUT> {
        final String splitId;
        final TableBucket bucket;

        /** The state of the split in the reader, bound once the reader added it. */
        @Nullable SourceSplitState state;

        /** The last epoch recorded as ended in the bucket, or {@link #NONE}. */
        long recordedProgress = NONE;

        /** The position at which the last recorded end with a change of the bucket ended. */
        long lastRecordedEnd;

        /** The recorded ends after the position of a restored split, first ones first. */
        final ArrayDeque<EpochEnd> pending = new ArrayDeque<>();

        /** Records that wait for the reader to end the epochs before theirs. */
        final ArrayDeque<Held<OUT>> held = new ArrayDeque<>();

        /** Whether the last emitted record is the update-before of an update. */
        boolean midUpdate;

        boolean finished;

        SplitEpochs(String splitId, TableBucket bucket) {
            this.splitId = splitId;
            this.bucket = bucket;
        }
    }

    /** Emits records with the epoch as their timestamp, whatever timestamp they come with. */
    private static final class StampingOutput<OUT> implements SourceOutput<OUT> {
        @Nullable SourceOutput<OUT> target;
        long epoch;

        @Override
        public void collect(OUT record) {
            target.collect(record, epoch);
        }

        @Override
        public void collect(OUT record, long timestamp) {
            target.collect(record, epoch);
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            target.emitWatermark(watermark);
        }

        @Override
        public void markIdle() {
            target.markIdle();
        }

        @Override
        public void markActive() {
            target.markActive();
        }
    }

    /** A record held back, prepared for its emission. */
    private static final class Held<OUT> {
        final long rank;
        final DeferredRecord<OUT> record;
        final SourceOutput<OUT> output;
        final boolean updateBefore;

        Held(
                long rank,
                DeferredRecord<OUT> record,
                SourceOutput<OUT> output,
                boolean updateBefore) {
            this.rank = rank;
            this.record = record;
            this.output = output;
            this.updateBefore = updateBefore;
        }
    }
}
