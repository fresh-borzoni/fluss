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

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.batch.IndexedRowIterator;
import org.apache.fluss.client.table.scanner.batch.LogRowIterator;
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketPart.ReadMode;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.exception.LakeTableRolledBackException;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.RowWithPosResult;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.messages.GetDvSnapshotRequest;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.ExceptionUtils;

import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Reads a {@link DvBucketPart}: its lake files with the rows the log changed masked, then the log
 * up to the masks' cut without superseded versions, deletes and before-images.
 *
 * <p>The masks are fetched from the bucket's leader when the part starts ({@link #resolve()}). If
 * the leader covers another lake snapshot than the planned one, the bucket is planned again and
 * read on that snapshot. If the masks stay unavailable, a lake that reads rows in key order is
 * merged with the log by key, as without deletion vectors. A dropped partition has only its lake
 * files read.
 *
 * <p>A part restored after it emitted rows needs the masks of the cut it was read with. A part that
 * kept them reads with them. One without them asks the server for that cut and refuses masks of
 * another cut, which would emit rows twice or lose them.
 *
 * <p>The reader starts at lake split {@link DvBucketPart#getCurrentLakeSplitIndex()} and at log
 * offset {@link DvBucketPart#getNextLogOffset()}. Lake rows come in {@link IndexedRowIterator}s and
 * log rows in {@link LogRowIterator}s, so a caller that restores reads can track its position. The
 * caller skips the rows of a lake split it read before.
 */
@PublicEvolving
public class DvBucketPartReader implements BatchScanner {

    private static final Logger LOG = LoggerFactory.getLogger(DvBucketPartReader.class);

    // The most re-plans on another snapshot than the planned one, when leaders move to newer ones.
    static final int MAX_REPLANS = 3;
    static final long REPLAN_BACKOFF_MS = 400;
    /** The most bytes of serialized log mask one paged response carries. */
    static final int MAX_LOG_DV_BYTES = 1 << 20;

    private static final Duration KEY_POLL_TIMEOUT = Duration.ofMillis(500);
    // Polls without progress after which deriving masks gives up on the log.
    static final int MAX_IDLE_KEY_POLLS = 600;

    /** What the reader needs of the engine beyond the part; see {@link DvTableReadContext}. */
    public interface Context {

        TablePath tablePath();

        DvMaskFetcher maskFetcher();

        /** The lake source, projected as the read emits rows. */
        LakeSource<LakeSplit> lakeSource();

        /** The lake splits of a bucket in a lake snapshot, null if the lake has none of it. */
        @Nullable
        List<LakeSplit> planLakeSplits(
                long snapshotId, TableBucket bucket, @Nullable String partitionName)
                throws IOException;

        /** The lake snapshot with the bucket's readable offsets; the coordinator's if null. */
        LakeSnapshot lakeSnapshot(@Nullable Long snapshotId) throws Exception;

        /** Whether the lake reads rows in key order, which merging with the log needs. */
        boolean lakeReadsInKeyOrder();

        /** What the lake commit of {@code snapshotId} is now, see LakeSource#snapshotIdentity. */
        @Nullable
        String snapshotIdentity(long snapshotId) throws IOException;

        LogScanner createLogScanner();

        /** A scanner merging the part's lake files with its log by key. */
        BatchScanner createMergeScanner(DvBucketPart part);

        /** Reads the primary keys of the bucket, to derive masks without the server. */
        KeyReader keyReader();

        /** Whether the partition was dropped from Fluss. */
        boolean isPartitionDropped(long partitionId);

        void sleep(long millis) throws InterruptedException;
    }

    /** Reads primary keys of lake rows and log rows, encoded alike. */
    public interface KeyReader {

        /** Reads the keys of a lake split's live rows, with their positions in its files. */
        CloseableIterator<RowWithPosResult> readLakeKeys(LakeSplit lakeSplit) throws IOException;

        /** A scanner of the log that reads the primary keys only. */
        LogScanner createLogScanner();

        /** Encodes the key of a row read by this reader; the bytes are not reused. */
        byte[] encode(InternalRow keyRow);
    }

    private final DvBucketPart part;
    private final TableBucket bucket;
    private final Context context;

    @Nullable private DvBucketPart resolved;
    @Nullable private DvBucketMasks masks;
    @Nullable private BatchScanner mergeScanner;

    private int nextLakeSplitIndex;
    @Nullable private CloseableIterator<InternalRow> currentLakeRows;
    private boolean lakeFinished;

    @Nullable private LogScanner logScanner;
    private boolean logFinished;
    // Once a row is returned, a dropped partition fails the read instead of ending it.
    private boolean returnedRows;

    public DvBucketPartReader(DvBucketPart part, Context context) {
        this.part = part;
        this.bucket = part.getTableBucket();
        this.context = context;
    }

    /**
     * Resolves how the part is read and fetches the bucket's masks if it is read with them. Returns
     * the resolved part with its cut and the masks a caller keeps to restore the part once it
     * emitted rows.
     */
    public DvBucketPart resolve() throws IOException {
        if (resolved != null) {
            return resolved;
        }
        try {
            if (part.isPinned() && part.getReadMode() == ReadMode.MASKED) {
                resolved = resolvePinned();
            } else if (part.isStarted() && part.getReadMode() != ReadMode.PENDING) {
                resolved = resume();
            } else {
                resolved = resolveFresh();
            }
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while fetching the masks of " + part.describe(), e);
        } catch (Exception e) {
            throw new IOException("Failed to fetch the masks of " + part.describe(), e);
        }
        nextLakeSplitIndex = resolved.getCurrentLakeSplitIndex();
        if (resolved.getReadMode() == ReadMode.MERGED) {
            mergeScanner = context.createMergeScanner(resolved);
        }
        LOG.info(
                "Reading {} as {} (snapshot {}, {} lake splits, masks up to log offset {}).",
                resolved.describe(),
                resolved.getReadMode(),
                resolved.getSnapshotId(),
                resolved.lakeSplitCount(),
                resolved.getMaskLogEndOffset());
        return resolved;
    }

    private DvBucketPart resolveFresh() throws Exception {
        long snapshotId = part.getSnapshotId();
        long snapshotLogOffset = part.getSnapshotLogOffset();
        List<LakeSplit> lakeSplits = part.getLakeSplits();
        int replans = 0;
        while (true) {
            List<String> firstFiles = fileNamesOf(lakeSplits, 0);
            GetDvSnapshotResponse first;
            try {
                long requested = snapshotId;
                first = context.maskFetcher().fetch(() -> request(requested, null, firstFiles));
            } catch (Exception e) {
                if (ExceptionUtils.findThrowable(e, LakeTableRolledBackException.class)
                        .isPresent()) {
                    throw e;
                }
                if (ExceptionUtils.findThrowable(e, PartitionNotExistException.class).isPresent()) {
                    // Dropped after planning: the log is gone and nothing changes the lake
                    // files, so they are read unmasked.
                    LOG.info(
                            "The partition of {} was dropped; reading its lake files only.",
                            part.describe());
                    return part.resolved(
                            ReadMode.LAKE_ONLY,
                            snapshotId,
                            snapshotLogOffset,
                            lakeSplits,
                            DvBucketPart.NO_MASK_CUT);
                }
                StaleSnapshotException stale = DvMaskFetcher.staleOnAnotherSnapshot(e, snapshotId);
                if (DvMaskFetcher.isTransientlyUnavailable(e) && ++replans < MAX_REPLANS) {
                    // The leader covers another snapshot, e.g. one tiering registered after
                    // planning. Lake snapshot ids are not ordered (Iceberg's are random), so read
                    // the leader's snapshot if its id is positive, else ask the coordinator for
                    // its readable snapshot again.
                    Long target =
                            stale != null && stale.getCurrentSnapshotId() > 0
                                    ? Long.valueOf(stale.getCurrentSnapshotId())
                                    : null;
                    LakeSnapshot lakeSnapshot = lakeSnapshotOrEmpty(target);
                    long targetId = lakeSnapshot == null ? -1L : lakeSnapshot.getSnapshotId();
                    if (targetId != snapshotId) {
                        snapshotLogOffset = readableOffset(lakeSnapshot);
                        lakeSplits = planBucket(targetId, snapshotLogOffset);
                        LOG.info(
                                "The leader of {} does not serve the masks of snapshot {};"
                                        + " reading snapshot {} instead ({}/{}).",
                                part.describe(),
                                snapshotId,
                                targetId,
                                replans,
                                MAX_REPLANS);
                        snapshotId = targetId;
                    }
                    context.sleep(REPLAN_BACKOFF_MS);
                    continue;
                }
                if (!context.lakeReadsInKeyOrder()) {
                    throw e;
                }
                LOG.warn(
                        "Masks of {} for snapshot {} are unavailable; merging the readable"
                                + " snapshot with the log by key instead.",
                        part.describe(),
                        snapshotId,
                        e);
                return mergeResolution(snapshotId, snapshotLogOffset, lakeSplits);
            }
            checkSnapshotIdentity(snapshotId, first);
            long cut = first.getLogEndOffset();
            DvBucketMaskState state;
            if (first.hasPaged() && first.isPaged()) {
                // Fetch all masks of the cut now: the leader may drop it while the part is read.
                state =
                        fetchCut(
                                snapshotId,
                                cut,
                                lakeSplits,
                                first.getSnapshotStartOffset(),
                                part.readsLog());
            } else {
                state = DvBucketMasks.stateOf(first);
            }
            masks = DvBucketMasks.fromState(state);
            // The caller keeps these masks to restore the part once it emitted rows.
            return part.resolved(ReadMode.MASKED, snapshotId, snapshotLogOffset, lakeSplits, cut)
                    .withMaskState(state);
        }
    }

    /** The coordinator's readable snapshot, read merged with the log by key. */
    private DvBucketPart mergeResolution(
            long snapshotId, long snapshotLogOffset, @Nullable List<LakeSplit> lakeSplits)
            throws Exception {
        LakeSnapshot readable = lakeSnapshotOrEmpty(null);
        long readableId = readable == null ? -1L : readable.getSnapshotId();
        if (readableId != snapshotId) {
            snapshotLogOffset = readableOffset(readable);
            lakeSplits = planBucket(readableId, snapshotLogOffset);
            snapshotId = readableId;
        }
        return part.resolved(
                ReadMode.MERGED,
                snapshotId,
                snapshotLogOffset,
                lakeSplits,
                DvBucketPart.NO_MASK_CUT);
    }

    /**
     * Fetches the masks of a part whose cut was pinned at planning: those of its own lake files,
     * and the log mask in pages. If the leader dropped the cut, a part that reads its whole bucket
     * and emitted nothing resolves afresh. A part that shares the cut derives its masks from the
     * keys, because other parts of the bucket read with that cut.
     */
    private DvBucketPart resolvePinned() throws Exception {
        long snapshotId = part.getSnapshotId();
        long cut = part.getMaskLogEndOffset();
        DvBucketMaskState stored = part.getMaskState();
        if (stored != null) {
            return fromKeptMasks(stored);
        }
        try {
            DvBucketMaskState fetched =
                    fetchCut(
                            snapshotId,
                            cut,
                            part.getLakeSplits(),
                            part.getPinnedLogStartOffset(),
                            part.readsLog());
            masks = DvBucketMasks.fromState(fetched);
            return part.withMaskState(fetched);
        } catch (Exception e) {
            if (ExceptionUtils.findThrowable(e, LakeTableRolledBackException.class).isPresent()) {
                throw e;
            }
            if (part.isStarted()) {
                throw cannotResume(
                        String.format(
                                "its leader no longer serves them (%s)",
                                ExceptionUtils.stripExecutionException(e).getMessage()),
                        e);
            }
            String why = ExceptionUtils.stripExecutionException(e).getMessage();
            if (!part.sharesBucket()) {
                LOG.info(
                        "The cut pinned for {} is no longer served ({}); resolving it afresh.",
                        part.describe(),
                        why);
                return resolveFresh();
            }
            LOG.warn(
                    "The cut pinned for {} up to log offset {} is no longer served ({}); deriving"
                            + " its masks from the keys of its lake files and the log.",
                    part.describe(),
                    cut,
                    why);
            DvBucketMaskState derived;
            try {
                derived = deriveMasks(cut);
            } catch (Exception derivation) {
                derivation.addSuppressed(e);
                throw new IllegalStateException(
                        String.format(
                                "The cut pinned for bucket %s of %s, lake snapshot %d up to log"
                                        + " offset %d, is no longer served (%s), and %s could not"
                                        + " derive its masks. Other splits of the bucket read with"
                                        + " that cut, so it cannot read with another one. Run the"
                                        + " read again.",
                                bucket, context.tablePath(), snapshotId, cut, why, part.describe()),
                        derivation);
            }
            masks = DvBucketMasks.fromState(derived);
            return part.withMaskState(derived);
        }
    }

    /**
     * Derives the masks of the pinned cut as the server does. A lake row is masked if the log
     * before the cut changed its key. A log version is masked if a later change of its key precedes
     * the cut. Reads the log from the snapshot's offset and the keys of the part's lake files.
     */
    private DvBucketMaskState deriveMasks(long cut) throws Exception {
        KeyReader keys = context.keyReader();
        long logStart = part.getPinnedLogStartOffset();
        // the offset of each changed key's live version, -1 if deleted
        Map<ByteBuffer, Long> changed = new HashMap<>();
        Roaring64Bitmap superseded = new Roaring64Bitmap();
        if (logStart < cut) {
            try (LogScanner scanner = keys.createLogScanner()) {
                long from = logStart < 0 ? EARLIEST_OFFSET : logStart;
                if (bucket.getPartitionId() != null) {
                    scanner.subscribe(bucket.getPartitionId(), bucket.getBucket(), from);
                } else {
                    scanner.subscribe(bucket.getBucket(), from);
                }
                long next = Math.max(0L, logStart);
                int idlePolls = 0;
                while (next < cut) {
                    ScanRecords records = scanner.poll(KEY_POLL_TIMEOUT);
                    boolean progressed = false;
                    for (ScanRecord record : records.records(bucket)) {
                        progressed = true;
                        long offset = record.logOffset();
                        if (offset >= cut) {
                            next = cut;
                            break;
                        }
                        next = offset + 1;
                        ChangeType changeType = record.getChangeType();
                        if (changeType == ChangeType.UPDATE_BEFORE) {
                            continue;
                        }
                        Long previous =
                                changed.put(
                                        ByteBuffer.wrap(keys.encode(record.getRow())),
                                        changeType == ChangeType.DELETE ? -1L : offset);
                        if (previous != null && previous >= 0) {
                            superseded.addLong(previous);
                        }
                    }
                    Long consumedUpTo = records.consumedUpToOffset(bucket);
                    if (consumedUpTo != null) {
                        progressed = true;
                        next = Math.max(next, consumedUpTo);
                    }
                    if (progressed) {
                        idlePolls = 0;
                    } else if (bucket.getPartitionId() != null
                            && context.isPartitionDropped(bucket.getPartitionId())) {
                        throw new IOException(
                                String.format(
                                        "Partition '%s' was dropped before its log was read.",
                                        part.getPartitionName()));
                    } else if (++idlePolls >= MAX_IDLE_KEY_POLLS) {
                        throw new IOException(
                                String.format(
                                        "The log of %s made no progress at offset %d.",
                                        bucket, next));
                    }
                }
            }
        }
        Map<String, byte[]> lakeMasks = new HashMap<>();
        if (part.getLakeSplits() != null && !changed.isEmpty()) {
            for (LakeSplit lakeSplit : part.getLakeSplits()) {
                List<String> names = lakeSplit.fileNames();
                String onlyFile = names != null && names.size() == 1 ? names.get(0) : null;
                Map<String, Roaring64Bitmap> fileMasks = new HashMap<>();
                try (CloseableIterator<RowWithPosResult> rows = keys.readLakeKeys(lakeSplit)) {
                    while (rows.hasNext()) {
                        RowWithPosResult row = rows.next();
                        if (!changed.containsKey(ByteBuffer.wrap(keys.encode(row.getRow())))) {
                            continue;
                        }
                        String file = row.getFileName() != null ? row.getFileName() : onlyFile;
                        if (file == null) {
                            throw new IOException(
                                    String.format(
                                            "A row of %s does not name its data file.", lakeSplit));
                        }
                        fileMasks
                                .computeIfAbsent(
                                        DvBucketMasks.fileNameOf(file), f -> new Roaring64Bitmap())
                                .addLong(row.getPos());
                    }
                }
                for (Map.Entry<String, Roaring64Bitmap> e : fileMasks.entrySet()) {
                    lakeMasks.put(e.getKey(), DvBucketMasks.serialize(e.getValue()));
                }
            }
        }
        byte[] logMask = part.readsLog() ? DvBucketMasks.serialize(superseded) : null;
        return new DvBucketMaskState(logStart, cut, null, lakeMasks, logMask);
    }

    /**
     * Fetches the masks of cut {@code cut}: those of {@code lakeSplits} and, if {@code readsLog},
     * the log mask in pages. Fails if the leader answers for another cut. The log mask starts at
     * {@code logStart}, or at the start offset the leader returns with the lake masks.
     */
    private DvBucketMaskState fetchCut(
            long snapshotId,
            long cut,
            @Nullable List<LakeSplit> lakeSplits,
            long logStart,
            boolean readsLog)
            throws Exception {
        String identity = null;
        Map<String, byte[]> lakeMasks = new HashMap<>();
        if (lakeSplits != null && !lakeSplits.isEmpty()) {
            List<String> fileNames = new ArrayList<>();
            boolean namesKnown = true;
            for (LakeSplit lakeSplit : lakeSplits) {
                List<String> names = lakeSplit.fileNames();
                if (names == null) {
                    namesKnown = false;
                    break;
                }
                fileNames.addAll(names);
            }
            boolean paged = namesKnown;
            List<String> requested = paged ? fileNames : null;
            GetDvSnapshotResponse response =
                    context.maskFetcher()
                            .fetch(
                                    () -> {
                                        GetDvSnapshotRequest request =
                                                request(snapshotId, cut, requested);
                                        // a part that does not name its files needs all masks
                                        return paged ? request : request.setPaged(false);
                                    });
            checkPinned(response, cut, paged);
            lakeMasks.putAll(DvBucketMasks.lakeMasksOf(response));
            logStart = response.getSnapshotStartOffset();
            identity =
                    response.hasLakeSnapshotIdentity() ? response.getLakeSnapshotIdentity() : null;
        }
        byte[] logMask = null;
        if (readsLog && logStart < cut) {
            Roaring64Bitmap log = new Roaring64Bitmap();
            // the leader pages the log from offset 0 at the earliest
            for (long from = Math.max(0L, logStart); from < cut; ) {
                long pageFrom = from;
                GetDvSnapshotResponse page =
                        context.maskFetcher()
                                .fetch(
                                        () ->
                                                request(snapshotId, cut, null)
                                                        .setLogDvFromOffset(pageFrom)
                                                        .setMaxLogDvBytes(MAX_LOG_DV_BYTES));
                checkPinned(page, cut, true);
                long to = page.hasLogDvToOffset() ? page.getLogDvToOffset() : cut;
                if (to <= from) {
                    throw new IOException(
                            String.format(
                                    "The log mask page of %s from offset %d ends at %d.",
                                    part.describe(), from, to));
                }
                if (page.hasLogDvBitmap()) {
                    log.or(DvBucketMasks.deserialize(page.getLogDvBitmap(), "the log"));
                }
                if (identity == null && page.hasLakeSnapshotIdentity()) {
                    identity = page.getLakeSnapshotIdentity();
                }
                from = to;
            }
            logMask = DvBucketMasks.serialize(log);
        }
        return new DvBucketMaskState(logStart, cut, identity, lakeMasks, logMask);
    }

    private void checkPinned(GetDvSnapshotResponse response, long cut, boolean paged)
            throws IOException {
        boolean answeredPaged = response.hasPaged() && response.isPaged();
        if (response.getLogEndOffset() != cut || (paged && !answeredPaged)) {
            throw new IOException(
                    String.format(
                            "The leader answered masks up to log offset %d%s instead of those of"
                                    + " the pinned cut %d.",
                            response.getLogEndOffset(), answeredPaged ? "" : ", unpaged", cut));
        }
    }

    /** Reads with the masks a started part kept. */
    private DvBucketPart fromKeptMasks(DvBucketMaskState stored) throws IOException {
        checkState(
                stored.getLogEndOffset() == part.getMaskLogEndOffset(),
                "The masks kept by %s are of another cut.",
                part);
        String registered = stored.getLakeSnapshotIdentity();
        if (registered != null) {
            checkSnapshotIdentity(part.getSnapshotId(), registered);
        }
        masks = DvBucketMasks.fromState(stored);
        return part;
    }

    /** Resumes a part that emitted rows, with the masks of the cut it was read with. */
    private DvBucketPart resume() throws Exception {
        if (part.getReadMode() != ReadMode.MASKED) {
            // a merge or lake-only read depends on the snapshot and planned offsets only
            return part;
        }
        long snapshotId = part.getSnapshotId();
        long cut = part.getMaskLogEndOffset();
        DvBucketMaskState stored = part.getMaskState();
        if (stored != null) {
            return fromKeptMasks(stored);
        }
        // a part restored without masks asks the server for those of its cut
        List<String> firstFiles = fileNamesOf(part.getLakeSplits(), nextIndexOf(part));
        GetDvSnapshotResponse first;
        try {
            first = context.maskFetcher().fetch(() -> request(snapshotId, cut, firstFiles));
        } catch (Exception e) {
            if (ExceptionUtils.findThrowable(e, LakeTableRolledBackException.class).isPresent()) {
                throw e;
            }
            throw cannotResume(
                    String.format(
                            "its leader no longer serves them (%s)",
                            ExceptionUtils.stripExecutionException(e).getMessage()),
                    e);
        }
        checkSnapshotIdentity(snapshotId, first);
        if (first.getLogEndOffset() != cut) {
            throw cannotResume(
                    String.format(
                            "its leader serves masks up to log offset %d now",
                            first.getLogEndOffset()),
                    null);
        }
        masks = DvBucketMasks.of(first, firstFiles, pages(snapshotId, cut), MAX_LOG_DV_BYTES);
        return part.withMaskState(DvBucketMasks.stateOf(first));
    }

    private IllegalStateException cannotResume(String why, @Nullable Throwable cause) {
        return new IllegalStateException(
                String.format(
                        "Cannot resume the union read of %s bucket %s exactly: rows were emitted"
                                + " with the deletion vector masks of lake snapshot %d up to log"
                                + " offset %d, but %s. Masks of another cut would emit rows"
                                + " twice or lose them; run the bounded read again without its"
                                + " state.",
                        context.tablePath(),
                        bucket,
                        part.getSnapshotId(),
                        part.getMaskLogEndOffset(),
                        why),
                cause);
    }

    private static int nextIndexOf(DvBucketPart part) {
        return part.getNextLogOffset() >= 0
                ? part.lakeSplitCount()
                : part.getCurrentLakeSplitIndex();
    }

    /**
     * Fails the read if the masks were derived for another commit than the one snapshot {@code
     * snapshotId} names now. A lake that reuses snapshot ids after a rollback (Paimon) may hold a
     * new commit under the readable snapshot's id. The registered snapshot's masks would hide and
     * resurrect the wrong rows of it.
     */
    private void checkSnapshotIdentity(long snapshotId, GetDvSnapshotResponse response)
            throws IOException {
        if (response.hasLakeSnapshotIdentity()) {
            checkSnapshotIdentity(snapshotId, response.getLakeSnapshotIdentity());
        }
    }

    private void checkSnapshotIdentity(long snapshotId, String registered) throws IOException {
        String current = context.snapshotIdentity(snapshotId);
        if (!registered.equals(current)) {
            throw LakeTableRolledBackException.ofReusedReadableSnapshot(
                    context.tablePath().toString(),
                    snapshotId,
                    registered,
                    current == null ? "unknown commit" : current);
        }
    }

    @Nullable
    private LakeSnapshot lakeSnapshotOrEmpty(@Nullable Long snapshotId) throws Exception {
        try {
            return context.lakeSnapshot(snapshotId);
        } catch (Exception e) {
            if (ExceptionUtils.findThrowable(e, LakeTableSnapshotNotExistException.class)
                    .isPresent()) {
                return null; // nothing tiered: the empty lake, whose log is read from offset 0
            }
            throw e;
        }
    }

    private long readableOffset(@Nullable LakeSnapshot lakeSnapshot) {
        if (lakeSnapshot == null) {
            return -1L;
        }
        Long offset = lakeSnapshot.getTableBucketsOffset().get(bucket);
        return offset == null || offset < 0 ? -1L : offset;
    }

    @Nullable
    private List<LakeSplit> planBucket(long snapshotId, long snapshotLogOffset) throws IOException {
        // a bucket with nothing tiered in the snapshot has its rows in the log only
        return snapshotId < 0 || snapshotLogOffset < 0
                ? null
                : context.planLakeSplits(snapshotId, bucket, part.getPartitionName());
    }

    private GetDvSnapshotRequest request(
            long snapshotId, @Nullable Long cut, @Nullable List<String> fileNames) {
        GetDvSnapshotRequest request =
                new GetDvSnapshotRequest()
                        .setTableId(bucket.getTableId())
                        .setBucketId(bucket.getBucket())
                        .setReadableSnapshotId(snapshotId)
                        .setPaged(true)
                        .setMaxLogDvBytes(MAX_LOG_DV_BYTES);
        if (bucket.getPartitionId() != null) {
            request.setPartitionId(bucket.getPartitionId());
        }
        if (cut != null) {
            request.setLogEndOffset(cut);
        }
        if (fileNames != null) {
            for (String fileName : fileNames) {
                request.addLakeDvFileName(fileName);
            }
        }
        return request;
    }

    /** Fetches pages of the masks of cut {@code cut} from a leader that pages. */
    private DvBucketMasks.PageFetcher pages(long snapshotId, long cut) {
        return pageRequest -> {
            GetDvSnapshotResponse response;
            try {
                response =
                        context.maskFetcher()
                                .fetch(() -> pageRequest.apply(request(snapshotId, cut, null)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            } catch (Exception e) {
                throw new IOException(
                        String.format(
                                "Failed to fetch the masks of %s for snapshot %d up to log offset"
                                        + " %d.",
                                part.describe(), snapshotId, cut),
                        e);
            }
            if (response.getLogEndOffset() != cut) {
                throw new IOException(
                        String.format(
                                "The leader of %s answered masks up to log offset %d instead of"
                                        + " %d.",
                                part.describe(), response.getLogEndOffset(), cut));
            }
            return response;
        };
    }

    @Nullable
    private static List<String> fileNamesOf(@Nullable List<LakeSplit> lakeSplits, int index) {
        if (lakeSplits == null || index >= lakeSplits.size()) {
            return Collections.emptyList();
        }
        return lakeSplits.get(index).fileNames();
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        DvBucketPart current = resolve();
        if (current.getReadMode() == ReadMode.MERGED) {
            CloseableIterator<InternalRow> rows = mergeScanner.pollBatch(timeout);
            returnedRows |= rows != null;
            return rows;
        }
        CloseableIterator<InternalRow> lakeRows = pollLake();
        if (lakeRows != null) {
            returnedRows = true;
            return lakeRows;
        }
        if (current.getReadMode() == ReadMode.LAKE_ONLY || !current.readsLog()) {
            return null;
        }
        return pollLog(timeout);
    }

    @Nullable
    private CloseableIterator<InternalRow> pollLake() throws IOException {
        if (lakeFinished) {
            return null;
        }
        if (currentLakeRows == null) {
            openNextLakeSplit();
        }
        // splits may be empty, or all their rows masked
        while (currentLakeRows != null && !currentLakeRows.hasNext()) {
            openNextLakeSplit();
        }
        if (currentLakeRows == null) {
            lakeFinished = true;
            return null;
        }
        return currentLakeRows;
    }

    private void openNextLakeSplit() throws IOException {
        // The caller closes a batch it emitted; a skipped or empty batch is closed only here.
        if (currentLakeRows != null) {
            currentLakeRows.close();
            currentLakeRows = null;
        }
        List<LakeSplit> lakeSplits = resolved.getLakeSplits();
        if (lakeSplits == null || nextLakeSplitIndex >= lakeSplits.size()) {
            return;
        }
        int index = nextLakeSplitIndex++;
        LakeSplit lakeSplit = lakeSplits.get(index);
        RecordReader reader =
                context.lakeSource()
                        .createRecordReader((LakeSource.ReaderContext<LakeSplit>) () -> lakeSplit);
        Map<String, Roaring64Bitmap> fileMasks =
                masks == null ? Collections.emptyMap() : masks.lakeMasksOf(lakeSplit.fileNames());
        if (fileMasks.isEmpty()) {
            currentLakeRows = new LakeRows(reader.read(), index);
        } else {
            currentLakeRows =
                    new MaskedLakeRows(
                            reader.readWithPos(), fileMasks, lakeSplit.fileNames(), index);
        }
    }

    @Nullable
    private CloseableIterator<InternalRow> pollLog(Duration timeout) throws IOException {
        if (logFinished) {
            return null;
        }
        checkState(masks != null, "A masked read of %s has no masks.", part);
        long logEnd = masks.logEndOffset();
        if (logScanner == null) {
            long from =
                    Math.max(
                            masks.logStartOffset() < 0 ? EARLIEST_OFFSET : masks.logStartOffset(),
                            resolved.getNextLogOffset());
            if (from >= logEnd || logEnd <= 0) {
                logFinished = true;
                return null;
            }
            logScanner = context.createLogScanner();
            try {
                if (bucket.getPartitionId() != null) {
                    logScanner.subscribe(bucket.getPartitionId(), bucket.getBucket(), from);
                } else {
                    logScanner.subscribe(bucket.getBucket(), from);
                }
            } catch (Exception e) {
                if (ExceptionUtils.findThrowable(e, PartitionNotExistException.class).isPresent()) {
                    return partitionDropped();
                }
                throw e;
            }
        }
        ScanRecords scanRecords = logScanner.poll(timeout);
        List<ScanRecord> rows = new ArrayList<>();
        boolean progressed = false;
        for (ScanRecord record : scanRecords.records(bucket)) {
            progressed = true;
            long offset = record.logOffset();
            if (offset >= logEnd) {
                logFinished = true;
                break;
            }
            if (offset >= logEnd - 1) {
                logFinished = true;
            }
            ChangeType changeType = record.getChangeType();
            if (changeType == ChangeType.DELETE
                    || changeType == ChangeType.UPDATE_BEFORE
                    || masks.isLogVersionMasked(offset)) {
                continue;
            }
            rows.add(record);
        }
        Long consumedUpTo = scanRecords.consumedUpToOffset(bucket);
        if (consumedUpTo != null) {
            progressed = true;
            if (consumedUpTo >= logEnd) {
                logFinished = true;
            }
        }
        if (!progressed
                && !logFinished
                && bucket.getPartitionId() != null
                && context.isPartitionDropped(bucket.getPartitionId())) {
            return partitionDropped();
        }
        returnedRows |= !rows.isEmpty();
        return new LogRows(rows, resolved.lakeSplitCount());
    }

    /**
     * Handles a partition dropped from Fluss while its log was to be read. Rows emitted so far were
     * masked by versions in that log, so the read cannot end without it and fails. A part that
     * emitted nothing of the bucket ends, as a read planned after the drop would read nothing of
     * it.
     */
    @Nullable
    private CloseableIterator<InternalRow> partitionDropped() {
        if (!returnedRows && !part.isStarted()) {
            LOG.warn(
                    "The partition of {} was dropped before its log was read; the part ends"
                            + " without rows.",
                    part.describe());
            logFinished = true;
            return null;
        }
        throw new IllegalStateException(
                String.format(
                        "Partition '%s' of %s was dropped while a union read was reading the log"
                                + " of its bucket %d. The rows read of the bucket were masked by"
                                + " changes in that log, which is gone, so the read cannot"
                                + " complete exactly. Run the read again: it reads the"
                                + " partition's lake data only.",
                        part.getPartitionName(), context.tablePath(), bucket.getBucket()));
    }

    @Override
    public void close() throws IOException {
        Exception failure = null;
        try {
            if (currentLakeRows != null) {
                currentLakeRows.close();
            }
        } catch (Exception e) {
            failure = e;
        }
        try {
            if (logScanner != null) {
                logScanner.close();
            }
        } catch (Exception e) {
            failure = failure == null ? e : failure;
        }
        try {
            if (mergeScanner != null) {
                mergeScanner.close();
            }
        } catch (Exception e) {
            failure = failure == null ? e : failure;
        }
        if (failure != null) {
            throw new IOException("Failed to close the scanner of " + part.describe(), failure);
        }
    }

    /** The rows of one lake split read without masks. */
    private static final class LakeRows implements IndexedRowIterator {

        private final CloseableIterator<LogRecord> records;
        private final int index;
        private boolean closed;

        LakeRows(CloseableIterator<LogRecord> records, int index) {
            this.records = records;
            this.index = index;
        }

        @Override
        public int getCurrentLakeSplitIndex() {
            return index;
        }

        @Override
        public boolean hasNext() {
            return !closed && records.hasNext();
        }

        @Override
        public InternalRow next() {
            return records.next().getRow();
        }

        @Override
        public void close() {
            // Closed twice: by the caller after it emitted the rows, and by the reader moving on.
            if (!closed) {
                closed = true;
                records.close();
            }
        }
    }

    /** The rows of one lake split whose positions are not masked. */
    private static final class MaskedLakeRows implements IndexedRowIterator {

        private final CloseableIterator<RowWithPosResult> delegate;
        private final Map<String, Roaring64Bitmap> fileMasks;
        // the only file's mask, for rows that do not name their file
        @Nullable private final Roaring64Bitmap singleFileMask;
        private final int index;
        @Nullable private InternalRow nextRow;
        private boolean closed;

        MaskedLakeRows(
                CloseableIterator<RowWithPosResult> delegate,
                Map<String, Roaring64Bitmap> fileMasks,
                @Nullable List<String> fileNames,
                int index) {
            this.delegate = delegate;
            this.fileMasks = fileMasks;
            this.singleFileMask =
                    fileNames != null && fileNames.size() == 1
                            ? fileMasks.get(fileNames.get(0))
                            : null;
            this.index = index;
        }

        @Override
        public int getCurrentLakeSplitIndex() {
            return index;
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            while (nextRow == null && delegate.hasNext()) {
                RowWithPosResult result = delegate.next();
                String fileName = result.getFileName();
                Roaring64Bitmap mask = fileName == null ? singleFileMask : fileMasks.get(fileName);
                if (mask != null && mask.contains(result.getPos())) {
                    continue;
                }
                nextRow = result.getRow();
            }
            return nextRow != null;
        }

        @Override
        public InternalRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            InternalRow row = nextRow;
            nextRow = null;
            return row;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                delegate.close();
            }
        }
    }

    /** Log rows emitted as inserts, each with its offset. */
    private static final class LogRows implements LogRowIterator {

        private final Iterator<ScanRecord> records;
        private final int index;
        @Nullable private ScanRecord current;

        LogRows(List<ScanRecord> records, int index) {
            this.records = records.iterator();
            this.index = index;
        }

        @Override
        public int getCurrentLakeSplitIndex() {
            return index;
        }

        @Override
        public long currentLogOffset() {
            return current == null ? -1L : current.logOffset();
        }

        @Override
        public long currentTimestamp() {
            return current == null ? -1L : current.timestamp();
        }

        @Override
        public boolean hasNext() {
            return records.hasNext();
        }

        @Override
        public InternalRow next() {
            current = records.next();
            return current.getRow();
        }

        @Override
        public void close() {}
    }
}
