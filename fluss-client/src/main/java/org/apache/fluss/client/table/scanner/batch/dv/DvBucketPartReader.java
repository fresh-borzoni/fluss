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
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.batch.IndexedRowIterator;
import org.apache.fluss.client.table.scanner.batch.LogRowIterator;
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketPart.ReadMode;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.RowWithPosResult;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Reads a {@link DvBucketPart}: its lake files with the rows the log changed masked, then the log
 * up to the masks' cut without superseded versions, deletes and before-images.
 *
 * <p>The masks are fetched from the bucket's leader when the part starts ({@link #resolve()}). If
 * the leader covers another lake snapshot than the planned one, the bucket is planned again and
 * read on that snapshot. A dropped partition has only its lake files read. A bucket whose leader
 * has no masks to serve yet is read by key ({@link ReadMode#FOLDED}): the reader derives the masks
 * from the keys of the lake files and of the log up to a cut. A read that does not wait gets that
 * cut from the leader at once. If the masks stay unavailable, the bucket is planned again on the
 * coordinator's readable snapshot and read by key up to the planned stopping offset.
 *
 * <p>A part that reads its whole bucket pins its own cut when it starts, unless planning pinned
 * one, and fetches all masks of the cut at once. If the leader loses the cut before they are
 * fetched, after a leader change or as it evicts cuts, the part pins again, and in the end reads
 * the bucket by key up to the last cut it pinned.
 *
 * <p>A part restored after it emitted rows reads with the masks it kept, of the cut it was read
 * with: masks of another cut would emit rows twice or lose them.
 *
 * <p>The read is a state machine ({@link DvPartMachine}, whose javadoc has the transitions): the
 * reader resolves the part with {@link DvPartResolver}, then reads its lake files with {@link
 * DvPartLakeReader} and its log with {@link DvPartLogReader}, and applies the end of each step to
 * the machine.
 *
 * <p>The reader starts at lake split {@link DvBucketPart#getCurrentLakeSplitIndex()} and at log
 * offset {@link DvBucketPart#getNextLogOffset()}. Lake rows come in {@link IndexedRowIterator}s and
 * log rows in {@link LogRowIterator}s, so a caller that restores reads can track its position. The
 * caller skips the rows of a lake split it read before.
 */
@Internal
public class DvBucketPartReader implements BatchScanner {

    private static final Logger LOG = LoggerFactory.getLogger(DvBucketPartReader.class);

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

        /** The coordinator's readable lake snapshot, with the buckets' readable offsets. */
        LakeSnapshot readableLakeSnapshot() throws Exception;

        /**
         * What the lake commit of {@code snapshotId} is now, see
         * SupportsRewritePlanning#snapshotIdentity.
         */
        @Nullable
        String snapshotIdentity(long snapshotId) throws IOException;

        LogScanner createLogScanner();

        /** Reads the primary keys of the bucket, to derive masks without the server. */
        KeyReader keyReader();

        /** Whether the partition was dropped from Fluss. */
        boolean isPartitionDropped(long partitionId);

        void sleep(long millis) throws InterruptedException;

        /**
         * Whether a part waits for its bucket's leader to switch to the planned snapshot, to have
         * masks to serve, and to finish its lake-position lookups. If not, the leader answers at
         * once: a leader that covers the snapshot registered right before has the part read that
         * snapshot, one with no masks yet has it read by key, and while lookups are pending the
         * part gets masks up to an offset below the high watermark and reads the log up to it.
         */
        default boolean waitForCatchUp() {
            return true;
        }
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
    private final DvPartMachine machine;
    private final DvPartResolver resolver;

    @Nullable private DvPartLakeReader lakeReader;
    @Nullable private DvPartLogReader logReader;

    // where the read's time goes, logged at DEBUG when the read ends
    private final ReadStats stats = new ReadStats();
    private boolean readEnded;
    @Nullable private Consumer<ReadStats> onReadEnd;

    public DvBucketPartReader(DvBucketPart part, Context context) {
        this.part = part;
        this.bucket = part.getTableBucket();
        this.context = context;
        this.machine =
                new DvPartMachine(part, context.tablePath(), stats, new DvOwnPin(part, context));
        this.resolver = new DvPartResolver(machine, context, stats);
    }

    /**
     * Resolves how the part is read and fetches the bucket's masks if it is read with them. Returns
     * the resolved part with its cut and the masks a caller keeps to restore the part once it
     * emitted rows.
     */
    public DvBucketPart resolve() throws IOException {
        DvBucketPart resolved = machine.resolved();
        if (resolved != null) {
            if (!stats.started && machine.state() == DvPartState.READY) {
                // a started lake-only part restored READY: its read starts here, once
                stats.start();
                logReading(resolved);
            }
            return resolved;
        }
        long start = stats.start();
        try {
            resolver.run();
        } finally {
            stats.resolveNanos += System.nanoTime() - start;
        }
        resolved = machine.resolved();
        if (resolved == null) {
            throw failure();
        }
        logReading(resolved);
        return resolved;
    }

    /** Logs how the part is read, once it resolved. */
    private static void logReading(DvBucketPart resolved) {
        LOG.info(
                "Reading {} as {} (snapshot {}, {} lake splits, masks up to log offset {}).",
                resolved.describe(),
                resolved.getReadMode(),
                resolved.getSnapshotId(),
                resolved.lakeSplitCount(),
                resolved.getMaskLogEndOffset());
    }

    /** The failure of a part that failed or was closed, as the reader's calls throw it. */
    private IOException failure() {
        if (machine.state() == DvPartState.CLOSED) {
            throw new IllegalStateException("The reader of " + part.describe() + " is closed.");
        }
        Throwable e = checkNotNull(machine.failure());
        if (e instanceof IOException) {
            return (IOException) e;
        }
        if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        }
        if (e instanceof Error) {
            throw (Error) e;
        }
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            return new IOException("Interrupted while fetching the masks of " + part.describe(), e);
        }
        return new IOException("Failed to fetch the masks of " + part.describe(), e);
    }

    /** Performs the action of the part's state, if it resolves, and applies how it ended. */
    @VisibleForTesting
    void step() {
        if (machine.state().isResolving()) {
            machine.apply(resolver.act());
        }
    }

    /** The state of the part's read. */
    @VisibleForTesting
    DvPartState state() {
        return machine.state();
    }

    /** The states the part's read went through. */
    @VisibleForTesting
    List<DvPartState> history() {
        return machine.history();
    }

    /**
     * Sets what receives the read's measurements once it ends: the reader returned its last rows or
     * was closed.
     */
    public void onReadEnd(Consumer<ReadStats> listener) {
        this.onReadEnd = listener;
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        resolve();
        try {
            while (true) {
                switch (machine.state()) {
                    case READY:
                        lakeReader =
                                new DvPartLakeReader(
                                        part, machine.resolved(), machine.masks(), context, stats);
                        machine.apply(DvPartEvent.of(DvPartEvent.Kind.POLL));
                        break;
                    case READING_LAKE:
                        CloseableIterator<InternalRow> lakeRows = checkNotNull(lakeReader).poll();
                        if (lakeRows != null) {
                            machine.returnedRows();
                            return lakeRows;
                        }
                        machine.apply(DvPartEvent.of(DvPartEvent.Kind.LAKE_END));
                        break;
                    case READING_LOG:
                        if (logReader == null) {
                            logReader =
                                    new DvPartLogReader(
                                            part,
                                            machine.resolved(),
                                            machine.masks(),
                                            context,
                                            stats);
                        }
                        // empty while the log has no records yet
                        CloseableIterator<InternalRow> logRows = logReader.poll(timeout);
                        if (logRows != null) {
                            if (logReader.hasReturnedRows()) {
                                machine.returnedRows();
                            }
                            return logRows;
                        }
                        machine.apply(
                                DvPartEvent.of(
                                        logReader.isPartitionDropped()
                                                ? DvPartEvent.Kind.PARTITION_DROPPED
                                                : DvPartEvent.Kind.LOG_END));
                        break;
                    case DONE:
                        readEnded(true);
                        return null;
                    default:
                        throw failure();
                }
            }
        } catch (IOException | RuntimeException e) {
            if (!machine.state().isTerminal()) {
                machine.apply(DvPartEvent.failed(e));
            }
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        // a read closed before it returned its last rows
        readEnded(false);
        machine.apply(DvPartEvent.of(DvPartEvent.Kind.CLOSE));
        Exception failure = null;
        try {
            if (lakeReader != null) {
                lakeReader.close();
            }
        } catch (Exception e) {
            failure = e;
        }
        try {
            if (logReader != null) {
                logReader.close();
            }
        } catch (Exception e) {
            failure = failure == null ? e : failure;
        }
        if (failure != null) {
            throw new IOException("Failed to close the scanner of " + part.describe(), failure);
        }
    }

    /**
     * Logs where the read's time went at DEBUG, once: when the reader returns its last rows ({@code
     * finished}), or when it is closed before. A reader that never started logs nothing.
     */
    private void readEnded(boolean finished) {
        if (readEnded || !stats.started) {
            return;
        }
        readEnded = true;
        stats.end();
        if (LOG.isDebugEnabled()) {
            logReadEnded(finished);
        }
        if (onReadEnd != null) {
            onReadEnd.accept(stats);
        }
    }

    private void logReadEnded(boolean finished) {
        DvBucketPart resolved = machine.resolved();
        DvBucketMasks masks = machine.masks();
        DvBucketPart current = resolved != null ? resolved : part;
        long logStart;
        long logStop;
        if (masks != null) {
            logStart = masks.logStartOffset();
            logStop = masks.logEndOffset();
        } else {
            logStart = current.getSnapshotLogOffset();
            logStop = current.getMaskLogEndOffset();
        }
        int fileGroup = current.getFileGroup();
        LOG.debug(
                "Read a deletion vector union read part: table={} partition={} bucket={}"
                        + " part={} mode={}"
                        + " snapshot={} log_start={} log_stop={} mask_fetch_ms={} lake_open_ms={}"
                        + " lake_rows_read={} lake_rows_masked={} log_read_ms={} log_records={}"
                        + " log_records_masked={} total_ms={}"
                        + " resolve_ms={} identity_ms={} derive_ms={} mask_fetches={} mask_rpcs={}"
                        + " replans={} replans_behind={} lake_splits={}"
                        + " log_rows_emitted={} reads_log={} finished={}",
                context.tablePath(),
                bucket.getPartitionId() == null ? "-" : bucket.getPartitionId(),
                bucket.getBucket(),
                fileGroup == DvBucketPart.BUCKET_LOG
                        ? "log"
                        : fileGroup == DvBucketPart.WHOLE_BUCKET ? "all" : "files-" + fileGroup,
                resolved == null ? "UNRESOLVED" : resolved.getReadMode(),
                current.getSnapshotId(),
                logStart,
                logStop,
                ms(stats.maskFetchNanos),
                ms(stats.lakeOpenNanos),
                stats.lakeRowsRead,
                stats.lakeRowsMasked,
                ms(stats.logReadNanos),
                stats.logRecords,
                stats.logRecordsMasked,
                ms(stats.totalNanos),
                ms(stats.resolveNanos),
                ms(stats.identityNanos),
                ms(stats.deriveNanos),
                stats.maskFetches,
                stats.maskRpcs,
                stats.replans,
                stats.replansBehind,
                stats.lakeSplits,
                stats.logRowsEmitted,
                current.readsLog(),
                finished);
    }

    /** Nanoseconds as milliseconds with one decimal. */
    private static String ms(long nanos) {
        return String.format(Locale.ROOT, "%.1f", nanos / 1_000_000.0);
    }

    /**
     * Where the time of a part's read goes, for the log and an engine's metrics. Counted per part,
     * not per row, but for the lake rows and log records read. The reader's threads update it one
     * at a time: an engine that iterates a batch on another thread than it polls on hands the batch
     * over and back. Read it once the read ended.
     *
     * <ul>
     *   <li>mask fetch: requests for masks and log mask pages, with their retries and the leader's
     *       waits;
     *   <li>lake open: opening the lake splits, but for fetching masks;
     *   <li>log read: polling the log up to the cut and dropping masked versions;
     *   <li>resolve: planning the part and fetching its masks (includes their fetch time).
     * </ul>
     *
     * <p>Reading the lake rows is not timed: the engine pulls them one by one, and a clock read per
     * row would cost more than the measurement is worth.
     */
    public static final class ReadStats {

        // package-private: the reader and its row iterators update them
        boolean started;
        long startNanos;
        long totalNanos;

        long resolveNanos;
        long identityNanos;
        long deriveNanos;
        // read by key, as the leader had no masks to serve
        boolean folded;
        int replans;
        // replans on the snapshot a leader that still switches covers
        int replansBehind;

        long maskFetchNanos;
        int maskFetches;
        int maskRpcs;

        long lakeOpenNanos;
        int lakeSplits;
        long lakeRowsRead;
        long lakeRowsMasked;

        long logReadNanos;
        long logRecords;
        long logRecordsMasked;
        long logRowsEmitted;

        /** Starts the read on its first call and returns the current time. */
        long start() {
            long now = System.nanoTime();
            if (!started) {
                started = true;
                startNanos = now;
            }
            return now;
        }

        void end() {
            this.totalNanos = System.nanoTime() - startNanos;
        }

        public long totalMs() {
            return totalNanos / 1_000_000;
        }

        public long resolveMs() {
            return resolveNanos / 1_000_000;
        }

        public long maskFetchMs() {
            return maskFetchNanos / 1_000_000;
        }

        public long lakeOpenMs() {
            return lakeOpenNanos / 1_000_000;
        }

        public long lakeRowsRead() {
            return lakeRowsRead;
        }

        public long lakeRowsMasked() {
            return lakeRowsMasked;
        }

        public long logReadMs() {
            return logReadNanos / 1_000_000;
        }

        public long logRecords() {
            return logRecords;
        }

        public long logRecordsMasked() {
            return logRecordsMasked;
        }

        /** Replans on the snapshot a leader that still switches covers. */
        public long replansBehind() {
            return replansBehind;
        }

        /** Whether the part was read by key, as its leader had no masks to serve. */
        public boolean isFolded() {
            return folded;
        }

        public long deriveMs() {
            return deriveNanos / 1_000_000;
        }
    }
}
