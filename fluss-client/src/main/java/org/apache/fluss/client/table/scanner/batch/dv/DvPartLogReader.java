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
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.batch.LogRowIterator;
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketPartReader.ReadStats;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.ExceptionUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Reads the log of a resolved part from its masks' start, or the part's next log offset, up to the
 * masks' cut, without the versions the masks hide, deletes and before-images.
 */
@Internal
final class DvPartLogReader {

    private final DvBucketPart part;
    private final DvBucketPart resolved;
    private final TableBucket bucket;
    @Nullable private final DvBucketMasks masks;
    private final DvBucketPartReader.Context context;
    private final ReadStats stats;

    @Nullable private LogScanner logScanner;
    private boolean logFinished;
    private boolean returnedRows;
    private boolean partitionDropped;

    DvPartLogReader(
            DvBucketPart part,
            DvBucketPart resolved,
            @Nullable DvBucketMasks masks,
            DvBucketPartReader.Context context,
            ReadStats stats) {
        this.part = part;
        this.resolved = resolved;
        this.bucket = part.getTableBucket();
        this.masks = masks;
        this.context = context;
        this.stats = stats;
    }

    /**
     * The log offset to subscribe a part's log from: the part's next log offset once it read the
     * log, else where its masks start. A start below 0 is a bucket with nothing tiered, whose whole
     * log is read: never subscribe at -1.
     */
    static long subscribeOffset(long maskStartOffset, long nextLogOffset) {
        long start = maskStartOffset < 0 ? EARLIEST_OFFSET : maskStartOffset;
        return nextLogOffset < 0 ? start : Math.max(start, nextLogOffset);
    }

    /** Subscribes {@code scanner} to the log of {@code bucket} from {@code offset}. */
    static void subscribe(LogScanner scanner, TableBucket bucket, long offset) {
        if (bucket.getPartitionId() != null) {
            scanner.subscribe(bucket.getPartitionId(), bucket.getBucket(), offset);
        } else {
            scanner.subscribe(bucket.getBucket(), offset);
        }
    }

    /** Reads the next batch of the log, timing it but for fetching mask pages. */
    @Nullable
    CloseableIterator<InternalRow> poll(Duration timeout) throws IOException {
        if (logFinished) {
            return null;
        }
        long start = System.nanoTime();
        long fetchedBefore = stats.maskFetchNanos;
        try {
            return readLog(timeout);
        } finally {
            stats.logReadNanos +=
                    System.nanoTime() - start - (stats.maskFetchNanos - fetchedBefore);
        }
    }

    @Nullable
    private CloseableIterator<InternalRow> readLog(Duration timeout) throws IOException {
        checkState(masks != null, "A masked read of %s has no masks.", part);
        long logEnd = masks.logEndOffset();
        if (logScanner == null) {
            long from = subscribeOffset(masks.logStartOffset(), resolved.getNextLogOffset());
            if (from >= logEnd || logEnd <= 0) {
                logFinished = true;
                return null;
            }
            logScanner = context.createLogScanner();
            try {
                subscribe(logScanner, bucket, from);
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
            stats.logRecords++;
            ChangeType changeType = record.getChangeType();
            if (changeType == ChangeType.DELETE || changeType == ChangeType.UPDATE_BEFORE) {
                continue;
            }
            if (masks.isLogVersionMasked(offset)) {
                stats.logRecordsMasked++;
                continue;
            }
            rows.add(record);
        }
        stats.logRowsEmitted += rows.size();
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

    /** Ends the log read, as the partition was dropped while its log was to be read. */
    @Nullable
    private CloseableIterator<InternalRow> partitionDropped() {
        partitionDropped = true;
        logFinished = true;
        return null;
    }

    /** Whether the log read ended as the partition was dropped, not at the cut. */
    boolean isPartitionDropped() {
        return partitionDropped;
    }

    /** Whether the reader returned log rows. */
    boolean hasReturnedRows() {
        return returnedRows;
    }

    void close() throws Exception {
        if (logScanner != null) {
            logScanner.close();
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
