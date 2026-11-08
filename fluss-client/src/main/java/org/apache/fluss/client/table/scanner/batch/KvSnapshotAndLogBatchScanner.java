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

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.SortMergeReader;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.KeyValueRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Comparator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * A bounded scanner that merges an immutable KV snapshot with a fixed primary-key changelog range.
 */
@Internal
public class KvSnapshotAndLogBatchScanner implements BatchScanner {

    private static final Logger LOG = LoggerFactory.getLogger(KvSnapshotAndLogBatchScanner.class);

    private final TableBucket tableBucket;
    private final long logStoppingOffset;
    private final int[] keyIndexesInScanRow;
    @Nullable private final int[] adjustProjectedFields;
    private final Comparator<InternalRow> primaryKeyComparator;
    private final Map<InternalRow, KeyValueRow> logRows;

    @Nullable private final BatchScanner snapshotScanner;
    @Nullable private final LogScanner logScanner;

    private boolean logScanFinished;
    // whether the bucket's partition was dropped from Fluss, null to wait for the log regardless
    @Nullable private final BooleanSupplier partitionDropped;
    private boolean finished;
    private boolean closed;

    @Nullable private CloseableIterator<LogRecord> snapshotRecordIterator;
    @Nullable private SortMergeReader sortMergeReader;
    @Nullable private CloseableIterator<InternalRow> sortMergeIterator;

    public KvSnapshotAndLogBatchScanner(
            Table table,
            TableBucket tableBucket,
            long snapshotId,
            long logStartingOffset,
            long logStoppingOffset,
            @Nullable int[] projectedFields) {
        this(
                table,
                tableBucket,
                snapshotId,
                logStartingOffset,
                logStoppingOffset,
                projectedFields,
                null);
    }

    /**
     * Creates a scanner that ends the log read early if {@code partitionDropped} says the bucket's
     * partition was dropped from Fluss, because its log is gone. The merge then holds the bucket's
     * rows as of the last log offset read, which is a state the bucket had.
     */
    public KvSnapshotAndLogBatchScanner(
            Table table,
            TableBucket tableBucket,
            long snapshotId,
            long logStartingOffset,
            long logStoppingOffset,
            @Nullable int[] projectedFields,
            @Nullable BooleanSupplier partitionDropped) {
        this.partitionDropped = partitionDropped;
        checkArgument(
                table.getTableInfo().hasPrimaryKey(),
                "KvSnapshotAndLogBatchScanner only supports primary-key tables.");
        this.tableBucket = tableBucket;
        this.logStoppingOffset = logStoppingOffset;

        ProjectionPlan projectionPlan = createProjectionPlan(table.getTableInfo(), projectedFields);
        this.keyIndexesInScanRow = projectionPlan.keyIndexesInScanRow;
        this.adjustProjectedFields = projectionPlan.adjustProjectedFields;
        this.primaryKeyComparator = createPrimaryKeyComparator(table.getTableInfo());
        this.logRows = new TreeMap<>(primaryKeyComparator);

        this.snapshotScanner =
                snapshotId >= 0
                        ? table.newScan()
                                .project(projectionPlan.scanProjectedFields)
                                .createBatchScanner(tableBucket, snapshotId)
                        : null;

        boolean emptyLogRange = logStartingOffset >= logStoppingOffset || logStoppingOffset <= 0;
        boolean logScanFinished = emptyLogRange;
        if (emptyLogRange) {
            this.logScanner = null;
        } else {
            this.logScanner =
                    table.newScan().project(projectionPlan.scanProjectedFields).createLogScanner();
            Long partitionId = tableBucket.getPartitionId();
            try {
                if (partitionId == null) {
                    this.logScanner.subscribe(tableBucket.getBucket(), logStartingOffset);
                } else {
                    this.logScanner.subscribe(
                            partitionId, tableBucket.getBucket(), logStartingOffset);
                }
            } catch (RuntimeException e) {
                if (partitionDropped == null
                        || !ExceptionUtils.findThrowable(e, PartitionNotExistException.class)
                                .isPresent()) {
                    throw e;
                }
                LOG.warn(
                        "The partition of bucket {} was dropped before its log was read; reading"
                                + " its snapshot only.",
                        tableBucket);
                logScanFinished = true;
            }
        }
        this.logScanFinished = logScanFinished;
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (closed || finished) {
            return null;
        }

        // 1. collect the bounded log range into memory before merging.
        if (!logScanFinished) {
            pollLogRecords(timeout);
            return CloseableIterator.emptyIterator();
        }

        if (sortMergeReader == null) {
            CloseableIterator<LogRecord> snapshotRecords = createSnapshotRecordIterator(timeout);

            // 3. merge the snapshot stream with the reduced changelog rows by primary key.
            sortMergeReader =
                    new SortMergeReader(
                            adjustProjectedFields,
                            keyIndexesInScanRow,
                            snapshotRecords,
                            primaryKeyComparator,
                            CloseableIterator.wrap(logRows.values().iterator()));
        }

        try {
            sortMergeIterator = sortMergeReader.readBatch();
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }

        // 4. mark the scanner as finished once the merge reader has been drained.
        if (sortMergeIterator == null) {
            finished = true;
        }
        return sortMergeIterator;
    }

    private void pollLogRecords(Duration timeout) {
        ScanRecords scanRecords = logScanner.poll(timeout);
        if (scanRecords.records(tableBucket).isEmpty()
                && scanRecords.consumedUpToOffset(tableBucket) == null
                && partitionDropped != null
                && partitionDropped.getAsBoolean()) {
            // A log scanner keeps polling the buckets of a dropped partition without an error.
            LOG.warn(
                    "The partition of bucket {} was dropped while its log was read; merging its"
                            + " snapshot with the log read so far.",
                    tableBucket);
            logScanFinished = true;
            return;
        }
        for (ScanRecord scanRecord : scanRecords.records(tableBucket)) {
            long logOffset = scanRecord.logOffset();
            if (logOffset >= logStoppingOffset) {
                logScanFinished = true;
                break;
            }

            reduceLogRecord(scanRecord);
            if (logOffset >= logStoppingOffset - 1) {
                logScanFinished = true;
                break;
            }
        }

        Long consumedUpToOffset = scanRecords.consumedUpToOffset(tableBucket);
        if (consumedUpToOffset != null && consumedUpToOffset >= logStoppingOffset) {
            logScanFinished = true;
        }
    }

    private void reduceLogRecord(ScanRecord scanRecord) {
        ChangeType changeType = scanRecord.getChangeType();
        boolean isDelete =
                changeType == ChangeType.DELETE || changeType == ChangeType.UPDATE_BEFORE;
        KeyValueRow keyValueRow =
                new KeyValueRow(keyIndexesInScanRow, scanRecord.getRow(), isDelete);
        logRows.put(keyValueRow.keyRow(), keyValueRow);
    }

    private CloseableIterator<LogRecord> createSnapshotRecordIterator(Duration timeout) {
        if (snapshotScanner == null) {
            snapshotRecordIterator = CloseableIterator.emptyIterator();
        } else {
            snapshotRecordIterator = new SnapshotRecordIterator(snapshotScanner, timeout);
        }
        return snapshotRecordIterator;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOUtils.closeQuietly(sortMergeIterator);
        IOUtils.closeQuietly(snapshotRecordIterator);
        IOUtils.closeQuietly(snapshotScanner);
        IOUtils.closeQuietly(logScanner);
    }

    private static ProjectionPlan createProjectionPlan(
            TableInfo tableInfo, @Nullable int[] projectedFields) {
        return ProjectionPlan.create(
                tableInfo.getRowType().getFieldCount(),
                getPhysicalPrimaryKeyIndexes(tableInfo),
                projectedFields);
    }

    private static Comparator<InternalRow> createPrimaryKeyComparator(TableInfo tableInfo) {
        int[] physicalPrimaryKeyIndexes = getPhysicalPrimaryKeyIndexes(tableInfo);
        RowType primaryKeyRowType =
                Schema.getKeyRowType(tableInfo.getSchema(), physicalPrimaryKeyIndexes);
        KeyEncoder primaryKeyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        primaryKeyRowType,
                        tableInfo.getPhysicalPrimaryKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());
        return (row1, row2) -> {
            byte[] key1 = primaryKeyEncoder.encodeKey(row1);
            byte[] key2 = primaryKeyEncoder.encodeKey(row2);
            return MemorySegment.wrap(key1)
                    .compare(MemorySegment.wrap(key2), 0, 0, key1.length, key2.length);
        };
    }

    private static int[] getPhysicalPrimaryKeyIndexes(TableInfo tableInfo) {
        return tableInfo.getPhysicalPrimaryKeys().stream()
                .mapToInt(primaryKey -> tableInfo.getRowType().getFieldIndex(primaryKey))
                .toArray();
    }

    @VisibleForTesting
    static class SnapshotRecordIterator implements CloseableIterator<LogRecord> {
        private final BatchScanner scanner;
        private final Duration timeout;
        @Nullable private CloseableIterator<InternalRow> rows;
        private boolean snapshotFinished;

        @VisibleForTesting
        SnapshotRecordIterator(BatchScanner scanner, Duration timeout) {
            this.scanner = scanner;
            this.timeout = timeout;
        }

        @Override
        public void close() {
            IOUtils.closeQuietly(rows);
            rows = null;
        }

        @Override
        public boolean hasNext() {
            while (true) {
                if (rows != null) {
                    if (rows.hasNext()) {
                        return true;
                    }
                    rows.close();
                    rows = null;
                }

                if (snapshotFinished) {
                    return false;
                }

                try {
                    rows = scanner.pollBatch(timeout);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                if (rows == null) {
                    snapshotFinished = true;
                    return false;
                }
            }
        }

        @Override
        public LogRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return new ScanRecord(rows.next());
        }
    }
}
