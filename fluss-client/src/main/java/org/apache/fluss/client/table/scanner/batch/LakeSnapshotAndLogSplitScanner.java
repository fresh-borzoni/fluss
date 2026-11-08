/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.SortMergeReader;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.SortedRecordReader;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.KeyValueRow;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;

/** A scanner to merge the lakehouse's snapshot and change log. */
public class LakeSnapshotAndLogSplitScanner implements BatchScanner {

    private static final Logger LOG = LoggerFactory.getLogger(LakeSnapshotAndLogSplitScanner.class);

    private final @Nullable List<LakeSplit> lakeSplits;
    private Comparator<InternalRow> rowComparator;
    private List<CloseableIterator<LogRecord>> lakeRecordIterators = new ArrayList<>();
    private boolean lakeRecordIteratorsInitialized;
    private final LakeSource<LakeSplit> lakeSource;

    private final int[] pkIndexes;

    // the indexes of primary key in emitted row by lake and fluss
    private int[] keyIndexesInRow;
    @Nullable private int[] adjustProjectedFields;

    // the sorted logs in memory, mapping from key -> value
    private Map<InternalRow, KeyValueRow> logRows;

    private final LogScanner logScanner;
    private final TableBucket tableBucket;
    private final long stoppingOffset;
    private boolean logScanFinished;
    // whether the bucket's partition was dropped from Fluss, null to wait for the log regardless
    @Nullable private final BooleanSupplier partitionDropped;

    private SortMergeReader currentSortMergeReader;

    public LakeSnapshotAndLogSplitScanner(
            Table table,
            LakeSource<LakeSplit> lakeSource,
            @Nullable List<LakeSplit> lakeSplits,
            TableBucket tableBucket,
            long startingOffset,
            long stoppingOffset,
            @Nullable int[] projectedFields) {
        this(
                table,
                lakeSource,
                lakeSplits,
                tableBucket,
                startingOffset,
                stoppingOffset,
                projectedFields,
                null);
    }

    /**
     * Creates a scanner that ends the log read early if {@code partitionDropped} says the bucket's
     * partition was dropped from Fluss, because its log is gone. The merge then holds the bucket's
     * rows as of the last log offset read, which is a state the bucket had.
     */
    public LakeSnapshotAndLogSplitScanner(
            Table table,
            LakeSource<LakeSplit> lakeSource,
            @Nullable List<LakeSplit> lakeSplits,
            TableBucket tableBucket,
            long startingOffset,
            long stoppingOffset,
            @Nullable int[] projectedFields,
            @Nullable BooleanSupplier partitionDropped) {
        this.tableBucket = tableBucket;
        this.partitionDropped = partitionDropped;
        this.pkIndexes = table.getTableInfo().getSchema().getPrimaryKeyIndexes();
        this.lakeSplits = lakeSplits;
        this.lakeSource = lakeSource;
        this.stoppingOffset = stoppingOffset;
        ProjectionPlan projectionPlan =
                ProjectionPlan.create(
                        table.getTableInfo().getRowType().getFieldCount(),
                        pkIndexes,
                        projectedFields);
        this.keyIndexesInRow = projectionPlan.keyIndexesInScanRow;
        this.adjustProjectedFields = projectionPlan.adjustProjectedFields;
        int[] newProjectedFields = projectionPlan.scanProjectedFields;

        this.logScanner = table.newScan().project(newProjectedFields).createLogScanner();
        this.lakeSource.withProject(
                Arrays.stream(newProjectedFields)
                        .mapToObj(field -> new int[] {field})
                        .toArray(int[][]::new));

        this.logScanFinished = startingOffset >= stoppingOffset || stoppingOffset <= 0;
        if (!logScanFinished) {
            try {
                if (tableBucket.getPartitionId() != null) {
                    this.logScanner.subscribe(
                            tableBucket.getPartitionId(), tableBucket.getBucket(), startingOffset);
                } else {
                    this.logScanner.subscribe(tableBucket.getBucket(), startingOffset);
                }
            } catch (RuntimeException e) {
                if (partitionDropped == null
                        || !ExceptionUtils.findThrowable(e, PartitionNotExistException.class)
                                .isPresent()) {
                    throw e;
                }
                LOG.warn(
                        "The partition of bucket {} was dropped before its log was read; merging"
                                + " its lake snapshot only.",
                        tableBucket);
                this.logScanFinished = true;
            }
        }
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (logScanFinished) {
            initializeLakeRecordIterators();
            if (currentSortMergeReader == null) {
                currentSortMergeReader =
                        new SortMergeReader(
                                adjustProjectedFields,
                                keyIndexesInRow,
                                lakeRecordIterators,
                                rowComparator,
                                CloseableIterator.wrap(
                                        logRows == null
                                                ? Collections.emptyIterator()
                                                : logRows.values().iterator()));
            }
            return currentSortMergeReader.readBatch();
        } else {
            initializeLakeRecordIterators();
            if (logRows == null) {
                logRows = new TreeMap<>(rowComparator);
            }
            pollLogRecords(timeout);
            return CloseableIterator.wrap(Collections.emptyIterator());
        }
    }

    private void initializeLakeRecordIterators() throws IOException {
        if (lakeRecordIteratorsInitialized) {
            return;
        }

        List<RecordReader> recordReaders = new ArrayList<>();
        if (lakeSplits == null || lakeSplits.isEmpty()) {
            // pass null split to get rowComparator
            recordReaders.add(lakeSource.createRecordReader(sortedReaderContext(null)));
        } else {
            for (LakeSplit lakeSplit : lakeSplits) {
                recordReaders.add(lakeSource.createRecordReader(sortedReaderContext(lakeSplit)));
            }
        }
        for (RecordReader reader : recordReaders) {
            if (reader instanceof SortedRecordReader) {
                rowComparator = ((SortedRecordReader) reader).order();
            } else {
                throw new UnsupportedOperationException(
                        "lake records must instance of sorted view.");
            }
            lakeRecordIterators.add(reader.read());
        }
        lakeRecordIteratorsInitialized = true;
    }

    private LakeSource.ReaderContext<LakeSplit> sortedReaderContext(@Nullable LakeSplit lakeSplit) {
        return new LakeSource.ReaderContext<LakeSplit>() {
            @Nullable
            @Override
            public LakeSplit lakeSplit() {
                return lakeSplit;
            }

            @Override
            public boolean requireSortedRecords() {
                return true;
            }
        };
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
                            + " lake snapshot with the log read so far.",
                    tableBucket);
            logScanFinished = true;
            return;
        }
        for (ScanRecord scanRecord : scanRecords) {
            boolean isDelete =
                    scanRecord.getChangeType() == ChangeType.DELETE
                            || scanRecord.getChangeType() == ChangeType.UPDATE_BEFORE;
            KeyValueRow keyValueRow =
                    new KeyValueRow(keyIndexesInRow, scanRecord.getRow(), isDelete);
            InternalRow keyRow = keyValueRow.keyRow();
            // upsert the key value row
            logRows.put(keyRow, keyValueRow);
            if (scanRecord.logOffset() >= stoppingOffset - 1) {
                // has reached to the end
                logScanFinished = true;
                break;
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (logScanner != null) {
                logScanner.close();
            }
            if (lakeRecordIterators != null) {
                for (CloseableIterator<LogRecord> iterator : lakeRecordIterators) {
                    iterator.close();
                }
            }
        } catch (Exception e) {
            throw new IOException("Failed to close resources", e);
        }
    }
}
