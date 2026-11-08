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

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.lake.iceberg.utils.IcebergUtils;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.RowWithPosResult;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.data.IcebergGenericReader;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.fluss.lake.iceberg.IcebergSchemaUtils.LEGACY_SYSTEM_COLUMNS;

/**
 * Iceberg record reader. The filter is applied during the plan phase of IcebergSplitPlanner, so the
 * RecordReader does not need to apply the filter again.
 *
 * <p>Refer to {@link org.apache.iceberg.data.GenericReader#open(FileScanTask)} and {@link
 * org.apache.iceberg.Scan#ignoreResiduals()} for details.
 */
public class IcebergRecordReader implements RecordReader {

    /** Sentinel value emitted when the lake table has no per-record offset/timestamp. */
    private static final long NO_SYSTEM_COLUMN_VALUE = -1L;

    protected IcebergRecordAsFlussRecordIterator iterator;
    protected @Nullable int[][] project;
    protected Types.StructType struct;

    private final FileScanTask fileScanTask;
    private final Table table;
    // The catalog that owns this reader's FileIO (S3 connection pool). It is closed when the row
    // iterator closes; otherwise each split leaks an S3FileIO and exhausts the connection pool.
    private final @Nullable Catalog catalog;

    public IcebergRecordReader(FileScanTask fileScanTask, Table table, @Nullable int[][] project) {
        this(fileScanTask, table, project, null);
    }

    public IcebergRecordReader(
            FileScanTask fileScanTask,
            Table table,
            @Nullable int[][] project,
            @Nullable Catalog catalog) {
        this.fileScanTask = fileScanTask;
        this.table = table;
        this.project = project;
        this.catalog = catalog;
        TableScan tableScan = table.newScan();
        if (project != null) {
            tableScan = applyProject(tableScan, project);
        }
        struct = tableScan.schema().asStruct();
        // The iterator opens lazily in read(): readWithPos() callers never close it, so an eager
        // open would leak a connection per split.
        this.readScan = tableScan;
    }

    private final TableScan readScan;

    @Override
    public CloseableIterator<LogRecord> read() throws IOException {
        if (iterator == null) {
            IcebergGenericReader reader = new IcebergGenericReader(readScan, true);
            iterator =
                    new IcebergRecordAsFlussRecordIterator(
                            reader.open(fileScanTask), struct, catalog);
        }
        return iterator;
    }

    @Override
    public CloseableIterator<RowWithPosResult> readWithPos() throws IOException {
        // Project the caller's columns plus Iceberg's _pos metadata column. _pos is the row's
        // position in its file; rows hidden by deletion vectors leave gaps.
        Types.StructType tableStruct = table.schema().asStruct();
        List<Types.NestedField> cols = new ArrayList<>();
        if (project != null && project.length > 0) {
            for (int[] p : project) {
                cols.add(tableStruct.fields().get(p[0]));
            }
        } else {
            // No projection pushed down (e.g. SELECT *): all columns of the table.
            cols.addAll(tableStruct.fields());
        }
        cols.add(MetadataColumns.ROW_POSITION);
        TableScan tableScan = table.newScan().project(new Schema(cols));
        Types.StructType posStruct = tableScan.schema().asStruct();
        int posIndex =
                posStruct.fields().indexOf(posStruct.field(MetadataColumns.ROW_POSITION.name()));
        IcebergGenericReader reader = new IcebergGenericReader(tableScan, true);
        return new IcebergRowWithPosIterator(
                reader.open(fileScanTask),
                posStruct,
                posIndex,
                IcebergSplit.fileNameOf(fileScanTask),
                catalog);
    }

    private TableScan applyProject(TableScan tableScan, int[][] projects) {
        Types.StructType structType = tableScan.schema().asStruct();
        List<Types.NestedField> cols = new ArrayList<>(projects.length);
        // The projected column ids reference the user (business) columns; the log offset /
        // timestamp
        // are not read from the lake table (a clean table has none, and for a legacy table we no
        // longer read them), so no system column needs to be projected.
        for (int[] project : projects) {
            cols.add(structType.fields().get(project[0]));
        }
        return tableScan.project(new Schema(cols));
    }

    /** Iterator for iceberg record as fluss record. */
    public static class IcebergRecordAsFlussRecordIterator implements CloseableIterator<LogRecord> {

        private final org.apache.iceberg.io.CloseableIterator<Record> icebergRecordIterator;

        private final ProjectedRow projectedRow;
        private final IcebergRecordAsFlussRow icebergRecordAsFlussRow;

        private final @Nullable Catalog catalog;

        public IcebergRecordAsFlussRecordIterator(
                CloseableIterable<Record> icebergRecordIterator, Types.StructType struct) {
            this(icebergRecordIterator, struct, null);
        }

        public IcebergRecordAsFlussRecordIterator(
                CloseableIterable<Record> icebergRecordIterator,
                Types.StructType struct,
                @Nullable Catalog catalog) {
            this.icebergRecordIterator = icebergRecordIterator.iterator();
            this.catalog = catalog;

            // A legacy table read without projection still exposes its three trailing system
            // columns; trim them so only the business columns are emitted. A clean table (or any
            // projected read) has no system columns to trim.
            int businessFieldCount =
                    IcebergUtils.isLegacyTable(new Schema(struct.fields()))
                            ? struct.fields().size() - LEGACY_SYSTEM_COLUMNS.size()
                            : struct.fields().size();
            projectedRow = ProjectedRow.from(IntStream.range(0, businessFieldCount).toArray());
            icebergRecordAsFlussRow = new IcebergRecordAsFlussRow();
        }

        @Override
        public void close() {
            try {
                icebergRecordIterator.close();
            } catch (Exception e) {
                throw new RuntimeException("Fail to close iterator.", e);
            } finally {
                closeCatalogQuietly(catalog);
            }
        }

        @Override
        public boolean hasNext() {
            return icebergRecordIterator.hasNext();
        }

        @Override
        public LogRecord next() {
            Record icebergRecord = icebergRecordIterator.next();
            // The lake table does not carry a per-record log offset / timestamp (a clean table has
            // no system columns, and for a legacy table we no longer read them), so a sentinel -1
            // is
            // emitted, consistent with the Paimon reader and the LakeRecordRecordEmitter contract.
            return new GenericRecord(
                    NO_SYSTEM_COLUMN_VALUE,
                    NO_SYSTEM_COLUMN_VALUE,
                    ChangeType.INSERT,
                    projectedRow.replaceRow(
                            icebergRecordAsFlussRow.replaceIcebergRecord(icebergRecord)));
        }
    }

    /** Iterator yielding each row with its physical {@code _pos} and the data file name. */
    public static class IcebergRowWithPosIterator implements CloseableIterator<RowWithPosResult> {

        private final org.apache.iceberg.io.CloseableIterator<Record> icebergRecordIterator;
        private final IcebergRecordAsFlussRow icebergRecordAsFlussRow =
                new IcebergRecordAsFlussRow();
        private final ProjectedRow projectedRow;
        private final RowWithPosResult reusable = new RowWithPosResult();
        private final int posIndex;
        private final String fileName;
        private final @Nullable Catalog catalog;

        public IcebergRowWithPosIterator(
                CloseableIterable<Record> records,
                Types.StructType struct,
                int posIndex,
                String fileName,
                @Nullable Catalog catalog) {
            this.icebergRecordIterator = records.iterator();
            this.posIndex = posIndex;
            this.fileName = fileName;
            this.catalog = catalog;
            // Project away the trailing _pos column so the caller sees only its own columns.
            int[] project =
                    IntStream.range(0, struct.fields().size()).filter(i -> i != posIndex).toArray();
            this.projectedRow = ProjectedRow.from(project);
        }

        @Override
        public boolean hasNext() {
            return icebergRecordIterator.hasNext();
        }

        @Override
        public RowWithPosResult next() {
            Record icebergRecord = icebergRecordIterator.next();
            long pos = icebergRecord.get(posIndex, Long.class);
            return reusable.set(
                    projectedRow.replaceRow(
                            icebergRecordAsFlussRow.replaceIcebergRecord(icebergRecord)),
                    pos,
                    fileName);
        }

        @Override
        public void close() {
            try {
                icebergRecordIterator.close();
            } catch (Exception e) {
                throw new RuntimeException("Fail to close iterator.", e);
            } finally {
                closeCatalogQuietly(catalog);
            }
        }
    }

    /** Closes the catalog and its FileIO (S3 connection pool). Never throws. */
    private static void closeCatalogQuietly(@Nullable Catalog catalog) {
        if (catalog instanceof AutoCloseable) {
            try {
                ((AutoCloseable) catalog).close();
            } catch (Exception ignored) {
                // best-effort; a failed catalog close must not fail the read
            }
        }
    }
}
