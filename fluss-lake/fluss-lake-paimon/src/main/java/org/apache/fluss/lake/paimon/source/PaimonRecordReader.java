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

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.lake.paimon.utils.PaimonRowAsFlussRow;
import org.apache.fluss.lake.paimon.utils.PaimonUtils;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.RowWithPosResult;
import org.apache.fluss.lake.source.SupportsRowPositions;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;

import static org.apache.fluss.lake.paimon.PaimonLakeCatalog.LEGACY_SYSTEM_COLUMNS;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toChangeType;

/** Record reader for paimon table. */
public class PaimonRecordReader implements RecordReader, SupportsRowPositions {

    /**
     * Sentinel log offset / timestamp emitted for rows read from a lake table. The lake table does
     * not carry a per-record log offset (a clean table has no system columns, and for a legacy
     * table we no longer read them), so a negative value is emitted and interpreted downstream as
     * "no valid offset" (snapshot phase), see {@code LakeRecordRecordEmitter}.
     */
    private static final long NO_SYSTEM_COLUMN_VALUE = -1L;

    protected final @Nullable int[][] project;
    protected final RowType paimonRowType;
    private final TableRead tableRead;
    private final @Nullable PaimonSplit split;

    public PaimonRecordReader(
            FileStoreTable fileStoreTable,
            @Nullable PaimonSplit split,
            @Nullable int[][] project,
            @Nullable Predicate predicate)
            throws IOException {
        this.project = project;
        this.split = split;
        ReadBuilder readBuilder = fileStoreTable.newReadBuilder();
        if (project != null) {
            readBuilder = applyProject(readBuilder, project);
        }

        if (predicate != null) {
            readBuilder.withFilter(predicate);
        }

        this.tableRead = readBuilder.newRead().executeFilter();
        this.paimonRowType = readBuilder.readType();
    }

    @Override
    public CloseableIterator<LogRecord> read() throws IOException {
        if (split == null) {
            return new PaimonRowAsFlussRecordIterator(
                    org.apache.paimon.utils.CloseableIterator.empty(), paimonRowType);
        }
        return new PaimonRowAsFlussRecordIterator(
                tableRead.createReader(split.dataSplit()).toCloseableIterator(), paimonRowType);
    }

    /**
     * Reads the split's rows with their physical positions in their data files. Paimon applies the
     * split's deletion vectors, so the rows they delete are skipped and positions may have gaps.
     * Positions come from Paimon's file iterators, which every raw data file read has. A read that
     * merges rows by key (a split with level-0 files of a table without deletion vectors) has no
     * positions and fails.
     */
    @Override
    public CloseableIterator<RowWithPosResult> readWithPos() throws IOException {
        if (split == null) {
            return CloseableIterator.emptyIterator();
        }
        return new PaimonRowWithPosIterator(tableRead.createReader(split.dataSplit()));
    }

    private ReadBuilder applyProject(ReadBuilder readBuilder, int[][] projects) {
        // The projected column ids reference the user (business) columns; the log offset /
        // timestamp are not read from the lake table, so no system column needs to be projected.
        int[] projectIds = Arrays.stream(projects).mapToInt(project -> project[0]).toArray();
        return readBuilder.withProjection(projectIds);
    }

    /** Iterates the rows of Paimon file batches with their file names and row positions. */
    private static class PaimonRowWithPosIterator implements CloseableIterator<RowWithPosResult> {

        private final org.apache.paimon.reader.RecordReader<InternalRow> reader;
        private final PaimonRowAsFlussRow row = new PaimonRowAsFlussRow();
        private final RowWithPosResult result = new RowWithPosResult();
        private @Nullable FileRecordIterator<InternalRow> batch;
        private boolean ready;
        private boolean end;

        PaimonRowWithPosIterator(org.apache.paimon.reader.RecordReader<InternalRow> reader) {
            this.reader = reader;
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean hasNext() {
            if (ready) {
                return true;
            }
            if (end) {
                return false;
            }
            try {
                while (true) {
                    if (batch != null) {
                        InternalRow next = batch.next();
                        if (next != null) {
                            result.set(
                                    row.replaceRow(next),
                                    batch.returnedPosition(),
                                    batch.filePath().getName());
                            ready = true;
                            return true;
                        }
                        batch.releaseBatch();
                        batch = null;
                    }
                    org.apache.paimon.reader.RecordReader.RecordIterator<InternalRow> nextBatch =
                            reader.readBatch();
                    if (nextBatch == null) {
                        end = true;
                        return false;
                    }
                    if (!(nextBatch instanceof FileRecordIterator)) {
                        nextBatch.releaseBatch();
                        throw new IllegalStateException(
                                "Row positions need a raw file read, but Paimon returned "
                                        + nextBatch.getClass().getName());
                    }
                    batch = (FileRecordIterator<InternalRow>) nextBatch;
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read Paimon rows with positions.", e);
            }
        }

        @Override
        public RowWithPosResult next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            ready = false;
            return result;
        }

        @Override
        public void close() {
            try {
                if (batch != null) {
                    batch.releaseBatch();
                    batch = null;
                }
                reader.close();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to close Paimon reader.", e);
            }
        }
    }

    /** Iterator for paimon row as fluss record. */
    public static class PaimonRowAsFlussRecordIterator implements CloseableIterator<LogRecord> {

        private final org.apache.paimon.utils.CloseableIterator<InternalRow> paimonRowIterator;

        private final ProjectedRow projectedRow;
        private final PaimonRowAsFlussRow paimonRowAsFlussRow;

        public PaimonRowAsFlussRecordIterator(
                org.apache.paimon.utils.CloseableIterator<InternalRow> paimonRowIterator,
                RowType paimonRowType) {
            this.paimonRowIterator = paimonRowIterator;

            // A legacy table read without projection still exposes its three trailing system
            // columns; trim them so only the business columns are emitted. A clean table (or any
            // projected read) has no system columns to trim.
            int fieldCount = paimonRowType.getFieldCount();
            int businessFieldCount =
                    PaimonUtils.isLegacyTable(paimonRowType)
                            ? fieldCount - LEGACY_SYSTEM_COLUMNS.size()
                            : fieldCount;
            projectedRow = ProjectedRow.from(IntStream.range(0, businessFieldCount).toArray());
            paimonRowAsFlussRow = new PaimonRowAsFlussRow();
        }

        @Override
        public void close() {
            try {
                paimonRowIterator.close();
            } catch (Exception e) {
                throw new RuntimeException("Fail to close iterator.", e);
            }
        }

        @Override
        public boolean hasNext() {
            return paimonRowIterator.hasNext();
        }

        @Override
        public LogRecord next() {
            InternalRow paimonRow = paimonRowIterator.next();
            ChangeType changeType = toChangeType(paimonRow.getRowKind());
            return new GenericRecord(
                    NO_SYSTEM_COLUMN_VALUE,
                    NO_SYSTEM_COLUMN_VALUE,
                    changeType,
                    projectedRow.replaceRow(paimonRowAsFlussRow.replaceRow(paimonRow)));
        }
    }
}
