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
import org.apache.fluss.client.table.scanner.batch.IndexedRowIterator;
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketPartReader.ReadStats;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.RowWithPosResult;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Reads the lake files of a resolved part, one lake split at a time from the part's current one,
 * without the rows its masks hide. A part read without masks (lake only) has all rows read.
 */
@Internal
final class DvPartLakeReader {

    private final DvBucketPart part;
    private final DvBucketPart resolved;
    @Nullable private final DvBucketMasks masks;
    private final DvBucketPartReader.Context context;
    private final ReadStats stats;

    private int nextLakeSplitIndex;
    @Nullable private CloseableIterator<InternalRow> currentLakeRows;
    private boolean lakeFinished;

    DvPartLakeReader(
            DvBucketPart part,
            DvBucketPart resolved,
            @Nullable DvBucketMasks masks,
            DvBucketPartReader.Context context,
            ReadStats stats) {
        this.part = part;
        this.resolved = resolved;
        this.masks = masks;
        this.context = context;
        this.stats = stats;
        this.nextLakeSplitIndex = resolved.getCurrentLakeSplitIndex();
    }

    /** The rows of the next lake split with rows left, null once all lake splits are read. */
    @Nullable
    CloseableIterator<InternalRow> poll() throws IOException {
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
        // opening the split, but for fetching masks
        long start = System.nanoTime();
        long fetchedBefore = stats.maskFetchNanos;
        stats.lakeSplits++;
        try {
            List<String> fileNames = DvLakeCalls.fileNames(lakeSplit);
            if (masks != null && fileNames == null) {
                throw new IOException(
                        String.format(
                                "Lake split %s of %s names no data files, by which its masks are"
                                        + " keyed.",
                                lakeSplit, part.describe()));
            }
            Map<String, Roaring64Bitmap> fileMasks =
                    masks == null ? Collections.emptyMap() : masks.lakeMasksOf(fileNames);
            // created last, so that the iterator of its rows owns it from the start and closes it
            RecordReader reader =
                    context.lakeSource()
                            .createRecordReader(
                                    (LakeSource.ReaderContext<LakeSplit>) () -> lakeSplit);
            if (fileMasks.isEmpty()) {
                currentLakeRows = new LakeRows(DvLakeCalls.read(reader), index, stats);
            } else {
                currentLakeRows =
                        new MaskedLakeRows(
                                DvLakeCalls.readWithPos(reader),
                                fileMasks,
                                fileNames,
                                index,
                                stats);
            }
        } finally {
            stats.lakeOpenNanos +=
                    System.nanoTime() - start - (stats.maskFetchNanos - fetchedBefore);
        }
    }

    void close() throws IOException {
        if (currentLakeRows != null) {
            currentLakeRows.close();
        }
    }

    /** The rows of one lake split read without masks. */
    private static final class LakeRows implements IndexedRowIterator {

        private final CloseableIterator<LogRecord> records;
        private final int index;
        private final ReadStats stats;
        private boolean closed;

        LakeRows(CloseableIterator<LogRecord> records, int index, ReadStats stats) {
            this.records = records;
            this.index = index;
            this.stats = stats;
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
            InternalRow row = records.next().getRow();
            stats.lakeRowsRead++;
            return row;
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
        private final ReadStats stats;
        @Nullable private InternalRow nextRow;
        private boolean closed;

        MaskedLakeRows(
                CloseableIterator<RowWithPosResult> delegate,
                Map<String, Roaring64Bitmap> fileMasks,
                @Nullable List<String> fileNames,
                int index,
                ReadStats stats) {
            this.delegate = delegate;
            this.fileMasks = fileMasks;
            this.singleFileMask =
                    fileNames != null && fileNames.size() == 1
                            ? fileMasks.get(fileNames.get(0))
                            : null;
            this.index = index;
            this.stats = stats;
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
                stats.lakeRowsRead++;
                String fileName = result.getFileName();
                Roaring64Bitmap mask = fileName == null ? singleFileMask : fileMasks.get(fileName);
                if (mask != null && mask.contains(result.getPos())) {
                    stats.lakeRowsMasked++;
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
}
