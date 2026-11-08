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

package org.apache.fluss.lake.iceberg.maintenance;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.RowPosSstIndex;
import org.apache.fluss.lake.dv.index.CompactionIndexPart;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.lake.dv.index.SpillingPositionSorter;
import org.apache.fluss.lake.iceberg.source.IcebergRecordAsFlussRow;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.encode.KeyEncoder;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.data.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Hands the new positions of the rows a compaction of one bucket rewrites to the position index of
 * a table with deletion vectors, so the rewrite needs no re-index from the lake.
 *
 * <p>The compaction writes every live row of its input files (deletes applied) into new files, one
 * file group after another. Each file group's rows land in its files in write order. This writer
 * encodes the primary key of every written row as the position index does, and sorts the keys in
 * bounded memory, spilling sorted runs to local disk. A key carries its row's number in write
 * order, which gives its file and position once the file group's files are known.
 *
 * <p>When the compaction completes, the writer writes the bucket's index part: every kept key at
 * its new file and position, sorted by key. The part numbers its files from 0 rather than taking
 * ids from the table's counter, because compactions of different buckets run concurrently and a
 * compaction the round gave up on may still run. The tiering committer gives the files their ids
 * when it publishes the index change. On any failure the writer hands nothing over, and the rewrite
 * is re-indexed from the lake.
 */
public class CompactionIndexWriter implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(CompactionIndexWriter.class);

    // the most entries a hand-off holds in memory before it spills a sorted run
    private static final int SORT_BUFFER_ENTRIES = 500_000;

    private final PositionIndexStore store;
    private final TableBucket bucket;
    private final long tableId;
    private final long roundTimestamp;
    private final KeyEncoder keyEncoder;
    private final IcebergRecordAsFlussRow row = new IcebergRecordAsFlussRow();
    @Nullable private final File localTmpDir;
    private final int maxBufferedEntries;

    // created with the first row, so a compaction that rewrites nothing leaves no directory
    @Nullable private SpillingPositionSorter sorter;
    // the rows added so far; a row's number in write order stands in for its position
    private long rows;
    // the number of the current file group's first row
    private long groupStart;
    // the kept files in write order, with the number of their first row and their row count
    private final List<String> files = new ArrayList<>();
    private long[] fileStarts = new long[16];
    private long[] fileRows = new long[16];
    @Nullable private String failure;

    public CompactionIndexWriter(
            PositionIndexStore store,
            TableInfo tableInfo,
            TableBucket bucket,
            long roundTimestamp,
            @Nullable File localTmpDir) {
        this(store, tableInfo, bucket, roundTimestamp, localTmpDir, SORT_BUFFER_ENTRIES);
    }

    @VisibleForTesting
    public CompactionIndexWriter(
            PositionIndexStore store,
            TableInfo tableInfo,
            TableBucket bucket,
            long roundTimestamp,
            @Nullable File localTmpDir,
            int maxBufferedEntries) {
        this.store = store;
        this.bucket = bucket;
        this.tableId = tableInfo.getTableId();
        this.roundTimestamp = roundTimestamp;
        this.localTmpDir = localTmpDir;
        this.maxBufferedEntries = maxBufferedEntries;
        this.keyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        tableInfo.getRowType(),
                        tableInfo.getPhysicalPrimaryKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());
    }

    /** Records the key of a row the current file group writes, in write order. */
    void add(Record record) {
        if (failure != null) {
            return;
        }
        byte[] key = keyEncoder.encodeKey(row.replaceIcebergRecord(record));
        try {
            if (sorter == null) {
                sorter =
                        new SpillingPositionSorter(
                                localTmpDir,
                                maxBufferedEntries,
                                (k, first, second) -> new DuplicateKeyException());
            }
            sorter.add(Arrays.copyOf(key, key.length), new FilePos(0, rows++));
        } catch (DuplicateKeyException e) {
            fail(e.getMessage());
        } catch (IOException e) {
            fail("its keys could not be spilled to local disk: " + e);
        }
    }

    /** Assigns the current file group's rows to the files it wrote, in write order. */
    void groupWritten(DataFile[] dataFiles) {
        if (failure != null) {
            return;
        }
        long written = 0;
        for (DataFile f : dataFiles) {
            written += f.recordCount();
        }
        if (written != rows - groupStart) {
            fail("its files hold " + written + " rows but it wrote " + (rows - groupStart));
            return;
        }
        long start = groupStart;
        for (DataFile f : dataFiles) {
            int i = files.size();
            if (i == fileStarts.length) {
                fileStarts = Arrays.copyOf(fileStarts, 2 * i);
                fileRows = Arrays.copyOf(fileRows, 2 * i);
            }
            files.add(fileName(f.location()));
            fileStarts[i] = start;
            fileRows[i] = f.recordCount();
            start += f.recordCount();
        }
        groupStart = rows;
    }

    /**
     * Forgets the rows of a file group whose files were not kept. They may already be in a sorted
     * run, but their numbers fall in no kept file, so writing the part skips them.
     */
    void groupDiscarded() {
        groupStart = rows;
    }

    /**
     * Writes the bucket's index part for the compaction of snapshot {@code readSnapshotId}, which
     * replaced {@code removedFiles}. Returns null if nothing is handed over.
     */
    @Nullable
    CompactionIndexPart complete(long readSnapshotId, List<String> removedFiles) {
        try {
            if (failure == null && roundTimestamp < 0) {
                fail("the tiering round is unknown");
            }
            if (failure == null) {
                try {
                    return write(readSnapshotId, removedFiles);
                } catch (DuplicateKeyException e) {
                    // found while merging the sorted runs
                    fail(e.getMessage());
                } catch (Exception e) {
                    LOG.warn(
                            "Derived DV: compaction of bucket {} could not hand its positions to"
                                    + " the index; its files are re-indexed from the lake.",
                            bucket,
                            e);
                    return null;
                }
            }
            LOG.info(
                    "Derived DV: compaction of bucket {} hands no positions to the index ({});"
                            + " its files are re-indexed from the lake.",
                    bucket,
                    failure);
            return null;
        } finally {
            close();
        }
    }

    private CompactionIndexPart write(long readSnapshotId, List<String> removedFiles)
            throws IOException {
        long changeId = CompactionIndexPart.changeIdOf(tableId, roundTimestamp, readSnapshotId);
        long kept = 0;
        for (int i = 0; i < files.size(); i++) {
            kept += fileRows[i];
        }
        PositionIndexStore.BucketPart part =
                kept == 0 || sorter == null
                        ? PositionIndexStore.BucketPart.NONE
                        : store.writeBucket(changeId, bucket, new KeptRows(sorter.sorted()));
        List<String> sstNames = new ArrayList<>();
        List<Long> sstSizes = new ArrayList<>();
        for (RowPosSstIndex.SstFileEntry f : part.files) {
            sstNames.add(f.getFileName());
            sstSizes.add(f.getFileSize());
        }
        Map<Integer, String> addedFiles = new HashMap<>();
        for (int i = 0; i < files.size(); i++) {
            addedFiles.put(i, files.get(i));
        }
        LOG.info(
                "Derived DV: compaction of bucket {} hands {} rows in {} files to index change {}"
                        + " ({} sorted runs).",
                bucket,
                kept,
                files.size(),
                changeId,
                sorter == null ? 0 : sorter.runs());
        return new CompactionIndexPart(
                changeId,
                readSnapshotId,
                sstNames,
                sstSizes,
                addedFiles,
                new ArrayList<>(removedFiles));
    }

    /** Deletes the sorted runs; the compaction calls it however it ends. */
    @Override
    public void close() {
        if (sorter != null) {
            sorter.close();
            sorter = null;
        }
    }

    private void fail(String reason) {
        if (failure == null) {
            failure = reason;
        }
        close();
    }

    static String fileName(String location) {
        return location.substring(location.lastIndexOf('/') + 1);
    }

    /** The sorted rows of the kept files at their positions, without discarded file groups. */
    private final class KeptRows implements Iterator<Map.Entry<byte[], FilePos>> {

        private final Iterator<Map.Entry<byte[], FilePos>> sorted;
        @Nullable private Map.Entry<byte[], FilePos> next;

        private KeptRows(Iterator<Map.Entry<byte[], FilePos>> sorted) {
            this.sorted = sorted;
        }

        @Override
        public boolean hasNext() {
            while (next == null && sorted.hasNext()) {
                Map.Entry<byte[], FilePos> e = sorted.next();
                FilePos pos = keptPosition(e.getValue().rowPosition());
                if (pos != null) {
                    next = new AbstractMap.SimpleImmutableEntry<>(e.getKey(), pos);
                }
            }
            return next != null;
        }

        @Override
        public Map.Entry<byte[], FilePos> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Map.Entry<byte[], FilePos> e = next;
            next = null;
            return e;
        }

        /** The file and position of row {@code n}, or null if its file group was discarded. */
        @Nullable
        private FilePos keptPosition(long n) {
            int i = Arrays.binarySearch(fileStarts, 0, files.size(), n);
            i = i >= 0 ? i : -i - 2;
            // an empty file starts where the next one does
            while (i >= 0 && i + 1 < files.size() && fileStarts[i + 1] <= n) {
                i++;
            }
            if (i < 0 || n - fileStarts[i] >= fileRows[i]) {
                return null;
            }
            return new FilePos(i, n - fileStarts[i]);
        }
    }

    /** One live row per key is expected; indexing either of two would leave the other unmasked. */
    private static final class DuplicateKeyException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        private DuplicateKeyException() {
            super("two of its rows have the same key", null, false, false);
        }
    }
}
