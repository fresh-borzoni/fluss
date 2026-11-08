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
import org.apache.fluss.lake.dv.DeletionVectorCapabilities;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.RecordReaders;
import org.apache.fluss.lake.source.RowWithPosResult;
import org.apache.fluss.lake.source.SupportsDataFiles;
import org.apache.fluss.lake.source.SupportsRewritePlanning;
import org.apache.fluss.lake.source.SupportsRowPositions;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/**
 * The calls of the read core on the lake plugin that a union read with deletion vectors needs
 * beyond a plain lake read: rows with their positions ({@link SupportsRowPositions}), the commit a
 * snapshot id names ({@link SupportsRewritePlanning}), and the data files and rows of a lake split
 * ({@link SupportsDataFiles}). They are capabilities of a lake, so the read core makes them here
 * only, and fails with a clear error on a lake without them.
 */
@Internal
final class DvLakeCalls {

    private DvLakeCalls() {}

    /**
     * The lake source of a table with deletion vectors as one that names the commits of snapshot
     * ids. Checked once, where the read core gets the lake source.
     */
    static SupportsRewritePlanning<LakeSplit> rewritePlanning(
            LakeSource<LakeSplit> lakeSource, TablePath tablePath) {
        return DeletionVectorCapabilities.rewritePlanning(lakeSource, tablePath);
    }

    /**
     * Reads the rows of {@code lakeSplit}, each with its data file and position in it. Closing the
     * iterator closes the reader it creates.
     */
    static CloseableIterator<RowWithPosResult> readWithPos(
            LakeSource<LakeSplit> lakeSource, LakeSplit lakeSplit) throws IOException {
        return readWithPos(
                lakeSource.createRecordReader(
                        (LakeSource.ReaderContext<LakeSplit>) () -> lakeSplit));
    }

    /**
     * Reads the rows of {@code reader}, each with its data file and position in it. The iterator
     * owns the reader: closing it, or a failure to open it, closes the reader.
     */
    static CloseableIterator<RowWithPosResult> readWithPos(RecordReader reader) throws IOException {
        return RecordReaders.readWithPos(reader);
    }

    /** Reads the rows of {@code reader}; the iterator owns the reader, as for readWithPos. */
    static CloseableIterator<LogRecord> read(RecordReader reader) throws IOException {
        return RecordReaders.read(reader);
    }

    /** The commit lake snapshot {@code snapshotId} names now, null if the lake does not tell. */
    @Nullable
    static String snapshotIdentity(SupportsRewritePlanning<?> lakeSource, long snapshotId)
            throws IOException {
        return lakeSource.snapshotIdentity(snapshotId);
    }

    /**
     * The names of the data files of {@code lakeSplit}, by which masks are keyed; null if unknown.
     */
    @Nullable
    static List<String> fileNames(LakeSplit lakeSplit) {
        return DeletionVectorCapabilities.dataFiles(lakeSplit).fileNames();
    }

    /** The rows of {@code lakeSplit}, -1 if unknown. */
    static long rowCount(LakeSplit lakeSplit) {
        return DeletionVectorCapabilities.dataFiles(lakeSplit).rowCount();
    }
}
