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

package org.apache.fluss.lake.source;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.utils.CloseableIterator;

import java.io.IOException;

/**
 * An interface for reading records from {@link LakeSplit}.
 *
 * <p>Implementations of this interface provide an iterator-style access to records, allowing
 * efficient sequential reading of potentially large datasets without loading all data into memory
 * at once. The reading should consider the pushed-down optimizations (project, filters, limits,
 * etc.) from {@link LakeSource}.
 *
 * @since 0.8
 */
@PublicEvolving
public interface RecordReader {

    /**
     * Read a {@link LakeSplit} into a closeable iterator.
     *
     * @return the closeable iterator of records
     * @throws IOException if an I/O error occurs
     */
    CloseableIterator<LogRecord> read() throws IOException;

    /**
     * Reads the split and returns each row with its physical row position, as a lazy iterator.
     *
     * <p>Like {@link #read()}, it respects the projection set with {@link
     * LakeSource#withProject(int[][])}. The caller should project only the columns it needs before
     * creating the reader, e.g. only the primary key columns to index a data file.
     *
     * <p>The position is the physical row position in the underlying data file. Deletes already
     * committed to the lake are applied, so the rows they delete are skipped and positions may have
     * gaps.
     *
     * <p>The {@link RowWithPosResult} the iterator returns may be reused across calls, so callers
     * extract the values they need before advancing.
     *
     * @return a closeable iterator of rows with their physical positions
     * @throws IOException if an I/O error occurs
     */
    default CloseableIterator<RowWithPosResult> readWithPos() throws IOException {
        throw new UnsupportedOperationException("readWithPos is not supported");
    }
}
