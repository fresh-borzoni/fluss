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
import org.apache.fluss.utils.CloseableIterator;

import java.io.IOException;

/**
 * A {@link RecordReader} that can read each row with its physical position in its data file.
 * Deletion vectors are keyed by data file and row position, so tables with deletion vectors need it
 * to index the lake's rows and to mask the rows Fluss superseded. Fluss checks a record reader for
 * this capability before it reads positions, and fails with a clear error otherwise.
 *
 * <p>The readers of a lake source that implements {@link SupportsRewritePlanning} implement it;
 * their splits implement {@link SupportsDataFiles}.
 *
 * @since 1.1
 */
@PublicEvolving
public interface SupportsRowPositions {

    /**
     * Reads the split and returns each row with its physical row position, as a lazy iterator.
     *
     * <p>Like {@link RecordReader#read()}, it respects the projection set with {@link
     * LakeSource#withProject(int[][])}. The caller should project only the columns it needs before
     * creating the reader, e.g. only the primary key columns to index a data file.
     *
     * <p>The position is the physical row position in the underlying data file. Deletes already
     * committed to the lake are applied, so the rows they delete are skipped and positions may have
     * gaps.
     *
     * <p>The {@link RowWithPosResult} the iterator returns may be reused across calls, so callers
     * extract the values they need before advancing. The caller closes the iterator and then the
     * reader ({@link RecordReader#close()}).
     *
     * @return a closeable iterator of rows with their physical positions
     * @throws IOException if an I/O error occurs
     */
    CloseableIterator<RowWithPosResult> readWithPos() throws IOException;
}
