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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.row.InternalRow;

import javax.annotation.Nullable;

/**
 * A row together with its physical position in the data file, returned by {@link
 * RecordReader#readWithPos()}.
 *
 * <p>The {@link InternalRow} returned by {@link #getRow()} may be reused across iterator calls, so
 * callers extract the values they need before advancing the iterator.
 */
@Internal
public class RowWithPosResult {

    private InternalRow row;
    private long pos;
    @Nullable private String fileName;

    /** Returns the projected row data. */
    public InternalRow getRow() {
        return row;
    }

    /** Returns the physical position of this row in the data file. */
    public long getPos() {
        return pos;
    }

    /** Returns the file name this row belongs to. Null when not available. */
    @Nullable
    public String getFileName() {
        return fileName;
    }

    /** Sets the row, position, and file name. Used by iterator implementations for object reuse. */
    public RowWithPosResult set(InternalRow row, long pos, @Nullable String fileName) {
        this.row = row;
        this.pos = pos;
        this.fileName = fileName;
        return this;
    }
}
