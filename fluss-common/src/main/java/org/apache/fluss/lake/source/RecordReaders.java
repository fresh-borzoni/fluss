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
import org.apache.fluss.lake.dv.DeletionVectorCapabilities;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.utils.CloseableIterator;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Reads a {@link RecordReader} so that the returned iterator owns the reader: closing the iterator
 * closes the reader too, and the reader is closed at once if it fails to open its rows. A caller
 * that keeps only the iterator thus releases the reader on every exit.
 */
@Internal
public final class RecordReaders {

    private RecordReaders() {}

    /** Reads {@code reader} with {@link RecordReader#read()}; the iterator closes the reader. */
    public static CloseableIterator<LogRecord> read(RecordReader reader) throws IOException {
        CloseableIterator<LogRecord> records;
        try {
            records = reader.read();
        } catch (IOException | RuntimeException e) {
            closeSuppressed(reader, e);
            throw e;
        }
        return new ReaderOwningIterator<>(records, reader);
    }

    /**
     * Reads {@code reader} with {@link SupportsRowPositions#readWithPos()}; the iterator closes the
     * reader. Fails with a clear error, after closing the reader, if the reader cannot read
     * positions.
     */
    public static CloseableIterator<RowWithPosResult> readWithPos(RecordReader reader)
            throws IOException {
        CloseableIterator<RowWithPosResult> rows;
        try {
            rows = DeletionVectorCapabilities.rowPositions(reader).readWithPos();
        } catch (IOException | RuntimeException e) {
            closeSuppressed(reader, e);
            throw e;
        }
        return new ReaderOwningIterator<>(rows, reader);
    }

    private static void closeSuppressed(RecordReader reader, Exception failure) {
        try {
            reader.close();
        } catch (Exception e) {
            failure.addSuppressed(e);
        }
    }

    /** An iterator that closes its reader after itself, once, however often it is closed. */
    private static final class ReaderOwningIterator<T> implements CloseableIterator<T> {

        private final CloseableIterator<T> rows;
        private final RecordReader reader;
        private boolean closed;

        private ReaderOwningIterator(CloseableIterator<T> rows, RecordReader reader) {
            this.rows = rows;
            this.reader = reader;
        }

        @Override
        public boolean hasNext() {
            return rows.hasNext();
        }

        @Override
        public T next() {
            return rows.next();
        }

        @Override
        public void close() {
            // e.g. a batch is closed by the one it was emitted to and again by its scanner
            if (closed) {
                return;
            }
            closed = true;
            try {
                rows.close();
            } catch (RuntimeException e) {
                closeSuppressed(reader, e);
                throw e;
            }
            try {
                reader.close();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to close the lake record reader.", e);
            }
        }
    }
}
