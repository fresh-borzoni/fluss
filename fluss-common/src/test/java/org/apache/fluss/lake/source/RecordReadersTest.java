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

import org.apache.fluss.record.LogRecord;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RecordReaders}: the iterator they return owns its reader. */
class RecordReadersTest {

    @Test
    void testClosingTheIteratorClosesTheReader() throws IOException {
        CountingReader reader = new CountingReader(null);
        CloseableIterator<LogRecord> records = RecordReaders.read(reader);
        assertThat(reader.closes).isZero();
        records.close();
        assertThat(reader.closes).isEqualTo(1);
        assertThat(reader.iteratorCloses).isEqualTo(1);
    }

    /** An iterator closed twice, e.g. by its batch's recycler and by its scanner, closes once. */
    @Test
    void testClosingTheIteratorTwiceClosesTheReaderOnce() throws IOException {
        CountingReader reader = new CountingReader(null);
        CloseableIterator<LogRecord> records = RecordReaders.read(reader);
        records.close();
        records.close();
        assertThat(reader.closes).isEqualTo(1);
        assertThat(reader.iteratorCloses).isEqualTo(1);
    }

    @Test
    void testAFailedReadClosesTheReader() {
        IOException failure = new IOException("cannot open");
        CountingReader reader = new CountingReader(failure);
        assertThatThrownBy(() -> RecordReaders.read(reader)).isSameAs(failure);
        assertThat(reader.closes).isEqualTo(1);
    }

    @Test
    void testAReaderWithoutPositionsIsClosedAndNamesTheCapability() {
        CountingReader reader = new CountingReader(null);
        assertThatThrownBy(() -> RecordReaders.readWithPos(reader))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(SupportsRowPositions.class.getName())
                .hasMessageContaining("table.deletion-vectors.enabled");
        assertThat(reader.closes).isEqualTo(1);
    }

    @Test
    void testAFailingIteratorCloseStillClosesTheReader() throws IOException {
        CountingReader reader = new CountingReader(null);
        reader.failIteratorClose = true;
        CloseableIterator<LogRecord> records = RecordReaders.read(reader);
        assertThatThrownBy(records::close).hasMessage("iterator close");
        assertThat(reader.closes).isEqualTo(1);
    }

    /** A reader that counts how often it and its iterator are closed. */
    private static final class CountingReader implements RecordReader {
        private final IOException readFailure;
        private int closes;
        private int iteratorCloses;
        private boolean failIteratorClose;

        private CountingReader(IOException readFailure) {
            this.readFailure = readFailure;
        }

        @Override
        public CloseableIterator<LogRecord> read() throws IOException {
            if (readFailure != null) {
                throw readFailure;
            }
            CloseableIterator<LogRecord> empty =
                    CloseableIterator.wrap(Collections.<LogRecord>emptyIterator());
            return new CloseableIterator<LogRecord>() {
                @Override
                public void close() {
                    iteratorCloses++;
                    empty.close();
                    if (failIteratorClose) {
                        throw new IllegalStateException("iterator close");
                    }
                }

                @Override
                public boolean hasNext() {
                    return empty.hasNext();
                }

                @Override
                public LogRecord next() {
                    return empty.next();
                }
            };
        }

        @Override
        public void close() {
            closes++;
        }
    }
}
