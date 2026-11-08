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

package org.apache.fluss.lake.dv.index;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.utils.FileUtils;

import io.github.fluss_contrib.rocksdb.EnvOptions;
import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.ReadOptions;
import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.RocksDBException;
import io.github.fluss_contrib.rocksdb.SstFileReader;
import io.github.fluss_contrib.rocksdb.SstFileReaderIterator;
import io.github.fluss_contrib.rocksdb.SstFileWriter;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.TreeMap;

/**
 * Sorts the position index entries of lake files, given in any order, into ascending unsigned key
 * order with bounded heap. It holds at most {@code maxBuffered} entries and writes a full buffer to
 * a sorted run, a local SST; {@link #sorted()} merges the sorted runs. Every key must occur once,
 * as a key has one live row; a second occurrence fails.
 */
@Internal
public final class SpillingPositionSorter implements Closeable {

    /**
     * The entries a sorter of the tiering service holds before it spills a sorted run. Its writers
     * buffer their rounds in the task's shared memory as well, so a sorter keeps a bounded share.
     */
    public static final int DEFAULT_MAX_BUFFERED = 500_000;

    /** Builds the error for a key that occurs twice, at {@code first} and {@code second}. */
    @FunctionalInterface
    public interface DuplicateKeyError {
        RuntimeException of(byte[] key, FilePos first, FilePos second);
    }

    private final File dir;
    private final int maxBuffered;
    private final DuplicateKeyError duplicateKeyError;
    private final List<File> runs = new ArrayList<>();
    private final List<AutoCloseable> openReaders = new ArrayList<>();
    private TreeMap<byte[], FilePos> buffer = newBuffer();
    private long size;

    /**
     * Creates a sorter whose sorted runs go to a new directory under {@code localTmpDir}, or under
     * the JVM's temporary directory if null.
     */
    public SpillingPositionSorter(
            @javax.annotation.Nullable File localTmpDir,
            int maxBuffered,
            DuplicateKeyError duplicateKeyError)
            throws IOException {
        if (maxBuffered <= 0) {
            throw new IllegalArgumentException("maxBuffered must be positive: " + maxBuffered);
        }
        if (localTmpDir == null) {
            this.dir = Files.createTempDirectory("fluss-pkidx-sort-").toFile();
        } else {
            Files.createDirectories(localTmpDir.toPath());
            this.dir =
                    Files.createTempDirectory(localTmpDir.toPath(), "fluss-pkidx-sort-").toFile();
        }
        this.maxBuffered = maxBuffered;
        this.duplicateKeyError = duplicateKeyError;
    }

    private static TreeMap<byte[], FilePos> newBuffer() {
        return new TreeMap<>(SpillingPositionSorter::compareUnsigned);
    }

    /** Compares keys as unsigned bytes, the order of RocksDB's default comparator. */
    private static int compareUnsigned(byte[] a, byte[] b) {
        int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            int c = (a[i] & 0xff) - (b[i] & 0xff);
            if (c != 0) {
                return c;
            }
        }
        return a.length - b.length;
    }

    /** Adds an entry; the key is not copied. */
    public void add(byte[] key, FilePos pos) throws IOException {
        FilePos other = buffer.put(key, pos);
        if (other != null) {
            throw duplicateKeyError.of(key, other, pos);
        }
        size++;
        if (buffer.size() >= maxBuffered) {
            spill();
        }
    }

    /** The number of entries added. */
    public long size() {
        return size;
    }

    /** The number of sorted runs written so far. */
    public int runs() {
        return runs.size();
    }

    private void spill() throws IOException {
        File run = new File(dir, "run-" + runs.size() + ".sst");
        RocksDB.loadLibrary();
        try (EnvOptions envOptions = new EnvOptions();
                Options options = new Options();
                SstFileWriter writer = new SstFileWriter(envOptions, options)) {
            writer.open(run.getPath());
            for (Map.Entry<byte[], FilePos> e : buffer.entrySet()) {
                writer.put(e.getKey(), e.getValue().encode());
            }
            writer.finish();
        } catch (RocksDBException e) {
            throw new IOException(
                    "Failed to write a sorted run of the position index to " + run, e);
        }
        runs.add(run);
        buffer = newBuffer();
    }

    /**
     * Returns every entry once, in ascending unsigned key order. Fails on a key that occurs in two
     * sorted runs. Without sorted runs, returns the buffer as is.
     */
    public Iterator<Map.Entry<byte[], FilePos>> sorted() throws IOException {
        if (runs.isEmpty()) {
            return buffer.entrySet().iterator();
        }
        if (!buffer.isEmpty()) {
            spill();
        }
        PriorityQueue<RunCursor> heads =
                new PriorityQueue<>(
                        (a, b) -> {
                            int c = compareUnsigned(a.key, b.key);
                            return c != 0 ? c : Integer.compare(a.run, b.run);
                        });
        try {
            for (int i = 0; i < runs.size(); i++) {
                Options options = new Options();
                openReaders.add(options);
                SstFileReader reader = new SstFileReader(options);
                openReaders.add(reader);
                ReadOptions readOptions = new ReadOptions();
                openReaders.add(readOptions);
                reader.open(runs.get(i).getPath());
                SstFileReaderIterator it = reader.newIterator(readOptions);
                openReaders.add(it);
                it.seekToFirst();
                if (it.isValid()) {
                    heads.add(new RunCursor(i, it));
                }
            }
        } catch (RocksDBException e) {
            throw new IOException("Failed to read the sorted runs of the position index", e);
        }
        return new Iterator<Map.Entry<byte[], FilePos>>() {
            @Override
            public boolean hasNext() {
                return !heads.isEmpty();
            }

            @Override
            public Map.Entry<byte[], FilePos> next() {
                if (heads.isEmpty()) {
                    throw new NoSuchElementException();
                }
                RunCursor head = heads.poll();
                byte[] key = head.key;
                FilePos pos = head.pos;
                advance(head);
                RunCursor same = heads.peek();
                if (same != null && compareUnsigned(same.key, key) == 0) {
                    throw duplicateKeyError.of(key, pos, same.pos);
                }
                return new AbstractMap.SimpleImmutableEntry<>(key, pos);
            }

            private void advance(RunCursor cursor) {
                cursor.it.next();
                if (cursor.it.isValid()) {
                    cursor.read();
                    heads.add(cursor);
                } else {
                    try {
                        cursor.it.status();
                    } catch (RocksDBException e) {
                        throw new UncheckedIOException(
                                new IOException("Failed to read a sorted run", e));
                    }
                }
            }
        };
    }

    @Override
    public void close() {
        // in reverse order of creation: iterators before their readers before their options
        for (int i = openReaders.size() - 1; i >= 0; i--) {
            try {
                openReaders.get(i).close();
            } catch (Exception e) {
                // best effort
            }
        }
        openReaders.clear();
        FileUtils.deleteDirectoryQuietly(dir);
    }

    /** The current entry of a sorted run. */
    private static final class RunCursor {
        final int run;
        final SstFileReaderIterator it;
        byte[] key;
        FilePos pos;

        RunCursor(int run, SstFileReaderIterator it) {
            this.run = run;
            this.it = it;
            read();
        }

        void read() {
            key = it.key();
            pos = FilePos.decode(it.value());
        }
    }
}
