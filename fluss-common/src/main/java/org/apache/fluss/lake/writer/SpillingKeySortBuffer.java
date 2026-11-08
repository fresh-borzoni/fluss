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

package org.apache.fluss.lake.writer;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.FileUtils;

import javax.annotation.Nullable;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * An external sort buffer of versioned keys. It collects (key, offset, value) records, sorts them
 * by key (unsigned bytes, the order of the position index) and then by offset, and spills sorted
 * runs to local disk when the {@link WriterMemoryPool} it shares with other writers asks it to.
 * {@link #lastVersions()} merges the sorted runs and the records in memory and returns the last
 * version of every key, the one with the highest offset, in key order. A null value is a delete.
 * Each sorted run keeps only the last version of its keys, so a key changed many times takes the
 * space of one version per sorted run.
 *
 * <p>At most {@link #MAX_FAN_IN} sorted runs are merged at once. When that many are spilled, the
 * smaller half of them is merged into one sorted run first.
 *
 * <p>Not thread-safe: the buffer and the other owners of its pool run on one thread.
 */
@Internal
public final class SpillingKeySortBuffer implements WriterMemoryPool.MemoryOwner, Closeable {

    /** The most runs merged at once, each reading through a buffer of {@link #IO_BUFFER} bytes. */
    @VisibleForTesting static final int MAX_FAN_IN = 64;

    private static final int IO_BUFFER = 32 * 1024;
    // a record: key length, key, offset, value length (-1: delete), value
    private static final int RECORD_HEADER = 4 + 8 + 4;
    // heap bytes of a byte[] besides its data, and of the record's slot in the array
    private static final int ARRAY_OVERHEAD = 16;
    private static final int SLOT_BYTES = 8;
    private static final int MIN_SLOTS = 1024;

    private static final Comparator<byte[]> ORDER = SpillingKeySortBuffer::compareRecords;

    private final WriterMemoryPool pool;
    private final String[] tmpDirs;
    private final boolean uniqueKeys;

    private byte[][] records = new byte[0][];
    private int size;
    // reserved heap bytes of the records in memory and of the array holding them
    private long recordBytes;
    private long arrayBytes;
    // encoded bytes of the records in memory
    private long recordLengths;

    private final List<Run> runs = new ArrayList<>();
    @Nullable private File spillDir;
    private int nextRun;
    private long spills;
    private boolean merged;
    private boolean closed;

    /**
     * @param pool the memory the buffer shares with the other writers of its task
     * @param tmpDirs local directories to spill to, one picked at random; null for the JVM's
     * @param uniqueKeys whether every key is added once; a key added twice then fails the buffer
     */
    public SpillingKeySortBuffer(
            WriterMemoryPool pool, @Nullable String[] tmpDirs, boolean uniqueKeys) {
        this.pool = pool;
        this.tmpDirs =
                tmpDirs == null || tmpDirs.length == 0
                        ? new String[] {System.getProperty("java.io.tmpdir")}
                        : tmpDirs;
        this.uniqueKeys = uniqueKeys;
        pool.register(this);
    }

    /**
     * Adds a version of a key, copying the key and the value. A null {@code value} is a delete.
     *
     * @param offset the version's order among the versions of the key: the last version is the one
     *     with the highest offset
     */
    public void add(byte[] key, long offset, @Nullable byte[] value) throws IOException {
        add(key, 0, key.length, offset, value, 0, value == null ? 0 : value.length);
    }

    public void add(
            byte[] key,
            int keyOffset,
            int keyLength,
            long offset,
            @Nullable byte[] value,
            int valueOffset,
            int valueLength)
            throws IOException {
        checkState(!merged && !closed, "The buffer was merged or closed.");
        int length = RECORD_HEADER + keyLength + (value == null ? 0 : valueLength);
        byte[] record = new byte[length];
        putInt(record, 0, keyLength);
        System.arraycopy(key, keyOffset, record, 4, keyLength);
        putLong(record, 4 + keyLength, offset);
        putInt(record, 12 + keyLength, value == null ? -1 : valueLength);
        if (value != null) {
            System.arraycopy(value, valueOffset, record, RECORD_HEADER + keyLength, valueLength);
        }
        while (size == records.length) {
            int slots = Math.max(MIN_SLOTS, records.length * 2);
            long growth = (long) SLOT_BYTES * (slots - records.length);
            // may spill this buffer, which empties it
            pool.reserve(this, growth);
            if (size < records.length) {
                pool.release(growth);
                continue;
            }
            records = Arrays.copyOf(records, slots);
            arrayBytes += growth;
        }
        long bytes = heapBytes(length);
        // may spill this buffer, which keeps room for a record
        pool.reserve(this, bytes);
        records[size++] = record;
        recordBytes += bytes;
        recordLengths += length;
    }

    /** The records in memory and in sorted runs on disk; a sorted run holds one version per key. */
    public long records() {
        long n = size;
        for (Run run : runs) {
            n += run.records;
        }
        return n;
    }

    /**
     * At least the number of distinct keys added. A key in several sorted runs, or added several
     * times since the last spill, is counted that many times.
     */
    public long distinctKeysUpperBound() {
        return records();
    }

    /** The encoded bytes of the records in memory and on local disk. */
    public long bytes() {
        long n = recordLengths;
        for (Run run : runs) {
            n += run.bytes;
        }
        return n;
    }

    /** How many times the buffer spilled its records. */
    public long spills() {
        return spills;
    }

    /** The number of sorted runs on local disk. */
    public int runs() {
        return runs.size();
    }

    @VisibleForTesting
    @Nullable
    File spillDir() {
        return spillDir;
    }

    @Override
    public long spillableBytes() {
        return size == 0 || merged || closed
                ? 0
                : recordBytes + arrayBytes - (long) SLOT_BYTES * MIN_SLOTS;
    }

    @Override
    public void spill() throws IOException {
        if (size == 0 || merged || closed) {
            return;
        }
        sortAndFold();
        Run run = newRun();
        try (RunWriter writer = new RunWriter(run)) {
            for (int i = 0; i < size; i++) {
                writer.write(records[i]);
            }
        }
        runs.add(run);
        spills++;
        clearMemory();
        if (runs.size() >= MAX_FAN_IN) {
            mergeSmallerRuns();
        }
    }

    /**
     * Returns the last version of every key, in key order. The buffer takes no more records
     * afterwards. The iterator reuses its entry, which is valid until the next {@code next()} call.
     */
    public CloseableIterator<Entry> lastVersions() throws IOException {
        checkState(!merged && !closed, "The buffer was merged or closed.");
        merged = true;
        sortAndFold();
        List<Cursor> cursors = new ArrayList<>(runs.size() + 1);
        try {
            for (Run run : runs) {
                cursors.add(new RunCursor(run));
            }
        } catch (IOException e) {
            for (Cursor c : cursors) {
                c.close();
            }
            throw e;
        }
        if (size > 0) {
            cursors.add(new MemoryCursor(records, size));
        }
        return new LastVersions(cursors, uniqueKeys);
    }

    /** Deletes the sorted runs on disk and releases the memory. */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        clearMemory();
        pool.release(arrayBytes);
        arrayBytes = 0;
        records = new byte[0][];
        pool.unregister(this);
        runs.clear();
        if (spillDir != null) {
            FileUtils.deleteDirectoryQuietly(spillDir);
        }
    }

    // ------------------------------------------------------------------------------------------

    /** Sorts the records in memory and keeps the last version of every key. */
    private void sortAndFold() {
        Arrays.sort(records, 0, size, ORDER);
        int kept = 0;
        for (int i = 0; i < size; i++) {
            if (kept > 0 && sameKey(records[kept - 1], records[i])) {
                if (uniqueKeys) {
                    throw duplicateKey(records[i]);
                }
                // sorted by offset: the later version replaces the earlier one
                releaseRecord(records[kept - 1]);
                records[kept - 1] = records[i];
            } else {
                records[kept++] = records[i];
            }
        }
        for (int i = kept; i < size; i++) {
            records[i] = null;
        }
        size = kept;
    }

    private void releaseRecord(byte[] record) {
        long bytes = heapBytes(record.length);
        recordBytes -= bytes;
        recordLengths -= record.length;
        pool.release(bytes);
    }

    private void clearMemory() {
        pool.release(recordBytes);
        recordBytes = 0;
        recordLengths = 0;
        size = 0;
        long kept = (long) SLOT_BYTES * Math.min(MIN_SLOTS, records.length);
        if (records.length > MIN_SLOTS) {
            records = new byte[MIN_SLOTS][];
        } else {
            Arrays.fill(records, null);
        }
        pool.release(arrayBytes - kept);
        arrayBytes = kept;
    }

    /** Merges the smaller half of the sorted runs into one, so no merge opens too many files. */
    private void mergeSmallerRuns() throws IOException {
        List<Run> sorted = new ArrayList<>(runs);
        sorted.sort(Comparator.comparingLong(r -> r.bytes));
        List<Run> inputs = new ArrayList<>(sorted.subList(0, MAX_FAN_IN / 2));
        List<Cursor> cursors = new ArrayList<>(inputs.size());
        Run output = newRun();
        try {
            for (Run run : inputs) {
                cursors.add(new RunCursor(run));
            }
            try (LastVersions versions = new LastVersions(cursors, uniqueKeys);
                    RunWriter writer = new RunWriter(output)) {
                cursors = null;
                while (versions.hasNext()) {
                    writer.write(versions.next().record);
                }
            }
        } finally {
            if (cursors != null) {
                for (Cursor c : cursors) {
                    c.close();
                }
            }
        }
        runs.removeAll(inputs);
        for (Run run : inputs) {
            if (!run.file.delete()) {
                throw new IOException("Could not delete the merged run " + run.file);
            }
        }
        // The order of sorted runs does not matter: the merge orders a key's versions by offset.
        runs.add(output);
    }

    private Run newRun() throws IOException {
        if (spillDir == null) {
            String tmp = tmpDirs[ThreadLocalRandom.current().nextInt(tmpDirs.length)];
            File dir = new File(tmp, "fluss-sort-" + UUID.randomUUID());
            if (!dir.mkdirs()) {
                throw new IOException("Could not create the spill directory " + dir);
            }
            spillDir = dir;
        }
        return new Run(new File(spillDir, "run-" + nextRun++));
    }

    private static long heapBytes(int length) {
        return ((ARRAY_OVERHEAD + length + 7L) & ~7L) + SLOT_BYTES;
    }

    private static IllegalStateException duplicateKey(byte[] record) {
        return new IllegalStateException(
                "A key was added twice to a buffer of unique keys: "
                        + Arrays.toString(Arrays.copyOfRange(record, 4, 4 + getInt(record, 0))));
    }

    // ------------------------------------------------------------------------------------------
    // Records
    // ------------------------------------------------------------------------------------------

    /** A version of a key, as {@link #lastVersions()} returns it. */
    public static final class Entry {
        private byte[] record;

        private Entry() {}

        public byte[] key() {
            return Arrays.copyOfRange(record, 4, 4 + keyLength());
        }

        public long offset() {
            return getLong(record, 4 + keyLength());
        }

        public boolean isDelete() {
            return valueLength() < 0;
        }

        /** The array holding the value, at {@link #valueOffset()}. */
        public byte[] valueArray() {
            return record;
        }

        public int valueOffset() {
            return RECORD_HEADER + keyLength();
        }

        /** The value's length; -1 for a delete. */
        public int valueLength() {
            return getInt(record, 12 + keyLength());
        }

        private int keyLength() {
            return getInt(record, 0);
        }
    }

    @VisibleForTesting
    static int compareRecords(byte[] a, byte[] b) {
        int keyA = getInt(a, 0);
        int keyB = getInt(b, 0);
        int n = Math.min(keyA, keyB);
        for (int i = 0; i < n; i++) {
            int c = (a[4 + i] & 0xff) - (b[4 + i] & 0xff);
            if (c != 0) {
                return c;
            }
        }
        if (keyA != keyB) {
            return keyA - keyB;
        }
        return Long.compare(getLong(a, 4 + keyA), getLong(b, 4 + keyB));
    }

    private static boolean sameKey(byte[] a, byte[] b) {
        int keyA = getInt(a, 0);
        if (keyA != getInt(b, 0)) {
            return false;
        }
        for (int i = 0; i < keyA; i++) {
            if (a[4 + i] != b[4 + i]) {
                return false;
            }
        }
        return true;
    }

    private static void putInt(byte[] b, int at, int v) {
        b[at] = (byte) (v >>> 24);
        b[at + 1] = (byte) (v >>> 16);
        b[at + 2] = (byte) (v >>> 8);
        b[at + 3] = (byte) v;
    }

    private static int getInt(byte[] b, int at) {
        return ((b[at] & 0xff) << 24)
                | ((b[at + 1] & 0xff) << 16)
                | ((b[at + 2] & 0xff) << 8)
                | (b[at + 3] & 0xff);
    }

    private static void putLong(byte[] b, int at, long v) {
        putInt(b, at, (int) (v >>> 32));
        putInt(b, at + 4, (int) v);
    }

    private static long getLong(byte[] b, int at) {
        return ((long) getInt(b, at) << 32) | (getInt(b, at + 4) & 0xffffffffL);
    }

    // ------------------------------------------------------------------------------------------
    // Runs and the merge
    // ------------------------------------------------------------------------------------------

    private static final class Run {
        final File file;
        long records;
        long bytes;

        Run(File file) {
            this.file = file;
        }
    }

    private static final class RunWriter implements Closeable {
        private final Run run;
        private final DataOutputStream out;

        RunWriter(Run run) throws IOException {
            this.run = run;
            this.out =
                    new DataOutputStream(
                            new BufferedOutputStream(new FileOutputStream(run.file), IO_BUFFER));
        }

        void write(byte[] record) throws IOException {
            out.writeInt(record.length);
            out.write(record);
            run.records++;
            run.bytes += 4 + record.length;
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }

    /** A sorted source of records, each key once. */
    private interface Cursor extends Closeable {
        /** The current record, or null when exhausted. */
        @Nullable
        byte[] current();

        void advance() throws IOException;

        @Override
        void close();
    }

    private static final class MemoryCursor implements Cursor {
        private final byte[][] records;
        private final int size;
        private int next;

        MemoryCursor(byte[][] records, int size) {
            this.records = records;
            this.size = size;
        }

        @Override
        public byte[] current() {
            return next < size ? records[next] : null;
        }

        @Override
        public void advance() {
            next++;
        }

        @Override
        public void close() {}
    }

    private static final class RunCursor implements Cursor {
        private final DataInputStream in;
        @Nullable private byte[] current;

        RunCursor(Run run) throws IOException {
            this.in =
                    new DataInputStream(
                            new BufferedInputStream(new FileInputStream(run.file), IO_BUFFER));
            try {
                advance();
            } catch (IOException e) {
                close();
                throw e;
            }
        }

        @Override
        public byte[] current() {
            return current;
        }

        @Override
        public void advance() throws IOException {
            int length;
            try {
                length = in.readInt();
            } catch (EOFException e) {
                current = null;
                return;
            }
            byte[] record = new byte[length];
            in.readFully(record);
            current = record;
        }

        @Override
        public void close() {
            try {
                in.close();
            } catch (IOException e) {
                // reading only
            }
        }
    }

    /** Merges sorted cursors and returns the last version of every key. */
    private static final class LastVersions implements CloseableIterator<Entry> {
        private final List<Cursor> cursors;
        private final PriorityQueue<Cursor> queue;
        private final boolean uniqueKeys;
        private final Entry entry = new Entry();

        LastVersions(List<Cursor> cursors, boolean uniqueKeys) {
            this.cursors = cursors;
            this.uniqueKeys = uniqueKeys;
            this.queue =
                    new PriorityQueue<>(
                            Math.max(1, cursors.size()),
                            (a, b) -> compareRecords(a.current(), b.current()));
            for (Cursor c : cursors) {
                if (c.current() != null) {
                    queue.add(c);
                }
            }
        }

        @Override
        public boolean hasNext() {
            return !queue.isEmpty();
        }

        @Override
        public Entry next() {
            if (queue.isEmpty()) {
                throw new NoSuchElementException();
            }
            try {
                byte[] last = poll();
                while (!queue.isEmpty() && sameKey(queue.peek().current(), last)) {
                    if (uniqueKeys) {
                        throw duplicateKey(last);
                    }
                    // the later version, sorted by offset
                    last = poll();
                }
                entry.record = last;
                return entry;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private byte[] poll() throws IOException {
            Cursor c = queue.poll();
            byte[] record = c.current();
            c.advance();
            if (c.current() != null) {
                queue.add(c);
            }
            return record;
        }

        @Override
        public void close() {
            for (Cursor c : cursors) {
                c.close();
            }
            queue.clear();
        }
    }
}
