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

package org.apache.fluss.server.kv.dv;

import org.apache.fluss.utils.crc.Crc32C;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;

/**
 * The file format of one segment of a change list saved with a KV snapshot: the updates and deletes
 * of a bucket at offsets in [from, end), each with the offset of the version it superseded, in
 * offset order. A segment is written once and read by a new leader; it is a fact about the log, so
 * it holds for any lake snapshot the leader covers.
 *
 * <pre>
 * header   magic "FDVC", u8 version (1), i64 table id, i64 partition id or -1, i32 bucket,
 *          i64 from, i64 end, i64 valid-from
 * entries  varlong(offset - previous offset), the first from (from - 1), so always 1 or more;
 *          varlong(offset - superseded), 0 for a superseded version before valid-from (-1);
 *          varint(key length); key bytes, the KV key encoding of the primary key
 * end      varlong 0; i64 entries; i64 CRC-32C of every byte before it
 * </pre>
 *
 * <p>A reader refuses an unknown version, a header of another bucket or range, or with a valid-from
 * offset above the change list's (which is the highest of its segments'), entries out of order or
 * outside [from, end), a wrong count or checksum, and bytes after the end. The caller builds a
 * fresh change list from the entries and installs it only after the whole segment read without
 * error, so a refusal leaves nothing behind.
 */
final class DvChangeListSegment {

    static final int VERSION = 1;
    private static final byte[] MAGIC = {'F', 'D', 'V', 'C'};
    // a corrupt length must not allocate the heap away; primary keys are far smaller
    static final int MAX_KEY_BYTES = 16 * 1024 * 1024;

    private DvChangeListSegment() {}

    /** What a segment describes, written first and checked by the reader. */
    static final class Header {
        final long tableId;
        final long partitionId;
        final int bucket;
        final long from;
        final long end;
        final long validFrom;

        Header(long tableId, long partitionId, int bucket, long from, long end, long validFrom) {
            if (from < 0 || end < from || validFrom < 0) {
                throw new IllegalArgumentException(
                        String.format(
                                "A segment covers [from, end) with from >= 0 and valid-from >= 0,"
                                        + " not [%d, %d) and %d.",
                                from, end, validFrom));
            }
            this.tableId = tableId;
            this.partitionId = partitionId;
            this.bucket = bucket;
            this.from = from;
            this.end = end;
            this.validFrom = validFrom;
        }

        /** Whether {@code other} is the same bucket's segment of the same range. */
        boolean sameRange(Header other) {
            return tableId == other.tableId
                    && partitionId == other.partitionId
                    && bucket == other.bucket
                    && from == other.from
                    && end == other.end;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Header)) {
                return false;
            }
            Header h = (Header) o;
            return tableId == h.tableId
                    && partitionId == h.partitionId
                    && bucket == h.bucket
                    && from == h.from
                    && end == h.end
                    && validFrom == h.validFrom;
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableId, partitionId, bucket, from, end, validFrom);
        }

        @Override
        public String toString() {
            return String.format(
                    "table=%d partition=%d bucket=%d [%d, %d) valid_from=%d",
                    tableId, partitionId, bucket, from, end, validFrom);
        }
    }

    /** Receives the entries of a segment as it is read. */
    interface EntrySink {
        /**
         * One update or delete at {@code offset} of {@code key}, which superseded the version at
         * {@code superseded}, or -1 for a version before the segment's valid-from offset.
         */
        void accept(long offset, long superseded, byte[] key) throws IOException;
    }

    /** A segment that cannot be read: its version is unknown, or its bytes are not a segment. */
    static final class UnreadableSegmentException extends IOException {
        private static final long serialVersionUID = 1L;

        // true for a version this build does not read, false for corrupt bytes
        final boolean unknownVersion;

        UnreadableSegmentException(String message, boolean unknownVersion) {
            super(message);
            this.unknownVersion = unknownVersion;
        }
    }

    /** Writes one segment to a stream it owns; {@link #finish} ends it. */
    static final class Writer implements Closeable {
        private final Header header;
        private final Checksum crc = Crc32C.create();
        private final CountingStream counted;
        private final DataOutputStream out;
        private long last;
        private long entries;
        private boolean finished;

        Writer(OutputStream target, Header header) throws IOException {
            this.header = header;
            this.counted = new CountingStream(new BufferedOutputStream(target, 64 * 1024));
            this.out = new DataOutputStream(new CheckedOutputStream(counted, crc));
            this.last = header.from - 1;
            out.write(MAGIC);
            out.writeByte(VERSION);
            out.writeLong(header.tableId);
            out.writeLong(header.partitionId);
            out.writeInt(header.bucket);
            out.writeLong(header.from);
            out.writeLong(header.end);
            out.writeLong(header.validFrom);
        }

        /** Adds the next change; offsets increase within [from, end). */
        void add(long offset, long superseded, byte[] key) throws IOException {
            if (offset <= last || offset >= header.end) {
                throw new IllegalArgumentException(
                        String.format(
                                "Change at offset %d out of order or outside [%d, %d) after %d.",
                                offset, header.from, header.end, last));
            }
            if (superseded < -1 || superseded >= offset) {
                throw new IllegalArgumentException(
                        String.format(
                                "Change at offset %d supersedes offset %d.", offset, superseded));
            }
            writeUnsignedVarLong(out, offset - last);
            writeUnsignedVarLong(out, superseded < 0 ? 0 : offset - superseded);
            writeUnsignedVarLong(out, key.length);
            out.write(key);
            last = offset;
            entries++;
        }

        long entries() {
            return entries;
        }

        /** Ends the segment and flushes it; returns the segment's bytes. */
        long finish() throws IOException {
            if (!finished) {
                finished = true;
                writeUnsignedVarLong(out, 0);
                out.writeLong(entries);
                out.flush();
                // the checksum covers every byte before it, so it is written past the checked
                // stream
                DataOutputStream tail = new DataOutputStream(counted);
                tail.writeLong(crc.getValue());
                tail.flush();
            }
            return counted.bytes;
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }

    /**
     * Reads a segment, handing its entries to {@code sink} in offset order, and checks it whole.
     * Returns the number of entries. The stream is read to its end and not closed.
     *
     * @throws UnreadableSegmentException if the segment's version is unknown, its header is not of
     *     {@code expected}'s bucket and range or has a valid-from offset above {@code expected}'s,
     *     or its bytes are not a whole segment
     */
    static long read(InputStream in, Header expected, EntrySink sink) throws IOException {
        return read(in, expected, -1, sink);
    }

    /**
     * Reads a segment as {@link #read(InputStream, Header, EntrySink)} does, and checks that it has
     * {@code expectedBytes} bytes, the size its handle names, unless that is negative (not known).
     */
    static long read(InputStream in, Header expected, long expectedBytes, EntrySink sink)
            throws IOException {
        Checksum crc = Crc32C.create();
        CountingInputStream counted = new CountingInputStream(in);
        CheckedInputStream checked =
                new CheckedInputStream(new BufferedInputStream(counted, 64 * 1024), crc);
        DataInputStream data = new DataInputStream(checked);
        try {
            byte[] magic = new byte[MAGIC.length];
            data.readFully(magic);
            for (int i = 0; i < MAGIC.length; i++) {
                if (magic[i] != MAGIC[i]) {
                    throw corrupt("it does not start with the segment magic");
                }
            }
            int version = data.readUnsignedByte();
            if (version != VERSION) {
                throw new UnreadableSegmentException(
                        String.format(
                                "Change list segment of version %d, this build reads version %d.",
                                version, VERSION),
                        true);
            }
            Header header;
            try {
                header =
                        new Header(
                                data.readLong(),
                                data.readLong(),
                                data.readInt(),
                                data.readLong(),
                                data.readLong(),
                                data.readLong());
            } catch (IllegalArgumentException e) {
                throw corrupt("its header " + e.getMessage());
            }
            if (!header.sameRange(expected) || header.validFrom > expected.validFrom) {
                throw corrupt(String.format("its header is %s, expected %s", header, expected));
            }
            long last = header.from - 1;
            long entries = 0;
            while (true) {
                long delta = readUnsignedVarLong(data);
                if (delta == 0) {
                    break;
                }
                long offset = last + delta;
                if (offset < last || offset >= header.end) {
                    throw corrupt(
                            String.format(
                                    "an entry at offset %d is outside [%d, %d)",
                                    offset, header.from, header.end));
                }
                long back = readUnsignedVarLong(data);
                if (back > offset) {
                    throw corrupt(
                            String.format("the entry at %d supersedes before offset 0", offset));
                }
                long keyLength = readUnsignedVarLong(data);
                if (keyLength > MAX_KEY_BYTES) {
                    throw corrupt(
                            String.format(
                                    "the entry at %d has a key of %d bytes", offset, keyLength));
                }
                byte[] key = new byte[(int) keyLength];
                data.readFully(key);
                sink.accept(offset, back == 0 ? -1 : offset - back, key);
                last = offset;
                entries++;
            }
            long written = data.readLong();
            // every byte before the checksum; reading the checksum changes the value, not this
            long computed = crc.getValue();
            long stored = data.readLong();
            if (written != entries) {
                throw corrupt(String.format("it counts %d entries, holds %d", written, entries));
            }
            if (stored != computed) {
                throw corrupt("its checksum does not match");
            }
            if (checked.read() != -1) {
                throw corrupt("it has bytes after its end");
            }
            if (expectedBytes >= 0 && counted.bytes != expectedBytes) {
                throw corrupt(
                        String.format(
                                "it has %d bytes, its handle names %d",
                                counted.bytes, expectedBytes));
            }
            return entries;
        } catch (EOFException e) {
            throw corrupt("it ends early");
        }
    }

    private static UnreadableSegmentException corrupt(String why) {
        return new UnreadableSegmentException("Corrupt change list segment: " + why + ".", false);
    }

    static void writeUnsignedVarLong(DataOutputStream out, long value) throws IOException {
        while ((value & ~0x7FL) != 0) {
            out.writeByte((int) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.writeByte((int) value);
    }

    static long readUnsignedVarLong(DataInputStream in) throws IOException {
        long value = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            int b = in.readUnsignedByte();
            value |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return value;
            }
        }
        throw corrupt("a variable-length number has more than 10 bytes");
    }

    /** Counts the bytes read through it. */
    private static final class CountingInputStream extends java.io.FilterInputStream {
        long bytes;

        CountingInputStream(InputStream in) {
            super(in);
        }

        @Override
        public int read() throws IOException {
            int b = super.read();
            if (b >= 0) {
                bytes++;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = super.read(b, off, len);
            if (n > 0) {
                bytes += n;
            }
            return n;
        }

        @Override
        public long skip(long n) throws IOException {
            long skipped = super.skip(n);
            bytes += skipped;
            return skipped;
        }
    }

    /** Counts the bytes written through it. */
    private static final class CountingStream extends OutputStream {
        private final OutputStream target;
        long bytes;

        CountingStream(OutputStream target) {
            this.target = target;
        }

        @Override
        public void write(int b) throws IOException {
            target.write(b);
            bytes++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            target.write(b, off, len);
            bytes += len;
        }

        @Override
        public void flush() throws IOException {
            target.flush();
        }

        @Override
        public void close() throws IOException {
            target.close();
        }
    }
}
