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

package org.apache.fluss.lake.dv;

import java.util.Objects;

/**
 * An immutable position of a row in a data file: (file_id, row_position).
 *
 * <p>The encoding uses unsigned varint (LEB128) for both fields, concatenated as {@code
 * varint(file_id) || varint(row_position)}. Typical encoded size is 3-5 bytes.
 */
public final class FilePos {

    /**
     * The version of the encoding. The files that hold encoded positions record it: the manifest of
     * an index change, and the format versions of checkpoints and change directories.
     */
    public static final int ENCODING_VERSION = 1;

    private final int fileId;
    private final long rowPosition;

    public FilePos(int fileId, long rowPosition) {
        this.fileId = fileId;
        this.rowPosition = rowPosition;
    }

    public int fileId() {
        return fileId;
    }

    public long rowPosition() {
        return rowPosition;
    }

    /** Encodes this FilePos to a byte array using unsigned varint encoding. */
    public byte[] encode() {
        // Max varint bytes: int=5, long=10, total max=15
        byte[] buf = new byte[15];
        int[] pos = {0};
        writeUnsignedVarint(fileId, buf, pos);
        writeUnsignedVarintLong(rowPosition, buf, pos);
        byte[] result = new byte[pos[0]];
        System.arraycopy(buf, 0, result, 0, pos[0]);
        return result;
    }

    /** Decodes a FilePos from the given byte array starting at offset 0. */
    public static FilePos decode(byte[] bytes) {
        return decode(bytes, 0);
    }

    /** Decodes a FilePos from the given byte array starting at the specified offset. */
    public static FilePos decode(byte[] bytes, int offset) {
        int[] pos = {offset};
        int fileId = readUnsignedVarint(bytes, pos);
        long rowPosition = readUnsignedVarintLong(bytes, pos);
        return new FilePos(fileId, rowPosition);
    }

    // ---- Unsigned varint (LEB128) encoding/decoding ----

    static void writeUnsignedVarint(int value, byte[] buf, int[] pos) {
        while ((value & 0xFFFFFF80) != 0) {
            buf[pos[0]++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buf[pos[0]++] = (byte) (value & 0x7F);
    }

    static void writeUnsignedVarintLong(long value, byte[] buf, int[] pos) {
        while ((value & 0xFFFFFFFFFFFFFF80L) != 0) {
            buf[pos[0]++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buf[pos[0]++] = (byte) (value & 0x7F);
    }

    static int readUnsignedVarint(byte[] buf, int[] pos) {
        int result = 0;
        int shift = 0;
        byte b;
        do {
            b = buf[pos[0]++];
            result |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return result;
    }

    static long readUnsignedVarintLong(byte[] buf, int[] pos) {
        long result = 0;
        int shift = 0;
        byte b;
        do {
            b = buf[pos[0]++];
            result |= (long) (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FilePos filePos = (FilePos) o;
        return fileId == filePos.fileId && rowPosition == filePos.rowPosition;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileId, rowPosition);
    }

    @Override
    public String toString() {
        return "FilePos{fileId=" + fileId + ", rowPosition=" + rowPosition + '}';
    }
}
