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

package org.apache.fluss.lake.iceberg.tiering.writer;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

/**
 * The keys of a masked round's index entries in its writer's sort buffer: a primary key and the
 * offset of its version, so that no two versions of a key fold into one, as the buffer folds equal
 * keys. The entries sort as their keys do in the index (unsigned bytes) and, within a key, by
 * offset: the key's zero bytes are escaped as 0x00 0xFF and the key ends with 0x00 0x01, which
 * sorts below any byte a longer key goes on with, and the offset follows in eight big-endian bytes.
 */
final class RoundIndexEntries {

    private static final int OFFSET_BYTES = Long.BYTES;

    private RoundIndexEntries() {}

    /** The entry key of {@code key}'s version at {@code offset}. */
    static byte[] entryKey(byte[] key, long offset) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(key.length + 2 + OFFSET_BYTES);
        for (byte b : key) {
            out.write(b);
            if (b == 0) {
                out.write(0xFF);
            }
        }
        out.write(0);
        out.write(1);
        for (int shift = 56; shift >= 0; shift -= 8) {
            out.write((int) (offset >>> shift));
        }
        return out.toByteArray();
    }

    /** The primary key of an entry key. */
    static byte[] keyOf(byte[] entryKey) {
        byte[] key = new byte[entryKey.length];
        int n = 0;
        int i = 0;
        while (true) {
            byte b = entryKey[i++];
            if (b == 0) {
                if (entryKey[i] == 1) {
                    return Arrays.copyOf(key, n);
                }
                // an escaped zero byte of the key
                i++;
            }
            key[n++] = b;
        }
    }

    /** The version's offset of an entry key. */
    static long offsetOf(byte[] entryKey) {
        long offset = 0;
        for (int i = entryKey.length - OFFSET_BYTES; i < entryKey.length; i++) {
            offset = (offset << 8) | (entryKey[i] & 0xFF);
        }
        return offset;
    }
}
