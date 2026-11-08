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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FilePos}. */
class FilePosTest {

    @Test
    void testEncodeDecodeSmallValues() {
        FilePos pos = new FilePos(1, 100L);
        byte[] encoded = pos.encode();
        FilePos decoded = FilePos.decode(encoded);
        assertThat(decoded).isEqualTo(pos);
        // fileId=1 -> 1 byte, rowPosition=100 -> 1 byte, total 2 bytes
        assertThat(encoded.length).isEqualTo(2);
    }

    @Test
    void testEncodeDecodeZeroValues() {
        FilePos pos = new FilePos(0, 0L);
        byte[] encoded = pos.encode();
        FilePos decoded = FilePos.decode(encoded);
        assertThat(decoded).isEqualTo(pos);
        // fileId=0 -> 1 byte, rowPosition=0 -> 1 byte, total 2 bytes
        assertThat(encoded.length).isEqualTo(2);
    }

    @Test
    void testEncodeDecodeMediumValues() {
        // fileId < 16384 -> 2 bytes, rowPosition < 2M -> 3 bytes, total 5 bytes
        FilePos pos = new FilePos(10000, 1_000_000L);
        byte[] encoded = pos.encode();
        FilePos decoded = FilePos.decode(encoded);
        assertThat(decoded).isEqualTo(pos);
        assertThat(encoded.length).isEqualTo(5);
    }

    @Test
    void testEncodeDecodeLargeValues() {
        FilePos pos = new FilePos(Integer.MAX_VALUE, Long.MAX_VALUE);
        byte[] encoded = pos.encode();
        FilePos decoded = FilePos.decode(encoded);
        assertThat(decoded).isEqualTo(pos);
    }

    @Test
    void testEncodeDecodeBoundaryValues() {
        // 127 = max value in 1 varint byte for int
        assertRoundTrip(new FilePos(127, 127L));
        // 128 = first value requiring 2 varint bytes
        assertRoundTrip(new FilePos(128, 128L));
        // 16383 = max value in 2 varint bytes
        assertRoundTrip(new FilePos(16383, 16383L));
        // 16384 = first value requiring 3 varint bytes
        assertRoundTrip(new FilePos(16384, 16384L));
    }

    @Test
    void testDecodeWithOffset() {
        FilePos pos = new FilePos(42, 9999L);
        byte[] encoded = pos.encode();

        byte[] padded = new byte[10 + encoded.length];
        System.arraycopy(encoded, 0, padded, 10, encoded.length);

        FilePos decoded = FilePos.decode(padded, 10);
        assertThat(decoded).isEqualTo(pos);
    }

    @Test
    void testEqualsAndHashCode() {
        FilePos a = new FilePos(1, 100L);
        FilePos b = new FilePos(1, 100L);
        FilePos c = new FilePos(2, 100L);
        FilePos d = new FilePos(1, 200L);

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
        assertThat(a).isNotEqualTo(c);
        assertThat(a).isNotEqualTo(d);
    }

    @Test
    void testToString() {
        FilePos pos = new FilePos(5, 42L);
        assertThat(pos.toString()).contains("fileId=5").contains("rowPosition=42");
    }

    private void assertRoundTrip(FilePos pos) {
        byte[] encoded = pos.encode();
        FilePos decoded = FilePos.decode(encoded);
        assertThat(decoded).isEqualTo(pos);
    }
}
