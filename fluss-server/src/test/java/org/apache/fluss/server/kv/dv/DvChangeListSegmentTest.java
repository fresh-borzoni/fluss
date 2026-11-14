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

import org.apache.fluss.server.kv.dv.DvChangeListSegment.Header;
import org.apache.fluss.server.kv.dv.DvChangeListSegment.UnreadableSegmentException;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the file format of {@link DvChangeListSegment}. */
class DvChangeListSegmentTest {

    private static final Header HEADER = new Header(7, -1, 3, 100, 200, 90);

    @Test
    void testRoundTrip() throws Exception {
        List<long[]> changes = new ArrayList<>();
        List<byte[]> keys = new ArrayList<>();
        // the first offset of the range; a version before valid-from; a version at offset 0
        add(changes, keys, 100, -1, "k1");
        add(changes, keys, 101, 100, "k1");
        add(changes, keys, 150, 0, "k2");
        add(changes, keys, 199, 98, "");
        byte[] bytes = write(HEADER, changes, keys);

        List<long[]> read = new ArrayList<>();
        List<byte[]> readKeys = new ArrayList<>();
        long n =
                DvChangeListSegment.read(
                        new ByteArrayInputStream(bytes),
                        HEADER,
                        (offset, superseded, key) -> {
                            read.add(new long[] {offset, superseded});
                            readKeys.add(key);
                        });

        assertThat(n).isEqualTo(4);
        assertThat(read).containsExactlyElementsOf(changes);
        assertThat(readKeys).containsExactlyElementsOf(keys);
    }

    @Test
    void testRandomRoundTripAndSize() throws Exception {
        Random random = new Random(42);
        Header header = new Header(1, 5, 0, 1_000_000, 3_000_000, 1_000_000);
        List<long[]> changes = new ArrayList<>();
        List<byte[]> keys = new ArrayList<>();
        long offset = header.from;
        while (changes.size() < 20_000) {
            offset += 1 + random.nextInt(50);
            long superseded =
                    random.nextInt(4) == 0 ? -1 : offset - 1 - random.nextInt((int) (offset / 2));
            byte[] key = new byte[9];
            random.nextBytes(key);
            changes.add(new long[] {offset, superseded});
            keys.add(key);
        }
        byte[] bytes = write(header, changes, keys);

        List<long[]> read = new ArrayList<>();
        DvChangeListSegment.read(
                new ByteArrayInputStream(bytes), header, (o, s, k) -> read.add(new long[] {o, s}));

        assertThat(read).containsExactlyElementsOf(changes);
        // a 9-byte key, small deltas: well under 20 bytes per change
        assertThat(bytes.length).isLessThan(20 * changes.size());
    }

    @Test
    void testEmptySegment() throws Exception {
        Header header = new Header(7, -1, 3, 100, 100, 100);
        byte[] bytes = write(header, new ArrayList<>(), new ArrayList<>());

        assertThat(
                        DvChangeListSegment.read(
                                new ByteArrayInputStream(bytes),
                                header,
                                (o, s, k) -> {
                                    throw new AssertionError("no entries");
                                }))
                .isZero();
    }

    @Test
    void testWriterRefusesChangesOutOfOrder() throws Exception {
        try (DvChangeListSegment.Writer w =
                new DvChangeListSegment.Writer(new ByteArrayOutputStream(), HEADER)) {
            w.add(120, -1, key("a"));
            assertThatThrownBy(() -> w.add(120, -1, key("b")))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> w.add(99, -1, key("b")))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> w.add(200, -1, key("b")))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> w.add(130, 130, key("b")))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> w.add(130, -2, key("b")))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void testEveryFlippedByteIsRefused() throws Exception {
        byte[] bytes = sample();
        for (int i = 0; i < bytes.length; i++) {
            byte[] flipped = Arrays.copyOf(bytes, bytes.length);
            flipped[i] ^= 0x01;
            int at = i;
            assertThatThrownBy(() -> readAll(flipped, HEADER))
                    .as("a flip at byte %d", at)
                    .isInstanceOf(UnreadableSegmentException.class);
        }
    }

    @Test
    void testTruncatedOrExtendedSegmentIsRefused() throws Exception {
        byte[] bytes = sample();
        for (int length = 0; length < bytes.length; length++) {
            byte[] truncated = Arrays.copyOf(bytes, length);
            assertThatThrownBy(() -> readAll(truncated, HEADER))
                    .isInstanceOf(UnreadableSegmentException.class);
        }
        byte[] extended = Arrays.copyOf(bytes, bytes.length + 1);
        assertThatThrownBy(() -> readAll(extended, HEADER))
                .isInstanceOf(UnreadableSegmentException.class)
                .hasMessageContaining("after its end");
    }

    @Test
    void testUnknownVersionIsRefusedAsSuch() throws Exception {
        byte[] bytes = sample();
        bytes[4] = 2; // the version byte after the magic

        assertThatThrownBy(() -> readAll(bytes, HEADER))
                .isInstanceOfSatisfying(
                        UnreadableSegmentException.class,
                        e -> assertThat(e.unknownVersion).isTrue())
                .hasMessageContaining("version 2");
    }

    @Test
    void testAnotherBucketsSegmentIsRefused() throws Exception {
        byte[] bytes = sample();

        assertThatThrownBy(() -> readAll(bytes, new Header(7, -1, 4, 100, 200, 90)))
                .isInstanceOfSatisfying(
                        UnreadableSegmentException.class,
                        e -> assertThat(e.unknownVersion).isFalse())
                .hasMessageContaining("header");
        assertThatThrownBy(() -> readAll(bytes, new Header(7, -1, 3, 100, 201, 90)))
                .isInstanceOf(UnreadableSegmentException.class);
    }

    @Test
    void testValidFromIsAnUpperBound() throws Exception {
        byte[] bytes = sample(); // valid-from 90

        // a change list whose valid-from is higher, from a later segment, reads it
        readAll(bytes, new Header(7, -1, 3, 100, 200, 95));
        assertThatThrownBy(() -> readAll(bytes, new Header(7, -1, 3, 100, 200, 89)))
                .isInstanceOf(UnreadableSegmentException.class)
                .hasMessageContaining("header");
    }

    // ---------------------------------------------------------------------------------------------

    private static byte[] sample() throws IOException {
        List<long[]> changes = new ArrayList<>();
        List<byte[]> keys = new ArrayList<>();
        add(changes, keys, 105, -1, "alpha");
        add(changes, keys, 110, 105, "alpha");
        add(changes, keys, 180, 95, "beta");
        return write(HEADER, changes, keys);
    }

    private static void readAll(byte[] bytes, Header expected) throws IOException {
        DvChangeListSegment.read(new ByteArrayInputStream(bytes), expected, (o, s, k) -> {});
    }

    private static byte[] write(Header header, List<long[]> changes, List<byte[]> keys)
            throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DvChangeListSegment.Writer w = new DvChangeListSegment.Writer(out, header)) {
            for (int i = 0; i < changes.size(); i++) {
                w.add(changes.get(i)[0], changes.get(i)[1], keys.get(i));
            }
            long bytes = w.finish();
            assertThat(w.entries()).isEqualTo(changes.size());
            assertThat(bytes).isEqualTo(out.size());
        }
        return out.toByteArray();
    }

    private static void add(
            List<long[]> changes, List<byte[]> keys, long offset, long superseded, String key) {
        changes.add(new long[] {offset, superseded});
        keys.add(key(key));
    }

    private static byte[] key(String key) {
        return key.getBytes(StandardCharsets.UTF_8);
    }
}
