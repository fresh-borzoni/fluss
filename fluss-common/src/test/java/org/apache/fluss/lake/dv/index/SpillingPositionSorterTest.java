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

import org.apache.fluss.lake.dv.FilePos;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SpillingPositionSorter}. */
class SpillingPositionSorterTest {

    @TempDir Path tempDir;

    @Test
    void testSortsMoreEntriesThanItBuffersThroughRuns() throws Exception {
        int entries = 1000;
        int maxBuffered = 64;
        List<Integer> order = new ArrayList<>();
        for (int i = 0; i < entries; i++) {
            order.add(i);
        }
        Collections.shuffle(order, new Random(42));
        TreeMap<byte[], FilePos> expected =
                new TreeMap<>(PositionIndexChangeBuilder::compareUnsigned);
        try (SpillingPositionSorter sorter =
                new SpillingPositionSorter(
                        tempDir.toFile(), maxBuffered, SpillingPositionSorterTest::duplicate)) {
            for (int i : order) {
                // keys with the high bit set sort after the others, unsigned
                byte[] key = ByteBuffer.allocate(4).putInt(i % 2 == 0 ? i : -i).array();
                FilePos pos = new FilePos(i % 7, i);
                expected.put(key, pos);
                sorter.add(key, pos);
            }
            assertThat(sorter.size()).isEqualTo(entries);
            assertThat(sorter.runs()).isEqualTo(entries / maxBuffered);
            List<String> actual = new ArrayList<>();
            Iterator<Map.Entry<byte[], FilePos>> it = sorter.sorted();
            while (it.hasNext()) {
                Map.Entry<byte[], FilePos> e = it.next();
                actual.add(hex(e.getKey()) + "=" + e.getValue());
            }
            List<String> wanted = new ArrayList<>();
            expected.forEach((k, v) -> wanted.add(hex(k) + "=" + v));
            assertThat(actual).isEqualTo(wanted);
        }
        // the sorted runs are deleted on close
        try (java.util.stream.Stream<Path> left = Files.list(tempDir)) {
            assertThat(left).isEmpty();
        }
    }

    @Test
    void testKeyInTwoRunsFails() throws Exception {
        try (SpillingPositionSorter sorter =
                new SpillingPositionSorter(
                        tempDir.toFile(), 2, SpillingPositionSorterTest::duplicate)) {
            sorter.add(new byte[] {1}, new FilePos(1, 0));
            sorter.add(new byte[] {2}, new FilePos(1, 1));
            sorter.add(new byte[] {1}, new FilePos(2, 0));
            Iterator<Map.Entry<byte[], FilePos>> it = sorter.sorted();
            assertThatThrownBy(it::next).hasMessageContaining("twice");
        }
    }

    @Test
    void testKeyInTheBufferTwiceFails() throws Exception {
        try (SpillingPositionSorter sorter =
                new SpillingPositionSorter(
                        tempDir.toFile(), 10, SpillingPositionSorterTest::duplicate)) {
            sorter.add(new byte[] {1}, new FilePos(1, 0));
            assertThatThrownBy(() -> sorter.add(new byte[] {1}, new FilePos(2, 0)))
                    .hasMessageContaining("twice");
            assertThat(sorter.runs()).isZero();
        }
    }

    private static RuntimeException duplicate(byte[] key, FilePos first, FilePos second) {
        return new IllegalStateException(hex(key) + " twice: " + first + ", " + second);
    }

    private static String hex(byte[] key) {
        StringBuilder sb = new StringBuilder();
        for (byte b : key) {
            sb.append(String.format("%02x", b & 0xff));
        }
        return sb.toString();
    }
}
