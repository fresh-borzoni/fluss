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

import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SpillingKeySortBuffer} and the {@link WriterMemoryPool} it shares. */
class SpillingKeySortBufferTest {

    @TempDir File tmp;

    @Test
    void testLastVersionOfEveryKeyInKeyOrder() throws Exception {
        WriterMemoryPool pool = new WriterMemoryPool(1 << 20);
        try (SpillingKeySortBuffer buffer = buffer(pool)) {
            buffer.add(key("b"), 1, value("b1"));
            buffer.add(key("a"), 2, value("a2"));
            buffer.add(key("b"), 3, value("b3"));
            buffer.add(key("c"), 4, value("c4"));
            buffer.add(key("c"), 5, null);
            buffer.add(key("a"), 6, value("a6"));
            // unsigned order: 0x80 sorts after ASCII
            buffer.add(new byte[] {(byte) 0x80}, 7, value("high"));
            assertThat(buffer.runs()).isZero();
            assertThat(drain(buffer))
                    .containsExactly("a@6=a6", "b@3=b3", "c@5=<deleted>", "[-128]@7=high");
        }
        assertThat(pool.used()).isZero();
    }

    @Test
    void testSpillsWhenThePoolIsFullAndMergesExactly() throws Exception {
        // room for a few hundred records only
        WriterMemoryPool pool = new WriterMemoryPool(64 * 1024);
        Random random = new Random(42);
        TreeMap<String, String> expected = new TreeMap<>();
        try (SpillingKeySortBuffer buffer = buffer(pool)) {
            for (int offset = 0; offset < 100_000; offset++) {
                // most keys get several versions across sorted runs
                String k = String.format("k%05d", random.nextInt(20_000));
                if (random.nextInt(10) == 0) {
                    buffer.add(key(k), offset, null);
                    expected.put(k, offset + "=<deleted>");
                } else {
                    String v = "v" + offset;
                    buffer.add(key(k), offset, value(v));
                    expected.put(k, offset + "=" + v);
                }
                assertThat(pool.used()).isLessThanOrEqualTo(pool.capacity());
            }
            assertThat(buffer.spills()).isGreaterThan(SpillingKeySortBuffer.MAX_FAN_IN);
            // the smaller sorted runs are merged, so no merge opens more than the fan-in
            assertThat(buffer.runs()).isLessThan(SpillingKeySortBuffer.MAX_FAN_IN);
            assertThat(buffer.distinctKeysUpperBound()).isGreaterThanOrEqualTo(expected.size());
            assertThat(buffer.spillDir()).exists();
            List<String> expectedEntries = new ArrayList<>();
            for (Map.Entry<String, String> e : expected.entrySet()) {
                expectedEntries.add(e.getKey() + "@" + e.getValue());
            }
            assertThat(drain(buffer)).containsExactlyElementsOf(expectedEntries);
            File dir = buffer.spillDir();
            buffer.close();
            assertThat(dir).doesNotExist();
        }
        assertThat(pool.used()).isZero();
    }

    @Test
    void testTheWriterHoldingTheMostSpillsFirst() throws Exception {
        WriterMemoryPool pool = new WriterMemoryPool(256 * 1024);
        try (SpillingKeySortBuffer big = buffer(pool);
                SpillingKeySortBuffer small = buffer(pool)) {
            byte[] large = new byte[1000];
            int i = 0;
            while (pool.used() < 200 * 1024) {
                big.add(key("big" + i), i++, large);
            }
            for (int j = 0; j < 10; j++) {
                small.add(key("small" + j), j, value("s"));
            }
            assertThat(big.spills()).isZero();
            // the small writer needs more than is left: the big one spills, not the small one
            for (int j = 10; j < 100; j++) {
                small.add(key("small" + j), j, large);
            }
            assertThat(big.spills()).isEqualTo(1);
            assertThat(small.spills()).isZero();
            assertThat(drain(small)).hasSize(100);
            assertThat(drain(big)).hasSize(i);
        }
        assertThat(pool.used()).isZero();
    }

    @Test
    void testARecordLargerThanThePoolStillFits() throws Exception {
        WriterMemoryPool pool = new WriterMemoryPool(1024);
        try (SpillingKeySortBuffer buffer = buffer(pool)) {
            buffer.add(key("a"), 0, new byte[4096]);
            buffer.add(key("b"), 1, new byte[4096]);
            assertThat(buffer.spills()).isGreaterThanOrEqualTo(1);
            List<String> keys = new ArrayList<>();
            try (CloseableIterator<SpillingKeySortBuffer.Entry> it = buffer.lastVersions()) {
                while (it.hasNext()) {
                    SpillingKeySortBuffer.Entry e = it.next();
                    keys.add(new String(e.key(), StandardCharsets.UTF_8));
                    assertThat(e.valueLength()).isEqualTo(4096);
                }
            }
            assertThat(keys).containsExactly("a", "b");
        }
        assertThat(pool.used()).isZero();
    }

    @Test
    void testCloseWithoutMergeDeletesTheRuns() throws Exception {
        WriterMemoryPool pool = new WriterMemoryPool(1 << 20);
        SpillingKeySortBuffer buffer = buffer(pool);
        buffer.add(key("a"), 0, value("x"));
        buffer.spill();
        File dir = buffer.spillDir();
        assertThat(dir).isNotNull();
        assertThat(dir.listFiles()).hasSize(1);
        assertThat(dir.getParentFile()).isEqualTo(tmp);
        buffer.close();
        assertThat(dir).doesNotExist();
        assertThat(pool.used()).isZero();
        assertThatThrownBy(() -> buffer.add(key("b"), 1, null))
                .isInstanceOf(IllegalStateException.class);
    }

    // ------------------------------------------------------------------------------------------

    private SpillingKeySortBuffer buffer(WriterMemoryPool pool) {
        return new SpillingKeySortBuffer(pool, new String[] {tmp.getPath()});
    }

    private static List<String> drain(SpillingKeySortBuffer buffer) throws IOException {
        List<String> out = new ArrayList<>();
        try (CloseableIterator<SpillingKeySortBuffer.Entry> it = buffer.lastVersions()) {
            while (it.hasNext()) {
                SpillingKeySortBuffer.Entry e = it.next();
                byte[] k = e.key();
                String key =
                        k.length > 0 && k[0] < 0
                                ? Arrays.toString(k)
                                : new String(k, StandardCharsets.UTF_8);
                String v =
                        e.isDelete()
                                ? "<deleted>"
                                : new String(
                                        e.valueArray(),
                                        e.valueOffset(),
                                        e.valueLength(),
                                        StandardCharsets.UTF_8);
                out.add(key + "@" + e.offset() + "=" + v);
            }
        }
        return out;
    }

    private static byte[] key(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] value(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
