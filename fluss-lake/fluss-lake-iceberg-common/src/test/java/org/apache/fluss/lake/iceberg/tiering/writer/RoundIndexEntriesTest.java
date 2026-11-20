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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The entry keys of a masked round ({@link RoundIndexEntries}) sort as their primary keys do in the
 * index, unsigned bytes with a shorter prefix first, and then by offset; and give their key and
 * offset back.
 */
class RoundIndexEntriesTest {

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 4})
    void testEntriesSortAsKeysThenOffsets(long seed) {
        Random random = new Random(seed);
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            byte[] key = new byte[random.nextInt(6)];
            for (int b = 0; b < key.length; b++) {
                // mostly zeros and 0xFF and 0x01, the bytes the escaping uses
                int pick = random.nextInt(4);
                key[b] =
                        (byte)
                                (pick == 0
                                        ? 0
                                        : pick == 1 ? 0xFF : pick == 2 ? 1 : random.nextInt());
            }
            keys.add(key);
            if (random.nextBoolean()) {
                // a longer key with this one as its prefix
                byte[] longer = new byte[key.length + 1 + random.nextInt(2)];
                System.arraycopy(key, 0, longer, 0, key.length);
                keys.add(longer);
            }
        }
        List<Object[]> versions = new ArrayList<>();
        for (byte[] key : keys) {
            versions.add(new Object[] {key, (long) random.nextInt(1000)});
            versions.add(new Object[] {key, (random.nextLong() >>> 1)});
        }
        List<Object[]> byKey = new ArrayList<>(versions);
        byKey.sort(
                Comparator.<Object[], byte[]>comparing(
                                v -> (byte[]) v[0], RoundIndexEntriesTest::compare)
                        .thenComparing(v -> (Long) v[1]));
        List<byte[]> entries = new ArrayList<>();
        for (Object[] v : versions) {
            entries.add(RoundIndexEntries.entryKey((byte[]) v[0], (Long) v[1]));
        }
        entries.sort(RoundIndexEntriesTest::compare);
        for (int i = 0; i < entries.size(); i++) {
            assertThat(RoundIndexEntries.keyOf(entries.get(i))).isEqualTo(byKey.get(i)[0]);
            assertThat(RoundIndexEntries.offsetOf(entries.get(i))).isEqualTo(byKey.get(i)[1]);
        }
    }

    private static int compare(byte[] a, byte[] b) {
        int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            int c = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (c != 0) {
                return c;
            }
        }
        return a.length - b.length;
    }
}
