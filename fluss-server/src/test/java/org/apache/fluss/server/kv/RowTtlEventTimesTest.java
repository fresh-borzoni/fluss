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

package org.apache.fluss.server.kv;

import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Random;

import static org.apache.fluss.server.kv.RowTtlEventTimes.NONE;
import static org.assertj.core.api.Assertions.assertThat;

/** The event-time bound of the row TTL filter's cap ({@link RowTtlEventTimes}). */
class RowTtlEventTimesTest {

    private final ManualClock clock = new ManualClock(1_000_000);
    private final RowTtlEventTimes times = new RowTtlEventTimes(clock, 0);

    /** One slice per second of writes; a slice goes once the readable offset passes its end. */
    @Test
    void testSlicesAreTrimmedOnlyOncePassed() {
        times.add(0, 500);
        times.add(1, 400);
        clock.advanceTime(Duration.ofSeconds(1));
        times.add(2, 900);
        times.add(5, 800);
        assertThat(times.size()).isEqualTo(2);
        assertThat(times.min()).isEqualTo(400);

        // offset 1 is below the readable offset, offset 0 too: the first slice ends at 2
        times.trim(1);
        assertThat(times.min()).isEqualTo(400);
        times.trim(2);
        assertThat(times.from()).isEqualTo(2);
        assertThat(times.min()).isEqualTo(800);
        times.trim(6);
        assertThat(times.min()).isEqualTo(NONE);
        assertThat(times.from()).isEqualTo(6);
    }

    /** A late row lowers the minimum at once, also within a slice that has later rows. */
    @Test
    void testALateRowLowersTheMinimum() {
        times.add(10, 5_000);
        times.add(11, 6_000);
        assertThat(times.min()).isEqualTo(5_000);
        times.add(12, 100);
        assertThat(times.min()).isEqualTo(100);
        clock.advanceTime(Duration.ofSeconds(1));
        times.add(13, 7_000);
        times.trim(13);
        assertThat(times.min()).isEqualTo(7_000);
    }

    /** Offsets below the first tracked one are not tracked: the bound is known only above it. */
    @Test
    void testOffsetsBelowFromAreNotTracked() {
        RowTtlEventTimes restored = new RowTtlEventTimes(clock, 20);
        restored.add(15, 1);
        assertThat(restored.min()).isEqualTo(NONE);
        restored.add(20, 50);
        assertThat(restored.min()).isEqualTo(50);
        restored.trim(10);
        assertThat(restored.from()).isEqualTo(20);
    }

    /**
     * A long stall merges neighbouring slices beyond the bound: the minimum stays the true one, and
     * slices only get longer.
     */
    @Test
    void testMemoryIsBoundedAndTheMinimumNeverRises() {
        Random random = new Random(42);
        long trueMin = NONE;
        long[] mins = new long[3 * RowTtlEventTimes.MAX_SLICES];
        for (int i = 0; i < mins.length; i++) {
            mins[i] = 1_000_000 + random.nextInt(1_000_000);
            times.add(i, mins[i]);
            trueMin = Math.min(trueMin, mins[i]);
            clock.advanceTime(Duration.ofSeconds(1));
        }
        assertThat(times.size()).isLessThanOrEqualTo(RowTtlEventTimes.MAX_SLICES);
        assertThat(times.min()).isEqualTo(trueMin);
        for (int o = 0; o < mins.length; o += 997) {
            times.trim(o);
            long above = NONE;
            for (int i = o; i < mins.length; i++) {
                above = Math.min(above, mins[i]);
            }
            assertThat(times.min()).as("o %d", o).isLessThanOrEqualTo(above);
        }
    }

    /** The slices below a KV snapshot's offset, the one across it cut there, and back. */
    @Test
    void testSlicesBelowAnOffsetRoundTrip() {
        times.add(0, 300);
        clock.advanceTime(Duration.ofSeconds(1));
        times.add(3, 200);
        times.add(7, 250);
        clock.advanceTime(Duration.ofSeconds(1));
        times.add(9, 100);

        List<long[]> below = times.below(5);
        assertThat(below).hasSize(2);
        assertThat(below.get(0)).containsExactly(1, 300);
        assertThat(below.get(1)).containsExactly(5, 200);

        RowTtlEventTimes restored = new RowTtlEventTimes(clock, 0);
        restored.restore(0, below);
        restored.add(5, 600);
        assertThat(restored.min()).isEqualTo(200);
        restored.trim(5);
        assertThat(restored.min()).isEqualTo(600);
        assertThat(restored.from()).isEqualTo(5);
    }
}
