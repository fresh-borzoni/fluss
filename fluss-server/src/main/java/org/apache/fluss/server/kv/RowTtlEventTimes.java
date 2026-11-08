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

import org.apache.fluss.utils.clock.Clock;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The oldest TTL time of the versions a bucket wrote at or after an offset, by processing or event
 * time, the bound of its row TTL filter ({@link RowTtlFilterClock}).
 *
 * <p>It keeps one minimum per slice of about a second of writes. Slices cover the offsets from
 * {@link #from()} without gaps, each up to the last tracked offset in it. A slice is dropped once
 * the readable offset passes its end, so a slice the readable offset is inside of stays: the
 * minimum is never above the true one. Past {@link #MAX_SLICES}, neighbours merge, which only makes
 * slices longer. Not thread-safe: {@link RowTtlFilterClock} guards it.
 */
final class RowTtlEventTimes {

    /** The most slices a bucket keeps; a long stall merges neighbours beyond it. */
    static final int MAX_SLICES = 512;

    // a slice's long[2] and its deque slot
    private static final long SLICE_HEAP_BYTES = 48;

    /** The minimum of no versions. */
    static final long NONE = Long.MAX_VALUE;

    private final Clock clock;
    // offsets below are not tracked
    private long from;
    // {end, min}: the slice covers [the previous end or from, end)
    private ArrayDeque<long[]> slices = new ArrayDeque<>();
    private long lastSecond = Long.MIN_VALUE;

    RowTtlEventTimes(Clock clock, long from) {
        this.clock = clock;
        this.from = from;
    }

    /** The first offset tracked: the bound is known for readable offsets at or above it. */
    long from() {
        return from;
    }

    /** Tracks the TTL time {@code time} of the version at {@code offset}, before its KV write. */
    void add(long offset, long time) {
        if (offset < from) {
            return;
        }
        long second = clock.milliseconds() / 1000;
        long[] last = slices.peekLast();
        // an offset below the last end lowers the last slice, which stays as long as any before it
        if (last != null && (second == lastSecond || offset < last[0])) {
            last[0] = Math.max(last[0], offset + 1);
            last[1] = Math.min(last[1], time);
            return;
        }
        lastSecond = second;
        slices.addLast(new long[] {offset + 1, time});
        if (slices.size() > MAX_SLICES) {
            mergeNeighbours();
        }
    }

    /** Drops the slices that end at or below {@code readableOffset}. */
    void trim(long readableOffset) {
        while (!slices.isEmpty() && slices.peekFirst()[0] <= readableOffset) {
            from = slices.pollFirst()[0];
        }
        if (slices.isEmpty()) {
            from = Math.max(from, readableOffset);
        }
    }

    /** The oldest TTL time of the tracked versions, or {@link #NONE}. */
    long min() {
        long min = NONE;
        for (long[] s : slices) {
            min = Math.min(min, s[1]);
        }
        return min;
    }

    int size() {
        return slices.size();
    }

    /** The estimated heap of the slices. */
    long heapBytes() {
        return slices.size() * SLICE_HEAP_BYTES;
    }

    /**
     * The slices below {@code end}, as {end, min} pairs, the last ending at {@code end} at most; a
     * slice across {@code end} is cut there and keeps its minimum.
     */
    List<long[]> below(long end) {
        List<long[]> out = new ArrayList<>();
        for (long[] s : slices) {
            long start = out.isEmpty() ? from : out.get(out.size() - 1)[0];
            if (start >= end) {
                break;
            }
            out.add(new long[] {Math.min(s[0], end), s[1]});
        }
        return out;
    }

    /** Starts from saved slices covering [{@code from}, the last end). */
    void restore(long from, List<long[]> saved) {
        this.from = from;
        slices = new ArrayDeque<>();
        for (long[] s : saved) {
            slices.addLast(new long[] {s[0], s[1]});
        }
        lastSecond = Long.MIN_VALUE;
    }

    private void mergeNeighbours() {
        ArrayDeque<long[]> merged = new ArrayDeque<>(slices.size() / 2 + 1);
        Iterator<long[]> it = slices.iterator();
        while (it.hasNext()) {
            long[] a = it.next();
            if (it.hasNext()) {
                long[] b = it.next();
                merged.addLast(new long[] {b[0], Math.min(a[1], b[1])});
            } else {
                merged.addLast(a);
            }
        }
        slices = merged;
    }
}
