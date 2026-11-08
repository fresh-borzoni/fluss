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

package org.apache.fluss.server.kv.snapshot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * What a KV snapshot of a table with deletion vectors and row TTL saves of the bucket's TTL filter
 * ({@code RowTtlFilterClock}): P, the highest readable offset the filter was capped for, and the
 * tracked TTL times of the versions in [{@link #from()}, the last slice's end), one {end, minimum}
 * pair per slice, each slice starting where the one before ends.
 */
public final class RowTtlFilterState {

    private final long cappedFrom;
    private final long from;
    private final List<long[]> slices;

    public RowTtlFilterState(long cappedFrom, long from, List<long[]> slices) {
        this.cappedFrom = cappedFrom;
        this.from = from;
        List<long[]> copy = new ArrayList<>(slices.size());
        for (long[] s : slices) {
            checkArgument(s.length == 2, "A slice is an {end, minimum} pair.");
            copy.add(new long[] {s[0], s[1]});
        }
        this.slices = Collections.unmodifiableList(copy);
    }

    /** P: no version at or after it expired before it was tiered. */
    public long cappedFrom() {
        return cappedFrom;
    }

    public long from() {
        return from;
    }

    public List<long[]> slices() {
        return slices;
    }

    /** Whether each slice ends after the one before, the first after {@link #from()}. */
    public boolean slicesFollowEachOther() {
        long previous = from;
        for (long[] s : slices) {
            if (s[0] <= previous) {
                return false;
            }
            previous = s[0];
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowTtlFilterState that = (RowTtlFilterState) o;
        if (cappedFrom != that.cappedFrom
                || from != that.from
                || slices.size() != that.slices.size()) {
            return false;
        }
        for (int i = 0; i < slices.size(); i++) {
            if (!Arrays.equals(slices.get(i), that.slices.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int h = 31 * Long.hashCode(cappedFrom) + Long.hashCode(from);
        for (long[] s : slices) {
            h = 31 * h + Arrays.hashCode(s);
        }
        return h;
    }

    @Override
    public String toString() {
        return "RowTtlFilterState{capped_from="
                + cappedFrom
                + ", from="
                + from
                + ", slices="
                + slices.size()
                + '}';
    }
}
