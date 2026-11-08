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

package org.apache.fluss.flink.tiering.committer;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * The deleted row positions of a round per data file name, kept as compressed bitmaps.
 *
 * <p>The bitmaps hold the deletion vectors the round commits, whatever the number of keys it
 * changed. The lake committer takes {@code long[]} positions per file, so the map builds a file's
 * array only when the committer reads its entry. Besides the bitmaps, only the arrays of the file
 * the committer is at exist.
 */
final class DeletedPositions extends AbstractMap<String, long[]> {

    private final Map<String, Roaring64NavigableMap> byFile = new HashMap<>();

    void add(String file, long position) {
        byFile.computeIfAbsent(file, f -> new Roaring64NavigableMap()).addLong(position);
    }

    void addAll(DeletedPositions other) {
        for (Map.Entry<String, Roaring64NavigableMap> e : other.byFile.entrySet()) {
            Roaring64NavigableMap mine = byFile.get(e.getKey());
            if (mine == null) {
                byFile.put(e.getKey(), e.getValue());
            } else {
                mine.or(e.getValue());
            }
        }
    }

    /** The number of positions of all files. */
    long positions() {
        long n = 0;
        for (Roaring64NavigableMap bitmap : byFile.values()) {
            n += bitmap.getLongCardinality();
        }
        return n;
    }

    @Override
    public int size() {
        return byFile.size();
    }

    @Override
    public boolean containsKey(Object file) {
        return byFile.containsKey(file);
    }

    @Override
    public long[] get(Object file) {
        Roaring64NavigableMap bitmap = byFile.get(file);
        return bitmap == null ? null : bitmap.toArray();
    }

    @Override
    public Set<Entry<String, long[]>> entrySet() {
        return new AbstractSet<Entry<String, long[]>>() {
            @Override
            public Iterator<Entry<String, long[]>> iterator() {
                Iterator<Map.Entry<String, Roaring64NavigableMap>> it =
                        byFile.entrySet().iterator();
                return new Iterator<Entry<String, long[]>>() {
                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public Entry<String, long[]> next() {
                        Map.Entry<String, Roaring64NavigableMap> e = it.next();
                        return new Entry<String, long[]>() {
                            @Override
                            public String getKey() {
                                return e.getKey();
                            }

                            @Override
                            public long[] getValue() {
                                return e.getValue().toArray();
                            }

                            @Override
                            public long[] setValue(long[] value) {
                                throw new UnsupportedOperationException();
                            }
                        };
                    }
                };
            }

            @Override
            public int size() {
                return byFile.size();
            }
        };
    }
}
