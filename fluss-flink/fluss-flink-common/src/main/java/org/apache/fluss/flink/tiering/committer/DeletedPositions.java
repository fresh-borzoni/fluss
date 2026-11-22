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

import org.apache.fluss.client.tiering.RoundDeletes;

import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
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
public final class DeletedPositions extends AbstractMap<String, long[]> {

    private final Map<String, Roaring64Bitmap> byFile = new HashMap<>();

    /** The positions a bucket's round deletes in the base. */
    public static DeletedPositions of(RoundDeletes deletes) throws IOException {
        DeletedPositions positions = new DeletedPositions();
        positions.addAll(deletes);
        return positions;
    }

    /** Adds the positions of a file, serialized as a {@link Roaring64Bitmap}. */
    void addSerialized(String file, byte[] positions) throws IOException {
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        bitmap.deserialize(ByteBuffer.wrap(positions));
        addAll(file, bitmap);
    }

    private void addAll(String file, Roaring64Bitmap positions) {
        Roaring64Bitmap mine = byFile.get(file);
        if (mine == null) {
            byFile.put(file, positions);
        } else {
            mine.or(positions);
        }
    }

    /** Adds the positions a bucket's round deletes in the base. */
    void addAll(RoundDeletes deletes) throws IOException {
        for (RoundDeletes.File file : deletes.files()) {
            addSerialized(file.path(), file.positions());
        }
    }

    void addAll(DeletedPositions other) {
        for (Map.Entry<String, Roaring64Bitmap> e : other.byFile.entrySet()) {
            addAll(e.getKey(), e.getValue());
        }
    }

    /** The number of positions of all files. */
    long positions() {
        long n = 0;
        for (Roaring64Bitmap bitmap : byFile.values()) {
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
        Roaring64Bitmap bitmap = byFile.get(file);
        return bitmap == null ? null : bitmap.toArray();
    }

    @Override
    public Set<Entry<String, long[]>> entrySet() {
        return new AbstractSet<Entry<String, long[]>>() {
            @Override
            public Iterator<Entry<String, long[]>> iterator() {
                Iterator<Map.Entry<String, Roaring64Bitmap>> it = byFile.entrySet().iterator();
                return new Iterator<Entry<String, long[]>>() {
                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public Entry<String, long[]> next() {
                        Map.Entry<String, Roaring64Bitmap> e = it.next();
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
