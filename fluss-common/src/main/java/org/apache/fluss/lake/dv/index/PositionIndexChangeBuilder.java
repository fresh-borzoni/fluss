/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.dv.index;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.metadata.TableBucket;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Builds the index change of one tiering round from the lake writers' {@link PositionIndexDelta}s
 * and commits it to the table's {@link PositionIndexStore}.
 */
@Internal
public final class PositionIndexChangeBuilder {

    private final PositionIndexStore store;
    // bucket -> key -> position of its live row, null if it has none
    private final Map<TableBucket, TreeMap<byte[], FilePos>> entries = new HashMap<>();
    // bucket -> file id -> data file added in this round
    private final Map<TableBucket, Map<Integer, String>> addedFiles = new HashMap<>();

    public PositionIndexChangeBuilder(PositionIndexStore store) {
        this.store = store;
    }

    /**
     * Adds one bucket's delta, with {@code fileIds} mapping every data file of {@code delta.puts()}
     * to its id; a bucket with an empty delta is left out of the change.
     */
    public void add(TableBucket bucket, PositionIndexDelta delta, Map<String, Integer> fileIds) {
        TreeMap<byte[], FilePos> e =
                entries.computeIfAbsent(
                        bucket, k -> new TreeMap<>(PositionIndexChangeBuilder::compareUnsigned));
        for (byte[] key : delta.deletes()) {
            e.put(key, null);
        }
        Map<Integer, String> files = addedFiles.computeIfAbsent(bucket, k -> new HashMap<>());
        for (Map.Entry<String, Integer> f : fileIds.entrySet()) {
            files.put(f.getValue(), f.getKey());
        }
        for (KeyPosition kp : delta.puts()) {
            Integer fileId = fileIds.get(kp.file());
            if (fileId == null) {
                throw new IllegalStateException("No file id for data file " + kp.file());
            }
            e.put(kp.key(), new FilePos(fileId, kp.pos()));
        }
    }

    /** Writes every bucket's part and commits the change under {@code changeId}. */
    public void commit(long changeId) throws IOException {
        Map<TableBucket, PositionIndexStore.BucketChange> change = new HashMap<>();
        for (Map.Entry<TableBucket, TreeMap<byte[], FilePos>> b : entries.entrySet()) {
            PositionIndexStore.BucketPart part =
                    b.getValue().isEmpty()
                            ? PositionIndexStore.BucketPart.NONE
                            : store.writeBucket(
                                    changeId, b.getKey(), b.getValue().entrySet().iterator());
            change.put(
                    b.getKey(),
                    new PositionIndexStore.BucketChange(
                            part,
                            addedFiles.getOrDefault(b.getKey(), Collections.emptyMap()),
                            Collections.emptyList()));
        }
        store.commitChange(changeId, change);
    }

    /** Compares keys as unsigned bytes, the order of RocksDB's default comparator. */
    public static int compareUnsigned(byte[] a, byte[] b) {
        int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            int c = (a[i] & 0xff) - (b[i] & 0xff);
            if (c != 0) {
                return c;
            }
        }
        return a.length - b.length;
    }
}
