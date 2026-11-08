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

import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.metadata.TableBucket;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Builds an index change in tests and commits it to a {@link PositionIndexStore}, the way the
 * tiering committer publishes the parts its writers staged.
 */
public final class PositionIndexChangeBuilder {

    private final PositionIndexStore store;
    // bucket -> key -> position of its live row, null if it has none
    private final Map<TableBucket, TreeMap<byte[], FilePos>> entries = new HashMap<>();
    // bucket -> data file -> its id
    private final Map<TableBucket, Map<String, Integer>> fileIds = new HashMap<>();

    public PositionIndexChangeBuilder(PositionIndexStore store) {
        this.store = store;
    }

    /**
     * Adds a bucket to the change, with {@code files} mapping every data file of its puts to its
     * id. A bucket without puts or deletes is left out of the change.
     */
    public PositionIndexChangeBuilder add(TableBucket bucket, Map<String, Integer> files) {
        entries.computeIfAbsent(
                bucket, k -> new TreeMap<>(PositionIndexChangeBuilder::compareUnsigned));
        fileIds.computeIfAbsent(bucket, k -> new HashMap<>()).putAll(files);
        return this;
    }

    /** Sets the live row of {@code key} to position {@code pos} of data file {@code file}. */
    public PositionIndexChangeBuilder put(TableBucket bucket, byte[] key, String file, long pos) {
        Integer fileId = fileIds.getOrDefault(bucket, Collections.emptyMap()).get(file);
        if (fileId == null) {
            throw new IllegalStateException("No file id for data file " + file);
        }
        entries.get(bucket).put(key, new FilePos(fileId, pos));
        return this;
    }

    /** Deletes the live row of {@code key}. */
    public PositionIndexChangeBuilder delete(TableBucket bucket, byte[] key) {
        entries.get(bucket).put(key, null);
        return this;
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
            Map<Integer, String> files = new HashMap<>();
            for (Map.Entry<String, Integer> f : fileIds.get(b.getKey()).entrySet()) {
                files.put(f.getValue(), f.getKey());
            }
            change.put(
                    b.getKey(),
                    new PositionIndexStore.BucketChange(part, files, Collections.emptyList()));
        }
        store.commitChange(changeId, change);
    }

    /** Compares keys as unsigned bytes, the order of the index. */
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
