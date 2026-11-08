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

package org.apache.fluss.server.kv.dv;

import java.nio.ByteBuffer;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/** The keys of applied changes that wait for their lake positions, with the heap they take. */
final class DvUnresolvedKeys extends AbstractCollection<ByteBuffer> {
    // a heap ByteBuffer, the header of its array and a list slot, besides the key's bytes
    private static final long KEY_OVERHEAD_BYTES = 48 + 16 + 8;

    private final ArrayList<ByteBuffer> keys = new ArrayList<>();
    private long bytes;

    /** The estimated heap bytes of a key held in a list, with its wrapper and its slot. */
    static long keyBytes(ByteBuffer key) {
        return KEY_OVERHEAD_BYTES + ((key.capacity() + 7) & ~7L);
    }

    @Override
    public boolean add(ByteBuffer key) {
        keys.add(key);
        bytes += keyBytes(key);
        return true;
    }

    @Override
    public void clear() {
        keys.clear();
        bytes = 0;
    }

    /** Removes and returns up to {@code n} keys, the oldest first. */
    List<ByteBuffer> take(int n) {
        if (keys.size() <= n) {
            List<ByteBuffer> all = new ArrayList<>(keys);
            clear();
            return all;
        }
        List<ByteBuffer> head = new ArrayList<>(keys.subList(0, n));
        keys.subList(0, n).clear();
        for (ByteBuffer k : head) {
            bytes -= keyBytes(k);
        }
        return head;
    }

    @Override
    public Iterator<ByteBuffer> iterator() {
        return Collections.unmodifiableList(keys).iterator();
    }

    @Override
    public int size() {
        return keys.size();
    }

    /** The estimated heap bytes of the keys. */
    long bytes() {
        return bytes;
    }
}
