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

package org.apache.fluss.server.kv.dv;

import org.apache.fluss.annotation.VisibleForTesting;

import io.github.fluss_contrib.rocksdb.ColumnFamilyHandle;
import io.github.fluss_contrib.rocksdb.ReadOptions;
import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.RocksDBException;
import io.github.fluss_contrib.rocksdb.RocksIterator;
import io.github.fluss_contrib.rocksdb.Snapshot;
import io.github.fluss_contrib.rocksdb.WriteBatch;
import io.github.fluss_contrib.rocksdb.WriteOptions;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * The spilled log tails of a bucket, in a column family of its DV RocksDB. A tail's pending changes
 * are keyed by their offset, so an advance removes the ones a newer snapshot holds with one range
 * delete. A replay that spills also keeps its map of each key's last version here. Each tail and
 * map has its own 8-byte id prefix, so a replay can build a new tail while the installed one
 * serves. Written without WAL and cleared when the RocksDB opens, because tails are rebuilt from
 * the log.
 */
public class TailStore {

    private static final int PREFIX_BYTES = Long.BYTES;

    private final RocksDB db;
    private final ColumnFamilyHandle cfHandle;
    private final WriteOptions writeOptions;
    // a test hook: makes writes fail, as a full or broken disk does
    @Nullable private volatile IOException writeFailure;

    TailStore(RocksDB db, ColumnFamilyHandle cfHandle, WriteOptions writeOptions) {
        this.db = db;
        this.cfHandle = cfHandle;
        this.writeOptions = writeOptions;
    }

    /** Removes every stored tail. */
    void clear() throws IOException {
        try {
            byte[] end = new byte[PREFIX_BYTES + 1];
            Arrays.fill(end, (byte) 0xFF);
            db.deleteRange(cfHandle, writeOptions, new byte[0], end);
        } catch (RocksDBException e) {
            throw new IOException("Failed to clear the spilled log tails", e);
        }
    }

    /** Removes tail or map {@code tailId}. */
    void drop(long tailId) throws IOException {
        try {
            db.deleteRange(cfHandle, writeOptions, prefix(tailId), prefix(tailId + 1));
        } catch (RocksDBException e) {
            throw new IOException("Failed to drop a spilled log tail", e);
        }
    }

    // ---- The pending changes of a tail, keyed by their offset. ----

    /** Appends pending changes of tail {@code tailId}. */
    void writePending(long tailId, List<DvTail.Pending> changes) throws IOException {
        checkWritable();
        if (changes.isEmpty()) {
            return;
        }
        try (WriteBatch batch = new WriteBatch()) {
            for (DvTail.Pending c : changes) {
                batch.put(cfHandle, pendingKey(tailId, c.offset), pendingValue(c));
            }
            db.write(writeOptions, batch);
        } catch (RocksDBException e) {
            throw new IOException("Failed to write a spilled log tail", e);
        }
    }

    /** Removes the pending changes of tail {@code tailId} below {@code offset}. */
    void deletePendingBelow(long tailId, long offset) throws IOException {
        checkWritable();
        try {
            db.deleteRange(cfHandle, writeOptions, prefix(tailId), pendingKey(tailId, offset));
        } catch (RocksDBException e) {
            throw new IOException("Failed to trim a spilled log tail", e);
        }
    }

    /**
     * Iterates the pending changes of tail {@code tailId} in offset order; the caller closes it.
     */
    PendingCursor pendingCursor(long tailId) {
        return new PendingCursor(tailId, null, 0, Long.MAX_VALUE);
    }

    /**
     * Iterates the pending changes of tail {@code tailId} at offsets in [{@code fromOffset}, {@code
     * toOffset}) as of {@code snapshot} (the latest if null); the caller closes it.
     */
    PendingCursor pendingCursor(
            long tailId, @Nullable Snapshot snapshot, long fromOffset, long toOffset) {
        return new PendingCursor(tailId, snapshot, fromOffset, toOffset);
    }

    /** The pending changes of one tail, a chunk at a time. */
    final class PendingCursor implements Closeable {
        private final byte[] end;
        private final ReadOptions readOptions = new ReadOptions();
        private final RocksIterator it;

        private PendingCursor(
                long tailId, @Nullable Snapshot snapshot, long fromOffset, long toOffset) {
            this.end =
                    toOffset == Long.MAX_VALUE ? prefix(tailId + 1) : pendingKey(tailId, toOffset);
            if (snapshot != null) {
                readOptions.setSnapshot(snapshot);
            }
            this.it = db.newIterator(cfHandle, readOptions);
            it.seek(fromOffset <= 0 ? prefix(tailId) : pendingKey(tailId, fromOffset));
        }

        /**
         * Reads the next at most {@code max} changes and adds the keys of those whose superseded
         * version is below {@code startOffset}, whose lake rows are masked, to {@code out}. Returns
         * false once there are no changes left.
         */
        boolean nextLakeKeys(int max, long startOffset, List<byte[]> out) throws IOException {
            return nextLakeKeys(max, startOffset, null, out);
        }

        /** As {@link #nextLakeKeys(int, long, List)}, only the keys {@code filter} accepts. */
        boolean nextLakeKeys(
                int max, long startOffset, @Nullable Predicate<byte[]> filter, List<byte[]> out)
                throws IOException {
            int read = 0;
            while (read < max && it.isValid()) {
                if (compareUnsigned(it.key(), end) >= 0) {
                    break;
                }
                byte[] value = it.value();
                if (ByteBuffer.wrap(value).getLong() < startOffset) {
                    byte[] key = Arrays.copyOfRange(value, Long.BYTES, value.length);
                    if (filter == null || filter.test(key)) {
                        out.add(key);
                    }
                }
                read++;
                it.next();
            }
            try {
                it.status();
            } catch (RocksDBException e) {
                throw new IOException("Failed to iterate a spilled log tail", e);
            }
            return read > 0;
        }

        @Override
        public void close() {
            it.close();
            readOptions.close();
        }
    }

    /** The bytes a pending change takes on disk, before compression. */
    static long storedPendingBytes(byte[] key) {
        return PREFIX_BYTES + 2L * Long.BYTES + key.length;
    }

    // ---- A replay's map of each key's last version. ----

    /** The last versions of {@code keys} in map {@code mapId}, -1 for keys it does not hold. */
    long[] getVersions(long mapId, List<byte[]> keys) throws IOException {
        long[] out = new long[keys.size()];
        if (keys.isEmpty()) {
            return out;
        }
        List<byte[]> storeKeys = new ArrayList<>(keys.size());
        for (byte[] key : keys) {
            storeKeys.add(versionKey(mapId, key));
        }
        try {
            List<byte[]> values =
                    db.multiGetAsList(Collections.nCopies(keys.size(), cfHandle), storeKeys);
            for (int i = 0; i < out.length; i++) {
                byte[] value = values.get(i);
                out[i] = value == null ? -1 : ByteBuffer.wrap(value).getLong();
            }
            return out;
        } catch (RocksDBException e) {
            throw new IOException("Failed to read a spilled map of last versions", e);
        }
    }

    /** Writes the last versions of keys in map {@code mapId}. */
    void putVersions(long mapId, Map<ByteBuffer, Long> versions) throws IOException {
        checkWritable();
        if (versions.isEmpty()) {
            return;
        }
        try (WriteBatch batch = new WriteBatch()) {
            for (Map.Entry<ByteBuffer, Long> e : versions.entrySet()) {
                batch.put(
                        cfHandle,
                        versionKey(mapId, e.getKey().array()),
                        ByteBuffer.allocate(Long.BYTES).putLong(e.getValue()).array());
            }
            db.write(writeOptions, batch);
        } catch (RocksDBException e) {
            throw new IOException("Failed to write a spilled map of last versions", e);
        }
    }

    private void checkWritable() throws IOException {
        IOException failure = writeFailure;
        if (failure != null) {
            throw failure;
        }
    }

    @VisibleForTesting
    void failWrites(@Nullable IOException failure) {
        this.writeFailure = failure;
    }

    private static byte[] prefix(long tailId) {
        return ByteBuffer.allocate(PREFIX_BYTES).putLong(tailId).array();
    }

    private static byte[] pendingKey(long tailId, long offset) {
        return ByteBuffer.allocate(PREFIX_BYTES + Long.BYTES)
                .putLong(tailId)
                .putLong(offset)
                .array();
    }

    private static byte[] pendingValue(DvTail.Pending change) {
        return ByteBuffer.allocate(Long.BYTES + change.key.length)
                .putLong(change.previous)
                .put(change.key)
                .array();
    }

    private static byte[] versionKey(long mapId, byte[] key) {
        return ByteBuffer.allocate(PREFIX_BYTES + key.length).putLong(mapId).put(key).array();
    }

    private static int compareUnsigned(byte[] a, byte[] b) {
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
