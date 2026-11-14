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
 * The change lists of a bucket on disk, in a column family of its DV RocksDB. A change list's
 * pending changes are keyed by their offset, so a switch removes the ones a newer snapshot holds
 * with one range delete. A replay whose map of each key's last version outgrows the heap keeps it
 * here. Each change list and map has its own 8-byte id prefix, so a replay can build a new change
 * list while the installed one serves. Written without WAL and cleared when the RocksDB opens,
 * because change lists are rebuilt from the log.
 */
public class DvChangeListStore {

    private static final int PREFIX_BYTES = Long.BYTES;

    private final RocksDB db;
    private final ColumnFamilyHandle cfHandle;
    private final WriteOptions writeOptions;

    DvChangeListStore(RocksDB db, ColumnFamilyHandle cfHandle, WriteOptions writeOptions) {
        this.db = db;
        this.cfHandle = cfHandle;
        this.writeOptions = writeOptions;
    }

    /** Removes every stored change list. */
    void clear() throws IOException {
        try {
            byte[] end = new byte[PREFIX_BYTES + 1];
            Arrays.fill(end, (byte) 0xFF);
            db.deleteRange(cfHandle, writeOptions, new byte[0], end);
        } catch (RocksDBException e) {
            throw new IOException("Failed to clear the change lists", e);
        }
    }

    /** Removes change list or map {@code listId}. */
    void drop(long listId) throws IOException {
        try {
            db.deleteRange(cfHandle, writeOptions, prefix(listId), prefix(listId + 1));
        } catch (RocksDBException e) {
            throw new IOException("Failed to drop a change list", e);
        }
    }

    // ---- The pending changes of a change list, keyed by their offset. ----

    /** Appends pending changes of change list {@code listId}. */
    void writePending(long listId, List<DvChangeList.Pending> changes) throws IOException {
        if (changes.isEmpty()) {
            return;
        }
        try (WriteBatch batch = new WriteBatch()) {
            for (DvChangeList.Pending c : changes) {
                batch.put(cfHandle, pendingKey(listId, c.offset), pendingValue(c));
            }
            db.write(writeOptions, batch);
        } catch (RocksDBException e) {
            throw new IOException("Failed to write a change list", e);
        }
    }

    /** Removes the pending changes of change list {@code listId} below {@code offset}. */
    void deletePendingBelow(long listId, long offset) throws IOException {
        try {
            db.deleteRange(cfHandle, writeOptions, prefix(listId), pendingKey(listId, offset));
        } catch (RocksDBException e) {
            throw new IOException("Failed to trim a change list", e);
        }
    }

    /**
     * Iterates the pending changes of change list {@code listId} at offsets in [{@code fromOffset},
     * {@code toOffset}) as of {@code snapshot} (the latest if null); the caller closes it.
     */
    PendingCursor pendingCursor(
            long listId, @Nullable Snapshot snapshot, long fromOffset, long toOffset) {
        return new PendingCursor(listId, snapshot, fromOffset, toOffset);
    }

    /** The pending changes of one change list, a chunk at a time. */
    final class PendingCursor implements Closeable {
        private final byte[] end;
        private final ReadOptions readOptions = new ReadOptions();
        private final RocksIterator it;

        private PendingCursor(
                long listId, @Nullable Snapshot snapshot, long fromOffset, long toOffset) {
            this.end =
                    toOffset == Long.MAX_VALUE ? prefix(listId + 1) : pendingKey(listId, toOffset);
            if (snapshot != null) {
                readOptions.setSnapshot(snapshot);
            }
            this.it = db.newIterator(cfHandle, readOptions);
            it.seek(fromOffset <= 0 ? prefix(listId) : pendingKey(listId, fromOffset));
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
                throw new IOException("Failed to iterate a change list", e);
            }
            return read > 0;
        }

        /**
         * Reads the next at most {@code max} changes whole, in offset order: offset, superseded
         * version and key. Returns false once there are no changes left.
         */
        boolean next(int max, DvChangeListSegment.EntrySink sink) throws IOException {
            int read = 0;
            while (read < max && it.isValid()) {
                byte[] storeKey = it.key();
                if (compareUnsigned(storeKey, end) >= 0) {
                    break;
                }
                byte[] value = it.value();
                sink.accept(
                        ByteBuffer.wrap(storeKey, PREFIX_BYTES, Long.BYTES).getLong(),
                        ByteBuffer.wrap(value).getLong(),
                        Arrays.copyOfRange(value, Long.BYTES, value.length));
                read++;
                it.next();
            }
            try {
                it.status();
            } catch (RocksDBException e) {
                throw new IOException("Failed to iterate a change list", e);
            }
            return read > 0;
        }

        /**
         * The offset of the next change the cursor reads, or {@link Long#MAX_VALUE} if it read them
         * all; a later cursor from there continues where this one stopped.
         */
        long nextOffset() {
            if (!it.isValid() || compareUnsigned(it.key(), end) >= 0) {
                return Long.MAX_VALUE;
            }
            return ByteBuffer.wrap(it.key(), PREFIX_BYTES, Long.BYTES).getLong();
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

    /** A key a replay's map of last versions does not hold. */
    static final long MISSING = Long.MIN_VALUE;

    /**
     * The last versions of {@code keys} in map {@code mapId}: -1 for a key whose last change
     * deleted it, {@link #MISSING} for keys the map does not hold.
     */
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
                out[i] = value == null ? MISSING : ByteBuffer.wrap(value).getLong();
            }
            return out;
        } catch (RocksDBException e) {
            throw new IOException("Failed to read a spilled map of last versions", e);
        }
    }

    /** Writes the last versions of keys in map {@code mapId}. */
    void putVersions(long mapId, Map<ByteBuffer, Long> versions) throws IOException {
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

    private static byte[] prefix(long listId) {
        return ByteBuffer.allocate(PREFIX_BYTES).putLong(listId).array();
    }

    private static byte[] pendingKey(long listId, long offset) {
        return ByteBuffer.allocate(PREFIX_BYTES + Long.BYTES)
                .putLong(listId)
                .putLong(offset)
                .array();
    }

    private static byte[] pendingValue(DvChangeList.Pending change) {
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
