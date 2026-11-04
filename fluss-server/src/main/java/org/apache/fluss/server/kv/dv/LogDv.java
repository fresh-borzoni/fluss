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

import org.apache.fluss.server.utils.RoaringBitmapUtils;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Operations wrapper for the LogDv Column Family.
 *
 * <p>LogDv uses a partition-style bitmap approach: log offsets are divided into fixed-size ranges
 * (default 1024). Each range is stored as a separate RocksDB entry with a 32-bit RoaringBitmap
 * containing the relative offsets within that range.
 *
 * <p>Key format: 8-byte BigEndian long representing the range start offset. Value format:
 * serialized 32-bit RoaringBitmap (since offsets within a range are always < RANGE_SIZE).
 */
public class LogDv {

    static final int RANGE_SIZE = 1024;

    private final RocksDB db;
    private final ColumnFamilyHandle cfHandle;
    private final WriteOptions writeOptions;

    LogDv(RocksDB db, ColumnFamilyHandle cfHandle, WriteOptions writeOptions) {
        this.db = db;
        this.cfHandle = cfHandle;
        this.writeOptions = writeOptions;
    }

    /** Marks the specified log offset as deleted (read-modify-write). */
    public void markDeleted(long offset) throws IOException {
        long rangeStart = (offset / RANGE_SIZE) * RANGE_SIZE;
        int offsetInRange = (int) (offset - rangeStart);

        byte[] key = encodeRangeStart(rangeStart);
        RoaringBitmap bitmap = getBitmap(key);
        if (bitmap == null) {
            bitmap = new RoaringBitmap();
        }
        bitmap.add(offsetInRange);
        putBitmap(key, bitmap);
    }

    /** Batched variant of {@link #markDeleted}: one read-modify-write per touched range. */
    public void markDeletedAll(Collection<Long> offsets) throws IOException {
        Map<Long, List<Integer>> byRange = new HashMap<>();
        for (long offset : offsets) {
            long rangeStart = (offset / RANGE_SIZE) * RANGE_SIZE;
            byRange.computeIfAbsent(rangeStart, k -> new ArrayList<>())
                    .add((int) (offset - rangeStart));
        }
        for (Map.Entry<Long, List<Integer>> e : byRange.entrySet()) {
            byte[] key = encodeRangeStart(e.getKey());
            RoaringBitmap bitmap = getBitmap(key);
            if (bitmap == null) {
                bitmap = new RoaringBitmap();
            }
            for (int o : e.getValue()) {
                bitmap.add(o);
            }
            putBitmap(key, bitmap);
        }
    }

    /** Checks whether the specified log offset has been marked as deleted. */
    public boolean isDeleted(long offset) throws IOException {
        long rangeStart = (offset / RANGE_SIZE) * RANGE_SIZE;
        int offsetInRange = (int) (offset - rangeStart);

        byte[] key = encodeRangeStart(rangeStart);
        RoaringBitmap bitmap = getBitmap(key);
        return bitmap != null && bitmap.contains(offsetInRange);
    }

    /**
     * Returns a snapshot of all deleted offsets within the range [fromOffset, toOffset). The
     * returned Roaring64Bitmap contains absolute offset values.
     */
    public Roaring64Bitmap snapshot(long fromOffset, long toOffset) throws IOException {
        Roaring64Bitmap result = new Roaring64Bitmap();

        // Calculate the range of rangeStarts we need to scan
        long firstRangeStart = (fromOffset / RANGE_SIZE) * RANGE_SIZE;
        long lastRangeStart = ((toOffset - 1) / RANGE_SIZE) * RANGE_SIZE;

        byte[] seekKey = encodeRangeStart(firstRangeStart);
        byte[] endKey = encodeRangeStart(lastRangeStart + RANGE_SIZE);

        try (ReadOptions readOptions = new ReadOptions();
                RocksIterator iterator = db.newIterator(cfHandle, readOptions)) {
            iterator.seek(seekKey);
            while (iterator.isValid()) {
                byte[] currentKey = iterator.key();
                // Stop if we've passed the last relevant range
                if (compareKeys(currentKey, endKey) >= 0) {
                    break;
                }

                long rangeStart = decodeRangeStart(currentKey);
                RoaringBitmap bitmap = new RoaringBitmap();
                RoaringBitmapUtils.deserializeRoaringBitmap32(bitmap, iterator.value());

                // Collect deleted offsets that fall within [fromOffset, toOffset)
                for (int offsetInRange : bitmap) {
                    long absoluteOffset = rangeStart + offsetInRange;
                    if (absoluteOffset >= fromOffset && absoluteOffset < toOffset) {
                        result.add(absoluteOffset);
                    }
                }

                iterator.next();
            }
        }

        return result;
    }

    /**
     * Cleans up expired LogDv entries. Deletes all range entries where the entire range is before
     * the cleanup offset (i.e., rangeStart + RANGE_SIZE <= cleanupOffset).
     */
    public void cleanup(long cleanupOffset) throws IOException {
        try (ReadOptions readOptions = new ReadOptions();
                RocksIterator iterator = db.newIterator(cfHandle, readOptions)) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                long rangeStart = decodeRangeStart(iterator.key());
                if (rangeStart + RANGE_SIZE > cleanupOffset) {
                    break;
                }
                try {
                    db.delete(cfHandle, writeOptions, iterator.key());
                } catch (RocksDBException e) {
                    throw new IOException(
                            "Failed to delete LogDv range at " + rangeStart + " during cleanup", e);
                }
                iterator.next();
            }
        }
    }

    private RoaringBitmap getBitmap(byte[] key) throws IOException {
        try {
            byte[] value = db.get(cfHandle, key);
            if (value == null) {
                return null;
            }
            RoaringBitmap bitmap = new RoaringBitmap();
            RoaringBitmapUtils.deserializeRoaringBitmap32(bitmap, value);
            return bitmap;
        } catch (RocksDBException e) {
            throw new IOException("Failed to get LogDv bitmap", e);
        }
    }

    private void putBitmap(byte[] key, RoaringBitmap bitmap) throws IOException {
        try {
            byte[] value = RoaringBitmapUtils.serializeRoaringBitmap32(bitmap);
            db.put(cfHandle, writeOptions, key, value);
        } catch (RocksDBException e) {
            throw new IOException("Failed to put LogDv bitmap", e);
        }
    }

    /** Encodes a range start offset as an 8-byte BigEndian key. */
    static byte[] encodeRangeStart(long rangeStart) {
        byte[] key = new byte[8];
        key[0] = (byte) (rangeStart >>> 56);
        key[1] = (byte) (rangeStart >>> 48);
        key[2] = (byte) (rangeStart >>> 40);
        key[3] = (byte) (rangeStart >>> 32);
        key[4] = (byte) (rangeStart >>> 24);
        key[5] = (byte) (rangeStart >>> 16);
        key[6] = (byte) (rangeStart >>> 8);
        key[7] = (byte) rangeStart;
        return key;
    }

    /** Decodes a range start offset from an 8-byte BigEndian key. */
    static long decodeRangeStart(byte[] key) {
        return ((long) (key[0] & 0xFF) << 56)
                | ((long) (key[1] & 0xFF) << 48)
                | ((long) (key[2] & 0xFF) << 40)
                | ((long) (key[3] & 0xFF) << 32)
                | ((long) (key[4] & 0xFF) << 24)
                | ((long) (key[5] & 0xFF) << 16)
                | ((long) (key[6] & 0xFF) << 8)
                | ((long) (key[7] & 0xFF));
    }

    /** Compares two byte array keys lexicographically. */
    private static int compareKeys(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int diff = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (diff != 0) {
                return diff;
            }
        }
        return a.length - b.length;
    }
}
