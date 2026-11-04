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

import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Operations wrapper for the LakeDv Column Family.
 *
 * <p>LakeDv stores per-file deletion vectors using Roaring64Bitmap, aligned with the Iceberg DV
 * format. Each entry maps a file_id (4-byte BigEndian int) to a serialized Roaring64Bitmap
 * containing the deleted row positions within that file.
 *
 * <p>Although row positions typically fit within the 32-bit range, 64-bit Roaring64Bitmap is used
 * for consistency with Iceberg and to avoid potential overflow. When values are small,
 * Roaring64Bitmap degrades to a single 32-bit container with minimal overhead.
 */
public class LakeDv {

    private final RocksDB db;
    private final ColumnFamilyHandle cfHandle;
    private final WriteOptions writeOptions;

    LakeDv(RocksDB db, ColumnFamilyHandle cfHandle, WriteOptions writeOptions) {
        this.db = db;
        this.cfHandle = cfHandle;
        this.writeOptions = writeOptions;
    }

    /** Gets the deleted position bitmap for the specified file. Returns null if not found. */
    @Nullable
    public Roaring64Bitmap get(int fileId) throws IOException {
        try {
            byte[] value = db.get(cfHandle, encodeFileId(fileId));
            if (value == null) {
                return null;
            }
            Roaring64Bitmap bitmap = new Roaring64Bitmap();
            RoaringBitmapUtils.deserializeRoaringBitmap64(bitmap, value);
            return bitmap;
        } catch (RocksDBException e) {
            throw new IOException("Failed to get LakeDv entry for fileId " + fileId, e);
        }
    }

    /** Sets the deleted position bitmap for the specified file (full replacement). */
    public void put(int fileId, Roaring64Bitmap bitmap) throws IOException {
        try {
            byte[] value = RoaringBitmapUtils.serializeRoaringBitmap64(bitmap);
            db.put(cfHandle, writeOptions, encodeFileId(fileId), value);
        } catch (RocksDBException e) {
            throw new IOException("Failed to put LakeDv entry for fileId " + fileId, e);
        }
    }

    /**
     * Marks a specific row position as deleted for the given file (read-modify-write). If no bitmap
     * exists for the file, a new one is created.
     */
    public void markDeleted(int fileId, long rowPosition) throws IOException {
        Roaring64Bitmap bitmap = get(fileId);
        if (bitmap == null) {
            bitmap = new Roaring64Bitmap();
        }
        bitmap.add(rowPosition);
        put(fileId, bitmap);
    }

    /** Batched variant of {@link #markDeleted}: one read-modify-write per touched file. */
    public void markDeletedAll(Map<Integer, List<Long>> positionsByFile) throws IOException {
        for (Map.Entry<Integer, List<Long>> e : positionsByFile.entrySet()) {
            Roaring64Bitmap bitmap = get(e.getKey());
            if (bitmap == null) {
                bitmap = new Roaring64Bitmap();
            }
            for (long pos : e.getValue()) {
                bitmap.addLong(pos);
            }
            put(e.getKey(), bitmap);
        }
    }

    /** Deletes the entire LakeDv entry for the specified file. */
    public void delete(int fileId) throws IOException {
        try {
            db.delete(cfHandle, writeOptions, encodeFileId(fileId));
        } catch (RocksDBException e) {
            throw new IOException("Failed to delete LakeDv entry for fileId " + fileId, e);
        }
    }

    /** Adds a delete operation to the given WriteBatch. */
    public void delete(WriteBatch batch, int fileId) throws IOException {
        try {
            batch.delete(cfHandle, encodeFileId(fileId));
        } catch (RocksDBException e) {
            throw new IOException(
                    "Failed to add LakeDv delete to WriteBatch for fileId " + fileId, e);
        }
    }

    /** Returns all LakeDv entries. Used for union read to collect all deletion vectors. */
    public Map<Integer, Roaring64Bitmap> getAll() throws IOException {
        Map<Integer, Roaring64Bitmap> result = new HashMap<>();
        try (ReadOptions readOptions = new ReadOptions();
                RocksIterator iterator = db.newIterator(cfHandle, readOptions)) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                int fileId = decodeFileId(iterator.key());
                Roaring64Bitmap bitmap = new Roaring64Bitmap();
                RoaringBitmapUtils.deserializeRoaringBitmap64(bitmap, iterator.value());
                result.put(fileId, bitmap);
                iterator.next();
            }
        }
        return result;
    }

    /** Encodes a fileId as a 4-byte BigEndian key. */
    static byte[] encodeFileId(int fileId) {
        byte[] key = new byte[4];
        key[0] = (byte) (fileId >>> 24);
        key[1] = (byte) (fileId >>> 16);
        key[2] = (byte) (fileId >>> 8);
        key[3] = (byte) fileId;
        return key;
    }

    /** Decodes a fileId from a 4-byte BigEndian key. */
    static int decodeFileId(byte[] key) {
        return ((key[0] & 0xFF) << 24)
                | ((key[1] & 0xFF) << 16)
                | ((key[2] & 0xFF) << 8)
                | (key[3] & 0xFF);
    }
}
