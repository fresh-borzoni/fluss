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

import io.github.fluss_contrib.rocksdb.ColumnFamilyHandle;
import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.RocksDBException;
import io.github.fluss_contrib.rocksdb.WriteBatch;
import io.github.fluss_contrib.rocksdb.WriteOptions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Access to the FileDict column family, a bidirectional mapping between file paths (String) and
 * file ids (int).
 *
 * <p>Both directions are stored in one column family, told apart by a 1-byte key prefix:
 *
 * <ul>
 *   <li>Forward (path → id): key = {@code 0x00 + file_path_bytes}, value = {@code
 *       BigEndian(file_id)}.
 *   <li>Reverse (id → path): key = {@code 0x01 + BigEndian(file_id)}, value = {@code
 *       file_path_bytes}.
 * </ul>
 *
 * <p>Both directions are written atomically in one WriteBatch.
 */
public class FileDict {

    private static final byte PREFIX_PATH_TO_ID = 0x00;
    private static final byte PREFIX_ID_TO_PATH = 0x01;

    private final RocksDB db;
    private final ColumnFamilyHandle cfHandle;
    private final WriteOptions writeOptions;

    FileDict(RocksDB db, ColumnFamilyHandle cfHandle, WriteOptions writeOptions) {
        this.db = db;
        this.cfHandle = cfHandle;
        this.writeOptions = writeOptions;
    }

    /** Looks up the file ID for the given file path. Returns -1 if not found. */
    public int getFileId(String filePath) throws IOException {
        byte[] key = encodePathToIdKey(filePath);
        try {
            byte[] value = db.get(cfHandle, key);
            if (value == null) {
                return -1;
            }
            return decodeFileId(value);
        } catch (RocksDBException e) {
            throw new IOException("Failed to get FileDict fileId for path " + filePath, e);
        }
    }

    /** Looks up the file path for the given file ID. Returns null if not found. */
    @Nullable
    public String getFilePath(int fileId) throws IOException {
        byte[] key = encodeIdToPathKey(fileId);
        try {
            byte[] value = db.get(cfHandle, key);
            if (value == null) {
                return null;
            }
            return new String(value, StandardCharsets.UTF_8);
        } catch (RocksDBException e) {
            throw new IOException("Failed to get FileDict filePath for fileId " + fileId, e);
        }
    }

    /** Returns all fileId to path entries as of {@code snapshot}, or the latest if null. */
    public Map<Integer, String> idToPathEntries(
            @Nullable io.github.fluss_contrib.rocksdb.Snapshot snapshot) throws IOException {
        Map<Integer, String> out = new HashMap<>();
        try (io.github.fluss_contrib.rocksdb.ReadOptions readOptions =
                new io.github.fluss_contrib.rocksdb.ReadOptions()) {
            if (snapshot != null) {
                readOptions.setSnapshot(snapshot);
            }
            try (io.github.fluss_contrib.rocksdb.RocksIterator it =
                    db.newIterator(cfHandle, readOptions)) {
                for (it.seek(new byte[] {PREFIX_ID_TO_PATH}); it.isValid(); it.next()) {
                    byte[] k = it.key();
                    if (k.length != 5 || k[0] != PREFIX_ID_TO_PATH) {
                        break;
                    }
                    int id =
                            ((k[1] & 0xFF) << 24)
                                    | ((k[2] & 0xFF) << 16)
                                    | ((k[3] & 0xFF) << 8)
                                    | (k[4] & 0xFF);
                    out.put(id, new String(it.value(), StandardCharsets.UTF_8));
                }
            }
        }
        return out;
    }

    /** Writes a bidirectional mapping atomically using a WriteBatch. */
    public void put(int fileId, String filePath) throws IOException {
        try (WriteBatch batch = new WriteBatch()) {
            putToBatch(batch, fileId, filePath);
            db.write(writeOptions, batch);
        } catch (RocksDBException e) {
            throw new IOException(
                    "Failed to put FileDict entry for fileId " + fileId + " and path " + filePath,
                    e);
        }
    }

    /** Deletes the bidirectional mapping for the given fileId. */
    public void delete(int fileId) throws IOException {
        String filePath = getFilePath(fileId);
        if (filePath == null) {
            return;
        }
        try (WriteBatch batch = new WriteBatch()) {
            batch.delete(cfHandle, encodePathToIdKey(filePath));
            batch.delete(cfHandle, encodeIdToPathKey(fileId));
            db.write(writeOptions, batch);
        } catch (RocksDBException e) {
            throw new IOException("Failed to delete FileDict entry for fileId " + fileId, e);
        }
    }

    private void putToBatch(WriteBatch batch, int fileId, String filePath) throws IOException {
        byte[] pathToIdKey = encodePathToIdKey(filePath);
        byte[] idToPathKey = encodeIdToPathKey(fileId);
        byte[] fileIdBytes = encodeFileIdValue(fileId);
        byte[] filePathBytes = filePath.getBytes(StandardCharsets.UTF_8);

        try {
            batch.put(cfHandle, pathToIdKey, fileIdBytes);
            batch.put(cfHandle, idToPathKey, filePathBytes);
        } catch (RocksDBException e) {
            throw new IOException(
                    "Failed to add FileDict entry to WriteBatch for fileId " + fileId, e);
        }
    }

    // ---- Key/Value encoding ----

    /** Encodes a path→id key: PREFIX_PATH_TO_ID + file_path_bytes. */
    private static byte[] encodePathToIdKey(String filePath) {
        byte[] pathBytes = filePath.getBytes(StandardCharsets.UTF_8);
        byte[] key = new byte[1 + pathBytes.length];
        key[0] = PREFIX_PATH_TO_ID;
        System.arraycopy(pathBytes, 0, key, 1, pathBytes.length);
        return key;
    }

    /** Encodes an id→path key: PREFIX_ID_TO_PATH + BigEndian(file_id). */
    private static byte[] encodeIdToPathKey(int fileId) {
        byte[] key = new byte[5];
        key[0] = PREFIX_ID_TO_PATH;
        key[1] = (byte) (fileId >>> 24);
        key[2] = (byte) (fileId >>> 16);
        key[3] = (byte) (fileId >>> 8);
        key[4] = (byte) fileId;
        return key;
    }

    /** Encodes a file ID as a 4-byte BigEndian value. */
    private static byte[] encodeFileIdValue(int fileId) {
        byte[] value = new byte[4];
        value[0] = (byte) (fileId >>> 24);
        value[1] = (byte) (fileId >>> 16);
        value[2] = (byte) (fileId >>> 8);
        value[3] = (byte) fileId;
        return value;
    }

    /** Decodes a file ID from a 4-byte BigEndian value. */
    private static int decodeFileId(byte[] value) {
        return ((value[0] & 0xFF) << 24)
                | ((value[1] & 0xFF) << 16)
                | ((value[2] & 0xFF) << 8)
                | (value[3] & 0xFF);
    }
}
