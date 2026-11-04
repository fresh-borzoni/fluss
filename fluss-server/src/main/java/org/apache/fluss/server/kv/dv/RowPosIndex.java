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

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.IntPredicate;

/**
 * Operations wrapper for the RowPosIndex Column Family.
 *
 * <p>The RowPosIndex maps RowId (8-byte BigEndian long) to FilePos (varint-encoded file_id +
 * row_position). This index enables O(1) lookup of a row's physical location given its logical
 * RowId.
 */
public class RowPosIndex {

    private final RocksDB db;
    private final ColumnFamilyHandle cfHandle;
    private final WriteOptions writeOptions;

    RowPosIndex(RocksDB db, ColumnFamilyHandle cfHandle, WriteOptions writeOptions) {
        this.db = db;
        this.cfHandle = cfHandle;
        this.writeOptions = writeOptions;
    }

    /** Point-get: looks up the FilePos for the given rowId. Returns null if not found. */
    @Nullable
    public FilePos get(long rowId) throws IOException {
        try {
            byte[] value = db.get(cfHandle, encodeRowId(rowId));
            return value == null ? null : FilePos.decode(value);
        } catch (RocksDBException e) {
            throw new IOException("Failed to get RowPosIndex entry for rowId " + rowId, e);
        }
    }

    /** Looks up by encoded primary key, which keys the index in derived mode. */
    @Nullable
    public FilePos getByKey(byte[] key) throws IOException {
        try {
            byte[] value = db.get(cfHandle, key);
            return value == null ? null : FilePos.decode(value);
        } catch (RocksDBException e) {
            throw new IOException("Failed to get position index entry", e);
        }
    }

    /** Batched {@link #get(long)}. */
    public List<FilePos> multiGet(List<Long> rowIds) throws IOException {
        List<byte[]> keys = new ArrayList<>(rowIds.size());
        for (long id : rowIds) {
            keys.add(encodeRowId(id));
        }
        return multiGetByKeys(keys);
    }

    /** Batched {@link #getByKey}; an entry is null when the key is not in the index. */
    public List<FilePos> multiGetByKeys(List<byte[]> keys) throws IOException {
        List<FilePos> out = new ArrayList<>(keys.size());
        if (keys.isEmpty()) {
            return out;
        }
        try {
            List<byte[]> values =
                    db.multiGetAsList(Collections.nCopies(keys.size(), cfHandle), keys);
            for (byte[] v : values) {
                out.add(v == null ? null : FilePos.decode(v));
            }
            return out;
        } catch (RocksDBException e) {
            throw new IOException("Failed to multi-get position index entries", e);
        }
    }

    /** Writes a rowId to filePos mapping. */
    public void put(long rowId, FilePos filePos) throws IOException {
        try {
            db.put(cfHandle, writeOptions, encodeRowId(rowId), filePos.encode());
        } catch (RocksDBException e) {
            throw new IOException("Failed to put RowPosIndex entry for rowId " + rowId, e);
        }
    }

    /** Deletes the mapping for the given rowId. */
    public void delete(long rowId) throws IOException {
        try {
            db.delete(cfHandle, writeOptions, encodeRowId(rowId));
        } catch (RocksDBException e) {
            throw new IOException("Failed to delete RowPosIndex entry for rowId " + rowId, e);
        }
    }

    /** Adds a put operation to the given WriteBatch. */
    public void put(WriteBatch batch, long rowId, FilePos filePos) throws IOException {
        try {
            batch.put(cfHandle, encodeRowId(rowId), filePos.encode());
        } catch (RocksDBException e) {
            throw new IOException(
                    "Failed to add RowPosIndex put to WriteBatch for rowId " + rowId, e);
        }
    }

    /** Adds a delete operation to the given WriteBatch. */
    public void delete(WriteBatch batch, long rowId) throws IOException {
        try {
            batch.delete(cfHandle, encodeRowId(rowId));
        } catch (RocksDBException e) {
            throw new IOException(
                    "Failed to add RowPosIndex delete to WriteBatch for rowId " + rowId, e);
        }
    }

    /**
     * Deletes every RowId-&gt;FilePos entry whose FilePos points to one of the given fileIds.
     * Called when files are removed (compaction/cleanup) so stale mappings can't later resolve a
     * PendingDelete onto a position that a surviving/live row now occupies.
     */
    public void pruneFiles(Set<Integer> fileIds) throws IOException {
        if (fileIds.isEmpty()) {
            return;
        }
        List<byte[]> keysToDelete = new ArrayList<>();
        try (RocksIterator it = db.newIterator(cfHandle)) {
            it.seekToFirst();
            while (it.isValid()) {
                if (fileIds.contains(FilePos.decode(it.value()).fileId())) {
                    keysToDelete.add(it.key());
                }
                it.next();
            }
        }
        try {
            for (byte[] key : keysToDelete) {
                db.delete(cfHandle, writeOptions, key);
            }
        } catch (RocksDBException e) {
            throw new IOException("Failed to prune RowPosIndex entries for removed files", e);
        }
    }

    /**
     * Writes the index as of {@code snapshot} into one SST at {@code sstPath} and returns the entry
     * count. An empty index writes no file.
     */
    public long exportTo(String sstPath, @Nullable org.rocksdb.Snapshot snapshot)
            throws IOException {
        return exportTo(sstPath, snapshot, null);
    }

    /** Like {@link #exportTo(String, org.rocksdb.Snapshot)}, filtered by file id. */
    public long exportTo(
            String sstPath,
            @Nullable org.rocksdb.Snapshot snapshot,
            @Nullable IntPredicate keepFileId)
            throws IOException {
        long n = 0;
        try (org.rocksdb.EnvOptions env = new org.rocksdb.EnvOptions();
                org.rocksdb.Options options = new org.rocksdb.Options();
                org.rocksdb.SstFileWriter writer = new org.rocksdb.SstFileWriter(env, options);
                org.rocksdb.ReadOptions readOptions = new org.rocksdb.ReadOptions()) {
            if (snapshot != null) {
                readOptions.setSnapshot(snapshot);
            }
            boolean opened = false;
            try (RocksIterator it = db.newIterator(cfHandle, readOptions)) {
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    if (keepFileId != null
                            && !keepFileId.test(FilePos.decode(it.value()).fileId())) {
                        continue;
                    }
                    if (!opened) {
                        writer.open(sstPath);
                        opened = true;
                    }
                    writer.put(it.key(), it.value());
                    n++;
                }
            }
            if (opened) {
                writer.finish();
            }
        } catch (RocksDBException e) {
            throw new IOException("Failed to export RowPosIndex to " + sstPath, e);
        }
        return n;
    }

    /** Ingests external SST files into the RowPosIndex column family. */
    public void ingestExternalFile(List<String> sstPaths) throws IOException {
        try (IngestExternalFileOptions options = new IngestExternalFileOptions()) {
            options.setMoveFiles(true);
            db.ingestExternalFile(cfHandle, sstPaths, options);
        } catch (RocksDBException e) {
            throw new IOException("Failed to ingest SST files into RowPosIndex", e);
        }
    }

    /** Encodes a rowId as an 8-byte BigEndian key. */
    static byte[] encodeRowId(long rowId) {
        byte[] key = new byte[8];
        key[0] = (byte) (rowId >>> 56);
        key[1] = (byte) (rowId >>> 48);
        key[2] = (byte) (rowId >>> 40);
        key[3] = (byte) (rowId >>> 32);
        key[4] = (byte) (rowId >>> 24);
        key[5] = (byte) (rowId >>> 16);
        key[6] = (byte) (rowId >>> 8);
        key[7] = (byte) rowId;
        return key;
    }

    /** Decodes a rowId from an 8-byte BigEndian key. */
    static long decodeRowId(byte[] key) {
        return ((long) (key[0] & 0xFF) << 56)
                | ((long) (key[1] & 0xFF) << 48)
                | ((long) (key[2] & 0xFF) << 40)
                | ((long) (key[3] & 0xFF) << 32)
                | ((long) (key[4] & 0xFF) << 24)
                | ((long) (key[5] & 0xFF) << 16)
                | ((long) (key[6] & 0xFF) << 8)
                | ((long) (key[7] & 0xFF));
    }
}
