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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.lake.dv.FilePos;

import io.github.fluss_contrib.rocksdb.ColumnFamilyHandle;
import io.github.fluss_contrib.rocksdb.EnvOptions;
import io.github.fluss_contrib.rocksdb.IngestExternalFileOptions;
import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.ReadOptions;
import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.RocksDBException;
import io.github.fluss_contrib.rocksdb.RocksIterator;
import io.github.fluss_contrib.rocksdb.Snapshot;
import io.github.fluss_contrib.rocksdb.SstFileReader;
import io.github.fluss_contrib.rocksdb.SstFileReaderIterator;
import io.github.fluss_contrib.rocksdb.SstFileWriter;
import io.github.fluss_contrib.rocksdb.WriteOptions;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

/**
 * Access to the RowPosIndex column family, the position index of the covered snapshot. It maps an
 * encoded primary key to the {@link FilePos} (file id and row position) of the key's live row.
 *
 * <p>The index is filled by ingesting the SSTs of published index changes and checkpoints, and is
 * exported to a checkpoint SST.
 */
public class RowPosIndex {

    // entries an export visits between two checks whether it is aborted; a power of two
    private static final int ABORT_CHECK_INTERVAL = 1024;
    // entries of a rewrite looked up at once
    private static final int REWRITE_CHUNK = 10_000;

    private final RocksDB db;
    private final ColumnFamilyHandle cfHandle;
    private final WriteOptions writeOptions;

    RowPosIndex(RocksDB db, ColumnFamilyHandle cfHandle, WriteOptions writeOptions) {
        this.db = db;
        this.cfHandle = cfHandle;
        this.writeOptions = writeOptions;
    }

    /** Looks up the position of an encoded primary key; returns null if it is not indexed. */
    @Nullable
    public FilePos getByKey(byte[] key) throws IOException {
        try {
            byte[] value = db.get(cfHandle, key);
            return value == null ? null : FilePos.decode(value);
        } catch (RocksDBException e) {
            throw new IOException("Failed to get position index entry", e);
        }
    }

    /** Batched {@link #getByKey}; an entry is null when the key is not in the index. */
    public List<FilePos> multiGetByKeys(List<byte[]> keys) throws IOException {
        return multiGetByKeys(keys, null);
    }

    /** Like {@link #multiGetByKeys(List)}, as of {@code snapshot} (the latest if null). */
    public List<FilePos> multiGetByKeys(List<byte[]> keys, @Nullable Snapshot snapshot)
            throws IOException {
        List<FilePos> out = new ArrayList<>(keys.size());
        if (keys.isEmpty()) {
            return out;
        }
        try (ReadOptions readOptions = new ReadOptions()) {
            if (snapshot != null) {
                readOptions.setSnapshot(snapshot);
            }
            List<byte[]> values =
                    db.multiGetAsList(
                            readOptions, Collections.nCopies(keys.size(), cfHandle), keys);
            for (byte[] v : values) {
                out.add(v == null ? null : FilePos.decode(v));
            }
            return out;
        } catch (RocksDBException e) {
            throw new IOException("Failed to multi-get position index entries", e);
        }
    }

    /** Writes the position of an encoded primary key. */
    @VisibleForTesting
    void put(byte[] key, FilePos filePos) throws IOException {
        try {
            db.put(cfHandle, writeOptions, key, filePos.encode());
        } catch (RocksDBException e) {
            throw new IOException("Failed to put position index entry", e);
        }
    }

    /** Deletes the position of an encoded primary key. */
    @VisibleForTesting
    void delete(byte[] key) throws IOException {
        try {
            db.delete(cfHandle, writeOptions, key);
        } catch (RocksDBException e) {
            throw new IOException("Failed to delete position index entry", e);
        }
    }

    /**
     * Writes the index as of {@code snapshot} (the latest if null) into one SST at {@code sstPath},
     * keeping only the entries whose file id passes {@code keepFileId} (all if null), and returns
     * the entry count. An empty index writes no file.
     */
    public long exportTo(
            String sstPath, @Nullable Snapshot snapshot, @Nullable IntPredicate keepFileId)
            throws IOException {
        return exportTo(sstPath, snapshot, keepFileId, () -> false);
    }

    /**
     * Like {@link #exportTo(String, Snapshot, IntPredicate)}, but stops with an IOException soon
     * after {@code aborted} turns true, e.g. when the RocksDB is about to close.
     */
    public long exportTo(
            String sstPath,
            @Nullable Snapshot snapshot,
            @Nullable IntPredicate keepFileId,
            BooleanSupplier aborted)
            throws IOException {
        long n = 0;
        long visited = 0;
        try (EnvOptions env = new EnvOptions();
                Options options = new Options();
                SstFileWriter writer = new SstFileWriter(env, options);
                ReadOptions readOptions = new ReadOptions()) {
            if (snapshot != null) {
                readOptions.setSnapshot(snapshot);
            }
            boolean opened = false;
            try (RocksIterator it = db.newIterator(cfHandle, readOptions)) {
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    if ((++visited & (ABORT_CHECK_INTERVAL - 1)) == 0 && aborted.getAsBoolean()) {
                        throw new IOException("Export of the position index aborted");
                    }
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
            throw new IOException("Failed to export the position index to " + sstPath, e);
        }
        return n;
    }

    /**
     * Ingests the SSTs of a rewrite. A key keeps its entry where {@code takesNewPosition} rejects
     * that entry (null if the key has none). Consumes the SSTs and returns the number of entries
     * skipped.
     */
    public long ingestRewrite(
            List<String> sstPaths, Predicate<FilePos> takesNewPosition, File tmpDir)
            throws IOException {
        List<String> kept = new ArrayList<>();
        long skipped = 0;
        try {
            for (String path : sstPaths) {
                try (Options options = new Options();
                        SstFileReader reader = new SstFileReader(options)) {
                    reader.open(path);
                    if (reader.getTableProperties().getNumDeletions() > 0) {
                        // its reader hides tombstones; rewrites write none
                        kept.add(path);
                        continue;
                    }
                    File out = File.createTempFile("rewrite-", ".sst", tmpDir);
                    Files.delete(out.toPath());
                    long written = 0;
                    try (ReadOptions readOptions = new ReadOptions();
                            SstFileReaderIterator it = reader.newIterator(readOptions);
                            EnvOptions env = new EnvOptions();
                            SstFileWriter writer = new SstFileWriter(env, options)) {
                        List<byte[]> keys = new ArrayList<>(REWRITE_CHUNK);
                        List<byte[]> values = new ArrayList<>(REWRITE_CHUNK);
                        it.seekToFirst();
                        while (true) {
                            boolean valid = it.isValid();
                            if (valid) {
                                keys.add(it.key());
                                values.add(it.value());
                                it.next();
                            }
                            if (keys.size() >= REWRITE_CHUNK || (!valid && !keys.isEmpty())) {
                                List<FilePos> current = multiGetByKeys(keys);
                                for (int i = 0; i < keys.size(); i++) {
                                    if (takesNewPosition.test(current.get(i))) {
                                        if (written++ == 0) {
                                            writer.open(out.getPath());
                                        }
                                        writer.put(keys.get(i), values.get(i));
                                    } else {
                                        skipped++;
                                    }
                                }
                                keys.clear();
                                values.clear();
                            }
                            if (!valid) {
                                break;
                            }
                        }
                        it.status();
                        if (written > 0) {
                            writer.finish();
                        }
                    }
                    if (written > 0) {
                        kept.add(out.getPath());
                    }
                    Files.deleteIfExists(Paths.get(path));
                }
            }
            if (!kept.isEmpty()) {
                ingestExternalFile(kept);
            }
            return skipped;
        } catch (RocksDBException e) {
            throw new IOException("Failed to ingest a rewrite into RowPosIndex", e);
        } finally {
            for (String k : kept) {
                if (k.startsWith(tmpDir.getPath())) {
                    Files.deleteIfExists(Paths.get(k));
                }
            }
        }
    }

    /** The keys with a position in an index change's SST: its entries less its tombstones. */
    static long countPositions(String sstPath) throws IOException {
        try (Options options = new Options();
                SstFileReader reader = new SstFileReader(options)) {
            reader.open(sstPath);
            return reader.getTableProperties().getNumEntries()
                    - reader.getTableProperties().getNumDeletions();
        } catch (RocksDBException e) {
            throw new IOException("Failed to read " + sstPath, e);
        }
    }

    /**
     * Hands the keys with a position in an index change's SST to {@code chunks}, at most {@code
     * chunkSize} at a time; tombstones are left out.
     */
    static void forEachPositionKeys(String sstPath, int chunkSize, IOConsumer<List<byte[]>> chunks)
            throws IOException {
        try (Options options = new Options();
                SstFileReader reader = new SstFileReader(options)) {
            reader.open(sstPath);
            try (ReadOptions readOptions = new ReadOptions();
                    SstFileReaderIterator it = reader.newIterator(readOptions)) {
                List<byte[]> keys = new ArrayList<>(chunkSize);
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    keys.add(it.key());
                    if (keys.size() >= chunkSize) {
                        chunks.accept(keys);
                        keys = new ArrayList<>(chunkSize);
                    }
                }
                it.status();
                if (!keys.isEmpty()) {
                    chunks.accept(keys);
                }
            }
        } catch (RocksDBException e) {
            throw new IOException("Failed to read " + sstPath, e);
        }
    }

    /** A consumer that may throw an IOException. */
    interface IOConsumer<T> {
        void accept(T value) throws IOException;
    }

    /** Ingests external SST files into the RowPosIndex column family, moving them. */
    public void ingestExternalFile(List<String> sstPaths) throws IOException {
        try (IngestExternalFileOptions options = new IngestExternalFileOptions()) {
            options.setMoveFiles(true);
            db.ingestExternalFile(cfHandle, sstPaths, options);
        } catch (RocksDBException e) {
            throw new IOException("Failed to ingest SST files into RowPosIndex", e);
        }
    }
}
