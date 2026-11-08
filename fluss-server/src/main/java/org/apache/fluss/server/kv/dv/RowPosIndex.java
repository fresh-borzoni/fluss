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
import org.apache.fluss.utils.function.ThrowingConsumer;

import io.github.fluss_contrib.rocksdb.ColumnFamilyHandle;
import io.github.fluss_contrib.rocksdb.CompactRangeOptions;
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
import java.util.Arrays;
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

    /**
     * Removes every entry with one range delete. Reads as of a snapshot taken before still see
     * them, as a switch's or rebuild's view of the covered index does.
     */
    void clear() throws IOException {
        byte[] last;
        try (RocksIterator it = db.newIterator(cfHandle)) {
            it.seekToLast();
            if (!it.isValid()) {
                return;
            }
            last = it.key();
        }
        try {
            // keys are compared bytewise: every key sorts before the last one followed by a 0 byte
            db.deleteRange(
                    cfHandle, writeOptions, new byte[0], Arrays.copyOf(last, last.length + 1));
        } catch (RocksDBException e) {
            throw new IOException("Failed to clear the position index", e);
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

    /** The keys an index change's SST names: its positions and its tombstones. */
    static long countEntries(String sstPath) throws IOException {
        try (Options options = new Options();
                SstFileReader reader = new SstFileReader(options)) {
            reader.open(sstPath);
            return reader.getTableProperties().getNumEntries();
        } catch (RocksDBException e) {
            throw new IOException("Failed to read " + sstPath, e);
        }
    }

    /**
     * Hands the keys with a position in an index change's SST to {@code chunks}, at most {@code
     * chunkSize} at a time; tombstones are left out.
     */
    static void forEachPositionKeys(
            String sstPath, int chunkSize, ThrowingConsumer<List<byte[]>, IOException> chunks)
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

    /**
     * Ingests external SST files into the RowPosIndex column family, moving them. The files get a
     * sequence number newer than any snapshot open now, so reads as of such a snapshot do not see
     * them: a load ingests while the covered snapshot is read through one.
     */
    public void ingestExternalFile(List<String> sstPaths) throws IOException {
        try (IngestExternalFileOptions options = new IngestExternalFileOptions()) {
            options.setMoveFiles(true);
            // the defaults, which snapshot reads during a load rely on
            options.setSnapshotConsistency(true);
            options.setAllowGlobalSeqNo(true);
            db.ingestExternalFile(cfHandle, sstPaths, options);
        } catch (RocksDBException e) {
            throw new IOException("Failed to ingest SST files into RowPosIndex", e);
        }
    }

    /**
     * Compacts the whole index, as an ingest of overlapping SSTs leaves them in level 0, where
     * every read searches each of them. Returns false if {@code compaction} was cancelled, which
     * leaves the index as it was: what it holds is the same, compacted or not.
     */
    boolean compact(Compaction compaction) throws IOException {
        try {
            db.compactRange(cfHandle, null, null, compaction.options);
            return true;
        } catch (RocksDBException e) {
            if (compaction.cancelled()) {
                return false;
            }
            throw new IOException("Failed to compact the position index", e);
        }
    }

    /**
     * A {@link #compact} that another thread may cancel, before it starts or while it runs. The
     * bucket's other column families compact meanwhile.
     */
    static final class Compaction implements AutoCloseable {
        // The binding allocates the flag a cancel sets on its first set, and a compaction reads
        // the flag that was there when it started: set here, a cancel of a running one is seen.
        private final CompactRangeOptions options =
                new CompactRangeOptions().setExclusiveManualCompaction(false).setCanceled(false);
        // guarded by this
        private boolean cancelled;
        private boolean closed;

        /** The compaction does not start, or ends soon; a no-op once it is closed. */
        synchronized void cancel() {
            if (!closed) {
                cancelled = true;
                options.setCanceled(true);
            }
        }

        synchronized boolean cancelled() {
            return cancelled;
        }

        @Override
        public synchronized void close() {
            if (!closed) {
                closed = true;
                options.close();
            }
        }
    }
}
