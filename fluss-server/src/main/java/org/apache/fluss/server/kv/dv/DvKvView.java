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

import org.apache.fluss.utils.FileUtils;

import io.github.fluss_contrib.rocksdb.BlockBasedTableConfig;
import io.github.fluss_contrib.rocksdb.BloomFilter;
import io.github.fluss_contrib.rocksdb.Cache;
import io.github.fluss_contrib.rocksdb.LRUCache;
import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.ReadOptions;
import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.function.ToLongFunction;

/**
 * A read-only view of a bucket's KV as a restored KV snapshot left it, at the snapshot's log offset
 * X, before the new leader wrote anything: a new leader's replay of the change list from X names a
 * key's first superseded version after X from it, the key's version offset at X.
 *
 * <p>The view is linked from the downloaded snapshot files before the KV opens them ({@link
 * #link}): SST files are hard links, as RocksDB never changes them, and the few other files
 * (CURRENT, MANIFEST, OPTIONS) are copies, as the KV rewrites them when it opens. So the view takes
 * no disk until the KV compacts the linked files away, and then at most the snapshot's size. It is
 * on the KV's file system, or not linked at all: it never copies data files. The view is opened
 * read-only, so no compaction runs on it, and it is closed and deleted once the bucket installs a
 * change list, closes, or the view expires.
 */
final class DvKvView implements Closeable, DvReplay.BaseVersions {

    private static final Logger LOG = LoggerFactory.getLogger(DvKvView.class);

    private static final String SST_SUFFIX = ".sst";
    // As the DV RocksDB: few open files, and index and filter blocks in the block cache, the
    // tablet server's if it configures one, else a small one of the view's own. So the view's
    // native memory is bounded however many files the restored KV has.
    private static final int MAX_OPEN_FILES = 32;
    private static final long OWN_BLOCK_CACHE_BYTES = 8L << 20;
    private static final double BLOOM_FILTER_BITS_PER_KEY = 10;

    private final Path dir;
    private final long linkedBytes;
    private final long linkedAtMs;
    private final ToLongFunction<byte[]> versionOffset;
    @Nullable private Options options;
    @Nullable private Cache ownBlockCache;
    @Nullable private BloomFilter bloomFilter;
    @Nullable private ReadOptions readOptions;
    @Nullable private RocksDB db;
    private boolean closed;

    private DvKvView(
            Path dir, long linkedBytes, long linkedAtMs, ToLongFunction<byte[]> versionOffset) {
        this.dir = dir;
        this.linkedBytes = linkedBytes;
        this.linkedAtMs = linkedAtMs;
        this.versionOffset = versionOffset;
    }

    /**
     * Links the view of the restored KV in {@code dbDir} into {@code viewDir}, which it replaces.
     * Call it after the snapshot is downloaded and before the KV opens the files. Returns the bytes
     * of the linked SST files, or -1 if the view could not be linked, in which case nothing is left
     * in {@code viewDir}: the new leader then replays the change list from the readable offset, as
     * without a checkpoint.
     */
    static long link(Path dbDir, Path viewDir) {
        FileUtils.deleteDirectoryQuietly(viewDir.toFile());
        long bytes = 0;
        try {
            Files.createDirectories(viewDir);
            try (DirectoryStream<Path> files = Files.newDirectoryStream(dbDir)) {
                for (Path file : files) {
                    if (!Files.isRegularFile(file)) {
                        continue;
                    }
                    Path target = viewDir.resolve(file.getFileName());
                    if (file.getFileName().toString().endsWith(SST_SUFFIX)) {
                        // a link or nothing: copying the data files would hold up the leader
                        Files.createLink(target, file);
                        bytes += Files.size(file);
                    } else {
                        Files.copy(file, target, StandardCopyOption.COPY_ATTRIBUTES);
                    }
                }
            }
            return bytes;
        } catch (IOException | RuntimeException e) {
            // a RuntimeException includes a DirectoryIteratorException of the listing
            LOG.warn(
                    "Could not link a view of the restored KV in {} into {} for the deletion"
                            + " vectors; the change list is replayed from the readable offset.",
                    dbDir,
                    viewDir,
                    e);
            FileUtils.deleteDirectoryQuietly(viewDir.toFile());
            return -1;
        }
    }

    /**
     * The view linked into {@code dir} at {@code linkedAtMs}, holding {@code linkedBytes}. Its
     * values' version offsets are read with {@code versionOffset}. Opened on its first read.
     */
    static DvKvView of(
            Path dir, long linkedBytes, long linkedAtMs, ToLongFunction<byte[]> versionOffset) {
        return new DvKvView(dir, linkedBytes, linkedAtMs, versionOffset);
    }

    /**
     * The version offset of each key at the view's offset: the offset of the change that wrote the
     * key's value, or -1 if the key had no value then.
     */
    @Override
    public synchronized long[] versionsAt(List<byte[]> keys) throws IOException {
        long[] versions = new long[keys.size()];
        if (keys.isEmpty()) {
            return versions;
        }
        RocksDB view = open(null);
        try {
            List<byte[]> values = view.multiGetAsList(readOptions, keys);
            for (int i = 0; i < versions.length; i++) {
                byte[] value = values.get(i);
                versions[i] = value == null ? -1 : versionOffset.applyAsLong(value);
            }
            return versions;
        } catch (RocksDBException e) {
            throw new IOException(
                    "Could not read the view of the restored KV for the deletion vectors", e);
        }
    }

    /**
     * Opens the view read-only, unless it is open: its blocks in {@code sharedBlockCache} if given,
     * else in a small cache of its own.
     *
     * @throws IOException if the view is closed or cannot be opened
     */
    synchronized RocksDB open(@Nullable Cache sharedBlockCache) throws IOException {
        if (closed) {
            throw new IOException("The view of the restored KV for the deletion vectors is closed");
        }
        if (db == null) {
            RocksDB.loadLibrary();
            Cache own = sharedBlockCache == null ? new LRUCache(OWN_BLOCK_CACHE_BYTES) : null;
            BloomFilter filter = new BloomFilter(BLOOM_FILTER_BITS_PER_KEY, false);
            Options opened =
                    new Options()
                            .setMaxOpenFiles(MAX_OPEN_FILES)
                            .setTableFormatConfig(
                                    new BlockBasedTableConfig()
                                            .setBlockCache(
                                                    sharedBlockCache != null
                                                            ? sharedBlockCache
                                                            : own)
                                            .setFilterPolicy(filter)
                                            .setCacheIndexAndFilterBlocks(true)
                                            .setCacheIndexAndFilterBlocksWithHighPriority(true)
                                            .setPinTopLevelIndexAndFilter(true));
            try {
                db = RocksDB.openReadOnly(opened, dir.toString());
            } catch (RocksDBException e) {
                opened.close();
                filter.close();
                if (own != null) {
                    own.close();
                }
                throw new IOException(
                        "Could not open the view of the restored KV in "
                                + dir
                                + " for the deletion vectors",
                        e);
            }
            options = opened;
            ownBlockCache = own;
            bloomFilter = filter;
            // Each key is read once: its data blocks stay out of the block cache, which keeps the
            // shared one for the server's own reads. Index and filter blocks are cached.
            readOptions = new ReadOptions().setFillCache(false);
        }
        return db;
    }

    /** The bytes of the SST files linked into the view. */
    long linkedBytes() {
        return linkedBytes;
    }

    /** How long the view has been held, in ms. */
    long heldMs() {
        return System.currentTimeMillis() - linkedAtMs;
    }

    /** When the view was linked, in ms since the epoch. */
    long linkedAtMs() {
        return linkedAtMs;
    }

    File dir() {
        return dir.toFile();
    }

    /** Closes the view and deletes its files; later reads fail. */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (readOptions != null) {
            readOptions.close();
        }
        if (db != null) {
            db.close();
        }
        if (options != null) {
            options.close();
        }
        if (bloomFilter != null) {
            bloomFilter.close();
        }
        if (ownBlockCache != null) {
            ownBlockCache.close();
        }
        FileUtils.deleteDirectoryQuietly(dir.toFile());
    }
}
