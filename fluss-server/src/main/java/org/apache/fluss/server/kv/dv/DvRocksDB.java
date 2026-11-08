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
import org.apache.fluss.utils.FileUtils;

import io.github.fluss_contrib.rocksdb.BlockBasedTableConfig;
import io.github.fluss_contrib.rocksdb.BloomFilter;
import io.github.fluss_contrib.rocksdb.Cache;
import io.github.fluss_contrib.rocksdb.ColumnFamilyDescriptor;
import io.github.fluss_contrib.rocksdb.ColumnFamilyHandle;
import io.github.fluss_contrib.rocksdb.ColumnFamilyOptions;
import io.github.fluss_contrib.rocksdb.DBOptions;
import io.github.fluss_contrib.rocksdb.IndexType;
import io.github.fluss_contrib.rocksdb.LRUCache;
import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.RocksDBException;
import io.github.fluss_contrib.rocksdb.Snapshot;
import io.github.fluss_contrib.rocksdb.WriteBufferManager;
import io.github.fluss_contrib.rocksdb.WriteOptions;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The local RocksDB of a bucket's deletion vector state, separate from the KV tablet's RocksDB.
 *
 * <p>It holds three column families:
 *
 * <ul>
 *   <li>RowPosIndex: the position index, encoded primary key to the {@code FilePos} (file id and
 *       row position) of the key's live row in the covered snapshot.
 *   <li>FileDict: the mapping between file ids and data file names.
 *   <li>Tail: the change lists, their updates and deletes ({@link DvChangeListStore}).
 * </ul>
 *
 * <p>The state is rebuilt from the published index changes whenever a replica becomes leader, so it
 * is written without a WAL.
 */
public class DvRocksDB implements Closeable {

    static final String CF_ROW_POS_INDEX = "RowPosIndex";
    static final String CF_FILE_DICT = "FileDict";
    static final String CF_TAIL = "Tail";

    private static final int MAX_OPEN_FILES = 64;
    private static final long WRITE_BUFFER_BYTES = 4L << 20;
    private static final long OWN_BLOCK_CACHE_BYTES = 8L << 20;
    // Most lookups of the index and of a replay's map are point lookups of keys that most files do
    // not hold, so each file keeps a filter of its keys.
    private static final double BLOOM_FILTER_BITS_PER_KEY = 10;
    // level-0 files of the index at which its writes slow down and stop: above any full load's
    private static final int INDEX_LEVEL0_SLOWDOWN_FILES = 1_000;
    private static final int INDEX_LEVEL0_STOP_FILES = 2_000;

    private final RocksDB db;
    private final DBOptions dbOptions;
    private final ColumnFamilyOptions cfOptions;
    private final ColumnFamilyOptions indexCfOptions;
    private final WriteOptions writeOptions;
    private final BloomFilter bloomFilter;
    private final List<ColumnFamilyHandle> cfHandles;

    private final RowPosIndex rowPosIndex;
    private final FileDict fileDict;
    private final DvChangeListStore changeListStore;
    private boolean closed;
    // the block cache of this RocksDB alone, when the tablet server has no shared one
    @Nullable private final Cache ownBlockCache;
    // local scratch files of the RocksDB's owner, next to it: emptied when it opens
    private final File tmpDir;

    private DvRocksDB(
            RocksDB db,
            DBOptions dbOptions,
            ColumnFamilyOptions cfOptions,
            ColumnFamilyOptions indexCfOptions,
            WriteOptions writeOptions,
            BloomFilter bloomFilter,
            List<ColumnFamilyHandle> cfHandles,
            @Nullable Cache ownBlockCache,
            File tmpDir) {
        this.ownBlockCache = ownBlockCache;
        this.tmpDir = tmpDir;
        this.db = db;
        this.dbOptions = dbOptions;
        this.cfOptions = cfOptions;
        this.indexCfOptions = indexCfOptions;
        this.writeOptions = writeOptions;
        this.bloomFilter = bloomFilter;
        this.cfHandles = cfHandles;
        // cfHandles order: [default, RowPosIndex, FileDict, Tail]
        this.rowPosIndex = new RowPosIndex(db, cfHandles.get(1), writeOptions);
        this.fileDict = new FileDict(db, cfHandles.get(2), writeOptions);
        this.changeListStore = new DvChangeListStore(db, cfHandles.get(3), writeOptions);
    }

    /**
     * Opens a DvRocksDB instance at the specified path. Creates the database and all column
     * families if they do not exist.
     */
    public static DvRocksDB open(String dbPath) throws IOException {
        return open(dbPath, null, null);
    }

    /**
     * Opens a DvRocksDB instance at the specified path. If given, {@code sharedBlockCache} caches
     * its blocks and {@code sharedWriteBufferManager} accounts its memtables; both are the ones of
     * the tablet server's KV tablets. A tablet server opens one per leader of a table with deletion
     * vectors, so each has small write buffers, few open files and, without a shared cache, a small
     * block cache.
     */
    public static DvRocksDB open(
            String dbPath,
            @Nullable Cache sharedBlockCache,
            @Nullable WriteBufferManager sharedWriteBufferManager)
            throws IOException {
        DBOptions dbOptions =
                new DBOptions()
                        .setCreateIfMissing(true)
                        .setCreateMissingColumnFamilies(true)
                        .setMaxOpenFiles(MAX_OPEN_FILES)
                        .setKeepLogFileNum(2)
                        .setMaxLogFileSize(1 << 20);
        if (sharedWriteBufferManager != null) {
            dbOptions.setWriteBufferManager(sharedWriteBufferManager);
        }
        Cache ownBlockCache = sharedBlockCache == null ? new LRUCache(OWN_BLOCK_CACHE_BYTES) : null;
        BloomFilter bloomFilter = new BloomFilter(BLOOM_FILTER_BITS_PER_KEY, false);
        ColumnFamilyOptions cfOptions =
                new ColumnFamilyOptions()
                        .setWriteBufferSize(WRITE_BUFFER_BYTES)
                        .setMaxWriteBufferNumber(2)
                        .setTableFormatConfig(
                                new BlockBasedTableConfig()
                                        .setBlockCache(
                                                sharedBlockCache != null
                                                        ? sharedBlockCache
                                                        : ownBlockCache)
                                        .setFilterPolicy(bloomFilter)
                                        // Index and filter blocks count against the cache too.
                                        // They are partitioned, so a lookup loads the small
                                        // parts it needs rather than a file's whole index, which
                                        // a small cache would evict on every batch.
                                        .setIndexType(IndexType.kTwoLevelIndexSearch)
                                        .setPartitionFilters(true)
                                        .setMetadataBlockSize(4096)
                                        .setCacheIndexAndFilterBlocks(true)
                                        .setCacheIndexAndFilterBlocksWithHighPriority(true)
                                        .setPinTopLevelIndexAndFilter(true));
        // A full load ingests every change SST of its chain in one call, into level 0, and RocksDB
        // stalls the writes of every column family on one column family's level 0 or pending
        // compaction bytes. The index takes no writes but a load's ingests and clear, so its own
        // never stall the change list's and the dictionary's.
        ColumnFamilyOptions indexCfOptions =
                new ColumnFamilyOptions(cfOptions)
                        .setLevel0SlowdownWritesTrigger(INDEX_LEVEL0_SLOWDOWN_FILES)
                        .setLevel0StopWritesTrigger(INDEX_LEVEL0_STOP_FILES)
                        .setSoftPendingCompactionBytesLimit(0)
                        .setHardPendingCompactionBytesLimit(0);
        WriteOptions writeOptions = new WriteOptions().setDisableWAL(true);

        try {
            List<ColumnFamilyDescriptor> cfDescriptors =
                    Arrays.asList(
                            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions),
                            new ColumnFamilyDescriptor(
                                    CF_ROW_POS_INDEX.getBytes(StandardCharsets.UTF_8),
                                    indexCfOptions),
                            new ColumnFamilyDescriptor(
                                    CF_FILE_DICT.getBytes(StandardCharsets.UTF_8), cfOptions),
                            new ColumnFamilyDescriptor(
                                    CF_TAIL.getBytes(StandardCharsets.UTF_8), cfOptions));
            List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
            RocksDB db = RocksDB.open(dbOptions, dbPath, cfDescriptors, cfHandles);
            DvRocksDB dvRocksDB =
                    new DvRocksDB(
                            db,
                            dbOptions,
                            cfOptions,
                            indexCfOptions,
                            writeOptions,
                            bloomFilter,
                            cfHandles,
                            ownBlockCache,
                            new File(dbPath + "-tmp"));
            try {
                // change lists are rebuilt from the log
                dvRocksDB.changeListStore.clear();
                // scratch files of a previous run, e.g. a checkpoint export a crash left
                FileUtils.deleteDirectoryQuietly(dvRocksDB.tmpDir);
                Files.createDirectories(dvRocksDB.tmpDir.toPath());
            } catch (IOException e) {
                dvRocksDB.close();
                throw e;
            }
            return dvRocksDB;
        } catch (RocksDBException e) {
            writeOptions.close();
            indexCfOptions.close();
            cfOptions.close();
            bloomFilter.close();
            dbOptions.close();
            if (ownBlockCache != null) {
                ownBlockCache.close();
            }
            throw new IOException("Failed to open DvRocksDB at " + dbPath, e);
        }
    }

    /**
     * Returns the directory for the local scratch files of the RocksDB's owner, e.g. checkpoint
     * exports. It is on the tablet's disk next to the RocksDB, not in the JVM's temporary
     * directory, and is emptied whenever the RocksDB opens.
     */
    public File tmpDir() {
        return tmpDir;
    }

    /** Returns a point-in-time view, which later writes do not change; release it when done. */
    public Snapshot snapshot() {
        return db.getSnapshot();
    }

    public void releaseSnapshot(Snapshot snapshot) {
        db.releaseSnapshot(snapshot);
    }

    public RowPosIndex rowPosIndex() {
        return rowPosIndex;
    }

    public FileDict fileDict() {
        return fileDict;
    }

    /** The capacity of the block cache the RocksDB uses, in bytes. */
    @VisibleForTesting
    long blockCacheCapacity() throws IOException {
        try {
            return db.getLongProperty(cfHandles.get(1), "rocksdb.block-cache-capacity");
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    /** The change lists of the bucket. */
    public DvChangeListStore changeListStore() {
        return changeListStore;
    }

    /** Whether the RocksDB is closed. */
    @VisibleForTesting
    synchronized boolean isClosed() {
        return closed;
    }

    /** The RocksDB snapshots taken and not released yet. */
    @VisibleForTesting
    long openSnapshots() {
        try {
            return db.getLongProperty("rocksdb.num-snapshots");
        } catch (RocksDBException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Stops the RocksDB's flushes and compactions, after a flush of its memtables, as its close
     * does first; a manual compaction waiting for an automatic one wakes. Only for a RocksDB about
     * to close.
     */
    void stopBackgroundWork() {
        db.cancelAllBackgroundWork(false);
    }

    /** Closes the RocksDB; repeated calls do nothing. */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        // Close CF handles first, then DB, then options (RocksDB requirement)
        for (ColumnFamilyHandle cfHandle : cfHandles) {
            cfHandle.close();
        }
        db.close();
        writeOptions.close();
        indexCfOptions.close();
        cfOptions.close();
        bloomFilter.close();
        dbOptions.close();
        if (ownBlockCache != null) {
            ownBlockCache.close();
        }
    }
}
