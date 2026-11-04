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

import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * An independent RocksDB instance for managing Deletion Vector data structures.
 *
 * <p>This RocksDB instance contains 5 Column Families:
 *
 * <ul>
 *   <li><b>RowPosIndex</b>: Maps RowId to FilePos (file_id + row_position)
 *   <li><b>LogDv</b>: Partition-style bitmap for deleted log offsets
 *   <li><b>LakeDv</b>: Per-file Roaring64Bitmap of deleted row positions
 *   <li><b>FileDict</b>: Bidirectional mapping between file paths and file IDs
 *   <li><b>PendingDeletes</b>: Pending delete entries awaiting resolution
 * </ul>
 *
 * <p>This RocksDB instance is completely independent from the KV tablet's RocksDB, with separate
 * DBOptions, ColumnFamilyOptions, and lifecycle management. DV data has a different I/O pattern
 * (point-gets + periodic bulk ingest) and a different checkpoint/recovery lifecycle (tied to lake
 * snapshots rather than KV data).
 */
public class DvRocksDB implements Closeable {

    static final String CF_ROW_POS_INDEX = "RowPosIndex";
    static final String CF_LOG_DV = "LogDv";
    static final String CF_LAKE_DV = "LakeDv";
    static final String CF_FILE_DICT = "FileDict";
    static final String CF_PENDING_DELETES = "PendingDeletes";

    private final RocksDB db;
    private final DBOptions dbOptions;
    private final ColumnFamilyOptions cfOptions;
    private final WriteOptions writeOptions;
    private final List<ColumnFamilyHandle> cfHandles;

    private final RowPosIndex rowPosIndex;
    private final LogDv logDv;
    private final LakeDv lakeDv;
    private final FileDict fileDict;
    private final PendingDeletes pendingDeletes;

    private DvRocksDB(
            RocksDB db,
            DBOptions dbOptions,
            ColumnFamilyOptions cfOptions,
            WriteOptions writeOptions,
            List<ColumnFamilyHandle> cfHandles) {
        this.db = db;
        this.dbOptions = dbOptions;
        this.cfOptions = cfOptions;
        this.writeOptions = writeOptions;
        this.cfHandles = cfHandles;
        // cfHandles order: [default, RowPosIndex, LogDv, LakeDv, FileDict, PendingDeletes]
        this.rowPosIndex = new RowPosIndex(db, cfHandles.get(1), writeOptions);
        this.logDv = new LogDv(db, cfHandles.get(2), writeOptions);
        this.lakeDv = new LakeDv(db, cfHandles.get(3), writeOptions);
        this.fileDict = new FileDict(db, cfHandles.get(4), writeOptions);
        this.pendingDeletes = new PendingDeletes(db, cfHandles.get(5), writeOptions);
    }

    /**
     * Opens a DvRocksDB instance at the specified path. Creates the database and all column
     * families if they do not exist.
     */
    public static DvRocksDB open(String dbPath) throws IOException {
        DBOptions dbOptions =
                new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
        WriteOptions writeOptions = new WriteOptions().setDisableWAL(true);

        try {
            List<ColumnFamilyDescriptor> cfDescriptors =
                    Arrays.asList(
                            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions),
                            new ColumnFamilyDescriptor(
                                    CF_ROW_POS_INDEX.getBytes(StandardCharsets.UTF_8), cfOptions),
                            new ColumnFamilyDescriptor(
                                    CF_LOG_DV.getBytes(StandardCharsets.UTF_8), cfOptions),
                            new ColumnFamilyDescriptor(
                                    CF_LAKE_DV.getBytes(StandardCharsets.UTF_8), cfOptions),
                            new ColumnFamilyDescriptor(
                                    CF_FILE_DICT.getBytes(StandardCharsets.UTF_8), cfOptions),
                            new ColumnFamilyDescriptor(
                                    CF_PENDING_DELETES.getBytes(StandardCharsets.UTF_8),
                                    cfOptions));
            List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
            RocksDB db = RocksDB.open(dbOptions, dbPath, cfDescriptors, cfHandles);
            return new DvRocksDB(db, dbOptions, cfOptions, writeOptions, cfHandles);
        } catch (RocksDBException e) {
            writeOptions.close();
            cfOptions.close();
            dbOptions.close();
            throw new IOException("Failed to open DvRocksDB at " + dbPath, e);
        }
    }

    /**
     * Restores a DvRocksDB instance from a checkpoint directory. This is equivalent to {@link
     * #open(String)} on the checkpoint path.
     */
    public static DvRocksDB restore(String checkpointPath) throws IOException {
        return open(checkpointPath);
    }

    /** Creates a RocksDB checkpoint at the specified path. */
    public void checkpoint(String checkpointPath) throws IOException {
        try (Checkpoint checkpoint = Checkpoint.create(db)) {
            checkpoint.createCheckpoint(checkpointPath);
        } catch (RocksDBException e) {
            throw new IOException("Failed to create checkpoint at " + checkpointPath, e);
        }
    }

    /** Writes a {@link WriteBatch} to this RocksDB instance. */
    public void write(WriteBatch batch) throws IOException {
        try {
            db.write(writeOptions, batch);
        } catch (RocksDBException e) {
            throw new IOException("Failed to write batch to DvRocksDB", e);
        }
    }

    /** Returns a point-in-time view for reads outside the manager lock; callers must release it. */
    public org.rocksdb.Snapshot snapshot() {
        return db.getSnapshot();
    }

    public void releaseSnapshot(org.rocksdb.Snapshot snapshot) {
        db.releaseSnapshot(snapshot);
    }

    public RowPosIndex rowPosIndex() {
        return rowPosIndex;
    }

    public LogDv logDv() {
        return logDv;
    }

    public LakeDv lakeDv() {
        return lakeDv;
    }

    public FileDict fileDict() {
        return fileDict;
    }

    public PendingDeletes pendingDeletes() {
        return pendingDeletes;
    }

    WriteOptions getWriteOptions() {
        return writeOptions;
    }

    /** Returns the underlying RocksDB instance. Intended for advanced operations. */
    RocksDB getDb() {
        return db;
    }

    @Override
    public void close() {
        // Close CF handles first, then DB, then options (RocksDB requirement)
        for (ColumnFamilyHandle cfHandle : cfHandles) {
            cfHandle.close();
        }
        db.close();
        writeOptions.close();
        cfOptions.close();
        dbOptions.close();
    }
}
