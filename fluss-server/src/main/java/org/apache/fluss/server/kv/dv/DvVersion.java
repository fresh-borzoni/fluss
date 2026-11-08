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

import org.apache.fluss.lake.dv.FilePos;

import io.github.fluss_contrib.rocksdb.Snapshot;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A version of a bucket's index: the index and file dictionary of one lake snapshot, as a RocksDB
 * snapshot taken when the version was published. Lookups are read through it, so a load that
 * ingests the next snapshot's changes into the latest index never shows through: an answer read
 * from a version is exact for that version.
 *
 * <p>The empty lake is a version too, snapshot -1, readable from offset 0, with an empty index.
 *
 * <p>The bucket's owner holds one reference to the version it serves. A reader takes another with
 * {@link #tryRetain} and releases it when done. The RocksDB snapshot is released, and its lease of
 * the RocksDB with it, once the last reference is released.
 */
final class DvVersion {

    final long snapshotId;
    // the bucket's readable offset in the snapshot
    final long readableOffset;
    // the identity of the lake snapshot the version covers, or null
    @Nullable final String identity;
    // the index changes of the chain applied, for checkpoints
    final long entriesApplied;

    private final DvRocksDB db;
    private final DvDbLeases leases;
    private final Snapshot snapshot;
    private final AtomicInteger refs = new AtomicInteger(1);

    private DvVersion(
            DvRocksDB db,
            DvDbLeases leases,
            Snapshot snapshot,
            long snapshotId,
            long readableOffset,
            @Nullable String identity,
            long entriesApplied) {
        this.db = db;
        this.leases = leases;
        this.snapshot = snapshot;
        this.snapshotId = snapshotId;
        this.readableOffset = readableOffset;
        this.identity = identity;
        this.entriesApplied = entriesApplied;
    }

    /**
     * Opens a version on the index as it is now, holding a lease of the RocksDB until its last
     * reference is released.
     *
     * @throws DvDbLeases.DvClosedException if the bucket closed
     */
    static DvVersion open(
            DvRocksDB db,
            DvDbLeases leases,
            long snapshotId,
            long readableOffset,
            @Nullable String identity,
            long entriesApplied) {
        leases.acquire();
        try {
            return new DvVersion(
                    db,
                    leases,
                    db.snapshot(),
                    snapshotId,
                    readableOffset,
                    identity,
                    entriesApplied);
        } catch (RuntimeException e) {
            leases.release();
            throw e;
        }
    }

    /** Takes a reference for a read; false if the version was released by all its holders. */
    boolean tryRetain() {
        while (true) {
            int r = refs.get();
            if (r <= 0) {
                return false; // released for good, also after a release too many
            }
            if (refs.compareAndSet(r, r + 1)) {
                return true;
            }
        }
    }

    /** Releases a reference; the last one releases the RocksDB snapshot and its lease. */
    void release() {
        int r = refs.decrementAndGet();
        if (r == 0) {
            try {
                db.releaseSnapshot(snapshot);
            } finally {
                leases.release();
            }
        } else if (r < 0) {
            throw new IllegalStateException("A deletion vector version was released twice");
        }
    }

    /** The RocksDB snapshot of the version, valid while a reference is held. */
    Snapshot snapshot() {
        return snapshot;
    }

    /** The positions of {@code keys} in the version's index, null for a key it does not hold. */
    List<FilePos> positions(List<byte[]> keys) throws IOException {
        return db.rowPosIndex().multiGetByKeys(keys, snapshot);
    }

    /** The path of lake file {@code fileId} in the version, or null if it has no such file. */
    @Nullable
    String filePath(int fileId) throws IOException {
        return db.fileDict().getFilePath(fileId, snapshot);
    }

    /** The lake files of the version by id. */
    Map<Integer, String> files() throws IOException {
        return db.fileDict().idToPathEntries(snapshot);
    }

    @Override
    public String toString() {
        return "version of snapshot " + snapshotId + " from offset " + readableOffset;
    }
}
