/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.dv.index;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.RowPosSstIndex;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.FlussPaths;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The remote copy of a primary-key table's position index: for each key, the data file and row
 * position of its live row in a lake snapshot.
 *
 * <p>A change turns one lake snapshot's index into the next one's and never changes once committed.
 * The chain is the registered changes in lake commit order, and a snapshot's index is the chain
 * applied up to it. A checkpoint is one bucket's full index at a chain entry, so the chain can be
 * trimmed. Tiering is the single writer: a change registers only if it extends the chain's end and
 * adds file ids above every registered one. Reads throw instead of returning missing data as empty.
 */
@Internal
public interface PositionIndexStore {

    /** Parent of a legacy chain entry that does not record one. */
    long UNKNOWN_PARENT = Long.MIN_VALUE;

    /** Returns the store of the given table. */
    static PositionIndexStore forTable(String remoteDataDir, TablePath tablePath, long tableId) {
        return forTable(FlussPaths.remoteLakeTableSnapshotDir(remoteDataDir, tablePath, tableId));
    }

    /** Returns the store of the table whose remote lake directory is {@code lakeTableDir}. */
    static PositionIndexStore forTable(FsPath lakeTableDir) {
        return new FlussPositionIndexStore(lakeTableDir);
    }

    // ------------------------------------------------------------------------------------------
    // Writing (tiering)
    // ------------------------------------------------------------------------------------------

    /** Hands out {@code count} data-file ids never handed out before; returns the first. */
    int allocateFileIds(int count) throws IOException;

    /**
     * Writes one bucket's part of change {@code changeId} from entries in strictly ascending
     * unsigned key order, a null position being a tombstone. Repeatable, and invisible until {@link
     * #commitChange}.
     */
    BucketPart writeBucket(
            long changeId, TableBucket bucket, Iterator<Map.Entry<byte[], FilePos>> entries)
            throws IOException;

    /**
     * Commits change {@code changeId} after checking every bucket part is stored; it never changes
     * afterwards, and buckets left out are untouched by it.
     */
    void commitChange(long changeId, Map<TableBucket, BucketChange> buckets) throws IOException;

    /** Deletes what is stored of a change; refuses a registered one. */
    void abortChange(long changeId) throws IOException;

    /**
     * Appends committed change {@code changeId} to the chain as the step from {@code
     * parentSnapshotId} (-1 for the empty lake) to {@code snapshotId}; repeating it is a no-op.
     * Fails unless the chain ends at the parent and the change's new file ids are above every
     * registered one, which stops a stale second writer.
     */
    void register(long parentSnapshotId, long snapshotId, long changeId) throws IOException;

    /**
     * Deletes unregistered changes started at least {@code minAgeMillis} ago and returns how many.
     * Call it only after a successful round, so it never races the repair of a crashed one.
     */
    int deleteOrphanChanges(long minAgeMillis) throws IOException;

    /**
     * Drops chain entries that every bucket with data has a checkpoint past, keeping {@code
     * keepExtra} more, and returns how many; a bucket without a checkpoint keeps the whole chain.
     */
    int trim(Collection<TableBucket> bucketsWithData, int keepExtra) throws IOException;

    // ------------------------------------------------------------------------------------------
    // Reading (tablet servers)
    // ------------------------------------------------------------------------------------------

    /** Returns the registered changes in commit order; throws if they do not link up. */
    List<ChainEntry> chain() throws IOException;

    /** Whether entries were trimmed off the start of the chain. */
    boolean chainTrimmed() throws IOException;

    /**
     * Reads one bucket's part of a registered change into {@code localDir}; empty only if the
     * change does not touch the bucket, and throws if any of it is gone.
     */
    BucketData readChange(ChainEntry entry, TableBucket bucket, File localDir) throws IOException;

    /** The bucket's newest complete checkpoint at or before chain index {@code upTo}, or null. */
    @Nullable
    Checkpoint latestCheckpoint(TableBucket bucket, List<ChainEntry> chain, int upTo)
            throws IOException;

    /** Reads a checkpoint into {@code localDir}; throws if any of it is gone. */
    BucketData readCheckpoint(Checkpoint checkpoint, TableBucket bucket, File localDir)
            throws IOException;

    /**
     * Stores the bucket's full index at {@code snapshotId} (a null SST for an empty index) and the
     * file ids it uses, keeping the bucket's two newest checkpoints.
     */
    void writeCheckpoint(
            long snapshotId,
            TableBucket bucket,
            @Nullable File localSst,
            Map<Integer, String> fileId2Name)
            throws IOException;

    // ------------------------------------------------------------------------------------------
    // Values
    // ------------------------------------------------------------------------------------------

    /** A registered change from {@code parentSnapshotId} to {@code snapshotId}. */
    final class ChainEntry {
        public final long sequence;
        public final long parentSnapshotId;
        public final long snapshotId;
        public final long changeId;

        public ChainEntry(long sequence, long parentSnapshotId, long snapshotId, long changeId) {
            this.sequence = sequence;
            this.parentSnapshotId = parentSnapshotId;
            this.snapshotId = snapshotId;
            this.changeId = changeId;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof ChainEntry)) {
                return false;
            }
            ChainEntry e = (ChainEntry) o;
            return sequence == e.sequence
                    && parentSnapshotId == e.parentSnapshotId
                    && snapshotId == e.snapshotId
                    && changeId == e.changeId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(sequence, parentSnapshotId, snapshotId, changeId);
        }

        @Override
        public String toString() {
            return "#"
                    + sequence
                    + " "
                    + parentSnapshotId
                    + "->"
                    + snapshotId
                    + " ("
                    + changeId
                    + ")";
        }
    }

    /** One bucket's stored part of a change, as {@link #writeBucket} returns it. */
    final class BucketPart {
        public static final BucketPart NONE = new BucketPart(Collections.emptyList());

        public final List<RowPosSstIndex.SstFileEntry> files;

        public BucketPart(List<RowPosSstIndex.SstFileEntry> files) {
            this.files = files;
        }
    }

    /** What a change does to one bucket, as {@link #commitChange} takes it. */
    final class BucketChange {
        public final BucketPart part;
        public final Map<Integer, String> addedFiles;
        public final List<String> removedFiles;

        public BucketChange(
                BucketPart part, Map<Integer, String> addedFiles, List<String> removedFiles) {
            this.part = part;
            this.addedFiles = addedFiles;
            this.removedFiles = removedFiles;
        }
    }

    /** A bucket's change or checkpoint as a tablet server applies it. */
    final class BucketData {
        public static final BucketData EMPTY =
                new BucketData(
                        Collections.emptyList(), Collections.emptyMap(), Collections.emptyList());

        /** Local SSTs to ingest, in order. */
        public final List<String> localSsts;
        /** File ids the data uses (a change: the files it adds; a checkpoint: all). */
        public final Map<Integer, String> fileId2Name;
        /** Data files that left the snapshot; their ids are dropped. */
        public final List<String> removedFiles;

        public BucketData(
                List<String> localSsts,
                Map<Integer, String> fileId2Name,
                List<String> removedFiles) {
            this.localSsts = localSsts;
            this.fileId2Name = fileId2Name;
            this.removedFiles = removedFiles;
        }
    }

    /** A complete checkpoint: the bucket's index at chain entry {@code chainIndex}. */
    final class Checkpoint {
        public final int chainIndex;
        public final long snapshotId;

        public Checkpoint(int chainIndex, long snapshotId) {
            this.chainIndex = chainIndex;
            this.snapshotId = snapshotId;
        }

        @Override
        public String toString() {
            return "checkpoint@" + snapshotId + " (chain index " + chainIndex + ")";
        }
    }
}
