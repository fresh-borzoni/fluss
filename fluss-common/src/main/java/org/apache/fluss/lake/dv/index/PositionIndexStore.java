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

import java.io.Closeable;
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

    /** Returns the store of the given table. */
    static PositionIndexStore forTable(String remoteDataDir, TablePath tablePath, long tableId) {
        return forTable(FlussPaths.remoteLakeTableSnapshotDir(remoteDataDir, tablePath, tableId));
    }

    /**
     * Returns the store of the given table, writing its local temporary files under {@code
     * localTmpDir} (the JVM's temporary directory if null).
     */
    static PositionIndexStore forTable(
            String remoteDataDir, TablePath tablePath, long tableId, @Nullable File localTmpDir) {
        return new FlussPositionIndexStore(
                FlussPaths.remoteLakeTableSnapshotDir(remoteDataDir, tablePath, tableId),
                localTmpDir);
    }

    /** Returns the store of the table whose remote lake directory is {@code lakeTableDir}. */
    static PositionIndexStore forTable(FsPath lakeTableDir) {
        return new FlussPositionIndexStore(lakeTableDir);
    }

    /**
     * Returns the store of the table whose remote lake directory is {@code lakeTableDir}, writing
     * its local temporary files under {@code localTmpDir} (the JVM's temporary directory if null).
     */
    static PositionIndexStore forTable(FsPath lakeTableDir, @Nullable File localTmpDir) {
        return new FlussPositionIndexStore(lakeTableDir, localTmpDir);
    }

    // ------------------------------------------------------------------------------------------
    // Writing (tiering)
    // ------------------------------------------------------------------------------------------

    /**
     * Hands out {@code count} data-file ids never handed out before, all above every registered id,
     * and returns the first. Only the tiering committer allocates, one round of a table at a time.
     * Concurrent allocations may hand out an id twice, and registration then refuses it.
     */
    int allocateFileIds(int count) throws IOException;

    /**
     * Rewrites a stored bucket part of change {@code changeId} so its positions use the file ids
     * that {@code newIds} maps their ids to. Returns the rewritten part, which replaces the old
     * one. Used for parts written before their files had ids, such as by compactions. The part must
     * hold no tombstones.
     */
    BucketPart remapFileIds(
            long changeId, TableBucket bucket, BucketPart part, Map<Integer, Integer> newIds)
            throws IOException;

    /**
     * Writes one bucket's part of change {@code changeId} from entries in strictly ascending
     * unsigned key order. A null position is a tombstone. The write is repeatable and invisible
     * until {@link #commitChange}.
     */
    default BucketPart writeBucket(
            long changeId, TableBucket bucket, Iterator<Map.Entry<byte[], FilePos>> entries)
            throws IOException {
        return writeBucket(changeId, bucket, "", entries);
    }

    /**
     * Like {@link #writeBucket(long, TableBucket, Iterator)}, with the part's SST names prefixed by
     * {@code namePrefix}. Several writers of one bucket's part, such as the splits of a re-index,
     * then write different files, and the change takes all of them.
     */
    BucketPart writeBucket(
            long changeId,
            TableBucket bucket,
            String namePrefix,
            Iterator<Map.Entry<byte[], FilePos>> entries)
            throws IOException;

    /**
     * Opens a writer for a tiering writer's part of a bucket's change, staged under {@code
     * stagingId} ({@link StagedIndexPart}). The writer takes keys in strictly ascending unsigned
     * order, each deleted or with the position of its new row. Positions use the tiering writer's
     * own file ids, numbered from 0. Unlike {@link #writeBucket}, deletes are stored as entries the
     * store can read back; only {@link #publishStaged} reads a staged part. The SST names are
     * prefixed by {@code namePrefix}, so writers of the bucket in different attempts of a round
     * write different files.
     *
     * @param localDir the local directory for the SSTs before they are stored, null for the store's
     *     temporary directory
     */
    StagedPartWriter stageBucket(
            long stagingId, TableBucket bucket, String namePrefix, @Nullable File localDir)
            throws IOException;

    /**
     * Publishes a part staged under {@code stagingId} as the bucket's part of change {@code
     * changeId}, with the file ids that {@code newIds} maps the part's ids to. Passes every key of
     * the part to {@code keys}, in key order. The staged part is kept, so a round that runs again
     * publishes it again. It is deleted with its staging change ({@link #abortChange}).
     */
    BucketPart publishStaged(
            long stagingId,
            long changeId,
            TableBucket bucket,
            BucketPart staged,
            Map<Integer, Integer> newIds,
            KeyConsumer keys)
            throws IOException;

    /**
     * Stores the serialized lake splits that re-index split {@code split} of change {@code
     * changeId} reads, so the request and the split carry only the returned reference. The plan is
     * deleted once the change commits, or with the change.
     */
    String writeReindexPlan(
            long changeId, int split, int splitSerializerVersion, List<byte[]> lakeSplits)
            throws IOException;

    /** Reads the lake splits {@link #writeReindexPlan} stored at {@code reference}. */
    static ReindexPlan readReindexPlan(String reference) throws IOException {
        return FlussPositionIndexStore.readPlan(reference);
    }

    /**
     * Commits change {@code changeId} after checking that every bucket part is stored. The change
     * never changes afterwards. Buckets left out, or given no SSTs, added files or removed files,
     * are untouched by it.
     */
    void commitChange(long changeId, Map<TableBucket, BucketChange> buckets) throws IOException;

    /** Deletes what is stored of a change; refuses a registered one. */
    void abortChange(long changeId) throws IOException;

    /**
     * Appends committed change {@code changeId} to the chain as the step from {@code
     * parentSnapshotId} (-1 for the empty lake) to {@code snapshotId}; repeating it is a no-op.
     * Fails unless the chain ends at the parent and the change's new file ids are above every
     * registered one, which stops a stale second writer. Of writers racing to append different
     * steps to the same chain end, at most one stays registered; the others fail.
     */
    default void register(long parentSnapshotId, long snapshotId, long changeId)
            throws IOException {
        register(parentSnapshotId, snapshotId, changeId, null);
    }

    /**
     * Like {@link #register(long, long, long)}, and records {@code snapshotIdentity}, which
     * describes the lake commit of snapshot {@code snapshotId} (see {@code
     * LakeSource#snapshotIdentity}). Readers use it to check that the id still names that commit.
     */
    void register(
            long parentSnapshotId,
            long snapshotId,
            long changeId,
            @Nullable String snapshotIdentity)
            throws IOException;

    /** The snapshot identity the entry was registered with, or null if none. */
    @Nullable
    String snapshotIdentity(ChainEntry entry) throws IOException;

    /**
     * Throws what {@link #register} would throw for committed change {@code changeId} from {@code
     * parentSnapshotId} to a snapshot not registered yet, without registering it. Called before a
     * lake commit makes the change irreversible, so a change that can never register fails its own
     * round instead of every round after it.
     */
    void checkRegistrable(long parentSnapshotId, long changeId) throws IOException;

    /**
     * Deletes unregistered changes started at least {@code minAgeMillis} ago and returns how many.
     * Call it only after a successful round, so it never races the repair of a crashed one.
     */
    int deleteOrphanChanges(long minAgeMillis) throws IOException;

    /**
     * Drops the chain entries that no bucket with data needs, except the last {@code keepExtra} of
     * them, and returns how many it drops. A bucket needs the changes that touch it after its
     * latest checkpoint, so it holds the chain at that checkpoint. If no change touches it since,
     * the checkpoint is carried over to the chain's start instead. A bucket whose index server does
     * not checkpoint it, as it has no checkpoint or its last change is long past, gets a checkpoint
     * built from its last one and the changes since, so idle buckets do not hold the chain.
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
     * Reads one bucket's part of a registered change into {@code localDir}. The part is empty only
     * if the change does not touch the bucket. Throws if any of it is gone.
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

    /** Writes a staged bucket part, as {@link #stageBucket} opens it. */
    interface StagedPartWriter extends Closeable {

        /** Adds a key at the position of its new row; keys in strictly ascending order. */
        void put(byte[] key, FilePos pos) throws IOException;

        /** Adds a deleted key; keys in strictly ascending order. */
        void delete(byte[] key) throws IOException;

        /** Stores what is left of the part and returns it. */
        BucketPart finish() throws IOException;

        /** Deletes what is local of an unfinished part; stored SSTs go with the staging change. */
        @Override
        void close();
    }

    /** Takes the keys of a published part. */
    @FunctionalInterface
    interface KeyConsumer {
        void accept(byte[] key) throws IOException;
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

    /** The lake splits of a re-index split, as {@link #writeReindexPlan} stored them. */
    final class ReindexPlan {
        /** The version of the lake source's split serializer that serialized the splits. */
        public final int splitSerializerVersion;

        public final List<byte[]> lakeSplits;

        public ReindexPlan(int splitSerializerVersion, List<byte[]> lakeSplits) {
            this.splitSerializerVersion = splitSerializerVersion;
            this.lakeSplits = lakeSplits;
        }
    }

    /**
     * A complete checkpoint: the bucket's index at chain entry {@code chainIndex}, or -1 for a
     * checkpoint carried over from before the chain's start, which no change since touched.
     */
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
