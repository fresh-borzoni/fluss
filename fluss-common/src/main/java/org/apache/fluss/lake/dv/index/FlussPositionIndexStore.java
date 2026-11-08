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
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.RowPosSstIndex;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.lake.dv.index.IndexFiles.exists;

/**
 * The Fluss-native {@link PositionIndexStore}, under {@code rowPos/} of the table's remote lake
 * directory. It hands each operation to the class that owns it:
 *
 * <ul>
 *   <li>{@link IndexLayout}: the paths, names and format versions of every file;
 *   <li>{@link IndexChangeWriter}: file ids, the parts of a change, its commit and its deletion;
 *   <li>{@link IndexChain}: listing the chain and registering a change in it;
 *   <li>{@link IndexChangeReader}: reading a registered change;
 *   <li>{@link IndexCheckpoints}: a bucket's checkpoints;
 *   <li>{@link IndexTrimmer}: trimming the chain, and counting the changes since the last trim;
 *   <li>{@link OrphanChanges}: removing the changes no chain entry references.
 * </ul>
 *
 * <p>Subclasses override the reads of committed metadata, which never change and which a tablet
 * server caches, and tests the package-private hooks. Every class above reads, deletes and
 * registers through them.
 */
@Internal
public class FlussPositionIndexStore implements PositionIndexStore {

    // The names the tests look at the files by; IndexLayout defines every name.
    static final String CHAIN_DIR = IndexLayout.CHAIN_DIR;
    static final String CHAIN_TRIMMED = IndexLayout.CHAIN_TRIMMED;
    static final String FILE_IDS_DIR = IndexLayout.FILE_IDS_DIR;
    static final String CKPT_DIR = IndexLayout.CKPT_DIR;
    static final String CHANGE_STARTED = IndexLayout.CHANGE_STARTED;
    static final String CHANGE_COMMITTED = IndexLayout.CHANGE_COMMITTED;
    static final String CKPT_DONE = IndexLayout.CKPT_DONE;
    static final String CKPT_SST = IndexLayout.CKPT_SST;
    public static final String FILE_DICT = IndexLayout.FILE_DICT;

    private final IndexChain indexChain;
    private final IndexChangeWriter writer;
    private final IndexChangeReader reader;
    private final IndexCheckpoints checkpoints;
    private final IndexTrimmer trimmer;
    private final OrphanChanges orphans;

    public FlussPositionIndexStore(FsPath lakeTableDir) {
        this(lakeTableDir, null);
    }

    public FlussPositionIndexStore(FsPath lakeTableDir, @Nullable File localTmpDir) {
        IndexLayout layout = new IndexLayout(lakeTableDir);
        this.indexChain = new IndexChain(layout, this);
        this.writer = new IndexChangeWriter(layout, this, indexChain, localTmpDir);
        this.reader = new IndexChangeReader(layout, this);
        this.checkpoints = new IndexCheckpoints(layout, this);
        this.trimmer =
                new IndexTrimmer(
                        layout, this, indexChain, writer, reader, checkpoints, localTmpDir);
        this.orphans = new OrphanChanges(layout, indexChain, writer);
    }

    // ------------------------------------------------------------------------------------------
    // Writing
    // ------------------------------------------------------------------------------------------

    @Override
    public int allocateFileIds(int count) throws IOException {
        return writer.allocateFileIds(count);
    }

    @Override
    public BucketPart remapFileIds(
            long changeId, TableBucket bucket, BucketPart part, Map<Integer, Integer> newIds)
            throws IOException {
        return writer.remapFileIds(changeId, bucket, part, newIds);
    }

    @Override
    public BucketPart writeBucket(
            long changeId,
            TableBucket bucket,
            String namePrefix,
            Iterator<Map.Entry<byte[], FilePos>> entries)
            throws IOException {
        return writer.writeBucket(changeId, bucket, namePrefix, entries);
    }

    @Override
    public void checkDistinctKeys(
            long changeId,
            TableBucket bucket,
            List<RowPosSstIndex.SstFileEntry> ssts,
            Map<Integer, String> fileNames)
            throws IOException {
        writer.checkDistinctKeys(changeId, bucket, ssts, fileNames);
    }

    @Override
    public StagedPartWriter stageBucket(
            long stagingId, TableBucket bucket, String namePrefix, @Nullable File localDir)
            throws IOException {
        return writer.stageBucket(stagingId, bucket, namePrefix, localDir);
    }

    @Override
    public BucketPart publishStaged(
            long stagingId,
            long changeId,
            TableBucket bucket,
            BucketPart staged,
            Map<Integer, Integer> newIds,
            KeyConsumer keys)
            throws IOException {
        return writer.publishStaged(stagingId, changeId, bucket, staged, newIds, keys);
    }

    @Override
    public String writeReindexPlan(
            long changeId, int split, int splitSerializerVersion, List<byte[]> lakeSplits)
            throws IOException {
        return writer.writeReindexPlan(changeId, split, splitSerializerVersion, lakeSplits);
    }

    @Override
    public void commitChange(long changeId, Map<TableBucket, BucketChange> buckets)
            throws IOException {
        writer.commitChange(changeId, buckets);
    }

    @Override
    public void abortChange(long changeId) throws IOException {
        writer.abortChange(changeId);
    }

    @Override
    public void register(
            long parentSnapshotId,
            long snapshotId,
            long changeId,
            @Nullable String snapshotIdentity)
            throws IOException {
        indexChain.register(parentSnapshotId, snapshotId, changeId, snapshotIdentity);
    }

    @Override
    @Nullable
    public String snapshotIdentity(ChainEntry entry) throws IOException {
        return indexChain.snapshotIdentity(entry);
    }

    @Override
    public void checkRegistrable(long parentSnapshotId, long changeId) throws IOException {
        indexChain.checkRegistrable(parentSnapshotId, changeId);
    }

    @Override
    public int deleteOrphanChanges(long minAgeMillis) throws IOException {
        return orphans.deleteOrphanChanges(minAgeMillis);
    }

    @Override
    public TrimResult trim(
            BucketsWithData bucketsWithData, int keepExtra, DroppedPartitions dropped)
            throws IOException {
        return trimmer.trim(bucketsWithData, keepExtra, dropped);
    }

    @Override
    public long changesSinceTrim() throws IOException {
        return trimmer.changesSinceTrim();
    }

    @Override
    public void startTrim() throws IOException {
        trimmer.startTrim();
    }

    // ------------------------------------------------------------------------------------------
    // Reading
    // ------------------------------------------------------------------------------------------

    @Override
    public List<ChainEntry> chain() throws IOException {
        return indexChain.list();
    }

    @Override
    public void repairFork(long registeredSnapshotId) throws IOException {
        indexChain.repairFork(registeredSnapshotId);
    }

    @Override
    public boolean chainTrimmed() throws IOException {
        return trimmer.chainTrimmed();
    }

    @Override
    public BucketData readChange(ChainEntry entry, TableBucket bucket, File localDir)
            throws IOException {
        return reader.readChange(entry, bucket, localDir);
    }

    @Override
    @Nullable
    public Checkpoint latestCheckpoint(TableBucket bucket, List<ChainEntry> chain, int upTo)
            throws IOException {
        return checkpoints.latestCheckpoint(bucket, chain, upTo);
    }

    @Override
    public BucketData readCheckpoint(Checkpoint checkpoint, TableBucket bucket, File localDir)
            throws IOException {
        return checkpoints.readCheckpoint(checkpoint, bucket, localDir);
    }

    @Override
    public void writeCheckpoint(
            long snapshotId,
            TableBucket bucket,
            @Nullable File localSst,
            Map<Integer, String> fileId2Name)
            throws IOException {
        checkpoints.writeCheckpoint(snapshotId, bucket, localSst, fileId2Name);
    }

    // ------------------------------------------------------------------------------------------
    // What subclasses override
    // ------------------------------------------------------------------------------------------

    /**
     * The partitions a committed change touches, one per line; null if the change is not committed
     * or is gone. The list never changes once written, so a reader may cache it.
     */
    @Nullable
    protected String readCommittedPartitions(long changeId) throws IOException {
        return reader.readCommittedPartitions(changeId);
    }

    /**
     * The manifest of a committed change's part of a partition; throws if it is gone. It never
     * changes once written, so a reader may cache it.
     */
    protected RowPosSstIndex readManifest(long changeId, @Nullable Long partitionId)
            throws IOException {
        return reader.readManifest(changeId, partitionId);
    }

    /** Runs right before a registration writes its chain marker; a hook for tests. */
    @VisibleForTesting
    void beforeMarkerWrite() throws IOException {}

    /** Runs right after a registration checked the end of the chain; a hook for tests. */
    @VisibleForTesting
    void afterChainChecked() throws IOException {}

    /** Deletes a path of the store; tests override it to observe or fail deletes. */
    boolean deletePath(FsPath path, boolean recursive) throws IOException {
        return path.getFileSystem().delete(path, recursive);
    }

    /** Deletes a path, and fails if it is still there afterwards. */
    void deleteOrThrow(FsPath path, boolean recursive) throws IOException {
        if (!deletePath(path, recursive) && exists(path)) {
            throw new IOException("Could not delete " + path + ".");
        }
    }
}
