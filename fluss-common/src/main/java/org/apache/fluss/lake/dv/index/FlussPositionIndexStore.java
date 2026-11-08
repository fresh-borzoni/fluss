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
import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.FormatVersions;
import org.apache.fluss.lake.dv.RowPosSstIndex;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.FlussPaths;

import io.github.fluss_contrib.rocksdb.EnvOptions;
import io.github.fluss_contrib.rocksdb.IngestExternalFileOptions;
import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.ReadOptions;
import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.RocksDBException;
import io.github.fluss_contrib.rocksdb.RocksIterator;
import io.github.fluss_contrib.rocksdb.SstFileReader;
import io.github.fluss_contrib.rocksdb.SstFileReaderIterator;
import io.github.fluss_contrib.rocksdb.SstFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * The Fluss-native {@link PositionIndexStore}, under {@code rowPos/} of the table's remote lake
 * directory; no other class knows these paths.
 *
 * <pre>
 * rowPos/{changeId}/[{partitionId}/]rowpos.manifest     manifest per partition
 * rowPos/{changeId}/[{partitionId}/]{bucket}/sst_N.sst  key -> varint(fileId)||varint(pos)
 * rowPos/{changeId}/_STARTED                            when the change was first written
 * rowPos/{changeId}/_plan/{split}                       lake splits a re-index split reads
 * rowPos/{stagingId}/[{partitionId}/]{bucket}/{prefix}sst_N.sst  a tiering writer's staged part:
 *                                                       key -> position, or empty if deleted
 * rowPos/{changeId}/_COMMITTED                          the change's partitions; written last
 * rowPos/_chain/v1-{sequence}_{parent}_{snapshot}_{changeId}_{maxFileId}  one per registered
 *                                                       change, holding the snapshot's identity
 * rowPos/_trimmed                                       present once the chain was trimmed
 * rowPos/_next_file_id                                  file-id counter, written ahead
 * rowPos/_ckpt/[{partitionId}/]{bucket}/{snapshotId}/   index.sst, filedict.txt, _DONE (last),
 *                                                      _CARRIED once trimmed off the chain
 * </pre>
 *
 * <p>Every format has a version, see {@link FormatVersions}. The text files start with a version
 * line. A change directory's {@code _STARTED} also versions the SSTs written into it before the
 * change commits, which no manifest lists yet. A manifest versions itself and the values of the
 * SSTs it lists, and a checkpoint's {@code _DONE} the whole checkpoint. A chain entry carries its
 * version in its name: listing the chain reads names only, and a newer entry must fail a listing
 * even when older entries come before it.
 *
 * <p>The chain is ordered by sequence numbers from the single writer, not by a clock. A staging
 * change is never committed. The committer publishes its parts into the round's change and deletes
 * it once the round ends; otherwise it is removed as an orphan.
 *
 * <p>Trimming keeps every bucket rebuildable from a checkpoint and the kept changes. It never drops
 * a change that touches a bucket after the bucket's checkpoint. A checkpoint whose snapshot is
 * trimmed off the chain is still the bucket's index at the chain's start if no change after it
 * touches the bucket; it is marked as carried over. A bucket whose index server does not checkpoint
 * it, such as one of an old partition that no round writes, gets its checkpoint from the trim. The
 * trim merges the last checkpoint and the changes since, so idle buckets do not hold the chain.
 */
@Internal
public class FlussPositionIndexStore implements PositionIndexStore {

    static final String ROW_POS_DIR = "rowPos";
    public static final String CHAIN_DIR = ROW_POS_DIR + "/_chain";
    public static final String CHAIN_TRIMMED = ROW_POS_DIR + "/_trimmed";
    static final String NEXT_FILE_ID = ROW_POS_DIR + "/_next_file_id";
    public static final String CKPT_DIR = ROW_POS_DIR + "/_ckpt";
    /** Written first into a change directory: its start time (for removing failed attempts). */
    public static final String CHANGE_STARTED = "_STARTED";
    /** Written last into a change directory: the partitions with a manifest (-1: no partition). */
    public static final String CHANGE_COMMITTED = "_COMMITTED";
    /** Written last into a checkpoint directory: "sst", or "empty" for an empty index. */
    public static final String CKPT_DONE = "_DONE";
    /**
     * Written into a complete checkpoint directory whose snapshot was trimmed off the chain while
     * no later change touched the bucket. The checkpoint is the bucket's index at the chain's
     * start.
     */
    public static final String CKPT_CARRIED = "_CARRIED";

    public static final String CKPT_SST = "index.sst";
    public static final String FILE_DICT = "filedict.txt";

    // Format versions of the store's files.
    // A chain entry's name, and its content: the identity LakeSource#snapshotIdentity gave the
    // snapshot. Readers compare it with the identity the lake gives now, so a change of that
    // string is a change of this format.
    static final int CHAIN_ENTRY_VERSION = 1;
    // A change directory: _STARTED, _COMMITTED and the SSTs written before the change commits.
    // The staged SSTs hold FilePos values or empty ones for deletes.
    static final int CHANGE_VERSION = 1;
    // A checkpoint, in its _DONE: index.sst with FilePos values, filedict.txt and _CARRIED.
    static final int CHECKPOINT_VERSION = 1;
    static final int COUNTER_VERSION = 1;
    static final int TRIMMED_VERSION = 1;

    private static final String CHAIN_ENTRY_VERSION_PREFIX = "v";
    private static final String CHAIN_ENTRY_VERSION_END = "-";

    private static final String DONE_SST = "sst";
    private static final String DONE_EMPTY = "empty";
    private static final int CHECKPOINTS_KEPT = 2;
    private static final int MAX_ENTRIES_PER_SST = 1_000_000;
    // name prefix of an SST whose file ids were remapped
    private static final String REMAPPED_PREFIX = "remapped_";
    // the re-index plans of a change, one per re-index split, deleted once the change commits
    private static final String REINDEX_PLAN_DIR = "_plan";
    private static final int PLAN_FORMAT_VERSION = 1;
    // A bucket whose last change is this many chain entries old is idle. Its index server
    // catches up only when a round writes the bucket, so the trim checkpoints it instead.
    static final int IDLE_ENTRIES = 50;
    // The trim builds checkpoints of idle buckets on a round's commit thread. Per trim it builds at
    // most this many and starts none after this long; later trims build the rest.
    static final int MAX_CHECKPOINT_BUILDS = 256;
    static final long MAX_CHECKPOINT_BUILD_MILLIS = 60_000;
    // A writer whose create of a change's start marker races another writer waits about 2.5 s for
    // that marker to land. A small object's upload takes far less.
    private static final int MARK_STARTED_ATTEMPTS = 10;
    private static final long MARK_STARTED_INITIAL_BACKOFF_MS = 20;
    private static final long MARK_STARTED_MAX_BACKOFF_MS = 500;

    private static final Logger LOG = LoggerFactory.getLogger(FlussPositionIndexStore.class);

    private final FsPath lakeTableDir;
    // local directory for temporary files, or null for the JVM's
    @Nullable private final File localTmpDir;
    // change id -> partition key -> the buckets the change touches; committed changes never
    // change, so each is read once
    private final Map<Long, Map<Long, Set<Integer>>> touchedBuckets = new ConcurrentHashMap<>();
    // changes whose directory's format version was checked; it never changes once written
    private final Set<Long> checkedChanges = ConcurrentHashMap.newKeySet();

    public FlussPositionIndexStore(FsPath lakeTableDir) {
        this(lakeTableDir, null);
    }

    public FlussPositionIndexStore(FsPath lakeTableDir, @Nullable File localTmpDir) {
        this.lakeTableDir = lakeTableDir;
        this.localTmpDir = localTmpDir;
    }

    public static FlussPositionIndexStore of(
            String remoteDataDir, TablePath tablePath, long tableId) {
        return new FlussPositionIndexStore(
                FlussPaths.remoteLakeTableSnapshotDir(remoteDataDir, tablePath, tableId));
    }

    // ------------------------------------------------------------------------------------------
    // Writing
    // ------------------------------------------------------------------------------------------

    @Override
    public int allocateFileIds(int count) throws IOException {
        // The counter is not cached: another writer may have moved it. Racing writers can still
        // get the same ids; register() refuses the second. A racing read-then-write can also move
        // the counter back, so ids always start above every registered id. A change with ids at or
        // below them would never register.
        FsPath counter = new FsPath(lakeTableDir, NEXT_FILE_ID);
        String s = readText(counter);
        int first =
                s == null
                        ? 0
                        : Integer.parseInt(
                                FormatVersions.withoutVersion(counter, s, COUNTER_VERSION).trim());
        long registeredMax = -1;
        for (String name : markerNames()) {
            registeredMax = Math.max(registeredMax, markerMaxFileId(name));
        }
        first = (int) Math.max(first, registeredMax + 1);
        // Written ahead, so a restart never hands out an id twice.
        writeText(
                counter,
                FormatVersions.withVersion(COUNTER_VERSION, String.valueOf(first + count)));
        return first;
    }

    @Override
    public BucketPart remapFileIds(
            long changeId, TableBucket bucket, BucketPart part, Map<Integer, Integer> newIds)
            throws IOException {
        if (part.files.isEmpty()) {
            return part;
        }
        checkChangeVersion(changeId);
        FsPath dir = bucketDir(changeId, bucket);
        File tmp = createTempDirectory("fluss-pkidx-remap-");
        try {
            List<RowPosSstIndex.SstFileEntry> remapped = new ArrayList<>();
            for (RowPosSstIndex.SstFileEntry f : part.files) {
                File source = new File(tmp, f.getFileName());
                download(new FsPath(dir, f.getFileName()), source); // throws if gone
                String name = REMAPPED_PREFIX + f.getFileName();
                File target = new File(tmp, name);
                remapSst(source, target, newIds);
                upload(target, new FsPath(dir, name));
                remapped.add(new RowPosSstIndex.SstFileEntry(name, target.length()));
            }
            for (RowPosSstIndex.SstFileEntry f : part.files) {
                dir.getFileSystem().delete(new FsPath(dir, f.getFileName()), false);
            }
            return new BucketPart(remapped);
        } finally {
            FileUtils.deleteDirectoryQuietly(tmp);
        }
    }

    @Override
    public BucketPart writeBucket(
            long changeId,
            TableBucket bucket,
            String namePrefix,
            Iterator<Map.Entry<byte[], FilePos>> entries)
            throws IOException {
        markStarted(changeId);
        File tmp = createTempDirectory("fluss-pkidx-");
        try {
            List<RowPosSstIndex.SstFileEntry> files = writeSsts(tmp, namePrefix, entries);
            FsPath dir = bucketDir(changeId, bucket);
            for (RowPosSstIndex.SstFileEntry f : files) {
                upload(new File(tmp, f.getFileName()), new FsPath(dir, f.getFileName()));
            }
            return files.isEmpty() ? BucketPart.NONE : new BucketPart(files);
        } finally {
            FileUtils.deleteDirectoryQuietly(tmp);
        }
    }

    @Override
    public StagedPartWriter stageBucket(
            long stagingId, TableBucket bucket, String namePrefix, @Nullable File localDir)
            throws IOException {
        markStarted(stagingId);
        File tmp =
                createTempDirectory(
                        localDir == null ? localTmpDir : localDir, "fluss-pkidx-stage-");
        return new FsStagedPartWriter(tmp, bucketDir(stagingId, bucket), namePrefix);
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
        markStarted(changeId);
        if (staged.files.isEmpty()) {
            return BucketPart.NONE;
        }
        checkChangeVersion(stagingId);
        FsPath source = bucketDir(stagingId, bucket);
        FsPath target = bucketDir(changeId, bucket);
        File tmp = createTempDirectory("fluss-pkidx-publish-");
        try {
            List<RowPosSstIndex.SstFileEntry> published = new ArrayList<>();
            for (RowPosSstIndex.SstFileEntry f : staged.files) {
                File in = new File(tmp, "staged-" + f.getFileName());
                download(new FsPath(source, f.getFileName()), in); // throws if gone
                File out = new File(tmp, f.getFileName());
                publishSst(in, out, newIds, keys);
                upload(out, new FsPath(target, f.getFileName()));
                published.add(new RowPosSstIndex.SstFileEntry(f.getFileName(), out.length()));
                Files.delete(in.toPath());
                Files.delete(out.toPath());
            }
            return new BucketPart(published);
        } finally {
            FileUtils.deleteDirectoryQuietly(tmp);
        }
    }

    @Override
    public void commitChange(long changeId, Map<TableBucket, BucketChange> buckets)
            throws IOException {
        FsPath committed = new FsPath(changeDir(changeId), CHANGE_COMMITTED);
        if (exists(committed)) {
            throw new IllegalStateException("Index change " + changeId + " is already committed.");
        }
        markStarted(changeId);
        Map<Long, Map<Integer, List<RowPosSstIndex.SstFileEntry>>> ssts = new TreeMap<>();
        Map<Long, Map<Integer, Map<Integer, String>>> added = new HashMap<>();
        Map<Long, Map<Integer, List<String>>> removed = new HashMap<>();
        for (Map.Entry<TableBucket, BucketChange> e : buckets.entrySet()) {
            TableBucket tb = e.getKey();
            BucketChange c = e.getValue();
            if (c.part.files.isEmpty() && c.addedFiles.isEmpty() && c.removedFiles.isEmpty()) {
                // A bucket the change does not touch is left out. Its partition gets no manifest
                // unless another of its buckets is touched, and trimming finds the buckets a
                // change touches from its manifests alone.
                continue;
            }
            for (RowPosSstIndex.SstFileEntry f : c.part.files) {
                FsPath p = new FsPath(bucketDir(changeId, tb), f.getFileName());
                if (!exists(p)) {
                    throw new IOException("Index change " + changeId + ": " + p + " is missing.");
                }
            }
            long pid = partitionKey(tb);
            ssts.computeIfAbsent(pid, k -> new HashMap<>()).put(tb.getBucket(), c.part.files);
            added.computeIfAbsent(pid, k -> new HashMap<>()).put(tb.getBucket(), c.addedFiles);
            if (!c.removedFiles.isEmpty()) {
                removed.computeIfAbsent(pid, k -> new HashMap<>())
                        .put(tb.getBucket(), c.removedFiles);
            }
        }
        StringBuilder partitions = new StringBuilder();
        for (long pid : ssts.keySet()) {
            FsPath manifest = manifestPath(changeId, pid == -1L ? null : pid);
            writeBytes(
                    manifest,
                    RowPosSstIndex.manifest(
                                    ssts.get(pid),
                                    added.get(pid),
                                    removed.getOrDefault(pid, Collections.emptyMap()))
                            .toJsonBytes());
            partitions.append(pid).append('\n');
        }
        // Written last: the change is complete once this exists.
        writeText(committed, FormatVersions.withVersion(CHANGE_VERSION, partitions.toString()));
        // The committed change needs no re-index plans; a leftover is deleted with the change.
        FsPath plans = new FsPath(changeDir(changeId), REINDEX_PLAN_DIR);
        try {
            plans.getFileSystem().delete(plans, true);
        } catch (IOException e) {
            // best effort
        }
    }

    @Override
    public String writeReindexPlan(
            long changeId, int split, int splitSerializerVersion, List<byte[]> lakeSplits)
            throws IOException {
        markStarted(changeId);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(PLAN_FORMAT_VERSION);
            out.writeInt(splitSerializerVersion);
            out.writeInt(lakeSplits.size());
            for (byte[] lakeSplit : lakeSplits) {
                out.writeInt(lakeSplit.length);
                out.write(lakeSplit);
            }
        }
        FsPath plan =
                new FsPath(
                        new FsPath(changeDir(changeId), REINDEX_PLAN_DIR), String.valueOf(split));
        writeBytes(plan, bytes.toByteArray());
        return plan.toString();
    }

    /** Reads a plan {@link #writeReindexPlan} stored at {@code reference}. */
    static ReindexPlan readPlan(String reference) throws IOException {
        byte[] bytes = readBytes(new FsPath(reference));
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            FormatVersions.check(
                    "the re-index plan " + reference, in.readInt(), PLAN_FORMAT_VERSION);
            int splitSerializerVersion = in.readInt();
            int count = in.readInt();
            List<byte[]> lakeSplits = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                byte[] lakeSplit = new byte[in.readInt()];
                in.readFully(lakeSplit);
                lakeSplits.add(lakeSplit);
            }
            return new ReindexPlan(splitSerializerVersion, lakeSplits);
        }
    }

    @Override
    public void abortChange(long changeId) throws IOException {
        for (ChainEntry e : chain()) {
            if (e.changeId == changeId) {
                throw new IllegalStateException(
                        "Index change " + changeId + " is registered (" + e + "); not aborting.");
            }
        }
        deleteChange(changeId);
    }

    @Override
    public void checkRegistrable(long parentSnapshotId, long changeId) throws IOException {
        checkCommitted(changeId);
        List<ChainEntry> chain = chain();
        checkChainEnd(chain, parentSnapshotId, -1);
        checkFileIds(changeId, fileIdRange(changeId));
    }

    @Override
    public void register(
            long parentSnapshotId,
            long snapshotId,
            long changeId,
            @Nullable String snapshotIdentity)
            throws IOException {
        checkCommitted(changeId);
        List<ChainEntry> chain = chain();
        for (ChainEntry e : chain) {
            if (e.snapshotId == snapshotId) {
                if (e.changeId == changeId) {
                    return; // a retried or repaired round
                }
                throw new IllegalStateException(
                        "Snapshot " + snapshotId + " is registered with another change: " + e);
            }
        }
        checkChainEnd(chain, parentSnapshotId, snapshotId);
        // New file ids must be above every registered id, or an id handed out twice would let one
        // file's positions mask another's.
        int[] ids = fileIdRange(changeId);
        checkFileIds(changeId, ids);
        long sequence = 1;
        for (String name : markerNames()) {
            sequence = Math.max(sequence, parseMarker(name).sequence + 1);
        }
        String name =
                markerName(
                        sequence,
                        parentSnapshotId,
                        snapshotId,
                        changeId,
                        ids == null ? -1 : ids[1]);
        FsPath marker = new FsPath(new FsPath(lakeTableDir, CHAIN_DIR), name);
        beforeMarkerWrite();
        writeText(marker, snapshotIdentity == null ? "" : snapshotIdentity);
        // The checks above and this write are not atomic, on object stores in particular, so a
        // writer racing on the same chain end can pass them too. After its write, each writer
        // looks for another marker of its sequence. Of two racing writers at most one finds none
        // (whoever lists first wrote first), so at most one stays registered. A racer that
        // registers the same step stays too, as either change describes it.
        for (String other : markerNames()) {
            ChainEntry e = parseMarker(other);
            if (!other.equals(name)
                    && e.sequence == sequence
                    && (e.parentSnapshotId != parentSnapshotId || e.snapshotId != snapshotId)) {
                marker.getFileSystem().delete(marker, false);
                throw new IllegalStateException(
                        String.format(
                                "Another writer registered %s at the end of the position index"
                                        + " chain concurrently; the change %d -> %d (%d) is"
                                        + " withdrawn.",
                                e, parentSnapshotId, snapshotId, changeId));
            }
        }
    }

    private void checkCommitted(long changeId) throws IOException {
        if (!exists(new FsPath(changeDir(changeId), CHANGE_COMMITTED))) {
            throw new IllegalStateException(
                    "Index change " + changeId + " is not committed; cannot register it.");
        }
    }

    private static void checkChainEnd(
            List<ChainEntry> chain, long parentSnapshotId, long snapshotId) {
        long tail = chain.isEmpty() ? -1L : chain.get(chain.size() - 1).snapshotId;
        if (parentSnapshotId != tail) {
            throw new IllegalStateException(
                    String.format(
                            "The position index chain ends at snapshot %d; the change %d -> %d"
                                    + " does not apply to it.",
                            tail, parentSnapshotId, snapshotId));
        }
    }

    /** Refuses a change whose new file ids are not all above every registered id. */
    private void checkFileIds(long changeId, @Nullable int[] ids) throws IOException {
        if (ids == null) {
            return;
        }
        long registeredMax = -1;
        for (String name : markerNames()) {
            registeredMax = Math.max(registeredMax, markerMaxFileId(name));
        }
        if (ids[0] <= registeredMax) {
            throw new IllegalStateException(
                    String.format(
                            "Index change %d uses file ids %d..%d, but ids up to %d are registered"
                                    + " already (another tiering writer?).",
                            changeId, ids[0], ids[1], registeredMax));
        }
    }

    @Override
    @Nullable
    public String snapshotIdentity(ChainEntry entry) throws IOException {
        FsPath chainDir = new FsPath(lakeTableDir, CHAIN_DIR);
        for (String name : markerNames()) {
            if (parseMarker(name).equals(entry)) {
                String identity = readText(new FsPath(chainDir, name));
                return identity == null || identity.isEmpty() ? null : identity;
            }
        }
        return null;
    }

    /** Runs right before a registration writes its chain marker; a hook for tests. */
    @VisibleForTesting
    void beforeMarkerWrite() throws IOException {}

    @Override
    public int deleteOrphanChanges(long minAgeMillis) throws IOException {
        FsPath rowPos = new FsPath(lakeTableDir, ROW_POS_DIR);
        FileSystem fs = rowPos.getFileSystem();
        if (!fs.exists(rowPos)) {
            return 0;
        }
        Set<Long> registered = new HashSet<>();
        for (ChainEntry e : rawChain()) {
            registered.add(e.changeId);
        }
        long now = System.currentTimeMillis();
        int deleted = 0;
        for (FileStatus st : fs.listStatus(rowPos)) {
            Long changeId = parseLong(st.getPath().getName());
            if (changeId == null || registered.contains(changeId)) {
                continue;
            }
            FsPath startedPath = new FsPath(changeDir(changeId), CHANGE_STARTED);
            String started = readText(startedPath);
            // A marker read while it is being written is empty; its change is new and is kept.
            Long startedAt =
                    started == null || started.isEmpty()
                            ? null
                            : parseLong(
                                    FormatVersions.withoutVersion(
                                                    startedPath, started, CHANGE_VERSION)
                                            .trim());
            if (startedAt != null && now - startedAt >= minAgeMillis) {
                deleteChange(changeId);
                deleted++;
            }
        }
        return deleted;
    }

    @Override
    public int trim(Collection<TableBucket> bucketsWithData, int keepExtra) throws IOException {
        return trim(
                bucketsWithData,
                keepExtra,
                IDLE_ENTRIES,
                MAX_CHECKPOINT_BUILDS,
                MAX_CHECKPOINT_BUILD_MILLIS);
    }

    /**
     * Trims the chain like {@link #trim(Collection, int)}, building checkpoints of buckets idle for
     * {@code idleEntries} chain entries, at most {@code maxBuilds} of them and none started after
     * {@code maxBuildMillis}.
     */
    @VisibleForTesting
    int trim(
            Collection<TableBucket> bucketsWithData,
            int keepExtra,
            int idleEntries,
            int maxBuilds,
            long maxBuildMillis)
            throws IOException {
        List<ChainEntry> chain = chain();
        if (bucketsWithData.isEmpty() || chain.isEmpty()) {
            return 0;
        }
        int last = chain.size() - 1;
        Map<TableBucket, Coverage> coverage = new LinkedHashMap<>();
        for (TableBucket tb : bucketsWithData) {
            coverage.put(tb, coverage(tb, chain));
        }
        // Buckets whose index servers checkpoint them hold the chain at their checkpoints. A
        // bucket without a checkpoint, or idle since a change its checkpoint lacks, would hold it
        // for good, so its checkpoint is built here. Buckets holding the chain furthest back go
        // first.
        int heldByOthers = last;
        List<Map.Entry<TableBucket, Coverage>> toBuild = new ArrayList<>();
        for (Map.Entry<TableBucket, Coverage> e : coverage.entrySet()) {
            Coverage c = e.getValue();
            if (c.covered >= last) {
                continue;
            }
            if (c.checkpoint == null || c.lastTouch <= last - idleEntries) {
                toBuild.add(e);
            } else {
                heldByOthers = Math.min(heldByOthers, c.covered);
            }
        }
        toBuild.sort(Comparator.comparingInt(e -> e.getValue().covered));
        long buildStart = System.currentTimeMillis();
        int built = 0;
        for (Map.Entry<TableBucket, Coverage> e : toBuild) {
            if (e.getValue().covered >= heldByOthers
                    || built >= maxBuilds
                    || System.currentTimeMillis() - buildStart > maxBuildMillis) {
                break;
            }
            TableBucket tb = e.getKey();
            try {
                buildCheckpoint(tb, chain, e.getValue());
                built++;
                coverage.put(tb, coverage(tb, chain));
            } catch (IOException ex) {
                LOG.warn(
                        "Could not build the position index checkpoint of {}; it holds the chain"
                                + " until a later trim builds it.",
                        tb,
                        ex);
            }
        }
        int cut = Integer.MAX_VALUE;
        for (Coverage c : coverage.values()) {
            cut = Math.min(cut, c.covered - keepExtra);
        }
        if (cut <= 0) {
            return 0;
        }
        Map<Long, Integer> indexOf = new HashMap<>();
        for (int i = 0; i < chain.size(); i++) {
            indexOf.put(chain.get(i).snapshotId, i);
        }
        // Carry over checkpoints before anything is deleted. A checkpoint the cut leaves behind is
        // its bucket's index at the chain's new start, as no change from it to the cut touches
        // the bucket. The bucket's other checkpoints left behind are not.
        for (Map.Entry<TableBucket, Coverage> e : coverage.entrySet()) {
            carryOver(e.getKey(), checkNotNull(e.getValue().checkpoint), indexOf, cut);
        }
        Set<Long> keptChanges = new HashSet<>();
        for (int i = cut; i < chain.size(); i++) {
            keptChanges.add(chain.get(i).changeId);
        }
        // Mark the chain trimmed before deleting anything, so no rebuild starts at its beginning.
        writeText(
                new FsPath(lakeTableDir, CHAIN_TRIMMED),
                FormatVersions.withVersion(
                        TRIMMED_VERSION, String.valueOf(chain.get(cut).snapshotId)));
        Set<Long> trimmedSnapshots = new HashSet<>();
        for (int i = 0; i < cut; i++) {
            trimmedSnapshots.add(chain.get(i).snapshotId);
            if (!keptChanges.contains(chain.get(i).changeId)) {
                deleteChange(chain.get(i).changeId);
                touchedBuckets.remove(chain.get(i).changeId);
            }
        }
        FsPath chainDir = new FsPath(lakeTableDir, CHAIN_DIR);
        for (String name : markerNames()) {
            if (trimmedSnapshots.contains(parseMarker(name).snapshotId)) {
                chainDir.getFileSystem().delete(new FsPath(chainDir, name), false);
            }
        }
        if (built > 0) {
            LOG.info(
                    "Built the position index checkpoints of {} idle buckets of {} in {} ms.",
                    built,
                    lakeTableDir,
                    System.currentTimeMillis() - buildStart);
        }
        return cut;
    }

    /** How far a bucket can be rebuilt from its latest checkpoint without trimmed changes. */
    private static final class Coverage {
        // the latest complete checkpoint, possibly carried over (chain index -1), or null
        @Nullable final Checkpoint checkpoint;
        // the last chain index at which the bucket's index is still the checkpoint's, as no change
        // after the checkpoint up to it touches the bucket; -1 without a checkpoint
        final int covered;
        // the last chain index whose change touches the bucket, -1 if none does
        final int lastTouch;

        Coverage(@Nullable Checkpoint checkpoint, int covered, int lastTouch) {
            this.checkpoint = checkpoint;
            this.covered = covered;
            this.lastTouch = lastTouch;
        }
    }

    private Coverage coverage(TableBucket bucket, List<ChainEntry> chain) throws IOException {
        Checkpoint checkpoint = latestCheckpoint(bucket, chain, chain.size() - 1);
        int from = checkpoint == null ? 0 : checkpoint.chainIndex + 1;
        int firstTouchAfter = chain.size();
        int lastTouch = -1;
        for (int i = 0; i < chain.size(); i++) {
            if (touches(chain.get(i), bucket)) {
                lastTouch = i;
                if (i >= from && firstTouchAfter == chain.size()) {
                    firstTouchAfter = i;
                }
            }
        }
        return new Coverage(checkpoint, checkpoint == null ? -1 : firstTouchAfter - 1, lastTouch);
    }

    /** Whether a registered change touches the bucket; true if that cannot be told. */
    private boolean touches(ChainEntry entry, TableBucket bucket) {
        Map<Long, Set<Integer>> touched = touchedBuckets(entry.changeId);
        if (touched == null) {
            return true;
        }
        Set<Integer> buckets = touched.get(partitionKey(bucket));
        return buckets != null && buckets.contains(bucket.getBucket());
    }

    /**
     * The buckets a committed change touches, by partition key: those its manifests give SSTs,
     * added or removed files. Null if they cannot be read.
     */
    @Nullable
    private Map<Long, Set<Integer>> touchedBuckets(long changeId) {
        Map<Long, Set<Integer>> cached = touchedBuckets.get(changeId);
        if (cached != null) {
            return cached;
        }
        Map<Long, Set<Integer>> touched = new HashMap<>();
        try {
            String partitions = readCommittedPartitions(changeId);
            if (partitions == null) {
                return null;
            }
            for (String line : lines(partitions)) {
                long pid = Long.parseLong(line);
                RowPosSstIndex index = readManifest(changeId, pid == -1L ? null : pid);
                Set<Integer> buckets = new HashSet<>();
                for (int b : index.getBucketIds()) {
                    if (!index.getFiles(b).isEmpty()
                            || !index.getFileId2Name(b).isEmpty()
                            || !index.getReplacedFiles(b).isEmpty()) {
                        buckets.add(b);
                    }
                }
                touched.put(pid, buckets);
            }
        } catch (IOException | RuntimeException e) {
            LOG.debug("Cannot tell the buckets index change {} touches.", changeId, e);
            return null;
        }
        touchedBuckets.put(changeId, touched);
        return touched;
    }

    /**
     * Marks the bucket's latest checkpoint carried over if the cut leaves it behind, and deletes
     * the bucket's other checkpoints left behind, which do not hold its index at the chain's start.
     */
    private void carryOver(
            TableBucket bucket, Checkpoint latest, Map<Long, Integer> indexOf, int cut)
            throws IOException {
        FsPath bucketDir = checkpointDir(bucket);
        if (latest.chainIndex < cut) {
            FsPath carried =
                    new FsPath(
                            new FsPath(bucketDir, String.valueOf(latest.snapshotId)), CKPT_CARRIED);
            if (!exists(carried)) {
                writeText(carried, String.valueOf(cut));
            }
        }
        FileSystem fs = bucketDir.getFileSystem();
        for (FileStatus st : fs.listStatus(bucketDir)) {
            Long id = parseLong(st.getPath().getName());
            if (id == null || id == latest.snapshotId) {
                continue;
            }
            Integer idx = indexOf.get(id);
            if (idx == null || idx < cut) {
                fs.delete(new FsPath(st.getPath(), CKPT_DONE), false);
                fs.delete(st.getPath(), true);
            }
        }
    }

    /**
     * Builds the bucket's checkpoint at the end of the chain, as its index server would: its latest
     * checkpoint, then every change since that touches it, in commit order, without the entries of
     * removed files.
     */
    private void buildCheckpoint(TableBucket bucket, List<ChainEntry> chain, Coverage coverage)
            throws IOException {
        int target = chain.size() - 1;
        File dir = createTempDirectory("fluss-pkidx-ckpt-");
        try {
            List<BucketData> parts = new ArrayList<>();
            int from = 0;
            if (coverage.checkpoint != null) {
                parts.add(readCheckpoint(coverage.checkpoint, bucket, new File(dir, "ckpt")));
                from = coverage.checkpoint.chainIndex + 1;
            }
            for (int i = from; i <= target; i++) {
                if (touches(chain.get(i), bucket)) {
                    parts.add(readChange(chain.get(i), bucket, new File(dir, "change-" + i)));
                }
            }
            File sst = new File(dir, CKPT_SST);
            Map<Integer, String> fileDict = new HashMap<>();
            long entries = mergeIndex(parts, new File(dir, "db"), sst, fileDict);
            writeCheckpoint(
                    chain.get(target).snapshotId,
                    bucket,
                    entries > 0 ? sst : null,
                    fileDict,
                    false);
        } finally {
            FileUtils.deleteDirectoryQuietly(dir);
        }
    }

    /**
     * Applies checkpoints and changes in order to an empty index, as a tablet server does: file ids
     * first, then the SSTs, then the removed files. Exports the index into {@code sst} without the
     * entries of files missing from {@code fileDict}, which receives the remaining file ids.
     * Returns the number of entries exported; writes no file for an empty index.
     */
    @VisibleForTesting
    static long mergeIndex(
            List<BucketData> parts, File dbDir, File sst, Map<Integer, String> fileDict)
            throws IOException {
        RocksDB.loadLibrary();
        Map<String, Integer> idOfPath = new HashMap<>();
        try (Options options = new Options().setCreateIfMissing(true);
                RocksDB db = RocksDB.open(options, dbDir.getPath());
                IngestExternalFileOptions ingest = new IngestExternalFileOptions()) {
            ingest.setMoveFiles(true);
            for (BucketData part : parts) {
                for (Map.Entry<Integer, String> e : part.fileId2Name.entrySet()) {
                    fileDict.put(e.getKey(), e.getValue());
                    idOfPath.put(e.getValue(), e.getKey());
                }
                if (!part.localSsts.isEmpty()) {
                    db.ingestExternalFile(part.localSsts, ingest);
                }
                for (String removed : part.removedFiles) {
                    Integer id = idOfPath.remove(removed);
                    if (id != null) {
                        fileDict.remove(id);
                    }
                }
            }
            long n = 0;
            try (EnvOptions envOptions = new EnvOptions();
                    SstFileWriter writer = new SstFileWriter(envOptions, options);
                    ReadOptions readOptions = new ReadOptions();
                    RocksIterator it = db.newIterator(readOptions)) {
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    if (!fileDict.containsKey(FilePos.decode(it.value()).fileId())) {
                        continue;
                    }
                    if (n == 0) {
                        writer.open(sst.getPath());
                    }
                    writer.put(it.key(), it.value());
                    n++;
                }
                it.status();
                if (n > 0) {
                    writer.finish();
                }
            }
            return n;
        } catch (RocksDBException e) {
            throw new IOException("Failed to merge the position index in " + dbDir, e);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Reading
    // ------------------------------------------------------------------------------------------

    @Override
    public List<ChainEntry> chain() throws IOException {
        List<ChainEntry> chain = withoutEquivalentDuplicates(rawChain());
        for (int i = 1; i < chain.size(); i++) {
            ChainEntry e = chain.get(i);
            if (e.parentSnapshotId != chain.get(i - 1).snapshotId) {
                throw new IllegalStateException(
                        "The position index chain is broken: "
                                + e
                                + " follows "
                                + chain.get(i - 1));
            }
        }
        return chain;
    }

    @Override
    public boolean chainTrimmed() throws IOException {
        return exists(new FsPath(lakeTableDir, CHAIN_TRIMMED));
    }

    @Override
    public BucketData readChange(ChainEntry entry, TableBucket bucket, File localDir)
            throws IOException {
        String partitions = readCommittedPartitions(entry.changeId);
        if (partitions == null) {
            throw new IOException(
                    "Index change of " + entry + " is gone (trimmed, or never committed).");
        }
        if (!lines(partitions).contains(String.valueOf(partitionKey(bucket)))) {
            return BucketData.EMPTY; // the change does not touch this partition
        }
        // Throws if the manifest is gone.
        RowPosSstIndex index = readManifest(entry.changeId, bucket.getPartitionId());
        int b = bucket.getBucket();
        FsPath bucketDir = bucketDir(entry.changeId, bucket);
        List<String> local = new ArrayList<>();
        List<RowPosSstIndex.SstFileEntry> files = index.getFiles(b);
        if (!files.isEmpty()) {
            Files.createDirectories(localDir.toPath());
            for (RowPosSstIndex.SstFileEntry f : files) {
                File target = new File(localDir, f.getFileName());
                download(new FsPath(bucketDir, f.getFileName()), target); // throws if gone
                local.add(target.getPath());
            }
        }
        return new BucketData(
                local,
                new HashMap<>(index.getFileId2Name(b)),
                new ArrayList<>(index.getReplacedFiles(b)));
    }

    /**
     * The partitions a committed change touches, one per line; null if the change is not committed
     * or is gone. The list never changes once written, so a reader may cache it.
     */
    @Nullable
    protected String readCommittedPartitions(long changeId) throws IOException {
        FsPath committed = new FsPath(changeDir(changeId), CHANGE_COMMITTED);
        String text = readText(committed);
        return text == null ? null : FormatVersions.withoutVersion(committed, text, CHANGE_VERSION);
    }

    /**
     * The manifest of a committed change's part of a partition; throws if it is gone. It never
     * changes once written, so a reader may cache it.
     */
    protected RowPosSstIndex readManifest(long changeId, @Nullable Long partitionId)
            throws IOException {
        FsPath manifest = manifestPath(changeId, partitionId);
        return RowPosSstIndex.fromJsonBytes(readBytes(manifest), manifest);
    }

    /** The directory of the table's lake snapshots, under which the index is kept. */
    public FsPath lakeTableDir() {
        return lakeTableDir;
    }

    @Override
    @Nullable
    public Checkpoint latestCheckpoint(TableBucket bucket, List<ChainEntry> chain, int upTo)
            throws IOException {
        FsPath dir = checkpointDir(bucket);
        FileSystem fs = dir.getFileSystem();
        if (!fs.exists(dir)) {
            return null;
        }
        Map<Long, Integer> indexOf = new HashMap<>();
        Set<Long> inChain = new HashSet<>();
        for (int i = 0; i < chain.size(); i++) {
            inChain.add(chain.get(i).snapshotId);
            if (i <= upTo) {
                indexOf.put(chain.get(i).snapshotId, i);
            }
        }
        Checkpoint best = null;
        for (FileStatus st : fs.listStatus(dir)) {
            Long snapshotId = parseLong(st.getPath().getName());
            if (snapshotId == null) {
                continue;
            }
            Integer idx = indexOf.get(snapshotId);
            if (idx == null) {
                // A carried-over checkpoint is the bucket's index at the start of the chain.
                if (inChain.contains(snapshotId)
                        || !fs.exists(new FsPath(st.getPath(), CKPT_CARRIED))) {
                    continue;
                }
                idx = -1;
            }
            if ((best == null || idx > best.chainIndex)
                    && fs.exists(new FsPath(st.getPath(), CKPT_DONE))) {
                best = new Checkpoint(idx, snapshotId);
            }
        }
        return best;
    }

    @Override
    public BucketData readCheckpoint(Checkpoint checkpoint, TableBucket bucket, File localDir)
            throws IOException {
        FsPath dir = new FsPath(checkpointDir(bucket), String.valueOf(checkpoint.snapshotId));
        FsPath donePath = new FsPath(dir, CKPT_DONE);
        String done = readText(donePath);
        String dict = readText(new FsPath(dir, FILE_DICT));
        if (done == null || dict == null) {
            throw new IOException("Index " + checkpoint + " of " + bucket + " is gone.");
        }
        done = FormatVersions.withoutVersion(donePath, done, CHECKPOINT_VERSION);
        FsPath sst = new FsPath(dir, CKPT_SST);
        List<String> local = Collections.emptyList();
        if (DONE_SST.equals(done.trim())) {
            Files.createDirectories(localDir.toPath());
            File target = new File(localDir, CKPT_SST);
            download(sst, target); // throws if gone
            local = Collections.singletonList(target.getPath());
        }
        return new BucketData(local, readDict(dict), Collections.emptyList());
    }

    @Override
    public void writeCheckpoint(
            long snapshotId,
            TableBucket bucket,
            @Nullable File localSst,
            Map<Integer, String> fileId2Name)
            throws IOException {
        writeCheckpoint(snapshotId, bucket, localSst, fileId2Name, true);
    }

    /**
     * Writes a checkpoint and drops the bucket's other checkpoints but the newest. With {@code
     * dropNewer} it also drops those newer than this one: a tablet server's checkpoint replaces any
     * its successor wrote on a snapshot it never covered.
     */
    private void writeCheckpoint(
            long snapshotId,
            TableBucket bucket,
            @Nullable File localSst,
            Map<Integer, String> fileId2Name,
            boolean dropNewer)
            throws IOException {
        FsPath bucketDir = checkpointDir(bucket);
        FsPath dir = new FsPath(bucketDir, String.valueOf(snapshotId));
        if (localSst != null) {
            upload(localSst, new FsPath(dir, CKPT_SST));
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Integer, String> e : fileId2Name.entrySet()) {
            sb.append(e.getKey()).append('\t').append(e.getValue()).append('\n');
        }
        writeText(new FsPath(dir, FILE_DICT), sb.toString());
        writeText(
                new FsPath(dir, CKPT_DONE),
                FormatVersions.withVersion(
                        CHECKPOINT_VERSION, localSst != null ? DONE_SST : DONE_EMPTY));
        // Keep the newest by chain order; drop those not in the chain or newer than this one.
        List<ChainEntry> chain = chain();
        Map<Long, Integer> indexOf = new HashMap<>();
        for (int i = 0; i < chain.size(); i++) {
            indexOf.put(chain.get(i).snapshotId, i);
        }
        int newest = indexOf.getOrDefault(snapshotId, Integer.MAX_VALUE);
        List<long[]> others = new ArrayList<>();
        Set<Long> carried = new HashSet<>();
        for (FileStatus st : bucketDir.getFileSystem().listStatus(bucketDir)) {
            Long id = parseLong(st.getPath().getName());
            if (id == null || id == snapshotId) {
                continue;
            }
            if (!indexOf.containsKey(id) && exists(new FsPath(st.getPath(), CKPT_CARRIED))) {
                carried.add(id);
            }
            others.add(new long[] {indexOf.getOrDefault(id, -1), id});
        }
        others.sort((a, b) -> Long.compare(b[0], a[0]));
        for (int i = 0; i < others.size(); i++) {
            long idx = others.get(i)[0];
            if (i >= CHECKPOINTS_KEPT - 1
                    || (idx < 0 && !carried.contains(others.get(i)[1]))
                    || (dropNewer && idx > newest)) {
                FsPath old = new FsPath(bucketDir, String.valueOf(others.get(i)[1]));
                // _DONE first, so a reader fails instead of reading part of the checkpoint.
                old.getFileSystem().delete(new FsPath(old, CKPT_DONE), false);
                old.getFileSystem().delete(old, true);
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // Layout
    // ------------------------------------------------------------------------------------------

    private FsPath changeDir(long changeId) {
        return new FsPath(lakeTableDir, ROW_POS_DIR + "/" + changeId);
    }

    private FsPath changeDir(long changeId, @Nullable Long partitionId) {
        return partitionId == null
                ? changeDir(changeId)
                : new FsPath(changeDir(changeId), String.valueOf(partitionId));
    }

    private FsPath manifestPath(long changeId, @Nullable Long partitionId) {
        return new FsPath(changeDir(changeId, partitionId), RowPosSstIndex.MANIFEST_FILE);
    }

    private FsPath bucketDir(long changeId, TableBucket bucket) {
        return new FsPath(
                changeDir(changeId, bucket.getPartitionId()), String.valueOf(bucket.getBucket()));
    }

    private FsPath checkpointDir(TableBucket bucket) {
        Long pid = bucket.getPartitionId();
        return new FsPath(
                lakeTableDir, CKPT_DIR + (pid == null ? "" : "/" + pid) + "/" + bucket.getBucket());
    }

    /**
     * A new local directory for temporary files, under the store's local directory if it has one.
     */
    private File createTempDirectory(String prefix) throws IOException {
        return createTempDirectory(localTmpDir, prefix);
    }

    /** A new local directory for temporary files, under {@code parent} if not null. */
    private static File createTempDirectory(@Nullable File parent, String prefix)
            throws IOException {
        if (parent == null) {
            return Files.createTempDirectory(prefix).toFile();
        }
        Files.createDirectories(parent.toPath());
        return Files.createTempDirectory(parent.toPath(), prefix).toFile();
    }

    private static long partitionKey(TableBucket bucket) {
        return bucket.getPartitionId() == null ? -1L : bucket.getPartitionId();
    }

    /** Deletes a change: its commit marker first, so a reader fails instead of reading part. */
    private void deleteChange(long changeId) throws IOException {
        FsPath dir = changeDir(changeId);
        FileSystem fs = dir.getFileSystem();
        if (fs.exists(dir)) {
            fs.delete(new FsPath(dir, CHANGE_COMMITTED), false);
            fs.delete(dir, true);
        }
        checkedChanges.remove(changeId);
    }

    /**
     * Marks the change started unless it is, and returns once the marker exists. The writers of a
     * change mark it concurrently, such as the compactions of several buckets. On an object store a
     * marker being written is not visible yet, and a create racing it may fail ("is a directory").
     * A failed create then waits for the other writer's marker instead of failing the writer.
     */
    private void markStarted(long changeId) throws IOException {
        FsPath started = new FsPath(changeDir(changeId), CHANGE_STARTED);
        long backoffMs = MARK_STARTED_INITIAL_BACKOFF_MS;
        for (int attempt = 1; ; attempt++) {
            if (exists(started)) {
                return;
            }
            try {
                // The first marker stays: the change started then.
                writeBytes(
                        started,
                        FormatVersions.withVersion(
                                        CHANGE_VERSION, String.valueOf(System.currentTimeMillis()))
                                .getBytes(StandardCharsets.UTF_8),
                        FileSystem.WriteMode.NO_OVERWRITE);
                return;
            } catch (IOException e) {
                if (attempt >= MARK_STARTED_ATTEMPTS) {
                    throw e;
                }
                LOG.debug(
                        "Could not mark index change {} started (attempt {}); checking again.",
                        changeId,
                        attempt,
                        e);
            }
            try {
                Thread.sleep(backoffMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException(
                        "Interrupted while marking index change " + changeId + " started.");
            }
            backoffMs = Math.min(2 * backoffMs, MARK_STARTED_MAX_BACKOFF_MS);
        }
    }

    /** {min, max} of the file ids a committed change adds, or null if it adds none. */
    @Nullable
    private int[] fileIdRange(long changeId) throws IOException {
        int[] range = null;
        for (String pid : lines(readCommittedPartitions(changeId))) {
            long p = Long.parseLong(pid);
            RowPosSstIndex index = readManifest(changeId, p == -1L ? null : p);
            for (int b : index.getBucketIds()) {
                for (int id : index.getFileId2Name(b).keySet()) {
                    range =
                            range == null
                                    ? new int[] {id, id}
                                    : new int[] {Math.min(range[0], id), Math.max(range[1], id)};
                }
            }
        }
        return range;
    }

    /** The largest file id a marker's change added, -1 if none. */
    private static long markerMaxFileId(String name) {
        return Long.parseLong(markerFields(name).split("_")[4]);
    }

    private List<String> markerNames() throws IOException {
        FsPath chainDir = new FsPath(lakeTableDir, CHAIN_DIR);
        FileSystem fs = chainDir.getFileSystem();
        List<String> names = new ArrayList<>();
        if (fs.exists(chainDir)) {
            for (FileStatus st : fs.listStatus(chainDir)) {
                names.add(st.getPath().getName());
            }
        }
        return names;
    }

    /**
     * Keeps one of the markers that register the same step under the same sequence: the one with
     * the smallest change id. Writers racing on the same step, such as two re-indexes of the same
     * rewrite, may both stay registered, and either change describes the step.
     */
    private static List<ChainEntry> withoutEquivalentDuplicates(List<ChainEntry> entries) {
        List<ChainEntry> kept = new ArrayList<>(entries.size());
        for (ChainEntry e : entries) {
            ChainEntry last = kept.isEmpty() ? null : kept.get(kept.size() - 1);
            if (last != null
                    && last.sequence == e.sequence
                    && last.parentSnapshotId == e.parentSnapshotId
                    && last.snapshotId == e.snapshotId) {
                if (Long.compareUnsigned(e.changeId, last.changeId) < 0) {
                    kept.set(kept.size() - 1, e);
                }
                continue;
            }
            kept.add(e);
        }
        return kept;
    }

    /** Every marker in sequence order. */
    private List<ChainEntry> rawChain() throws IOException {
        List<ChainEntry> entries = new ArrayList<>();
        for (String name : markerNames()) {
            entries.add(parseMarker(name));
        }
        entries.sort((a, b) -> Long.compare(a.sequence, b.sequence));
        return entries;
    }

    static ChainEntry parseMarker(String name) {
        String[] f = markerFields(name).split("_");
        return new ChainEntry(
                Long.parseLong(f[0]),
                Long.parseLong(f[1]),
                Long.parseLong(f[2]),
                Long.parseLong(f[3]));
    }

    /** The name of a chain entry, its format version first. */
    @VisibleForTesting
    static String markerName(
            long sequence, long parentSnapshotId, long snapshotId, long changeId, long maxFileId) {
        return CHAIN_ENTRY_VERSION_PREFIX
                + CHAIN_ENTRY_VERSION
                + CHAIN_ENTRY_VERSION_END
                + String.format(
                        "%020d_%d_%d_%d_%d",
                        sequence, parentSnapshotId, snapshotId, changeId, maxFileId);
    }

    /** The fields of a chain entry's name, once the version it starts with is checked. */
    private static String markerFields(String name) {
        int end = name.indexOf(CHAIN_ENTRY_VERSION_END);
        Integer version = null;
        if (name.startsWith(CHAIN_ENTRY_VERSION_PREFIX) && end > 1) {
            version = parseInteger(name.substring(1, end));
        }
        String source = "the position index chain entry " + CHAIN_DIR + "/" + name;
        if (version == null) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot read %s: it has no format version; this Fluss version reads"
                                    + " format versions up to %d.",
                            source, CHAIN_ENTRY_VERSION));
        }
        FormatVersions.check(source, version, CHAIN_ENTRY_VERSION);
        return name.substring(end + 1);
    }

    /**
     * Checks the format version of a change's directory before its uncommitted SSTs are read. Its
     * {@code _STARTED} is the first file written into it.
     */
    private void checkChangeVersion(long changeId) throws IOException {
        if (checkedChanges.contains(changeId)) {
            return;
        }
        FsPath started = new FsPath(changeDir(changeId), CHANGE_STARTED);
        String text = readText(started);
        if (text == null) {
            throw new IOException("Index change " + changeId + ": " + started + " is missing.");
        }
        FormatVersions.withoutVersion(started, text, CHANGE_VERSION);
        checkedChanges.add(changeId);
    }

    /** Copies an index SST, replacing the file id of every position by its new id. */
    private static void remapSst(File source, File target, Map<Integer, Integer> newIds)
            throws IOException {
        try (Options options = new Options();
                EnvOptions envOptions = new EnvOptions();
                SstFileReader reader = new SstFileReader(options);
                ReadOptions readOptions = new ReadOptions();
                SstFileWriter writer = new SstFileWriter(envOptions, options)) {
            reader.open(source.getPath());
            if (reader.getTableProperties().getNumDeletions() > 0) {
                // the reader's iterator skips tombstones, which a copy would then lose
                throw new IOException(
                        "Cannot remap the file ids of " + source + ": it has deletes.");
            }
            writer.open(target.getPath());
            try (SstFileReaderIterator it = reader.newIterator(readOptions)) {
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    FilePos pos = FilePos.decode(it.value());
                    Integer id = newIds.get(pos.fileId());
                    if (id == null) {
                        throw new IOException(
                                "Cannot remap "
                                        + source
                                        + ": no new id for file id "
                                        + pos.fileId());
                    }
                    writer.put(it.key(), new FilePos(id, pos.rowPosition()).encode());
                }
                it.status();
            }
            writer.finish();
        } catch (RocksDBException e) {
            throw new IOException("Failed to remap the file ids of " + source, e);
        }
    }

    /**
     * Copies a staged SST into an index SST: a staged delete becomes a tombstone, and every
     * position gets the new id of its file; passes every key to {@code keys}.
     */
    private static void publishSst(
            File source, File target, Map<Integer, Integer> newIds, KeyConsumer keys)
            throws IOException {
        try (Options options = new Options();
                EnvOptions envOptions = new EnvOptions();
                SstFileReader reader = new SstFileReader(options);
                ReadOptions readOptions = new ReadOptions();
                SstFileWriter writer = new SstFileWriter(envOptions, options)) {
            reader.open(source.getPath());
            writer.open(target.getPath());
            try (SstFileReaderIterator it = reader.newIterator(readOptions)) {
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    byte[] key = it.key();
                    byte[] value = it.value();
                    if (value.length == 0) {
                        writer.delete(key);
                    } else {
                        FilePos pos = FilePos.decode(value);
                        Integer id = newIds.get(pos.fileId());
                        if (id == null) {
                            throw new IOException(
                                    "Cannot publish "
                                            + source
                                            + ": no new id for file id "
                                            + pos.fileId());
                        }
                        writer.put(key, new FilePos(id, pos.rowPosition()).encode());
                    }
                    keys.accept(key);
                }
                it.status();
            }
            writer.finish();
        } catch (RocksDBException e) {
            throw new IOException("Failed to publish the staged index SST " + source, e);
        }
    }

    /**
     * Writes a staged part into local SSTs of at most {@link #MAX_ENTRIES_PER_SST} entries and
     * stores each once it is full. A delete is an empty value; no encoded position is empty.
     */
    private static final class FsStagedPartWriter implements StagedPartWriter {
        private static final byte[] DELETED = new byte[0];

        private final File localDir;
        private final FsPath dir;
        private final String namePrefix;
        private final List<RowPosSstIndex.SstFileEntry> stored = new ArrayList<>();
        @Nullable private EnvOptions envOptions;
        @Nullable private Options options;
        @Nullable private SstFileWriter writer;
        @Nullable private File file;
        private int entries;
        private boolean closed;

        FsStagedPartWriter(File localDir, FsPath dir, String namePrefix) {
            this.localDir = localDir;
            this.dir = dir;
            this.namePrefix = namePrefix;
        }

        @Override
        public void put(byte[] key, FilePos pos) throws IOException {
            add(key, pos.encode());
        }

        @Override
        public void delete(byte[] key) throws IOException {
            add(key, DELETED);
        }

        private void add(byte[] key, byte[] value) throws IOException {
            if (closed) {
                throw new IllegalStateException("The staged part writer is closed.");
            }
            try {
                if (writer == null) {
                    if (options == null) {
                        envOptions = new EnvOptions();
                        options = new Options();
                    }
                    file = new File(localDir, namePrefix + "sst_" + stored.size() + ".sst");
                    writer = new SstFileWriter(envOptions, options);
                    writer.open(file.getPath());
                }
                // RocksDB rejects keys out of strictly ascending order
                writer.put(key, value);
            } catch (RocksDBException e) {
                throw new IOException("Failed to write a staged index SST in " + localDir, e);
            }
            if (++entries == MAX_ENTRIES_PER_SST) {
                storeCurrent();
            }
        }

        private void storeCurrent() throws IOException {
            SstFileWriter w = writer;
            File f = file;
            writer = null;
            file = null;
            entries = 0;
            try {
                w.finish();
            } catch (RocksDBException e) {
                throw new IOException("Failed to write the staged index SST " + f, e);
            } finally {
                w.close();
            }
            upload(f, new FsPath(dir, f.getName()));
            stored.add(new RowPosSstIndex.SstFileEntry(f.getName(), f.length()));
            Files.delete(f.toPath());
        }

        @Override
        public BucketPart finish() throws IOException {
            try {
                if (writer != null) {
                    storeCurrent();
                }
                return stored.isEmpty() ? BucketPart.NONE : new BucketPart(new ArrayList<>(stored));
            } finally {
                close();
            }
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            if (writer != null) {
                writer.close();
                writer = null;
            }
            if (options != null) {
                options.close();
                envOptions.close();
            }
            FileUtils.deleteDirectoryQuietly(localDir);
        }
    }

    private static List<RowPosSstIndex.SstFileEntry> writeSsts(
            File dir, String namePrefix, Iterator<Map.Entry<byte[], FilePos>> entries)
            throws IOException {
        List<RowPosSstIndex.SstFileEntry> out = new ArrayList<>();
        int n = 0;
        SstFileWriter writer = null;
        File file = null;
        try (EnvOptions envOptions = new EnvOptions();
                Options options = new Options()) {
            while (entries.hasNext()) {
                Map.Entry<byte[], FilePos> e = entries.next();
                if (writer == null) {
                    file = new File(dir, namePrefix + "sst_" + out.size() + ".sst");
                    writer = new SstFileWriter(envOptions, options);
                    writer.open(file.getPath());
                }
                // RocksDB rejects keys out of strictly ascending order
                if (e.getValue() == null) {
                    writer.delete(e.getKey());
                } else {
                    writer.put(e.getKey(), e.getValue().encode());
                }
                if (++n == MAX_ENTRIES_PER_SST) {
                    writer.finish();
                    writer.close();
                    writer = null;
                    out.add(new RowPosSstIndex.SstFileEntry(file.getName(), file.length()));
                    n = 0;
                }
            }
            if (writer != null) {
                writer.finish();
                writer.close();
                writer = null;
                out.add(new RowPosSstIndex.SstFileEntry(file.getName(), file.length()));
            }
        } catch (RocksDBException ex) {
            throw new IOException("Failed to write position index SST in " + dir, ex);
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
        return out;
    }

    @Nullable
    private static Integer parseInteger(String s) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Nullable
    private static Long parseLong(String s) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Map<Integer, String> readDict(@Nullable String text) {
        Map<Integer, String> out = new HashMap<>();
        for (String line : lines(text)) {
            String[] f = line.split("\t", 2);
            out.put(Integer.parseInt(f[0]), f[1]);
        }
        return out;
    }

    private static List<String> lines(@Nullable String text) {
        List<String> out = new ArrayList<>();
        if (text != null) {
            for (String line : text.split("\n")) {
                if (!line.trim().isEmpty()) {
                    out.add(line.trim());
                }
            }
        }
        return out;
    }

    private static boolean exists(FsPath p) throws IOException {
        return p.getFileSystem().exists(p);
    }

    @Nullable
    private static String readText(FsPath p) throws IOException {
        return exists(p) ? new String(readBytes(p), StandardCharsets.UTF_8) : null;
    }

    private static byte[] readBytes(FsPath p) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (FSDataInputStream in = p.getFileSystem().open(p)) {
            copy(in, bos);
        }
        return bos.toByteArray();
    }

    private static void writeText(FsPath path, String content) throws IOException {
        writeBytes(path, content.getBytes(StandardCharsets.UTF_8));
    }

    private static void writeBytes(FsPath path, byte[] content) throws IOException {
        writeBytes(path, content, FileSystem.WriteMode.OVERWRITE);
    }

    private static void writeBytes(FsPath path, byte[] content, FileSystem.WriteMode mode)
            throws IOException {
        try (FSDataOutputStream out = path.getFileSystem().create(path, mode)) {
            out.write(content);
        }
    }

    private static void upload(File local, FsPath target) throws IOException {
        try (InputStream in = Files.newInputStream(local.toPath());
                FSDataOutputStream out =
                        target.getFileSystem().create(target, FileSystem.WriteMode.OVERWRITE)) {
            copy(in, out);
        }
    }

    private static void download(FsPath src, File target) throws IOException {
        try (FSDataInputStream in = src.getFileSystem().open(src);
                OutputStream out = Files.newOutputStream(target.toPath())) {
            copy(in, out);
        }
    }

    private static void copy(InputStream in, OutputStream out) throws IOException {
        byte[] buf = new byte[1 << 16];
        int n;
        while ((n = in.read(buf)) > 0) {
            out.write(buf, 0, n);
        }
    }
}
