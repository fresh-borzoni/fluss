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
import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.RowPosSstFileWriter;
import org.apache.fluss.lake.dv.RowPosSstIndex;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.FlussPaths;

import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * The Fluss-native {@link PositionIndexStore}, under {@code rowPos/} of the table's remote lake
 * directory; no other class knows these paths.
 *
 * <pre>
 * rowPos/{changeId}/[{partitionId}/]rowpos.manifest     manifest (version 2) per partition
 * rowPos/{changeId}/[{partitionId}/]{bucket}/sst_N.sst  key -> varint(fileId)||varint(pos)
 * rowPos/{changeId}/_STARTED                            when the change was first written
 * rowPos/{changeId}/_COMMITTED                          the change's partitions; written last
 * rowPos/_chain/{sequence}_{parent}_{snapshot}_{changeId}_{maxFileId}  one per registered change
 * rowPos/_trimmed                                       present once the chain was trimmed
 * rowPos/_next_file_id                                  file-id counter, written ahead
 * rowPos/_ckpt/[{partitionId}/]{bucket}/{snapshotId}/   index.sst, filedict.txt, _DONE (last)
 * </pre>
 *
 * <p>The chain is ordered by sequence numbers from the single writer, not by a clock. The legacy
 * layout (no {@code _COMMITTED}, chain markers without a parent) is still read.
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
    /** Written last into a change directory: the partitions with a manifest (-1: none). */
    public static final String CHANGE_COMMITTED = "_COMMITTED";
    /** Written last into a checkpoint directory: "sst", or "empty" for an empty index. */
    public static final String CKPT_DONE = "_DONE";

    public static final String CKPT_SST = "index.sst";
    public static final String FILE_DICT = "filedict.txt";

    private static final String DONE_SST = "sst";
    private static final String DONE_EMPTY = "empty";
    /** Legacy version-1 layout: per-bucket removed files and the index.json. */
    private static final String LEGACY_REPLACED_FILES = "replaced.txt";

    private static final String LEGACY_INDEX = "index.json";
    private static final int CHECKPOINTS_KEPT = 2;
    private static final int MAX_ENTRIES_PER_SST = RowPosSstFileWriter.MAX_ENTRIES_PER_SST;

    private final FsPath lakeTableDir;

    public FlussPositionIndexStore(FsPath lakeTableDir) {
        this.lakeTableDir = lakeTableDir;
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
        // Not cached: another writer may have moved it. Racing writers can still get the same ids,
        // and register() refuses the second.
        FsPath counter = new FsPath(lakeTableDir, NEXT_FILE_ID);
        String s = readText(counter);
        int first = s == null ? 0 : Integer.parseInt(s.trim());
        // Written ahead, so a restart never hands out an id twice.
        writeText(counter, String.valueOf(first + count));
        return first;
    }

    @Override
    public BucketPart writeBucket(
            long changeId, TableBucket bucket, Iterator<Map.Entry<byte[], FilePos>> entries)
            throws IOException {
        markStarted(changeId);
        File tmp = Files.createTempDirectory("fluss-pkidx-").toFile();
        try {
            List<RowPosSstIndex.SstFileEntry> files = writeSsts(tmp, entries);
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
        writeText(committed, partitions.toString());
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
    public void register(long parentSnapshotId, long snapshotId, long changeId) throws IOException {
        if (!exists(new FsPath(changeDir(changeId), CHANGE_COMMITTED))) {
            throw new IllegalStateException(
                    "Index change " + changeId + " is not committed; cannot register it.");
        }
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
        long tail = chain.isEmpty() ? -1L : chain.get(chain.size() - 1).snapshotId;
        if (parentSnapshotId != tail) {
            throw new IllegalStateException(
                    String.format(
                            "The position index chain ends at snapshot %d; the change %d -> %d"
                                    + " does not apply to it.",
                            tail, parentSnapshotId, snapshotId));
        }
        // New file ids must be above every registered id, or an id handed out twice would let one
        // file's positions mask another's.
        int[] ids = fileIdRange(changeId);
        long sequence = 1;
        long registeredMax = -1;
        for (String name : markerNames()) {
            sequence = Math.max(sequence, parseMarker(name).sequence + 1);
            registeredMax = Math.max(registeredMax, markerMaxFileId(name));
        }
        if (ids != null && ids[0] <= registeredMax) {
            throw new IllegalStateException(
                    String.format(
                            "Index change %d uses file ids %d..%d, but ids up to %d are registered"
                                    + " already (another tiering writer?).",
                            changeId, ids[0], ids[1], registeredMax));
        }
        writeText(
                new FsPath(
                        lakeTableDir,
                        String.format(
                                "%s/%020d_%d_%d_%d_%d",
                                CHAIN_DIR,
                                sequence,
                                parentSnapshotId,
                                snapshotId,
                                changeId,
                                ids == null ? -1 : ids[1])),
                "");
    }

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
            // Legacy changes have no start marker and are kept.
            String started = readText(new FsPath(changeDir(changeId), CHANGE_STARTED));
            if (started != null && now - Long.parseLong(started.trim()) >= minAgeMillis) {
                deleteChange(changeId);
                deleted++;
            }
        }
        return deleted;
    }

    @Override
    public int trim(Collection<TableBucket> bucketsWithData, int keepExtra) throws IOException {
        List<ChainEntry> chain = chain();
        if (bucketsWithData.isEmpty() || chain.isEmpty()) {
            return 0;
        }
        int oldestCheckpoint = Integer.MAX_VALUE;
        for (TableBucket tb : bucketsWithData) {
            Checkpoint cp = latestCheckpoint(tb, chain, chain.size() - 1);
            if (cp == null) {
                return 0; // this bucket can only be rebuilt from the whole chain
            }
            oldestCheckpoint = Math.min(oldestCheckpoint, cp.chainIndex);
        }
        int cut = oldestCheckpoint - keepExtra;
        if (cut <= 0) {
            return 0;
        }
        Set<Long> keptChanges = new HashSet<>();
        for (int i = cut; i < chain.size(); i++) {
            keptChanges.add(chain.get(i).changeId);
        }
        // Mark the chain trimmed before deleting anything, so no rebuild starts at its beginning.
        writeText(
                new FsPath(lakeTableDir, CHAIN_TRIMMED), String.valueOf(chain.get(cut).snapshotId));
        Set<Long> trimmedSnapshots = new HashSet<>();
        for (int i = 0; i < cut; i++) {
            trimmedSnapshots.add(chain.get(i).snapshotId);
            if (!keptChanges.contains(chain.get(i).changeId)) {
                deleteChange(chain.get(i).changeId);
            }
        }
        FsPath chainDir = new FsPath(lakeTableDir, CHAIN_DIR);
        for (String name : markerNames()) {
            if (trimmedSnapshots.contains(parseMarker(name).snapshotId)) {
                chainDir.getFileSystem().delete(new FsPath(chainDir, name), false);
            }
        }
        return cut;
    }

    // ------------------------------------------------------------------------------------------
    // Reading
    // ------------------------------------------------------------------------------------------

    @Override
    public List<ChainEntry> chain() throws IOException {
        List<ChainEntry> chain = new ArrayList<>();
        Set<Long> seen = new HashSet<>();
        for (ChainEntry e : rawChain()) {
            // Legacy chains may hold a repaired round's second marker; the first one counts.
            if (seen.add(e.snapshotId)) {
                chain.add(e);
            }
        }
        for (int i = 1; i < chain.size(); i++) {
            ChainEntry e = chain.get(i);
            if (e.parentSnapshotId != UNKNOWN_PARENT
                    && e.parentSnapshotId != chain.get(i - 1).snapshotId) {
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
        String partitions = readText(new FsPath(changeDir(entry.changeId), CHANGE_COMMITTED));
        Long pid = bucket.getPartitionId();
        RowPosSstIndex index;
        boolean legacyFiles = false;
        if (partitions != null) {
            if (!lines(partitions).contains(String.valueOf(partitionKey(bucket)))) {
                return BucketData.EMPTY; // the change does not touch this partition
            }
            // Throws if the manifest is gone.
            index = RowPosSstIndex.fromJsonBytes(readBytes(manifestPath(entry.changeId, pid)));
        } else if (entry.parentSnapshotId == UNKNOWN_PARENT) {
            // Legacy change without a commit marker: a missing manifest means untouched.
            FsPath manifest = manifestPath(entry.changeId, pid);
            FsPath v1 = new FsPath(changeDir(entry.changeId, pid), LEGACY_INDEX);
            if (exists(manifest)) {
                index = RowPosSstIndex.fromJsonBytes(readBytes(manifest));
            } else if (exists(v1)) {
                index = RowPosSstIndex.fromJsonBytes(readBytes(v1));
                legacyFiles = true;
            } else {
                return BucketData.EMPTY;
            }
        } else {
            throw new IOException(
                    "Index change of " + entry + " is gone (trimmed, or never committed).");
        }
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
        if (legacyFiles) {
            return new BucketData(
                    local,
                    readDict(readText(new FsPath(bucketDir, FILE_DICT))),
                    lines(readText(new FsPath(bucketDir, LEGACY_REPLACED_FILES))));
        }
        return new BucketData(
                local,
                new HashMap<>(index.getFileId2Name(b)),
                new ArrayList<>(index.getReplacedFiles(b)));
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
        for (int i = 0; i <= upTo && i < chain.size(); i++) {
            indexOf.put(chain.get(i).snapshotId, i);
        }
        Checkpoint best = null;
        for (FileStatus st : fs.listStatus(dir)) {
            Long snapshotId = parseLong(st.getPath().getName());
            Integer idx = snapshotId == null ? null : indexOf.get(snapshotId);
            if (idx != null
                    && (best == null || idx > best.chainIndex)
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
        String done = readText(new FsPath(dir, CKPT_DONE));
        String dict = readText(new FsPath(dir, FILE_DICT));
        if (done == null || dict == null) {
            throw new IOException("Index " + checkpoint + " of " + bucket + " is gone.");
        }
        FsPath sst = new FsPath(dir, CKPT_SST);
        boolean hasSst =
                DONE_SST.equals(done.trim())
                        // legacy empty _DONE: the SST exists unless the index was empty
                        || (done.trim().isEmpty() && exists(sst));
        List<String> local = Collections.emptyList();
        if (hasSst) {
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
        writeText(new FsPath(dir, CKPT_DONE), localSst != null ? DONE_SST : DONE_EMPTY);
        // Keep the newest by chain order; drop those not in the chain or newer than this one.
        List<ChainEntry> chain = chain();
        Map<Long, Integer> indexOf = new HashMap<>();
        for (int i = 0; i < chain.size(); i++) {
            indexOf.put(chain.get(i).snapshotId, i);
        }
        int newest = indexOf.getOrDefault(snapshotId, Integer.MAX_VALUE);
        List<long[]> others = new ArrayList<>();
        for (FileStatus st : bucketDir.getFileSystem().listStatus(bucketDir)) {
            Long id = parseLong(st.getPath().getName());
            if (id != null && id != snapshotId) {
                others.add(new long[] {indexOf.getOrDefault(id, -1), id});
            }
        }
        others.sort((a, b) -> Long.compare(b[0], a[0]));
        for (int i = 0; i < others.size(); i++) {
            long idx = others.get(i)[0];
            if (i >= CHECKPOINTS_KEPT - 1 || idx < 0 || idx > newest) {
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
    }

    private void markStarted(long changeId) throws IOException {
        FsPath started = new FsPath(changeDir(changeId), CHANGE_STARTED);
        if (!exists(started)) {
            writeText(started, String.valueOf(System.currentTimeMillis()));
        }
    }

    /** {min, max} of the file ids a committed change adds, or null if it adds none. */
    @Nullable
    private int[] fileIdRange(long changeId) throws IOException {
        int[] range = null;
        for (String pid : lines(readText(new FsPath(changeDir(changeId), CHANGE_COMMITTED)))) {
            long p = Long.parseLong(pid);
            RowPosSstIndex index =
                    RowPosSstIndex.fromJsonBytes(
                            readBytes(manifestPath(changeId, p == -1L ? null : p)));
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

    /** The largest file id a marker's change added (-1: none, or a marker without the field). */
    private static long markerMaxFileId(String name) {
        String[] f = name.split("_");
        return f.length > 4 ? Long.parseLong(f[4]) : -1L;
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

    /** Every marker in sequence order, duplicates of legacy chains included. */
    private List<ChainEntry> rawChain() throws IOException {
        List<ChainEntry> entries = new ArrayList<>();
        for (String name : markerNames()) {
            entries.add(parseMarker(name));
        }
        entries.sort((a, b) -> Long.compare(a.sequence, b.sequence));
        return entries;
    }

    static ChainEntry parseMarker(String name) {
        if (name.indexOf('_') >= 0) {
            String[] f = name.split("_");
            return new ChainEntry(
                    Long.parseLong(f[0]),
                    Long.parseLong(f[1]),
                    Long.parseLong(f[2]),
                    Long.parseLong(f[3]));
        }
        // Legacy marker {commit millis}-{snapshot}[-{changeId}], without a parent.
        String[] f = name.split("-");
        long snapshotId = Long.parseLong(f[1]);
        return new ChainEntry(
                Long.parseLong(f[0]),
                UNKNOWN_PARENT,
                snapshotId,
                f.length > 2 ? Long.parseLong(f[2]) : snapshotId);
    }

    private static List<RowPosSstIndex.SstFileEntry> writeSsts(
            File dir, Iterator<Map.Entry<byte[], FilePos>> entries) throws IOException {
        List<RowPosSstIndex.SstFileEntry> out = new ArrayList<>();
        int n = 0;
        SstFileWriter writer = null;
        File file = null;
        try (EnvOptions envOptions = new EnvOptions();
                Options options = new Options()) {
            while (entries.hasNext()) {
                Map.Entry<byte[], FilePos> e = entries.next();
                if (writer == null) {
                    file = new File(dir, "sst_" + out.size() + ".sst");
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
        try (FSDataOutputStream out =
                path.getFileSystem().create(path, FileSystem.WriteMode.OVERWRITE)) {
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
