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

import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.FormatVersions;
import org.apache.fluss.lake.dv.index.PositionIndexStore.BucketData;
import org.apache.fluss.lake.dv.index.PositionIndexStore.ChainEntry;
import org.apache.fluss.lake.dv.index.PositionIndexStore.Checkpoint;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.fluss.lake.dv.index.IndexFiles.download;
import static org.apache.fluss.lake.dv.index.IndexFiles.lines;
import static org.apache.fluss.lake.dv.index.IndexFiles.parseInteger;
import static org.apache.fluss.lake.dv.index.IndexFiles.parseLong;
import static org.apache.fluss.lake.dv.index.IndexFiles.readDict;
import static org.apache.fluss.lake.dv.index.IndexFiles.readText;
import static org.apache.fluss.lake.dv.index.IndexFiles.upload;
import static org.apache.fluss.lake.dv.index.IndexFiles.writeBytes;
import static org.apache.fluss.lake.dv.index.IndexFiles.writeText;
import static org.apache.fluss.lake.dv.index.IndexLayout.CHECKPOINT_VERSION;
import static org.apache.fluss.lake.dv.index.IndexLayout.CKPT_DONE;
import static org.apache.fluss.lake.dv.index.IndexLayout.CKPT_SST;
import static org.apache.fluss.lake.dv.index.IndexLayout.DONE_FILE;
import static org.apache.fluss.lake.dv.index.IndexLayout.FILE_DICT;
import static org.apache.fluss.lake.dv.index.IndexLayout.checkpointName;
import static org.apache.fluss.lake.dv.index.IndexLayout.checkpointSnapshot;

/**
 * The checkpoints of the {@link FlussPositionIndexStore}: one bucket's full index at a chain entry.
 * Tablet servers write them, and the trim writes them for buckets whose servers do not.
 *
 * <p>Each writer attempt writes a directory of its own, {@code {snapshot}_{attempt}}, and its
 * {@code _DONE} last, listing the files and their sizes. Writers only add checkpoints. The trim,
 * which the table's one tiering service runs, is the only thing that deletes them, and it deletes
 * only checkpoints it listed: see {@link IndexTrimmer}. A directory is deleted {@code _DONE} first,
 * so a reader fails instead of reading part of it.
 */
final class IndexCheckpoints {

    // A checkpoint directory without _DONE may still be uploading, and one of a snapshot not on the
    // chain may be newer than a listing of the chain; either is deleted only once this old.
    static final long STALE_MILLIS = 6 * 60 * 60 * 1000L;

    private final IndexLayout layout;
    // the store's hooks: deletes
    private final FlussPositionIndexStore store;

    IndexCheckpoints(IndexLayout layout, FlussPositionIndexStore store) {
        this.layout = layout;
        this.store = store;
    }

    /** A checkpoint directory of a bucket, as listed. */
    static final class Listed {
        final FsPath dir;
        final String name;
        final long snapshotId;
        // _DONE is there, of a version this one reads, and every file it lists is there whole
        final boolean complete;
        // _DONE is of a newer version: neither used nor deleted
        final boolean foreign;
        // the latest modification of the directory or a file in it
        final long lastModified;

        Listed(
                FsPath dir,
                String name,
                long snapshotId,
                boolean complete,
                boolean foreign,
                long lastModified) {
            this.dir = dir;
            this.name = name;
            this.snapshotId = snapshotId;
            this.complete = complete;
            this.foreign = foreign;
            this.lastModified = lastModified;
        }
    }

    /**
     * Lists the bucket's checkpoint directories. Throws on a directory of the layout before
     * per-attempt directories, which this version does not read: the table's {@code rowPos/} was
     * written by an older build and must be cleared.
     */
    List<Listed> list(TableBucket bucket) throws IOException {
        FsPath bucketDir = layout.checkpointDir(bucket);
        FileSystem fs = bucketDir.getFileSystem();
        if (!fs.exists(bucketDir)) {
            return Collections.emptyList();
        }
        List<Listed> out = new ArrayList<>();
        for (FileStatus st : fs.listStatus(bucketDir)) {
            String name = st.getPath().getName();
            Long snapshotId = checkpointSnapshot(name);
            if (snapshotId == null) {
                if (parseLong(name) != null) {
                    throw new IllegalStateException(
                            String.format(
                                    "The position index checkpoint %s is of a layout this Fluss"
                                            + " version does not read. Delete %s and let the"
                                            + " tiering service index the table again.",
                                    st.getPath(), layout.path(IndexLayout.ROW_POS_DIR)));
                }
                continue;
            }
            FsPath dir = st.getPath();
            Map<String, Long> sizes = new HashMap<>();
            long lastModified = st.getModificationTime();
            FileStatus[] children;
            try {
                children = fs.listStatus(dir);
            } catch (IOException e) {
                continue; // deleted meanwhile
            }
            for (FileStatus f : children) {
                sizes.put(f.getPath().getName(), f.getLen());
                lastModified = Math.max(lastModified, f.getModificationTime());
            }
            boolean complete = false;
            boolean foreign = false;
            if (sizes.containsKey(CKPT_DONE)) {
                String done = readText(new FsPath(dir, CKPT_DONE));
                Integer version = versionOf(done);
                foreign = version != null && version > CHECKPOINT_VERSION;
                complete =
                        version != null
                                && version <= CHECKPOINT_VERSION
                                && filesThere(doneFiles(done), sizes);
            }
            out.add(new Listed(dir, name, snapshotId, complete, foreign, lastModified));
        }
        return out;
    }

    /** The version a text file's first line names, or null if it has none (or is gone). */
    @Nullable
    private static Integer versionOf(@Nullable String text) {
        if (text == null) {
            return null;
        }
        int end = text.indexOf('\n');
        String header = end < 0 ? text : text.substring(0, end);
        return header.length() > 1 && header.charAt(0) == 'v'
                ? parseInteger(header.substring(1))
                : null;
    }

    /** The files a {@code _DONE} of this version lists, with their sizes. */
    private static Map<String, Long> doneFiles(String done) {
        Map<String, Long> files = new HashMap<>();
        for (String line : lines(done)) {
            if (line.startsWith(DONE_FILE)) {
                String[] f = line.substring(DONE_FILE.length()).split(" ");
                files.put(f[0], Long.parseLong(f[1]));
            }
        }
        return files;
    }

    private static boolean filesThere(Map<String, Long> listed, Map<String, Long> sizes) {
        for (Map.Entry<String, Long> f : listed.entrySet()) {
            Long size = sizes.get(f.getKey());
            if (size == null || !size.equals(f.getValue())) {
                return false;
            }
        }
        return !listed.isEmpty();
    }

    /**
     * The newest complete checkpoint among {@code listed} whose snapshot is on the chain at an
     * index {@code indexOf} gives, or null if none.
     */
    @Nullable
    static Checkpoint newest(List<Listed> listed, Map<Long, Integer> indexOf) {
        Listed best = null;
        int bestIndex = -1;
        for (Listed l : listed) {
            Integer idx = l.complete ? indexOf.get(l.snapshotId) : null;
            if (idx == null) {
                continue;
            }
            if (best == null
                    || idx > bestIndex
                    || (idx == bestIndex && l.name.compareTo(best.name) > 0)) {
                best = l;
                bestIndex = idx;
            }
        }
        return best == null ? null : new Checkpoint(bestIndex, best.snapshotId, best.name);
    }

    /** See {@link PositionIndexStore#latestCheckpoint}. */
    @Nullable
    Checkpoint latestCheckpoint(TableBucket bucket, List<ChainEntry> chain, int upTo)
            throws IOException {
        Map<Long, Integer> indexOf = new HashMap<>();
        for (int i = 0; i <= upTo && i < chain.size(); i++) {
            indexOf.put(chain.get(i).snapshotId, i);
        }
        return newest(list(bucket), indexOf);
    }

    /** See {@link PositionIndexStore#readCheckpoint}. */
    BucketData readCheckpoint(Checkpoint checkpoint, TableBucket bucket, File localDir)
            throws IOException {
        FsPath dir = directoryOf(checkpoint, bucket);
        if (dir == null) {
            throw new IOException("Index " + checkpoint + " of " + bucket + " is gone.");
        }
        FsPath donePath = new FsPath(dir, CKPT_DONE);
        String done = readText(donePath);
        String dict = readText(new FsPath(dir, FILE_DICT));
        if (done == null || dict == null) {
            throw new IOException("Index " + checkpoint + " of " + bucket + " is gone.");
        }
        Map<String, Long> files =
                doneFiles(FormatVersions.withoutVersion(donePath, done, CHECKPOINT_VERSION));
        checkSize(dir, FILE_DICT, files, dict.getBytes(StandardCharsets.UTF_8).length);
        List<String> local = Collections.emptyList();
        if (files.containsKey(CKPT_SST)) {
            Files.createDirectories(localDir.toPath());
            File target = new File(localDir, CKPT_SST);
            download(new FsPath(dir, CKPT_SST), target); // throws if gone
            checkSize(dir, CKPT_SST, files, target.length());
            local = Collections.singletonList(target.getPath());
        }
        return new BucketData(local, readDict(dict), Collections.emptyList());
    }

    private static void checkSize(FsPath dir, String file, Map<String, Long> files, long size)
            throws IOException {
        Long listed = files.get(file);
        if (listed == null || listed != size) {
            throw new IOException(
                    String.format(
                            "%s/%s has %d bytes, but its _DONE lists %s.",
                            dir, file, size, listed));
        }
    }

    /**
     * The directory of a checkpoint: the one {@link #latestCheckpoint} chose, or else the newest
     * complete one of its snapshot, or one of a newer version so that reading it fails on that.
     */
    @Nullable
    private FsPath directoryOf(Checkpoint checkpoint, TableBucket bucket) throws IOException {
        if (checkpoint.directory != null) {
            return new FsPath(layout.checkpointDir(bucket), checkpoint.directory);
        }
        Listed best = null;
        for (Listed l : list(bucket)) {
            if (l.snapshotId != checkpoint.snapshotId || !(l.complete || l.foreign)) {
                continue;
            }
            if (best == null
                    || (l.complete && !best.complete)
                    || (l.complete == best.complete && l.name.compareTo(best.name) > 0)) {
                best = l;
            }
        }
        return best == null ? null : best.dir;
    }

    /**
     * Writes a checkpoint into a directory of its own and returns the directory's name. It deletes
     * nothing: the trim drops the checkpoints that newer ones make redundant.
     */
    String writeCheckpoint(
            long snapshotId,
            TableBucket bucket,
            @Nullable File localSst,
            Map<Integer, String> fileId2Name)
            throws IOException {
        String name =
                checkpointName(
                        snapshotId, UUID.randomUUID().toString().replace("-", "").substring(0, 16));
        FsPath dir = new FsPath(layout.checkpointDir(bucket), name);
        StringBuilder done = new StringBuilder();
        if (localSst != null) {
            upload(localSst, new FsPath(dir, CKPT_SST));
            done.append(DONE_FILE)
                    .append(CKPT_SST)
                    .append(' ')
                    .append(localSst.length())
                    .append('\n');
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Integer, String> e : fileId2Name.entrySet()) {
            sb.append(e.getKey()).append('\t').append(e.getValue()).append('\n');
        }
        byte[] dict = sb.toString().getBytes(StandardCharsets.UTF_8);
        writeBytes(new FsPath(dir, FILE_DICT), dict);
        done.append(DONE_FILE).append(FILE_DICT).append(' ').append(dict.length).append('\n');
        writeText(
                new FsPath(dir, CKPT_DONE),
                FormatVersions.withVersion(CHECKPOINT_VERSION, done.toString()));
        return name;
    }

    /**
     * Deletes the bucket's checkpoints the trim listed that the kept one makes redundant: complete
     * ones of snapshots below the cut on the trim's chain, and, once {@link #STALE_MILLIS} old,
     * incomplete ones and complete ones of snapshots not on it. Never the kept one, nor one of a
     * newer version. Returns how many it deleted.
     */
    int deleteLeftBehind(
            List<Listed> listed,
            Map<Long, Integer> indexOf,
            int cut,
            @Nullable Checkpoint kept,
            long now)
            throws IOException {
        int deleted = 0;
        for (Listed l : listed) {
            if (l.foreign || (kept != null && l.name.equals(kept.directory))) {
                continue;
            }
            Integer idx = indexOf.get(l.snapshotId);
            boolean stale = now - l.lastModified >= STALE_MILLIS;
            boolean drop = l.complete ? (idx != null ? idx < cut : stale) : stale;
            if (drop) {
                // _DONE first, so a reader fails instead of reading part of the checkpoint.
                store.deleteOrThrow(new FsPath(l.dir, CKPT_DONE), false);
                store.deleteOrThrow(l.dir, true);
                deleted++;
            }
        }
        return deleted;
    }

    /** The partitions with a directory under {@code _ckpt/}: those of a partitioned table. */
    List<Long> partitions() throws IOException {
        FsPath dir = layout.path(IndexLayout.CKPT_DIR);
        FileSystem fs = dir.getFileSystem();
        if (!fs.exists(dir)) {
            return Collections.emptyList();
        }
        List<Long> out = new ArrayList<>();
        for (FileStatus st : fs.listStatus(dir)) {
            Long partitionId = parseLong(st.getPath().getName());
            if (partitionId != null) {
                out.add(partitionId);
            }
        }
        return out;
    }

    /**
     * Deletes the checkpoints of a dropped partition, each {@code _DONE} first: the checkpoint
     * directories listed here, then the bucket and partition directories once empty, best effort.
     * One a server still writes after the listing stays for a later trim. Returns how many it
     * deleted.
     */
    int deletePartition(long partitionId) throws IOException {
        FsPath partitionDir =
                new FsPath(layout.path(IndexLayout.CKPT_DIR), String.valueOf(partitionId));
        FileSystem fs = partitionDir.getFileSystem();
        if (!fs.exists(partitionDir)) {
            return 0;
        }
        int deleted = 0;
        for (FileStatus bucket : fs.listStatus(partitionDir)) {
            if (parseInteger(bucket.getPath().getName()) == null) {
                continue;
            }
            FileStatus[] dirs;
            try {
                dirs = fs.listStatus(bucket.getPath());
            } catch (IOException e) {
                continue; // deleted meanwhile
            }
            for (FileStatus st : dirs) {
                if (checkpointSnapshot(st.getPath().getName()) == null) {
                    continue;
                }
                store.deleteOrThrow(new FsPath(st.getPath(), CKPT_DONE), false);
                store.deleteOrThrow(st.getPath(), true);
                deleted++;
            }
            deleteIfEmpty(bucket.getPath());
        }
        deleteIfEmpty(partitionDir);
        return deleted;
    }

    private void deleteIfEmpty(FsPath dir) {
        try {
            store.deletePath(dir, false);
        } catch (IOException e) {
            // not empty: something written after the listing, for a later trim
        }
    }
}
