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
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.FormatVersions;
import org.apache.fluss.lake.dv.RowPosSstIndex;
import org.apache.fluss.lake.dv.index.PositionIndexStore.BucketChange;
import org.apache.fluss.lake.dv.index.PositionIndexStore.BucketPart;
import org.apache.fluss.lake.dv.index.PositionIndexStore.ChainEntry;
import org.apache.fluss.lake.dv.index.PositionIndexStore.KeyConsumer;
import org.apache.fluss.lake.dv.index.PositionIndexStore.ReindexPlan;
import org.apache.fluss.lake.dv.index.PositionIndexStore.StagedPartWriter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.FileUtils;

import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.ReadOptions;
import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.RocksDBException;
import io.github.fluss_contrib.rocksdb.SstFileReader;
import io.github.fluss_contrib.rocksdb.SstFileReaderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.fluss.lake.dv.index.IndexFiles.download;
import static org.apache.fluss.lake.dv.index.IndexFiles.exists;
import static org.apache.fluss.lake.dv.index.IndexFiles.readBytes;
import static org.apache.fluss.lake.dv.index.IndexFiles.readText;
import static org.apache.fluss.lake.dv.index.IndexFiles.upload;
import static org.apache.fluss.lake.dv.index.IndexFiles.writeBytes;
import static org.apache.fluss.lake.dv.index.IndexFiles.writeText;
import static org.apache.fluss.lake.dv.index.IndexLayout.CHANGE_COMMITTED;
import static org.apache.fluss.lake.dv.index.IndexLayout.CHANGE_STARTED;
import static org.apache.fluss.lake.dv.index.IndexLayout.CHANGE_VERSION;
import static org.apache.fluss.lake.dv.index.IndexLayout.FILE_IDS_DIR;
import static org.apache.fluss.lake.dv.index.IndexLayout.PLAN_FORMAT_VERSION;
import static org.apache.fluss.lake.dv.index.IndexLayout.REINDEX_PLAN_DIR;
import static org.apache.fluss.lake.dv.index.IndexLayout.REMAPPED_PREFIX;
import static org.apache.fluss.lake.dv.index.IndexLayout.fileIdMarkerName;
import static org.apache.fluss.lake.dv.index.IndexLayout.fileIdMarkerValue;
import static org.apache.fluss.lake.dv.index.IndexLayout.markerMaxFileId;
import static org.apache.fluss.lake.dv.index.IndexLayout.partitionKey;
import static org.apache.fluss.lake.dv.index.IndexSsts.compareUnsigned;
import static org.apache.fluss.lake.dv.index.IndexSsts.publishSst;
import static org.apache.fluss.lake.dv.index.IndexSsts.remapSst;
import static org.apache.fluss.lake.dv.index.IndexSsts.writeSsts;

/**
 * Writes the changes of the {@link FlussPositionIndexStore}: allocates file ids, stores bucket
 * parts, commits and deletes changes. Every write to a change starts with its {@code _STARTED}, and
 * {@code _COMMITTED} is written last; a change can be rewritten until it is committed. A staging
 * change is never committed: the committer publishes its parts into the round's change and deletes
 * it once the round ends; otherwise it is removed as an orphan.
 */
final class IndexChangeWriter {

    // A writer whose create of a change's start marker races another writer waits about 2.5 s for
    // that marker to land. A small object's upload takes far less.
    private static final int MARK_STARTED_ATTEMPTS = 10;
    private static final long MARK_STARTED_INITIAL_BACKOFF_MS = 20;
    private static final long MARK_STARTED_MAX_BACKOFF_MS = 500;

    // the store's logger
    private static final Logger LOG = LoggerFactory.getLogger(FlussPositionIndexStore.class);

    private final IndexLayout layout;
    // the store's hooks: deletes
    private final FlussPositionIndexStore store;
    private final IndexChain indexChain;
    // local directory for temporary files, or null for the JVM's
    @Nullable private final File localTmpDir;
    // changes whose directory's format version was checked; it never changes once written
    private final Set<Long> checkedChanges = ConcurrentHashMap.newKeySet();

    IndexChangeWriter(
            IndexLayout layout,
            FlussPositionIndexStore store,
            IndexChain indexChain,
            @Nullable File localTmpDir) {
        this.layout = layout;
        this.store = store;
        this.indexChain = indexChain;
        this.localTmpDir = localTmpDir;
    }

    int allocateFileIds(int count) throws IOException {
        // The counter is one empty marker per allocation, named after the next free id, and the
        // largest name counts. Nothing is overwritten, so a crash cannot empty or cut it, and a
        // racer's lower marker never moves it back. It is not cached: another writer may have
        // moved it. Racing writers can still get the same ids; register() refuses the second. The
        // ids also start above every registered id, for changes that add ids allocated elsewhere.
        layout.checkLayout();
        FsPath dir = layout.path(FILE_IDS_DIR);
        FileSystem fs = dir.getFileSystem();
        List<FileStatus> markers =
                fs.exists(dir) ? Arrays.asList(fs.listStatus(dir)) : Collections.emptyList();
        long first = 0;
        for (FileStatus st : markers) {
            first = Math.max(first, fileIdMarkerValue(st.getPath().getName()));
        }
        long registeredMax = -1;
        for (String name : indexChain.markerNames()) {
            registeredMax = Math.max(registeredMax, markerMaxFileId(name));
        }
        first = Math.max(first, registeredMax + 1);
        // Written ahead, so a restart never hands out an id twice.
        long next = first + count;
        writeBytes(new FsPath(dir, fileIdMarkerName(next)), new byte[0]);
        // The largest marker is the one that counts; older ones go, best effort.
        for (FileStatus st : markers) {
            if (fileIdMarkerValue(st.getPath().getName()) < next) {
                try {
                    store.deletePath(st.getPath(), false);
                } catch (IOException e) {
                    LOG.debug("Could not delete the old file-id counter {}.", st.getPath(), e);
                }
            }
        }
        return (int) first;
    }

    BucketPart remapFileIds(
            long changeId, TableBucket bucket, BucketPart part, Map<Integer, Integer> newIds)
            throws IOException {
        if (part.files.isEmpty()) {
            return part;
        }
        checkChangeVersion(changeId);
        FsPath dir = layout.bucketDir(changeId, bucket);
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

    BucketPart writeBucket(
            long changeId,
            TableBucket bucket,
            String namePrefix,
            Iterator<Map.Entry<byte[], FilePos>> entries)
            throws IOException {
        markStarted(changeId);
        File tmp = createTempDirectory("fluss-pkidx-");
        try {
            List<RowPosSstIndex.SstFileEntry> files = writeSsts(tmp, namePrefix, entries);
            FsPath dir = layout.bucketDir(changeId, bucket);
            for (RowPosSstIndex.SstFileEntry f : files) {
                upload(new File(tmp, f.getFileName()), new FsPath(dir, f.getFileName()));
            }
            return files.isEmpty() ? BucketPart.NONE : new BucketPart(files);
        } finally {
            FileUtils.deleteDirectoryQuietly(tmp);
        }
    }

    void checkDistinctKeys(
            long changeId,
            TableBucket bucket,
            List<RowPosSstIndex.SstFileEntry> ssts,
            Map<Integer, String> fileNames)
            throws IOException {
        if (ssts.size() < 2) {
            return; // a writer's SSTs hold distinct keys
        }
        checkChangeVersion(changeId);
        FsPath dir = layout.bucketDir(changeId, bucket);
        File tmp = createTempDirectory("fluss-pkidx-distinct-");
        RocksDB.loadLibrary();
        List<SstFileReader> readers = new ArrayList<>();
        List<SstFileReaderIterator> iterators = new ArrayList<>();
        try (Options options = new Options();
                ReadOptions readOptions = new ReadOptions()) {
            // A k-way merge of the SSTs by key: equal keys come out one after the other.
            PriorityQueue<SstFileReaderIterator> heads =
                    new PriorityQueue<>(ssts.size(), (a, b) -> compareUnsigned(a.key(), b.key()));
            for (RowPosSstIndex.SstFileEntry f : ssts) {
                File local = new File(tmp, f.getFileName());
                download(new FsPath(dir, f.getFileName()), local); // throws if gone
                SstFileReader reader = new SstFileReader(options);
                readers.add(reader);
                reader.open(local.getPath());
                SstFileReaderIterator it = reader.newIterator(readOptions);
                iterators.add(it);
                it.seekToFirst();
                if (it.isValid()) {
                    heads.add(it);
                } else {
                    it.status();
                }
            }
            byte[] lastKey = null;
            byte[] lastValue = null;
            while (!heads.isEmpty()) {
                SstFileReaderIterator it = heads.poll();
                byte[] key = it.key();
                byte[] value = it.value();
                if (lastKey != null && Arrays.equals(lastKey, key)) {
                    FilePos first = FilePos.decode(lastValue);
                    FilePos second = FilePos.decode(value);
                    throw new IllegalStateException(
                            String.format(
                                    "Index change %d of %s: two live rows of one key, in %s at %d"
                                            + " and in %s at %d. The lake table has duplicates.",
                                    changeId,
                                    bucket,
                                    fileNames.get(first.fileId()),
                                    first.rowPosition(),
                                    fileNames.get(second.fileId()),
                                    second.rowPosition()));
                }
                lastKey = key;
                lastValue = value;
                it.next();
                if (it.isValid()) {
                    heads.add(it);
                } else {
                    it.status();
                }
            }
        } catch (RocksDBException e) {
            throw new IOException(
                    "Failed to check the keys of index change " + changeId + " of " + bucket, e);
        } finally {
            for (SstFileReaderIterator it : iterators) {
                it.close();
            }
            for (SstFileReader reader : readers) {
                reader.close();
            }
            FileUtils.deleteDirectoryQuietly(tmp);
        }
    }

    StagedPartWriter stageBucket(
            long stagingId, TableBucket bucket, String namePrefix, @Nullable File localDir)
            throws IOException {
        markStarted(stagingId);
        File tmp =
                IndexFiles.createTempDirectory(
                        localDir == null ? localTmpDir : localDir, "fluss-pkidx-stage-");
        return new FsStagedPartWriter(tmp, layout.bucketDir(stagingId, bucket), namePrefix);
    }

    BucketPart publishStaged(
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
        FsPath source = layout.bucketDir(stagingId, bucket);
        FsPath target = layout.bucketDir(changeId, bucket);
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

    void commitChange(long changeId, Map<TableBucket, BucketChange> buckets) throws IOException {
        FsPath committed = new FsPath(layout.changeDir(changeId), CHANGE_COMMITTED);
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
                FsPath p = new FsPath(layout.bucketDir(changeId, tb), f.getFileName());
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
            FsPath manifest = layout.manifestPath(changeId, pid == -1L ? null : pid);
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
        FsPath plans = new FsPath(layout.changeDir(changeId), REINDEX_PLAN_DIR);
        try {
            plans.getFileSystem().delete(plans, true);
        } catch (IOException e) {
            // best effort
        }
    }

    String writeReindexPlan(
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
                        new FsPath(layout.changeDir(changeId), REINDEX_PLAN_DIR),
                        String.valueOf(split));
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

    void abortChange(long changeId) throws IOException {
        for (ChainEntry e : indexChain.list()) {
            if (e.changeId == changeId) {
                throw new IllegalStateException(
                        "Index change " + changeId + " is registered (" + e + "); not aborting.");
            }
        }
        deleteChange(changeId);
    }

    /**
     * A new local directory for temporary files, under the store's local directory if it has one.
     */
    private File createTempDirectory(String prefix) throws IOException {
        return IndexFiles.createTempDirectory(localTmpDir, prefix);
    }

    /** Deletes a change: its commit marker first, so a reader fails instead of reading part. */
    void deleteChange(long changeId) throws IOException {
        FsPath dir = layout.changeDir(changeId);
        FileSystem fs = dir.getFileSystem();
        if (fs.exists(dir)) {
            store.deletePath(new FsPath(dir, CHANGE_COMMITTED), false);
            store.deletePath(dir, true);
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
        FsPath started = new FsPath(layout.changeDir(changeId), CHANGE_STARTED);
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

    /**
     * Checks the format version of a change's directory before its uncommitted SSTs are read. Its
     * {@code _STARTED} is the first file written into it.
     */
    private void checkChangeVersion(long changeId) throws IOException {
        if (checkedChanges.contains(changeId)) {
            return;
        }
        FsPath started = new FsPath(layout.changeDir(changeId), CHANGE_STARTED);
        String text = readText(started);
        if (text == null) {
            throw new IOException("Index change " + changeId + ": " + started + " is missing.");
        }
        FormatVersions.withoutVersion(started, text, CHANGE_VERSION);
        checkedChanges.add(changeId);
    }
}
