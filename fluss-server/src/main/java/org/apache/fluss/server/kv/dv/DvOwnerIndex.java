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

import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.server.kv.dv.DerivedDvManager.RecoverySource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * The covered index's writes that a bucket's owner ({@link DvOwner}) makes: the file dictionary
 * batches of a swap, a load's ingest, and the checkpoints exported every few chain entries. Exports
 * run on the server's IO pool, through a retained version; the rest are owner steps or a load's
 * job.
 */
final class DvOwnerIndex {

    private static final Logger LOG = LoggerFactory.getLogger(DvOwnerIndex.class);

    private final DvOwner o;

    DvOwnerIndex(DvOwner o) {
        this.o = o;
    }

    long entriesAtCheckpoint = 0;
    boolean checkpointing = false;
    // delays the start of checkpoints: the server's scheduler, or a test's
    volatile ScheduledExecutorService checkpointTimer;
    // checkpoints of the buckets that catch up to one snapshot together start apart
    static final long CHECKPOINT_JITTER_MS = 10_000;
    // volatile as tests set it
    volatile long checkpointJitterMs = CHECKPOINT_JITTER_MS;

    /**
     * Starts an export of the covered index every checkpointInterval chain entries, after a random
     * delay, so the buckets that catch up to one snapshot together do not export at once.
     */
    void maybeCheckpoint() {
        if (o.recoverySource == null
                || o.closed
                || checkpointing
                || o.coveredSnapshotId < 0
                || o.entriesApplied - entriesAtCheckpoint < o.checkpointInterval) {
            return;
        }
        checkpointing = true;
        long delayMs =
                checkpointJitterMs <= 0
                        ? 0
                        : ThreadLocalRandom.current().nextLong(checkpointJitterMs);
        try {
            checkpointTimer.schedule(this::startCheckpoint, delayMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            // e.g. the executor shuts down
            checkpointing = false;
            LOG.warn(
                    "The position index checkpoint at snapshot {} did not start ({}); it"
                            + " is tried again after the next index change.",
                    o.coveredSnapshotId,
                    e.toString());
        }
    }

    /**
     * Retains the covered version when a checkpoint's export starts, and runs the export. The
     * export has the version the bucket covers at that point, which may be newer than when the
     * checkpoint was due, so a bucket that advances more often than the delay still checkpoints. A
     * version is the index of exactly its snapshot, whatever a load ingests meanwhile.
     */
    void startCheckpoint() {
        o.post(
                () -> {
                    RecoverySource src = o.recoverySource;
                    // The version holds a lease of the RocksDB until it is released, so a
                    // closing tablet waits for the export, which checks closed as it goes.
                    DvVersion view =
                            src == null || o.version.snapshotId < 0 ? null : o.retainForJob();
                    if (view == null) {
                        checkpointing = false;
                        return;
                    }
                    try {
                        // the export runs on the server's IO pool, after the delay a test may hold
                        o.runtime.checkpoints().execute(() -> checkpoint(src, view));
                    } catch (RejectedExecutionException e) {
                        view.release();
                        checkpointing = false;
                        LOG.warn(
                                "The position index checkpoint at snapshot {} did not start ({}).",
                                view.snapshotId,
                                e.toString());
                    }
                });
    }

    /** Exports and uploads a checkpoint of the index of {@code view}, then releases it. */
    void checkpoint(RecoverySource src, DvVersion view) {
        long snapshotId = view.snapshotId;
        long applied = view.entriesApplied;
        long t0 = System.currentTimeMillis();
        File tmp = null;
        Throwable error = null;
        try {
            Map<Integer, String> dict;
            long n;
            try {
                if (o.closed) {
                    return;
                }
                // on the tablet's disk, which is sized for the index, not the JVM's temp dir
                tmp = Files.createTempFile(o.db.tmpDir().toPath(), "ckpt-", ".sst").toFile();
                tmp.delete();
                dict = view.files();
                // Leaves out entries of dropped files; a closing tablet stops the export.
                n =
                        o.db.rowPosIndex()
                                .exportTo(
                                        tmp.getPath(),
                                        view.snapshot(),
                                        dict::containsKey,
                                        () -> o.closed);
            } finally {
                // Released before the upload, which can take long on an object store and needs
                // only the exported file.
                view.release();
            }
            if (o.closed) {
                return;
            }
            src.uploadCheckpoint(snapshotId, n > 0 ? tmp.getPath() : null, dict);
            o.post(() -> entriesAtCheckpoint = Math.max(entriesAtCheckpoint, applied));
            o.metrics.checkpointed(System.currentTimeMillis() - t0);
            LOG.info(
                    "Checkpointed the position index at snapshot {} ({} entries, {} files)"
                            + " in {} ms.",
                    snapshotId,
                    n,
                    dict.size(),
                    System.currentTimeMillis() - t0);
        } catch (Throwable e) {
            if (!(e instanceof Exception)) {
                o.faults.jobError(e, "an index checkpoint");
                error = e;
            } else if (o.closed) {
                LOG.info(
                        "The position index checkpoint at snapshot {} stopped as the bucket"
                                + " closed.",
                        snapshotId);
            } else {
                LOG.warn(
                        "The position index checkpoint at snapshot {} failed; it is tried"
                                + " again after the next index change.",
                        snapshotId,
                        e);
            }
        } finally {
            if (tmp != null) {
                tmp.delete();
            }
            o.post(() -> checkpointEnded(applied));
        }
        if (error != null) {
            // after the checkpoint is ended, as on every job's thread
            DvOwnerFaults.rethrowIfFatal(error);
        }
    }

    /**
     * A checkpoint of the index at {@code exported} chain entries ended, done or not. A newer
     * version a switch covered meanwhile skipped its own checkpoint, so it starts one now if it is
     * due.
     */
    private void checkpointEnded(long exported) {
        checkpointing = false;
        if (o.entriesApplied > exported) {
            maybeCheckpoint();
        }
    }

    /**
     * Removes files that left the lake snapshot from the dictionary in one write, so either all or
     * none are gone, and returns their ids. File ids are never reused; checkpoints leave out their
     * stale index entries.
     */
    Set<Integer> dropFiles(List<String> fileNames) throws IOException {
        for (String name : fileNames) {
            o.testHooks.beforeFileDrop(name);
        }
        return new HashSet<>(o.db.fileDict().deleteAll(fileNames));
    }

    /**
     * Keeps in the dictionary only the files of the snapshot a full rebuild loaded from {@code
     * index}, a checkpoint and the changes after it, in one write: what the bucket covered before
     * may name files the snapshot no longer has.
     */
    void retainFilesOf(List<PositionIndexStore.BucketData> index) throws IOException {
        Set<Integer> keep = new HashSet<>();
        for (PositionIndexStore.BucketData d : index) {
            keep.addAll(d.fileId2Name.keySet());
            for (String name : d.removedFiles) {
                keep.remove(o.db.fileDict().getFileId(name));
            }
        }
        o.db.fileDict().retainOnly(keep);
    }

    /**
     * Whether a full load may ingest its checkpoint and changes in one call ({@link
     * #ingestInOrder}), rather than one by one.
     *
     * <p>A full load applies them in chain order onto a cleared index, and one by one only a
     * rewrite's filter could differ from one call: it keeps a key's position where that position is
     * in a file newer than every file the rewrite adds, and is not a rewrite input. A position of
     * an earlier entry is in a file that entry uses ({@link
     * PositionIndexStore.BucketData#fileId2Name}). Registration hands out file ids above every
     * registered one, so a rewrite that adds a file adds one newer than every earlier entry's, and
     * its filter keeps no position. This checks that, per bucket: if a rewrite's newest file is not
     * newer than every file of the entries before it, the load ingests one by one.
     */
    static boolean mergesInOrder(List<PositionIndexStore.BucketData> downloads) {
        int newestBefore = -1;
        for (PositionIndexStore.BucketData d : downloads) {
            int newest = -1;
            for (int id : d.fileId2Name.keySet()) {
                newest = Math.max(newest, id);
            }
            if (!d.removedFiles.isEmpty() && !d.localSsts.isEmpty() && newest < newestBefore) {
                return false;
            }
            newestBefore = Math.max(newestBefore, newest);
        }
        return true;
    }

    /**
     * Ingests what a full load downloaded in one call ({@link #mergesInOrder}): adds the files of
     * each download to the dictionary in order, as {@link #ingest} does, then ingests all their
     * SSTs at once, each over the ones before it where they name the same key.
     */
    void ingestInOrder(List<PositionIndexStore.BucketData> downloads) throws IOException {
        List<String> ssts = new ArrayList<>();
        for (PositionIndexStore.BucketData d : downloads) {
            for (Map.Entry<Integer, String> e : d.fileId2Name.entrySet()) {
                o.db.fileDict().put(e.getKey(), e.getValue());
            }
            ssts.addAll(d.localSsts);
        }
        if (!ssts.isEmpty()) {
            o.db.rowPosIndex().ingestExternalFile(ssts);
        }
    }

    /**
     * Ingests an index change or a checkpoint into the index and adds its files to the dictionary,
     * but does not drop the files it removed: readers of the covered snapshot still need them.
     * Returns the number of SSTs ingested.
     */
    int ingest(PositionIndexStore.BucketData change) throws IOException {
        Set<Integer> inputs = new HashSet<>();
        for (String name : change.removedFiles) {
            int id = o.db.fileDict().getFileId(name);
            if (id >= 0) {
                inputs.add(id);
            }
        }
        int newestAdded = -1;
        for (Map.Entry<Integer, String> e : change.fileId2Name.entrySet()) {
            o.db.fileDict().put(e.getKey(), e.getValue());
            newestAdded = Math.max(newestAdded, e.getKey());
        }
        if (!change.localSsts.isEmpty()) {
            if (change.removedFiles.isEmpty()) {
                o.db.rowPosIndex().ingestExternalFile(change.localSsts);
            } else {
                // File ids grow with every commit, so a file newer than all the rewrite added
                // comes from a later change. Any other current file may be a rewrite input: a
                // Paimon compaction also moves rows of level-0 files, which are not indexed and
                // not listed as removed.
                final int newest = newestAdded;
                long skipped =
                        o.db.rowPosIndex()
                                .ingestRewrite(
                                        change.localSsts,
                                        current ->
                                                current == null
                                                        || current.fileId() <= newest
                                                        || inputs.contains(current.fileId()),
                                        o.db.tmpDir());
                if (skipped > 0) {
                    LOG.warn(
                            "A rewrite of {} would move {} keys out of files that a later change"
                                    + " added; they keep their position. The position index changes were"
                                    + " applied out of order or twice.",
                            change.removedFiles,
                            skipped);
                }
            }
        }
        return change.localSsts.size();
    }
}
