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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.server.entity.DvPositionReportData;
import org.apache.fluss.server.utils.RoaringBitmapUtils;

import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Derives union-read deletion vectors from the covered lake snapshot and the high watermark: the
 * snapshot positions of keys changed since its tiered offset, and tail versions superseded by a
 * later change of the same key. A reader of an older snapshot gets a stale error and re-plans.
 */
@Internal
public class DerivedDvManager extends DvManager {

    private static final Logger LOG = LoggerFactory.getLogger(DerivedDvManager.class);
    private static final int APPLY_THRESHOLD = 100_000;

    private final DvRocksDB db;
    private final LongSupplier highWatermark;

    private long coveredSnapshotId = -1;
    // coveredSnapshotId -1 is the empty lake at offset 0, whose tail is the whole log.
    private long snapshotStartOffset = 0;
    // The empty lake's tail is built on the first union read that needs it, so an initial load
    // before the first snapshot does not grow memory.
    private boolean emptyLakeTracked = false;
    /** Default of {@code kv.deletion-vectors.checkpoint-interval}. */
    public static final int DEFAULT_CHECKPOINT_INTERVAL = 50;

    // The index is exported every checkpointInterval chain entries, so a rebuild replays only the
    // later entries and tiering can trim the chain.
    private final int checkpointInterval;
    private final DerivedDvMetrics metrics;
    private long entriesApplied = 0;
    private long entriesAtCheckpoint = 0;
    private boolean checkpointing = false;
    // Background readers of the DV RocksDB run outside the lock and must drain before it closes.
    private boolean closed = false;
    private int backgroundReaders = 0;
    private long preparedSnapshotId = -1; // index id of the last prepare
    private long preparedReadableId = -1; // readable snapshot that prepare is for
    private PositionIndexStore.BucketData pendingChange = PositionIndexStore.BucketData.EMPTY;
    private long pendingReadableOffset = -1;

    private final ArrayDeque<DvEntry> queue = new ArrayDeque<>();
    // key -> {last change record offset, live version offset or -1}
    private final HashMap<ByteBuffer, long[]> tail = new HashMap<>();
    private final Roaring64Bitmap logDv = new Roaring64Bitmap();
    private final List<ByteBuffer> unresolved = new ArrayList<>();
    private final Map<Integer, Roaring64Bitmap> lakeDv = new HashMap<>();

    // Applies queued changes once the high watermark passes them and looks up lake positions of
    // changed keys, so a union read finds the masks ready.
    private static final ScheduledExecutorService RESOLVER =
            Executors.newScheduledThreadPool(
                    4,
                    r -> {
                        Thread t = new Thread(r, "derived-dv-resolver");
                        t.setDaemon(true);
                        return t;
                    });
    private static final long RESOLVE_DELAY_MS = 2;
    private static final long RESOLVE_WAIT_HW_MS = 20;
    private boolean resolveScheduled = false;
    private int inFlight = 0;
    private long generation = 0;
    @Nullable private RecoverySource recoverySource;

    // Snapshots already moved past, so a late switch or request never moves the state backwards;
    // lake snapshot ids are not ordered, only the chain is.
    private final LinkedHashSet<Long> passed = new LinkedHashSet<>();
    private static final int PASSED_LIMIT = 4096;
    // One catch-up at a time; downloads hold this lock, not the manager's.
    private final Object advanceLock = new Object();
    private static final ScheduledExecutorService ADVANCER =
            Executors.newScheduledThreadPool(
                    2,
                    r -> {
                        Thread t = new Thread(r, "derived-dv-advance");
                        t.setDaemon(true);
                        return t;
                    });

    public DerivedDvManager(DvRocksDB dvRocksDB, DvRWLock dvRWLock, LongSupplier highWatermark) {
        this(
                dvRocksDB,
                dvRWLock,
                highWatermark,
                DEFAULT_CHECKPOINT_INTERVAL,
                DerivedDvMetrics.noop());
    }

    public DerivedDvManager(
            DvRocksDB dvRocksDB,
            DvRWLock dvRWLock,
            LongSupplier highWatermark,
            int checkpointInterval,
            DerivedDvMetrics metrics) {
        super(dvRocksDB, dvRWLock);
        this.db = dvRocksDB;
        this.highWatermark = highWatermark;
        this.checkpointInterval = checkpointInterval;
        this.metrics = metrics;
    }

    @Override
    public synchronized void handleChangelogSynced(List<DvEntry> entries) {
        if (coveredSnapshotId < 0 && recoverySource != null && !emptyLakeTracked) {
            // No reader yet; the first snapshot or union read rebuilds the tail from the log.
            return;
        }
        queue.addAll(entries);
        if (queue.size() > APPLY_THRESHOLD) {
            applyUpTo(highWatermark.getAsLong());
        }
        scheduleResolve();
    }

    private void scheduleResolve() {
        scheduleResolve(RESOLVE_DELAY_MS);
    }

    private void scheduleResolve(long delayMs) {
        if (!resolveScheduled) {
            resolveScheduled = true;
            RESOLVER.schedule(this::backgroundResolve, delayMs, TimeUnit.MILLISECONDS);
        }
    }

    private void backgroundResolve() {
        List<ByteBuffer> batch;
        long gen;
        synchronized (this) {
            resolveScheduled = false;
            if (closed) {
                return;
            }
            applyUpTo(highWatermark.getAsLong());
            if (unresolved.isEmpty()) {
                if (!queue.isEmpty()) {
                    // The high watermark has not passed the queue; back off rather than spin.
                    scheduleResolve(RESOLVE_WAIT_HW_MS);
                }
                return;
            }
            batch = new ArrayList<>(unresolved);
            unresolved.clear();
            gen = generation;
            inFlight++;
        }
        List<FilePos> found = null;
        try {
            List<byte[]> keys = new ArrayList<>(batch.size());
            for (ByteBuffer k : batch) {
                keys.add(k.array());
            }
            found = db.rowPosIndex().multiGetByKeys(keys);
        } catch (Exception e) {
            LOG.warn("Derived DV background resolve failed, reads will resolve inline", e);
        }
        synchronized (this) {
            if (gen == generation) {
                if (found == null) {
                    unresolved.addAll(batch);
                } else {
                    for (FilePos fp : found) {
                        if (fp != null) {
                            lakeDv.computeIfAbsent(fp.fileId(), k -> new Roaring64Bitmap())
                                    .addLong(fp.rowPosition());
                        }
                    }
                }
            }
            inFlight--;
            notifyAll();
            if (!queue.isEmpty() || !unresolved.isEmpty()) {
                scheduleResolve();
            }
        }
    }

    private void applyUpTo(long hw) {
        while (!queue.isEmpty() && queue.peekFirst().getRecordOffset() < hw) {
            DvEntry e = queue.pollFirst();
            applyChange(e.getKey(), e.getRecordOffset(), e.getVersionOffset());
        }
    }

    private void applyChange(byte[] keyBytes, long recordOffset, long versionOffset) {
        if (recordOffset < snapshotStartOffset) {
            return; // already in the covered snapshot
        }
        ByteBuffer key = ByteBuffer.wrap(keyBytes);
        long[] st = tail.get(key);
        if (st != null && recordOffset <= st[0]) {
            // Replay and the write path may both deliver a change; a second apply would mask its
            // own live version.
            return;
        }
        if (st == null) {
            tail.put(key, new long[] {recordOffset, versionOffset});
            if (coveredSnapshotId >= 0) {
                unresolved.add(key); // the empty lake has no positions to look up
            }
        } else {
            if (st[1] >= 0) {
                logDv.addLong(st[1]);
            }
            st[0] = recordOffset;
            st[1] = versionOffset;
        }
    }

    /** Durable data the derived state is rebuilt and advanced from. */
    public interface RecoverySource {
        /** {snapshotId, tiered offset of this bucket} of the latest readable snapshot, or null. */
        @Nullable
        long[] readableSnapshot() throws Exception;

        /** This bucket's tiered offset in a registered lake snapshot, or null if not registered. */
        @Nullable
        Long snapshotOffset(long snapshotId) throws Exception;

        /** Returns the registered position-index changes in commit order. */
        List<PositionIndexStore.ChainEntry> chain() throws Exception;

        /** Downloads this bucket's part of a change into fresh local files; throws if gone. */
        PositionIndexStore.BucketData readChange(PositionIndexStore.ChainEntry entry)
                throws Exception;

        /** Replays the local log from {@code fromOffset}: (key, record offset, version or -1). */
        void replayLog(long fromOffset, ChangeConsumer consumer) throws Exception;

        /** Whether old chain entries were trimmed (a rebuild must then start at a checkpoint). */
        default boolean chainTrimmed() throws Exception {
            return false;
        }

        /** This bucket's newest complete checkpoint at or before chain index upTo, or null. */
        @Nullable
        default PositionIndexStore.Checkpoint latestCheckpoint(
                List<PositionIndexStore.ChainEntry> chain, int upTo) throws Exception {
            return null;
        }

        /** Downloads a checkpoint: its SST (none for an empty index) and file ids. */
        default PositionIndexStore.BucketData readCheckpoint(PositionIndexStore.Checkpoint cp)
                throws Exception {
            throw new UnsupportedOperationException("no checkpoints");
        }

        /** Publishes this bucket's full index covering {@code snapshotId}. */
        default void uploadCheckpoint(
                long snapshotId, @Nullable String localSst, Map<Integer, String> fileDict)
                throws Exception {}
    }

    /** Receives one replayed change. */
    public interface ChangeConsumer {
        void accept(byte[] key, long recordOffset, long versionOffset);
    }

    /**
     * Rebuilds the derived state on a new leader by replaying the published index chain up to the
     * readable snapshot and the local log from its tiered offset.
     */
    public synchronized void recover(RecoverySource src) throws Exception {
        this.recoverySource = src;
        long[] readable = src.readableSnapshot();
        if (readable == null) {
            // Nothing tiered yet: cover the empty lake and replay the log on the first union read.
            generation++;
            coveredSnapshotId = -1;
            snapshotStartOffset = 0;
            emptyLakeTracked = false;
            queue.clear();
            tail.clear();
            logDv.clear();
            lakeDv.clear();
            unresolved.clear();
            LOG.info(
                    "Derived DV recovery: no lake snapshot yet; covers the empty lake from offset 0"
                            + " (the log is replayed on the first union read).");
            return;
        }
        rebuildTo(src, readable[0], readable[1], src.chain());
    }

    private synchronized void rebuildTo(
            RecoverySource src,
            long snapshotId,
            long startOffset,
            List<PositionIndexStore.ChainEntry> chain)
            throws Exception {
        if (closed) {
            // A catch-up that started before close() must not write into the closing RocksDB.
            throw new IllegalStateException("Derived DV manager is closed");
        }
        long t0 = System.currentTimeMillis();
        int upTo = -1;
        for (int i = 0; i < chain.size(); i++) {
            if (chain.get(i).snapshotId == snapshotId) {
                upTo = i;
            }
        }
        if (upTo < 0) {
            throw new IllegalStateException(
                    "Derived DV recovery: readable snapshot "
                            + snapshotId
                            + " is not in the published index chain ("
                            + chain.size()
                            + " entries)");
        }
        int sstFiles = 0;
        int from = 0;
        PositionIndexStore.Checkpoint checkpoint = null;
        if (startOffset > 0) {
            checkpoint = src.latestCheckpoint(chain, upTo);
            if (checkpoint == null && src.chainTrimmed()) {
                // Complete only if this bucket had nothing tiered at the oldest kept entry
                // (offsets are cumulative), e.g. a partition created after the trim.
                Long firstOffset =
                        chain.isEmpty() ? null : src.snapshotOffset(chain.get(0).snapshotId);
                if (firstOffset == null || firstOffset > 0) {
                    throw new IllegalStateException(
                            "Derived DV recovery: the index chain was trimmed but no checkpoint of "
                                    + "this bucket covers it; refusing to rebuild a partial index");
                }
            }
        }
        if (checkpoint != null) {
            sstFiles += apply(src.readCheckpoint(checkpoint));
            from = checkpoint.chainIndex + 1;
        }
        // Offset 0 means nothing of this bucket is tiered, so no index change holds its rows.
        for (int i = from; startOffset > 0 && i <= upTo; i++) {
            sstFiles += apply(src.readChange(chain.get(i))); // in commit order: later wins
        }
        long t1 = System.currentTimeMillis();
        generation++;
        markPassed(coveredSnapshotId);
        for (int i = 0; i < upTo; i++) {
            markPassed(chain.get(i).snapshotId);
        }
        passed.remove(snapshotId);
        coveredSnapshotId = snapshotId;
        preparedSnapshotId = chain.get(upTo).changeId;
        preparedReadableId = snapshotId;
        pendingChange = PositionIndexStore.BucketData.EMPTY;
        snapshotStartOffset = startOffset;
        queue.clear();
        tail.clear();
        logDv.clear();
        lakeDv.clear();
        unresolved.clear();
        // Replay into the queue: the local log can run past the high watermark, and a change above
        // it must not mask a version below it.
        src.replayLog(startOffset, this::enqueueReplayed);
        applyUpTo(highWatermark.getAsLong());
        unresolved.clear();
        unresolved.addAll(tail.keySet());
        scheduleResolve();
        entriesApplied = upTo + 1;
        entriesAtCheckpoint = checkpoint == null ? 0 : checkpoint.chainIndex + 1;
        maybeCheckpoint();
        metrics.rebuilt(System.currentTimeMillis() - t0);
        LOG.info(
                "Derived DV recovery: covers snapshot {} from offset {}; replayed {} index changes "
                        + "({} SSTs{}) in {} ms and the log tail ({} keys) in {} ms.",
                snapshotId,
                startOffset,
                upTo + 1 - from,
                sstFiles,
                checkpoint == null ? "" : ", from checkpoint " + checkpoint.snapshotId,
                t1 - t0,
                tail.size(),
                System.currentTimeMillis() - t1);
    }

    /**
     * Exports the covered index every checkpointInterval chain entries. Callers hold the manager
     * lock, so the RocksDB snapshot taken here is exactly the covered index.
     */
    private void maybeCheckpoint() {
        final RecoverySource src = recoverySource;
        if (src == null
                || closed
                || checkpointing
                || coveredSnapshotId < 0
                || entriesApplied - entriesAtCheckpoint < checkpointInterval) {
            return;
        }
        checkpointing = true;
        final long snapshotId = coveredSnapshotId;
        final long applied = entriesApplied;
        final org.rocksdb.Snapshot view = db.snapshot();
        ADVANCER.execute(
                () -> {
                    long t0 = System.currentTimeMillis();
                    File tmp = null;
                    try {
                        tmp = Files.createTempFile("fluss-dv-ckpt-", ".sst").toFile();
                        tmp.delete();
                        Map<Integer, String> dict = db.fileDict().idToPathEntries(view);
                        // Entries of dropped files are garbage.
                        long n = db.rowPosIndex().exportTo(tmp.getPath(), view, dict::containsKey);
                        src.uploadCheckpoint(snapshotId, n > 0 ? tmp.getPath() : null, dict);
                        synchronized (DerivedDvManager.this) {
                            entriesAtCheckpoint = applied;
                        }
                        metrics.checkpointed(System.currentTimeMillis() - t0);
                        LOG.info(
                                "Derived DV: checkpointed the position index at snapshot {} "
                                        + "({} entries, {} files) in {} ms.",
                                snapshotId,
                                n,
                                dict.size(),
                                System.currentTimeMillis() - t0);
                    } catch (Exception e) {
                        LOG.warn(
                                "Derived DV: index checkpoint at snapshot {} failed; retried "
                                        + "after the next index change.",
                                snapshotId,
                                e);
                    } finally {
                        db.releaseSnapshot(view);
                        if (tmp != null) {
                            tmp.delete();
                        }
                        synchronized (DerivedDvManager.this) {
                            checkpointing = false;
                            notifyAll();
                        }
                    }
                });
    }

    /** Stops background work and waits for in-flight readers before the tablet closes RocksDB. */
    @Override
    public synchronized void close() {
        closed = true;
        long deadline = System.currentTimeMillis() + 30_000;
        while ((checkpointing || inFlight > 0 || backgroundReaders > 0)
                && System.currentTimeMillis() < deadline) {
            try {
                wait(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * Removes files that left the lake snapshot from the dictionary, so no lookup or mask resolves
     * into them; ids are never reused, and their stale index entries are left out of checkpoints.
     */
    private void dropFiles(List<String> fileNames) throws IOException {
        for (String name : fileNames) {
            int id = db.fileDict().getFileId(name);
            if (id >= 0) {
                db.fileDict().delete(id);
                lakeDv.remove(id);
            }
        }
    }

    /** Applies an index change or a checkpoint and returns the number of SSTs ingested. */
    private int apply(PositionIndexStore.BucketData change) throws IOException {
        for (Map.Entry<Integer, String> e : change.fileId2Name.entrySet()) {
            db.fileDict().put(e.getKey(), e.getValue());
        }
        if (!change.localSsts.isEmpty()) {
            db.rowPosIndex().ingestExternalFile(change.localSsts);
        }
        dropFiles(change.removedFiles);
        return change.localSsts.size();
    }

    /** The error for a snapshot this bucket does not cover; the caller re-plans on a newer one. */
    private StaleSnapshotException stale(long requestedSnapshotId) {
        metrics.staleSnapshot();
        return new StaleSnapshotException(coveredSnapshotId, requestedSnapshotId);
    }

    private void enqueueReplayed(byte[] key, long recordOffset, long versionOffset) {
        queue.add(DvEntry.derived(key, recordOffset, versionOffset));
    }

    /** Builds the tail of the empty lake (the whole log) for the first union read that needs it. */
    private void trackEmptyLake() throws IOException {
        long t0 = System.currentTimeMillis();
        generation++;
        snapshotStartOffset = 0;
        queue.clear();
        tail.clear();
        logDv.clear();
        lakeDv.clear();
        unresolved.clear();
        try {
            recoverySource.replayLog(0, this::enqueueReplayed);
        } catch (Exception e) {
            queue.clear();
            throw new IOException(
                    "Derived DV: no lake snapshot yet and the log cannot be replayed from offset 0",
                    e);
        }
        emptyLakeTracked = true;
        LOG.info(
                "Derived DV: no lake snapshot yet; replayed the log from offset 0 for a union read"
                        + " ({} changes) in {} ms.",
                queue.size(),
                System.currentTimeMillis() - t0);
    }

    private void markPassed(long snapshotId) {
        if (snapshotId >= 0 && passed.add(snapshotId) && passed.size() > PASSED_LIMIT) {
            Iterator<Long> it = passed.iterator();
            it.next();
            it.remove();
        }
    }

    private static int chainIndex(List<PositionIndexStore.ChainEntry> chain, long snapshotId) {
        for (int i = chain.size() - 1; i >= 0; i--) {
            if (chain.get(i).snapshotId == snapshotId) {
                return i;
            }
        }
        return -1;
    }

    /** Starts a background catch-up to a newly registered snapshot. */
    public void advanceAsync(long snapshotId) {
        synchronized (this) {
            if (recoverySource == null
                    || coveredSnapshotId == snapshotId
                    || passed.contains(snapshotId)) {
                return;
            }
        }
        ADVANCER.execute(
                () -> {
                    try {
                        ensureCovered(snapshotId);
                    } catch (Exception e) {
                        LOG.warn("Derived DV catch-up to snapshot {} failed.", snapshotId, e);
                    }
                });
    }

    /**
     * Advances to {@code snapshotId} from the published chain if it is newer than the covered one,
     * without a coordinator round. Returns whether {@code snapshotId} is covered now.
     */
    public boolean ensureCovered(long snapshotId) throws Exception {
        synchronized (advanceLock) {
            RecoverySource src;
            long from;
            synchronized (this) {
                if (coveredSnapshotId == snapshotId) {
                    return true;
                }
                if (recoverySource == null || passed.contains(snapshotId)) {
                    return false;
                }
                src = recoverySource;
                from = coveredSnapshotId;
            }
            Long offset = src.snapshotOffset(snapshotId);
            if (offset == null) {
                return false; // not registered yet
            }
            List<PositionIndexStore.ChainEntry> chain = src.chain();
            int target = chainIndex(chain, snapshotId);
            if (target < 0) {
                return false; // chain marker not visible yet
            }
            int start = from < 0 ? -1 : chainIndex(chain, from);
            if (start < 0) {
                // Full rebuild; close() is checked under the same lock hold, so a closed
                // tablet is never written and a shutdown is not reported as a failure.
                synchronized (this) {
                    if (closed) {
                        return false;
                    }
                    rebuildTo(src, snapshotId, offset, chain);
                }
                return true;
            }
            if (target < start) {
                return false; // older than the covered snapshot
            }
            long t0 = System.currentTimeMillis();
            // Downloads run outside the lock so writes are not blocked.
            List<PositionIndexStore.BucketData> changes = new ArrayList<>();
            int sstFiles = 0;
            for (int i = start + 1; i <= target; i++) {
                PositionIndexStore.BucketData c = src.readChange(chain.get(i));
                changes.add(c);
                sstFiles += c.localSsts.size();
            }
            synchronized (this) {
                if (closed) {
                    return false;
                }
                if (coveredSnapshotId != from) {
                    // Moved concurrently by a coordinator switch; the next call starts over.
                    return coveredSnapshotId == snapshotId;
                }
                for (PositionIndexStore.BucketData c : changes) {
                    apply(c); // commit order
                }
                for (int i = start + 1; i < target; i++) {
                    markPassed(chain.get(i).snapshotId);
                }
                preparedSnapshotId = chain.get(target).changeId;
                advanceTo(snapshotId, offset);
                entriesApplied += target - start;
                maybeCheckpoint();
                metrics.caughtUp(System.currentTimeMillis() - t0);
                LOG.info(
                        "Derived DV caught up from snapshot {} to {} on its own: {} index changes "
                                + "({} SSTs) in {} ms; {} tail keys.",
                        from,
                        snapshotId,
                        target - start,
                        sstFiles,
                        System.currentTimeMillis() - t0,
                        tail.size());
                return true;
            }
        }
    }

    /** Covers {@code snapshotId} from {@code readableOffset}; call after ingesting its change. */
    private void advanceTo(long snapshotId, long readableOffset) {
        applyUpTo(highWatermark.getAsLong());
        pendingChange = PositionIndexStore.BucketData.EMPTY;
        generation++;
        markPassed(coveredSnapshotId);
        coveredSnapshotId = snapshotId;
        preparedReadableId = snapshotId;
        snapshotStartOffset = readableOffset;
        // Keep only keys changed at or after the new tiered offset.
        Iterator<Map.Entry<ByteBuffer, long[]>> it = tail.entrySet().iterator();
        while (it.hasNext()) {
            if (it.next().getValue()[0] < readableOffset) {
                it.remove();
            }
        }
        if (readableOffset > 0 && !logDv.isEmpty()) {
            Roaring64Bitmap kept = new Roaring64Bitmap();
            org.roaringbitmap.longlong.LongIterator li = logDv.getLongIterator();
            while (li.hasNext()) {
                long off = li.next();
                if (off >= readableOffset) {
                    kept.addLong(off);
                }
            }
            logDv.clear();
            logDv.or(kept);
        }
        lakeDv.clear();
        unresolved.clear();
        unresolved.addAll(tail.keySet());
        scheduleResolve();
    }

    /** Catches up to {@code snapshotId} first if it is newer than the covered one. */
    private void catchUp(long snapshotId) {
        if (snapshotId < 0) {
            return; // the empty lake; a newer covered snapshot answers stale
        }
        boolean behind;
        synchronized (this) {
            behind = coveredSnapshotId != snapshotId;
        }
        if (behind) {
            try {
                ensureCovered(snapshotId);
            } catch (Exception e) {
                LOG.warn("Derived DV catch-up to snapshot {} failed.", snapshotId, e);
            }
        }
    }

    private void resolveUnresolved() throws IOException {
        List<byte[]> keys = new ArrayList<>(unresolved.size());
        for (ByteBuffer key : unresolved) {
            keys.add(key.array());
        }
        for (FilePos fp : db.rowPosIndex().multiGetByKeys(keys)) {
            if (fp != null) {
                lakeDv.computeIfAbsent(fp.fileId(), k -> new Roaring64Bitmap())
                        .addLong(fp.rowPosition());
            }
        }
        unresolved.clear();
    }

    @Override
    public DvSnapshot getDvForUnionRead(long requestedSnapshotId, long ignoredLeo)
            throws IOException, StaleSnapshotException {
        catchUp(requestedSnapshotId);
        return dvForUnionRead(requestedSnapshotId);
    }

    private synchronized DvSnapshot dvForUnionRead(long requestedSnapshotId)
            throws IOException, StaleSnapshotException {
        if (closed) {
            throw new IOException("Derived DV manager is closed");
        }
        if (coveredSnapshotId != requestedSnapshotId) {
            throw stale(requestedSnapshotId);
        }
        while (inFlight > 0) {
            try {
                wait();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException(ie);
            }
        }
        if (coveredSnapshotId != requestedSnapshotId) {
            throw stale(requestedSnapshotId);
        }
        if (coveredSnapshotId < 0 && recoverySource != null && !emptyLakeTracked) {
            trackEmptyLake();
        }
        long hw = highWatermark.getAsLong();
        applyUpTo(hw);
        resolveUnresolved();
        Map<String, byte[]> resolved = new HashMap<>();
        for (Map.Entry<Integer, Roaring64Bitmap> e : lakeDv.entrySet()) {
            String path = db.fileDict().getFilePath(e.getKey());
            if (path != null) {
                resolved.put(path, RoaringBitmapUtils.serializeRoaringBitmap64(e.getValue()));
            }
        }
        byte[] logDvBytes =
                logDv.isEmpty() ? null : RoaringBitmapUtils.serializeRoaringBitmap64(logDv);
        // Masks cover exactly [snapshotStartOffset, hw), so the reader must read the log up to hw.
        return new DvSnapshot(resolved, logDvBytes, hw, snapshotStartOffset);
    }

    /** Returns {key, file path, row position} of each key found in snapshot {@code snapshotId}. */
    public List<Object[]> lookup(long snapshotId, List<byte[]> keys)
            throws IOException, StaleSnapshotException {
        catchUp(snapshotId);
        return lookupCovered(snapshotId, keys);
    }

    private List<Object[]> lookupCovered(long snapshotId, List<byte[]> keys)
            throws IOException, StaleSnapshotException {
        long gen;
        synchronized (this) {
            if (closed) {
                throw new IOException("Derived DV manager is closing");
            }
            if (coveredSnapshotId != snapshotId) {
                throw stale(snapshotId);
            }
            gen = generation;
            backgroundReaders++;
        }
        try {
            return lookupOutsideLock(snapshotId, keys, gen);
        } finally {
            synchronized (this) {
                backgroundReaders--;
                notifyAll();
            }
        }
    }

    private List<Object[]> lookupOutsideLock(long snapshotId, List<byte[]> keys, long gen)
            throws IOException, StaleSnapshotException {
        // The index changes only when generation advances, so read it outside the lock and recheck
        // generation after; tiering lookups must not block the write path.
        List<Object[]> out = new ArrayList<>();
        List<FilePos> fps = db.rowPosIndex().multiGetByKeys(keys);
        Map<Integer, String> paths = new HashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            FilePos fp = fps.get(i);
            if (fp != null) {
                String path = paths.get(fp.fileId());
                if (path == null) {
                    path = db.fileDict().getFilePath(fp.fileId());
                    if (path != null) {
                        paths.put(fp.fileId(), path);
                    }
                }
                if (path != null) {
                    out.add(new Object[] {keys.get(i), path, fp.rowPosition()});
                }
            }
        }
        synchronized (this) {
            if (coveredSnapshotId != snapshotId || generation != gen) {
                // Advanced while reading, so the answer may mix two snapshots; the round retries.
                throw stale(snapshotId);
            }
        }
        return out;
    }

    @Override
    @Nullable
    public byte[] getLogDvSnapshot(long fromOffset, long toOffset) {
        return null; // derived tiering folds the round by key itself
    }

    @Override
    public synchronized void handlePrepare(
            int bucketId,
            @Nullable Long partitionId,
            DvPositionReportData.DvBucketOffset bucketOffset,
            RowPosSstDownloader downloader,
            long snapshotId,
            String localTempDir)
            throws IOException {
        if (preparedSnapshotId == snapshotId) {
            return;
        }
        // Read the change from the index store as a rebuild does, so its removed files are dropped
        // at the switch too; the downloader is unused.
        if (recoverySource == null) {
            throw new IllegalStateException("Derived DV prepare before recovery");
        }
        PositionIndexStore.BucketData change;
        try {
            PositionIndexStore.ChainEntry entry = null;
            for (PositionIndexStore.ChainEntry e : recoverySource.chain()) {
                if (e.changeId == snapshotId) {
                    entry = e;
                }
            }
            if (entry == null) {
                throw new IllegalStateException(
                        "Index change " + snapshotId + " is not registered in the chain");
            }
            change = recoverySource.readChange(entry);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(
                    "Derived DV prepare of index change " + snapshotId + " failed", e);
        }
        for (Map<Integer, String> dict :
                Arrays.asList(change.fileId2Name, bucketOffset.getNewFileDictEntries())) {
            for (Map.Entry<Integer, String> e : dict.entrySet()) {
                String existing = db.fileDict().getFilePath(e.getKey());
                if (existing != null && !existing.equals(e.getValue())) {
                    throw new IllegalStateException(
                            "file id "
                                    + e.getKey()
                                    + " maps to "
                                    + existing
                                    + " and "
                                    + e.getValue());
                }
                db.fileDict().put(e.getKey(), e.getValue());
            }
        }
        pendingChange = change;
        pendingReadableOffset = bucketOffset.getReadableOffset();
        preparedSnapshotId = snapshotId;
    }

    /** Records the readable snapshot that the last prepare, keyed by index change id, is for. */
    public synchronized void setPreparedReadable(long readableSnapshotId) {
        this.preparedReadableId = readableSnapshotId;
    }

    @Override
    public synchronized long getPendingReadableOffset(int bucketId) {
        return pendingReadableOffset;
    }

    @Override
    public synchronized void handleReadableSwitch(
            int bucketId, long readableSnapshotId, long readableOffset) throws IOException {
        if (coveredSnapshotId == readableSnapshotId || passed.contains(readableSnapshotId)) {
            return; // already there, or caught up past it on its own
        }
        if ((preparedReadableId != readableSnapshotId || coveredSnapshotId < 0)
                && recoverySource != null) {
            {
                // First snapshot, or leadership moved between prepare and switch: the snapshot
                // is registered and its change is in the chain, so rebuild to it directly.
                LOG.info(
                        "Derived DV switch to {} without prepare (prepared {}), rebuilding.",
                        readableSnapshotId,
                        preparedReadableId);
                try {
                    recover(recoverySource);
                } catch (IOException e) {
                    throw e;
                } catch (Exception e) {
                    throw new IOException("Derived DV rebuild on switch failed", e);
                }
                if (coveredSnapshotId == readableSnapshotId
                        || passed.contains(readableSnapshotId)) {
                    return;
                }
            }
            throw new IllegalStateException(
                    "Derived DV rebuild did not reach snapshot " + readableSnapshotId);
        }
        if (preparedReadableId != readableSnapshotId) {
            throw new IllegalStateException(
                    "Derived DV switch to "
                            + readableSnapshotId
                            + " but prepared "
                            + preparedReadableId);
        }
        applyUpTo(highWatermark.getAsLong());
        apply(pendingChange);
        pendingChange = PositionIndexStore.BucketData.EMPTY;
        advanceTo(readableSnapshotId, readableOffset);
        LOG.info(
                "Derived DV bucket {} covers snapshot {} from offset {}: {} tail keys, {} dead tail versions.",
                bucketId,
                readableSnapshotId,
                readableOffset,
                tail.size(),
                logDv.getLongCardinality());
    }

    @Override
    public synchronized long getReadableSnapshotId() {
        return coveredSnapshotId;
    }

    @Override
    public synchronized long getSnapshotStartLogOffset() {
        return snapshotStartOffset;
    }

    /** Returns the size of the derived state, for diagnostics. */
    public synchronized String stateSummary() {
        return "tailKeys="
                + tail.size()
                + ", queued="
                + queue.size()
                + ", deadTailVersions="
                + logDv.getLongCardinality()
                + ", lakeDvFiles="
                + lakeDv.size();
    }
}
