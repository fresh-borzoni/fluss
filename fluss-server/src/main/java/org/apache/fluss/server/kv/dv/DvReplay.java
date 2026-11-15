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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.server.kv.dv.DerivedDvManager.ChangeConsumer;
import org.apache.fluss.server.kv.dv.DerivedDvManager.RecoverySource;
import org.apache.fluss.server.kv.dv.DvOwner.TailDiskException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.apache.fluss.server.kv.dv.DvChangeListStore.MISSING;

/**
 * A log replay into a fresh change list in the bucket's RocksDB, which readers see only once it is
 * installed. Each batch it writes takes a lease of the RocksDB of its own.
 *
 * <p>The replay reads the log off the owner, in passes. Once it is near the log end, it asks the
 * owner to enter the final pass and waits for the acknowledgement; from then on the owner also
 * buffers every batch it drains from the hand-off. The replay then reads to the log end, and the
 * load's swap installs the replayed change list with the buffered changes at or after where the
 * replay ended. A batch drained before the acknowledgement was appended to the log before the final
 * read, so the replay has it; one drained later is buffered, or repeats one the replay has.
 */
final class DvReplay implements ChangeConsumer, DerivedDvHeapBudget.Owner {

    private static final Logger LOG = LoggerFactory.getLogger(DvReplay.class);

    /**
     * The version offsets keys had where a replay resumes after a change-list checkpoint: the KV
     * restored from the KV snapshot at that offset.
     */
    interface BaseVersions {
        /** The version offset of each key, or -1 if the key had no value. */
        long[] versionsAt(List<byte[]> keys) throws IOException;
    }

    // A replay makes another pass before its final pass while the log end is more than this
    // many offsets ahead. This keeps the final pass, whose drained changes the owner buffers,
    // short.
    private static final long FINAL_PASS_MAX_OFFSETS = 4096;
    private static final int MAX_EARLY_PASSES = 8;
    private static final int REPLAY_APPLY_BATCH = 4096;
    // Reports of a replay's memory between two measurements of its bitmaps.
    private static final int BITMAP_MEASURE_INTERVAL = 64;

    private final DvOwner m;
    // whether the replay's load stops: the bucket closes, or a fault of the owner stopped it
    private final BooleanSupplier stopped;
    private final RecoverySource src;
    private final long startOffset;
    // where the replay started reading the log: the start offset, or the end of a change-list
    // checkpoint loaded into the change list first
    final long resumedAt;
    // names a key's version where the replay resumed, for keys it has not replayed; null: such a
    // key's superseded version is before the start offset
    @Nullable private final BaseVersions base;
    // keys named from the base, and the time it took, for the rebuild's log line
    long baseReads;
    long baseNanos;
    // runs once the change list is installed, an owner step
    @Nullable Runnable onInstall;
    // runs when the base cannot be read, before the replay fails: the base is not used again
    @Nullable Runnable onBaseFailure;
    // runs once the replay ends, installed or not
    @Nullable Runnable onFinish;
    final DvChangeList tail;
    // The last version of each key replayed, to name the version a replayed change
    // supersedes, as the log does not. In memory until the budget asks the replay to spill;
    // then on disk under versionsId, and the changes wait in unversioned to be named a batch
    // at a time.
    @Nullable private HashMap<ByteBuffer, Long> lastVersions = new HashMap<>();
    long versionsId = -1;
    private final List<DvEntry> unversioned = new ArrayList<>();
    private long lastVersionBytes;
    // where the replay continues
    long next;
    private int sinceProgress = 0;
    private long reportedBacklog = 0;
    // the budget asked the replay to spill its map; it does at its next batch
    private volatile boolean spillRequested;
    // the change list became the bucket's
    boolean installed;
    // the bucket lost its change list during the final pass, as the hand-off overflowed or
    // failed: batches after the final read may be lost too, so the replay installs nothing
    boolean lost;
    private long batches;
    private long bitmapBytes;
    // the log changes replayed, for the rebuild's log line
    long changes;

    private DvReplay(
            DvOwner m,
            RecoverySource src,
            long startOffset,
            long resumeAt,
            @Nullable BaseVersions base,
            long listId,
            BooleanSupplier stopped) {
        this.m = m;
        this.stopped = stopped;
        this.src = src;
        this.startOffset = startOffset;
        this.resumedAt = resumeAt;
        this.base = base;
        this.tail = new DvChangeList(m.db.changeListStore(), listId, startOffset, m.testHooks);
        this.next = resumeAt;
        reportBacklog(resumeAt);
        m.rebuild.replaying = this;
    }

    /**
     * Starts a replay from {@code startOffset} for a load that stops once {@code stopped} says so,
     * or returns null if it stopped already.
     */
    @Nullable
    static DvReplay start(
            DvOwner m, RecoverySource src, long startOffset, BooleanSupplier stopped) {
        return start(m, src, startOffset, startOffset, null, stopped);
    }

    /**
     * Starts a replay of the change list from {@code startOffset} that reads the log from {@code
     * resumeAt}: the changes before it are loaded from a change-list checkpoint, and {@code base}
     * names the version a key had at {@code resumeAt}. Returns null if the load stopped already.
     */
    @Nullable
    static DvReplay start(
            DvOwner m,
            RecoverySource src,
            long startOffset,
            long resumeAt,
            @Nullable BaseVersions base,
            BooleanSupplier stopped) {
        if (stopped.getAsBoolean()) {
            return null;
        }
        return new DvReplay(
                m, src, startOffset, resumeAt, base, m.tailIds.incrementAndGet(), stopped);
    }

    /** Whether the replay's load stops: the bucket closes, or a fault stopped the load. */
    boolean stopped() {
        return stopped.getAsBoolean();
    }

    /**
     * Replays the log from the replay's start, off the owner. Makes further passes while writes
     * keep the log end far ahead. Returns the failure, or null.
     */
    @Nullable
    Exception replayEarlyPasses() {
        try {
            replayToLogEnd();
            for (int pass = 1;
                    pass < MAX_EARLY_PASSES && behindLogEnd() > FINAL_PASS_MAX_OFFSETS;
                    pass++) {
                replayToLogEnd();
            }
            return null;
        } catch (Exception e) {
            return e;
        }
    }

    /**
     * Replays what was written after the earlier passes, in the final pass: the owner buffers every
     * change it drains from the hand-off from before this read on. Every change appended so far is
     * replayed here, and every later one is buffered. Returns the failure, or null.
     */
    @Nullable
    Exception finalRead() {
        try {
            if (behindLogEnd() > 0) {
                replayToLogEnd();
            }
            return null;
        } catch (Exception e) {
            return e;
        }
    }

    @Override
    public void accept(byte[] key, long recordOffset, long versionOffset, boolean insert) {
        changes++;
        // With row TTL an insert may follow an expired version of the key, which the map
        // names if it is in the replayed log; one before it is in the lake, and the key is
        // looked up there. So the replay names an expired version in the log exactly, which
        // the write path cannot: its KV value is gone.
        boolean supersedesNothing = insert && !m.rowTtl;
        if (versionsId >= 0 || m.rebuild.spillReplayVersions || base != null) {
            if (versionsId < 0 && m.rebuild.spillReplayVersions) {
                spillVersions();
            }
            // the map is on disk, or keys it lacks are named from the base: the superseded
            // versions are named a batch at a time
            unversioned.add(new DvEntry(key, recordOffset, versionOffset, supersedesNothing, -1));
            if (unversioned.size() >= REPLAY_APPLY_BATCH) {
                nameVersions();
            }
            progress(recordOffset);
            return;
        }
        // -1: the superseded version is before the replay's start, so in the lake
        Long last = lastVersions.put(ByteBuffer.wrap(key), versionOffset);
        if (last == null) {
            // a map node, a ByteBuffer, a Long and the key array
            lastVersionBytes += 112 + ((16 + key.length + 7) & ~7L);
        }
        enqueue(
                new DvEntry(
                        key,
                        recordOffset,
                        versionOffset,
                        supersedesNothing,
                        last == null ? -1 : last));
        progress(recordOffset);
    }

    /**
     * Names the version each waiting change supersedes from the map of last versions, on disk or on
     * the heap, reading the changes' keys in one lookup and writing their new last versions back
     * together. A key the map lacks is named from the base, if any, else as before the start.
     */
    private void nameVersions() {
        if (unversioned.isEmpty()) {
            return;
        }
        List<DvEntry> named;
        m.leases.acquire();
        try {
            Map<ByteBuffer, Long> last = new HashMap<>();
            List<byte[]> distinct = new ArrayList<>();
            for (DvEntry e : unversioned) {
                if (last.putIfAbsent(ByteBuffer.wrap(e.getKey()), MISSING) == null) {
                    distinct.add(e.getKey());
                }
            }
            long[] read =
                    versionsId >= 0
                            ? m.db.changeListStore().getVersions(versionsId, distinct)
                            : heapVersions(distinct);
            long[] based = baseVersions(distinct, read);
            for (int i = 0; i < distinct.size(); i++) {
                last.put(ByteBuffer.wrap(distinct.get(i)), read[i] == MISSING ? based[i] : read[i]);
            }
            named = new ArrayList<>(unversioned.size());
            for (DvEntry e : unversioned) {
                ByteBuffer k = ByteBuffer.wrap(e.getKey());
                named.add(
                        new DvEntry(
                                e.getKey(),
                                e.getRecordOffset(),
                                e.getVersionOffset(),
                                e.isInsert(),
                                last.put(k, e.getVersionOffset())));
            }
            if (versionsId >= 0) {
                putVersions(last);
            } else {
                putHeapVersions(last);
            }
            unversioned.clear();
        } catch (IOException e) {
            throw new TailDiskException(e);
        } finally {
            m.leases.release();
        }
        for (DvEntry e : named) {
            enqueue(e);
        }
    }

    /** The heap map's last versions of {@code keys}, {@link #MISSING} for those it lacks. */
    private long[] heapVersions(List<byte[]> keys) {
        long[] out = new long[keys.size()];
        for (int i = 0; i < out.length; i++) {
            Long v = lastVersions.get(ByteBuffer.wrap(keys.get(i)));
            out[i] = v == null ? MISSING : v;
        }
        return out;
    }

    private void putHeapVersions(Map<ByteBuffer, Long> versions) {
        for (Map.Entry<ByteBuffer, Long> e : versions.entrySet()) {
            if (lastVersions.put(e.getKey(), e.getValue()) == null) {
                lastVersionBytes += 112 + ((16 + e.getKey().capacity() + 7) & ~7L);
            }
        }
    }

    /**
     * The base's versions of the keys the map lacks ({@code read} is {@link #MISSING}), aligned
     * with {@code keys}; -1 for all of them without a base.
     */
    private long[] baseVersions(List<byte[]> keys, long[] read) {
        long[] out = new long[keys.size()];
        java.util.Arrays.fill(out, -1);
        if (base == null) {
            return out;
        }
        List<byte[]> unseen = new ArrayList<>();
        for (int i = 0; i < read.length; i++) {
            if (read[i] == MISSING) {
                unseen.add(keys.get(i));
            }
        }
        if (unseen.isEmpty()) {
            return out;
        }
        long start = System.nanoTime();
        long[] versions;
        try {
            versions = base.versionsAt(unseen);
        } catch (IOException e) {
            // The load fails as for a disk failure, retriable after a backoff, and its retry
            // replays without the view, which this releases.
            Runnable hook = onBaseFailure;
            if (hook != null) {
                hook.run();
            }
            throw new TailDiskException(e);
        }
        baseNanos += System.nanoTime() - start;
        baseReads += unseen.size();
        for (int i = 0, j = 0; i < read.length; i++) {
            if (read[i] == MISSING) {
                out[i] = versions[j++];
            }
        }
        return out;
    }

    private void putVersions(Map<ByteBuffer, Long> versions) throws IOException {
        m.testHooks.beforeChangeListWrite();
        m.db.changeListStore().putVersions(versionsId, versions);
    }

    /** Moves the map of last versions to the bucket's RocksDB. Runs on the replay's thread. */
    private void spillVersions() {
        if (stopped()) {
            throw new IllegalStateException("The deletion vector replay stopped");
        }
        versionsId = m.tailIds.incrementAndGet();
        m.leases.acquire();
        try {
            Map<ByteBuffer, Long> batch = new HashMap<>();
            for (Map.Entry<ByteBuffer, Long> e : lastVersions.entrySet()) {
                batch.put(e.getKey(), e.getValue());
                if (batch.size() >= REPLAY_APPLY_BATCH) {
                    putVersions(batch);
                    batch.clear();
                }
            }
            putVersions(batch);
        } catch (IOException e) {
            throw new TailDiskException(e);
        } finally {
            m.leases.release();
        }
        lastVersions = null;
        lastVersionBytes = 0;
        LOG.info(
                "A deletion vector replay of the log from offset {} spilled its map of"
                        + " last versions to disk, as the deletion vector state of the tablet"
                        + " server exceeds {}.",
                startOffset,
                ConfigOptions.KV_DELETION_VECTORS_HEAP_MEMORY.key());
    }

    private void enqueue(DvEntry entry) {
        tail.enqueue(entry);
        // A lake far behind the log can leave tens of millions of changes to replay. They are
        // applied once the high watermark passes them, so only the part above it waits in the
        // queue; the applied ones go to disk.
        if (tail.queue.size() >= REPLAY_APPLY_BATCH) {
            m.leases.acquire();
            try {
                tail.applyUpTo(m.highWatermark.getAsLong(), startOffset, null);
            } catch (IOException e) {
                throw new TailDiskException(e);
            } finally {
                m.leases.release();
            }
            if (spillRequested && lastVersions != null && !lastVersions.isEmpty()) {
                spillVersions();
            }
            // its changes count against the tablet server's memory as they grow
            if (batches++ % BITMAP_MEASURE_INTERVAL == 0) {
                bitmapBytes = tail.deadVersions.getLongSizeInBytes();
            }
            m.heapBudget.update(
                    this, tail.heapBytes() + bitmapBytes + lastVersionBytes, lastVersionBytes);
        }
    }

    private void progress(long recordOffset) {
        if (++sinceProgress >= REPLAY_APPLY_BATCH) {
            sinceProgress = 0;
            if (stopped()) {
                throw new IllegalStateException("The deletion vector replay stopped");
            }
            reportBacklog(recordOffset + 1);
        }
    }

    /** Replays from where the replay stopped to the current log end, which writes move on. */
    private void replayToLogEnd() throws Exception {
        next = src.replayLog(next, src.logEndOffset(), this);
        // the changes still waiting for their superseded versions, before the list is used
        nameVersions();
        reportBacklog(next);
    }

    private long behindLogEnd() {
        return src.logEndOffset() - next;
    }

    private void reportBacklog(long position) {
        long backlog = Math.max(0, src.logEndOffset() - position);
        m.metrics.replayBacklogChanged(backlog - reportedBacklog);
        reportedBacklog = backlog;
    }

    @Override
    public boolean free() {
        if (lastVersions == null || lastVersions.isEmpty()) {
            // nothing left in memory to spill
            return false;
        }
        spillRequested = true;
        return true;
    }

    // the changes the owner drained during the final pass, owner-confined
    private final List<DvEntry> buffered = new ArrayList<>();

    /** Buffers changes the owner drained during the final pass; an owner step. */
    void buffer(List<DvEntry> entries) {
        buffered.addAll(entries);
    }

    /**
     * Makes the replayed change list the covered snapshot's, an owner step: with the changes
     * buffered in the final pass from where the replay ended. The earlier ones are in the replay
     * already.
     */
    void install() {
        if (lost) {
            // the change list stays missing; the next replay reads the log again
            buffered.clear();
            return;
        }
        for (DvEntry e : buffered) {
            if (e.getRecordOffset() >= next) {
                tail.enqueue(e);
            }
        }
        buffered.clear();
        m.maskState.installTail(tail, next);
        installed = true;
        Runnable hook = onInstall;
        if (hook != null) {
            hook.run();
        }
    }

    /** Ends the replay: drops its change list unless installed, and its map of versions. */
    void finish() {
        Runnable finished = onFinish;
        onFinish = null;
        if (finished != null) {
            finished.run();
        }
        m.metrics.replayBacklogChanged(-reportedBacklog);
        reportedBacklog = 0;
        m.heapBudget.remove(this);
        if (m.rebuild.replaying == this) {
            m.rebuild.replaying = null;
        }
        // nothing to drop once the bucket closed: change lists are cleared when the RocksDB
        // opens again
        if ((installed && versionsId < 0) || !m.leases.tryAcquire()) {
            return;
        }
        try {
            if (!installed) {
                tail.drop();
            }
            // the map of last versions is the replay's own
            if (versionsId >= 0) {
                m.db.changeListStore().drop(versionsId);
            }
        } catch (IOException e) {
            LOG.warn("Could not drop a replayed deletion vector change list.", e);
        } finally {
            m.leases.release();
        }
    }
}
