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

import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.server.kv.dv.DvFrontiers.KeepReady;

import io.github.fluss_contrib.rocksdb.Snapshot;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.apache.fluss.server.kv.dv.DvOwner.RESOLVE_CHUNK;

/**
 * What a switch to a newer snapshot derives again, so it keeps the other lake masks. A key the
 * switch's index changes do not name keeps its row: the index of the new snapshot is the old one
 * plus the changes. Nor did the key change in the log the new snapshot tiered, or that change would
 * name the version the snapshot holds and the key would be in the changes; so none of its pending
 * changes left the change list, and its lake mask stays valid. Only the keys the changes name, the
 * keys whose masking changes left the change list, and the keys a resolve was looking up are
 * derived again.
 *
 * <p>{@link #prepare} reads the keys the downloaded changes name and their positions in the version
 * the switch leaves, before the switch ingests. {@link #complete} runs after the swap: it removes
 * the old positions of every key derived again from the masks, and hands the keys that still mask a
 * lake row to the resolves. If anything fails, or the switch would derive more than {@link
 * #MAX_KEYS} keys again, every lake key is looked up again instead. Its methods run as owner steps
 * unless their javadoc names another thread.
 */
final class DvTargetedDerivation implements DerivedDvHeapBudget.Owner {

    private static final Logger LOG = LoggerFactory.getLogger(DvTargetedDerivation.class);

    // The most keys a switch may derive again for it to keep the other masks, the keys its index
    // changes name (deletes too) and those its dropped changes delete: their hashes take 8 bytes
    // each; beyond, looking up every lake key of the change list again is cheaper anyway.
    static final long MAX_KEYS = 8_000_000;

    private final DvOwner m;
    // hashes of the keys derived again, sorted up to sealedCount; a collision only looks a key up
    // again
    private long[] hashes;
    private int hashCount;
    private int sealedCount;
    // positions the derived-again keys had in the version the switch leaves, removed from the masks
    private final Map<Integer, Roaring64Bitmap> oldPositions = new HashMap<>();
    final List<ByteBuffer> pendingKeys = new ArrayList<>();
    // the version the switch leaves, for the positions the keys had
    @Nullable DvVersion oldVersion;
    // the change list at the swap, before the trim, for the changes the switch dropped; it holds a
    // lease of the RocksDB
    @Nullable Snapshot listView;
    // the epoch of the masks the swap left: the derivation's results are for it only
    DvMaskState.Epoch epoch;
    long oldStart;
    long newStart;
    long listId = -1;
    // The heap the derivation reports to the budget, a part at a time: the hash array, counted
    // before it is allocated; the old positions as last measured, and an estimate for a chunk
    // before it is added; the keys pending at the swap; and the keys complete() collected for the
    // owner to look up again, a chunk at a time. Guarded by this.
    private long hashBytes;
    private long oldPositionBytes;
    private long pendingKeyBytes;
    private long collectedKeyBytes;
    private boolean forgotten;
    // whether complete() posted its result
    private volatile boolean posted;
    // for the switch's log line
    long changeKeys;
    long removed;
    long lookedUpAgain;

    // the heap a row position takes in the old positions, reserved before a chunk is added: about
    // 2 bytes in a dense bitmap container, more in a sparse one; measured once the chunk is in
    private static final long OLD_POSITION_BYTES = 8;

    private DvTargetedDerivation(DvOwner m) {
        this.m = m;
        this.hashes = new long[0];
    }

    /**
     * Whether a switch can keep the masks and derive only what it changes: they must be complete
     * for the covered snapshot but for the keys waiting for their positions.
     */
    static boolean targetable(DvOwner m) {
        return m.targetedSwitches
                && !m.closed
                && m.coveredSnapshotId >= 0
                && m.recoverySource != null
                && m.maskState.tailFailure == null
                && !m.maskState.fullResolvePending
                && !m.maskState.targetedPending
                && !m.latestIndex.isDirty()
                && !m.maskState.tailMissing();
    }

    /**
     * Reads the keys a switch's downloaded index changes name and looks up their positions in the
     * covered version, on the switch's job thread. Returns null if the switch should look up every
     * lake key again: the masks are not complete, or the changes name more than half as many keys
     * as the change list keeps after the switch, with deletes, or more than {@link #MAX_KEYS} keys.
     * A targeted switch looks up the keys it names and reads every change it keeps, while looking
     * every key up again costs one lookup per change kept; a lookup costs about three times reading
     * a change, so naming half as many keys is about where the two cost the same. A catch-up after
     * a burst of inserts keeps nothing and looks nothing up.
     */
    @Nullable
    static DvTargetedDerivation prepare(
            DvOwner m, List<PositionIndexStore.BucketData> changes, DvVersion covered, long kept)
            throws IOException {
        try {
            // tombstones count too: a deleted key's position is looked up in the derivation
            long keys = 0;
            for (PositionIndexStore.BucketData c : changes) {
                for (String sst : c.localSsts) {
                    keys += RowPosIndex.countEntries(sst);
                }
            }
            if (keys > MAX_KEYS || keys > Math.max(kept / 2, RESOLVE_CHUNK)) {
                return null;
            }
            DvTargetedDerivation d = new DvTargetedDerivation(m);
            d.growHashes(Math.max(16, (int) keys));
            d.changeKeys = keys;
            for (PositionIndexStore.BucketData c : changes) {
                for (String sst : c.localSsts) {
                    if (m.closed) {
                        return null;
                    }
                    RowPosIndex.forEachPositionKeys(
                            sst,
                            RESOLVE_CHUNK,
                            chunk -> {
                                for (byte[] k : chunk) {
                                    d.add(k);
                                }
                                d.addOld(covered.positions(chunk));
                                m.lookedUpKeys.addAndGet(chunk.size());
                            });
                }
            }
            return d;
        } finally {
            covered.release();
        }
    }

    /**
     * Reports the derivation's heap to the tablet server's budget: the hashes, the old positions,
     * the keys pending at the swap and the keys collected to look up again. It holds them from its
     * prepare until its owner step ends it, and each part is counted before it is allocated or as
     * it grows, a chunk at a time; thread-safe.
     */
    private synchronized void report() {
        if (!forgotten) {
            m.heapBudget.update(
                    this,
                    64 + hashBytes + oldPositionBytes + pendingKeyBytes + collectedKeyBytes,
                    0);
        }
    }

    /** Leaves the budget: the derivation ended, or its switch did not swap it in. */
    synchronized void forget() {
        forgotten = true;
        m.heapBudget.remove(this);
    }

    @Override
    public boolean free() {
        return false; // nothing to free: the derivation needs what it holds until it ends
    }

    /**
     * Ends a derivation its switch dropped before posting its result: releases its views and leaves
     * the budget. A posted one ends in its owner step.
     */
    void abandonUnlessPosted() {
        if (!posted) {
            releaseViews();
            forget();
        }
    }

    /**
     * Takes the views the derivation reads after the swap: the version the switch leaves, and the
     * change list before its trim. Returns false if it cannot keep the masks. An owner step, before
     * the swap.
     */
    boolean holdViews() {
        if (!targetable(m) || !m.leases.tryAcquire()) {
            return false;
        }
        oldVersion = m.version;
        oldVersion.tryRetain();
        listView = m.db.snapshot();
        return true;
    }

    /**
     * Records what the swap left to derive again; an owner step, after the swap. The keys resolves
     * were to look up, or look up now: their lookups of the old version are dropped, and they are
     * looked up again if they are still lake keys.
     */
    void swapped(long previousStart, long readableOffset) {
        pendingKeys.addAll(m.maskState.unresolved);
        for (List<ByteBuffer> batch : m.maskState.resolving) {
            pendingKeys.addAll(batch);
        }
        long bytes = 0;
        for (ByteBuffer k : pendingKeys) {
            bytes += DvUnresolvedKeys.keyBytes(k);
        }
        synchronized (this) {
            pendingKeyBytes = bytes;
        }
        oldStart = previousStart;
        newStart = readableOffset;
        epoch = m.maskState.epoch;
        listId = m.maskState.tail.id;
        m.maskState.targetedPending = true;
        // the pending keys leave the unresolved ones, and so the bucket's own account
        report();
    }

    void complete() {
        boolean tooManyKeys = false;
        try {
            // A dropped change of a key the changes name had its position looked up already; the
            // others are deletes, which the changes hold no position of. The dropped changes are
            // read a chunk at a time, and a switch that would derive more than MAX_KEYS keys again
            // looks every lake key up again instead.
            seal();
            try (DvChangeListStore.PendingCursor cursor =
                    m.db.changeListStore().pendingCursor(listId, listView, 0, newStart)) {
                List<byte[]> dropped = new ArrayList<>(RESOLVE_CHUNK);
                List<byte[]> deleted = new ArrayList<>(RESOLVE_CHUNK);
                while (!m.closed
                        && !tooManyKeys
                        && cursor.nextLakeKeys(RESOLVE_CHUNK, oldStart, dropped)) {
                    for (byte[] k : dropped) {
                        if (!names(k)) {
                            deleted.add(k);
                        }
                    }
                    dropped.clear();
                    if (deleted.isEmpty()) {
                        continue;
                    }
                    if (keys() + deleted.size() > MAX_KEYS) {
                        tooManyKeys = true;
                        break;
                    }
                    addOld(oldVersion.positions(deleted));
                    m.lookedUpKeys.addAndGet(deleted.size());
                    for (byte[] k : deleted) {
                        add(k);
                    }
                    deleted.clear();
                }
            }
            tooManyKeys |= keys() + pendingKeys.size() > MAX_KEYS;
            if (tooManyKeys) {
                boolean many = true;
                posted = true;
                m.post(() -> fallBack(many, null));
                return;
            }
            for (ByteBuffer k : pendingKeys) {
                add(k.array());
            }
            seal();
            Predicate<byte[]> named = this::names;
            // the pending changes are read from disk in offset order, off the owner
            List<byte[]> keys = new ArrayList<>();
            try (DvChangeListStore.PendingCursor cursor =
                    m.db.changeListStore().pendingCursor(listId, null, newStart, Long.MAX_VALUE)) {
                // collects the lake keys the switch named, counted a chunk at a time
                boolean more = true;
                int counted = 0;
                while (more && !m.closed) {
                    more = cursor.nextLakeKeys(RESOLVE_CHUNK, newStart, named, keys);
                    counted = countCollected(keys, counted);
                }
            }
            posted = true;
            m.post(() -> derived(keys));
        } catch (IOException | RuntimeException e) {
            posted = true;
            m.post(() -> fallBack(false, e));
        } catch (Throwable t) {
            // Every lake key is looked up again, as after any failed derivation. A fatal error is
            // logged, counted and rethrown by the load's job boundary once the fallback is posted.
            Exception e =
                    t instanceof VirtualMachineError
                            ? new Exception(t)
                            : m.faults.jobError(t, "a switch's derivation");
            posted = true;
            m.post(() -> fallBack(false, e));
            DvOwnerFaults.rethrowIfFatal(t);
        } finally {
            releaseViews();
        }
    }

    /**
     * Ends the derivation: the old positions of the keys derived again leave the masks, and the
     * keys that still mask a lake row are looked up again. No resolve ran meanwhile, so no position
     * of the new version is removed.
     */
    private void derived(List<byte[]> keys) {
        // the keys move to the unresolved ones, which the bucket's account counts
        forget();
        DvMaskState masks = m.maskState;
        if (epoch != masks.epoch || m.closed || masks.tail.id != listId || !masks.targetedPending) {
            if (epoch != masks.epoch && !m.closed) {
                // the change list was replaced meanwhile, and its masks derived from scratch
                m.maskState.targetedPending = false;
            }
            return;
        }
        removed = removeFromMasks(oldPositions);
        for (byte[] k : keys) {
            m.maskState.unresolved.add(ByteBuffer.wrap(k));
        }
        lookedUpAgain = keys.size();
        done();
    }

    /**
     * Ends a derivation that failed, or would derive too many keys again: every lake key is looked
     * up again.
     */
    private void fallBack(boolean tooManyKeys, @Nullable Exception failure) {
        forget();
        DvMaskState masks = m.maskState;
        if (epoch != masks.epoch || m.closed || !masks.targetedPending) {
            if (epoch != masks.epoch && !m.closed) {
                m.maskState.targetedPending = false;
            }
            return;
        }
        if (tooManyKeys) {
            LOG.info(
                    "The deletion vector switch to snapshot {} derives more than {} keys"
                            + " again; every lake key is looked up again instead.",
                    m.coveredSnapshotId,
                    MAX_KEYS);
        } else {
            LOG.warn(
                    "The deletion vector masks of snapshot {} could not be kept on its"
                            + " switch; every lake key is looked up again.",
                    m.coveredSnapshotId,
                    failure);
        }
        m.targetedFallbacks.incrementAndGet();
        m.maskState.renew(KeepReady.SAME_SNAPSHOT);
        m.maskState.targetedPending = false;
        m.maskState.lakeMasks.clear();
        m.maskState.unresolved.clear();
        m.maskState.resolveWholeTail();
        m.maskState.reportTail(true);
        m.maskState.scheduleResolve();
    }

    /** Releases the views the derivation reads, once; thread-safe. */
    synchronized void releaseViews() {
        if (listView != null) {
            m.db.releaseSnapshot(listView);
            listView = null;
            m.leases.release();
        }
        if (oldVersion != null) {
            oldVersion.release();
            oldVersion = null;
        }
    }

    /** Ends the derivation: the masks are served again. */
    private void done() {
        m.targetedCount.incrementAndGet();
        m.maskState.targetedPending = false;
        m.maskState.reportTail(true);
        m.maskState.refreshLatest();
        m.maskState.scheduleResolve();
    }

    /** Removes positions from the lake masks and returns how many were there. */
    private long removeFromMasks(Map<Integer, Roaring64Bitmap> positions) {
        long count = 0;
        for (Map.Entry<Integer, Roaring64Bitmap> e : positions.entrySet()) {
            count += m.maskState.lakeMasks.remove(e.getKey(), e.getValue());
        }
        return count;
    }

    private void add(byte[] key) {
        if (hashCount == hashes.length) {
            growHashes(hashCount * 2);
        }
        hashes[hashCount++] = keyHash(key);
    }

    /**
     * Grows the hash array to {@code capacity}: the budget counts the old array and the new one
     * before the new one is allocated, and the new one once the old is gone.
     */
    private void growHashes(int capacity) {
        synchronized (this) {
            hashBytes = 8L * (hashes.length + capacity);
        }
        report();
        hashes = Arrays.copyOf(hashes, capacity);
        synchronized (this) {
            hashBytes = 8L * capacity;
        }
        report();
    }

    /**
     * Adds the old positions of a chunk: the budget counts an estimate before they are added, and
     * their measured size once they are in.
     */
    private void addOld(List<FilePos> positions) {
        long added = 0;
        for (FilePos fp : positions) {
            if (fp != null) {
                added++;
            }
        }
        synchronized (this) {
            oldPositionBytes += OLD_POSITION_BYTES * added;
        }
        report();
        long measured = 0;
        for (FilePos fp : positions) {
            if (fp != null) {
                oldPositions
                        .computeIfAbsent(fp.fileId(), k -> new Roaring64Bitmap())
                        .addLong(fp.rowPosition());
            }
        }
        for (Roaring64Bitmap bitmap : oldPositions.values()) {
            measured += bitmap.getLongSizeInBytes();
        }
        synchronized (this) {
            oldPositionBytes = measured;
        }
        report();
        m.testHooks.afterDerivationChunk();
    }

    /** Counts the keys collected from {@code from} on; returns how many are counted now. */
    private int countCollected(List<byte[]> keys, int from) {
        long bytes = 0;
        for (int i = from; i < keys.size(); i++) {
            bytes += DvUnresolvedKeys.keyBytes(ByteBuffer.wrap(keys.get(i)));
        }
        synchronized (this) {
            collectedKeyBytes += bytes;
        }
        report();
        return keys.size();
    }

    /** The keys derived again so far. */
    private int keys() {
        return hashCount;
    }

    /** Sorts the keys added so far, which {@link #names} looks up from then on. */
    private void seal() {
        Arrays.sort(hashes, 0, hashCount);
        sealedCount = hashCount;
    }

    /** Whether {@code key} is among the keys added before the last {@link #seal}. */
    private boolean names(byte[] key) {
        return Arrays.binarySearch(hashes, 0, sealedCount, keyHash(key)) >= 0;
    }

    /** A 64-bit hash of an encoded key. */
    private static long keyHash(byte[] key) {
        long h = 0xcbf29ce484222325L;
        for (byte b : key) {
            h = (h ^ (b & 0xFF)) * 0x100000001b3L;
        }
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        return h ^ (h >>> 33);
    }
}
