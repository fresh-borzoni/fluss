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

package org.apache.fluss.server.kv.dv;

import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The change list of the covered snapshot: the changes the high watermark has not passed, the
 * updates and deletes the high watermark passed, and the versions they superseded.
 *
 * <p>Each change names the version it supersedes, the offset its KV value is tagged with. A change
 * whose superseded version is in the change list masks that version in the log. One whose
 * superseded version is before the change list, in the lake, masks the key's lake row, so its key
 * is looked up in the index. Applying a change therefore reads nothing.
 *
 * <p>The updates and deletes are kept in log order in the bucket's RocksDB ({@link
 * DvChangeListStore}) until a newer lake snapshot holds them, to find the keys whose lake rows that
 * snapshot holds. RocksDB memtables and flushes bound the memory they take however far tiering
 * lags. A switch removes the ones the new snapshot holds with one range delete. Only the changes
 * the high watermark has not passed and the superseded versions in the log stay on the heap.
 */
final class DvChangeList {

    /** An update or delete, with the version it supersedes. */
    static final class Pending {
        final long offset;
        // the superseded version, or -1 if it is before the replay's start
        final long previous;
        final byte[] key;

        Pending(long offset, long previous, byte[] key) {
            this.offset = offset;
            this.previous = previous;
            this.key = key;
        }
    }

    // Pending changes written to the RocksDB in one batch.
    static final int WRITE_BATCH = 4096;

    final ArrayDeque<DvEntry> queue = new ArrayDeque<>();
    final Roaring64Bitmap deadVersions = new Roaring64Bitmap();
    // counts the changes of the dead versions, so a frontier build serializes them only when they
    // changed
    long deadVersionChanges;
    private final DvChangeListStore store;
    private final DvTestHooks testHooks;
    // the id of the change list's changes in the store, unique per bucket
    final long id;
    private long pendingCount;
    private long storedBytes;
    private boolean dropped;
    // runs of stored pending changes {last offset, changes, bytes}, to count what a trim removes
    private final ArrayDeque<long[]> storedRuns = new ArrayDeque<>();
    // estimated heap bytes of the queued changes
    private long queueBytes;
    // the record offset of the last change applied; changes arrive in log order
    private long lastApplied = -1;

    DvChangeList(DvChangeListStore store, long id, DvTestHooks testHooks) {
        this.store = store;
        this.id = id;
        this.testHooks = testHooks;
    }

    /** The pending updates and deletes. */
    long changeCount() {
        return pendingCount;
    }

    /**
     * The record offset of the last change applied, or -1: every pending change is at or before it.
     */
    long lastApplied() {
        return lastApplied;
    }

    /** The pending changes and the changes waiting for the high watermark. */
    long size() {
        return pendingCount + queue.size();
    }

    /** The bytes of the pending changes on disk, before compression. */
    long storedBytes() {
        return storedBytes;
    }

    /** Estimated heap bytes of the queued changes, without the bitmaps. */
    long heapBytes() {
        return queueBytes;
    }

    void enqueue(DvEntry e) {
        queue.add(e);
        queueBytes += queuedBytes(e.getKey());
    }

    void enqueueAll(List<DvEntry> entries) {
        for (DvEntry e : entries) {
            enqueue(e);
        }
    }

    /**
     * The offset of the first queued change below {@code hw} that needs its key looked up in the
     * lake once applied: an update or delete whose superseded version is before {@code
     * startOffset}. Returns {@code hw} if there is none. The queue is in log order.
     */
    long firstLakeChangeBelow(long hw, long startOffset) {
        for (DvEntry e : queue) {
            long offset = e.getRecordOffset();
            if (offset >= hw) {
                break;
            }
            if (offset < startOffset || offset <= lastApplied || e.isInsert()) {
                continue; // skipped when applied, or no superseded version
            }
            if (e.getPreviousVersionOffset() < startOffset) {
                return offset;
            }
        }
        return hw;
    }

    /** Whether changes below {@code hw} are queued. */
    boolean hasBelow(long hw) {
        return !queue.isEmpty() && queue.peekFirst().getRecordOffset() < hw;
    }

    /**
     * Applies the queued changes below {@code hw}. An update or delete masks the version it
     * supersedes if that is at or after {@code startOffset}; otherwise the superseded version is in
     * the lake, and its key goes to {@code newKeys} to look up. A change below {@code startOffset}
     * is in the covered snapshot already. A change at or before the last one applied is skipped: a
     * replay and the write path may both deliver a change written while the replay installs its
     * change list, and the copy would come after later changes.
     */
    void applyUpTo(long hw, long startOffset, @Nullable Collection<ByteBuffer> newKeys)
            throws IOException {
        applyUpTo(hw, startOffset, newKeys, Integer.MAX_VALUE);
    }

    /**
     * Applies at most {@code maxChanges} of the queued changes below {@code hw}, as {@link
     * #applyUpTo(long, long, Collection)} does. Returns whether changes below {@code hw} are still
     * queued.
     */
    boolean applyUpTo(
            long hw, long startOffset, @Nullable Collection<ByteBuffer> newKeys, int maxChanges)
            throws IOException {
        List<Pending> run = new ArrayList<>();
        long bytes = 0;
        int polled = 0;
        while (polled < maxChanges && hasBelow(hw)) {
            polled++;
            DvEntry e = queue.pollFirst();
            queueBytes -= queuedBytes(e.getKey());
            if (e.getRecordOffset() < startOffset || e.getRecordOffset() <= lastApplied) {
                continue; // in the covered snapshot, or applied already
            }
            lastApplied = e.getRecordOffset();
            if (e.isInsert()) {
                continue; // no superseded version
            }
            long previous = e.getPreviousVersionOffset();
            pendingCount++;
            run.add(new Pending(e.getRecordOffset(), previous, e.getKey()));
            bytes += DvChangeListStore.storedPendingBytes(e.getKey());
            if (run.size() >= WRITE_BATCH) {
                writeRun(run, bytes);
                run.clear();
                bytes = 0;
            }
            if (previous >= startOffset) {
                deadVersions.addLong(previous);
                deadVersionChanges++;
            } else if (newKeys != null) {
                newKeys.add(ByteBuffer.wrap(e.getKey()));
            }
        }
        if (!run.isEmpty()) {
            writeRun(run, bytes);
        }
        return hasBelow(hw);
    }

    private void writeRun(List<Pending> run, long bytes) throws IOException {
        testHooks.beforeChangeListWrite();
        store.writePending(id, run);
        storedRuns.add(new long[] {run.get(run.size() - 1).offset, run.size(), bytes});
        storedBytes += bytes;
    }

    /**
     * The updates and deletes at or after {@code offset}, applied or queued: what the change list
     * keeps once a switch to a snapshot readable from {@code offset} removes the rest. A stored run
     * partly below the offset counts whole.
     */
    long changesFrom(long offset) {
        long n = 0;
        for (long[] run : storedRuns) {
            if (run[0] >= offset) {
                n += run[1];
            }
        }
        for (DvEntry e : queue) {
            if (!e.isInsert() && e.getRecordOffset() >= offset) {
                n++;
            }
        }
        return n;
    }

    /** Removes the change list's changes from disk; later calls do nothing. */
    void drop() throws IOException {
        if (!dropped) {
            dropped = true;
            store.drop(id);
        }
    }

    /**
     * Removes the pending changes before {@code offset}, which a newer lake snapshot holds, with
     * one range delete, and the superseded versions before it.
     */
    void retainFrom(long offset) throws IOException {
        testHooks.beforeChangeListWrite();
        store.deletePendingBelow(id, offset);
        // a run partly below the offset stays counted until a later trim
        while (!storedRuns.isEmpty() && storedRuns.peekFirst()[0] < offset) {
            long[] run = storedRuns.pollFirst();
            pendingCount -= run[1];
            storedBytes -= run[2];
        }
        if (offset > 0 && !deadVersions.isEmpty()) {
            // one range operation instead of a pass over every dead version
            Roaring64Bitmap below = new Roaring64Bitmap();
            below.addRange(0, offset);
            deadVersions.andNot(below);
            deadVersionChanges++;
        }
    }

    // Estimated heap bytes, measured on a 64-bit JVM with compressed oops: a queued change is a
    // DvEntry (32), its deque slot (8) and its key array.
    static long queuedBytes(byte[] key) {
        return 40 + ((16 + key.length + 7) & ~7L);
    }
}
