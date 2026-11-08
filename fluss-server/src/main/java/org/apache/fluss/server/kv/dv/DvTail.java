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
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * The changes of a log tail: the changes the high watermark has not passed, the updates and deletes
 * the high watermark passed, and the versions they superseded.
 *
 * <p>Each change names the version it supersedes, the offset its KV value is tagged with. A change
 * whose superseded version is in the tail masks that version in the log. One whose superseded
 * version is before the tail, in the lake, masks the key's lake row, so its key is looked up in the
 * index. Applying a change therefore reads nothing.
 *
 * <p>The updates and deletes are kept in log order until a newer lake snapshot holds them, to find
 * the keys whose lake rows that snapshot holds. They are held in memory, or spilled to the bucket's
 * RocksDB ({@link TailStore}) once the tablet server's tail memory is exceeded. An advance removes
 * the ones the new snapshot holds with one range delete.
 */
final class DvTail {

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
    static final int SPILL_WRITE_BATCH = 4096;

    final ArrayDeque<DvEntry> queue = new ArrayDeque<>();
    final Roaring64Bitmap deadVersions = new Roaring64Bitmap();
    // the pending changes in log order; null once spilled
    @Nullable private ArrayDeque<Pending> pending = new ArrayDeque<>();
    private long pendingCount;
    // where the pending changes are once spilled
    @Nullable private TailStore store;
    // the id of the spilled changes in the store
    long id = -1;
    private long spilledBytes;
    // runs of spilled pending changes {last offset, changes, bytes}, to count what a trim removes
    private final ArrayDeque<long[]> spilledRuns = new ArrayDeque<>();
    // estimated heap bytes of the in-memory pending changes and of the queued changes
    private long pendingBytes;
    private long queueBytes;
    // the record offset of the last change applied; changes arrive in log order
    private long lastApplied = -1;

    /** The pending updates and deletes. */
    long changeCount() {
        return pendingCount;
    }

    /** The pending changes and the changes waiting for the high watermark. */
    long size() {
        return pendingCount + queue.size();
    }

    boolean spilled() {
        return pending == null;
    }

    long spilledBytes() {
        return spilledBytes;
    }

    /** Estimated heap bytes of the pending and queued changes, without the bitmaps. */
    long heapBytes() {
        return pendingBytes + queueBytes;
    }

    /** Estimated heap bytes of the in-memory pending changes, which a spill moves to disk. */
    long pendingHeapBytes() {
        return pendingBytes;
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
     * tail, and the copy would come after later changes.
     */
    void applyUpTo(long hw, long startOffset, @Nullable List<ByteBuffer> newKeys)
            throws IOException {
        List<Pending> toDisk = pending == null ? new ArrayList<>() : null;
        long bytes = 0;
        while (hasBelow(hw)) {
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
            Pending p = new Pending(e.getRecordOffset(), previous, e.getKey());
            pendingCount++;
            if (toDisk == null) {
                pending.add(p);
                pendingBytes += pendingBytes(e.getKey());
            } else {
                toDisk.add(p);
                bytes += TailStore.storedPendingBytes(e.getKey());
                if (toDisk.size() >= SPILL_WRITE_BATCH) {
                    writeRun(toDisk, bytes);
                    toDisk.clear();
                    bytes = 0;
                }
            }
            if (previous >= startOffset) {
                deadVersions.addLong(previous);
            } else if (newKeys != null) {
                newKeys.add(ByteBuffer.wrap(e.getKey()));
            }
        }
        if (toDisk != null && !toDisk.isEmpty()) {
            writeRun(toDisk, bytes);
        }
    }

    private void writeRun(List<Pending> run, long bytes) throws IOException {
        store.writePending(id, run);
        spilledRuns.add(new long[] {run.get(run.size() - 1).offset, run.size(), bytes});
        spilledBytes += bytes;
    }

    /**
     * The updates and deletes at or after {@code offset}, applied or queued: what the tail keeps
     * once a move to a snapshot readable from {@code offset} removes the rest. A spilled run partly
     * below the offset counts whole.
     */
    long changesFrom(long offset) {
        long n = 0;
        if (pending != null) {
            Iterator<Pending> it = pending.descendingIterator();
            while (it.hasNext() && it.next().offset >= offset) {
                n++;
            }
        } else {
            for (long[] run : spilledRuns) {
                if (run[0] >= offset) {
                    n += run[1];
                }
            }
        }
        for (DvEntry e : queue) {
            if (!e.isInsert() && e.getRecordOffset() >= offset) {
                n++;
            }
        }
        return n;
    }

    /**
     * The keys whose rows in the lake snapshot readable from {@code startOffset} are masked: those
     * of the pending changes whose superseded version is before it. An in-memory tail only.
     */
    List<ByteBuffer> lakeKeys(long startOffset) {
        return lakeKeys(startOffset, null);
    }

    /** As {@link #lakeKeys(long)}, only the keys {@code filter} accepts. */
    List<ByteBuffer> lakeKeys(long startOffset, @Nullable Predicate<byte[]> filter) {
        Set<ByteBuffer> out = new LinkedHashSet<>();
        for (Pending p : pending) {
            if (p.previous < startOffset && (filter == null || filter.test(p.key))) {
                out.add(ByteBuffer.wrap(p.key));
            }
        }
        return new ArrayList<>(out);
    }

    /**
     * The keys of the pending changes before {@code endOffset} whose superseded version is before
     * {@code startOffset}: the lake rows a move to a snapshot readable from {@code endOffset} stops
     * masking for them. An in-memory tail only.
     */
    List<byte[]> lakeKeysBefore(long endOffset, long startOffset) {
        List<byte[]> out = new ArrayList<>();
        for (Pending p : pending) {
            if (p.offset >= endOffset) {
                break;
            }
            if (p.previous < startOffset) {
                out.add(p.key);
            }
        }
        return out;
    }

    /** Moves the in-memory pending changes to {@code store} under {@code tailId}. */
    void spill(TailStore store, long tailId) throws IOException {
        if (pending == null) {
            return;
        }
        this.store = store;
        this.id = tailId;
        List<Pending> run = new ArrayList<>();
        long bytes = 0;
        try {
            for (Pending p : pending) {
                run.add(p);
                bytes += TailStore.storedPendingBytes(p.key);
                if (run.size() >= SPILL_WRITE_BATCH) {
                    writeRun(run, bytes);
                    run.clear();
                    bytes = 0;
                }
            }
            if (!run.isEmpty()) {
                writeRun(run, bytes);
            }
        } catch (IOException e) {
            // the tail stays in memory, so nothing would drop the runs written so far
            try {
                store.drop(tailId);
            } catch (IOException dropFailure) {
                e.addSuppressed(dropFailure);
            }
            spilledRuns.clear();
            spilledBytes = 0;
            throw e;
        }
        this.pending = null;
        this.pendingBytes = 0;
    }

    /** Removes the spilled changes from disk; nothing for an in-memory tail. */
    void drop() throws IOException {
        if (store != null) {
            store.drop(id);
            store = null;
        }
    }

    /**
     * Removes the pending changes before {@code offset}, which a newer lake snapshot holds, and the
     * superseded versions before it. A spilled tail removes them with one range delete.
     */
    void retainFrom(long offset) throws IOException {
        if (pending != null) {
            while (!pending.isEmpty() && pending.peekFirst().offset < offset) {
                Pending p = pending.pollFirst();
                pendingCount--;
                pendingBytes -= pendingBytes(p.key);
            }
        } else {
            store.deletePendingBelow(id, offset);
            // a run partly below the offset stays counted until a later trim
            while (!spilledRuns.isEmpty() && spilledRuns.peekFirst()[0] < offset) {
                long[] run = spilledRuns.pollFirst();
                pendingCount -= run[1];
                spilledBytes -= run[2];
            }
        }
        if (offset > 0 && !deadVersions.isEmpty()) {
            // one range operation instead of a pass over every dead version
            Roaring64Bitmap below = new Roaring64Bitmap();
            below.addRange(0, offset);
            deadVersions.andNot(below);
        }
    }

    // Estimated heap bytes, measured on a 64-bit JVM with compressed oops (see
    // DerivedDvTailSpillTest#testTailMemoryEstimateMatchesTheHeap). A pending change is a Pending
    // (32), its deque slot (8) and its key array. A queued change is a DvEntry (32), its deque
    // slot (8) and its key array. A key waiting for its lake position is a list slot (8).
    private static long pendingBytes(byte[] key) {
        return 40 + arrayBytes(key);
    }

    private static long queuedBytes(byte[] key) {
        return 40 + arrayBytes(key);
    }

    private static long arrayBytes(byte[] key) {
        return (16 + key.length + 7) & ~7L;
    }
}
