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

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.kv.dv.DvRestoredChangeList.Reason;
import org.apache.fluss.server.kv.snapshot.DvChangeListCheckpoints;
import org.apache.fluss.server.kv.snapshot.DvChangeListHandle;
import org.apache.fluss.server.kv.snapshot.KvFileHandle;
import org.apache.fluss.server.kv.snapshot.KvFileHandleAndLocalPath;
import org.apache.fluss.server.kv.snapshot.PlaceholderKvFileHandler;
import org.apache.fluss.server.kv.snapshot.SnapshotFileScope;
import org.apache.fluss.server.kv.snapshot.SnapshotLocation;
import org.apache.fluss.server.utils.SnapshotUtil;

import io.github.fluss_contrib.rocksdb.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The owner's side of change-list checkpoints ({@link DvChangeListCheckpoints}): saves the bucket's
 * change list with each KV snapshot at the snapshot's offset X.
 *
 * <p>In the snapshot's synchronous phase, an owner step waits (in steps) until every change below X
 * is in the change list, then plans the segments: the ones that can be carried over, and one new
 * segment from where they end to X. A job on the change lists' share of the server's IO pool
 * ({@link DerivedDvRuntime#changeLists}, at most {@link DerivedDvRuntime#CHANGE_LIST_THREADS} at
 * once) writes the new segment into the KV snapshot's shared directory while the KV files upload.
 * When it starts, an owner step takes a lease of the RocksDB and a snapshot of it, if the change
 * list still holds every change the plan saves; the job holds both for as long as it reads them, so
 * a close waits for it rather than close the RocksDB under its cursor, and a job waiting for a
 * thread holds neither. The KV snapshot attaches the change list if it is ready within {@link
 * #ATTACH_WAIT_MS} after its KV files; else it attaches only the carried segments, as a part that
 * ends before X and keeps them referenced, and the late segment is carried by the next snapshot.
 *
 * <p>Carried segments are those of the part of the last KV snapshot the bucket committed, and a
 * late segment that no snapshot references yet: a later snapshot's commit registers its references
 * before the coordinator subsumes older snapshots, so neither can be deleted under it, whatever
 * number of snapshots the coordinator retains. A change list that is missing, replaced in the step,
 * behind X for {@link #PREPARE_WAIT_MS}, or a lake ahead of X leave the change list out.
 *
 * <p>Owner-confined but for {@link Export}, which the snapshot's threads and the job call. One KV
 * snapshot of a bucket runs at a time.
 */
final class DvOwnerChangeListExport {

    private static final Logger LOG = LoggerFactory.getLogger(DvOwnerChangeListExport.class);

    // how long the owner tries to apply the changes below the snapshot's offset
    static final long PREPARE_WAIT_MS = 5_000;
    // how long a KV snapshot waits for its change list after its KV files are uploaded
    static final long ATTACH_WAIT_MS = 1_000;
    // how long a job waits for the owner to take the RocksDB snapshot it reads
    private static final long START_WAIT_MS = 5_000;
    // the owner looks again this often while changes below the offset are not applied
    private static final long PREPARE_RETRY_MS = 10;
    // past this many segments, the next change list is saved as one segment again
    static final int MAX_SEGMENTS = 32;
    private static final int READ_CHUNK = 4096;

    private final DvOwner o;
    // the segments of the part the last KV snapshot the bucket committed holds, while they
    // describe the installed change list; null: none can be carried
    @Nullable private Saved saved;
    // a segment written for a KV snapshot that did not take it, which no snapshot references
    @Nullable private Late late;
    // the KV snapshot whose change list is being saved
    @Nullable private Export inFlight;
    // KV snapshots the bucket committed, or may have, since it took the leadership over
    private int committedSinceTakeover;
    // committed snapshots whose metadata is still being read back: nothing is carried meanwhile
    private int pendingCommits;
    // the latest snapshot whose commit, or possible commit, was recorded
    private long latestCommitted = Long.MIN_VALUE;
    // tests make them small
    volatile long prepareWaitMs = PREPARE_WAIT_MS;
    volatile long attachWaitMs = ATTACH_WAIT_MS;

    DvOwnerChangeListExport(DvOwner o) {
        this.o = o;
    }

    /** Segments a committed KV snapshot references, and the change list they describe. */
    private static final class Saved {
        final long listId;
        final List<DvChangeListHandle.Segment> segments;
        final long end;

        Saved(long listId, List<DvChangeListHandle.Segment> segments, long end) {
            this.listId = listId;
            this.segments = segments;
            this.end = end;
        }
    }

    /** A finished segment of a change list that no KV snapshot references. */
    private static final class Late {
        final long listId;
        final DvChangeListHandle.Segment segment;

        Late(long listId, DvChangeListHandle.Segment segment) {
            this.listId = listId;
            this.segment = segment;
        }
    }

    /** A carried segment, and whether the part that carries it owns its file (a late one). */
    private static final class Carried {
        final DvChangeListHandle.Segment segment;
        final boolean owned;

        Carried(DvChangeListHandle.Segment segment, boolean owned) {
            this.segment = segment;
            this.owned = owned;
        }
    }

    /**
     * Starts saving the change list of KV snapshot {@code kvSnapshotId} at {@code x} into {@code
     * location}; any thread.
     */
    Export prepare(long kvSnapshotId, long x, SnapshotLocation location) {
        Export export = new Export(kvSnapshotId, x, location);
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(prepareWaitMs);
        o.post(() -> prepareStep(export, deadline));
        return export;
    }

    /**
     * The change list a load installed from the restored one: its segments from the readable offset
     * on describe it up to {@code end}. They are carried while the restored KV snapshot is the last
     * one the bucket committed, so no subsumption can delete them before this leader's own part
     * references them. An owner step.
     */
    void seed(DvChangeList list, List<DvChangeListHandle.Segment> segments, long end) {
        if (committedSinceTakeover == 0 && !segments.isEmpty()) {
            saved = new Saved(list.id, Collections.unmodifiableList(segments), end);
        }
    }

    /**
     * Abandons the export in flight, whose job ends at its next chunk and releases what it holds,
     * deletes a late segment no snapshot references, and releases the restored change list, as the
     * bucket closes. An owner step.
     */
    void close() {
        Export export = inFlight;
        inFlight = null;
        if (export != null) {
            export.abandon();
        }
        Late l = late;
        late = null;
        if (l != null) {
            discardLater(l.segment);
        }
        if (o.recoverySource != null) {
            DvRestoredChangeList restored = o.recoverySource.restoredChangeList();
            if (restored != null) {
                restored.release(Reason.CLOSED);
            }
        }
    }

    // ---- owner steps ----------------------------------------------------------------------------

    private void prepareStep(Export export, long deadlineNanos) {
        String skip = skipReason(export);
        if (skip != null) {
            export.planned(Plan.skip(skip));
            return;
        }
        DvChangeList before = o.maskState.tail;
        o.drain.drainHandOff();
        if (o.handedOffEnd < export.x || o.maskState.tail.hasBelow(export.x)) {
            o.maskState.applyUpTo(o.highWatermark.getAsLong());
        }
        // The drain may have lost the change list (an overflow, a failed hand-off), and the apply
        // may have dropped it (a disk failure): plan from the change list the step leaves.
        if (o.maskState.tailMissing() || o.maskState.tail != before) {
            export.planned(Plan.skip("missing"));
            return;
        }
        DvChangeList list = o.maskState.tail;
        if (o.handedOffEnd < export.x || list.hasBelow(export.x)) {
            if (System.nanoTime() >= deadlineNanos) {
                export.planned(Plan.skip("not_applied"));
                return;
            }
            try {
                o.runtime
                        .scheduler()
                        .schedule(
                                () -> o.post(() -> prepareStep(export, deadlineNanos)),
                                PREPARE_RETRY_MS,
                                TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                export.planned(Plan.skip("closed"));
            }
            return;
        }
        inFlight = export;
        export.planned(plan(list, export.x));
    }

    @Nullable
    private String skipReason(Export export) {
        if (o.closed || export.abandoned) {
            return "closed";
        }
        if (o.rowTtl) {
            return "ttl";
        }
        if (o.recoverySource == null || o.maskState.tailMissing()) {
            return "missing";
        }
        if (export.x < o.snapshotStartOffset) {
            return "lake_ahead";
        }
        return null;
    }

    /** Plans the segments of {@code list} up to {@code x}. */
    private Plan plan(DvChangeList list, long x) {
        long readable = o.snapshotStartOffset;
        List<Carried> carry = new ArrayList<>();
        long carryEnd = readable;
        // while a commit's metadata is read back, the segments it references are not known
        Saved s = pendingCommits > 0 ? null : saved;
        if (s != null && s.listId == list.id && s.end <= x) {
            for (DvChangeListHandle.Segment segment : s.segments) {
                if (segment.end() > readable) {
                    carry.add(new Carried(segment, false));
                    carryEnd = segment.end();
                }
            }
        }
        Late l = late;
        late = null;
        if (l != null) {
            DvChangeListHandle.Segment segment = l.segment;
            boolean follows =
                    carry.isEmpty()
                            ? segment.from() <= readable && segment.end() > readable
                            : segment.from() == carryEnd;
            if (l.listId == list.id && segment.end() <= x && follows) {
                carry.add(new Carried(segment, true));
                carryEnd = segment.end();
            } else {
                discardLater(segment);
            }
        }
        if (carry.size() >= MAX_SEGMENTS) {
            // the next part is saved whole; a late segment no part takes is deleted
            for (Carried c : carry) {
                if (c.owned) {
                    discardLater(c.segment);
                }
            }
            carry.clear();
            carryEnd = readable;
        }
        return new Plan(list, o.coveredSnapshotId, carry, carryEnd, x);
    }

    /**
     * Records how the KV snapshot of {@code export} ended, and what becomes of its new segment once
     * its job wrote it; an owner step, run on each of the two.
     */
    private void settle(Export export) {
        Outcome outcome = export.outcome;
        if (outcome == null) {
            return; // the snapshot's outcome comes later
        }
        if (!export.outcomeTaken) {
            export.outcomeTaken = true;
            takeOutcome(export, outcome);
        }
        if (outcome == Outcome.COMMITTED && !export.commitResolved) {
            if (!export.keptRead) {
                return; // the committed metadata is still being read back
            }
            export.commitResolved = true;
            resolveCommit(export, export.keptResult);
        }
        Written written = export.result;
        if (written == null || export.writtenTaken) {
            return; // the job is still running, or done with already
        }
        export.writtenTaken = true;
        DvChangeListHandle.Segment segment = written.segment;
        if (segment == null || export.attachedWhole) {
            return; // nothing new, or the snapshot's part holds it
        }
        Plan plan = export.plan.getNow(null);
        boolean follows =
                saved != null
                        ? saved.listId == plan.listId && segment.from() == saved.end
                        : plan.carry.isEmpty();
        boolean committed = outcome == Outcome.COMMITTED && export.keptResult != null;
        if (committed && !o.closed && late == null && follows) {
            // late for its own snapshot: the next one carries it
            late = new Late(plan.listId, segment);
        } else {
            discardLater(segment);
        }
    }

    private void takeOutcome(Export export, Outcome outcome) {
        if (inFlight == export) {
            inFlight = null;
        }
        DvChangeListHandle part = export.attached;
        Plan plan = export.plan.getNow(null);
        if (part == null && plan != null) {
            // a late segment the plan carried, which no snapshot took: nothing references it
            for (Carried c : plan.carry) {
                if (c.owned) {
                    discardLater(c.segment);
                }
            }
        }
        switch (outcome) {
            case COMMITTED:
                committedSinceTakeover++;
                if (part == null) {
                    // nothing attached: nothing to read back
                    export.keptResult = Boolean.TRUE;
                    export.keptRead = true;
                } else {
                    // Whether the committed metadata holds the part is read back off the
                    // snapshot's thread; until then, the next snapshots carry nothing.
                    pendingCommits++;
                    export.readBack();
                }
                break;
            case ABORTED:
                // The snapshot's new files are deleted with it; the last committed one's part
                // is still the one to carry.
                break;
            case UNCERTAIN:
            default:
                // The snapshot may have been committed, and subsumed the restored one: no seed.
                // It may hold the part or not: carry nothing of either, unless a later snapshot
                // was committed since.
                committedSinceTakeover++;
                if (newer(export)) {
                    saved = null;
                }
                break;
        }
    }

    /**
     * Records a committed snapshot once its metadata was read back: {@code kept} whether it holds
     * the part attached, or null if it could not be read, which counts as uncertain. The part's
     * segments are carried from then on, unless the outcome of a later snapshot came first.
     */
    private void resolveCommit(Export export, @Nullable Boolean kept) {
        DvChangeListHandle part = export.attached;
        Plan plan = export.plan.getNow(null);
        if (part != null) {
            pendingCommits--;
        }
        boolean newer = newer(export);
        if (kept == null) {
            if (newer) {
                saved = null;
            }
            return;
        }
        if (part != null && !kept) {
            // The coordinator dropped the part (it does not know the field): no snapshot
            // references the files the part owns, so they go, and nothing is carried.
            LOG.warn(
                    "The committed KV snapshot {} of {} does not hold the deletion vector"
                            + " change list attached to it; its new segments are deleted."
                            + " Is the coordinator older than the tablet server?",
                    export.kvSnapshotId,
                    o.logName);
            for (DvChangeListHandle.Segment s : part.segments()) {
                if (!(s.file().getKvFileHandle() instanceof PlaceholderKvFileHandler)) {
                    discardLater(s);
                }
            }
            if (newer) {
                saved = null;
            }
            return;
        }
        if (newer) {
            // the segments the snapshot references can be carried, also a late one it took
            saved =
                    part == null || plan == null || plan.skip != null
                            ? null
                            : new Saved(plan.listId, asCommitted(part), part.end());
        }
    }

    /**
     * Whether no later snapshot's commit, or possible commit, was recorded before this one's; makes
     * this one the latest. An older one's outcome never changes what is carried.
     */
    private boolean newer(Export export) {
        if (export.kvSnapshotId <= latestCommitted) {
            return false;
        }
        latestCommitted = export.kvSnapshotId;
        return true;
    }

    /** The segments of a committed part, as plain handles to carry: no longer references. */
    private static List<DvChangeListHandle.Segment> asCommitted(DvChangeListHandle part) {
        List<DvChangeListHandle.Segment> segments = new ArrayList<>();
        for (DvChangeListHandle.Segment s : part.segments()) {
            KvFileHandle h = s.file().getKvFileHandle();
            segments.add(
                    new DvChangeListHandle.Segment(
                            KvFileHandleAndLocalPath.of(
                                    new KvFileHandle(h.getFilePath(), h.getSize()),
                                    s.file().getLocalPath()),
                            s.from(),
                            s.end(),
                            s.entries()));
        }
        return Collections.unmodifiableList(segments);
    }

    /** Deletes a segment no KV snapshot references, on the IO pool; best effort. */
    private void discardLater(DvChangeListHandle.Segment segment) {
        KvFileHandle file = segment.file().getKvFileHandle();
        try {
            o.runtime.changeLists().execute(() -> SnapshotUtil.discardKvFileQuietly(file));
        } catch (RejectedExecutionException e) {
            // the server shuts down: the file stays, as an orphan of a snapshot would
        }
    }

    // ---- the plan, the job, and the export the KV snapshot's threads call ---------------------

    /** How a KV snapshot ended. */
    private enum Outcome {
        COMMITTED,
        ABORTED,
        UNCERTAIN
    }

    /** What a KV snapshot saves, taken by the owner; or why it saves nothing. */
    static final class Plan {
        @Nullable final String skip;
        // the change list the new segment is read from; owner-confined
        @Nullable private final DvChangeList list;
        final long listId;
        final long validFrom;
        final long lakeSnapshotId;
        final List<Carried> carry;
        // where the new segment starts: the end of the carried ones
        final long newFrom;
        final long x;
        // The RocksDB snapshot, with a lease of the RocksDB, from when the job starts until it
        // releases both: a job waiting for a thread holds neither.
        @Nullable private Snapshot snapshot;
        // the job stopped waiting for the owner to take them, or why the owner did not
        private boolean startCancelled;
        @Nullable private String startFailure;

        private Plan(
                DvChangeList list, long lakeSnapshotId, List<Carried> carry, long newFrom, long x) {
            this.skip = null;
            this.list = list;
            this.listId = list.id;
            this.validFrom = list.base;
            this.lakeSnapshotId = lakeSnapshotId;
            this.carry = carry;
            this.newFrom = newFrom;
            this.x = x;
        }

        private Plan(String skip) {
            this.skip = skip;
            this.list = null;
            this.listId = -1;
            this.validFrom = -1;
            this.lakeSnapshotId = -1;
            this.carry = Collections.emptyList();
            this.newFrom = -1;
            this.x = -1;
        }

        static Plan skip(String reason) {
            return new Plan(reason);
        }

        /** Where the change list the plan saves starts. */
        long from() {
            return carry.isEmpty() ? newFrom : carry.get(0).segment.from();
        }

        /**
         * Takes the lease and the RocksDB snapshot the job reads the new segment through, if the
         * change list still holds every change the plan saves: it is still the bucket's, and no
         * newer lake snapshot trimmed it past the new segment's start. An owner step.
         */
        synchronized void take(DvOwner o, boolean abandoned) {
            if (startCancelled) {
                return;
            }
            if (o.closed || abandoned) {
                startFailure = "the change list's export was abandoned";
                return;
            }
            if (o.maskState.tailMissing()
                    || o.maskState.tail != list
                    || list.retainedFrom() > newFrom) {
                startFailure = "the change list changed before its export started";
                return;
            }
            if (!o.leases.tryAcquire()) {
                startFailure = "the bucket closed";
                return;
            }
            try {
                snapshot = o.db.snapshot();
            } catch (RuntimeException e) {
                o.leases.release();
                startFailure = "could not take a snapshot of the RocksDB: " + e;
            }
        }

        /**
         * The snapshot the owner took for the job, or null with {@link #startFailure} set; once the
         * job stops waiting, a later {@link #take} takes nothing.
         */
        @Nullable
        synchronized Snapshot started() {
            startCancelled = true;
            if (snapshot == null && startFailure == null) {
                startFailure = "the owner did not start the export in time";
            }
            return snapshot;
        }

        @Nullable
        synchronized String startFailure() {
            return startFailure;
        }

        /** Releases the RocksDB snapshot and its lease; later calls do nothing. */
        synchronized void release(DvOwner o) {
            Snapshot s = snapshot;
            snapshot = null;
            if (s != null) {
                try {
                    o.db.releaseSnapshot(s);
                } finally {
                    o.leases.release();
                }
            }
        }
    }

    /** What the job wrote: the new segment, or null if nothing was new; or its failure. */
    private static final class Written {
        @Nullable final DvChangeListHandle.Segment segment;
        @Nullable final Throwable failure;

        Written(@Nullable DvChangeListHandle.Segment segment, @Nullable Throwable failure) {
            this.segment = segment;
            this.failure = failure;
        }
    }

    /** The change list of one KV snapshot. */
    final class Export implements DvChangeListCheckpoints.Checkpoint {
        final long kvSnapshotId;
        final long x;
        private final SnapshotLocation location;
        final long preparedAtNanos = System.nanoTime();
        final CompletableFuture<Plan> plan = new CompletableFuture<>();
        final CompletableFuture<Written> written = new CompletableFuture<>();
        // what the job wrote, set before it posts the settle step and completes {@link #written}
        @Nullable private volatile Written result;
        // the bucket closed or the snapshot ended: the job stops at its next chunk
        volatile boolean abandoned;
        // what attach returned, and whether it was the whole change list
        @Nullable private volatile DvChangeListHandle attached;
        private volatile boolean attachedWhole;
        @Nullable private volatile Outcome outcome;
        // reads back whether the committed metadata holds the part attach returned
        @Nullable private volatile Callable<Boolean> keptReader;
        // what it read: null if it could not; set before the settle step is posted
        @Nullable private volatile Boolean keptResult;
        private volatile boolean keptRead;
        // completes once the read-back's settle step is posted
        final CompletableFuture<Void> readBackPosted = new CompletableFuture<>();
        private volatile long planNanos = -1;
        private volatile long writeNanos;
        private volatile long uploadNanos;
        // when the job finished, for the latency from the prepare to the upload
        private volatile long writtenAtNanos;
        // owner-confined
        private boolean outcomeTaken;
        private boolean commitResolved;
        private boolean writtenTaken;

        Export(long kvSnapshotId, long x, SnapshotLocation location) {
            this.kvSnapshotId = kvSnapshotId;
            this.x = x;
            this.location = location;
        }

        void abandon() {
            abandoned = true;
        }

        /** The owner planned the export: the job writes the new segment; an owner step. */
        void planned(Plan p) {
            planNanos = System.nanoTime() - preparedAtNanos;
            plan.complete(p);
            if (p.skip != null) {
                written.complete(new Written(null, null));
                return;
            }
            try {
                o.runtime.changeLists().execute(() -> write(p));
            } catch (RejectedExecutionException e) {
                written.complete(new Written(null, e));
            }
        }

        /**
         * Writes the plan's new segment into the KV snapshot's shared directory, on the IO pool,
         * through the plan's RocksDB snapshot, holding its lease until done.
         */
        private void write(Plan p) {
            DvChangeListHandle.Segment segment = null;
            Throwable failure = null;
            SnapshotLocation.FsSnapshotOutputStream out = null;
            long start = System.nanoTime();
            try {
                if (abandoned || o.closed) {
                    throw new IOException("the change list's export was abandoned");
                }
                if (p.newFrom < x) {
                    Snapshot snapshot = start(p);
                    out = location.createSnapshotOutputStream(SnapshotFileScope.SHARED);
                    TableBucket b = o.tableBucket;
                    DvChangeListSegment.Writer writer =
                            new DvChangeListSegment.Writer(
                                    out,
                                    new DvChangeListSegment.Header(
                                            b.getTableId(),
                                            b.getPartitionId() == null ? -1 : b.getPartitionId(),
                                            b.getBucket(),
                                            p.newFrom,
                                            x,
                                            p.validFrom));
                    try (DvChangeListStore.PendingCursor cursor =
                            o.db.changeListStore()
                                    .pendingCursor(p.listId, snapshot, p.newFrom, x)) {
                        boolean more = true;
                        while (more) {
                            if (abandoned || o.closed) {
                                throw new IOException("the change list's export was abandoned");
                            }
                            more = cursor.next(READ_CHUNK, writer::add);
                        }
                    }
                    long bytes = writer.finish();
                    long uploadStart = System.nanoTime();
                    KvFileHandle file = out.closeAndGetHandle();
                    out = null;
                    uploadNanos = System.nanoTime() - uploadStart;
                    if (file == null) {
                        throw new IOException("the segment's stream closed without a file");
                    }
                    // the size the segment's reader checks: what was written, not a best guess
                    segment =
                            new DvChangeListHandle.Segment(
                                    KvFileHandleAndLocalPath.of(
                                            new KvFileHandle(file.getFilePath(), bytes),
                                            new FsPath(file.getFilePath()).getName()),
                                    p.newFrom,
                                    x,
                                    writer.entries());
                }
            } catch (Throwable t) {
                failure = t;
                if (out != null) {
                    // deletes what the stream wrote
                    out.close();
                }
            } finally {
                // the cursor is closed: the RocksDB may close now
                p.release(o);
                writeNanos = System.nanoTime() - start - uploadNanos;
            }
            Written w = new Written(segment, failure);
            writtenAtNanos = System.nanoTime();
            result = w;
            // the settle step is posted before anyone sees the job done: a later step sees it
            o.post(() -> settle(this));
            written.complete(w);
            if (failure instanceof VirtualMachineError) {
                throw (VirtualMachineError) failure;
            }
        }

        /**
         * Has the owner take the lease and the RocksDB snapshot for the job, now that it starts,
         * and waits for that step, {@link #START_WAIT_MS} at most.
         */
        private Snapshot start(Plan p) throws IOException {
            CompletableFuture<Void> taken = new CompletableFuture<>();
            o.post(
                    () -> {
                        try {
                            p.take(o, abandoned);
                        } finally {
                            taken.complete(null);
                        }
                    });
            boolean interrupted = false;
            try {
                taken.get(START_WAIT_MS, TimeUnit.MILLISECONDS);
            } catch (TimeoutException | ExecutionException e) {
                // started() below stops a later step from taking them
            } catch (InterruptedException e) {
                interrupted = true;
            }
            // the plan's release, in the job's finally, releases what the owner took
            Snapshot snapshot = p.started();
            if (interrupted) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException("the change list's export was interrupted");
            }
            if (snapshot == null) {
                throw new IOException(p.startFailure());
            }
            return snapshot;
        }

        @Nullable
        @Override
        public DvChangeListHandle attach() {
            long waitStart = System.nanoTime();
            try {
                long deadline = waitStart + TimeUnit.MILLISECONDS.toNanos(attachWaitMs);
                Plan p;
                try {
                    p = plan.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                } catch (TimeoutException e) {
                    // no plan yet: the snapshot goes on without it, and no job starts for it
                    abandon();
                    return skipped("late", null, waitStart);
                }
                if (p.skip != null) {
                    return skipped(p.skip, null, waitStart);
                }
                Written w;
                try {
                    w =
                            written.get(
                                    Math.max(0, deadline - System.nanoTime()),
                                    TimeUnit.NANOSECONDS);
                } catch (TimeoutException e) {
                    return skipped("late", p, waitStart);
                }
                if (w.failure != null) {
                    LOG.warn(
                            "Could not save the deletion vector change list of {} with KV"
                                    + " snapshot {}.",
                            o.logName,
                            kvSnapshotId,
                            w.failure);
                    return skipped("failed", p, waitStart);
                }
                DvChangeListHandle part = part(p, w.segment, x);
                attached = part;
                attachedWhole = true;
                long bytesNew =
                        w.segment == null ? 0 : w.segment.file().getKvFileHandle().getSize();
                long entriesNew = w.segment == null ? 0 : w.segment.entries();
                o.metrics.changeListCheckpointed(
                        (writtenAtNanos - preparedAtNanos) / 1_000_000, bytesNew);
                log(part, p, p.carry.size(), entriesNew, bytesNew, waitStart, "attached", "-");
                return part;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                abandon();
                return skipped("closed", null, waitStart);
            } catch (VirtualMachineError e) {
                throw e;
            } catch (Throwable t) {
                LOG.warn(
                        "Could not save the deletion vector change list of {} with KV snapshot {}.",
                        o.logName,
                        kvSnapshotId,
                        t);
                attached = null;
                attachedWhole = false;
                return null;
            }
        }

        /**
         * Leaves the change list out of the snapshot: attaches only the plan's carried segments, if
         * any, as a part that ends before X and keeps them referenced for the next snapshot.
         */
        @Nullable
        private DvChangeListHandle skipped(String reason, @Nullable Plan p, long waitStart) {
            o.metrics.changeListCheckpointSkipped();
            DvChangeListHandle carry =
                    p == null || p.skip != null || p.carry.isEmpty() || o.closed
                            ? null
                            : part(p, null, p.newFrom);
            attached = carry;
            attachedWhole = false;
            log(carry, p, carry == null ? 0 : p.carry.size(), 0, 0, waitStart, "skipped", reason);
            return carry;
        }

        @Override
        public void committed(Callable<Boolean> changeListKept) {
            keptReader = changeListKept;
            outcome = Outcome.COMMITTED;
            o.post(() -> settle(this));
        }

        /** Reads back the committed metadata on the change lists' lane, then settles again. */
        private void readBack() {
            Callable<Boolean> reader = keptReader;
            try {
                o.runtime
                        .changeLists()
                        .execute(
                                () -> {
                                    Boolean kept = null;
                                    try {
                                        kept = reader.call();
                                    } catch (Exception e) {
                                        LOG.warn(
                                                "Could not read back the committed metadata of KV"
                                                        + " snapshot {} of {}; its deletion vector"
                                                        + " change list is not built on.",
                                                kvSnapshotId,
                                                o.logName,
                                                e);
                                    }
                                    readBackDone(kept);
                                });
            } catch (RejectedExecutionException e) {
                // the server shuts down
                readBackDone(null);
            }
        }

        private void readBackDone(@Nullable Boolean kept) {
            keptResult = kept;
            keptRead = true;
            o.post(() -> settle(this));
            readBackPosted.complete(null);
        }

        @Override
        public void aborted() {
            outcome = Outcome.ABORTED;
            abandon();
            o.post(() -> settle(this));
        }

        @Override
        public void uncertain() {
            outcome = Outcome.UNCERTAIN;
            abandon();
            o.post(() -> settle(this));
        }

        /**
         * The part of the plan's carried segments and {@code segment} up to {@code end}: carried
         * segments of a committed snapshot as references, a late one and the new one owned.
         */
        private DvChangeListHandle part(
                Plan p, @Nullable DvChangeListHandle.Segment segment, long end) {
            List<DvChangeListHandle.Segment> segments = new ArrayList<>();
            long entries = 0;
            for (Carried c : p.carry) {
                segments.add(c.owned ? c.segment : c.segment.asReference());
                entries += c.segment.entries();
            }
            if (segment != null) {
                segments.add(segment);
                entries += segment.entries();
            }
            // no change below X since the readable offset, which is X: an empty part
            long from = segments.isEmpty() ? end : segments.get(0).from();
            return new DvChangeListHandle(
                    from, end, p.validFrom, p.lakeSnapshotId, entries, segments);
        }

        private void log(
                @Nullable DvChangeListHandle part,
                @Nullable Plan p,
                int carried,
                long entriesNew,
                long bytesNew,
                long waitStart,
                String outcomeName,
                String reason) {
            LOG.info(
                    "Checkpointed the deletion vector change list of {} with KV snapshot {}:"
                            + " end={} from={} valid_from={} lake_snapshot={} full={} segments={}"
                            + " segments_reused={} entries_new={} entries_total={} bytes_new={}"
                            + " bytes_total={} wait_ms={} write_ms={} upload_ms={} plan_ms={}"
                            + " outcome={} reason={}",
                    o.logName,
                    kvSnapshotId,
                    x,
                    part == null ? "-" : part.from(),
                    p == null || p.skip != null ? "-" : p.validFrom,
                    p == null || p.skip != null ? "-" : p.lakeSnapshotId,
                    p == null || p.skip != null ? "-" : p.carry.isEmpty(),
                    part == null ? 0 : part.segments().size(),
                    carried,
                    entriesNew,
                    part == null ? 0 : part.entries(),
                    bytesNew,
                    part == null ? 0 : part.bytes(),
                    (System.nanoTime() - waitStart) / 1_000_000,
                    writeNanos / 1_000_000,
                    uploadNanos / 1_000_000,
                    planNanos < 0 ? -1 : planNanos / 1_000_000,
                    outcomeName,
                    reason);
        }
    }
}
