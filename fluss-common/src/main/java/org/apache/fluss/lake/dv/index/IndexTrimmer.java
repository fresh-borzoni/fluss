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
import org.apache.fluss.lake.dv.index.PositionIndexStore.BucketData;
import org.apache.fluss.lake.dv.index.PositionIndexStore.ChainEntry;
import org.apache.fluss.lake.dv.index.PositionIndexStore.Checkpoint;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.lake.dv.index.IndexFiles.exists;
import static org.apache.fluss.lake.dv.index.IndexFiles.writeBytes;
import static org.apache.fluss.lake.dv.index.IndexLayout.CHAIN_DIR;
import static org.apache.fluss.lake.dv.index.IndexLayout.CKPT_SST;
import static org.apache.fluss.lake.dv.index.IndexLayout.TRIMS_DIR;
import static org.apache.fluss.lake.dv.index.IndexLayout.parseMarker;
import static org.apache.fluss.lake.dv.index.IndexLayout.partitionKey;
import static org.apache.fluss.lake.dv.index.IndexLayout.trimMarkerName;
import static org.apache.fluss.lake.dv.index.IndexLayout.trimMarkerSequence;
import static org.apache.fluss.lake.dv.index.IndexSsts.mergeIndex;

/**
 * Trims the chain of the {@link FlussPositionIndexStore}, and counts the changes registered since
 * the last trim began ({@code _trims/}).
 *
 * <p>The trim is the only thing that deletes checkpoints and chain entries. It lists the buckets'
 * checkpoints first and the chain after, keeps each bucket's newest complete checkpoint on the
 * chain, and cuts the chain below every one of them, so each bucket stays rebuildable from that
 * checkpoint and the kept changes. It deletes only checkpoints it listed: complete ones of
 * snapshots below its cut, and, once {@link IndexCheckpoints#STALE_MILLIS} old, incomplete ones and
 * ones of snapshots not on its chain. Anything written after its listing it leaves alone. Two trims
 * racing, as a paused service's and the new one's, both keep the newest checkpoint they listed, and
 * each cuts below its own, so the newest of all stays on the chain whichever deletes last.
 *
 * <p>A bucket without a checkpoint, or idle and checkpointed long ago, would hold the chain for
 * good, so the trim builds its checkpoint at the chain's end from its last one and the changes
 * since. A bucket whose rows changes trimmed off the chain hold, as its trim record lists, is never
 * built from the chain's start: without a checkpoint it stops the trim.
 *
 * <p>It deletes the checkpoints of dropped partitions, by id, which is never reused, and only of
 * partitions the caller does not list. It lists the partitions with checkpoints before the chain
 * and asks which are dropped after, so a partition created since its listing is never among them,
 * and it asks about all of them before it deletes anything.
 *
 * <p>A trim records what it deletes in {@code _trimmed} before it deletes chain entries, then
 * deletes the chain markers oldest first and their changes after them. A trim cut short leaves a
 * chain whose every entry has its change, and the next trim finishes it.
 */
final class IndexTrimmer {

    // A bucket whose last change is this many chain entries old is idle. Its index server
    // catches up only when a round writes the bucket, so the trim checkpoints it instead.
    static final int IDLE_ENTRIES = 50;
    // The trim builds checkpoints of idle buckets on a round's commit thread. Per trim it builds at
    // most this many and starts none after this long; later trims build the rest.
    static final int MAX_CHECKPOINT_BUILDS = 256;
    static final long MAX_CHECKPOINT_BUILD_MILLIS = 60_000;

    // the store's logger
    private static final Logger LOG = LoggerFactory.getLogger(FlussPositionIndexStore.class);

    private final IndexLayout layout;
    // the store's hooks: deletes and the reads of committed metadata
    private final FlussPositionIndexStore store;
    private final IndexChain indexChain;
    private final IndexChangeWriter writer;
    private final IndexChangeReader reader;
    private final IndexCheckpoints checkpoints;
    private final TrimRecords records;
    // local directory for temporary files, or null for the JVM's
    @Nullable private final File localTmpDir;
    private final TouchedBuckets touchedBuckets;

    IndexTrimmer(
            IndexLayout layout,
            FlussPositionIndexStore store,
            IndexChain indexChain,
            IndexChangeWriter writer,
            IndexChangeReader reader,
            IndexCheckpoints checkpoints,
            @Nullable File localTmpDir) {
        this.layout = layout;
        this.store = store;
        this.indexChain = indexChain;
        this.writer = writer;
        this.reader = reader;
        this.checkpoints = checkpoints;
        this.records = new TrimRecords(layout, store);
        this.touchedBuckets = new TouchedBuckets(store);
        this.localTmpDir = localTmpDir;
    }

    /** See {@link PositionIndexStore#trim}. */
    PositionIndexStore.TrimResult trim(
            PositionIndexStore.BucketsWithData bucketsWithData,
            int keepExtra,
            PositionIndexStore.DroppedPartitions dropped)
            throws IOException {
        return trim(
                bucketsWithData,
                keepExtra,
                dropped,
                IDLE_ENTRIES,
                MAX_CHECKPOINT_BUILDS,
                MAX_CHECKPOINT_BUILD_MILLIS);
    }

    /** See {@link PositionIndexStore#changesSinceTrim}. */
    long changesSinceTrim() throws IOException {
        long from = 0;
        for (FileStatus st : trimMarkers()) {
            Long sequence = trimMarkerSequence(st.getPath().getName());
            if (sequence != null) {
                from = Math.max(from, sequence);
            }
        }
        return Math.max(0, indexChain.lastSequence() - from);
    }

    /** See {@link PositionIndexStore#startTrim}. */
    void startTrim() throws IOException {
        // One marker per trim, under a name no other trim uses and with nothing in it: a write
        // cut short still names its sequence, and no marker is ever rewritten.
        long sequence = indexChain.lastSequence();
        writeBytes(
                new FsPath(layout.path(TRIMS_DIR), trimMarkerName(sequence)),
                new byte[0],
                FileSystem.WriteMode.OVERWRITE);
        // The newest marker is the one that counts; older ones go, best effort.
        for (FileStatus st : trimMarkers()) {
            Long older = trimMarkerSequence(st.getPath().getName());
            if (older != null && older < sequence) {
                try {
                    store.deletePath(st.getPath(), false);
                } catch (IOException e) {
                    LOG.debug("Could not delete the old trim marker {}.", st.getPath(), e);
                }
            }
        }
    }

    private List<FileStatus> trimMarkers() throws IOException {
        FsPath dir = layout.path(TRIMS_DIR);
        FileSystem fs = dir.getFileSystem();
        return fs.exists(dir) ? Arrays.asList(fs.listStatus(dir)) : Collections.emptyList();
    }

    /**
     * Trims the chain like {@link #trim(Collection, int)}, building checkpoints of buckets idle for
     * {@code idleEntries} chain entries, at most {@code maxBuilds} of them and none started after
     * {@code maxBuildMillis}.
     */
    private PositionIndexStore.TrimResult trim(
            PositionIndexStore.BucketsWithData bucketsOf,
            int keepExtra,
            PositionIndexStore.DroppedPartitions dropped,
            int idleEntries,
            int maxBuilds,
            long maxBuildMillis)
            throws IOException {
        layout.checkLayout();
        TrimRecords.Record previous = finishInterruptedTrim();
        // A barrier, not the chain the cut uses, which is listed after the checkpoints: the buckets
        // with data are read only after it, so a caller that reads them from Fluss's registration
        // reads one no older than this listing. Entries added since may touch buckets the caller
        // missed; protectUnlisted keeps those whatever the caller lists.
        indexChain.markerNames();
        Collection<TableBucket> bucketsWithData = bucketsOf.get();
        // The checkpoints first, then the chain. A server checkpoints only a registered snapshot,
        // so each checkpoint listed is of a snapshot on the chain listed after, unless a trim cut
        // it off or its lineage was abandoned: never one of a snapshot registered meanwhile.
        Map<TableBucket, List<IndexCheckpoints.Listed>> listed = new LinkedHashMap<>();
        for (TableBucket tb : bucketsWithData) {
            listed.put(tb, checkpoints.list(tb));
        }
        // And the partitions with checkpoints: the dropped ones among them are told after the
        // chain is listed, so a partition created since this listing is never among them.
        Set<Long> listedPartitions = new HashSet<>();
        boolean partitioned = !bucketsWithData.isEmpty();
        for (TableBucket tb : bucketsWithData) {
            if (tb.getPartitionId() == null) {
                partitioned = false;
            } else {
                listedPartitions.add(tb.getPartitionId());
            }
        }
        List<Long> checkpointed =
                partitioned ? checkpoints.partitions() : Collections.<Long>emptyList();
        List<ChainEntry> chain = indexChain.list();
        if (bucketsWithData.isEmpty() || chain.isEmpty()) {
            return new PositionIndexStore.TrimResult(0, chain.size(), 0, 0);
        }
        int last = chain.size() - 1;
        Map<Long, Integer> indexOf = new HashMap<>();
        for (int i = 0; i < chain.size(); i++) {
            indexOf.put(chain.get(i).snapshotId, i);
        }
        Map<TableBucket, Coverage> coverage = new LinkedHashMap<>();
        List<TableBucket> lost = new ArrayList<>();
        for (TableBucket tb : bucketsWithData) {
            Coverage c = coverage(tb, chain, IndexCheckpoints.newest(listed.get(tb), indexOf));
            coverage.put(tb, c);
            // A bucket whose rows trimmed changes hold cannot be built from the chain's start. A
            // bucket whose first rows came after the trims can, such as one of a new partition.
            if (c.checkpoint == null
                    && previous != null
                    && previous.buckets.contains(bucketKey(tb))) {
                lost.add(tb);
            }
        }
        if (!lost.isEmpty()) {
            throw lostBuckets(lost);
        }
        // Each bucket holds the chain at its newest checkpoint. Buckets whose servers checkpoint
        // them move it on themselves. A bucket without a checkpoint, or idle and checkpointed long
        // ago, would hold it for good, so its checkpoint is built here at the chain's end. Buckets
        // holding the chain furthest back go first.
        int heldByOthers = last;
        List<Map.Entry<TableBucket, Coverage>> toBuild = new ArrayList<>();
        for (Map.Entry<TableBucket, Coverage> e : coverage.entrySet()) {
            Coverage c = e.getValue();
            int held = c.held();
            if (held >= last) {
                continue;
            }
            if (c.checkpoint == null
                    || (held <= last - idleEntries && c.lastTouch <= last - idleEntries)) {
                toBuild.add(e);
            } else {
                heldByOthers = Math.min(heldByOthers, held);
            }
        }
        toBuild.sort(Comparator.comparingInt(e -> e.getValue().held()));
        long buildStart = System.currentTimeMillis();
        int built = 0;
        int failed = 0;
        for (Map.Entry<TableBucket, Coverage> e : toBuild) {
            if (e.getValue().held() >= heldByOthers
                    || built >= maxBuilds
                    || System.currentTimeMillis() - buildStart > maxBuildMillis) {
                break;
            }
            TableBucket tb = e.getKey();
            try {
                String name = buildCheckpoint(tb, chain, e.getValue());
                built++;
                coverage.put(
                        tb,
                        new Coverage(
                                new Checkpoint(last, chain.get(last).snapshotId, name),
                                e.getValue().lastTouch));
            } catch (IOException ex) {
                failed++;
                LOG.warn(
                        "Could not build the position index checkpoint of {}; it holds the chain"
                                + " until a later trim builds it.",
                        tb,
                        ex);
            }
        }
        // The cut stays below every bucket's newest checkpoint, which this trim never deletes.
        int cut = Integer.MAX_VALUE;
        for (Coverage c : coverage.values()) {
            cut = Math.min(cut, c.held() - keepExtra);
        }
        cut = protectUnlisted(bucketsWithData, chain, indexOf, cut, keepExtra, dropped, previous);
        // The dropped partitions whose checkpoints go, all told before anything is deleted, so a
        // failed lookup deletes nothing. Never one the caller lists: the cut is based on its
        // checkpoints.
        List<Long> droppedWithCheckpoints = new ArrayList<>();
        for (long partitionId : checkpointed) {
            if (!listedPartitions.contains(partitionId) && dropped.isDropped(partitionId)) {
                droppedWithCheckpoints.add(partitionId);
            }
        }
        int holding = toBuild.size() - built;
        long now = System.currentTimeMillis();
        if (cut <= 0) {
            // nothing to cut; old leftovers go all the same
            for (Map.Entry<TableBucket, Coverage> e : coverage.entrySet()) {
                checkpoints.deleteLeftBehind(
                        listed.get(e.getKey()), indexOf, 0, e.getValue().checkpoint, now);
            }
            deleteDroppedPartitions(droppedWithCheckpoints);
            return new PositionIndexStore.TrimResult(0, chain.size(), holding, failed);
        }
        Set<Long> keptChanges = new HashSet<>();
        for (int i = cut; i < chain.size(); i++) {
            keptChanges.add(chain.get(i).changeId);
        }
        Set<Long> trimmedSnapshots = new HashSet<>();
        for (int i = 0; i < cut; i++) {
            trimmedSnapshots.add(chain.get(i).snapshotId);
        }
        // The markers of the trimmed entries, duplicates of a step included, oldest first, and
        // the changes only they reference.
        List<String> trimmedMarkers = new ArrayList<>();
        Set<Long> trimmedChanges = new LinkedHashSet<>();
        for (String name : indexChain.markerNamesInOrder()) {
            ChainEntry e = parseMarker(name);
            if (trimmedSnapshots.contains(e.snapshotId)) {
                trimmedMarkers.add(name);
                if (!keptChanges.contains(e.changeId)) {
                    trimmedChanges.add(e.changeId);
                }
            }
        }
        // The buckets whose rows trimmed changes hold, now or by an earlier trim: they need a
        // checkpoint from now on. A change whose buckets cannot be told counts for every bucket.
        // The earlier trims' buckets all stay, so one trim that leaves a bucket out of the buckets
        // with data cannot drop it, but those of dropped partitions: a partition id is never
        // reused.
        Set<String> withData = new HashSet<>();
        for (TableBucket tb : bucketsWithData) {
            withData.add(bucketKey(tb));
        }
        Set<String> heldRows = new HashSet<>();
        if (previous != null) {
            heldRows.addAll(previous.buckets);
        }
        for (int i = 0; i < cut; i++) {
            Map<Long, Set<Integer>> touched = touchedBuckets.of(chain.get(i).changeId);
            if (touched == null) {
                heldRows.addAll(withData);
                break;
            }
            for (Map.Entry<Long, Set<Integer>> e : touched.entrySet()) {
                for (int b : e.getValue()) {
                    heldRows.add(TrimRecords.bucketKey(e.getKey(), b));
                }
            }
        }
        // told before anything is deleted, as the partitions with checkpoints are
        Set<Long> heldPartitions = new HashSet<>();
        for (String key : heldRows) {
            heldPartitions.add(Long.parseLong(key.substring(0, key.indexOf(' '))));
        }
        Set<Long> droppedHeld = new HashSet<>();
        for (long partitionId : heldPartitions) {
            if (partitionId != -1L
                    && !listedPartitions.contains(partitionId)
                    && dropped.isDropped(partitionId)) {
                droppedHeld.add(partitionId);
            }
        }
        heldRows.removeIf(
                key -> droppedHeld.contains(Long.parseLong(key.substring(0, key.indexOf(' ')))));
        // The checkpoints the kept ones make redundant, of those listed before the chain, before
        // the chain changes: a trim that cannot delete one stops with the chain whole.
        for (Map.Entry<TableBucket, Coverage> e : coverage.entrySet()) {
            checkpoints.deleteLeftBehind(
                    listed.get(e.getKey()), indexOf, cut, e.getValue().checkpoint, now);
        }
        deleteDroppedPartitions(droppedWithCheckpoints);
        // Record the trim before deleting anything else: that the chain is trimmed, so no rebuild
        // starts at its beginning, and what is left to delete, so the next trim finishes an
        // interrupted one.
        TrimRecords.Record record =
                records.write(
                        chain.get(cut).sequence,
                        chain.get(cut).snapshotId,
                        trimmedMarkers,
                        trimmedChanges,
                        heldRows);
        // The chain entries first, oldest first: an interrupted trim leaves a chain whose every
        // entry has its change, as a later part of the chain.
        deleteTrimmed(trimmedMarkers, trimmedChanges);
        records.deleteBefore(record);
        if (built > 0) {
            LOG.info(
                    "Built the position index checkpoints of {} idle buckets of {} in {} ms.",
                    built,
                    layout.lakeTableDir,
                    System.currentTimeMillis() - buildStart);
        }
        return new PositionIndexStore.TrimResult(cut, chain.size() - cut, holding, failed);
    }

    /** Deletes the chain markers, in order, and then the changes of a trim. */
    private void deleteTrimmed(List<String> markers, Collection<Long> changes) throws IOException {
        FsPath chainDir = layout.path(CHAIN_DIR);
        Set<String> present = new HashSet<>(indexChain.markerNames());
        for (String name : markers) {
            if (present.contains(name)) {
                store.deleteOrThrow(new FsPath(chainDir, name), false);
            }
        }
        indexChain.deleteIdentities(markers);
        for (long changeId : changes) {
            writer.deleteChange(changeId);
            touchedBuckets.forget(changeId);
        }
    }

    /**
     * Finishes a trim that stopped before it deleted all it recorded, and returns the latest
     * complete record, or null if none. Its markers were the oldest entries of the chain, which
     * only grows at its end, so they still are. Finishing a finished trim deletes nothing.
     */
    @Nullable
    private TrimRecords.Record finishInterruptedTrim() throws IOException {
        TrimRecords.Record record = records.latest();
        if (record == null) {
            return null;
        }
        Set<String> present = new HashSet<>(indexChain.markerNames());
        List<String> toDelete = new ArrayList<>();
        for (String name : record.markers) {
            if (present.contains(name)) {
                toDelete.add(name);
            }
        }
        // Never a change the rest of the chain still references.
        Set<Long> changes = new LinkedHashSet<>();
        for (long changeId : record.changes) {
            if (exists(layout.changeDir(changeId))) {
                changes.add(changeId);
            }
        }
        Set<String> deleted = new HashSet<>(toDelete);
        for (String name : present) {
            if (!deleted.contains(name)) {
                changes.remove(parseMarker(name).changeId);
            }
        }
        if (!toDelete.isEmpty() || !changes.isEmpty()) {
            LOG.info(
                    "Finishing an interrupted trim of the position index chain of {}: {} chain"
                            + " entries and {} changes left to delete.",
                    layout.lakeTableDir,
                    toDelete.size(),
                    changes.size());
            deleteTrimmed(toDelete, changes);
        }
        records.deleteBefore(record);
        return record;
    }

    /**
     * Lowers the cut so that it keeps every bucket the caller did not list, as a caller whose
     * bucket list is out of date (a paused service) would not: the buckets a trim record lists,
     * whose rows only their checkpoints may hold, and the buckets the changes below the cut touch.
     * Each needs a complete checkpoint on the chain at least {@code keepExtra} past the cut, unless
     * its partition was dropped. Without one, the cut stays below the bucket's first touch, and a
     * recorded bucket stops the trim, as one the caller lists would. A change whose buckets cannot
     * be told stays. So the trim never depends on the caller's list for what it keeps.
     */
    private int protectUnlisted(
            Collection<TableBucket> bucketsWithData,
            List<ChainEntry> chain,
            Map<Long, Integer> indexOf,
            int cut,
            int keepExtra,
            PositionIndexStore.DroppedPartitions dropped,
            @Nullable TrimRecords.Record previous)
            throws IOException {
        Set<String> listed = new HashSet<>();
        long tableId = -1;
        for (TableBucket tb : bucketsWithData) {
            listed.add(bucketKey(tb));
            tableId = tb.getTableId();
        }
        // the first chain index below the cut whose change touches each unlisted bucket
        Map<TableBucket, Integer> firstTouch = new LinkedHashMap<>();
        for (int i = 0; i < cut && i < chain.size(); i++) {
            Map<Long, Set<Integer>> touched = touchedBuckets.of(chain.get(i).changeId);
            if (touched == null) {
                cut = i;
                break;
            }
            for (Map.Entry<Long, Set<Integer>> e : touched.entrySet()) {
                for (int b : e.getValue()) {
                    if (!listed.contains(TrimRecords.bucketKey(e.getKey(), b))) {
                        firstTouch.putIfAbsent(
                                new TableBucket(tableId, e.getKey() == -1L ? null : e.getKey(), b),
                                i);
                    }
                }
            }
        }
        // the buckets earlier trims recorded, whose rows only their checkpoints may hold now
        if (previous != null) {
            for (String key : previous.buckets) {
                if (!listed.contains(key)) {
                    String[] f = key.split(" ");
                    long pid = Long.parseLong(f[0]);
                    firstTouch.putIfAbsent(
                            new TableBucket(
                                    tableId, pid == -1L ? null : pid, Integer.parseInt(f[1])),
                            Integer.MAX_VALUE);
                }
            }
        }
        List<TableBucket> lost = new ArrayList<>();
        for (Map.Entry<TableBucket, Integer> e : firstTouch.entrySet()) {
            TableBucket tb = e.getKey();
            boolean recorded = previous != null && previous.buckets.contains(bucketKey(tb));
            if ((e.getValue() >= cut && !recorded)
                    || (tb.getPartitionId() != null && dropped.isDropped(tb.getPartitionId()))) {
                continue;
            }
            Checkpoint k = IndexCheckpoints.newest(checkpoints.list(tb), indexOf);
            if (k != null) {
                cut = Math.min(cut, k.chainIndex - keepExtra);
            } else if (recorded) {
                lost.add(tb);
            } else {
                cut = Math.min(cut, e.getValue());
            }
        }
        if (!lost.isEmpty()) {
            throw lostBuckets(lost);
        }
        if (!firstTouch.isEmpty()) {
            LOG.info(
                    "The position index of {} holds rows of buckets the trim was not given ({});"
                            + " the cut stays at {} for them.",
                    layout.lakeTableDir,
                    firstTouch.keySet(),
                    cut);
        }
        return cut;
    }

    /** Deletes the checkpoints of the dropped partitions, which no reader needs again. */
    private void deleteDroppedPartitions(List<Long> partitionIds) throws IOException {
        for (long partitionId : partitionIds) {
            int deleted = checkpoints.deletePartition(partitionId);
            LOG.info(
                    "Deleted {} position index checkpoints of the dropped partition {} of {}.",
                    deleted,
                    partitionId,
                    layout.lakeTableDir);
        }
    }

    /**
     * The error of buckets whose rows changes trimmed off the chain hold and which have no
     * checkpoint.
     */
    private IOException lostBuckets(List<TableBucket> lost) {
        // Their rows are only in the lake now. Nothing here can index a single bucket from the
        // lake, so the trim stops loudly instead of building an index without those rows.
        return new IOException(
                String.format(
                        "The position index buckets %s of %s have no checkpoint, but changes"
                                + " trimmed off the chain hold their rows, so they cannot be"
                                + " rebuilt from the chain, and their leaders refuse to rebuild"
                                + " them. The chain is not trimmed and orphan changes are not"
                                + " removed until each has a checkpoint again: restore the"
                                + " bucket's last checkpoint directory under %s (for example"
                                + " from object-store versioning), or tier the table into a new"
                                + " table.",
                        lost, layout.lakeTableDir, layout.path(IndexLayout.CKPT_DIR)));
    }

    /** A bucket's newest checkpoint on the chain, and the last change that touches it. */
    private static final class Coverage {
        // the newest complete checkpoint on the chain, or null
        @Nullable final Checkpoint checkpoint;
        // the last chain index whose change touches the bucket, -1 if none does
        final int lastTouch;

        Coverage(@Nullable Checkpoint checkpoint, int lastTouch) {
            this.checkpoint = checkpoint;
            this.lastTouch = lastTouch;
        }

        /** The chain index the bucket holds the chain at: its checkpoint's, or -1 without one. */
        int held() {
            return checkpoint == null ? -1 : checkpoint.chainIndex;
        }
    }

    private Coverage coverage(
            TableBucket bucket, List<ChainEntry> chain, @Nullable Checkpoint checkpoint) {
        int lastTouch = -1;
        for (int i = 0; i < chain.size(); i++) {
            if (touchedBuckets.touches(chain.get(i), bucket)) {
                lastTouch = i;
            }
        }
        return new Coverage(checkpoint, lastTouch);
    }

    private static String bucketKey(TableBucket bucket) {
        return TrimRecords.bucketKey(partitionKey(bucket), bucket.getBucket());
    }

    /**
     * Builds the bucket's checkpoint at the end of the chain, as its index server would: its latest
     * checkpoint, then every change since that touches it, in commit order, without the entries of
     * removed files.
     */
    private String buildCheckpoint(TableBucket bucket, List<ChainEntry> chain, Coverage coverage)
            throws IOException {
        int target = chain.size() - 1;
        File dir = IndexFiles.createTempDirectory(localTmpDir, "fluss-pkidx-ckpt-");
        try {
            List<BucketData> parts = new ArrayList<>();
            int from = 0;
            if (coverage.checkpoint != null) {
                parts.add(
                        checkpoints.readCheckpoint(
                                coverage.checkpoint, bucket, new File(dir, "ckpt")));
                from = coverage.checkpoint.chainIndex + 1;
            }
            for (int i = from; i <= target; i++) {
                if (touchedBuckets.touches(chain.get(i), bucket)) {
                    parts.add(
                            reader.readChange(chain.get(i), bucket, new File(dir, "change-" + i)));
                }
            }
            File sst = new File(dir, CKPT_SST);
            Map<Integer, String> fileDict = new HashMap<>();
            long entries = mergeIndex(parts, new File(dir, "db"), sst, fileDict);
            return checkpoints.writeCheckpoint(
                    chain.get(target).snapshotId, bucket, entries > 0 ? sst : null, fileDict);
        } finally {
            FileUtils.deleteDirectoryQuietly(dir);
        }
    }

    /** Whether entries may have been trimmed off the start of the chain: a trim recorded itself. */
    boolean chainTrimmed() throws IOException {
        return records.any();
    }
}
