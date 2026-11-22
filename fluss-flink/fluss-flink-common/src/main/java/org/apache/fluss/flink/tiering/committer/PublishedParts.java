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

package org.apache.fluss.flink.tiering.committer;

import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.tiering.DvRoundRequests;
import org.apache.fluss.client.tiering.RoundDeletes;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.lake.dv.index.StagedIndexPart;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/** What publishing the writers' parts produced. */
final class PublishedParts {

    private static final Comparator<TableBucket> BUCKET_ORDER =
            Comparator.comparingLong(
                            (TableBucket tb) ->
                                    tb.getPartitionId() == null ? -1L : tb.getPartitionId())
                    .thenComparingInt(TableBucket::getBucket);

    final Map<TableBucket, PositionIndexStore.BucketChange> changes = new HashMap<>();
    final DeletedPositions deleted = new DeletedPositions();
    long keys;
    long deletes;
    long deletedRows;
    // buckets in the base committed from the deletion vectors their writers prepared, and those
    // whose deletes the committer merged: from their write results, or from their leaders
    int vectorsFromWriters;
    int deletesFromReaders;
    int deletesFromLeaders;

    private PublishedParts() {}

    /**
     * Publishes every bucket's staged part under change {@code indexId}, {@code threads} buckets at
     * once, with file ids from the table's counter, and collects the positions of the base's rows
     * the round deletes, as a bitmap per data file. A bucket in the base takes them from its
     * leader, which derives them from its change list from the base's offset, where the bucket's
     * round starts ({@code roundStarts}), up to its round end ({@code roundEnds}). Deletes the
     * bucket's reader fetched ({@code prefetched}) are used if they hold on the base (see {@link
     * #holdOnBase}); otherwise they are fetched while the bucket's part is published. A bucket not
     * in the base deletes nothing, and neither does a bucket whose writer prepared its deletion
     * vectors ({@code preparedByWriters}), which hold its round's deletes.
     */
    static PublishedParts publish(
            PositionIndexStore store,
            long indexId,
            @Nullable LakeSnapshot baseSnapshot,
            Map<TableBucket, StagedIndexPart> parts,
            Map<TableBucket, Long> roundStarts,
            Map<TableBucket, Long> roundEnds,
            Map<TableBucket, RoundDeletes> prefetched,
            ChangedDataFiles changedDataFiles,
            Set<TableBucket> preparedByWriters,
            DvRoundRequests.Fetcher roundDeletes,
            int threads)
            throws Exception {
        PublishedParts out = new PublishedParts();
        if (parts.isEmpty()) {
            return out;
        }
        List<TableBucket> order = new ArrayList<>(parts.keySet());
        order.sort(BUCKET_ORDER);
        long base = baseSnapshot == null ? -1L : baseSnapshot.getSnapshotId();
        Map<TableBucket, Long> inBase =
                base > 0 ? baseSnapshot.getTableBucketsOffset() : Collections.emptyMap();
        Map<TableBucket, RoundDeletes> usable = new HashMap<>();
        // by the snapshot deletes were fetched on, the data files the snapshots since changed
        Map<Long, Optional<Set<String>>> changedSince = new HashMap<>();
        for (TableBucket tb : order) {
            Long baseOffset = inBase.get(tb);
            if (baseOffset == null) {
                continue;
            }
            // the leader derives the deletes from the base's offset on, where the writer starts
            Long start = roundStarts.get(tb);
            checkState(
                    start != null && start.longValue() == baseOffset,
                    "The round of bucket %s starts at offset %s, not at offset %s of base"
                            + " snapshot %s.",
                    tb,
                    start,
                    baseOffset,
                    base);
            Long end = roundEnds.get(tb);
            checkState(end != null, "The round of bucket %s has no end offset.", tb);
            if (preparedByWriters.contains(tb)) {
                out.vectorsFromWriters++;
                continue;
            }
            RoundDeletes fetched = prefetched.get(tb);
            if (fetched != null
                    && holdOnBase(fetched, base, baseOffset, end, changedSince, changedDataFiles)) {
                usable.put(tb, fetched);
                out.deletesFromReaders++;
            } else {
                out.deletesFromLeaders++;
            }
        }
        // File ids come from the table's persisted counter; a colliding id masks the wrong file.
        int files = 0;
        for (StagedIndexPart part : parts.values()) {
            files += part.files().size();
        }
        int nextId = files == 0 ? 0 : store.allocateFileIds(files);
        Map<TableBucket, Map<Integer, Integer>> newIds = new HashMap<>();
        Map<TableBucket, Map<Integer, String>> addedFiles = new HashMap<>();
        for (TableBucket tb : order) {
            Map<Integer, Integer> ids = new HashMap<>();
            Map<Integer, String> added = new HashMap<>();
            for (Map.Entry<Integer, String> f : new TreeMap<>(parts.get(tb).files()).entrySet()) {
                ids.put(f.getKey(), nextId);
                added.put(nextId, f.getValue());
                nextId++;
            }
            newIds.put(tb, ids);
            addedFiles.put(tb, added);
        }
        ExecutorService publishers = Executors.newFixedThreadPool(Math.min(threads, order.size()));
        // a publisher waits for its bucket's fetch, so each has a thread of its own
        ExecutorService fetchers =
                out.deletesFromLeaders == 0
                        ? null
                        : Executors.newFixedThreadPool(Math.min(threads, out.deletesFromLeaders));
        try {
            Map<TableBucket, Future<PublishedBucket>> futures = new HashMap<>();
            for (TableBucket tb : order) {
                StagedIndexPart part = parts.get(tb);
                Long baseOffset = preparedByWriters.contains(tb) ? null : inBase.get(tb);
                RoundDeletes known = usable.get(tb);
                futures.put(
                        tb,
                        publishers.submit(
                                () -> {
                                    Future<DeletedPositions> fetched =
                                            baseOffset == null || known != null
                                                    ? null
                                                    : checkNotNull(fetchers)
                                                            .submit(
                                                                    () ->
                                                                            leaderDeletes(
                                                                                    tb,
                                                                                    base,
                                                                                    baseOffset,
                                                                                    roundEnds.get(
                                                                                            tb),
                                                                                    roundDeletes));
                                    PositionIndexStore.BucketPart published =
                                            store.publishStaged(
                                                    part.stagingId(),
                                                    indexId,
                                                    tb,
                                                    part.part(),
                                                    newIds.get(tb));
                                    DeletedPositions deleted =
                                            known != null
                                                    ? DeletedPositions.of(known)
                                                    : fetched != null
                                                            ? join(fetched)
                                                            : new DeletedPositions();
                                    return new PublishedBucket(published, deleted);
                                }));
            }
            for (TableBucket tb : order) {
                PublishedBucket done = join(futures.get(tb));
                StagedIndexPart part = parts.get(tb);
                out.changes.put(
                        tb,
                        new PositionIndexStore.BucketChange(
                                done.part, addedFiles.get(tb), Collections.emptyList()));
                out.deleted.addAll(done.deleted);
                out.keys += part.keys();
                out.deletes += part.deletes();
            }
        } finally {
            publishers.shutdownNow();
            if (fetchers != null) {
                fetchers.shutdownNow();
            }
        }
        out.deletedRows = out.deleted.positions();
        return out;
    }

    /**
     * Whether a bucket's deletes its reader fetched hold on the base: they are of the bucket's
     * round from the base's offset, and fetched on the base or on a snapshot before it, after which
     * no snapshot changed the rows or the deletes of a data file they name. The round's keys then
     * keep their base rows in those files, at the same positions, and the base's index, which
     * re-indexed only rewritten files since, has them there too: the bucket's leader would answer
     * the same deletes on the base. This holds whoever changed the other files, e.g. the round's
     * own compaction or another engine's rewrite of other buckets.
     */
    private static boolean holdOnBase(
            RoundDeletes fetched,
            long base,
            long baseOffset,
            long roundEnd,
            Map<Long, Optional<Set<String>>> changedSince,
            ChangedDataFiles changedDataFiles)
            throws IOException {
        if (fetched.startOffset() != baseOffset || fetched.endOffset() != roundEnd) {
            return false;
        }
        long snapshotId = fetched.snapshotId();
        if (snapshotId == base) {
            return true;
        }
        Optional<Set<String>> changed = changedSince.get(snapshotId);
        if (changed == null) {
            changed = Optional.ofNullable(changedDataFiles.since(snapshotId));
            changedSince.put(snapshotId, changed);
        }
        if (!changed.isPresent()) {
            return false;
        }
        for (RoundDeletes.File file : fetched.files()) {
            if (changed.get().contains(file.path())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Takes the deletes of a bucket whose writer prepared its deletion vectors at the committer
     * instead, from its leader on the base.
     */
    void addCentrally(
            TableBucket tb,
            long base,
            long baseOffset,
            long roundEnd,
            DvRoundRequests.Fetcher roundDeletes)
            throws Exception {
        deleted.addAll(leaderDeletes(tb, base, baseOffset, roundEnd, roundDeletes));
        vectorsFromWriters--;
        deletesFromLeaders++;
        deletedRows = deleted.positions();
    }

    /**
     * The publish threads of the round's index parts, from {@code
     * lake.tiering.deletion-vectors.publish-threads}.
     */
    static int publishThreads(Configuration lakeTieringConfig) {
        int threads =
                lakeTieringConfig.get(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_PUBLISH_THREADS);
        checkArgument(
                threads > 0,
                "%s must be positive.",
                ConfigOptions.LAKE_TIERING_DELETION_VECTORS_PUBLISH_THREADS.key());
        return threads;
    }

    /**
     * The base positions a bucket's round deletes, from its leader, whatever their size; kept as
     * bitmaps only.
     */
    private static DeletedPositions leaderDeletes(
            TableBucket tb,
            long base,
            long baseOffset,
            long roundEnd,
            DvRoundRequests.Fetcher roundDeletes)
            throws Exception {
        return DeletedPositions.of(
                RoundDeletes.fetch(tb, base, baseOffset, roundEnd, Long.MAX_VALUE, roundDeletes));
    }

    private static <T> T join(Future<T> future) throws Exception {
        try {
            return future.get();
        } catch (ExecutionException e) {
            throw e.getCause() instanceof Exception ? (Exception) e.getCause() : e;
        }
    }

    /**
     * The data files the lake snapshots after a snapshot up to the round's base changed the rows or
     * the deletes of, by name; see {@link
     * org.apache.fluss.lake.committer.SupportsPositionDeleteCommit#changedDataFiles}.
     */
    @FunctionalInterface
    interface ChangedDataFiles {
        /** The changed data files, or null if they are not known. */
        @Nullable
        Set<String> since(long snapshotId) throws IOException;
    }

    /** A bucket's published part and the base positions its round deletes. */
    private static final class PublishedBucket {
        final PositionIndexStore.BucketPart part;
        final DeletedPositions deleted;

        PublishedBucket(PositionIndexStore.BucketPart part, DeletedPositions deleted) {
            this.part = part;
            this.deleted = deleted;
        }
    }
}
