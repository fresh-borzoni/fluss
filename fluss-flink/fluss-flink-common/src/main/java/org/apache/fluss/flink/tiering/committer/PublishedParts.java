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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.lake.dv.index.StagedIndexPart;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

/** What publishing the writers' parts produced. */
final class PublishedParts {

    static final int LOOKUP_CHUNK = 50_000;
    // Lookup chunks in flight at once across all buckets of a round; bounds the keys held.
    @VisibleForTesting static final int LOOKUPS_IN_FLIGHT = 8;
    // Buckets whose parts are published at once.
    static final int PUBLISH_THREADS = 8;

    private static final Comparator<TableBucket> BUCKET_ORDER =
            Comparator.comparingLong(
                            (TableBucket tb) ->
                                    tb.getPartitionId() == null ? -1L : tb.getPartitionId())
                    .thenComparingInt(TableBucket::getBucket);

    final Map<TableBucket, PositionIndexStore.BucketChange> changes = new HashMap<>();
    final DeletedPositions deleted = new DeletedPositions();
    long keys;
    long deletes;
    long found;

    private PublishedParts() {}

    /**
     * Publishes every bucket's staged part under change {@code indexId}, buckets in parallel, with
     * file ids from the table's counter. While it streams a part's keys, it looks up their base
     * positions in chunks of {@link #LOOKUP_CHUNK} keys, at most {@link #LOOKUPS_IN_FLIGHT} chunks
     * at a time. Each found position goes into its data file's bitmap. A bucket not in the base has
     * nothing to look up.
     */
    static PublishedParts publish(
            PositionIndexStore store,
            long indexId,
            @Nullable LakeSnapshot baseSnapshot,
            Map<TableBucket, StagedIndexPart> parts,
            DerivedDvLookups.PositionLookup lookup)
            throws Exception {
        PublishedParts out = new PublishedParts();
        if (parts.isEmpty()) {
            return out;
        }
        List<TableBucket> order = new ArrayList<>(parts.keySet());
        order.sort(BUCKET_ORDER);
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
        long base = baseSnapshot == null ? -1L : baseSnapshot.getSnapshotId();
        Map<TableBucket, Long> inBase =
                base > 0 ? baseSnapshot.getTableBucketsOffset() : Collections.emptyMap();
        ExecutorService publishers =
                Executors.newFixedThreadPool(Math.min(PUBLISH_THREADS, order.size()));
        ExecutorService lookups = Executors.newFixedThreadPool(LOOKUPS_IN_FLIGHT);
        Semaphore inFlight = new Semaphore(LOOKUPS_IN_FLIGHT);
        try {
            Map<TableBucket, Future<BucketLookups>> futures = new HashMap<>();
            for (TableBucket tb : order) {
                StagedIndexPart part = parts.get(tb);
                BucketLookups bucketLookups =
                        new BucketLookups(
                                tb, base, inBase.containsKey(tb), lookup, lookups, inFlight);
                futures.put(
                        tb,
                        publishers.submit(
                                () -> {
                                    bucketLookups.published =
                                            store.publishStaged(
                                                    part.stagingId(),
                                                    indexId,
                                                    tb,
                                                    part.part(),
                                                    newIds.get(tb),
                                                    bucketLookups::accept);
                                    bucketLookups.finish();
                                    return bucketLookups;
                                }));
            }
            for (TableBucket tb : order) {
                BucketLookups done = futures.get(tb).get();
                StagedIndexPart part = parts.get(tb);
                out.changes.put(
                        tb,
                        new PositionIndexStore.BucketChange(
                                done.published, addedFiles.get(tb), Collections.emptyList()));
                out.deleted.addAll(done.deleted);
                out.keys += part.keys();
                out.deletes += part.deletes();
                out.found += done.found;
            }
        } finally {
            publishers.shutdownNow();
            lookups.shutdownNow();
        }
        return out;
    }
}
