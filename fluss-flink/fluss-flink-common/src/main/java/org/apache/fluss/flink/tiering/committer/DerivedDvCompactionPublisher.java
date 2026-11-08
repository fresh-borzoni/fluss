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
import org.apache.fluss.client.tiering.TableBucketWriteResult;
import org.apache.fluss.lake.committer.SupportsPositionDeleteCommit;
import org.apache.fluss.lake.dv.RowPosSstIndex;
import org.apache.fluss.lake.dv.index.CompactionIndexPart;
import org.apache.fluss.lake.dv.index.CompactionIndexPartProvider;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Publishes the index changes of Fluss's own compactions from the positions the compactions in the
 * tiering writers handed over ({@link CompactionIndexPart}), so their rewrites need no re-index.
 *
 * <p>On a lake that takes deletes, the round's compaction is a lake commit of its own, before the
 * round's ({@link #publishCompactionIndex}).
 *
 * <p>Crash rule: a hand-off that is not published, because it does not match the lake or the
 * committer failed, leaves the rewrite unindexed. The re-index of {@link DerivedDvReindexer} then
 * indexes it from the lake like another engine's rewrite, and the handed-over change ages out as an
 * orphan.
 */
final class DerivedDvCompactionPublisher<WriteResult, Committable> {

    private static final Logger LOG = DerivedDvContext.LOG;

    private final DerivedDvContext<WriteResult, Committable> context;

    private final DerivedDvRegistrations<WriteResult, Committable> registrations;

    DerivedDvCompactionPublisher(
            DerivedDvContext<WriteResult, Committable> context,
            DerivedDvRegistrations<WriteResult, Committable> registrations) {
        this.context = context;
        this.registrations = registrations;
    }

    /**
     * Publishes the index change of this round's own compaction from the positions its writers
     * handed over ({@link CompactionIndexPart}), so the round needs no re-index. The change is
     * registered as the step from the base to the rewrite, and the rewrite in Fluss. Returns false
     * if a compacted bucket handed nothing over, the compaction read a snapshot other than the
     * base, or publishing fails; the rewrite is then re-indexed like another engine's.
     *
     * <p>The rewrite commits only on the snapshot its compaction read (see {@link
     * SupportsPositionDeleteCommit#commitPendingRewrite}), so a change from the base covers it
     * exactly.
     */
    boolean publishCompactionIndex(
            long tableId,
            TablePath tablePath,
            LakeSnapshot base,
            @Nullable Long rewriteSnapshotId,
            List<TableBucketWriteResult<WriteResult>> results) {
        Map<TableBucket, CompactionIndexPart> parts = new HashMap<>();
        boolean complete = true;
        for (TableBucketWriteResult<WriteResult> r : results) {
            Object wr = r.writeResult();
            if (wr instanceof CompactionIndexPartProvider
                    && ((CompactionIndexPartProvider) wr).compacted()) {
                CompactionIndexPart part = ((CompactionIndexPartProvider) wr).compactionIndexPart();
                if (part == null) {
                    complete = false;
                } else {
                    parts.put(r.tableBucket(), part);
                }
            }
        }
        if (parts.isEmpty()) {
            return false;
        }
        PositionIndexStore store = context.indexStore(tableId, tablePath);
        Set<Long> changeIds = new HashSet<>();
        Set<Long> readSnapshots = new HashSet<>();
        for (CompactionIndexPart part : parts.values()) {
            changeIds.add(part.changeId());
            readSnapshots.add(part.readSnapshotId());
        }
        String reason =
                rewriteSnapshotId == null
                        ? "the rewrite was not committed"
                        : !complete
                                ? "a compacted bucket handed no positions over"
                                : changeIds.size() > 1 || readSnapshots.size() > 1
                                        ? "the compactions read different snapshots"
                                        : readSnapshots.iterator().next() != base.getSnapshotId()
                                                ? "the compactions read snapshot "
                                                        + readSnapshots.iterator().next()
                                                        + ", not the base"
                                                : null;
        if (reason == null) {
            long changeId = changeIds.iterator().next();
            try {
                long t0 = System.currentTimeMillis();
                Map<TableBucket, PositionIndexStore.BucketChange> change =
                        assignFileIds(store, changeId, parts);
                store.commitChange(changeId, change);
                store.register(base.getSnapshotId(), rewriteSnapshotId, changeId);
                registrations.registerSnapshot(tableId, tablePath, rewriteSnapshotId, base);
                context.metrics.compactionHandOffs.inc();
                LOG.info(
                        "The Fluss compaction of {} committed as snapshot {}; {} buckets"
                                + " handed their positions to index change {} (no re-index), published in"
                                + " {} ms.",
                        tablePath,
                        rewriteSnapshotId,
                        parts.size(),
                        changeId,
                        System.currentTimeMillis() - t0);
                return true;
            } catch (Exception e) {
                reason = "publishing failed: " + e;
            }
        }
        LOG.info("The compaction of {} is re-indexed from the lake: {}.", tablePath, reason);
        for (long changeId : changeIds) {
            try {
                store.abortChange(changeId);
            } catch (Exception e) {
                LOG.debug("Could not delete the position index change {}.", changeId, e);
            }
        }
        return false;
    }

    /**
     * Gives the files of the compactions' parts their ids from the table's counter. Compactions of
     * different buckets run concurrently and may outlive their round, so their writers number the
     * files of their part from 0. This committer is the single allocator of a table's ids. The
     * parts' SSTs are rewritten with the ids, in parallel.
     */
    private static Map<TableBucket, PositionIndexStore.BucketChange> assignFileIds(
            PositionIndexStore store, long changeId, Map<TableBucket, CompactionIndexPart> parts)
            throws Exception {
        int files = 0;
        for (CompactionIndexPart part : parts.values()) {
            files += part.addedFiles().size();
        }
        int nextId = files == 0 ? 0 : store.allocateFileIds(files);
        Map<TableBucket, Map<Integer, Integer>> newIds = new HashMap<>();
        Map<TableBucket, Map<Integer, String>> addedFiles = new HashMap<>();
        for (Map.Entry<TableBucket, CompactionIndexPart> e : parts.entrySet()) {
            Map<Integer, Integer> ids = new HashMap<>();
            Map<Integer, String> added = new HashMap<>();
            for (Map.Entry<Integer, String> f :
                    new TreeMap<>(e.getValue().addedFiles()).entrySet()) {
                ids.put(f.getKey(), nextId);
                added.put(nextId, f.getValue());
                nextId++;
            }
            newIds.put(e.getKey(), ids);
            addedFiles.put(e.getKey(), added);
        }
        Map<TableBucket, PositionIndexStore.BucketChange> change = new HashMap<>();
        ExecutorService pool =
                Executors.newFixedThreadPool(
                        Math.min(PublishedParts.PUBLISH_THREADS, parts.size()));
        try {
            Map<TableBucket, Future<PositionIndexStore.BucketPart>> remapped = new HashMap<>();
            for (Map.Entry<TableBucket, CompactionIndexPart> e : parts.entrySet()) {
                CompactionIndexPart part = e.getValue();
                List<RowPosSstIndex.SstFileEntry> ssts = new ArrayList<>();
                for (int i = 0; i < part.sstFileNames().size(); i++) {
                    ssts.add(
                            new RowPosSstIndex.SstFileEntry(
                                    part.sstFileNames().get(i), part.sstFileSizes().get(i)));
                }
                PositionIndexStore.BucketPart stored =
                        ssts.isEmpty()
                                ? PositionIndexStore.BucketPart.NONE
                                : new PositionIndexStore.BucketPart(ssts);
                TableBucket tb = e.getKey();
                remapped.put(
                        tb,
                        pool.submit(
                                () -> store.remapFileIds(changeId, tb, stored, newIds.get(tb))));
            }
            for (Map.Entry<TableBucket, CompactionIndexPart> e : parts.entrySet()) {
                change.put(
                        e.getKey(),
                        new PositionIndexStore.BucketChange(
                                remapped.get(e.getKey()).get(),
                                addedFiles.get(e.getKey()),
                                new ArrayList<>(e.getValue().removedFiles())));
            }
        } finally {
            pool.shutdownNow();
        }
        return change;
    }
}
