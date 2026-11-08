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

package org.apache.fluss.client.table.scanner.batch.dv;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.admin.DvAdmin;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.LakeTableRolledBackException;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.SupportsRewritePlanning;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Plans the parts of a bounded union read of a table with deletion vectors. It pins one cut per
 * bucket on the buckets' leaders, so several parts can read a bucket with the same masks: groups of
 * its lake files and its log. Planning fetches no masks.
 *
 * <p>A cut takes one hold per part of its bucket. Each part releases its hold when it is done
 * ({@link #releasePinnedCuts} of that part alone), and the engine releases the holds of the parts
 * left when the read ends. The leader drops the cut once all its holds are released.
 */
@Internal
public class DvBucketPlanner {

    private static final Logger LOG = LoggerFactory.getLogger(DvBucketPlanner.class);

    /** The lake splits one part reads at most when a bucket is read in several parts. */
    public static final int MAX_FILES_PER_GROUP = 64;

    private final DvAdmin admin;
    private final TablePath tablePath;
    // checked when the planner is created: a lake without it cannot read deletion vectors
    private final SupportsRewritePlanning<LakeSplit> lakeSource;
    private long fileGroupRows =
            ConfigOptions.CLIENT_SCANNER_DELETION_VECTORS_FILE_GROUP_ROWS.defaultValue();
    private boolean waitForCatchUp =
            ConfigOptions.CLIENT_SCANNER_DELETION_VECTORS_WAIT_FOR_CATCH_UP.defaultValue();
    private boolean pinSplitBucketsOnly = false;

    public DvBucketPlanner(DvAdmin admin, TablePath tablePath, LakeSource<LakeSplit> lakeSource) {
        this.admin = admin;
        this.tablePath = tablePath;
        this.lakeSource = DvLakeCalls.rewritePlanning(lakeSource, tablePath);
    }

    /** Sets the rows of lake files one part reads at most. */
    public DvBucketPlanner withFileGroupRows(long fileGroupRows) {
        checkArgument(fileGroupRows > 0, "The rows of a file group must be positive.");
        this.fileGroupRows = fileGroupRows;
        return this;
    }

    /**
     * Sets whether pinning waits for a leader that still switches to the snapshot. If not, a bucket
     * whose leader covers the snapshot registered before is not pinned, and its one part reads that
     * snapshot.
     */
    public DvBucketPlanner withWaitForCatchUp(boolean waitForCatchUp) {
        this.waitForCatchUp = waitForCatchUp;
        return this;
    }

    /**
     * Sets whether only the buckets read in several parts are pinned. A bucket read by one part is
     * left to that part, which pins its own cut when it starts and releases it when it ends. This
     * suits an engine that may plan a read more often than it runs it, as Spark does, and has no
     * hook when the read ends to release the cuts of its planning.
     */
    public DvBucketPlanner withPinSplitBucketsOnly(boolean pinSplitBucketsOnly) {
        this.pinSplitBucketsOnly = pinSplitBucketsOnly;
        return this;
    }

    /**
     * Pins a cut of lake snapshot {@code snapshotId} for the buckets of {@code buckets}, parts that
     * each read a whole bucket ({@link DvBucketPart#ofBucket}), and splits each pinned bucket into
     * the parts that read it. Returns those parts by bucket. A bucket that could not be pinned is
     * read by one part, which fetches masks when it starts: its given part, absent from the result,
     * or, for a bucket with more lake files than one part reads, a part that plans them when it
     * starts ({@link DvBucketPart#withLakeSplitsPlannedAtStart()}).
     *
     * <p>If planning fails, the cuts it pinned are released.
     *
     * @throws LakeTableRolledBackException if the lake snapshot under {@code snapshotId} is not the
     *     commit the masks were derived for, as after a Paimon rollback that reused the id
     */
    public Map<TableBucket, List<DvBucketPart>> pin(long snapshotId, List<DvBucketPart> buckets)
            throws Exception {
        List<TableBucket> tableBuckets = new ArrayList<>(buckets.size());
        Map<TableBucket, Integer> holds = new HashMap<>();
        Map<TableBucket, Integer> groupCounts = new HashMap<>();
        for (DvBucketPart bucket : buckets) {
            // a bucket of one file group is read by one part
            int groups = fileGroups(bucket.getLakeSplits()).size();
            groupCounts.put(bucket.getTableBucket(), groups);
            if (!pinSplitBucketsOnly || groups > 1) {
                tableBuckets.add(bucket.getTableBucket());
                // the file groups and the log, at most; a hold the log part does not take is
                // released once the bucket is split
                holds.put(bucket.getTableBucket(), groups > 1 ? groups + 1 : 1);
            }
        }
        Map<TableBucket, DvCutPinner.PinnedCut> pins =
                tableBuckets.isEmpty()
                        ? new HashMap<>()
                        : new DvCutPinner(admin, tablePath, Thread::sleep, waitForCatchUp)
                                .pin(snapshotId, tableBuckets, holds);
        Map<TableBucket, List<DvBucketPart>> parts = new HashMap<>();
        int partCount = 0;
        int folded = 0;
        try {
            if (!pins.isEmpty()) {
                checkSnapshotIdentity(snapshotId, pins.values());
            }
            for (DvBucketPart bucket : buckets) {
                TableBucket tableBucket = bucket.getTableBucket();
                DvCutPinner.PinnedCut pin = pins.get(tableBucket);
                if (pin == null) {
                    if (groupCounts.get(tableBucket) > 1) {
                        // one part carrying all lake files of the bucket could exceed what an
                        // engine assigns in one message
                        parts.put(
                                tableBucket,
                                Collections.singletonList(bucket.withLakeSplitsPlannedAtStart()));
                    }
                    continue;
                }
                List<DvBucketPart> bucketParts = split(bucket, pin);
                parts.put(tableBucket, bucketParts);
                int unused = holds.get(tableBucket) - bucketParts.size();
                if (unused > 0 && !pin.folded) {
                    releaseHolds(admin, tablePath, bucketParts.get(0), unused);
                }
                partCount += bucketParts.size();
                folded += pin.folded ? 1 : 0;
            }
        } catch (Exception | Error e) {
            // no part reads with the cuts
            for (Map.Entry<TableBucket, DvCutPinner.PinnedCut> pin : pins.entrySet()) {
                if (!pin.getValue().folded) {
                    new DvMaskFetcher(admin, tablePath, Thread::sleep)
                            .release(
                                    DvMaskFetcher.releaseRequest(
                                            pin.getKey(),
                                            snapshotId,
                                            pin.getValue().cut,
                                            holds.get(pin.getKey())));
                }
            }
            throw e;
        }
        LOG.info(
                "Pinned the mask cuts of {} of {} buckets of {} for lake snapshot {} ({} read by"
                        + " key, their leaders having no masks yet); they are read in {} parts.",
                pins.size(),
                buckets.size(),
                tablePath,
                snapshotId,
                folded,
                partCount);
        return parts;
    }

    private void checkSnapshotIdentity(long snapshotId, Iterable<DvCutPinner.PinnedCut> pins)
            throws Exception {
        for (DvCutPinner.PinnedCut pin : pins) {
            if (pin.lakeSnapshotIdentity == null) {
                continue;
            }
            String current = DvLakeCalls.snapshotIdentity(lakeSource, snapshotId);
            if (!pin.lakeSnapshotIdentity.equals(current)) {
                throw LakeTableRolledBackException.ofReusedReadableSnapshot(
                        tablePath.toString(),
                        snapshotId,
                        pin.lakeSnapshotIdentity,
                        current == null ? "unknown commit" : current);
            }
            return;
        }
    }

    /**
     * Releases the holds {@code parts} took of the cuts they were pinned at, one hold per part, in
     * one release per bucket and cut, as the parts or the read that pinned them end: finished or
     * cancelled. Best effort, waiting at most {@code timeout}; a cut that is not released is
     * dropped once no request asks for it within the leader's idle timeout. Parts not pinned at
     * planning hold none and are skipped.
     */
    public static void releasePinnedCuts(
            DvAdmin admin, TablePath tablePath, Collection<DvBucketPart> parts, Duration timeout) {
        Map<String, DvBucketPart> cuts = new LinkedHashMap<>();
        Map<String, Integer> holds = new HashMap<>();
        for (DvBucketPart part : parts) {
            // a folded bucket's leader holds no cut
            if (!part.isPinned()
                    || part.getMaskLogEndOffset() < 0
                    || part.getReadMode() == DvBucketPart.ReadMode.FOLDED) {
                continue;
            }
            String cut =
                    part.getTableBucket()
                            + "@"
                            + part.getSnapshotId()
                            + "/"
                            + part.getMaskLogEndOffset();
            cuts.putIfAbsent(cut, part);
            holds.merge(cut, 1, Integer::sum);
        }
        if (cuts.isEmpty()) {
            return;
        }
        List<CompletableFuture<GetDvSnapshotResponse>> sent = new ArrayList<>();
        for (Map.Entry<String, DvBucketPart> cut : cuts.entrySet()) {
            sent.add(releaseHolds(admin, tablePath, cut.getValue(), holds.get(cut.getKey())));
        }
        try {
            CompletableFuture.allOf(sent.toArray(new CompletableFuture<?>[0]))
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            LOG.info(
                    "Released {} holds of the mask cuts of {} buckets of {} pinned for the read.",
                    holds.values().stream().mapToInt(Integer::intValue).sum(),
                    cuts.size(),
                    tablePath);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | TimeoutException e) {
            LOG.info(
                    "Could not release all {} mask cuts pinned for a read of {} ({}); the rest are"
                            + " dropped once they idle.",
                    cuts.size(),
                    tablePath,
                    e.toString());
        }
    }

    /**
     * Releases {@code holds} holds of the cut {@code part} was pinned at, without waiting; best
     * effort, a failed release is sent once more.
     */
    private static CompletableFuture<GetDvSnapshotResponse> releaseHolds(
            DvAdmin admin, TablePath tablePath, DvBucketPart part, int holds) {
        return new DvMaskFetcher(admin, tablePath, Thread::sleep)
                .release(
                        DvMaskFetcher.releaseRequest(
                                part.getTableBucket(),
                                part.getSnapshotId(),
                                part.getMaskLogEndOffset(),
                                holds));
    }

    /**
     * The parts of a pinned bucket: one, or groups of its lake files and its log. The parts of a
     * folded bucket all derive their masks of the cut its leader named from the keys, each those of
     * its lake files and the log from the snapshot's readable offset, so they read one cut.
     */
    private List<DvBucketPart> split(DvBucketPart bucket, DvCutPinner.PinnedCut pin) {
        List<List<LakeSplit>> groups = fileGroups(bucket.getLakeSplits());
        List<DvBucketPart> parts = new ArrayList<>();
        if (groups.size() <= 1) {
            parts.add(pinned(bucket, bucket.getLakeSplits(), DvBucketPart.WHOLE_BUCKET, pin));
            return parts;
        }
        for (int i = 0; i < groups.size(); i++) {
            parts.add(pinned(bucket, groups.get(i), i, pin));
        }
        long logStart = pin.folded ? bucket.getSnapshotLogOffset() : pin.logStartOffset;
        if (logStart < pin.cut) {
            parts.add(pinned(bucket, null, DvBucketPart.BUCKET_LOG, pin));
        }
        return parts;
    }

    private static DvBucketPart pinned(
            DvBucketPart bucket,
            @Nullable List<LakeSplit> lakeSplits,
            int fileGroup,
            DvCutPinner.PinnedCut pin) {
        if (pin.folded) {
            return DvBucketPart.pinnedFolded(
                    bucket.getTableBucket(),
                    bucket.getPartitionName(),
                    bucket.getSnapshotId(),
                    bucket.getSnapshotLogOffset(),
                    bucket.getStoppingOffset(),
                    lakeSplits,
                    fileGroup,
                    pin.cut);
        }
        return DvBucketPart.pinned(
                bucket.getTableBucket(),
                bucket.getPartitionName(),
                bucket.getSnapshotId(),
                bucket.getSnapshotLogOffset(),
                bucket.getStoppingOffset(),
                lakeSplits,
                fileGroup,
                pin.cut,
                pin.logStartOffset);
    }

    /** Groups lake splits by rows, so each group is a sizeable but bounded read. */
    private List<List<LakeSplit>> fileGroups(@Nullable List<LakeSplit> lakeSplits) {
        List<List<LakeSplit>> groups = new ArrayList<>();
        if (lakeSplits == null || lakeSplits.isEmpty()) {
            return groups;
        }
        List<LakeSplit> group = new ArrayList<>();
        long groupRows = 0;
        for (LakeSplit lakeSplit : lakeSplits) {
            // a split of unknown size counts as a fraction of a group
            long rowCount = DvLakeCalls.rowCount(lakeSplit);
            long rows = rowCount >= 0 ? rowCount : Math.max(1, fileGroupRows / 8);
            if (!group.isEmpty()
                    && (groupRows + rows > fileGroupRows || group.size() >= MAX_FILES_PER_GROUP)) {
                groups.add(group);
                group = new ArrayList<>();
                groupRows = 0;
            }
            group.add(lakeSplit);
            groupRows += rows;
        }
        groups.add(group);
        return groups;
    }
}
