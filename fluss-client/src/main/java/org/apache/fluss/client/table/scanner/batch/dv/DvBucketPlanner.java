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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Plans the parts of a bounded union read of a table with deletion vectors. A bucket with more lake
 * files than one part reads is read in several parts, groups of its lake files and its log, and
 * planning pins one cut on its leader so they all read it with the same masks. A bucket read by one
 * part is not pinned: its part pins its own cut when it starts. Planning fetches no masks.
 *
 * <p>The leader keeps a cut while the parts' requests ask for it. A part that starts after the
 * leader dropped the cut derives the cut's masks from the keys.
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
    private long requestTimeoutMs = ConfigOptions.CLIENT_REQUEST_TIMEOUT.defaultValue().toMillis();

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

    /** Sets how long a pin waits for its leader's answer ({@code client.request-timeout}). */
    public DvBucketPlanner withRequestTimeout(Duration requestTimeout) {
        this.requestTimeoutMs = requestTimeout.toMillis();
        return this;
    }

    /**
     * Pins a cut of lake snapshot {@code snapshotId} for each of {@code buckets} (parts that each
     * read a whole bucket, {@link DvBucketPart#ofBucket}) with more lake files than one part reads,
     * and splits the bucket into the parts that read it. Returns those parts by bucket. Any other
     * bucket is read by its given part, absent from the result, which pins its own cut when it
     * starts. A bucket whose cut could not be pinned is read by one part too, which carries none of
     * its lake files and plans them when it starts ({@link
     * DvBucketPart#withLakeSplitsPlannedAtStart()}).
     *
     * @throws LakeTableRolledBackException if the lake snapshot under {@code snapshotId} is not the
     *     commit the masks were derived for, as after a Paimon rollback that reused the id
     */
    public Map<TableBucket, List<DvBucketPart>> pin(long snapshotId, List<DvBucketPart> buckets)
            throws Exception {
        List<DvBucketPart> splitBuckets = new ArrayList<>();
        List<TableBucket> tableBuckets = new ArrayList<>();
        for (DvBucketPart bucket : buckets) {
            if (fileGroups(bucket.getLakeSplits()).size() > 1) {
                splitBuckets.add(bucket);
                tableBuckets.add(bucket.getTableBucket());
            }
        }
        Map<TableBucket, List<DvBucketPart>> parts = new HashMap<>();
        if (splitBuckets.isEmpty()) {
            return parts;
        }
        Map<TableBucket, DvCutPinner.PinnedCut> pins =
                new DvCutPinner(admin, tablePath, Thread::sleep, waitForCatchUp, requestTimeoutMs)
                        .pin(snapshotId, tableBuckets);
        if (!pins.isEmpty()) {
            checkSnapshotIdentity(snapshotId, pins.values());
        }
        int partCount = 0;
        int folded = 0;
        for (DvBucketPart bucket : splitBuckets) {
            TableBucket tableBucket = bucket.getTableBucket();
            DvCutPinner.PinnedCut pin = pins.get(tableBucket);
            if (pin == null) {
                // one part carrying all lake files of the bucket could exceed what an engine
                // assigns in one message
                parts.put(
                        tableBucket,
                        Collections.singletonList(bucket.withLakeSplitsPlannedAtStart()));
                continue;
            }
            List<DvBucketPart> bucketParts = split(bucket, pin);
            parts.put(tableBucket, bucketParts);
            partCount += bucketParts.size();
            folded += pin.folded ? 1 : 0;
        }
        LOG.info(
                "Pinned the mask cuts of {} of {} buckets of {} that are read in several parts,"
                        + " for lake snapshot {} ({} read by key, their leaders having no masks"
                        + " yet); they are read in {} parts.",
                pins.size(),
                splitBuckets.size(),
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
     * The parts of a pinned bucket: groups of its lake files and its log. The parts of a folded
     * bucket all derive their masks of the cut its leader named from the keys, each those of its
     * lake files and the log from the snapshot's readable offset, so they read one cut.
     */
    private List<DvBucketPart> split(DvBucketPart bucket, DvCutPinner.PinnedCut pin) {
        List<List<LakeSplit>> groups = fileGroups(bucket.getLakeSplits());
        List<DvBucketPart> parts = new ArrayList<>();
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
