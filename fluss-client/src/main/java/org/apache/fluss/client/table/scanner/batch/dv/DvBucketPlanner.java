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

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.LakeTableRolledBackException;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Plans the parts of a bounded union read of a table with deletion vectors. It pins one cut per
 * bucket on the buckets' leaders, so several parts can read a bucket with the same masks: groups of
 * its lake files and its log. Planning fetches no masks.
 */
@PublicEvolving
public class DvBucketPlanner {

    private static final Logger LOG = LoggerFactory.getLogger(DvBucketPlanner.class);

    /** The rows of lake files one part reads at most when a bucket is read in several parts. */
    public static final long DEFAULT_FILE_GROUP_ROWS =
            ConfigOptions.CLIENT_SCANNER_DELETION_VECTORS_FILE_GROUP_ROWS.defaultValue();

    /** The lake splits one part reads at most when a bucket is read in several parts. */
    public static final int MAX_FILES_PER_GROUP = 64;

    private final Admin admin;
    private final TablePath tablePath;
    private final LakeSource<LakeSplit> lakeSource;
    private long fileGroupRows = DEFAULT_FILE_GROUP_ROWS;

    public DvBucketPlanner(Admin admin, TablePath tablePath, LakeSource<LakeSplit> lakeSource) {
        this.admin = admin;
        this.tablePath = tablePath;
        this.lakeSource = lakeSource;
    }

    /** Sets the rows of lake files one part reads at most. */
    public DvBucketPlanner withFileGroupRows(long fileGroupRows) {
        checkArgument(fileGroupRows > 0, "The rows of a file group must be positive.");
        this.fileGroupRows = fileGroupRows;
        return this;
    }

    /**
     * Pins a cut of lake snapshot {@code snapshotId} for the buckets of {@code buckets}, parts that
     * each read a whole bucket ({@link DvBucketPart#ofBucket}), and splits each pinned bucket into
     * the parts that read it. Returns those parts by bucket. A bucket that could not be pinned is
     * absent and is read by its given part, which fetches masks when it starts. The map is empty if
     * the server does not pin cuts.
     *
     * @throws LakeTableRolledBackException if the lake snapshot under {@code snapshotId} is not the
     *     commit the masks were derived for, as after a Paimon rollback that reused the id
     */
    public Map<TableBucket, List<DvBucketPart>> pin(long snapshotId, List<DvBucketPart> buckets)
            throws Exception {
        List<TableBucket> tableBuckets = new ArrayList<>(buckets.size());
        for (DvBucketPart bucket : buckets) {
            tableBuckets.add(bucket.getTableBucket());
        }
        Map<TableBucket, DvCutPinner.PinnedCut> pins =
                new DvCutPinner(admin, tablePath, Thread::sleep).pin(snapshotId, tableBuckets);
        Map<TableBucket, List<DvBucketPart>> parts = new HashMap<>();
        if (pins == null || pins.isEmpty()) {
            return parts;
        }
        checkSnapshotIdentity(snapshotId, pins.values());
        int partCount = 0;
        for (DvBucketPart bucket : buckets) {
            DvCutPinner.PinnedCut pin = pins.get(bucket.getTableBucket());
            if (pin != null) {
                List<DvBucketPart> bucketParts = split(bucket, pin);
                parts.put(bucket.getTableBucket(), bucketParts);
                partCount += bucketParts.size();
            }
        }
        LOG.info(
                "Pinned the mask cuts of {} of {} buckets of {} for lake snapshot {}; they are read"
                        + " in {} parts.",
                parts.size(),
                buckets.size(),
                tablePath,
                snapshotId,
                partCount);
        return parts;
    }

    private void checkSnapshotIdentity(long snapshotId, Iterable<DvCutPinner.PinnedCut> pins)
            throws Exception {
        for (DvCutPinner.PinnedCut pin : pins) {
            if (pin.lakeSnapshotIdentity == null) {
                continue;
            }
            String current = lakeSource.snapshotIdentity(snapshotId);
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

    /** The parts of a pinned bucket: one, or groups of its lake files and its log. */
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
        if (pin.logStartOffset < pin.cut) {
            parts.add(pinned(bucket, null, DvBucketPart.BUCKET_LOG, pin));
        }
        return parts;
    }

    private static DvBucketPart pinned(
            DvBucketPart bucket,
            @Nullable List<LakeSplit> lakeSplits,
            int fileGroup,
            DvCutPinner.PinnedCut pin) {
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
            long rows =
                    lakeSplit.rowCount() >= 0
                            ? lakeSplit.rowCount()
                            : Math.max(1, fileGroupRows / 8);
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
