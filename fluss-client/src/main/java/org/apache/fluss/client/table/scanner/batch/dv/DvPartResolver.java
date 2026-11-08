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
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketPartReader.ReadStats;
import org.apache.fluss.client.table.scanner.batch.dv.DvPartMachine.Target;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static org.apache.fluss.client.table.scanner.batch.dv.DvPartMachine.MAX_REPLANS;
import static org.apache.fluss.client.table.scanner.batch.dv.DvPartMachine.REPLAN_BACKOFF_MS;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Performs the action of each resolving {@link DvPartState} of a part: pins a cut, plans the bucket
 * again, fetches the masks of a cut, derives them from the keys, or checks the masks a restored
 * part kept. Each action ends in a {@link DvPartEvent}, which {@link DvPartMachine} applies;
 * failures are events too, so no action decides what follows.
 */
@Internal
final class DvPartResolver {

    private static final Logger LOG = LoggerFactory.getLogger(DvPartResolver.class);

    private final DvPartMachine machine;
    private final DvBucketPart part;
    private final TableBucket bucket;
    private final DvBucketPartReader.Context context;
    private final DvCutFetcher cuts;
    private final DvKeyFolder folder;
    private final ReadStats stats;

    DvPartResolver(DvPartMachine machine, DvBucketPartReader.Context context, ReadStats stats) {
        this.machine = machine;
        this.part = machine.part();
        this.bucket = part.getTableBucket();
        this.context = context;
        this.cuts = new DvCutFetcher(part, context, stats);
        this.folder = new DvKeyFolder(bucket, context);
        this.stats = stats;
    }

    /** Runs the actions of the part's states until it has its masks, failed, or was closed. */
    void run() {
        while (machine.state().isResolving()) {
            machine.apply(act());
        }
    }

    /** Performs the action of the part's current state. */
    DvPartEvent act() {
        switch (machine.state()) {
            case PLANNED:
                return pin();
            case REPLANNING:
                return replan();
            case PINNED:
                return fetchOwnCut();
            case PINNED_AT_PLANNING:
                return fetchPlannedCut();
            case FOLDING:
                return fold();
            case RESUMING:
                return checkKeptMasks();
            default:
                throw new IllegalStateException(
                        String.format(
                                "%s does not resolve in state %s.",
                                part.describe(), machine.state()));
        }
    }

    /** PLANNED: pins a cut of the target snapshot. */
    private DvPartEvent pin() {
        Target target = machine.target();
        try {
            return DvPartEvent.pinned(cuts.pin(target.snapshotId, target.lakeSplits));
        } catch (Exception e) {
            return DvPartEvent.of(e);
        }
    }

    /** REPLANNING: plans the bucket's lake files on the snapshot the part reads next. */
    private DvPartEvent replan() {
        Target target = machine.target();
        try {
            switch (machine.replan()) {
                case OWN_FILES:
                    return DvPartEvent.replanned(
                            new Target(
                                    target.snapshotId,
                                    target.snapshotLogOffset,
                                    planBucket(target.snapshotId, target.snapshotLogOffset)));
                case COVERED:
                    return DvPartEvent.replanned(planCovered(target, machine.covered()));
                case READABLE_BY_KEY:
                    return DvPartEvent.replanned(planReadable(target));
                default:
                    throw new IllegalStateException("Unknown re-plan " + machine.replan());
            }
        } catch (Exception e) {
            return DvPartEvent.failed(e);
        }
    }

    /**
     * The leader covers another snapshot, e.g. one tiering registered after planning. Read it from
     * the readable offset the leader named. Without one (a read that waits), read the coordinator's
     * readable snapshot: the leader switches to it.
     */
    private Target planCovered(Target target, StaleSnapshotException stale) throws Exception {
        // a leader that still switches serves the snapshot before right away
        boolean behind = stale.isBehind();
        long targetId;
        long targetOffset;
        if (stale.getCurrentStartOffset() >= 0) {
            targetId = stale.getCurrentSnapshotId();
            targetOffset = stale.getCurrentStartOffset();
        } else {
            LakeSnapshot readable = readableLakeSnapshotOrEmpty();
            targetId = readable == null ? -1L : readable.getSnapshotId();
            targetOffset = readableOffset(readable);
        }
        List<LakeSplit> lakeSplits = target.lakeSplits;
        if (targetId != target.snapshotId) {
            lakeSplits = planBucket(targetId, targetOffset);
            LOG.info(
                    "The leader of {} does not serve the masks of snapshot {}{};"
                            + " reading snapshot {} instead ({}/{}).",
                    part.describe(),
                    target.snapshotId,
                    behind ? " while it switches to it" : "",
                    targetId,
                    machine.replans(),
                    MAX_REPLANS);
        }
        if (!behind) {
            context.sleep(REPLAN_BACKOFF_MS);
        }
        return new Target(targetId, targetOffset, lakeSplits);
    }

    /** The coordinator's readable snapshot, to read the bucket by key. */
    private Target planReadable(Target target) throws Exception {
        LakeSnapshot readable = readableLakeSnapshotOrEmpty();
        long readableId = readable == null ? -1L : readable.getSnapshotId();
        long readableOffset = readableOffset(readable);
        List<LakeSplit> readableSplits =
                readableId == target.snapshotId && readableOffset == target.snapshotLogOffset
                        ? target.lakeSplits
                        : planBucket(readableId, readableOffset);
        return new Target(readableId, readableOffset, readableSplits);
    }

    /**
     * PINNED: checks the snapshot names the commit the cut's masks are of, and fetches all masks of
     * the cut now: the leader may drop it while the part is read. The pin's answer carries those of
     * the first lake split.
     */
    private DvPartEvent fetchOwnCut() {
        Target target = machine.target();
        GetDvSnapshotResponse pinned = machine.pinAnswer();
        try {
            cuts.checkSnapshotIdentity(target.snapshotId, pinned);
        } catch (Exception e) {
            return DvPartEvent.failed(e);
        }
        try {
            return DvPartEvent.masks(
                    cuts.fetchCut(
                            target.snapshotId,
                            machine.cut(),
                            target.lakeSplits,
                            pinned,
                            -1L,
                            part.readsLog()));
        } catch (Exception e) {
            return DvPartEvent.of(e);
        }
    }

    /**
     * PINNED_AT_PLANNING: fetches the masks of the part's own lake files, and the log mask in
     * pages, from the offset the cut's masks started at when planning pinned it.
     */
    private DvPartEvent fetchPlannedCut() {
        try {
            return DvPartEvent.masks(
                    cuts.fetchCut(
                            part.getSnapshotId(),
                            machine.cut(),
                            part.getLakeSplits(),
                            null,
                            part.getPinnedLogStartOffset(),
                            part.readsLog()));
        } catch (Exception e) {
            return DvPartEvent.of(e);
        }
    }

    /** FOLDING: derives the masks of the cut from the keys. */
    private DvPartEvent fold() {
        long start = System.nanoTime();
        try {
            if (machine.foldsByKey()) {
                stats.folded = true;
            }
            return DvPartEvent.derived(
                    folder.deriveMasks(
                            machine.foldPart(), machine.foldLogStart(), machine.foldCut()));
        } catch (Exception e) {
            return DvPartEvent.failed(e);
        } finally {
            stats.deriveNanos += System.nanoTime() - start;
        }
    }

    /** RESUMING: checks the masks a restored part kept are of its cut and snapshot commit. */
    private DvPartEvent checkKeptMasks() {
        DvBucketMaskState stored = part.getMaskState();
        try {
            checkState(
                    stored != null && stored.getLogEndOffset() == part.getMaskLogEndOffset(),
                    "The masks kept by %s are of another cut.",
                    part);
            String registered = stored.getLakeSnapshotIdentity();
            if (registered != null) {
                cuts.checkSnapshotIdentity(part.getSnapshotId(), registered);
            }
            return DvPartEvent.masks(stored);
        } catch (Exception e) {
            return DvPartEvent.failed(e);
        }
    }

    @Nullable
    private LakeSnapshot readableLakeSnapshotOrEmpty() throws Exception {
        try {
            return context.readableLakeSnapshot();
        } catch (Exception e) {
            if (ExceptionUtils.findThrowable(e, LakeTableSnapshotNotExistException.class)
                    .isPresent()) {
                return null; // nothing tiered: the empty lake, whose log is read from offset 0
            }
            throw e;
        }
    }

    private long readableOffset(@Nullable LakeSnapshot lakeSnapshot) {
        if (lakeSnapshot == null) {
            return -1L;
        }
        Long offset = lakeSnapshot.getTableBucketsOffset().get(bucket);
        return offset == null || offset < 0 ? -1L : offset;
    }

    @Nullable
    private List<LakeSplit> planBucket(long snapshotId, long snapshotLogOffset) throws IOException {
        // a bucket with nothing tiered in the snapshot has its rows in the log only
        return snapshotId < 0 || snapshotLogOffset < 0
                ? null
                : context.planLakeSplits(snapshotId, bucket, part.getPartitionName());
    }
}
