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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.DescriptiveStatisticsHistogram;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.SimpleCounter;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Metrics of derived deletion vectors, shared by the buckets of a tablet server. */
public final class DerivedDvMetrics {

    /** Returns metrics that are not reported. */
    @VisibleForTesting
    public static DerivedDvMetrics noop() {
        return new DerivedDvMetrics(
                new SimpleCounter(),
                new DescriptiveStatisticsHistogram(1),
                new SimpleCounter(),
                new DescriptiveStatisticsHistogram(1),
                new SimpleCounter(),
                new DescriptiveStatisticsHistogram(1),
                new SimpleCounter(),
                new SimpleCounter(),
                new SimpleCounter(),
                new SimpleCounter(),
                new AtomicLong(),
                new AtomicLong(),
                new AtomicInteger(),
                new AtomicLong(),
                new AtomicLong(),
                new AtomicLong(),
                new AtomicLong(),
                new AtomicInteger(),
                new AtomicLong(),
                new SimpleCounter(),
                new SimpleCounter(),
                new SimpleCounter(),
                new MaskRequestHistograms(
                        new DescriptiveStatisticsHistogram(1),
                        new DescriptiveStatisticsHistogram(1),
                        new DescriptiveStatisticsHistogram(1),
                        new DescriptiveStatisticsHistogram(1),
                        new DescriptiveStatisticsHistogram(1),
                        new DescriptiveStatisticsHistogram(1),
                        new DescriptiveStatisticsHistogram(1)),
                new ChangeListCheckpoints(
                        new SimpleCounter(),
                        new SimpleCounter(),
                        new DescriptiveStatisticsHistogram(1),
                        new SimpleCounter(),
                        new SimpleCounter()));
    }

    public static DerivedDvMetrics of(TabletServerMetricGroup group) {
        return new DerivedDvMetrics(
                group.dvSwitchCount(),
                group.dvSwitchLatencyHistogram(),
                group.dvRebuildCount(),
                group.dvRebuildLatencyHistogram(),
                group.dvCheckpointCount(),
                group.dvCheckpointLatencyHistogram(),
                group.dvStaleSnapshotCount(),
                group.dvOwnerFaultCount(),
                group.dvHandOffStallCount(),
                group.dvHandOffOverflowCount(),
                group.dvOwnerStepMaxNanos(),
                group.dvOwnerMailboxDelayMaxNanos(),
                group.dvRebuildsInProgress(),
                group.dvLastRebuildMs(),
                group.dvReplayBacklog(),
                group.dvHeapBytes(),
                group.dvChangeListBytes(),
                group.dvPinnedCuts(),
                group.dvPinnedCutBytes(),
                group.dvPinnedCutEvictionCount(),
                group.dvPinnedCutExpirationCount(),
                group.dvMaskFrontierCount(),
                new MaskRequestHistograms(
                        group.dvMaskRequestHistogram(),
                        group.dvMaskWaitSwitchHistogram(),
                        group.dvMaskWaitDeriveHistogram(),
                        group.dvMaskWaitReplayHistogram(),
                        group.dvMaskApplyHistogram(),
                        group.dvMaskResolveWaitHistogram(),
                        group.dvMaskBuildHistogram()),
                new ChangeListCheckpoints(
                        group.dvChangeListCheckpointCount(),
                        group.dvChangeListCheckpointSkipCount(),
                        group.dvChangeListCheckpointLatencyHistogram(),
                        group.dvChangeListCheckpointUploadBytes(),
                        group.dvRebuildFromCheckpointCount()));
    }

    private final Counter catchUps;
    private final Histogram catchUpMs;
    private final Counter rebuilds;
    private final Histogram rebuildMs;
    private final Counter checkpoints;
    private final Histogram checkpointMs;
    private final Counter staleSnapshots;
    // steps of a bucket's owner that failed unexpectedly
    private final Counter ownerFaults;
    // writes that waited more than a second for their bucket's owner
    private final Counter handOffStalls;
    // hand-offs dropped as their bucket's owner fell behind
    private final Counter handOffsDropped;
    // the longest owner step and mailbox wait since the gauges were last read, in nanoseconds
    private final AtomicLong ownerStepMaxNanos;
    private final AtomicLong ownerMailboxDelayMaxNanos;
    private final AtomicInteger rebuildsInProgress;
    private final AtomicLong lastRebuildMs;
    // log offsets the running rebuilds of all buckets still have to replay
    private final AtomicLong replayBacklog;
    // estimated heap bytes of the deletion vector state of all buckets
    private final AtomicLong heapBytes;
    // the bytes the change lists of all buckets hold on disk
    private final AtomicLong changeListBytes;
    // pinned cuts of all buckets and their bytes; evictions count cuts dropped over the memory
    // limit, expirations count cuts no request asked for within the idle timeout
    private final AtomicInteger pinnedCuts;
    private final AtomicLong pinnedCutBytes;
    private final Counter pinnedCutEvictions;
    private final Counter pinnedCutExpirations;
    // reads that do not wait served below the high watermark as lookups were pending
    private final Counter frontierReads;
    private final MaskRequestHistograms maskRequests;
    private final ChangeListCheckpoints changeListCheckpoints;

    private DerivedDvMetrics(
            Counter catchUps,
            Histogram catchUpMs,
            Counter rebuilds,
            Histogram rebuildMs,
            Counter checkpoints,
            Histogram checkpointMs,
            Counter staleSnapshots,
            Counter ownerFaults,
            Counter handOffStalls,
            Counter handOffsDropped,
            AtomicLong ownerStepMaxNanos,
            AtomicLong ownerMailboxDelayMaxNanos,
            AtomicInteger rebuildsInProgress,
            AtomicLong lastRebuildMs,
            AtomicLong replayBacklog,
            AtomicLong heapBytes,
            AtomicLong changeListBytes,
            AtomicInteger pinnedCuts,
            AtomicLong pinnedCutBytes,
            Counter pinnedCutEvictions,
            Counter pinnedCutExpirations,
            Counter frontierReads,
            MaskRequestHistograms maskRequests,
            ChangeListCheckpoints changeListCheckpoints) {
        this.catchUps = catchUps;
        this.catchUpMs = catchUpMs;
        this.rebuilds = rebuilds;
        this.rebuildMs = rebuildMs;
        this.checkpoints = checkpoints;
        this.checkpointMs = checkpointMs;
        this.staleSnapshots = staleSnapshots;
        this.ownerFaults = ownerFaults;
        this.handOffStalls = handOffStalls;
        this.handOffsDropped = handOffsDropped;
        this.ownerStepMaxNanos = ownerStepMaxNanos;
        this.ownerMailboxDelayMaxNanos = ownerMailboxDelayMaxNanos;
        this.rebuildsInProgress = rebuildsInProgress;
        this.lastRebuildMs = lastRebuildMs;
        this.replayBacklog = replayBacklog;
        this.heapBytes = heapBytes;
        this.changeListBytes = changeListBytes;
        this.pinnedCuts = pinnedCuts;
        this.pinnedCutBytes = pinnedCutBytes;
        this.pinnedCutEvictions = pinnedCutEvictions;
        this.pinnedCutExpirations = pinnedCutExpirations;
        this.frontierReads = frontierReads;
        this.maskRequests = maskRequests;
        this.changeListCheckpoints = changeListCheckpoints;
    }

    /** Records a change list saved with a KV snapshot, and the bytes it uploaded. */
    void changeListCheckpointed(long millis, long uploadedBytes) {
        changeListCheckpoints.attached.inc();
        changeListCheckpoints.latencyMs.update(millis);
        changeListCheckpoints.uploadBytes.inc(uploadedBytes);
    }

    /** Records a KV snapshot of a bucket with deletion vectors saved without its change list. */
    void changeListCheckpointSkipped() {
        changeListCheckpoints.skipped.inc();
    }

    /** Records a rebuild that loaded the change list saved with the KV snapshot it restored. */
    void rebuiltFromCheckpoint() {
        changeListCheckpoints.rebuildsFromCheckpoint.inc();
    }

    /** Records a catch-up that applied the index changes after the covered snapshot. */
    void caughtUp(long millis) {
        catchUps.inc();
        catchUpMs.update(millis);
    }

    /**
     * Records a rebuild. A full rebuild reads a checkpoint, the chain and the log. A tail-only
     * rebuild replays the log tail alone, for an empty lake or after a failed replay.
     */
    void rebuilt(long millis) {
        rebuilds.inc();
        rebuildMs.update(millis);
        lastRebuildMs.set(millis);
    }

    void rebuildStarted() {
        rebuildsInProgress.incrementAndGet();
    }

    void rebuildEnded() {
        rebuildsInProgress.decrementAndGet();
    }

    void replayBacklogChanged(long delta) {
        replayBacklog.addAndGet(delta);
    }

    int rebuildsInProgress() {
        return rebuildsInProgress.get();
    }

    long lastRebuildMs() {
        return lastRebuildMs.get();
    }

    long replayBacklog() {
        return replayBacklog.get();
    }

    void heapBytesChanged(long delta) {
        heapBytes.addAndGet(delta);
    }

    void changeListBytesChanged(long delta) {
        changeListBytes.addAndGet(delta);
    }

    long heapBytes() {
        return heapBytes.get();
    }

    long changeListBytes() {
        return changeListBytes.get();
    }

    void pinnedCutAdded(long bytes) {
        pinnedCuts.incrementAndGet();
        pinnedCutBytes.addAndGet(bytes);
    }

    void pinnedCutRemoved(long bytes) {
        pinnedCuts.decrementAndGet();
        pinnedCutBytes.addAndGet(-bytes);
    }

    /** The pin requests a cut remembers changed its heap by {@code delta} bytes. */
    void pinnedCutBytesChanged(long delta) {
        pinnedCutBytes.addAndGet(delta);
    }

    void pinnedCutEvicted() {
        pinnedCutEvictions.inc();
    }

    void pinnedCutExpired() {
        pinnedCutExpirations.inc();
    }

    int pinnedCuts() {
        return pinnedCuts.get();
    }

    long pinnedCutBytes() {
        return pinnedCutBytes.get();
    }

    long pinnedCutEvictions() {
        return pinnedCutEvictions.getCount();
    }

    long pinnedCutExpirations() {
        return pinnedCutExpirations.getCount();
    }

    long rebuildCount() {
        return rebuilds.getCount();
    }

    void checkpointed(long millis) {
        checkpoints.inc();
        checkpointMs.update(millis);
    }

    void staleSnapshot() {
        staleSnapshots.inc();
    }

    /**
     * Records a write that waited more than a second for its bucket's owner to take its changes.
     */
    void handOffStalled() {
        handOffStalls.inc();
    }

    /** A step of a bucket's owner took {@code nanos}, including the reads it answered. */
    void ownerStep(long nanos) {
        ownerStepMaxNanos.accumulateAndGet(nanos, Math::max);
    }

    /** A step waited {@code nanos} in its bucket's mailbox. */
    void ownerMailboxDelay(long nanos) {
        ownerMailboxDelayMaxNanos.accumulateAndGet(nanos, Math::max);
    }

    /** Records a step of a bucket's owner that failed unexpectedly. */
    void ownerFault() {
        ownerFaults.inc();
    }

    /** {@code batches} hand-offs were dropped, as their bucket's owner fell behind. */
    void handOffsDropped(long batches) {
        handOffsDropped.inc(batches);
    }

    @VisibleForTesting
    long handOffsDropped() {
        return handOffsDropped.getCount();
    }

    /** Writes that waited more than a second for their bucket's owner. */
    @VisibleForTesting
    long handOffStalls() {
        return handOffStalls.getCount();
    }

    @VisibleForTesting
    long ownerFaults() {
        return ownerFaults.getCount();
    }

    /** Records a read that does not wait served below the high watermark: lookups were pending. */
    void frontierServed() {
        frontierReads.inc();
    }

    long frontierReads() {
        return frontierReads.getCount();
    }

    /** Records the steps of a union read's mask request, each in ms. */
    void maskRequest(
            long totalMs,
            long waitMoveMs,
            long waitDeriveMs,
            long waitReplayMs,
            long applyMs,
            long resolveWaitMs,
            long buildMs) {
        maskRequests.total.update(totalMs);
        maskRequests.waitMove.update(waitMoveMs);
        maskRequests.waitDerive.update(waitDeriveMs);
        maskRequests.waitReplay.update(waitReplayMs);
        maskRequests.apply.update(applyMs);
        maskRequests.resolveWait.update(resolveWaitMs);
        maskRequests.build.update(buildMs);
    }

    /** The longest wait for lake-position lookups of the mask requests recorded last. */
    @VisibleForTesting
    long maskResolveWaitMaxMs() {
        return maskRequests.resolveWait.getStatistics().getMax();
    }

    /** The histograms of the steps of union reads' mask requests. */
    /** The metrics of change lists saved with KV snapshots. */
    private static final class ChangeListCheckpoints {
        final Counter attached;
        final Counter skipped;
        final Histogram latencyMs;
        final Counter uploadBytes;
        final Counter rebuildsFromCheckpoint;

        ChangeListCheckpoints(
                Counter attached,
                Counter skipped,
                Histogram latencyMs,
                Counter uploadBytes,
                Counter rebuildsFromCheckpoint) {
            this.attached = attached;
            this.skipped = skipped;
            this.latencyMs = latencyMs;
            this.uploadBytes = uploadBytes;
            this.rebuildsFromCheckpoint = rebuildsFromCheckpoint;
        }
    }

    private static final class MaskRequestHistograms {
        final Histogram total;
        final Histogram waitMove;
        final Histogram waitDerive;
        final Histogram waitReplay;
        final Histogram apply;
        final Histogram resolveWait;
        final Histogram build;

        MaskRequestHistograms(
                Histogram total,
                Histogram waitMove,
                Histogram waitDerive,
                Histogram waitReplay,
                Histogram apply,
                Histogram resolveWait,
                Histogram build) {
            this.total = total;
            this.waitMove = waitMove;
            this.waitDerive = waitDerive;
            this.waitReplay = waitReplay;
            this.apply = apply;
            this.resolveWait = resolveWait;
            this.build = build;
        }
    }
}
