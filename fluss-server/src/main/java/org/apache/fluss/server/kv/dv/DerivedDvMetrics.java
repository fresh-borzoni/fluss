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

import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.DescriptiveStatisticsHistogram;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.SimpleCounter;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Metrics of derived deletion vectors, shared by the buckets of a tablet server. */
public final class DerivedDvMetrics {

    /** Returns metrics that are not reported, for tests. */
    public static DerivedDvMetrics noop() {
        return new DerivedDvMetrics(
                new SimpleCounter(),
                new DescriptiveStatisticsHistogram(1),
                new SimpleCounter(),
                new DescriptiveStatisticsHistogram(1),
                new SimpleCounter(),
                new DescriptiveStatisticsHistogram(1),
                new SimpleCounter(),
                new AtomicInteger(),
                new AtomicLong(),
                new AtomicLong(),
                new AtomicLong(),
                new AtomicInteger(),
                new AtomicLong(),
                new AtomicInteger(),
                new AtomicLong(),
                new SimpleCounter(),
                new SimpleCounter());
    }

    public static DerivedDvMetrics of(TabletServerMetricGroup group) {
        return new DerivedDvMetrics(
                group.dvCatchUpCount(),
                group.dvCatchUpLatencyHistogram(),
                group.dvRebuildCount(),
                group.dvRebuildLatencyHistogram(),
                group.dvCheckpointCount(),
                group.dvCheckpointLatencyHistogram(),
                group.dvStaleSnapshotCount(),
                group.dvRebuildsInProgress(),
                group.dvLastRebuildMs(),
                group.dvReplayBacklog(),
                group.dvTailMemoryBytes(),
                group.dvSpilledTails(),
                group.dvSpilledTailBytes(),
                group.dvPinnedCuts(),
                group.dvPinnedCutBytes(),
                group.dvPinnedCutEvictionCount(),
                group.dvPinnedCutExpirationCount());
    }

    private final Counter catchUps;
    private final Histogram catchUpMs;
    private final Counter rebuilds;
    private final Histogram rebuildMs;
    private final Counter checkpoints;
    private final Histogram checkpointMs;
    private final Counter staleSnapshots;
    private final AtomicInteger rebuildsInProgress;
    private final AtomicLong lastRebuildMs;
    // log offsets the running rebuilds of all buckets still have to replay
    private final AtomicLong replayBacklog;
    // estimated heap bytes of the log tails of all buckets
    private final AtomicLong tailMemoryBytes;
    // buckets whose log tail is spilled to disk, and the bytes they spilled
    private final AtomicInteger spilledTails;
    private final AtomicLong spilledTailBytes;
    // pinned cuts of all buckets and their bytes; evictions count cuts dropped over the memory
    // limit, expirations count cuts no request asked for within the idle timeout
    private final AtomicInteger pinnedCuts;
    private final AtomicLong pinnedCutBytes;
    private final Counter pinnedCutEvictions;
    private final Counter pinnedCutExpirations;

    private DerivedDvMetrics(
            Counter catchUps,
            Histogram catchUpMs,
            Counter rebuilds,
            Histogram rebuildMs,
            Counter checkpoints,
            Histogram checkpointMs,
            Counter staleSnapshots,
            AtomicInteger rebuildsInProgress,
            AtomicLong lastRebuildMs,
            AtomicLong replayBacklog,
            AtomicLong tailMemoryBytes,
            AtomicInteger spilledTails,
            AtomicLong spilledTailBytes,
            AtomicInteger pinnedCuts,
            AtomicLong pinnedCutBytes,
            Counter pinnedCutEvictions,
            Counter pinnedCutExpirations) {
        this.catchUps = catchUps;
        this.catchUpMs = catchUpMs;
        this.rebuilds = rebuilds;
        this.rebuildMs = rebuildMs;
        this.checkpoints = checkpoints;
        this.checkpointMs = checkpointMs;
        this.staleSnapshots = staleSnapshots;
        this.rebuildsInProgress = rebuildsInProgress;
        this.lastRebuildMs = lastRebuildMs;
        this.replayBacklog = replayBacklog;
        this.tailMemoryBytes = tailMemoryBytes;
        this.spilledTails = spilledTails;
        this.spilledTailBytes = spilledTailBytes;
        this.pinnedCuts = pinnedCuts;
        this.pinnedCutBytes = pinnedCutBytes;
        this.pinnedCutEvictions = pinnedCutEvictions;
        this.pinnedCutExpirations = pinnedCutExpirations;
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

    void tailMemoryChanged(long delta) {
        tailMemoryBytes.addAndGet(delta);
    }

    void spilledTailsChanged(int delta) {
        spilledTails.addAndGet(delta);
    }

    void spilledTailBytesChanged(long delta) {
        spilledTailBytes.addAndGet(delta);
    }

    long tailMemoryBytes() {
        return tailMemoryBytes.get();
    }

    int spilledTails() {
        return spilledTails.get();
    }

    long spilledTailBytes() {
        return spilledTailBytes.get();
    }

    void pinnedCutAdded(long bytes) {
        pinnedCuts.incrementAndGet();
        pinnedCutBytes.addAndGet(bytes);
    }

    void pinnedCutRemoved(long bytes) {
        pinnedCuts.decrementAndGet();
        pinnedCutBytes.addAndGet(-bytes);
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
}
