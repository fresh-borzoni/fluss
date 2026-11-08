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
                new AtomicLong(),
                new AtomicLong(),
                new AtomicInteger(),
                new AtomicLong());
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
                group.dvOwnerStepMaxNanos(),
                group.dvOwnerMailboxDelayMaxNanos(),
                group.dvRebuildsInProgress(),
                group.dvLastRebuildMs());
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
    // the longest owner step and mailbox wait since the gauges were last read, in nanoseconds
    private final AtomicLong ownerStepMaxNanos;
    private final AtomicLong ownerMailboxDelayMaxNanos;
    private final AtomicInteger rebuildsInProgress;
    private final AtomicLong lastRebuildMs;

    private DerivedDvMetrics(
            Counter catchUps,
            Histogram catchUpMs,
            Counter rebuilds,
            Histogram rebuildMs,
            Counter checkpoints,
            Histogram checkpointMs,
            Counter staleSnapshots,
            Counter ownerFaults,
            AtomicLong ownerStepMaxNanos,
            AtomicLong ownerMailboxDelayMaxNanos,
            AtomicInteger rebuildsInProgress,
            AtomicLong lastRebuildMs) {
        this.catchUps = catchUps;
        this.catchUpMs = catchUpMs;
        this.rebuilds = rebuilds;
        this.rebuildMs = rebuildMs;
        this.checkpoints = checkpoints;
        this.checkpointMs = checkpointMs;
        this.staleSnapshots = staleSnapshots;
        this.ownerFaults = ownerFaults;
        this.ownerStepMaxNanos = ownerStepMaxNanos;
        this.ownerMailboxDelayMaxNanos = ownerMailboxDelayMaxNanos;
        this.rebuildsInProgress = rebuildsInProgress;
        this.lastRebuildMs = lastRebuildMs;
    }

    /** Records a catch-up that applied the index changes after the covered snapshot. */
    void caughtUp(long millis) {
        catchUps.inc();
        catchUpMs.update(millis);
    }

    /** Records a rebuild, which reads a checkpoint and the chain. */
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

    int rebuildsInProgress() {
        return rebuildsInProgress.get();
    }

    long lastRebuildMs() {
        return lastRebuildMs.get();
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

    /** A step of a bucket's owner took {@code nanos}. */
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

    long ownerFaults() {
        return ownerFaults.getCount();
    }
}
