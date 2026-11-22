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

package org.apache.fluss.flink.tiering.source.metrics;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metrics.MetricNames;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;

/**
 * Metrics for the Flink-based tiering source.
 *
 * <p>All metrics are registered under group "fluss.tieringService", as a child of {@link
 * SourceReaderMetricGroup} for reader metrics or {@link SplitEnumeratorMetricGroup} for enumerator
 * metrics.
 *
 * <p>The following metrics are available:
 *
 * <ul>
 *   <li>{@code fluss.tieringService.readBytes} - Counter: cumulative bytes read from Fluss records
 *       since the job started (actual Fluss records size).
 *   <li>{@code fluss.tieringService.readBytesPerSecond} - Meter: bytes-per-second rate derived from
 *       the counter using a 60-second sliding window.
 *   <li>{@code fluss.tieringService.dvRoundMaskMismatches} - Counter: rounds of tables with
 *       deletion vectors whose leader's log mask did not match the round; they were written in sort
 *       mode, read again if the reader had begun to write them through the mask.
 *   <li>{@code fluss.tieringService.vectorsNotPrepared} - Counter: rounds of buckets of tables with
 *       deletion vectors whose deletion vectors the reader failed to prepare; the committer merged
 *       their deletes instead.
 *   <li>{@code fluss.tieringService.readersAwaitingSplitRatio} - Gauge: fraction of source readers
 *       waiting for splits, reported by the enumerator.
 *   <li>{@code fluss.tieringService.maxConsecutiveFailedRounds} - Gauge: the most rounds in a row a
 *       table tiered by the job failed, reported by the enumerator.
 * </ul>
 */
@Internal
public class TieringMetrics {

    // Metric group names
    public static final String FLUSS_METRIC_GROUP = "fluss";
    public static final String TIERING_SERVICE_GROUP = "tieringService";

    private final Counter readBytesCounter;
    private final Counter dvRoundMaskMismatches;
    private final Counter vectorsNotPrepared;

    public TieringMetrics(SourceReaderMetricGroup sourceReaderMetricGroup) {
        MetricGroup tieringServiceGroup =
                sourceReaderMetricGroup
                        .addGroup(FLUSS_METRIC_GROUP)
                        .addGroup(TIERING_SERVICE_GROUP);

        this.readBytesCounter = tieringServiceGroup.counter(MetricNames.TIERING_SERVICE_READ_BYTES);
        tieringServiceGroup.meter(
                MetricNames.TIERING_SERVICE_READ_BYTES_RATE, new MeterView(readBytesCounter));
        this.dvRoundMaskMismatches =
                tieringServiceGroup.counter(MetricNames.TIERING_SERVICE_DV_ROUND_MASK_MISMATCHES);
        this.vectorsNotPrepared =
                tieringServiceGroup.counter(MetricNames.TIERING_SERVICE_VECTORS_NOT_PREPARED);
    }

    /** Records bytes read from Fluss records. Called per batch or record processing. */
    public void recordBytesRead(long bytes) {
        readBytesCounter.inc(bytes);
    }

    /** Counts a planned round whose leader's log mask did not match it, written in sort mode. */
    public void dvRoundMaskMismatch() {
        dvRoundMaskMismatches.inc();
    }

    /** Counts a round of a bucket whose deletion vectors could not be prepared. */
    public void recordVectorsNotPrepared() {
        vectorsNotPrepared.inc();
    }

    /**
     * Registers the enumerator's metrics, the waiting reader ratio and the most consecutive failed
     * rounds of a table, and returns a handle to close their metric subgroup.
     */
    public static AutoCloseable registerEnumeratorMetrics(
            SplitEnumeratorMetricGroup metricGroup,
            Gauge<Double> readersAwaitingSplitRatio,
            Gauge<Integer> maxConsecutiveFailedRounds) {
        MetricGroup tieringServiceGroup =
                metricGroup.addGroup(FLUSS_METRIC_GROUP).addGroup(TIERING_SERVICE_GROUP);
        tieringServiceGroup.gauge(
                MetricNames.TIERING_SERVICE_READERS_AWAITING_SPLIT_RATIO,
                readersAwaitingSplitRatio);
        tieringServiceGroup.gauge(
                MetricNames.TIERING_SERVICE_MAX_CONSECUTIVE_FAILED_ROUNDS,
                maxConsecutiveFailedRounds);
        return () -> {
            if (tieringServiceGroup instanceof AbstractMetricGroup) {
                ((AbstractMetricGroup<?>) tieringServiceGroup).close();
            }
        };
    }
}
