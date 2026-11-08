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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** The metrics of derived DV rounds, in the operator's group {@code derivedDeletionVectors}. */
final class DerivedDvMetrics {

    final Counter rounds;
    final Counter changedKeys;
    final Counter reindexes;
    final Counter reindexMs;
    final Counter compactionHandOffs;
    final Counter chainEntriesTrimmed;
    final Counter orphanChangesRemoved;
    final Counter checkpointBuildsFailed;
    final Counter indexTrimsFailed;
    // per table, as of its last trim: the chain's length, and the buckets holding it back
    private final Map<Long, Integer> chainLengths = new ConcurrentHashMap<>();
    private final Map<Long, Integer> holdingBuckets = new ConcurrentHashMap<>();
    volatile long lastLookupMs;
    volatile long lastLakeCommitMs;
    volatile long lastReindexMs;

    DerivedDvMetrics(MetricGroup metricGroup) {
        MetricGroup dv = metricGroup.addGroup("derivedDeletionVectors");
        this.rounds = dv.counter("rounds", new ConcurrentCounter());
        this.changedKeys = dv.counter("changedKeys", new ConcurrentCounter());
        this.reindexes = dv.counter("reindexes", new ConcurrentCounter());
        this.reindexMs = dv.counter("reindexMs", new ConcurrentCounter());
        this.compactionHandOffs = dv.counter("compactionHandOffs", new ConcurrentCounter());
        this.chainEntriesTrimmed = dv.counter("chainEntriesTrimmed", new ConcurrentCounter());
        this.orphanChangesRemoved = dv.counter("orphanChangesRemoved", new ConcurrentCounter());
        this.checkpointBuildsFailed = dv.counter("checkpointBuildsFailed", new ConcurrentCounter());
        this.indexTrimsFailed = dv.counter("indexTrimsFailed", new ConcurrentCounter());
        dv.gauge(
                "indexChainLength",
                (Gauge<Integer>) () -> chainLengths.values().stream().reduce(0, Math::max));
        dv.gauge(
                "bucketsHoldingIndexChain",
                (Gauge<Integer>) () -> holdingBuckets.values().stream().reduce(0, Integer::sum));
        dv.gauge("lastLookupMs", (Gauge<Long>) () -> lastLookupMs);
        dv.gauge("lastLakeCommitMs", (Gauge<Long>) () -> lastLakeCommitMs);
        dv.gauge("lastReindexMs", (Gauge<Long>) () -> lastReindexMs);
    }

    /** Records what a trim of a table left: its chain's length and the buckets holding it. */
    void trimmed(long tableId, int chainLength, int holding) {
        chainLengths.put(tableId, chainLength);
        holdingBuckets.put(tableId, holding);
    }
}
