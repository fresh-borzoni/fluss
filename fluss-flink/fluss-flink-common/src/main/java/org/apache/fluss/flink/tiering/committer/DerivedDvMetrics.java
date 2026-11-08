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

/** The metrics of derived DV rounds, in the operator's group {@code derivedDeletionVectors}. */
final class DerivedDvMetrics {

    final Counter rounds;
    final Counter changedKeys;
    final Counter reindexes;
    final Counter reindexMs;
    volatile long lastLookupMs;
    volatile long lastLakeCommitMs;
    volatile long lastReindexMs;

    DerivedDvMetrics(MetricGroup metricGroup) {
        MetricGroup dv = metricGroup.addGroup("derivedDeletionVectors");
        this.rounds = dv.counter("rounds", new ConcurrentCounter());
        this.changedKeys = dv.counter("changedKeys", new ConcurrentCounter());
        this.reindexes = dv.counter("reindexes", new ConcurrentCounter());
        this.reindexMs = dv.counter("reindexMs", new ConcurrentCounter());
        dv.gauge("lastLookupMs", (Gauge<Long>) () -> lastLookupMs);
        dv.gauge("lastLakeCommitMs", (Gauge<Long>) () -> lastLakeCommitMs);
        dv.gauge("lastReindexMs", (Gauge<Long>) () -> lastReindexMs);
    }
}
