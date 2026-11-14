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

package org.apache.fluss.server.metrics.group;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.CharacterFilter;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.DescriptiveStatisticsHistogram;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.MeterView;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.SimpleCounter;
import org.apache.fluss.metrics.ThreadSafeSimpleCounter;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.server.kv.rocksdb.RocksDBStatistics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** The metric group for tablet server. */
public class TabletServerMetricGroup extends AbstractMetricGroup {

    private static final String NAME = "tabletserver";
    private static final int WINDOW_SIZE = 1024;

    private final Map<TablePath, TableMetricGroup> metricGroupByTable = new ConcurrentHashMap<>();

    protected final String clusterId;
    protected final String rack;
    protected final String hostname;
    protected final int serverId;

    // ---- metrics ----
    private final Counter replicationBytesIn;
    private final Counter replicationBytesOut;
    private final Counter delayedWriteExpireCount;
    private final Counter delayedFetchFromFollowerExpireCount;
    private final Counter delayedFetchFromClientExpireCount;

    // aggregated metrics
    private final Counter messagesIn;
    private final Counter bytesIn;
    private final Counter bytesOut;

    // aggregated log metrics
    private final Counter logFlushCount;
    private final Histogram logFlushLatencyHistogram;

    // aggregated kv metrics
    private final Counter kvFlushCount;
    private final Histogram kvFlushLatencyHistogram;

    // derived deletion vectors
    private final Counter dvSwitchCount;
    private final Histogram dvSwitchLatencyHistogram;
    private final Counter dvRebuildCount;
    private final Histogram dvRebuildLatencyHistogram;
    private final Counter dvCheckpointCount;
    private final Histogram dvCheckpointLatencyHistogram;
    private final Counter dvChangeListCheckpointCount;
    private final Counter dvChangeListCheckpointSkipCount;
    private final Histogram dvChangeListCheckpointLatencyHistogram;
    private final Counter dvChangeListCheckpointUploadBytes;
    private final Counter dvRebuildFromCheckpointCount;
    private final Counter dvStaleSnapshotCount;
    private final Counter dvOwnerFaultCount;
    private final Counter dvHandOffStallCount;
    private final Counter dvHandOffOverflowCount;
    // the longest step of a bucket's owner, and the longest wait of a step in its mailbox, since
    // each gauge was last read, in nanoseconds
    private final AtomicLong dvOwnerStepMaxNanos = new AtomicLong();
    private final AtomicLong dvOwnerMailboxDelayMaxNanos = new AtomicLong();
    private final AtomicInteger dvRebuildsInProgress = new AtomicInteger();
    private final AtomicLong dvLastRebuildMs = new AtomicLong();
    private final AtomicLong dvReplayBacklog = new AtomicLong();
    private final AtomicLong dvHeapBytes = new AtomicLong();
    private final AtomicLong dvChangeListBytes = new AtomicLong();
    private final AtomicInteger dvPinnedCuts = new AtomicInteger();
    private final AtomicLong dvPinnedCutBytes = new AtomicLong();
    private final Counter dvPinnedCutEvictionCount;
    private final Counter dvPinnedCutExpirationCount;
    // the steps of union reads' mask requests
    private final Histogram dvMaskRequestHistogram;
    private final Histogram dvMaskWaitSwitchHistogram;
    private final Histogram dvMaskWaitDeriveHistogram;
    private final Histogram dvMaskWaitReplayHistogram;
    private final Histogram dvMaskApplyHistogram;
    private final Histogram dvMaskResolveWaitHistogram;
    private final Histogram dvMaskBuildHistogram;
    private final Counter dvMaskFrontierCount;
    private final Counter kvTruncateAsDuplicatedCount;
    private final Counter kvTruncateAsErrorCount;

    // aggregated replica metrics
    private final Counter isrShrinks;
    private final Counter isrExpands;
    private final Counter failedIsrUpdates;

    /** Suppliers for shared block cache usage, set by KvManager when shared cache is enabled. */
    private volatile LongSupplier sharedBlockCacheUsageSupplier = () -> 0L;

    private volatile LongSupplier sharedBlockCachePinnedUsageSupplier = () -> 0L;

    private volatile long sharedBlockCacheCapacity;

    /** Supplier for shared write buffer usage, set by KvManager. */
    private volatile LongSupplier sharedWriteBufferUsageSupplier = () -> 0L;

    private volatile long sharedWriteBufferCapacity;

    public TabletServerMetricGroup(
            MetricRegistry registry, String clusterId, String rack, String hostname, int serverId) {
        super(registry, new String[] {clusterId, hostname, NAME}, null);
        this.clusterId = clusterId;
        this.rack = rack;
        this.hostname = hostname;
        this.serverId = serverId;

        replicationBytesIn = new ThreadSafeSimpleCounter();
        meter(MetricNames.REPLICATION_IN_RATE, new MeterView(replicationBytesIn));
        replicationBytesOut = new ThreadSafeSimpleCounter();
        meter(MetricNames.REPLICATION_OUT_RATE, new MeterView(replicationBytesOut));

        delayedWriteExpireCount = new ThreadSafeSimpleCounter();
        meter(MetricNames.DELAYED_WRITE_EXPIRES_RATE, new MeterView(delayedWriteExpireCount));
        delayedFetchFromFollowerExpireCount = new ThreadSafeSimpleCounter();
        meter(
                MetricNames.DELAYED_FETCH_FROM_FOLLOWER_EXPIRES_RATE,
                new MeterView(delayedFetchFromFollowerExpireCount));
        delayedFetchFromClientExpireCount = new ThreadSafeSimpleCounter();
        meter(
                MetricNames.DELAYED_FETCH_FROM_CLIENT_EXPIRES_RATE,
                new MeterView(delayedFetchFromClientExpireCount));

        messagesIn = new ThreadSafeSimpleCounter();
        meter(MetricNames.MESSAGES_IN_RATE, new MeterView(messagesIn));
        bytesIn = new ThreadSafeSimpleCounter();
        meter(MetricNames.BYTES_IN_RATE, new MeterView(bytesIn));
        bytesOut = new ThreadSafeSimpleCounter();
        meter(MetricNames.BYTES_OUT_RATE, new MeterView(bytesOut));

        // about flush
        logFlushCount = new SimpleCounter();
        meter(MetricNames.LOG_FLUSH_RATE, new MeterView(logFlushCount));
        logFlushLatencyHistogram =
                new DescriptiveStatisticsHistogram(
                        WINDOW_SIZE, DescriptiveStatisticsHistogram.DEFAULT_MAX_AGE);
        histogram(MetricNames.LOG_FLUSH_LATENCY_MS, logFlushLatencyHistogram);

        // about pre-write buffer.
        kvFlushCount = new SimpleCounter();
        meter(MetricNames.KV_FLUSH_RATE, new MeterView(kvFlushCount));
        kvFlushLatencyHistogram =
                new DescriptiveStatisticsHistogram(
                        WINDOW_SIZE, DescriptiveStatisticsHistogram.DEFAULT_MAX_AGE);
        histogram(MetricNames.KV_FLUSH_LATENCY_MS, kvFlushLatencyHistogram);

        // about derived deletion vectors
        dvSwitchCount = new SimpleCounter();
        meter(MetricNames.DV_SWITCH_RATE, new MeterView(dvSwitchCount));
        dvSwitchLatencyHistogram = new DescriptiveStatisticsHistogram(WINDOW_SIZE);
        histogram(MetricNames.DV_SWITCH_LATENCY_MS, dvSwitchLatencyHistogram);
        dvRebuildCount = new SimpleCounter();
        meter(MetricNames.DV_REBUILD_RATE, new MeterView(dvRebuildCount));
        dvRebuildLatencyHistogram = new DescriptiveStatisticsHistogram(WINDOW_SIZE);
        histogram(MetricNames.DV_REBUILD_LATENCY_MS, dvRebuildLatencyHistogram);
        dvCheckpointCount = new SimpleCounter();
        meter(MetricNames.DV_CHECKPOINT_RATE, new MeterView(dvCheckpointCount));
        dvCheckpointLatencyHistogram = new DescriptiveStatisticsHistogram(WINDOW_SIZE);
        histogram(MetricNames.DV_CHECKPOINT_LATENCY_MS, dvCheckpointLatencyHistogram);
        dvChangeListCheckpointCount = new SimpleCounter();
        meter(
                MetricNames.DV_CHANGE_LIST_CHECKPOINT_RATE,
                new MeterView(dvChangeListCheckpointCount));
        dvChangeListCheckpointSkipCount = new SimpleCounter();
        meter(
                MetricNames.DV_CHANGE_LIST_CHECKPOINT_SKIP_RATE,
                new MeterView(dvChangeListCheckpointSkipCount));
        dvChangeListCheckpointLatencyHistogram = new DescriptiveStatisticsHistogram(WINDOW_SIZE);
        histogram(
                MetricNames.DV_CHANGE_LIST_CHECKPOINT_LATENCY_MS,
                dvChangeListCheckpointLatencyHistogram);
        dvChangeListCheckpointUploadBytes = new SimpleCounter();
        meter(
                MetricNames.DV_CHANGE_LIST_CHECKPOINT_UPLOAD_BYTES_RATE,
                new MeterView(dvChangeListCheckpointUploadBytes));
        dvRebuildFromCheckpointCount = new SimpleCounter();
        meter(
                MetricNames.DV_REBUILD_FROM_CHECKPOINT_RATE,
                new MeterView(dvRebuildFromCheckpointCount));
        dvStaleSnapshotCount = new SimpleCounter();
        meter(MetricNames.DV_STALE_SNAPSHOT_RATE, new MeterView(dvStaleSnapshotCount));
        dvOwnerFaultCount = new SimpleCounter();
        meter(MetricNames.DV_OWNER_FAULT_RATE, new MeterView(dvOwnerFaultCount));
        dvHandOffStallCount = new SimpleCounter();
        meter(MetricNames.DV_HAND_OFF_STALL_RATE, new MeterView(dvHandOffStallCount));
        dvHandOffOverflowCount = new SimpleCounter();
        meter(MetricNames.DV_HAND_OFF_OVERFLOW_RATE, new MeterView(dvHandOffOverflowCount));
        gauge(
                MetricNames.DV_OWNER_STEP_MAX_MS,
                () -> dvOwnerStepMaxNanos.getAndSet(0) / 1_000_000L);
        gauge(
                MetricNames.DV_OWNER_MAILBOX_DELAY_MAX_MS,
                () -> dvOwnerMailboxDelayMaxNanos.getAndSet(0) / 1_000_000L);
        gauge(MetricNames.DV_REBUILDS_IN_PROGRESS, dvRebuildsInProgress::get);
        gauge(MetricNames.DV_LAST_REBUILD_MS, dvLastRebuildMs::get);
        gauge(MetricNames.DV_REPLAY_BACKLOG, dvReplayBacklog::get);
        gauge(MetricNames.DV_HEAP_BYTES, dvHeapBytes::get);
        gauge(MetricNames.DV_CHANGE_LIST_BYTES, dvChangeListBytes::get);
        gauge(MetricNames.DV_PINNED_CUTS, dvPinnedCuts::get);
        gauge(MetricNames.DV_PINNED_CUT_BYTES, dvPinnedCutBytes::get);
        dvPinnedCutEvictionCount = new SimpleCounter();
        meter(MetricNames.DV_PINNED_CUT_EVICTION_RATE, new MeterView(dvPinnedCutEvictionCount));
        dvPinnedCutExpirationCount = new SimpleCounter();
        meter(MetricNames.DV_PINNED_CUT_EXPIRATION_RATE, new MeterView(dvPinnedCutExpirationCount));
        dvMaskRequestHistogram = new DescriptiveStatisticsHistogram(WINDOW_SIZE);
        histogram(MetricNames.DV_MASK_REQUEST_MS, dvMaskRequestHistogram);
        dvMaskWaitSwitchHistogram = new DescriptiveStatisticsHistogram(WINDOW_SIZE);
        histogram(MetricNames.DV_MASK_WAIT_SWITCH_MS, dvMaskWaitSwitchHistogram);
        dvMaskWaitDeriveHistogram = new DescriptiveStatisticsHistogram(WINDOW_SIZE);
        histogram(MetricNames.DV_MASK_WAIT_DERIVE_MS, dvMaskWaitDeriveHistogram);
        dvMaskWaitReplayHistogram = new DescriptiveStatisticsHistogram(WINDOW_SIZE);
        histogram(MetricNames.DV_MASK_WAIT_REPLAY_MS, dvMaskWaitReplayHistogram);
        dvMaskApplyHistogram = new DescriptiveStatisticsHistogram(WINDOW_SIZE);
        histogram(MetricNames.DV_MASK_APPLY_MS, dvMaskApplyHistogram);
        dvMaskResolveWaitHistogram = new DescriptiveStatisticsHistogram(WINDOW_SIZE);
        histogram(MetricNames.DV_MASK_RESOLVE_WAIT_MS, dvMaskResolveWaitHistogram);
        dvMaskBuildHistogram = new DescriptiveStatisticsHistogram(WINDOW_SIZE);
        histogram(MetricNames.DV_MASK_BUILD_MS, dvMaskBuildHistogram);
        dvMaskFrontierCount = new SimpleCounter();
        meter(MetricNames.DV_MASK_FRONTIER_RATE, new MeterView(dvMaskFrontierCount));
        kvTruncateAsDuplicatedCount = new SimpleCounter();
        meter(
                MetricNames.KV_PRE_WRITE_BUFFER_TRUNCATE_AS_DUPLICATED_RATE,
                new MeterView(kvTruncateAsDuplicatedCount));
        kvTruncateAsErrorCount = new SimpleCounter();
        meter(
                MetricNames.KV_PRE_WRITE_BUFFER_TRUNCATE_AS_ERROR_RATE,
                new MeterView(kvTruncateAsErrorCount));

        // replica metrics
        isrExpands = new SimpleCounter();
        meter(MetricNames.ISR_EXPANDS_RATE, new MeterView(isrExpands));
        isrShrinks = new SimpleCounter();
        meter(MetricNames.ISR_SHRINKS_RATE, new MeterView(isrShrinks));
        failedIsrUpdates = new SimpleCounter();
        meter(MetricNames.FAILED_ISR_UPDATES_RATE, new MeterView(failedIsrUpdates));

        // Register server-level RocksDB aggregated metrics
        registerServerRocksDBMetrics();
    }

    /**
     * Register server-level RocksDB aggregated metrics. These metrics aggregate memory usage from
     * all tables.
     */
    private void registerServerRocksDBMetrics() {
        // Total memory usage across all RocksDB instances in this server.
        // When shared block cache is enabled, per-tablet stats exclude block cache,
        // so we add the shared block cache usage once separately.
        gauge(
                MetricNames.ROCKSDB_MEMORY_USAGE_TOTAL,
                () ->
                        metricGroupByTable.values().stream()
                                        .flatMap(TableMetricGroup::allRocksDBStatistics)
                                        .mapToLong(RocksDBStatistics::getTotalMemoryUsage)
                                        .sum()
                                + sharedBlockCacheUsageSupplier.getAsLong());
        gauge(
                MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_USAGE,
                () -> sharedBlockCacheUsageSupplier.getAsLong());
        gauge(MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_CAPACITY, () -> sharedBlockCacheCapacity);
        gauge(
                MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_PINNED_USAGE,
                () -> sharedBlockCachePinnedUsageSupplier.getAsLong());
        gauge(
                MetricNames.ROCKSDB_SHARED_WRITE_BUFFER_USAGE,
                () -> sharedWriteBufferUsageSupplier.getAsLong());
        gauge(MetricNames.ROCKSDB_SHARED_WRITE_BUFFER_CAPACITY, () -> sharedWriteBufferCapacity);
    }

    /**
     * Sets the shared block cache metrics. Called by KvManager when shared block cache is enabled.
     *
     * @param usageSupplier supplier for current cache usage
     * @param pinnedUsageSupplier supplier for current pinned cache usage
     * @param capacity configured cache capacity in bytes
     */
    public void setSharedBlockCacheMetrics(
            LongSupplier usageSupplier, LongSupplier pinnedUsageSupplier, long capacity) {
        LongSupplier checkedUsageSupplier =
                checkNotNull(usageSupplier, "usageSupplier must not be null");
        LongSupplier checkedPinnedUsageSupplier =
                checkNotNull(pinnedUsageSupplier, "pinnedUsageSupplier must not be null");
        this.sharedBlockCacheUsageSupplier = checkedUsageSupplier;
        this.sharedBlockCachePinnedUsageSupplier = checkedPinnedUsageSupplier;
        this.sharedBlockCacheCapacity = capacity;
    }

    /**
     * Sets the shared write buffer metrics.
     *
     * <p>The usage is RocksDB's approximate logical charge for memtable allocations. It is not a
     * process RSS measurement or a hard memory bound.
     *
     * @param usageSupplier supplier for current shared write buffer usage
     * @param capacity configured soft capacity in bytes, or 0 when disabled
     */
    public void setSharedWriteBufferMetrics(LongSupplier usageSupplier, long capacity) {
        this.sharedWriteBufferUsageSupplier =
                checkNotNull(usageSupplier, "usageSupplier must not be null");
        this.sharedWriteBufferCapacity = capacity;
    }

    /**
     * Registers gauges for the server-wide WAL memory pool used by primary key tables. Called once
     * by KvManager when creating the server buffer pool.
     *
     * @param usageSupplier supplier for the bytes currently allocated from the pool
     * @param capacity the total pool capacity in bytes
     */
    public void registerKvWalMemoryPoolMetrics(LongSupplier usageSupplier, long capacity) {
        gauge(MetricNames.KV_WAL_MEMORY_POOL_USAGE, usageSupplier::getAsLong);
        gauge(MetricNames.KV_WAL_MEMORY_POOL_CAPACITY, () -> capacity);
    }

    @Override
    protected final void putVariables(Map<String, String> variables) {
        variables.put("cluster_id", clusterId);
        if (rack != null) {
            variables.put("rack", rack);
        } else {
            // The value of an empty string indicates no rack
            variables.put("rack", "");
        }
        variables.put("host", hostname);
        variables.put("server_id", String.valueOf(serverId));
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return NAME;
    }

    public Counter replicationBytesIn() {
        return replicationBytesIn;
    }

    public Counter replicationBytesOut() {
        return replicationBytesOut;
    }

    public Counter delayedWriteExpireCount() {
        return delayedWriteExpireCount;
    }

    public Counter delayedFetchFromFollowerExpireCount() {
        return delayedFetchFromFollowerExpireCount;
    }

    public Counter delayedFetchFromClientExpireCount() {
        return delayedFetchFromClientExpireCount;
    }

    public Counter messageIn() {
        return messagesIn;
    }

    public Counter bytesIn() {
        return bytesIn;
    }

    public Counter bytesOut() {
        return bytesOut;
    }

    public Counter logFlushCount() {
        return logFlushCount;
    }

    public Histogram logFlushLatencyHistogram() {
        return logFlushLatencyHistogram;
    }

    public Counter kvFlushCount() {
        return kvFlushCount;
    }

    public Histogram kvFlushLatencyHistogram() {
        return kvFlushLatencyHistogram;
    }

    public Counter dvSwitchCount() {
        return dvSwitchCount;
    }

    public Histogram dvSwitchLatencyHistogram() {
        return dvSwitchLatencyHistogram;
    }

    public Counter dvRebuildCount() {
        return dvRebuildCount;
    }

    public Histogram dvRebuildLatencyHistogram() {
        return dvRebuildLatencyHistogram;
    }

    public Counter dvCheckpointCount() {
        return dvCheckpointCount;
    }

    public Histogram dvCheckpointLatencyHistogram() {
        return dvCheckpointLatencyHistogram;
    }

    public Counter dvChangeListCheckpointCount() {
        return dvChangeListCheckpointCount;
    }

    public Counter dvChangeListCheckpointSkipCount() {
        return dvChangeListCheckpointSkipCount;
    }

    public Histogram dvChangeListCheckpointLatencyHistogram() {
        return dvChangeListCheckpointLatencyHistogram;
    }

    public Counter dvChangeListCheckpointUploadBytes() {
        return dvChangeListCheckpointUploadBytes;
    }

    public Counter dvRebuildFromCheckpointCount() {
        return dvRebuildFromCheckpointCount;
    }

    public Counter dvStaleSnapshotCount() {
        return dvStaleSnapshotCount;
    }

    public Counter dvOwnerFaultCount() {
        return dvOwnerFaultCount;
    }

    public Counter dvHandOffStallCount() {
        return dvHandOffStallCount;
    }

    public Counter dvHandOffOverflowCount() {
        return dvHandOffOverflowCount;
    }

    public AtomicLong dvOwnerStepMaxNanos() {
        return dvOwnerStepMaxNanos;
    }

    public AtomicLong dvOwnerMailboxDelayMaxNanos() {
        return dvOwnerMailboxDelayMaxNanos;
    }

    public AtomicInteger dvRebuildsInProgress() {
        return dvRebuildsInProgress;
    }

    public AtomicLong dvLastRebuildMs() {
        return dvLastRebuildMs;
    }

    public AtomicLong dvReplayBacklog() {
        return dvReplayBacklog;
    }

    public AtomicLong dvHeapBytes() {
        return dvHeapBytes;
    }

    public AtomicLong dvChangeListBytes() {
        return dvChangeListBytes;
    }

    public AtomicInteger dvPinnedCuts() {
        return dvPinnedCuts;
    }

    public AtomicLong dvPinnedCutBytes() {
        return dvPinnedCutBytes;
    }

    public Counter dvPinnedCutEvictionCount() {
        return dvPinnedCutEvictionCount;
    }

    public Counter dvPinnedCutExpirationCount() {
        return dvPinnedCutExpirationCount;
    }

    public Histogram dvMaskRequestHistogram() {
        return dvMaskRequestHistogram;
    }

    public Histogram dvMaskWaitSwitchHistogram() {
        return dvMaskWaitSwitchHistogram;
    }

    public Histogram dvMaskWaitDeriveHistogram() {
        return dvMaskWaitDeriveHistogram;
    }

    public Histogram dvMaskWaitReplayHistogram() {
        return dvMaskWaitReplayHistogram;
    }

    public Histogram dvMaskApplyHistogram() {
        return dvMaskApplyHistogram;
    }

    public Histogram dvMaskResolveWaitHistogram() {
        return dvMaskResolveWaitHistogram;
    }

    public Histogram dvMaskBuildHistogram() {
        return dvMaskBuildHistogram;
    }

    public Counter dvMaskFrontierCount() {
        return dvMaskFrontierCount;
    }

    public Counter kvTruncateAsDuplicatedCount() {
        return kvTruncateAsDuplicatedCount;
    }

    public Counter kvTruncateAsErrorCount() {
        return kvTruncateAsErrorCount;
    }

    public Counter isrShrinks() {
        return isrShrinks;
    }

    public Counter isrExpands() {
        return isrExpands;
    }

    public Counter failedIsrUpdates() {
        return failedIsrUpdates;
    }

    // ------------------------------------------------------------------------
    //  table buckets groups
    // ------------------------------------------------------------------------
    public BucketMetricGroup addTableBucketMetricGroup(
            PhysicalTablePath physicalTablePath, TableBucket bucket, boolean isKvTable) {
        TablePath tablePath = physicalTablePath.getTablePath();
        TableMetricGroup tableMetricGroup =
                metricGroupByTable.computeIfAbsent(
                        tablePath,
                        table -> new TableMetricGroup(registry, tablePath, isKvTable, this));
        return tableMetricGroup.addBucketMetricGroup(physicalTablePath.getPartitionName(), bucket);
    }

    public void removeTableBucketMetricGroup(TablePath tablePath, TableBucket bucket) {
        // get the metric group of the table
        TableMetricGroup tableMetricGroup = metricGroupByTable.get(tablePath);
        // if get the table metric group
        if (tableMetricGroup != null) {
            // remove the bucket metric group
            tableMetricGroup.removeBucketMetricGroup(bucket);
            // if no any bucket groups remain in the physical table metrics group,
            // close and remove the physical table metric group
            if (tableMetricGroup.bucketGroupsCount() == 0) {
                tableMetricGroup.close();
                metricGroupByTable.remove(tablePath);
            }
        }
    }
}
