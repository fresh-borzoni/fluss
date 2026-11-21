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

package org.apache.fluss.flink.source.reader;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.adapter.SingleThreadMultiplexSourceReaderBaseAdapter;
import org.apache.fluss.flink.lake.LakeSplitStateInitializer;
import org.apache.fluss.flink.source.emitter.FlinkRecordEmitter;
import org.apache.fluss.flink.source.epochs.ReaderEpochs;
import org.apache.fluss.flink.source.event.EpochOpenedEvent;
import org.apache.fluss.flink.source.event.FinishedKvSnapshotConsumeEvent;
import org.apache.fluss.flink.source.event.PartitionBucketsUnsubscribedEvent;
import org.apache.fluss.flink.source.event.PartitionsRemovedEvent;
import org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics;
import org.apache.fluss.flink.source.reader.fetcher.FlinkSourceFetcherManager;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplit;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplitState;
import org.apache.fluss.flink.source.split.KvBatchSplitState;
import org.apache.fluss.flink.source.split.LogSplitState;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.split.SourceSplitState;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.types.RowType;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * The source reader for Fluss.
 *
 * <p>With {@link ReaderEpochs}, it cuts the changes it emits into epochs and emits their
 * watermarks.
 */
public class FlinkSourceReader<OUT>
        extends SingleThreadMultiplexSourceReaderBaseAdapter<
                RecordAndPos, OUT, SourceSplitBase, SourceSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSourceReader.class);

    /** the tableBuckets ignore to send FinishedKvSnapshotConsumeEvent as it already sending. */
    private final Set<TableBucket> finishedKvSnapshotConsumeBuckets;

    /** The epochs that the reader cuts its changes into, or null. */
    @Nullable private final ReaderEpochs<OUT> epochs;

    private final FutureCompletingBlockingQueue<RecordsWithSplitIds<RecordAndPos>> elementsQueue;

    public FlinkSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<RecordAndPos>> elementsQueue,
            Configuration flussConfig,
            TablePath tablePath,
            RowType sourceOutputType,
            SourceReaderContext context,
            @Nullable int[] projectedFields,
            @Nullable Predicate logRecordBatchFilter,
            FlinkSourceReaderMetrics flinkSourceReaderMetrics,
            FlinkRecordEmitter<OUT> recordEmitter,
            LakeSource<LakeSplit> lakeSource) {
        this(
                elementsQueue,
                flussConfig,
                tablePath,
                sourceOutputType,
                context,
                projectedFields,
                logRecordBatchFilter,
                flinkSourceReaderMetrics,
                recordEmitter,
                lakeSource,
                null);
    }

    /**
     * @param epochs the epochs that the reader cuts its changes into, or null
     */
    public FlinkSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<RecordAndPos>> elementsQueue,
            Configuration flussConfig,
            TablePath tablePath,
            RowType sourceOutputType,
            SourceReaderContext context,
            @Nullable int[] projectedFields,
            @Nullable Predicate logRecordBatchFilter,
            FlinkSourceReaderMetrics flinkSourceReaderMetrics,
            FlinkRecordEmitter<OUT> recordEmitter,
            LakeSource<LakeSplit> lakeSource,
            @Nullable ReaderEpochs<OUT> epochs) {
        super(
                elementsQueue,
                new FlinkSourceFetcherManager(
                        elementsQueue,
                        () ->
                                new FlinkSourceSplitReader(
                                        flussConfig,
                                        tablePath,
                                        sourceOutputType,
                                        projectedFields,
                                        logRecordBatchFilter,
                                        lakeSource,
                                        flinkSourceReaderMetrics),
                        (ignore) -> {}),
                epochs == null ? recordEmitter : epochs.wrap(recordEmitter),
                context.getConfiguration(),
                context);
        this.finishedKvSnapshotConsumeBuckets = new HashSet<>();
        this.epochs = epochs;
        this.elementsQueue = elementsQueue;
    }

    @Override
    public InputStatus pollNext(ReaderOutput<OUT> output) throws Exception {
        if (epochs == null) {
            return super.pollNext(output);
        }
        InputStatus status = epochs.beforePoll(output);
        if (status != null) {
            return status;
        }
        return epochs.afterPoll(super.pollNext(output));
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        CompletableFuture<Void> records = super.isAvailable();
        return epochs == null ? records : epochs.availability(records);
    }

    @Override
    public void addSplits(List<SourceSplitBase> splits) {
        if (epochs != null) {
            try {
                splits = epochs.addingSplits(splits);
            } catch (Exception e) {
                throw new FlinkRuntimeException("Failed to prepare the epochs of " + splits, e);
            }
        }
        super.addSplits(splits);
        if (epochs != null) {
            // new splits need a watermark even if they have no records
            elementsQueue.notifyAvailable();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, SourceSplitState> map) {
        if (epochs != null) {
            epochs.splitsFinished(map.keySet());
        }
    }

    @Override
    public List<SourceSplitBase> snapshotState(long checkpointId) {
        Set<TableBucket> bucketsFinishedConsumeKvSnapshot = new HashSet<>();

        // do not modify this state.
        List<SourceSplitBase> sourceSplitBases = super.snapshotState(checkpointId);
        if (epochs != null) {
            // a finished split with records held back reads them again after a restore
            sourceSplitBases.addAll(epochs.finishedSplitsHoldingRecords());
        }
        for (SourceSplitBase sourceSplitBase : sourceSplitBases) {
            TableBucket tableBucket = sourceSplitBase.getTableBucket();
            if (finishedKvSnapshotConsumeBuckets.contains(tableBucket)) {
                continue;
            }

            if (sourceSplitBase.isHybridSnapshotLogSplit()) {
                HybridSnapshotLogSplit hybridSnapshotLogSplit =
                        sourceSplitBase.asHybridSnapshotLogSplit();
                if (hybridSnapshotLogSplit.isSnapshotFinished()) {
                    bucketsFinishedConsumeKvSnapshot.add(tableBucket);
                }
            }
        }

        // report finished kv snapshot consume event.
        if (!bucketsFinishedConsumeKvSnapshot.isEmpty()) {
            LOG.info(
                    "reader has finished kv snapshot read for bucket: {}, checkpoint id {}",
                    bucketsFinishedConsumeKvSnapshot,
                    checkpointId);

            // TODO Reduce the external IO operation, trace by
            // https://github.com/apache/fluss/issues/2597.
            context.sendSourceEventToCoordinator(
                    new FinishedKvSnapshotConsumeEvent(
                            checkpointId, bucketsFinishedConsumeKvSnapshot));
            // It won't be sent anymore in the future for this table bucket, but will be resent
            // after failover recovery as ignoreBuckets is cleared.
            finishedKvSnapshotConsumeBuckets.addAll(bucketsFinishedConsumeKvSnapshot);
        }

        return sourceSplitBases;
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof EpochOpenedEvent) {
            if (epochs != null) {
                epochs.epochOpened(((EpochOpenedEvent) sourceEvent).getEpoch());
            }
            return;
        }
        if (sourceEvent instanceof PartitionsRemovedEvent) {
            PartitionsRemovedEvent partitionsRemovedEvent = (PartitionsRemovedEvent) sourceEvent;
            Consumer<Set<TableBucket>> unsubscribeTableBucketsCallback =
                    (unsubscribedTableBuckets) -> {
                        // send remove partitions ack event to coordinator
                        context.sendSourceEventToCoordinator(
                                new PartitionBucketsUnsubscribedEvent(unsubscribedTableBuckets));
                    };
            ((FlinkSourceFetcherManager) splitFetcherManager)
                    .removePartitions(
                            partitionsRemovedEvent.getRemovedPartitions(),
                            unsubscribeTableBucketsCallback);
        }
    }

    @Override
    protected SourceSplitState initializedState(SourceSplitBase split) {
        SourceSplitState state;
        if (split.isHybridSnapshotLogSplit()) {
            state = new HybridSnapshotLogSplitState(split.asHybridSnapshotLogSplit());
        } else if (split.isLogSplit()) {
            state = new LogSplitState(split.asLogSplit());
        } else if (split.isKvBatchSplit()) {
            state = new KvBatchSplitState(split.asKvBatchSplit());
        } else if (split.isLakeSplit()) {
            state = LakeSplitStateInitializer.initializedState(split);
        } else {
            throw new UnsupportedOperationException("Unsupported split type: " + split);
        }
        if (epochs != null) {
            epochs.track(split, state);
        }
        return state;
    }

    @Override
    public void close() throws Exception {
        try {
            super.close();
        } finally {
            if (epochs != null) {
                epochs.close();
            }
        }
    }

    @Override
    protected SourceSplitBase toSplitType(String splitId, SourceSplitState splitState) {
        return splitState.toSourceSplit();
    }
}
