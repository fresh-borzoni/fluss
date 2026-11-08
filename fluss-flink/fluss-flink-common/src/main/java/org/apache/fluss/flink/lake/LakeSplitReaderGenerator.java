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

package org.apache.fluss.flink.lake;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.DroppedPartitionDetector;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.batch.LakeSnapshotAndLogSplitScanner;
import org.apache.fluss.client.table.scanner.batch.dv.DvTableReadContext;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.flink.lake.reader.DvBucketSplitReader;
import org.apache.fluss.flink.lake.reader.DvBucketSplitScanner;
import org.apache.fluss.flink.lake.reader.LakeSnapshotScanner;
import org.apache.fluss.flink.lake.reader.SeekableLakeSnapshotSplitScanner;
import org.apache.fluss.flink.lake.split.DvBucketSplit;
import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
import org.apache.fluss.flink.source.reader.BoundedSplitReader;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;

import org.apache.flink.metrics.MetricGroup;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Queue;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/** A generator to generate reader for lake split. */
public class LakeSplitReaderGenerator {

    private final Table table;

    private final @Nullable int[] projectedFields;
    private final @Nullable LakeSource<LakeSplit> lakeSource;
    private final Supplier<Admin> admin;
    private final DroppedPartitionDetector droppedPartitions;

    // shared by the splits of tables with deletion vectors this reader reads
    @Nullable private DvTableReadContext dvContext;
    // the source reader's metrics, and the counters of its splits of tables with deletion vectors,
    // registered with the first such split
    @Nullable private final MetricGroup sourceReaderMetrics;
    @Nullable private DvBucketSplitScanner.ReadMetrics dvReadMetrics;
    // whether a split waits for its bucket's leader to switch to its planned snapshot
    private boolean dvWaitForCatchUp =
            ConfigOptions.CLIENT_SCANNER_DELETION_VECTORS_WAIT_FOR_CATCH_UP.defaultValue();

    public LakeSplitReaderGenerator(
            Table table,
            @Nullable int[] projectedFields,
            @Nullable LakeSource<LakeSplit> lakeSource,
            Supplier<Admin> admin,
            DroppedPartitionDetector droppedPartitions) {
        this(table, projectedFields, lakeSource, admin, droppedPartitions, null);
    }

    public LakeSplitReaderGenerator(
            Table table,
            @Nullable int[] projectedFields,
            @Nullable LakeSource<LakeSplit> lakeSource,
            Supplier<Admin> admin,
            DroppedPartitionDetector droppedPartitions,
            @Nullable MetricGroup sourceReaderMetrics) {
        this.table = table;
        this.projectedFields = projectedFields;
        this.lakeSource = lakeSource;
        this.admin = admin;
        this.droppedPartitions = droppedPartitions;
        this.sourceReaderMetrics = sourceReaderMetrics;
    }

    /**
     * Sets whether a split of a table with deletion vectors waits for its bucket's leader to switch
     * to its planned snapshot and have masks to serve, or takes the leader's answer at once.
     */
    public void setDvWaitForCatchUp(boolean waitForCatchUp) {
        this.dvWaitForCatchUp = waitForCatchUp;
    }

    public void addSplit(SourceSplitBase split, Queue<SourceSplitBase> boundedSplits) {
        if (split instanceof LakeSnapshotSplit || split instanceof DvBucketSplit) {
            boundedSplits.add(split);
        } else if (split instanceof LakeSnapshotAndFlussLogSplit) {
            LakeSnapshotAndFlussLogSplit lakeSnapshotAndFlussLogSplit =
                    (LakeSnapshotAndFlussLogSplit) split;
            boolean isStreaming = ((LakeSnapshotAndFlussLogSplit) split).isStreaming();
            // if is streaming and lake split not finished, add to it
            if (isStreaming) {
                if (!lakeSnapshotAndFlussLogSplit.isLakeSplitFinished()) {
                    boundedSplits.add(split);
                }
            } else {
                // otherwise, in batch mode, always add it
                boundedSplits.add(split);
            }
        } else {
            throw new UnsupportedOperationException(
                    String.format("The split type of %s is not supported.", split.getClass()));
        }
    }

    public BoundedSplitReader getBoundedSplitScanner(SourceSplitBase split) {
        if (split instanceof LakeSnapshotSplit) {
            LakeSnapshotSplit lakeSnapshotSplit = (LakeSnapshotSplit) split;
            // Splits of a table with deletion vectors set their projection: they share the lake
            // source with key reads, which project it differently.
            LakeSnapshotScanner lakeSnapshotScanner =
                    new LakeSnapshotScanner(
                            lakeSource,
                            lakeSnapshotSplit,
                            table.getTableInfo().getTableConfig().isDeletionVectorsEnabled()
                                    ? lakeSnapshotProjection()
                                    : null);
            return new BoundedSplitReader(
                    lakeSnapshotScanner, lakeSnapshotSplit.getRecordsToSkip());
        } else if (split instanceof LakeSnapshotAndFlussLogSplit) {
            LakeSnapshotAndFlussLogSplit lakeSplit = (LakeSnapshotAndFlussLogSplit) split;
            return new BoundedSplitReader(getBatchScanner(lakeSplit), lakeSplit.getRecordsToSkip());
        } else if (split instanceof DvBucketSplit) {
            DvBucketSplit dvSplit = (DvBucketSplit) split;
            if (dvContext == null) {
                dvContext =
                        new DvTableReadContext(
                                        table,
                                        admin,
                                        Objects.requireNonNull(lakeSource),
                                        projectedFields,
                                        droppedPartitions)
                                .withWaitForCatchUp(dvWaitForCatchUp);
                if (sourceReaderMetrics != null) {
                    dvReadMetrics = new DvBucketSplitScanner.ReadMetrics(sourceReaderMetrics);
                }
            }
            return new DvBucketSplitReader(
                    new DvBucketSplitScanner(dvSplit, dvContext, dvReadMetrics), dvSplit);
        } else {
            throw new UnsupportedOperationException(
                    String.format("The split type of %s is not supported.", split.getClass()));
        }
    }

    /**
     * The projection of lake snapshot reads, one column per entry: the pushed-down projection, or
     * all user columns without one.
     */
    private int[][] lakeSnapshotProjection() {
        int[] fields =
                (projectedFields != null && projectedFields.length > 0)
                        ? projectedFields
                        : IntStream.range(0, table.getTableInfo().getRowType().getFieldCount())
                                .toArray();
        return Arrays.stream(fields).mapToObj(f -> new int[] {f}).toArray(int[][]::new);
    }

    private BatchScanner getBatchScanner(LakeSnapshotAndFlussLogSplit lakeSplit) {
        BatchScanner lakeBatchScanner;
        if (lakeSplit.isStreaming()) {
            lakeBatchScanner =
                    new SeekableLakeSnapshotSplitScanner(
                            lakeSource,
                            lakeSplit.getLakeSplits() == null
                                    ? Collections.emptyList()
                                    : lakeSplit.getLakeSplits(),
                            lakeSplit.getCurrentLakeSplitIndex());

        } else {
            long stoppingOffset =
                    lakeSplit
                            .getStoppingOffset()
                            .orElseThrow(
                                    () ->
                                            new RuntimeException(
                                                    "StoppingOffset is null for split: "
                                                            + lakeSplit));
            lakeBatchScanner =
                    new LakeSnapshotAndLogSplitScanner(
                            table,
                            lakeSource,
                            lakeSplit.getLakeSplits(),
                            lakeSplit.getTableBucket(),
                            lakeSplit.getStartingOffset(),
                            stoppingOffset,
                            projectedFields);
        }
        return lakeBatchScanner;
    }
}
