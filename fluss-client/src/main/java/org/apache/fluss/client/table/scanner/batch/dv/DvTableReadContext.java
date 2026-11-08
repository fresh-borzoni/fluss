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
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.DvAdmin;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.DroppedPartitionDetector;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.RowWithPosResult;
import org.apache.fluss.lake.source.SupportsRewritePlanning;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.apache.fluss.metadata.ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * The {@link DvBucketPartReader.Context} of a table read through its lake source. One context
 * serves the parts one reader thread reads and plans a lake snapshot once for all of them.
 */
@Internal
public class DvTableReadContext implements DvBucketPartReader.Context {

    private final Table table;
    private final Supplier<Admin> admin;
    private final LakeSource<LakeSplit> lakeSource;
    // the lake source as one that names the commits of snapshot ids, checked when the context is
    // created: a lake without it cannot read deletion vectors
    private final SupportsRewritePlanning<LakeSplit> rewritePlanning;
    @Nullable private final int[] projectedFields;
    private final DroppedPartitionDetector droppedPartitions;
    private final TablePath tablePath;

    // the lake splits of the last snapshot planned, by partition and bucket
    private long plannedSnapshotId = Long.MIN_VALUE;
    @Nullable private Map<String, Map<Integer, List<LakeSplit>>> plannedLakeSplits;
    private boolean waitForCatchUp =
            ConfigOptions.CLIENT_SCANNER_DELETION_VECTORS_WAIT_FOR_CATCH_UP.defaultValue();

    /**
     * @param projectedFields the fields the read emits, all if null
     * @param droppedPartitions tells the reader a partition was dropped while it reads the log
     */
    public DvTableReadContext(
            Table table,
            Supplier<Admin> admin,
            LakeSource<LakeSplit> lakeSource,
            @Nullable int[] projectedFields,
            DroppedPartitionDetector droppedPartitions) {
        this.table = table;
        this.admin = admin;
        this.lakeSource = checkNotNull(lakeSource, "lakeSource");
        this.projectedFields = projectedFields;
        this.droppedPartitions = droppedPartitions;
        this.tablePath = table.getTableInfo().getTablePath();
        this.rewritePlanning = DvLakeCalls.rewritePlanning(lakeSource, tablePath);
    }

    /**
     * Sets whether a part waits for its bucket's leader to switch to the planned snapshot and to
     * have masks to serve ({@link
     * ConfigOptions#CLIENT_SCANNER_DELETION_VECTORS_WAIT_FOR_CATCH_UP}).
     */
    public DvTableReadContext withWaitForCatchUp(boolean waitForCatchUp) {
        this.waitForCatchUp = waitForCatchUp;
        return this;
    }

    @Override
    public boolean waitForCatchUp() {
        return waitForCatchUp;
    }

    @Override
    public TablePath tablePath() {
        return tablePath;
    }

    @Override
    public DvMaskFetcher maskFetcher() {
        return new DvMaskFetcher(DvAdmin.of(admin.get()), tablePath, Thread::sleep);
    }

    @Override
    public LakeSource<LakeSplit> lakeSource() {
        // A key read may have projected the shared source differently, and rows read with
        // positions need an explicit projection.
        lakeSource.withProject(lakeProjection());
        return lakeSource;
    }

    /**
     * The projection of lake reads as {@code int[][]}, one column per entry. Without projected
     * fields it is all columns, so a read with positions never emits an empty row.
     */
    private int[][] lakeProjection() {
        int[] fields =
                (projectedFields != null && projectedFields.length > 0)
                        ? projectedFields
                        : IntStream.range(0, table.getTableInfo().getRowType().getFieldCount())
                                .toArray();
        return Arrays.stream(fields).mapToObj(f -> new int[] {f}).toArray(int[][]::new);
    }

    /** The lake splits of a bucket in a snapshot; plans the whole snapshot once. */
    @Nullable
    @Override
    public List<LakeSplit> planLakeSplits(
            long snapshotId, TableBucket bucket, @Nullable String partitionName)
            throws IOException {
        if (plannedLakeSplits == null || plannedSnapshotId != snapshotId) {
            Map<String, Map<Integer, List<LakeSplit>>> grouped = new HashMap<>();
            for (LakeSplit split :
                    lakeSource.createPlanner((LakeSource.PlannerContext) () -> snapshotId).plan()) {
                grouped.computeIfAbsent(
                                String.join(PARTITION_SPEC_SEPARATOR, split.partition()),
                                k -> new HashMap<>())
                        .computeIfAbsent(split.bucket(), k -> new ArrayList<>())
                        .add(split);
            }
            plannedLakeSplits = grouped;
            plannedSnapshotId = snapshotId;
        }
        Map<Integer, List<LakeSplit>> buckets =
                plannedLakeSplits.get(partitionName == null ? "" : partitionName);
        return buckets == null ? null : buckets.get(bucket.getBucket());
    }

    @Override
    public LakeSnapshot readableLakeSnapshot() throws Exception {
        return admin.get().getReadableLakeSnapshot(tablePath).get();
    }

    @Nullable
    @Override
    public String snapshotIdentity(long snapshotId) throws IOException {
        // not cached: a rollback may reuse the id any time
        return DvLakeCalls.snapshotIdentity(rewritePlanning, snapshotId);
    }

    @Override
    public LogScanner createLogScanner() {
        return table.newScan().project(projectedFields).createLogScanner();
    }

    @Override
    public DvBucketPartReader.KeyReader keyReader() {
        return new PrimaryKeyReader();
    }

    @Override
    public boolean isPartitionDropped(long partitionId) {
        return droppedPartitions.isDropped(partitionId);
    }

    @Override
    public void sleep(long millis) throws InterruptedException {
        Thread.sleep(millis);
    }

    /**
     * Reads primary keys projected to the physical key columns and encodes them as the tablet
     * servers index lake rows.
     */
    private class PrimaryKeyReader implements DvBucketPartReader.KeyReader {

        private final int[] keyIndexes;
        private final KeyEncoder keyEncoder;

        PrimaryKeyReader() {
            TableInfo tableInfo = table.getTableInfo();
            RowType rowType = tableInfo.getRowType();
            List<String> keyNames = tableInfo.getPhysicalPrimaryKeys();
            keyIndexes = keyNames.stream().mapToInt(rowType::getFieldIndex).toArray();
            keyEncoder =
                    KeyEncoder.ofPrimaryKeyEncoder(
                            rowType.project(keyIndexes),
                            keyNames,
                            tableInfo.getTableConfig(),
                            tableInfo.isDefaultBucketKey());
        }

        @Override
        public CloseableIterator<RowWithPosResult> readLakeKeys(LakeSplit lakeSplit)
                throws IOException {
            // reads of other parts set their own projection before reading
            lakeSource.withProject(
                    Arrays.stream(keyIndexes)
                            .mapToObj(index -> new int[] {index})
                            .toArray(int[][]::new));
            return DvLakeCalls.readWithPos(lakeSource, lakeSplit);
        }

        @Override
        public LogScanner createLogScanner() {
            return table.newScan().project(keyIndexes).createLogScanner();
        }

        @Override
        public byte[] encode(InternalRow keyRow) {
            byte[] key = keyEncoder.encodeKey(keyRow);
            return Arrays.copyOf(key, key.length);
        }
    }
}
