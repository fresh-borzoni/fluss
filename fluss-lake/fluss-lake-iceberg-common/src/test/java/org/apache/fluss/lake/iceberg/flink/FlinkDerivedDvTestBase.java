/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.flink;

import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.lake.iceberg.source.IcebergSplitPlanner;
import org.apache.fluss.lake.iceberg.tiering.writer.TaskWriterFactory;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.data.IcebergGenericReader;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.junit.jupiter.api.BeforeAll;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The setup and helpers of the integration tests of tables with deletion vectors ({@code
 * table.deletion-vectors.enabled = true}) on Iceberg v2 (file-scoped position deletes) and v3
 * (deletion vectors).
 */
abstract class FlinkDerivedDvTestBase extends FlinkUnionReadTestBase {
    protected static final int BUCKETS = 3;

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
        // Derived commits write the position index under the remote data dir; the tiering job
        // takes its Fluss config from clientConf, which has no remote dir by default.
        clientConf.set(ConfigOptions.REMOTE_DATA_DIR, FLUSS_CLUSTER_EXTENSION.getRemoteDataDir());
    }

    @Override
    protected org.apache.fluss.config.Configuration defaultLakeTieringConfig() {
        org.apache.fluss.config.Configuration conf = super.defaultLakeTieringConfig();
        // One split per rewritten file, so the re-index of a bucket takes several splits.
        conf.set(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_REINDEX_FILES_PER_SPLIT, 1);
        return conf;
    }

    /** Upserts keys [from, to) with values {@code prefix_key}. */
    protected void upsert(
            TablePath tablePath, Map<Integer, String> expected, int from, int to, String prefix)
            throws Exception {
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (int key = from; key < to; key++) {
                writer.upsert(rowOf(key, prefix + "_" + key, null));
                expected.put(key, prefix + "_" + key);
            }
            writer.flush();
        }
    }

    protected long partitionId(TablePath tablePath, String partitionName) throws Exception {
        for (PartitionInfo info : admin.listPartitionInfos(tablePath).get()) {
            if (info.getPartitionName().equals(partitionName)) {
                return info.getPartitionId();
            }
        }
        throw new AssertionError("no partition " + partitionName + " of " + tablePath);
    }

    protected void assertCount(String tableName, long expected) throws Exception {
        try (CloseableIterator<Row> it =
                batchTEnv.executeSql("select count(*) from " + tableName).collect()) {
            assertThat(it.next().getField(0)).isEqualTo(expected);
        }
    }

    /**
     * Waits until every write of every partition is tiered and readable; returns the readable
     * snapshot.
     */
    protected long waitUntilAllTiered(TablePath tablePath, long tableId, boolean partitioned) {
        if (!partitioned) {
            return waitUntilAllTiered(tablePath, tableId);
        }
        return waitValue(
                () -> {
                    LakeSnapshot snapshot;
                    try {
                        snapshot = admin.getReadableLakeSnapshot(tablePath).get();
                    } catch (Exception e) {
                        Thread.sleep(200); // nothing readable yet
                        return Optional.empty();
                    }
                    for (PartitionInfo partition : admin.listPartitionInfos(tablePath).get()) {
                        for (int b = 0; b < BUCKETS; b++) {
                            TableBucket tb =
                                    new TableBucket(tableId, partition.getPartitionId(), b);
                            long highWatermark = getLeaderReplica(tb).getLogHighWatermark();
                            if (snapshot.getTableBucketsOffset().getOrDefault(tb, 0L)
                                    < highWatermark) {
                                Thread.sleep(200);
                                return Optional.empty();
                            }
                        }
                    }
                    return Optional.of(snapshot.getSnapshotId());
                },
                Duration.ofMinutes(2),
                "writes of " + tablePath + " not all tiered");
    }

    /** Waits until every write is tiered and readable; returns the readable snapshot. */
    protected long waitUntilAllTiered(TablePath tablePath, long tableId) {
        return waitValue(
                () -> {
                    LakeSnapshot snapshot;
                    try {
                        snapshot = admin.getReadableLakeSnapshot(tablePath).get();
                    } catch (Exception e) {
                        Thread.sleep(200); // nothing readable yet
                        return Optional.empty();
                    }
                    for (int b = 0; b < BUCKETS; b++) {
                        TableBucket tb = new TableBucket(tableId, b);
                        long highWatermark = getLeaderReplica(tb).getLogHighWatermark();
                        if (snapshot.getTableBucketsOffset().getOrDefault(tb, 0L) < highWatermark) {
                            Thread.sleep(200);
                            return Optional.empty();
                        }
                    }
                    return Optional.of(snapshot.getSnapshotId());
                },
                Duration.ofMinutes(2),
                "writes of " + tablePath + " not all tiered");
    }

    /**
     * Rewrites every live data file of the unpartitioned lake table into one file per bucket,
     * dropping the delete files, like a compaction by another engine; returns the rewrite's
     * snapshot.
     */
    protected long rewriteAllDataFiles(TablePath tablePath) throws Exception {
        org.apache.iceberg.Table table = icebergCatalog.loadTable(toIceberg(tablePath));
        long baseSnapshotId = table.currentSnapshot().snapshotId();
        Function<FileScanTask, Integer> bucketOf = IcebergSplitPlanner.createBucketExtractor(table);
        Map<String, DataFile> oldDataFiles = new HashMap<>();
        Map<String, DeleteFile> oldDeleteFiles = new HashMap<>();
        Map<Integer, List<Record>> liveByBucket = new TreeMap<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                oldDataFiles.put(task.file().location(), task.file());
                for (DeleteFile delete : task.deletes()) {
                    oldDeleteFiles.put(delete.location(), delete);
                }
                List<Record> live =
                        liveByBucket.computeIfAbsent(bucketOf.apply(task), k -> new ArrayList<>());
                // the reader applies the task's deletes
                try (CloseableIterable<Record> records =
                        new IcebergGenericReader(table.newScan(), true).open(task)) {
                    for (Record record : records) {
                        live.add(record.copy());
                    }
                }
            }
        }
        RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(baseSnapshotId);
        oldDataFiles.values().forEach(rewrite::deleteFile);
        oldDeleteFiles.values().forEach(rewrite::deleteFile);
        for (Map.Entry<Integer, List<Record>> e : liveByBucket.entrySet()) {
            TaskWriter<Record> writer =
                    TaskWriterFactory.createTaskWriter(table, null, e.getKey(), true);
            for (Record record : e.getValue()) {
                writer.write(record);
            }
            for (DataFile file : writer.complete().dataFiles()) {
                rewrite.addFile(file);
            }
        }
        rewrite.commit();
        table.refresh();
        return table.currentSnapshot().snapshotId();
    }

    protected void assertLakeRead(String tableName, String columns, List<String> expected)
            throws Exception {
        CloseableIterator<Row> lakeIter =
                batchTEnv
                        .executeSql("select " + columns + " from " + tableName + "$lake")
                        .collect();
        assertResultsIgnoreOrder(lakeIter, expected, true);
    }

    protected long createDerivedTable(TablePath tablePath, int formatVersion, boolean partitioned)
            throws Exception {
        return createDerivedTable(tablePath, formatVersion, partitioned, Collections.emptyMap());
    }

    protected long createDerivedTable(
            TablePath tablePath,
            int formatVersion,
            boolean partitioned,
            Map<String, String> properties)
            throws Exception {
        Schema.Builder schema =
                Schema.newBuilder().column("c1", DataTypes.INT()).column("c2", DataTypes.STRING());
        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .distributedBy(BUCKETS)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key(), "true")
                        .customProperty("iceberg.format-version", String.valueOf(formatVersion));
        properties.forEach(builder::property);
        if (partitioned) {
            schema.column("c3", DataTypes.STRING()).primaryKey("c1", "c3");
            builder.partitionedBy("c3");
        } else {
            schema.primaryKey("c1");
        }
        return createTable(tablePath, builder.schema(schema.build()).build());
    }

    /** Upserts a tick row until the readable snapshot advances. */
    protected long tickUntilReadableAdvances(
            TablePath tablePath, long previous, @Nullable String partition) throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(3).toMillis();
        while (System.currentTimeMillis() < deadline) {
            try {
                long id = admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId();
                if (id != previous) {
                    return id;
                }
            } catch (Exception e) {
                // no readable snapshot yet
            }
            writeRows(tablePath, Collections.singletonList(rowOf(-1, "tick", partition)), false);
            Thread.sleep(1000);
        }
        throw new AssertionError("readable snapshot of " + tablePath + " stuck at " + previous);
    }

    protected static InternalRow rowOf(int key, String value, @Nullable String partition) {
        return partition == null ? row(key, value) : row(key, value, partition);
    }

    protected static String rowString(int key, String value, @Nullable String partition) {
        return partition == null
                ? String.format("+I[%d, %s]", key, value)
                : String.format("+I[%d, %s, %s]", key, value, partition);
    }
}
