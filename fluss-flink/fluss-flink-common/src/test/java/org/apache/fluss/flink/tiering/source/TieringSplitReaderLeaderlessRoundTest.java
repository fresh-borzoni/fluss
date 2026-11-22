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

package org.apache.fluss.flink.tiering.source;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.client.tiering.DvRoundRequests;
import org.apache.fluss.client.tiering.TableBucketWriteResult;
import org.apache.fluss.client.tiering.TieringLogSplit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.tiering.TestingLakeTieringFactory;
import org.apache.fluss.flink.tiering.TestingWriteResult;
import org.apache.fluss.flink.tiering.source.metrics.TieringMetrics;
import org.apache.fluss.flink.tiering.source.split.FlinkTieringSplit;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A planned round of a bucket whose leader is gone ends past the time limit without writing, so its
 * table's round commits the other buckets and the reader goes on to its next table, as it does for
 * rounds that are not planned.
 */
class TieringSplitReaderLeaderlessRoundTest {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(
                            new Configuration()
                                    .set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON)
                                    .set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 1))
                    .setNumOfTabletServers(2)
                    .build();

    private static final int BUCKETS = 4;

    @Test
    void testRoundOfABucketWithoutLeaderEndsPastTheTimeLimit() throws Exception {
        TablePath tablePath = TablePath.of("leaderless_rounds", "leaderless");
        long tableId;
        Map<TableBucket, TieringLogSplit> plans = new HashMap<>();
        Set<TableBucket> leaderless = new HashSet<>();
        try (Connection conn =
                ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig())) {
            Admin admin = conn.getAdmin();
            admin.createDatabase("leaderless_rounds", DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(
                            tablePath,
                            TableDescriptor.builder()
                                    .schema(
                                            Schema.newBuilder()
                                                    .primaryKey("id")
                                                    .column("id", DataTypes.INT())
                                                    .column("name", DataTypes.STRING())
                                                    .build())
                                    .distributedBy(BUCKETS, "id")
                                    .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                                    .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                                    .property(
                                            ConfigOptions.TABLE_DATALAKE_DELETION_VECTORS_ENABLED,
                                            true)
                                    .build(),
                            true)
                    .get();
            tableId = admin.getTableInfo(tablePath).get().getTableId();
            Map<TableBucket, Long> starts = new HashMap<>();
            for (int bucket = 0; bucket < BUCKETS; bucket++) {
                TableBucket tb = new TableBucket(tableId, bucket);
                FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);
                starts.put(tb, 0L);
            }
            try (Table table = conn.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                for (int version = 0; version < 2; version++) {
                    for (int key = 0; key < 100; key++) {
                        writer.upsert(row(key, "v" + version));
                    }
                    writer.flush();
                }
            }
            for (Map.Entry<TableBucket, GetDvSnapshotResponse> plan :
                    new DvRoundRequests(admin, tablePath, Thread::sleep)
                            .planRounds(-1L, starts, 1_000_000, 1L << 30)
                            .entrySet()) {
                plans.put(
                        plan.getKey(),
                        TieringLogSplit.plannedRound(
                                tablePath,
                                plan.getKey(),
                                null,
                                0,
                                plan.getValue().getRoundEndOffset(),
                                -1,
                                false));
            }
            // stop the leader of bucket 0, and with it every bucket it leads
            int stopped = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(new TableBucket(tableId, 0));
            for (TableBucket tb : plans.keySet()) {
                if (FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb) == stopped) {
                    leaderless.add(tb);
                }
            }
            FLUSS_CLUSTER_EXTENSION.stopTabletServer(stopped);
        }
        assertThat(leaderless).isNotEmpty().hasSizeLessThan(BUCKETS);

        Map<TableBucket, TableBucketWriteResult<TestingWriteResult>> results = new HashMap<>();
        try (Connection connection =
                        ConnectionFactory.createConnection(
                                FLUSS_CLUSTER_EXTENSION.getClientConfig());
                TieringSplitReader<TestingWriteResult> reader =
                        new TieringSplitReader<>(
                                connection,
                                new TestingLakeTieringFactory(),
                                Thread.currentThread().getContextClassLoader(),
                                new TieringMetrics(
                                        InternalSourceReaderMetricGroup.mock(
                                                new MetricListener().getMetricGroup())),
                                new Configuration())) {
            List<FlinkTieringSplit> splits = new ArrayList<>();
            for (TieringLogSplit split : plans.values()) {
                splits.add(new FlinkTieringSplit(split.copy(plans.size())));
            }
            reader.handleSplitsChanges(new SplitsAddition<>(splits));
            reader.handleTableReachTieringMaxDuration(tableId);
            Set<String> finished = new HashSet<>();
            long deadline = System.currentTimeMillis() + 180_000;
            while (finished.size() < splits.size()) {
                assertThat(System.currentTimeMillis())
                        .as("the rounds end, %s of %s ended", finished.size(), splits.size())
                        .isLessThan(deadline);
                RecordsWithSplitIds<TableBucketWriteResult<TestingWriteResult>> fetched =
                        reader.fetch();
                finished.addAll(fetched.finishedSplits());
                while (fetched.nextSplit() != null) {
                    TableBucketWriteResult<TestingWriteResult> result =
                            fetched.nextRecordFromSplit();
                    results.put(result.tableBucket(), result);
                }
            }
        }
        for (TableBucket tb : plans.keySet()) {
            TableBucketWriteResult<TestingWriteResult> result = results.get(tb);
            if (leaderless.contains(tb)) {
                // nothing tiered: the bucket keeps its offset
                assertThat(result.logEndOffset()).as("%s", tb).isEqualTo(-1);
                assertThat(result.writeResult()).as("%s", tb).isNull();
            } else {
                assertThat(result.logEndOffset())
                        .as("%s", tb)
                        .isEqualTo(plans.get(tb).getStoppingOffset());
            }
        }
    }
}
