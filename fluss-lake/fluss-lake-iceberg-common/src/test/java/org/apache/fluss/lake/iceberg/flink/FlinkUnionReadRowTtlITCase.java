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

package org.apache.fluss.lake.iceberg.flink;

import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.kv.RowTtlFilterClock;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Union reads of a table with row TTL whose tiering stalls longer than the TTL. Each leader caps
 * the clock of its TTL filter below the TTL of the first change after the readable snapshot, so a
 * version written after it stays in the KV until it is tiered: the next write of its key names it,
 * and union reads keep their masks. Once a lake snapshot holds it, it expires.
 */
class FlinkUnionReadRowTtlITCase extends FlinkDerivedDvTestBase {

    private static final Duration TTL = Duration.ofSeconds(3);
    private static final int KEY = 100;

    @Test
    void testAVersionNotTieredYetOutlivesItsTtl() throws Exception {
        String tableName = "row_ttl_cap";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        Map<Integer, String> expected = new TreeMap<>();
        long tableId;
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            tableId =
                    createDerivedTable(
                            tablePath,
                            3,
                            false,
                            Collections.singletonMap(
                                    ConfigOptions.TABLE_KV_TTL.key(), TTL.toMillis() + "ms"));
            upsert(tablePath, expected, 0, 10, "tiered");
            waitUntilAllTiered(tablePath, tableId);
        } finally {
            // tiering stops: what is written from now on stays above the readable offset
            jobClient.cancel().get();
        }

        upsert(tablePath, expected, KEY, KEY + 1, "untiered");
        Thread.sleep(TTL.toMillis() + 1_000);
        compact(tableId);
        assertThat(lookup(tablePath, KEY)).isNotNull();
        upsert(tablePath, expected, KEY, KEY + 1, "rewritten");

        // the write named the version it superseded: the masks are exact
        assertThat(unionRead(tableName)).containsExactlyInAnyOrderElementsOf(asRows(expected));
        assertThat(rowTtlCorruptions()).isZero();

        // a lake snapshot holds the key's version: the cap moves past it, and it expires
        tierAll(tablePath, tableId);
        retry(
                Duration.ofMinutes(1),
                () -> {
                    compact(tableId);
                    assertThat(lookup(tablePath, KEY)).isNull();
                });
    }

    /**
     * By event time: while tiering stalls, a row backfilled with an event time a day old outlives
     * its TTL, its next write names it, and union reads keep their masks. Once tiered, it expires.
     */
    @Test
    void testABackfilledRowOutlivesItsEventTimeTtlUntilItIsTiered() throws Exception {
        String tableName = "row_ttl_event_time_cap";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        long tableId =
                createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("c1", DataTypes.INT())
                                                .column("c2", DataTypes.STRING())
                                                .column("ts", DataTypes.BIGINT())
                                                .primaryKey("c1")
                                                .build())
                                .distributedBy(BUCKETS)
                                .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                                .property(
                                        ConfigOptions.TABLE_DATALAKE_FRESHNESS,
                                        Duration.ofMillis(500))
                                .property(
                                        ConfigOptions.TABLE_DATALAKE_DELETION_VECTORS_ENABLED.key(),
                                        "true")
                                .property(ConfigOptions.TABLE_KV_TTL.key(), TTL.toMillis() + "ms")
                                .property(ConfigOptions.TABLE_KV_TTL_TIME_COLUMN.key(), "ts")
                                .customProperty("iceberg.format-version", "3")
                                .build());
        Map<Integer, String> expected = new TreeMap<>();
        long now = System.currentTimeMillis();
        write(tablePath, expected, 0, 10, "tiered", now + Duration.ofHours(1).toMillis());
        tierAll(tablePath, tableId);

        // tiering stalls: a backfilled row, expired by its event time when it is written
        long backfilled = now - Duration.ofDays(1).toMillis();
        write(tablePath, expected, KEY, KEY + 1, "backfilled", backfilled);
        Thread.sleep(TTL.toMillis() + 1_000);
        compact(tableId);
        assertThat(lookup(tablePath, KEY)).isNotNull();
        write(tablePath, expected, KEY, KEY + 1, "rewritten", backfilled);

        assertThat(unionRead(tableName, "c1, c2"))
                .containsExactlyInAnyOrderElementsOf(asRows(expected));
        assertThat(rowTtlCorruptions()).isZero();

        // new leaders restore the buckets from KV snapshots with the filter's state: the cap
        // holds the backfilled row at once, and reads keep masks
        for (int b = 0; b < BUCKETS; b++) {
            FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(new TableBucket(tableId, b));
        }
        restartLeaders(tableId);
        retry(
                Duration.ofMinutes(1),
                () -> {
                    long lag = 0;
                    for (int b = 0; b < BUCKETS; b++) {
                        RowTtlFilterClock filter =
                                getLeaderReplica(new TableBucket(tableId, b))
                                        .getKvTablet()
                                        .getDvManager()
                                        .rowTtlFilterClock();
                        assertThat(filter.getAsLong())
                                .isNotEqualTo(RowTtlFilterClock.EXPIRES_NOTHING);
                        lag = Math.max(lag, filter.capLagMs());
                    }
                    assertThat(lag).isGreaterThan(Duration.ofHours(23).toMillis());
                });
        compact(tableId);
        assertThat(lookup(tablePath, KEY)).isNotNull();
        assertThat(unionRead(tableName, "c1, c2"))
                .containsExactlyInAnyOrderElementsOf(asRows(expected));
        assertThat(rowTtlCorruptions()).isZero();

        tierAll(tablePath, tableId);
        retry(
                Duration.ofMinutes(1),
                () -> {
                    compact(tableId);
                    assertThat(lookup(tablePath, KEY)).isNull();
                });
        // the tiered rows of an hour ahead are still there
        assertThat(lookup(tablePath, 0)).isNotNull();
    }

    /** Restarts the leaders of the table's buckets, so their buckets get new leaders. */
    private void restartLeaders(long tableId) throws Exception {
        Set<Integer> leaders = new HashSet<>();
        for (int b = 0; b < BUCKETS; b++) {
            leaders.add(FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(new TableBucket(tableId, b)));
        }
        for (int leader : leaders) {
            FLUSS_CLUSTER_EXTENSION.restartTabletServer(
                    leader, new org.apache.fluss.config.Configuration());
        }
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);
        // the shared client may still know the restarted servers' old endpoints
        admin.close();
        conn.close();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    private void tierAll(TablePath tablePath, long tableId) throws Exception {
        // batch reads leave execEnv in batch mode; tiering is a streaming job
        StreamExecutionEnvironment tieringEnv =
                StreamExecutionEnvironment.getExecutionEnvironment();
        tieringEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        tieringEnv.setParallelism(2);
        JobClient tiering = buildTieringJob(tieringEnv);
        try {
            waitUntilAllTiered(tablePath, tableId);
        } finally {
            tiering.cancel().get();
        }
    }

    private void write(
            TablePath tablePath,
            Map<Integer, String> expected,
            int from,
            int to,
            String prefix,
            long eventTime)
            throws Exception {
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (int key = from; key < to; key++) {
                writer.upsert(
                        GenericRow.of(key, BinaryString.fromString(prefix + "_" + key), eventTime));
                expected.put(key, prefix + "_" + key);
            }
            writer.flush();
        }
    }

    private void compact(long tableId) throws Exception {
        for (int b = 0; b < BUCKETS; b++) {
            getLeaderReplica(new TableBucket(tableId, b))
                    .getKvTablet()
                    .getRocksDBKv()
                    .getDb()
                    .compactRange();
        }
    }

    @Nullable
    private InternalRow lookup(TablePath tablePath, int key) throws Exception {
        try (Table table = conn.getTable(tablePath)) {
            return table.newLookup()
                    .createLookuper()
                    .lookup(GenericRow.of(key))
                    .get()
                    .getSingletonRow();
        }
    }

    /** Buckets whose saved row TTL state was found corrupt, on the running tablet servers. */
    private static long rowTtlCorruptions() {
        long corruptions = 0;
        for (TabletServer server : FLUSS_CLUSTER_EXTENSION.getTabletServers()) {
            corruptions +=
                    server.getReplicaManager()
                            .getServerMetricGroup()
                            .dvRowTtlCorruptCount()
                            .getCount();
        }
        return corruptions;
    }

    private List<String> unionRead(String tableName) throws Exception {
        return unionRead(tableName, "*");
    }

    private List<String> unionRead(String tableName, String columns) throws Exception {
        List<String> rows = new ArrayList<>();
        try (CloseableIterator<Row> it =
                batchTEnv.executeSql("select " + columns + " from " + tableName).collect()) {
            while (it.hasNext()) {
                rows.add(it.next().toString());
            }
        }
        return rows;
    }

    private static List<String> asRows(Map<Integer, String> expected) {
        List<String> rows = new ArrayList<>();
        expected.forEach((key, value) -> rows.add(rowString(key, value, null)));
        return rows;
    }
}
