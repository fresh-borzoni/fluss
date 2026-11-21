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

package org.apache.fluss.flink.source;

import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.testutils.common.MultiVersionTest;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.functions.ScalarFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A delta join of Flink that reads its Fluss tables as of the epochs of their scans: the scans cut
 * their changes into epochs, the lookups read the versioned tables as of them, and the output
 * converges to the join of the tables.
 */
@MultiVersionTest
class Flink23VersionedDeltaJoinITCase extends FlinkTestBase {

    private static final String CATALOG_NAME = "versioned_catalog";
    private static final String DB = "versioned_db";
    private static final int KEYS = 6;
    private static final int IDS = 16;

    private StreamTableEnvironment tEnv;

    @BeforeEach
    void before() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());
        tEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tEnv.useCatalog(CATALOG_NAME);
        tEnv.executeSql(String.format("create database if not exists `%s`", DB));
        tEnv.useDatabase(DB);
        tEnv.getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_ENABLED, true)
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_GROUPING,
                        OptimizerConfigOptions.DeltaJoinGrouping.ANCHOR)
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_READ_BACK,
                        false)
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_VERSIONED,
                        OptimizerConfigOptions.DeltaJoinVersioned.FORCE);
    }

    @AfterEach
    void after() {
        tEnv.useDatabase(BUILTIN_DATABASE);
        tEnv.executeSql(String.format("drop database `%s` cascade", DB));
    }

    @Test
    @MultiVersionTest
    void versionedDeltaJoinConverges() throws Exception {
        runAndConverge("a_v", 30);
    }

    /**
     * The job fails once after checkpoints completed: the scans replay the epochs they ended, and
     * the output still converges.
     */
    @Test
    @MultiVersionTest
    void versionedDeltaJoinRecoversFromAFailure() throws Exception {
        tEnv.getConfig().set("execution.checkpointing.interval", "200 ms");
        tEnv.getConfig().set("restart-strategy.type", "fixed-delay");
        tEnv.getConfig().set("restart-strategy.fixed-delay.attempts", "3");
        tEnv.getConfig().set("restart-strategy.fixed-delay.delay", "100 ms");
        FailOnce.reset();
        tEnv.createTemporarySystemFunction("fail_once", FailOnce.class);
        runAndConverge("fail_once(a_v)", 80);
        assertThat(FailOnce.FAILED.get()).isTrue();
    }

    private void runAndConverge(String aValue, int rounds) throws Exception {
        String versioned =
                String.format(
                        "'%s' = 'true', 'scan.epoch.interval' = '100 ms'",
                        ConfigOptions.TABLE_KV_MVCC_ENABLED.key());
        tEnv.executeSql(
                "create table a (a_k int, a_id int, a_v int, primary key (a_k, a_id) not enforced)"
                        + " with ('bucket.key' = 'a_k', 'bucket.num' = '2', "
                        + versioned
                        + ")");
        tEnv.executeSql(
                "create table b (b_id int, b_v int, primary key (b_id) not enforced)"
                        + " with ('bucket.num' = '2', "
                        + versioned
                        + ")");
        tEnv.executeSql(
                "create table sink (a_k int, a_id int, a_v int, b_v int,"
                        + " primary key (a_k, a_id) not enforced)");

        Random random = new Random(42);
        Tables tables = new Tables();
        for (int key = 0; key < KEYS; key++) {
            tables.upsertB(key, random.nextInt(1000));
        }
        for (int i = 0; i < IDS; i++) {
            tables.upsertA(random.nextInt(KEYS), i, random.nextInt(1000));
        }

        String insert =
                "INSERT INTO sink SELECT a_k, a_id, "
                        + aValue
                        + ", b_v FROM a JOIN b ON a_k = b_id";
        String plan = tEnv.explainSql(insert);
        assertThat(plan)
                .contains("GroupRefreshDeltaJoin(")
                .contains("mode=[VERSIONED]")
                .contains("epochs=[");

        TableResult result = tEnv.executeSql(insert);
        JobClient job = result.getJobClient().get();
        try {
            for (int round = 0; round < rounds; round++) {
                tables.change(random);
                Thread.sleep(random.nextInt(60));
            }
            try (Table sink = conn.getTable(TablePath.of(DB, "sink"))) {
                Lookuper lookuper = sink.newLookup().createLookuper();
                Map<String, String> expected = tables.joined();
                waitUntil(
                        () -> rowsOf(lookuper).equals(expected),
                        Duration.ofMinutes(2),
                        Duration.ofMillis(200),
                        "The versioned delta join did not converge to the join of its tables.");
            }
        } finally {
            job.cancel().get(30, TimeUnit.SECONDS);
            tables.close();
        }
    }

    /** Returns its argument, and fails once, a while after its first call. */
    public static class FailOnce extends ScalarFunction {
        static final AtomicBoolean FAILED = new AtomicBoolean();
        static final AtomicLong FIRST_CALL = new AtomicLong();

        static void reset() {
            FAILED.set(false);
            FIRST_CALL.set(0L);
        }

        public Integer eval(Integer value) {
            long now = System.currentTimeMillis();
            FIRST_CALL.compareAndSet(0L, now);
            if (now - FIRST_CALL.get() > 2000 && FAILED.compareAndSet(false, true)) {
                throw new RuntimeException("Injected failure of the versioned delta join.");
            }
            return value;
        }
    }

    /** The rows of the sink, by their key. */
    private static Map<String, String> rowsOf(Lookuper sink) throws Exception {
        Map<String, String> rows = new TreeMap<>();
        for (int key = 0; key < KEYS; key++) {
            for (int id = 0; id < IDS; id++) {
                InternalRow row = sink.lookup(GenericRow.of(key, id)).get().getSingletonRow();
                if (row != null) {
                    rows.put(
                            key + "/" + id,
                            row.getInt(2) + "," + (row.isNullAt(3) ? "null" : row.getInt(3)));
                }
            }
        }
        return rows;
    }

    /** The two tables, written through Fluss, and what they hold. */
    private static final class Tables implements AutoCloseable {
        private final Table a = conn.getTable(TablePath.of(DB, "a"));
        private final Table b = conn.getTable(TablePath.of(DB, "b"));
        private final UpsertWriter aWriter = a.newUpsert().createWriter();
        private final UpsertWriter bWriter = b.newUpsert().createWriter();

        /** Per id of a: its key and value. */
        private final Map<Integer, int[]> aRows = new TreeMap<>();

        private final Map<Integer, Integer> bRows = new TreeMap<>();

        void upsertA(int key, int id, int value) throws Exception {
            int[] old = aRows.get(id);
            if (old != null && old[0] != key) {
                // the key of a row of a is part of its primary key: it moves
                aWriter.delete(GenericRow.of(old[0], id, old[1])).get();
            }
            aWriter.upsert(GenericRow.of(key, id, value)).get();
            aRows.put(id, new int[] {key, value});
        }

        void upsertB(int id, int value) throws Exception {
            bWriter.upsert(GenericRow.of(id, value)).get();
            bRows.put(id, value);
        }

        void change(Random random) throws Exception {
            int choice = random.nextInt(10);
            if (choice < 4) {
                upsertA(random.nextInt(KEYS), random.nextInt(IDS), random.nextInt(1000));
            } else if (choice < 7) {
                upsertB(random.nextInt(KEYS), random.nextInt(1000));
            } else if (choice < 8) {
                int id = random.nextInt(IDS);
                int[] old = aRows.remove(id);
                if (old != null) {
                    aWriter.delete(GenericRow.of(old[0], id, old[1])).get();
                }
            } else {
                int id = random.nextInt(KEYS);
                Integer old = bRows.remove(id);
                if (old != null) {
                    bWriter.delete(GenericRow.of(id, old)).get();
                }
            }
        }

        /** The inner join of the tables, by the key of a row of a. */
        Map<String, String> joined() {
            Map<String, String> rows = new TreeMap<>();
            for (Map.Entry<Integer, int[]> row : aRows.entrySet()) {
                Integer bValue = bRows.get(row.getValue()[0]);
                if (bValue != null) {
                    rows.put(
                            row.getValue()[0] + "/" + row.getKey(),
                            row.getValue()[1] + "," + bValue);
                }
            }
            return rows;
        }

        @Override
        public void close() throws Exception {
            a.close();
            b.close();
        }
    }
}
