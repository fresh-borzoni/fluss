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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Union reads of a table with row TTL whose tiering lags more than the TTL: a version written after
 * the readable snapshot expires from the KV before it is tiered, and the next write of its key
 * cannot name it. The leaders have such buckets read by key, so the key is read once.
 */
class FlinkUnionReadRowTtlITCase extends FlinkDerivedDvTestBase {

    private static final Duration TTL = Duration.ofSeconds(3);
    private static final int KEY = 100;

    @Test
    void testAVersionThatExpiresBeforeItIsTieredIsReadOnce() throws Exception {
        String tableName = "row_ttl_fold";
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

        upsert(tablePath, expected, KEY, KEY + 1, "expired");
        Thread.sleep(TTL.toMillis() + 1_000);
        // the TTL filter drops the expired version; the next write of the key is an insert
        for (int b = 0; b < BUCKETS; b++) {
            getLeaderReplica(new TableBucket(tableId, b))
                    .getKvTablet()
                    .getRocksDBKv()
                    .getDb()
                    .compactRange();
        }
        upsert(tablePath, expected, KEY, KEY + 1, "rewritten");

        List<String> rows = unionRead(tableName);
        assertThat(rows).containsExactlyInAnyOrderElementsOf(asRows(expected));

        // with masks served regardless, the expired version is read next to the new one
        FLUSS_CLUSTER_EXTENSION.ignoreDvRowTtlExpiry(true);
        try {
            List<String> duplicated = new ArrayList<>(asRows(expected));
            duplicated.add(rowString(KEY, "expired_" + KEY, null));
            assertThat(unionRead(tableName)).containsExactlyInAnyOrderElementsOf(duplicated);
        } finally {
            FLUSS_CLUSTER_EXTENSION.ignoreDvRowTtlExpiry(false);
        }
    }

    private List<String> unionRead(String tableName) throws Exception {
        List<String> rows = new ArrayList<>();
        try (CloseableIterator<Row> it =
                batchTEnv.executeSql("select * from " + tableName).collect()) {
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
