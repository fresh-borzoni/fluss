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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.testutils.DataTestUtils.row;

/**
 * Union-read tests for Iceberg primary-key tables with deletion vectors: rows updated or deleted
 * after they were tiered must be read exactly once, in their latest version. {@link
 * FlinkUnionReadDerivedDvITCase} covers more buckets, partitions and both format versions.
 */
class FlinkUnionReadDvTableITCase extends FlinkUnionReadTestBase {

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
        // Tiering writes the position index under the remote data dir; the tiering job takes its
        // Fluss config from clientConf, which has no remote dir by default.
        clientConf.set(ConfigOptions.REMOTE_DATA_DIR, FLUSS_CLUSTER_EXTENSION.getRemoteDataDir());
    }

    /** A union read of rows updated after they were tiered returns only their latest versions. */
    @Test
    void testUnionReadWithUpdates() throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName = "dv_update_table";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId = createPkTable(tablePath);

            writeRows(tablePath, upsertRows(0, 5, "v"), false);
            waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, false);

            writeRows(tablePath, upsertRows(0, 3, "updated"), false);
            waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, false);

            List<String> expected =
                    Arrays.asList(
                            "+I[0, updated_0]",
                            "+I[1, updated_1]",
                            "+I[2, updated_2]",
                            "+I[3, v_3]",
                            "+I[4, v_4]");
            CloseableIterator<Row> rowIter =
                    batchTEnv.executeSql("select * from " + tableName).collect();
            assertResultsIgnoreOrder(rowIter, expected, true);
        } finally {
            jobClient.cancel().get();
        }
    }

    private long createPkTable(TablePath tablePath) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .primaryKey("c1")
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(DEFAULT_BUCKET_NUM)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key(), "true")
                        .build();
        return createTable(tablePath, tableDescriptor);
    }

    private static List<InternalRow> upsertRows(int from, int to, String valuePrefix) {
        List<InternalRow> rows = new ArrayList<>();
        for (int i = from; i < to; i++) {
            rows.add(row(i, valuePrefix + "_" + i));
        }
        return rows;
    }
}
