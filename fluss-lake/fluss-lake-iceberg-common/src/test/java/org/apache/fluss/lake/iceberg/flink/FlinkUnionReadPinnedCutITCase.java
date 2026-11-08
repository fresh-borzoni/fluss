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
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.source.testutils.UnionReadUnderWrites;
import org.apache.fluss.lake.iceberg.testutils.LogCapture;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.fluss.flink.source.testutils.UnionReadUnderWrites.DV_READ_LOGGER;
import static org.apache.fluss.flink.source.testutils.UnionReadUnderWrites.FILE_GROUP_ROWS;
import static org.apache.fluss.flink.source.testutils.UnionReadUnderWrites.everyNth;
import static org.apache.fluss.flink.source.testutils.UnionReadUnderWrites.leaderPinsCuts;
import static org.apache.fluss.flink.source.testutils.UnionReadUnderWrites.partsReadPerBucket;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests union reads of Iceberg tables with deletion vectors whose buckets are read by several
 * parts, groups of lake files and the log, all as of the cut planning pinned on the bucket's
 * leader. The tests are skipped when the leaders do not pin cuts.
 */
class FlinkUnionReadPinnedCutITCase extends FlinkUnionReadTestBase {

    private static final int BUCKETS = 3;
    private static final int ROUNDS = 3;
    private static final int KEYS_PER_ROUND = 300;
    private static final String IDLE_TIMEOUT = "kv.deletion-vectors.pinned-cut-idle-timeout";

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
        // the tiering job writes the position index under the remote data dir
        clientConf.set(ConfigOptions.REMOTE_DATA_DIR, FLUSS_CLUSTER_EXTENSION.getRemoteDataDir());
    }

    /**
     * Each bucket is read by a split per lake file and one for its log while writes go on: the
     * splits read the bucket as of one cut, with the masks the leader serves for it.
     */
    @Test
    void testBucketsReadInSeveralSplitsWhileWritesGoOn() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "pinned_parts");
        long tableId = createDerivedTable(tablePath);
        Map<Integer, String> before = tierInRounds(tablePath, tableId);
        assumeTrue(leaderPinsCuts(admin, tablePath, tableId), "the bucket leaders do not pin cuts");

        try (LogCapture log = LogCapture.of(DV_READ_LOGGER)) {
            new UnionReadUnderWrites(clientConf, tablePath)
                    .withHint(FILE_GROUP_ROWS, "1")
                    .withMillisPerRow(2)
                    .readAndCheck(before, everyNth(before, 7), Duration.ofMinutes(5));

            assertThat(log.messagesWith("Pinned the mask cuts of 3 of 3 buckets")).hasSize(1);
            assertEachBucketReadInParts(log.messages(), ROUNDS);
            assertThat(log.messagesWith("deriving its masks")).isEmpty();
        }
    }

    /**
     * Splits that start after the leader dropped an idle pinned cut derive the cut's masks from the
     * keys of the log and their lake files, and read the bucket as of the same cut as the splits
     * that started before, while writes go on. A leader drops a cut no request asked for within its
     * idle timeout.
     */
    @Test
    void testSplitsDeriveTheMasksOfACutTheLeaderDropped() throws Exception {
        Configuration shortIdle = new Configuration();
        shortIdle.setString(IDLE_TIMEOUT, "1s");
        restartTabletServers(shortIdle);
        try {
            TablePath tablePath = TablePath.of(DEFAULT_DB, "pinned_parts_dropped_cut");
            long tableId = createDerivedTable(tablePath);
            Map<Integer, String> before = tierInRounds(tablePath, tableId);
            assumeTrue(
                    leaderPinsCuts(admin, tablePath, tableId),
                    "the bucket leaders do not pin cuts");

            try (LogCapture log = LogCapture.of(DV_READ_LOGGER)) {
                // each split takes longer than the idle timeout, so later ones find the cut gone
                new UnionReadUnderWrites(clientConf, tablePath)
                        .withHint(FILE_GROUP_ROWS, "1")
                        .withMillisPerRow(12)
                        .readAndCheck(before, everyNth(before, 7), Duration.ofMinutes(8));

                assertEachBucketReadInParts(log.messages(), ROUNDS);
                assertThat(log.messagesWith("deriving its masks")).isNotEmpty();
            }
        } finally {
            restartTabletServers(new Configuration());
        }
    }

    /**
     * Each bucket was read by a part per lake file, {@code files} at least, and one for its log.
     */
    private static void assertEachBucketReadInParts(List<String> messages, int files) {
        Map<Integer, Set<String>> read = partsReadPerBucket(messages);
        assertThat(read).as("parts read per bucket").hasSize(BUCKETS);
        read.forEach(
                (bucket, parts) ->
                        assertThat(parts)
                                .as("parts of bucket %s", bucket)
                                .contains("log", "files 0")
                                .hasSizeGreaterThanOrEqualTo(files + 1));
    }

    /**
     * Writes {@link #ROUNDS} rounds of new keys, each tiered into its own lake file per bucket,
     * then a log tail of updates and deletes; returns the table's rows.
     */
    private Map<Integer, String> tierInRounds(TablePath tablePath, long tableId) throws Exception {
        Map<Integer, String> rows = new TreeMap<>();
        JobClient tiering = buildTieringJob(execEnv);
        try {
            for (int round = 0; round < ROUNDS; round++) {
                List<InternalRow> written = new ArrayList<>();
                for (int key = round * KEYS_PER_ROUND; key < (round + 1) * KEYS_PER_ROUND; key++) {
                    written.add(row(key, "v_" + key));
                    rows.put(key, "v_" + key);
                }
                writeRows(tablePath, written, false);
                waitUntilAllTiered(tablePath, tableId);
            }
        } finally {
            tiering.cancel().get();
        }
        // a log tail, so the masks hide the tiered versions of its keys
        List<InternalRow> updates = new ArrayList<>();
        for (int key = 0; key < ROUNDS * KEYS_PER_ROUND; key += 11) {
            updates.add(row(key, "u_" + key));
            rows.put(key, "u_" + key);
        }
        writeRows(tablePath, updates, false);
        try (org.apache.fluss.client.table.Table table = conn.getTable(tablePath)) {
            org.apache.fluss.client.table.writer.UpsertWriter writer =
                    table.newUpsert().createWriter();
            for (int key = 5; key < ROUNDS * KEYS_PER_ROUND; key += 13) {
                writer.delete(row(key, "x"));
                rows.remove(key);
            }
            writer.flush();
        }
        return rows;
    }

    private long waitUntilAllTiered(TablePath tablePath, long tableId) {
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

    private long createDerivedTable(TablePath tablePath) throws Exception {
        return createTable(
                tablePath,
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("c1", DataTypes.INT())
                                        .column("c2", DataTypes.STRING())
                                        .primaryKey("c1")
                                        .build())
                        .distributedBy(BUCKETS)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key(), "true")
                        .customProperty("iceberg.format-version", "3")
                        .build());
    }

    private static void restartTabletServers(Configuration overrides) throws Exception {
        for (int server = 0; server < 3; server++) {
            FLUSS_CLUSTER_EXTENSION.restartTabletServer(server, overrides);
        }
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);
        // the shared client would keep the restarted servers' old endpoints and stall writes
        admin.close();
        conn.close();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }
}
