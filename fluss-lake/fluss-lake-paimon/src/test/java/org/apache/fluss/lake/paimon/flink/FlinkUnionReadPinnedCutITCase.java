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

package org.apache.fluss.lake.paimon.flink;

import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.source.testutils.UnionReadUnderWrites;
import org.apache.fluss.flink.utils.LakeSourceUtils;
import org.apache.fluss.lake.paimon.testutils.LogCapture;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.fluss.flink.source.testutils.UnionReadUnderWrites.DV_READ_LOGGER;
import static org.apache.fluss.flink.source.testutils.UnionReadUnderWrites.FILE_GROUP_ROWS;
import static org.apache.fluss.flink.source.testutils.UnionReadUnderWrites.everyNth;
import static org.apache.fluss.flink.source.testutils.UnionReadUnderWrites.leaderPinsCuts;
import static org.apache.fluss.flink.source.testutils.UnionReadUnderWrites.partsReadPerBucket;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Union reads of Paimon tables with deletion vectors whose buckets are read by several splits:
 * groups of their lake files, and their log. All splits of a bucket use the cut that planning
 * pinned on its leader. The tests are skipped when the leaders do not pin cuts.
 */
class FlinkUnionReadPinnedCutITCase extends FlinkUnionReadTestBase {

    private static final int BUCKETS = 3;
    // lake files per bucket before a read; each is read by its own split
    private static final int FILES_PER_BUCKET = 3;
    private static final int MAX_ROUNDS = 6;
    private static final int KEYS_PER_ROUND = 400;
    // Each round's keys, including those of the rows that only advance the readable snapshot, lie
    // in a range of their own, so files of different rounds do not overlap.
    private static final int ROUND_KEY_SPAN = 2000;
    private static final String IDLE_TIMEOUT = "kv.deletion-vectors.pinned-cut-idle-timeout";

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
        // the tiering job writes the position index under the remote data dir
        clientConf.set(ConfigOptions.REMOTE_DATA_DIR, FLUSS_CLUSTER_EXTENSION.getRemoteDataDir());
    }

    /**
     * While writes go on, each bucket is read by a split per lake file and one for its log. All
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
            assertEachBucketReadInParts(log.messages());
            assertThat(log.messagesWith("deriving its masks")).isEmpty();
        }
    }

    /**
     * A leader drops a cut that no request asked for within its idle timeout. The splits of a
     * bucket that start after that derive the cut's masks from the keys of the log and their lake
     * files. While writes go on, they read the bucket as of the same cut as the splits that started
     * before.
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

                assertEachBucketReadInParts(log.messages());
                assertThat(log.messagesWith("deriving its masks")).isNotEmpty();
            }
        } finally {
            restartTabletServers(new Configuration());
        }
    }

    /** Each bucket was read by several parts: groups of its lake files and its log. */
    private static void assertEachBucketReadInParts(List<String> messages) {
        Map<Integer, Set<String>> read = partsReadPerBucket(messages);
        assertThat(read).as("parts read per bucket").hasSize(BUCKETS);
        read.forEach(
                (bucket, parts) ->
                        assertThat(parts)
                                .as("parts of bucket %s", bucket)
                                .contains("log", "files 0")
                                .hasSizeGreaterThanOrEqualTo(3));
    }

    /**
     * Writes rounds of new keys, each readable in the lake before the next, until the readable
     * snapshot holds {@link #FILES_PER_BUCKET} lake splits of every bucket. Then writes a log tail
     * of updates and deletes. Returns the table's rows.
     */
    private Map<Integer, String> tierInRounds(TablePath tablePath, long tableId) throws Exception {
        Map<Integer, String> rows = new TreeMap<>();
        Map<Integer, Integer> lakeSplits = new TreeMap<>();
        JobClient tiering = buildTieringJob(execEnv);
        try {
            for (int round = 0; round < MAX_ROUNDS; round++) {
                int firstKey = round * ROUND_KEY_SPAN;
                List<InternalRow> written = new ArrayList<>();
                for (int key = firstKey; key < firstKey + KEYS_PER_ROUND; key++) {
                    written.add(row(key, "v_" + key));
                    rows.put(key, "v_" + key);
                }
                writeRows(tablePath, written, false);
                tickUntilReadable(tablePath, tableId, rows, firstKey + KEYS_PER_ROUND);
                lakeSplits = readableLakeSplitsPerBucket(tablePath);
                if (lakeSplits.size() == BUCKETS
                        && lakeSplits.values().stream().allMatch(n -> n >= FILES_PER_BUCKET)) {
                    break;
                }
            }
        } finally {
            tiering.cancel().get();
        }
        assertThat(lakeSplits.values())
                .as("lake splits per bucket of %s: %s", tablePath, lakeSplits)
                .hasSize(BUCKETS)
                .allMatch(n -> n >= FILES_PER_BUCKET);
        // a log tail, whose keys' lake versions the masks hide
        List<Integer> keys = new ArrayList<>(rows.keySet());
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (int i = 0; i < keys.size(); i += 11) {
                writer.upsert(row(keys.get(i), "u_" + keys.get(i)));
                rows.put(keys.get(i), "u_" + keys.get(i));
            }
            for (int i = 5; i < keys.size(); i += 13) {
                writer.delete(row(keys.get(i), "x"));
                rows.remove(keys.get(i));
            }
            writer.flush();
        }
        return rows;
    }

    /**
     * Writes rows of new keys until the readable snapshot covers the log of every bucket as it is
     * now. A Paimon table with deletion vectors is readable once a compaction covered its writes.
     */
    private void tickUntilReadable(
            TablePath tablePath, long tableId, Map<Integer, String> rows, int firstTickKey)
            throws Exception {
        Map<TableBucket, Long> written = new HashMap<>();
        for (int b = 0; b < BUCKETS; b++) {
            TableBucket tb = new TableBucket(tableId, b);
            written.put(tb, getLeaderReplica(tb).getLogHighWatermark());
        }
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(3).toMillis();
        int key = firstTickKey;
        while (System.currentTimeMillis() < deadline) {
            if (readableCovers(tablePath, written)) {
                return;
            }
            key++;
            writeRows(tablePath, java.util.Collections.singletonList(row(key, "tick")), false);
            rows.put(key, "tick");
            Thread.sleep(1000);
        }
        throw new AssertionError("writes of " + tablePath + " not readable in time");
    }

    /** The lake splits of each bucket in the readable snapshot, as a union read plans them. */
    private Map<Integer, Integer> readableLakeSplitsPerBucket(TablePath tablePath)
            throws Exception {
        long snapshotId = admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId();
        LakeSource<LakeSplit> lakeSource =
                LakeSourceUtils.createLakeSource(
                        tablePath, admin.getTableInfo(tablePath).get().getProperties().toMap());
        Map<Integer, Integer> splits = new TreeMap<>();
        for (LakeSplit split :
                lakeSource.createPlanner((LakeSource.PlannerContext) () -> snapshotId).plan()) {
            splits.merge(split.bucket(), 1, Integer::sum);
        }
        return splits;
    }

    private boolean readableCovers(TablePath tablePath, Map<TableBucket, Long> offsets) {
        Map<TableBucket, Long> readable;
        try {
            readable = admin.getReadableLakeSnapshot(tablePath).get().getTableBucketsOffset();
        } catch (Exception e) {
            return false; // nothing readable yet
        }
        for (Map.Entry<TableBucket, Long> e : offsets.entrySet()) {
            if (readable.getOrDefault(e.getKey(), 0L) < e.getValue()) {
                return false;
            }
        }
        return true;
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
                        .property(ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION.key(), "true")
                        // a split per data file, so a bucket's files make several groups
                        .customProperty("paimon.source.split.target-size", "1 b")
                        // Compactions only lift new files out of level 0. They do not merge sorted
                        // runs for their number or their sizes, as a Paimon compaction otherwise
                        // does when one round's file is no smaller than the last one.
                        .customProperty("paimon.num-sorted-run.compaction-trigger", "100")
                        .customProperty("paimon.compaction.size-ratio", "-100")
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
