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
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.dv.DerivedDvManager;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.DvChangeListHandle;
import org.apache.fluss.server.replica.Replica;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A new leader of a bucket with deletion vectors rebuilds its change list from the one its old
 * leader saved with the KV snapshot it restores, and replays the log only from the snapshot's
 * offset, while tiering stalls. Union reads stay exact in every case, also when the new leader
 * cannot use the saved change list and replays the whole log since the readable snapshot.
 */
class FlinkDerivedDvChangeListCheckpointITCase extends FlinkDerivedDvTestBase {

    private static final Pattern FIELD = Pattern.compile("(\\w+)=(\\S+)");
    // keys written again when a KV snapshot's change list was late, so the next snapshot has data
    private static final int LATE_KEYS = 1_000;

    @Test
    void testNewLeadersRebuildFromTheCheckpointWhileTieringStalls() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "clckpt_used");
        long tableId = createDerivedTable(tablePath, 3, false);
        Map<Integer, String> expected = new TreeMap<>();
        long readable = tierThenStall(tablePath, tableId, expected);

        // after the readable snapshot: updates of tiered rows, deletes, new keys
        upsert(tablePath, expected, 0, 20, "b");
        delete(tablePath, expected, 20, 30);
        upsert(tablePath, expected, 60, 80, "c");
        Map<TableBucket, Long> x = snapshotWithChangeLists(tablePath, tableId, expected);
        // after the KV snapshot: the keys inserted before it, a deleted key again, a tiered one
        upsert(tablePath, expected, 60, 70, "d");
        upsert(tablePath, expected, 20, 22, "e");
        upsert(tablePath, expected, 40, 45, "f");

        moveLeaders(tableId, new org.apache.fluss.config.Configuration());

        for (TableBucket tb : buckets(tableId)) {
            Map<String, String> fields = rebuildFields(tb);
            assertThat(fields).containsEntry("ckpt", "used");
            assertThat(Long.parseLong(fields.get("replay_from")))
                    .isEqualTo(Long.parseLong(fields.get("ckpt_end")))
                    .isGreaterThanOrEqualTo(x.get(tb));
        }
        assertUnionRead(tablePath, expected);
        assertThat(admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId())
                .isEqualTo(readable);

        // the new leaders save their change lists, and the next leaders rebuild from those
        upsert(tablePath, expected, 0, 5, "g");
        Map<TableBucket, Long> x2 = snapshotWithChangeLists(tablePath, tableId, expected);
        upsert(tablePath, expected, 60, 65, "h");
        delete(tablePath, expected, 0, 2);
        moveLeaders(tableId, new org.apache.fluss.config.Configuration());

        for (TableBucket tb : buckets(tableId)) {
            Map<String, String> fields = rebuildFields(tb);
            assertThat(fields).containsEntry("ckpt", "used");
            assertThat(Long.parseLong(fields.get("ckpt_end"))).isGreaterThanOrEqualTo(x2.get(tb));
        }
        assertUnionRead(tablePath, expected);
    }

    @Test
    void testWithCheckpointsOffNewLeadersReplayTheWholeLog() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "clckpt_off");
        long tableId = createDerivedTable(tablePath, 3, false);
        Map<Integer, String> expected = new TreeMap<>();
        tierThenStall(tablePath, tableId, expected);
        upsert(tablePath, expected, 0, 20, "b");
        delete(tablePath, expected, 20, 25);
        snapshotWithChangeLists(tablePath, tableId, expected);
        upsert(tablePath, expected, 10, 30, "c");

        org.apache.fluss.config.Configuration off = new org.apache.fluss.config.Configuration();
        off.set(ConfigOptions.KV_DELETION_VECTORS_CHANGE_LIST_CHECKPOINT_ENABLED, false);
        try {
            restartAll(off);
            moveLeaders(tableId, off);
            for (TableBucket tb : buckets(tableId)) {
                Map<String, String> fields = rebuildFields(tb);
                assertThat(fields).containsEntry("ckpt", "off");
            }
            assertUnionRead(tablePath, expected);
        } finally {
            restartAll(new org.apache.fluss.config.Configuration());
        }
    }

    @Test
    void testUnreadableCheckpointsFallBackToTheFullReplay() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "clckpt_unreadable");
        long tableId = createDerivedTable(tablePath, 3, false);
        Map<Integer, String> expected = new TreeMap<>();
        tierThenStall(tablePath, tableId, expected);
        upsert(tablePath, expected, 0, 40, "b");
        delete(tablePath, expected, 40, 45);
        Map<TableBucket, CompletedSnapshot> snapshots =
                snapshotsWithChangeLists(tablePath, tableId, expected);
        upsert(tablePath, expected, 20, 30, "c");
        // bucket 0's saved change list is corrupt, bucket 1's is gone, bucket 2's is intact
        File corrupt = firstSegment(snapshots.get(new TableBucket(tableId, 0)));
        byte[] bytes = Files.readAllBytes(corrupt.toPath());
        bytes[bytes.length / 2] ^= 0x10;
        Files.write(corrupt.toPath(), bytes);
        Files.delete(firstSegment(snapshots.get(new TableBucket(tableId, 1))).toPath());

        moveLeaders(tableId, new org.apache.fluss.config.Configuration());

        assertThat(rebuildFields(new TableBucket(tableId, 0))).containsEntry("ckpt", "corrupt");
        assertThat(rebuildFields(new TableBucket(tableId, 1))).containsEntry("ckpt", "missing");
        assertThat(rebuildFields(new TableBucket(tableId, 2))).containsEntry("ckpt", "used");
        assertUnionRead(tablePath, expected);
    }

    @Test
    void testCheckpointOfAnotherLakeSnapshot() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "clckpt_other_snapshot");
        long tableId = createDerivedTable(tablePath, 3, false);
        Map<Integer, String> expected = new TreeMap<>();
        tierThenStall(tablePath, tableId, expected);
        upsert(tablePath, expected, 0, 30, "b");
        snapshotWithChangeLists(tablePath, tableId, expected);

        // tiering catches up to the KV snapshot's offsets: the new leaders' readable offsets are
        // the KV snapshot's, and the saved change lists hold nothing after them
        tierAllThenStall(tablePath, tableId);
        upsert(tablePath, expected, 0, 10, "c");
        moveLeaders(tableId, new org.apache.fluss.config.Configuration());
        for (TableBucket tb : buckets(tableId)) {
            Map<String, String> fields = rebuildFields(tb);
            assertThat(fields).containsEntry("ckpt", "used").containsEntry("ckpt_changes", "0");
        }
        assertUnionRead(tablePath, expected);

        // tiering gets past the KV snapshot: the lake is ahead, and the leaders replay from it
        upsert(tablePath, expected, 10, 20, "d");
        tierAllThenStall(tablePath, tableId);
        upsert(tablePath, expected, 15, 25, "e");
        moveLeaders(tableId, new org.apache.fluss.config.Configuration());
        for (TableBucket tb : buckets(tableId)) {
            assertThat(rebuildFields(tb).get("ckpt")).isIn("lake_ahead", "used");
        }
        assertUnionRead(tablePath, expected);
    }

    @Test
    void testSnapshotsSubsumedOneByOneKeepTheSegmentsTheLatestReferences() throws Exception {
        // the coordinator retains one KV snapshot per bucket: each commit subsumes the one before
        org.apache.fluss.config.Configuration retainOne =
                new org.apache.fluss.config.Configuration();
        retainOne.set(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, 1);
        FLUSS_CLUSTER_EXTENSION.restartCoordinatorServer(retainOne);
        try {
            TablePath tablePath = TablePath.of(DEFAULT_DB, "clckpt_retain_one");
            long tableId = createDerivedTable(tablePath, 3, false);
            Map<Integer, String> expected = new TreeMap<>();
            tierThenStall(tablePath, tableId, expected);

            upsert(tablePath, expected, 0, 20, "b");
            Map<TableBucket, CompletedSnapshot> s1 =
                    snapshotsWithChangeLists(tablePath, tableId, expected);
            upsert(tablePath, expected, 20, 40, "c");
            delete(tablePath, expected, 40, 45);
            Map<TableBucket, CompletedSnapshot> s2 =
                    snapshotsWithChangeLists(tablePath, tableId, expected);
            for (TableBucket tb : buckets(tableId)) {
                // snapshot 2 references the segment snapshot 1 wrote, which outlives snapshot 1
                assertThat(segmentPaths(s2.get(tb))).startsWith(segmentPaths(s1.get(tb)).get(0));
                assertThat(firstSegment(s1.get(tb))).exists();
            }

            // the new leaders rebuild from snapshot 2
            upsert(tablePath, expected, 10, 15, "d");
            moveLeaders(tableId, new org.apache.fluss.config.Configuration());
            for (TableBucket tb : buckets(tableId)) {
                assertThat(rebuildFields(tb)).containsEntry("ckpt", "used");
            }
            assertUnionRead(tablePath, expected);

            // their first snapshots reference the restored segments, then subsume snapshot 2
            upsert(tablePath, expected, 50, 60, "e");
            Map<TableBucket, CompletedSnapshot> s3 =
                    snapshotsWithChangeLists(tablePath, tableId, expected);
            for (TableBucket tb : buckets(tableId)) {
                assertThat(segmentPaths(s3.get(tb))).startsWith(segmentPaths(s2.get(tb)).get(0));
                assertThat(firstSegment(s1.get(tb))).exists();
            }
            moveLeaders(tableId, new org.apache.fluss.config.Configuration());
            for (TableBucket tb : buckets(tableId)) {
                assertThat(rebuildFields(tb)).containsEntry("ckpt", "used");
            }
            assertUnionRead(tablePath, expected);

            // tiering passes the segments: the next snapshots leave them out, and the coordinator
            // deletes them with the snapshots it subsumes
            tierAllThenStall(tablePath, tableId);
            assertUnionRead(tablePath, expected);
            upsert(tablePath, expected, 0, 5, "f");
            Map<TableBucket, CompletedSnapshot> s4 =
                    snapshotsWithChangeLists(tablePath, tableId, expected);
            for (TableBucket tb : buckets(tableId)) {
                String first = segmentPaths(s1.get(tb)).get(0);
                assertThat(segmentPaths(s4.get(tb))).doesNotContain(first);
                File gone = firstSegment(s1.get(tb));
                retry(Duration.ofMinutes(1), () -> assertThat(gone).doesNotExist());
            }
            upsert(tablePath, expected, 5, 8, "g");
            moveLeaders(tableId, new org.apache.fluss.config.Configuration());
            for (TableBucket tb : buckets(tableId)) {
                assertThat(rebuildFields(tb)).containsEntry("ckpt", "used");
            }
            assertUnionRead(tablePath, expected);
        } finally {
            FLUSS_CLUSTER_EXTENSION.restartCoordinatorServer(
                    new org.apache.fluss.config.Configuration());
            reconnect();
        }
    }

    // ---------------------------------------------------------------------------------------------

    /** Writes and tiers a first version of keys 0..60, then stops tiering; returns the snapshot. */
    private long tierThenStall(TablePath tablePath, long tableId, Map<Integer, String> expected)
            throws Exception {
        upsert(tablePath, expected, 0, 60, "a");
        return tierAllThenStall(tablePath, tableId);
    }

    private long tierAllThenStall(TablePath tablePath, long tableId) throws Exception {
        // batch reads leave execEnv in batch mode; tiering is a streaming job
        StreamExecutionEnvironment tieringEnv =
                StreamExecutionEnvironment.getExecutionEnvironment();
        tieringEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        tieringEnv.setParallelism(2);
        JobClient tiering = buildTieringJob(tieringEnv);
        try {
            return waitUntilAllTiered(tablePath, tableId);
        } finally {
            tiering.cancel().get();
        }
    }

    /** Takes a KV snapshot of every bucket, each with its change list; returns their offsets. */
    private Map<TableBucket, Long> snapshotWithChangeLists(
            TablePath tablePath, long tableId, Map<Integer, String> expected) throws Exception {
        Map<TableBucket, Long> offsets = new HashMap<>();
        for (Map.Entry<TableBucket, CompletedSnapshot> e :
                snapshotsWithChangeLists(tablePath, tableId, expected).entrySet()) {
            offsets.put(e.getKey(), e.getValue().getLogOffset());
        }
        return offsets;
    }

    /**
     * Takes a KV snapshot of every bucket, each with its whole change list. A snapshot whose change
     * list was not written within the bound a snapshot waits for it keeps only the segments of the
     * one before, and the next snapshot carries the late one: then some rows are written again, so
     * the bucket has new data, and the next snapshot is taken.
     */
    private Map<TableBucket, CompletedSnapshot> snapshotsWithChangeLists(
            TablePath tablePath, long tableId, Map<Integer, String> expected) throws Exception {
        Map<TableBucket, CompletedSnapshot> snapshots = new HashMap<>();
        for (TableBucket tb : buckets(tableId)) {
            for (int attempt = 0; ; attempt++) {
                CompletedSnapshot snapshot = FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tb);
                DvChangeListHandle changeList = snapshot.getKvSnapshotHandle().getDvChangeList();
                if (changeList != null && changeList.end() == snapshot.getLogOffset()) {
                    snapshots.put(tb, snapshot);
                    break;
                }
                assertThat(attempt)
                        .as("KV snapshots of %s without their whole change list", tb)
                        .isLessThan(5);
                upsert(tablePath, expected, LATE_KEYS, LATE_KEYS + 30, "late" + attempt);
            }
        }
        return snapshots;
    }

    private static List<String> segmentPaths(CompletedSnapshot snapshot) {
        List<String> paths = new ArrayList<>();
        for (DvChangeListHandle.Segment s :
                snapshot.getKvSnapshotHandle().getDvChangeList().segments()) {
            paths.add(s.file().getKvFileHandle().getFilePath());
        }
        return paths;
    }

    private static File firstSegment(CompletedSnapshot snapshot) {
        DvChangeListHandle changeList = snapshot.getKvSnapshotHandle().getDvChangeList();
        String path = changeList.segments().get(0).file().getKvFileHandle().getFilePath();
        return new File(new FsPath(path).toUri().getPath());
    }

    private void delete(TablePath tablePath, Map<Integer, String> expected, int from, int to)
            throws Exception {
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (int key = from; key < to; key++) {
                writer.delete(rowOf(key, "", null));
                expected.remove(key);
            }
            writer.flush();
        }
    }

    private static List<TableBucket> buckets(long tableId) {
        List<TableBucket> out = new ArrayList<>();
        for (int b = 0; b < BUCKETS; b++) {
            out.add(new TableBucket(tableId, b));
        }
        return out;
    }

    /** Restarts the leaders of the table's buckets, so their buckets get new leaders. */
    private void moveLeaders(long tableId, org.apache.fluss.config.Configuration conf)
            throws Exception {
        Set<Integer> leaders = new HashSet<>();
        for (TableBucket tb : buckets(tableId)) {
            leaders.add(FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb));
        }
        for (int leader : leaders) {
            FLUSS_CLUSTER_EXTENSION.restartTabletServer(leader, conf);
        }
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);
        reconnect();
    }

    private void restartAll(org.apache.fluss.config.Configuration conf) throws Exception {
        List<Integer> servers = new ArrayList<>();
        for (org.apache.fluss.cluster.ServerNode node :
                FLUSS_CLUSTER_EXTENSION.getTabletServerNodes()) {
            servers.add(node.id());
        }
        for (int server : servers) {
            FLUSS_CLUSTER_EXTENSION.restartTabletServer(server, conf);
        }
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);
        reconnect();
    }

    // the shared client may still know the restarted servers' old endpoints
    private void reconnect() throws Exception {
        admin.close();
        conn.close();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    /** The change-list fields of the rebuild of {@code tb}'s current leader, once it is done. */
    private Map<String, String> rebuildFields(TableBucket tb) {
        String line =
                waitValue(
                        () -> {
                            Replica leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tb);
                            KvTablet kv = leader.getKvTablet();
                            DerivedDvManager dv = kv == null ? null : kv.getDvManager();
                            String fields = dv == null ? "" : dv.lastRebuildCheckpointFields();
                            return fields.isEmpty()
                                    ? java.util.Optional.<String>empty()
                                    : java.util.Optional.of(fields);
                        },
                        Duration.ofMinutes(2),
                        "no rebuild of " + tb);
        Map<String, String> fields = new HashMap<>();
        Matcher m = FIELD.matcher(line);
        while (m.find()) {
            fields.put(m.group(1), m.group(2));
        }
        return fields;
    }

    private void assertUnionRead(TablePath tablePath, Map<Integer, String> expected)
            throws Exception {
        List<String> rows = new ArrayList<>();
        for (Map.Entry<Integer, String> e : expected.entrySet()) {
            rows.add(rowString(e.getKey(), e.getValue(), null));
        }
        CloseableIterator<Row> it =
                batchTEnv.executeSql("select * from " + tablePath.getTableName()).collect();
        assertResultsIgnoreOrder(it, rows, true);
    }
}
