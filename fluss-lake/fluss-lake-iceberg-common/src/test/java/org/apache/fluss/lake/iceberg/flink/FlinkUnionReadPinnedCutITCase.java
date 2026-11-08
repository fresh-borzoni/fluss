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
import org.apache.fluss.client.admin.DvAdmin;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.source.testutils.UnionReadUnderWrites;
import org.apache.fluss.flink.utils.LogCapture;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.messages.GetDvSnapshotRequest;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.server.kv.dv.DerivedDvManager;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.fluss.flink.source.testutils.UnionReadUnderWrites.DV_READ_LOGGER;
import static org.apache.fluss.flink.source.testutils.UnionReadUnderWrites.FILE_GROUP_ROWS;
import static org.apache.fluss.flink.source.testutils.UnionReadUnderWrites.everyNth;
import static org.apache.fluss.flink.source.testutils.UnionReadUnderWrites.partsReadPerBucket;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests union reads of Iceberg tables with deletion vectors whose buckets are read by several
 * parts, groups of lake files and the log, all as of the cut planning pinned on the bucket's
 * leader.
 */
class FlinkUnionReadPinnedCutITCase extends FlinkUnionReadTestBase {

    private static final int BUCKETS = 3;
    private static final int ROUNDS = 3;
    private static final int KEYS_PER_ROUND = 300;
    private static final String IDLE_TIMEOUT = "kv.deletion-vectors.pinned-cut-idle-timeout";
    // the lake snapshot a part is read on, in the message it logs when it starts
    private static final Pattern READ_SNAPSHOT =
            Pattern.compile("^Reading .* as (?:MASKED|FOLDED) \\(snapshot (-?\\d+),");

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
     * Reads while tiering keeps registering snapshots, so leaders switch during the read. A read
     * that does not wait may read a bucket whose leader still covers the snapshot before on that
     * one; both reads are exact.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testReadWhileTieringRegistersSnapshots(boolean waitForCatchUp) throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "pinned_parts_tiering_" + waitForCatchUp);
        long tableId = createDerivedTable(tablePath);
        Map<Integer, String> before = tierInRounds(tablePath, tableId);

        JobClient tiering = buildTieringJob(execEnv);
        try (LogCapture log = LogCapture.of(DV_READ_LOGGER)) {
            new UnionReadUnderWrites(clientConf, tablePath)
                    .withHint(FILE_GROUP_ROWS, "1")
                    .withHint(
                            ConfigOptions.CLIENT_SCANNER_DELETION_VECTORS_WAIT_FOR_CATCH_UP.key(),
                            String.valueOf(waitForCatchUp))
                    .withMillisPerRow(2)
                    .readAndCheck(before, everyNth(before, 7), Duration.ofMinutes(5));
            if (waitForCatchUp) {
                assertThat(log.messagesWith("while it switches to it")).isEmpty();
            }
        } finally {
            tiering.cancel().get();
        }
    }

    /**
     * A read that does not wait, planned on a snapshot its buckets' leaders do not cover yet. Their
     * switches are held while tiering registers the snapshot, so each leader still covers the one
     * before: it answers at once that it is behind, and the read reads every bucket on the snapshot
     * before, exactly. Once the switches go on, a read uses the new snapshot.
     */
    @Test
    void testReadThatDoesNotWaitWhileLeadersSwitch() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "pinned_parts_switching");
        long tableId = createDerivedTable(tablePath);
        Map<Integer, String> before = tierInRounds(tablePath, tableId);
        long previous = admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId();
        waitUntilLeadersCover(tableId, previous);

        CountDownLatch switches = new CountDownLatch(1);
        FLUSS_CLUSTER_EXTENSION.holdDvSwitches(switches);
        long next;
        Map<Integer, Set<String>> written;
        try {
            // a round tiers the log tail into a new snapshot, which the leaders do not switch to
            JobClient tiering = buildTieringJob(execEnv);
            try {
                next = waitUntilAllTiered(tablePath, tableId);
            } finally {
                tiering.cancel().get();
            }
            assertThat(next).isNotEqualTo(previous);
            for (int bucket = 0; bucket < BUCKETS; bucket++) {
                GetDvSnapshotResponse answer = noWaitAnswer(tablePath, tableId, bucket, next);
                assertThat(answer.hasBehind() && answer.isBehind()).isTrue();
                assertThat(answer.getCoveredSnapshotId()).isEqualTo(previous);
                assertThat(answer.hasLogEndOffset()).isFalse();
            }

            try (LogCapture log = LogCapture.of(DV_READ_LOGGER)) {
                written =
                        noWaitRead(tablePath)
                                .readAndCheck(before, everyNth(before, 7), Duration.ofMinutes(5));
                // planned on the new snapshot, each bucket was read on the one before
                assertThat(
                                log.messagesWith(
                                        "snapshot " + next + " while it switches to it",
                                        "reading snapshot " + previous + " instead"))
                        .hasSize(BUCKETS);
                assertThat(snapshotsRead(log.messages())).containsOnly(previous);
            }
        } finally {
            FLUSS_CLUSTER_EXTENSION.holdDvSwitches(null);
            switches.countDown();
        }

        waitUntilLeadersCover(tableId, next);
        Map<Integer, String> after = lastValues(before, written);
        try (LogCapture log = LogCapture.of(DV_READ_LOGGER)) {
            noWaitRead(tablePath).readAndCheck(after, everyNth(after, 7), Duration.ofMinutes(5));
            assertThat(log.messagesWith("while it switches to it")).isEmpty();
            assertThat(snapshotsRead(log.messages())).containsOnly(next);
        }
    }

    /** The table as writes left it: each key's last value written, numbered by the writes. */
    private static Map<Integer, String> lastValues(
            Map<Integer, String> before, Map<Integer, Set<String>> written) {
        Map<Integer, String> after = new TreeMap<>(before);
        written.forEach(
                (key, values) ->
                        after.put(
                                key,
                                values.stream()
                                        .max(
                                                (a, b) ->
                                                        Integer.compare(
                                                                Integer.parseInt(a.substring(1)),
                                                                Integer.parseInt(b.substring(1))))
                                        .get()));
        return after;
    }

    private UnionReadUnderWrites noWaitRead(TablePath tablePath) {
        return new UnionReadUnderWrites(clientConf, tablePath)
                .withHint(FILE_GROUP_ROWS, "1")
                .withHint(
                        ConfigOptions.CLIENT_SCANNER_DELETION_VECTORS_WAIT_FOR_CATCH_UP.key(),
                        "false");
    }

    /**
     * The answer of the bucket's leader to a pin of {@code snapshotId} that does not wait, sent
     * over the RPC as a reader sends it. Waits while the leader has not seen the registration yet.
     */
    private GetDvSnapshotResponse noWaitAnswer(
            TablePath tablePath, long tableId, int bucket, long snapshotId) throws Exception {
        long deadline = System.nanoTime() + Duration.ofMinutes(1).toNanos();
        while (true) {
            try {
                return DvAdmin.of(admin)
                        .getDvSnapshot(
                                tablePath,
                                new GetDvSnapshotRequest()
                                        .setTableId(tableId)
                                        .setBucketId(bucket)
                                        .setReadableSnapshotId(snapshotId)
                                        .setWaitForCatchUp(false))
                        .get();
            } catch (Exception e) {
                // not ready while the leader does not know the snapshot yet
                if (System.nanoTime() > deadline) {
                    throw new AssertionError(
                            "no answer of the leader of bucket " + bucket + " to " + snapshotId, e);
                }
                Thread.sleep(100);
            }
        }
    }

    private void waitUntilLeadersCover(long tableId, long snapshotId) {
        waitUntil(
                () -> {
                    for (int b = 0; b < BUCKETS; b++) {
                        DerivedDvManager dv =
                                getLeaderReplica(new TableBucket(tableId, b))
                                        .getKvTablet()
                                        .getDvManager();
                        if (dv.getReadableSnapshotId() != snapshotId) {
                            return false;
                        }
                    }
                    return true;
                },
                Duration.ofMinutes(2),
                "the leaders do not cover snapshot " + snapshotId);
    }

    /** The lake snapshots the parts of a read were read on, from the read's log messages. */
    private static Set<Long> snapshotsRead(List<String> messages) {
        Set<Long> snapshots = new TreeSet<>();
        for (String message : messages) {
            Matcher read = READ_SNAPSHOT.matcher(message);
            if (read.find()) {
                snapshots.add(Long.parseLong(read.group(1)));
            }
        }
        return snapshots;
    }

    /**
     * A read that does not wait, while the buckets' new leaders rebuild after the tablet servers
     * restarted, reads the buckets by key at once, exactly, while writes go on. A read that waits,
     * once the leaders rebuilt, reads with their masks.
     */
    @Test
    void testReadThatDoesNotWaitWhileLeadersRebuild() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "pinned_parts_rebuilding");
        long tableId = createDerivedTable(tablePath);
        Map<Integer, String> before = tierInRounds(tablePath, tableId);

        CountDownLatch rebuilds = new CountDownLatch(1);
        FLUSS_CLUSTER_EXTENSION.holdDvRebuilds(rebuilds);
        Map<Integer, Set<String>> written;
        // the lines of part reads and quick mask requests are logged at DEBUG
        try (LogCapture read = LogCapture.ofDebug(DV_READ_LOGGER);
                LogCapture leaders =
                        LogCapture.ofDebug(DerivedDvManager.class.getPackage().getName())) {
            restartTabletServers(new Configuration());
            written =
                    new UnionReadUnderWrites(clientConf, tablePath)
                            .withHint(FILE_GROUP_ROWS, "1")
                            .withHint(
                                    ConfigOptions.CLIENT_SCANNER_DELETION_VECTORS_WAIT_FOR_CATCH_UP
                                            .key(),
                                    "false")
                            .readAndCheck(before, everyNth(before, 7), Duration.ofMinutes(5));

            assertThat(read.messagesWith("Read a deletion vector union read part", "mode=FOLDED"))
                    .isNotEmpty();
            assertThat(read.messagesWith("Read a deletion vector union read part", "mode=MASKED"))
                    .isEmpty();
            assertThat(
                            leaders.messagesWith(
                                    "Served the deletion vector masks of",
                                    "result=rebuilding",
                                    "cause=rebuild"))
                    .isNotEmpty();
        } finally {
            FLUSS_CLUSTER_EXTENSION.holdDvRebuilds(null);
            rebuilds.countDown();
        }

        // the table as the writes left it: each key's last value written
        Map<Integer, String> after = new TreeMap<>(before);
        written.forEach(
                (key, values) ->
                        after.put(
                                key,
                                values.stream()
                                        .max(
                                                (a, b) ->
                                                        Integer.compare(
                                                                Integer.parseInt(a.substring(1)),
                                                                Integer.parseInt(b.substring(1))))
                                        .get()));
        try (LogCapture read = LogCapture.ofDebug(DV_READ_LOGGER)) {
            new UnionReadUnderWrites(clientConf, tablePath)
                    .withHint(FILE_GROUP_ROWS, "1")
                    .readAndCheck(after, everyNth(after, 7), Duration.ofMinutes(5));
            assertThat(read.messagesWith("Read a deletion vector union read part", "mode=FOLDED"))
                    .isEmpty();
            assertThat(read.messagesWith("Read a deletion vector union read part", "mode=MASKED"))
                    .isNotEmpty();
        }
    }

    /**
     * A read releases the cuts it pinned on the buckets' leaders when it ends, instead of leaving
     * them to the idle timeout: when it finishes, and when it is cancelled.
     */
    @Test
    void testCutsAreReleasedWhenTheReadEnds() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "pinned_parts_released");
        long tableId = createDerivedTable(tablePath);
        Map<Integer, String> before = tierInRounds(tablePath, tableId);
        assertThat(pinnedCuts(tableId)).isZero();

        new UnionReadUnderWrites(clientConf, tablePath)
                .withHint(FILE_GROUP_ROWS, "1")
                .readAndCheck(before, everyNth(before, 7), Duration.ofMinutes(5));
        waitUntil(
                () -> pinnedCuts(tableId) == 0,
                Duration.ofSeconds(30),
                "the cuts of the finished read are still held");

        // a slow read holds its cuts until it is cancelled
        JobClient read =
                new UnionReadUnderWrites(clientConf, tablePath)
                        .withHint(FILE_GROUP_ROWS, "1")
                        .withMillisPerRow(100)
                        .startRead();
        try {
            waitUntil(
                    () -> pinnedCuts(tableId) > 0,
                    Duration.ofMinutes(1),
                    "the read pinned no cuts");
        } finally {
            read.cancel().get();
        }
        waitUntil(
                () -> pinnedCuts(tableId) == 0,
                Duration.ofSeconds(30),
                "the cuts of the cancelled read are still held");
    }

    /** The cuts the table's bucket leaders hold. */
    private int pinnedCuts(long tableId) {
        int cuts = 0;
        for (int b = 0; b < BUCKETS; b++) {
            cuts +=
                    getLeaderReplica(new TableBucket(tableId, b))
                            .getKvTablet()
                            .getDvManager()
                            .pinnedCutCount();
        }
        return cuts;
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
