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

package org.apache.fluss.lake.paimon.flink;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.flink.utils.LogCapture;
import org.apache.fluss.lake.paimon.tiering.HandOffFailingTieringFactory;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.FlussPaths;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.fluss.lake.paimon.utils.PaimonTestUtils.CompactHelper;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Union reads of Paimon tables with deletion vector masks ({@code table.deletion-vectors.enabled =
 * true}).
 *
 * <p>The Paimon table uses its own deletion vectors and the readable snapshot is the latest
 * compacted one. Rows updated or deleted after it must not surface through the union read.
 */
class FlinkUnionReadDerivedDvITCase extends FlinkUnionReadTestBase {

    private static final int BUCKETS = 3;

    @TempDir private File compactionTempDir;

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
        // The tiering job writes the position index under the remote data dir; it takes its Fluss
        // config from clientConf, which has no remote dir by default.
        clientConf.set(ConfigOptions.REMOTE_DATA_DIR, FLUSS_CLUSTER_EXTENSION.getRemoteDataDir());
    }

    @Override
    protected org.apache.fluss.config.Configuration defaultLakeTieringConfig() {
        org.apache.fluss.config.Configuration conf = super.defaultLakeTieringConfig();
        // Re-index each newly readable file in its own split, so a bucket's files take several.
        conf.set(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_REINDEX_FILES_PER_SPLIT, 1);
        return conf;
    }

    @Override
    protected LakeTieringFactory<?, ?> lakeTieringFactory() {
        // lets a test fail the hand-offs of the compactions
        return new HandOffFailingTieringFactory(
                org.apache.fluss.config.Configuration.fromMap(getPaimonCatalogConf()));
    }

    /**
     * Tiering compacts every round, so each round's COMPACT snapshot is readable at its tiered
     * offsets and indexed before it is registered.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUpdatesAndDeletesAcrossRoundsWithAutoCompaction(boolean partitioned) throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName = "derived_auto" + (partitioned ? "_part" : "");
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId = createDerivedTable(tablePath, partitioned, true);
            List<String> parts =
                    partitioned ? Arrays.asList("p0", "p1") : Collections.singletonList(null);
            Map<String, String> expected = new TreeMap<>();

            // round 1: keys 0..9
            List<InternalRow> rows = new ArrayList<>();
            for (String p : parts) {
                for (int i = 0; i < 10; i++) {
                    rows.add(rowOf(i, "v_" + i, p));
                    expected.put(rowKey(i, p), rowString(i, "v_" + i, p));
                }
            }
            writeRows(tablePath, rows, false);
            long readable = tickUntilReadableAdvances(tablePath, Long.MIN_VALUE, parts.get(0));
            expected.put(rowKey(-1, parts.get(0)), rowString(-1, "tick", parts.get(0)));
            assertUnionRead(tableName, expected);

            // round 2: update 0..2, delete 3..4, delete and re-insert 5
            for (int round = 0; round < 2; round++) {
                try (Table table = conn.getTable(tablePath)) {
                    UpsertWriter writer = table.newUpsert().createWriter();
                    for (String p : parts) {
                        for (int i = 0; i < 3; i++) {
                            String value = "updated_" + round + "_" + i;
                            writer.upsert(rowOf(i, value, p));
                            expected.put(rowKey(i, p), rowString(i, value, p));
                        }
                        writer.delete(rowOf(3 + round * 3, "x", p));
                        writer.delete(rowOf(4 + round * 3, "x", p));
                        expected.remove(rowKey(3 + round * 3, p));
                        expected.remove(rowKey(4 + round * 3, p));
                        writer.delete(rowOf(5, "x", p));
                        writer.upsert(rowOf(5, "reinserted_" + round, p));
                        expected.put(rowKey(5, p), rowString(5, "reinserted_" + round, p));
                    }
                    writer.flush();
                }
                // Before the round is readable from the lake, the masks hide the old versions.
                assertUnionRead(tableName, expected);
                readable = tickUntilReadableAdvances(tablePath, readable, parts.get(0));
                assertUnionRead(tableName, expected);
            }
            // Once all is compacted, the lake on its own holds the same rows.
            waitUntilAllReadable(tablePath, tableId, partitioned);
            assertUnionRead(tableName, expected);
            assertLakeRead(tableName, partitioned, expected);
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * Tiering compacts every round, and the compactions in the tiering writers hand the positions
     * of the rows they wrote to the index. Each round then registers its readable snapshot without
     * holding for a re-index of the compacted files. A round whose hand-off fails re-indexes them,
     * as for a compaction by another engine. Both keep union and lake reads exact.
     */
    @Test
    void testCompactionsInTheTieringWritersHandTheirPositionsOver() throws Exception {
        String committer = "org.apache.fluss.flink.tiering.committer.DerivedDvTieringCommitter";
        JobClient jobClient = buildTieringJob(execEnv);
        try (LogCapture log = LogCapture.of(committer)) {
            String tableName = "derived_hand_off";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId = createDerivedTable(tablePath, false, true);
            Map<String, String> expected = new TreeMap<>();
            List<InternalRow> rows = new ArrayList<>();
            for (int i = 0; i < 30; i++) {
                rows.add(rowOf(i, "v_" + i, null));
                expected.put(rowKey(i, null), rowString(i, "v_" + i, null));
            }
            writeRows(tablePath, rows, false);
            long readable = tickUntilReadableAdvances(tablePath, Long.MIN_VALUE, null);
            expected.put(rowKey(-1, null), rowString(-1, "tick", null));

            int rounds = 4;
            int[] handOffs = new int[2];
            int[] reindexes = new int[2];
            for (int failed = 0; failed < 2; failed++) {
                // the second half of the rounds fails every hand-off, so they re-index
                HandOffFailingTieringFactory.failNextHandOffs(failed == 0 ? 0 : Integer.MAX_VALUE);
                int handOffsBefore = handOffs(log);
                int reindexesBefore = reindexes(log);
                for (int round = 0; round < rounds; round++) {
                    try (Table table = conn.getTable(tablePath)) {
                        UpsertWriter writer = table.newUpsert().createWriter();
                        for (int i = round * 3; i < round * 3 + 6; i++) {
                            String value = "r" + failed + "_" + round + "_" + i;
                            writer.upsert(rowOf(i, value, null));
                            expected.put(rowKey(i, null), rowString(i, value, null));
                        }
                        int deleted = 29 - round - failed * rounds;
                        writer.delete(rowOf(deleted, "x", null));
                        expected.remove(rowKey(deleted, null));
                        writer.flush();
                    }
                    readable = tickUntilReadableAdvances(tablePath, readable, null);
                    assertUnionRead(tableName, expected);
                }
                waitUntilAllReadable(tablePath, tableId, false);
                handOffs[failed] = handOffs(log) - handOffsBefore;
                reindexes[failed] = reindexes(log) - reindexesBefore;
            }
            HandOffFailingTieringFactory.failNextHandOffs(0);
            assertThat(handOffs[0]).isGreaterThanOrEqualTo(rounds);
            assertThat(reindexes[0]).isZero();
            assertThat(handOffs[1]).isZero();
            assertThat(reindexes[1]).isGreaterThanOrEqualTo(rounds);
            assertUnionRead(tableName, expected);
            assertLakeRead(tableName, false, expected);
        } finally {
            HandOffFailingTieringFactory.failNextHandOffs(0);
            jobClient.cancel().get();
        }
    }

    /**
     * A table dropped while the tiering readers re-index its files ends only its own round. The
     * readers skip its re-index splits and the round fails in the committer. The job keeps tiering
     * the other tables.
     */
    @Test
    void testTableDroppedDuringItsReindex() throws Exception {
        TablePath droppedPath = TablePath.of(DEFAULT_DB, "derived_dropped_during_reindex");
        long droppedId = createDerivedTable(droppedPath, false, true);
        TablePath keptPath = TablePath.of(DEFAULT_DB, "derived_kept_during_reindex");
        String keptName = keptPath.getTableName();
        long keptId = createDerivedTable(keptPath, false, true);
        Map<String, String> expected = new TreeMap<>();
        List<InternalRow> rows = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            rows.add(rowOf(i, "v_" + i, null));
            expected.put(rowKey(i, null), rowString(i, "v_" + i, null));
        }
        // every compaction is re-indexed by the readers
        HandOffFailingTieringFactory.failNextHandOffs(Integer.MAX_VALUE);
        JobClient jobClient = null;
        try {
            try (LogCapture log =
                            LogCapture.of(
                                    "org.apache.fluss.flink.tiering.source.TieringSplitReader");
                    LogCapture.Pause reading =
                            log.pauseAt("add split tiering-row-pos-split-" + droppedId + "-")) {
                writeRows(droppedPath, rows, false);
                writeRows(keptPath, rows, false);
                jobClient = buildTieringJob(execEnv);
                // a reader is about to read a re-index split of the table, which is dropped now
                reading.awaitReached(Duration.ofMinutes(2));
                admin.dropTable(droppedPath, false).get();
                waitUntil(
                        () -> !admin.tableExists(droppedPath).get(),
                        Duration.ofMinutes(1),
                        Duration.ofMillis(200),
                        droppedPath + " not dropped");
            }
            long readable = tickUntilReadableAdvances(keptPath, Long.MIN_VALUE, null);
            expected.put(rowKey(-1, null), rowString(-1, "tick", null));
            try (Table table = conn.getTable(keptPath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                writer.upsert(rowOf(0, "updated_0", null));
                writer.delete(rowOf(1, "x", null));
                writer.flush();
            }
            expected.put(rowKey(0, null), rowString(0, "updated_0", null));
            expected.remove(rowKey(1, null));
            tickUntilReadableAdvances(keptPath, readable, null);
            waitUntilAllReadable(keptPath, keptId, false);
            assertUnionRead(keptName, expected);
            assertThat(jobClient.getJobStatus().get(1, TimeUnit.MINUTES))
                    .isEqualTo(JobStatus.RUNNING);
        } finally {
            HandOffFailingTieringFactory.failNextHandOffs(0);
            // a job that failed is gone with its mini cluster already
            if (jobClient != null && !jobClient.getJobExecutionResult().isDone()) {
                jobClient.cancel().get();
            }
        }
    }

    private static int handOffs(LogCapture log) {
        return log.messagesWith("in the tiering writers handed the positions").size();
    }

    private static int reindexes(LogCapture log) {
        return log.messagesWith("they are indexed on the tiering readers").size();
    }

    /**
     * A table in WAL changelog image mode reads exact, also with several changes of one key in a
     * round. Its log has no UPDATE_BEFORE records and turns upserts of new keys into UPDATE_AFTER.
     * The masks need only the keys changed after the readable offset and their last versions. The
     * compacted lake holds the same rows.
     */
    @Test
    void testWalChangelogImage() throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName = "derived_wal";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId =
                    createDerivedTable(
                            tablePath,
                            false,
                            true,
                            Collections.singletonMap(
                                    ConfigOptions.TABLE_CHANGELOG_IMAGE.key(), "WAL"));
            assertThat(admin.getTableInfo(tablePath).get().getTableConfig().getChangelogImage())
                    .isEqualTo(ChangelogImage.WAL);
            Map<String, String> expected = new TreeMap<>();
            List<InternalRow> rows = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                rows.add(rowOf(i, "v_" + i, null));
                expected.put(rowKey(i, null), rowString(i, "v_" + i, null));
            }
            writeRows(tablePath, rows, false);
            long readable = tickUntilReadableAdvances(tablePath, Long.MIN_VALUE, null);
            expected.put(rowKey(-1, null), rowString(-1, "tick", null));
            assertUnionRead(tableName, expected);

            for (int round = 0; round < 2; round++) {
                try (Table table = conn.getTable(tablePath)) {
                    UpsertWriter writer = table.newUpsert().createWriter();
                    // several updates of one key, and of a key new in this round
                    for (int n = 0; n < 3; n++) {
                        writer.upsert(rowOf(0, "updated_" + round + "_" + n, null));
                        writer.upsert(rowOf(20 + round, "new_" + round + "_" + n, null));
                    }
                    expected.put(rowKey(0, null), rowString(0, "updated_" + round + "_2", null));
                    expected.put(
                            rowKey(20 + round, null),
                            rowString(20 + round, "new_" + round + "_2", null));
                    writer.upsert(rowOf(1 + round, "updated_" + round, null));
                    expected.put(
                            rowKey(1 + round, null),
                            rowString(1 + round, "updated_" + round, null));
                    writer.delete(rowOf(3 + round, "x", null));
                    expected.remove(rowKey(3 + round, null));
                    // deleted and inserted again, and inserted and deleted again
                    writer.delete(rowOf(5 + round, "x", null));
                    writer.upsert(rowOf(5 + round, "reinserted_" + round, null));
                    expected.put(
                            rowKey(5 + round, null),
                            rowString(5 + round, "reinserted_" + round, null));
                    writer.upsert(rowOf(30 + round, "gone", null));
                    writer.delete(rowOf(30 + round, "gone", null));
                    if (round == 1) {
                        writer.delete(rowOf(20, "x", null));
                        expected.remove(rowKey(20, null));
                    }
                    writer.flush();
                }
                // before the round is readable from the lake, the masks hide the old versions
                assertUnionRead(tableName, expected);
                readable = tickUntilReadableAdvances(tablePath, readable, null);
                assertUnionRead(tableName, expected);
            }
            waitUntilAllReadable(tablePath, tableId, false);
            assertUnionRead(tableName, expected);
            assertLakeRead(tableName, false, expected);
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * Tiering only appends level-0 files and another engine compacts some buckets at a time, so the
     * readable snapshot lags the tiered offsets differently per bucket.
     */
    @Test
    void testExternalCompactionOfSomeBuckets() throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName = "derived_external";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId = createDerivedTable(tablePath, false, false);
            Map<String, String> expected = new TreeMap<>();
            List<InternalRow> rows = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                rows.add(rowOf(i, "v_" + i, null));
                expected.put(rowKey(i, null), rowString(i, "v_" + i, null));
            }
            writeRows(tablePath, rows, false);
            waitUntilTiered(tablePath, tableId);
            // Nothing is compacted, so nothing is readable: the log alone, masked.
            assertUnionRead(tableName, expected);

            FileStoreTable paimonTable =
                    (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
            CompactHelper compactHelper = new CompactHelper(paimonTable, compactionTempDir);
            for (int b = 0; b < BUCKETS; b++) {
                compactHelper.compactBucket(BinaryRow.EMPTY_ROW, b).commit();
            }
            long readable = tickUntilReadableAdvances(tablePath, Long.MIN_VALUE, null);
            expected.put(rowKey(-1, null), rowString(-1, "tick", null));
            assertUnionRead(tableName, expected);

            for (int round = 0; round < 3; round++) {
                try (Table table = conn.getTable(tablePath)) {
                    UpsertWriter writer = table.newUpsert().createWriter();
                    for (int i = round * 4; i < round * 4 + 4; i++) {
                        writer.upsert(rowOf(i, "r" + round + "_" + i, null));
                        expected.put(rowKey(i, null), rowString(i, "r" + round + "_" + i, null));
                    }
                    int deleted = 19 - round;
                    writer.delete(rowOf(deleted, "x", null));
                    expected.remove(rowKey(deleted, null));
                    writer.delete(rowOf(round * 4, "x", null));
                    writer.upsert(rowOf(round * 4, "back_" + round, null));
                    expected.put(
                            rowKey(round * 4, null), rowString(round * 4, "back_" + round, null));
                    writer.flush();
                }
                assertUnionRead(tableName, expected);
                waitUntilTiered(tablePath, tableId);
                assertUnionRead(tableName, expected);
                // Compact one bucket: the others keep level-0 files and read from older offsets.
                compactHelper.compactBucket(BinaryRow.EMPTY_ROW, round % BUCKETS).commit();
                readable = tickUntilReadableAdvances(tablePath, readable, null);
                assertUnionRead(tableName, expected);
            }
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * A restarted tablet server rebuilds the masks of its buckets from the index chain and the log
     * after the readable offsets. Tiering is stopped meanwhile, so the tail is not empty.
     */
    @Test
    void testTabletServerRestart() throws Exception {
        String tableName = "derived_restart";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        long tableId = createDerivedTable(tablePath, false, true);
        Map<String, String> expected = new TreeMap<>();
        // Writes of this test use their own connection, which the restarts leave behind.
        try (Connection writes = ConnectionFactory.createConnection(clientConf)) {
            JobClient jobClient = buildTieringJob(execEnv);
            long readable;
            try {
                List<InternalRow> rows = new ArrayList<>();
                for (int i = 0; i < 30; i++) {
                    rows.add(rowOf(i, "v_" + i, null));
                    expected.put(rowKey(i, null), rowString(i, "v_" + i, null));
                }
                upsert(writes, tablePath, rows);
                readable = tickUntilReadableAdvances(writes, tablePath, Long.MIN_VALUE, null);
                expected.put(rowKey(-1, null), rowString(-1, "tick", null));
                waitUntilAllReadable(tablePath, tableId, false);
            } finally {
                jobClient.cancel().get();
            }

            try (Table table = writes.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                for (int i = 0; i < 30; i += 3) {
                    writer.upsert(rowOf(i, "updated_" + i, null));
                    expected.put(rowKey(i, null), rowString(i, "updated_" + i, null));
                    writer.delete(rowOf(i + 1, "x", null));
                    expected.remove(rowKey(i + 1, null));
                }
                writer.delete(rowOf(2, "x", null));
                writer.upsert(rowOf(2, "reinserted_2", null));
                expected.put(rowKey(2, null), rowString(2, "reinserted_2", null));
                writer.flush();
            }
            assertUnionRead(tableName, expected);

            Set<Integer> leaders = new TreeSet<>();
            for (int b = 0; b < BUCKETS; b++) {
                leaders.add(FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(new TableBucket(tableId, b)));
            }
            for (int leader : leaders) {
                FLUSS_CLUSTER_EXTENSION.stopTabletServer(leader);
                FLUSS_CLUSTER_EXTENSION.startTabletServer(leader);
            }
            for (int b = 0; b < BUCKETS; b++) {
                FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(new TableBucket(tableId, b));
            }
            // The masks are rebuilt from the index chain and the log after the readable offsets.
            assertUnionRead(tableName, expected);

            // Tiering resumes on the restarted servers; the batch reads left execEnv in batch mode.
            StreamExecutionEnvironment tieringEnv =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            tieringEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            tieringEnv.setParallelism(2);
            jobClient = buildTieringJob(tieringEnv);
            try {
                tickUntilReadableAdvances(writes, tablePath, readable, null);
                waitUntilAllReadable(tablePath, tableId, false);
                assertUnionRead(tableName, expected);
                assertLakeRead(tableName, false, expected);
            } finally {
                jobClient.cancel().get();
            }
        }
    }

    /**
     * New leaders rebuild their buckets from the change lists their old leaders saved with the KV
     * snapshots they restore, and replay the log only from the snapshots' offsets, while tiering
     * stalls. Keys inserted before a KV snapshot and updated after it are masked in the log.
     */
    @Test
    void testTabletServerRestartFromTheChangeListCheckpoint() throws Exception {
        String tableName = "derived_restart_checkpoint";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        long tableId = createDerivedTable(tablePath, false, true);
        Map<String, String> expected = new TreeMap<>();
        try (Connection writes = ConnectionFactory.createConnection(clientConf)) {
            JobClient jobClient = buildTieringJob(execEnv);
            try {
                List<InternalRow> rows = new ArrayList<>();
                for (int i = 0; i < 30; i++) {
                    rows.add(rowOf(i, "v_" + i, null));
                    expected.put(rowKey(i, null), rowString(i, "v_" + i, null));
                }
                upsert(writes, tablePath, rows);
                tickUntilReadableAdvances(writes, tablePath, Long.MIN_VALUE, null);
                expected.put(rowKey(-1, null), rowString(-1, "tick", null));
                waitUntilAllReadable(tablePath, tableId, false);
            } finally {
                jobClient.cancel().get();
            }

            try (Table table = writes.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                for (int i = 0; i < 10; i++) {
                    writer.upsert(rowOf(i, "updated_" + i, null));
                    expected.put(rowKey(i, null), rowString(i, "updated_" + i, null));
                }
                for (int i = 40; i < 50; i++) {
                    writer.upsert(rowOf(i, "new_" + i, null));
                    expected.put(rowKey(i, null), rowString(i, "new_" + i, null));
                }
                writer.delete(rowOf(10, "x", null));
                expected.remove(rowKey(10, null));
                writer.flush();
            }
            for (int b = 0; b < BUCKETS; b++) {
                TableBucket tb = new TableBucket(tableId, b);
                for (int attempt = 0; ; attempt++) {
                    org.apache.fluss.server.kv.snapshot.CompletedSnapshot snapshot =
                            FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tb);
                    org.apache.fluss.server.kv.snapshot.DvChangeListHandle changeList =
                            snapshot.getKvSnapshotHandle().getDvChangeList();
                    if (changeList != null && changeList.end() == snapshot.getLogOffset()) {
                        break;
                    }
                    // its change list was late for it: the next snapshot carries it, once the
                    // bucket has new data
                    assertThat(attempt)
                            .as("KV snapshots of %s without their whole change list", tb)
                            .isLessThan(5);
                    try (Table table = writes.getTable(tablePath)) {
                        UpsertWriter writer = table.newUpsert().createWriter();
                        for (int i = 1000; i < 1030; i++) {
                            writer.upsert(rowOf(i, "late" + attempt, null));
                            expected.put(rowKey(i, null), rowString(i, "late" + attempt, null));
                        }
                        writer.flush();
                    }
                }
            }
            try (Table table = writes.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                // inserted before the KV snapshots, updated after them
                for (int i = 40; i < 45; i++) {
                    writer.upsert(rowOf(i, "again_" + i, null));
                    expected.put(rowKey(i, null), rowString(i, "again_" + i, null));
                }
                writer.upsert(rowOf(10, "back_10", null));
                expected.put(rowKey(10, null), rowString(10, "back_10", null));
                writer.flush();
            }

            Set<Integer> leaders = new TreeSet<>();
            for (int b = 0; b < BUCKETS; b++) {
                leaders.add(FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(new TableBucket(tableId, b)));
            }
            for (int leader : leaders) {
                FLUSS_CLUSTER_EXTENSION.stopTabletServer(leader);
                FLUSS_CLUSTER_EXTENSION.startTabletServer(leader);
            }
            assertUnionRead(tableName, expected);
            for (int b = 0; b < BUCKETS; b++) {
                TableBucket tb = new TableBucket(tableId, b);
                String fields =
                        org.apache.fluss.testutils.common.CommonTestUtils.waitValue(
                                () -> {
                                    org.apache.fluss.server.kv.KvTablet kv =
                                            FLUSS_CLUSTER_EXTENSION
                                                    .waitAndGetLeaderReplica(tb)
                                                    .getKvTablet();
                                    String f =
                                            kv == null || kv.getDvManager() == null
                                                    ? ""
                                                    : kv.getDvManager()
                                                            .lastRebuildCheckpointFields();
                                    return f.isEmpty()
                                            ? java.util.Optional.<String>empty()
                                            : java.util.Optional.of(f);
                                },
                                Duration.ofMinutes(2),
                                "no rebuild of " + tb);
                assertThat(fields).contains("ckpt=used");
            }
        }
    }

    /**
     * Union reads stay exact after a tiered partition is dropped in Fluss. Its lake rows are read
     * from the lake alone, no tablet server is asked for its masks, and the next round stops
     * carrying its offsets.
     */
    @Test
    void testUnionReadAfterPartitionDrop() throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName = "derived_drop_partition";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId = createDerivedTable(tablePath, true, true);
            Map<String, String> expected = new TreeMap<>();
            List<InternalRow> rows = new ArrayList<>();
            for (String p : Arrays.asList("a", "b")) {
                for (int i = 0; i < 10; i++) {
                    rows.add(rowOf(i, "v_" + i, p));
                    expected.put(rowKey(i, p), rowString(i, "v_" + i, p));
                }
            }
            writeRows(tablePath, rows, false);
            long readable = tickUntilReadableAdvances(tablePath, Long.MIN_VALUE, "b");
            expected.put(rowKey(-1, "b"), rowString(-1, "tick", "b"));
            readable = waitUntilAllPartitionsReadable(tablePath, tableId);
            long droppedId = -1;
            for (PartitionInfo info : admin.listPartitionInfos(tablePath).get()) {
                if (info.getPartitionName().equals("a")) {
                    droppedId = info.getPartitionId();
                }
            }
            assertThat(admin.getReadableLakeSnapshot(tablePath).get().getTableBucketsOffset())
                    .containsKey(new TableBucket(tableId, droppedId, 0));

            admin.dropPartition(
                            tablePath,
                            new PartitionSpec(Collections.singletonMap("c3", "a")),
                            false)
                    .get();
            waitUntil(
                    () -> admin.listPartitionInfos(tablePath).get().size() == 1,
                    Duration.ofMinutes(1),
                    Duration.ofMillis(200),
                    "partition a of " + tablePath + " not dropped");
            try (Table table = conn.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                writer.upsert(rowOf(0, "updated_0", "b"));
                writer.delete(rowOf(1, "x", "b"));
                writer.flush();
            }
            expected.put(rowKey(0, "b"), rowString(0, "updated_0", "b"));
            expected.remove(rowKey(1, "b"));

            // The readable snapshot still has the dropped partition's offsets: its lake rows are
            // read without masks, the other partition with them.
            assertUnionRead(tableName, expected);

            // The next round carries no offsets of the dropped partition.
            tickUntilReadableAdvances(tablePath, readable, "b");
            waitUntilAllPartitionsReadable(tablePath, tableId);
            long dropped = droppedId;
            assertThat(admin.getReadableLakeSnapshot(tablePath).get().getTableBucketsOffset())
                    .isNotEmpty()
                    .noneSatisfy(
                            (tb, offset) -> assertThat(tb.getPartitionId()).isEqualTo(dropped));
            assertUnionRead(tableName, expected);
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * A round held for a re-index after its lake commit is lost with the tiering job, as on a
     * failover. The next round registers the lost commit with its readable snapshot once that
     * snapshot is indexed. The readable snapshot catches up with the lake without a further write,
     * and later rounds go on from there.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testLostHeldRoundIsRegisteredWithItsReadableSnapshot(boolean partitioned)
            throws Exception {
        String tableName = "derived_lost_round" + (partitioned ? "_part" : "");
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        long tableId = createDerivedTable(tablePath, partitioned, true);
        List<String> parts =
                partitioned ? Arrays.asList("p0", "p1") : Collections.singletonList(null);
        Map<String, String> expected = new TreeMap<>();
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            List<InternalRow> rows = new ArrayList<>();
            for (String p : parts) {
                for (int i = 0; i < 10; i++) {
                    rows.add(rowOf(i, "v_" + i, p));
                    expected.put(rowKey(i, p), rowString(i, "v_" + i, p));
                }
            }
            writeRows(tablePath, rows, false);
            tickUntilReadableAdvances(tablePath, Long.MIN_VALUE, parts.get(0));
            expected.put(rowKey(-1, parts.get(0)), rowString(-1, "tick", parts.get(0)));
            waitUntilAllReadable(tablePath, tableId, partitioned);
        } finally {
            jobClient.cancel().get();
        }
        long registered = admin.getLatestLakeSnapshot(tablePath).get().getSnapshotId();

        // The compactions of the next round cannot hand their positions over, so the round holds
        // for a re-index after its lake commit, and the job is cancelled there.
        FileStoreTable lakeTable = (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
        HandOffFailingTieringFactory.failNextHandOffs(Integer.MAX_VALUE);
        try (LogCapture log = LogCapture.of("org.apache.fluss.flink.tiering.committer");
                LogCapture.Pause held =
                        log.pauseAt("The round of " + tablePath + " is held until")) {
            try (Table table = conn.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                for (String p : parts) {
                    for (int i = 0; i < 3; i++) {
                        writer.upsert(rowOf(i, "updated_" + i, p));
                        expected.put(rowKey(i, p), rowString(i, "updated_" + i, p));
                    }
                    writer.delete(rowOf(3, "x", p));
                    expected.remove(rowKey(3, p));
                }
                writer.flush();
            }
            jobClient = buildTieringJob(streamingEnv());
            held.awaitReached(Duration.ofMinutes(2));
            jobClient.cancel().get();
        } finally {
            HandOffFailingTieringFactory.failNextHandOffs(0);
        }
        long lostReadable = latestCompaction(lakeTable);
        assertThat(lostReadable).isGreaterThan(registered);
        assertThat(admin.getLatestLakeSnapshot(tablePath).get().getSnapshotId())
                .isEqualTo(registered);
        assertUnionRead(tableName, expected);

        jobClient = buildTieringJob(streamingEnv());
        try {
            waitUntil(
                    () ->
                            admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId()
                                    >= lostReadable,
                    Duration.ofMinutes(2),
                    Duration.ofMillis(200),
                    "readable snapshot of " + tablePath + " stays behind the lost commit");
            assertUnionRead(tableName, expected);
            assertLakeRead(tableName, partitioned, expected);

            long readable = admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId();
            for (int round = 0; round < 2; round++) {
                try (Table table = conn.getTable(tablePath)) {
                    UpsertWriter writer = table.newUpsert().createWriter();
                    for (String p : parts) {
                        String value = "round_" + round;
                        writer.upsert(rowOf(5, value, p));
                        expected.put(rowKey(5, p), rowString(5, value, p));
                        writer.delete(rowOf(6 + round, "x", p));
                        expected.remove(rowKey(6 + round, p));
                    }
                    writer.flush();
                }
                assertUnionRead(tableName, expected);
                readable = tickUntilReadableAdvances(tablePath, readable, parts.get(0));
                assertUnionRead(tableName, expected);
            }
            waitUntilAllReadable(tablePath, tableId, partitioned);
            assertUnionRead(tableName, expected);
            assertLakeRead(tableName, partitioned, expected);
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * A round in flight when a partition with rows in it is dropped commits none of the partition's
     * rows and registers none of its offsets. The dropped partition keeps the rows of the round
     * before, read from the lake alone. The other partition reads exact.
     */
    @Test
    void testPartitionDroppedWhileARoundWithItsRowsIsInFlight() throws Exception {
        String tableName = "derived_drop_during_round";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        long tableId = createDerivedTable(tablePath, true, true);
        Map<String, String> expected = new TreeMap<>();
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            writeRound(tablePath, 0, expected);
            waitUntilAllPartitionsReadable(tablePath, tableId);
            long droppedId = -1;
            for (PartitionInfo info : admin.listPartitionInfos(tablePath).get()) {
                if (info.getPartitionName().equals("drop")) {
                    droppedId = info.getPartitionId();
                }
            }
            Map<String, String> beforeTheRound = new TreeMap<>(expected);
            try (LogCapture log =
                            LogCapture.of(
                                    "org.apache.fluss.flink.tiering.source.TieringSplitReader");
                    LogCapture.Pause finishing =
                            log.pauseAt(
                                    "Finish tier bucket TableBucket{tableId="
                                            + tableId
                                            + ", partitionId="
                                            + droppedId
                                            + ",")) {
                writeRound(tablePath, 1, expected);
                // The round wrote the partition's rows; drop it before the round commits them.
                finishing.awaitReached(Duration.ofMinutes(2));
                admin.dropPartition(
                                tablePath,
                                new PartitionSpec(Collections.singletonMap("c3", "drop")),
                                false)
                        .get();
                waitUntil(
                        () -> admin.listPartitionInfos(tablePath).get().size() == 1,
                        Duration.ofMinutes(1),
                        Duration.ofMillis(200),
                        "partition 'drop' of " + tablePath + " not dropped");
            }
            for (Map.Entry<String, String> e : beforeTheRound.entrySet()) {
                if (e.getKey().endsWith("/drop")) {
                    expected.put(e.getKey(), e.getValue());
                }
            }
            waitUntilAllPartitionsReadable(tablePath, tableId);
            long dropped = droppedId;
            assertThat(admin.getLatestLakeSnapshot(tablePath).get().getTableBucketsOffset())
                    .isNotEmpty()
                    .noneSatisfy(
                            (tb, offset) -> assertThat(tb.getPartitionId()).isEqualTo(dropped));
            assertUnionRead(tableName, expected);
            assertLakeRead(tableName, true, expected);
            assertCount(tableName, expected.size());
            assertCount(tableName + "$lake", expected.size());
        } finally {
            jobClient.cancel().get();
        }
    }

    /** Upserts keys 0 to 29 of partitions keep and drop with values of the round. */
    private void writeRound(TablePath tablePath, int round, Map<String, String> expected)
            throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        for (String p : Arrays.asList("keep", "drop")) {
            for (int i = 0; i < 30; i++) {
                String value = "r" + round + "_" + i;
                rows.add(rowOf(i, value, p));
                expected.put(rowKey(i, p), rowString(i, value, p));
            }
        }
        writeRows(tablePath, rows, false);
    }

    private void assertCount(String tableName, long expected) throws Exception {
        try (CloseableIterator<Row> it =
                batchTEnv.executeSql("select count(*) from " + tableName).collect()) {
            assertThat(it.next().getField(0)).isEqualTo(expected);
        }
    }

    /**
     * A partition dropped in Fluss stays in the lake, where a compaction makes new files of it
     * readable. No tablet server serves the partition, so these files are not re-indexed. The
     * readable snapshot advances to the compaction and reads stay exact, with the dropped
     * partition's rows read from the lake alone.
     */
    @Test
    void testCompactionOfADroppedPartitionIsNotReindexed() throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName = "derived_dropped_partition_compaction";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId = createDerivedTable(tablePath, true, false);
            Map<String, String> expected = new TreeMap<>();
            for (int write = 0; write < 2; write++) {
                List<InternalRow> rows = new ArrayList<>();
                for (String p : Arrays.asList("a", "b")) {
                    for (int i = write * 5; i < 10; i++) {
                        String value = "w" + write + "_" + i;
                        rows.add(rowOf(i, value, p));
                        expected.put(rowKey(i, p), rowString(i, value, p));
                    }
                }
                writeRows(tablePath, rows, false);
                waitUntilAllPartitionsTiered(tablePath, tableId);
            }

            admin.dropPartition(
                            tablePath,
                            new PartitionSpec(Collections.singletonMap("c3", "a")),
                            false)
                    .get();
            waitUntil(
                    () -> admin.listPartitionInfos(tablePath).get().size() == 1,
                    Duration.ofMinutes(1),
                    Duration.ofMillis(200),
                    "partition a of " + tablePath + " not dropped");
            FileStoreTable paimonTable =
                    (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
            CompactHelper compactHelper = new CompactHelper(paimonTable, compactionTempDir);
            for (String p : Arrays.asList("a", "b")) {
                for (int b = 0; b < BUCKETS; b++) {
                    compactHelper.compactBucket(BinaryRow.singleColumn(p), b).commit();
                }
            }
            long compacted = latestCompaction(paimonTable);
            waitUntil(
                    () -> {
                        try {
                            return admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId()
                                    >= compacted;
                        } catch (Exception e) {
                            return false; // nothing readable yet
                        }
                    },
                    Duration.ofMinutes(2),
                    Duration.ofMillis(200),
                    "readable snapshot of " + tablePath + " never moved to the compaction");
            assertUnionRead(tableName, expected);
            assertLakeRead(tableName, true, expected);

            // the remaining partition keeps being written and compacted
            try (Table table = conn.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                writer.upsert(rowOf(0, "updated_0", "b"));
                writer.delete(rowOf(1, "x", "b"));
                writer.flush();
            }
            expected.put(rowKey(0, "b"), rowString(0, "updated_0", "b"));
            expected.remove(rowKey(1, "b"));
            assertUnionRead(tableName, expected);
            waitUntilAllPartitionsTiered(tablePath, tableId);
            for (int b = 0; b < BUCKETS; b++) {
                compactHelper.compactBucket(BinaryRow.singleColumn("b"), b).commit();
            }
            long compactedAgain = latestCompaction(paimonTable);
            waitUntil(
                    () ->
                            admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId()
                                    >= compactedAgain,
                    Duration.ofMinutes(2),
                    Duration.ofMillis(200),
                    "readable snapshot of " + tablePath + " never moved to the second compaction");
            assertUnionRead(tableName, expected);
            assertLakeRead(tableName, true, expected);
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * Tiering only appends level-0 files and another engine compacts some buckets at a time. A
     * bucket stays unwritten through the compaction of another one. That readable snapshot does not
     * need the registration of the round before the bucket's own last compaction, so Fluss drops
     * it. Once the bucket is written again, the readable snapshot of the next compaction still
     * finds where the bucket's compacted rows end, and advances.
     */
    @Test
    void testReadableSnapshotAdvancesAfterABucketStayedUnwritten() throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName = "derived_unwritten_bucket";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId = createDerivedTable(tablePath, false, false);
            Map<Integer, List<Integer>> keysByBucket = new TreeMap<>();
            for (int key = 0; key < 30; key++) {
                keysByBucket.computeIfAbsent(bucketOf(key), b -> new ArrayList<>()).add(key);
            }
            assertThat(keysByBucket).hasSize(BUCKETS);
            Map<String, String> expected = new TreeMap<>();
            List<InternalRow> rows = new ArrayList<>();
            for (int key = 0; key < 30; key++) {
                rows.add(rowOf(key, "v_" + key, null));
                expected.put(rowKey(key, null), rowString(key, "v_" + key, null));
            }
            writeRows(tablePath, rows, false);
            waitUntilTiered(tablePath, tableId);

            // every bucket compacted once
            FileStoreTable paimonTable =
                    (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
            CompactHelper compactHelper = new CompactHelper(paimonTable, compactionTempDir);
            for (int b = 0; b < BUCKETS; b++) {
                compactHelper.compactBucket(BinaryRow.EMPTY_ROW, b).commit();
            }
            waitUntilReadable(tablePath, latestCompaction(paimonTable));
            assertUnionRead(tableName, expected);

            // only bucket 0 is written and compacted
            long[] highWatermarks = new long[BUCKETS];
            for (int b = 0; b < BUCKETS; b++) {
                highWatermarks[b] =
                        getLeaderReplica(new TableBucket(tableId, b)).getLogHighWatermark();
            }
            upsertKeys(tablePath, keysByBucket.get(0), "w1", expected);
            waitUntilTiered(tablePath, tableId);
            for (int b = 1; b < BUCKETS; b++) {
                assertThat(getLeaderReplica(new TableBucket(tableId, b)).getLogHighWatermark())
                        .isEqualTo(highWatermarks[b]);
            }
            compactHelper.compactBucket(BinaryRow.EMPTY_ROW, 0).commit();
            waitUntilReadable(tablePath, latestCompaction(paimonTable));
            assertUnionRead(tableName, expected);

            // buckets 1 and 2 are written, only bucket 2 compacted: bucket 1 keeps level 0
            upsertKeys(tablePath, keysByBucket.get(1), "w2", expected);
            upsertKeys(tablePath, keysByBucket.get(2), "w2", expected);
            waitUntilTiered(tablePath, tableId);
            compactHelper.compactBucket(BinaryRow.EMPTY_ROW, 2).commit();
            waitUntilReadable(tablePath, latestCompaction(paimonTable));
            assertUnionRead(tableName, expected);

            compactHelper.compactBucket(BinaryRow.EMPTY_ROW, 1).commit();
            waitUntilReadable(tablePath, latestCompaction(paimonTable));
            waitUntilAllReadable(tablePath, tableId, false);
            assertUnionRead(tableName, expected);
            assertLakeRead(tableName, false, expected);
        } finally {
            jobClient.cancel().get();
        }
    }

    private void upsertKeys(
            TablePath tablePath, List<Integer> keys, String value, Map<String, String> expected)
            throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        for (int key : keys) {
            rows.add(rowOf(key, value + "_" + key, null));
            expected.put(rowKey(key, null), rowString(key, value + "_" + key, null));
        }
        writeRows(tablePath, rows, false);
    }

    /** Waits until the readable snapshot is {@code snapshotId} or later; returns it. */
    private long waitUntilReadable(TablePath tablePath, long snapshotId) {
        return waitValue(
                () -> {
                    try {
                        long readable =
                                admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId();
                        if (readable >= snapshotId) {
                            return Optional.of(readable);
                        }
                    } catch (Exception e) {
                        // nothing readable yet
                    }
                    Thread.sleep(200);
                    return Optional.empty();
                },
                Duration.ofMinutes(2),
                "readable snapshot of " + tablePath + " never moved to snapshot " + snapshotId);
    }

    /** The bucket of a key of a table created by {@link #createDerivedTable}. */
    private static int bucketOf(int key) {
        KeyEncoder encoder =
                KeyEncoder.ofBucketKeyEncoder(
                        RowType.of(
                                new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                                new String[] {"c1", "c2"}),
                        Collections.singletonList("c1"),
                        DataLakeFormat.PAIMON);
        return BucketingFunction.of(DataLakeFormat.PAIMON)
                .bucketing(encoder.encodeKey(row(key, "x")), BUCKETS);
    }

    /**
     * When another engine compacts an idle table, its readable snapshot advances to the compaction
     * and the rewritten files are indexed without a write. The idle table then stays unchanged.
     */
    @Test
    void testIdleTableReadableAfterExternalCompaction() throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName = "derived_idle_compaction";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId = createDerivedTable(tablePath, false, false);
            Map<String, String> expected = new TreeMap<>();
            List<InternalRow> rows = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                rows.add(rowOf(i, "v_" + i, null));
                expected.put(rowKey(i, null), rowString(i, "v_" + i, null));
            }
            writeRows(tablePath, rows, false);
            waitUntilTiered(tablePath, tableId);
            try (Table table = conn.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                writer.upsert(rowOf(0, "updated_0", null));
                writer.delete(rowOf(1, "x", null));
                writer.flush();
            }
            expected.put(rowKey(0, null), rowString(0, "updated_0", null));
            expected.remove(rowKey(1, null));
            waitUntilTiered(tablePath, tableId);

            // The writers are idle from here on; another engine compacts every bucket.
            FileStoreTable paimonTable =
                    (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
            CompactHelper compactHelper = new CompactHelper(paimonTable, compactionTempDir);
            for (int b = 0; b < BUCKETS; b++) {
                compactHelper.compactBucket(BinaryRow.EMPTY_ROW, b).commit();
            }
            // An idle round may commit between the compactions; the last COMPACT is readable.
            long compacted = latestCompaction(paimonTable);
            waitUntil(
                    () -> {
                        try {
                            return admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId()
                                    == compacted;
                        } catch (Exception e) {
                            return false; // nothing readable yet
                        }
                    },
                    Duration.ofMinutes(2),
                    Duration.ofMillis(200),
                    "readable snapshot of idle " + tablePath + " never moved to the compaction");
            waitUntilAllReadable(tablePath, tableId, false);
            assertUnionRead(tableName, expected);
            assertLakeRead(tableName, false, expected);

            // Idle rounds keep coming and commit nothing more.
            long latest = paimonTable.snapshotManager().latestSnapshotId();
            Thread.sleep(5_000);
            assertThat(paimonTable.snapshotManager().latestSnapshotId()).isEqualTo(latest);

            // The next round resolves its keys against the compaction's index.
            try (Table table = conn.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                writer.upsert(rowOf(2, "updated_2", null));
                writer.delete(rowOf(3, "x", null));
                writer.flush();
            }
            expected.put(rowKey(2, null), rowString(2, "updated_2", null));
            expected.remove(rowKey(3, null));
            assertUnionRead(tableName, expected);
            waitUntilTiered(tablePath, tableId);
            assertUnionRead(tableName, expected);
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * A rollback of the Paimon table outside Fluss, after which another engine commits under the
     * deleted snapshots' ids, stops the table's tiering without registering or writing anything.
     * Restoring the registered snapshot from a branch resumes it. Until then, union reads read the
     * lake at the readable snapshot's id, which the rollback deleted or reused.
     */
    @Test
    void testRollbackOfLakeTableStopsTiering() throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName = "derived_rollback";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId = createDerivedTable(tablePath, false, true);
            Map<String, String> expected = new TreeMap<>();
            List<InternalRow> rows = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                rows.add(rowOf(i, "v_" + i, null));
                expected.put(rowKey(i, null), rowString(i, "v_" + i, null));
            }
            writeRows(tablePath, rows, false);
            long first = waitUntilTiered(tablePath, tableId);
            try (Table table = conn.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                writer.upsert(rowOf(0, "updated_0", null));
                writer.delete(rowOf(1, "x", null));
                writer.flush();
            }
            expected.put(rowKey(0, null), rowString(0, "updated_0", null));
            expected.remove(rowKey(1, null));
            long second = waitUntilTiered(tablePath, tableId);
            assertThat(second).isGreaterThan(first);
            long readable = waitUntilAllReadable(tablePath, tableId, false);

            FileStoreTable lakeTable = (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
            // Paimon's rollback deletes later tags, so a branch keeps the registered snapshot.
            lakeTable.createTag("registered", second);
            lakeTable.createBranch("keep", "registered");
            // As Flink SQL's CALL sys.rollback_to does; another engine then commits under the ids
            // of the deleted snapshots.
            lakeTable.rollbackTo(first);
            while (lakeTable.snapshotManager().latestSnapshotId() <= second) {
                try (TableCommitImpl commit = lakeTable.newCommit("external-engine")) {
                    commit.ignoreEmptyCommit(false)
                            .commit(new ManifestCommittable(BatchWriteBuilder.COMMIT_IDENTIFIER));
                }
            }
            long lakeLatest = lakeTable.snapshotManager().latestSnapshotId();
            Set<String> offsetsFiles = offsetsFiles(tablePath, tableId);
            Set<String> lakeFiles = filesUnder(lakeTable.location().toString());

            writeRows(tablePath, Collections.singletonList(rowOf(100, "v_100", null)), false);
            expected.put(rowKey(100, null), rowString(100, "v_100", null));
            // Rounds keep being scheduled every second or so.
            Thread.sleep(10_000);

            assertThat(admin.getLatestLakeSnapshot(tablePath).get().getSnapshotId())
                    .isEqualTo(second);
            assertThat(admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId())
                    .isEqualTo(readable);
            assertThat(lakeTable.snapshotManager().latestSnapshotId()).isEqualTo(lakeLatest);
            assertThat(offsetsFiles(tablePath, tableId)).isEqualTo(offsetsFiles);
            assertThat(filesUnder(lakeTable.location().toString())).isEqualTo(lakeFiles);

            // A union read plans on the readable snapshot's id, which another commit holds now:
            // the masks of the registered snapshot must not be applied to it.
            assertThat(readable).isGreaterThan(first);
            assertThatThrownBy(() -> assertUnionRead(tableName, expected))
                    .hasStackTraceContaining("rolled back outside Fluss")
                    .hasStackTraceContaining("Snapshot " + readable + ", the readable snapshot");

            // Undone, as Flink SQL's CALL sys.fast_forward does: tiering resumes.
            lakeTable.fastForward("keep");
            long resumed = waitUntilTiered(tablePath, tableId);
            assertThat(resumed).isGreaterThan(second);
            waitUntilAllReadable(tablePath, tableId, false);
            assertUnionRead(tableName, expected);
            assertLakeRead(tableName, false, expected);
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * Waits until every write to the table's partitions is readable from the lake; returns the
     * readable snapshot.
     */
    private long waitUntilAllPartitionsReadable(TablePath tablePath, long tableId) {
        return waitValue(
                () -> {
                    LakeSnapshot snapshot;
                    try {
                        snapshot = admin.getReadableLakeSnapshot(tablePath).get();
                    } catch (Exception e) {
                        Thread.sleep(200);
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
                "writes of " + tablePath + " not all readable");
    }

    // ------------------------------------------------------------------------------------------

    /** A streaming environment for a tiering job, as batch reads leave execEnv in batch mode. */
    private static StreamExecutionEnvironment streamingEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(2);
        return env;
    }

    /** Waits until every write to the table's partitions is tiered (readable or not). */
    private void waitUntilAllPartitionsTiered(TablePath tablePath, long tableId) {
        waitUntil(
                () -> {
                    LakeSnapshot snapshot;
                    try {
                        snapshot = admin.getLatestLakeSnapshot(tablePath).get();
                    } catch (Exception e) {
                        return false;
                    }
                    for (PartitionInfo partition : admin.listPartitionInfos(tablePath).get()) {
                        for (int b = 0; b < BUCKETS; b++) {
                            TableBucket tb =
                                    new TableBucket(tableId, partition.getPartitionId(), b);
                            long highWatermark = getLeaderReplica(tb).getLogHighWatermark();
                            if (snapshot.getTableBucketsOffset().getOrDefault(tb, 0L)
                                    < highWatermark) {
                                return false;
                            }
                        }
                    }
                    return true;
                },
                Duration.ofMinutes(2),
                Duration.ofMillis(200),
                "writes of " + tablePath + " not all tiered");
    }

    /**
     * Waits until every bucket's log is tiered to the lake (readable or not); returns the latest
     * registered snapshot.
     */
    private long waitUntilTiered(TablePath tablePath, long tableId) {
        return waitValue(
                () -> {
                    LakeSnapshot snapshot;
                    try {
                        snapshot = admin.getLatestLakeSnapshot(tablePath).get();
                    } catch (Exception e) {
                        Thread.sleep(200);
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

    /** Waits until every write is readable from the lake; returns the readable snapshot. */
    private long waitUntilAllReadable(TablePath tablePath, long tableId, boolean partitioned) {
        return waitValue(
                () -> {
                    LakeSnapshot snapshot;
                    try {
                        snapshot = admin.getReadableLakeSnapshot(tablePath).get();
                    } catch (Exception e) {
                        Thread.sleep(200);
                        return Optional.empty();
                    }
                    for (Map.Entry<TableBucket, Long> e :
                            snapshot.getTableBucketsOffset().entrySet()) {
                        long highWatermark = getLeaderReplica(e.getKey()).getLogHighWatermark();
                        if (e.getValue() < highWatermark) {
                            Thread.sleep(200);
                            return Optional.empty();
                        }
                    }
                    if (!partitioned && snapshot.getTableBucketsOffset().size() < BUCKETS) {
                        Thread.sleep(200);
                        return Optional.empty();
                    }
                    return Optional.of(snapshot.getSnapshotId());
                },
                Duration.ofMinutes(2),
                "writes of " + tablePath + " not all readable");
    }

    private static long latestCompaction(FileStoreTable table) throws Exception {
        for (long id = table.snapshotManager().latestSnapshotId(); ; id--) {
            if (table.snapshotManager().snapshot(id).commitKind()
                    == org.apache.paimon.Snapshot.CommitKind.COMPACT) {
                return id;
            }
        }
    }

    private static Set<String> offsetsFiles(TablePath tablePath, long tableId) throws Exception {
        return filesUnder(
                FlussPaths.remoteLakeTableSnapshotDir(
                                FLUSS_CLUSTER_EXTENSION.getRemoteDataDir(), tablePath, tableId)
                        + "/metadata");
    }

    /** The files under a directory, without checksum files. */
    private static Set<String> filesUnder(String dir) throws Exception {
        java.nio.file.Path path = Paths.get(new URI(dir.startsWith("/") ? "file://" + dir : dir));
        if (!Files.exists(path)) {
            return Collections.emptySet();
        }
        try (Stream<java.nio.file.Path> files = Files.walk(path)) {
            return files.filter(Files::isRegularFile)
                    .map(java.nio.file.Path::toString)
                    .filter(f -> !f.endsWith(".crc"))
                    .collect(Collectors.toSet());
        }
    }

    private void assertUnionRead(String tableName, Map<String, String> expected) throws Exception {
        CloseableIterator<Row> rowIter =
                batchTEnv.executeSql("select * from " + tableName).collect();
        assertResultsIgnoreOrder(rowIter, new ArrayList<>(expected.values()), true);
    }

    private void assertLakeRead(String tableName, boolean partitioned, Map<String, String> expected)
            throws Exception {
        String columns = partitioned ? "c1, c2, c3" : "c1, c2";
        CloseableIterator<Row> lakeIter =
                batchTEnv
                        .executeSql("select " + columns + " from " + tableName + "$lake")
                        .collect();
        assertResultsIgnoreOrder(lakeIter, new ArrayList<>(expected.values()), true);
    }

    private long createDerivedTable(
            TablePath tablePath, boolean partitioned, boolean autoCompaction) throws Exception {
        return createDerivedTable(tablePath, partitioned, autoCompaction, Collections.emptyMap());
    }

    private long createDerivedTable(
            TablePath tablePath,
            boolean partitioned,
            boolean autoCompaction,
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
                        .property(
                                ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION.key(),
                                String.valueOf(autoCompaction));
        properties.forEach(builder::property);
        if (partitioned) {
            schema.column("c3", DataTypes.STRING()).primaryKey("c1", "c3");
            builder.partitionedBy("c3");
        } else {
            schema.primaryKey("c1");
        }
        long tableId = createTable(tablePath, builder.schema(schema.build()).build());
        FileStoreTable paimonTable = (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
        assertThat(paimonTable.coreOptions().deletionVectorsEnabled()).isTrue();
        return tableId;
    }

    /** Upserts a tick row until the readable snapshot advances. */
    private long tickUntilReadableAdvances(
            TablePath tablePath, long previous, @Nullable String partition) throws Exception {
        return tickUntilReadableAdvances(conn, tablePath, previous, partition);
    }

    private long tickUntilReadableAdvances(
            Connection writes, TablePath tablePath, long previous, @Nullable String partition)
            throws Exception {
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
            upsert(writes, tablePath, Collections.singletonList(rowOf(-1, "tick", partition)));
            Thread.sleep(1000);
        }
        throw new AssertionError("readable snapshot of " + tablePath + " stuck at " + previous);
    }

    private static void upsert(Connection writes, TablePath tablePath, List<InternalRow> rows)
            throws Exception {
        try (Table table = writes.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (InternalRow row : rows) {
                writer.upsert(row);
            }
            writer.flush();
        }
    }

    private static InternalRow rowOf(int key, String value, @Nullable String partition) {
        return partition == null ? row(key, value) : row(key, value, partition);
    }

    private static String rowKey(int key, @Nullable String partition) {
        return key + "/" + partition;
    }

    private static String rowString(int key, String value, @Nullable String partition) {
        return partition == null
                ? String.format("+I[%d, %s]", key, value)
                : String.format("+I[%d, %s, %s]", key, value, partition);
    }
}
