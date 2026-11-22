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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.utils.LogCapture;
import org.apache.fluss.lake.iceberg.testutils.CommitHookCatalog;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.core.execution.JobClient;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.util.ContentFileUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tiering rounds of tables with deletion vectors on Iceberg that commit the deletes their readers
 * fetched from the buckets' leaders while they read the rounds, and rounds whose committer fetches
 * them again because they no longer apply: after the round's own compaction, a rewrite by another
 * engine, a leader change, or a restart of the tiering job. Each test checks the lake, read through
 * Iceberg, key by key against what was written to Fluss.
 */
class FlinkDerivedDvRoundDeletesITCase extends FlinkDerivedDvTestBase {

    private static final String COMMITTER_LOGGER = "org.apache.fluss.flink.tiering.committer";
    private static final String LAKE_COMMITTER_LOGGER =
            "org.apache.fluss.lake.iceberg.tiering.IcebergLakeCommitter";
    private static final String READER_LOGGER =
            "org.apache.fluss.flink.tiering.source.TieringSplitReader";
    private static final Pattern DELETES_SOURCES =
            Pattern.compile(
                    "of buckets (\\d+) prepared by their writers, (\\d+) from their readers and"
                            + " (\\d+) from their leaders");
    private static final Pattern COMMITTED_SNAPSHOT = Pattern.compile("-> snapshot (\\d+) ");
    private static final Pattern REFUSED = Pattern.compile("(\\d+) prepared ones refused");
    // where the deletes of a round's buckets came from, as deletesSources counts them
    private static final int WRITERS = 0;
    private static final int READERS = 1;
    private static final int LEADERS = 2;

    /**
     * Rounds whose bases do not change commit the deletes their readers fetched, and fetch none at
     * the committer. On Iceberg v3 the writers prepare the buckets' deletion vectors from them, one
     * Puffin file per bucket, unless that is turned off; v2 has no deletion vectors to prepare. The
     * lake keeps one live row per key, and every Puffin file is committed.
     */
    @ParameterizedTest
    @CsvSource({"2, true", "3, true", "3, false"})
    void testRoundsCommitTheDeletesTheirReadersFetched(int formatVersion, boolean writersPrepare)
            throws Exception {
        String tableName =
                "round_deletes_prefetched_v" + formatVersion + (writersPrepare ? "" : "_central");
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        long tableId = createDerivedTable(tablePath, formatVersion, false);
        Map<Integer, String> expected = new TreeMap<>();
        JobClient jobClient =
                buildTieringJob(execEnv, getIcebergCatalogConf(), writersPrepare(writersPrepare));
        boolean prepared = writersPrepare && formatVersion >= 3;
        try (LogCapture committerLog = LogCapture.of(COMMITTER_LOGGER)) {
            upsert(tablePath, expected, 0, 60, "r0");
            waitUntilAllTiered(tablePath, tableId);
            for (int round = 1; round <= 3; round++) {
                changeRound(tablePath, expected, round);
                waitUntilAllTiered(tablePath, tableId);
                assertLakeExact(tableName, expected);
            }
            int[] sources = deletesSources(committerLog, tablePath);
            assertThat(sources[prepared ? WRITERS : READERS])
                    .as("buckets with deletes from their %s", prepared ? "writers" : "readers")
                    .isPositive();
            assertThat(sources[prepared ? READERS : WRITERS]).isZero();
            assertThat(sources[LEADERS]).as("buckets with deletes from their leaders").isZero();
            if (prepared) {
                // a bucket's writer writes its deletion vectors into one Puffin file
                for (String round :
                        committerLog.messagesWith("Committed the round of " + tablePath + ":")) {
                    assertThat(puffinFilesAdded(tablePath, committedSnapshot(round)))
                            .as(round)
                            .isEqualTo(deletesSources(round)[WRITERS]);
                }
            }
            if (formatVersion >= 3) {
                assertNoOrphanPuffinFiles(tablePath);
            }
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * A round that compacts first runs again on its compaction's snapshot. The deletes its readers
     * fetched on the snapshot before still hold for the buckets the compaction left alone, whose
     * files, deletes and offsets did not change; only the compacted bucket's deletes are fetched
     * again from its leader. The lake stays exact.
     *
     * <p>Two rounds that change one key leave three small files in that key's bucket only, so the
     * next round, which changes every bucket, compacts that bucket alone. A round can finish before
     * a bucket's reader reached its end, and that bucket then commits in the next round, so the
     * test counts every round that tiers the change.
     */
    @Test
    void testOwnCompactionKeepsTheDeletesOfTheBucketsItLeftAlone() throws Exception {
        String tableName = "round_deletes_own_compaction";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        Map<String, String> properties = new HashMap<>();
        properties.put(ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION.key(), "true");
        properties.put(ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION_INTERVAL.key(), "0s");
        properties.put(ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION_MIN_SMALL_FILES.key(), "3");
        long tableId = createDerivedTable(tablePath, 3, false, properties);
        Map<Integer, String> expected = new TreeMap<>();
        JobClient jobClient = buildTieringJob(execEnv);
        try (LogCapture committerLog = LogCapture.of(COMMITTER_LOGGER)) {
            upsert(tablePath, expected, 0, 60, "r0");
            waitUntilAllTiered(tablePath, tableId);
            for (int round = 1; round <= 2; round++) {
                upsert(tablePath, expected, 0, 1, "one" + round);
                waitUntilAllTiered(tablePath, tableId);
            }
            String compacted = "Fluss compaction of " + tablePath;
            assertThat(committerLog.messagesWith(compacted, "committed")).isEmpty();
            int[] before = deletesSources(committerLog, tablePath);

            changeRound(tablePath, expected, 3);
            waitUntilAllTiered(tablePath, tableId);

            assertThat(committerLog.messagesWith(compacted, "committed", "1 buckets handed"))
                    .hasSize(1);
            int[] after = deletesSources(committerLog, tablePath);
            assertThat(after[WRITERS] - before[WRITERS])
                    .as("buckets whose writers prepared their deletion vectors")
                    .isEqualTo(2);
            assertThat(after[READERS] - before[READERS])
                    .as("buckets with deletes from their readers")
                    .isZero();
            assertThat(after[LEADERS] - before[LEADERS])
                    .as("buckets with deletes from their leaders")
                    .isEqualTo(1);
            assertLakeExact(tableName, expected);

            changeRound(tablePath, expected, 4);
            waitUntilAllTiered(tablePath, tableId);
            assertLakeExact(tableName, expected);
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * Another engine rewrites files right before the round's commit: the base fence rejects it, the
     * round re-indexes the rewrite and runs again on it. A bucket whose deletes name none of the
     * rewritten files keeps them, as the deletion vectors its writer prepared or the deletes its
     * reader fetched: its files, their deletion vectors, its index and its offset are the same on
     * the rewrite. A bucket whose named files were rewritten fetches its deletes again from its
     * leader. The lake stays exact.
     */
    @ParameterizedTest(name = "rewritten {0}, writers prepare {1}")
    @CsvSource({"others, true", "others, false", "bucket, true", "bucket, false"})
    void testRewriteDuringTheRoundKeepsTheDeletesNamingNoRewrittenFile(
            String rewritten, boolean writersPrepare) throws Exception {
        String tableName =
                "round_deletes_rewrite_" + rewritten + (writersPrepare ? "" : "_central");
        String lakeTableName = DEFAULT_DB + "." + tableName;
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        long tableId = createDerivedTable(tablePath, 3, false);
        Map<Integer, String> expected = new TreeMap<>();
        JobClient jobClient =
                buildTieringJob(
                        execEnv, getCommitHookCatalogConf(), writersPrepare(writersPrepare));
        boolean oneBucket = rewritten.equals("bucket");
        try (LogCapture committerLog = LogCapture.of(COMMITTER_LOGGER)) {
            upsert(tablePath, expected, 0, 60, "r0");
            waitUntilAllTiered(tablePath, tableId);
            changeRound(tablePath, expected, 1);
            waitUntilAllTiered(tablePath, tableId);
            int[] before = deletesSources(committerLog, tablePath);

            List<Long> rewrites = new ArrayList<>();
            CommitHookCatalog.armOnce(
                    lakeTableName,
                    "fluss-dv-index-id",
                    (base, pending, io) -> {
                        if (oneBucket) {
                            rewrites.add(rewriteDataFilesOfBucket(tablePath, 0));
                            return;
                        }
                        // every file but those the round deletes rows in
                        Set<String> referenced = new HashSet<>();
                        for (DeleteFile delete : pending.currentSnapshot().addedDeleteFiles(io)) {
                            referenced.add(ContentFileUtil.referencedDataFileLocation(delete));
                        }
                        rewrites.add(rewriteDataFilesExcept(tablePath, referenced));
                    });
            changeRound(tablePath, expected, 2);
            waitUntilAllTiered(tablePath, tableId);
            assertThat(CommitHookCatalog.isArmed(lakeTableName)).isFalse();
            assertThat(rewrites).hasSize(1);
            assertLakeExact(tableName, expected);
            int[] after = deletesSources(committerLog, tablePath);
            int kept = writersPrepare ? WRITERS : READERS;
            int refetched = oneBucket ? 1 : 0;
            assertThat(after[kept] - before[kept])
                    .as("buckets whose deletes hold on the rewrite")
                    .isEqualTo(BUCKETS - refetched);
            assertThat(after[LEADERS] - before[LEADERS])
                    .as("buckets with deletes from their leaders")
                    .isEqualTo(refetched);
            List<String> rounds = committerLog.messagesWith("Committed the round of " + tablePath);
            assertThat(refused(rounds.get(rounds.size() - 1)))
                    .as("buckets whose prepared deletion vectors the committer refused")
                    .isEqualTo(writersPrepare ? refetched : 0);

            // the rows the rewrite moved are deleted by position in their new files
            changeRound(tablePath, expected, 3);
            waitUntilAllTiered(tablePath, tableId);
            assertLakeExact(tableName, expected);
            assertNoOrphanPuffinFiles(tablePath);
        } finally {
            CommitHookCatalog.disarm(lakeTableName);
            jobClient.cancel().get();
        }
    }

    /**
     * The tablet servers restart after the readers fetched a round's deletes and before the round
     * commits: the deletes depend on the round's snapshot and offsets only, so the committer uses
     * those its readers fetched from the old leaders, or fetches them from the new ones, and the
     * lake stays exact.
     */
    @Test
    void testLeaderChangeBetweenTheFetchAndTheCommit() throws Exception {
        String tableName = "round_deletes_leader_change";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        long tableId = createDerivedTable(tablePath, 3, false);
        Map<Integer, String> expected = new TreeMap<>();
        JobClient jobClient = buildTieringJob(execEnv);
        try (LogCapture committerLog = LogCapture.of(COMMITTER_LOGGER)) {
            upsert(tablePath, expected, 0, 60, "r0");
            waitUntilAllTiered(tablePath, tableId);
            int[] before = deletesSources(committerLog, tablePath);
            try (LogCapture readerLog = LogCapture.of(READER_LOGGER);
                    LogCapture.Pause finishing =
                            readerLog.pauseAt(
                                    "Finish tier bucket TableBucket{tableId=" + tableId + ",")) {
                changeRound(tablePath, expected, 1);
                finishing.awaitReached(Duration.ofMinutes(2));
                restartTabletServers();
            }
            waitUntilAllTiered(tablePath, tableId);
            assertLakeExact(tableName, expected);
            int[] after = deletesSources(committerLog, tablePath);
            assertThat(total(after)).isGreaterThan(total(before));

            changeRound(tablePath, expected, 2);
            waitUntilAllTiered(tablePath, tableId);
            assertLakeExact(tableName, expected);
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * The tiering job stops while a round is in flight, as on a failover, and a new job tiers the
     * table: at a reader that completed a bucket's round, or at the committer once the round's lake
     * commit landed, before its registration. Nothing of the stopped round's fetched deletes is
     * kept; the next rounds fetch their own, and the lake stays exact.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testJobRestartDuringTheRoundDropsItsFetchedDeletes(boolean atTheReader) throws Exception {
        String tableName = "round_deletes_restart_" + (atTheReader ? "reader" : "committer");
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        long tableId = createDerivedTable(tablePath, 3, false);
        Map<Integer, String> expected = new TreeMap<>();
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            upsert(tablePath, expected, 0, 60, "r0");
            waitUntilAllTiered(tablePath, tableId);
            String[] pauseAt =
                    atTheReader
                            ? new String[] {
                                "Finish tier bucket TableBucket{tableId=" + tableId + ","
                            }
                            : new String[] {"Committed snapshot", "deletion vectors"};
            try (LogCapture log =
                            LogCapture.of(atTheReader ? READER_LOGGER : LAKE_COMMITTER_LOGGER);
                    LogCapture.Pause paused = log.pauseAt(pauseAt)) {
                changeRound(tablePath, expected, 1);
                paused.awaitReached(Duration.ofMinutes(2));
                jobClient.cancel().get();
            }
            jobClient = buildTieringJob(execEnv);
            waitUntilAllTiered(tablePath, tableId);
            assertLakeExact(tableName, expected);

            changeRound(tablePath, expected, 2);
            waitUntilAllTiered(tablePath, tableId);
            assertLakeExact(tableName, expected);
        } finally {
            jobClient.cancel().get();
        }
    }

    // ---- helpers --------------------------------------------------------------------------------

    /** Updates a third of the keys, deletes a few and adds new ones. */
    private void changeRound(TablePath tablePath, Map<Integer, String> expected, int round)
            throws Exception {
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (int key = round % 3; key < 60; key += 3) {
                if (!expected.containsKey(key)) {
                    continue;
                }
                if (key % 11 == round % 11) {
                    writer.delete(rowOf(key, expected.remove(key), null));
                } else {
                    String value = "r" + round + "_" + key;
                    writer.upsert(rowOf(key, value, null));
                    expected.put(key, value);
                }
            }
            for (int key = 60 + 5 * (round - 1); key < 60 + 5 * round; key++) {
                String value = "r" + round + "_" + key;
                writer.upsert(rowOf(key, value, null));
                expected.put(key, value);
            }
            writer.flush();
        }
    }

    private void assertLakeExact(String tableName, Map<Integer, String> expected) throws Exception {
        List<String> rows = new ArrayList<>();
        expected.forEach((key, value) -> rows.add(rowString(key, value, null)));
        // the read collects only as many rows as expected; the count catches extra rows
        assertLakeRead(tableName, "c1, c2", rows);
        assertCount(tableName + "$lake", rows.size());
    }

    /**
     * The buckets of the table's committed rounds whose writers prepared their deletion vectors,
     * whose deletes came from their readers, and those whose deletes the committer fetched from
     * their leaders, at {@link #WRITERS}, {@link #READERS} and {@link #LEADERS}.
     */
    private static int[] deletesSources(LogCapture committerLog, TablePath tablePath) {
        int[] sources = new int[3];
        for (String message :
                committerLog.messagesWith("Committed the round of " + tablePath + ":")) {
            int[] round = deletesSources(message);
            for (int i = 0; i < sources.length; i++) {
                sources[i] += round[i];
            }
        }
        return sources;
    }

    /** Where the deletes of a committed round's buckets came from, as above. */
    private static int[] deletesSources(String roundMessage) {
        Matcher m = DELETES_SOURCES.matcher(roundMessage);
        assertThat(m.find()).as(roundMessage).isTrue();
        return new int[] {
            Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)), Integer.parseInt(m.group(3))
        };
    }

    private static int refused(String roundMessage) {
        Matcher m = REFUSED.matcher(roundMessage);
        assertThat(m.find()).as(roundMessage).isTrue();
        return Integer.parseInt(m.group(1));
    }

    private static int total(int[] sources) {
        return sources[WRITERS] + sources[READERS] + sources[LEADERS];
    }

    private static long committedSnapshot(String roundMessage) {
        Matcher m = COMMITTED_SNAPSHOT.matcher(roundMessage);
        assertThat(m.find()).as(roundMessage).isTrue();
        return Long.parseLong(m.group(1));
    }

    private static Configuration writersPrepare(boolean enabled) {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_WRITER_PREPARED_ENABLED, enabled);
        return conf;
    }

    /** The Puffin files of the deletion vectors snapshot {@code snapshotId} added. */
    private static long puffinFilesAdded(TablePath tablePath, long snapshotId) {
        org.apache.iceberg.Table table = icebergCatalog.loadTable(toIceberg(tablePath));
        Set<String> puffins = new HashSet<>();
        for (DeleteFile delete : table.snapshot(snapshotId).addedDeleteFiles(table.io())) {
            if (delete.format() == FileFormat.PUFFIN) {
                puffins.add(delete.location());
            }
        }
        return puffins.size();
    }

    private static String localPath(String location) {
        return Paths.get(location.startsWith("file:") ? URI.create(location).getPath() : location)
                .normalize()
                .toString();
    }

    /** Checks that every Puffin file in the lake table's directory is in one of its snapshots. */
    private static void assertNoOrphanPuffinFiles(TablePath tablePath) throws Exception {
        org.apache.iceberg.Table table = icebergCatalog.loadTable(toIceberg(tablePath));
        Set<String> committed = new HashSet<>();
        for (Snapshot snapshot : table.snapshots()) {
            for (DeleteFile delete : snapshot.addedDeleteFiles(table.io())) {
                committed.add(localPath(delete.location()));
            }
        }
        Set<String> written;
        try (Stream<java.nio.file.Path> files =
                Files.walk(Paths.get(localPath(table.location())))) {
            written =
                    files.map(java.nio.file.Path::toString)
                            .filter(f -> f.endsWith(".puffin"))
                            .collect(Collectors.toSet());
        }
        assertThat(written).isNotEmpty();
        assertThat(committed).containsAll(written);
    }

    private void restartTabletServers() throws Exception {
        for (int server = 0; server < 3; server++) {
            FLUSS_CLUSTER_EXTENSION.restartTabletServer(
                    server, new org.apache.fluss.config.Configuration());
        }
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);
        // the shared client would keep the restarted servers' old endpoints and stall writes
        admin.close();
        conn.close();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
        waitUntil(
                () -> !admin.listTables(DEFAULT_DB).get().isEmpty(),
                Duration.ofMinutes(1),
                Duration.ofMillis(200),
                "the restarted cluster serves no metadata");
    }
}
