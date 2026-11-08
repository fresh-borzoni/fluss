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

package org.apache.fluss.flink.tiering.committer;

import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.tiering.FlussTableLakeSnapshotCommitter;
import org.apache.fluss.client.tiering.TableBucketWriteResult;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.FlussConfigUtils;
import org.apache.fluss.flink.tiering.TestingWriteResult;
import org.apache.fluss.flink.tiering.committer.TieringEpochFencingTest.FakeLakeCommitter;
import org.apache.fluss.flink.tiering.committer.TieringEpochFencingTest.FakeLakeSource;
import org.apache.fluss.flink.tiering.committer.TieringEpochFencingTest.FakeLakeTieringFactory;
import org.apache.fluss.flink.tiering.source.TieringTestBase;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.LakeCommitRejectedException;
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.RowPosSstIndex;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.lake.source.RowPosResult;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.flink.tiering.committer.TieringEpochFencingTest.failTable;
import static org.apache.fluss.flink.tiering.committer.TieringEpochFencingTest.takeTable;
import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the steps of a round of a table with deletion vectors and the order they run in. */
class DerivedDvRoundTest extends TieringTestBase {

    private Configuration flussConfig;
    private Configuration lakeTieringConfig;
    private FlussTableLakeSnapshotCommitter snapshotCommitter;
    private FakeLakeCommitter lakeCommitter;
    private FakeLakeTieringFactory lakeTieringFactory;

    @BeforeEach
    protected void beforeEach() throws Exception {
        super.beforeEach();
        flussConfig = new Configuration(clientConf);
        flussConfig.set(ConfigOptions.REMOTE_DATA_DIR, FLUSS_CLUSTER_EXTENSION.getRemoteDataDir());
        lakeTieringConfig = new Configuration();
        snapshotCommitter = new FlussTableLakeSnapshotCommitter(flussConfig);
        snapshotCommitter.open();
        lakeCommitter = new FakeLakeCommitter();
        lakeTieringFactory = new FakeLakeTieringFactory(lakeCommitter);
    }

    @AfterEach
    void afterEach() throws Exception {
        snapshotCommitter.close();
    }

    /**
     * The trim runs only after the round is registered in Fluss. Before that, a restarted tablet
     * server rebuilds from the registered snapshot, which an earlier trim must leave rebuildable.
     */
    @Test
    void testTrimRunsOnlyAfterTheRoundIsRegistered() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "trim_after_registration");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        lakeTieringConfig.set(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_TRIM_INTERVAL, 1);
        long epoch = takeTable(tableId);
        snapshotCommitter.fenceOnTieringEpoch(tableId, epoch);
        FsPath checkpoints = new FsPath(lakeTableDir(tablePath), "rowPos/_ckpt");

        // the table is handed to another tiering service during the lake commit, so Fluss
        // rejects the round's registration
        lakeCommitter.onCommitWithDeletes = () -> failTable(tableId, epoch);
        assertThatThrownBy(() -> commitRound(tablePath, roundResults(tableId, tablePath, 10)))
                .satisfies(TieringEpochFencingTest::assertFenced);
        assertThat(checkpoints.getFileSystem().exists(checkpoints)).isFalse();

        // a registered round trims, which checkpoints the bucket first
        lakeCommitter.onCommitWithDeletes = null;
        TablePath registeredPath = TablePath.of(DEFAULT_DB, "trim_after_registration_ok");
        long registeredId = createTable(registeredPath, DEFAULT_PK_TABLE_DESCRIPTOR);
        snapshotCommitter.fenceOnTieringEpoch(registeredId, takeTable(registeredId));
        commitRound(registeredPath, roundResults(registeredId, registeredPath, 20));
        FsPath registeredCheckpoints = new FsPath(lakeTableDir(registeredPath), "rowPos/_ckpt");
        assertThat(registeredCheckpoints.getFileSystem().exists(registeredCheckpoints)).isTrue();
    }

    /**
     * The trim interval counts the changes registered in the index store, not the rounds of one
     * committer, so a tiering service that fails over more often than the interval still trims.
     */
    @Test
    void testTrimIntervalHoldsAcrossCommitterRestarts() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "trim_across_restarts");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        lakeTieringConfig.set(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_TRIM_INTERVAL, 2);
        snapshotCommitter.fenceOnTieringEpoch(tableId, takeTable(tableId));
        FsPath checkpoints = new FsPath(lakeTableDir(tablePath), "rowPos/_ckpt");

        // every round runs on a new committer, as after a failover
        commitRound(tablePath, roundResults(tableId, tablePath, 10));
        assertThat(exists(checkpoints)).isFalse();
        commitRound(tablePath, roundResults(tableId, tablePath, 20));
        assertThat(exists(checkpoints)).isTrue();
    }

    /**
     * The count of changes since the last trim restarts once a trim begins, also across committers,
     * so a table is trimmed every interval rather than on every round past it.
     */
    @Test
    void testTrimCountRestartsAfterATrim() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "trim_count_restarts");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        lakeTieringConfig.set(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_TRIM_INTERVAL, 2);
        snapshotCommitter.fenceOnTieringEpoch(tableId, takeTable(tableId));
        PositionIndexStore store = PositionIndexStore.forTable(lakeTableDir(tablePath), null);

        List<Long> counts = new ArrayList<>();
        for (int round = 1; round <= 5; round++) {
            commitRound(tablePath, roundResults(tableId, tablePath, 10L * round));
            counts.add(store.changesSinceTrim());
        }
        // trims began at the second and the fourth round's changes
        assertThat(counts).containsExactly(1L, 0L, 1L, 0L, 1L);
    }

    /**
     * A trim marker cut short by a crash, as an overwrite on HDFS or a local file system can leave
     * it, does not stop later trims.
     */
    @Test
    void testCutTrimMarkerDoesNotStopTrims() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "cut_trim_marker");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        lakeTieringConfig.set(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_TRIM_INTERVAL, 2);
        snapshotCommitter.fenceOnTieringEpoch(tableId, takeTable(tableId));
        commitRound(tablePath, roundResults(tableId, tablePath, 10));
        commitRound(tablePath, roundResults(tableId, tablePath, 20));

        // empty every marker the trim wrote
        FsPath rowPos = new FsPath(lakeTableDir(tablePath), "rowPos");
        FileSystem fs = rowPos.getFileSystem();
        List<FsPath> markers = new ArrayList<>();
        for (FileStatus st : fs.listStatus(rowPos)) {
            String name = st.getPath().getName();
            if (name.equals("_trims")) {
                for (FileStatus marker : fs.listStatus(st.getPath())) {
                    markers.add(marker.getPath());
                }
            } else if (name.startsWith("_trim") && !name.equals("_trimmed")) {
                markers.add(st.getPath());
            }
        }
        assertThat(markers).isNotEmpty();
        for (FsPath marker : markers) {
            fs.create(marker, FileSystem.WriteMode.OVERWRITE).close();
        }

        commitRound(tablePath, roundResults(tableId, tablePath, 30));
        commitRound(tablePath, roundResults(tableId, tablePath, 40));
        PositionIndexStore store = PositionIndexStore.forTable(lakeTableDir(tablePath), null);
        // a trim began at the fourth round's change
        assertThat(store.changesSinceTrim()).isZero();
    }

    /**
     * A round checks its tiering epoch before it commits its own compaction to the lake, which is a
     * lake commit of its own.
     */
    @Test
    void testEpochIsCheckedBeforeTheRoundsCompactionCommit() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "epoch_before_compaction_commit");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        long epoch = takeTable(tableId);
        snapshotCommitter.fenceOnTieringEpoch(tableId, epoch);
        commitRound(tablePath, roundResults(tableId, tablePath, 10));
        int rewrites = lakeCommitter.pendingRewrites.get();

        failTable(tableId, epoch);
        assertThatThrownBy(() -> commitRound(tablePath, roundResults(tableId, tablePath, 20)))
                .satisfies(TieringEpochFencingTest::assertFenced);
        assertThat(lakeCommitter.pendingRewrites).hasValue(rewrites);
    }

    /**
     * A round whose buckets advanced without writing any row, e.g. over records the writer folds
     * away, registers the advanced offsets, so the next round does not read the log again. A round
     * that advanced nothing commits nothing.
     */
    @Test
    void testRoundWithoutRowsRegistersItsAdvancedOffsets() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "round_without_rows");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        snapshotCommitter.fenceOnTieringEpoch(tableId, takeTable(tableId));
        commitRound(tablePath, roundResults(tableId, tablePath, 10));
        int commits = lakeCommitter.commitsWithDeletes.get();

        commitRound(tablePath, emptyRoundResults(tableId, tablePath, 15));
        LakeSnapshot registered = admin.getLatestLakeSnapshot(tablePath).get();
        assertThat(registered.getTableBucketsOffset())
                .containsEntry(new TableBucket(tableId, 0), 15L);
        assertThat(lakeCommitter.commitsWithDeletes).hasValue(commits + 1);

        // an idle round: its bucket ends at the registered offset
        commitRound(tablePath, emptyRoundResults(tableId, tablePath, 15));
        assertThat(lakeCommitter.commitsWithDeletes).hasValue(commits + 1);
        assertThat(admin.getLatestLakeSnapshot(tablePath).get().getSnapshotId())
                .isEqualTo(registered.getSnapshotId());
    }

    /**
     * A registration that crashed between its chain marker and its re-list left a fork at the
     * chain's end. The next round of a lake that merges rows by primary key (Paimon) reads the
     * chain before it registers anything: it repairs the fork first, keeping the branch whose
     * snapshot Fluss registered.
     */
    @Test
    void testMergingRoundRepairsAForkByFlussRegistration() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "merging_fork");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        mergingLake();
        snapshotCommitter.fenceOnTieringEpoch(tableId, takeTable(tableId));
        commitRound(tablePath, roundResults(tableId, tablePath, 10));
        commitRound(tablePath, roundResults(tableId, tablePath, 20));
        assertForkIsRepairedByTheNextRound(tableId, tablePath, 30);
    }

    /** The same for a lake that keeps every row version (Iceberg). */
    @Test
    void testRoundRepairsAForkByFlussRegistration() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "round_fork");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        snapshotCommitter.fenceOnTieringEpoch(tableId, takeTable(tableId));
        commitRound(tablePath, roundResults(tableId, tablePath, 10));
        commitRound(tablePath, roundResults(tableId, tablePath, 20));
        assertForkIsRepairedByTheNextRound(tableId, tablePath, 30);
    }

    private void assertForkIsRepairedByTheNextRound(
            long tableId, TablePath tablePath, long logEndOffset) throws Exception {
        PositionIndexStore store = PositionIndexStore.forTable(lakeTableDir(tablePath), null);
        List<PositionIndexStore.ChainEntry> chain = store.chain();
        PositionIndexStore.ChainEntry end = chain.get(chain.size() - 1);
        long registered = admin.getLatestLakeSnapshot(tablePath).get().getSnapshotId();
        assertThat(end.snapshotId).isEqualTo(registered);
        // the marker a crashed racer left: the same sequence, another step
        long other = registered + 1_000_000;
        FsPath marker =
                new FsPath(
                        new FsPath(lakeTableDir(tablePath), "rowPos/_chain"),
                        String.format(
                                "v1-%020d_%d_%d_%d_%d",
                                end.sequence, end.parentSnapshotId, other, 987654321L, -1));
        marker.getFileSystem().create(marker, FileSystem.WriteMode.NO_OVERWRITE).close();
        assertThatThrownBy(store::chain).hasMessageContaining("being repaired");

        commitRound(tablePath, roundResults(tableId, tablePath, logEndOffset));

        List<Long> snapshots = new ArrayList<>();
        for (PositionIndexStore.ChainEntry e : store.chain()) {
            snapshots.add(e.snapshotId);
        }
        assertThat(snapshots).contains(registered).doesNotContain(other);
        assertThat(admin.getLatestLakeSnapshot(tablePath).get().getTableBucketsOffset())
                .containsEntry(new TableBucket(tableId, 0), logEndOffset);
    }

    /**
     * A round registered in Fluss whose table is handed to another tiering service before its trim
     * does not trim: the trim checks the tiering epoch first, as a paused service's would.
     */
    @Test
    void testRoundFencedAfterItsRegistrationDoesNotTrim() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "trim_fenced_after_registration");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        lakeTieringConfig.set(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_TRIM_INTERVAL, 1);
        long epoch = takeTable(tableId);
        snapshotCommitter.close();
        snapshotCommitter =
                new FlussTableLakeSnapshotCommitter(flussConfig) {
                    @Override
                    public void commit(
                            long tableId,
                            TablePath tablePath,
                            org.apache.fluss.lake.committer.LakeCommitResult lakeCommitResult,
                            String lakeBucketTieredOffsetsPath,
                            Map<TableBucket, Long> logEndOffsets,
                            Map<TableBucket, Long> logMaxTieredTimestamps)
                            throws IOException {
                        super.commit(
                                tableId,
                                tablePath,
                                lakeCommitResult,
                                lakeBucketTieredOffsetsPath,
                                logEndOffsets,
                                logMaxTieredTimestamps);
                        // the table is handed away right after the registration
                        try {
                            failTable(tableId, epoch);
                        } catch (Exception e) {
                            throw new IOException(e);
                        }
                    }
                };
        snapshotCommitter.open();
        snapshotCommitter.fenceOnTieringEpoch(tableId, epoch);

        commitRound(tablePath, roundResults(tableId, tablePath, 10));

        FsPath checkpoints = new FsPath(lakeTableDir(tablePath), "rowPos/_ckpt");
        assertThat(checkpoints.getFileSystem().exists(checkpoints)).isFalse();
        FsPath records = new FsPath(lakeTableDir(tablePath), "rowPos/_trimmed");
        assertThat(records.getFileSystem().exists(records)).isFalse();
    }

    /**
     * Fluss keeps a table's lake snapshot registrations from the chain's oldest snapshot as the
     * round saw it before its trim. So they may lag the chain by a round, but never run ahead of
     * it: every snapshot on the chain stays registered, and a server rebuilding from the chain's
     * start finds its offsets.
     */
    @Test
    void testRegistrationsNeverRunAheadOfTheChain() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "registrations_follow_chain");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        lakeTieringConfig.set(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_TRIM_INTERVAL, 1);
        lakeTieringConfig.set(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_TRIM_RETAIN, 0);
        snapshotCommitter.fenceOnTieringEpoch(tableId, takeTable(tableId));
        PositionIndexStore store = PositionIndexStore.forTable(lakeTableDir(tablePath), null);
        for (int round = 1; round <= 5; round++) {
            List<PositionIndexStore.ChainEntry> before = store.chain();
            if (!before.isEmpty()) {
                // the bucket's leader checkpoints the chain's end, so the trim can cut up to it
                store.writeCheckpoint(
                        before.get(before.size() - 1).snapshotId,
                        new TableBucket(tableId, 0),
                        null,
                        Collections.emptyMap());
            }
            commitRound(tablePath, roundResults(tableId, tablePath, 10L * round));
            List<Long> chain = new ArrayList<>();
            for (PositionIndexStore.ChainEntry e : store.chain()) {
                chain.add(e.snapshotId);
            }
            List<Long> registered = new ArrayList<>();
            for (org.apache.fluss.server.zk.data.lake.LakeTable.LakeSnapshotMetadata m :
                    FLUSS_CLUSTER_EXTENSION
                            .getZooKeeperClient()
                            .getLakeTable(tableId)
                            .get()
                            .getLakeSnapshotMetadatas()) {
                registered.add(m.getSnapshotId());
            }
            assertThat(registered).as("round %s", round).containsAll(chain);
        }
        assertThat(store.chainTrimmed()).isTrue();
    }

    /** The same for a lake that merges rows by primary key (Paimon). */
    @Test
    void testMergingRoundWithoutRowsRegistersItsAdvancedOffsets() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "merging_round_without_rows");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        mergingLake();
        snapshotCommitter.fenceOnTieringEpoch(tableId, takeTable(tableId));
        commitRound(tablePath, roundResults(tableId, tablePath, 10));
        int commits = lakeCommitter.commits.get();

        commitRound(tablePath, emptyRoundResults(tableId, tablePath, 15));
        assertThat(admin.getLatestLakeSnapshot(tablePath).get().getTableBucketsOffset())
                .containsEntry(new TableBucket(tableId, 0), 15L);
        assertThat(lakeCommitter.commits).hasValue(commits + 1);
    }

    /**
     * An idle round of a lake that merges rows by primary key marks partitions done, and commits
     * when that changed their state.
     */
    @Test
    void testIdleMergingRoundMarksPartitionsDone() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "idle_merging_round_mark_done");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        mergingLake();
        lakeTieringConfig.set(ConfigOptions.LAKE_TIERING_PARTITION_MARK_DONE_ENABLED, true);
        snapshotCommitter.fenceOnTieringEpoch(tableId, takeTable(tableId));
        commitRound(tablePath, roundResults(tableId, tablePath, 10));
        int commits = lakeCommitter.commits.get();
        int markDones = lakeCommitter.markDones.get();

        // nothing to mark done: no commit
        commitRound(tablePath, emptyRoundResults(tableId, tablePath, 10));
        assertThat(lakeCommitter.markDones.get()).isGreaterThan(markDones);
        assertThat(lakeCommitter.commits).hasValue(commits);

        lakeCommitter.markDoneChanges = true;
        commitRound(tablePath, emptyRoundResults(tableId, tablePath, 10));
        assertThat(lakeCommitter.commits).hasValue(commits + 1);
    }

    /**
     * A merging round whose lake commit the lake surely rejected deletes its data files and offsets
     * file. One whose outcome is unknown keeps them, also when the lake shows no commit of the
     * round: the commit may still land after the call failed, e.g. on a REST catalog.
     */
    @Test
    void testMergingCommitDeletesItsFilesOnlyIfRejected() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "merging_commit_rejected");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        mergingLake();
        snapshotCommitter.fenceOnTieringEpoch(tableId, takeTable(tableId));

        lakeCommitter.onCommit =
                () -> {
                    throw new LakeCommitRejectedException(
                            "rejected", new IOException("commit conflict"));
                };
        assertThatThrownBy(() -> commitRound(tablePath, roundResults(tableId, tablePath, 10)))
                .hasStackTraceContaining("commit conflict");
        assertThat(lakeCommitter.aborts).hasValue(1);
        assertThat(exists(offsetsFileOfLastCommit())).isFalse();

        // the outcome is unknown, and the lake has no commit of the round yet
        lakeCommitter.onCommit =
                () -> {
                    throw new IOException("lost connection");
                };
        assertThatThrownBy(() -> commitRound(tablePath, roundResults(tableId, tablePath, 10)))
                .hasStackTraceContaining("lost connection");
        assertThat(lakeCommitter.aborts).hasValue(1);
        assertThat(exists(offsetsFileOfLastCommit())).isTrue();
    }

    /**
     * A merging commit that landed, with a commit of another Fluss committer (the table's new
     * owner) on top of it before the failed call returned, keeps its files. The lake's latest Fluss
     * commit then records another offsets file, which must not be taken for "did not land".
     */
    @Test
    void testMergingCommitLandedUnderAnotherFlussCommitKeepsItsFiles() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "merging_landed_under_other");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        mergingLake();
        snapshotCommitter.fenceOnTieringEpoch(tableId, takeTable(tableId));

        lakeCommitter.onCommit =
                () -> {
                    // this round's commit landed as snapshot `landed`; the other committer's
                    // commit landed on top of it as `landed + 1`
                    long landed = lakeCommitter.snapshots.get() + 1;
                    Map<String, String> other = new HashMap<>(lakeCommitter.lastProperties);
                    other.put(
                            FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY,
                            "file:/tmp/other-committer.offsets");
                    lakeCommitter.missing = new CommittedLakeSnapshot(landed + 1, other);
                    throw new IOException("lost connection");
                };
        assertThatThrownBy(() -> commitRound(tablePath, roundResults(tableId, tablePath, 10)))
                .hasStackTraceContaining("lost connection");
        assertThat(lakeCommitter.aborts).hasValue(0);
        assertThat(exists(offsetsFileOfLastCommit())).isTrue();
    }

    /**
     * A re-index whose splits of one bucket hold two live rows of one key fails instead of
     * publishing one of them: the table has duplicates, and either row would be left unmasked. A
     * re-index without duplicates is published.
     */
    @Test
    void testReindexWithTwoLiveRowsOfOneKeyAcrossSplitsFails() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "reindex_duplicate_across_splits");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        snapshotCommitter.fenceOnTieringEpoch(tableId, takeTable(tableId));
        commitRound(tablePath, roundResults(tableId, tablePath, 10));
        long base = admin.getLatestLakeSnapshot(tablePath).get().getSnapshotId();
        PositionIndexStore store = PositionIndexStore.forTable(lakeTableDir(tablePath), null);
        TableBucket bucket = new TableBucket(tableId, 0);

        Map<TableBucket, List<RowPosResult>> duplicates = new HashMap<>();
        duplicates.put(
                bucket,
                Arrays.asList(
                        splitResult(store, 55L, bucket, 0, 100, "a.parquet", "k1", "k2"),
                        splitResult(store, 55L, bucket, 1, 101, "b.parquet", "k0", "k2")));
        assertThatThrownBy(
                        () ->
                                resumeRound(
                                        tablePath,
                                        new Reindex(
                                                tablePath,
                                                base,
                                                base + 1,
                                                55L,
                                                null,
                                                Collections.emptyList(),
                                                2,
                                                System.currentTimeMillis(),
                                                null),
                                        duplicates))
                .hasStackTraceContaining("two live rows");
        assertThat(store.chain()).extracting(e -> e.snapshotId).containsExactly(base);

        Map<TableBucket, List<RowPosResult>> distinct = new HashMap<>();
        distinct.put(
                bucket,
                Arrays.asList(
                        splitResult(store, 56L, bucket, 0, 100, "a.parquet", "k1", "k2"),
                        splitResult(store, 56L, bucket, 1, 101, "b.parquet", "k0", "k3")));
        resumeRound(
                tablePath,
                new Reindex(
                        tablePath,
                        base,
                        base + 1,
                        56L,
                        null,
                        Collections.emptyList(),
                        2,
                        System.currentTimeMillis(),
                        null),
                distinct);
        assertThat(store.chain()).extracting(e -> e.snapshotId).containsExactly(base, base + 1);
    }

    /**
     * A round whose lake commit the lake surely rejected deletes its data files, its offsets file
     * and its index change. One whose outcome is unknown keeps them, also when the lake shows no
     * commit of the round: the commit may still land after the call failed, e.g. on a REST catalog.
     */
    @Test
    void testFailedLakeCommitDeletesItsFilesOnlyIfRejected() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "failed_lake_commit");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        snapshotCommitter.fenceOnTieringEpoch(tableId, takeTable(tableId));
        commitRound(tablePath, roundResults(tableId, tablePath, 10));
        int aborts = lakeCommitter.aborts.get();

        lakeCommitter.onCommitWithDeletes =
                () -> {
                    throw new LakeCommitRejectedException(
                            "rejected", new IOException("conflicting commit"));
                };
        assertThatThrownBy(() -> commitRound(tablePath, roundResults(tableId, tablePath, 20)))
                .hasStackTraceContaining("conflicting commit");
        assertThat(lakeCommitter.aborts).hasValue(aborts + 1);
        assertThat(exists(offsetsFileOfLastCommitWithDeletes())).isFalse();
        assertThat(exists(indexChangeOfLastCommitWithDeletes(tablePath))).isFalse();

        // the outcome is unknown, and the lake has no commit of the round yet
        lakeCommitter.onCommitWithDeletes =
                () -> {
                    throw new IOException("lost connection");
                };
        assertThatThrownBy(() -> commitRound(tablePath, roundResults(tableId, tablePath, 20)))
                .hasStackTraceContaining("lost connection");
        assertThat(lakeCommitter.aborts).hasValue(aborts + 1);
        assertThat(exists(offsetsFileOfLastCommitWithDeletes())).isTrue();
        assertThat(exists(indexChangeOfLastCommitWithDeletes(tablePath))).isTrue();
    }

    /**
     * A round whose lake commit landed although the call failed leaves its snapshot unregistered.
     * The next round registers it in the chain and in Fluss from the properties the snapshot
     * records, deletes its own data files and fails, so it runs again on top of the snapshot.
     */
    @Test
    void testLostCommitIsRegisteredBeforeTheNextRoundCommits() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "lost_commit_registered");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        snapshotCommitter.fenceOnTieringEpoch(tableId, takeTable(tableId));
        commitRound(tablePath, roundResults(tableId, tablePath, 10));
        long base = admin.getLatestLakeSnapshot(tablePath).get().getSnapshotId();

        // the commit lands, then the call fails
        AtomicLong lost = new AtomicLong();
        lakeCommitter.onCommitWithDeletes =
                () -> {
                    lost.set(lakeCommitter.snapshots.incrementAndGet());
                    lakeCommitter.missing =
                            new CommittedLakeSnapshot(
                                    lost.get(), lakeCommitter.lastDeleteProperties);
                    throw new IOException("lost connection");
                };
        assertThatThrownBy(() -> commitRound(tablePath, roundResults(tableId, tablePath, 20)))
                .hasStackTraceContaining("lost connection");
        assertThat(admin.getLatestLakeSnapshot(tablePath).get().getSnapshotId()).isEqualTo(base);
        lakeCommitter.onCommitWithDeletes = null;
        int aborts = lakeCommitter.aborts.get();
        int commits = lakeCommitter.commitsWithDeletes.get();

        assertThatThrownBy(() -> commitRound(tablePath, roundResults(tableId, tablePath, 20)))
                .hasMessageContaining("Repaired a lake snapshot left unregistered");
        assertThat(lakeCommitter.commitsWithDeletes).hasValue(commits);
        assertThat(lakeCommitter.aborts).hasValue(aborts + 1);
        LakeSnapshot registered = admin.getLatestLakeSnapshot(tablePath).get();
        assertThat(registered.getSnapshotId()).isEqualTo(lost.get());
        assertThat(registered.getTableBucketsOffset())
                .containsEntry(new TableBucket(tableId, 0), 20L);
        PositionIndexStore store = PositionIndexStore.forTable(lakeTableDir(tablePath), null);
        assertThat(store.chain()).extracting(e -> e.snapshotId).containsExactly(base, lost.get());

        lakeCommitter.missing = null;
        commitRound(tablePath, roundResults(tableId, tablePath, 30));
        assertThat(store.chain())
                .extracting(e -> e.snapshotId)
                .containsExactly(base, lost.get(), lakeCommitter.lastSnapshotId);
    }

    /**
     * A round's own compaction that committed without handing its positions over, as after a crash
     * between the two, is re-indexed from the lake and registered. The round then commits on top of
     * it.
     */
    @Test
    void testCompactionCommittedWithoutItsPositionsIsReindexed() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "compaction_not_handed_over");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        snapshotCommitter.fenceOnTieringEpoch(tableId, takeTable(tableId));
        commitRound(tablePath, roundResults(tableId, tablePath, 10));
        long base = admin.getLatestLakeSnapshot(tablePath).get().getSnapshotId();

        lakeCommitter.rewrites = true;
        lakeTieringFactory.lakeSource = new FakeLakeSource(base + 1);
        commitRound(tablePath, roundResults(tableId, tablePath, 20));

        PositionIndexStore store = PositionIndexStore.forTable(lakeTableDir(tablePath), null);
        assertThat(store.chain())
                .extracting(e -> e.snapshotId)
                .containsExactly(base, base + 1, base + 2);
        LakeSnapshot registered = admin.getLatestLakeSnapshot(tablePath).get();
        assertThat(registered.getSnapshotId()).isEqualTo(base + 2);
        assertThat(registered.getTableBucketsOffset())
                .containsEntry(new TableBucket(tableId, 0), 20L);
    }

    /** Writes a re-index split's SST of the keys, at positions 0, 1, ... of one file. */
    private static RowPosResult splitResult(
            PositionIndexStore store,
            long changeId,
            TableBucket bucket,
            int split,
            int fileId,
            String fileName,
            String... keys)
            throws IOException {
        TreeMap<byte[], FilePos> entries = new TreeMap<>(DerivedDvRoundTest::compareUnsigned);
        for (int i = 0; i < keys.length; i++) {
            entries.put(keys[i].getBytes(StandardCharsets.UTF_8), new FilePos(fileId, i));
        }
        PositionIndexStore.BucketPart part =
                store.writeBucket(
                        changeId, bucket, "s" + split + "_", entries.entrySet().iterator());
        List<RowPosResult.SstMeta> ssts = new ArrayList<>();
        for (RowPosSstIndex.SstFileEntry f : part.files) {
            ssts.add(new RowPosResult.SstMeta(f.getFileName(), f.getFileSize()));
        }
        return new RowPosResult(changeId, split, Collections.singletonMap(fileId, fileName), ssts);
    }

    private static int compareUnsigned(byte[] a, byte[] b) {
        for (int i = 0; i < Math.min(a.length, b.length); i++) {
            int c = Integer.compare(a[i] & 0xff, b[i] & 0xff);
            if (c != 0) {
                return c;
            }
        }
        return Integer.compare(a.length, b.length);
    }

    private void resumeRound(
            TablePath tablePath, Reindex reindex, Map<TableBucket, List<RowPosResult>> results)
            throws Exception {
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        new DerivedDvTieringCommitter<>(
                        admin,
                        flussConfig,
                        lakeTieringConfig,
                        lakeTieringFactory,
                        snapshotCommitter,
                        UnregisteredMetricsGroup.createOperatorMetricGroup(),
                        null)
                .resumeRound(
                        tableInfo.getTableId(),
                        reindex,
                        results,
                        Collections.<TableBucketWriteResult<TestingWriteResult>>emptyList());
    }

    private FsPath offsetsFileOfLastCommit() {
        Map<String, String> properties = lakeCommitter.lastProperties;
        assertThat(properties).containsKey(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY);
        return new FsPath(properties.get(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY));
    }

    private FsPath offsetsFileOfLastCommitWithDeletes() {
        Map<String, String> properties = lakeCommitter.lastDeleteProperties;
        assertThat(properties).containsKey(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY);
        return new FsPath(properties.get(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY));
    }

    private FsPath indexChangeOfLastCommitWithDeletes(TablePath tablePath) throws Exception {
        Map<String, String> properties = lakeCommitter.lastDeleteProperties;
        assertThat(properties).containsKey("fluss-dv-index-id");
        return new FsPath(lakeTableDir(tablePath), "rowPos/" + properties.get("fluss-dv-index-id"));
    }

    private static boolean exists(FsPath path) throws IOException {
        return path.getFileSystem().exists(path);
    }

    private void mergingLake() {
        lakeTieringFactory.merging = true;
        lakeTieringFactory.lakeSource = new FakeLakeSource(Long.MAX_VALUE);
    }

    private RoundOutcome<?> commitRound(
            TablePath tablePath, List<TableBucketWriteResult<TestingWriteResult>> results)
            throws Exception {
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        return new DerivedDvTieringCommitter<>(
                        admin,
                        flussConfig,
                        lakeTieringConfig,
                        lakeTieringFactory,
                        snapshotCommitter,
                        UnregisteredMetricsGroup.createOperatorMetricGroup(),
                        null)
                .commitRound(tableInfo.getTableId(), tablePath, tableInfo, results);
    }

    private FsPath lakeTableDir(TablePath tablePath) throws Exception {
        return PositionIndexStore.lakeTableDirOf(
                admin.getTableInfo(tablePath).get(),
                () -> FlussConfigUtils.getDefaultRemoteDataDir(flussConfig));
    }

    private static List<TableBucketWriteResult<TestingWriteResult>> roundResults(
            long tableId, TablePath tablePath, long logEndOffset) {
        return results(tableId, tablePath, new TestingWriteResult(1), logEndOffset);
    }

    private static List<TableBucketWriteResult<TestingWriteResult>> emptyRoundResults(
            long tableId, TablePath tablePath, long logEndOffset) {
        return results(tableId, tablePath, null, logEndOffset);
    }

    private static List<TableBucketWriteResult<TestingWriteResult>> results(
            long tableId,
            TablePath tablePath,
            @Nullable TestingWriteResult writeResult,
            long logEndOffset) {
        List<TableBucketWriteResult<TestingWriteResult>> results = new ArrayList<>();
        results.add(
                new TableBucketWriteResult<>(
                        tablePath,
                        new TableBucket(tableId, 0),
                        null,
                        writeResult,
                        logEndOffset,
                        logEndOffset,
                        1));
        return results;
    }
}
