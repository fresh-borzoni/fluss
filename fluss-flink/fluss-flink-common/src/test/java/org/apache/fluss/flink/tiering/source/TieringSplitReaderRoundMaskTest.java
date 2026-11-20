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

package org.apache.fluss.flink.tiering.source;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.client.tiering.DvRoundRequests;
import org.apache.fluss.client.tiering.TableBucketWriteResult;
import org.apache.fluss.client.tiering.TieringLogSplit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.flink.tiering.TestingLakeTieringFactory;
import org.apache.fluss.flink.tiering.TestingWriteResult;
import org.apache.fluss.flink.tiering.source.metrics.TieringMetrics;
import org.apache.fluss.flink.tiering.source.split.FlinkTieringSplit;
import org.apache.fluss.flink.utils.LogCapture;
import org.apache.fluss.lake.writer.DeletionVectorWriterInitContext;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.RoundMaskMismatchException;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The rounds of tables with deletion vectors that their buckets' leaders planned, as the tiering
 * reader reads them: through the leader's log mask, without it when the leader serves none, and
 * again without it when the mask does not match the round.
 */
class TieringSplitReaderRoundMaskTest {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(
                            new Configuration()
                                    .set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON))
                    .setNumOfTabletServers(1)
                    .build();

    private static final String DB = "round_masks";
    private static final int DEFAULT_BUCKET_NUM = 3;
    private static final Schema DEFAULT_PK_TABLE_SCHEMA =
            Schema.newBuilder()
                    .primaryKey("id")
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .build();
    private static final TableDescriptor DV_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(DEFAULT_PK_TABLE_SCHEMA)
                    .distributedBy(DEFAULT_BUCKET_NUM, "id")
                    .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                    .property(ConfigOptions.TABLE_DATALAKE_DELETION_VECTORS_ENABLED, true)
                    .build();
    private static final TableDescriptor PARTITIONED_DV_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(
                            Schema.newBuilder()
                                    .primaryKey("id", "p")
                                    .column("id", DataTypes.INT())
                                    .column("name", DataTypes.STRING())
                                    .column("p", DataTypes.STRING())
                                    .build())
                    .distributedBy(DEFAULT_BUCKET_NUM, "id")
                    .partitionedBy("p")
                    .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                    .property(ConfigOptions.TABLE_DATALAKE_DELETION_VECTORS_ENABLED, true)
                    .build();
    private static final TableDescriptor ONE_BUCKET_DV_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(DEFAULT_PK_TABLE_SCHEMA)
                    .distributedBy(1, "id")
                    .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                    .property(ConfigOptions.TABLE_DATALAKE_DELETION_VECTORS_ENABLED, true)
                    .build();
    private static final TableDescriptor PK_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(DEFAULT_PK_TABLE_SCHEMA)
                    .distributedBy(DEFAULT_BUCKET_NUM, "id")
                    .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                    .build();

    // a connection per test, as a test may restart the tablet server
    private Connection conn;
    private Admin admin;

    @BeforeEach
    void beforeEach() throws Exception {
        conn = ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig());
        admin = conn.getAdmin();
        admin.createDatabase(DB, DatabaseDescriptor.EMPTY, true).get();
    }

    @AfterEach
    void afterEach() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }

    /**
     * A planned round of a table with deletion vectors is read through its leader's log mask: the
     * lake writer gets only the versions live at the round's end, without -U records, and knows it.
     */
    @Test
    void testPlannedRoundIsReadThroughItsLeadersMask() throws Exception {
        TablePath tablePath = TablePath.of(DB, "tiering_masked_rounds");
        long tableId = createTable(tablePath, DV_TABLE_DESCRIPTOR);
        Map<Integer, String> live = writeVersions(tablePath, 30, 3);
        Map<TableBucket, TieringLogSplit> plans = planRounds(tablePath, tableId);
        RecordingLakeTieringFactory factory = new RecordingLakeTieringFactory();
        MetricListener metrics = new MetricListener();
        readRounds(tablePath, plans, factory, metrics, new Configuration(), null);

        Map<Integer, String> written = new HashMap<>();
        for (RecordingWriter writer : factory.writers) {
            assertThat(writer.masked).isTrue();
            assertThat(writer.completed).isTrue();
            for (Object[] record : writer.records) {
                assertThat(record[1]).isEqualTo(ChangeType.UPDATE_AFTER);
                assertThat(written.put((Integer) record[2], (String) record[3])).isNull();
            }
        }
        assertThat(written).isEqualTo(live);
        assertThat(counter(metrics, MetricNames.TIERING_SERVICE_DV_ROUND_MASK_MISMATCHES)).isZero();
    }

    /**
     * The leader restarts between the round's plan and its read: the new leader replays its change
     * list from the log, the reader waits for it, and the mask it serves is that of the planned
     * round, so the writer still gets only the live versions.
     */
    @Test
    void testLeaderRestartBetweenPlanAndRead() throws Exception {
        TablePath tablePath = TablePath.of(DB, "tiering_restarted_leader_rounds");
        long tableId = createTable(tablePath, DV_TABLE_DESCRIPTOR);
        Map<Integer, String> live = writeVersions(tablePath, 30, 3);
        Map<TableBucket, TieringLogSplit> plans = planRounds(tablePath, tableId);
        FLUSS_CLUSTER_EXTENSION.restartTabletServer(0, new Configuration());
        for (TableBucket bucket : plans.keySet()) {
            FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(bucket);
        }
        RecordingLakeTieringFactory factory = new RecordingLakeTieringFactory();
        MetricListener metrics = new MetricListener();
        readRounds(tablePath, plans, factory, metrics, new Configuration(), null);

        Map<Integer, String> written = new HashMap<>();
        for (RecordingWriter writer : factory.writers) {
            assertThat(writer.masked).isTrue();
            for (Object[] record : writer.records) {
                assertThat(written.put((Integer) record[2], (String) record[3])).isNull();
            }
        }
        assertThat(written).isEqualTo(live);
        assertThat(counter(metrics, MetricNames.TIERING_SERVICE_DV_ROUND_MASK_MISMATCHES)).isZero();
    }

    /**
     * With the masks off, every planned round is written in sort mode: the lake writer gets every
     * record of the round and keeps the last version of each key itself.
     */
    @Test
    void testRoundsAreWrittenInSortModeWithTheMasksOff() throws Exception {
        TablePath tablePath = TablePath.of(DB, "tiering_sort_mode_rounds");
        long tableId = createTable(tablePath, DV_TABLE_DESCRIPTOR);
        writeVersions(tablePath, 30, 2);
        Map<TableBucket, TieringLogSplit> plans = planRounds(tablePath, tableId);
        Configuration masksOff = new Configuration();
        masksOff.set(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_ROUND_MASK_ENABLED, false);
        RecordingLakeTieringFactory factory = new RecordingLakeTieringFactory();
        MetricListener metrics = new MetricListener();
        readRounds(
                tablePath,
                plans,
                factory,
                metrics,
                masksOff,
                (split, fetched) -> {
                    throw new AssertionError("no mask is fetched with the masks off");
                });

        assertWrittenWhole(factory, plans);
        assertThat(counter(metrics, MetricNames.TIERING_SERVICE_DV_ROUND_MASK_MISMATCHES)).isZero();
    }

    /**
     * A mask that does not match its round before the round is written, e.g. one of another range,
     * has the round written in sort mode, logged and counted once a round.
     */
    @Test
    void testRoundWhoseMaskDoesNotMatchIsWrittenInSortMode() throws Exception {
        TablePath tablePath = TablePath.of(DB, "tiering_mismatched_rounds");
        long tableId = createTable(tablePath, DV_TABLE_DESCRIPTOR);
        writeVersions(tablePath, 30, 2);
        Map<TableBucket, TieringLogSplit> plans = planRounds(tablePath, tableId);
        RecordingLakeTieringFactory factory = new RecordingLakeTieringFactory();
        MetricListener metrics = new MetricListener();
        try (LogCapture log = LogCapture.of(TieringSplitReader.class.getName())) {
            readRounds(
                    tablePath,
                    plans,
                    factory,
                    metrics,
                    new Configuration(),
                    (split, fetched) -> {
                        CompletableFuture<RoundMask.Fetched> failed = new CompletableFuture<>();
                        failed.completeExceptionally(
                                new RoundMaskMismatchException("of another range"));
                        return failed;
                    });
            assertThat(log.messagesWith("of another range", "is written in sort mode"))
                    .hasSize(DEFAULT_BUCKET_NUM);
        }
        assertWrittenWhole(factory, plans);
        assertThat(counter(metrics, MetricNames.TIERING_SERVICE_DV_ROUND_MASK_MISMATCHES))
                .isEqualTo(DEFAULT_BUCKET_NUM);
    }

    /**
     * A leader that serves no mask, here as the table has no deletion vectors, fails the round, as
     * it would fail the round's deletes; the round is planned and tiered again.
     */
    @Test
    void testALeaderThatServesNoMaskFailsTheRound() throws Exception {
        TablePath tablePath = TablePath.of(DB, "tiering_unmasked_rounds");
        long tableId = createTable(tablePath, PK_TABLE_DESCRIPTOR);
        writeVersions(tablePath, 30, 2);
        Map<TableBucket, TieringLogSplit> plans = new HashMap<>();
        for (int bucket = 0; bucket < DEFAULT_BUCKET_NUM; bucket++) {
            TableBucket tb = new TableBucket(tableId, bucket);
            plans.put(
                    tb,
                    TieringLogSplit.plannedRound(
                            tablePath, tb, null, 0, highWatermark(tablePath, bucket), -1, false));
        }
        assertThatThrownBy(
                        () ->
                                readRounds(
                                        tablePath,
                                        plans,
                                        new RecordingLakeTieringFactory(),
                                        new MetricListener(),
                                        new Configuration(),
                                        null))
                .hasMessageContaining("Failed to get the log mask of the round")
                .hasRootCauseInstanceOf(InvalidTableException.class);
    }

    /**
     * A round whose changes are not as many as its leader counted does not match its mask. The
     * reader finds it at the round's end, closes the round's writer without completing it, and
     * reads the round again without the mask: the round's result has every record, and the mismatch
     * is counted.
     */
    @Test
    void testRoundWhoseChangesDoNotMatchIsReadAgainWithoutItsMask() throws Exception {
        TablePath tablePath = TablePath.of(DB, "tiering_miscounted_rounds");
        long tableId = createTable(tablePath, DV_TABLE_DESCRIPTOR);
        writeVersions(tablePath, 30, 2);
        Map<TableBucket, TieringLogSplit> plans = planRounds(tablePath, tableId);
        RecordingLakeTieringFactory factory = new RecordingLakeTieringFactory();
        MetricListener metrics = new MetricListener();
        try (LogCapture log = LogCapture.of(TieringSplitReader.class.getName())) {
            readRounds(
                    tablePath,
                    plans,
                    factory,
                    metrics,
                    new Configuration(),
                    (split, fetched) ->
                            fetched.thenApply(TieringSplitReaderRoundMaskTest::miscounted));
            assertThat(log.messagesWith("counts", "is read again from offset 0", "sort mode"))
                    .hasSize(DEFAULT_BUCKET_NUM);
        }

        Map<TableBucket, List<RecordingWriter>> writers = new HashMap<>();
        for (RecordingWriter writer : factory.writers) {
            writers.computeIfAbsent(writer.bucket, b -> new ArrayList<>()).add(writer);
        }
        assertThat(writers).hasSize(DEFAULT_BUCKET_NUM);
        for (Map.Entry<TableBucket, List<RecordingWriter>> e : writers.entrySet()) {
            List<RecordingWriter> bucketWriters = e.getValue();
            assertThat(bucketWriters).hasSize(2);
            RecordingWriter first = bucketWriters.get(0);
            assertThat(first.masked).isTrue();
            assertThat(first.completed).isFalse();
            assertThat(first.closed).isTrue();
            RecordingWriter second = bucketWriters.get(1);
            assertThat(second.masked).isFalse();
            assertThat(second.completed).isTrue();
            assertThat(second.records).hasSize((int) plans.get(e.getKey()).getStoppingOffset());
        }
        assertThat(counter(metrics, MetricNames.TIERING_SERVICE_DV_ROUND_MASK_MISMATCHES))
                .isEqualTo(DEFAULT_BUCKET_NUM);
    }

    /**
     * A mask that misses a version passes the reader's checks, as the version is read and the
     * round's changes are as many as its leader counted. The round's writer finds the key twice as
     * it completes; the reader reads the round again from its start without the mask, and the
     * mismatch is counted.
     */
    @Test
    void testRoundWhoseWriterFindsAKeyTwiceIsReadAgainInSortMode() throws Exception {
        TablePath tablePath = TablePath.of(DB, "tiering_key_twice_rounds");
        long tableId = createTable(tablePath, DV_TABLE_DESCRIPTOR);
        Map<Integer, String> live = writeVersions(tablePath, 30, 2);
        Map<TableBucket, TieringLogSplit> plans = planRounds(tablePath, tableId);
        RecordingLakeTieringFactory factory = new RecordingLakeTieringFactory();
        MetricListener metrics = new MetricListener();
        try (LogCapture log = LogCapture.of(TieringSplitReader.class.getName())) {
            readRounds(
                    tablePath,
                    plans,
                    factory,
                    metrics,
                    new Configuration(),
                    (split, fetched) ->
                            fetched.thenApply(TieringSplitReaderRoundMaskTest::missingAVersion));
            assertThat(log.messagesWith("comes twice", "is read again from offset 0", "sort mode"))
                    .hasSize(DEFAULT_BUCKET_NUM);
        }

        Map<TableBucket, List<RecordingWriter>> writers = new HashMap<>();
        for (RecordingWriter writer : factory.writers) {
            writers.computeIfAbsent(writer.bucket, b -> new ArrayList<>()).add(writer);
        }
        assertThat(writers).hasSize(DEFAULT_BUCKET_NUM);
        Map<Integer, String> written = new HashMap<>();
        for (Map.Entry<TableBucket, List<RecordingWriter>> e : writers.entrySet()) {
            List<RecordingWriter> bucketWriters = e.getValue();
            assertThat(bucketWriters).hasSize(2);
            RecordingWriter first = bucketWriters.get(0);
            assertThat(first.masked).isTrue();
            assertThat(first.completed).isFalse();
            assertThat(first.closed).isTrue();
            RecordingWriter second = bucketWriters.get(1);
            assertThat(second.masked).isFalse();
            assertThat(second.completed).isTrue();
            assertThat(second.records).hasSize((int) plans.get(e.getKey()).getStoppingOffset());
            for (Object[] record : second.records) {
                written.put((Integer) record[2], (String) record[3]);
            }
        }
        assertThat(written).isEqualTo(live);
        assertThat(counter(metrics, MetricNames.TIERING_SERVICE_DV_ROUND_MASK_MISMATCHES))
                .isEqualTo(DEFAULT_BUCKET_NUM);
    }

    /**
     * The planned rounds of a partition dropped before their masks are fetched end without writing
     * anything, so their buckets keep their offsets; the rounds of the other partition still run to
     * their planned ends. Those whose fetches find the partition gone end at once; past the time
     * limit all of them end, also any whose fetch the leader still answered.
     */
    @ParameterizedTest(name = "past the time limit {0}")
    @ValueSource(booleans = {false, true})
    void testRoundsOfADroppedPartitionEndWithoutWriting(boolean pastTheTimeLimit) throws Exception {
        TablePath tablePath =
                TablePath.of(DB, "tiering_dropped_partition_rounds_" + pastTheTimeLimit);
        admin.createTable(tablePath, PARTITIONED_DV_TABLE_DESCRIPTOR, true).get();
        long tableId = admin.getTableInfo(tablePath).get().getTableId();
        Map<TableBucket, TieringLogSplit> plans = new HashMap<>();
        Map<TableBucket, String> partitionNames = new HashMap<>();
        for (String partition : Arrays.asList("keep", "drop")) {
            admin.createPartition(
                            tablePath,
                            new PartitionSpec(Collections.singletonMap("p", partition)),
                            false)
                    .get();
        }
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (int version = 0; version < 2; version++) {
                for (int key = 0; key < 30; key++) {
                    writer.upsert(row(key, "v" + version, "keep"));
                    writer.upsert(row(key, "v" + version, "drop"));
                }
            }
            writer.flush();
        }
        List<Integer> buckets = new ArrayList<>();
        for (int bucket = 0; bucket < DEFAULT_BUCKET_NUM; bucket++) {
            buckets.add(bucket);
        }
        for (PartitionInfo partition : admin.listPartitionInfos(tablePath).get()) {
            String name = partition.getPartitionName();
            Map<Integer, Long> ends =
                    admin.listOffsets(tablePath, name, buckets, new OffsetSpec.LatestSpec())
                            .all()
                            .get();
            for (int bucket : buckets) {
                TableBucket tb = new TableBucket(tableId, partition.getPartitionId(), bucket);
                plans.put(
                        tb,
                        TieringLogSplit.plannedRound(
                                tablePath, tb, name, 0, ends.get(bucket), -1, false));
                partitionNames.put(tb, name);
            }
        }
        RecordingLakeTieringFactory factory = new RecordingLakeTieringFactory();
        Map<TableBucket, TableBucketWriteResult<TestingWriteResult>> results;
        try (Connection connection =
                ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig())) {
            // the reader's connection knows the partition, as when it was dropped after the
            // round was planned
            try (Table table = connection.getTable(tablePath);
                    LogScanner scanner = table.newScan().createLogScanner()) {
                for (TableBucket tb : plans.keySet()) {
                    scanner.subscribe(checkNotNull(tb.getPartitionId()), tb.getBucket(), 0);
                }
            }
            admin.dropPartition(
                            tablePath,
                            new PartitionSpec(Collections.singletonMap("p", "drop")),
                            false)
                    .get();
            results =
                    readRounds(
                            connection,
                            tableId,
                            plans,
                            factory,
                            pastTheTimeLimit,
                            (split, fetch) ->
                                    pastTheTimeLimit
                                                    || partitionNames
                                                            .get(split.getTableBucket())
                                                            .equals("keep")
                                            ? fetch.get()
                                            : failed(new PartitionNotExistException("dropped")));
        }

        assertThat(results).hasSize(plans.size());
        for (Map.Entry<TableBucket, TableBucketWriteResult<TestingWriteResult>> e :
                results.entrySet()) {
            TableBucketWriteResult<TestingWriteResult> result = e.getValue();
            if (partitionNames.get(e.getKey()).equals("drop")) {
                assertThat(result.logEndOffset()).isEqualTo(-1);
                assertThat(result.writeResult()).isNull();
            } else {
                assertThat(result.logEndOffset())
                        .isEqualTo(plans.get(e.getKey()).getStoppingOffset());
            }
        }
        for (RecordingWriter writer : factory.writers) {
            assertThat(partitionNames.get(writer.bucket)).isEqualTo("keep");
            assertThat(writer.completed).isTrue();
        }
    }

    /**
     * A round whose mask turns out not to match it, at its end (its leader's count of changes is
     * off) or in its middle (it masks a -U), is read again from its start and written in sort mode
     * whole, whatever the scanner had fetched past the round's end when the first read stopped:
     * here the log goes on after the round's end, and the scanner hands over a few records a poll
     * or many. The round commits up to its end only with every record of the round written, the
     * state at its end.
     */
    @ParameterizedTest(name = "max poll records {0}, log past the end {1}, mismatch {2}")
    @CsvSource({
        "500, false, atTheEnd",
        "500, true, atTheEnd",
        "7, false, atTheEnd",
        "7, true, atTheEnd",
        "7, true, beforeImage",
        "500, true, beforeImage",
    })
    void testRoundReadAgainInSortModeIsWrittenWhole(
            int maxPollRecords, boolean logPastTheEnd, String mismatch) throws Exception {
        TablePath tablePath =
                TablePath.of(
                        DB,
                        "tiering_redo_" + maxPollRecords + "_" + logPastTheEnd + "_" + mismatch);
        long tableId = createTable(tablePath, ONE_BUCKET_DV_TABLE_DESCRIPTOR, 1);
        TableBucket tb = new TableBucket(tableId, 0);
        // 60 keys, each written twice: 60 inserts, then 60 updates of a -U and a +U each
        Map<Integer, String> live = writeVersions(tablePath, 60, 2);
        Map<TableBucket, TieringLogSplit> plans = planRounds(tablePath, tableId, 1);
        long end = plans.get(tb).getStoppingOffset();
        assertThat(end).isEqualTo(180);
        if (logPastTheEnd) {
            writeVersions(tablePath, 300, 1);
        }
        // the -U at end - 2 is no version at all
        long beforeImage = end - 2;
        Configuration clientConf = new Configuration(FLUSS_CLUSTER_EXTENSION.getClientConfig());
        clientConf.set(ConfigOptions.CLIENT_SCANNER_LOG_MAX_POLL_RECORDS, maxPollRecords);
        RecordingLakeTieringFactory factory = new RecordingLakeTieringFactory();
        MetricListener metrics = new MetricListener();

        Map<TableBucket, TableBucketWriteResult<TestingWriteResult>> results =
                read(
                        clientConf,
                        plans,
                        factory,
                        metrics,
                        new Configuration(),
                        (split, fetched) ->
                                fetched.thenApply(
                                        mask -> {
                                            if (mismatch.equals("atTheEnd")) {
                                                return miscounted(mask);
                                            }
                                            mask.offsets.addLong(beforeImage);
                                            return mask;
                                        }),
                        null);

        TableBucketWriteResult<TestingWriteResult> result = results.get(tb);
        assertThat(result.logEndOffset()).isEqualTo(end);
        assertThat(factory.writers).hasSize(2);
        RecordingWriter first = factory.writers.get(0);
        assertThat(first.masked).isTrue();
        assertThat(first.completed).isFalse();
        assertThat(first.closed).isTrue();
        RecordingWriter second = factory.writers.get(1);
        assertThat(second.masked).isFalse();
        assertThat(second.completed).isTrue();
        List<Long> offsets = new ArrayList<>();
        Map<Integer, String> written = new HashMap<>();
        for (Object[] record : second.records) {
            offsets.add((Long) record[0]);
            if (record[1] != ChangeType.UPDATE_BEFORE) {
                written.put((Integer) record[2], (String) record[3]);
            }
        }
        List<Long> round = new ArrayList<>();
        for (long offset = 0; offset < end; offset++) {
            round.add(offset);
        }
        assertThat(offsets).isEqualTo(round);
        assertThat(written).isEqualTo(live);
        assertThat(checkNotNull(result.writeResult()).getWriteResult()).isEqualTo((int) end);
        assertThat(counter(metrics, MetricNames.TIERING_SERVICE_DV_ROUND_MASK_MISMATCHES))
                .isEqualTo(1);
    }

    /**
     * A log's offsets are not dense: a batch of deletes of keys that do not exist writes no record
     * but takes an offset. A round over such a log is read through its mask and completes; the
     * round's checks count its records and changes, not its offsets.
     */
    @Test
    void testRoundOverOffsetsWithoutRecordsCompletes() throws Exception {
        TablePath tablePath = TablePath.of(DB, "tiering_empty_batches");
        long tableId = createTable(tablePath, ONE_BUCKET_DV_TABLE_DESCRIPTOR, 1);
        TableBucket tb = new TableBucket(tableId, 0);
        writeVersions(tablePath, 10, 1);
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (int key = 100; key < 105; key++) {
                writer.delete(row(key, "absent"));
            }
            writer.flush();
        }
        Map<Integer, String> live = writeVersions(tablePath, 10, 1);
        Map<TableBucket, TieringLogSplit> plans = planRounds(tablePath, tableId, 1);
        long end = plans.get(tb).getStoppingOffset();
        // 10 inserts and 10 updates of a -U and a +U each: 30 records below the end
        assertThat(end).isGreaterThan(30);

        RecordingLakeTieringFactory factory = new RecordingLakeTieringFactory();
        MetricListener metrics = new MetricListener();
        readRounds(tablePath, plans, factory, metrics, new Configuration(), null);

        assertThat(factory.writers).hasSize(1);
        RecordingWriter writer = factory.writers.get(0);
        assertThat(writer.masked).isTrue();
        assertThat(writer.completed).isTrue();
        Map<Integer, String> written = new HashMap<>();
        for (Object[] record : writer.records) {
            assertThat(written.put((Integer) record[2], (String) record[3])).isNull();
        }
        assertThat(written).isEqualTo(live);
        assertThat(counter(metrics, MetricNames.TIERING_SERVICE_DV_ROUND_MASK_MISMATCHES)).isZero();
    }

    /**
     * Past the time limit, a planned round whose mask cannot be fetched as its leader is not
     * reachable ends without writing, so its table's round commits the other buckets and the reader
     * goes on; the bucket keeps its offset. Before the time limit the reader fetches the mask
     * again.
     */
    @Test
    void testRoundWhoseLeaderIsUnreachableEndsPastTheTimeLimit() throws Exception {
        TablePath tablePath = TablePath.of(DB, "tiering_unreachable_rounds");
        long tableId = createTable(tablePath, DV_TABLE_DESCRIPTOR);
        Map<Integer, String> live = writeVersions(tablePath, 30, 2);
        Map<TableBucket, TieringLogSplit> plans = planRounds(tablePath, tableId);
        TableBucket unreachable = new TableBucket(tableId, 0);
        RecordingLakeTieringFactory factory = new RecordingLakeTieringFactory();

        Map<TableBucket, TableBucketWriteResult<TestingWriteResult>> results =
                read(
                        FLUSS_CLUSTER_EXTENSION.getClientConfig(),
                        plans,
                        factory,
                        new MetricListener(),
                        new Configuration(),
                        (split, fetched) -> {
                            if (!split.getTableBucket().equals(unreachable)) {
                                return fetched;
                            }
                            CompletableFuture<RoundMask.Fetched> failed = new CompletableFuture<>();
                            failed.completeExceptionally(
                                    new NotLeaderOrFollowerException("no leader"));
                            return failed;
                        },
                        tableId);

        assertThat(results).hasSize(DEFAULT_BUCKET_NUM);
        for (Map.Entry<TableBucket, TableBucketWriteResult<TestingWriteResult>> e :
                results.entrySet()) {
            if (e.getKey().equals(unreachable)) {
                assertThat(e.getValue().logEndOffset()).isEqualTo(-1);
                assertThat(e.getValue().writeResult()).isNull();
            } else {
                assertThat(e.getValue().logEndOffset())
                        .isEqualTo(plans.get(e.getKey()).getStoppingOffset());
            }
        }
        for (RecordingWriter writer : factory.writers) {
            assertThat(writer.bucket).isNotEqualTo(unreachable);
        }
        assertThat(live).isNotEmpty();
    }

    /**
     * A planned round whose mask fetch finds its partition gone, while its table and bucket are
     * there, does not end: the reader fetches the mask again, and the round runs to its end.
     */
    @Test
    void testRoundWhoseBucketIsGoneOnlyToItsMaskFetchIsFetchedAgain() throws Exception {
        TablePath tablePath = TablePath.of(DB, "tiering_gone_to_the_fetch");
        long tableId = createTable(tablePath, DV_TABLE_DESCRIPTOR);
        writeVersions(tablePath, 30, 2);
        Map<TableBucket, TieringLogSplit> plans = planRounds(tablePath, tableId);
        TableBucket gone = new TableBucket(tableId, 2);
        List<TableBucket> fetches = new CopyOnWriteArrayList<>();

        RecordingLakeTieringFactory factory = new RecordingLakeTieringFactory();
        readRounds(
                tablePath,
                plans,
                factory,
                new MetricListener(),
                new Configuration(),
                (split, fetched) -> {
                    fetches.add(split.getTableBucket());
                    if (!split.getTableBucket().equals(gone)
                            || Collections.frequency(fetches, gone) > 2) {
                        return fetched;
                    }
                    CompletableFuture<RoundMask.Fetched> failed = new CompletableFuture<>();
                    failed.completeExceptionally(
                            new PartitionNotExistException("partition dropped"));
                    return failed;
                });

        assertThat(Collections.frequency(fetches, gone)).isEqualTo(3);
        assertThat(factory.writers).hasSize(plans.size());
        for (RecordingWriter writer : factory.writers) {
            assertThat(writer.masked).isTrue();
            assertThat(writer.completed).isTrue();
        }
    }

    /**
     * The planned rounds of a table dropped before their masks are fetched, whose fetches find the
     * table gone, end without writing at once, not at the time limit, and do not fail the reader.
     */
    @Test
    void testRoundsOfATableDroppedBeforeTheirMasksEndWithoutWriting() throws Exception {
        TablePath tablePath = TablePath.of(DB, "tiering_dropped_table_rounds");
        long tableId = createTable(tablePath, DV_TABLE_DESCRIPTOR);
        writeVersions(tablePath, 30, 2);
        Map<TableBucket, TieringLogSplit> plans = planRounds(tablePath, tableId);
        CompletableFuture<Void> dropped = new CompletableFuture<>();
        RecordingLakeTieringFactory factory = new RecordingLakeTieringFactory();
        Map<TableBucket, TableBucketWriteResult<TestingWriteResult>> results = new HashMap<>();
        try (Connection connection =
                        ConnectionFactory.createConnection(
                                FLUSS_CLUSTER_EXTENSION.getClientConfig());
                TieringSplitReader<TestingWriteResult> reader =
                        reader(
                                connection,
                                factory,
                                new Configuration(),
                                // the fetches end once the table is dropped, finding it gone
                                (split, fetch) ->
                                        dropped.thenCompose(
                                                v -> failed(new TableNotExistException("gone"))))) {
            addSplits(reader, plans);
            // the reader moves to the table and starts its rounds
            fetchUntil(reader, results, () -> true);
            admin.dropTable(tablePath, false).get();
            dropped.complete(null);
            fetchUntil(reader, results, () -> results.size() == plans.size());
        }

        for (TableBucketWriteResult<TestingWriteResult> result : results.values()) {
            assertThat(result.logEndOffset()).isEqualTo(-1);
            assertThat(result.writeResult()).isNull();
        }
        assertThat(factory.writers).isEmpty();
    }

    /**
     * A bucket whose round ended, of a table without partitions too, is no longer read: the records
     * written to it after the round's end, while the round of another bucket still runs, reach no
     * writer.
     */
    @Test
    void testRecordsPastTheEndOfARoundThatEndedReachNoWriter() throws Exception {
        TablePath tablePath = TablePath.of(DB, "tiering_past_an_ended_round");
        long tableId = createTable(tablePath, DV_TABLE_DESCRIPTOR);
        writeVersions(tablePath, 30, 2);
        Map<TableBucket, TieringLogSplit> plans = planRounds(tablePath, tableId);
        TableBucket last = new TableBucket(tableId, 1);
        CompletableFuture<Void> released = new CompletableFuture<>();
        RecordingLakeTieringFactory factory = new RecordingLakeTieringFactory();
        Map<TableBucket, TableBucketWriteResult<TestingWriteResult>> results = new HashMap<>();
        try (Connection connection =
                        ConnectionFactory.createConnection(
                                FLUSS_CLUSTER_EXTENSION.getClientConfig());
                TieringSplitReader<TestingWriteResult> reader =
                        reader(
                                connection,
                                factory,
                                new Configuration(),
                                (split, fetch) -> {
                                    CompletableFuture<RoundMask.Fetched> fetched = fetch.get();
                                    return split.getTableBucket().equals(last)
                                            ? released.thenCompose(v -> fetched)
                                            : fetched;
                                })) {
            addSplits(reader, plans);
            fetchUntil(reader, results, () -> results.size() == plans.size() - 1);
            writeVersions(tablePath, 30, 1);
            for (TableBucket tb : plans.keySet()) {
                assertThat(highWatermark(tablePath, tb.getBucket()))
                        .isGreaterThan(plans.get(tb).getStoppingOffset());
            }
            long until = System.currentTimeMillis() + 2_000;
            fetchUntil(reader, results, () -> System.currentTimeMillis() >= until);
            released.complete(null);
            fetchUntil(reader, results, () -> results.size() == plans.size());
        }

        for (TableBucket tb : plans.keySet()) {
            assertThat(results.get(tb).logEndOffset()).isEqualTo(plans.get(tb).getStoppingOffset());
        }
        assertThat(factory.writers).hasSize(plans.size());
        for (RecordingWriter writer : factory.writers) {
            assertThat(writer.completed).isTrue();
            for (Object[] record : writer.records) {
                assertThat((Long) record[0])
                        .isLessThan(plans.get(writer.bucket).getStoppingOffset());
            }
        }
    }

    /**
     * The reader holds the masks of the rounds it reads in as much heap as its writers': the fetch
     * of a mask that does not fit waits until the rounds before it end; one always fits.
     */
    @ParameterizedTest(name = "the masks fit {0}")
    @ValueSource(booleans = {true, false})
    void testMasksThatDoNotFitWaitForTheRoundsBeforeThem(boolean fit) throws Exception {
        TablePath tablePath = TablePath.of(DB, "tiering_mask_memory_" + fit);
        long tableId = createTable(tablePath, DV_TABLE_DESCRIPTOR);
        writeVersions(tablePath, 30, 2);
        Map<TableBucket, TieringLogSplit> plans = planRounds(tablePath, tableId);
        Configuration lakeTieringConfig = new Configuration();
        if (!fit) {
            // less than the most a mask may take
            lakeTieringConfig.set(
                    ConfigOptions.LAKE_TIERING_DELETION_VECTORS_WRITE_BUFFER_SIZE,
                    MemorySize.parse("1kb"));
        }
        RecordingLakeTieringFactory factory = new RecordingLakeTieringFactory();
        // the rounds completed when each mask fetch started
        List<Long> completedAtFetch = new CopyOnWriteArrayList<>();

        readRounds(
                tablePath,
                plans,
                factory,
                new MetricListener(),
                lakeTieringConfig,
                (split, fetched) -> {
                    completedAtFetch.add(factory.writers.stream().filter(w -> w.completed).count());
                    return fetched;
                });

        assertThat(completedAtFetch)
                .isEqualTo(fit ? Arrays.asList(0L, 0L, 0L) : Arrays.asList(0L, 1L, 2L));
        for (RecordingWriter writer : factory.writers) {
            assertThat(writer.masked).isTrue();
            assertThat(writer.completed).isTrue();
        }
    }

    /**
     * Past the time limit, a planned round that gets nothing for the stall timeout ends without
     * writing: its mask never comes, or it is read, in sort mode, and its log stops short of its
     * end. The other buckets' rounds still run to their ends.
     */
    @ParameterizedTest(name = "read {0}")
    @ValueSource(booleans = {false, true})
    void testStalledRoundEndsPastTheTimeLimit(boolean read) throws Exception {
        TablePath tablePath = TablePath.of(DB, "tiering_stalled_rounds_" + read);
        long tableId = createTable(tablePath, DV_TABLE_DESCRIPTOR);
        writeVersions(tablePath, 30, 2);
        Map<TableBucket, TieringLogSplit> plans = planRounds(tablePath, tableId);
        TableBucket stalled = new TableBucket(tableId, 1);
        Configuration lakeTieringConfig = new Configuration();
        if (read) {
            lakeTieringConfig.set(
                    ConfigOptions.LAKE_TIERING_DELETION_VECTORS_ROUND_MASK_ENABLED, false);
            plans.put(
                    stalled,
                    TieringLogSplit.plannedRound(
                            tablePath,
                            stalled,
                            null,
                            0,
                            plans.get(stalled).getStoppingOffset() + 5,
                            -1,
                            false));
        }
        RecordingLakeTieringFactory factory = new RecordingLakeTieringFactory();
        Map<TableBucket, TableBucketWriteResult<TestingWriteResult>> results = new HashMap<>();
        try (Connection connection =
                        ConnectionFactory.createConnection(
                                FLUSS_CLUSTER_EXTENSION.getClientConfig());
                TieringSplitReader<TestingWriteResult> reader =
                        new TieringSplitReader<TestingWriteResult>(
                                connection,
                                factory,
                                Thread.currentThread().getContextClassLoader(),
                                new TieringMetrics(
                                        InternalSourceReaderMetricGroup.mock(
                                                new MetricListener().getMetricGroup())),
                                lakeTieringConfig) {
                            @Override
                            protected CompletableFuture<RoundMask.Fetched> fetchRoundMask(
                                    TieringLogSplit split) {
                                return split.getTableBucket().equals(stalled)
                                        ? new CompletableFuture<>()
                                        : super.fetchRoundMask(split);
                            }
                        }) {
            reader.setStalledRoundTimeout(2_000);
            List<FlinkTieringSplit> splits = new ArrayList<>();
            for (TieringLogSplit split : plans.values()) {
                splits.add(new FlinkTieringSplit(split.copy(plans.size())));
            }
            reader.handleSplitsChanges(new SplitsAddition<>(splits));
            reader.handleTableReachTieringMaxDuration(tableId);
            long start = System.currentTimeMillis();
            Set<String> finished = new HashSet<>();
            while (finished.size() < splits.size()) {
                assertThat(System.currentTimeMillis() - start).isLessThan(60_000);
                RecordsWithSplitIds<TableBucketWriteResult<TestingWriteResult>> fetched =
                        reader.fetch();
                finished.addAll(fetched.finishedSplits());
                while (fetched.nextSplit() != null) {
                    TableBucketWriteResult<TestingWriteResult> result =
                            fetched.nextRecordFromSplit();
                    results.put(result.tableBucket(), result);
                }
            }
            assertThat(System.currentTimeMillis() - start).isGreaterThanOrEqualTo(2_000);
        }
        assertThat(results.get(stalled).logEndOffset()).isEqualTo(-1);
        assertThat(results.get(stalled).writeResult()).isNull();
        for (TableBucket tb : plans.keySet()) {
            if (!tb.equals(stalled)) {
                assertThat(results.get(tb).logEndOffset())
                        .isEqualTo(plans.get(tb).getStoppingOffset());
            }
        }
        for (RecordingWriter writer : factory.writers) {
            assertThat(writer.completed).isEqualTo(!writer.bucket.equals(stalled));
        }
        assertThat(factory.writers).hasSize(read ? plans.size() : plans.size() - 1);
    }

    // ---- helpers --------------------------------------------------------------------------------

    /** Makes the mask fetch of a planned round, given the reader's own fetch to start. */
    @FunctionalInterface
    private interface MaskFetches {
        CompletableFuture<RoundMask.Fetched> fetch(
                TieringLogSplit split, Supplier<CompletableFuture<RoundMask.Fetched>> fetch);
    }

    /** A reader that polls 200 ms at most, its masks fetched as {@code masks} makes them. */
    private static TieringSplitReader<TestingWriteResult> reader(
            Connection connection,
            RecordingLakeTieringFactory factory,
            Configuration lakeTieringConfig,
            MaskFetches masks) {
        return new TieringSplitReader<TestingWriteResult>(
                connection,
                factory,
                Thread.currentThread().getContextClassLoader(),
                Duration.ofMillis(200),
                new TieringMetrics(
                        InternalSourceReaderMetricGroup.mock(
                                new MetricListener().getMetricGroup())),
                null,
                lakeTieringConfig) {
            @Override
            protected CompletableFuture<RoundMask.Fetched> fetchRoundMask(TieringLogSplit split) {
                return masks.fetch(split, () -> super.fetchRoundMask(split));
            }
        };
    }

    private static CompletableFuture<RoundMask.Fetched> failed(Exception failure) {
        CompletableFuture<RoundMask.Fetched> failed = new CompletableFuture<>();
        failed.completeExceptionally(failure);
        return failed;
    }

    private static void addSplits(
            TieringSplitReader<TestingWriteResult> reader,
            Map<TableBucket, TieringLogSplit> plans) {
        List<FlinkTieringSplit> splits = new ArrayList<>();
        for (TieringLogSplit split : plans.values()) {
            splits.add(new FlinkTieringSplit(split.copy(plans.size())));
        }
        reader.handleSplitsChanges(new SplitsAddition<>(splits));
    }

    /** Fetches once, and again until {@code done}, collecting the results of the rounds. */
    private static void fetchUntil(
            TieringSplitReader<TestingWriteResult> reader,
            Map<TableBucket, TableBucketWriteResult<TestingWriteResult>> results,
            BooleanSupplier done)
            throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();
        do {
            assertThat(System.currentTimeMillis())
                    .as("the rounds end, %s ended", results.size())
                    .isLessThan(deadline);
            RecordsWithSplitIds<TableBucketWriteResult<TestingWriteResult>> fetched =
                    reader.fetch();
            while (fetched.nextSplit() != null) {
                TableBucketWriteResult<TestingWriteResult> result = fetched.nextRecordFromSplit();
                results.put(result.tableBucket(), result);
            }
        } while (!done.getAsBoolean());
    }

    /** The mask with its leader's count of changes off by one. */
    private static RoundMask.Fetched miscounted(RoundMask.Fetched mask) {
        return new RoundMask.Fetched(
                mask.offsets, checkNotNull(mask.changes) + 1, mask.changesWithInserts);
    }

    /** The leader's mask without its first masked offset, a version it missed. */
    private static RoundMask.Fetched missingAVersion(RoundMask.Fetched mask) {
        Roaring64Bitmap offsets = new Roaring64Bitmap();
        offsets.or(mask.offsets);
        offsets.removeLong(offsets.first());
        return new RoundMask.Fetched(offsets, mask.changes, mask.changesWithInserts);
    }

    /**
     * Reads the planned rounds of a table, past its time limit from the start if {@code
     * pastTheTimeLimit}, until every split ends; returns their results.
     */
    private Map<TableBucket, TableBucketWriteResult<TestingWriteResult>> readRounds(
            Connection connection,
            long tableId,
            Map<TableBucket, TieringLogSplit> plans,
            RecordingLakeTieringFactory factory,
            boolean pastTheTimeLimit,
            MaskFetches masks)
            throws Exception {
        Map<TableBucket, TableBucketWriteResult<TestingWriteResult>> results = new HashMap<>();
        try (TieringSplitReader<TestingWriteResult> reader =
                reader(connection, factory, new Configuration(), masks)) {
            List<FlinkTieringSplit> splits = new ArrayList<>();
            for (Map.Entry<TableBucket, TieringLogSplit> plan : plans.entrySet()) {
                splits.add(new FlinkTieringSplit(plan.getValue().copy(plans.size())));
            }
            reader.handleSplitsChanges(new SplitsAddition<>(splits));
            if (pastTheTimeLimit) {
                reader.handleTableReachTieringMaxDuration(tableId);
            }
            Set<String> finished = new HashSet<>();
            long deadline = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();
            while (finished.size() < splits.size()) {
                assertThat(System.currentTimeMillis())
                        .as("the rounds end, %s of %s ended", finished.size(), splits.size())
                        .isLessThan(deadline);
                RecordsWithSplitIds<TableBucketWriteResult<TestingWriteResult>> fetched =
                        reader.fetch();
                finished.addAll(fetched.finishedSplits());
                while (fetched.nextSplit() != null) {
                    TableBucketWriteResult<TestingWriteResult> result =
                            fetched.nextRecordFromSplit();
                    results.put(result.tableBucket(), result);
                }
            }
        }
        return results;
    }

    private long createTable(TablePath tablePath, TableDescriptor descriptor) throws Exception {
        return createTable(tablePath, descriptor, DEFAULT_BUCKET_NUM);
    }

    private long createTable(TablePath tablePath, TableDescriptor descriptor, int buckets)
            throws Exception {
        admin.createTable(tablePath, descriptor, true).get();
        long tableId = admin.getTableInfo(tablePath).get().getTableId();
        for (int bucket = 0; bucket < buckets; bucket++) {
            FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(new TableBucket(tableId, bucket));
        }
        return tableId;
    }

    /**
     * Writes {@code versions} versions of every key in [0, keys), each after the one before, and
     * returns the live value of each key.
     */
    private Map<Integer, String> writeVersions(TablePath tablePath, int keys, int versions)
            throws Exception {
        Map<Integer, String> live = new HashMap<>();
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (int version = 0; version < versions; version++) {
                for (int key = 0; key < keys; key++) {
                    String value = "v" + version + "_" + key;
                    writer.upsert(row(key, value));
                    live.put(key, value);
                }
                writer.flush();
            }
        }
        return live;
    }

    private long highWatermark(TablePath tablePath, int bucket) throws Exception {
        return conn.getAdmin()
                .listOffsets(
                        tablePath, Collections.singletonList(bucket), new OffsetSpec.LatestSpec())
                .all()
                .get()
                .get(bucket);
    }

    /** Plans every bucket's round from 0 at its leader. */
    private Map<TableBucket, TieringLogSplit> planRounds(TablePath tablePath, long tableId)
            throws Exception {
        return planRounds(tablePath, tableId, DEFAULT_BUCKET_NUM);
    }

    private Map<TableBucket, TieringLogSplit> planRounds(
            TablePath tablePath, long tableId, int buckets) throws Exception {
        Map<TableBucket, Long> starts = new HashMap<>();
        for (int bucket = 0; bucket < buckets; bucket++) {
            starts.put(new TableBucket(tableId, bucket), 0L);
        }
        Map<TableBucket, TieringLogSplit> plans = new HashMap<>();
        for (Map.Entry<TableBucket, GetDvSnapshotResponse> plan :
                new DvRoundRequests(conn.getAdmin(), tablePath, Thread::sleep)
                        .planRounds(-1L, starts, 1_000_000, 1L << 30)
                        .entrySet()) {
            plans.put(
                    plan.getKey(),
                    TieringLogSplit.plannedRound(
                            tablePath,
                            plan.getKey(),
                            null,
                            0,
                            plan.getValue().getRoundEndOffset(),
                            -1,
                            plan.getValue().isRoundCapped()));
        }
        for (int bucket = 0; bucket < buckets; bucket++) {
            assertThat(plans.get(new TableBucket(tableId, bucket)).getStoppingOffset())
                    .isEqualTo(highWatermark(tablePath, bucket));
        }
        return plans;
    }

    /**
     * Reads the planned rounds to their ends, their masks as {@code masks} makes them of the
     * fetched ones if not null.
     */
    private void readRounds(
            TablePath tablePath,
            Map<TableBucket, TieringLogSplit> plans,
            RecordingLakeTieringFactory factory,
            MetricListener metrics,
            Configuration lakeTieringConfig,
            @Nullable
                    BiFunction<
                                    TieringLogSplit,
                                    CompletableFuture<RoundMask.Fetched>,
                                    CompletableFuture<RoundMask.Fetched>>
                            masks)
            throws Exception {
        Map<TableBucket, TableBucketWriteResult<TestingWriteResult>> results =
                read(
                        FLUSS_CLUSTER_EXTENSION.getClientConfig(),
                        plans,
                        factory,
                        metrics,
                        lakeTieringConfig,
                        masks,
                        null);
        for (Map.Entry<TableBucket, TableBucketWriteResult<TestingWriteResult>> e :
                results.entrySet()) {
            assertThat(e.getValue().logEndOffset())
                    .isEqualTo(plans.get(e.getKey()).getStoppingOffset());
        }
    }

    /**
     * Reads the planned rounds until every split ends, past the time limit of {@code
     * pastTimeLimitOf} from the start if not null; returns the rounds' results.
     */
    private Map<TableBucket, TableBucketWriteResult<TestingWriteResult>> read(
            Configuration clientConf,
            Map<TableBucket, TieringLogSplit> plans,
            RecordingLakeTieringFactory factory,
            MetricListener metrics,
            Configuration lakeTieringConfig,
            @Nullable
                    BiFunction<
                                    TieringLogSplit,
                                    CompletableFuture<RoundMask.Fetched>,
                                    CompletableFuture<RoundMask.Fetched>>
                            masks,
            @Nullable Long pastTimeLimitOf)
            throws Exception {
        Map<TableBucket, TableBucketWriteResult<TestingWriteResult>> results = new HashMap<>();
        try (Connection connection = ConnectionFactory.createConnection(clientConf);
                TieringSplitReader<TestingWriteResult> reader =
                        new TieringSplitReader<TestingWriteResult>(
                                connection,
                                factory,
                                Thread.currentThread().getContextClassLoader(),
                                new TieringMetrics(
                                        InternalSourceReaderMetricGroup.mock(
                                                metrics.getMetricGroup())),
                                lakeTieringConfig) {
                            @Override
                            protected CompletableFuture<RoundMask.Fetched> fetchRoundMask(
                                    TieringLogSplit split) {
                                return masks == null
                                        ? super.fetchRoundMask(split)
                                        : masks.apply(split, super.fetchRoundMask(split));
                            }
                        }) {
            List<FlinkTieringSplit> splits = new ArrayList<>();
            for (TieringLogSplit split : plans.values()) {
                splits.add(new FlinkTieringSplit(split.copy(plans.size())));
            }
            reader.handleSplitsChanges(new SplitsAddition<>(splits));
            if (pastTimeLimitOf != null) {
                reader.handleTableReachTieringMaxDuration(pastTimeLimitOf);
            }
            Set<String> finished = new HashSet<>();
            long deadline = System.currentTimeMillis() + Duration.ofMinutes(3).toMillis();
            while (finished.size() < splits.size()) {
                assertThat(System.currentTimeMillis())
                        .as("the rounds end, %s of %s ended", finished.size(), splits.size())
                        .isLessThan(deadline);
                RecordsWithSplitIds<TableBucketWriteResult<TestingWriteResult>> fetched =
                        reader.fetch();
                finished.addAll(fetched.finishedSplits());
                while (fetched.nextSplit() != null) {
                    TableBucketWriteResult<TestingWriteResult> result =
                            fetched.nextRecordFromSplit();
                    results.put(result.tableBucket(), result);
                }
            }
        }
        return results;
    }

    /** Every record of each round reached a writer in sort mode, which completed. */
    private static void assertWrittenWhole(
            RecordingLakeTieringFactory factory, Map<TableBucket, TieringLogSplit> plans) {
        assertThat(factory.writers).hasSize(plans.size());
        for (RecordingWriter writer : factory.writers) {
            assertThat(writer.masked).isFalse();
            assertThat(writer.completed).isTrue();
            assertThat(writer.records).hasSize((int) plans.get(writer.bucket).getStoppingOffset());
        }
    }

    private static long counter(MetricListener metrics, String name) {
        return metrics.getCounter(
                        TieringMetrics.FLUSS_METRIC_GROUP,
                        TieringMetrics.TIERING_SERVICE_GROUP,
                        name)
                .map(Counter::getCount)
                .orElse(-1L);
    }

    /** Creates writers that record what they get. */
    private static class RecordingLakeTieringFactory extends TestingLakeTieringFactory {
        final List<RecordingWriter> writers = new CopyOnWriteArrayList<>();

        @Override
        public LakeWriter<TestingWriteResult> createLakeWriter(WriterInitContext context) {
            RecordingWriter writer =
                    new RecordingWriter(
                            context.tableBucket(),
                            ((DeletionVectorWriterInitContext) context).roundMasked());
            writers.add(writer);
            return writer;
        }
    }

    /**
     * Records {offset, change type, key, value} of every record it gets. Through a mask, it finds a
     * row of a key that a later record of the key follows as it completes, as a writer that stages
     * its rows' positions by key does.
     */
    private static class RecordingWriter implements LakeWriter<TestingWriteResult> {
        final TableBucket bucket;
        final boolean masked;
        final List<Object[]> records = new ArrayList<>();
        boolean completed;
        boolean closed;

        RecordingWriter(TableBucket bucket, boolean masked) {
            this.bucket = bucket;
            this.masked = masked;
        }

        @Override
        public void write(LogRecord record) {
            records.add(
                    new Object[] {
                        record.logOffset(),
                        record.getChangeType(),
                        record.getRow().getInt(0),
                        record.getRow().getString(1).toString()
                    });
        }

        @Override
        public TestingWriteResult complete() throws RoundMaskMismatchException {
            if (masked) {
                // by key, whether its last record so far was a row
                Map<Integer, Boolean> rows = new HashMap<>();
                for (Object[] record : records) {
                    int key = (Integer) record[2];
                    Boolean row = rows.put(key, record[1] != ChangeType.DELETE);
                    if (row != null && row) {
                        throw new RoundMaskMismatchException("key " + key + " comes twice");
                    }
                }
            }
            completed = true;
            return new TestingWriteResult(records.size());
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
