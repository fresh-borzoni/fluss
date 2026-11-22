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
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.client.tiering.DvRoundRequests;
import org.apache.fluss.client.tiering.RoundDeletes;
import org.apache.fluss.client.tiering.TableBucketWriteResult;
import org.apache.fluss.client.tiering.TieringLogSplit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.flink.tiering.TestingLakeTieringFactory;
import org.apache.fluss.flink.tiering.TestingWriteResult;
import org.apache.fluss.flink.tiering.source.metrics.TieringMetrics;
import org.apache.fluss.flink.tiering.source.split.FlinkTieringSplit;
import org.apache.fluss.lake.writer.DeletionVectorPreparer;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.SupportsDeletionVectorPreparation;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.testutils.common.CommonTestUtils;
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
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The deletes of the planned rounds of tables with deletion vectors, as the tiering reader fetches
 * them while it reads the rounds and hands them to the committer with the rounds' write results.
 */
class TieringSplitReaderRoundDeletesTest {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(
                            new Configuration()
                                    .set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON))
                    .setNumOfTabletServers(1)
                    .build();

    private static final String DB = "round_deletes";
    private static final int BUCKETS = 3;
    // the lake snapshot the rounds are planned on; the leaders' masks are of the empty lake
    private static final long SNAPSHOT = 7L;
    // the write result a preparer of these tests hands on
    private static final int PREPARED = -1;
    // the deletes of a test round: the row at position 1 of the bucket's data file
    private static final byte[] POSITION_1 = serialized(1L);
    private static final TableDescriptor DV_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(
                            Schema.newBuilder()
                                    .primaryKey("id")
                                    .column("id", DataTypes.INT())
                                    .column("name", DataTypes.STRING())
                                    .build())
                    .distributedBy(BUCKETS, "id")
                    .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                    .property(ConfigOptions.TABLE_DATALAKE_DELETION_VECTORS_ENABLED, true)
                    .build();

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

    /** Deletes fetched before the round's writer completes go with its write result. */
    @Test
    void testDeletesFetchedWhileTheRoundIsReadGoWithItsResult() throws Exception {
        Map<TableBucket, TieringLogSplit> plans = plannedRounds("fetched_deletes", SNAPSHOT);
        Deletes deletes = new Deletes(split -> CompletableFuture.completedFuture(of(split)));

        Map<TableBucket, TableBucketWriteResult<TestingWriteResult>> results =
                readAll(plans, new Configuration(), deletes, new RecordingFactory());

        assertThat(deletes.requested).isEqualTo(plans.keySet());
        assertThat(deletes.fetches.get()).isEqualTo(plans.size());
        for (TieringLogSplit split : plans.values()) {
            RoundDeletes carried = results.get(split.getTableBucket()).roundDeletes();
            assertThat(carried).isNotNull();
            assertThat(
                            new long[] {
                                carried.snapshotId(), carried.startOffset(), carried.endOffset()
                            })
                    .containsExactly(
                            SNAPSHOT, split.getStartingOffset(), split.getStoppingOffset());
            assertThat(carried.files()).hasSize(1);
        }
    }

    /**
     * The write result of a round whose writer completed before its deletes are fetched waits for
     * them, its split still open, and goes on with them once they are fetched.
     */
    @Test
    void testResultWaitsForDeletesStillBeingFetched() throws Exception {
        Map<TableBucket, TieringLogSplit> plans = plannedRounds("waiting_results", SNAPSHOT);
        Map<TableBucket, CompletableFuture<RoundDeletes>> pending = new ConcurrentHashMap<>();
        Deletes deletes =
                new Deletes(
                        split ->
                                pending.computeIfAbsent(
                                        split.getTableBucket(), b -> new CompletableFuture<>()));
        RecordingFactory factory = new RecordingFactory();
        Map<TableBucket, TableBucketWriteResult<TestingWriteResult>> results = new HashMap<>();
        Set<String> finished = new HashSet<>();
        try (TieringSplitReader<TestingWriteResult> reader =
                reader(new Configuration(), deletes, factory)) {
            addSplits(reader, plans);
            fetchUntil(reader, results, finished, () -> factory.completed() == plans.size());
            // the writers completed, the results wait
            for (int i = 0; i < 3; i++) {
                fetch(reader, results, finished);
            }
            assertThat(results).isEmpty();
            assertThat(finished).isEmpty();

            for (TieringLogSplit split : plans.values()) {
                pending.get(split.getTableBucket()).complete(of(split));
            }
            long start = System.currentTimeMillis();
            fetchUntil(reader, results, finished, () -> finished.size() == plans.size());
            // woken up by the fetches, not after a poll's time
            assertThat(System.currentTimeMillis() - start).isLessThan(5_000);
        }
        for (TieringLogSplit split : plans.values()) {
            assertThat(results.get(split.getTableBucket()).roundDeletes()).isNotNull();
        }
    }

    /**
     * A round whose deletes fail to be fetched, or are not fetched within the prefetch timeout,
     * goes on without them, so the committer fetches them itself; a late fetch is stopped.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRoundGoesOnWithoutDeletesThatFailOrComeLate(boolean fail) throws Exception {
        Map<TableBucket, TieringLogSplit> plans =
                plannedRounds(fail ? "failed_deletes" : "late_deletes", SNAPSHOT);
        List<CompletableFuture<RoundDeletes>> fetches = new CopyOnWriteArrayList<>();
        Deletes deletes =
                new Deletes(
                        split -> {
                            CompletableFuture<RoundDeletes> fetch = new CompletableFuture<>();
                            if (fail) {
                                fetch.completeExceptionally(
                                        new NotLeaderOrFollowerException("moved"));
                            }
                            fetches.add(fetch);
                            return fetch;
                        });
        long timeoutMs = 500;
        Configuration conf = new Configuration();
        conf.set(
                ConfigOptions.LAKE_TIERING_DELETION_VECTORS_ROUND_DELETES_PREFETCH_TIMEOUT,
                Duration.ofMillis(timeoutMs));
        RecordingFactory factory = new RecordingFactory();

        long start = System.currentTimeMillis();
        Map<TableBucket, TableBucketWriteResult<TestingWriteResult>> results =
                readAll(plans, conf, deletes, factory);

        assertThat(results).hasSize(plans.size());
        for (TableBucketWriteResult<TestingWriteResult> result : results.values()) {
            assertThat(result.writeResult()).isNotNull();
            assertThat(result.roundDeletes()).isNull();
        }
        assertThat(fetches).hasSize(plans.size());
        if (!fail) {
            assertThat(System.currentTimeMillis() - start).isGreaterThanOrEqualTo(timeoutMs);
            for (CompletableFuture<RoundDeletes> fetch : fetches) {
                assertThat(fetch.isCancelled()).isTrue();
            }
        }
    }

    /**
     * A round read again in sort mode, as its mask did not match, keeps the deletes fetched for it:
     * they depend on its snapshot and offsets only. They are fetched once.
     */
    @Test
    void testRoundReadAgainInSortModeKeepsItsDeletes() throws Exception {
        Map<TableBucket, TieringLogSplit> plans = plannedRounds("sort_mode_deletes", SNAPSHOT);
        Deletes deletes = new Deletes(split -> CompletableFuture.completedFuture(of(split)));
        RecordingFactory factory = new RecordingFactory();
        Map<TableBucket, TableBucketWriteResult<TestingWriteResult>> results = new HashMap<>();
        try (TieringSplitReader<TestingWriteResult> reader =
                new DeletesReader(conn, new Configuration(), deletes, factory) {
                    @Override
                    protected CompletableFuture<RoundMask.Fetched> fetchRoundMask(
                            TieringLogSplit split) {
                        return super.fetchRoundMask(split)
                                .thenApply(
                                        mask ->
                                                new RoundMask.Fetched(
                                                        mask.offsets,
                                                        mask.changes + 1,
                                                        mask.changesWithInserts));
                    }
                }) {
            addSplits(reader, plans);
            fetchUntil(reader, results, new HashSet<>(), () -> results.size() == plans.size());
        }
        // a masked writer dropped, a sort mode writer completed per bucket
        assertThat(factory.writers).hasSize(2 * plans.size());
        assertThat(factory.completed()).isEqualTo(plans.size());
        assertThat(deletes.fetches.get()).isEqualTo(plans.size());
        for (TableBucketWriteResult<TestingWriteResult> result : results.values()) {
            assertThat(result.roundDeletes()).isNotNull();
        }
    }

    /**
     * No deletes are fetched for a round on the empty lake, which deletes nothing, nor with the
     * prefetch turned off.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testNoDeletesAreFetchedOnTheEmptyLakeOrWithThePrefetchOff(boolean emptyLake)
            throws Exception {
        Map<TableBucket, TieringLogSplit> plans =
                plannedRounds(
                        emptyLake ? "empty_lake_deletes" : "prefetch_off_deletes",
                        emptyLake ? -1L : SNAPSHOT);
        Deletes deletes = new Deletes(split -> CompletableFuture.completedFuture(of(split)));
        Configuration conf = new Configuration();
        if (!emptyLake) {
            conf.set(
                    ConfigOptions.LAKE_TIERING_DELETION_VECTORS_ROUND_DELETES_PREFETCH_TIMEOUT,
                    Duration.ZERO);
        }

        Map<TableBucket, TableBucketWriteResult<TestingWriteResult>> results =
                readAll(plans, conf, deletes, new RecordingFactory());

        assertThat(deletes.fetches.get()).isZero();
        assertThat(results).hasSize(plans.size());
        for (TableBucketWriteResult<TestingWriteResult> result : results.values()) {
            assertThat(result.roundDeletes()).isNull();
        }
    }

    /**
     * A fetch that waits for the deletes of results while the reader has nothing else to read
     * returns once the reader is woken up, e.g. for a new split, not after a poll's time.
     */
    @Test
    void testWakeUpEndsTheWaitForDeletes() throws Exception {
        Map<TableBucket, TieringLogSplit> plans = plannedRounds("woken_reader", SNAPSHOT);
        Deletes deletes = new Deletes(split -> new CompletableFuture<>());
        Configuration conf = new Configuration();
        conf.set(
                ConfigOptions.LAKE_TIERING_DELETION_VECTORS_ROUND_DELETES_PREFETCH_TIMEOUT,
                Duration.ofMinutes(5));
        RecordingFactory factory = new RecordingFactory();
        try (TieringSplitReader<TestingWriteResult> reader =
                new DeletesReader(conn, conf, deletes, factory, Duration.ofMinutes(5))) {
            addSplits(reader, plans);
            fetchUntil(
                    reader,
                    new HashMap<>(),
                    new HashSet<>(),
                    () -> factory.completed() == plans.size());
            Thread waker =
                    new Thread(
                            () -> {
                                try {
                                    Thread.sleep(500);
                                } catch (InterruptedException e) {
                                    return;
                                }
                                reader.wakeUp();
                            });
            long start = System.currentTimeMillis();
            waker.start();
            RecordsWithSplitIds<TableBucketWriteResult<TestingWriteResult>> fetched =
                    reader.fetch();
            waker.join();
            assertThat(fetched.finishedSplits()).isEmpty();
            assertThat(System.currentTimeMillis() - start).isLessThan(30_000);
        }
    }

    /** A reader closed, e.g. on a failover, stops the fetches of the results that wait. */
    @Test
    void testClosedReaderStopsTheFetchesItsResultsWaitFor() throws Exception {
        Map<TableBucket, TieringLogSplit> plans = plannedRounds("closed_reader", SNAPSHOT);
        List<CompletableFuture<RoundDeletes>> fetches = new CopyOnWriteArrayList<>();
        Deletes deletes =
                new Deletes(
                        split -> {
                            CompletableFuture<RoundDeletes> fetch = new CompletableFuture<>();
                            fetches.add(fetch);
                            return fetch;
                        });
        RecordingFactory factory = new RecordingFactory();
        try (TieringSplitReader<TestingWriteResult> reader =
                reader(new Configuration(), deletes, factory)) {
            addSplits(reader, plans);
            fetchUntil(
                    reader,
                    new HashMap<>(),
                    new HashSet<>(),
                    () -> factory.completed() == plans.size());
        }
        assertThat(fetches).hasSize(plans.size());
        for (CompletableFuture<RoundDeletes> fetch : fetches) {
            assertThat(fetch.isCancelled()).isTrue();
        }
    }

    /**
     * The reader's own fetch of a round's deletes asks the bucket's leader with the split's
     * snapshot and offsets, and a cancelled fetch stops: here the leader does not cover the
     * snapshot, so the fetch fails or is cancelled while it waits, and the round goes on without.
     */
    @Test
    void testOwnFetchAsksTheLeaderAndGoesOnWithoutAFailedFetch() throws Exception {
        Map<TableBucket, TieringLogSplit> plans = plannedRounds("own_fetch", SNAPSHOT);
        Configuration conf = new Configuration();
        conf.set(
                ConfigOptions.LAKE_TIERING_DELETION_VECTORS_ROUND_DELETES_PREFETCH_TIMEOUT,
                Duration.ofSeconds(2));
        List<CompletableFuture<RoundDeletes>> fetches = new CopyOnWriteArrayList<>();
        Map<TableBucket, TableBucketWriteResult<TestingWriteResult>> results = new HashMap<>();
        try (TieringSplitReader<TestingWriteResult> reader =
                new DeletesReader(conn, conf, null, new RecordingFactory()) {
                    @Override
                    protected CompletableFuture<RoundDeletes> fetchRoundDeletes(
                            TieringLogSplit split) {
                        CompletableFuture<RoundDeletes> fetch = super.fetchRoundDeletes(split);
                        fetches.add(fetch);
                        return fetch;
                    }
                }) {
            addSplits(reader, plans);
            fetchUntil(reader, results, new HashSet<>(), () -> results.size() == plans.size());
        }
        assertThat(fetches).hasSize(plans.size());
        for (CompletableFuture<RoundDeletes> fetch : fetches) {
            assertThat(fetch.isDone()).isTrue();
            assertThat(fetch.isCompletedExceptionally()).isTrue();
        }
        for (TableBucketWriteResult<TestingWriteResult> result : results.values()) {
            assertThat(result.roundDeletes()).isNull();
        }
    }

    /**
     * A writer that prepares its bucket's deletion vectors from the round's deletes hands on the
     * prepared result without the deletes. One that does not prepare them, whose preparation fails,
     * or whose preparer cannot be created hands on the deletes, and a failure is counted; with the
     * writers' preparation turned off none is asked.
     */
    @ParameterizedTest
    @EnumSource(Preparation.class)
    void testWritersPrepareTheDeletionVectorsOfTheirRounds(Preparation preparation)
            throws Exception {
        Map<TableBucket, TieringLogSplit> plans =
                plannedRounds("prepared_" + preparation.name().toLowerCase(), SNAPSHOT);
        Deletes deletes = new Deletes(split -> CompletableFuture.completedFuture(of(split)));
        Configuration conf = new Configuration();
        conf.set(
                ConfigOptions.LAKE_TIERING_DELETION_VECTORS_WRITER_PREPARED_ENABLED,
                preparation != Preparation.OFF);
        PreparingFactory factory = new PreparingFactory(preparation);
        MetricListener metrics = new MetricListener();

        Map<TableBucket, TableBucketWriteResult<TestingWriteResult>> results =
                readAll(plans, conf, deletes, factory, metrics);

        assertThat(results).hasSize(plans.size());
        for (TieringLogSplit split : plans.values()) {
            TableBucketWriteResult<TestingWriteResult> result = results.get(split.getTableBucket());
            if (preparation == Preparation.PREPARED) {
                assertThat(result.roundDeletes()).isNull();
                assertThat(checkNotNull(result.writeResult()).getWriteResult()).isEqualTo(PREPARED);
                PreparingFactory.Prepared asked = factory.prepared.get(split.getTableBucket());
                assertThat(asked.snapshotId).isEqualTo(SNAPSHOT);
                assertThat(asked.startOffset).isEqualTo(split.getStartingOffset());
                assertThat(asked.endOffset).isEqualTo(split.getStoppingOffset());
                assertThat(asked.files)
                        .containsOnlyKeys("data-" + split.getTableBucket().getBucket());
                assertThat(asked.files.get("data-" + split.getTableBucket().getBucket()))
                        .containsExactly(1L);
            } else {
                assertThat(result.roundDeletes()).isNotNull();
                assertThat(checkNotNull(result.writeResult()).getWriteResult())
                        .isNotEqualTo(PREPARED);
            }
        }
        boolean asked = preparation != Preparation.OFF && preparation != Preparation.NO_PREPARER;
        assertThat(factory.prepared.size() + factory.failed.get())
                .isEqualTo(asked ? plans.size() : 0);
        boolean fails = preparation == Preparation.FAILED || preparation == Preparation.NO_PREPARER;
        assertThat(preparationFailures(metrics)).isEqualTo(fails ? plans.size() : 0);
        // tried when a round starts, to read ahead, and again when it ends, uncounted the first
        // time
        assertThat(factory.created.get())
                .as("a preparer that cannot be created is tried again by the next round")
                .isEqualTo(
                        preparation == Preparation.OFF
                                ? 0
                                : preparation == Preparation.NO_PREPARER ? 2 * plans.size() : 1);
        assertThat(factory.prefetched)
                .as("the snapshots the rounds' preparations read ahead on")
                .containsExactlyElementsOf(
                        preparation == Preparation.OFF || preparation == Preparation.NO_PREPARER
                                ? Collections.emptyList()
                                : Collections.nCopies(plans.size(), SNAPSHOT));
        if (preparation == Preparation.PREPARED) {
            // the files of the round's deletes, read ahead once they arrived
            assertThat(factory.prefetchedFiles).hasSize(plans.size());
            factory.prefetchedFiles.forEach(
                    (bucket, files) ->
                            assertThat(files).containsExactly("data-" + bucket.getBucket()));
        }
        assertThat(factory.closed.get()).isEqualTo(factory.preparers.get());
    }

    /**
     * A round whose deletion vectors are not prepared within the prefetch timeout goes on with its
     * deletes, and what its writer prepares later is deleted.
     */
    @Test
    void testLatePreparationGoesOnWithTheDeletesAndIsDiscarded() throws Exception {
        Map<TableBucket, TieringLogSplit> plans = plannedRounds("late_prepared", SNAPSHOT);
        Deletes deletes = new Deletes(split -> CompletableFuture.completedFuture(of(split)));
        Configuration conf = new Configuration();
        conf.set(
                ConfigOptions.LAKE_TIERING_DELETION_VECTORS_ROUND_DELETES_PREFETCH_TIMEOUT,
                Duration.ofMillis(500));
        CountDownLatch release = new CountDownLatch(1);
        PreparingFactory factory = new PreparingFactory(Preparation.PREPARED, release);
        Map<TableBucket, TableBucketWriteResult<TestingWriteResult>> results = new HashMap<>();
        try (TieringSplitReader<TestingWriteResult> reader = reader(conf, deletes, factory)) {
            addSplits(reader, plans);
            fetchUntil(reader, results, new HashSet<>(), () -> results.size() == plans.size());

            for (TableBucketWriteResult<TestingWriteResult> result : results.values()) {
                assertThat(result.roundDeletes()).isNotNull();
                assertThat(checkNotNull(result.writeResult()).getWriteResult())
                        .isNotEqualTo(PREPARED);
            }
            release.countDown();
            CommonTestUtils.waitUntil(
                    () -> factory.discarded.get() == plans.size(),
                    Duration.ofMinutes(1),
                    "the late prepared deletion vectors are deleted");
        }
    }

    // ---- helpers --------------------------------------------------------------------------------

    private static RoundDeletes of(TieringLogSplit split) {
        return new RoundDeletes(
                split.getRoundSnapshotId(),
                split.getStartingOffset(),
                split.getStoppingOffset(),
                Collections.singletonList(
                        new RoundDeletes.File(
                                "data-" + split.getTableBucket().getBucket(), POSITION_1)));
    }

    private static byte[] serialized(long... positions) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream data = new DataOutputStream(bytes)) {
            Roaring64Bitmap.bitmapOf(positions).serialize(data);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bytes.toByteArray();
    }

    /** The fetches of the rounds' deletes, as {@code fetch} makes them. */
    private static final class Deletes {
        final Function<TieringLogSplit, CompletableFuture<RoundDeletes>> fetch;
        final Set<TableBucket> requested = ConcurrentHashMap.newKeySet();
        final AtomicInteger fetches = new AtomicInteger();

        Deletes(Function<TieringLogSplit, CompletableFuture<RoundDeletes>> fetch) {
            this.fetch = fetch;
        }

        CompletableFuture<RoundDeletes> fetch(TieringLogSplit split) {
            requested.add(split.getTableBucket());
            fetches.incrementAndGet();
            return fetch.apply(split);
        }
    }

    /**
     * A reader that polls 200 ms at most, its rounds' deletes fetched by {@code deletes} if not
     * null, and their masks fetched from the leaders as of the empty lake.
     */
    private static class DeletesReader extends TieringSplitReader<TestingWriteResult> {
        private final Deletes deletes;

        DeletesReader(
                Connection connection,
                Configuration lakeTieringConfig,
                Deletes deletes,
                RecordingFactory factory) {
            this(connection, lakeTieringConfig, deletes, factory, Duration.ofMillis(200));
        }

        DeletesReader(
                Connection connection,
                Configuration lakeTieringConfig,
                Deletes deletes,
                RecordingFactory factory,
                Duration pollTimeout) {
            this(
                    connection,
                    lakeTieringConfig,
                    deletes,
                    factory,
                    pollTimeout,
                    new MetricListener());
        }

        DeletesReader(
                Connection connection,
                Configuration lakeTieringConfig,
                Deletes deletes,
                RecordingFactory factory,
                Duration pollTimeout,
                MetricListener metrics) {
            super(
                    connection,
                    factory,
                    Thread.currentThread().getContextClassLoader(),
                    pollTimeout,
                    new TieringMetrics(
                            InternalSourceReaderMetricGroup.mock(metrics.getMetricGroup())),
                    null,
                    lakeTieringConfig);
            this.deletes = deletes;
        }

        @Override
        protected CompletableFuture<RoundMask.Fetched> fetchRoundMask(TieringLogSplit split) {
            return super.fetchRoundMask(onSnapshot(split, -1L));
        }

        @Override
        protected CompletableFuture<RoundDeletes> fetchRoundDeletes(TieringLogSplit split) {
            return deletes == null ? super.fetchRoundDeletes(split) : deletes.fetch(split);
        }
    }

    private TieringSplitReader<TestingWriteResult> reader(
            Configuration lakeTieringConfig, Deletes deletes, RecordingFactory factory) {
        return new DeletesReader(conn, lakeTieringConfig, deletes, factory);
    }

    private Map<TableBucket, TableBucketWriteResult<TestingWriteResult>> readAll(
            Map<TableBucket, TieringLogSplit> plans,
            Configuration lakeTieringConfig,
            Deletes deletes,
            RecordingFactory factory)
            throws Exception {
        return readAll(plans, lakeTieringConfig, deletes, factory, new MetricListener());
    }

    private Map<TableBucket, TableBucketWriteResult<TestingWriteResult>> readAll(
            Map<TableBucket, TieringLogSplit> plans,
            Configuration lakeTieringConfig,
            Deletes deletes,
            RecordingFactory factory,
            MetricListener metrics)
            throws Exception {
        Map<TableBucket, TableBucketWriteResult<TestingWriteResult>> results = new HashMap<>();
        Set<String> finished = new HashSet<>();
        try (TieringSplitReader<TestingWriteResult> reader =
                new DeletesReader(
                        conn,
                        lakeTieringConfig,
                        deletes,
                        factory,
                        Duration.ofMillis(200),
                        metrics)) {
            addSplits(reader, plans);
            fetchUntil(reader, results, finished, () -> finished.size() == plans.size());
        }
        return results;
    }

    private static TieringLogSplit onSnapshot(TieringLogSplit split, long snapshotId) {
        return TieringLogSplit.plannedRound(
                split.getTablePath(),
                split.getTableBucket(),
                split.getPartitionName(),
                split.getStartingOffset(),
                split.getStoppingOffset(),
                snapshotId,
                split.isRoundCapped());
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

    private static void fetch(
            TieringSplitReader<TestingWriteResult> reader,
            Map<TableBucket, TableBucketWriteResult<TestingWriteResult>> results,
            Set<String> finished)
            throws Exception {
        RecordsWithSplitIds<TableBucketWriteResult<TestingWriteResult>> fetched = reader.fetch();
        finished.addAll(fetched.finishedSplits());
        while (fetched.nextSplit() != null) {
            TableBucketWriteResult<TestingWriteResult> result = fetched.nextRecordFromSplit();
            results.put(result.tableBucket(), result);
        }
    }

    private static void fetchUntil(
            TieringSplitReader<TestingWriteResult> reader,
            Map<TableBucket, TableBucketWriteResult<TestingWriteResult>> results,
            Set<String> finished,
            BooleanSupplier done)
            throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();
        do {
            assertThat(System.currentTimeMillis())
                    .as("the rounds end, %s ended", finished.size())
                    .isLessThan(deadline);
            fetch(reader, results, finished);
        } while (!done.getAsBoolean());
    }

    /** Writes two versions of 30 keys and plans every bucket's round on {@code snapshotId}. */
    private Map<TableBucket, TieringLogSplit> plannedRounds(String table, long snapshotId)
            throws Exception {
        TablePath tablePath = TablePath.of(DB, table);
        admin.createTable(tablePath, DV_TABLE_DESCRIPTOR, true).get();
        long tableId = admin.getTableInfo(tablePath).get().getTableId();
        for (int bucket = 0; bucket < BUCKETS; bucket++) {
            FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(new TableBucket(tableId, bucket));
        }
        try (Table t = conn.getTable(tablePath)) {
            UpsertWriter writer = t.newUpsert().createWriter();
            for (int version = 0; version < 2; version++) {
                for (int key = 0; key < 30; key++) {
                    writer.upsert(row(key, "v" + version + "_" + key));
                }
                writer.flush();
            }
        }
        Map<TableBucket, Long> starts = new HashMap<>();
        for (int bucket = 0; bucket < BUCKETS; bucket++) {
            starts.put(new TableBucket(tableId, bucket), 0L);
        }
        Map<TableBucket, TieringLogSplit> plans = new HashMap<>();
        for (Map.Entry<TableBucket, GetDvSnapshotResponse> plan :
                new DvRoundRequests(admin, tablePath, Thread::sleep)
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
                            snapshotId,
                            plan.getValue().isRoundCapped()));
        }
        return plans;
    }

    private static long preparationFailures(MetricListener metrics) {
        return metrics.getCounter(
                        TieringMetrics.FLUSS_METRIC_GROUP,
                        TieringMetrics.TIERING_SERVICE_GROUP,
                        MetricNames.TIERING_SERVICE_VECTORS_NOT_PREPARED)
                .map(Counter::getCount)
                .orElse(0L);
    }

    /** How the preparers of a {@link PreparingFactory} prepare. */
    private enum Preparation {
        PREPARED,
        NOT_PREPARED,
        FAILED,
        NO_PREPARER,
        OFF
    }

    /**
     * A factory whose writers prepare their rounds' deletion vectors as {@link Preparation} says.
     */
    private static final class PreparingFactory extends RecordingFactory
            implements SupportsDeletionVectorPreparation<TestingWriteResult> {
        final Preparation preparation;
        @Nullable final CountDownLatch release;
        final Map<TableBucket, Prepared> prepared = new ConcurrentHashMap<>();
        final AtomicInteger failed = new AtomicInteger();
        final AtomicInteger discarded = new AtomicInteger();
        final AtomicInteger created = new AtomicInteger();
        final AtomicInteger preparers = new AtomicInteger();
        final AtomicInteger closed = new AtomicInteger();
        final List<Long> prefetched = new CopyOnWriteArrayList<>();
        final Map<TableBucket, Collection<String>> prefetchedFiles = new ConcurrentHashMap<>();

        PreparingFactory(Preparation preparation) {
            this(preparation, null);
        }

        PreparingFactory(Preparation preparation, @Nullable CountDownLatch release) {
            this.preparation = preparation;
            this.release = release;
        }

        @Override
        public DeletionVectorPreparer<TestingWriteResult> createDeletionVectorPreparer(
                TableInfo tableInfo, Configuration lakeTieringConfig) throws IOException {
            created.incrementAndGet();
            if (preparation == Preparation.NO_PREPARER) {
                throw new IOException("no catalog");
            }
            preparers.incrementAndGet();
            return new DeletionVectorPreparer<TestingWriteResult>() {
                @Override
                public TestingWriteResult prepare(
                        TestingWriteResult writeResult,
                        TableBucket bucket,
                        long snapshotId,
                        long startOffset,
                        long endOffset,
                        Map<String, long[]> deletedPositionsByFile)
                        throws IOException {
                    if (release != null) {
                        try {
                            release.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException(e);
                        }
                    }
                    if (preparation == Preparation.FAILED) {
                        failed.incrementAndGet();
                        throw new IOException("the lake is not there");
                    }
                    prepared.put(
                            bucket,
                            new Prepared(
                                    snapshotId, startOffset, endOffset, deletedPositionsByFile));
                    return preparation == Preparation.PREPARED
                            ? new TestingWriteResult(PREPARED)
                            : null;
                }

                @Override
                public void prefetch(long snapshotId) {
                    prefetched.add(snapshotId);
                }

                @Override
                public void prefetch(
                        TableBucket bucket, long snapshotId, Collection<String> dataFiles) {
                    assertThat(snapshotId).isEqualTo(SNAPSHOT);
                    prefetchedFiles.put(bucket, dataFiles);
                }

                @Override
                public void discard(TestingWriteResult prepared) {
                    assertThat(prepared.getWriteResult()).isEqualTo(PREPARED);
                    discarded.incrementAndGet();
                }

                @Override
                public void close() {
                    closed.incrementAndGet();
                }
            };
        }

        /** What a preparer was asked to prepare. */
        private static final class Prepared {
            final long snapshotId;
            final long startOffset;
            final long endOffset;
            final Map<String, long[]> files;

            Prepared(long snapshotId, long startOffset, long endOffset, Map<String, long[]> files) {
                this.snapshotId = snapshotId;
                this.startOffset = startOffset;
                this.endOffset = endOffset;
                this.files = files;
            }
        }
    }

    /** Creates writers that count their records and whether they completed. */
    private static class RecordingFactory extends TestingLakeTieringFactory {
        final List<RecordingWriter> writers = new CopyOnWriteArrayList<>();

        @Override
        public LakeWriter<TestingWriteResult> createLakeWriter(WriterInitContext context) {
            RecordingWriter writer = new RecordingWriter();
            writers.add(writer);
            return writer;
        }

        int completed() {
            int completed = 0;
            for (RecordingWriter writer : writers) {
                if (writer.completed) {
                    completed++;
                }
            }
            return completed;
        }
    }

    private static class RecordingWriter implements LakeWriter<TestingWriteResult> {
        volatile int records;
        volatile boolean completed;

        @Override
        public void write(LogRecord record) {
            records++;
        }

        @Override
        public TestingWriteResult complete() {
            completed = true;
            return new TestingWriteResult(records);
        }

        @Override
        public void close() {}
    }
}
