/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.kv;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.server.kv.autoinc.AutoIncrementManager;
import org.apache.fluss.server.kv.autoinc.TestingSequenceGeneratorFactory;
import org.apache.fluss.server.kv.dv.DerivedDvManager;
import org.apache.fluss.server.kv.dv.DerivedDvMetrics;
import org.apache.fluss.server.kv.dv.DvEntry;
import org.apache.fluss.server.kv.dv.DvRocksDB;
import org.apache.fluss.server.kv.dv.DvSnapshot;
import org.apache.fluss.server.kv.dv.DvTestManagers;
import org.apache.fluss.server.kv.dv.DvTestReads;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTestUtils;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.utils.RoaringBitmapUtils;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.server.kv.KvTabletTestUtils.flushAndWait;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the hand-off of a {@link KvTablet}'s key changes to its deletion vectors. */
class KvTabletDvHandOffTest {

    private static final short SCHEMA_ID = 1;

    private final Configuration conf = new Configuration();
    private final KvRecordTestUtils.KvRecordBatchFactory batches =
            KvRecordTestUtils.KvRecordBatchFactory.of(SCHEMA_ID);
    private final KvRecordTestUtils.KvRecordFactory records =
            KvRecordTestUtils.KvRecordFactory.of(DATA1_SCHEMA_PK.getRowType());

    private @TempDir File tempLogDir;
    private @TempDir File tmpKvDir;

    private LogTablet logTablet;
    private KvTablet kvTablet;

    @BeforeEach
    void beforeEach() throws Exception {
        PhysicalTablePath path = PhysicalTablePath.of(TablePath.of("db", "dv_handoff"));
        logTablet =
                LogTablet.create(
                        tempLogDir,
                        path,
                        LogTestUtils.makeRandomLogTabletDir(
                                tempLogDir, path.getDatabaseName(), 0L, path.getTableName()),
                        conf,
                        new AtomicBoolean(false),
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        0,
                        new FlussScheduler(1),
                        LogFormat.ARROW,
                        1,
                        true,
                        SystemClock.getInstance(),
                        true);
    }

    /**
     * Opens the KV tablet of a table with deletion vectors, with row TTL in the versioned value
     * layout or without it in the tagged one.
     */
    private void open(boolean rowTtl) throws Exception {
        PhysicalTablePath path = PhysicalTablePath.of(TablePath.of("db", "dv_handoff"));
        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(new SchemaInfo(DATA1_SCHEMA_PK, SCHEMA_ID));
        Configuration tableConf = new Configuration();
        tableConf.set(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED, true);
        if (rowTtl) {
            // long enough that no value expires during the test
            tableConf.set(ConfigOptions.TABLE_KV_TTL, Duration.ofDays(1));
        }
        TableConfig tableConfig = new TableConfig(tableConf);
        kvTablet =
                KvTablet.create(
                        path,
                        logTablet.getTableBucket(),
                        logTablet,
                        tmpKvDir,
                        conf,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        new RootAllocator(Long.MAX_VALUE),
                        new TestingMemorySegmentPool(10 * 1024),
                        KvFormat.COMPACTED,
                        RowMerger.create(tableConfig, KvFormat.COMPACTED, schemaGetter),
                        DEFAULT_COMPRESSION,
                        schemaGetter,
                        tableConfig.getChangelogImage(),
                        KvManager.getDefaultRateLimiter(),
                        new AutoIncrementManager(
                                schemaGetter,
                                path.getTablePath(),
                                new TableConfig(new Configuration()),
                                new TestingSequenceGeneratorFactory()),
                        SystemClock.getInstance(),
                        tableConfig,
                        DvTestManagers.budget(),
                        DvTestManagers.executors());
        assertThat(kvTablet.isDvEnabled()).isTrue();
        assertThat(kvTablet.getKvValueLayout())
                .isEqualTo(rowTtl ? KvValueLayout.VERSIONED : KvValueLayout.TAGGED);
    }

    @AfterEach
    void afterEach() throws Exception {
        if (kvTablet != null) {
            kvTablet.close();
        }
        IOUtils.closeAll(logTablet::close);
    }

    /**
     * Each KV value carries the log offset of its version, and each change hands over the version
     * it supersedes, read from the value: an insert none, an update and a delete the version before
     * them. With row TTL an insert names its superseded version as unknown, as the key's value may
     * have expired.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testChangesNameTheVersionsTheySupersede(boolean rowTtl) throws Exception {
        open(rowTtl);
        List<DvEntry> handedOver = new ArrayList<>();
        replaceManager(
                db ->
                        new DerivedDvManager(
                                db,
                                logTablet::getHighWatermark,
                                DvTestManagers.CHECKPOINT_INTERVAL,
                                DerivedDvMetrics.noop(),
                                DvTestManagers.budget(),
                                DvTestManagers.executors()) {
                            @Override
                            public synchronized void handleChangelogSynced(
                                    List<DvEntry> entries, long logEnd) {
                                handedOver.addAll(entries);
                            }
                        });

        byte[] k1 = "k1".getBytes();
        // an insert at 0, an update of -U at 1 and +U at 2, a delete at 3
        kvTablet.putAsLeader(batches.ofRecords(records.ofRecord(k1, new Object[] {1, "v1"})), null);
        assertThat(kvTablet.versionOffset(k1)).isEqualTo(0L);
        kvTablet.putAsLeader(batches.ofRecords(records.ofRecord(k1, new Object[] {1, "v2"})), null);
        assertThat(kvTablet.versionOffset(k1)).isEqualTo(2L);
        kvTablet.putAsLeader(batches.ofRecords(records.ofRecord(k1, null)), null);
        assertThat(kvTablet.versionOffset(k1)).isNull();

        // an insert at 4, flushed to RocksDB; its update (-U at 5, +U at 6) reads the version
        // from the value in RocksDB
        kvTablet.putAsLeader(batches.ofRecords(records.ofRecord(k1, new Object[] {1, "v3"})), null);
        logTablet.updateHighWatermark(logTablet.localLogEndOffset());
        flushAndWait(kvTablet, logTablet.localLogEndOffset());
        assertThat(kvTablet.versionOffset(k1)).isEqualTo(4L);
        kvTablet.putAsLeader(batches.ofRecords(records.ofRecord(k1, new Object[] {1, "v4"})), null);
        assertThat(kvTablet.versionOffset(k1)).isEqualTo(6L);
        assertThat(handedOver).hasSize(5);
        assertThat(handedOver.get(3).isInsert()).isEqualTo(!rowTtl);
        assertThat(handedOver.get(3).getPreviousVersionOffset()).isEqualTo(-1L);
        assertThat(handedOver.get(4).getRecordOffset()).isEqualTo(6L);
        assertThat(handedOver.get(4).getPreviousVersionOffset()).isEqualTo(4L);

        assertThat(handedOver.get(0).isInsert()).isEqualTo(!rowTtl);
        assertThat(handedOver.get(0).getPreviousVersionOffset()).isEqualTo(-1L);
        assertThat(handedOver.get(1).getRecordOffset()).isEqualTo(2L);
        assertThat(handedOver.get(1).getPreviousVersionOffset()).isEqualTo(0L);
        assertThat(handedOver.get(2).getRecordOffset()).isEqualTo(3L);
        assertThat(handedOver.get(2).getVersionOffset()).isEqualTo(-1L);
        assertThat(handedOver.get(2).getPreviousVersionOffset()).isEqualTo(2L);
    }

    /**
     * A hand-off of a batch's key changes to the deletion vectors fails after the batch is in the
     * log. The write succeeds and keeps its KV state, and the changes are replayed from the log
     * instead.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFailedHandOffKeepsTheAppendedBatch(boolean rowTtl) throws Exception {
        open(rowTtl);
        // the write path hands its changes to a manager that fails to take them
        AtomicInteger handOffs = new AtomicInteger();
        replaceManager(
                db ->
                        new DerivedDvManager(
                                db,
                                logTablet::getHighWatermark,
                                DvTestManagers.CHECKPOINT_INTERVAL,
                                DerivedDvMetrics.noop(),
                                DvTestManagers.budget(),
                                DvTestManagers.executors()) {
                            @Override
                            public synchronized void handleChangelogSynced(
                                    List<DvEntry> entries, long logEnd) {
                                handOffs.incrementAndGet();
                                throw new IllegalStateException("hand-off failed");
                            }
                        });

        assertThatCode(
                        () ->
                                kvTablet.putAsLeader(
                                        batches.ofRecords(
                                                records.ofRecord(
                                                        "k1".getBytes(), new Object[] {1, "v1"})),
                                        null))
                .doesNotThrowAnyException();
        assertThat(handOffs).hasValue(1);
        long logEnd = logTablet.localLogEndOffset();
        assertThat(logEnd).isEqualTo(1L);
        assertThat(kvTablet.multiGetFromBufferOrKv(Collections.singletonList("k1".getBytes())))
                .doesNotContainNull();

        // the next write of the key is an update of the appended row, not a second insert
        kvTablet.putAsLeader(
                batches.ofRecords(records.ofRecord("k1".getBytes(), new Object[] {1, "v2"})), null);
        assertThat(logTablet.localLogEndOffset()).isEqualTo(logEnd + 2);
    }

    /**
     * A batch is in the log before the write path hands its changes to the deletion vectors, so
     * followers can move the high watermark past it first. A union read meanwhile builds its masks
     * up to the last batch handed over, not up to the high watermark: masks up to the high
     * watermark would leave the batch's superseded version unmasked, and its key read twice.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testMasksGoNoFurtherThanTheChangesHandedOver(boolean rowTtl) throws Exception {
        open(rowTtl);
        DerivedDvManager manager = kvTablet.getDvManager();
        DvTestManagers.setMaskCacheMillis(manager, 0);
        DvTestManagers.coverTheEmptyLake(manager, logTablet::localLogEndOffset);
        byte[] k1 = "k1".getBytes();
        // an insert at 0, an update of -U at 1 and +U at 2
        kvTablet.putAsLeader(batches.ofRecords(records.ofRecord(k1, new Object[] {1, "v1"})), null);
        kvTablet.putAsLeader(batches.ofRecords(records.ofRecord(k1, new Object[] {1, "v2"})), null);
        logTablet.updateHighWatermark(3);

        ExecutorService writer = Executors.newSingleThreadExecutor();
        CountDownLatch handingOff = new CountDownLatch(1);
        CountDownLatch handOff = new CountDownLatch(1);
        // the write path waits before it hands a batch over to the tablet's manager
        replaceManager(
                db ->
                        new DerivedDvManager(
                                db,
                                logTablet::getHighWatermark,
                                DvTestManagers.CHECKPOINT_INTERVAL,
                                DerivedDvMetrics.noop(),
                                DvTestManagers.budget(),
                                DvTestManagers.executors()) {
                            @Override
                            public void handleChangelogSynced(List<DvEntry> entries, long logEnd) {
                                handingOff.countDown();
                                try {
                                    handOff.await(30, TimeUnit.SECONDS);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                manager.handleChangelogSynced(entries, logEnd);
                            }

                            @Override
                            public void handOffFailed(Throwable cause) {
                                manager.handOffFailed(cause);
                            }
                        });
        try {
            DvSnapshot during;
            Future<?> write;
            // an update of -U at 3 and +U at 4 is appended and waits before its hand-off
            write =
                    writer.submit(
                            () -> {
                                kvTablet.putAsLeader(
                                        batches.ofRecords(
                                                records.ofRecord(k1, new Object[] {1, "v3"})),
                                        null);
                                return null;
                            });
            assertThat(handingOff.await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(logTablet.localLogEndOffset()).isEqualTo(5L);
            // followers fetched it: the high watermark passes the batch before its hand-off
            logTablet.updateHighWatermark(5);
            during = DvTestReads.unionRead(manager, -1);
            handOff.countDown();
            write.get(30, TimeUnit.SECONDS);
            // the masks stop at the batch handed over last, where only offset 0 is superseded
            assertThat(during.getLogEndOffset()).isEqualTo(3L);
            assertThat(deadVersions(during)).containsExactly(0L);

            DvSnapshot after = DvTestReads.unionRead(manager, -1);
            assertThat(after.getLogEndOffset()).isEqualTo(5L);
            assertThat(deadVersions(after)).containsExactly(0L, 2L);
        } finally {
            writer.shutdownNow();
        }
    }

    /**
     * A tablet dropped while a background reader still holds its deletion vector RocksDB open keeps
     * its directory, where that RocksDB lives, until the RocksDB closes, then deletes it.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDropDeletesTheDirectoryOnceTheDvRocksDbCloses(boolean rowTtl) throws Exception {
        open(rowTtl);
        DerivedDvManager manager = kvTablet.getDvManager();
        DvTestManagers.setCloseWaitMillis(manager, 100);
        // a reader still uses the RocksDB
        DvTestManagers.holdReader(manager);
        // the close waits for the reader a while, then leaves the RocksDB to it
        CompletableFuture<Void> deleted = kvTablet.drop();
        assertThat(deleted).isNotDone();
        assertThat(kvTablet.getKvTabletDir()).exists();

        DvTestManagers.releaseReader(manager);
        deleted.get(30, TimeUnit.SECONDS);
        assertThat(manager.dbClosed()).isDone();
        assertThat(kvTablet.getKvTabletDir()).doesNotExist();
    }

    /**
     * A tablet whose deletion vector RocksDB cannot open fails to open, and closes its KV RocksDB
     * again, so the next open of the directory succeeds.
     */
    @Test
    void testFailedDvRocksDbOpenClosesTheKvRocksDb() throws Exception {
        // a file where the directory of the deletion vector RocksDB goes
        File blocker = new File(tmpKvDir, "dv");
        assertThat(blocker.createNewFile()).isTrue();
        assertThatThrownBy(() -> open(false)).isNotNull();
        kvTablet = null;

        assertThat(blocker.delete()).isTrue();
        open(false);
        assertThat(kvTablet.isDvEnabled()).isTrue();
    }

    private static List<Long> deadVersions(DvSnapshot masks) throws Exception {
        List<Long> out = new ArrayList<>();
        if (masks.getLogDvBitmap() != null) {
            Roaring64Bitmap bitmap = new Roaring64Bitmap();
            RoaringBitmapUtils.deserializeRoaringBitmap64(bitmap, masks.getLogDvBitmap());
            bitmap.forEach(out::add);
        }
        return out;
    }

    /** Makes the write path hand its changes to a manager on the tablet's DV RocksDB. */
    private void replaceManager(Function<DvRocksDB, DerivedDvManager> factory) {
        kvTablet.handChangesTo(factory.apply(DvTestManagers.db(kvTablet.getDvManager())));
    }
}
