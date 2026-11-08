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
import org.apache.fluss.server.kv.autoinc.AutoIncrementManager;
import org.apache.fluss.server.kv.autoinc.TestingSequenceGeneratorFactory;
import org.apache.fluss.server.kv.dv.DerivedDvManager;
import org.apache.fluss.server.kv.dv.DvEntry;
import org.apache.fluss.server.kv.dv.DvRocksDB;
import org.apache.fluss.server.kv.dv.DvSnapshot;
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
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

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
        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(new SchemaInfo(DATA1_SCHEMA_PK, SCHEMA_ID));
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
        Configuration tableConf = new Configuration();
        tableConf.set(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED, true);
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
                        tableConfig);
        assertThat(kvTablet.isDvEnabled()).isTrue();
    }

    @AfterEach
    void afterEach() throws Exception {
        IOUtils.closeAll(kvTablet::close, logTablet::close);
    }

    /**
     * Each KV value is tagged with the log offset of its version, and each change hands over the
     * version it supersedes, read from that tag: an insert none, an update and a delete the version
     * before them.
     */
    @Test
    void testChangesNameTheVersionsTheySupersede() throws Exception {
        List<DvEntry> handedOver = new ArrayList<>();
        replaceManager(
                db ->
                        new DerivedDvManager(db, logTablet::getHighWatermark) {
                            @Override
                            public synchronized void handleChangelogSynced(
                                    List<DvEntry> entries, long logEnd) {
                                handedOver.addAll(entries);
                            }
                        });

        byte[] k1 = "k1".getBytes();
        // an insert at 0, an update of -U at 1 and +U at 2, a delete at 3
        kvTablet.putAsLeader(batches.ofRecords(records.ofRecord(k1, new Object[] {1, "v1"})), null);
        assertThat(kvTablet.valueTag(k1)).isEqualTo(0L);
        kvTablet.putAsLeader(batches.ofRecords(records.ofRecord(k1, new Object[] {1, "v2"})), null);
        assertThat(kvTablet.valueTag(k1)).isEqualTo(2L);
        kvTablet.putAsLeader(batches.ofRecords(records.ofRecord(k1, null)), null);
        assertThat(kvTablet.valueTag(k1)).isNull();

        assertThat(handedOver).hasSize(3);
        assertThat(handedOver.get(0).isInsert()).isTrue();
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
    @Test
    void testFailedHandOffKeepsTheAppendedBatch() throws Exception {
        // the write path hands its changes to a manager that fails to take them
        AtomicInteger handOffs = new AtomicInteger();
        replaceManager(
                db ->
                        new DerivedDvManager(db, logTablet::getHighWatermark) {
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

    /** Makes the write path hand its changes to a manager on the tablet's DV RocksDB. */
    /**
     * A batch is in the log before the write path hands its changes to the deletion vectors, so
     * followers can move the high watermark past it first. A union read meanwhile builds its masks
     * up to the last batch handed over, not up to the high watermark: masks up to the high
     * watermark would leave the batch's superseded version unmasked, and its key read twice.
     */
    @Test
    void testMasksGoNoFurtherThanTheChangesHandedOver() throws Exception {
        DerivedDvManager manager = manager();
        Method noCache = DerivedDvManager.class.getDeclaredMethod("setMaskCacheMillis", long.class);
        noCache.setAccessible(true);
        noCache.invoke(manager, 0L);
        byte[] k1 = "k1".getBytes();
        // an insert at 0, an update of -U at 1 and +U at 2
        kvTablet.putAsLeader(batches.ofRecords(records.ofRecord(k1, new Object[] {1, "v1"})), null);
        kvTablet.putAsLeader(batches.ofRecords(records.ofRecord(k1, new Object[] {1, "v2"})), null);
        logTablet.updateHighWatermark(3);

        ExecutorService writer = Executors.newSingleThreadExecutor();
        try {
            DvSnapshot during;
            Future<?> write;
            synchronized (manager) {
                // an update of -U at 3 and +U at 4 is appended and waits for the manager's lock
                write =
                        writer.submit(
                                () -> {
                                    kvTablet.putAsLeader(
                                            batches.ofRecords(
                                                    records.ofRecord(k1, new Object[] {1, "v3"})),
                                            null);
                                    return null;
                                });
                long deadline = System.currentTimeMillis() + 30_000;
                while (logTablet.localLogEndOffset() < 5) {
                    assertThat(System.currentTimeMillis()).isLessThan(deadline);
                    Thread.sleep(10);
                }
                // followers fetched it: the high watermark passes the batch before its hand-off
                logTablet.updateHighWatermark(5);
                during = manager.getDvForUnionRead(-1);
            }
            write.get(30, TimeUnit.SECONDS);
            // the masks stop at the batch handed over last, where only offset 0 is superseded
            assertThat(during.getLogEndOffset()).isEqualTo(3L);
            assertThat(deadVersions(during)).containsExactly(0L);

            DvSnapshot after = manager.getDvForUnionRead(-1);
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
    @Test
    void testDropDeletesTheDirectoryOnceTheDvRocksDbCloses() throws Exception {
        DerivedDvManager manager = manager();
        Field readers = DerivedDvManager.class.getDeclaredField("backgroundReaders");
        readers.setAccessible(true);
        synchronized (manager) {
            readers.setInt(manager, 1); // a reader still uses the RocksDB
        }
        // the close waits for the reader a while, then leaves the RocksDB to it
        CompletableFuture<Void> deleted = kvTablet.drop();
        assertThat(deleted).isNotDone();
        assertThat(kvTablet.getKvTabletDir()).exists();

        Method release = DerivedDvManager.class.getDeclaredMethod("releaseDb");
        release.setAccessible(true);
        release.invoke(manager);
        deleted.get(30, TimeUnit.SECONDS);
        assertThat(manager.dbClosed()).isDone();
        assertThat(kvTablet.getKvTabletDir()).doesNotExist();
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

    private DerivedDvManager manager() throws Exception {
        Field processorField = KvTablet.class.getDeclaredField("kvWriteProcessor");
        processorField.setAccessible(true);
        Object processor = processorField.get(kvTablet);
        Field managerField = processor.getClass().getDeclaredField("dvManager");
        managerField.setAccessible(true);
        return (DerivedDvManager) managerField.get(processor);
    }

    private void replaceManager(Function<DvRocksDB, DerivedDvManager> factory) throws Exception {
        Field processorField = KvTablet.class.getDeclaredField("kvWriteProcessor");
        processorField.setAccessible(true);
        Object processor = processorField.get(kvTablet);
        Field managerField = processor.getClass().getDeclaredField("dvManager");
        managerField.setAccessible(true);
        DerivedDvManager manager = (DerivedDvManager) managerField.get(processor);
        Field dbField = DerivedDvManager.class.getDeclaredField("db");
        dbField.setAccessible(true);
        managerField.set(processor, factory.apply((DvRocksDB) dbField.get(manager)));
    }
}
