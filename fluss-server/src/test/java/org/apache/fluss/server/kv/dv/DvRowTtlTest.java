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

package org.apache.fluss.server.kv.dv;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.server.kv.KvManager;
import org.apache.fluss.server.kv.KvRecoverHelper;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.KvTabletTestUtils;
import org.apache.fluss.server.kv.RemoteLogFetcher;
import org.apache.fluss.server.kv.autoinc.AutoIncrementManager;
import org.apache.fluss.server.kv.autoinc.TestingSequenceGeneratorFactory;
import org.apache.fluss.server.kv.dv.DerivedDvManagerTest.FakeSource;
import org.apache.fluss.server.kv.dv.DerivedDvManagerTest.Index;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTestUtils;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.utils.clock.ManualClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import io.github.fluss_contrib.rocksdb.Checkpoint;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.dict;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.lakeMasks;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.offsets;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Deletion vectors of a table with row TTL: the versioned value layout keeps the version offset and
 * the row TTL timestamp in slots of their own.
 *
 * <p>Row TTL removes a key's value without a change in the log, while the lake can still hold the
 * key's row. So in such a table a write that finds no value cannot count as superseding nothing:
 * its key is looked up in the lake index, and a miss there masks nothing. Without row TTL an insert
 * keeps its free pass. A value whose time is up but that compaction has not removed yet is read
 * like any other, and a change of it names its version from the slot.
 */
class DvRowTtlTest {

    private static final short SCHEMA_ID = 1;
    private static final TablePath TABLE_PATH = TablePath.of("db", "dv_row_ttl");
    private static final long T0 = 1_000_000_000L;
    private static final Duration TTL = Duration.ofHours(1);
    private static final long SNAPSHOT = 100;

    @TempDir Path tempDir;

    private final KvRecordTestUtils.KvRecordBatchFactory batches =
            KvRecordTestUtils.KvRecordBatchFactory.of(SCHEMA_ID);
    private final KvRecordTestUtils.KvRecordFactory records =
            KvRecordTestUtils.KvRecordFactory.of(DATA1_SCHEMA_PK.getRowType());
    private final TestingSchemaGetter schemaGetter =
            new TestingSchemaGetter(new SchemaInfo(DATA1_SCHEMA_PK, SCHEMA_ID));
    private final ManualClock clock = new ManualClock(T0);

    private TableRegistration registration;
    private TableInfo tableInfo;
    private KeyEncoder keyEncoder;
    private FlussScheduler scheduler;
    private RootAllocator allocator;
    private LogTablet log;
    private KvTablet kv;
    // the manager the test hands the write path's changes to, and its RocksDB
    @Nullable private DerivedDvManager manager;
    @Nullable private DvRocksDB managerDb;
    private final List<DvEntry> handedOver = new ArrayList<>();

    @AfterEach
    void afterEach() throws Exception {
        if (kv != null) {
            processorManager().set(processor(), null);
            kv.close();
        }
        if (manager != null) {
            manager.close();
        }
        if (managerDb != null) {
            managerDb.close();
        }
        if (log != null) {
            log.close();
        }
        if (scheduler != null) {
            scheduler.shutdown();
        }
        if (allocator != null) {
            try {
                allocator.close();
            } catch (RuntimeException e) {
                // the KV's Arrow writers may still hold buffers of it
            }
        }
    }

    /**
     * Key 1 is in the lake, row TTL expires it, and it is written again. The union read masks its
     * lake row, which would otherwise be read next to the new version. Key 3, never in the lake,
     * costs a lookup too, which misses; key 2, expired and not written again, keeps its lake row.
     */
    @Test
    void testWriteAfterExpiryMasksTheLakeRow() throws Exception {
        open(true);
        assertThat(kv.getKvValueLayout()).isSameAs(KvValueLayout.VERSIONED);
        long loadEnd = loadIntoLake(1, 2);
        assertSlots(1, 0L, T0);
        assertSlots(2, 1L, T0);

        // the time of both is up, and compaction removes their values
        clock.advanceTime(TTL.plusMinutes(1));
        kv.getRocksDBKv().getDb().compactRange();
        assertThat(kv.versionOffset(key(1))).isNull();
        assertThat(kv.versionOffset(key(2))).isNull();
        assertThat(kv.getRocksDBKv().get(key(2))).isNull();

        long lookupsBefore = manager.lookedUpKeys();
        put(upsert(1, "again"), upsert(3, "new"));
        // two inserts in the log, as the KV holds neither key
        assertThat(log.localLogEndOffset()).isEqualTo(loadEnd + 2);
        assertThat(handedOver).hasSize(2);
        for (DvEntry e : handedOver) {
            assertThat(e.isInsert()).isFalse();
            assertThat(e.getPreviousVersionOffset()).isEqualTo(-1L);
        }
        assertSlots(1, loadEnd, clock.milliseconds());

        DvSnapshot masks = unionRead();
        assertThat(lakeMasks(masks)).containsOnlyKeys("f1");
        assertThat(lakeMasks(masks).get("f1")).containsExactly(0L);
        assertThat(offsets(masks.getLogDvBitmap())).isEmpty();
        assertThat(manager.lookedUpKeys() - lookupsBefore).isEqualTo(2);
    }

    /**
     * A value whose time is up but that compaction has not removed is still the key's value: a
     * change of it is an update that names its version from the slot, in the lake or in the log
     * tail, and writes -U and +U.
     */
    @Test
    void testExpiredValueNotYetCompactedIsUpdated() throws Exception {
        open(true);
        long loadEnd = loadIntoLake(1);
        // key 2 has its version in the log tail; its insert is looked up and misses
        put(upsert(2, "tail"));
        assertThat(kv.versionOffset(key(2))).isEqualTo(loadEnd);
        assertThat(lakeMasks(unionRead())).isEmpty();
        KvTabletTestUtils.flushAndWait(kv, log.localLogEndOffset());

        clock.advanceTime(TTL.plusMinutes(1));
        handedOver.clear();
        long lookupsBefore = manager.lookedUpKeys();
        long before = log.localLogEndOffset();
        put(upsert(1, "late"), upsert(2, "late"));
        assertThat(log.localLogEndOffset()).isEqualTo(before + 4);
        assertThat(handedOver).hasSize(2);
        assertThat(handedOver.get(0).isInsert()).isFalse();
        assertThat(handedOver.get(0).getPreviousVersionOffset()).isEqualTo(0L);
        assertThat(handedOver.get(1).isInsert()).isFalse();
        assertThat(handedOver.get(1).getPreviousVersionOffset()).isEqualTo(loadEnd);
        // the new versions are stamped with the time of their batch
        assertSlots(1, before + 1, clock.milliseconds());
        assertSlots(2, before + 3, clock.milliseconds());

        DvSnapshot masks = unionRead();
        assertThat(lakeMasks(masks).get("f1")).containsExactly(0L);
        // key 2's version in the tail is named exactly, without a lookup
        assertThat(offsets(masks.getLogDvBitmap())).containsExactly(loadEnd);
        assertThat(manager.lookedUpKeys() - lookupsBefore).isEqualTo(1);
    }

    /**
     * Without row TTL an insert supersedes nothing: new keys are handed over as inserts and looked
     * up nowhere. Only the update of a lake key is looked up.
     */
    @Test
    void testInsertsWithoutRowTtlLookNothingUp() throws Exception {
        open(false);
        loadIntoLake(1, 2);

        long lookupsBefore = manager.lookedUpKeys();
        put(upsert(3, "new"), upsert(4, "new"));
        assertThat(handedOver).hasSize(2);
        for (DvEntry e : handedOver) {
            assertThat(e.isInsert()).isTrue();
            assertThat(e.getPreviousVersionOffset()).isEqualTo(-1L);
        }
        DvSnapshot masks = unionRead();
        assertThat(lakeMasks(masks)).isEmpty();
        assertThat(offsets(masks.getLogDvBitmap())).isEmpty();
        assertThat(manager.lookedUpKeys()).isEqualTo(lookupsBefore);

        put(upsert(2, "update"));
        masks = unionRead();
        assertThat(lakeMasks(masks).get("f1")).containsExactly(1L);
        assertThat(manager.lookedUpKeys() - lookupsBefore).isEqualTo(1);
    }

    /**
     * The versioned layout carries the version offset and the row TTL timestamp of each value
     * through a KV recovery: the restored KV replays the log after its checkpoint and writes each
     * replayed version with its offset and the commit time of its batch, as the write path stamped
     * it.
     */
    @Test
    void testRecoveryWritesBothSlots() throws Exception {
        open(true);
        processorManager().set(processor(), null);
        put(upsert(1, "a"), upsert(2, "b"));
        advanceHighWatermark();
        KvTabletTestUtils.flushAndWait(kv, log.localLogEndOffset());
        long recoverPoint = kv.getFlushedLogOffset();
        assertThat(recoverPoint).isEqualTo(2L);
        File restored = tempDir.resolve("restored").toFile();
        assertThat(restored.mkdirs()).isTrue();
        try (Checkpoint checkpoint = Checkpoint.create(kv.getRocksDBKv().getDb())) {
            checkpoint.createCheckpoint(new File(restored, "db").getPath());
        }

        // after the checkpoint, only in the log: key 1 updated (-U at 2, +U at 3), key 3 at 4
        clock.advanceTime(Duration.ofMinutes(10));
        long t1 = clock.milliseconds();
        put(upsert(1, "c"), upsert(3, "d"));
        advanceHighWatermark();
        KvTabletTestUtils.flushAndWait(kv, log.localLogEndOffset());
        assertSlots(1, 3L, t1);
        assertSlots(2, 1L, T0);
        assertSlots(3, 4L, t1);
        kv.close();
        kv = null;

        kv = createKv(restored, tableInfo.getTableConfig());
        assertSlots(1, 0L, T0);
        ZooKeeperClient zk = mock(ZooKeeperClient.class);
        when(zk.getCurrentSchemaId(TABLE_PATH)).thenReturn((int) SCHEMA_ID);
        when(zk.getSchemaById(TABLE_PATH, SCHEMA_ID))
                .thenReturn(Optional.of(new SchemaInfo(DATA1_SCHEMA_PK, SCHEMA_ID)));
        when(zk.getTable(TABLE_PATH)).thenReturn(Optional.of(registration));
        new KvRecoverHelper(
                        kv,
                        log,
                        recoverPoint,
                        null,
                        null,
                        new KvRecoverHelper.KvRecoverContext(TABLE_PATH, zk, 1024 * 1024),
                        KvFormat.COMPACTED,
                        LogFormat.ARROW,
                        schemaGetter,
                        mock(RemoteLogFetcher.class),
                        false)
                .recover();
        assertThat(kv.getFlushedLogOffset()).isEqualTo(5L);
        assertSlots(1, 3L, t1);
        assertSlots(2, 1L, T0);
        assertSlots(3, 4L, t1);
    }

    /**
     * A log replay of a table with row TTL treats a replayed insert as a change of the key's last
     * version, as the write path does: a key whose lake row expired and was inserted again masks
     * that row, and a key whose version in the replayed log expired and was inserted again masks
     * that version, which the replay names exactly from its map of last versions. Without row TTL
     * the same inserts mask nothing.
     */
    @Test
    void testReplayedInsertsSupersedeTheLastVersionWithRowTtl() throws Exception {
        assertThat(replayMasks(true)).containsExactly("lake f1 [0]", "log [11]");
        assertThat(replayMasks(false)).containsExactly("log []");
    }

    /** The masks of a replay of three inserts, with or without row TTL. */
    private List<String> replayMasks(boolean rowTtl) throws Exception {
        Path dir = Files.createDirectories(tempDir.resolve("replay-" + rowTtl));
        FakeSource src = new FakeSource(Files.createDirectories(dir.resolve("remote")));
        // the lake holds key 1, readable from offset 10
        src.publish(SNAPSHOT, 1, 10, dict(1, "f1"), new Index().put("k1", 1, 0).build());
        src.readable = new long[] {SNAPSHOT, 10};
        // key 1 inserted after its lake row expired; key 3 inserted, expired in the log, inserted
        src.append("k1", 10, 10, true);
        src.append("k3", 11, 11, true);
        src.append("k3", 12, 12, true);
        AtomicLong highWatermark = new AtomicLong(13);
        try (DvRocksDB db = DvRocksDB.open(dir.resolve("dv").toString())) {
            DerivedDvManager m = DvTestManagers.newManager(db, highWatermark::get);
            try {
                m.setMaskCacheMillis(0);
                if (rowTtl) {
                    m.useRowTtl();
                }
                m.recover(src);
                DvSnapshot masks = unionRead(m);
                List<String> out = new ArrayList<>();
                lakeMasks(masks).forEach((file, rows) -> out.add("lake " + file + " " + rows));
                out.add("log " + offsets(masks.getLogDvBitmap()));
                return out;
            } finally {
                m.close();
            }
        }
    }

    // ------------------------------------------------------------------------------------------

    /**
     * Inserts the keys with the write path's manager detached, flushes them, and starts a manager
     * that covers lake snapshot {@link #SNAPSHOT}, which holds key i at row i - 1 of file f1 and is
     * readable from the log end. Returns the log end.
     */
    private long loadIntoLake(int... ids) throws Exception {
        processorManager().set(processor(), null);
        List<KvRecord> load = new ArrayList<>();
        Index index = new Index();
        for (int id : ids) {
            load.add(upsert(id, "lake"));
            index.put(key(id), 1, id - 1);
        }
        put(load.toArray(new KvRecord[0]));
        long loadEnd = log.localLogEndOffset();
        advanceHighWatermark();
        KvTabletTestUtils.flushAndWait(kv, loadEnd);

        FakeSource src = new FakeSource(Files.createDirectories(tempDir.resolve("remote")));
        src.publish(SNAPSHOT, 1, loadEnd, dict(1, "f1"), index.build());
        src.readable = new long[] {SNAPSHOT, loadEnd};
        managerDb = DvRocksDB.open(tempDir.resolve("dv").toString());
        manager =
                new DerivedDvManager(
                        managerDb,
                        log::getHighWatermark,
                        DvTestManagers.CHECKPOINT_INTERVAL,
                        DerivedDvMetrics.noop(),
                        DvTestManagers.budget(),
                        DvTestManagers.executors()) {
                    @Override
                    public synchronized void handleChangelogSynced(
                            List<DvEntry> entries, long logEnd) {
                        handedOver.addAll(entries);
                        super.handleChangelogSynced(entries, logEnd);
                    }
                };
        manager.setMaskCacheMillis(0);
        if (tableInfo.getTableConfig().getKvTTL().isPresent()) {
            manager.useRowTtl();
        }
        manager.recover(src);
        processorManager().set(processor(), manager);
        assertThat(lakeMasks(unionRead())).isEmpty();
        return loadEnd;
    }

    private KvRecord upsert(int id, String value) {
        return records.ofRecord(key(id), new Object[] {id, value});
    }

    private byte[] key(int id) {
        return keyEncoder.encodeKey(row(id, ""));
    }

    private void put(KvRecord... batch) throws Exception {
        kv.putAsLeader(batches.ofRecords(Arrays.asList(batch)), null);
    }

    private void advanceHighWatermark() {
        log.updateHighWatermark(log.localLogEndOffset());
    }

    /** The masks of the covered snapshot, once the high watermark passed every change. */
    private DvSnapshot unionRead() throws Exception {
        advanceHighWatermark();
        return unionRead(manager);
    }

    private static DvSnapshot unionRead(DerivedDvManager m) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (true) {
            try {
                return DvTestReads.unionRead(m, SNAPSHOT);
            } catch (RetriableException e) {
                assertThat(System.currentTimeMillis()).isLessThan(deadline);
                Thread.sleep(10);
            }
        }
    }

    /** The key's value carries the version offset and the TTL timestamp in their slots. */
    private void assertSlots(int id, long versionOffset, long ttlTimestamp) throws Exception {
        assertThat(kv.versionOffset(key(id))).isEqualTo(versionOffset);
        Method buffer = KvTablet.class.getDeclaredMethod("getKvPreWriteBuffer");
        buffer.setAccessible(true);
        KvPreWriteBuffer.Value buffered =
                ((KvPreWriteBuffer) buffer.invoke(kv)).get(KvPreWriteBuffer.Key.of(key(id)));
        byte[] raw = buffered != null ? buffered.get() : kv.getRocksDBKv().get(key(id));
        assertThat(raw).isNotNull();
        MemorySegment value = MemorySegment.wrap(raw);
        assertThat(KvValueLayout.VERSIONED.readVersionOffset(value, 0)).isEqualTo(versionOffset);
        assertThat(KvValueLayout.VERSIONED.readTtlTimestamp(value, 0)).isEqualTo(ttlTimestamp);
    }

    private void open(boolean rowTtl) throws Exception {
        // the table as the coordinator creates it: the versioned layout with row TTL, the tagged
        // one without
        KvValueLayout layout = rowTtl ? KvValueLayout.VERSIONED : KvValueLayout.TAGGED;
        TableDescriptor.Builder descriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA_PK)
                        .distributedBy(1, "a")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key(), "true")
                        .property(
                                ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION.key(),
                                String.valueOf(layout.version()));
        if (rowTtl) {
            descriptor.property(ConfigOptions.TABLE_KV_TTL.key(), "1 h");
        }
        registration =
                TableRegistration.newTable(
                        1L, tempDir.resolve("remote-data").toString(), descriptor.build());
        tableInfo =
                registration.toTableInfo(TABLE_PATH, new SchemaInfo(DATA1_SCHEMA_PK, SCHEMA_ID));
        keyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        tableInfo.getRowType(),
                        tableInfo.getPhysicalPrimaryKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());
        File logDir = Files.createDirectories(tempDir.resolve("log")).toFile();
        scheduler = new FlussScheduler(1);
        allocator = new RootAllocator(Long.MAX_VALUE);
        PhysicalTablePath path = PhysicalTablePath.of(TABLE_PATH);
        log =
                LogTablet.create(
                        logDir,
                        path,
                        LogTestUtils.makeRandomLogTabletDir(
                                logDir, path.getDatabaseName(), 0L, path.getTableName()),
                        new Configuration(),
                        new AtomicBoolean(false),
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        0,
                        scheduler,
                        LogFormat.ARROW,
                        1,
                        true,
                        clock,
                        true);
        kv =
                createKv(
                        Files.createDirectories(tempDir.resolve("kv")).toFile(),
                        tableInfo.getTableConfig());
    }

    private KvTablet createKv(File kvDir, TableConfig tableConfig) throws Exception {
        return KvTablet.create(
                PhysicalTablePath.of(TABLE_PATH),
                log.getTableBucket(),
                log,
                kvDir,
                new Configuration(),
                TestingMetricGroups.TABLET_SERVER_METRICS,
                allocator,
                new TestingMemorySegmentPool(10 * 1024),
                KvFormat.COMPACTED,
                RowMerger.create(tableConfig, KvFormat.COMPACTED, schemaGetter),
                DEFAULT_COMPRESSION,
                schemaGetter,
                tableConfig.getChangelogImage(),
                KvManager.getDefaultRateLimiter(),
                new AutoIncrementManager(
                        schemaGetter,
                        TABLE_PATH,
                        new TableConfig(new Configuration()),
                        new TestingSequenceGeneratorFactory()),
                clock,
                tableConfig,
                DvTestManagers.budget(),
                DvTestManagers.executors());
    }

    private Object processor() throws Exception {
        Field f = KvTablet.class.getDeclaredField("kvWriteProcessor");
        f.setAccessible(true);
        return f.get(kv);
    }

    private Field processorManager() throws Exception {
        Field f = processor().getClass().getDeclaredField("dvManager");
        f.setAccessible(true);
        return f;
    }
}
