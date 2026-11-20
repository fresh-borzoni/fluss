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
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
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
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.KvTabletTestUtils;
import org.apache.fluss.server.kv.RowTtlFilterClock;
import org.apache.fluss.server.kv.autoinc.AutoIncrementManager;
import org.apache.fluss.server.kv.autoinc.TestingSequenceGeneratorFactory;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTestUtils;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.clock.ManualClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Row TTL by event time on a table with deletion vectors: the KV's filter is capped below the
 * oldest event time of a version at or after the readable offset, which the tablet tracks from each
 * value's TTL slot before the value reaches the KV.
 */
class DvRowTtlEventTimeTest {

    private static final short SCHEMA_ID = 1;
    private static final TablePath TABLE_PATH = TablePath.of("db", "dv_row_ttl_event_time");
    private static final long T0 = 1_000_000_000_000L;
    private static final Duration TTL = Duration.ofHours(1);
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .column("b", DataTypes.STRING())
                    .column("t", DataTypes.BIGINT())
                    .primaryKey("a")
                    .build();

    @TempDir Path tempDir;

    private final KvRecordTestUtils.KvRecordBatchFactory batches =
            KvRecordTestUtils.KvRecordBatchFactory.of(SCHEMA_ID);
    private final KvRecordTestUtils.KvRecordFactory records =
            KvRecordTestUtils.KvRecordFactory.of(SCHEMA.getRowType());
    private final TestingSchemaGetter schemaGetter =
            new TestingSchemaGetter(new SchemaInfo(SCHEMA, SCHEMA_ID));
    private final ManualClock clock = new ManualClock(T0);

    private KeyEncoder keyEncoder;
    private FlussScheduler scheduler;
    private RootAllocator allocator;
    private LogTablet log;
    private KvTablet kv;

    @BeforeEach
    void beforeEach() throws Exception {
        TableRegistration registration =
                TableRegistration.newTable(
                        1L,
                        tempDir.resolve("remote-data").toString(),
                        TableDescriptor.builder()
                                .schema(SCHEMA)
                                .distributedBy(1, "a")
                                .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                                .property(
                                        ConfigOptions.TABLE_DATALAKE_DELETION_VECTORS_ENABLED.key(),
                                        "true")
                                .property(
                                        ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION.key(),
                                        String.valueOf(KvValueLayout.VERSIONED.version()))
                                .property(ConfigOptions.TABLE_KV_TTL.key(), "1 h")
                                .property(ConfigOptions.TABLE_KV_TTL_TIME_COLUMN.key(), "t")
                                .build());
        TableInfo tableInfo =
                registration.toTableInfo(TABLE_PATH, new SchemaInfo(SCHEMA, SCHEMA_ID));
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
        TableConfig tableConfig = tableInfo.getTableConfig();
        kv =
                KvTablet.create(
                        path,
                        log.getTableBucket(),
                        log,
                        Files.createDirectories(tempDir.resolve("kv")).toFile(),
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

    @AfterEach
    void afterEach() throws Exception {
        if (kv != null) {
            processorManager().set(processor(), null);
            kv.close();
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
     * Key 1 is backfilled with an event time two days old, its TTL long passed when it is written;
     * key 2 is current. While neither is tiered, the filter keeps both, however long it waits; key
     * 3, whose event time is null, never expires. Once a snapshot holds keys 1 and 2, the cap lifts
     * and both expire.
     */
    @Test
    void testABackfilledRowOutlivesItsTtlUntilItIsTiered() throws Exception {
        RowTtlFilterClock filter = kv.getDvManager().rowTtlFilterClock();
        // the bucket's deletion state is not recovered here: its changes are not handed over
        processorManager().set(processor(), null);
        // the leader covers a snapshot readable from 0: every version is above it
        filter.coveredAt(0);
        long backfilled = T0 - Duration.ofDays(2).toMillis();
        put(upsert(1, "late", backfilled), upsert(2, "now", T0), upsert(3, "never", null));
        long end = log.localLogEndOffset();
        log.updateHighWatermark(end);
        KvTabletTestUtils.flushAndWait(kv, end);

        clock.advanceTime(Duration.ofDays(7));
        assertThat(filter.getAsLong()).isEqualTo(backfilled + TTL.toMillis() - 1);
        kv.getRocksDBKv().getDb().compactRange();
        assertThat(kv.versionOffset(key(1))).isNotNull();
        assertThat(kv.versionOffset(key(2))).isNotNull();

        filter.coveredAt(end);
        assertThat(filter.safe(end)).isTrue();
        kv.getRocksDBKv().getDb().compactRange();
        assertThat(kv.versionOffset(key(1))).isNull();
        assertThat(kv.versionOffset(key(2))).isNull();
        assertThat(kv.versionOffset(key(3))).isNotNull();
    }

    private KvRecord upsert(int id, String value, Long eventTime) {
        return records.ofRecord(key(id), new Object[] {id, value, eventTime});
    }

    private byte[] key(int id) {
        return keyEncoder.encodeKey(row(id, "", 0L));
    }

    private void put(KvRecord... batch) throws Exception {
        kv.putAsLeader(batches.ofRecords(Arrays.asList(batch)), null);
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
