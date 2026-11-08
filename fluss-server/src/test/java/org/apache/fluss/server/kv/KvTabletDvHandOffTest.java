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
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.server.kv.autoinc.AutoIncrementManager;
import org.apache.fluss.server.kv.autoinc.TestingSequenceGeneratorFactory;
import org.apache.fluss.server.kv.dv.DerivedDvManager;
import org.apache.fluss.server.kv.dv.DvTestManagers;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTestUtils;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
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

import java.io.File;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the deletion vector RocksDB of a {@link KvTablet}: it is closed and dropped with the
 * tablet.
 */
class KvTabletDvHandOffTest {

    private static final short SCHEMA_ID = 1;

    private final Configuration conf = new Configuration();

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
}
