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

package org.apache.fluss.server.kv;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.server.kv.autoinc.AutoIncrementManager;
import org.apache.fluss.server.kv.autoinc.TestingSequenceGeneratorFactory;
import org.apache.fluss.server.kv.dv.DerivedDvManager;
import org.apache.fluss.server.kv.dv.DerivedDvMetrics;
import org.apache.fluss.server.kv.dv.DvEntry;
import org.apache.fluss.server.kv.dv.DvSnapshot;
import org.apache.fluss.server.kv.dv.DvTestManagers;
import org.apache.fluss.server.kv.dv.DvTestReads;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTestUtils;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.utils.RoaringBitmapUtils;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.utils.ByteArraySlice;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.annotation.Nullable;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.server.kv.KvTabletTestUtils.flushAndWait;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A table with deletion vectors keeps each value's version offset in the versioned layout, with row
 * TTL or without. Both hand the same changes to the deletion vectors and give the same masks,
 * whether the superseded version is in the pre-write buffer or in RocksDB. A tablet refuses the
 * tagged layout for such a table.
 */
class KvTabletDvVersionOffsetTest {

    private static final short SCHEMA_ID = 1;
    private static final int KEYS = 40;

    private final KvRecordTestUtils.KvRecordBatchFactory batches =
            KvRecordTestUtils.KvRecordBatchFactory.of(SCHEMA_ID);
    private final KvRecordTestUtils.KvRecordFactory records =
            KvRecordTestUtils.KvRecordFactory.of(DATA1_SCHEMA_PK.getRowType());

    private @TempDir File tempDir;

    private final List<Bucket> buckets = new ArrayList<>();

    @AfterEach
    void afterEach() throws Exception {
        for (Bucket b : buckets) {
            IOUtils.closeAll(b.kv::close, b.log::close);
        }
    }

    /**
     * The same random batches of inserts, updates and deletes, with random flushes in between, go
     * to a bucket without row TTL and one with row TTL. Both hand over the same changes, each
     * naming the version the model of the log gives, keep the same values and version offsets, and
     * serve the same masks. The row TTL is long enough that no value expires; an insert into the
     * table with row TTL names its superseded version as unknown, as a value may have expired.
     * Without row TTL every value keeps the TTL timestamp that never expires.
     */
    @ParameterizedTest
    @CsvSource({"FULL, 1", "FULL, 7", "FULL, 42", "WAL, 1", "WAL, 7", "WAL, 42"})
    void testWithAndWithoutRowTtlNameTheSameVersions(ChangelogImage image, long seed)
            throws Exception {
        Bucket noTtl = open(false, image);
        Bucket withTtl = open(true, image);
        assertThat(noTtl.kv.getKvValueLayout()).isEqualTo(KvValueLayout.VERSIONED);
        assertThat(withTtl.kv.getKvValueLayout()).isEqualTo(KvValueLayout.VERSIONED);
        assertThat(noTtl.kv.getDvManager().rowTtlFilterClock()).isNull();
        List<Bucket> all = Arrays.asList(noTtl, withTtl);

        Random random = new Random(seed);
        // key -> the offset of its live version, from the changes handed over
        Map<Integer, Long> model = new TreeMap<>();
        // the versions superseded so far, the log mask of a read without a lake snapshot
        TreeSet<Long> superseded = new TreeSet<>();
        int compared = 0;
        for (int step = 0; step < 300; step++) {
            List<KvRecord> batch = new ArrayList<>();
            int size = 1 + random.nextInt(20);
            for (int i = 0; i < size; i++) {
                int k = random.nextInt(KEYS);
                batch.add(
                        random.nextInt(4) == 0
                                ? records.ofRecord(key(k), null)
                                : records.ofRecord(
                                        key(k), new Object[] {k, "v" + random.nextInt(1000)}));
            }
            int before = noTtl.entries.size();
            for (Bucket b : all) {
                b.kv.putAsLeader(batches.ofRecords(batch), null);
                assertThat(b.log.localLogEndOffset()).isEqualTo(noTtl.log.localLogEndOffset());
            }
            assertThat(describe(withTtl.entries, true)).isEqualTo(describe(noTtl.entries, true));
            assertThat(withTtl.entries.subList(before, withTtl.entries.size()))
                    .allSatisfy(e -> assertThat(e.isInsert()).isFalse());

            // each change names the version the model gives
            for (DvEntry e : noTtl.entries.subList(before, noTtl.entries.size())) {
                int k = keyOf(e.getKey());
                Long previous = model.get(k);
                assertThat(e.getPreviousVersionOffset())
                        .isEqualTo(previous == null ? -1 : previous);
                assertThat(e.isInsert()).isEqualTo(previous == null);
                if (previous != null) {
                    superseded.add(previous);
                }
                if (e.getVersionOffset() < 0) {
                    model.remove(k);
                } else {
                    model.put(k, e.getVersionOffset());
                }
            }

            if (random.nextInt(10) < 4) {
                // the high watermark passes the batch and the KV flushes it to RocksDB
                for (Bucket b : all) {
                    long leo = b.log.localLogEndOffset();
                    b.log.updateHighWatermark(leo);
                    flushAndWait(b.kv, leo);
                }
            }
            if (step % 10 == 9) {
                assertSameState(all, model);
                long leo = noTtl.log.localLogEndOffset();
                for (Bucket b : all) {
                    b.log.updateHighWatermark(leo);
                }
                List<Long> noTtlMask = deadVersions(unionRead(noTtl.manager));
                assertThat(deadVersions(unionRead(withTtl.manager))).isEqualTo(noTtlMask);
                assertThat(noTtlMask).containsExactlyElementsOf(superseded);
                compared++;
            }
        }
        assertThat(compared).isEqualTo(30);

        // after a full flush RocksDB holds exactly the live keys with their versions
        for (Bucket b : all) {
            long leo = b.log.localLogEndOffset();
            b.log.updateHighWatermark(leo);
            flushAndWait(b.kv, leo);
        }
        assertSameState(all, model);
        for (int k : model.keySet()) {
            byte[] raw = noTtl.kv.getRocksDBKv().get(key(k));
            assertThat(KvValueLayout.VERSIONED.readVersionOffset(MemorySegment.wrap(raw), 0))
                    .isEqualTo(model.get(k));
            assertThat(KvValueLayout.VERSIONED.readTtlTimestamp(MemorySegment.wrap(raw), 0))
                    .isEqualTo(KvValueLayout.NEVER_EXPIRE_TIMESTAMP);
        }
    }

    /**
     * A tablet of a table with deletion vectors refuses the tagged layout, with row TTL or without:
     * its tag holds no version offset.
     */
    @ParameterizedTest
    @CsvSource({"false", "true"})
    void testTaggedLayoutIsRefused(boolean rowTtl) {
        assertThatThrownBy(() -> open(rowTtl, ChangelogImage.FULL, KvValueLayout.TAGGED.version()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("deletion vectors")
                .hasMessageContaining("layout " + KvValueLayout.TAGGED.version());
    }

    /** The buckets serve the same value bodies and keep the version offsets of the model. */
    private void assertSameState(List<Bucket> all, Map<Integer, Long> model) throws Exception {
        List<byte[]> keys = new ArrayList<>();
        for (int k = 0; k < KEYS; k++) {
            keys.add(key(k));
        }
        List<ByteArraySlice> first = all.get(0).kv.multiGetFromBufferOrKv(keys);
        for (Bucket b : all) {
            List<ByteArraySlice> values = b.kv.multiGetFromBufferOrKv(keys);
            for (int k = 0; k < KEYS; k++) {
                assertThat(bytes(values.get(k))).isEqualTo(bytes(first.get(k)));
                assertThat(values.get(k) == null).isEqualTo(!model.containsKey(k));
                assertThat(b.kv.versionOffset(key(k))).isEqualTo(model.get(k));
            }
        }
    }

    private static byte[] bytes(ByteArraySlice slice) {
        return slice == null ? null : slice.toByteArray();
    }

    private static byte[] key(int k) {
        return String.format("k%03d", k).getBytes(StandardCharsets.UTF_8);
    }

    private static int keyOf(byte[] key) {
        return Integer.parseInt(new String(key, StandardCharsets.UTF_8).substring(1));
    }

    /** The changes, without whether they inserted the key if {@code withoutInsert}. */
    private static List<String> describe(List<DvEntry> entries, boolean withoutInsert) {
        List<String> out = new ArrayList<>(entries.size());
        for (DvEntry e : entries) {
            out.add(
                    new String(e.getKey(), StandardCharsets.UTF_8)
                            + "@"
                            + e.getRecordOffset()
                            + " v"
                            + e.getVersionOffset()
                            + (e.isInsert() && !withoutInsert ? " insert" : "")
                            + " prev"
                            + e.getPreviousVersionOffset());
        }
        return out;
    }

    private static DvSnapshot unionRead(DerivedDvManager manager) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (true) {
            try {
                return DvTestReads.unionRead(manager, -1);
            } catch (RetriableException e) {
                assertThat(System.currentTimeMillis()).isLessThan(deadline);
                Thread.sleep(10);
            }
        }
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

    /**
     * A bucket of a table with deletion vectors, with row TTL or without, and the changes it hands
     * over.
     */
    private Bucket open(boolean rowTtl, ChangelogImage image) throws Exception {
        return open(rowTtl, image, null);
    }

    /** Like {@link #open(boolean, ChangelogImage)}, with the persisted layout version if set. */
    private Bucket open(boolean rowTtl, ChangelogImage image, @Nullable Integer layoutVersion)
            throws Exception {
        String name = (rowTtl ? "row_ttl" : "no_row_ttl") + "_" + image.name().toLowerCase();
        File logDir = new File(tempDir, name + "-log");
        File kvDir = new File(tempDir, name + "-kv");
        assertThat(logDir.mkdirs() && kvDir.mkdirs()).isTrue();
        Configuration conf = new Configuration();
        PhysicalTablePath path = PhysicalTablePath.of(TablePath.of("db", name));
        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(new SchemaInfo(DATA1_SCHEMA_PK, SCHEMA_ID));
        LogTablet log =
                LogTablet.create(
                        logDir,
                        path,
                        LogTestUtils.makeRandomLogTabletDir(
                                logDir, path.getDatabaseName(), 0L, path.getTableName()),
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
        tableConf.set(ConfigOptions.TABLE_DATALAKE_DELETION_VECTORS_ENABLED, true);
        if (rowTtl) {
            // long enough that no value expires during the test
            tableConf.set(ConfigOptions.TABLE_KV_TTL, Duration.ofDays(1));
        }
        tableConf.set(ConfigOptions.TABLE_CHANGELOG_IMAGE, image);
        if (layoutVersion != null) {
            tableConf.set(ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION, layoutVersion);
        }
        TableConfig tableConfig = new TableConfig(tableConf);
        KvTablet kv;
        try {
            kv =
                    KvTablet.create(
                            path,
                            log.getTableBucket(),
                            log,
                            kvDir,
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
        } catch (Throwable t) {
            log.close();
            throw t;
        }
        Bucket bucket = new Bucket(log, kv);
        buckets.add(bucket);
        bucket.manager = recordingManager(bucket);
        return bucket;
    }

    /**
     * Makes the write path hand its changes to a manager that also records them, and makes that
     * manager cover the empty lake, as a new leader does.
     */
    private static DerivedDvManager recordingManager(Bucket bucket) throws Exception {
        DerivedDvManager recording =
                new DerivedDvManager(
                        DvTestManagers.db(bucket.kv.getDvManager()),
                        bucket.log::getHighWatermark,
                        DvTestManagers.CHECKPOINT_INTERVAL,
                        DerivedDvMetrics.noop(),
                        DvTestManagers.budget(),
                        DvTestManagers.executors()) {
                    @Override
                    public synchronized void handleChangelogSynced(
                            List<DvEntry> entries, long logEnd) {
                        bucket.entries.addAll(entries);
                        super.handleChangelogSynced(entries, logEnd);
                    }
                };
        DvTestManagers.setMaskCacheMillis(recording, 0);
        bucket.kv.handChangesTo(recording);
        DvTestManagers.coverTheEmptyLake(recording, bucket.log::localLogEndOffset);
        return recording;
    }

    private static final class Bucket {
        final LogTablet log;
        final KvTablet kv;
        final List<DvEntry> entries = new ArrayList<>();
        DerivedDvManager manager;

        Bucket(LogTablet log, KvTablet kv) {
            this.log = log;
            this.kv = kv;
        }
    }
}
