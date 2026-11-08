/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.tiering.FlussTableLakeSnapshotCommitter;
import org.apache.fluss.client.tiering.TieringCommitterInitContext;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.committer.SupportsPrimaryKeyMergeCommit;
import org.apache.fluss.lake.paimon.tiering.PaimonCommittable;
import org.apache.fluss.lake.paimon.tiering.PaimonLakeTieringFactory;
import org.apache.fluss.lake.paimon.tiering.PaimonWriteResult;
import org.apache.fluss.lake.paimon.utils.DvTableReadableSnapshotRetriever;
import org.apache.fluss.lake.source.DataDeltaPlan;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.RowWithPosResult;
import org.apache.fluss.lake.source.SupportsRowPositions;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.fluss.lake.paimon.utils.PaimonTestUtils.CompactHelper;
import static org.apache.fluss.server.testutils.LakeTieringTestUtils.renewTiering;
import static org.apache.fluss.server.testutils.LakeTieringTestUtils.takeTableToTier;
import static org.apache.fluss.server.utils.LakeStorageUtils.extractLakeProperties;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The readable snapshot of a Paimon table with deletion vector masks, and the position index it
 * plans.
 *
 * <p>A union read takes the readable snapshot R (Paimon's deletion-vector read, which skips level-0
 * files), masks the rows of keys changed at or after a bucket's readable offset X, and reads the
 * log from X. The tests check on a real table that R holds exactly each bucket's state at X, also
 * when level-0 files are not compacted yet. They also check that the index learned file by file
 * from {@link PaimonLakeSource#planRewrite} gives masks that make the union read exact.
 */
class PaimonDerivedDvReadableTest {

    private static final String DEFAULT_DB = "fluss";
    private static Catalog paimonCatalog;
    private static Configuration paimonConfig;

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(initConfig())
                    .build();

    @TempDir private File compactionTempDir;

    private Connection connection;
    private Admin admin;
    private FlussTableLakeSnapshotCommitter lakeSnapshotCommitter;
    private Configuration flussConf;
    // the tiering epoch this test tiers its table under, as the coordinator registers the lake
    // snapshots of a table with deletion vectors only under its round's epoch
    private long tieringEpoch;

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setString("datalake.format", "paimon");
        conf.setString("datalake.paimon.metastore", "filesystem");
        try {
            conf.setString(
                    "datalake.paimon.warehouse",
                    Files.createTempDirectory("fluss-derived-dv-readable")
                            .resolve("warehouse")
                            .toString());
        } catch (Exception e) {
            throw new FlussRuntimeException("Failed to create warehouse path", e);
        }
        paimonConfig = Configuration.fromMap(extractLakeProperties(conf));
        paimonCatalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(Options.fromMap(extractLakeProperties(conf))));
        return conf;
    }

    @BeforeEach
    void setUp() {
        flussConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        connection = ConnectionFactory.createConnection(flussConf);
        admin = connection.getAdmin();
        lakeSnapshotCommitter = new FlussTableLakeSnapshotCommitter(flussConf);
        lakeSnapshotCommitter.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        lakeSnapshotCommitter.close();
        admin.close();
        connection.close();
    }

    @Test
    void testReadableSnapshotAndMasksWithUncompactedLevel0() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "derived_dv_readable");
        long tableId = createDerivedTable(tablePath);
        FileStoreTable table = paimonTable(tablePath);
        assertThat(table.coreOptions().deletionVectorsEnabled()).isTrue();
        TableBucket b0 = new TableBucket(tableId, 0);
        TableBucket b1 = new TableBucket(tableId, 1);
        Log log = new Log();

        // Round 1, offsets 0..2 of both buckets: all in level 0.
        log.upsert(0, 0, "a").upsert(0, 2, "a").upsert(0, 4, "a");
        log.upsert(1, 1, "a").upsert(1, 3, "a").upsert(1, 5, "a");
        long a1 = tierRound(tableId, tablePath, table, log);
        assertThat(readable(tablePath, tableId, table, a1, log)).isNull();

        // Another engine compacts bucket 0; bucket 1 keeps level 0, so nothing is readable.
        compact(table, 0);
        log.upsert(0, 0, "b").delete(0, 2);
        log.upsert(1, 1, "b").delete(1, 3);
        long a2 = tierRound(tableId, tablePath, table, log);
        assertThat(readable(tablePath, tableId, table, a2, log)).isNull();

        // Bucket 1 is compacted (both rounds); bucket 0 keeps round 2 in level 0.
        long c2 = compact(table, 1);
        log.upsert(0, 4, "c");
        log.upsert(1, 5, "c");
        long a3 = tierRound(tableId, tablePath, table, log);
        DvTableReadableSnapshotRetriever.ReadableSnapshotResult r =
                readable(tablePath, tableId, table, a3, log);

        // R is the compaction of bucket 1; bucket 0 reads from the offset its compacted level-0
        // files end at (round 1), bucket 1 from the round compacted last (round 2).
        assertThat(r).isNotNull();
        assertThat(r.getReadableSnapshotId()).isEqualTo(c2);
        Map<TableBucket, Long> expectedOffsets = new HashMap<>();
        expectedOffsets.put(b0, 3L);
        expectedOffsets.put(b1, 5L);
        assertThat(r.getReadableOffsets()).isEqualTo(expectedOffsets);

        // R holds exactly each bucket's state at its readable offset.
        PaimonLakeSource source = new PaimonLakeSource(table);
        source.withProject(new int[][] {{0}, {1}});
        Map<Integer, Map<Integer, Located>> lake = readLake(source, c2);
        assertThat(values(lake.get(0))).isEqualTo(log.stateAt(0, 3));
        assertThat(values(lake.get(1))).isEqualTo(log.stateAt(1, 5));

        // The index learned from the first readable snapshot gives masks that make the union
        // read exact: R minus masked rows plus the log from the readable offsets.
        Index index = new Index();
        index.apply(source, source.planRewrite(-1, c2));
        assertThat(index.positions).isEqualTo(positions(lake));
        assertIndexFits(index, lake);
        assertUnionReadExact(lake, index, log, r.getReadableOffsets());

        // Fluss registers R; the next compaction moves bucket 0's level-0 files up.
        commitToFluss(tableId, tablePath, a3, log.tiered(tableId), r);
        long c3 = compact(table, 0);
        log.upsert(0, 6, "d");
        long a4 = tierRound(tableId, tablePath, table, log);
        r = readable(tablePath, tableId, table, a4, log);
        assertThat(r).isNotNull();
        assertThat(r.getReadableSnapshotId()).isEqualTo(c3);
        expectedOffsets.put(b0, 6L);
        assertThat(r.getReadableOffsets()).isEqualTo(expectedOffsets);

        // Only the files that became readable are planned, the files that left are dropped, and
        // retained files are not read again.
        DataDeltaPlan<PaimonSplit> plan = source.planRewrite(c2, c3);
        assertThat(plan).isNotNull();
        assertThat(plan.getCompactSnapshotId()).isEqualTo(c3);
        assertThat(plan.getSplits()).isNotEmpty();
        assertThat(plan.getSplits()).allMatch(s -> s.bucket() == 0 && s.fileName() != null);
        assertThat(deletedFiles(plan, 1)).isEmpty();
        index.apply(source, plan);
        lake = readLake(source, c3);
        for (String removed : deletedFiles(plan, 0)) {
            assertThat(positions(lake).values()).noneMatch(at -> at.file.equals(removed));
        }
        assertThat(values(lake.get(0))).isEqualTo(log.stateAt(0, 6));
        assertIndexFits(index, lake);
        assertUnionReadExact(lake, index, log, r.getReadableOffsets());

        // An APPEND changes no readable file.
        assertThat(source.planRewrite(c3, a4)).isNull();
    }

    /**
     * When another engine compacts an idle table, a round without data commits the registered
     * offsets again. The compaction becomes readable at them without waiting for a write.
     */
    @Test
    void testCompactionOfIdleTableBecomesReadableWithoutData() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "derived_dv_idle");
        long tableId = createDerivedTable(tablePath);
        FileStoreTable table = paimonTable(tablePath);
        Log log = new Log();
        log.upsert(0, 0, "a").upsert(0, 2, "a").delete(0, 0).upsert(0, 4, "a");
        log.upsert(1, 1, "a").upsert(1, 3, "a").upsert(1, 1, "b");
        long a1 = tierRound(tableId, tablePath, table, log);
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> committer = committer(tablePath)) {
            // Nothing compacted since the registered snapshot: an idle round commits nothing.
            assertThat(
                            ((SupportsPrimaryKeyMergeCommit) committer)
                                    .readableSnapshotMayAdvanceSince(a1))
                    .isFalse();
        }

        compact(table, 0);
        long compacted = compact(table, 1);
        Map<TableBucket, Long> tiered = log.tiered(tableId);
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> committer = committer(tablePath)) {
            assertThat(
                            ((SupportsPrimaryKeyMergeCommit) committer)
                                    .readableSnapshotMayAdvanceSince(a1))
                    .isTrue();
            assertThat(
                            ((SupportsPrimaryKeyMergeCommit) committer)
                                    .readableSnapshotMayAdvanceSince(compacted))
                    .isFalse();

            // The idle round: no data, the registered offsets.
            String offsetsPath =
                    lakeSnapshotCommitter.prepareLakeSnapshot(tableId, tablePath, tiered);
            LakeCommitResult result =
                    committer.commit(
                            committer.toCommittable(Collections.emptyList()),
                            new LakeCommitter.CommitContext(
                                    Collections.singletonMap(
                                            LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY,
                                            offsetsPath),
                                    tiered));
            Snapshot idle = table.snapshotManager().latestSnapshot();
            assertThat(result.getCommittedSnapshotId()).isEqualTo(idle.id());
            assertThat(idle.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
            assertThat(idle.deltaRecordCount()).isZero();
            assertThat(result.getReadableSnapshot()).isNotNull();
            assertThat(result.getReadableSnapshot().getReadableSnapshotId()).isEqualTo(compacted);
            assertThat(result.getReadableSnapshot().getReadableLogEndOffsets()).isEqualTo(tiered);
            // The next idle round finds no newer compaction.
            assertThat(
                            ((SupportsPrimaryKeyMergeCommit) committer)
                                    .readableSnapshotMayAdvanceSince(idle.id()))
                    .isFalse();
        }

        // The compaction holds each bucket's state at its readable offset.
        PaimonLakeSource source = new PaimonLakeSource(table);
        source.withProject(new int[][] {{0}, {1}});
        Map<Integer, Map<Integer, Located>> lake = readLake(source, compacted);
        assertThat(values(lake.get(0))).isEqualTo(log.stateAt(0, Long.MAX_VALUE));
        assertThat(values(lake.get(1))).isEqualTo(log.stateAt(1, Long.MAX_VALUE));
    }

    // ------------------------------------------------------------------------------------------

    private LakeCommitter<PaimonWriteResult, PaimonCommittable> committer(TablePath tablePath)
            throws Exception {
        return new PaimonLakeTieringFactory(paimonConfig)
                .createLakeCommitter(
                        new TieringCommitterInitContext(
                                tablePath,
                                admin.getTableInfo(tablePath).get(),
                                new Configuration(),
                                flussConf));
    }

    /** Union read: the lake rows minus the masks of keys changed since X, plus the log from X. */
    private static void assertUnionReadExact(
            Map<Integer, Map<Integer, Located>> lake,
            Index index,
            Log log,
            Map<TableBucket, Long> readableOffsets) {
        for (Map.Entry<TableBucket, Long> e : readableOffsets.entrySet()) {
            int bucket = e.getKey().getBucket();
            long from = e.getValue();
            Set<Located> masks = new HashSet<>();
            for (int key : log.keysChangedFrom(bucket, from)) {
                Located at = index.positions.get(key);
                if (at != null) {
                    masks.add(at);
                }
            }
            Map<Integer, String> union = new TreeMap<>();
            for (Map.Entry<Integer, Located> row :
                    lake.getOrDefault(bucket, Collections.emptyMap()).entrySet()) {
                if (!masks.contains(row.getValue())) {
                    assertThat(union.put(row.getKey(), row.getValue().value)).isNull();
                }
            }
            for (Map.Entry<Integer, String> tail : log.lastVersionsFrom(bucket, from).entrySet()) {
                if (tail.getValue() != null) {
                    assertThat(union.put(tail.getKey(), tail.getValue())).isNull();
                }
            }
            assertThat(union).isEqualTo(log.stateAt(bucket, Long.MAX_VALUE));
        }
    }

    /**
     * Every live row is indexed at its position. A key without a live row may keep the position of
     * its last version until that file leaves the snapshot. The lake's own deletion vectors delete
     * that row, so masking it hides no live row.
     */
    private static void assertIndexFits(Index index, Map<Integer, Map<Integer, Located>> lake) {
        Map<Integer, Located> live = positions(lake);
        Set<Located> liveAt = new HashSet<>(live.values());
        for (Map.Entry<Integer, Located> e : live.entrySet()) {
            assertThat(index.positions.get(e.getKey())).isEqualTo(e.getValue());
        }
        for (Map.Entry<Integer, Located> e : index.positions.entrySet()) {
            if (!live.containsKey(e.getKey())) {
                assertThat(liveAt).doesNotContain(e.getValue());
            }
        }
    }

    /** Reads every readable row of a snapshot with its file and position, by bucket and key. */
    private static Map<Integer, Map<Integer, Located>> readLake(
            PaimonLakeSource source, long snapshotId) throws Exception {
        Map<Integer, Map<Integer, Located>> rows = new HashMap<>();
        for (PaimonSplit split : source.createPlanner(() -> snapshotId).plan()) {
            for (Map.Entry<Integer, Located> e : readWithPos(source, split).entrySet()) {
                assertThat(
                                rows.computeIfAbsent(split.bucket(), b -> new TreeMap<>())
                                        .put(e.getKey(), e.getValue()))
                        .as("one live row per key")
                        .isNull();
            }
        }
        return rows;
    }

    private static Map<Integer, Located> readWithPos(PaimonLakeSource source, PaimonSplit split)
            throws Exception {
        Map<Integer, Located> rows = new TreeMap<>();
        RecordReader reader = source.createRecordReader(() -> split);
        try (CloseableIterator<RowWithPosResult> it =
                ((SupportsRowPositions) reader).readWithPos()) {
            while (it.hasNext()) {
                RowWithPosResult r = it.next();
                assertThat(split.fileNames()).contains(r.getFileName());
                rows.put(
                        r.getRow().getInt(0),
                        new Located(
                                r.getFileName(), r.getPos(), r.getRow().getString(1).toString()));
            }
        }
        return rows;
    }

    private static Map<Integer, Located> positions(Map<Integer, Map<Integer, Located>> lake) {
        Map<Integer, Located> all = new TreeMap<>();
        lake.values().forEach(all::putAll);
        return all;
    }

    private static Map<Integer, String> values(Map<Integer, Located> rows) {
        Map<Integer, String> values = new TreeMap<>();
        if (rows != null) {
            rows.forEach((k, v) -> values.put(k, v.value));
        }
        return values;
    }

    /** Compacts a bucket like another engine does; returns the COMPACT snapshot. */
    private long compact(FileStoreTable table, int bucket) throws Exception {
        long before = table.snapshotManager().latestSnapshotId();
        new CompactHelper(table, compactionTempDir).compactBucket(bucket).commit();
        Snapshot compacted = table.snapshotManager().latestSnapshot();
        assertThat(compacted.id()).isEqualTo(before + 1);
        assertThat(compacted.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
        return compacted.id();
    }

    /** Writes the log's untiered records as one Fluss commit (an APPEND snapshot). */
    private long tierRound(long tableId, TablePath tablePath, FileStoreTable table, Log log)
            throws Exception {
        List<Record> round = new ArrayList<>(log.untiered());
        log.markTiered();
        Map<TableBucket, Long> tiered = log.tiered(tableId);
        String offsetsPath = lakeSnapshotCommitter.prepareLakeSnapshot(tableId, tablePath, tiered);
        FileStoreTable writeTable =
                table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));
        List<CommitMessage> messages;
        try (BatchTableWrite write = writeTable.newBatchWriteBuilder().newWrite()) {
            for (Record record : round) {
                write.write(record.toRow(), record.bucket);
            }
            messages = write.prepareCommit();
        }
        try (TableCommitImpl commit = writeTable.newCommit("fluss-test-tiering")) {
            ManifestCommittable committable =
                    new ManifestCommittable(BatchWriteBuilder.COMMIT_IDENTIFIER);
            messages.forEach(committable::addFileCommittable);
            committable.addProperty(
                    LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, offsetsPath);
            commit.ignoreEmptyCommit(false).commit(committable);
        }
        Snapshot appended = table.snapshotManager().latestSnapshot();
        assertThat(appended.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
        long snapshotId = appended.id();
        renewTieringOf(tableId);
        lakeSnapshotCommitter.commit(
                tableId,
                tablePath,
                LakeCommitResult.unknownReadableSnapshot(snapshotId),
                offsetsPath,
                tiered,
                Collections.emptyMap());
        return snapshotId;
    }

    private DvTableReadableSnapshotRetriever.ReadableSnapshotResult readable(
            TablePath tablePath, long tableId, FileStoreTable table, long appendId, Log log)
            throws Exception {
        try (DvTableReadableSnapshotRetriever retriever =
                new DvTableReadableSnapshotRetriever(tablePath, tableId, table, flussConf)) {
            return retriever.getReadableSnapshotAndOffsets(appendId, log.tiered(tableId));
        }
    }

    private void commitToFluss(
            long tableId,
            TablePath tablePath,
            long committedId,
            Map<TableBucket, Long> tiered,
            DvTableReadableSnapshotRetriever.ReadableSnapshotResult r)
            throws Exception {
        String offsetsPath = lakeSnapshotCommitter.prepareLakeSnapshot(tableId, tablePath, tiered);
        renewTieringOf(tableId);
        lakeSnapshotCommitter.commit(
                tableId,
                tablePath,
                LakeCommitResult.withReadableSnapshot(
                        committedId,
                        r.getReadableSnapshotId(),
                        r.getTieredOffsets(),
                        r.getReadableOffsets(),
                        r.getEarliestSnapshotIdToKeep()),
                offsetsPath,
                tiered,
                Collections.emptyMap());
    }

    private long createDerivedTable(TablePath tablePath) throws Exception {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                org.apache.fluss.metadata.Schema.newBuilder()
                                        .column("c1", DataTypes.INT())
                                        .column("c2", DataTypes.STRING())
                                        .primaryKey("c1")
                                        .build())
                        .distributedBy(2)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED, true)
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(100))
                        .build();
        admin.createTable(tablePath, descriptor, false).get();
        long tableId = admin.getTableInfo(tablePath).get().getTableId();
        tieringEpoch = takeTableToTier(FLUSS_CLUSTER_EXTENSION.newCoordinatorClient(), tableId);
        lakeSnapshotCommitter.fenceOnTieringEpoch(tableId, tieringEpoch);
        return tableId;
    }

    /** Keeps the test's round from timing out at the coordinator before it commits. */
    private void renewTieringOf(long tableId) throws Exception {
        renewTiering(FLUSS_CLUSTER_EXTENSION.newCoordinatorClient(), tableId, tieringEpoch);
    }

    private static FileStoreTable paimonTable(TablePath tablePath) throws Exception {
        return (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
    }

    /** The position index of the readable snapshot, learned from planned files only. */
    private static final class Index {
        private final Map<Integer, Located> positions = new TreeMap<>();

        void apply(PaimonLakeSource source, DataDeltaPlan<PaimonSplit> plan) throws Exception {
            Set<String> removed = new HashSet<>();
            plan.getDeletedFiles().values().forEach(m -> m.values().forEach(removed::addAll));
            positions.values().removeIf(at -> removed.contains(at.file));
            for (PaimonSplit split : plan.getSplits()) {
                assertThat(split.fileNames()).hasSize(1);
                positions.putAll(readWithPos(source, split));
            }
            // Entries of keys deleted since the last plan stay until their file leaves; they
            // only mask rows the lake's own deletion vectors already delete.
        }
    }

    /** A row at a file position. */
    private static final class Located {
        private final String file;
        private final long pos;
        private final String value;

        private Located(String file, long pos, String value) {
            this.file = file;
            this.pos = pos;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Located)) {
                return false;
            }
            Located that = (Located) o;
            return pos == that.pos && file.equals(that.file);
        }

        @Override
        public int hashCode() {
            return Objects.hash(file, pos);
        }

        @Override
        public String toString() {
            return file + "@" + pos + "=" + value;
        }
    }

    /** A Fluss change record of a bucket at a log offset; a null value is a delete. */
    private static final class Record {
        private final int bucket;
        private final long offset;
        private final int key;
        private final String value;

        private Record(int bucket, long offset, int key, String value) {
            this.bucket = bucket;
            this.offset = offset;
            this.key = key;
            this.value = value;
        }

        GenericRow toRow() {
            return value == null
                    ? GenericRow.ofKind(RowKind.DELETE, key, BinaryString.fromString("-"))
                    : GenericRow.of(key, BinaryString.fromString(value));
        }
    }

    /** The changelog of each bucket, one offset per change. */
    private static final class Log {
        private final List<Record> records = new ArrayList<>();
        private final Map<Integer, Long> next = new HashMap<>();
        private int tieredCount = 0;

        Log upsert(int bucket, int key, String value) {
            return add(bucket, key, value);
        }

        Log delete(int bucket, int key) {
            return add(bucket, key, null);
        }

        private Log add(int bucket, int key, String value) {
            long offset = next.getOrDefault(bucket, 0L);
            next.put(bucket, offset + 1);
            records.add(new Record(bucket, offset, key, value));
            return this;
        }

        List<Record> untiered() {
            return records.subList(tieredCount, records.size());
        }

        void markTiered() {
            tieredCount = records.size();
        }

        Map<TableBucket, Long> tiered(long tableId) {
            Map<TableBucket, Long> offsets = new HashMap<>();
            for (Record r : records.subList(0, tieredCount)) {
                offsets.merge(new TableBucket(tableId, r.bucket), r.offset + 1, Math::max);
            }
            return offsets;
        }

        Map<Integer, String> stateAt(int bucket, long offset) {
            Map<Integer, String> state = new TreeMap<>();
            for (Record r : records) {
                if (r.bucket == bucket && r.offset < offset) {
                    if (r.value == null) {
                        state.remove(r.key);
                    } else {
                        state.put(r.key, r.value);
                    }
                }
            }
            return state;
        }

        Set<Integer> keysChangedFrom(int bucket, long offset) {
            return lastVersionsFrom(bucket, offset).keySet();
        }

        Map<Integer, String> lastVersionsFrom(int bucket, long offset) {
            Map<Integer, String> last = new HashMap<>();
            for (Record r : records) {
                if (r.bucket == bucket && r.offset >= offset) {
                    last.put(r.key, r.value);
                }
            }
            return last;
        }
    }

    /**
     * The files the plan's rewrite removed from bucket {@code bucket} of the unpartitioned table.
     */
    private static List<String> deletedFiles(DataDeltaPlan<?> plan, int bucket) {
        return plan.getDeletedFiles()
                .getOrDefault(null, Collections.emptyMap())
                .getOrDefault(bucket, Collections.emptyList());
    }
}
