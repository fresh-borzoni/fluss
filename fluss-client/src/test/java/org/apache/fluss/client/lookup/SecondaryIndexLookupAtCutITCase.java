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

package org.apache.fluss.client.lookup;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.LookupPositionExpiredException;
import org.apache.fluss.exception.LookupPositionNotYetReadableException;
import org.apache.fluss.metadata.BucketReadRange;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.IndexTableUtils;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Model-based test of secondary index reads at cuts of a main table with bucket history: every read
 * at a random cut must equal the replay of each bucket's changelog up to its position. Also covers
 * an index that lags behind a cut and the purge of removed pointers below the history floor.
 */
class SecondaryIndexLookupAtCutITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(SecondaryIndexLookupAtCutITCase.class);

    private static final ManualClock CLOCK = new ManualClock(System.currentTimeMillis());

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(2)
                    .setClusterConf(clusterConfig())
                    .setClock(CLOCK)
                    .build();

    private static final int BUCKETS = 3;
    private static final int INDEX_BUCKETS = 2;
    private static final int IDS = 24;
    private static final int VALUES = 4;

    /** A value that no row has until the index lags behind. */
    private static final int LAGGING_VALUE = 99;

    private static Configuration clusterConfig() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 1);
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofHours(12));
        return conf;
    }

    @Test
    void readsAtCutsMatchTheReplayedChangelog() throws Exception {
        long seed = System.nanoTime();
        LOG.info("Random seed: {}", seed);
        try (Model model = new Model(new Random(seed), TablePath.of("cut_db", "orders"))) {
            // random changes, each round read at random cuts while the index catches up
            for (int round = 0; round < 20; round++) {
                model.writeRound(12);
                model.readAtRandomCuts(4);
            }
            assertThat(model.removedPointers()).isNotEmpty();

            // an index that lags behind a cut makes the read wait, not answer
            model.pauseIndexReads(true);
            Map<TableBucket, Long> lagging;
            CompletableFuture<Set<String>> read;
            int waitsBefore = model.notYetReadable.get();
            try {
                model.writeRound(6);
                for (int id = 0; id < 6; id++) {
                    model.upsert(id, LAGGING_VALUE);
                }
                model.flushAndAwait();
                lagging = model.latestCut();
                Map<TableBucket, Long> cut = lagging;
                read =
                        CompletableFuture.supplyAsync(
                                () -> {
                                    try {
                                        return model.readAt(LAGGING_VALUE, cut);
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                });
                Thread.sleep(1000);
                assertThat(read).as("a read at a cut the index has not reached").isNotDone();
                assertThat(model.notYetReadable.get()).isGreaterThan(waitsBefore);
            } finally {
                model.pauseIndexReads(false);
            }
            assertThat(read.get(1, TimeUnit.MINUTES))
                    .isEqualTo(model.expected(LAGGING_VALUE, lagging))
                    .hasSize(6);
            model.readAtRandomCuts(3);

            // the history floor passes the changes so far: the index purges the removed pointers
            // below it, and reads inside the window stay exact
            CLOCK.advanceTime(Duration.ofHours(2));
            for (int round = 0; round < 4; round++) {
                model.writeRound(12);
            }
            Map<TableBucket, Long> floors = model.advanceHistoryFloors();
            int removedBelowFloors = model.removedPointersBelow(floors);
            assertThat(removedBelowFloors).isPositive();
            // windows of every source bucket carry its floor to every index bucket
            for (int round = 0; round < 3; round++) {
                for (int id = 0; id < IDS; id++) {
                    model.upsert(id, model.random.nextInt(VALUES));
                }
                model.flushAndAwait();
            }
            model.awaitPurgedBelow(floors);
            waitUntil(
                    () -> model.removedPointersBelow(floors) == 0,
                    Duration.ofSeconds(30),
                    "Removed pointers below the history floors remain.");
            model.readAtRandomCuts(6);
            model.assertExpiredBelow(floors);
        }
    }

    /** The table, its index, and the changelog that a read at a cut must agree with. */
    private static final class Model implements AutoCloseable {

        final Random random;
        final AtomicInteger notYetReadable = new AtomicInteger();

        private final Connection connection;
        private final Admin admin;
        private final Table table;
        private final Table indexTable;
        private final UpsertWriter writer;
        private final SecondaryIndexLookuper index;
        private final LogScanner changelogScanner;
        private final List<TableBucket> buckets = new ArrayList<>();

        /** Per bucket: the changes the scan read, by offset. */
        private final Map<Integer, TreeMap<Long, Change>> changelog = new HashMap<>();

        /** The rows after every write so far: id to {value, version}. */
        private final Map<Integer, int[]> current = new HashMap<>();

        private final List<CompletableFuture<?>> pendingWrites = new ArrayList<>();
        private int version;

        Model(Random random, TablePath path) throws Exception {
            this.random = random;
            Configuration clientConf = CLUSTER.getClientConfig();
            // a read is two hops of lookups: send each without waiting for a fuller batch
            clientConf.set(ConfigOptions.CLIENT_LOOKUP_BATCH_TIMEOUT, Duration.ofMillis(1));
            this.connection = ConnectionFactory.createConnection(clientConf);
            this.admin = connection.getAdmin();
            admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
            Schema schema =
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .column("grp", DataTypes.INT())
                            .column("v", DataTypes.INT())
                            .primaryKey("id")
                            .index(
                                    "idx_grp",
                                    IndexType.SECONDARY,
                                    Collections.singletonList("grp"),
                                    IndexVisibility.ASYNC,
                                    INDEX_BUCKETS)
                            .build();
            admin.createTable(
                            path,
                            TableDescriptor.builder()
                                    .schema(schema)
                                    .distributedBy(BUCKETS, "id")
                                    .property(ConfigOptions.TABLE_KV_MVCC_ENABLED, true)
                                    .property(ConfigOptions.TABLE_KV_MVCC_TTL, Duration.ofHours(1))
                                    .build(),
                            false)
                    .get();
            this.table = connection.getTable(path);
            long tableId = table.getTableInfo().getTableId();
            for (int bucket = 0; bucket < BUCKETS; bucket++) {
                TableBucket tableBucket = new TableBucket(tableId, bucket);
                CLUSTER.waitUntilAllReplicaReady(tableBucket);
                buckets.add(tableBucket);
                changelog.put(bucket, new TreeMap<>());
            }
            this.indexTable =
                    connection.getTable(
                            TablePath.of(
                                    path.getDatabaseName(),
                                    IndexTableUtils.indexTableName(
                                            path.getTableName(), "idx_grp")));
            long indexTableId = indexTable.getTableInfo().getTableId();
            for (int bucket = 0; bucket < INDEX_BUCKETS; bucket++) {
                CLUSTER.waitUntilAllReplicaReady(new TableBucket(indexTableId, bucket));
            }
            this.writer = table.newUpsert().createWriter();
            this.index = (SecondaryIndexLookuper) table.getSecondaryIndexLookuper("idx_grp");
            this.changelogScanner = table.newScan().createLogScanner();
            for (int bucket = 0; bucket < BUCKETS; bucket++) {
                changelogScanner.subscribeFromBeginning(bucket);
            }
        }

        // ------------------------------------------------------------------------------------
        // writes

        /** Random upserts and deletes, several per write batch, so a batch changes a row twice. */
        void writeRound(int changes) throws Exception {
            for (int i = 0; i < changes; i++) {
                int id = random.nextInt(IDS);
                if (current.containsKey(id) && random.nextInt(5) == 0) {
                    int[] old = current.remove(id);
                    pendingWrites.add(writer.delete(row(id, old[0], old[1])));
                } else {
                    upsert(id, random.nextInt(VALUES));
                }
                if (random.nextInt(4) == 0) {
                    flushAndAwait();
                }
            }
            flushAndAwait();
        }

        void upsert(int id, int value) {
            int next = ++version;
            current.put(id, new int[] {value, next});
            pendingWrites.add(writer.upsert(row(id, value, next)));
        }

        void flushAndAwait() throws Exception {
            writer.flush();
            for (CompletableFuture<?> write : pendingWrites) {
                write.get(1, TimeUnit.MINUTES);
            }
            pendingWrites.clear();
        }

        void pauseIndexReads(boolean paused) {
            for (TabletServer server : CLUSTER.getTabletServers()) {
                server.getReplicaManager()
                        .getIndexReplicationManager()
                        .pauseReadsForTesting(paused);
            }
        }

        // ------------------------------------------------------------------------------------
        // reads

        /** Reads every value at random cuts and compares them with the replayed changelog. */
        void readAtRandomCuts(int cuts) throws Exception {
            for (int i = 0; i < cuts; i++) {
                Map<TableBucket, Long> cut = randomCut();
                for (int value = 0; value < VALUES; value++) {
                    assertThat(readAt(value, cut))
                            .as("value %s at %s", value, cut)
                            .isEqualTo(expected(value, cut));
                }
            }
        }

        /** Reads the rows of a value at a cut, again while it is not readable yet. */
        Set<String> readAt(int value, Map<TableBucket, Long> cut) throws Exception {
            long deadline = System.currentTimeMillis() + 60_000;
            while (true) {
                try {
                    Set<String> rows = new HashSet<>();
                    for (InternalRow found :
                            index.lookup(row(value), cut).get(1, TimeUnit.MINUTES).getRowList()) {
                        assertThat(rows.add(toString(found))).as("rows at most once").isTrue();
                    }
                    return rows;
                } catch (ExecutionException e) {
                    if (!ExceptionUtils.findThrowable(
                                            e, LookupPositionNotYetReadableException.class)
                                    .isPresent()
                            || System.currentTimeMillis() > deadline) {
                        throw e;
                    }
                    notYetReadable.incrementAndGet();
                    Thread.sleep(10);
                }
            }
        }

        /** A cut with a random position in the history window of every bucket. */
        Map<TableBucket, Long> randomCut() throws Exception {
            Map<TableBucket, Long> cut = new HashMap<>();
            for (TableBucket bucket : buckets) {
                BucketReadRange range = admin.getBucketReadRange(bucket).get();
                long earliest = range.getEarliestPosition().getNextOffset();
                long latest = range.getLatestPosition().getNextOffset();
                int pick = random.nextInt(10);
                long position;
                if (pick == 0) {
                    position = SecondaryIndexLookuper.EMPTY_BUCKET_POSITION;
                } else if (pick == 1) {
                    position = earliest;
                } else if (pick == 2) {
                    position = latest;
                } else {
                    position = earliest + (long) (random.nextDouble() * (latest - earliest + 1));
                }
                cut.put(bucket, position);
            }
            syncChangelog(cut);
            return cut;
        }

        Map<TableBucket, Long> latestCut() throws Exception {
            Map<TableBucket, Long> cut = new HashMap<>();
            for (TableBucket bucket : buckets) {
                cut.put(
                        bucket,
                        admin.getBucketReadRange(bucket).get().getLatestPosition().getNextOffset());
            }
            syncChangelog(cut);
            return cut;
        }

        /** The rows of a value at a cut: the changelog of every bucket up to its position. */
        Set<String> expected(int value, Map<TableBucket, Long> cut) {
            Set<String> rows = new HashSet<>();
            for (TableBucket bucket : buckets) {
                long position = cut.get(bucket);
                Map<Integer, int[]> state = new HashMap<>();
                for (Change change : changelog.get(bucket.getBucket()).headMap(position).values()) {
                    if (change.type == ChangeType.INSERT
                            || change.type == ChangeType.UPDATE_AFTER) {
                        state.put(change.id, new int[] {change.value, change.version});
                    } else if (change.type == ChangeType.DELETE) {
                        state.remove(change.id);
                    }
                }
                state.forEach(
                        (id, row) -> {
                            if (row[0] == value) {
                                rows.add(id + "," + row[0] + "," + row[1]);
                            }
                        });
            }
            return rows;
        }

        /** Reads the changelog of every bucket up to the positions of a cut. */
        private void syncChangelog(Map<TableBucket, Long> cut) {
            long deadline = System.currentTimeMillis() + 60_000;
            while (!changelogReaches(cut)) {
                assertThat(System.currentTimeMillis())
                        .as("the changelog reaches %s", cut)
                        .isLessThan(deadline);
                ScanRecords records = changelogScanner.poll(Duration.ofMillis(100));
                for (TableBucket bucket : records.buckets()) {
                    for (ScanRecord record : records.records(bucket)) {
                        InternalRow row = record.getRow();
                        changelog
                                .get(bucket.getBucket())
                                .put(
                                        record.logOffset(),
                                        new Change(
                                                record.getChangeType(),
                                                row.getInt(0),
                                                row.getInt(1),
                                                row.getInt(2)));
                    }
                }
            }
        }

        private boolean changelogReaches(Map<TableBucket, Long> cut) {
            for (TableBucket bucket : buckets) {
                TreeMap<Long, Change> changes = changelog.get(bucket.getBucket());
                long position = cut.get(bucket);
                // every offset holds one change: the writes here never leave a batch empty
                if (position > 0 && (changes.isEmpty() || changes.lastKey() < position - 1)) {
                    return false;
                }
            }
            return true;
        }

        private static String toString(InternalRow row) {
            return row.getInt(0) + "," + row.getInt(1) + "," + row.getInt(2);
        }

        // ------------------------------------------------------------------------------------
        // history floors and purges

        /** Snapshots every bucket so that its floor passes the changes older than the TTL. */
        Map<TableBucket, Long> advanceHistoryFloors() throws Exception {
            Map<TableBucket, Long> floors = new HashMap<>();
            for (TableBucket bucket : buckets) {
                CompletedSnapshot snapshot = CLUSTER.triggerAndWaitSnapshot(bucket);
                long floor = snapshot.getHistoryFloor();
                assertThat(floor).as("history floor of %s", bucket).isPositive();
                waitUntil(
                        () ->
                                admin.getBucketReadRange(bucket)
                                                .get(10, TimeUnit.SECONDS)
                                                .getEarliestPosition()
                                                .getNextOffset()
                                        == floor,
                        Duration.ofSeconds(30),
                        "The leader did not publish the history floor of " + bucket);
                floors.put(bucket, floor);
            }
            return floors;
        }

        /** The removed pointers of the index: source bucket to the offsets that removed them. */
        List<long[]> removedPointers() throws Exception {
            List<long[]> removed = new ArrayList<>();
            for (InternalRow row : indexRows(IndexTableUtils.REMOVED_RECORD_KIND)) {
                removed.add(new long[] {sourceBucketOf(row), sourceOffsetOf(row)});
            }
            return removed;
        }

        int removedPointersBelow(Map<TableBucket, Long> floors) throws Exception {
            int below = 0;
            for (long[] removed : removedPointers()) {
                if (removed[1] < floors.get(buckets.get((int) removed[0]))) {
                    below++;
                }
            }
            return below;
        }

        /** Waits until every index bucket recorded the floor of every source bucket. */
        void awaitPurgedBelow(Map<TableBucket, Long> floors) {
            waitUntil(
                    () -> {
                        Map<String, Long> purged = new HashMap<>();
                        long indexTableId = indexTable.getTableInfo().getTableId();
                        for (int target = 0; target < INDEX_BUCKETS; target++) {
                            for (InternalRow row :
                                    scan(
                                            new TableBucket(indexTableId, target),
                                            IndexTableUtils.PURGE_FLOOR_RECORD_KIND)) {
                                purged.put(target + "/" + sourceBucketOf(row), sourceOffsetOf(row));
                            }
                        }
                        for (int target = 0; target < INDEX_BUCKETS; target++) {
                            for (TableBucket source : buckets) {
                                Long floor = purged.get(target + "/" + source.getBucket());
                                if (floor == null || floor < floors.get(source)) {
                                    return false;
                                }
                            }
                        }
                        return true;
                    },
                    Duration.ofMinutes(1),
                    "The index buckets did not purge below the history floors " + floors);
        }

        /**
         * A cut one below the floor of a bucket expires: every index bucket purged below it, so a
         * read must not answer, whether or not it would have needed a purged pointer.
         */
        void assertExpiredBelow(Map<TableBucket, Long> floors) throws Exception {
            Map<TableBucket, Long> cut = latestCut();
            for (TableBucket bucket : buckets) {
                Map<TableBucket, Long> below = new HashMap<>(cut);
                below.put(bucket, floors.get(bucket) - 1);
                for (int value = 0; value < VALUES; value++) {
                    int read = value;
                    assertThatThrownBy(() -> readAt(read, below))
                            .as("value %s at %s", value, below)
                            .isInstanceOf(ExecutionException.class)
                            .hasCauseInstanceOf(LookupPositionExpiredException.class);
                }
            }
        }

        private List<InternalRow> indexRows(byte recordKind) throws Exception {
            List<InternalRow> rows = new ArrayList<>();
            long indexTableId = indexTable.getTableInfo().getTableId();
            for (int target = 0; target < INDEX_BUCKETS; target++) {
                rows.addAll(scan(new TableBucket(indexTableId, target), recordKind));
            }
            return rows;
        }

        private List<InternalRow> scan(TableBucket bucket, byte recordKind) throws Exception {
            int kind = column(IndexTableUtils.RECORD_KIND_SYSTEM_COLUMN);
            int rowKey = column(IndexTableUtils.ROW_KEY_SYSTEM_COLUMN);
            int offset = column(IndexTableUtils.SOURCE_PROGRESS_SYSTEM_COLUMN);
            List<InternalRow> rows = new ArrayList<>();
            try (BatchScanner scanner = indexTable.newScan().createBatchScanner(bucket)) {
                CloseableIterator<InternalRow> batch;
                while ((batch = scanner.pollBatch(Duration.ofSeconds(30))) != null) {
                    try (CloseableIterator<InternalRow> records = batch) {
                        while (records.hasNext()) {
                            InternalRow row = records.next();
                            if (row.getByte(kind) == recordKind) {
                                rows.add(
                                        row(
                                                row.getByte(kind),
                                                row.getBytes(rowKey).clone(),
                                                row.getLong(offset)));
                            }
                        }
                    }
                }
            }
            return rows;
        }

        private int column(String name) {
            return indexTable.getTableInfo().getSchema().getColumnNames().indexOf(name);
        }

        private static int sourceBucketOf(InternalRow scanned) {
            return IndexTableUtils.sourceBucketOf(scanned.getBytes(1));
        }

        private static long sourceOffsetOf(InternalRow scanned) {
            return scanned.getLong(2);
        }

        @Override
        public void close() throws Exception {
            pauseIndexReads(false);
            changelogScanner.close();
            writer.flush();
            indexTable.close();
            table.close();
            admin.close();
            connection.close();
        }
    }

    /** A change of the changelog of a bucket. */
    private static final class Change {
        final ChangeType type;
        final int id;
        final int value;
        final int version;

        Change(ChangeType type, int id, int value, int version) {
            this.type = type;
            this.id = id;
            this.value = value;
            this.version = version;
        }
    }
}
