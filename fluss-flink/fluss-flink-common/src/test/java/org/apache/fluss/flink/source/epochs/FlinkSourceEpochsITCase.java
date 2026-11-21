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

package org.apache.fluss.flink.source.epochs;

import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.client.lookup.LookupType;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.flink.FlinkConnectorOptions;
import org.apache.fluss.flink.source.FlinkSource;
import org.apache.fluss.flink.source.FlinkTableSource;
import org.apache.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import org.apache.fluss.flink.source.lookup.FlinkAsyncLookupFunction;
import org.apache.fluss.flink.source.lookup.LookupNormalizer;
import org.apache.fluss.flink.source.reader.LeaseContext;
import org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests of epochs on a Fluss cluster: a scan of a versioned table cuts its changes into
 * epochs and emits their watermarks, and lookups read the table as of each epoch exactly as the
 * changes of the scan up to the epoch left it.
 */
class FlinkSourceEpochsITCase extends FlinkTestBase {

    private static final long INTERVAL_MS = 100L;
    private static final long CLOSE_AFTER_MS = 25L;
    private static final int BUCKETS = 3;
    private static final long NONE = Long.MIN_VALUE;

    /** The groups of the versions of a table with groups. */
    private static final int GROUPS = 3;

    /** Per run and subtask: the events that the collector checkpointed, after a restore too. */
    private static final Map<String, List<String>> VIEWS = new ConcurrentHashMap<>();

    /** Per run, subtask and attempt: every event the collector received. */
    private static final Map<String, List<String>> RAW = new ConcurrentHashMap<>();

    private static final Set<String> FAILED = ConcurrentHashMap.newKeySet();

    @ParameterizedTest(name = "coordinated = {0}")
    @ValueSource(booleans = {false, true})
    void scanCutsEpochsThatLookupsReadAsOf(boolean coordinated) throws Exception {
        TablePath path = createVersionedTable("epochs_scan" + (coordinated ? "_coordinated" : ""));
        String run = "scan-" + UUID.randomUUID();
        EpochSettings epochs = settings(run, coordinated);
        Changes changes = new Changes(path);
        changes.insertNew(6);

        JobClient job = startScan(path, epochs, OffsetsInitializer.earliest(), -1, false);
        try {
            changes.writeRounds(14);
            awaitScanned(run, changes.records);
        } finally {
            job.cancel().get(30, TimeUnit.SECONDS);
        }

        List<Change> scanned = epochsOf(run);
        verifyLookupsAsOfEpochs(path, epochs, scanned, changes.ids);
    }

    /**
     * Lookups by a secondary index read as of an epoch at the cut of every bucket: the rows of a
     * key move between buckets' positions as the changes of the scan change the indexed column.
     */
    @Test
    void lookupsByASecondaryIndexReadAsOfEpochs() throws Exception {
        TablePath path = createVersionedTable("epochs_index", true);
        String run = "index-" + UUID.randomUUID();
        EpochSettings epochs = settings(run, false);
        Changes changes = new Changes(path, true);
        changes.insertNew(6);

        JobClient job = startScan(path, epochs, OffsetsInitializer.earliest(), -1, false);
        try {
            changes.writeRounds(14);
            awaitScanned(run, changes.records);
        } finally {
            job.cancel().get(30, TimeUnit.SECONDS);
        }

        verifyIndexLookupsAsOfEpochs(path, epochs, epochsOf(run));
    }

    /**
     * The scan that the planner gets from the table source, which starts with a snapshot: every
     * change comes with its epoch as its timestamp, the rows of the snapshot too.
     */
    @Test
    void tableSourceScanStampsEveryChangeWithItsEpoch() throws Exception {
        TablePath path = createVersionedTable("epochs_table_source");
        String run = "table-source-" + UUID.randomUUID();
        Changes changes = new Changes(path);
        changes.insertNew(6);
        changes.writeRounds(3);
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(path);
        changes.records = changes.current.size();

        JobClient job = startTableSourceScan(path, run);
        try {
            changes.writeRounds(10);
            // asserts the timestamps of every change on the way
            awaitScanned(run, changes.records);
        } finally {
            job.cancel().get(30, TimeUnit.SECONDS);
        }

        List<Change> scanned = epochsOf(run);
        verifyLookupsAsOfEpochs(path, settings(run, false), scanned, changes.ids);
    }

    @Test
    void scanOfASnapshotStartsFromAnEmptyTable() throws Exception {
        TablePath path = createVersionedTable("epochs_snapshot");
        String run = "snapshot-" + UUID.randomUUID();
        EpochSettings epochs = settings(run, false);
        Changes changes = new Changes(path);
        changes.insertNew(6);
        changes.writeRounds(3);
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(path);
        // the scan emits the rows of the snapshot as inserts, then the changes after it
        changes.records = changes.current.size();

        JobClient job = startScan(path, epochs, OffsetsInitializer.full(), -1, false);
        try {
            changes.writeRounds(10);
            awaitScanned(run, changes.records);
        } finally {
            job.cancel().get(30, TimeUnit.SECONDS);
        }

        List<Change> scanned = epochsOf(run);
        verifyLookupsAsOfEpochs(path, epochs, scanned, changes.ids);
    }

    @ParameterizedTest(name = "coordinated = {0}")
    @ValueSource(booleans = {false, true})
    void restoredScanEndsTheEpochsItEndedBeforeAtTheSamePositions(boolean coordinated)
            throws Exception {
        TablePath path =
                createVersionedTable("epochs_restore" + (coordinated ? "_coordinated" : ""));
        String run = "restore-" + UUID.randomUUID();
        EpochSettings epochs = settings(run, coordinated);
        Changes changes = new Changes(path);
        changes.insertNew(6);

        // the collector fails once after a checkpoint, when the epoch of a change after it ended
        JobClient job = startScan(path, epochs, OffsetsInitializer.earliest(), 1, true);
        try {
            changes.writeRounds(30);
            awaitScanned(run, changes.records);
        } finally {
            job.cancel().get(30, TimeUnit.SECONDS);
        }
        assertThat(FAILED).contains(run);

        // the changes after the checkpoint come again, in the epochs they were in before
        int replayed = 0;
        for (int subtask = 0; subtask < 2; subtask++) {
            List<String> first = RAW.getOrDefault(key(run, subtask, 0), new ArrayList<>());
            List<String> second = RAW.getOrDefault(key(run, subtask, 1), new ArrayList<>());
            Map<String, Long> before = epochsByChange(first);
            long endedBefore = lastWatermark(first);
            for (Map.Entry<String, Long> again : epochsByChange(second).entrySet()) {
                Long epoch = before.get(again.getKey());
                if (epoch != null && epoch <= endedBefore) {
                    assertThat(again.getValue()).as(again.getKey()).isEqualTo(epoch);
                    replayed++;
                }
            }
        }
        assertThat(replayed).isGreaterThan(0);

        // the checkpointed view holds every change once, in the epochs of the scan
        List<Change> scanned = epochsOf(run);
        verifyLookupsAsOfEpochs(path, epochs, scanned, changes.ids);
    }

    // ------------------------------------------------------------------------------------------

    private TablePath createVersionedTable(String name) throws Exception {
        return createVersionedTable(name, false);
    }

    /**
     * @param withGroup whether the table has a column {@code grp}, the group of the version, with
     *     an asynchronous secondary index on it
     */
    private TablePath createVersionedTable(String name, boolean withGroup) throws Exception {
        TablePath path = TablePath.of(DEFAULT_DB, name);
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("version", DataTypes.INT());
        if (withGroup) {
            schema.column("grp", DataTypes.INT())
                    .index(
                            "idx_grp",
                            IndexType.SECONDARY,
                            Collections.singletonList("grp"),
                            IndexVisibility.ASYNC,
                            2);
        }
        createTable(
                path,
                TableDescriptor.builder()
                        .schema(schema.primaryKey("id").build())
                        .distributedBy(BUCKETS, "id")
                        .property(ConfigOptions.TABLE_KV_MVCC_ENABLED, true)
                        .build());
        return path;
    }

    /** The group of a version, by which the secondary index of a table with groups finds it. */
    private static int group(int version) {
        return version % GROUPS;
    }

    private JobClient startScan(
            TablePath path,
            EpochSettings epochs,
            OffsetsInitializer startingOffsets,
            int failAfter,
            boolean checkpoints)
            throws Exception {
        String run = epochs.getDomain();
        Configuration conf = new Configuration();
        conf.setString("restart-strategy.type", "fixed-delay");
        conf.setString("restart-strategy.fixed-delay.attempts", "3");
        conf.setString("restart-strategy.fixed-delay.delay", "200 ms");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        if (checkpoints) {
            env.enableCheckpointing(400);
        }
        Table table = conn.getTable(path);
        FlinkSource<RowData> source =
                new FlinkSource<>(
                                clientConf,
                                path,
                                true,
                                false,
                                table.getTableInfo().getRowType(),
                                null,
                                null,
                                startingOffsets,
                                1000L,
                                new RowDataDeserializationSchema(),
                                true,
                                null,
                                LeaseContext.DEFAULT)
                        .withEpochs(epochs);
        table.close();
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "fluss")
                .transform("collect", Types.VOID, new Collector(run, failAfter))
                .addSink(new DiscardingSink<>());
        return env.executeAsync("epochs " + run);
    }

    /** Starts the scan that the table source provides, in full startup mode, with epochs. */
    private JobClient startTableSourceScan(TablePath path, String run) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        RowType rowType = flinkRowType(path);
        org.apache.fluss.config.Configuration tableConfig =
                new org.apache.fluss.config.Configuration();
        tableConfig.set(ConfigOptions.TABLE_KV_MVCC_ENABLED, true);
        FlinkConnectorOptionsUtils.StartupOptions startup =
                new FlinkConnectorOptionsUtils.StartupOptions();
        startup.startupMode = FlinkConnectorOptions.ScanStartupMode.FULL;
        Map<String, String> options = new HashMap<>();
        options.put(FlinkConnectorOptions.BUCKET_NUMBER.key(), Integer.toString(BUCKETS));
        options.put(FlinkConnectorOptions.SCAN_EPOCH_INTERVAL.key(), INTERVAL_MS + " ms");
        FlinkTableSource source =
                new FlinkTableSource(
                        path,
                        clientConf,
                        new TableConfig(tableConfig),
                        rowType,
                        new int[] {0},
                        new int[] {0},
                        new int[0],
                        true,
                        startup,
                        FlinkConnectorOptionsUtils.BoundedOptions.unbounded(),
                        true,
                        false,
                        null,
                        LookupOptions.LookupCacheType.NONE,
                        1000L,
                        FlinkConnectorOptions.SCAN_SPLIT_ASSIGNMENT_BATCH_SIZE.defaultValue(),
                        false,
                        null,
                        options,
                        LeaseContext.DEFAULT);
        assertThat(source.applyEpochs(run)).isTrue();
        DataStreamScanProvider provider =
                (DataStreamScanProvider)
                        source.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        provider.produceDataStream(name -> Optional.empty(), env)
                .transform("collect", Types.VOID, new Collector(run, -1))
                .addSink(new DiscardingSink<>());
        return env.executeAsync("table source epochs " + run);
    }

    /** Waits until the scan emitted every change, and ended the epochs of all of them. */
    private static void awaitScanned(String run, int records) {
        waitUntil(
                () -> {
                    List<Change> changes = epochsOf(run);
                    if (changes.size() < records) {
                        return false;
                    }
                    long last = changes.stream().mapToLong(c -> c.epoch).max().orElse(NONE);
                    for (int subtask = 0; subtask < 2; subtask++) {
                        List<String> view = VIEWS.get(run + "/" + subtask);
                        if (view == null || lastWatermark(view) < last) {
                            return false;
                        }
                    }
                    return true;
                },
                Duration.ofMinutes(2),
                Duration.ofMillis(50),
                "The scan did not emit and end every change.");
        assertThat(epochsOf(run)).hasSize(records);
    }

    /**
     * The changes of the checkpointed view of every subtask with their epochs: one after the last
     * watermark before them. Checks the watermarks on the way.
     */
    private static List<Change> epochsOf(String run) {
        List<Change> changes = new ArrayList<>();
        for (int subtask = 0; subtask < 2; subtask++) {
            List<String> view = VIEWS.get(run + "/" + subtask);
            if (view == null) {
                continue;
            }
            long watermark = NONE;
            for (String event : view) {
                if (event.equals("RESTORE")) {
                    // the restored scan announces the epoch of its next change again
                    watermark = NONE;
                } else if (event.startsWith("W")) {
                    long next = Long.parseLong(event.substring(1));
                    assertThat(next)
                            .as("watermarks of subtask %s", subtask)
                            .isGreaterThan(watermark);
                    watermark = next;
                } else {
                    assertThat(watermark)
                            .as("a change before the watermark that tells its epoch")
                            .isNotEqualTo(NONE);
                    assertStamped(event, watermark + 1);
                    changes.add(Change.parse(change(event), watermark + 1));
                }
            }
        }
        return changes;
    }

    private static Map<String, Long> epochsByChange(List<String> events) {
        Map<String, Long> epochs = new HashMap<>();
        long watermark = NONE;
        for (String event : events) {
            if (event.startsWith("W")) {
                watermark = Long.parseLong(event.substring(1));
            } else if (!event.equals("RESTORE")) {
                assertStamped(event, watermark + 1);
                epochs.put(change(event), watermark + 1);
            }
        }
        return epochs;
    }

    /** The change of a collected record, without the timestamp it came with. */
    private static String change(String event) {
        return event.substring(0, event.indexOf('@'));
    }

    /** A change comes with its epoch as its timestamp: one after the last watermark before it. */
    private static void assertStamped(String event, long epoch) {
        assertThat(event.substring(event.indexOf('@') + 1))
                .as("the timestamp of change %s", event)
                .isEqualTo(Long.toString(epoch));
    }

    private static long lastWatermark(List<String> events) {
        long watermark = NONE;
        for (String event : events) {
            if (event.startsWith("W")) {
                watermark = Long.parseLong(event.substring(1));
            }
        }
        return watermark;
    }

    /**
     * Looks every key up as of the epochs around every change, and compares the rows with the
     * changes of the scan up to the epoch applied.
     */
    private void verifyLookupsAsOfEpochs(
            TablePath path, EpochSettings epochs, List<Change> scanned, Set<Integer> ids)
            throws Exception {
        assertThat(scanned.get(0).epoch).isGreaterThanOrEqualTo(0L);
        long lastEnded = lastEnded(epochs.getDomain());
        RowType rowType = flinkRowType(path);
        FlinkAsyncLookupFunction lookup =
                new FlinkAsyncLookupFunction(
                        clientConf,
                        path,
                        rowType,
                        LookupNormalizer.createPrimaryKeyLookupNormalizer(new int[] {0}, rowType),
                        null,
                        false,
                        false,
                        epochs);
        lookup.open(null);
        try {
            Map<String, CompletableFuture<Collection<RowData>>> reads = new LinkedHashMap<>();
            Map<String, Set<String>> expected = new HashMap<>();
            for (long epoch : epochsAround(scanned, lastEnded)) {
                Map<Integer, Integer> state = stateAsOf(scanned, epoch);
                for (int id : ids) {
                    String read = id + " as of " + epoch;
                    reads.put(read, lookup.asyncLookupAsOf(GenericRowData.of(id), epoch));
                    Set<String> rows = new HashSet<>();
                    if (state.containsKey(id)) {
                        rows.add(id + "," + state.get(id));
                    }
                    expected.put(read, rows);
                }
            }
            for (Map.Entry<String, CompletableFuture<Collection<RowData>>> read :
                    reads.entrySet()) {
                Set<String> rows = new HashSet<>();
                for (RowData row : read.getValue().get(1, TimeUnit.MINUTES)) {
                    rows.add(row.getInt(0) + "," + row.getInt(1));
                }
                assertThat(rows).as(read.getKey()).isEqualTo(expected.get(read.getKey()));
            }
            // an epoch that the scan did not end yet waits
            CompletableFuture<Collection<RowData>> later =
                    lookup.asyncLookupAsOf(GenericRowData.of(0), lastEnded + 100_000);
            Thread.sleep(300);
            assertThat(later).isNotDone();
        } finally {
            lookup.close();
        }
    }

    /**
     * Looks every group up by the secondary index as of the epochs around every change, and
     * compares the rows with the changes of the scan up to the epoch applied.
     */
    private void verifyIndexLookupsAsOfEpochs(
            TablePath path, EpochSettings epochs, List<Change> scanned) throws Exception {
        long lastEnded = lastEnded(epochs.getDomain());
        RowType rowType = flinkRowType(path);
        LookupNormalizer byGroup =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        new int[][] {{2}},
                        new int[] {0},
                        new int[] {0},
                        new int[0],
                        rowType,
                        null,
                        new int[][] {{2}});
        assertThat(byGroup.getLookupType()).isEqualTo(LookupType.SECONDARY_INDEX_LOOKUP);
        FlinkAsyncLookupFunction lookup =
                new FlinkAsyncLookupFunction(
                        clientConf, path, rowType, byGroup, null, false, false, epochs);
        lookup.open(null);
        try {
            Map<String, CompletableFuture<Collection<RowData>>> reads = new LinkedHashMap<>();
            Map<String, Set<String>> expected = new HashMap<>();
            int matches = 0;
            for (long epoch : epochsAround(scanned, lastEnded)) {
                Map<Integer, Integer> state = stateAsOf(scanned, epoch);
                for (int group = 0; group < GROUPS; group++) {
                    String read = "group " + group + " as of " + epoch;
                    reads.put(read, lookup.asyncLookupAsOf(GenericRowData.of(group), epoch));
                    Set<String> rows = new HashSet<>();
                    for (Map.Entry<Integer, Integer> row : state.entrySet()) {
                        if (group(row.getValue()) == group) {
                            rows.add(row.getKey() + "," + row.getValue());
                        }
                    }
                    matches += rows.size();
                    expected.put(read, rows);
                }
            }
            assertThat(matches).isPositive();
            for (Map.Entry<String, CompletableFuture<Collection<RowData>>> read :
                    reads.entrySet()) {
                Set<String> rows = new HashSet<>();
                for (RowData row : read.getValue().get(1, TimeUnit.MINUTES)) {
                    assertThat(row.getInt(2)).isEqualTo(group(row.getInt(1)));
                    rows.add(row.getInt(0) + "," + row.getInt(1));
                }
                assertThat(rows).as(read.getKey()).isEqualTo(expected.get(read.getKey()));
            }
            // an epoch that the scan did not end yet waits
            CompletableFuture<Collection<RowData>> later =
                    lookup.asyncLookupAsOf(GenericRowData.of(0), lastEnded + 100_000);
            Thread.sleep(300);
            assertThat(later).isNotDone();
        } finally {
            lookup.close();
        }
    }

    /** The epochs before and after every change, before the first and the last one ended. */
    private static TreeSet<Long> epochsAround(List<Change> scanned, long lastEnded) {
        TreeSet<Long> epochs = new TreeSet<>();
        epochs.add(Epochs.BEFORE_FIRST);
        for (Change change : scanned) {
            epochs.add(change.epoch);
            epochs.add(change.epoch - 1);
        }
        epochs.add(lastEnded);
        return epochs;
    }

    /** The last epoch that every subtask ended. */
    private static long lastEnded(String run) {
        long lastEnded = Long.MAX_VALUE;
        for (int subtask = 0; subtask < 2; subtask++) {
            lastEnded = Math.min(lastEnded, lastWatermark(VIEWS.get(run + "/" + subtask)));
        }
        return lastEnded;
    }

    private static RowType flinkRowType(TablePath path) throws Exception {
        try (Table table = conn.getTable(path)) {
            return FlinkConversions.toFlinkRowType(table.getTableInfo().getRowType());
        }
    }

    /** The rows of the table after the scanned changes of the epochs up to the given one. */
    private static Map<Integer, Integer> stateAsOf(List<Change> scanned, long epoch) {
        Map<Integer, Integer> state = new HashMap<>();
        for (Change change : scanned) {
            if (change.epoch > epoch) {
                continue;
            }
            if (change.kind == RowKind.INSERT || change.kind == RowKind.UPDATE_AFTER) {
                state.put(change.id, change.version);
            } else {
                state.remove(change.id);
            }
        }
        return state;
    }

    private static String key(String run, int subtask, int attempt) {
        return run + "/" + subtask + "/" + attempt;
    }

    // ------------------------------------------------------------------------------------------

    /** A change that the scan emitted, in the order of its subtask. */
    private static final class Change {
        final long epoch;
        final RowKind kind;
        final int id;
        final int version;

        Change(long epoch, RowKind kind, int id, int version) {
            this.epoch = epoch;
            this.kind = kind;
            this.id = id;
            this.version = version;
        }

        static Change parse(String event, long epoch) {
            String[] parts = event.split(",");
            return new Change(
                    epoch,
                    RowKind.fromByteValue(Byte.parseByte(parts[0])),
                    Integer.parseInt(parts[1]),
                    Integer.parseInt(parts[2]));
        }
    }

    /** Writes changes and counts the changelog records they make. */
    private static final class Changes {
        final Set<Integer> ids = new TreeSet<>();
        final Map<Integer, Integer> current = new HashMap<>();
        final UpsertWriter writer;
        final boolean withGroup;
        int version;
        int nextId;
        int records;

        Changes(TablePath path) {
            this(path, false);
        }

        Changes(TablePath path, boolean withGroup) {
            this.writer = conn.getTable(path).newUpsert().createWriter();
            this.withGroup = withGroup;
        }

        private GenericRow row(int id, int version) {
            return withGroup
                    ? GenericRow.of(id, version, group(version))
                    : GenericRow.of(id, version);
        }

        void insertNew(int count) throws Exception {
            for (int i = 0; i < count; i++) {
                upsert(nextId++);
            }
        }

        void upsert(int id) throws Exception {
            int next = ++version;
            writer.upsert(row(id, next)).get(30, TimeUnit.SECONDS);
            records += current.containsKey(id) ? 2 : 1;
            current.put(id, next);
            ids.add(id);
        }

        void delete(int id) throws Exception {
            Integer old = current.remove(id);
            if (old != null) {
                writer.delete(row(id, old)).get(30, TimeUnit.SECONDS);
                records++;
            }
        }

        /** Updates, inserts and deletes in rounds that span several epochs. */
        void writeRounds(int rounds) throws Exception {
            for (int round = 0; round < rounds; round++) {
                upsert(round % 5);
                upsert((round * 7 + 3) % 9);
                if (round % 2 == 0) {
                    insertNew(1);
                }
                if (round % 4 == 3) {
                    delete((round * 3) % nextId);
                }
                Thread.sleep(round % 3 == 0 ? 40 : 130);
            }
        }
    }

    /**
     * Collects records and watermarks. It keeps them in state, so that its view after a restore
     * holds every change once, and fails once after a checkpoint if asked to.
     */
    private static final class Collector extends AbstractStreamOperator<Void>
            implements OneInputStreamOperator<RowData, Void> {

        private static final long serialVersionUID = 1L;

        private final String run;
        private final int failAfter;

        private transient ListState<String> state;
        private transient List<String> events;

        /** The last checkpoint this collector took a snapshot for, and the last completed one. */
        private transient long lastSnapshot;

        private transient long lastCompleted;

        /** The changes since the last snapshot. */
        private transient int sinceSnapshot;

        Collector(String run, int failAfter) {
            this.run = run;
            this.failAfter = failAfter;
            setChainingStrategy(ChainingStrategy.ALWAYS);
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            state =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("events", String.class));
            events = new ArrayList<>();
            lastSnapshot = -1L;
            lastCompleted = -1L;
            if (context.isRestored()) {
                for (String event : state.get()) {
                    events.add(event);
                }
                events.add("RESTORE");
            }
            publish();
        }

        @Override
        public void processElement(StreamRecord<RowData> element) {
            RowData row = element.getValue();
            // the change, and the epoch that the scan stamped it with
            add(
                    row.getRowKind().toByteValue()
                            + ","
                            + row.getInt(0)
                            + ","
                            + row.getInt(1)
                            + "@"
                            + (element.hasTimestamp() ? element.getTimestamp() : "none"));
            sinceSnapshot++;
        }

        @Override
        public void processWatermark(Watermark mark) throws Exception {
            // the restore starts at the last completed snapshot and replays the epochs since
            if (failAfter >= 0
                    && lastSnapshot >= 0
                    && lastCompleted == lastSnapshot
                    && sinceSnapshot >= failAfter
                    && FAILED.add(run)) {
                add("W" + mark.getTimestamp());
                throw new RuntimeException("Injected failure after a checkpoint.");
            }
            add("W" + mark.getTimestamp());
            super.processWatermark(mark);
        }

        private void add(String event) {
            events.add(event);
            RAW.computeIfAbsent(
                            key(
                                    run,
                                    getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(),
                                    getRuntimeContext().getTaskInfo().getAttemptNumber()),
                            k -> Collections.synchronizedList(new ArrayList<>()))
                    .add(event);
            publish();
        }

        private void publish() {
            VIEWS.put(
                    run + "/" + getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(),
                    new ArrayList<>(events));
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            state.update(events);
            lastSnapshot = context.getCheckpointId();
            sinceSnapshot = 0;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            super.notifyCheckpointComplete(checkpointId);
            lastCompleted = Math.max(lastCompleted, checkpointId);
        }
    }

    /** Epochs by the wall clock, or opened by the coordinators shortly after a change. */
    private static EpochSettings settings(String run, boolean coordinated) {
        return coordinated
                ? new EpochSettings(run, INTERVAL_MS, CLOSE_AFTER_MS)
                : new EpochSettings(run, INTERVAL_MS);
    }
}
