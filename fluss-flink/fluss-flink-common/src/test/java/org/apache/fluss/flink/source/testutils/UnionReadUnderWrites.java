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

package org.apache.fluss.flink.source.testutils;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.rpc.messages.GetDvSnapshotRequest;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs a bounded union read of a table {@code (c1 INT PRIMARY KEY, c2 STRING)} while other keys
 * keep being updated, and checks that the read is exact: each key once, none lost, and each value
 * one the key had. Splits of a bucket read with different cuts would emit a key twice or lose it.
 */
public final class UnionReadUnderWrites {

    private static final List<String> ROWS = new CopyOnWriteArrayList<>();

    private final Configuration clientConf;
    private final TablePath tablePath;
    private final Map<String, String> hints = new HashMap<>();
    private long millisPerRow;
    private long millisBetweenWrites = 10;

    public UnionReadUnderWrites(Configuration clientConf, TablePath tablePath) {
        this.clientConf = clientConf;
        this.tablePath = tablePath;
    }

    /** Adds a table option of the read, as a SQL hint. */
    public UnionReadUnderWrites withHint(String key, String value) {
        hints.put(key, value);
        return this;
    }

    /** Slows the read down, so its splits start at different times while the writes go on. */
    public UnionReadUnderWrites withMillisPerRow(long millisPerRow) {
        this.millisPerRow = millisPerRow;
        return this;
    }

    /**
     * Reads the table once while {@code keysToUpdate} are updated in turn, and checks the rows
     * against {@code before}, the table before the writes. Returns the values written meanwhile.
     */
    public Map<Integer, Set<String>> readAndCheck(
            Map<Integer, String> before, List<Integer> keysToUpdate, Duration timeout)
            throws Exception {
        Map<Integer, Set<String>> written = new ConcurrentHashMap<>();
        AtomicBoolean stop = new AtomicBoolean();
        AtomicReference<Throwable> writeFailure = new AtomicReference<>();
        Thread writer =
                new Thread(
                        () -> {
                            try {
                                update(keysToUpdate, written, stop);
                            } catch (Throwable t) {
                                writeFailure.set(t);
                            }
                        },
                        "union-read-writer");
        ROWS.clear();
        writer.start();
        try {
            // the writes are under way before the read is planned
            long deadline = System.currentTimeMillis() + 30_000;
            while (written.isEmpty() && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            read(timeout);
        } finally {
            stop.set(true);
            writer.join(TimeUnit.MINUTES.toMillis(1));
        }
        assertThat(writeFailure.get()).isNull();
        assertThat(written).as("writes during the read").isNotEmpty();

        Map<Integer, String> read = new HashMap<>();
        for (String row : ROWS) {
            String[] keyAndValue = row.split("=", 2);
            int key = Integer.parseInt(keyAndValue[0]);
            assertThat(read.put(key, keyAndValue[1])).as("key %s read twice", key).isNull();
        }
        assertThat(read.keySet()).isEqualTo(before.keySet());
        for (Map.Entry<Integer, String> e : read.entrySet()) {
            Set<String> values = new HashSet<>(written.getOrDefault(e.getKey(), new HashSet<>()));
            values.add(before.get(e.getKey()));
            assertThat(values).as("values of key %s", e.getKey()).contains(e.getValue());
        }
        return written;
    }

    private void update(List<Integer> keys, Map<Integer, Set<String>> written, AtomicBoolean stop)
            throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(clientConf);
                Table table = connection.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (int i = 0; !stop.get(); i++) {
                int key = keys.get(i % keys.size());
                String value = "w" + i;
                // recorded first: a write may land though its flush fails
                written.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(value);
                writer.upsert(GenericRow.of(key, BinaryString.fromString(value)));
                writer.flush();
                Thread.sleep(millisBetweenWrites);
            }
        }
    }

    private void read(Duration timeout) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        // one reader takes the splits in turn, so later ones start while the writes go on
        env.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, EnvironmentSettings.inBatchMode());
        tEnv.executeSql(
                String.format(
                        "create catalog union_read_under_writes with ('type' = 'fluss',"
                                + " 'bootstrap.servers' = '%s')",
                        String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS))));
        String options =
                hints.entrySet().stream()
                        .map(e -> String.format("'%s' = '%s'", e.getKey(), e.getValue()))
                        .collect(Collectors.joining(", "));
        String query =
                String.format(
                        "select c1, c2 from union_read_under_writes.`%s`.`%s`%s",
                        tablePath.getDatabaseName(),
                        tablePath.getTableName(),
                        options.isEmpty() ? "" : " /*+ OPTIONS(" + options + ") */");
        long sleep = millisPerRow;
        tEnv.toDataStream(tEnv.sqlQuery(query))
                .map(new SlowRows(sleep))
                .returns(Types.STRING)
                .addSink(new CollectRows());
        JobClient job = env.executeAsync("union read under writes of " + tablePath);
        job.getJobExecutionResult().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Turns rows into {@code key=value}, sleeping {@code millisPerRow} per row. */
    private static final class SlowRows implements MapFunction<Row, String> {
        private static final long serialVersionUID = 1L;
        private final long millisPerRow;

        SlowRows(long millisPerRow) {
            this.millisPerRow = millisPerRow;
        }

        @Override
        public String map(Row row) throws Exception {
            if (millisPerRow > 0) {
                Thread.sleep(millisPerRow);
            }
            return row.getField(0) + "=" + row.getField(1);
        }
    }

    private static final class CollectRows implements SinkFunction<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public void invoke(String value, Context context) {
            ROWS.add(value);
        }
    }

    /** The logger of the deletion vector union read, whose messages name the parts read. */
    public static final String DV_READ_LOGGER = "org.apache.fluss.client.table.scanner.batch.dv";

    /** The table option that sets the rows of a bucket's lake files one split reads at most. */
    public static final String FILE_GROUP_ROWS =
            ConfigOptions.CLIENT_SCANNER_DELETION_VECTORS_FILE_GROUP_ROWS.key();

    private static final Pattern PART_READ =
            Pattern.compile("Reading bucket TableBucket\\{[^}]*bucket=(\\d+)\\} \\(([^)]*)\\)");

    /** The parts read of each bucket, parsed from the read's log messages. */
    public static Map<Integer, Set<String>> partsReadPerBucket(List<String> messages) {
        Map<Integer, Set<String>> parts = new TreeMap<>();
        for (String message : messages) {
            Matcher part = PART_READ.matcher(message);
            if (part.find()) {
                parts.computeIfAbsent(Integer.parseInt(part.group(1)), b -> new TreeSet<>())
                        .add(part.group(2));
            }
        }
        return parts;
    }

    /** Whether the bucket's leader pins cuts: it answers a pin with a paged header. */
    public static boolean leaderPinsCuts(Admin admin, TablePath tablePath, long tableId)
            throws Exception {
        long snapshotId = admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId();
        GetDvSnapshotResponse pin =
                admin.getDvSnapshot(
                                tablePath,
                                new GetDvSnapshotRequest()
                                        .setTableId(tableId)
                                        .setBucketId(0)
                                        .setReadableSnapshotId(snapshotId)
                                        .setPaged(true))
                        .get();
        return pin.hasPaged() && pin.isPaged();
    }

    /** The keys of {@code before} in an order that spreads the updates over all of them. */
    public static List<Integer> everyNth(Map<Integer, String> before, int n) {
        List<Integer> keys = new ArrayList<>();
        int i = 0;
        for (Integer key : before.keySet()) {
            if (i++ % n == 0) {
                keys.add(key);
            }
        }
        return keys;
    }
}
