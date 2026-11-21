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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.flink.source.deserializer.FlussDeserializationSchema;
import org.apache.fluss.flink.source.emitter.FlinkRecordEmitter;
import org.apache.fluss.flink.source.reader.RecordAndPos;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.LogSplitState;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.split.SourceSplitState;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.RowType;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.core.io.InputStatus;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Epochs as a job uses them in one task manager: several readers, each with its registry, end short
 * epochs of sparse buckets of two tables, while several lookup functions, each with its registry,
 * resolve recent epochs of all buckets at once. Every resolution must be where the bucket was when
 * its reader ended the epoch.
 */
class EpochsInOneTaskManagerITCase extends FlinkTestBase {

    private static final long INTERVAL_MS = 50L;
    private static final int BUCKETS = 4;
    private static final int READERS = 4;
    private static final int LOOKUP_FUNCTIONS = 4;
    private static final long RUN_MS = 8_000L;

    @Test
    void readersAndLookupsOfSparseBucketsAgreeOnEveryEpoch() throws Exception {
        List<TablePath> tables = new ArrayList<>();
        List<TableBucket> buckets = new ArrayList<>();
        for (String name : new String[] {"sparse_sales", "sparse_items"}) {
            TablePath path = TablePath.of(DEFAULT_DB, name);
            long tableId = createTable(path, DEFAULT_PK_TABLE_DESCRIPTOR);
            tables.add(path);
            for (int b = 0; b < BUCKETS; b++) {
                buckets.add(new TableBucket(tableId, b));
            }
        }
        String domain = "one-task-manager";
        EpochSettings settings = new EpochSettings(domain, INTERVAL_MS);
        for (TablePath table : tables) {
            EpochRegistry.clear(clientConf, table, domain);
        }

        // per bucket: the epoch of each emitted change and the position after it
        Map<TableBucket, List<long[]>> emitted = new ConcurrentHashMap<>();
        for (TableBucket bucket : buckets) {
            emitted.put(bucket, Collections.synchronizedList(new ArrayList<>()));
        }
        AtomicBoolean reading = new AtomicBoolean(true);
        AtomicBoolean looking = new AtomicBoolean(true);
        List<String> failures = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger resolved = new AtomicInteger();
        ExecutorService threads = Executors.newCachedThreadPool();
        List<Future<?>> readers = new ArrayList<>();
        List<Future<?>> lookups = new ArrayList<>();
        try {
            // each reader owns two buckets of one table; some buckets rarely change
            for (int reader = 0; reader < READERS; reader++) {
                int table = reader / 2;
                List<TableBucket> owned = new ArrayList<>();
                for (int b = (reader % 2) * 2; b < (reader % 2) * 2 + 2; b++) {
                    owned.add(buckets.get(table * BUCKETS + b));
                }
                int seed = reader;
                readers.add(
                        threads.submit(
                                () ->
                                        runReader(
                                                tables.get(table),
                                                settings,
                                                owned,
                                                emitted,
                                                reading,
                                                new Random(seed))));
            }
            for (int function = 0; function < LOOKUP_FUNCTIONS; function++) {
                int seed = 100 + function;
                lookups.add(
                        threads.submit(
                                () ->
                                        runLookups(
                                                domain,
                                                buckets,
                                                emitted,
                                                looking,
                                                failures,
                                                resolved,
                                                new Random(seed))));
            }
            Thread.sleep(RUN_MS);
        } finally {
            // the lookups finish while the readers still end epochs
            looking.set(false);
            for (Future<?> task : lookups) {
                task.get(2, TimeUnit.MINUTES);
            }
            reading.set(false);
            for (Future<?> task : readers) {
                task.get(2, TimeUnit.MINUTES);
            }
            threads.shutdownNow();
        }
        assertThat(failures).isEmpty();
        assertThat(resolved.get()).isGreaterThan(1000);
    }

    /** Drives a reader like a source task: polls, and emits a change now and then. */
    private static Void runReader(
            TablePath table,
            EpochSettings settings,
            List<TableBucket> owned,
            Map<TableBucket, List<long[]>> emitted,
            AtomicBoolean running,
            Random random)
            throws Exception {
        try (ReaderEpochs<String> epochs = ReaderEpochs.open(clientConf, table, settings)) {
            RecordEmitter<RecordAndPos, String, SourceSplitState> emitter =
                    epochs.wrap(new FlinkRecordEmitter<>(new Offsets()));
            NoOutput output = new NoOutput();
            List<SourceSplitState> states = new ArrayList<>();
            for (TableBucket bucket : owned) {
                SourceSplitBase split =
                        epochs.addingSplits(
                                        Collections.singletonList(new LogSplit(bucket, null, 0L)))
                                .get(0);
                SourceSplitState state = new LogSplitState(split.asLogSplit());
                epochs.track(split, state);
                states.add(state);
            }
            while (running.get()) {
                InputStatus status = epochs.beforePoll(output);
                if (status == InputStatus.MORE_AVAILABLE) {
                    continue;
                }
                if (status == null) {
                    // the first bucket of a reader changes often, the second one rarely
                    int split = random.nextInt(10) < 9 ? 0 : 1;
                    if (random.nextInt(split == 0 ? 3 : 200) == 0) {
                        LogSplitState state = (LogSplitState) states.get(split);
                        long offset = state.getNextOffset();
                        long epoch = epochs.getLastWatermark() + 1;
                        emitter.emitRecord(
                                new RecordAndPos(
                                        new ScanRecord(
                                                offset,
                                                1L,
                                                ChangeType.INSERT,
                                                GenericRow.of((int) offset))),
                                output,
                                state);
                        emitted.get(owned.get(split))
                                .add(new long[] {epoch, state.getNextOffset()});
                    }
                }
                Thread.sleep(1);
            }
        }
        return null;
    }

    /** Resolves recent epochs of every bucket from several threads, like a lookup function. */
    private static Void runLookups(
            String domain,
            List<TableBucket> buckets,
            Map<TableBucket, List<long[]>> emitted,
            AtomicBoolean running,
            List<String> failures,
            AtomicInteger resolved,
            Random random)
            throws Exception {
        ScheduledExecutorService retries = Executors.newSingleThreadScheduledExecutor();
        try (Connection connection = ConnectionFactory.createConnection(clientConf);
                EpochRegistry registry =
                        EpochRegistry.open(connection, false, DEFAULT_DB, domain)) {
            EpochResolver resolver = new EpochResolver(registry, retries, 12L);
            List<CompletableFuture<Void>> reads = new ArrayList<>();
            while (running.get()) {
                TableBucket bucket = buckets.get(random.nextInt(buckets.size()));
                long epoch = System.currentTimeMillis() / INTERVAL_MS - random.nextInt(20);
                reads.add(
                        resolver.endOf(bucket, epoch)
                                .handle(
                                        (end, error) -> {
                                            String failure =
                                                    error != null
                                                            ? error.toString()
                                                            : check(
                                                                    bucket,
                                                                    epoch,
                                                                    end,
                                                                    emitted.get(bucket));
                                            if (failure != null) {
                                                failures.add(failure);
                                            } else {
                                                resolved.incrementAndGet();
                                            }
                                            return null;
                                        }));
                if (reads.size() >= 64) {
                    CompletableFuture.allOf(reads.toArray(new CompletableFuture<?>[0]))
                            .get(1, TimeUnit.MINUTES);
                    reads.clear();
                }
            }
            CompletableFuture.allOf(reads.toArray(new CompletableFuture<?>[0]))
                    .get(1, TimeUnit.MINUTES);
        } finally {
            retries.shutdownNow();
        }
        return null;
    }

    /** Null if the end is where the bucket was at the end of the epoch, else what differs. */
    private static String check(TableBucket bucket, long epoch, long end, List<long[]> emitted) {
        long expected = 0L;
        synchronized (emitted) {
            for (long[] change : emitted) {
                if (change[0] <= epoch) {
                    expected = change[1];
                }
            }
        }
        return end == expected
                ? null
                : String.format(
                        "%s ends epoch %s at %s, but its reader was at %s",
                        bucket, epoch, end, expected);
    }

    /** Deserializes a change into its offset. */
    private static final class Offsets implements FlussDeserializationSchema<String> {
        @Override
        public void open(InitializationContext context) {}

        @Override
        public String deserialize(LogRecord record) {
            return Long.toString(record.logOffset());
        }

        @Override
        public TypeInformation<String> getProducedType(RowType scanRowType) {
            return Types.STRING;
        }
    }

    /** An output that drops what it gets. */
    private static final class NoOutput implements ReaderOutput<String> {
        @Override
        public void collect(String record) {}

        @Override
        public void collect(String record, long timestamp) {}

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}

        @Override
        public SourceOutput<String> createOutputForSplit(String splitId) {
            return this;
        }

        @Override
        public void releaseOutputForSplit(String splitId) {}
    }
}
