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
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link EpochRegistry} on a Fluss cluster. */
class EpochRegistryITCase extends FlinkTestBase {

    @Test
    void recordsEndsAndFindsTheLatestAtOrBeforeAnEpoch() throws Exception {
        TableBucket bucket = new TableBucket(createScannedTable("registry_reads"), 1);
        try (Connection connection = ConnectionFactory.createConnection(clientConf);
                EpochRegistry registry =
                        EpochRegistry.open(connection, false, DEFAULT_DB, "reads")) {
            assertThat(registry.endedThrough(bucket).get()).isNull();
            assertThat(registry.latestEnd(bucket, 100L).get()).isNull();

            List<EpochRecord> records = new ArrayList<>();
            records.add(EpochRecord.end(bucket, Epochs.BEFORE_FIRST, Epochs.EMPTY));
            for (long epoch = 10; epoch <= 50; epoch += 10) {
                records.add(EpochRecord.end(bucket, epoch, epoch * 2));
            }
            records.add(EpochRecord.progress(bucket, 55L));
            registry.write(records).get();

            assertThat(registry.endedThrough(bucket).get()).isEqualTo(55L);
            // epochs order numerically, the one before the first too
            assertThat(registry.latestEnd(bucket, Epochs.BEFORE_FIRST).get())
                    .isEqualTo(new EpochEnd(Epochs.BEFORE_FIRST, Epochs.EMPTY));
            assertThat(registry.latestEnd(bucket, 9L).get())
                    .isEqualTo(new EpochEnd(Epochs.BEFORE_FIRST, Epochs.EMPTY));
            assertThat(registry.latestEnd(bucket, 30L).get()).isEqualTo(new EpochEnd(30L, 60L));
            assertThat(registry.latestEnd(bucket, 39L).get()).isEqualTo(new EpochEnd(30L, 60L));
            // the progress row is not an end
            assertThat(registry.latestEnd(bucket, Epochs.AFTER_LAST).get())
                    .isEqualTo(new EpochEnd(50L, 100L));
            assertThat(registry.latestEnds(bucket, 45L, 3).get())
                    .containsExactly(
                            new EpochEnd(40L, 80L), new EpochEnd(30L, 60L), new EpochEnd(20L, 40L));

            // another bucket and another domain do not see the ends
            TableBucket other = new TableBucket(bucket.getTableId(), 0);
            assertThat(registry.latestEnd(other, 100L).get()).isNull();
            try (EpochRegistry otherDomain =
                    EpochRegistry.open(connection, false, DEFAULT_DB, "reads-other")) {
                assertThat(otherDomain.endedThrough(bucket).get()).isNull();
            }
        }
    }

    @Test
    void freshScanClearsTheEpochsOfItsTableInItsDomain() throws Exception {
        TablePath scanned = TablePath.of(DEFAULT_DB, "registry_clear");
        long tableId = createScannedTable(scanned.getTableName());
        long otherTableId = createScannedTable("registry_clear_other");
        TableBucket bucket = new TableBucket(tableId, 0);
        TableBucket otherTable = new TableBucket(otherTableId, 0);
        try (Connection connection = ConnectionFactory.createConnection(clientConf);
                EpochRegistry registry =
                        EpochRegistry.open(connection, false, DEFAULT_DB, "clear");
                EpochRegistry otherDomain =
                        EpochRegistry.open(connection, false, DEFAULT_DB, "clear-other")) {
            List<EpochRecord> records = new ArrayList<>();
            for (long epoch = 0; epoch < 3000; epoch++) {
                records.add(EpochRecord.end(bucket, epoch, epoch));
            }
            records.add(EpochRecord.progress(bucket, 3000L));
            records.add(EpochRecord.end(otherTable, 1L, 1L));
            registry.write(records).get();
            otherDomain.write(Collections.singletonList(EpochRecord.end(bucket, 1L, 1L))).get();

            EpochRegistry.clear(clientConf, scanned, "clear");

            assertThat(registry.endedThrough(bucket).get()).isNull();
            assertThat(registry.latestEnd(bucket, Epochs.AFTER_LAST).get()).isNull();
            assertThat(registry.latestEnd(otherTable, 1L).get()).isEqualTo(new EpochEnd(1L, 1L));
            assertThat(otherDomain.latestEnd(bucket, 1L).get()).isEqualTo(new EpochEnd(1L, 1L));
        }
    }

    /**
     * The reads of a lookup function come from its caller and from the callbacks of earlier reads
     * at once: each must find the rows of its own bucket.
     */
    @Test
    void concurrentReadsFindTheRowsOfTheirOwnBuckets() throws Exception {
        long tableId = createScannedTable("registry_concurrent");
        int buckets = 16;
        try (Connection connection = ConnectionFactory.createConnection(clientConf);
                EpochRegistry registry =
                        EpochRegistry.open(connection, false, DEFAULT_DB, "concurrent")) {
            // bucket b started at offset b, changed in epoch 100 + b, and ended through 200 + b
            List<EpochRecord> records = new ArrayList<>();
            for (int b = 0; b < buckets; b++) {
                TableBucket bucket = new TableBucket(tableId, b);
                records.add(EpochRecord.end(bucket, Epochs.BEFORE_FIRST, b));
                records.add(EpochRecord.end(bucket, 100L + b, 1000L + b));
                records.add(EpochRecord.progress(bucket, 200L + b));
            }
            registry.write(records).get();

            ExecutorService threads = Executors.newFixedThreadPool(8);
            try {
                List<Future<List<String>>> mismatches = new ArrayList<>();
                for (int thread = 0; thread < 8; thread++) {
                    int seed = thread;
                    mismatches.add(
                            threads.submit(
                                    () -> readConcurrently(registry, tableId, buckets, seed)));
                }
                List<String> all = new ArrayList<>();
                for (Future<List<String>> mismatch : mismatches) {
                    all.addAll(mismatch.get(2, TimeUnit.MINUTES));
                }
                assertThat(all).isEmpty();
            } finally {
                threads.shutdownNow();
            }
        }
    }

    /** Issues many reads without waiting in between; returns the ones with a wrong result. */
    private static List<String> readConcurrently(
            EpochRegistry registry, long tableId, int buckets, int seed) throws Exception {
        Random random = new Random(seed);
        List<CompletableFuture<String>> reads = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            int b = random.nextInt(buckets);
            TableBucket bucket = new TableBucket(tableId, b);
            String expectedBefore = new EpochEnd(Epochs.BEFORE_FIRST, b).toString();
            String expectedChange = new EpochEnd(100L + b, 1000L + b).toString();
            reads.add(
                    registry.endedThrough(bucket)
                            .thenApply(ended -> check("progress " + b, 200L + b, ended)));
            reads.add(
                    registry.latestEnd(bucket, 99L)
                            .thenApply(end -> check("before " + b, expectedBefore, end)));
            reads.add(
                    registry.latestEnd(bucket, 150L)
                            .thenApply(end -> check("change " + b, expectedChange, end)));
        }
        List<String> mismatches = new ArrayList<>();
        for (CompletableFuture<String> read : reads) {
            String mismatch = read.get(1, TimeUnit.MINUTES);
            if (mismatch != null) {
                mismatches.add(mismatch);
            }
        }
        return mismatches;
    }

    private static String check(String read, Object expected, Object actual) {
        String value = actual == null ? null : actual.toString();
        return expected.toString().equals(value)
                ? null
                : read + ": expected " + expected + " but was " + value;
    }

    /**
     * The ends of a bucket are found once the server flushed them out of memory too, between the
     * rows of other buckets and domains.
     */
    @Test
    void endsAreFoundAfterTheRegistryFlushedThem() throws Exception {
        long tableId = createScannedTable("registry_flushed");
        String domain = "flushed-" + String.join("", Collections.nCopies(40, "x"));
        try (Connection connection = ConnectionFactory.createConnection(clientConf);
                EpochRegistry registry =
                        EpochRegistry.open(connection, false, DEFAULT_DB, domain)) {
            List<EpochRecord> records = new ArrayList<>();
            for (int b = 0; b < 4; b++) {
                TableBucket bucket = new TableBucket(tableId, b);
                records.add(EpochRecord.end(bucket, Epochs.BEFORE_FIRST, Epochs.EMPTY));
                // bucket 0 never changes; the others change in sparse epochs
                for (long epoch = 1000; b > 0 && epoch < 1300; epoch += 3 * b) {
                    records.add(EpochRecord.end(bucket, epoch, epoch * 10 + b));
                }
                records.add(EpochRecord.progress(bucket, 1400L));
            }
            registry.write(records).get();
            checkEnds(registry, tableId, "in memory", false);
            FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(EpochRegistry.tablePath(DEFAULT_DB));
            checkEnds(registry, tableId, "flushed", false);
            // more ends after the flush, in memory again, above the flushed ones
            List<EpochRecord> later = new ArrayList<>();
            for (int b = 0; b < 4; b++) {
                TableBucket bucket = new TableBucket(tableId, b);
                if (b > 0) {
                    later.add(EpochRecord.end(bucket, 1350L, 13500L + b));
                }
                later.add(EpochRecord.progress(bucket, 1500L));
            }
            registry.write(later).get();
            checkEnds(registry, tableId, "partly flushed", true);
        }
    }

    private static void checkEnds(EpochRegistry registry, long tableId, String phase, boolean later)
            throws Exception {
        for (int b = 0; b < 4; b++) {
            TableBucket bucket = new TableBucket(tableId, b);
            for (long epoch = -1; epoch < 1400; epoch += 7) {
                long expectedEpoch = Epochs.BEFORE_FIRST;
                for (long changed = 1000;
                        b > 0 && changed <= epoch && changed < 1300;
                        changed += 3 * b) {
                    expectedEpoch = changed;
                }
                EpochEnd expected =
                        expectedEpoch == Epochs.BEFORE_FIRST
                                ? new EpochEnd(Epochs.BEFORE_FIRST, Epochs.EMPTY)
                                : new EpochEnd(expectedEpoch, expectedEpoch * 10 + b);
                if (later && b > 0 && epoch >= 1350) {
                    expected = new EpochEnd(1350L, 13500L + b);
                }
                assertThat(registry.latestEnd(bucket, epoch).get())
                        .as("%s: latest end of %s at or before %s", phase, bucket, epoch)
                        .isEqualTo(expected);
            }
        }
    }

    private long createScannedTable(String name) throws Exception {
        return createTable(TablePath.of(DEFAULT_DB, name), DEFAULT_PK_TABLE_DESCRIPTOR);
    }
}
