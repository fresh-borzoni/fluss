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
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.ReadDirection;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.Range;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * An {@link EpochStore} in a primary-key table of Fluss, so that the readers and lookups of all
 * tasks agree on the epochs. The table is {@value #TABLE_NAME} in the database of the scanned
 * table; the first reader or lookup that needs it creates it.
 *
 * <p>Its rows, keyed by (domain, table id, partition id, bucket, epoch):
 *
 * <ul>
 *   <li>the end of an epoch in which the bucket changed: the next offset of the bucket after the
 *       last change of the epoch in {@code end_offset}. Epoch {@link Epochs#BEFORE_FIRST} holds the
 *       position the scan started from;
 *   <li>at epoch {@link #PROGRESS_EPOCH}, how far a reader ended epochs in the bucket, in {@code
 *       ended_through}.
 * </ul>
 *
 * <p>The keys are ordered, so the end of the latest epoch at or before another one is the first row
 * of a reverse range lookup.
 */
public final class EpochRegistry implements EpochStore {

    private static final Logger LOG = LoggerFactory.getLogger(EpochRegistry.class);

    /** The table of the epochs in the database of the scanned table. */
    public static final String TABLE_NAME = "_flink_epochs";

    /** The epoch of the row that records the progress of a bucket, after every other epoch. */
    static final long PROGRESS_EPOCH = Long.MAX_VALUE;

    /** The partition id of the buckets of a table without partitions. */
    static final long NO_PARTITION = -1L;

    private static final int NUM_BUCKETS = 4;
    private static final int CLEAR_BATCH = 1024;
    private static final long READY_TIMEOUT_MS = 60_000L;

    private static final int DOMAIN = 0;
    private static final int TABLE_ID = 1;
    private static final int PARTITION_ID = 2;
    private static final int BUCKET = 3;
    private static final int EPOCH = 4;
    private static final int END_OFFSET = 5;
    private static final int ENDED_THROUGH = 6;

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("domain", DataTypes.STRING())
                    .column("table_id", DataTypes.BIGINT())
                    .column("partition_id", DataTypes.BIGINT())
                    .column("bucket", DataTypes.INT())
                    .column("epoch", DataTypes.BIGINT())
                    .column("end_offset", DataTypes.BIGINT())
                    .column("ended_through", DataTypes.BIGINT())
                    .primaryKey("domain", "table_id", "partition_id", "bucket", "epoch")
                    .build();

    private final Connection connection;
    private final boolean ownsConnection;
    private final Table table;
    private final BinaryString domain;

    /** Looks up rows by their whole key. */
    private final Lookuper points;

    /** Looks up the rows of a bucket by a range of epochs, latest first. */
    private final Lookuper latest;

    /**
     * Serializes the lookupers, which reuse their key buffers: a lookup function calls them from
     * its caller and from callbacks of earlier reads at once.
     */
    private final Object lookupLock = new Object();

    private final Object writerLock = new Object();
    private @Nullable UpsertWriter writer;
    private @Nullable ExecutorService writes;

    private EpochRegistry(
            Connection connection, boolean ownsConnection, Table table, String domain) {
        this.connection = connection;
        this.ownsConnection = ownsConnection;
        this.table = table;
        this.domain = BinaryString.fromString(domain);
        this.points = table.newLookup().createLookuper();
        this.latest =
                table.newLookup()
                        .lookupBy("domain", "table_id", "partition_id", "bucket")
                        .rangeBy("epoch")
                        .direction(ReadDirection.REVERSE)
                        .createLookuper();
    }

    /** The path of the table of the epochs of the tables of a database. */
    public static TablePath tablePath(String database) {
        return TablePath.of(database, TABLE_NAME);
    }

    /** The descriptor that the table of the epochs is created with. */
    public static TableDescriptor tableDescriptor() {
        return TableDescriptor.builder()
                .schema(SCHEMA)
                .distributedBy(NUM_BUCKETS, "domain", "table_id")
                .property(ConfigOptions.TABLE_KV_ORDERED_KEY_ENABLED, true)
                .comment("The epochs that Flink scans of the tables of this database ended.")
                .build();
    }

    /**
     * Opens the epochs of the domain in the database, and creates their table if it does not exist
     * yet.
     *
     * @param ownsConnection whether closing the registry closes the connection
     */
    public static EpochRegistry open(
            Connection connection, boolean ownsConnection, String database, String domain)
            throws Exception {
        TablePath path = tablePath(database);
        Admin admin = connection.getAdmin();
        if (!admin.tableExists(path).get()) {
            try {
                admin.createTable(path, tableDescriptor(), true).get();
                LOG.info("Created the table {} of the epochs of Flink scans.", path);
            } catch (ExecutionException e) {
                if (!ExceptionUtils.findThrowable(e, TableAlreadyExistException.class)
                        .isPresent()) {
                    throw e;
                }
            }
        }
        EpochRegistry registry =
                new EpochRegistry(connection, ownsConnection, connection.getTable(path), domain);
        registry.awaitReady();
        return registry;
    }

    /**
     * Removes every epoch of the scanned table in the domain, for a scan that starts afresh; the
     * epochs of an earlier run would otherwise be read as its own.
     */
    public static void clear(Configuration flussConf, TablePath scanned, String domain)
            throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(flussConf)) {
            long tableId = connection.getAdmin().getTableInfo(scanned).get().getTableId();
            try (EpochRegistry registry =
                    open(connection, false, scanned.getDatabaseName(), domain)) {
                int removed = registry.clear(tableId);
                LOG.info(
                        "Removed {} epoch rows of table {} in domain '{}' before a fresh scan.",
                        removed,
                        scanned,
                        domain);
            }
        }
    }

    /** Waits until the buckets of a table created just now serve lookups. */
    private void awaitReady() throws Exception {
        long deadline = System.currentTimeMillis() + READY_TIMEOUT_MS;
        while (true) {
            try {
                endedThrough(new TableBucket(-1L, 0)).get(10, TimeUnit.SECONDS);
                return;
            } catch (Exception e) {
                if (System.currentTimeMillis() > deadline) {
                    throw new IllegalStateException(
                            "The table of the epochs "
                                    + table.getTableInfo().getTablePath()
                                    + " does not serve lookups.",
                            e);
                }
                Thread.sleep(100);
            }
        }
    }

    @Override
    public CompletableFuture<Long> endedThrough(TableBucket bucket) {
        CompletableFuture<LookupResult> lookup;
        synchronized (lookupLock) {
            lookup = points.lookup(key(bucket, PROGRESS_EPOCH));
        }
        return lookup.thenApply(
                result -> {
                    InternalRow row = result.getSingletonRow();
                    return row == null || row.isNullAt(ENDED_THROUGH)
                            ? null
                            : row.getLong(ENDED_THROUGH);
                });
    }

    @Override
    public CompletableFuture<List<EpochEnd>> latestEnds(TableBucket bucket, long epoch, int limit) {
        // the progress row is after every epoch, so the bound excludes it
        long bound = Math.min(epoch, PROGRESS_EPOCH - 1);
        CompletableFuture<LookupResult> lookup;
        synchronized (lookupLock) {
            lookup = latest.lookupFirst(prefix(bucket), Range.atMost(GenericRow.of(bound)), limit);
        }
        return lookup.thenApply(
                result -> {
                    List<EpochEnd> ends = new ArrayList<>();
                    for (InternalRow row : result.getRowList()) {
                        if (!row.isNullAt(END_OFFSET)) {
                            ends.add(new EpochEnd(row.getLong(EPOCH), row.getLong(END_OFFSET)));
                        }
                    }
                    return ends;
                });
    }

    @Override
    public CompletableFuture<Void> write(List<EpochRecord> records) {
        if (records.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        List<InternalRow> rows = new ArrayList<>(records.size());
        for (EpochRecord record : records) {
            rows.add(toRow(record));
        }
        return CompletableFuture.runAsync(
                () -> {
                    List<CompletableFuture<?>> acks = new ArrayList<>(rows.size());
                    synchronized (writerLock) {
                        UpsertWriter upserts = writer();
                        for (InternalRow row : rows) {
                            acks.add(upserts.upsert(row));
                        }
                        upserts.flush();
                    }
                    CompletableFuture.allOf(acks.toArray(new CompletableFuture<?>[0])).join();
                },
                writes());
    }

    /** Removes every row of the table in the domain; returns how many. */
    public int clear(long tableId) throws Exception {
        Lookuper rowsOfTable = table.newLookup().lookupBy("domain", "table_id").createLookuper();
        UpsertWriter deletes = writer();
        int removed = 0;
        while (true) {
            CompletableFuture<LookupResult> lookup;
            synchronized (lookupLock) {
                lookup = rowsOfTable.lookupFirst(GenericRow.of(domain, tableId), CLEAR_BATCH);
            }
            List<InternalRow> rows = lookup.get().getRowList();
            if (rows.isEmpty()) {
                return removed;
            }
            List<CompletableFuture<?>> acks = new ArrayList<>(rows.size());
            synchronized (writerLock) {
                for (InternalRow row : rows) {
                    acks.add(deletes.delete(row));
                }
                deletes.flush();
            }
            CompletableFuture.allOf(acks.toArray(new CompletableFuture<?>[0])).get();
            removed += rows.size();
        }
    }

    private GenericRow toRow(EpochRecord record) {
        TableBucket bucket = record.getBucket();
        GenericRow row = new GenericRow(7);
        row.setField(DOMAIN, domain);
        row.setField(TABLE_ID, bucket.getTableId());
        row.setField(PARTITION_ID, partitionOf(bucket));
        row.setField(BUCKET, bucket.getBucket());
        if (record.isProgress()) {
            row.setField(EPOCH, PROGRESS_EPOCH);
            row.setField(ENDED_THROUGH, record.getEpoch());
        } else {
            row.setField(EPOCH, record.getEpoch());
            row.setField(END_OFFSET, record.getEnd());
        }
        return row;
    }

    private GenericRow key(TableBucket bucket, long epoch) {
        return GenericRow.of(
                domain, bucket.getTableId(), partitionOf(bucket), bucket.getBucket(), epoch);
    }

    private GenericRow prefix(TableBucket bucket) {
        return GenericRow.of(domain, bucket.getTableId(), partitionOf(bucket), bucket.getBucket());
    }

    private static long partitionOf(TableBucket bucket) {
        Long partitionId = bucket.getPartitionId();
        return partitionId == null ? NO_PARTITION : partitionId;
    }

    private UpsertWriter writer() {
        synchronized (writerLock) {
            if (writer == null) {
                writer = table.newUpsert().createWriter();
            }
            return writer;
        }
    }

    private ExecutorService writes() {
        synchronized (writerLock) {
            if (writes == null) {
                writes =
                        Executors.newSingleThreadExecutor(
                                r -> {
                                    Thread thread =
                                            new Thread(
                                                    r,
                                                    "fluss-epochs-"
                                                            + table.getTableInfo().getTablePath());
                                    thread.setDaemon(true);
                                    return thread;
                                });
            }
            return writes;
        }
    }

    @Override
    public void close() throws Exception {
        synchronized (writerLock) {
            if (writes != null) {
                writes.shutdownNow();
            }
        }
        table.close();
        if (ownsConnection) {
            connection.close();
        }
    }
}
