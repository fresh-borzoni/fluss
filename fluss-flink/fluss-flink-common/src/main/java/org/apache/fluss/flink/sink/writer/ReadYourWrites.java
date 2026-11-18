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

package org.apache.fluss.flink.sink.writer;

import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.serializer.RowSerializer;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Lets the lookups of a task read what the sink writers of the same task wrote.
 *
 * <p>A sink writer and a lookup function that are chained into one task run in its thread. A writer
 * that promised read-your-writes registers its table in that thread. It keeps every row it wrote
 * until the write is acknowledged, since acknowledged writes are served by lookups. A lookup of the
 * table issued in that thread applies the rows that were not acknowledged when it was issued to the
 * rows it reads, so it never waits for the writer.
 *
 * <p>A writer that opens while the system property {@code fluss.read-your-writes.mode} is {@code
 * flush} keeps no rows: a lookup instead waits until the writer flushed every row it had received,
 * and then reads the table as it is. Flushes run in the background, one at a time per writer:
 * requests that arrive while a flush runs are served by the next flush.
 */
public final class ReadYourWrites {

    private static final ThreadLocal<Map<TablePath, List<Writer>>> WRITERS =
            ThreadLocal.withInitial(HashMap::new);

    private ReadYourWrites() {}

    /** Registers a writer of the table in the current thread; close it to unregister. */
    static Writer register(TablePath tablePath, RowType rowType, int[] primaryKey, Runnable flush) {
        Writer writer = new Writer(tablePath, rowType, primaryKey, flush);
        WRITERS.get().computeIfAbsent(tablePath, path -> new ArrayList<>()).add(writer);
        return writer;
    }

    /**
     * Prepares a lookup of the table issued in the current thread: what it has to wait for, and the
     * rows it has to apply to the rows it reads. Null if no writer of the table is registered in
     * this thread.
     *
     * <p>Lookups are by the primary key or a prefix of it, so a write cannot move a row to another
     * lookup key without changing its primary key, and the old row is deleted by a write of its
     * own, which the lookup of the old key applies.
     *
     * @param keyColumns the columns of the table that the lookup key holds, in its order
     * @param key the lookup key
     */
    public static @Nullable View view(TablePath tablePath, int[] keyColumns, InternalRow key) {
        List<Writer> writers = WRITERS.get().get(tablePath);
        if (writers == null || writers.isEmpty()) {
            return null;
        }
        Writer first = writers.get(0);
        if (first.fence != null) {
            CompletableFuture<?>[] flushes = new CompletableFuture<?>[writers.size()];
            for (int i = 0; i < flushes.length; i++) {
                flushes[i] = checkNotNull(writers.get(i).fence).request();
            }
            return new View(CompletableFuture.allOf(flushes), Collections.emptyList(), null);
        }
        GenericRow keyRow = first.key(key, keyColumns);
        List<Pending> pending = new ArrayList<>();
        for (Writer writer : writers) {
            pending.addAll(writer.pending(keyColumns, keyRow));
        }
        return new View(CompletableFuture.completedFuture(null), pending, first.primaryKeyGetters);
    }

    /** What a lookup has to wait for, and the rows it has to apply to the rows it reads. */
    public static final class View {

        private final CompletableFuture<Void> ready;
        private final List<Pending> pending;
        private final @Nullable InternalRow.FieldGetter[] primaryKeyGetters;

        private View(
                CompletableFuture<Void> ready,
                List<Pending> pending,
                @Nullable InternalRow.FieldGetter[] primaryKeyGetters) {
            this.ready = ready;
            this.pending = pending;
            this.primaryKeyGetters = primaryKeyGetters;
        }

        /** Completes once the lookup may read. */
        public CompletableFuture<Void> ready() {
            return ready;
        }

        /**
         * One page of the rows of a scan that opened together with this view: without the rows
         * written but not acknowledged when it opened, which the first page adds instead, beyond
         * the size of the page.
         */
        public List<InternalRow> applyToPage(List<InternalRow> rows, boolean first) {
            if (pending.isEmpty()) {
                return rows;
            }
            Set<GenericRow> written = new HashSet<>();
            List<InternalRow> result = new ArrayList<>(rows.size() + pending.size());
            for (Pending p : pending) {
                written.add(p.primaryKey);
                if (first && !p.delete) {
                    result.add(p.row);
                }
            }
            for (InternalRow row : rows) {
                if (row != null && !written.contains(copy(row, primaryKeyGetters))) {
                    result.add(row);
                }
            }
            return result;
        }

        /** The rows read, with the rows written but not acknowledged when the lookup was issued. */
        public List<InternalRow> apply(List<InternalRow> rows) {
            if (pending.isEmpty()) {
                return rows;
            }
            Set<GenericRow> written = new HashSet<>();
            List<InternalRow> result = new ArrayList<>(rows.size() + pending.size());
            for (Pending p : pending) {
                written.add(p.primaryKey);
                if (!p.delete) {
                    result.add(p.row);
                }
            }
            for (InternalRow row : rows) {
                if (row != null && !written.contains(copy(row, primaryKeyGetters))) {
                    result.add(row);
                }
            }
            return result;
        }
    }

    /** A row written and not acknowledged yet. */
    private static final class Pending {
        final GenericRow primaryKey;
        final GenericRow row;
        final boolean delete;

        Pending(GenericRow primaryKey, GenericRow row, boolean delete) {
            this.primaryKey = primaryKey;
            this.row = row;
            this.delete = delete;
        }
    }

    /** The rows of one writer that are not acknowledged yet. */
    static final class Writer implements AutoCloseable {

        private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

        private final TablePath tablePath;
        private final RowType rowType;
        private final int[] primaryKey;
        private final InternalRow.FieldGetter[] fieldGetters;
        private final InternalRow.FieldGetter[] primaryKeyGetters;

        /** Copies rows with nested values into memory of their own, null without nested types. */
        private final @Nullable RowSerializer nestedCopier;

        private final Map<TablePath, List<Writer>> registry;
        private final @Nullable Fence fence;

        // only touched in the task thread, except the queue of acknowledged rows
        private final Map<GenericRow, Pending> pending = new HashMap<>();
        private final Map<List<Integer>, Map<GenericRow, Set<GenericRow>>> indexes =
                new HashMap<>();
        private final Queue<Pending> acknowledged = new ConcurrentLinkedQueue<>();

        private Writer(TablePath tablePath, RowType rowType, int[] primaryKey, Runnable flush) {
            this.tablePath = tablePath;
            this.rowType = rowType;
            this.primaryKey = primaryKey;
            this.fieldGetters = new InternalRow.FieldGetter[rowType.getFieldCount()];
            for (int i = 0; i < fieldGetters.length; i++) {
                fieldGetters[i] = InternalRow.createFieldGetter(rowType.getTypeAt(i), i);
            }
            this.primaryKeyGetters = new InternalRow.FieldGetter[primaryKey.length];
            for (int i = 0; i < primaryKey.length; i++) {
                primaryKeyGetters[i] = fieldGetters[primaryKey[i]];
            }
            this.nestedCopier =
                    hasNestedType(rowType)
                            ? new RowSerializer(
                                    rowType.getChildren().toArray(new DataType[0]),
                                    BinaryRow.BinaryRowFormat.COMPACTED)
                            : null;
            this.registry = WRITERS.get();
            this.fence =
                    "flush".equals(System.getProperty("fluss.read-your-writes.mode"))
                            ? new Fence(tablePath, flush)
                            : null;
        }

        private static boolean hasNestedType(RowType rowType) {
            for (DataType type : rowType.getChildren()) {
                DataTypeRoot root = type.getTypeRoot();
                if (root == DataTypeRoot.ARRAY
                        || root == DataTypeRoot.MAP
                        || root == DataTypeRoot.ROW) {
                    return true;
                }
            }
            return false;
        }

        /** Keeps a row the writer wrote until its write is acknowledged. */
        void written(InternalRow row, boolean delete, CompletableFuture<?> acknowledgement) {
            if (fence != null) {
                return;
            }
            drainAcknowledged();
            // the row may share memory with a record that the operator before the sink reuses:
            // nested values are only views into it, so they are serialized into a row of its own
            InternalRow owned = nestedCopier == null ? row : nestedCopier.toBinaryRow(row).copy();
            GenericRow copy = copy(owned, fieldGetters);
            Pending next = new Pending(copy(copy, primaryKeyGetters), copy, delete);
            if (pending.put(next.primaryKey, next) == null) {
                for (Map.Entry<List<Integer>, Map<GenericRow, Set<GenericRow>>> index :
                        indexes.entrySet()) {
                    add(index.getKey(), index.getValue(), next);
                }
            }
            acknowledgement.whenComplete(
                    (ignored, throwable) -> {
                        // a failed write fails the writer, so the row can stay
                        if (throwable == null) {
                            acknowledged.add(next);
                        }
                    });
        }

        /** The rows not acknowledged yet whose key columns hold the key. */
        List<Pending> pending(int[] keyColumns, GenericRow key) {
            drainAcknowledged();
            if (pending.isEmpty()) {
                return Collections.emptyList();
            }
            if (Arrays.equals(keyColumns, primaryKey)) {
                Pending p = pending.get(key);
                return p == null ? Collections.emptyList() : Collections.singletonList(p);
            }
            List<Integer> columns = new ArrayList<>(keyColumns.length);
            for (int column : keyColumns) {
                columns.add(column);
            }
            Map<GenericRow, Set<GenericRow>> index = indexes.get(columns);
            if (index == null) {
                index = new HashMap<>();
                for (Pending p : pending.values()) {
                    add(columns, index, p);
                }
                indexes.put(columns, index);
            }
            Set<GenericRow> primaryKeys = index.get(key);
            if (primaryKeys == null) {
                return Collections.emptyList();
            }
            List<Pending> result = new ArrayList<>(primaryKeys.size());
            for (GenericRow pk : primaryKeys) {
                result.add(pending.get(pk));
            }
            return result;
        }

        /** A copy of a lookup key whose fields are the given columns of the table. */
        GenericRow key(InternalRow key, int[] keyColumns) {
            GenericRow copy = new GenericRow(keyColumns.length);
            for (int i = 0; i < keyColumns.length; i++) {
                copy.setField(
                        i,
                        InternalRow.createFieldGetter(rowType.getTypeAt(keyColumns[i]), i)
                                .getFieldOrNull(key));
            }
            return copy;
        }

        private void drainAcknowledged() {
            Pending p;
            while ((p = acknowledged.poll()) != null) {
                // otherwise a later write of the same key replaced it
                if (pending.get(p.primaryKey) == p) {
                    pending.remove(p.primaryKey);
                    for (Map.Entry<List<Integer>, Map<GenericRow, Set<GenericRow>>> index :
                            indexes.entrySet()) {
                        GenericRow key = project(p.row, index.getKey());
                        Set<GenericRow> primaryKeys = index.getValue().get(key);
                        if (primaryKeys != null) {
                            primaryKeys.remove(p.primaryKey);
                            if (primaryKeys.isEmpty()) {
                                index.getValue().remove(key);
                            }
                        }
                    }
                }
            }
        }

        private void add(List<Integer> columns, Map<GenericRow, Set<GenericRow>> index, Pending p) {
            index.computeIfAbsent(project(p.row, columns), k -> new HashSet<>()).add(p.primaryKey);
        }

        private GenericRow project(GenericRow row, List<Integer> columns) {
            GenericRow projected = new GenericRow(columns.size());
            for (int i = 0; i < columns.size(); i++) {
                projected.setField(i, row.getField(columns.get(i)));
            }
            return projected;
        }

        @Override
        public void close() {
            List<Writer> writers = registry.get(tablePath);
            if (writers != null) {
                writers.remove(this);
                if (writers.isEmpty()) {
                    registry.remove(tablePath);
                }
            }
            if (fence != null) {
                fence.close();
            }
            LOG.debug("Unregistered the read-your-writes writer of table {}.", tablePath);
        }
    }

    private static GenericRow copy(InternalRow row, InternalRow.FieldGetter[] getters) {
        GenericRow copy = new GenericRow(getters.length);
        for (int i = 0; i < getters.length; i++) {
            Object value = getters[i].getFieldOrNull(row);
            // a byte array may be the array of the record itself
            copy.setField(i, value instanceof byte[] ? ((byte[]) value).clone() : value);
        }
        return copy;
    }

    /** The flushes of one writer, in the flush mode. */
    private static final class Fence implements AutoCloseable {

        private final Runnable flush;
        private final ExecutorService executor;

        private @Nullable CompletableFuture<Void> running;
        private @Nullable CompletableFuture<Void> next;

        private Fence(TablePath tablePath, Runnable flush) {
            this.flush = flush;
            this.executor =
                    Executors.newSingleThreadExecutor(
                            runnable -> {
                                Thread thread =
                                        new Thread(runnable, "fluss-read-your-writes-" + tablePath);
                                thread.setDaemon(true);
                                return thread;
                            });
        }

        synchronized CompletableFuture<Void> request() {
            if (running == null) {
                running = new CompletableFuture<>();
                start(running);
                return running;
            }
            if (next == null) {
                next = new CompletableFuture<>();
            }
            return next;
        }

        private void start(CompletableFuture<Void> result) {
            executor.execute(
                    () -> {
                        try {
                            flush.run();
                            result.complete(null);
                        } catch (Throwable t) {
                            result.completeExceptionally(t);
                        }
                        synchronized (this) {
                            running = next;
                            next = null;
                            if (running != null) {
                                start(running);
                            }
                        }
                    });
        }

        @Override
        public void close() {
            executor.shutdownNow();
        }
    }
}
