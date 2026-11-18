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

package org.apache.fluss.flink.source.lookup;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.Lookup;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.LookupType;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.flink.sink.writer.ReadYourWrites;
import org.apache.fluss.flink.source.lookup.LookupNormalizer.RemainingFilter;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.flink.utils.FlinkUtils;
import org.apache.fluss.flink.utils.FlussRowToFlinkRowConverter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.ExceptionUtils;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

/** A flink async lookup function for fluss. */
public class FlinkAsyncLookupFunction extends AsyncLookupFunction {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkAsyncLookupFunction.class);

    private static final long serialVersionUID = 1L;

    /** The most scans that the pages of one lookup function keep open at once. */
    private static final int MAX_OPEN_SCANS = 2;

    /** How long a page waits for a batch of a scan before it asks again. */
    private static final Duration PAGE_POLL_TIMEOUT = Duration.ofMillis(100);

    private final Configuration flussConfig;
    private final TablePath tablePath;
    private final RowType flinkRowType;
    private final LookupNormalizer lookupNormalizer;
    @Nullable private int[] projection;
    private final boolean insertIfNotExists;
    private final boolean requiresWriter;

    private transient FlussRowToFlinkRowConverter flussRowToFlinkRowConverter;
    private transient Connection connection;
    private transient Table table;
    private transient Lookuper lookuper;
    private transient FlinkAsFlussRow lookupRow;

    /** The columns of a prefix lookup, by which pages scan the table; null for other lookups. */
    private transient @Nullable List<String> lookupColumnNames;

    /** Reads pages, whose scanners block; created when the first pages open. */
    private transient @Nullable ExecutorService pageReader;

    /** Copies the key rows of prefix lookups, for pages that outlive the call. */
    private transient @Nullable RowData.FieldGetter[] lookupKeyGetters;

    private transient @Nullable ScanPermits scanPermits;

    /** The pages that asked for a scan and are not closed yet, closed with this function. */
    private transient @Nullable Set<ScanPages> openScans;

    public FlinkAsyncLookupFunction(
            Configuration flussConfig,
            TablePath tablePath,
            RowType flinkRowType,
            LookupNormalizer lookupNormalizer,
            @Nullable int[] projection,
            boolean insertIfNotExists) {
        this(
                flussConfig,
                tablePath,
                flinkRowType,
                lookupNormalizer,
                projection,
                insertIfNotExists,
                false);
    }

    /**
     * @param requiresWriter whether the lookups must read what a sink writer of the same task wrote
     *     and fail if there is none, see {@link ReadYourWrites}
     */
    public FlinkAsyncLookupFunction(
            Configuration flussConfig,
            TablePath tablePath,
            RowType flinkRowType,
            LookupNormalizer lookupNormalizer,
            @Nullable int[] projection,
            boolean insertIfNotExists,
            boolean requiresWriter) {
        this.requiresWriter = requiresWriter;
        this.flussConfig = flussConfig;
        this.tablePath = tablePath;
        this.flinkRowType = flinkRowType;
        this.lookupNormalizer = lookupNormalizer;
        this.projection = projection;
        this.insertIfNotExists = insertIfNotExists;
    }

    @Override
    public void open(FunctionContext context) {
        LOG.info("start open ...");
        connection = ConnectionFactory.createConnection(flussConfig);
        table = connection.getTable(tablePath);
        lookupRow = new FlinkAsFlussRow();

        final RowType outputRowType;
        if (projection == null) {
            outputRowType = flinkRowType;
            // we force to do projection if no projection pushdown, in order to handle schema
            // changes (ADD COLUMN LAST), this guarantees the input row of
            // flussRowToFlinkRowConverter is in expected schema even new columns are added.
            projection = IntStream.range(0, flinkRowType.getFieldCount()).toArray();
        } else {
            outputRowType = FlinkUtils.projectRowType(flinkRowType, projection);
        }
        // TODO: currently, we assume only ADD COLUMN LAST schema changes, so the projection
        //  positions can still work even after such changes.
        flussRowToFlinkRowConverter =
                new FlussRowToFlinkRowConverter(FlinkConversions.toFlussRowType(outputRowType));

        Lookup lookup = table.newLookup();
        if (lookupNormalizer.getLookupType() == LookupType.PREFIX_LOOKUP) {
            int[] lookupKeyIndexes = lookupNormalizer.getLookupKeyIndexes();
            RowType lookupKeyRowType = FlinkUtils.projectRowType(flinkRowType, lookupKeyIndexes);
            lookup = lookup.lookupBy(lookupKeyRowType.getFieldNames());
            lookupColumnNames = lookupKeyRowType.getFieldNames();
            lookupKeyGetters = new RowData.FieldGetter[lookupKeyRowType.getFieldCount()];
            for (int i = 0; i < lookupKeyGetters.length; i++) {
                lookupKeyGetters[i] = RowData.createFieldGetter(lookupKeyRowType.getTypeAt(i), i);
            }
            scanPermits = new ScanPermits(MAX_OPEN_SCANS);
            openScans = ConcurrentHashMap.newKeySet();
        } else if (insertIfNotExists) {
            lookup = lookup.enableInsertIfNotExists();
        }
        lookuper = lookup.createLookuper();

        LOG.info("end open.");
    }

    /**
     * The invoke entry point of lookup function.
     *
     * @param keyRow A {@link RowData} that wraps lookup keys.
     */
    @Override
    public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
        RowData normalizedKeyRow = lookupNormalizer.normalizeLookupKey(keyRow);
        RemainingFilter remainingFilter = lookupNormalizer.createRemainingFilter(keyRow);
        CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();

        // a sink writer of this task that writes the table: read what it wrote too
        InternalRow flussKeyRow = lookupRow.replace(normalizedKeyRow);
        ReadYourWrites.View view =
                ReadYourWrites.view(tablePath, lookupNormalizer.getLookupKeyIndexes(), flussKeyRow);
        if (view == null && requiresWriter) {
            future.completeExceptionally(
                    new IllegalStateException(
                            "Lookups of Fluss table '"
                                    + tablePath
                                    + "' must read what the sink writer of the same task wrote,"
                                    + " but no writer of the table runs in this task."));
        } else if (view == null
                || view.ready().isDone() && !view.ready().isCompletedExceptionally()) {
            lookup(flussKeyRow, view, remainingFilter, future);
        } else {
            InternalRow deferredKeyRow = new FlinkAsFlussRow().replace(normalizedKeyRow);
            view.ready()
                    .whenComplete(
                            (ignored, throwable) -> {
                                if (throwable != null) {
                                    future.completeExceptionally(
                                            new RuntimeException(
                                                    "Fluss writer of table '"
                                                            + tablePath
                                                            + "' failed to flush before a lookup.",
                                                    throwable));
                                } else {
                                    lookup(deferredKeyRow, view, remainingFilter, future);
                                }
                            });
        }
        return future;
    }

    private void lookup(
            InternalRow flussKeyRow,
            @Nullable ReadYourWrites.View view,
            @Nullable RemainingFilter remainingFilter,
            CompletableFuture<Collection<RowData>> future) {
        // the retry mechanism is now handled by the underlying LookupClient layer; the lookuper is
        // not thread-safe, and lookups that waited for a writer call it from another thread
        CompletableFuture<LookupResult> lookup;
        synchronized (this) {
            lookup = lookuper.lookup(flussKeyRow);
        }
        lookup.whenComplete(
                (result, throwable) -> {
                    if (throwable != null) {
                        if (ExceptionUtils.findThrowable(throwable, TableNotExistException.class)
                                .isPresent()) {
                            LOG.error("Table '{}' not found ", tablePath, throwable);
                            future.completeExceptionally(
                                    new RuntimeException(
                                            "Fluss table '" + tablePath + "' not found.",
                                            throwable));
                        } else {
                            LOG.error("Fluss asyncLookup error", throwable);
                            future.completeExceptionally(
                                    new RuntimeException(
                                            "Execution of Fluss asyncLookup failed: "
                                                    + throwable.getMessage(),
                                            throwable));
                        }
                    } else {
                        List<InternalRow> rows = result.getRowList();
                        handleLookupSuccess(
                                future, view == null ? rows : view.apply(rows), remainingFilter);
                    }
                });
    }

    private void handleLookupSuccess(
            CompletableFuture<Collection<RowData>> resultFuture,
            List<InternalRow> lookupResult,
            @Nullable RemainingFilter remainingFilter) {
        if (lookupResult.isEmpty()) {
            resultFuture.complete(Collections.emptyList());
            return;
        }

        List<RowData> projectedRow = new ArrayList<>();
        for (InternalRow row : lookupResult) {
            if (row != null) {
                RowData flinkRow = flussRowToFlinkRowConverter.toFlinkRowData(maybeProject(row));
                if (remainingFilter == null || remainingFilter.isMatch(flinkRow)) {
                    projectedRow.add(flinkRow);
                }
            }
        }
        resultFuture.complete(projectedRow);
    }

    private InternalRow maybeProject(InternalRow row) {
        if (projection == null) {
            return row;
        }
        // should not reuse objects for async operations
        return ProjectedRow.from(projection).replaceRow(row);
    }

    /**
     * Opens the pages of the rows that {@link #asyncLookup} would return at once, with the rows a
     * sink writer of this task has not acknowledged yet in their place. A prefix lookup reads the
     * first page with a lookup and the rest with a scan opened later, after the last key of the
     * first page: each primary key comes at most once, and each row that did not change meanwhile
     * exactly once. Called in the thread that issues the lookups.
     */
    public LookupPages openPages(RowData keyRow, int pageSize) {
        if (lookupColumnNames == null) {
            CompletableFuture<Collection<RowData>> rows = asyncLookup(keyRow);
            return new LookupPages() {
                @Override
                public CompletableFuture<Page> next() {
                    return rows.thenApply(found -> new Page(new ArrayList<>(found), true));
                }

                @Override
                public void close() {}
            };
        }
        RowData normalizedKeyRow = lookupNormalizer.normalizeLookupKey(keyRow);
        RemainingFilter remainingFilter = lookupNormalizer.createRemainingFilter(keyRow);
        InternalRow flussKeyRow = new FlinkAsFlussRow().replace(normalizedKeyRow);
        ReadYourWrites.View view =
                ReadYourWrites.view(tablePath, lookupNormalizer.getLookupKeyIndexes(), flussKeyRow);
        if (view == null && requiresWriter) {
            throw new IllegalStateException(
                    "Lookups of Fluss table '"
                            + tablePath
                            + "' must read what the sink writer of the same task wrote,"
                            + " but no writer of the table runs in this task.");
        }
        // the key row of the lookup is reused: the pages keep their own
        return new ScanPages(
                new FlinkAsFlussRow().replace(copyKey(normalizedKeyRow)),
                view,
                remainingFilter,
                pageSize);
    }

    /** A copy of a key row, for a lookup that outlives the call. */
    private RowData copyKey(RowData keyRow) {
        GenericRowData copy = new GenericRowData(lookupKeyGetters.length);
        for (int i = 0; i < lookupKeyGetters.length; i++) {
            Object value = lookupKeyGetters[i].getFieldOrNull(keyRow);
            if (value instanceof StringData) {
                value = StringData.fromString(value.toString());
            } else if (value instanceof byte[]) {
                value = ((byte[]) value).clone();
            }
            copy.setField(i, value);
        }
        return copy;
    }

    private synchronized ExecutorService pageReader() {
        if (pageReader == null) {
            pageReader =
                    Executors.newCachedThreadPool(
                            r -> {
                                Thread thread = new Thread(r, "fluss-lookup-pages-" + tablePath);
                                thread.setDaemon(true);
                                return thread;
                            });
        }
        return pageReader;
    }

    /** The pages of the rows of one lookup key. */
    public interface LookupPages extends AutoCloseable {

        /** The next page. Called again only after the previous page completed, and not last. */
        CompletableFuture<Page> next();

        @Override
        void close();
    }

    /**
     * At most the page size of rows, and whether no rows follow. The first page of a prefix lookup
     * also carries the rows of the key that a sink writer of this task has not acknowledged yet,
     * beyond the page size.
     */
    public static final class Page {
        public final List<RowData> rows;
        public final boolean last;

        Page(List<RowData> rows, boolean last) {
            this.rows = rows;
            this.last = last;
        }
    }

    /**
     * Pages of the rows with one primary key prefix. The first page is a prefix lookup of one row
     * more than a page, batched with other lookups; only if that finds more rows, a scan of the
     * prefix reads the rest, skipping the keys up to the last one of the first page.
     */
    private final class ScanPages implements LookupPages {

        private final InternalRow keyRow;
        private final @Nullable ReadYourWrites.View view;
        private final @Nullable RemainingFilter remainingFilter;
        private final int pageSize;

        /** Rows scanned but not handed over yet: a scan returns batches of bytes. */
        private final ArrayDeque<InternalRow> buffer = new ArrayDeque<>();

        /** Encodes primary keys as the table stores them; not shared, as encoders keep state. */
        private @Nullable KeyEncoder keyEncoder;

        /** The primary key of the last row of the first page, once the scan is needed. */
        private @Nullable byte[] firstPageEnd;

        /** The permit of the scan, asked for with the second page. */
        private @Nullable CompletableFuture<Void> permit;

        private @Nullable BatchScanner scanner;
        private boolean firstPage = true;
        private boolean exhausted;

        /** Whether a page is being read, which then closes the scan if the pages closed. */
        private boolean reading;

        private volatile boolean closed;

        private ScanPages(
                InternalRow keyRow,
                @Nullable ReadYourWrites.View view,
                @Nullable RemainingFilter remainingFilter,
                int pageSize) {
            this.keyRow = keyRow;
            this.view = view;
            this.remainingFilter = remainingFilter;
            this.pageSize = pageSize;
        }

        @Override
        public CompletableFuture<Page> next() {
            if (firstPage) {
                firstPage = false;
                // in the flush mode, the writer of the task flushes first
                CompletableFuture<Void> ready =
                        view == null ? CompletableFuture.completedFuture(null) : view.ready();
                return ready.thenCompose(ignored -> lookupFirst())
                        .thenApply(result -> firstPage(result.getRowList()));
            }
            if (permit == null) {
                permit = scanPermits.acquire();
                openScans.add(this);
            }
            return permit.thenApplyAsync(ignored -> scanPage(), pageReader());
        }

        private CompletableFuture<LookupResult> lookupFirst() {
            synchronized (FlinkAsyncLookupFunction.this) {
                return lookuper.lookupFirst(keyRow, pageSize + 1);
            }
        }

        private Page firstPage(List<InternalRow> rows) {
            if (rows.size() <= pageSize) {
                close();
                return toPage(rows, true, true);
            }
            List<InternalRow> page = new ArrayList<>(rows.subList(0, pageSize));
            keyEncoder = newPrimaryKeyEncoder();
            firstPageEnd = keyEncoder.encodeKey(page.get(pageSize - 1));
            return toPage(page, true, false);
        }

        private Page scanPage() {
            synchronized (this) {
                if (closed) {
                    throw new CancellationException("The pages are closed.");
                }
                reading = true;
            }
            try {
                if (scanner == null) {
                    scanner =
                            table.newScan().equalTo(lookupColumnNames, keyRow).createBatchScanner();
                }
                while (buffer.size() < pageSize && !exhausted && !closed) {
                    CloseableIterator<InternalRow> batch = scanner.pollBatch(PAGE_POLL_TIMEOUT);
                    if (batch == null) {
                        exhausted = true;
                        break;
                    }
                    try {
                        while (batch.hasNext()) {
                            InternalRow row = batch.next();
                            // the rows come in key order: skip those of the first page
                            if (firstPageEnd != null) {
                                if (compare(keyEncoder.encodeKey(row), firstPageEnd) <= 0) {
                                    continue;
                                }
                                firstPageEnd = null;
                            }
                            buffer.add(row);
                        }
                    } finally {
                        batch.close();
                    }
                }
                List<InternalRow> rows = new ArrayList<>(Math.min(pageSize, buffer.size()));
                while (rows.size() < pageSize && !buffer.isEmpty()) {
                    rows.add(buffer.poll());
                }
                boolean last = exhausted && buffer.isEmpty();
                if (last) {
                    close();
                }
                return toPage(rows, false, last);
            } catch (IOException e) {
                close();
                throw new UncheckedIOException("Could not scan table " + tablePath, e);
            } finally {
                boolean closeScan;
                synchronized (this) {
                    reading = false;
                    closeScan = closed;
                }
                if (closeScan) {
                    closeScan();
                }
            }
        }

        private Page toPage(List<InternalRow> rows, boolean first, boolean last) {
            List<InternalRow> applied = view == null ? rows : view.applyToPage(rows, first);
            List<RowData> page = new ArrayList<>(applied.size());
            for (InternalRow row : applied) {
                RowData flinkRow = flussRowToFlinkRowConverter.toFlinkRowData(maybeProject(row));
                if (remainingFilter == null || remainingFilter.isMatch(flinkRow)) {
                    page.add(flinkRow);
                }
            }
            return new Page(page, last);
        }

        @Override
        public void close() {
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
                if (reading) {
                    return;
                }
            }
            closeScan();
        }

        /** Closes the scan and returns its permit, once the pages closed and no page is read. */
        private void closeScan() {
            openScans.remove(this);
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (IOException e) {
                    LOG.warn("Could not close a scan of table {}", tablePath, e);
                }
            }
            if (permit != null) {
                scanPermits.release(permit);
            }
        }
    }

    /** Encodes primary keys as the table stores them. */
    private KeyEncoder newPrimaryKeyEncoder() {
        TableInfo tableInfo = table.getTableInfo();
        return KeyEncoder.ofPrimaryKeyEncoder(
                tableInfo.getRowType(),
                tableInfo.getPhysicalPrimaryKeys(),
                tableInfo.getTableConfig(),
                tableInfo.isDefaultBucketKey());
    }

    /** Compares keys as the table orders them: unsigned bytes. */
    private static int compare(byte[] a, byte[] b) {
        int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            int c = (a[i] & 0xff) - (b[i] & 0xff);
            if (c != 0) {
                return c;
            }
        }
        return a.length - b.length;
    }

    /**
     * Bounds the scans of one lookup function that are open at once: a tablet server allows few
     * scans per bucket. Further pages wait for a scan to close.
     */
    private static final class ScanPermits {
        private int available;
        private final ArrayDeque<CompletableFuture<Void>> waiting = new ArrayDeque<>();

        private ScanPermits(int permits) {
            this.available = permits;
        }

        synchronized CompletableFuture<Void> acquire() {
            if (available > 0) {
                available--;
                return CompletableFuture.completedFuture(null);
            }
            CompletableFuture<Void> waiter = new CompletableFuture<>();
            waiting.add(waiter);
            return waiter;
        }

        /** Returns a permit, or withdraws the request for it if it was not granted yet. */
        void release(CompletableFuture<Void> permit) {
            boolean granted;
            CompletableFuture<Void> next = null;
            synchronized (this) {
                granted = !waiting.remove(permit);
                if (granted) {
                    next = waiting.poll();
                    if (next == null) {
                        available++;
                    }
                }
            }
            if (!granted) {
                permit.cancel(false);
            } else if (next != null) {
                next.complete(null);
            }
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("start close ...");
        if (openScans != null) {
            for (ScanPages pages : openScans) {
                pages.close();
            }
        }
        if (pageReader != null) {
            pageReader.shutdownNow();
        }
        if (table != null) {
            table.close();
        }
        if (connection != null) {
            connection.close();
        }
        LOG.info("end close.");
    }
}
