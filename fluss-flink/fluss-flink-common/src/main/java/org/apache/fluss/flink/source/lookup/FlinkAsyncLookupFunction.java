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
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.LookupType;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.lookup.PrefixPages;
import org.apache.fluss.client.lookup.SecondaryIndexLookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.flink.sink.writer.ReadYourWrites;
import org.apache.fluss.flink.source.lookup.LookupNormalizer.RemainingFilter;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.flink.utils.FlinkUtils;
import org.apache.fluss.flink.utils.FlussRowToFlinkRowConverter;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
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
        if (requiresWriter
                && lookupNormalizer.getLookupType() == LookupType.SECONDARY_INDEX_LOOKUP) {
            // the rows a writer has not flushed yet are found by their primary key, not by the
            // columns of a secondary index
            throw new UnsupportedOperationException(
                    "Lookups of Fluss table '"
                            + tablePath
                            + "' that must read what the sink writer of the same task wrote"
                            + " support only primary-key and prefix lookups, not lookups by a"
                            + " secondary index.");
        }
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

        lookuper = lookupNormalizer.createLookuper(table, flinkRowType, insertIfNotExists);
        if (lookupNormalizer.getLookupType() == LookupType.PREFIX_LOOKUP) {
            // the rows of a prefix can also be read in pages
            int[] lookupKeyIndexes = lookupNormalizer.getLookupKeyIndexes();
            RowType lookupKeyRowType = FlinkUtils.projectRowType(flinkRowType, lookupKeyIndexes);
            lookupColumnNames = lookupKeyRowType.getFieldNames();
            lookupKeyGetters = new RowData.FieldGetter[lookupKeyRowType.getFieldCount()];
            for (int i = 0; i < lookupKeyGetters.length; i++) {
                lookupKeyGetters[i] = RowData.createFieldGetter(lookupKeyRowType.getTypeAt(i), i);
            }
        }
        // the pages of prefix and index lookups scan
        scanPermits = new ScanPermits(MAX_OPEN_SCANS);
        openScans = ConcurrentHashMap.newKeySet();

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
        // Bind a wrapper to this call. Multiple async lookups can be in flight at the same time, so
        // a shared mutable wrapper would let a later call overwrite an earlier call's key data.
        InternalRow flussKeyRow = new FlinkAsFlussRow(normalizedKeyRow);
        CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();

        // a sink writer of this task that writes the table: read what it wrote too
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
                                    lookup(flussKeyRow, view, remainingFilter, future);
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
     * Opens pages of what {@link #asyncLookup} returns, with unacknowledged writes of this task in
     * place. Prefix and index lookups page through {@link PrefixPages}, index pages capped at
     * {@link ConfigOptions#CLIENT_LOOKUP_SECONDARY_INDEX_MAX_CANDIDATES}; others are one page.
     * Called in the lookup thread.
     */
    public LookupPages openPages(RowData keyRow, int pageSize) {
        if (lookuper instanceof SecondaryIndexLookuper) {
            // at most as many candidates as one index lookup may read
            int indexPageSize =
                    Math.min(
                            pageSize,
                            flussConfig.get(
                                    ConfigOptions.CLIENT_LOOKUP_SECONDARY_INDEX_MAX_CANDIDATES));
            RowData normalizedKeyRow = lookupNormalizer.normalizeLookupKey(keyRow);
            return new ScanPages(
                    ((SecondaryIndexLookuper) lookuper)
                            .openPages(
                                    new FlinkAsFlussRow(normalizedKeyRow),
                                    indexPageSize,
                                    pageReader()),
                    null,
                    lookupNormalizer.createRemainingFilter(keyRow));
        }
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
        InternalRow prefix = new FlinkAsFlussRow().replace(copyKey(normalizedKeyRow));
        PrefixPages pages =
                new PrefixPages(
                        table,
                        lookupColumnNames,
                        prefix,
                        pageSize,
                        limit -> {
                            synchronized (this) {
                                return lookuper.lookupFirst(prefix, limit);
                            }
                        },
                        pageReader(),
                        null);
        return new ScanPages(pages, view, remainingFilter);
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
     * Client pages as rows of this function: overlaid, filtered and converted. Pages after the
     * first scan, and wait for a permit.
     */
    private final class ScanPages implements LookupPages {

        private final PrefixPages pages;
        private final @Nullable ReadYourWrites.View view;
        private final @Nullable RemainingFilter remainingFilter;

        /** The permit of the scan, asked for with the second page. */
        private @Nullable CompletableFuture<Void> permit;

        /** The page being read after the first, which holds the permit until it completes. */
        private @Nullable CompletableFuture<?> reading;

        private boolean firstPage = true;
        private boolean closed;

        private ScanPages(
                PrefixPages pages,
                @Nullable ReadYourWrites.View view,
                @Nullable RemainingFilter remainingFilter) {
            this.pages = pages;
            this.view = view;
            this.remainingFilter = remainingFilter;
        }

        @Override
        public CompletableFuture<Page> next() {
            if (firstPage) {
                firstPage = false;
                // in the flush mode, the writer of the task flushes first
                CompletableFuture<Void> ready =
                        view == null ? CompletableFuture.completedFuture(null) : view.ready();
                return ready.thenCompose(ignored -> pages.next()).thenApply(this::toPage);
            }
            synchronized (this) {
                if (permit == null) {
                    permit = scanPermits.acquire();
                    openScans.add(this);
                }
                CompletableFuture<Page> page =
                        permit.thenCompose(ignored -> pages.next()).thenApply(this::toPage);
                reading = page;
                page.whenComplete((ignored, throwable) -> pageDone());
                return page;
            }
        }

        private Page toPage(PrefixPages.Page page) {
            if (page.last) {
                close();
            }
            List<InternalRow> applied =
                    view == null ? page.rows : view.applyToPage(page.rows, page.first);
            List<RowData> rows = new ArrayList<>(applied.size());
            for (InternalRow row : applied) {
                RowData flinkRow = flussRowToFlinkRowConverter.toFlinkRowData(maybeProject(row));
                if (remainingFilter == null || remainingFilter.isMatch(flinkRow)) {
                    rows.add(flinkRow);
                }
            }
            return new Page(rows, page.last);
        }

        @Override
        public void close() {
            boolean release;
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
                // a permit not granted yet is withdrawn, which ends the page that waits for it
                release = reading == null || (permit != null && !permit.isDone());
            }
            pages.close();
            if (release) {
                releasePermit();
            }
        }

        /** A page after the first completed: the permit returns now if the pages closed. */
        private void pageDone() {
            boolean release;
            synchronized (this) {
                reading = null;
                release = closed;
            }
            if (release) {
                releasePermit();
            }
        }

        private void releasePermit() {
            CompletableFuture<Void> granted;
            synchronized (this) {
                granted = permit;
                permit = null;
            }
            openScans.remove(this);
            if (granted != null) {
                scanPermits.release(granted);
            }
        }
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
