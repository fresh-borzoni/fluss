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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.IntFunction;

/**
 * Pages of the rows with one primary key prefix. The first page is a batched lookup of one row more
 * than a page; only if it is full, a scan reads the rest after its last key. Each row comes at most
 * once; one moving into or out of the prefix meanwhile may be missed. An optional resolve step maps
 * each page, e.g. index entries to the rows they point to.
 */
@Internal
public final class PrefixPages implements AutoCloseable {

    /** How long a page waits for a batch of the scan before it checks whether it was closed. */
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    private final Table table;
    private final List<String> prefixColumns;
    private final InternalRow prefix;
    private final int pageSize;
    private final IntFunction<CompletableFuture<LookupResult>> lookupFirst;
    private final Executor scanExecutor;
    private final @Nullable Function<List<InternalRow>, CompletableFuture<List<InternalRow>>>
            resolve;

    /** Rows scanned but not handed over yet: a scan returns batches of bytes. */
    private final ArrayDeque<InternalRow> buffer = new ArrayDeque<>();

    /** Encodes primary keys as the table stores them; not shared, as encoders keep state. */
    private @Nullable KeyEncoder keyEncoder;

    /** The primary key of the last row of the first page, until the scan passed it. */
    private @Nullable byte[] firstPageEnd;

    private @Nullable BatchScanner scanner;
    private boolean firstPage = true;
    private boolean exhausted;

    /** Whether a page is being scanned, which then closes the scan if the pages closed. */
    private boolean reading;

    private volatile boolean closed;

    /**
     * @param table the table, which must have a primary key
     * @param prefixColumns the columns of the prefix, the leading columns of the primary key
     * @param prefix the values of the prefix columns; the pages keep it
     * @param lookupFirst looks up the given number of first rows of the prefix, see {@link
     *     Lookuper#lookupFirst}
     * @param scanExecutor runs the blocking reads of the scan
     * @param resolve turns the rows of a page into the rows it returns, or null to return them
     */
    public PrefixPages(
            Table table,
            List<String> prefixColumns,
            InternalRow prefix,
            int pageSize,
            IntFunction<CompletableFuture<LookupResult>> lookupFirst,
            Executor scanExecutor,
            @Nullable Function<List<InternalRow>, CompletableFuture<List<InternalRow>>> resolve) {
        this.table = table;
        this.prefixColumns = prefixColumns;
        this.prefix = prefix;
        this.pageSize = pageSize;
        this.lookupFirst = lookupFirst;
        this.scanExecutor = scanExecutor;
        this.resolve = resolve;
    }

    /** The rows of one page, whether it is the first and whether no rows follow. */
    public static final class Page {
        public final List<InternalRow> rows;
        public final boolean first;
        public final boolean last;

        Page(List<InternalRow> rows, boolean first, boolean last) {
            this.rows = rows;
            this.first = first;
            this.last = last;
        }
    }

    /** The next page; called again only after the previous page completed and was not last. */
    public CompletableFuture<Page> next() {
        if (firstPage) {
            firstPage = false;
            return lookupFirst
                    .apply(pageSize + 1)
                    .thenCompose(result -> firstPage(result.getRowList()));
        }
        return CompletableFuture.supplyAsync(this::scanPage, scanExecutor)
                .thenCompose(rows -> resolved(rows, false, exhausted && buffer.isEmpty()));
    }

    private CompletableFuture<Page> firstPage(List<InternalRow> rows) {
        if (rows.size() <= pageSize) {
            close();
            return resolved(rows, true, true);
        }
        List<InternalRow> page = new ArrayList<>(rows.subList(0, pageSize));
        TableInfo tableInfo = table.getTableInfo();
        keyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        tableInfo.getRowType(),
                        tableInfo.getPhysicalPrimaryKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());
        firstPageEnd = keyEncoder.encodeKey(page.get(pageSize - 1));
        return resolved(page, true, false);
    }

    private CompletableFuture<Page> resolved(List<InternalRow> rows, boolean first, boolean last) {
        if (last && !first) {
            close();
        }
        if (resolve == null) {
            return CompletableFuture.completedFuture(new Page(rows, first, last));
        }
        return resolve.apply(rows).thenApply(resolvedRows -> new Page(resolvedRows, first, last));
    }

    /** At most a page of the rows after the first page, from a scan of the prefix. */
    private List<InternalRow> scanPage() {
        synchronized (this) {
            if (closed) {
                throw new CancellationException("The pages are closed.");
            }
            reading = true;
        }
        try {
            if (scanner == null) {
                scanner = table.newScan().equalTo(prefixColumns, prefix).createBatchScanner();
            }
            while (buffer.size() < pageSize && !exhausted && !closed) {
                CloseableIterator<InternalRow> batch = scanner.pollBatch(POLL_TIMEOUT);
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
            return rows;
        } catch (IOException e) {
            close();
            throw new UncheckedIOException(
                    "Could not scan table " + table.getTableInfo().getTablePath(), e);
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

    /** Closes the pages; a scan closes once no page is scanned any more. */
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

    private void closeScan() {
        if (scanner != null) {
            try {
                scanner.close();
            } catch (IOException e) {
                throw new UncheckedIOException(
                        "Could not close a scan of table " + table.getTableInfo().getTablePath(),
                        e);
            }
        }
    }

    /** Compares keys as tables order them: unsigned bytes. */
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
}
