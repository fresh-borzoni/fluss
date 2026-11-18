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

package org.apache.fluss.flink.adapter;

import org.apache.fluss.flink.source.lookup.FlinkAsyncLookupFunction;

import org.apache.flink.table.connector.source.lookup.SupportsPagedLookup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/** An adapter for the lookup functions of Flink versions that can read in pages. */
public class PagedLookupAdapter {

    private PagedLookupAdapter() {}

    /** The lookup function as Flink sees it: one that also reads the rows of a key in pages. */
    public static AsyncLookupFunction wrap(FlinkAsyncLookupFunction function) {
        return new PagedFunction(function);
    }

    /**
     * Delegates lookups to the Fluss function, and reads pages through it. Public, as Flink
     * instantiates only public function classes.
     */
    public static final class PagedFunction extends AsyncLookupFunction
            implements SupportsPagedLookup {

        private static final long serialVersionUID = 1L;

        private final FlinkAsyncLookupFunction function;

        public PagedFunction(FlinkAsyncLookupFunction function) {
            this.function = function;
        }

        @Override
        public void open(FunctionContext context) throws Exception {
            function.open(context);
        }

        @Override
        public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
            return function.asyncLookup(keyRow);
        }

        @Override
        public LookupCursor openCursor(RowData keyRow, int pageSize) {
            FlinkAsyncLookupFunction.LookupPages pages = function.openPages(keyRow, pageSize);
            return new LookupCursor() {
                @Override
                public CompletableFuture<LookupPage> nextPage() {
                    return pages.next().thenApply(page -> LookupPage.of(page.rows, page.last));
                }

                @Override
                public void close() {
                    pages.close();
                }
            };
        }

        @Override
        public void close() throws Exception {
            function.close();
        }
    }
}
