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

import org.apache.flink.table.data.RowData;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Version-neutral adapter for Flink's lookup functions that read the rows of a key as of an epoch
 * of the scan of the same table.
 *
 * <p>The implementation of this interface for Flink versions that have the ability extends {@code
 * SupportsVersionedLookup}; other Flink versions use this interface only as an internal connector
 * contract.
 */
public interface SupportsVersionedLookupAdapter {

    /**
     * Reads the rows that match the key row as of the end of the given epoch: the rows the scan
     * started from, with exactly the changes the scan emitted in epochs up to it. Waits while the
     * scan has not ended the epoch.
     */
    CompletableFuture<Collection<RowData>> asyncLookupAsOf(RowData keyRow, long epoch);
}
