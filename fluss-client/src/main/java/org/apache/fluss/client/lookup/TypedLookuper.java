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

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.BucketReadPosition;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.Range;

import java.util.concurrent.CompletableFuture;

/**
 * A typed lookuper performs key-based lookups against a primary key table using POJOs.
 *
 * @param <T> the type of the lookup key
 * @since 0.6
 */
@PublicEvolving
public interface TypedLookuper<T> {

    /**
     * Looks up the latest state visible when the request executes.
     *
     * <p>A lookuper configured with {@link Lookup#rangeBy(String)} requires {@link #lookup(Object,
     * Range)} instead.
     *
     * @param lookupKey the lookup key
     * @return the result of lookup.
     * @throws UnsupportedOperationException if the lookuper requires range bounds
     */
    CompletableFuture<LookupResult> lookup(T lookupKey);

    /**
     * Looks up rows using the POJO equality key and bounds for the configured range column.
     *
     * <p>Has the same semantics as {@link Lookuper#lookup(org.apache.fluss.row.InternalRow,
     * Range)}.
     *
     * @param lookupKey values for the configured lookup columns
     * @param bounds the range bounds
     * @return the complete lookup result
     */
    default CompletableFuture<LookupResult> lookup(
            T lookupKey, Range<? extends InternalRow> bounds) {
        throw new UnsupportedOperationException("This lookuper does not support range bounds.");
    }

    /**
     * Looks up a typed primary key or prefix in a versioned table at the supplied bucket position.
     *
     * <p>Uses the historical read and error semantics of {@link
     * Lookuper#lookup(org.apache.fluss.row.InternalRow, BucketReadPosition)}.
     *
     * @param lookupKey the typed primary key or prefix used by this lookuper
     * @param position the exclusive log position in the bucket to which the key routes
     * @return the future containing the lookup result in the lookuper's target schema
     */
    default CompletableFuture<LookupResult> lookup(T lookupKey, BucketReadPosition position) {
        CompletableFuture<LookupResult> failed = new CompletableFuture<>();
        failed.completeExceptionally(
                new UnsupportedOperationException("Positioned lookup is unavailable."));
        return failed;
    }
}
