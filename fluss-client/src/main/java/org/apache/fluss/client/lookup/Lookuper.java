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
import org.apache.fluss.exception.LookupPositionExpiredException;
import org.apache.fluss.exception.LookupPositionNotYetReadableException;
import org.apache.fluss.exception.TableNotVersionedException;
import org.apache.fluss.metadata.BucketReadPosition;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.Range;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.CompletableFuture;

/**
 * A lookuper performs key-based lookups against a primary key table, using either the full primary
 * key or a prefix of the primary key (when configured via {@code Lookup#lookupBy}).
 *
 * <p>Usage examples:
 *
 * <pre>{@code
 * // Row-based key (InternalRow)
 * Lookuper lookuper = table.newLookup().createLookuper();
 * LookupResult res = lookuper.lookup(keyRow).get();
 * }</pre>
 *
 * @since 0.6
 */
@PublicEvolving
@ThreadSafe
public interface Lookuper {

    /**
     * Looks up the latest state visible when the request executes.
     *
     * <p>The lookup key must be a primary key if the lookuper is a Primary Key Lookuper (created by
     * {@code table.newLookup().createLookuper()}), or be the prefix key if the lookuper is a Prefix
     * Key Lookuper (created by {@code table.newLookup().lookupBy(prefixKeys).createLookuper()}).
     *
     * <p>A lookuper configured with {@link Lookup#rangeBy(String)} requires {@link
     * #lookup(InternalRow, Range)} instead.
     *
     * @param lookupKey the lookup key
     * @return the result of lookup.
     * @throws UnsupportedOperationException if the lookuper requires range bounds
     */
    CompletableFuture<LookupResult> lookup(InternalRow lookupKey);

    /**
     * Looks up all rows matching the configured equality columns and the supplied range.
     *
     * <p>Requires a lookuper created with {@link Lookup#rangeBy(String)}. Key and endpoint values
     * are captured before this method returns, so callers may reuse their input rows. Each lookup
     * reads a fixed iterator view within the request and returns the complete result in index key
     * order in the direction configured on {@link Lookup}.
     *
     * @param lookupKey values for the configured lookup columns
     * @param bounds bounds for the configured range column, with one non-null field per endpoint
     * @return the complete lookup result
     * @throws UnsupportedOperationException if this lookuper has no configured range column
     */
    default CompletableFuture<LookupResult> lookup(
            InternalRow lookupKey, Range<? extends InternalRow> bounds) {
        throw new UnsupportedOperationException("This lookuper does not support range bounds.");
    }

    /**
     * Looks up the first rows with the given prefix key, at most {@code limit} of them, in the
     * order of their primary keys as the table stores them. Only forward prefix key lookupers whose
     * lookup columns are the prefix support it; it needs no ordered keys, but fails with an {@link
     * org.apache.fluss.exception.UnsupportedVersionException} on servers older than PREFIX_LOOKUP
     * version 3.
     *
     * @param limit the most rows to return, positive
     */
    default CompletableFuture<LookupResult> lookupFirst(InternalRow lookupKey, int limit) {
        throw new UnsupportedOperationException("This lookuper does not support limits.");
    }

    /**
     * Looks up a primary key or prefix in a versioned table at the supplied bucket position.
     *
     * <p>The key must route to the physical bucket identified by the position. The result in the
     * lookuper's target schema reflects the committed changes with log offsets strictly below the
     * position's next offset. A key or prefix with no match returns an empty result.
     *
     * <p>The future fails with {@link TableNotVersionedException} for a table created without
     * version history, {@link LookupPositionExpiredException} below the retained floor, or {@link
     * LookupPositionNotYetReadableException} above the committed and applied end. The client
     * propagates position errors without automatically retrying. This overload is unavailable when
     * the lookuper is configured to insert missing rows.
     *
     * @param lookupKey the primary key or prefix used by this lookuper
     * @param position the exclusive log position in the bucket to which the key routes
     * @return the future containing the lookup result at the supplied position
     */
    default CompletableFuture<LookupResult> lookup(
            InternalRow lookupKey, BucketReadPosition position) {
        CompletableFuture<LookupResult> failed = new CompletableFuture<>();
        failed.completeExceptionally(
                new UnsupportedOperationException("Positioned lookup is unavailable."));
        return failed;
    }
}
