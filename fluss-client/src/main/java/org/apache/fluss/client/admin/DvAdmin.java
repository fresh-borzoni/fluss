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

package org.apache.fluss.client.admin;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.GetDvSnapshotRequest;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * The deletion vector requests of union reads and tiering, sent to the leader of the bucket they
 * name. {@link FlussAdmin} implements them; Fluss's own readers and tiering reach them with {@link
 * #of(Admin)}. They take and return protocol messages, so they are not part of {@link Admin}.
 */
@Internal
public interface DvAdmin {

    /**
     * Sends a deletion vector request to the leader of the bucket it names: a union read's, which
     * pins a cut or fetches parts of the masks of a pinned cut, or one for a part of a tiering
     * round's deletes.
     *
     * @param tablePath the table path for leader discovery
     * @param request the request, naming the table, partition and bucket
     * @return a CompletableFuture containing the masks or the deletes
     */
    CompletableFuture<GetDvSnapshotResponse> getDvSnapshot(
            TablePath tablePath, GetDvSnapshotRequest request);

    /**
     * Gives up on {@code answer}, the answer of a request sent with this admin that did not come in
     * time: cancels it, and drops the client's connection to the server the request went to. A
     * server that stopped reading a connection leaves every request on it unanswered until it
     * closes the connection as idle, so the requests in flight on it fail, and the next request
     * connects again. By default it only cancels the answer.
     */
    default void abandon(CompletableFuture<?> answer) {
        answer.cancel(false);
    }

    /**
     * Waits at most {@code timeoutMs} for {@code answer}, the answer of a request sent with this
     * admin; the RPC layer bounds no request. Without the answer by then, gives up on it ({@link
     * #abandon}) and fails with a retriable {@link TimeoutException}.
     *
     * @param unanswered the message of the timeout
     */
    default <T> T await(CompletableFuture<T> answer, long timeoutMs, Supplier<String> unanswered)
            throws Exception {
        try {
            return answer.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            abandon(answer);
            throw new TimeoutException(unanswered.get());
        }
    }

    /** The deletion vector requests of {@code admin}, an admin of a Fluss connection. */
    static DvAdmin of(Admin admin) {
        if (admin instanceof DvAdmin) {
            return (DvAdmin) admin;
        }
        throw new UnsupportedOperationException(
                String.format(
                        "%s sends no deletion vector requests; use the admin of a Fluss"
                                + " connection.",
                        admin.getClass().getName()));
    }
}
