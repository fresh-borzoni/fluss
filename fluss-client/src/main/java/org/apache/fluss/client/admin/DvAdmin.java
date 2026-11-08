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
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.GetDvSnapshotRequest;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The deletion vector requests of union reads and tiering, sent to the leader of the bucket they
 * name. {@link FlussAdmin} implements them; Fluss's own readers and tiering reach them with {@link
 * #of(Admin)}. They take and return protocol messages, so they are not part of {@link Admin}.
 */
@Internal
public interface DvAdmin {

    /**
     * Sends a union read's deletion vector request to the leader of the bucket it names: one that
     * pins a cut, fetches parts of the masks of a pinned cut, or releases it.
     *
     * @param tablePath the table path for leader discovery
     * @param request the request, naming the table, partition and bucket
     * @return a CompletableFuture containing the masks
     */
    CompletableFuture<GetDvSnapshotResponse> getDvSnapshot(
            TablePath tablePath, GetDvSnapshotRequest request);

    /**
     * Looks up the live data file and row position of each encoded primary key at readable snapshot
     * {@code snapshotId}. Fails with a stale-snapshot error unless the leader's index covers
     * exactly that snapshot, and with a not-ready error while the leader switches to it or
     * rebuilds.
     */
    CompletableFuture<GetDvSnapshotResponse> lookupDvPositions(
            TablePath tablePath,
            long tableId,
            @Nullable Long partitionId,
            int bucketId,
            long snapshotId,
            List<byte[]> keys);

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
