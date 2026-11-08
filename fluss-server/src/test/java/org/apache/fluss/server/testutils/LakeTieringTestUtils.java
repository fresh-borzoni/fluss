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

package org.apache.fluss.server.testutils;

import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatRequest;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatResponse;

import java.time.Duration;
import java.util.Optional;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Utilities for tests that act as a lake tiering service. */
public final class LakeTieringTestUtils {

    private LakeTieringTestUtils() {}

    /**
     * Acts as a tiering service that also tiers tables with deletion vectors: requests tables to
     * tier until the coordinator hands out {@code tableId}, and returns the tiering epoch to commit
     * its lake snapshots under. Tables handed out meanwhile stay assigned to no one.
     */
    public static long takeTableToTier(CoordinatorGateway coordinator, long tableId) {
        return waitValue(
                () -> {
                    LakeTieringHeartbeatResponse response =
                            coordinator
                                    .lakeTieringHeartbeat(
                                            new LakeTieringHeartbeatRequest()
                                                    .setRequestTable(true)
                                                    .setSupportsDeletionVectors(true))
                                    .get();
                    return response.hasTieringTable()
                                    && response.getTieringTable().getTableId() == tableId
                            ? Optional.of(response.getTieringTable().getTieringEpoch())
                            : Optional.empty();
                },
                Duration.ofMinutes(1),
                "The coordinator did not hand out table " + tableId + " to tier");
    }

    /**
     * Renews the heartbeat of a table a test tiers under {@code tieringEpoch}, as a tiering service
     * does while its round runs, so the coordinator does not time the round out.
     */
    public static void renewTiering(CoordinatorGateway coordinator, long tableId, long tieringEpoch)
            throws Exception {
        int coordinatorEpoch =
                coordinator
                        .lakeTieringHeartbeat(new LakeTieringHeartbeatRequest())
                        .get()
                        .getCoordinatorEpoch();
        LakeTieringHeartbeatRequest request = new LakeTieringHeartbeatRequest();
        request.addTieringTable()
                .setTableId(tableId)
                .setCoordinatorEpoch(coordinatorEpoch)
                .setTieringEpoch(tieringEpoch);
        LakeTieringHeartbeatResponse response = coordinator.lakeTieringHeartbeat(request).get();
        checkState(
                !response.getTieringTableRespAt(0).hasError(),
                "The tiering of table %s under epoch %s was not renewed: %s",
                tableId,
                tieringEpoch,
                response.getTieringTableRespAt(0).hasError()
                        ? response.getTieringTableRespAt(0).getError().getErrorMessage()
                        : "");
    }
}
