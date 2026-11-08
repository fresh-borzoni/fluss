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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatRequest;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatResponse;
import org.apache.fluss.rpc.messages.PbCommitLakeTableSnapshotRespForTable;
import org.apache.fluss.rpc.messages.PbHeartbeatRespForTable;
import org.apache.fluss.rpc.messages.PbLakeTableSnapshotMetadata;
import org.apache.fluss.rpc.messages.PbLakeTieringTableInfo;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Optional;

import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newCreateTableRequest;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * ITCase for lake snapshot commits fenced on the tiering epoch of the round that commits them, and
 * for the check of that epoch in {@link CoordinatorService#lakeTieringHeartbeat}.
 */
class CommitLakeTableSnapshotFencingITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(
                            new Configuration()
                                    .set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON))
                    .build();

    private static CoordinatorGateway coordinatorGateway;
    private static ZooKeeperClient zkClient;

    @BeforeAll
    static void beforeAll() {
        coordinatorGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
    }

    @Test
    void testCommitsAreFencedOnTieringEpoch() throws Exception {
        long tableId = createLakeTable(TablePath.of("fluss", "fenced_commits"));
        assertThat(waitForTableToTier(tableId)).isEqualTo(1);

        // the service the table is assigned to registers its snapshot, as does an older client
        // that sends no epoch
        assertAccepted(commit(tableId, 1, 1L));
        assertRegistered(tableId, 1);
        assertAccepted(commit(tableId, 2, null));
        assertRegistered(tableId, 2);
        assertThat(checkTieringEpoch(tableId, 1).hasError()).isFalse();

        // the round fails, so the table is handed out again under epoch 2 and the service it was
        // taken from cannot register any more
        coordinatorGateway
                .lakeTieringHeartbeat(heartbeat(tableId, 1, false))
                .get()
                .getFailedTableRespsList()
                .forEach(resp -> assertThat(resp.hasError()).isFalse());
        assertThat(waitForTableToTier(tableId)).isEqualTo(2);
        String staleEpoch =
                String.format(
                        "The tiering epoch 1 is not match current epoch 2 in coordinator for table %d.",
                        tableId);
        assertFenced(commit(tableId, 3, 1L), staleEpoch);
        assertRegistered(tableId, 2);
        assertFenced(checkTieringEpoch(tableId, 1), staleEpoch);
        assertAccepted(commit(tableId, 3, 2L));
        assertRegistered(tableId, 3);

        // a finished round keeps its epoch until the table is pending again, but registers
        // nothing more under it
        coordinatorGateway
                .lakeTieringHeartbeat(heartbeat(tableId, 2, true))
                .get()
                .getFinishedTableRespsList()
                .forEach(resp -> assertThat(resp.hasError()).isFalse());
        String finished =
                String.format(
                        "The table %d is no longer tiered under epoch 2, it is in Scheduled state.",
                        tableId);
        assertFenced(commit(tableId, 4, 2L), finished);
        assertFenced(checkTieringEpoch(tableId, 2), finished);
        assertRegistered(tableId, 3);
        assertAccepted(commit(tableId, 4, null));
        assertRegistered(tableId, 4);
    }

    private static long createLakeTable(TablePath tablePath) throws Exception {
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("f1", DataTypes.INT()).build())
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        // long enough for a finished round to stay scheduled while checked
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofSeconds(5))
                        .build();
        coordinatorGateway
                .createTable(newCreateTableRequest(tablePath, tableDescriptor, false))
                .get();
        return zkClient.getTable(tablePath).get().tableId;
    }

    /** Requests a table to tier until it is the given one, and returns its tiering epoch. */
    private static long waitForTableToTier(long tableId) {
        PbLakeTieringTableInfo table =
                waitValue(
                        () -> {
                            LakeTieringHeartbeatResponse response =
                                    coordinatorGateway
                                            .lakeTieringHeartbeat(
                                                    new LakeTieringHeartbeatRequest()
                                                            .setRequestTable(true))
                                            .get();
                            return response.hasTieringTable()
                                    ? Optional.of(response.getTieringTable())
                                    : Optional.empty();
                        },
                        Duration.ofMinutes(1),
                        "No table to tier.");
        assertThat(table.getTableId()).isEqualTo(tableId);
        return table.getTieringEpoch();
    }

    private static LakeTieringHeartbeatRequest heartbeat(
            long tableId, long tieringEpoch, boolean finished) {
        LakeTieringHeartbeatRequest request = new LakeTieringHeartbeatRequest();
        (finished ? request.addFinishedTable() : request.addFailedTable())
                .setTableId(tableId)
                .setCoordinatorEpoch(INITIAL_COORDINATOR_EPOCH)
                .setTieringEpoch(tieringEpoch);
        return request;
    }

    private static PbHeartbeatRespForTable checkTieringEpoch(long tableId, long tieringEpoch)
            throws Exception {
        LakeTieringHeartbeatRequest request = new LakeTieringHeartbeatRequest();
        request.addCheckedTable().setTableId(tableId).setTieringEpoch(tieringEpoch);
        LakeTieringHeartbeatResponse response =
                coordinatorGateway.lakeTieringHeartbeat(request).get();
        assertThat(response.getCheckedTableRespsCount()).isEqualTo(1);
        assertThat(response.getCheckedTableRespAt(0).getTableId()).isEqualTo(tableId);
        return response.getCheckedTableRespAt(0);
    }

    private static PbCommitLakeTableSnapshotRespForTable commit(
            long tableId, long snapshotId, @Nullable Long tieringEpoch) throws Exception {
        PbLakeTableSnapshotMetadata metadata =
                new PbLakeTableSnapshotMetadata()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .setTieredBucketOffsetsFilePath(
                                "/tmp/fluss-fencing-it/" + snapshotId + ".offsets");
        if (tieringEpoch != null) {
            metadata.setTieringEpoch(tieringEpoch);
        }
        CommitLakeTableSnapshotRequest request = new CommitLakeTableSnapshotRequest();
        request.addLakeTableSnapshotMetadata().copyFrom(metadata);
        PbCommitLakeTableSnapshotRespForTable resp =
                coordinatorGateway.commitLakeTableSnapshot(request).get().getTableRespAt(0);
        assertThat(resp.getTableId()).isEqualTo(tableId);
        return resp;
    }

    private static void assertAccepted(PbCommitLakeTableSnapshotRespForTable resp) {
        assertThat(resp.hasErrorCode())
                .as("%s", resp.hasErrorMessage() ? resp.getErrorMessage() : "")
                .isFalse();
    }

    private static void assertFenced(PbCommitLakeTableSnapshotRespForTable resp, String message) {
        assertThat(resp.getErrorCode()).isEqualTo(Errors.FENCED_TIERING_EPOCH_EXCEPTION.code());
        assertThat(resp.getErrorMessage()).isEqualTo(message);
    }

    private static void assertFenced(PbHeartbeatRespForTable resp, String message) {
        assertThat(resp.getError().getErrorCode())
                .isEqualTo(Errors.FENCED_TIERING_EPOCH_EXCEPTION.code());
        assertThat(resp.getError().getErrorMessage()).isEqualTo(message);
    }

    private static void assertRegistered(long tableId, long snapshotId) throws Exception {
        assertThat(
                        zkClient.getLakeTable(tableId)
                                .get()
                                .getLatestLakeSnapshotMetadata()
                                .getSnapshotId())
                .isEqualTo(snapshotId);
    }
}
