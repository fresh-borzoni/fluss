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

package org.apache.fluss.server.tablet;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.exception.DeletionVectorsNotReadyException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.GetDvSnapshotRequest;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.utils.ExceptionUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.ExecutionException;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that every kind of {@code GetDvSnapshot} request needs the permission to read the table:
 * union reads pin, fetch and release cuts of its buckets, and tiering looks up its keys, which
 * reveals whether a key exists.
 */
class TabletServiceDvAuthorizationITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setCoordinatorServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                    .setTabletServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                    .setClusterConf(clusterConf())
                    .build();

    @Test
    void testEveryRequestNeedsThePermissionToReadTheTable() throws Exception {
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        TablePath.of("test_db_1", "dv_authorized"),
                        TableDescriptor.builder()
                                .schema(DATA1_SCHEMA_PK)
                                .distributedBy(1, "a")
                                .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                                .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED, true)
                                .build());
        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);
        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);

        try (RpcClient guestClient = client("guest", "password2");
                RpcClient rootClient = client("root", "password")) {
            TabletServerGateway guest = gateway(guestClient, leader);
            assertDenied(guest, request(tableId));
            assertDenied(guest, request(tableId).setLogEndOffset(0).setLogDvFromOffset(0));
            assertDenied(guest, request(tableId).setLogEndOffset(0).setReleaseCut(true));
            assertDenied(guest, request(tableId).setWaitForCatchUp(false));
            GetDvSnapshotRequest lookup = request(tableId);
            lookup.addLookupKey(new byte[] {1});
            assertDenied(guest, lookup);

            // a principal allowed to read the table pins a cut
            TabletServerGateway root = gateway(rootClient, leader);
            GetDvSnapshotResponse pin = getDvWhenReady(root, request(tableId));
            assertThat(pin.getLogEndOffset()).isGreaterThanOrEqualTo(0L);
        }
    }

    private static void assertDenied(TabletServerGateway gateway, GetDvSnapshotRequest request) {
        assertThatThrownBy(() -> gateway.getDvSnapshot(request).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(AuthorizationException.class);
    }

    private static GetDvSnapshotResponse getDvWhenReady(
            TabletServerGateway gateway, GetDvSnapshotRequest request) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        while (true) {
            try {
                return gateway.getDvSnapshot(request).get();
            } catch (ExecutionException e) {
                if (!ExceptionUtils.findThrowable(e, DeletionVectorsNotReadyException.class)
                                .isPresent()
                        || System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(50);
            }
        }
    }

    private static GetDvSnapshotRequest request(long tableId) {
        return new GetDvSnapshotRequest()
                .setTableId(tableId)
                .setBucketId(0)
                .setReadableSnapshotId(-1L);
    }

    private static RpcClient client(String user, String password) {
        Configuration conf = new Configuration(FLUSS_CLUSTER_EXTENSION.getClientConfig("CLIENT"));
        conf.set(ConfigOptions.CLIENT_SECURITY_PROTOCOL, "sasl");
        conf.set(ConfigOptions.CLIENT_SASL_MECHANISM, "plain");
        conf.setString("client.security.sasl.username", user);
        conf.setString("client.security.sasl.password", password);
        return RpcClient.create(
                conf, new ClientMetricGroup(MetricRegistry.create(conf, null), "dv-" + user));
    }

    private static TabletServerGateway gateway(RpcClient client, int serverId) {
        ServerNode node =
                FLUSS_CLUSTER_EXTENSION.getTabletServerNodes("CLIENT").stream()
                        .filter(n -> n.id() == serverId)
                        .findFirst()
                        .orElseThrow(IllegalStateException::new);
        return GatewayClientProxy.createGatewayProxy(() -> node, client, TabletServerGateway.class);
    }

    private static Configuration clusterConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);
        conf.setString(ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(), "CLIENT:sasl");
        conf.setString("security.sasl.enabled.mechanisms", "plain");
        conf.setString(
                "security.sasl.plain.jaas.config",
                "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required "
                        + "    user_root=\"password\" "
                        + "    user_guest=\"password2\";");
        conf.set(ConfigOptions.SUPER_USERS, "User:root");
        conf.set(ConfigOptions.AUTHORIZER_ENABLED, true);
        return conf;
    }
}
