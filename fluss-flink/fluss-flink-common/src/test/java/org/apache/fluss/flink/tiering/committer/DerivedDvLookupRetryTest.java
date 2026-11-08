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

package org.apache.fluss.flink.tiering.committer;

import org.apache.fluss.client.admin.FlussAdmin;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.DeletionVectorsNotReadyException;
import org.apache.fluss.exception.LeaderNotAvailableException;
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that the position lookups of a tiering round with deletion vectors follow the bucket's
 * leader when it moves or its server restarts: the admin drops the cached leader, and the lookup
 * retries with backoff, a bounded number of times.
 */
class DerivedDvLookupRetryTest {

    private static final TablePath TABLE = TablePath.of("db", "t");
    private static final TableBucket BUCKET = new TableBucket(1L, 0);
    private static final List<byte[]> KEYS = Collections.singletonList(new byte[] {1});

    private final List<Integer> contactedServers = new ArrayList<>();
    private final List<Set<PhysicalTablePath>> invalidations = new ArrayList<>();
    private final List<Long> sleeps = new ArrayList<>();
    private FlussAdmin admin;

    @AfterEach
    void afterEach() {
        if (admin != null) {
            admin.close();
        }
    }

    @Test
    void testRetriesOnTheNewLeaderAfterARestartAndALeaderMove() throws Exception {
        // Server 1 restarted and is not in the metadata cache yet. The refreshed leader 2 has
        // lost the leadership meanwhile. The next refresh finds leader 3.
        admin =
                newAdmin(
                        new int[] {1, 2, 3},
                        server ->
                                server == 2
                                        ? failed(new NotLeaderOrFollowerException("not leader"))
                                        : found("file-a.parquet", 7L));

        GetDvSnapshotResponse response =
                DerivedDvTieringCommitter.lookupWithRetry(
                        admin, TABLE, BUCKET, 42L, KEYS, sleeps::add);

        assertThat(response.getKeyPositionsList()).hasSize(1);
        assertThat(response.getKeyPositionAt(0).getFilePath()).isEqualTo("file-a.parquet");
        assertThat(response.getKeyPositionAt(0).getPos()).isEqualTo(7L);
        // Server 1 was never contacted: it is not in the metadata cache.
        assertThat(contactedServers).containsExactly(2, 3);
        // Each failure dropped the bucket's cached leader, so the next attempt resolved it again.
        assertThat(invalidations)
                .containsExactly(
                        Collections.singleton(PhysicalTablePath.of(TABLE)),
                        Collections.singleton(PhysicalTablePath.of(TABLE)));
        // With backoff.
        assertThat(sleeps).containsExactly(200L, 400L);
    }

    @Test
    void testGivesUpAfterBoundedRetriesWhenTheLeaderStaysUnavailable() {
        admin = newAdmin(new int[] {1}, server -> found("unused", 0L));

        assertThatThrownBy(
                        () ->
                                DerivedDvTieringCommitter.lookupWithRetry(
                                        admin, TABLE, BUCKET, 42L, KEYS, sleeps::add))
                .hasRootCauseInstanceOf(LeaderNotAvailableException.class)
                .hasRootCauseMessage("Server 1 is not found in metadata cache.");
        assertThat(sleeps).hasSize(DerivedDvTieringCommitter.MAX_LEADER_RETRIES);
        assertThat(sleeps).allMatch(ms -> ms <= 5_000L);
        assertThat(invalidations).hasSize(DerivedDvTieringCommitter.MAX_LEADER_RETRIES + 1);
        assertThat(contactedServers).isEmpty();
    }

    @Test
    void testDoesNotRetryOtherErrors() {
        admin =
                newAdmin(
                        new int[] {2},
                        server -> failed(new PartitionNotExistException("partition dropped")));

        assertThatThrownBy(
                        () ->
                                DerivedDvTieringCommitter.lookupWithRetry(
                                        admin, TABLE, BUCKET, 42L, KEYS, sleeps::add))
                .hasRootCauseInstanceOf(PartitionNotExistException.class);
        assertThat(contactedServers).containsExactly(2);
        assertThat(sleeps).isEmpty();
        assertThat(invalidations).isEmpty();
    }

    @Test
    void testRetriesWhileTheLeaderCatchesUpToTheSnapshot() throws Exception {
        int[] attempts = {0};
        admin =
                newAdmin(
                        new int[] {2},
                        server ->
                                attempts[0]++ < 2
                                        ? failed(new StaleSnapshotException(41L, 42L))
                                        : found("file-b.parquet", 3L));

        GetDvSnapshotResponse response =
                DerivedDvTieringCommitter.lookupWithRetry(
                        admin, TABLE, BUCKET, 42L, KEYS, sleeps::add);

        assertThat(response.getKeyPositionAt(0).getFilePath()).isEqualTo("file-b.parquet");
        assertThat(sleeps).containsExactly(250L, 250L);
        assertThat(invalidations).isEmpty();
    }

    /**
     * Tests that the lookup waits for a leader rebuilding its deletion vectors with a budget of its
     * own, since the rebuild takes longer than a leader election.
     */
    @Test
    void testRetriesWhileTheLeaderIsNotReady() throws Exception {
        int notReady = DerivedDvTieringCommitter.MAX_LEADER_RETRIES * 3;
        int[] attempts = {0};
        admin =
                newAdmin(
                        new int[] {2},
                        server ->
                                attempts[0]++ < notReady
                                        ? failed(new DeletionVectorsNotReadyException("rebuilding"))
                                        : found("file-c.parquet", 5L));

        GetDvSnapshotResponse response =
                DerivedDvTieringCommitter.lookupWithRetry(
                        admin, TABLE, BUCKET, 42L, KEYS, sleeps::add);

        assertThat(response.getKeyPositionAt(0).getFilePath()).isEqualTo("file-c.parquet");
        assertThat(sleeps).hasSize(notReady).allMatch(ms -> ms <= 5_000L);
        assertThat(invalidations).isEmpty();
    }

    @Test
    void testGivesUpWhenTheLeaderStaysNotReady() {
        admin =
                newAdmin(
                        new int[] {2},
                        server -> failed(new DeletionVectorsNotReadyException("rebuilding")));

        assertThatThrownBy(
                        () ->
                                DerivedDvTieringCommitter.lookupWithRetry(
                                        admin, TABLE, BUCKET, 42L, KEYS, sleeps::add))
                .hasRootCauseInstanceOf(DeletionVectorsNotReadyException.class);
        long waited = sleeps.stream().mapToLong(Long::longValue).sum();
        assertThat(waited).isGreaterThanOrEqualTo(DerivedDvTieringCommitter.MAX_NOT_READY_WAIT_MS);
        assertThat(waited).isLessThan(DerivedDvTieringCommitter.MAX_NOT_READY_WAIT_MS + 5_000L);
    }

    /**
     * An admin whose bucket leader is {@code leaders[i]} after {@code i} invalidations of the
     * bucket's metadata. Server 1 is never in the metadata cache.
     */
    private FlussAdmin newAdmin(
            int[] leaders, Function<Integer, CompletableFuture<ApiMessage>> responses) {
        RpcClient rpcClient = new StubRpcClient(responses);
        MetadataUpdater metadataUpdater =
                new MetadataUpdater(rpcClient, new Configuration(), Cluster.empty()) {
                    @Override
                    public int leaderFor(TablePath tablePath, TableBucket tableBucket) {
                        return leaders[Math.min(invalidations.size(), leaders.length - 1)];
                    }

                    @Override
                    public TabletServerGateway newTabletServerClientForNode(int serverId) {
                        if (serverId == 1) {
                            return null;
                        }
                        ServerNode node =
                                new ServerNode(
                                        serverId,
                                        "tablet-" + serverId,
                                        9124,
                                        ServerType.TABLET_SERVER);
                        return GatewayClientProxy.createGatewayProxy(
                                () -> node, rpcClient, TabletServerGateway.class);
                    }

                    @Override
                    public void invalidPhysicalTableBucketMeta(
                            Set<PhysicalTablePath> physicalTablesToInvalid) {
                        invalidations.add(physicalTablesToInvalid);
                    }
                };
        return new FlussAdmin(rpcClient, metadataUpdater);
    }

    private static CompletableFuture<ApiMessage> found(String file, long pos) {
        GetDvSnapshotResponse response =
                new GetDvSnapshotResponse().setLogEndOffset(-1L).setSnapshotStartOffset(-1L);
        response.addKeyPosition().setKey(new byte[] {1}).setFilePath(file).setPos(pos);
        return CompletableFuture.completedFuture(response);
    }

    private static CompletableFuture<ApiMessage> failed(Exception e) {
        CompletableFuture<ApiMessage> future = new CompletableFuture<>();
        future.completeExceptionally(e);
        return future;
    }

    /** Answers the lookups by the server they are sent to. */
    private final class StubRpcClient implements RpcClient {
        private final Function<Integer, CompletableFuture<ApiMessage>> responses;

        private StubRpcClient(Function<Integer, CompletableFuture<ApiMessage>> responses) {
            this.responses = responses;
        }

        @Override
        public boolean connect(ServerNode node) {
            return true;
        }

        @Override
        public CompletableFuture<Void> disconnect(ServerNode node) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> disconnect(String serverUid) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public boolean isReady(ServerNode node) {
            return true;
        }

        @Override
        public CompletableFuture<ApiMessage> sendRequest(
                ServerNode node, ApiKeys apiKey, ApiMessage request) {
            assertThat(apiKey).isEqualTo(ApiKeys.GET_DV_SNAPSHOT);
            contactedServers.add(node.id());
            return responses.apply(node.id());
        }

        @Override
        public void close() {}
    }
}
