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

package org.apache.fluss.client.tiering;

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
import org.apache.fluss.exception.UnknownServerException;
import org.apache.fluss.flink.utils.LogCapture;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.GetDvSnapshotRequest;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that a round's requests follow the bucket's leader when it moves or its server restarts:
 * the admin drops the cached leader, and the request retries with backoff, a bounded number of
 * times. A leader that is not ready is waited for as long as the request may wait, and the wait is
 * logged once a bucket and round. A plan and the parts of a round name what they ask for.
 */
class DvRoundRequestsTest {

    private static final TablePath TABLE = TablePath.of("db", "t");
    private static final TableBucket BUCKET = new TableBucket(1L, 0);

    private final List<Integer> contactedServers = new ArrayList<>();
    private final List<GetDvSnapshotRequest> requests = new ArrayList<>();
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
                                        : served("file-a.parquet"));

        GetDvSnapshotResponse response = fetch();

        assertThat(response.getLakeDvEntryAt(0).getFilePath()).isEqualTo("file-a.parquet");
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
        admin = newAdmin(new int[] {1}, server -> served("unused"));

        assertThatThrownBy(this::fetch)
                .hasRootCauseInstanceOf(LeaderNotAvailableException.class)
                .hasRootCauseMessage("Server 1 is not found in metadata cache.");
        assertThat(sleeps).hasSize(DvRoundRequests.MAX_LEADER_RETRIES);
        assertThat(sleeps).allMatch(ms -> ms <= 5_000L);
        assertThat(invalidations).hasSize(DvRoundRequests.MAX_LEADER_RETRIES + 1);
        assertThat(contactedServers).isEmpty();
    }

    @Test
    void testDoesNotRetryOtherErrors() {
        admin =
                newAdmin(
                        new int[] {2},
                        server -> failed(new PartitionNotExistException("partition dropped")));

        assertThatThrownBy(this::fetch).hasRootCauseInstanceOf(PartitionNotExistException.class);
        assertThat(contactedServers).containsExactly(2);
        assertThat(sleeps).isEmpty();
        assertThat(invalidations).isEmpty();
    }

    /** A stale answer fails at once, also one that names no covered snapshot. */
    @Test
    void testFailsAtOnceWhenTheLeaderCoversNoSnapshot() {
        admin = newAdmin(new int[] {2}, server -> failed(new StaleSnapshotException(-1L, 42L)));

        assertThatThrownBy(this::fetch).hasRootCauseInstanceOf(StaleSnapshotException.class);
        assertThat(sleeps).isEmpty();
    }

    /** A leader that covers another snapshot never comes back to the round's base. */
    @Test
    void testFailsAtOnceWhenTheLeaderCoversAnotherSnapshot() {
        admin = newAdmin(new int[] {2}, server -> failed(new StaleSnapshotException(43L, 42L)));

        assertThatThrownBy(this::fetch).hasRootCauseInstanceOf(StaleSnapshotException.class);
        assertThat(sleeps).isEmpty();
    }

    /** A server error is not retried, even if its message names a stale snapshot. */
    @Test
    void testDoesNotRetryAServerErrorThatNamesAStaleSnapshot() {
        admin =
                newAdmin(
                        new int[] {2},
                        server ->
                                failed(
                                        new UnknownServerException(
                                                "Failed to get the round deletes: Stale snapshot:"
                                                        + " requested 42, current -1")));

        assertThatThrownBy(this::fetch).hasRootCauseInstanceOf(UnknownServerException.class);
        assertThat(sleeps).isEmpty();
    }

    /**
     * Tests that a part waits for a leader rebuilding its deletion vectors with a budget of its
     * own, since the rebuild takes longer than a leader election.
     */
    @Test
    void testRetriesWhileTheLeaderIsNotReady() throws Exception {
        int notReady = DvRoundRequests.MAX_LEADER_RETRIES * 3;
        int[] attempts = {0};
        admin =
                newAdmin(
                        new int[] {2},
                        server ->
                                attempts[0]++ < notReady
                                        ? failed(new DeletionVectorsNotReadyException("rebuilding"))
                                        : served("file-c.parquet"));

        GetDvSnapshotResponse response = fetch();

        assertThat(response.getLakeDvEntryAt(0).getFilePath()).isEqualTo("file-c.parquet");
        assertThat(sleeps).hasSize(notReady).allMatch(ms -> ms <= 5_000L);
        assertThat(invalidations).isEmpty();
    }

    /**
     * The leader moves during a round, and the new leader answers not ready while it recovers its
     * deletion vectors: replays its log, or reads its index while a switch moves it on. The part
     * resolves the new leader and waits for it, and does not fail the round.
     */
    @Test
    void testRetriesANewLeaderThatIsNotReadyWhileItRecovers() throws Exception {
        int[] recovering = {3};
        admin =
                newAdmin(
                        new int[] {2, 3},
                        server -> {
                            if (server == 2) {
                                return failed(new NotLeaderOrFollowerException("not leader"));
                            }
                            return recovering[0]-- > 0
                                    ? failed(
                                            new DeletionVectorsNotReadyException(
                                                    "recovering the deletion vectors of the"
                                                            + " bucket"))
                                    : served("file-d.parquet");
                        });

        GetDvSnapshotResponse response = fetch();

        assertThat(response.getLakeDvEntryAt(0).getFilePath()).isEqualTo("file-d.parquet");
        assertThat(contactedServers).containsExactly(2, 3, 3, 3, 3);
        // one leader retry, then the not-ready waits
        assertThat(sleeps).containsExactly(200L, 250L, 500L, 1_000L);
    }

    @Test
    void testGivesUpWhenTheLeaderStaysNotReady() {
        admin =
                newAdmin(
                        new int[] {2},
                        server -> failed(new DeletionVectorsNotReadyException("rebuilding")));

        assertThatThrownBy(this::fetch)
                .hasRootCauseInstanceOf(DeletionVectorsNotReadyException.class);
        long waited = sleeps.stream().mapToLong(Long::longValue).sum();
        assertThat(waited).isGreaterThanOrEqualTo(DvRoundRequests.MAX_NOT_READY_WAIT_MS);
        assertThat(waited).isLessThan(DvRoundRequests.MAX_NOT_READY_WAIT_MS + 5_000L);
    }

    /** Each attempt names the round's base, its end and the part's start. */
    @Test
    void testRequestsNameThePart() throws Exception {
        admin = newAdmin(new int[] {2}, server -> served("file-e.parquet"));
        DvRoundRequests deletes = new DvRoundRequests(admin, TABLE, sleeps::add);

        deletes.fetch(BUCKET, 42L, null, 20L);
        deletes.fetch(new TableBucket(1L, 3), 42L, 15L, 20L);

        assertThat(requests).hasSize(2);
        GetDvSnapshotRequest first = requests.get(0);
        assertThat(first.getTableId()).isEqualTo(1L);
        assertThat(first.getBucketId()).isZero();
        assertThat(first.hasPartitionId()).isFalse();
        assertThat(first.getReadableSnapshotId()).isEqualTo(42L);
        assertThat(first.getRoundEndOffset()).isEqualTo(20L);
        assertThat(first.hasRoundFromOffset()).isFalse();
        GetDvSnapshotRequest second = requests.get(1);
        assertThat(second.getBucketId()).isEqualTo(3);
        assertThat(second.getRoundFromOffset()).isEqualTo(15L);
        assertThat(second.hasLogEndOffset() || second.hasPinRequestId()).isFalse();
    }

    /**
     * A bucket whose leader replays its change list is not ready for every part until the replay
     * ends. The round logs that wait once a bucket, with the leader's reason, and once when the
     * leader serves the part; the next round logs it again.
     */
    @Test
    void testLogsAWaitOnceABucketAndRound() throws Exception {
        TableBucket other = new TableBucket(1L, 1);
        Map<Integer, Integer> notReady = new HashMap<>();
        admin =
                newAdmin(
                        new int[] {2},
                        server -> {
                            int bucket = requests.get(requests.size() - 1).getBucketId();
                            int left = notReady.getOrDefault(bucket, 0);
                            if (left > 0) {
                                notReady.put(bucket, left - 1);
                                return failed(
                                        new DeletionVectorsNotReadyException(
                                                "Deletion vectors of lake snapshot 42 are"
                                                        + " replaying the log from offset 10."));
                            }
                            return served("file-f.parquet");
                        });
        try (LogCapture log = LogCapture.of(DvRoundRequests.class.getName())) {
            DvRoundRequests round = new DvRoundRequests(admin, TABLE, sleeps::add);
            notReady.put(0, 3);
            round.fetch(BUCKET, 42L, null, 20L);
            notReady.put(0, 2);
            round.fetch(BUCKET, 42L, 15L, 20L);
            notReady.put(1, 2);
            round.fetch(other, 42L, null, 20L);

            assertThat(log.messagesWith("waits for its leader")).hasSize(2);
            assertThat(log.messagesWith("waits for its leader", BUCKET.toString(), "replaying"))
                    .hasSize(1);
            assertThat(log.messagesWith("served the deletes", BUCKET.toString())).hasSize(1);
            assertThat(log.messagesWith("served the deletes", other.toString())).hasSize(1);
            assertThat(log.messagesWith("not ready")).isEmpty();

            notReady.put(0, 1);
            new DvRoundRequests(admin, TABLE, sleeps::add).fetch(BUCKET, 42L, null, 20L);
            assertThat(log.messagesWith("waits for its leader", BUCKET.toString())).hasSize(2);
        }
        assertThat(sleeps).hasSize(8);
    }

    /** A plan names the round's start and caps, and is not retried when the leader is not ready. */
    @Test
    void testPlanNamesTheStartAndTheCaps() throws Exception {
        int[] attempts = {0};
        admin =
                newAdmin(
                        new int[] {2, 3},
                        server ->
                                attempts[0]++ == 0
                                        ? failed(new NotLeaderOrFollowerException("not leader"))
                                        : CompletableFuture.completedFuture(
                                                new GetDvSnapshotResponse()
                                                        .setRoundEndOffset(30L)
                                                        .setRoundCapped(true)
                                                        .setLeaderEpoch(7)));
        DvRoundRequests round = new DvRoundRequests(admin, TABLE, sleeps::add);

        GetDvSnapshotResponse plan = round.plan(new TableBucket(1L, 3), 42L, 10L, 20L, 4096L);

        assertThat(plan.getRoundEndOffset()).isEqualTo(30L);
        assertThat(contactedServers).containsExactly(2, 3);
        GetDvSnapshotRequest request = requests.get(1);
        assertThat(request.getBucketId()).isEqualTo(3);
        assertThat(request.getReadableSnapshotId()).isEqualTo(42L);
        assertThat(request.getRoundStartOffset()).isEqualTo(10L);
        assertThat(request.getRoundMaxRecords()).isEqualTo(20L);
        assertThat(request.getRoundMaxBytes()).isEqualTo(4096L);
        assertThat(request.hasRoundEndOffset() || request.hasRoundLogMask()).isFalse();

        admin.close();
        sleeps.clear();
        admin =
                newAdmin(
                        new int[] {2},
                        server -> failed(new DeletionVectorsNotReadyException("rebuilding")));
        assertThatThrownBy(
                        () ->
                                new DvRoundRequests(admin, TABLE, sleeps::add)
                                        .plan(BUCKET, 42L, 10L, 20L, 4096L))
                .hasRootCauseInstanceOf(DeletionVectorsNotReadyException.class);
        assertThat(sleeps).isEmpty();
    }

    /**
     * A part of a round's log mask names the part and asks for the mask, and waits for a leader
     * that is not ready at most as long as its caller allows.
     */
    @Test
    void testMaskPartWaitsAtMostItsBound() throws Exception {
        int[] notReady = {2};
        admin =
                newAdmin(
                        new int[] {2},
                        server ->
                                notReady[0]-- > 0
                                        ? failed(new DeletionVectorsNotReadyException("replaying"))
                                        : served("unused"));
        DvRoundRequests round = new DvRoundRequests(admin, TABLE, sleeps::add);

        round.fetchMask(BUCKET, 42L, 12L, 20L, 10_000L);
        GetDvSnapshotRequest request = requests.get(requests.size() - 1);
        assertThat(request.getRoundEndOffset()).isEqualTo(20L);
        assertThat(request.getRoundFromOffset()).isEqualTo(12L);
        assertThat(request.isRoundLogMask()).isTrue();
        assertThat(sleeps).containsExactly(250L, 500L);

        sleeps.clear();
        notReady[0] = Integer.MAX_VALUE;
        assertThatThrownBy(() -> round.fetchMask(BUCKET, 42L, null, 20L, 1_000L))
                .hasRootCauseInstanceOf(DeletionVectorsNotReadyException.class);
        assertThat(sleeps.stream().mapToLong(Long::longValue).sum()).isEqualTo(1_000L);
        sleeps.clear();
        assertThatThrownBy(() -> round.fetchMask(BUCKET, 42L, null, 20L, 0L))
                .hasRootCauseInstanceOf(DeletionVectorsNotReadyException.class);
        assertThat(sleeps).isEmpty();
    }

    /**
     * Planning asks every bucket's leader at once, plans a bucket whose leader moved again with the
     * retries of a single plan, and refuses an end outside the round's caps.
     */
    @Test
    void testPlansEveryBucket() throws Exception {
        Map<Integer, Integer> attempts = new HashMap<>();
        admin =
                newAdmin(
                        new int[] {2, 3},
                        server -> {
                            GetDvSnapshotRequest request = requests.get(requests.size() - 1);
                            int bucket = request.getBucketId();
                            if (attempts.merge(bucket, 1, Integer::sum) == 1 && bucket == 1) {
                                return failed(new NotLeaderOrFollowerException("not leader"));
                            }
                            long end = request.getRoundStartOffset() + 10 * (bucket + 1);
                            return CompletableFuture.completedFuture(
                                    new GetDvSnapshotResponse()
                                            .setRoundEndOffset(end)
                                            .setRoundCapped(bucket == 2)
                                            .setLeaderEpoch(5));
                        });
        Map<TableBucket, Long> starts = new LinkedHashMap<>();
        for (int bucket = 0; bucket < 3; bucket++) {
            starts.put(new TableBucket(1L, bucket), 100L * bucket);
        }

        DvRoundRequests rounds = new DvRoundRequests(admin, TABLE, sleeps::add);
        Map<TableBucket, GetDvSnapshotResponse> plans = rounds.planRounds(42L, starts, 30, 1 << 20);

        assertThat(plans).hasSize(3);
        long[] ends = {10L, 120L, 230L};
        for (int bucket = 0; bucket < 3; bucket++) {
            GetDvSnapshotResponse plan = plans.get(new TableBucket(1L, bucket));
            assertThat(plan.getRoundEndOffset()).isEqualTo(ends[bucket]);
            assertThat(plan.isRoundCapped()).isEqualTo(bucket == 2);
        }
        // bucket 1 was asked again on its new leader
        assertThat(attempts).containsEntry(1, 2);
        for (GetDvSnapshotRequest request : requests) {
            assertThat(request.getReadableSnapshotId()).isEqualTo(42L);
        }

        // an end past the record cap is refused
        assertThatThrownBy(() -> rounds.planRounds(42L, starts, 15, 1 << 20))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("planned its round");
    }

    private GetDvSnapshotResponse fetch() throws Exception {
        return new DvRoundRequests(admin, TABLE, sleeps::add).fetch(BUCKET, 42L, null, 20L);
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

    /** A leader's last part of a round ending at 20, which deletes a row of {@code file}. */
    private static CompletableFuture<ApiMessage> served(String file) {
        GetDvSnapshotResponse response =
                new GetDvSnapshotResponse().setSnapshotStartOffset(10L).setRoundNextOffset(20L);
        response.addLakeDvEntry().setFilePath(file).setDeletedPositionsBitmap(new byte[] {1});
        return CompletableFuture.completedFuture(response);
    }

    private static CompletableFuture<ApiMessage> failed(Exception e) {
        CompletableFuture<ApiMessage> future = new CompletableFuture<>();
        future.completeExceptionally(e);
        return future;
    }

    /** Answers the requests by the server they are sent to. */
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
            requests.add((GetDvSnapshotRequest) request);
            return responses.apply(node.id());
        }

        @Override
        public void close() {}
    }
}
