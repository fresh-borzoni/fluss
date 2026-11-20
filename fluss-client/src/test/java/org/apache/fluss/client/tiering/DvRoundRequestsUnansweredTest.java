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
import org.apache.fluss.client.utils.TestingRpcClient;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.utils.ExceptionUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A request of a tiering round that its leader does not answer, e.g. one queued on a channel the
 * leader stopped reading: each attempt waits at most the request timeout, then the request is
 * abandoned, which drops the connection to the leader, and is sent again on a new connection under
 * the budget of a leader that is unavailable.
 */
class DvRoundRequestsUnansweredTest {

    private static final TablePath TABLE_PATH = TablePath.of("db", "t");
    private static final long TABLE_ID = 1L;
    private static final TableBucket BUCKET = new TableBucket(TABLE_ID, 0);
    private static final ServerNode COORDINATOR =
            new ServerNode(0, "localhost", 90, ServerType.COORDINATOR);
    private static final ServerNode LEADER =
            new ServerNode(1, "localhost", 91, ServerType.TABLET_SERVER);
    private static final long REQUEST_TIMEOUT_MS = 100;
    // far above what the attempts take, far below the ten minutes of a hung read
    private static final Duration BOUND = Duration.ofSeconds(30);

    private final List<Long> sleeps = new CopyOnWriteArrayList<>();
    // for each request sent, how many times the connection to the leader was dropped before
    private final List<Integer> dropsBeforeRequest = new CopyOnWriteArrayList<>();
    private final AtomicReference<TestingRpcClient> client = new AtomicReference<>();
    private ExecutorService requests;
    private FlussAdmin admin;

    @BeforeEach
    void beforeEach() {
        requests = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void afterEach() {
        requests.shutdownNow();
        if (admin != null) {
            admin.close();
        }
    }

    /** The first request is never answered, its retry on a new connection is. */
    @Test
    void testUnansweredRequestIsSentAgainOnANewConnection() throws Exception {
        admin = admin(1);

        GetDvSnapshotResponse answer =
                within(
                        () ->
                                new DvRoundRequests(
                                                admin, TABLE_PATH, sleeps::add, REQUEST_TIMEOUT_MS)
                                        .fetch(BUCKET, 42L, null, 20L));

        assertThat(answer.getRoundNextOffset()).isEqualTo(20L);
        // the connection was dropped once, before the retry
        assertThat(dropsBeforeRequest).containsExactly(0, 1);
        assertThat(client.get().getDisconnectedNodes()).containsExactly(LEADER);
        assertThat(sleeps).containsExactly(200L);
    }

    /** A plan of a round asked of all leaders at once is planned again on a new connection. */
    @Test
    void testUnansweredPlanIsPlannedAgainOnANewConnection() throws Exception {
        admin = admin(1);

        GetDvSnapshotResponse plan =
                within(
                                () ->
                                        new DvRoundRequests(
                                                        admin,
                                                        TABLE_PATH,
                                                        sleeps::add,
                                                        REQUEST_TIMEOUT_MS)
                                                .planRounds(
                                                        42L,
                                                        Collections.singletonMap(BUCKET, 10L),
                                                        10,
                                                        1 << 20))
                        .get(BUCKET);

        assertThat(plan.getRoundEndOffset()).isEqualTo(20L);
        assertThat(dropsBeforeRequest).containsExactly(0, 1);
        assertThat(client.get().getDisconnectedNodes()).containsExactly(LEADER);
    }

    /** A leader that never answers fails the request once the retries of a leader run out. */
    @Test
    void testNeverAnsweredRequestFailsWithinTheBudget() throws Exception {
        admin = admin(Integer.MAX_VALUE);

        CompletableFuture<Object> fetched =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return new DvRoundRequests(
                                                admin, TABLE_PATH, sleeps::add, REQUEST_TIMEOUT_MS)
                                        .fetch(BUCKET, 42L, null, 20L);
                            } catch (Exception e) {
                                throw new CompletionException(e);
                            }
                        },
                        requests);

        Throwable failure =
                assertThat(fetched)
                        .failsWithin(BOUND)
                        .withThrowableOfType(ExecutionException.class)
                        .actual();
        assertThat(ExceptionUtils.findThrowable(failure, TimeoutException.class)).isPresent();
        int attempts = DvRoundRequests.MAX_LEADER_RETRIES + 1;
        assertThat(dropsBeforeRequest).hasSize(attempts);
        assertThat(client.get().getDisconnectedNodes()).hasSize(attempts);
        assertThat(sleeps).hasSize(DvRoundRequests.MAX_LEADER_RETRIES);
    }

    private <T> T within(Callable<T> request) {
        CompletableFuture<T> done =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return request.call();
                            } catch (Exception e) {
                                throw new CompletionException(e);
                            }
                        },
                        requests);
        return assertThat(done).succeedsWithin(BOUND).actual();
    }

    /**
     * An admin whose metadata has the table's one bucket, led by {@link #LEADER}, which leaves the
     * first {@code unanswered} requests unanswered and answers the others.
     */
    private FlussAdmin admin(int unanswered) {
        TestingRpcClient rpcClient =
                new TestingRpcClient(
                        (node, api) -> {
                            assertThat(node).isEqualTo(LEADER);
                            assertThat(api).isEqualTo(ApiKeys.GET_DV_SNAPSHOT);
                            dropsBeforeRequest.add(client.get().getDisconnectedNodes().size());
                            if (dropsBeforeRequest.size() <= unanswered) {
                                return new CompletableFuture<>();
                            }
                            CompletableFuture<ApiMessage> answer = new CompletableFuture<>();
                            answer.complete(
                                    new GetDvSnapshotResponse()
                                            .setSnapshotStartOffset(10L)
                                            .setRoundNextOffset(20L)
                                            .setRoundEndOffset(20L)
                                            .setLeaderEpoch(1));
                            return answer;
                        });
        client.set(rpcClient);
        PhysicalTablePath path = PhysicalTablePath.of(TABLE_PATH);
        Cluster cluster =
                new Cluster(
                        Collections.singletonMap(LEADER.id(), LEADER),
                        COORDINATOR,
                        Collections.singletonMap(
                                path,
                                Collections.singletonList(
                                        new BucketLocation(
                                                path,
                                                TABLE_ID,
                                                0,
                                                LEADER.id(),
                                                new int[] {LEADER.id()}))),
                        Collections.singletonMap(TABLE_PATH, TABLE_ID),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        return new FlussAdmin(
                rpcClient, new MetadataUpdater(rpcClient, new Configuration(), cluster));
    }
}
