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

package org.apache.fluss.client.table.scanner.batch.dv;

import org.apache.fluss.client.admin.FlussAdmin;
import org.apache.fluss.client.lookup.Lookup;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.DroppedPartitionDetector;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.writer.Append;
import org.apache.fluss.client.table.writer.Upsert;
import org.apache.fluss.client.utils.TestingRpcClient;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.GetDvSnapshotRequest;
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

import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.SNAPSHOT;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.TABLE_ID;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR_PK;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A union read whose request a server never answers, e.g. one queued on a channel the server
 * stopped reading: the RPC layer bounds no request, so the read waited until the server closed the
 * idle connection, ten minutes. The read must end within the request timeout instead, and drop the
 * connection to that server, so that what the read does next connects again.
 */
class DvUnansweredRequestTest {

    private static final ServerNode COORDINATOR =
            new ServerNode(0, "localhost", 90, ServerType.COORDINATOR);
    private static final ServerNode LEADER =
            new ServerNode(1, "localhost", 91, ServerType.TABLET_SERVER);
    private static final TableInfo TABLE_INFO =
            TableInfo.of(
                    TABLE_PATH,
                    TABLE_ID,
                    1,
                    DATA1_TABLE_DESCRIPTOR_PK,
                    DEFAULT_REMOTE_DATA_DIR,
                    0L,
                    0L);
    private static final long REQUEST_TIMEOUT_MS = 100;
    // far above the request timeout, far below the ten minutes of the hung read
    private static final Duration BOUND = Duration.ofSeconds(30);

    // the servers each kind of request went to
    private final List<ServerNode> maskRequestsTo = new CopyOnWriteArrayList<>();
    private final List<ServerNode> readableRequestsTo = new CopyOnWriteArrayList<>();
    private ExecutorService reads;

    @BeforeEach
    void beforeEach() {
        reads = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void afterEach() {
        reads.shutdownNow();
    }

    /** The leader never answers a mask request of a part. */
    @Test
    void testUnansweredMaskRequestFailsAndDropsTheConnection() throws Exception {
        TestingRpcClient client =
                new TestingRpcClient(
                        (node, api) -> {
                            assertThat(api).isEqualTo(ApiKeys.GET_DV_SNAPSHOT);
                            maskRequestsTo.add(node);
                            return new CompletableFuture<>();
                        });
        try (FlussAdmin admin = admin(client)) {
            DvMaskFetcher fetcher =
                    new DvMaskFetcher(admin, TABLE_PATH, millis -> {}, REQUEST_TIMEOUT_MS);

            Throwable failure =
                    failureOf(
                            () ->
                                    fetcher.fetch(
                                            () ->
                                                    new GetDvSnapshotRequest()
                                                            .setTableId(TABLE_ID)
                                                            .setBucketId(0)
                                                            .setReadableSnapshotId(SNAPSHOT)));

            assertThat(ExceptionUtils.findThrowable(failure, TimeoutException.class)).isPresent();
            assertThat(maskRequestsTo).containsExactly(LEADER);
            assertThat(client.getDisconnectedNodes()).containsExactly(LEADER);
        }
    }

    /**
     * The path of the hung read: the leader covers another snapshot than the planned one (a read
     * that waits gets that as a stale error without an offset), so the part asks a server for the
     * readable lake snapshot, and that server never answers.
     */
    @Test
    void testUnansweredReadableSnapshotRequestFailsTheResolveAndDropsTheConnection()
            throws Exception {
        TestingRpcClient client =
                new TestingRpcClient(
                        (node, api) -> {
                            if (api == ApiKeys.GET_DV_SNAPSHOT) {
                                maskRequestsTo.add(node);
                                CompletableFuture<ApiMessage> stale = new CompletableFuture<>();
                                stale.completeExceptionally(
                                        new StaleSnapshotException(SNAPSHOT + 1, SNAPSHOT));
                                return stale;
                            }
                            assertThat(api).isEqualTo(ApiKeys.GET_LAKE_SNAPSHOT);
                            readableRequestsTo.add(node);
                            return new CompletableFuture<>();
                        });
        DvTestBucket bucket = new DvTestBucket();
        bucket.standardBucket();
        try (FlussAdmin admin = admin(client)) {
            DvTableReadContext context =
                    new DvTableReadContext(
                                    new TestingTable(TABLE_INFO),
                                    () -> admin,
                                    bucket.lake,
                                    null,
                                    new DroppedPartitionDetector(() -> admin, TABLE_PATH))
                            .withRequestTimeout(Duration.ofMillis(REQUEST_TIMEOUT_MS));
            DvBucketPartReader reader = new DvBucketPartReader(bucket.bucketPart(), context);

            Throwable failure = failureOf(reader::resolve);

            assertThat(ExceptionUtils.findThrowable(failure, TimeoutException.class)).isPresent();
            assertThat(maskRequestsTo).containsExactly(LEADER);
            assertThat(readableRequestsTo).containsExactly(LEADER);
            assertThat(client.getDisconnectedNodes()).containsExactly(LEADER);
        }
    }

    /** Runs the read on another thread and returns its failure, which must come within BOUND. */
    private Throwable failureOf(Callable<?> read) {
        CompletableFuture<Object> done =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return read.call();
                            } catch (Exception e) {
                                throw new CompletionException(e);
                            }
                        },
                        reads);
        return assertThat(done)
                .failsWithin(BOUND)
                .withThrowableOfType(ExecutionException.class)
                .actual();
    }

    /** An admin whose metadata has the table's one bucket, led by {@link #LEADER}. */
    private static FlussAdmin admin(TestingRpcClient client) {
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
        return new FlussAdmin(client, new MetadataUpdater(client, new Configuration(), cluster));
    }

    /** A table that only has its info; a part that reads it reads no rows here. */
    private static final class TestingTable implements Table {
        private final TableInfo tableInfo;

        TestingTable(TableInfo tableInfo) {
            this.tableInfo = tableInfo;
        }

        @Override
        public TableInfo getTableInfo() {
            return tableInfo;
        }

        @Override
        public Scan newScan() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Lookup newLookup() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Append newAppend() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Upsert newUpsert() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }
}
