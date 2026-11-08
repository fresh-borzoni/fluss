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

package org.apache.fluss.flink.tiering.source.enumerator;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FencedTieringEpochException;
import org.apache.fluss.flink.tiering.TestingLakeTieringFactory;
import org.apache.fluss.flink.tiering.event.FailedTieringEvent;
import org.apache.fluss.flink.tiering.event.FinishedTieringEvent;
import org.apache.fluss.flink.tiering.source.TieringTestBase;
import org.apache.fluss.flink.tiering.source.split.FlinkTieringSplit;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatRequest;
import org.apache.fluss.server.coordinator.LakeTableTieringManager;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that a tiering service goes on tiering without a restart when its coordinator fails over.
 * The round the previous coordinator assigned is fenced, and the service reports its next rounds to
 * the new coordinator.
 */
class TieringSourceEnumeratorCoordinatorFailoverTest extends TieringTestBase {

    @Test
    void testTieringGoesOnAfterCoordinatorFailover() throws Throwable {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "tiering-coordinator-failover");
        long tableId = createTable(tablePath, DEFAULT_LOG_TABLE_DESCRIPTOR);
        appendRow(tablePath, DEFAULT_LOG_TABLE_DESCRIPTOR, 0, 10);
        try (FlussMockSplitEnumeratorContext<FlinkTieringSplit> context =
                new FlussMockSplitEnumeratorContext<>(DEFAULT_BUCKET_NUM)) {
            TieringSourceEnumerator enumerator =
                    new TieringSourceEnumerator(
                            new Configuration(clientConf),
                            context,
                            new TestingLakeTieringFactory(),
                            500);
            enumerator.start();
            for (int subtask = 0; subtask < DEFAULT_BUCKET_NUM; subtask++) {
                context.registerSourceReader(subtask, 0, "localhost-" + subtask);
                enumerator.addReader(subtask);
            }
            long oldEpoch = takeRound(context, enumerator);

            // The new coordinator listens on the previous one's endpoints: unlike the admin's
            // gateway, this service's gateway does not look the coordinator up again.
            String listeners =
                    Endpoint.toListenersString(
                            FLUSS_CLUSTER_EXTENSION
                                    .getCoordinatorServer()
                                    .getRpcServer()
                                    .getBindEndpoints());
            FLUSS_CLUSTER_EXTENSION.stopCoordinatorServer();
            FLUSS_CLUSTER_EXTENSION.startCoordinatorServer(listeners);
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            FLUSS_CLUSTER_EXTENSION
                                    .newCoordinatorClient()
                                    .lakeTieringHeartbeat(new LakeTieringHeartbeatRequest())
                                    .get());
            LakeTableTieringManager tieringManager =
                    FLUSS_CLUSTER_EXTENSION
                            .getCoordinatorServer()
                            .getCoordinatorService()
                            .getLakeTableTieringManager();

            // the round the previous coordinator assigned fails at its commit, and the new
            // coordinator hands the table out again, above every epoch before it
            enumerator.handleSourceEvent(0, new FailedTieringEvent(tableId, "fenced"));
            long newEpoch = takeRound(context, enumerator);
            assertThat(newEpoch).isGreaterThan(oldEpoch);
            tieringManager.validateTieringEpoch(tableId, newEpoch);
            assertThatThrownBy(() -> tieringManager.validateTieringEpoch(tableId, oldEpoch))
                    .isInstanceOf(FencedTieringEpochException.class);

            // the round's end reaches the new coordinator, so the table is not timed out again
            enumerator.handleSourceEvent(0, new FinishedTieringEvent(tableId));
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        // the heartbeat the event asked for; taking one blocks if none is left
                        if (!context.getOneTimeCallables().isEmpty()) {
                            context.runNextOneTimeCallable();
                        }
                        assertThatThrownBy(
                                        () ->
                                                tieringManager.validateTieringEpoch(
                                                        tableId, newEpoch))
                                .isInstanceOf(FencedTieringEpochException.class);
                    });
        }
    }

    /** Lets every reader ask for a split until the table's round is assigned; returns its epoch. */
    private static long takeRound(
            FlussMockSplitEnumeratorContext<FlinkTieringSplit> context,
            TieringSourceEnumerator enumerator) {
        context.getSplitsAssignmentSequence().clear();
        waitUntil(
                () -> {
                    for (int subtask = 0; subtask < DEFAULT_BUCKET_NUM; subtask++) {
                        enumerator.handleSplitRequest(subtask, "localhost-" + subtask);
                    }
                    return context.getSplitsAssignmentSequence().size() >= DEFAULT_BUCKET_NUM;
                },
                Duration.ofMinutes(1),
                "the table's round was not assigned");
        List<FlinkTieringSplit> splits = new ArrayList<>();
        context.getSplitsAssignmentSequence()
                .forEach(a -> a.assignment().values().forEach(splits::addAll));
        long epoch = splits.get(0).unwrap().getTieringEpoch();
        assertThat(splits)
                .allSatisfy(s -> assertThat(s.unwrap().getTieringEpoch()).isEqualTo(epoch));
        return epoch;
    }
}
