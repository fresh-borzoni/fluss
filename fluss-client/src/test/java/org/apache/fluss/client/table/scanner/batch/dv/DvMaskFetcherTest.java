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

import org.apache.fluss.exception.DeletionVectorsNotReadyException;
import org.apache.fluss.exception.LeaderNotAvailableException;
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.rpc.messages.GetDvSnapshotRequest;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.SNAPSHOT;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.TABLE_ID;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.TABLE_PATH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests of {@link DvMaskFetcher}: its retries, the no-wait answers, and releases. */
class DvMaskFetcherTest {

    private final DvTestBucket.MaskServer server = new DvTestBucket.MaskServer();
    private final List<Long> sleeps = new ArrayList<>();
    private final DvMaskFetcher fetcher = new DvMaskFetcher(server, TABLE_PATH, sleeps::add);

    private static GetDvSnapshotRequest pin() {
        return new GetDvSnapshotRequest()
                .setTableId(TABLE_ID)
                .setBucketId(0)
                .setReadableSnapshotId(SNAPSHOT);
    }

    private static GetDvSnapshotRequest release() {
        return pin().setLogEndOffset(106L).setReleaseCut(true);
    }

    @Test
    void testNotReadyIsWaitedForWithBackoff() throws Exception {
        server.answer = request -> DvTestBucket.masks(SNAPSHOT, 100, 106);
        for (int i = 0; i < 5; i++) {
            server.failures.add(new DeletionVectorsNotReadyException("switching"));
        }

        GetDvSnapshotResponse response = fetcher.fetch(DvMaskFetcherTest::pin);

        assertThat(response.getLogEndOffset()).isEqualTo(106L);
        assertThat(sleeps).containsExactly(300L, 600L, 1200L, 2000L, 2000L);
        assertThat(server.requests()).hasSize(6);
    }

    @Test
    void testNotReadyBeyondTheWaitFails() {
        server.answer = request -> DvTestBucket.masks(SNAPSHOT, 100, 106);
        for (int i = 0; i < 200; i++) {
            server.failures.add(new DeletionVectorsNotReadyException("rebuilding"));
        }

        assertThatThrownBy(() -> fetcher.fetch(DvMaskFetcherTest::pin))
                .hasRootCauseInstanceOf(DeletionVectorsNotReadyException.class);
        assertThat(sleeps.stream().mapToLong(Long::longValue).sum())
                .isGreaterThanOrEqualTo(DvMaskFetcher.MAX_NOT_READY_WAIT_MS)
                .isLessThan(
                        DvMaskFetcher.MAX_NOT_READY_WAIT_MS
                                + DvMaskFetcher.MAX_NOT_READY_BACKOFF_MS);
    }

    @Test
    void testLeaderMovesAreFollowedWithinTheirBudget() throws Exception {
        server.answer = request -> DvTestBucket.masks(SNAPSHOT, 100, 106);
        server.failures.add(new NotLeaderOrFollowerException("moved"));
        server.failures.add(new LeaderNotAvailableException("unknown leader"));

        assertThat(fetcher.fetch(DvMaskFetcherTest::pin).getLogEndOffset()).isEqualTo(106L);
        assertThat(sleeps).containsExactly(300L, 600L);

        sleeps.clear();
        for (int i = 0; i <= DvMaskFetcher.MAX_LEADER_RETRIES; i++) {
            server.failures.add(new NotLeaderOrFollowerException("no leader"));
        }
        assertThatThrownBy(() -> fetcher.fetch(DvMaskFetcherTest::pin))
                .hasRootCauseInstanceOf(NotLeaderOrFollowerException.class);
        assertThat(sleeps).hasSize(DvMaskFetcher.MAX_LEADER_RETRIES);
        assertThat(sleeps).allMatch(ms -> ms <= DvMaskFetcher.MAX_LEADER_BACKOFF_MS);
    }

    @Test
    void testStaleFailsAtOnce() {
        server.failures.add(new StaleSnapshotException(SNAPSHOT + 1, SNAPSHOT));

        assertThatThrownBy(() -> fetcher.fetch(DvMaskFetcherTest::pin))
                .satisfies(e -> assertThat(DvMaskFetcher.stale(e)).isNotNull());
        assertThat(sleeps).isEmpty();
        assertThat(server.requests()).hasSize(1);
    }

    /** A read that does not wait gets the leader's answer at once, as an exception. */
    @Test
    void testNoWaitAnswersFailAtOnce() {
        server.answers.add(new GetDvSnapshotResponse().setFoldCut(120L));
        server.answers.add(DvTestBucket.covered(SNAPSHOT - 1, true, 97L));

        assertThatThrownBy(() -> fetcher.fetch(DvMaskFetcherTest::pin))
                .satisfies(e -> assertThat(DvMaskFetcher.rebuildingCut(e)).isEqualTo(120L));
        assertThatThrownBy(() -> fetcher.fetch(DvMaskFetcherTest::pin))
                .satisfies(
                        e -> {
                            StaleSnapshotException stale = DvMaskFetcher.stale(e);
                            assertThat(stale.getCurrentSnapshotId()).isEqualTo(SNAPSHOT - 1);
                            assertThat(stale.isBehind()).isTrue();
                            assertThat(stale.getCurrentStartOffset()).isEqualTo(97L);
                        });
        assertThat(sleeps).isEmpty();
    }

    /** A release that fails is sent once more: a cut not released stays until it idles out. */
    @Test
    void testFailedReleaseIsSentAgainOnce() throws Exception {
        server.releaseFailures.add(new NotLeaderOrFollowerException("moved"));

        fetcher.release(release()).get();

        assertThat(server.requests()).hasSize(2).allMatch(GetDvSnapshotRequest::isReleaseCut);
    }

    @Test
    void testReleaseFailingTwiceGivesUp() {
        server.releaseFailures.add(new NotLeaderOrFollowerException("moved"));
        server.releaseFailures.add(new NotLeaderOrFollowerException("moved again"));

        CompletableFuture<GetDvSnapshotResponse> released = fetcher.release(release());

        assertThatThrownBy(released::get).isInstanceOf(ExecutionException.class);
        assertThat(server.requests()).hasSize(2);
    }
}
