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

package org.apache.fluss.rpc.protocol;

import org.apache.fluss.exception.ApiException;
import org.apache.fluss.exception.StaleSnapshotException;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.CompletionException;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link Errors}. */
class ErrorsTest {

    @Test
    void testErrorCodesAreContinuous() {
        int[] codes = Arrays.stream(Errors.values()).mapToInt(Errors::code).sorted().toArray();
        // Include UNKNOWN_SERVER_ERROR (-1) and NONE (0).
        assertThat(codes).containsExactly(IntStream.range(-1, -1 + codes.length).toArray());
    }

    /** A stale snapshot crosses the RPC as itself, with both snapshot ids. */
    @Test
    void testStaleSnapshotCrossesTheRpc() {
        StaleSnapshotException stale =
                new StaleSnapshotException(-7123456789L, 42L, "bucket 3, cut 30");
        assertThat(Errors.forException(stale)).isEqualTo(Errors.STALE_SNAPSHOT);
        assertThat(Errors.STALE_SNAPSHOT.code()).isEqualTo(82);

        ApiError error = ApiError.fromThrowable(new CompletionException(stale));
        ApiException received = ApiError.fromErrorMessage(error.toErrorResponse()).exception();

        assertThat(received).isInstanceOf(StaleSnapshotException.class);
        StaleSnapshotException receivedStale = (StaleSnapshotException) received;
        assertThat(receivedStale.getCurrentSnapshotId()).isEqualTo(-7123456789L);
        assertThat(receivedStale.getRequestedSnapshotId()).isEqualTo(42L);
        assertThat(receivedStale).hasMessageContaining("cut 30");
        assertThat(
                        ((StaleSnapshotException) Errors.STALE_SNAPSHOT.exception())
                                .getCurrentSnapshotId())
                .isEqualTo(-1L);
    }
}
