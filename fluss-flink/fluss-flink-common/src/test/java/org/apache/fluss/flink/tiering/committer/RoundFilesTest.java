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

import org.apache.fluss.flink.tiering.committer.DerivedDvRound.Phase;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.apache.fluss.flink.tiering.committer.DerivedDvRound.Phase.CHAIN_REGISTERED;
import static org.apache.fluss.flink.tiering.committer.DerivedDvRound.Phase.FLUSS_REGISTERED;
import static org.apache.fluss.flink.tiering.committer.DerivedDvRound.Phase.INDEX_PUBLISHED;
import static org.apache.fluss.flink.tiering.committer.DerivedDvRound.Phase.LAKE_COMMITTED;
import static org.apache.fluss.flink.tiering.committer.DerivedDvRound.Phase.LAKE_COMMITTING;
import static org.apache.fluss.flink.tiering.committer.DerivedDvRound.Phase.LAKE_REJECTED;
import static org.apache.fluss.flink.tiering.committer.DerivedDvRound.Phase.OFFSETS_WRITTEN;
import static org.apache.fluss.flink.tiering.committer.DerivedDvRound.Phase.STARTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the phases of an attempt of a derived DV round, and which of them keep its files. */
class RoundFilesTest {

    @Test
    void testRoundWithDeletesTakesEveryPhase() {
        RoundFiles round = new RoundFiles();
        assertThat(round.phase()).isEqualTo(STARTED);
        for (Phase next :
                Arrays.asList(
                        OFFSETS_WRITTEN,
                        INDEX_PUBLISHED,
                        LAKE_COMMITTING,
                        LAKE_COMMITTED,
                        CHAIN_REGISTERED,
                        FLUSS_REGISTERED)) {
            round.advance(next);
            assertThat(round.phase()).isEqualTo(next);
        }
    }

    /**
     * A merging round reaches its lake commit without a published index change, as it publishes its
     * change after the commit, and can be rejected there.
     */
    @Test
    void testMergingRoundReachesItsLakeCommitWithoutAPublishedChange() {
        RoundFiles round = new RoundFiles();
        round.advance(OFFSETS_WRITTEN);
        round.advance(LAKE_COMMITTING);
        round.advance(LAKE_REJECTED);
        assertThat(round.phase()).isEqualTo(LAKE_REJECTED);
    }

    /**
     * A failed round keeps its files from its lake commit on, unless the lake surely rejected it: a
     * lake snapshot may reference them.
     */
    @Test
    void testFilesAreKeptFromTheLakeCommitOnUnlessRejected() {
        assertThat(
                        Arrays.stream(Phase.values())
                                .filter(phase -> phase.lakeMayReferenceFiles)
                                .collect(Collectors.toList()))
                .containsExactly(
                        LAKE_COMMITTING, LAKE_COMMITTED, CHAIN_REGISTERED, FLUSS_REGISTERED);
    }

    @Test
    void testPhasesNeitherSkipTheLakeCommitNorGoBack() {
        RoundFiles round = new RoundFiles();
        assertThatThrownBy(() -> round.advance(LAKE_COMMITTED))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("from phase STARTED to phase LAKE_COMMITTED");
        round.advance(OFFSETS_WRITTEN);
        assertThatThrownBy(() -> round.advance(CHAIN_REGISTERED))
                .isInstanceOf(IllegalStateException.class);
        round.advance(LAKE_COMMITTING);
        round.advance(LAKE_REJECTED);
        assertThatThrownBy(() -> round.advance(LAKE_COMMITTED))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> round.advance(LAKE_COMMITTING))
                .isInstanceOf(IllegalStateException.class);
        assertThat(round.phase()).isEqualTo(LAKE_REJECTED);
    }
}
