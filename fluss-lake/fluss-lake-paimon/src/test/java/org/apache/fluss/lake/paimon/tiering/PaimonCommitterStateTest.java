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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.lake.paimon.tiering.PaimonCommitterState.FailedCommit;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The undecided failed commits a tiering job keeps are bounded: by age, by the tables the job still
 * commits, and by their number.
 */
class PaimonCommitterStateTest {

    private static final long MIN_AGE = 10;
    private static final long ABANDON_AFTER = 1_000;

    private final AtomicLong clock = new AtomicLong(1_000_000L);

    @Test
    void testAFailedCommitIsDueOnceOldEnough() {
        PaimonCommitterState state = state(10);
        state.addFailedCommit(1L, failed("a"));
        clock.addAndGet(MIN_AGE - 1);
        assertThat(state.takeDue(1L)).isEmpty();
        clock.addAndGet(1);
        assertThat(state.takeDue(1L)).extracting(c -> c.commitUser).containsExactly("a");
        assertThat(state.failedCommits(1L)).isZero();
    }

    /** Past the maximum, the oldest record of the job goes, whichever table it is of. */
    @Test
    void testTheOldestRecordIsDroppedPastTheMaximum() {
        PaimonCommitterState state = state(2);
        state.addFailedCommit(1L, failed("oldest"));
        clock.incrementAndGet();
        state.addFailedCommit(2L, failed("second"));
        clock.incrementAndGet();
        state.addFailedCommit(2L, failed("third"));

        assertThat(state.failedCommits(1L)).isZero();
        assertThat(state.failedCommits(2L)).isEqualTo(2);
        clock.addAndGet(MIN_AGE);
        assertThat(state.takeDue(2L))
                .extracting(c -> c.commitUser)
                .containsExactly("second", "third");
    }

    /**
     * The records of a table the job no longer commits, e.g. a dropped table or one another tiering
     * service tiers now, are dropped once another table's commit finds them abandoned.
     */
    @Test
    void testTheRecordsOfATableTheJobNoLongerCommitsAreDropped() {
        PaimonCommitterState state = state(10);
        state.addFailedCommit(1L, failed("of dropped table"));
        state.addFailedCommit(2L, failed("of live table"));

        clock.addAndGet(ABANDON_AFTER - 1);
        // table 2 is still tiered by the job
        List<FailedCommit> due = state.takeDue(2L);
        assertThat(due).extracting(c -> c.commitUser).containsExactly("of live table");
        assertThat(state.failedCommits(1L)).isEqualTo(1);

        clock.addAndGet(1);
        state.takeDue(2L);
        assertThat(state.failedCommits(1L)).isZero();
    }

    private PaimonCommitterState state(int maxRecords) {
        return new PaimonCommitterState(MIN_AGE, ABANDON_AFTER, maxRecords, clock::get);
    }

    private FailedCommit failed(String commitUser) {
        return new FailedCommit(
                commitUser, 1L, 1L, Collections.singletonList("file"), null, clock.get());
    }
}
