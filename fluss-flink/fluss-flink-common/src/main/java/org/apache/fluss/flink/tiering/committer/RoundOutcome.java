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

import org.apache.fluss.client.tiering.TieringCommitResult;

import javax.annotation.Nullable;

/**
 * The outcome of a round of a table with deletion vectors: committed, or held until a re-index is
 * done.
 */
final class RoundOutcome<Committable> {
    @Nullable private final TieringCommitResult<Committable> result;
    @Nullable private final Reindex reindex;

    private RoundOutcome(
            @Nullable TieringCommitResult<Committable> result, @Nullable Reindex reindex) {
        this.result = result;
        this.reindex = reindex;
    }

    static <Committable> RoundOutcome<Committable> committed(
            TieringCommitResult<Committable> result) {
        return new RoundOutcome<>(result, null);
    }

    static <Committable> RoundOutcome<Committable> held(Reindex reindex) {
        return new RoundOutcome<>(null, reindex);
    }

    /** The committed round's result, or null if the round is held. */
    @Nullable
    TieringCommitResult<Committable> result() {
        return result;
    }

    /** The re-index a held round waits for, or null if the round is committed. */
    @Nullable
    Reindex reindex() {
        return reindex;
    }
}
