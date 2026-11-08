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

/** The outcome of a round of a table with deletion vectors: committed. */
final class RoundOutcome<Committable> {
    private final TieringCommitResult<Committable> result;

    private RoundOutcome(TieringCommitResult<Committable> result) {
        this.result = result;
    }

    static <Committable> RoundOutcome<Committable> committed(
            TieringCommitResult<Committable> result) {
        return new RoundOutcome<>(result);
    }

    /** The committed round's result. */
    TieringCommitResult<Committable> result() {
        return result;
    }
}
