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

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * What an attempt of a round wrote besides the committable's files, and the phase it reached. A
 * failed round deletes its files only when no lake snapshot can reference them ({@link
 * Phase#lakeMayReferenceFiles}).
 */
final class RoundFiles {
    @Nullable String offsetsFile;
    @Nullable Long indexChangeId;
    private Phase phase = Phase.STARTED;

    /** The phase the attempt reached. */
    Phase phase() {
        return phase;
    }

    /** Moves the attempt to phase {@code next}, which must follow the current one. */
    void advance(Phase next) {
        checkState(
                next.follows(phase), "A round cannot move from phase %s to phase %s.", phase, next);
        phase = next;
    }
}
