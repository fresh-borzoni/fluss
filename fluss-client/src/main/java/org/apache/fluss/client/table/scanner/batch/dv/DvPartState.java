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

import org.apache.fluss.annotation.Internal;

/**
 * The state of the read of one {@link DvBucketPart}. {@link DvPartMachine} holds it and changes it
 * on each answer of the bucket's leader, lake or log; its javadoc has the transitions. A part's own
 * pin is the cut its leader pinned for it when it started unpinned at planning.
 */
@Internal
enum DvPartState {

    /** Reads a lake snapshot from its readable offset, and holds no cut: pins one. */
    PLANNED,

    /**
     * Plans the bucket's lake files in a lake snapshot: the one its leader covers instead of the
     * planned one (at most {@link DvPartMachine#MAX_REPLANS} - 1 times), the coordinator's readable
     * one to read by key, or its own, for a part carried without its lake files.
     */
    REPLANNING,

    /** Holds its own pin of a cut, and fetches the masks of the cut. */
    PINNED,

    /** Reads with a cut pinned at planning, which the engine holds, and fetches its masks. */
    PINNED_AT_PLANNING,

    /** Derives the masks of a cut from the keys of its lake files and of the log. */
    FOLDING,

    /** Restored with the masks it kept: checks they are of its cut and snapshot commit. */
    RESUMING,

    /**
     * Has its masks, fetched, derived or kept, or reads its lake files only. Holds its own pin if
     * it read with masks of that pin.
     */
    READY,

    /** Returns the rows of its lake files that the masks do not hide. */
    READING_LAKE,

    /** Returns the log rows up to the cut that the masks do not hide. */
    READING_LOG,

    /** Read to the end. */
    DONE,

    /** Failed; the failure is rethrown. */
    FAILED,

    /** Closed by the engine. */
    CLOSED;

    /** Whether the part's masks are not in hand yet. */
    boolean isResolving() {
        switch (this) {
            case PLANNED:
            case REPLANNING:
            case PINNED:
            case PINNED_AT_PLANNING:
            case FOLDING:
            case RESUMING:
                return true;
            default:
                return false;
        }
    }

    /** Whether the part returns rows or is about to. */
    boolean isReading() {
        return this == READY || this == READING_LAKE || this == READING_LOG;
    }

    /** Whether the part's read ended. */
    boolean isTerminal() {
        return this == DONE || this == FAILED || this == CLOSED;
    }
}
