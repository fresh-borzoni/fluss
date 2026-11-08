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

package org.apache.fluss.server.kv.dv;

import javax.annotation.Nullable;

/**
 * What lookups of a bucket see of its state at one point: an immutable value, so a lookup decides
 * on one consistent state.
 */
final class DvState {

    /** The state kinds; see {@link DvOwner} for the transitions. */
    enum Kind {
        // a new leader that has not taken over yet: reads and lookups are not ready
        UNRECOVERED,
        // A recovery or FULL load runs or is queued. Serves the covered version, if any; without
        // one, reads that do not wait read by key, and the empty lake still answers lookups.
        REBUILDING,
        // serves the covered version
        COVERING,
        // serves the covered version while a SWITCH load reaches a newer one
        SWITCHING,
        // the bucket closed: requests go to the new leader, and jobs stop at their next unit
        CLOSED
    }

    final Kind kind;
    // the covered version, which lookups read; null once closed
    @Nullable final DvVersion version;
    // the covered snapshot, -1 for the empty lake, and its readable offset
    final long coveredSnapshotId;
    final long readableOffset;
    @Nullable final String identity;
    // the snapshot the last catch-up found unreachable, or Long.MIN_VALUE
    final long unreachable;

    DvState(
            Kind kind,
            @Nullable DvVersion version,
            long coveredSnapshotId,
            long readableOffset,
            @Nullable String identity,
            long unreachable) {
        this.kind = kind;
        this.version = version;
        this.coveredSnapshotId = coveredSnapshotId;
        this.readableOffset = readableOffset;
        this.identity = identity;
        this.unreachable = unreachable;
    }

    @Override
    public String toString() {
        return kind + " covered=" + coveredSnapshotId + " from=" + readableOffset;
    }
}
