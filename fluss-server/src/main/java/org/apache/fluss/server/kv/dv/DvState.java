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
 * What union reads of a bucket see of its state at one point: an immutable value, so a read decides
 * on one consistent state ({@link DvReadPolicy}).
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
        // Serves the covered version's lookups while a TAIL load replays its change list, and
        // masks once it is installed; reads that do not wait read by key meanwhile.
        REPLAYING,
        // the bucket closed: requests go to the new leader, and jobs stop at their next unit
        CLOSED
    }

    final Kind kind;
    // the covered version, which lookups read; null once closed
    @Nullable final DvVersion version;
    // the covered snapshot, -1 for the empty lake, and where its change list starts
    final long coveredSnapshotId;
    final long readableOffset;
    @Nullable final String identity;
    // the change list of the covered snapshot is complete; else it is replayed first
    final boolean changeListPresent;
    // the change list waits for a replay that backs off, after a disk failure or a hand-off
    // overflow: reads do not start it
    final boolean replayRetryPending;
    // no lake-position lookup is pending: the masks are exact up to the changes applied
    final boolean lookupsDrained;
    // the pending lookups are a rebuild's or a replay's, which may take long
    final boolean rebuiltLookups;
    // the newest frontier of the covered state, or null
    @Nullable final DvFrontier latest;
    // the frontier kept from the state before, or null
    @Nullable final DvFrontier kept;
    // the snapshot registered right after the covered one, the covered one it follows, and the
    // newest registration heard of; Long.MIN_VALUE if none
    final long nextRegistration;
    final long nextRegistrationOf;
    final long newestRegistration;
    // the snapshot the last catch-up found unreachable, or Long.MIN_VALUE
    final long unreachable;
    // the hand-off failures the owner had taken when it published this state
    final long handOffFailuresTaken;

    DvState(
            Kind kind,
            @Nullable DvVersion version,
            long coveredSnapshotId,
            long readableOffset,
            @Nullable String identity,
            boolean changeListPresent,
            boolean replayRetryPending,
            boolean lookupsDrained,
            boolean rebuiltLookups,
            @Nullable DvFrontier latest,
            @Nullable DvFrontier kept,
            long nextRegistration,
            long nextRegistrationOf,
            long newestRegistration,
            long unreachable,
            long handOffFailuresTaken) {
        this.kind = kind;
        this.version = version;
        this.coveredSnapshotId = coveredSnapshotId;
        this.readableOffset = readableOffset;
        this.identity = identity;
        this.changeListPresent = changeListPresent;
        this.replayRetryPending = replayRetryPending;
        this.lookupsDrained = lookupsDrained;
        this.rebuiltLookups = rebuiltLookups;
        this.latest = latest;
        this.kept = kept;
        this.nextRegistration = nextRegistration;
        this.nextRegistrationOf = nextRegistrationOf;
        this.newestRegistration = newestRegistration;
        this.unreachable = unreachable;
        this.handOffFailuresTaken = handOffFailuresTaken;
    }

    /**
     * Whether a read of {@code requested} reads the covered snapshot instead: the bucket covers the
     * snapshot registered right before it, that one is the newest registered, and the change list
     * is in place. The covered snapshot is the previous readable one when the read was planned,
     * whose files both lakes still keep.
     */
    boolean servesTheSnapshotBefore(long requested) {
        return coveredSnapshotId >= 0
                && nextRegistrationOf == coveredSnapshotId
                && nextRegistration == requested
                && newestRegistration == requested
                && changeListPresent;
    }

    @Override
    public String toString() {
        return kind
                + " covered="
                + coveredSnapshotId
                + " from="
                + readableOffset
                + " present="
                + changeListPresent
                + " drained="
                + lookupsDrained
                + " latest="
                + (latest == null ? "-" : latest.cut())
                + " kept="
                + (kept == null ? "-" : kept.snapshotId + "@" + kept.cut());
    }
}
