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

package org.apache.fluss.exception;

import org.apache.fluss.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Thrown when a deletion vector request names a lake snapshot other than the one the tablet
 * server's position index currently covers, or a cut the server no longer holds.
 *
 * <p>The reader plans the bucket again on the snapshot the server covers. A read that does not wait
 * for the server gets the same answer in its response instead; when the server still switches to
 * the requested snapshot from the one registered right before, the answer is {@link #isBehind()
 * behind}, and the reader reads that earlier snapshot.
 *
 * <p>It is an {@link ApiException}, so it reaches the client as itself, with both snapshot ids. An
 * error response carries only the message, so the message names them as {@code "requested <id>,
 * current <id>"}, which {@link #StaleSnapshotException(String)} reads back. That form is part of
 * the protocol.
 */
@PublicEvolving
public class StaleSnapshotException extends ApiException {
    private static final long serialVersionUID = 1L;

    private static final Pattern IDS = Pattern.compile("requested (-?\\d+), current (-?\\d+)");

    private final long currentSnapshotId;
    private final long requestedSnapshotId;
    // the leader covers the snapshot registered right before the requested one and answered a read
    // that does not wait at once
    private final boolean behind;
    // the readable offset of the current snapshot, where the reader reads its log from; -1 if not
    // known
    private final long currentStartOffset;

    public StaleSnapshotException(long currentSnapshotId, long requestedSnapshotId) {
        this(currentSnapshotId, requestedSnapshotId, null);
    }

    /**
     * The answer to a read that does not wait for the server, as {@code GetDvSnapshotResponse}
     * carries it: the leader covers {@code currentSnapshotId}, readable from {@code
     * currentStartOffset}, and is {@code behind} the requested snapshot or covers another one.
     */
    public StaleSnapshotException(
            long currentSnapshotId,
            long requestedSnapshotId,
            boolean behind,
            long currentStartOffset) {
        super(
                String.format(
                        "Stale snapshot: requested %d, current %d (%s)",
                        requestedSnapshotId,
                        currentSnapshotId,
                        behind
                                ? "the leader still switches to the requested snapshot"
                                : "the leader covers another snapshot"));
        this.currentSnapshotId = currentSnapshotId;
        this.requestedSnapshotId = requestedSnapshotId;
        this.behind = behind;
        this.currentStartOffset = currentStartOffset;
    }

    /** Appends {@code detail}, e.g. the bucket and the cut asked for, to the message. */
    public StaleSnapshotException(
            long currentSnapshotId, long requestedSnapshotId, @Nullable String detail) {
        super(
                String.format(
                        "Stale snapshot: requested %d, current %d%s",
                        requestedSnapshotId,
                        currentSnapshotId,
                        detail == null ? "" : " (" + detail + ")"));
        this.currentSnapshotId = currentSnapshotId;
        this.requestedSnapshotId = requestedSnapshotId;
        this.behind = false;
        this.currentStartOffset = -1L;
    }

    /** Recreates the exception from its message, as an RPC delivers it; ids not in it are -1. */
    public StaleSnapshotException(String message) {
        super(message);
        Matcher ids = message == null ? null : IDS.matcher(message);
        boolean found = ids != null && ids.find();
        this.requestedSnapshotId = found ? Long.parseLong(ids.group(1)) : -1L;
        this.currentSnapshotId = found ? Long.parseLong(ids.group(2)) : -1L;
        this.behind = false;
        this.currentStartOffset = -1L;
    }

    /**
     * Whether the leader covers the snapshot registered right before the requested one and answered
     * without waiting to switch to the requested one.
     */
    public boolean isBehind() {
        return behind;
    }

    /**
     * The readable offset of the current snapshot on the leader, from which a reader reads its log,
     * or -1 if the answer does not carry it.
     */
    public long getCurrentStartOffset() {
        return currentStartOffset;
    }

    /** Returns the lake snapshot ID the tablet server covers, or -1 for the empty lake. */
    public long getCurrentSnapshotId() {
        return currentSnapshotId;
    }

    /** Returns the snapshot ID that was requested by the client. */
    public long getRequestedSnapshotId() {
        return requestedSnapshotId;
    }
}
