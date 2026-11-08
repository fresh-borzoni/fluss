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

import org.apache.fluss.annotation.Internal;

import javax.annotation.Nullable;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Thrown when a deletion vector request names a lake snapshot other than the one the tablet
 * server's position index currently covers.
 *
 * <p>The server catches up to newer registered snapshots on its own, so a request for a newer
 * snapshot can be retried. A request for an older snapshot fails until the client plans again on
 * the snapshot the server covers. A request for a cut the server dropped fails too.
 *
 * <p>It is an {@link ApiException}, so it reaches the client as itself, with both snapshot ids.
 */
@Internal
public class StaleSnapshotException extends ApiException {
    private static final long serialVersionUID = 1L;

    private static final Pattern IDS = Pattern.compile("requested (-?\\d+), current (-?\\d+)");

    private final long currentSnapshotId;
    private final long requestedSnapshotId;

    public StaleSnapshotException(long currentSnapshotId, long requestedSnapshotId) {
        this(currentSnapshotId, requestedSnapshotId, null);
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
    }

    /** Recreates the exception from its message, as an RPC delivers it; ids not in it are -1. */
    public StaleSnapshotException(String message) {
        super(message);
        Matcher ids = message == null ? null : IDS.matcher(message);
        boolean found = ids != null && ids.find();
        this.requestedSnapshotId = found ? Long.parseLong(ids.group(1)) : -1L;
        this.currentSnapshotId = found ? Long.parseLong(ids.group(2)) : -1L;
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
