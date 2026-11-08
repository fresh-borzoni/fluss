/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Thrown when a bucket's deletion vector state cannot answer a request yet. The bucket is switching
 * to or rebuilding at the requested lake snapshot, or replaying its change list, in the background.
 * The request is retried after a backoff.
 *
 * <p>A read that does not wait for the server is answered at once by a leader that has nothing to
 * serve yet ({@link #isRebuilding()}), in its response. The answer names a cut, and the reader
 * reads the bucket's log up to it by key instead of with masks.
 */
@PublicEvolving
public class DeletionVectorsNotReadyException extends RetriableException {

    private static final long serialVersionUID = 1L;

    // the cut a reader reads the bucket up to by key, -1 if the leader named none
    private final long rebuildingCut;

    public DeletionVectorsNotReadyException(String message) {
        this(message, null, -1L);
    }

    public DeletionVectorsNotReadyException(String message, Throwable cause) {
        this(message, cause, -1L);
    }

    private DeletionVectorsNotReadyException(
            String message, @Nullable Throwable cause, long rebuildingCut) {
        super(message, cause);
        this.rebuildingCut = rebuildingCut;
    }

    /**
     * The answer of a leader that has nothing to serve yet, for {@code cause}: the reader reads the
     * bucket's log by key up to {@code cut}.
     */
    public static DeletionVectorsNotReadyException rebuilding(
            String detail, String cause, long cut) {
        checkArgument(cut >= 0, "A read by key up to log offset %s.", cut);
        return new DeletionVectorsNotReadyException(
                String.format(
                        "%s (the leader has no masks to serve yet: %s; read by key up to log"
                                + " offset %d)",
                        detail, cause, cut),
                null,
                cut);
    }

    /** Whether the leader has nothing to serve yet and named a cut. */
    public boolean isRebuilding() {
        return rebuildingCut >= 0;
    }

    /** The cut a reader reads the bucket up to by key, or -1 if not {@link #isRebuilding()}. */
    public long getRebuildingCut() {
        return rebuildingCut;
    }
}
