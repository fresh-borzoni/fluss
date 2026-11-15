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

package org.apache.fluss.server.kv.snapshot;

import javax.annotation.Nullable;

import java.util.concurrent.Callable;

/**
 * Saves the change list of a bucket with derived deletion vectors with each KV snapshot, so that a
 * new leader replays the log only from the snapshot's offset. A KV snapshot never waits long for
 * it: the change list is written on the server's IO pool while the KV files upload, and one that is
 * not ready in time is left out (its segments serve the next snapshot). A new leader that restores
 * a KV snapshot without a change list replays from the readable offset.
 */
public interface DvChangeListCheckpoints {

    /**
     * Starts saving the change list of KV snapshot {@code kvSnapshotId}, whose log offset is {@code
     * logOffset}, into {@code location}'s shared directory; called in the snapshot's synchronous
     * phase.
     */
    Checkpoint prepare(long kvSnapshotId, long logOffset, SnapshotLocation location);

    /** The change list of one KV snapshot. */
    interface Checkpoint {
        /**
         * The change list to attach, in the snapshot's asynchronous phase after its KV files: the
         * one saved for it if it is ready within a short bound, else null or a part that ends
         * before the snapshot's offset, which only keeps segments referenced for the next snapshot
         * and is never used to rebuild. Logs the outcome. Never throws.
         */
        @Nullable
        DvChangeListHandle attach();

        /**
         * The snapshot was committed, with what {@link #attach} returned. {@code changeListKept}
         * reads back whether the committed metadata holds the change list attached: a coordinator
         * that does not know the field writes it without. The change list calls it off the
         * snapshot's thread, if it attached one, and carries nothing into the next snapshots until
         * it knows. If it returns false, the change list's new segments are deleted, as no snapshot
         * references them; if it throws, the outcome counts as unknown.
         */
        void committed(Callable<Boolean> changeListKept);

        /** The snapshot failed or was cancelled: it was not committed. */
        void aborted();

        /** Whether the snapshot was committed is not known. */
        void uncertain();
    }
}
