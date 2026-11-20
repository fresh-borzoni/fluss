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

import org.apache.fluss.annotation.Internal;

import javax.annotation.Nullable;

/**
 * A part of the log mask of a tiering round on a lake snapshot: the offsets in [readable offset,
 * round end) whose versions the part's changes supersede. The union of a round's parts is its mask.
 *
 * <p>A part also counts its changes, the records that supersede a version: the +U and -D records,
 * and on a table with row TTL the +I records too, as an insert there may follow an expired version.
 * A reader of the round counts the same records, so a round read in part does not pass.
 */
@Internal
public final class DvRoundMask {

    @Nullable private final byte[] bitmap;
    private final long startOffset;
    private final long nextOffset;
    private final long changes;
    private final boolean changesWithInserts;

    /** A part whose next part starts at {@code nextOffset}, the round's end if it is the last. */
    DvRoundMask(
            @Nullable byte[] bitmap,
            long startOffset,
            long nextOffset,
            long changes,
            boolean changesWithInserts) {
        this.bitmap = bitmap;
        this.startOffset = startOffset;
        this.nextOffset = nextOffset;
        this.changes = changes;
        this.changesWithInserts = changesWithInserts;
    }

    /** The serialized Roaring64Bitmap of the part's offsets, or null if it has none. */
    @Nullable
    public byte[] bitmap() {
        return bitmap;
    }

    /** The snapshot's readable offset, where the round starts. */
    public long startOffset() {
        return startOffset;
    }

    /** Where the next part starts; the round's end if this part is the last. */
    public long nextOffset() {
        return nextOffset;
    }

    /** The changes from the part's start to {@link #nextOffset()}. */
    public long changes() {
        return changes;
    }

    /** Whether the changes count the +I records, as on a table with row TTL. */
    public boolean changesWithInserts() {
        return changesWithInserts;
    }
}
