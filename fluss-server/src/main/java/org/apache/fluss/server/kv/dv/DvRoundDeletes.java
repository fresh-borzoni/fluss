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

import java.util.Map;

/**
 * A part of the deletes of a tiering round on a lake snapshot: per lake data file of the snapshot,
 * the serialized positions of the rows whose keys changed in the log from the snapshot's readable
 * offset up to the round's end.
 */
@Internal
public final class DvRoundDeletes {

    private final Map<String, byte[]> deletes;
    private final long startOffset;
    private final long nextOffset;

    /** A part whose next part starts at {@code nextOffset}, the round's end if it is the last. */
    DvRoundDeletes(Map<String, byte[]> deletes, long startOffset, long nextOffset) {
        this.deletes = deletes;
        this.startOffset = startOffset;
        this.nextOffset = nextOffset;
    }

    /** The serialized positions of the part by data file path. */
    public Map<String, byte[]> deletes() {
        return deletes;
    }

    /** The snapshot's readable offset, where the round starts. */
    public long startOffset() {
        return startOffset;
    }

    /** Where the next part starts; the round's end if this part is the last. */
    public long nextOffset() {
        return nextOffset;
    }
}
