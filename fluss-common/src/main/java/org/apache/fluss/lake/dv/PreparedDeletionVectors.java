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

package org.apache.fluss.lake.dv;

import org.apache.fluss.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * The round a tiering writer prepared its bucket's deletion vectors for: the lake snapshot whose
 * deletion vectors they replace and the offsets of the round. The lake's write result holds the
 * deletion vectors themselves.
 *
 * @since 1.1
 */
@PublicEvolving
public final class PreparedDeletionVectors implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long snapshotId;
    private final long startOffset;
    private final long endOffset;

    public PreparedDeletionVectors(long snapshotId, long startOffset, long endOffset) {
        this.snapshotId = snapshotId;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    /** The lake snapshot the deletion vectors were prepared on. */
    public long snapshotId() {
        return snapshotId;
    }

    /** The offset the bucket's round starts at, the snapshot's offset of the bucket. */
    public long startOffset() {
        return startOffset;
    }

    /** The offset the bucket's round ends at. */
    public long endOffset() {
        return endOffset;
    }

    @Override
    public String toString() {
        return "PreparedDeletionVectors{snapshotId="
                + snapshotId
                + ", startOffset="
                + startOffset
                + ", endOffset="
                + endOffset
                + '}';
    }
}
