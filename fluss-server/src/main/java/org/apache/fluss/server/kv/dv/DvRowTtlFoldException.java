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

/**
 * A pin of a bucket with row TTL on Paimon, whose readable offset moved below the offset its TTL
 * filter was capped for, so its masks may miss a version that expired from the KV before it was
 * tiered ({@link DvRowTtlGuard}): the reader reads the bucket by key up to {@link #cut()}, whether
 * it waits or not, which is exact whatever expired. No masks are built or pinned for it.
 */
@Internal
public final class DvRowTtlFoldException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final long cut;

    DvRowTtlFoldException(long snapshotId, long cut) {
        super(
                String.format(
                        "Row TTL may have removed a version of the log after lake snapshot %d"
                                + " before it was tiered: read the bucket by key up to %d",
                        snapshotId, cut),
                null,
                false,
                false);
        this.cut = cut;
    }

    /** The offset the reader reads the bucket's log by key up to. */
    public long cut() {
        return cut;
    }
}
