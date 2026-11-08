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

package org.apache.fluss.client.table.scanner.batch.dv;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.rpc.messages.GetDvSnapshotRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * The cut a part's leader pinned for the part when it started unpinned at planning: held from the
 * leader's answer to the pin until {@link DvPartMachine} leaves the cut or the part ends, then
 * released once. Best effort: a cut not released is dropped once it idles. Cuts pinned at planning
 * are not held here; the engine that planned them releases them.
 */
@Internal
final class DvOwnPin {

    private static final Logger LOG = LoggerFactory.getLogger(DvOwnPin.class);

    private final DvBucketPart part;
    private final DvBucketPartReader.Context context;

    private long snapshotId = Long.MIN_VALUE;
    private long cut = -1;

    DvOwnPin(DvBucketPart part, DvBucketPartReader.Context context) {
        this.part = part;
        this.context = context;
    }

    /** Holds the cut the leader pinned, before anything checks the answer. */
    void hold(long snapshotId, long cut) {
        checkState(!isHeld(), "%s pins a cut while it holds cut %s.", part.describe(), this.cut);
        this.snapshotId = snapshotId;
        this.cut = cut;
    }

    boolean isHeld() {
        return cut >= 0;
    }

    /** Releases the cut if held; forgets it first, so it is released once. */
    void release() {
        if (!isHeld()) {
            return;
        }
        GetDvSnapshotRequest release =
                DvMaskFetcher.releaseRequest(part.getTableBucket(), snapshotId, cut, 1);
        cut = -1;
        try {
            context.maskFetcher().release(release);
        } catch (RuntimeException e) {
            LOG.warn("Could not release the mask cut of {}.", part.describe(), e);
        }
    }
}
