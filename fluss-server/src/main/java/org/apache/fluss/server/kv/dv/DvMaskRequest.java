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

import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Which masks of a cut a union read asks a bucket for. Without a cut the request pins one: the
 * bucket's masks up to the high watermark, frozen until no request asks for them for the idle
 * timeout ({@link DerivedDvPinnedCuts}).
 *
 * <p>The response carries the masks of the named lake data files and the log mask from {@code
 * logFromOffset} on, at most {@code maxLogBytes} of it.
 */
@Internal
public final class DvMaskRequest {

    @Nullable private final Long cut;
    private final List<String> lakeFileNames;
    @Nullable private final Long logFromOffset;
    private final int maxLogBytes;
    private final boolean waitForCatchUp;
    @Nullable private final Long pinRequestId;

    public DvMaskRequest(
            @Nullable Long cut,
            List<String> lakeFileNames,
            @Nullable Long logFromOffset,
            int maxLogBytes,
            boolean waitForCatchUp,
            @Nullable Long pinRequestId) {
        checkArgument(maxLogBytes > 0, "A log mask page of %s bytes is requested.", maxLogBytes);
        this.cut = cut;
        this.lakeFileNames = lakeFileNames;
        this.logFromOffset = logFromOffset;
        this.maxLogBytes = maxLogBytes;
        this.waitForCatchUp = waitForCatchUp;
        this.pinRequestId = pinRequestId;
    }

    /**
     * Whether the read waits for its bucket's leader: to switch to the requested snapshot, to
     * rebuild or replay its masks, and to finish lake-position lookups. If not, the leader answers
     * at once (see {@link DerivedDvManager#pinCut(long, boolean, Long)}).
     */
    public boolean waitForCatchUp() {
        return waitForCatchUp;
    }

    /**
     * The id of a request that pins a cut, the same in its retries, or null. A retry of a pin the
     * server took already gets that cut again without another pin.
     */
    @Nullable
    public Long pinRequestId() {
        return pinRequestId;
    }

    /** The cut a read pinned earlier, or null to pin one. */
    @Nullable
    public Long cut() {
        return cut;
    }

    public List<String> lakeFileNames() {
        return lakeFileNames;
    }

    @Nullable
    public Long logFromOffset() {
        return logFromOffset;
    }

    public int maxLogBytes() {
        return maxLogBytes;
    }

    @Override
    public String toString() {
        return "DvMaskRequest{cut="
                + cut
                + ", lakeFiles="
                + lakeFileNames.size()
                + ", logFrom="
                + logFromOffset
                + ", maxLogBytes="
                + maxLogBytes
                + ", waitForCatchUp="
                + waitForCatchUp
                + ", pinRequestId="
                + pinRequestId
                + '}';
    }
}
