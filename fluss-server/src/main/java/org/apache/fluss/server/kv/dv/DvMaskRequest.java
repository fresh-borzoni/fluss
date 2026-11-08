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

import java.util.Collections;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Which masks a union read asks a bucket for.
 *
 * <ul>
 *   <li>Neither paged nor a cut: all masks up to the high watermark, in one response.
 *   <li>Paged without a cut: pins the masks up to the high watermark as a cut and answers with the
 *       requested parts of them.
 *   <li>A cut: the masks of that pinned cut, the requested parts if paged, all of them otherwise.
 * </ul>
 *
 * <p>The parts are the masks of the named lake data files and the log mask from {@code
 * logFromOffset} on, at most {@code maxLogBytes} of it.
 */
@Internal
public final class DvMaskRequest {

    /** All masks up to the high watermark, as a request without the paging fields gets them. */
    public static final DvMaskRequest ALL =
            new DvMaskRequest(null, false, Collections.emptyList(), null, 1);

    @Nullable private final Long cut;
    private final boolean paged;
    private final List<String> lakeFileNames;
    @Nullable private final Long logFromOffset;
    private final int maxLogBytes;

    public DvMaskRequest(
            @Nullable Long cut,
            boolean paged,
            List<String> lakeFileNames,
            @Nullable Long logFromOffset,
            int maxLogBytes) {
        checkArgument(maxLogBytes > 0, "A log mask page of %s bytes is requested.", maxLogBytes);
        this.cut = cut;
        this.paged = paged;
        this.lakeFileNames = lakeFileNames;
        this.logFromOffset = logFromOffset;
        this.maxLogBytes = maxLogBytes;
    }

    /** Whether the request names or pins a cut; otherwise it gets all masks in one response. */
    public boolean usesCut() {
        return paged || cut != null;
    }

    /** The cut a read pinned earlier, or null to pin one. */
    @Nullable
    public Long cut() {
        return cut;
    }

    public boolean paged() {
        return paged;
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
                + ", paged="
                + paged
                + ", lakeFiles="
                + lakeFileNames.size()
                + ", logFrom="
                + logFromOffset
                + ", maxLogBytes="
                + maxLogBytes
                + '}';
    }
}
