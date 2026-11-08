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

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.lake.source.LakeSplit;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The masks a {@link DvBucketPart} is read with, all of one cut: the lake data files' bitmaps by
 * file name, and the log's bitmap. An engine keeps them with a started part, because a restored
 * part needs masks of the same cut and the server may have dropped that cut.
 */
@PublicEvolving
public final class DvBucketMaskState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long logStartOffset;
    private final long logEndOffset;
    @Nullable private final String lakeSnapshotIdentity;
    // serialized bitmaps of the lake files by file name
    private final Map<String, byte[]> lakeMasks;
    @Nullable private final byte[] logMask;

    public DvBucketMaskState(
            long logStartOffset,
            long logEndOffset,
            @Nullable String lakeSnapshotIdentity,
            Map<String, byte[]> lakeMasks,
            @Nullable byte[] logMask) {
        this.logStartOffset = logStartOffset;
        this.logEndOffset = logEndOffset;
        this.lakeSnapshotIdentity = lakeSnapshotIdentity;
        this.lakeMasks = lakeMasks;
        this.logMask = logMask;
    }

    public long getLogStartOffset() {
        return logStartOffset;
    }

    public long getLogEndOffset() {
        return logEndOffset;
    }

    @Nullable
    public String getLakeSnapshotIdentity() {
        return lakeSnapshotIdentity;
    }

    public Map<String, byte[]> getLakeMasks() {
        return lakeMasks;
    }

    @Nullable
    public byte[] getLogMask() {
        return logMask;
    }

    /**
     * Returns these masks with only the lake masks of the lake splits from {@code fromLakeSplit}
     * on.
     */
    public DvBucketMaskState withoutLakeSplitsBefore(
            int fromLakeSplit, @Nullable List<LakeSplit> lakeSplits) {
        if (lakeMasks.isEmpty()) {
            return this;
        }
        int count = lakeSplits == null ? 0 : lakeSplits.size();
        if (fromLakeSplit >= count) {
            return new DvBucketMaskState(
                    logStartOffset,
                    logEndOffset,
                    lakeSnapshotIdentity,
                    Collections.emptyMap(),
                    logMask);
        }
        Set<String> remaining = new HashSet<>();
        for (int i = fromLakeSplit; i < count; i++) {
            List<String> fileNames = lakeSplits.get(i).fileNames();
            if (fileNames == null) {
                // a split that does not name its files may need any mask
                return this;
            }
            remaining.addAll(fileNames);
        }
        Map<String, byte[]> kept = new HashMap<>();
        for (Map.Entry<String, byte[]> e : lakeMasks.entrySet()) {
            if (remaining.contains(e.getKey())) {
                kept.put(e.getKey(), e.getValue());
            }
        }
        return kept.size() == lakeMasks.size()
                ? this
                : new DvBucketMaskState(
                        logStartOffset, logEndOffset, lakeSnapshotIdentity, kept, logMask);
    }

    /** The bytes of the bitmaps held. */
    public long maskBytes() {
        long bytes = logMask == null ? 0 : logMask.length;
        for (byte[] mask : lakeMasks.values()) {
            bytes += mask.length;
        }
        return bytes;
    }
}
