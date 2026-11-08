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

import java.util.Map;

/**
 * The masks of one union read of a bucket, derived from the covered snapshot and the high
 * watermark. The lake masks hide lake rows whose keys changed in Fluss but are not tiered yet. They
 * are computed from the primary-key index when a read asks for them, not kept on the write path.
 *
 * <ul>
 *   <li>lakeDvEntries: per data file name, the serialized Roaring64Bitmap of the row positions to
 *       skip.
 *   <li>logDvBitmap: the serialized Roaring64Bitmap of the log offsets to skip within
 *       [snapshotStartOffset, logEndOffset), or null if there are none.
 *   <li>logEndOffset: the cut, the high watermark the masks cover the log up to.
 *   <li>snapshotStartOffset: the tiered offset of the covered snapshot.
 *   <li>lakeSnapshotIdentity: the commit the covered snapshot's id named when Fluss registered it,
 *       or null for a lake that never reuses snapshot ids.
 * </ul>
 */
@Internal
public class DvSnapshot {

    private final Map<String, byte[]> lakeDvEntries;
    @Nullable private final byte[] logDvBitmap;
    private final long logEndOffset;
    private final long snapshotStartOffset;
    @Nullable private final String lakeSnapshotIdentity;
    // the high watermark when the masks were built, at or after logEndOffset
    private final long highWatermark;

    public DvSnapshot(
            Map<String, byte[]> lakeDvEntries,
            @Nullable byte[] logDvBitmap,
            long logEndOffset,
            long snapshotStartOffset) {
        this(lakeDvEntries, logDvBitmap, logEndOffset, snapshotStartOffset, null);
    }

    public DvSnapshot(
            Map<String, byte[]> lakeDvEntries,
            @Nullable byte[] logDvBitmap,
            long logEndOffset,
            long snapshotStartOffset,
            @Nullable String lakeSnapshotIdentity) {
        this(
                lakeDvEntries,
                logDvBitmap,
                logEndOffset,
                snapshotStartOffset,
                lakeSnapshotIdentity,
                logEndOffset);
    }

    public DvSnapshot(
            Map<String, byte[]> lakeDvEntries,
            @Nullable byte[] logDvBitmap,
            long logEndOffset,
            long snapshotStartOffset,
            @Nullable String lakeSnapshotIdentity,
            long highWatermark) {
        this.lakeDvEntries = lakeDvEntries;
        this.logDvBitmap = logDvBitmap;
        this.logEndOffset = logEndOffset;
        this.snapshotStartOffset = snapshotStartOffset;
        this.lakeSnapshotIdentity = lakeSnapshotIdentity;
        this.highWatermark = Math.max(highWatermark, logEndOffset);
    }

    /** The same masks, served when the bucket's high watermark is {@code highWatermark}. */
    public DvSnapshot withHighWatermark(long highWatermark) {
        return new DvSnapshot(
                lakeDvEntries,
                logDvBitmap,
                logEndOffset,
                snapshotStartOffset,
                lakeSnapshotIdentity,
                highWatermark);
    }

    /**
     * The bucket's high watermark when the masks were built. They stop below it while the write
     * path has not handed over the changes in between.
     */
    public long getHighWatermark() {
        return highWatermark;
    }

    /** The commit the covered snapshot's id named when it was registered, or null. */
    @Nullable
    public String getLakeSnapshotIdentity() {
        return lakeSnapshotIdentity;
    }

    /** Returns the masks of lake data files: file name to serialized Roaring64Bitmap. */
    public Map<String, byte[]> getLakeDvEntries() {
        return lakeDvEntries;
    }

    /** Returns the serialized Roaring64Bitmap of masked log offsets. Null if empty. */
    @Nullable
    public byte[] getLogDvBitmap() {
        return logDvBitmap;
    }

    /** Returns the offset the masks cover the log up to (exclusive). */
    public long getLogEndOffset() {
        return logEndOffset;
    }

    /** Returns the tiered offset of the covered lake snapshot. */
    public long getSnapshotStartOffset() {
        return snapshotStartOffset;
    }
}
