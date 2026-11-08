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
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.rpc.messages.PbLakeDvEntry;

import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The deletion vector masks of one bucket for a union read, valid for the log up to {@link
 * #logEndOffset()}. A lake mask per data file holds the positions of rows whose keys changed in the
 * log after the snapshot. The log mask holds the offsets of versions superseded within the log.
 *
 * <p>The reader fetches all masks of its part when it starts, as the leader may drop the cut while
 * the part is read, or derives them from the keys. It holds them while it reads the part and drops
 * each file's mask once the file is read.
 */
@Internal
public final class DvBucketMasks {

    private final long logStartOffset;
    private final long logEndOffset;
    private final Map<String, byte[]> lakeMasks;
    @Nullable private byte[] logMaskBytes;
    @Nullable private Roaring64Bitmap logMask;

    private DvBucketMasks(
            long logStartOffset,
            long logEndOffset,
            Map<String, byte[]> lakeMasks,
            @Nullable byte[] logMaskBytes) {
        this.logStartOffset = logStartOffset;
        this.logEndOffset = logEndOffset;
        this.lakeMasks = lakeMasks;
        this.logMaskBytes = logMaskBytes;
    }

    /** Creates the masks a part fetched or derived, as it keeps them in its state. */
    static DvBucketMasks fromState(DvBucketMaskState state) {
        return new DvBucketMasks(
                state.getLogStartOffset(),
                state.getLogEndOffset(),
                new HashMap<>(state.getLakeMasks()),
                state.getLogMask());
    }

    /** The offset the log masks start from, the snapshot's readable offset. */
    public long logStartOffset() {
        return logStartOffset;
    }

    /** The offset (exclusive) the masks cover the log up to: the read stops there. */
    public long logEndOffset() {
        return logEndOffset;
    }

    /**
     * Returns the masks of the given lake data files by file name; files without a mask are absent.
     * Each file is read once, so its mask is released with it.
     */
    public Map<String, Roaring64Bitmap> lakeMasksOf(List<String> fileNames) throws IOException {
        Map<String, Roaring64Bitmap> masks = new HashMap<>();
        for (String fileName : fileNames) {
            byte[] bytes = lakeMasks.remove(fileName);
            if (bytes != null) {
                masks.put(fileName, deserialize(bytes, fileName));
            }
        }
        return masks;
    }

    /** Whether the log version at {@code offset} is superseded. */
    public boolean isLogVersionMasked(long offset) throws IOException {
        if (logMask == null) {
            logMask =
                    logMaskBytes == null
                            ? new Roaring64Bitmap()
                            : deserialize(logMaskBytes, "the log");
            logMaskBytes = null;
        }
        return logMask.contains(offset);
    }

    public static String fileNameOf(String path) {
        return path.substring(path.lastIndexOf('/') + 1);
    }

    public static Roaring64Bitmap deserialize(byte[] bytes, String what) throws IOException {
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        if (bytes.length > 0) {
            try {
                bitmap.deserialize(ByteBuffer.wrap(bytes));
            } catch (IOException | RuntimeException e) {
                throw new IOException("Failed to deserialize the deletion vector of " + what, e);
            }
        }
        return bitmap;
    }

    /** The masks of the lake data files a response carries, by file name. */
    public static Map<String, byte[]> lakeMasksOf(GetDvSnapshotResponse response) {
        Map<String, byte[]> entries = new HashMap<>();
        for (int i = 0; i < response.getLakeDvEntriesCount(); i++) {
            PbLakeDvEntry entry = response.getLakeDvEntryAt(i);
            // The bitmaps may be keyed by data file path; rows name their file by basename.
            entries.put(fileNameOf(entry.getFilePath()), entry.getDeletedPositionsBitmap());
        }
        return entries;
    }

    public static byte[] serialize(Roaring64Bitmap bitmap) throws IOException {
        bitmap.runOptimize();
        try (java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
                java.io.DataOutputStream out = new java.io.DataOutputStream(bytes)) {
            bitmap.serialize(out);
            out.flush();
            return bytes.toByteArray();
        }
    }
}
