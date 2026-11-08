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
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.rpc.messages.PbLakeDvEntry;

import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * The deletion vector masks of one bucket for a union read, valid for the log up to {@link
 * #logEndOffset()}. A lake mask per data file holds the positions of rows whose keys changed in the
 * log after the snapshot. The log mask holds the offsets of versions superseded within the log.
 *
 * <p>A server that pages answers each part when the reader gets to it, all of the cut the first
 * response named: a lake split's file masks when the split is opened, and the log mask range by
 * range as the log is read. A server that does not page answers all masks of the bucket at once.
 * The reader holds them while it reads the bucket and drops each file's mask once the file is read.
 */
@Internal
public abstract class DvBucketMasks {

    private final long logStartOffset;
    private final long logEndOffset;

    DvBucketMasks(long logStartOffset, long logEndOffset) {
        this.logStartOffset = logStartOffset;
        this.logEndOffset = logEndOffset;
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
     * Returns the masks of the given lake data files by file name (files without a mask are
     * absent), or of all files if the names are unknown.
     */
    public abstract Map<String, Roaring64Bitmap> lakeMasksOf(@Nullable List<String> fileNames)
            throws IOException;

    /** Whether the log version at {@code offset} is superseded; offsets are asked in order. */
    public abstract boolean isLogVersionMasked(long offset) throws IOException;

    /** Creates the masks of a first response, which pages them if the server does. */
    static DvBucketMasks of(
            GetDvSnapshotResponse first,
            @Nullable List<String> firstFileNames,
            PageFetcher pageFetcher,
            int maxLogDvBytes) {
        if (first.hasPaged() && first.isPaged()) {
            return new Paged(first, firstFileNames, pageFetcher, maxLogDvBytes);
        }
        return new Full(first);
    }

    /** Creates the masks a started split kept in its state. */
    static DvBucketMasks fromState(DvBucketMaskState state) {
        return new Full(
                state.getLogStartOffset(),
                state.getLogEndOffset(),
                new HashMap<>(state.getLakeMasks()),
                state.getLogMask());
    }

    /**
     * Returns the masks of a response that holds them all, to keep with a started part. Returns
     * null for a paged response, whose cut the server keeps.
     */
    @Nullable
    static DvBucketMaskState stateOf(GetDvSnapshotResponse response) {
        if (response.hasPaged() && response.isPaged()) {
            return null;
        }
        return new DvBucketMaskState(
                response.getSnapshotStartOffset(),
                response.getLogEndOffset(),
                response.hasLakeSnapshotIdentity() ? response.getLakeSnapshotIdentity() : null,
                lakeEntries(response),
                response.hasLogDvBitmap() ? response.getLogDvBitmap() : null);
    }

    /** Fetches a part of the masks of the first response's cut. */
    interface PageFetcher {
        GetDvSnapshotResponse fetch(Function<GetDvSnapshotRequest, GetDvSnapshotRequest> part)
                throws IOException;
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

    public static Map<String, byte[]> lakeMasksOf(GetDvSnapshotResponse response) {
        return lakeEntries(response);
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

    private static Map<String, byte[]> lakeEntries(GetDvSnapshotResponse response) {
        Map<String, byte[]> entries = new HashMap<>();
        for (int i = 0; i < response.getLakeDvEntriesCount(); i++) {
            PbLakeDvEntry entry = response.getLakeDvEntryAt(i);
            // The bitmaps may be keyed by data file path; rows name their file by basename.
            entries.put(fileNameOf(entry.getFilePath()), entry.getDeletedPositionsBitmap());
        }
        return entries;
    }

    /** All masks of the bucket, from one response of a server that does not page. */
    static final class Full extends DvBucketMasks {

        private final Map<String, byte[]> lakeMasks;
        @Nullable private byte[] logMaskBytes;
        @Nullable private Roaring64Bitmap logMask;

        Full(GetDvSnapshotResponse response) {
            this(
                    response.getSnapshotStartOffset(),
                    response.getLogEndOffset(),
                    lakeEntries(response),
                    response.hasLogDvBitmap() ? response.getLogDvBitmap() : null);
        }

        Full(
                long logStartOffset,
                long logEndOffset,
                Map<String, byte[]> lakeMasks,
                @Nullable byte[] logMaskBytes) {
            super(logStartOffset, logEndOffset);
            this.lakeMasks = lakeMasks;
            this.logMaskBytes = logMaskBytes;
        }

        @Override
        public Map<String, Roaring64Bitmap> lakeMasksOf(@Nullable List<String> fileNames)
                throws IOException {
            Map<String, Roaring64Bitmap> masks = new HashMap<>();
            if (fileNames == null) {
                for (Map.Entry<String, byte[]> e : lakeMasks.entrySet()) {
                    masks.put(e.getKey(), deserialize(e.getValue(), e.getKey()));
                }
                return masks;
            }
            for (String fileName : fileNames) {
                // each file is read once: its mask is released with it
                byte[] bytes = lakeMasks.remove(fileName);
                if (bytes != null) {
                    masks.put(fileName, deserialize(bytes, fileName));
                }
            }
            return masks;
        }

        @Override
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
    }

    /** Masks fetched part by part from a server that pages, all of one cut. */
    static final class Paged extends DvBucketMasks {

        private final PageFetcher pageFetcher;
        private final int maxLogDvBytes;
        // the lake masks the first response carried, for the files it asked for
        @Nullable private Map<String, byte[]> firstLakeMasks;
        @Nullable private Set<String> firstFileNames;

        @Nullable private Roaring64Bitmap logPage;
        private long logPageFrom = -1;
        private long logPageTo = -1;

        Paged(
                GetDvSnapshotResponse first,
                @Nullable List<String> firstFileNames,
                PageFetcher pageFetcher,
                int maxLogDvBytes) {
            super(first.getSnapshotStartOffset(), first.getLogEndOffset());
            this.pageFetcher = pageFetcher;
            this.maxLogDvBytes = maxLogDvBytes;
            this.firstLakeMasks = lakeEntries(first);
            this.firstFileNames = firstFileNames == null ? null : new HashSet<>(firstFileNames);
        }

        @Override
        public Map<String, Roaring64Bitmap> lakeMasksOf(@Nullable List<String> fileNames)
                throws IOException {
            Map<String, byte[]> entries;
            if (fileNames != null
                    && firstLakeMasks != null
                    && firstFileNames != null
                    && firstFileNames.containsAll(fileNames)) {
                entries = firstLakeMasks;
            } else if (fileNames == null) {
                // A split that does not name its files needs every mask: the cut's masks unpaged.
                GetDvSnapshotResponse all = pageFetcher.fetch(request -> request.setPaged(false));
                entries = lakeEntries(all);
            } else if (fileNames.isEmpty()) {
                entries = Collections.emptyMap();
            } else {
                GetDvSnapshotResponse page =
                        pageFetcher.fetch(
                                request -> {
                                    for (String fileName : fileNames) {
                                        request.addLakeDvFileName(fileName);
                                    }
                                    return request;
                                });
                entries = lakeEntries(page);
            }
            firstLakeMasks = null;
            firstFileNames = null;
            Map<String, Roaring64Bitmap> masks = new HashMap<>();
            for (Map.Entry<String, byte[]> e : entries.entrySet()) {
                if (fileNames == null || fileNames.contains(e.getKey())) {
                    masks.put(e.getKey(), deserialize(e.getValue(), e.getKey()));
                }
            }
            return masks;
        }

        @Override
        public boolean isLogVersionMasked(long offset) throws IOException {
            if (offset >= logEndOffset()) {
                return false; // beyond the masks, never read
            }
            if (logPage == null || offset < logPageFrom || offset >= logPageTo) {
                GetDvSnapshotResponse page =
                        pageFetcher.fetch(
                                request ->
                                        request.setLogDvFromOffset(offset)
                                                .setMaxLogDvBytes(maxLogDvBytes));
                long to = page.hasLogDvToOffset() ? page.getLogDvToOffset() : logEndOffset();
                if (to <= offset) {
                    throw new IOException(
                            String.format(
                                    "The log mask page from offset %d ends at %d, before it"
                                            + " starts.",
                                    offset, to));
                }
                logPage =
                        page.hasLogDvBitmap()
                                ? deserialize(page.getLogDvBitmap(), "the log")
                                : new Roaring64Bitmap();
                logPageFrom = offset;
                logPageTo = to;
            }
            return logPage.contains(offset);
        }
    }
}
