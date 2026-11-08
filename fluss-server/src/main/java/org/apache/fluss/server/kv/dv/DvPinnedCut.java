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
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.server.utils.RoaringBitmapUtils;

import org.roaringbitmap.longlong.PeekableLongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * The frozen masks of a pinned cut: those of one lake snapshot up to one high watermark (the cut).
 * The workers of a union read fetch them in parts while the bucket's own masks move on. A cut never
 * changes once built, so any number of requests read it at once without a lock.
 *
 * <p>The log mask is served in pages of whole roaring containers (blocks of 2^16 offsets). A page
 * covers a range of offsets exactly and fits the size the reader asks for, unless a single
 * container is larger than that.
 */
@Internal
public final class DvPinnedCut {

    /** The log mask bytes one page carries unless the request says otherwise. */
    public static final int DEFAULT_LOG_PAGE_BYTES = 1 << 20;

    private static final int CONTAINER_BITS = 16;

    private final long snapshotId;
    private final long logEndOffset;
    private final long snapshotStartOffset;
    @Nullable private final String lakeSnapshotIdentity;
    // by data file name, the last path segment readers match on
    private final Map<String, LakeMask> lakeMasks;
    private final Roaring64Bitmap logMask;
    private final long bytes;
    // the high watermark when the masks were built
    private final long highWatermark;

    private DvPinnedCut(
            long snapshotId,
            long logEndOffset,
            long snapshotStartOffset,
            @Nullable String lakeSnapshotIdentity,
            Map<String, LakeMask> lakeMasks,
            Roaring64Bitmap logMask,
            long highWatermark) {
        this.snapshotId = snapshotId;
        this.logEndOffset = logEndOffset;
        this.highWatermark = highWatermark;
        this.snapshotStartOffset = snapshotStartOffset;
        this.lakeSnapshotIdentity = lakeSnapshotIdentity;
        this.lakeMasks = lakeMasks;
        this.logMask = logMask;
        long size = 128 + logMask.getLongSizeInBytes();
        for (LakeMask mask : lakeMasks.values()) {
            // the entry, the path and name strings, the bitmap bytes
            size += 96 + 2L * mask.path.length() + mask.bitmap.length;
        }
        this.bytes = size;
    }

    /**
     * Freezes the masks of a union read of {@code snapshotId}. The bitmaps are kept without a copy:
     * they are serialized copies, built in an owner step, and never change.
     */
    static DvPinnedCut freeze(long snapshotId, DvSnapshot masks) throws IOException {
        Map<String, LakeMask> lake = new HashMap<>();
        for (Map.Entry<String, byte[]> e : masks.getLakeDvEntries().entrySet()) {
            lake.put(fileNameOf(e.getKey()), new LakeMask(e.getKey(), e.getValue()));
        }
        Roaring64Bitmap log = new Roaring64Bitmap();
        if (masks.getLogDvBitmap() != null) {
            RoaringBitmapUtils.deserializeRoaringBitmap64(log, masks.getLogDvBitmap());
        }
        return new DvPinnedCut(
                snapshotId,
                masks.getLogEndOffset(),
                masks.getSnapshotStartOffset(),
                masks.getLakeSnapshotIdentity(),
                lake,
                log,
                masks.getHighWatermark());
    }

    static String fileNameOf(String path) {
        return path.substring(path.lastIndexOf('/') + 1);
    }

    public long snapshotId() {
        return snapshotId;
    }

    /** The cut: the offset (exclusive) the masks cover the log up to. */
    public long logEndOffset() {
        return logEndOffset;
    }

    public long snapshotStartOffset() {
        return snapshotStartOffset;
    }

    @Nullable
    public String lakeSnapshotIdentity() {
        return lakeSnapshotIdentity;
    }

    /** Estimated heap bytes. */
    public long bytes() {
        return bytes;
    }

    /** Whether {@code masks} are of the same commit and tiered offset as this cut. */
    boolean sameSnapshotAs(DvSnapshot masks) {
        return snapshotStartOffset == masks.getSnapshotStartOffset()
                && Objects.equals(lakeSnapshotIdentity, masks.getLakeSnapshotIdentity());
    }

    /**
     * Returns the masks of the named lake data files, by path. A file without a mask has no entry.
     * Names are matched on the last path segment.
     */
    public Map<String, byte[]> lakeMasksOf(Iterable<String> fileNames) {
        Map<String, byte[]> out = new HashMap<>();
        for (String name : fileNames) {
            LakeMask mask = lakeMasks.get(fileNameOf(name));
            if (mask != null) {
                out.put(mask.path, mask.bitmap);
            }
        }
        return out;
    }

    /** All masks of the cut at once, to compare them. */
    @VisibleForTesting
    DvSnapshot toDvSnapshot() throws IOException {
        Map<String, byte[]> lake = new HashMap<>();
        for (LakeMask mask : lakeMasks.values()) {
            lake.put(mask.path, mask.bitmap);
        }
        return new DvSnapshot(
                lake,
                logMask.isEmpty() ? null : serialize(logMask),
                logEndOffset,
                snapshotStartOffset,
                lakeSnapshotIdentity,
                highWatermark);
    }

    /**
     * Returns the log mask from offset {@code from} on: the masked offsets of {@code [from, to)} in
     * at most {@code maxBytes} serialized bytes, but at least one container. {@code to} is past
     * {@code from} unless {@code from} is the cut.
     */
    public LogPage logPage(long from, int maxBytes) throws IOException {
        checkArgument(
                from >= 0 && from <= logEndOffset,
                "The log mask is requested from offset %s, outside [0, %s].",
                from,
                logEndOffset);
        checkArgument(maxBytes > 0, "A log mask page of %s bytes is requested.", maxBytes);
        // Collects the page's containers, each with a masked offset of [from, logEndOffset). A
        // container serialized alone takes at least its share of a page, so their sum bounds the
        // page. The serialized page is still checked below.
        List<Long> containers = new ArrayList<>();
        long estimate = 0;
        long to = logEndOffset;
        for (long next = nextMasked(from); next >= 0; ) {
            long container = next >>> CONTAINER_BITS;
            long size = slice(from, Collections.singletonList(container)).serializedSizeInBytes();
            if (!containers.isEmpty() && estimate + size > maxBytes) {
                to = next;
                break;
            }
            containers.add(container);
            estimate += size;
            next = nextMasked((container + 1) << CONTAINER_BITS);
        }
        if (containers.isEmpty()) {
            return new LogPage(from, to, null);
        }
        while (true) {
            byte[] page = serialize(slice(from, containers));
            if (page.length <= maxBytes || containers.size() == 1) {
                return new LogPage(from, to, page);
            }
            // the bound fell short: keep half the containers
            int keep = containers.size() / 2;
            to = containers.get(keep) << CONTAINER_BITS;
            containers = containers.subList(0, keep);
        }
    }

    /** The first masked offset at or after {@code offset} below the cut, or -1. */
    private long nextMasked(long offset) {
        // a fresh iterator: advancing one past the last value leaves it where it was
        PeekableLongIterator it = logMask.getLongIteratorFrom(offset);
        if (!it.hasNext()) {
            return -1;
        }
        long next = it.peekNext();
        return next < logEndOffset ? next : -1;
    }

    /** The masked offsets of {@code containers} from {@code from} on, below the cut. */
    private Roaring64Bitmap slice(long from, List<Long> containers) {
        Roaring64Bitmap slice = new Roaring64Bitmap();
        for (long container : containers) {
            long start = Math.max(from, container << CONTAINER_BITS);
            long end = Math.min(logEndOffset, (container + 1) << CONTAINER_BITS);
            slice.addRange(start, end);
        }
        // reads the cut's bitmap only
        slice.and(logMask);
        return slice;
    }

    /** Serializes without optimizing, which would change the bitmap. */
    private static byte[] serialize(Roaring64Bitmap bitmap) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream((int) bitmap.serializedSizeInBytes());
        try (DataOutputStream data = new DataOutputStream(out)) {
            bitmap.serialize(data);
        }
        return out.toByteArray();
    }

    /** The mask of one lake data file. */
    private static final class LakeMask {
        final String path;
        final byte[] bitmap;

        LakeMask(String path, byte[] bitmap) {
            this.path = path;
            this.bitmap = bitmap;
        }
    }

    /** The log mask of {@code [from, to)}; {@code bitmap} is null if none of it is masked. */
    public static final class LogPage {
        private final long from;
        private final long to;
        @Nullable private final byte[] bitmap;

        LogPage(long from, long to, @Nullable byte[] bitmap) {
            this.from = from;
            this.to = to;
            this.bitmap = bitmap;
        }

        public long from() {
            return from;
        }

        public long to() {
            return to;
        }

        @Nullable
        public byte[] bitmap() {
            return bitmap;
        }
    }
}
