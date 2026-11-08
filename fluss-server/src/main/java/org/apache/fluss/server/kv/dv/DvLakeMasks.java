/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.kv.dv;

import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.server.utils.RoaringBitmapUtils;

import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The lake masks of a mask state: per lake file, the row positions whose rows a change superseded.
 * Each file's bitmap is serialized again only after it changed: a frontier build serializes the
 * files whose masks changed since the last build ({@link #serializeDirty}), a bounded amount per
 * step, so its cost follows the changes since the last build, not the size of the masks. The
 * owner's, as the mask state is.
 */
final class DvLakeMasks {

    private final Map<Integer, Roaring64Bitmap> masks = new HashMap<>();
    // each file's bitmap as last serialized, and its path; frontiers share the arrays, which are
    // never changed
    private final Map<Integer, byte[]> serialized = new HashMap<>();
    private final Map<Integer, String> paths = new HashMap<>();
    // the files whose bitmap changed since it was last serialized, in the order they changed
    private final Set<Integer> dirty = new LinkedHashSet<>();
    private long serializedBytes;
    // each file's bitmap heap as last measured, and their sum
    private final Map<Integer, Long> heapSizes = new HashMap<>();
    private long heapBytes;

    /** Adds the positions found to the masks. */
    void addAll(List<FilePos> found) {
        for (FilePos fp : found) {
            if (fp != null) {
                masks.computeIfAbsent(fp.fileId(), k -> new Roaring64Bitmap())
                        .addLong(fp.rowPosition());
                dirty.add(fp.fileId());
            }
        }
    }

    /** The mask of a file, or null; the caller does not change it. */
    @Nullable
    Roaring64Bitmap get(int fileId) {
        return masks.get(fileId);
    }

    /** Removes {@code positions} from the mask of a file; returns how many were in it. */
    long remove(int fileId, Roaring64Bitmap positions) {
        Roaring64Bitmap mask = masks.get(fileId);
        if (mask == null) {
            return 0;
        }
        long before = mask.getLongCardinality();
        mask.andNot(positions);
        long removed = before - mask.getLongCardinality();
        if (mask.isEmpty()) {
            drop(fileId);
        } else if (removed > 0) {
            dirty.add(fileId);
        }
        return removed;
    }

    /** Drops the masks of files a newer snapshot removed. */
    void dropFiles(Collection<Integer> fileIds) {
        for (int id : fileIds) {
            drop(id);
        }
    }

    private void drop(int fileId) {
        masks.remove(fileId);
        byte[] b = serialized.remove(fileId);
        if (b != null) {
            serializedBytes -= b.length;
        }
        paths.remove(fileId);
        dirty.remove(fileId);
        Long size = heapSizes.remove(fileId);
        if (size != null) {
            heapBytes -= size;
        }
    }

    void clear() {
        masks.clear();
        serialized.clear();
        paths.clear();
        dirty.clear();
        serializedBytes = 0;
        heapSizes.clear();
        heapBytes = 0;
    }

    /** The files with masks. */
    int files() {
        return masks.size();
    }

    /**
     * Serializes the bitmaps of changed files until about {@code maxBytes} are serialized; a single
     * file's bitmap is serialized whole. Returns whether every file is serialized as it is now.
     */
    boolean serializeDirty(long maxBytes) throws IOException {
        long done = 0;
        Iterator<Integer> it = dirty.iterator();
        while (it.hasNext() && done < maxBytes) {
            int id = it.next();
            it.remove();
            Roaring64Bitmap mask = masks.get(id);
            if (mask == null) {
                continue;
            }
            byte[] b = RoaringBitmapUtils.serializeRoaringBitmap64(mask);
            byte[] old = serialized.put(id, b);
            serializedBytes += b.length - (old == null ? 0 : old.length);
            sized(id, mask.getLongSizeInBytes());
            done += b.length;
        }
        return dirty.isEmpty();
    }

    private void sized(int fileId, long size) {
        Long old = heapSizes.put(fileId, size);
        heapBytes += size - (old == null ? 0 : old);
    }

    /** Measures the heap of every file's bitmap: a pass over all of them. */
    void measure() {
        for (Map.Entry<Integer, Roaring64Bitmap> e : masks.entrySet()) {
            sized(e.getKey(), e.getValue().getLongSizeInBytes());
        }
    }

    /** Heap bytes of the bitmaps, as last measured. */
    long heapBytes() {
        return heapBytes;
    }

    /** The serialized masks by file id, as of the last {@link #serializeDirty}. */
    Map<Integer, byte[]> serialized() {
        return Collections.unmodifiableMap(serialized);
    }

    /** The cached path of a file, or null. */
    @Nullable
    String path(int fileId) {
        return paths.get(fileId);
    }

    void cachePath(int fileId, String path) {
        paths.put(fileId, path);
    }

    /** Forgets the cached paths, as the covered version changes. */
    void forgetPaths() {
        paths.clear();
    }

    /** Heap bytes of the serialized bitmaps. */
    long serializedBytes() {
        return serializedBytes;
    }

    /**
     * Forgets the serialized bitmaps, once no union read comes: the next build serializes every
     * file again.
     */
    void forgetSerialized() {
        if (serialized.isEmpty()) {
            return;
        }
        dirty.addAll(serialized.keySet());
        serialized.clear();
        serializedBytes = 0;
    }
}
