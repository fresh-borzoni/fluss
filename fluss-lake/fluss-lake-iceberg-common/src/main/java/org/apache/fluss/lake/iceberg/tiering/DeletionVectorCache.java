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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.deletes.PositionDeleteIndex;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The deletion vectors the committers of a tiering job wrote, by the location of their data file,
 * so a later round takes a data file's deletion vector from here instead of reading it back. An
 * entry is used only while the data file's one current delete file is the deletion vector it was
 * written as, so after a rollback or another engine's rewrite the vector is read again. Bounded by
 * the bytes of the serialized vectors; the least recently used go first.
 */
final class DeletionVectorCache {

    private final long maxBytes;
    private final LinkedHashMap<String, Entry> entries = new LinkedHashMap<>(16, 0.75f, true);
    private long bytes;
    private long hits;

    DeletionVectorCache(long maxBytes) {
        this.maxBytes = maxBytes;
    }

    /**
     * The deletion vector of the data file, if {@code deletes}, its current delete files, are one
     * deletion vector this cache holds; else null.
     */
    @Nullable
    synchronized PositionDeleteIndex get(String dataFile, List<DeleteFile> deletes) {
        DeleteFile current = DeletionVectorId.onlyDeletionVector(deletes);
        if (current == null) {
            return null;
        }
        Entry entry = entries.get(dataFile);
        if (entry == null || !entry.id.isOf(current)) {
            return null;
        }
        hits++;
        return PositionDeleteIndex.deserialize(entry.vector, current);
    }

    /**
     * Keeps {@code vector}, the serialized positions of deletion vector {@code dv}, which a commit
     * of the data file's deletes made current. A vector that does not fit is not kept.
     */
    synchronized void put(String dataFile, DeleteFile dv, byte[] vector) {
        Entry old = entries.remove(dataFile);
        if (old != null) {
            bytes -= old.vector.length;
        }
        if (vector.length > maxBytes) {
            return;
        }
        entries.put(dataFile, new Entry(dv, vector));
        bytes += vector.length;
        Iterator<Map.Entry<String, Entry>> eldest = entries.entrySet().iterator();
        while (bytes > maxBytes) {
            bytes -= eldest.next().getValue().vector.length;
            eldest.remove();
        }
    }

    /** Whether the cache keeps deletion vectors at all. */
    boolean enabled() {
        return maxBytes > 0;
    }

    /** The most bytes of vectors it keeps. */
    long maxBytes() {
        return maxBytes;
    }

    /** The bytes of the vectors it keeps. */
    synchronized long bytes() {
        return bytes;
    }

    /** How many deletion vectors it handed out. */
    synchronized long hits() {
        return hits;
    }

    /** A deletion vector and its serialized positions. */
    private static final class Entry {
        private final DeletionVectorId id;
        private final byte[] vector;

        private Entry(DeleteFile dv, byte[] vector) {
            this.id = DeletionVectorId.of(dv);
            this.vector = vector;
        }
    }
}
