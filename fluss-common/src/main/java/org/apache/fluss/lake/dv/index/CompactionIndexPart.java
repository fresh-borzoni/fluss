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

package org.apache.fluss.lake.dv.index;

import org.apache.fluss.annotation.Internal;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * One bucket's part of the position index change of a Fluss compaction, written by the compactor
 * while it rewrites the bucket's files. It holds the new position of every row the compactor kept,
 * so the rewrite needs no re-index. The part is stored with {@link PositionIndexStore#writeBucket}
 * under {@link #changeId}, which all buckets compacted in a tiering round share. The committable
 * carries only this reference.
 */
@Internal
public final class CompactionIndexPart implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long changeId;
    private final long readSnapshotId;
    private final List<String> sstFileNames;
    private final List<Long> sstFileSizes;
    private final Map<Integer, String> addedFiles;
    private final List<String> removedFiles;

    public CompactionIndexPart(
            long changeId,
            long readSnapshotId,
            List<String> sstFileNames,
            List<Long> sstFileSizes,
            Map<Integer, String> addedFiles,
            List<String> removedFiles) {
        this.changeId = changeId;
        this.readSnapshotId = readSnapshotId;
        this.sstFileNames = sstFileNames;
        this.sstFileSizes = sstFileSizes;
        this.addedFiles = addedFiles;
        this.removedFiles = removedFiles;
    }

    /**
     * The id of the change the parts of a tiering round's compactions are written under. It is the
     * same for every bucket of the round, derived from the table, the round and the snapshot the
     * compactions read, and never the id of another round's change.
     */
    public static long changeIdOf(long tableId, long roundTimestamp, long readSnapshotId) {
        long h = 0x9E3779B97F4A7C15L;
        for (long v : new long[] {tableId, roundTimestamp, readSnapshotId}) {
            h ^= v;
            h *= 0xBF58476D1CE4E5B9L;
            h ^= h >>> 31;
        }
        long id = h & Long.MAX_VALUE;
        return id == 0 ? 1 : id;
    }

    /** The change the part is stored under. */
    public long changeId() {
        return changeId;
    }

    /** The lake snapshot the compaction read: the part is a change from its index. */
    public long readSnapshotId() {
        return readSnapshotId;
    }

    /** The part's SST files, as {@link PositionIndexStore#writeBucket} returned them. */
    public List<String> sstFileNames() {
        return Collections.unmodifiableList(sstFileNames);
    }

    public List<Long> sstFileSizes() {
        return Collections.unmodifiableList(sstFileSizes);
    }

    /**
     * The data files the compaction wrote, by provisional ids from 0 that the part's positions use;
     * the committer replaces them with ids from the table's counter ({@link
     * PositionIndexStore#remapFileIds}).
     */
    public Map<Integer, String> addedFiles() {
        return Collections.unmodifiableMap(addedFiles);
    }

    /** The data files the compaction replaced; they leave the index. */
    public List<String> removedFiles() {
        return Collections.unmodifiableList(removedFiles);
    }

    @Override
    public String toString() {
        return "CompactionIndexPart{change="
                + changeId
                + ", read="
                + readSnapshotId
                + ", ssts="
                + sstFileNames
                + ", added="
                + addedFiles
                + ", removed="
                + removedFiles
                + '}';
    }
}
