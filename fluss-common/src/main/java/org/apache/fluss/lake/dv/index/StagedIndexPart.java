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
import org.apache.fluss.lake.dv.RowPosSstIndex;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * A tiering writer's part of its bucket's position index change, staged in the store ({@link
 * PositionIndexStore#stageBucket}) when the writer completes. It holds every key the round changed
 * in the bucket, in key order, either deleted or with the position of its new row in the data files
 * the writer wrote.
 *
 * <p>The part numbers its data files from 0. The committer gives them ids from the table's counter
 * when it publishes the part into the round's change ({@link PositionIndexStore#publishStaged}),
 * and looks up the superseded rows of the part's keys while doing so. A write result carries only
 * this reference, whatever the size of the round.
 */
@Internal
public final class StagedIndexPart implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long stagingId;
    private final List<String> sstFileNames;
    private final List<Long> sstFileSizes;
    private final Map<Integer, String> files;
    private final long keys;
    private final long deletes;

    public StagedIndexPart(
            long stagingId,
            List<String> sstFileNames,
            List<Long> sstFileSizes,
            Map<Integer, String> files,
            long keys,
            long deletes) {
        checkArgument(
                sstFileNames.size() == sstFileSizes.size(),
                "%s SST names but %s sizes.",
                sstFileNames.size(),
                sstFileSizes.size());
        this.stagingId = stagingId;
        this.sstFileNames = sstFileNames;
        this.sstFileSizes = sstFileSizes;
        this.files = files;
        this.keys = keys;
        this.deletes = deletes;
    }

    public static StagedIndexPart of(
            long stagingId,
            PositionIndexStore.BucketPart part,
            Map<Integer, String> files,
            long keys,
            long deletes) {
        List<String> names = new ArrayList<>();
        List<Long> sizes = new ArrayList<>();
        for (RowPosSstIndex.SstFileEntry f : part.files) {
            names.add(f.getFileName());
            sizes.add(f.getFileSize());
        }
        return new StagedIndexPart(stagingId, names, sizes, files, keys, deletes);
    }

    /**
     * The id of the change the writers of a tiering round stage their parts under. It is the same
     * for every bucket of the round, from the table and the round, and never the id of the change a
     * round publishes or of a compaction's change ({@link CompactionIndexPart#changeIdOf}).
     */
    public static long stagingIdOf(long tableId, long roundTimestamp) {
        // a read snapshot id no compaction has
        return CompactionIndexPart.changeIdOf(tableId, roundTimestamp, Long.MIN_VALUE);
    }

    /** The change the part is staged under; deleted once the round ends. */
    public long stagingId() {
        return stagingId;
    }

    public List<String> sstFileNames() {
        return Collections.unmodifiableList(sstFileNames);
    }

    public List<Long> sstFileSizes() {
        return Collections.unmodifiableList(sstFileSizes);
    }

    /** The staged SSTs, as {@link PositionIndexStore#publishStaged} takes them. */
    public PositionIndexStore.BucketPart part() {
        if (sstFileNames.isEmpty()) {
            return PositionIndexStore.BucketPart.NONE;
        }
        List<RowPosSstIndex.SstFileEntry> entries = new ArrayList<>();
        for (int i = 0; i < sstFileNames.size(); i++) {
            entries.add(new RowPosSstIndex.SstFileEntry(sstFileNames.get(i), sstFileSizes.get(i)));
        }
        return new PositionIndexStore.BucketPart(entries);
    }

    /** The data files the writer wrote, by the ids from 0 the part's positions use. */
    public Map<Integer, String> files() {
        return Collections.unmodifiableMap(files);
    }

    /** The number of keys the round changed in the bucket. */
    public long keys() {
        return keys;
    }

    /** The number of keys the round deleted in the bucket. */
    public long deletes() {
        return deletes;
    }

    @Override
    public String toString() {
        return "StagedIndexPart{staging="
                + stagingId
                + ", ssts="
                + sstFileNames
                + ", files="
                + files.size()
                + ", keys="
                + keys
                + ", deletes="
                + deletes
                + '}';
    }
}
