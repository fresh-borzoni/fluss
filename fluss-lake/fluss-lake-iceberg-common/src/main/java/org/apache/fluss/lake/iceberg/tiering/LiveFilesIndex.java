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

import org.apache.fluss.annotation.VisibleForTesting;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.ContentFileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The live data files of a snapshot of a table with deletion vectors, by file name, each with its
 * position deletes (deletion vectors or file-scoped position delete files). A round's deletes need
 * this of their base.
 *
 * <p>Planning the base reads every manifest of the table. Instead, a table's index is kept across
 * rounds and advanced snapshot by snapshot with the files each snapshot added and removed. Only the
 * manifests those snapshots wrote list these files, so a round reads only what changed since the
 * last one. The index is built by one scan of the snapshot when it cannot be advanced: on the first
 * round after a start, after more snapshots than one advance replays, or when a snapshot adds
 * deletes that do not belong to one data file (equality or partition-scoped position deletes, which
 * only another engine writes).
 */
final class LiveFilesIndex {

    private static final Logger LOG = LoggerFactory.getLogger(LiveFilesIndex.class);

    // the most snapshots one advance replays; with more since the last round, the base is planned
    private static final int MAX_REPLAYED_SNAPSHOTS = 64;
    // a table with more live files is planned every round, rather than held in memory
    private static final int MAX_CACHED_FILES = 2_000_000;

    private static final Map<String, LiveFilesIndex> BY_TABLE = new ConcurrentHashMap<>();
    private static final AtomicLong SCANS = new AtomicLong();
    private static volatile boolean caching = true;

    private final long snapshotId;
    // file name -> data file, without stats
    private final Map<String, DataFile> files;
    // data file location -> its position deletes
    private final Map<String, List<DeleteFile>> deletes;
    // whether a later snapshot can be indexed by advancing this one
    private final boolean advanceable;

    private LiveFilesIndex(
            long snapshotId,
            Map<String, DataFile> files,
            Map<String, List<DeleteFile>> deletes,
            boolean advanceable) {
        this.snapshotId = snapshotId;
        this.files = files;
        this.deletes = deletes;
        this.advanceable = advanceable;
    }

    /** Returns the index of the table's snapshot, advancing the table's last one if it can. */
    static LiveFilesIndex of(Table table, long snapshotId) throws IOException {
        String key = key(table);
        LiveFilesIndex last = caching ? BY_TABLE.remove(key) : null;
        LiveFilesIndex index = null;
        if (last != null) {
            try {
                index = last.advanceTo(table, snapshotId);
            } catch (RuntimeException | IOException e) {
                LOG.warn(
                        "Could not advance the live files of {} from snapshot {} to {}; planning"
                                + " the snapshot instead.",
                        table.name(),
                        last.snapshotId,
                        snapshotId,
                        e);
            }
        }
        if (index == null) {
            index = scan(table, snapshotId);
        }
        if (caching && index.advanceable && index.files.size() <= MAX_CACHED_FILES) {
            BY_TABLE.put(key, index);
        }
        return index;
    }

    /**
     * Plans the table's snapshot again, without the table's cached index, which may lag behind it.
     */
    static LiveFilesIndex rescan(Table table, long snapshotId) throws IOException {
        BY_TABLE.remove(key(table));
        return of(table, snapshotId);
    }

    private static String key(Table table) {
        return table.name() + "|" + table.location();
    }

    /** Whether the snapshot has a live data file of each of the given names. */
    boolean hasFiles(Collection<String> fileNames) {
        return files.keySet().containsAll(fileNames);
    }

    /** The live data file of the given name, or null if the snapshot has none. */
    @Nullable
    DataFile file(String fileName) {
        return files.get(fileName);
    }

    /** The locations of the live data files. */
    Collection<String> locations() {
        List<String> out = new ArrayList<>(files.size());
        for (DataFile f : files.values()) {
            out.add(f.location());
        }
        return out;
    }

    /** The position deletes of a live data file. */
    List<DeleteFile> deletesOf(String location) {
        List<DeleteFile> d = deletes.get(location);
        return d == null ? Collections.emptyList() : d;
    }

    private static LiveFilesIndex scan(Table table, long snapshotId) throws IOException {
        SCANS.incrementAndGet();
        Map<String, DataFile> files = new HashMap<>();
        Map<String, List<DeleteFile>> deletes = new HashMap<>();
        boolean advanceable = true;
        try (CloseableIterable<FileScanTask> tasks =
                table.newScan().useSnapshot(snapshotId).planFiles()) {
            for (FileScanTask task : tasks) {
                DataFile file = task.file();
                files.put(nameOf(file.location()), file.copyWithoutStats());
                for (DeleteFile d : task.deletes()) {
                    // Equality deletes and partition-scoped position delete files apply to more
                    // than one data file, so later snapshots' deletes cannot be told by file.
                    if (d.content() != FileContent.POSITION_DELETES) {
                        advanceable = false;
                        continue;
                    }
                    if (ContentFileUtil.referencedDataFileLocation(d) == null) {
                        advanceable = false;
                    }
                    deletes.computeIfAbsent(file.location(), k -> new ArrayList<>())
                            .add(d.copyWithoutStats());
                }
            }
        }
        return new LiveFilesIndex(snapshotId, files, deletes, advanceable);
    }

    /**
     * Returns the index of {@code target}, a descendant of this index's snapshot, built from the
     * files the snapshots in between added and removed. Returns null if they do not suffice.
     */
    @Nullable
    private LiveFilesIndex advanceTo(Table table, long target) throws IOException {
        if (target == snapshotId) {
            return this;
        }
        List<Snapshot> replayed = new ArrayList<>();
        Snapshot s = table.snapshot(target);
        while (s != null && s.snapshotId() != snapshotId) {
            if (replayed.size() == MAX_REPLAYED_SNAPSHOTS) {
                return null;
            }
            replayed.add(s);
            s = s.parentId() == null ? null : table.snapshot(s.parentId());
        }
        if (s == null) {
            return null; // not a descendant, or a snapshot in between expired
        }
        Collections.reverse(replayed);
        FileIO io = table.io();
        // In place: the index left the cache, and is dropped if it cannot be advanced.
        Map<String, DataFile> nextFiles = files;
        Map<String, List<DeleteFile>> nextDeletes = deletes;
        for (Snapshot snapshot : replayed) {
            for (DataFile removed : snapshot.removedDataFiles(io)) {
                nextFiles.remove(nameOf(removed.location()));
                nextDeletes.remove(removed.location());
            }
            for (DataFile added : snapshot.addedDataFiles(io)) {
                nextFiles.put(nameOf(added.location()), added.copyWithoutStats());
            }
            for (DeleteFile removed : snapshot.removedDeleteFiles(io)) {
                String dataFile = ContentFileUtil.referencedDataFileLocation(removed);
                List<DeleteFile> of = dataFile == null ? null : nextDeletes.get(dataFile);
                if (of != null) {
                    // deletion vectors of several data files share a Puffin file
                    of.removeIf(
                            d ->
                                    d.location().equals(removed.location())
                                            && Objects.equals(
                                                    d.contentOffset(), removed.contentOffset()));
                    if (of.isEmpty()) {
                        nextDeletes.remove(dataFile);
                    }
                }
            }
            for (DeleteFile added : snapshot.addedDeleteFiles(io)) {
                String dataFile = ContentFileUtil.referencedDataFileLocation(added);
                if (added.content() != FileContent.POSITION_DELETES || dataFile == null) {
                    return null; // deletes of more than one data file: plan the snapshot
                }
                nextDeletes
                        .computeIfAbsent(dataFile, k -> new ArrayList<>())
                        .add(added.copyWithoutStats());
            }
        }
        return new LiveFilesIndex(target, nextFiles, nextDeletes, true);
    }

    private static String nameOf(String location) {
        return location.substring(location.lastIndexOf('/') + 1);
    }

    /** How many times a snapshot was planned to index it. */
    @VisibleForTesting
    static long scans() {
        return SCANS.get();
    }

    /** Turns off keeping indexes across rounds, so each round plans its base, or turns it on. */
    @VisibleForTesting
    static void setCaching(boolean enabled) {
        caching = enabled;
        BY_TABLE.clear();
    }
}
