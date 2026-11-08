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

package org.apache.fluss.lake.iceberg.maintenance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Chooses which data files of a bucket a compaction merges on a table with deletion vectors. It is
 * a pure, deterministic function of the files' sizes, record counts and deleted-row counts.
 *
 * <p>Files fall into tiers by size relative to the target file size {@code T}: small ({@code <
 * T/16}), medium ({@code < T/4}), large ({@code < T}) and full ({@code >= T}). Merging the files of
 * one tier lets rows climb tiers. Re-merging one growing file with every batch of new small files
 * would rewrite the early rows dozens of times at short tiering intervals.
 *
 * <ul>
 *   <li>The small tier is due once it holds {@code minSmallFiles} files. Its files are merged
 *       together, in groups of at most {@code T/4}; merging tiny files {@link #FACTOR} at a time
 *       would only add levels.
 *   <li>Medium and large files are merged {@link #FACTOR} at a time, smallest first, once a tier
 *       holds {@link #FACTOR} of them. A group of {@link #FACTOR} files of one tier reaches about
 *       the next tier. Fewer files wait.
 *   <li>Full files are rewritten only to purge deletes.
 *   <li>A file whose deletion vector or position deletes hide more than {@link
 *       #DELETED_ROWS_THRESHOLD} of its rows is rewritten in any tier, merged into its tier's group
 *       or else on its own.
 * </ul>
 *
 * <p>Groups are returned smallest tier first.
 */
public final class TieredCompactionPolicy {

    /** How many medium or large files are merged at a time. */
    public static final int FACTOR = 4;

    /** The share of a file's rows that, once deleted, makes the file worth rewriting. */
    public static final double DELETED_ROWS_THRESHOLD = 0.2;

    private TieredCompactionPolicy() {}

    /** The size tier of a data file. */
    public enum Tier {
        SMALL,
        MEDIUM,
        LARGE,
        FULL
    }

    /** What the policy knows of a data file. */
    public static final class FileStats {
        final String path;
        final long sizeBytes;
        final long recordCount;
        final long deletedRecords;

        public FileStats(String path, long sizeBytes, long recordCount, long deletedRecords) {
            this.path = path;
            this.sizeBytes = sizeBytes;
            this.recordCount = recordCount;
            this.deletedRecords = deletedRecords;
        }

        public String path() {
            return path;
        }

        public long sizeBytes() {
            return sizeBytes;
        }

        boolean mostlyDeleted() {
            return recordCount > 0 && deletedRecords > DELETED_ROWS_THRESHOLD * recordCount;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof FileStats)) {
                return false;
            }
            FileStats f = (FileStats) o;
            return path.equals(f.path)
                    && sizeBytes == f.sizeBytes
                    && recordCount == f.recordCount
                    && deletedRecords == f.deletedRecords;
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, sizeBytes, recordCount, deletedRecords);
        }

        @Override
        public String toString() {
            return path
                    + "("
                    + sizeBytes
                    + "B, "
                    + deletedRecords
                    + "/"
                    + recordCount
                    + " deleted)";
        }
    }

    /** The tier of a file of {@code sizeBytes} at target size {@code targetSize}. */
    public static Tier tierOf(long sizeBytes, long targetSize) {
        if (sizeBytes < targetSize / 16) {
            return Tier.SMALL;
        } else if (sizeBytes < targetSize / 4) {
            return Tier.MEDIUM;
        } else if (sizeBytes < targetSize) {
            return Tier.LARGE;
        }
        return Tier.FULL;
    }

    /**
     * Whether a bucket with these files is due for compaction once the compaction interval has
     * passed. It is due when its small tier holds {@code minSmallFiles} files, its medium or large
     * tier holds {@link #FACTOR} files, or a file is mostly deleted. Without the medium and large
     * tiers, a bucket written in medium files would never be compacted.
     */
    public static boolean due(List<FileStats> files, long targetSize, int minSmallFiles) {
        int[] counts = new int[Tier.values().length];
        for (FileStats f : files) {
            if (f.mostlyDeleted()) {
                return true;
            }
            counts[tierOf(f.sizeBytes, targetSize).ordinal()]++;
        }
        return counts[Tier.SMALL.ordinal()] >= minSmallFiles
                || counts[Tier.MEDIUM.ordinal()] >= FACTOR
                || counts[Tier.LARGE.ordinal()] >= FACTOR;
    }

    /**
     * Returns the groups of files to merge, each into new files, or none if the bucket is not due.
     */
    public static List<List<FileStats>> select(
            List<FileStats> files, long targetSize, int minSmallFiles) {
        if (!due(files, targetSize, minSmallFiles)) {
            return Collections.emptyList();
        }
        List<FileStats> sorted = new ArrayList<>(files);
        sorted.sort(
                Comparator.comparingLong((FileStats f) -> f.sizeBytes).thenComparing(f -> f.path));
        List<List<FileStats>> byTier = new ArrayList<>();
        for (int i = 0; i < Tier.values().length; i++) {
            byTier.add(new ArrayList<>());
        }
        for (FileStats f : sorted) {
            byTier.get(tierOf(f.sizeBytes, targetSize).ordinal()).add(f);
        }

        List<List<FileStats>> groups = new ArrayList<>();
        Set<String> grouped = new HashSet<>();
        // small: together, in groups up to the medium tier's top
        List<FileStats> small = byTier.get(Tier.SMALL.ordinal());
        if (small.size() >= minSmallFiles) {
            List<FileStats> group = new ArrayList<>();
            long groupBytes = 0;
            for (FileStats f : small) {
                if (!group.isEmpty() && groupBytes + f.sizeBytes > targetSize / 4) {
                    groups.add(group);
                    group = new ArrayList<>();
                    groupBytes = 0;
                }
                group.add(f);
                groupBytes += f.sizeBytes;
            }
            if (group.size() > 1 || (group.size() == 1 && groups.isEmpty())) {
                groups.add(group);
            } else if (!group.isEmpty()) {
                // a lone leftover joins the previous group rather than waiting alone
                groups.get(groups.size() - 1).addAll(group);
            }
        }
        // medium and large: FACTOR at a time
        for (Tier tier : new Tier[] {Tier.MEDIUM, Tier.LARGE}) {
            List<FileStats> inTier = byTier.get(tier.ordinal());
            for (int from = 0; from + FACTOR <= inTier.size(); from += FACTOR) {
                groups.add(new ArrayList<>(inTier.subList(from, from + FACTOR)));
            }
        }
        for (List<FileStats> group : groups) {
            for (FileStats f : group) {
                grouped.add(f.path);
            }
        }
        // mostly deleted files left out: on their own
        for (FileStats f : sorted) {
            if (f.mostlyDeleted() && !grouped.contains(f.path)) {
                groups.add(Collections.singletonList(f));
                grouped.add(f.path);
            }
        }
        // a single small file alone is not worth a rewrite unless it purges deletes
        groups.removeIf(g -> g.size() == 1 && !g.get(0).mostlyDeleted());
        return groups;
    }
}
