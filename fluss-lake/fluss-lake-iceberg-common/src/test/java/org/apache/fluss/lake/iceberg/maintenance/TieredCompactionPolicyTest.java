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

import org.apache.fluss.lake.iceberg.maintenance.TieredCompactionPolicy.FileStats;
import org.apache.fluss.lake.iceberg.maintenance.TieredCompactionPolicy.Tier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests of {@link TieredCompactionPolicy}, and its write amplification against bin-packing. */
class TieredCompactionPolicyTest {

    private static final long MB = 1024 * 1024;
    private static final long T = 128 * MB;

    @Test
    void testTiers() {
        assertThat(TieredCompactionPolicy.tierOf(0, T)).isEqualTo(Tier.SMALL);
        assertThat(TieredCompactionPolicy.tierOf(T / 16 - 1, T)).isEqualTo(Tier.SMALL);
        assertThat(TieredCompactionPolicy.tierOf(T / 16, T)).isEqualTo(Tier.MEDIUM);
        assertThat(TieredCompactionPolicy.tierOf(T / 4 - 1, T)).isEqualTo(Tier.MEDIUM);
        assertThat(TieredCompactionPolicy.tierOf(T / 4, T)).isEqualTo(Tier.LARGE);
        assertThat(TieredCompactionPolicy.tierOf(T - 1, T)).isEqualTo(Tier.LARGE);
        assertThat(TieredCompactionPolicy.tierOf(T, T)).isEqualTo(Tier.FULL);
        assertThat(TieredCompactionPolicy.tierOf(10 * T, T)).isEqualTo(Tier.FULL);
    }

    @Test
    void testNothingToCompact() {
        assertThat(select(Collections.emptyList(), 3)).isEmpty();
        // below the small-file trigger, with medium, large and full files waiting
        assertThat(select(files(1, 1, 10, 10, 10, 40, 128, 200), 3)).isEmpty();
        assertThat(TieredCompactionPolicy.due(files(1, 1, 10, 10, 10, 40, 40, 40), T, 3)).isFalse();
        // FACTOR medium or large files make a bucket due without small files
        assertThat(TieredCompactionPolicy.due(files(10, 10, 10, 10), T, 3)).isTrue();
        assertThat(select(files(10, 11, 12, 13), 3)).containsExactly(paths(10, 11, 12, 13));
        assertThat(select(files(40, 41, 42, 43, 128), 3)).containsExactly(paths(40, 41, 42, 43));
        // a single small file is not rewritten alone
        assertThat(select(files(1, 128), 1)).isEmpty();
    }

    @Test
    void testSmallFilesAreMergedTogetherOnceDue() {
        assertThat(select(files(1, 2, 3), 3)).containsExactly(paths(1, 2, 3));
        assertThat(select(files(3, 1, 2, 4, 7), 3)).containsExactly(paths(1, 2, 3, 4, 7));
        // groups up to the medium tier's top (32 MB); a lone leftover joins the last group
        assertThat(select(files(7, 7, 7, 7, 7, 7, 7), 3))
                .containsExactly(
                        Arrays.asList("f7", "f7_1", "f7_2", "f7_3"),
                        Arrays.asList("f7_4", "f7_5", "f7_6"));
        assertThat(select(files(7, 7, 7, 7, 7), 3)).containsExactly(paths(7, 7, 7, 7, 7));
    }

    @Test
    void testMediumAndLargeFilesMergeFactorAtATime() {
        // the small trigger lets the other tiers merge FACTOR files of a tier, smallest first
        List<List<String>> groups =
                select(files(1, 1, 1, 9, 10, 11, 12, 13, 40, 41, 42, 43, 128, 300), 3);
        assertThat(groups)
                .containsExactly(paths(1, 1, 1), paths(9, 10, 11, 12), paths(40, 41, 42, 43));
        // fewer than FACTOR files of a tier wait
        assertThat(select(files(1, 1, 1, 9, 10, 11, 40, 41), 3)).containsExactly(paths(1, 1, 1));
    }

    @Test
    void testMostlyDeletedFilesAreRewrittenInAnyTier() {
        List<FileStats> files = new ArrayList<>(files(9, 40, 128));
        // 30% of the full file's rows are deleted: rewritten on its own, without small files
        files.add(new FileStats("deleted-200", 200 * MB, 1000, 300));
        files.add(new FileStats("deleted-12", 12 * MB, 1000, 201));
        files.add(new FileStats("kept-150", 150 * MB, 1000, 200)); // exactly 20%: kept
        assertThat(TieredCompactionPolicy.due(files, T, 3)).isTrue();
        assertThat(names(TieredCompactionPolicy.select(files, T, 3)))
                .containsExactly(
                        Collections.singletonList("deleted-12"),
                        Collections.singletonList("deleted-200"));
        // a mostly deleted file of a tier that merges joins its tier's group
        files.addAll(files(10, 11, 13));
        assertThat(names(TieredCompactionPolicy.select(files, T, 3)))
                .containsExactly(
                        Arrays.asList("f9", "f10", "f11", "deleted-12"),
                        Collections.singletonList("deleted-200"));
    }

    @Test
    void testSelectionIsDeterministic() {
        Random random = new Random(42);
        for (int run = 0; run < 1000; run++) {
            List<FileStats> files = new ArrayList<>();
            int n = random.nextInt(30);
            for (int i = 0; i < n; i++) {
                long size = 1 + (long) (random.nextDouble() * 2 * T);
                long records = 1 + random.nextInt(10_000);
                files.add(
                        new FileStats(
                                "r" + i, size, records, random.nextInt((int) records + 1) / 2));
            }
            int minSmall = 1 + random.nextInt(5);
            List<List<FileStats>> groups = TieredCompactionPolicy.select(files, T, minSmall);
            List<FileStats> shuffled = new ArrayList<>(files);
            Collections.shuffle(shuffled, random);
            assertThat(TieredCompactionPolicy.select(shuffled, T, minSmall)).isEqualTo(groups);
            // every file at most once, no full file without deletes, no lone file without them
            List<FileStats> all = new ArrayList<>();
            groups.forEach(all::addAll);
            assertThat(all).doesNotHaveDuplicates();
            for (List<FileStats> group : groups) {
                for (FileStats f : group) {
                    assertThat(
                                    TieredCompactionPolicy.tierOf(f.sizeBytes, T) != Tier.FULL
                                            || f.mostlyDeleted())
                            .isTrue();
                }
                if (group.size() == 1) {
                    assertThat(group.get(0).mostlyDeleted()).isTrue();
                }
            }
        }
    }

    /**
     * The tiered policy keeps write amplification low and the bucket at few files.
     *
     * <p>Simulates one bucket that a tiering round writes {@code mbPerRound} MB to every {@code
     * roundMinutes} minutes, up to 4 GB, compacting at the start of each round as the tiering
     * writer does, at the default interval (10 minutes) and min-small-files (3). Write
     * amplification is the bytes compactions rewrite per byte tiered.
     */
    @ParameterizedTest
    @CsvSource({"1, 1", "12, 1", "1, 3", "12, 3", "1, 0.5", "4, 1"})
    void testWriteAmplification(double mbPerRound, double roundMinutes) {
        long tiered4Gb = 4096 * MB;
        long roundBytes = (long) (mbPerRound * MB);
        List<FileStats> files = new ArrayList<>();
        long written = 0;
        long compacted = 0;
        double lastCompaction = Double.NEGATIVE_INFINITY;
        int next = 0;
        for (int round = 0; (long) round * roundBytes < tiered4Gb; round++) {
            double now = round * roundMinutes;
            List<List<FileStats>> groups =
                    now - lastCompaction >= 10
                            ? TieredCompactionPolicy.select(files, T, 3)
                            : Collections.<List<FileStats>>emptyList();
            if (!groups.isEmpty()) {
                lastCompaction = now;
            }
            for (List<FileStats> group : groups) {
                long bytes = 0;
                for (FileStats f : group) {
                    files.remove(f);
                    bytes += f.sizeBytes;
                }
                compacted += bytes;
                // a rolling writer cuts files at the target size
                for (long left = bytes; left > 0; left -= T) {
                    files.add(new FileStats("c" + next++, Math.min(left, T), 1, 0));
                }
            }
            files.add(new FileStats("t" + next++, roundBytes, 1, 0));
            written += roundBytes;
        }
        assertThat(compacted / (double) written).isLessThanOrEqualTo(3.7);
        // about one file per target size, plus a few per tier
        assertThat(files.size()).isLessThan(64);
    }

    // ------------------------------------------------------------------------------------------

    private static List<List<String>> select(List<FileStats> files, int minSmallFiles) {
        return names(TieredCompactionPolicy.select(files, T, minSmallFiles));
    }

    private static List<List<String>> names(List<List<FileStats>> groups) {
        return groups.stream()
                .map(g -> g.stream().map(FileStats::path).collect(Collectors.toList()))
                .collect(Collectors.toList());
    }

    /** Files of the given sizes in MB, named "f{size}" (with a suffix if repeated). */
    private static List<FileStats> files(long... sizesMb) {
        List<FileStats> files = new ArrayList<>();
        for (int i = 0; i < sizesMb.length; i++) {
            files.add(new FileStats(name(sizesMb, i), sizesMb[i] * MB, 100, 0));
        }
        return files;
    }

    private static List<String> paths(long... sizesMb) {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < sizesMb.length; i++) {
            names.add(name(sizesMb, i));
        }
        return names;
    }

    private static String name(long[] sizesMb, int i) {
        int repeat = 0;
        for (int j = 0; j < i; j++) {
            if (sizesMb[j] == sizesMb[i]) {
                repeat++;
            }
        }
        return "f" + sizesMb[i] + (repeat == 0 ? "" : "_" + repeat);
    }
}
