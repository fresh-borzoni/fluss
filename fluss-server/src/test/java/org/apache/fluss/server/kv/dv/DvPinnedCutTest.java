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

import org.apache.fluss.server.utils.RoaringBitmapUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DvPinnedCut}. */
class DvPinnedCutTest {

    private static final long START = 1_000L;

    @Test
    void testLakeMasksAreMatchedOnTheFileName() throws IOException {
        Map<String, byte[]> lake = new HashMap<>();
        lake.put("s3://lake/t/data/f1.parquet", bitmap(1, 2));
        lake.put("s3://lake/t/data/f2.parquet", bitmap(3));
        DvPinnedCut cut = DvPinnedCut.freeze(7, new DvSnapshot(lake, null, 50, 10, "commit-7"));

        Map<String, byte[]> masks =
                cut.lakeMasksOf(Arrays.asList("f2.parquet", "other/f1.parquet", "f9.parquet"));
        assertThat(masks).containsOnlyKeys(lake.keySet());
        assertThat(offsets(masks.get("s3://lake/t/data/f1.parquet"))).containsExactly(1L, 2L);
        assertThat(cut.lakeMasksOf(Collections.emptyList())).isEmpty();
        assertThat(cut.snapshotId()).isEqualTo(7L);
        assertThat(cut.logEndOffset()).isEqualTo(50L);
        assertThat(cut.snapshotStartOffset()).isEqualTo(10L);
        assertThat(cut.lakeSnapshotIdentity()).isEqualTo("commit-7");

        // the whole cut is the masks it was frozen from
        DvSnapshot whole = cut.toDvSnapshot();
        assertThat(whole.getLakeDvEntries()).containsOnlyKeys(lake.keySet());
        assertThat(whole.getLogDvBitmap()).isNull();
        assertThat(whole.getLogEndOffset()).isEqualTo(50L);
        assertThat(whole.getSnapshotStartOffset()).isEqualTo(10L);
        assertThat(whole.getLakeSnapshotIdentity()).isEqualTo("commit-7");
    }

    @Test
    void testPagesFromTheCutAndOfAnEmptyMask() throws IOException {
        DvPinnedCut empty = freeze(new Roaring64Bitmap(), 500);
        DvPinnedCut.LogPage page = empty.logPage(START, 1024);
        assertThat(page.to()).isEqualTo(500L + START);
        assertThat(page.bitmap()).isNull();

        DvPinnedCut cut = freeze(of(START, START + 3), 10);
        page = cut.logPage(START + 10, 1024);
        assertThat(page.from()).isEqualTo(START + 10);
        assertThat(page.to()).isEqualTo(START + 10);
        assertThat(page.bitmap()).isNull();
        // nothing masked from START + 4 on
        page = cut.logPage(START + 4, 1024);
        assertThat(page.to()).isEqualTo(START + 10);
        assertThat(page.bitmap()).isNull();

        assertThatThrownBy(() -> cut.logPage(START + 11, 1024))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> cut.logPage(-1, 1024))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> cut.logPage(START, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testPageStartsWithinAContainer() throws IOException {
        long base = 5L << 16;
        long end = base + 200_000;
        DvPinnedCut cut = freeze(of(base + 1, base + 2, base + 70_000, base + 70_001), end - START);

        DvPinnedCut.LogPage page = cut.logPage(base + 2, 1 << 20);
        assertThat(page.to()).isEqualTo(end);
        assertThat(offsets(page.bitmap())).containsExactly(base + 2, base + 70_000, base + 70_001);

        // one container a page: the next page starts at the next masked offset
        page = cut.logPage(base + 2, 1);
        assertThat(offsets(page.bitmap())).containsExactly(base + 2);
        assertThat(page.to()).isEqualTo(base + 70_000);
        page = cut.logPage(page.to(), 1);
        assertThat(offsets(page.bitmap())).containsExactly(base + 70_000, base + 70_001);
        assertThat(page.to()).isEqualTo(end);
    }

    /** Pages of masks of every shape, more than 8 MB of them, fit the page size and chain up. */
    @ParameterizedTest
    @ValueSource(ints = {1, 300, 64 * 1024, 1 << 20})
    void testPagesCoverTheLogMaskExactly(int maxBytes) throws IOException {
        Roaring64Bitmap mask = mixedMask(new Random(maxBytes));
        long end = mask.last() + 1 - START + 17;
        byte[] before = RoaringBitmapUtils.serializeRoaringBitmap64(mask.clone());
        DvPinnedCut cut = freeze(mask, end);

        List<DvPinnedCut.LogPage> pages = pageAll(cut, START, maxBytes);

        Roaring64Bitmap union = new Roaring64Bitmap();
        for (DvPinnedCut.LogPage page : pages) {
            Roaring64Bitmap content = deserialize(page.bitmap());
            if (page.bitmap() != null && page.bitmap().length > maxBytes) {
                // only a single container may exceed the page size
                assertThat(containers(content)).isEqualTo(1);
            }
            // exactly the masked offsets of [from, to)
            Roaring64Bitmap expected = new Roaring64Bitmap();
            expected.addRange(page.from(), page.to());
            expected.and(mask);
            assertThat(sameOffsets(content, expected)).isTrue();
            union.or(content);
        }
        assertThat(sameOffsets(union, mask)).isTrue();
        // the pages leave the cut's mask as it was
        assertThat(cut.toDvSnapshot().getLogDvBitmap()).isEqualTo(before);
        if (maxBytes == 1 << 20) {
            assertThat(before.length).isGreaterThan(8 << 20);
            assertThat(pages.size()).isGreaterThan(8);
        }
    }

    /** Pages read by many threads at once are the same as read by one. */
    @Test
    void testConcurrentPageReads() throws Exception {
        Roaring64Bitmap mask = mixedMask(new Random(3));
        DvPinnedCut cut = freeze(mask, mask.last() + 1 - START);
        List<DvPinnedCut.LogPage> expected = pageAll(cut, START, 128 * 1024);
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<CompletableFuture<List<DvPinnedCut.LogPage>>> reads = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                reads.add(
                        CompletableFuture.supplyAsync(
                                () -> {
                                    try {
                                        return pageAll(cut, START, 128 * 1024);
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                },
                                pool));
            }
            for (CompletableFuture<List<DvPinnedCut.LogPage>> read : reads) {
                List<DvPinnedCut.LogPage> pages = read.get(60, TimeUnit.SECONDS);
                assertThat(pages).hasSameSizeAs(expected);
                for (int p = 0; p < pages.size(); p++) {
                    assertThat(pages.get(p).to()).isEqualTo(expected.get(p).to());
                    assertThat(pages.get(p).bitmap()).isEqualTo(expected.get(p).bitmap());
                }
            }
        } finally {
            pool.shutdownNow();
        }
    }

    // ------------------------------------------------------------------------------------------

    private static List<DvPinnedCut.LogPage> pageAll(DvPinnedCut cut, long from, int maxBytes)
            throws IOException {
        List<DvPinnedCut.LogPage> pages = new ArrayList<>();
        long offset = from;
        while (true) {
            DvPinnedCut.LogPage page = cut.logPage(offset, maxBytes);
            assertThat(page.from()).isEqualTo(offset);
            if (offset == cut.logEndOffset()) {
                assertThat(page.to()).isEqualTo(offset);
                return pages;
            }
            assertThat(page.to()).isGreaterThan(offset).isLessThanOrEqualTo(cut.logEndOffset());
            pages.add(page);
            offset = page.to();
        }
    }

    /**
     * Masked offsets from START: sparse containers far apart, sparse ones next to each other, dense
     * bitmap containers (more than 8 MB of them) and runs.
     */
    private static Roaring64Bitmap mixedMask(Random random) {
        Roaring64Bitmap mask = new Roaring64Bitmap();
        long base = START;
        for (int i = 0; i < 2_000; i++) {
            mask.addLong(base + (long) random.nextInt(1 << 30));
        }
        base += 1L << 31;
        for (int i = 0; i < 3_000; i++) {
            mask.addLong(base + ((long) i << 16) + random.nextInt(1 << 16));
        }
        base += 3_000L << 16;
        // every other offset: a bitmap container of 8 KB each, which no run shrinks
        for (long o = base; o < base + (1_100L << 16); o += 2) {
            mask.addLong(o);
        }
        base += 1_100L << 16;
        mask.addRange(base + 5, base + (40L << 16));
        mask.runOptimize();
        return mask;
    }

    private static DvPinnedCut freeze(Roaring64Bitmap logMask, long tailLength) throws IOException {
        byte[] bytes =
                logMask.isEmpty() ? null : RoaringBitmapUtils.serializeRoaringBitmap64(logMask);
        return DvPinnedCut.freeze(
                1, new DvSnapshot(Collections.emptyMap(), bytes, START + tailLength, START));
    }

    private static Roaring64Bitmap of(long... values) {
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        for (long v : values) {
            bitmap.addLong(v);
        }
        return bitmap;
    }

    private static byte[] bitmap(long... values) throws IOException {
        return RoaringBitmapUtils.serializeRoaringBitmap64(of(values));
    }

    private static Roaring64Bitmap deserialize(@Nullable byte[] bytes) throws IOException {
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        if (bytes != null) {
            RoaringBitmapUtils.deserializeRoaringBitmap64(bitmap, bytes);
        }
        return bitmap;
    }

    private static List<Long> offsets(@Nullable byte[] bytes) throws IOException {
        List<Long> out = new ArrayList<>();
        for (LongIterator it = deserialize(bytes).getLongIterator(); it.hasNext(); ) {
            out.add(it.next());
        }
        return out;
    }

    private static boolean sameOffsets(Roaring64Bitmap a, Roaring64Bitmap b) {
        long cardinality = a.getLongCardinality();
        return cardinality == b.getLongCardinality()
                && Roaring64Bitmap.andCardinality(a, b) == cardinality;
    }

    private static int containers(Roaring64Bitmap bitmap) {
        int n = 0;
        long last = -1;
        for (LongIterator it = bitmap.getLongIterator(); it.hasNext(); ) {
            long container = it.next() >>> 16;
            if (container != last) {
                n++;
                last = container;
            }
        }
        return n;
    }
}
