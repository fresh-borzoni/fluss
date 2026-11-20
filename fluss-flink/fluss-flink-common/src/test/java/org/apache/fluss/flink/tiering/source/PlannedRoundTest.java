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

package org.apache.fluss.flink.tiering.source;

import org.apache.fluss.client.tiering.RoundDeletes;
import org.apache.fluss.client.tiering.TieringLogSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A planned round as the tiering reader reads it ({@link PlannedRound}): it completes only if the
 * reader read every offset of the round once, as a record or as an offset of a batch without
 * records, the records in increasing offsets, whatever the leader's count of changes sees. Its mask
 * takes no more heap than the reader holds for it before the fetch.
 */
class PlannedRoundTest {

    private static final TablePath TABLE = TablePath.of("db", "t");
    private static final TableBucket BUCKET = new TableBucket(1L, 0);

    // 0 +I k1, 1 +I k2, 2 -U k1, 3 +U k1, 4 -D k2, 5 +I k3, 6 -U k3, 7 +U k3, 8 +I k4
    private static final ChangeType[] ROUND = {
        ChangeType.INSERT,
        ChangeType.INSERT,
        ChangeType.UPDATE_BEFORE,
        ChangeType.UPDATE_AFTER,
        ChangeType.DELETE,
        ChangeType.INSERT,
        ChangeType.UPDATE_BEFORE,
        ChangeType.UPDATE_AFTER,
        ChangeType.INSERT
    };

    @Test
    void testARoundReadWholeCompletes() throws Exception {
        assertThat(read(offsets())).isEqualTo("written [3, 4, 7, 8]");
    }

    /**
     * A record of the round the reader did not get keeps the round from completing, also one the
     * leader's count of changes does not see: a -U, or the +I of a key the round leaves live.
     */
    @ParameterizedTest
    @ValueSource(longs = {0, 2, 3, 4, 6, 8})
    void testARoundThatMissesARecordDoesNotComplete(long missing) throws Exception {
        List<Long> offsets = offsets();
        offsets.remove(Long.valueOf(missing));
        assertThat(read(offsets)).isEqualTo("not read whole: it read 8 of its 9 offsets");
    }

    /**
     * A record the reader got twice keeps the round from completing, also a +I the leader's count
     * of changes does not see, which the masked writer got twice.
     */
    @ParameterizedTest
    @ValueSource(longs = {1, 3, 4, 8})
    void testARoundThatGetsARecordTwiceDoesNotComplete(long twice) throws Exception {
        List<Long> offsets = offsets();
        offsets.add(offsets.indexOf(twice) + 1, twice);
        assertThat(read(offsets))
                .isEqualTo("not read whole: a record of it came twice or out of order");
    }

    @Test
    void testARoundReadOutOfOrderDoesNotComplete() throws Exception {
        List<Long> offsets = offsets();
        Collections.swap(offsets, 5, 6);
        assertThat(read(offsets))
                .isEqualTo("not read whole: a record of it came twice or out of order");
    }

    /**
     * The offsets of batches without records count as read, those of the round only; in sort mode
     * the round is read whole the same way.
     */
    @Test
    void testOffsetsWithoutRecordsCountWithinTheRound() throws Exception {
        PlannedRound round = new PlannedRound(split(10, 20), false, 0);
        assertThat(round.tryDecide(0)).isTrue();
        assertThat(round.mask()).isNull();
        for (long offset = 10; offset < 15; offset++) {
            round.read(offset, 0);
        }
        round.readWithoutRecords(5, 10);
        assertThat(round.notReadWhole()).isEqualTo("it read 5 of its 10 offsets");
        round.readWithoutRecords(15, 18);
        round.readWithoutRecords(18, 25);
        assertThat(round.notReadWhole()).isNull();
    }

    /** A round read again from its start in sort mode counts its offsets afresh. */
    @Test
    void testARoundReadAgainCountsItsOffsetsAfresh() throws Exception {
        PlannedRound round = maskedRound();
        for (long offset = 0; offset < 5; offset++) {
            round.read(offset, 0);
        }
        round.readAgainInSortMode("its mask does not match it");
        assertThat(round.mask()).isNull();
        assertThat(round.sortModeBecause()).isEqualTo("its mask does not match it");
        for (long offset = 0; offset < ROUND.length; offset++) {
            round.read(offset, 0);
        }
        assertThat(round.notReadWhole()).isNull();
    }

    /**
     * A round fetches its mask only once the reader starts it; while it is fetched it holds the
     * most heap its mask may take, then what the mask takes, and in sort mode none.
     */
    @Test
    void testTheHeapOfARoundsMask() throws Exception {
        TieringLogSplit split = split(0, ROUND.length);
        PlannedRound round = new PlannedRound(split, true, 0);
        assertThat(round.waitsToFetch()).isTrue();
        assertThat(round.maskBytes()).isZero();
        assertThat(round.tryDecide(0)).isFalse();

        CompletableFuture<RoundMask.Fetched> fetched = new CompletableFuture<>();
        round.startFetch(fetched, 0);
        assertThat(round.waitsToFetch()).isFalse();
        assertThat(round.maskBytes()).isEqualTo(PlannedRound.maxMaskBytes(split));
        assertThat(round.tryDecide(0)).isFalse();

        fetched.complete(new RoundMask.Fetched(offsetsOf(0, 1, 5), 3L, false));
        assertThat(round.tryDecide(0)).isTrue();
        assertThat(round.maskBytes()).isEqualTo(round.mask().bytes()).isPositive();
        round.readAgainInSortMode("its mask does not match it");
        assertThat(round.maskBytes()).isZero();

        PlannedRound sortMode = new PlannedRound(split, false, 0);
        assertThat(sortMode.waitsToFetch()).isFalse();
        assertThat(sortMode.tryDecide(0)).isTrue();
        assertThat(sortMode.maskBytes()).isZero();
    }

    /** Masks of every density take at most the heap the reader holds for them before the fetch. */
    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 16, 17, 4096, 70_000, 0})
    void testAMaskTakesAtMostTheHeapHeldForIt(int every) {
        long start = 12_345;
        long end = start + 1_000_003;
        Roaring64Bitmap offsets = new Roaring64Bitmap();
        Random random = new Random(every);
        for (long offset = start; offset < end; offset++) {
            if (every > 0 ? (offset - start) % every == 0 : random.nextInt(3) == 0) {
                offsets.addLong(offset);
            }
        }
        RoundMask mask = new RoundMask(BUCKET, new RoundMask.Fetched(offsets, null, false));
        assertThat(mask.bytes()).isLessThanOrEqualTo(PlannedRound.maxMaskBytes(split(start, end)));
    }

    // ---- helpers --------------------------------------------------------------------------------

    /**
     * Reads the round's records at the given offsets as the reader does, and says how the round
     * ends: written through its mask, or why not.
     */
    /**
     * A round dropped before it completes stops the fetch of its deletes; a completed round hands
     * the fetch to its write result, and releasing it then leaves the fetch running.
     */
    @Test
    void testReleaseStopsTheDeletesFetchUnlessHandedOver() {
        PlannedRound dropped = new PlannedRound(split(0, 10), false, 0);
        CompletableFuture<RoundDeletes> droppedFetch = new CompletableFuture<>();
        dropped.startDeletesFetch(droppedFetch);
        dropped.release();
        assertThat(droppedFetch.isCancelled()).isTrue();

        PlannedRound completed = new PlannedRound(split(0, 10), false, 0);
        CompletableFuture<RoundDeletes> handedOver = new CompletableFuture<>();
        completed.startDeletesFetch(handedOver);
        assertThat(completed.takeDeletes()).isSameAs(handedOver);
        completed.release();
        assertThat(handedOver.isCancelled()).isFalse();
        assertThat(completed.takeDeletes()).isNull();
    }

    /**
     * The fetched deletes of rounds being read are kept in the order of the rounds while they fit
     * in the budget; the others are dropped. Fetches still running or failed take nothing.
     */
    @Test
    void testDeletesOverTheBudgetAreDropped() {
        List<PlannedRound> rounds = new ArrayList<>();
        long[] sizes = {10, 10, -1, 0, 10, 4};
        for (long size : sizes) {
            PlannedRound round = new PlannedRound(split(0, 10), false, 0);
            CompletableFuture<RoundDeletes> fetch = new CompletableFuture<>();
            if (size > 0) {
                fetch.complete(deletesOf(size));
            } else if (size < 0) {
                fetch.completeExceptionally(new IllegalStateException("failed"));
            }
            round.startDeletesFetch(fetch);
            rounds.add(round);
        }

        List<PlannedRound> dropped = PlannedRound.dropDeletesOverBudget(rounds, 24);

        assertThat(dropped).containsExactly(rounds.get(4));
        assertThat(rounds.get(0).fetchedDeletes()).isNotNull();
        assertThat(rounds.get(1).fetchedDeletes()).isNotNull();
        assertThat(rounds.get(4).fetchedDeletes()).isNull();
        assertThat(rounds.get(4).takeDeletes()).isNull();
        assertThat(rounds.get(5).fetchedDeletes()).isNotNull();
        // the running fetch is not dropped
        assertThat(rounds.get(3).takeDeletes()).isNotNull();
        assertThat(PlannedRound.dropDeletesOverBudget(rounds, 24)).isEmpty();
    }

    /** Deletes of {@code bytes} bytes: one file of a one-character path. */
    private static RoundDeletes deletesOf(long bytes) {
        return new RoundDeletes(
                1L,
                0L,
                10L,
                Collections.singletonList(new RoundDeletes.File("f", new byte[(int) bytes - 1])));
    }

    private static String read(List<Long> offsets) throws Exception {
        PlannedRound round = maskedRound();
        RoundMask mask = round.mask();
        List<Long> written = new ArrayList<>();
        for (long offset : offsets) {
            round.read(offset, 0);
            if (mask.pass(offset, ROUND[(int) offset])) {
                written.add(offset);
            }
        }
        String notReadWhole = round.notReadWhole();
        if (notReadWhole != null) {
            return "not read whole: " + notReadWhole;
        }
        mask.finish();
        return "written " + written;
    }

    /** The round, through its leader's mask: k1 at 0, k2 at 1 and k3 at 5 are superseded. */
    private static PlannedRound maskedRound() throws Exception {
        PlannedRound round = new PlannedRound(split(0, ROUND.length), true, 0);
        round.startFetch(
                CompletableFuture.completedFuture(
                        new RoundMask.Fetched(offsetsOf(0, 1, 5), 3L, false)),
                0);
        assertThat(round.tryDecide(0)).isTrue();
        assertThat(round.mask()).isNotNull();
        return round;
    }

    private static List<Long> offsets() {
        List<Long> offsets = new ArrayList<>();
        for (long offset = 0; offset < ROUND.length; offset++) {
            offsets.add(offset);
        }
        return offsets;
    }

    private static Roaring64Bitmap offsetsOf(long... offsets) {
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        for (long offset : offsets) {
            bitmap.addLong(offset);
        }
        return bitmap;
    }

    private static TieringLogSplit split(long start, long end) {
        return TieringLogSplit.plannedRound(TABLE, BUCKET, null, start, end, 1L, false);
    }
}
