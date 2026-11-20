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

import org.apache.fluss.client.tiering.TieringLogSplit;
import org.apache.fluss.lake.writer.RoundMaskMismatchException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The leader's log mask as the tiering reader applies it ({@link RoundMask}): it skips the masked
 * versions and the -U records, and fails a round whose mask does not match it.
 */
class RoundMaskTest {

    private static final TableBucket BUCKET = new TableBucket(1L, 0);

    @Test
    void testSkipsMaskedVersionsAndBeforeImages() throws Exception {
        // k1 at 0 and k2 at 1, k1 updated at 2/3, k2 deleted at 4, k3 at 5
        List<LogRecord> round = new ArrayList<>();
        round.add(insert(0, 1));
        round.add(insert(1, 2));
        round.add(record(2, ChangeType.UPDATE_BEFORE, 1));
        round.add(record(3, ChangeType.UPDATE_AFTER, 1));
        round.add(record(4, ChangeType.DELETE, 2));
        round.add(insert(5, 3));
        RoundMask mask = mask(0L, 1L);
        List<Long> passed = new ArrayList<>();
        for (LogRecord r : round) {
            if (pass(mask, r)) {
                passed.add(r.logOffset());
            }
        }
        assertThat(passed).containsExactly(3L, 4L, 5L);
        mask.finish();
    }

    /** A masked -U or -D is no version; a masked offset the round does not read is not either. */
    @Test
    void testMaskedOffsetsMustBeVersionsOfTheRound() throws Exception {
        assertThatThrownBy(() -> pass(mask(0L), record(0, ChangeType.UPDATE_BEFORE, 1)))
                .isInstanceOf(RoundMaskMismatchException.class)
                .hasMessageContaining("-U");
        assertThatThrownBy(() -> pass(mask(0L), record(0, ChangeType.DELETE, 1)))
                .isInstanceOf(RoundMaskMismatchException.class)
                .hasMessageContaining("-D");
        RoundMask mask = mask(0L, 7L);
        assertThat(pass(mask, insert(0, 1))).isFalse();
        assertThat(pass(mask, insert(1, 1))).isTrue();
        assertThatThrownBy(mask::finish)
                .isInstanceOf(RoundMaskMismatchException.class)
                .hasMessageContaining("masks 2 offsets");
    }

    /**
     * Random rounds of inserts, updates, deletes and re-inserts of a few keys. With the exact mask,
     * or one that misses versions as under row TTL, the reader passes exactly the versions the mask
     * keeps and the checks pass. A round that misses one of its changes always fails.
     */
    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
    void testChecksOnRandomRounds(long seed) throws Exception {
        Random random = new Random(seed);
        for (int trial = 0; trial < 200; trial++) {
            int keys = 1 + random.nextInt(20);
            int changes = random.nextInt(200);
            List<LogRecord> round = new ArrayList<>();
            // the latest version of each key, its offset
            Map<Integer, Long> live = new HashMap<>();
            // the round's changes, its +U and -D records
            List<LogRecord> changeRecords = new ArrayList<>();
            Roaring64Bitmap exact = new Roaring64Bitmap();
            long offset = 100;
            for (int c = 0; c < changes; c++) {
                int k = random.nextInt(keys);
                Long previous = live.get(k);
                int op = random.nextInt(10);
                if (previous == null) {
                    round.add(insert(offset, k));
                    live.put(k, offset++);
                } else if (op < 2) {
                    round.add(record(offset++, ChangeType.DELETE, k));
                    changeRecords.add(round.get(round.size() - 1));
                    exact.addLong(previous);
                    live.remove(k);
                } else {
                    round.add(record(offset++, ChangeType.UPDATE_BEFORE, k));
                    round.add(record(offset, ChangeType.UPDATE_AFTER, k));
                    changeRecords.add(round.get(round.size() - 1));
                    exact.addLong(previous);
                    live.put(k, offset++);
                }
            }
            // the exact mask, and one that misses some versions
            Roaring64Bitmap missing = exact.clone();
            for (long v : exact.toArray()) {
                if (random.nextInt(3) == 0) {
                    missing.removeLong(v);
                }
            }
            long counted = changeRecords.size();
            for (Roaring64Bitmap m : new Roaring64Bitmap[] {exact, missing}) {
                RoundMask mask = new RoundMask(BUCKET, fetched(m.clone(), counted));
                for (LogRecord r : round) {
                    boolean passes = pass(mask, r);
                    assertThat(passes)
                            .as("trial %d, offset %d", trial, r.logOffset())
                            .isEqualTo(
                                    r.getChangeType() != ChangeType.UPDATE_BEFORE
                                            && !m.contains(r.logOffset()));
                }
                mask.finish();
            }
            if (changeRecords.isEmpty()) {
                continue;
            }
            // the round without one of its changes
            LogRecord missed = changeRecords.get(random.nextInt(changeRecords.size()));
            RoundMask mask = new RoundMask(BUCKET, fetched(exact.clone(), counted));
            assertThatThrownBy(
                            () -> {
                                for (LogRecord r : round) {
                                    if (r != missed) {
                                        pass(mask, r);
                                    }
                                }
                                mask.finish();
                            })
                    .as("trial %d", trial)
                    .isInstanceOf(RoundMaskMismatchException.class);
        }
    }

    /**
     * The parts of a mask must be of the round's snapshot, start and end, from one leader epoch,
     * and mask offsets of the round only, as bitmaps that read; their union is the mask.
     */
    @Test
    void testFetchChecksTheParts() throws Exception {
        TieringLogSplit split =
                TieringLogSplit.plannedRound(
                        TablePath.of("db", "t"), BUCKET, null, 10, 30, 7, false);
        List<GetDvSnapshotResponse> parts = new ArrayList<>();
        parts.add(part(7, 10, 30, 3, 20, 11L, 12L).setRoundChanges(4));
        parts.add(part(7, 10, 30, 3, 30, 25L).setRoundChanges(3));
        RoundMask.Fetched fetched = RoundMask.fetch(fetcher(parts), split, 1000);
        assertThat(fetched.offsets.toArray()).containsExactly(11L, 12L, 25L);
        assertThat(fetched.changes).isEqualTo(7L);
        assertThat(fetched.changesWithInserts).isFalse();
        // a part without a count, or one that counts otherwise, leaves the round without one
        parts.add(part(7, 10, 30, 3, 20).setRoundChanges(4));
        parts.add(part(7, 10, 30, 3, 30));
        assertThat(RoundMask.fetch(fetcher(parts), split, 1000).changes).isNull();
        parts.add(part(7, 10, 30, 3, 20).setRoundChanges(4));
        parts.add(part(7, 10, 30, 3, 30).setRoundChanges(3).setRoundChangesWithInserts(true));
        assertThat(RoundMask.fetch(fetcher(parts), split, 1000).changes).isNull();

        assertFetchFails(split, part(8, 10, 30, 3, 30), "of snapshot 8");
        assertFetchFails(split, part(7, 0, 30, 3, 30), "from 0");
        assertFetchFails(split, part(7, 10, 40, 3, 40), "to 40");
        assertFetchFails(split, part(7, 10, 30, 3, 30, 9L), "masks offsets from 9");
        assertFetchFails(split, part(7, 10, 30, 3, 30, 30L), "to 30");
        assertFetchFails(split, part(7, 10, 30, 3, 10), "next part at 10");
        assertFetchFails(
                split,
                part(7, 10, 30, 3, 30).setLogDvBitmap(new byte[] {1, 2, 3}),
                "is not a bitmap of offsets");
        parts.clear();
        parts.add(part(7, 10, 30, 3, 20));
        parts.add(part(7, 10, 30, 4, 30));
        assertThatThrownBy(() -> RoundMask.fetch(fetcher(parts), split, 1000))
                .isInstanceOf(RoundMaskMismatchException.class)
                .hasMessageContaining("leader epochs 3 and 4");
    }

    /**
     * The round's changes, its +U and -D records (and +I under row TTL), must be as many as its
     * leader counted: a round that misses one fails, also where the other checks cannot see it.
     */
    @Test
    void testChangesMustBeTheLeadersCount() throws Exception {
        // k1 at 0, k2 at 1, k1 updated at 2/3, k2 deleted at 4: two changes, one masked version
        List<LogRecord> round = new ArrayList<>();
        round.add(insert(0, 1));
        round.add(insert(1, 2));
        round.add(record(2, ChangeType.UPDATE_BEFORE, 1));
        round.add(record(3, ChangeType.UPDATE_AFTER, 1));
        round.add(record(4, ChangeType.DELETE, 2));
        Roaring64Bitmap versions = new Roaring64Bitmap();
        versions.addLong(0);
        versions.addLong(1);

        RoundMask whole = new RoundMask(BUCKET, fetched(versions.clone(), 2L));
        for (LogRecord r : round) {
            pass(whole, r);
        }
        whole.finish();

        // the delete at 4 never reached the reader: the masked offsets are all read
        RoundMask partial = new RoundMask(BUCKET, fetched(versions.clone(), 2L));
        for (LogRecord r : round.subList(0, 4)) {
            pass(partial, r);
        }
        assertThatThrownBy(partial::finish)
                .isInstanceOf(RoundMaskMismatchException.class)
                .hasMessageContaining("counts 2 changes")
                .hasMessageContaining("read 1");

        // under row TTL the inserts count too
        RoundMask withInserts =
                new RoundMask(BUCKET, new RoundMask.Fetched(versions.clone(), 4L, true));
        for (LogRecord r : round) {
            pass(withInserts, r);
        }
        withInserts.finish();
    }

    // ---- helpers --------------------------------------------------------------------------------

    /** Passes a record through the mask, as the reader does. */
    private static boolean pass(RoundMask mask, LogRecord r) throws Exception {
        return mask.pass(r.logOffset(), r.getChangeType());
    }

    private static RoundMask.Fetched fetched(Roaring64Bitmap offsets, Long changes) {
        return new RoundMask.Fetched(offsets, changes, false);
    }

    private void assertFetchFails(TieringLogSplit split, GetDvSnapshotResponse part, String what) {
        assertThatThrownBy(
                        () ->
                                RoundMask.fetch(
                                        fetcher(new ArrayList<>(Collections.singletonList(part))),
                                        split,
                                        1000))
                .isInstanceOf(RoundMaskMismatchException.class)
                .hasMessageContaining(what);
    }

    private static RoundMask.PartFetcher fetcher(List<GetDvSnapshotResponse> parts) {
        return (from, maxWaitMs) -> parts.remove(0);
    }

    private static GetDvSnapshotResponse part(
            long snapshot, long start, long end, int epoch, long next, Long... offsets)
            throws Exception {
        GetDvSnapshotResponse part =
                new GetDvSnapshotResponse()
                        .setRoundSnapshotId(snapshot)
                        .setSnapshotStartOffset(start)
                        .setRoundEndOffset(end)
                        .setLeaderEpoch(epoch)
                        .setRoundNextOffset(next);
        if (offsets.length > 0) {
            Roaring64Bitmap bitmap = new Roaring64Bitmap();
            for (long o : offsets) {
                bitmap.addLong(o);
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            bitmap.serialize(new DataOutputStream(out));
            part.setLogDvBitmap(out.toByteArray());
        }
        return part;
    }

    private RoundMask mask(Long... offsets) throws Exception {
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        for (long o : offsets) {
            bitmap.addLong(o);
        }
        return new RoundMask(BUCKET, fetched(bitmap, null));
    }

    private static LogRecord insert(long offset, int key) {
        return record(offset, ChangeType.INSERT, key);
    }

    private static LogRecord record(long offset, ChangeType changeType, int key) {
        return new GenericRecord(
                offset, 0L, changeType, GenericRow.of(key, BinaryString.fromString("v" + offset)));
    }
}
