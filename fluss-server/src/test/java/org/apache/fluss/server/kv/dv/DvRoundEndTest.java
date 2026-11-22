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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.server.kv.dv.DvRoundEnd.LogPiece;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTestUtils;
import org.apache.fluss.server.log.OffsetPosition;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The end of a round its leader plans ({@link DvRoundEnd}): the largest end within the record and
 * byte caps as the offset indexes place the records, past the start whenever there is anything to
 * tier.
 */
class DvRoundEndTest {

    @TempDir File tempDir;

    @Test
    void testNothingToTier() throws Exception {
        List<LogPiece> log = Collections.singletonList(segment(0, 100, 10, 10));
        assertEnd(DvRoundEnd.plan(log, 100, 100, 10, 10), 100, false);
        assertEnd(DvRoundEnd.plan(log, 100, 90, 10, 10), 100, false);
    }

    @Test
    void testRoundUpToTheHighWatermark() throws Exception {
        // appends of 10 records and 100 bytes
        List<LogPiece> log = Arrays.asList(segment(0, 100, 10, 100), segment(100, 200, 10, 100));
        assertEnd(DvRoundEnd.plan(log, 30, 170, 1000, 1 << 20), 170, false);
        // exactly the bytes up to the high watermark
        assertEnd(DvRoundEnd.plan(log, 30, 170, 1000, 1400), 170, false);
    }

    @Test
    void testRecordCap() throws Exception {
        List<LogPiece> log = Arrays.asList(segment(0, 100, 10, 100), segment(100, 200, 10, 100));
        assertEnd(DvRoundEnd.plan(log, 30, 200, 100, 1 << 20), 130, true);
        assertEnd(DvRoundEnd.plan(log, 30, 200, 1, 1 << 20), 31, true);
    }

    @Test
    void testByteCapEndsBeforeTheFirstAppendThatDoesNotFit() throws Exception {
        List<LogPiece> log = Arrays.asList(segment(0, 100, 10, 100), segment(100, 200, 10, 100));
        // from 30, at byte 300: the appends up to 70 hold 400 bytes, the next one 100 more
        assertEnd(DvRoundEnd.plan(log, 30, 200, 1000, 450), 70, true);
        assertEnd(DvRoundEnd.plan(log, 30, 200, 1000, 500), 80, true);
        // across the segments: the first holds 700 bytes from 30, the next 200 more
        assertEnd(DvRoundEnd.plan(log, 30, 200, 1000, 950), 120, true);
        // the next segment's first append does not fit: the round ends where the segment starts
        assertEnd(DvRoundEnd.plan(log, 30, 200, 1000, 750), 100, true);
    }

    @Test
    void testAnAppendAboveTheByteCapIsTiered() throws Exception {
        List<LogPiece> log = Collections.singletonList(segment(0, 100, 10, 100));
        assertEnd(DvRoundEnd.plan(log, 30, 100, 1000, 1), 40, true);
        // from the middle of an append, up to its end
        assertEnd(DvRoundEnd.plan(log, 35, 100, 1000, 1), 40, true);
        // the last append, up to the high watermark
        assertEnd(DvRoundEnd.plan(log, 95, 100, 1000, 1), 100, false);
    }

    @Test
    void testRemoteThenLocalSegments() throws Exception {
        // a remote segment that serves its offsets from 50 on, then a local one
        List<LogPiece> log =
                Arrays.asList(appends(0, 100, 10, 100).piece(50, 100), segment(100, 150, 10, 100));
        assertEnd(DvRoundEnd.plan(log, 60, 150, 1000, 1 << 20), 150, false);
        assertEnd(DvRoundEnd.plan(log, 60, 150, 1000, 500), 110, true);
        // offsets no segment serves hold no bytes
        assertEnd(DvRoundEnd.plan(log, 20, 150, 1000, 300), 80, true);
    }

    /**
     * Random logs of segments of appends, and random starts, high watermarks and caps: the end
     * stays within the caps, is past the start, and the next append after it would break the byte
     * cap. Only a round that cannot take a whole append takes the one holding its start.
     */
    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 5, 8, 13, 21, 34})
    void testRandomLogs(long seed) throws Exception {
        Random random = new Random(seed);
        for (int trial = 0; trial < 500; trial++) {
            List<LogPiece> log = new ArrayList<>();
            // the last offset of every append
            TreeSet<Long> appendEnds = new TreeSet<>();
            long offset = 0;
            int segments = 1 + random.nextInt(4);
            for (int s = 0; s < segments; s++) {
                Appends segment = new Appends(offset);
                int n = 1 + random.nextInt(8);
                for (int a = 0; a < n; a++) {
                    segment.append(1 + random.nextInt(20), 1 + random.nextInt(500));
                }
                appendEnds.addAll(segment.entries.keySet());
                offset = segment.end;
                log.add(segment.piece(segment.base, segment.end));
            }
            long logEnd = offset;
            long start = random.nextInt((int) logEnd);
            long hw = start + 1 + random.nextInt((int) (logEnd - start));
            long maxRecords = 1 + random.nextInt((int) logEnd + 5);
            long maxBytes = 1 + random.nextInt(3000);
            DvRoundEnd end = DvRoundEnd.plan(log, start, hw, maxRecords, maxBytes);
            long e = end.endOffset();
            String trialName = "trial " + trial + ": [" + start + ", " + e + ")";
            assertThat(e).as(trialName).isGreaterThan(start).isLessThanOrEqualTo(hw);
            assertThat(e - start).as(trialName).isLessThanOrEqualTo(maxRecords);
            assertThat(end.capped()).as(trialName).isEqualTo(e < hw);
            long limit = Math.min(hw, start + maxRecords);
            long firstAppendEnd = appendEnds.ceiling(start) + 1;
            if (e > firstAppendEnd) {
                assertThat(bytes(log, start, e)).as(trialName).isLessThanOrEqualTo(maxBytes);
            }
            if (e < limit) {
                // the round ends where an append starts, and that append breaks the byte cap
                assertThat(appendEnds).as(trialName).contains(e - 1);
                long next = Math.min(limit, appendEnds.ceiling(e) + 1);
                assertThat(bytes(log, start, next)).as(trialName).isGreaterThan(maxBytes);
            }
        }
    }

    /**
     * A real log of small segments, indexed at every append but each segment's first: a record of a
     * first append is placed where the second append starts.
     */
    @Test
    void testLocalLogSegments() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.LOG_INDEX_INTERVAL_SIZE, new MemorySize(1));
        LogTablet log = createLogTablet(conf);
        List<Integer> appendBytes = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            List<Object[]> rows = new ArrayList<>();
            for (int r = 0; r < 5; r++) {
                rows.add(new Object[] {1000 + i * 5 + r, String.format("v%02d", i)});
            }
            MemoryLogRecords records = genMemoryLogRecordsByObject(rows);
            appendBytes.add(records.sizeInBytes());
            log.appendAsLeader(records);
            if (i % 10 == 9) {
                log.roll(Optional.empty());
            }
        }
        log.updateHighWatermark(150);
        assertThat(new TreeSet<>(appendBytes)).hasSize(1);
        assertThat(log.logSegments()).hasSizeGreaterThanOrEqualTo(3);
        List<LogPiece> pieces =
                DvRoundEnd.logPieces(new TableBucket(DATA1_TABLE_ID, 0), log, null, 0, 150);

        assertEnd(DvRoundEnd.plan(pieces, 0, 150, 1000, 1 << 20), 150, false);
        assertEnd(DvRoundEnd.plan(pieces, 12, 150, 20, 1 << 20), 32, true);
        long append = appendBytes.get(0);
        // three appends from 10, which the index places at the third append
        assertEnd(DvRoundEnd.plan(pieces, 10, 150, 1000, 3 * append), 25, true);
        // past a segment boundary
        assertEnd(DvRoundEnd.plan(pieces, 40, 150, 1000, 4 * append), 60, true);
        // an append larger than the cap: the first indexed one
        assertEnd(DvRoundEnd.plan(pieces, 0, 150, 1000, 1), 10, true);
    }

    // ---- helpers --------------------------------------------------------------------------------

    private static void assertEnd(DvRoundEnd end, long offset, boolean capped) {
        assertThat(end.endOffset()).isEqualTo(offset);
        assertThat(end.capped()).isEqualTo(capped);
    }

    /** The bytes of [from, to) as the pieces place them. */
    private static long bytes(List<LogPiece> log, long from, long to) throws Exception {
        long bytes = 0;
        for (LogPiece piece : log) {
            long a = Math.max(from, piece.startOffset());
            long b = Math.min(to, piece.endOffset());
            if (a < b) {
                bytes += piece.positionOf(b) - piece.positionOf(a);
            }
        }
        return bytes;
    }

    private static Appends appends(long base, long end, int records, int bytes) {
        Appends segment = new Appends(base);
        while (segment.end < end) {
            segment.append(records, bytes);
        }
        return segment;
    }

    /** Segment [base, end) of appends of {@code records} records and {@code bytes} bytes. */
    private static LogPiece segment(long base, long end, int records, int bytes) {
        Appends segment = appends(base, end, records, bytes);
        return segment.piece(base, segment.end);
    }

    /** A segment of appends, each with an index entry. */
    private static final class Appends {
        final long base;
        // the last offset of an append -> where it starts
        final TreeMap<Long, Integer> entries = new TreeMap<>();
        int size;
        long end;

        Appends(long base) {
            this.base = base;
            this.end = base;
        }

        void append(int records, int bytes) {
            end += records;
            entries.put(end - 1, size);
            size += bytes;
        }

        /** The segment as a piece that serves [start, servedEnd). */
        LogPiece piece(long start, long servedEnd) {
            return new DvRoundEnd.IndexedPiece(start, servedEnd, base, end, size) {
                @Override
                protected OffsetPosition ceiling(long offset) {
                    Map.Entry<Long, Integer> e = entries.ceilingEntry(offset);
                    return e == null ? null : new OffsetPosition(e.getKey(), e.getValue());
                }
            };
        }
    }

    private LogTablet createLogTablet(Configuration config) throws Exception {
        File logDir =
                LogTestUtils.makeRandomLogTabletDir(
                        tempDir,
                        DATA1_TABLE_PATH.getDatabaseName(),
                        DATA1_TABLE_ID,
                        DATA1_TABLE_PATH.getTableName());
        FlussScheduler scheduler = new FlussScheduler(1);
        scheduler.startup();
        return LogTablet.create(
                tempDir,
                PhysicalTablePath.of(DATA1_TABLE_PATH),
                logDir,
                config,
                new AtomicBoolean(false),
                TestingMetricGroups.TABLET_SERVER_METRICS,
                0,
                scheduler,
                LogFormat.ARROW,
                1,
                false,
                SystemClock.getInstance(),
                true);
    }
}
