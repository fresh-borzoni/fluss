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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.kv.dv.DvRestoredChangeList.Reason;
import org.apache.fluss.server.kv.snapshot.DvChangeListHandle;
import org.apache.fluss.server.kv.snapshot.KvFileHandle;
import org.apache.fluss.server.kv.snapshot.KvFileHandleAndLocalPath;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests when a new leader uses the change list it restored with its KV snapshot. */
class DvRestoredChangeListTest {

    // the change list covers [100, 300) exactly from 120 on; the KV snapshot is at 300
    private static final long FROM = 100;
    private static final long VALID_FROM = 120;
    private static final long X = 300;
    private static final String BUCKET = "table=db.t partition=- bucket=0";
    private static final TableBucket TABLE_BUCKET = new TableBucket(1, 0);

    @ParameterizedTest(name = "readable offset {0}: {1}")
    @CsvSource({
        // below what the change list covers exactly
        "0, range",
        "99, range",
        "100, range",
        "119, range",
        // from max(from, valid-from) to the KV snapshot's offset, inclusive
        "120, used",
        "200, used",
        "299, used",
        "300, used",
        // the lake is ahead of the KV snapshot
        "301, lake_ahead",
        "1000, lake_ahead",
    })
    void testRangeRule(long readableOffset, String expected) {
        assertThat(
                        DvRestoredChangeList.rangeRule(
                                        changeList(FROM, X, VALID_FROM), readableOffset)
                                .field)
                .isEqualTo(expected);
    }

    @Test
    void testValidFromBelowFrom() {
        // a change list carried over from a replay that started earlier: from bounds it
        DvChangeListHandle changeList = changeList(150, X, 120);

        assertThat(DvRestoredChangeList.rangeRule(changeList, 149)).isEqualTo(Reason.RANGE);
        assertThat(DvRestoredChangeList.rangeRule(changeList, 150)).isEqualTo(Reason.USED);
    }

    @Test
    void testReasonsThatHoldForAnyReadableOffset(@TempDir Path dir) throws Exception {
        DvChangeListHandle changeList = changeList(FROM, X, VALID_FROM);

        assertThat(decide(dir, false, false, changeList, X, true)).isEqualTo(Reason.OFF);
        assertThat(decide(dir, true, true, changeList, X, true)).isEqualTo(Reason.TTL);
        assertThat(decide(dir, true, false, null, X, true)).isEqualTo(Reason.NONE);
        assertThat(decide(dir, true, false, DvChangeListHandle.unsupported(2), X, true))
                .isEqualTo(Reason.VERSION);
        assertThat(decide(dir, true, false, changeList, X + 1, true))
                .isEqualTo(Reason.END_MISMATCH);
        assertThat(decide(dir, true, false, changeList, X, false)).isEqualTo(Reason.KV_VIEW);
        assertThat(decide(dir, true, false, changeList, X, true)).isEqualTo(Reason.USED);
        assertThat(DvRestoredChangeList.none(BUCKET, TABLE_BUCKET, true, false).decide(200, now()))
                .isEqualTo(Reason.NONE);
        assertThat(DvRestoredChangeList.none(BUCKET, TABLE_BUCKET, false, false).decide(200, now()))
                .isEqualTo(Reason.OFF);
        assertThat(DvRestoredChangeList.none(BUCKET, TABLE_BUCKET, true, true).decide(200, now()))
                .isEqualTo(Reason.TTL);
    }

    @Test
    void testUnusableChangeListClosesItsViewAtOnce(@TempDir Path dir) throws Exception {
        DvKvView view = view(dir, now());

        DvRestoredChangeList restored =
                DvRestoredChangeList.restored(
                        BUCKET,
                        TABLE_BUCKET,
                        true,
                        true,
                        5,
                        X,
                        changeList(FROM, X, VALID_FROM),
                        view,
                        3);

        assertThat(restored.view()).isNull();
        assertThat(view.dir()).doesNotExist();
    }

    @Test
    void testReleaseAfterInstall(@TempDir Path dir) throws Exception {
        DvKvView view = view(dir, now());
        DvRestoredChangeList restored =
                DvRestoredChangeList.restored(
                        BUCKET,
                        TABLE_BUCKET,
                        true,
                        false,
                        5,
                        X,
                        changeList(FROM, X, VALID_FROM),
                        view,
                        3);
        assertThat(restored.decide(200, now())).isEqualTo(Reason.USED);
        assertThat(restored.view()).isSameAs(view);

        restored.release(Reason.RELEASED);

        assertThat(restored.view()).isNull();
        assertThat(view.dir()).doesNotExist();
        assertThat(restored.decide(200, now())).isEqualTo(Reason.RELEASED);
        // a later release keeps the first reason
        restored.release(Reason.VIEW_EXPIRED);
        assertThat(restored.decide(200, now())).isEqualTo(Reason.RELEASED);
    }

    @Test
    void testViewExpiresFifteenMinutesAfterTheLeaderStarts(@TempDir Path dir) throws Exception {
        long linkedAt = now();
        DvKvView view = view(dir, linkedAt);
        DvRestoredChangeList restored =
                DvRestoredChangeList.restored(
                        BUCKET,
                        TABLE_BUCKET,
                        true,
                        false,
                        5,
                        X,
                        changeList(FROM, X, VALID_FROM),
                        view,
                        3);
        long justBefore = linkedAt + DvRestoredChangeList.VIEW_MAX_AGE_MS - 1;

        assertThat(restored.expireIfDue(justBefore)).isFalse();
        assertThat(restored.decide(200, justBefore)).isEqualTo(Reason.USED);

        assertThat(restored.decide(200, linkedAt + DvRestoredChangeList.VIEW_MAX_AGE_MS))
                .isEqualTo(Reason.VIEW_EXPIRED);
        assertThat(view.dir()).doesNotExist();
        assertThat(restored.expireIfDue(Long.MAX_VALUE)).isFalse();
        assertThat(restored.decide(200, now())).isEqualTo(Reason.VIEW_EXPIRED);
    }

    @Test
    void testViewDoesNotExpireWhileALoadUsesIt(@TempDir Path dir) throws Exception {
        long linkedAt = now();
        DvKvView view = view(dir, linkedAt);
        DvRestoredChangeList restored =
                DvRestoredChangeList.restored(
                        BUCKET,
                        TABLE_BUCKET,
                        true,
                        false,
                        5,
                        X,
                        changeList(FROM, X, VALID_FROM),
                        view,
                        3);
        long age = DvRestoredChangeList.VIEW_MAX_AGE_MS;

        assertThat(restored.beginUse(linkedAt + 1)).isSameAs(view);
        // a load still reads the view past its age: it stays
        assertThat(restored.expireIfDue(linkedAt + age + 1)).isFalse();
        assertThat(restored.nextExpiryCheckMs(linkedAt + age + 1)).isEqualTo(age);
        assertThat(view.dir()).exists();

        // it expires 15 minutes after the load's last use
        long ended = linkedAt + age + 2;
        restored.endUse(ended);
        assertThat(restored.expireIfDue(ended + age - 1)).isFalse();
        assertThat(restored.nextExpiryCheckMs(ended + age - 1)).isEqualTo(1);
        assertThat(restored.expireIfDue(ended + age)).isTrue();
        assertThat(view.dir()).doesNotExist();
        assertThat(restored.releasedAs()).isEqualTo(Reason.VIEW_EXPIRED);
        assertThat(restored.beginUse(ended + age)).isNull();
    }

    @Test
    void testSegmentsFromTheReadableOffset(@TempDir Path dir) throws Exception {
        DvChangeListHandle.Segment a = segment(100, 150, 2);
        DvChangeListHandle.Segment b = segment(150, 220, 3);
        DvChangeListHandle.Segment c = segment(220, 300, 4);
        DvRestoredChangeList restored =
                DvRestoredChangeList.restored(
                        BUCKET,
                        TABLE_BUCKET,
                        true,
                        false,
                        5,
                        X,
                        new DvChangeListHandle(100, 300, 100, 1, 9, Arrays.asList(a, b, c)),
                        view(dir, now()),
                        3);

        assertThat(restored.segmentsFrom(100)).containsExactly(a, b, c);
        assertThat(restored.segmentsFrom(150)).containsExactly(b, c);
        assertThat(restored.segmentsFrom(219)).containsExactly(b, c);
        assertThat(restored.segmentsFrom(220)).containsExactly(c);
        assertThat(restored.segmentsFrom(300)).isEmpty();
        restored.release(Reason.RELEASED);
    }

    // ---------------------------------------------------------------------------------------------

    private static Reason decide(
            Path dir,
            boolean enabled,
            boolean rowTtl,
            DvChangeListHandle changeList,
            long kvOffset,
            boolean linked)
            throws Exception {
        DvKvView view = linked ? view(Files.createTempDirectory(dir, "v"), now()) : null;
        DvRestoredChangeList restored =
                DvRestoredChangeList.restored(
                        BUCKET, TABLE_BUCKET, enabled, rowTtl, 5, kvOffset, changeList, view, 3);
        try {
            return restored.decide(200, now());
        } finally {
            restored.release(Reason.RELEASED);
        }
    }

    private static DvKvView view(Path dir, long linkedAt) throws Exception {
        Path viewDir = Files.createDirectories(dir.resolve("view"));
        Files.write(viewDir.resolve("CURRENT"), new byte[] {1});
        return DvKvView.of(viewDir, 1, linkedAt, v -> 0);
    }

    private static DvChangeListHandle changeList(long from, long end, long validFrom) {
        return new DvChangeListHandle(
                from, end, validFrom, 1, 1, Arrays.asList(segment(from, end, 1)));
    }

    private static DvChangeListHandle.Segment segment(long from, long end, long entries) {
        return new DvChangeListHandle.Segment(
                KvFileHandleAndLocalPath.of(
                        new KvFileHandle("/remote/shared/dvcl-" + from, 10), "dvcl-" + from),
                from,
                end,
                entries);
    }

    private static long now() {
        return System.currentTimeMillis();
    }
}
