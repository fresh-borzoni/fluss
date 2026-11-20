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

package org.apache.fluss.client.tiering;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The deletes of a bucket's round, as they are fetched from its leader part by part. */
class RoundDeletesTest {

    private static final TableBucket BUCKET = new TableBucket(1L, 0);
    private static final long SNAPSHOT = 5L;
    private static final long START = 100L;
    private static final long END = 130L;

    /** The parts concatenate, a file named by two parts twice; each part asks from the last. */
    @Test
    void testPartsConcatenate() throws Exception {
        List<Long> asked = new ArrayList<>();
        RoundDeletes deletes =
                RoundDeletes.fetch(
                        BUCKET,
                        SNAPSHOT,
                        START,
                        END,
                        Long.MAX_VALUE,
                        (tb, snapshotId, from, end) -> {
                            assertThat(tb).isEqualTo(BUCKET);
                            assertThat(snapshotId).isEqualTo(SNAPSHOT);
                            assertThat(end).isEqualTo(END);
                            asked.add(from);
                            if (from == null) {
                                return part(START, 110L, "a", "b");
                            }
                            return from == 110L ? part(START, 120L, "b") : part(START, END, "c");
                        });

        assertThat(asked).containsExactly(null, 110L, 120L);
        assertThat(deletes.snapshotId()).isEqualTo(SNAPSHOT);
        assertThat(deletes.startOffset()).isEqualTo(START);
        assertThat(deletes.endOffset()).isEqualTo(END);
        List<String> paths = new ArrayList<>();
        for (RoundDeletes.File file : deletes.files()) {
            paths.add(file.path());
        }
        assertThat(paths).containsExactly("a", "b", "b", "c");
        assertThat(deletes.bytes()).isEqualTo(4 * (1 + 8));
    }

    /** A leader that reads the snapshot from another offset fails the fetch. */
    @Test
    void testWrongStartFails() {
        assertThatThrownBy(() -> fetch(Long.MAX_VALUE, part(START + 1, END, "a")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("reads base snapshot 5 from offset 101");
    }

    /** A next part that does not advance, or that is past the round's end, fails the fetch. */
    @Test
    void testNextPartOutsideTheRoundFails() {
        assertThatThrownBy(() -> fetch(Long.MAX_VALUE, part(START, START, "a")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("with the next part at 100");
        assertThatThrownBy(() -> fetch(Long.MAX_VALUE, part(START, END + 1, "a")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("with the next part at 131");
        assertThatThrownBy(() -> fetch(Long.MAX_VALUE, part(START, null, "a")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("with the next part at -1");
    }

    /** Deletes up to the cap are fetched; more fail the fetch before the next part is asked. */
    @Test
    void testDeletesOverTheCapFail() throws Exception {
        assertThat(fetch(2 * 9, part(START, END, "a", "b")).files()).hasSize(2);
        List<Long> asked = new ArrayList<>();
        assertThatThrownBy(
                        () ->
                                RoundDeletes.fetch(
                                        BUCKET,
                                        SNAPSHOT,
                                        START,
                                        END,
                                        2 * 9 - 1,
                                        (tb, snapshotId, from, end) -> {
                                            asked.add(from);
                                            return part(START, 110L, "a", "b");
                                        }))
                .hasMessageContaining("take more than 17 bytes");
        assertThat(asked).containsExactly((Long) null);
    }

    private static RoundDeletes fetch(long maxBytes, GetDvSnapshotResponse part) throws Exception {
        return RoundDeletes.fetch(
                BUCKET, SNAPSHOT, START, END, maxBytes, (tb, snapshotId, from, end) -> part);
    }

    /** A part of 8 bytes of positions per file. */
    private static GetDvSnapshotResponse part(long start, @Nullable Long next, String... files) {
        GetDvSnapshotResponse part = new GetDvSnapshotResponse().setSnapshotStartOffset(start);
        if (next != null) {
            part.setRoundNextOffset(next);
        }
        for (String file : Arrays.asList(files)) {
            part.addLakeDvEntry().setFilePath(file).setDeletedPositionsBitmap(new byte[8]);
        }
        return part;
    }
}
