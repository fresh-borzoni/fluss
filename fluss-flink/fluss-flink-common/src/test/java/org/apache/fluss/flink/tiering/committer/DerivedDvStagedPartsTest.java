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

package org.apache.fluss.flink.tiering.committer;

import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.index.FlussPositionIndexStore;
import org.apache.fluss.lake.dv.index.KeyPosition;
import org.apache.fluss.lake.dv.index.PositionIndexDelta;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.lake.dv.index.StagedIndexPart;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the deleted positions a round with deletion vectors collects, the lookups it streams while
 * it publishes the writers' staged parts, and the staging of index deltas that write results of an
 * older version carry.
 */
class DerivedDvStagedPartsTest {

    @TempDir Path tempDir;

    @Test
    void testDeletedPositionsAreArraysOnlyWhenRead() {
        DeletedPositions deleted = new DeletedPositions();
        deleted.add("a.parquet", 7);
        deleted.add("a.parquet", 3);
        deleted.add("a.parquet", 7);
        deleted.add("b.parquet", 1L << 33);
        DeletedPositions other = new DeletedPositions();
        other.add("a.parquet", 5);
        other.add("c.parquet", 0);
        deleted.addAll(other);

        assertThat(deleted.positions()).isEqualTo(5);
        assertThat(deleted).hasSize(3).containsKeys("a.parquet", "b.parquet", "c.parquet");
        assertThat(deleted.containsKey("d.parquet")).isFalse();
        assertThat(deleted.get("a.parquet")).containsExactly(3, 5, 7);
        Map<String, long[]> read = new HashMap<>();
        for (Map.Entry<String, long[]> e : deleted.entrySet()) {
            read.put(e.getKey(), e.getValue());
        }
        assertThat(read.get("b.parquet")).containsExactly(1L << 33);
        assertThat(read.get("c.parquet")).containsExactly(0L);
    }

    @Test
    void testAnIndexDeltaOfAnEarlierVersionIsStaged() throws Exception {
        PositionIndexStore store =
                new FlussPositionIndexStore(new FsPath(tempDir.resolve("lake").toUri()));
        TableBucket bucket = new TableBucket(1L, 0);
        byte[] high = new byte[] {(byte) 0x80};
        PositionIndexDelta delta =
                new PositionIndexDelta(
                        Arrays.asList(bytes("k2"), bytes("k1"), high, bytes("k3")),
                        Arrays.asList(
                                new KeyPosition(high, "b.parquet", 4),
                                new KeyPosition(bytes("k1"), "a.parquet", 0)),
                        Collections.singletonList(bytes("k3")));
        StagedIndexPart part = DerivedDvTieringCommitter.stageDelta(store, 77L, bucket, delta);
        assertThat(part.stagingId()).isEqualTo(77L);
        // k2 was changed without a new row: it is deleted like k3
        assertThat(part.keys()).isEqualTo(4);
        assertThat(part.deletes()).isEqualTo(2);
        assertThat(part.files()).hasSize(2).containsValues("a.parquet", "b.parquet");

        Map<Integer, Integer> newIds = new HashMap<>();
        for (int id : part.files().keySet()) {
            newIds.put(id, 10 + id);
        }
        List<String> keys = new ArrayList<>();
        store.publishStaged(
                77L,
                78L,
                bucket,
                part.part(),
                newIds,
                k -> keys.add(new String(k, StandardCharsets.ISO_8859_1)));
        // every changed key, in the unsigned order of the index
        assertThat(keys).containsExactly("k1", "k2", "k3", "\u0080");
    }

    /**
     * Tests that publishing streams the parts' keys to the lookups in chunks, so the keys held at
     * once stay bounded however many keys the round changed. The found positions end up exactly in
     * their files' bitmaps, and a bucket not in the base is not looked up.
     */
    @Test
    void testLookupsHoldABoundedNumberOfKeys() throws Exception {
        PositionIndexStore store =
                new FlussPositionIndexStore(new FsPath(tempDir.resolve("lake").toUri()));
        long stagingId = StagedIndexPart.stagingIdOf(1L, 5L);
        int keysPerBucket = 4 * DerivedDvTieringCommitter.LOOKUP_CHUNK + 123;
        Map<TableBucket, StagedIndexPart> parts = new HashMap<>();
        Map<TableBucket, Long> baseOffsets = new HashMap<>();
        for (int b = 0; b < 3; b++) {
            TableBucket bucket = new TableBucket(1L, b);
            try (PositionIndexStore.StagedPartWriter writer =
                    store.stageBucket(stagingId, bucket, "w_", null)) {
                for (int k = 0; k < keysPerBucket; k++) {
                    if (k % 10 == 0) {
                        writer.delete(key(k));
                    } else {
                        writer.put(key(k), new FilePos(0, k));
                    }
                }
                parts.put(
                        bucket,
                        StagedIndexPart.of(
                                stagingId,
                                writer.finish(),
                                Collections.singletonMap(0, "new-" + b + ".parquet"),
                                keysPerBucket,
                                keysPerBucket / 10 + 1));
            }
            if (b < 2) {
                // bucket 2 is new: nothing of it is in the base
                baseOffsets.put(bucket, 100L);
            }
        }
        AtomicLong keysInFlight = new AtomicLong();
        AtomicLong mostKeysInFlight = new AtomicLong();
        AtomicLong lookedUp = new AtomicLong();
        Set<Integer> lookedUpBuckets = ConcurrentHashMap.newKeySet();
        DerivedDvTieringCommitter.PublishedParts published =
                DerivedDvTieringCommitter.publishParts(
                        store,
                        99L,
                        new LakeSnapshot(7L, baseOffsets),
                        parts,
                        (bucket, snapshotId, keys) -> {
                            assertThat(snapshotId).isEqualTo(7L);
                            lookedUpBuckets.add(bucket.getBucket());
                            long now = keysInFlight.addAndGet(keys.size());
                            mostKeysInFlight.accumulateAndGet(now, Math::max);
                            Thread.sleep(5);
                            GetDvSnapshotResponse response = new GetDvSnapshotResponse();
                            for (byte[] k : keys) {
                                int key = keyOf(k);
                                // every third key had a row in the base, in one of two files
                                if (key % 3 == 0) {
                                    response.addKeyPosition()
                                            .setKey(k)
                                            .setFilePath(
                                                    "old-" + bucket.getBucket() + "-" + (key % 2))
                                            .setPos(key);
                                }
                            }
                            lookedUp.addAndGet(keys.size());
                            keysInFlight.addAndGet(-keys.size());
                            return response;
                        });

        assertThat(lookedUpBuckets).containsExactlyInAnyOrder(0, 1);
        assertThat(lookedUp.get()).isEqualTo(2L * keysPerBucket);
        assertThat(mostKeysInFlight.get())
                .isLessThanOrEqualTo(
                        (long) DerivedDvTieringCommitter.LOOKUPS_IN_FLIGHT
                                * DerivedDvTieringCommitter.LOOKUP_CHUNK);
        assertThat(published.keys).isEqualTo(3L * keysPerBucket);
        long expectedFound = 0;
        for (int b = 0; b < 2; b++) {
            for (int file = 0; file < 2; file++) {
                List<Long> expected = new ArrayList<>();
                for (int k = 0; k < keysPerBucket; k++) {
                    if (k % 3 == 0 && k % 2 == file) {
                        expected.add((long) k);
                    }
                }
                expectedFound += expected.size();
                assertThat(published.deleted.get("old-" + b + "-" + file))
                        .containsExactly(expected.stream().mapToLong(Long::longValue).toArray());
            }
        }
        assertThat(published.found).isEqualTo(expectedFound);
        assertThat(published.deleted.positions()).isEqualTo(expectedFound);
        assertThat(published.deleted).hasSize(4);
        // every bucket's part is published, with the file ids from the table's counter
        assertThat(published.changes).hasSize(3);
        for (PositionIndexStore.BucketChange change : published.changes.values()) {
            assertThat(change.part.files).hasSize(1);
            assertThat(change.addedFiles).hasSize(1);
        }
    }

    private static byte[] key(int k) {
        return new byte[] {(byte) (k >>> 24), (byte) (k >>> 16), (byte) (k >>> 8), (byte) k};
    }

    private static int keyOf(byte[] k) {
        return ((k[0] & 0xff) << 24) | ((k[1] & 0xff) << 16) | ((k[2] & 0xff) << 8) | (k[3] & 0xff);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
