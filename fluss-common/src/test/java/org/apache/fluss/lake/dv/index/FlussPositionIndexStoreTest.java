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

package org.apache.fluss.lake.dv.index;

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.SstFileReader;
import org.rocksdb.SstFileReaderIterator;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Tests for {@link FlussPositionIndexStore} and the {@link PositionIndexChangeBuilder} on it. */
class FlussPositionIndexStoreTest {

    private static final long TABLE_ID = 1L;
    private static final TableBucket B0 = new TableBucket(TABLE_ID, 0);

    @TempDir Path tempDir;

    private Path root;
    private FlussPositionIndexStore store;

    @BeforeEach
    void setUp() throws IOException {
        root = Files.createDirectories(tempDir.resolve("lake"));
        store = new FlussPositionIndexStore(new FsPath(root.toUri()));
    }

    // ------------------------------------------------------------------------------------------
    // writing and reading a change
    // ------------------------------------------------------------------------------------------

    @Test
    void testBuilderChangeRoundTripsSortedWithTombstones() throws Exception {
        byte[] high = new byte[] {(byte) 0x80, 1}; // sorts after "k*" unsigned, before signed
        byte[] k1 = bytes("k1");
        byte[] k2 = bytes("k2");
        byte[] k3 = bytes("k3");
        Map<String, Integer> fileIds = new HashMap<>();
        fileIds.put("a.parquet", 1);
        fileIds.put("b.parquet", 2);
        TableBucket bucket = new TableBucket(TABLE_ID, 3L, 2);
        PositionIndexChangeBuilder builder = new PositionIndexChangeBuilder(store);
        builder.add(
                bucket,
                new PositionIndexDelta(
                        Arrays.asList(high, k1, k2, k3),
                        Arrays.asList(
                                new KeyPosition(high, "b.parquet", 7),
                                new KeyPosition(k2, "b.parquet", 5),
                                new KeyPosition(k1, "a.parquet", 0)),
                        Collections.singletonList(k3)),
                fileIds);
        builder.commit(9);
        store.register(-1, 100, 9);

        assertThat(root.resolve("rowPos/9/3/rowpos.manifest")).exists();
        PositionIndexStore.BucketData data = store.readChange(chainEntry(100), bucket, dl());
        assertThat(data.fileId2Name).containsOnly(entry(1, "a.parquet"), entry(2, "b.parquet"));
        assertThat(data.removedFiles).isEmpty();
        assertThat(data.localSsts).hasSize(1);
        // unsigned key order; the k3 tombstone is not visible to reads
        assertThat(readSst(Paths.get(data.localSsts.get(0))))
                .containsExactly(
                        "k1=" + new FilePos(1, 0),
                        "k2=" + new FilePos(2, 5),
                        "80 01=" + new FilePos(2, 7));
    }

    @Test
    void testChangeWithRemovedFilesAndUntouchedBuckets() throws Exception {
        TableBucket b0 = new TableBucket(TABLE_ID, 5L, 0);
        TableBucket b1 = new TableBucket(TABLE_ID, 5L, 1);
        Map<TableBucket, PositionIndexStore.BucketChange> change = new HashMap<>();
        change.put(
                b0,
                new PositionIndexStore.BucketChange(
                        store.writeBucket(42, b0, entries("a", 4, 0)),
                        Collections.singletonMap(4, "n.parquet"),
                        Arrays.asList("old1.parquet", "old2.parquet")));
        store.commitChange(42, change);
        store.register(-1, 100, 42);

        PositionIndexStore.BucketData d0 = store.readChange(chainEntry(100), b0, dl());
        assertThat(d0.localSsts).hasSize(1);
        assertThat(d0.fileId2Name).containsOnly(entry(4, "n.parquet"));
        assertThat(d0.removedFiles).containsExactly("old1.parquet", "old2.parquet");
        // untouched: another bucket of the partition, another partition, no partition
        assertThat(store.readChange(chainEntry(100), b1, dl()).localSsts).isEmpty();
        assertThat(store.readChange(chainEntry(100), new TableBucket(TABLE_ID, 6L, 0), dl()))
                .isSameAs(PositionIndexStore.BucketData.EMPTY);
        assertThat(store.readChange(chainEntry(100), B0, dl()))
                .isSameAs(PositionIndexStore.BucketData.EMPTY);
    }

    @Test
    void testEmptyChangeIsCommittedAndReadsEmpty() throws Exception {
        store.commitChange(7, Collections.emptyMap());
        store.register(-1, 100, 7);
        assertThat(store.readChange(chainEntry(100), B0, dl()))
                .isSameAs(PositionIndexStore.BucketData.EMPTY);
    }

    @Test
    void testWriteBucketRejectsKeysOutOfOrder() {
        TreeMap<byte[], FilePos> wrong = new TreeMap<>((a, b) -> -compare(a, b)); // descending
        wrong.put(bytes("a"), new FilePos(1, 0));
        wrong.put(bytes("b"), new FilePos(1, 1));
        assertThatThrownBy(() -> store.writeBucket(1, B0, wrong.entrySet().iterator()))
                .isInstanceOf(IOException.class);
    }

    @Test
    void testCommitChangeChecksPartsAndIsFinal() throws Exception {
        PositionIndexStore.BucketPart part = store.writeBucket(5, B0, entries("a", 1, 0));
        Files.delete(root.resolve("rowPos/5/0/" + part.files.get(0).getFileName()));
        assertThatThrownBy(() -> store.commitChange(5, change(B0, part)))
                .hasMessageContaining("is missing");

        PositionIndexStore.BucketPart again = store.writeBucket(5, B0, entries("a", 1, 0));
        store.commitChange(5, change(B0, again));
        assertThatThrownBy(() -> store.commitChange(5, change(B0, again)))
                .hasMessageContaining("already committed");
    }

    @Test
    void testMissingDataOfARegisteredChangeFailsTheRead() throws Exception {
        PositionIndexStore.BucketPart part = store.writeBucket(5, B0, entries("a", 1, 0));
        store.commitChange(5, change(B0, part));
        store.register(-1, 100, 5);

        // an SST gone (e.g. a trim racing a reader)
        Files.delete(root.resolve("rowPos/5/0/" + part.files.get(0).getFileName()));
        assertThatThrownBy(() -> store.readChange(chainEntry(100), B0, dl()))
                .isInstanceOf(IOException.class);
        Files.delete(root.resolve("rowPos/5/rowpos.manifest"));
        assertThatThrownBy(() -> store.readChange(chainEntry(100), B0, dl()))
                .isInstanceOf(IOException.class);
        // the whole change gone: never read as "no change"
        deleteRecursively(root.resolve("rowPos/5"));
        assertThatThrownBy(() -> store.readChange(chainEntry(100), B0, dl()))
                .hasMessageContaining("is gone");
    }

    @Test
    void testReadsChangesWrittenBeforeCommitMarkers() throws Exception {
        // version 1: index.json plus per-bucket filedict.txt and replaced.txt, old marker name
        Path dir = Files.createDirectories(root.resolve("rowPos/7/0"));
        Files.write(
                root.resolve("rowPos/7/index.json"),
                bytes("{\"version\":1,\"buckets\":{\"0\":{\"files\":[]}}}"));
        Files.write(dir.resolve("filedict.txt"), bytes("3\tx.parquet\n"));
        Files.write(dir.resolve("replaced.txt"), bytes("y.parquet\n"));
        Path chainDir = Files.createDirectories(root.resolve(FlussPositionIndexStore.CHAIN_DIR));
        Files.write(chainDir.resolve(String.format("%020d-%d-%d", 1, 100, 7)), new byte[0]);
        // an old change without any manifest for this partition: untouched
        Files.write(chainDir.resolve(String.format("%020d-%d-%d", 2, 101, 8)), new byte[0]);

        List<PositionIndexStore.ChainEntry> chain = store.chain();
        assertThat(chain.get(0).parentSnapshotId).isEqualTo(PositionIndexStore.UNKNOWN_PARENT);
        PositionIndexStore.BucketData c = store.readChange(chain.get(0), B0, dl());
        assertThat(c.fileId2Name).containsOnly(entry(3, "x.parquet"));
        assertThat(c.removedFiles).containsExactly("y.parquet");
        assertThat(store.readChange(chain.get(1), B0, dl()))
                .isSameAs(PositionIndexStore.BucketData.EMPTY);
    }

    // ------------------------------------------------------------------------------------------
    // the chain
    // ------------------------------------------------------------------------------------------

    @Test
    void testRegisterLinksChangesInOrder() throws Exception {
        commitEmpty(1);
        commitEmpty(2);
        commitEmpty(3);
        store.register(-1, 100, 1);
        store.register(100, 101, 2);
        store.register(101, 102, 3);

        assertThat(store.chain())
                .containsExactly(
                        new PositionIndexStore.ChainEntry(1, -1, 100, 1),
                        new PositionIndexStore.ChainEntry(2, 100, 101, 2),
                        new PositionIndexStore.ChainEntry(3, 101, 102, 3));
    }

    @Test
    void testRegisterIsIdempotentAndRefusesWrongParentOrChange() throws Exception {
        commitEmpty(1);
        commitEmpty(2);
        commitEmpty(3);
        store.register(-1, 100, 1);
        store.register(-1, 100, 1); // a retried or repaired round

        // the change was computed against another index than the chain's end
        assertThatThrownBy(() -> store.register(99, 101, 2))
                .hasMessageContaining("ends at snapshot 100");
        // a snapshot has exactly one change
        assertThatThrownBy(() -> store.register(100, 100, 3))
                .hasMessageContaining("registered with another change");
        assertThatThrownBy(() -> store.register(100, 101, 4)).hasMessageContaining("not committed");
        assertThat(store.chain()).hasSize(1);
    }

    @Test
    void testOrderComesFromSequenceNotTime() throws Exception {
        // legacy markers are named by commit time; new ones continue after them, whatever the clock
        marker(5_000_000_000_000L, 100, 1);
        commitEmpty(2);
        store.register(100, 101, 2);
        assertThat(store.chain()).extracting(e -> e.snapshotId).containsExactly(100L, 101L);
        assertThat(store.chain().get(1).sequence).isEqualTo(5_000_000_000_001L);
    }

    @Test
    void testBrokenChainIsRefused() throws Exception {
        commitEmpty(1);
        store.register(-1, 100, 1);
        // a marker whose parent is not its predecessor (corruption, or a bug elsewhere)
        Files.write(
                root.resolve(FlussPositionIndexStore.CHAIN_DIR)
                        .resolve(String.format("%020d_%d_%d_%d", 2, 99, 101, 2)),
                new byte[0]);
        assertThatThrownBy(() -> store.chain()).hasMessageContaining("chain is broken");
    }

    @Test
    void testAbortOnlyUnregisteredChanges() throws Exception {
        store.writeBucket(1, B0, entries("a", 1, 0));
        store.abortChange(1);
        assertThat(indexDir(1)).doesNotExist();
        store.abortChange(2); // nothing there

        commitEmpty(3);
        store.register(-1, 100, 3);
        assertThatThrownBy(() -> store.abortChange(3)).hasMessageContaining("is registered");
        assertThat(indexDir(3)).exists();
    }

    @Test
    void testRegistrationFencesFileIdsHandedOutTwice() throws Exception {
        // writer A registers a change adding file ids 5..6
        store.commitChange(1, addedFiles(5, 6));
        store.register(-1, 100, 1);
        // a stale writer B was handed ids overlapping A's (e.g. a cached or raced counter)
        store.commitChange(2, addedFiles(6, 7));
        assertThatThrownBy(() -> store.register(100, 101, 2))
                .hasMessageContaining("ids up to 6 are registered already");
        // fresh ids, and changes without new files, register fine
        store.commitChange(3, addedFiles(7, 8));
        store.register(100, 101, 3);
        commitEmpty(4);
        store.register(101, 102, 4);
        assertThat(chainSnapshots()).containsExactly(100L, 101L, 102L);
    }

    @Test
    void testCounterIsReadEveryTime() throws Exception {
        FlussPositionIndexStore other = new FlussPositionIndexStore(new FsPath(root.toUri()));
        assertThat(store.allocateFileIds(3)).isZero();
        assertThat(other.allocateFileIds(2)).isEqualTo(3);
        // no stale cache: continues after the other writer's ids
        assertThat(store.allocateFileIds(1)).isEqualTo(5);
    }

    @Test
    void testDeleteOrphanChanges() throws Exception {
        store.writeBucket(10, B0, entries("a", 1, 0)); // a failed attempt: never committed
        commitEmpty(11); // committed, never registered (its lake commit failed)
        commitEmpty(12);
        store.register(-1, 100, 12);
        Files.createDirectories(indexDir(13).resolve("0")); // legacy: no start marker

        // nothing is old enough yet
        assertThat(store.deleteOrphanChanges(3_600_000L)).isZero();
        assertThat(store.deleteOrphanChanges(0)).isEqualTo(2);
        assertThat(indexDir(10)).doesNotExist();
        assertThat(indexDir(11)).doesNotExist();
        assertThat(indexDir(12)).exists();
        assertThat(indexDir(13)).exists();
        assertThat(store.readChange(chainEntry(100), B0, dl()))
                .isSameAs(PositionIndexStore.BucketData.EMPTY);
    }

    @Test
    void testFileIdsAreNeverHandedOutTwice() throws Exception {
        assertThat(store.allocateFileIds(3)).isZero();
        assertThat(store.allocateFileIds(2)).isEqualTo(3);
        // a restarted writer continues after every id handed out before
        assertThat(new FlussPositionIndexStore(new FsPath(root.toUri())).allocateFileIds(1))
                .isEqualTo(5);
    }

    // ------------------------------------------------------------------------------------------
    // trim
    // ------------------------------------------------------------------------------------------

    @Test
    void testBucketWithoutCheckpointBlocksTrimming() throws Exception {
        for (int i = 0; i < 6; i++) {
            marker(i, 100 + i, 1 + i);
        }
        checkpoint(null, 0, 104, true);
        // bucket 1 only has an incomplete checkpoint (no _DONE)
        checkpoint(null, 1, 104, false);
        List<String> markersBefore = chainMarkers();

        int trimmed =
                store.trim(
                        Arrays.asList(new TableBucket(TABLE_ID, 0), new TableBucket(TABLE_ID, 1)),
                        0);

        assertThat(trimmed).isZero();
        assertThat(chainMarkers()).isEqualTo(markersBefore);
        for (int i = 1; i <= 6; i++) {
            assertThat(indexDir(i)).exists();
        }
        assertThat(root.resolve(FlussPositionIndexStore.CHAIN_TRIMMED)).doesNotExist();
    }

    @Test
    void testTrimsEntriesOlderThanOldestCheckpointMinusSafety() throws Exception {
        for (int i = 0; i < 6; i++) {
            marker(i, 100 + i, 1 + i); // snapshot 100 + i at chain index i, change 1 + i
        }
        checkpoint(null, 0, 104, true); // chain index 4
        checkpoint(7L, 0, 103, true); // partition 7, chain index 3 (the oldest)
        checkpoint(7L, 0, 105, false); // newer, but incomplete: ignored

        int trimmed =
                store.trim(
                        Arrays.asList(
                                new TableBucket(TABLE_ID, 0), new TableBucket(TABLE_ID, 7L, 0)),
                        1);

        // oldest checkpoint at index 3, safety 1: entries 0 and 1 go
        assertThat(trimmed).isEqualTo(2);
        assertThat(chainSnapshots()).containsExactly(102L, 103L, 104L, 105L);
        assertThat(indexDir(1)).doesNotExist();
        assertThat(indexDir(2)).doesNotExist();
        for (int i = 3; i <= 6; i++) {
            assertThat(indexDir(i)).exists();
        }
        assertThat(read(root.resolve(FlussPositionIndexStore.CHAIN_TRIMMED))).isEqualTo("102");
        // checkpoints are untouched
        assertThat(checkpointDir(null, 0).resolve("104")).exists();
        assertThat(checkpointDir(7L, 0).resolve("103")).exists();
        assertThat(checkpointDir(7L, 0).resolve("105")).exists();

        // trimming again with the same checkpoints removes nothing more
        assertThat(
                        store.trim(
                                Arrays.asList(
                                        new TableBucket(TABLE_ID, 0),
                                        new TableBucket(TABLE_ID, 7L, 0)),
                                1))
                .isZero();
        assertThat(chainSnapshots()).containsExactly(102L, 103L, 104L, 105L);
    }

    @Test
    void testDuplicateMarkersOfOneSnapshot() throws Exception {
        marker(0, 100, 1);
        marker(1, 101, 2);
        marker(2, 100, 1); // a repaired round re-wrote the marker of snapshot 100
        marker(3, 102, 3);
        marker(4, 103, 4);
        marker(5, 104, 5);
        marker(6, 103, 4); // duplicate of a snapshot that is kept
        // the duplicate does not take a chain position: snapshot 103 is at index 3
        checkpoint(null, 0, 103, true);

        int trimmed = store.trim(Collections.singletonList(new TableBucket(TABLE_ID, 0)), 0);

        assertThat(trimmed).isEqualTo(3);
        // both markers of snapshot 100 are gone, both of 103 are kept
        assertThat(chainSnapshots()).containsExactly(103L, 104L, 103L);
        assertThat(indexDir(1)).doesNotExist();
        assertThat(indexDir(2)).doesNotExist();
        assertThat(indexDir(3)).doesNotExist();
        assertThat(indexDir(4)).exists();
        assertThat(indexDir(5)).exists();
        assertThat(read(root.resolve(FlussPositionIndexStore.CHAIN_TRIMMED))).isEqualTo("103");
    }

    @Test
    void testIndexFilesSharedWithKeptEntryAreKept() throws Exception {
        marker(0, 100, 1);
        marker(1, 101, 2);
        marker(2, 102, 2); // an index-only round reuses change 2
        marker(3, 103, 3);
        checkpoint(null, 0, 103, true);

        int trimmed = store.trim(Collections.singletonList(new TableBucket(TABLE_ID, 0)), 1);

        assertThat(trimmed).isEqualTo(2);
        assertThat(chainSnapshots()).containsExactly(102L, 103L);
        assertThat(indexDir(1)).doesNotExist();
        assertThat(indexDir(2)).exists();
        assertThat(indexDir(3)).exists();
    }

    @Test
    void testNothingToTrim() throws Exception {
        TableBucket bucket = new TableBucket(TABLE_ID, 0);
        // no chain yet
        assertThat(store.trim(Collections.singletonList(bucket), 0)).isZero();

        for (int i = 0; i < 4; i++) {
            marker(i, 100 + i, 1 + i);
        }
        checkpoint(null, 0, 102, true);
        // no bucket has data
        assertThat(store.trim(Collections.emptyList(), 0)).isZero();
        // the safety margin covers everything before the checkpoint
        assertThat(store.trim(Collections.singletonList(bucket), 2)).isZero();
        assertThat(store.trim(Collections.singletonList(bucket), 5)).isZero();
        // a checkpoint of a snapshot that is not in the chain does not count
        checkpoint(null, 1, 999, true);
        assertThat(store.trim(Arrays.asList(bucket, new TableBucket(TABLE_ID, 1)), 0)).isZero();
        assertThat(chainSnapshots()).containsExactly(100L, 101L, 102L, 103L);
        assertThat(root.resolve(FlussPositionIndexStore.CHAIN_TRIMMED)).doesNotExist();
    }

    @Test
    void testTrimRegisteredChanges() throws Exception {
        commitEmpty(1);
        commitEmpty(2);
        commitEmpty(3);
        store.register(-1, 100, 1);
        store.register(100, 101, 2);
        store.register(101, 102, 3);
        store.writeCheckpoint(102, B0, null, Collections.emptyMap());
        PositionIndexStore.ChainEntry first = store.chain().get(0);

        assertThat(store.trim(Collections.singletonList(B0), 0)).isEqualTo(2);
        assertThat(chainSnapshots()).containsExactly(102L);
        assertThat(indexDir(1)).doesNotExist();
        assertThat(store.chainTrimmed()).isTrue();
        // a reader that listed the chain before the trim fails instead of reading nothing
        assertThatThrownBy(() -> store.readChange(first, B0, dl())).hasMessageContaining("is gone");
        // the kept chain still links up, and grows from its end
        commitEmpty(4);
        store.register(102, 103, 4);
        assertThat(chainSnapshots()).containsExactly(102L, 103L);
    }

    // ------------------------------------------------------------------------------------------
    // checkpoints
    // ------------------------------------------------------------------------------------------

    @Test
    void testCheckpointRoundTripAndRetention() throws Exception {
        TableBucket bucket = new TableBucket(TABLE_ID, 9L, 1);
        for (int i = 0; i < 3; i++) {
            commitEmpty(i + 1);
            store.register(i == 0 ? -1 : 99 + i, 100 + i, i + 1);
        }
        File sst = Files.write(tempDir.resolve("full.sst"), new byte[] {1, 2, 3}).toFile();

        store.writeCheckpoint(100, bucket, sst, Collections.singletonMap(1, "a"));
        store.writeCheckpoint(101, bucket, null, Collections.emptyMap()); // empty index
        store.writeCheckpoint(102, bucket, sst, Collections.singletonMap(2, "b"));

        // the two newest are kept
        assertThat(checkpointDir(9L, 1).resolve("100")).doesNotExist();
        assertThat(checkpointDir(9L, 1).resolve("101")).exists();
        List<PositionIndexStore.ChainEntry> chain = store.chain();
        assertThat(store.latestCheckpoint(bucket, chain, 2).snapshotId).isEqualTo(102);
        assertThat(store.latestCheckpoint(bucket, chain, 1).chainIndex).isEqualTo(1);
        assertThat(store.latestCheckpoint(bucket, chain, 0)).isNull();
        assertThat(store.latestCheckpoint(new TableBucket(TABLE_ID, 9L, 0), chain, 2)).isNull();

        PositionIndexStore.BucketData full =
                store.readCheckpoint(store.latestCheckpoint(bucket, chain, 2), bucket, dl());
        assertThat(full.fileId2Name).containsOnly(entry(2, "b"));
        assertThat(Files.readAllBytes(Paths.get(full.localSsts.get(0)))).containsExactly(1, 2, 3);
        assertThat(
                        store.readCheckpoint(store.latestCheckpoint(bucket, chain, 1), bucket, dl())
                                .localSsts)
                .isEmpty();
    }

    @Test
    void testMissingCheckpointDataFailsTheRead() throws Exception {
        commitEmpty(1);
        store.register(-1, 100, 1);
        File sst = Files.write(tempDir.resolve("full.sst"), new byte[] {1}).toFile();
        store.writeCheckpoint(100, B0, sst, Collections.singletonMap(1, "a"));
        PositionIndexStore.Checkpoint cp = store.latestCheckpoint(B0, store.chain(), 0);

        // an index with an SST is never read as an empty index
        Files.delete(
                checkpointDir(null, 0).resolve("100").resolve(FlussPositionIndexStore.CKPT_SST));
        assertThatThrownBy(() -> store.readCheckpoint(cp, B0, dl()))
                .isInstanceOf(IOException.class);
        // deleted by retention while a rebuild picked it
        deleteRecursively(checkpointDir(null, 0).resolve("100"));
        assertThatThrownBy(() -> store.readCheckpoint(cp, B0, dl()))
                .hasMessageContaining("is gone");
    }

    // ------------------------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------------------------

    private PositionIndexStore.ChainEntry chainEntry(long snapshotId) throws IOException {
        for (PositionIndexStore.ChainEntry e : store.chain()) {
            if (e.snapshotId == snapshotId) {
                return e;
            }
        }
        throw new AssertionError("snapshot " + snapshotId + " not in the chain");
    }

    private static Map<TableBucket, PositionIndexStore.BucketChange> addedFiles(int... ids) {
        Map<Integer, String> files = new HashMap<>();
        for (int id : ids) {
            files.put(id, "f" + id + ".parquet");
        }
        return Collections.singletonMap(
                B0,
                new PositionIndexStore.BucketChange(
                        PositionIndexStore.BucketPart.NONE, files, Collections.emptyList()));
    }

    private void commitEmpty(long changeId) throws IOException {
        store.commitChange(changeId, Collections.emptyMap());
    }

    private static Map<TableBucket, PositionIndexStore.BucketChange> change(
            TableBucket bucket, PositionIndexStore.BucketPart part) {
        return Collections.singletonMap(
                bucket,
                new PositionIndexStore.BucketChange(
                        part, Collections.emptyMap(), Collections.emptyList()));
    }

    private static Iterator<Map.Entry<byte[], FilePos>> entries(String key, int fileId, long pos) {
        TreeMap<byte[], FilePos> m = new TreeMap<>(FlussPositionIndexStoreTest::compare);
        m.put(bytes(key), new FilePos(fileId, pos));
        return m.entrySet().iterator();
    }

    private static int compare(byte[] a, byte[] b) {
        return PositionIndexChangeBuilder.compareUnsigned(a, b);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private File dl() throws IOException {
        return Files.createTempDirectory(tempDir, "dl").toFile();
    }

    private static void deleteRecursively(Path p) throws IOException {
        try (Stream<Path> files = Files.walk(p)) {
            files.sorted(java.util.Comparator.reverseOrder()).forEach(f -> f.toFile().delete());
        }
    }

    private void marker(long millis, long snapshotId, long indexId) throws IOException {
        Path dir = Files.createDirectories(root.resolve(FlussPositionIndexStore.CHAIN_DIR));
        Files.write(
                dir.resolve(String.format("%020d-%d-%d", millis, snapshotId, indexId)),
                new byte[0]);
        Path index = Files.createDirectories(indexDir(indexId).resolve("0"));
        Files.write(index.resolve("sst_0.sst"), new byte[] {1});
        Files.write(indexDir(indexId).resolve("index.json"), new byte[] {1});
    }

    private void checkpoint(@Nullable Long partitionId, int bucket, long snapshotId, boolean done)
            throws IOException {
        Path dir =
                Files.createDirectories(
                        checkpointDir(partitionId, bucket).resolve(String.valueOf(snapshotId)));
        Files.write(dir.resolve(FlussPositionIndexStore.CKPT_SST), new byte[] {1});
        if (done) {
            Files.write(dir.resolve(FlussPositionIndexStore.CKPT_DONE), new byte[0]);
        }
    }

    private Path checkpointDir(@Nullable Long partitionId, int bucket) {
        return root.resolve(FlussPositionIndexStore.CKPT_DIR)
                .resolve(partitionId == null ? "" : String.valueOf(partitionId))
                .resolve(String.valueOf(bucket));
    }

    private Path indexDir(long indexId) {
        return root.resolve("rowPos").resolve(String.valueOf(indexId));
    }

    private List<String> chainMarkers() throws IOException {
        Path dir = root.resolve(FlussPositionIndexStore.CHAIN_DIR);
        if (!Files.exists(dir)) {
            return Collections.emptyList();
        }
        try (Stream<Path> files = Files.list(dir)) {
            return files.map(p -> p.getFileName().toString()).sorted().collect(Collectors.toList());
        }
    }

    /** Snapshot ids of the chain markers, in commit order (duplicates included). */
    private List<Long> chainSnapshots() throws IOException {
        List<Long> out = new ArrayList<>();
        for (String name : chainMarkers()) {
            out.add(FlussPositionIndexStore.parseMarker(name).snapshotId);
        }
        return out;
    }

    private static String read(Path path) throws IOException {
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }

    private static List<String> readSst(Path sst) throws Exception {
        RocksDB.loadLibrary();
        List<String> out = new ArrayList<>();
        try (Options options = new Options();
                SstFileReader reader = new SstFileReader(options);
                ReadOptions readOptions = new ReadOptions()) {
            reader.open(sst.toString());
            try (SstFileReaderIterator it = reader.newIterator(readOptions)) {
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    out.add(keyString(it.key()) + "=" + FilePos.decode(it.value()));
                }
            }
        }
        return out;
    }

    private static String keyString(byte[] key) {
        if (key.length > 0 && (key[0] & 0x80) != 0) {
            StringBuilder sb = new StringBuilder();
            for (byte b : key) {
                if (sb.length() > 0) {
                    sb.append(' ');
                }
                sb.append(String.format("%02x", b & 0xff));
            }
            return sb.toString();
        }
        return new String(key, StandardCharsets.UTF_8);
    }
}
