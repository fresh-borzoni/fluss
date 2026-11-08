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

import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.TestFileSystem;
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.FormatVersions;
import org.apache.fluss.lake.dv.RowPosSstIndex;
import org.apache.fluss.metadata.TableBucket;

import io.github.fluss_contrib.rocksdb.EnvOptions;
import io.github.fluss_contrib.rocksdb.IngestExternalFileOptions;
import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.ReadOptions;
import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.RocksIterator;
import io.github.fluss_contrib.rocksdb.SstFileReader;
import io.github.fluss_contrib.rocksdb.SstFileReaderIterator;
import io.github.fluss_contrib.rocksdb.SstFileWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
    private long lastMarkedSnapshot = -1;

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

    /**
     * Two writers append different steps to the same chain end at once, and both pass the checks
     * before either writes its marker. The writer that sees the other's marker after its write
     * withdraws its own, so the chain stays linear.
     */
    @Test
    void testRacingRegistrationsKeepTheChainLinear() throws Exception {
        commitEmpty(1);
        commitEmpty(2);
        commitEmpty(3);
        store.register(-1, 100, 1);
        FlussPositionIndexStore other = new FlussPositionIndexStore(new FsPath(root.toUri()));
        FlussPositionIndexStore racing =
                new FlussPositionIndexStore(new FsPath(root.toUri())) {
                    @Override
                    void beforeMarkerWrite() throws IOException {
                        // the other writer registers between this one's checks and its write
                        other.register(100, 101, 2);
                    }
                };

        assertThatThrownBy(() -> racing.register(100, 102, 3))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("concurrently")
                .hasMessageContaining("100 -> 102 (3) is withdrawn");
        assertThat(store.chain())
                .containsExactly(
                        new PositionIndexStore.ChainEntry(1, -1, 100, 1),
                        new PositionIndexStore.ChainEntry(2, 100, 101, 2));
        // the next step extends the registered one
        commitEmpty(4);
        store.register(101, 103, 4);
        assertThat(store.chain()).extracting(e -> e.snapshotId).containsExactly(100L, 101L, 103L);
    }

    /**
     * Writers racing to append the same step, such as two re-indexes of the same rewrite, both stay
     * registered, and the chain reads the same one of them every time.
     */
    @Test
    void testRacingRegistrationsOfTheSameStepReadAsOne() throws Exception {
        commitEmpty(1);
        commitEmpty(7);
        commitEmpty(5);
        store.register(-1, 100, 1);
        FlussPositionIndexStore other = new FlussPositionIndexStore(new FsPath(root.toUri()));
        FlussPositionIndexStore racing =
                new FlussPositionIndexStore(new FsPath(root.toUri())) {
                    @Override
                    void beforeMarkerWrite() throws IOException {
                        other.register(100, 101, 7);
                    }
                };
        racing.register(100, 101, 5);

        assertThat(store.chain())
                .containsExactly(
                        new PositionIndexStore.ChainEntry(1, -1, 100, 1),
                        new PositionIndexStore.ChainEntry(2, 100, 101, 5));
        store.register(100, 101, 5); // repeated: a no-op
        commitEmpty(8);
        store.register(101, 102, 8);
        assertThat(store.chain()).extracting(e -> e.snapshotId).containsExactly(100L, 101L, 102L);
    }

    @Test
    void testOrderComesFromSequenceNotTime() throws Exception {
        // registration continues after the highest sequence, whatever the clock
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
                        .resolve(FlussPositionIndexStore.markerName(2, 99, 101, 2, -1)),
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

    /**
     * Allocations stay above every registered id after a racing read-then-write moves the counter
     * back below them, so the next change registers.
     */
    @Test
    void testAllocationStaysAboveRegisteredIdsWhenTheCounterMovedBack() throws Exception {
        int first = store.allocateFileIds(10);
        store.commitChange(1, addedFiles(first, first + 9));
        store.register(-1, 100, 1);
        Files.write(
                root.resolve("rowPos/_next_file_id"), bytes(FormatVersions.withVersion(1, "3")));

        int next = store.allocateFileIds(2);
        assertThat(next).isGreaterThan(first + 9);
        store.commitChange(2, addedFiles(next, next + 1));
        store.register(100, 101, 2);
        assertThat(store.allocateFileIds(1)).isEqualTo(next + 2);
    }

    /** A part written with provisional file ids gets the committer's ids before its commit. */
    @Test
    void testRemapFileIdsRewritesThePart() throws Exception {
        TreeMap<byte[], FilePos> rows = new TreeMap<>(FlussPositionIndexStoreTest::compare);
        rows.put(bytes("k1"), new FilePos(0, 3));
        rows.put(bytes("k2"), new FilePos(1, 7));
        rows.put(bytes("k3"), new FilePos(0, 4));
        PositionIndexStore.BucketPart provisional =
                store.writeBucket(5, B0, rows.entrySet().iterator());
        Map<Integer, Integer> newIds = new HashMap<>();
        newIds.put(0, 20);
        newIds.put(1, 21);

        PositionIndexStore.BucketPart part = store.remapFileIds(5, B0, provisional, newIds);
        Map<Integer, String> added = new HashMap<>();
        added.put(20, "a.parquet");
        added.put(21, "b.parquet");
        store.commitChange(
                5,
                Collections.singletonMap(
                        B0,
                        new PositionIndexStore.BucketChange(part, added, Collections.emptyList())));
        store.register(-1, 100, 5);

        PositionIndexStore.BucketData data = store.readChange(chainEntry(100), B0, dl());
        assertThat(data.fileId2Name).containsOnly(entry(20, "a.parquet"), entry(21, "b.parquet"));
        List<String> indexed = new ArrayList<>();
        for (String sst : data.localSsts) {
            indexed.addAll(readSst(Paths.get(sst)));
        }
        assertThat(indexed)
                .containsExactly(
                        "k1=" + new FilePos(20, 3),
                        "k2=" + new FilePos(21, 7),
                        "k3=" + new FilePos(20, 4));
        // the provisional SSTs are gone
        for (RowPosSstIndex.SstFileEntry f : provisional.files) {
            assertThat(indexDir(5).resolve("0").resolve(f.getFileName())).doesNotExist();
        }

        // a position of a file without a new id fails the remap
        PositionIndexStore.BucketPart other = store.writeBucket(6, B0, entries("k", 2, 0));
        assertThatThrownBy(() -> store.remapFileIds(6, B0, other, newIds))
                .hasMessageContaining("no new id for file id 2");
    }

    /**
     * A change the chain would refuse fails the check that runs before the lake commit makes the
     * change permanent.
     */
    @Test
    void testCheckRegistrableRefusesWhatRegistrationWould() throws Exception {
        store.commitChange(1, addedFiles(5, 6));
        store.register(-1, 100, 1);
        store.commitChange(2, addedFiles(6, 7));
        assertThatThrownBy(() -> store.checkRegistrable(100, 2))
                .hasMessageContaining("ids up to 6 are registered already");
        store.commitChange(3, addedFiles(7, 8));
        assertThatThrownBy(() -> store.checkRegistrable(99, 3))
                .hasMessageContaining("chain ends at snapshot 100");
        assertThatThrownBy(() -> store.checkRegistrable(100, 4))
                .hasMessageContaining("is not committed");

        store.checkRegistrable(100, 3);
        assertThat(chainSnapshots()).containsExactly(100L);
        store.register(100, 101, 3);
        assertThat(chainSnapshots()).containsExactly(100L, 101L);
    }

    /** A re-index split's lake splits are stored with the change until it commits. */
    @Test
    void testReindexPlanIsStoredWithTheChange() throws Exception {
        List<byte[]> lakeSplits =
                Arrays.asList(new byte[] {1, 2, 3}, new byte[0], new byte[70_000]);
        String reference = store.writeReindexPlan(7, 2, 5, lakeSplits);
        assertThat(reference).contains("rowPos/7/_plan/2");

        PositionIndexStore.ReindexPlan plan = PositionIndexStore.readReindexPlan(reference);
        assertThat(plan.splitSerializerVersion).isEqualTo(5);
        assertThat(plan.lakeSplits).hasSize(3);
        for (int i = 0; i < 3; i++) {
            assertThat(plan.lakeSplits.get(i)).isEqualTo(lakeSplits.get(i));
        }

        // an unregistered change's plan goes with it, a committed change drops its plans
        store.abortChange(7);
        assertThat(indexDir(7)).doesNotExist();
        reference = store.writeReindexPlan(8, 0, 1, lakeSplits);
        commitEmpty(8);
        assertThat(indexDir(8).resolve("_plan")).doesNotExist();
        String gone = reference;
        assertThatThrownBy(() -> PositionIndexStore.readReindexPlan(gone))
                .isInstanceOf(IOException.class);
    }

    /** The splits of a bucket's re-index write their own SSTs, all of which the change takes. */
    @Test
    void testSplitsOfABucketWriteTheirOwnSsts() throws Exception {
        PositionIndexStore.BucketPart first = store.writeBucket(9, B0, "s0_", entries("a", 1, 0));
        PositionIndexStore.BucketPart second = store.writeBucket(9, B0, "s1_", entries("b", 2, 5));
        List<RowPosSstIndex.SstFileEntry> files = new ArrayList<>(first.files);
        files.addAll(second.files);
        Map<Integer, String> added = new HashMap<>();
        added.put(1, "a.parquet");
        added.put(2, "b.parquet");
        store.commitChange(
                9,
                Collections.singletonMap(
                        B0,
                        new PositionIndexStore.BucketChange(
                                new PositionIndexStore.BucketPart(files),
                                added,
                                Collections.emptyList())));
        store.register(-1, 100, 9);

        PositionIndexStore.BucketData data = store.readChange(chainEntry(100), B0, dl());
        assertThat(data.localSsts).hasSize(2);
        List<String> indexed = new ArrayList<>();
        for (String sst : data.localSsts) {
            indexed.addAll(readSst(Paths.get(sst)));
        }
        assertThat(indexed)
                .containsExactlyInAnyOrder("a=" + new FilePos(1, 0), "b=" + new FilePos(2, 5));
    }

    /** A registration may record what the snapshot's id names, for readers to check. */
    @Test
    void testRegistrationRecordsTheSnapshotIdentity() throws Exception {
        commitEmpty(1);
        store.register(-1, 100, 1, "COMPACT commit 7 of 'user' at t (manifest-list-a)");
        commitEmpty(2);
        store.register(100, 101, 2);

        assertThat(store.snapshotIdentity(chainEntry(100)))
                .isEqualTo("COMPACT commit 7 of 'user' at t (manifest-list-a)");
        assertThat(store.snapshotIdentity(chainEntry(101))).isNull();
        // repeating a registration keeps the recorded identity
        store.register(-1, 100, 1, "something else");
        assertThat(store.snapshotIdentity(chainEntry(100)))
                .isEqualTo("COMPACT commit 7 of 'user' at t (manifest-list-a)");
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

        // nothing is old enough yet
        assertThat(store.deleteOrphanChanges(3_600_000L)).isZero();
        assertThat(store.deleteOrphanChanges(0)).isEqualTo(2);
        assertThat(indexDir(10)).doesNotExist();
        assertThat(indexDir(11)).doesNotExist();
        assertThat(indexDir(12)).exists();
        assertThat(store.readChange(chainEntry(100), B0, dl()))
                .isSameAs(PositionIndexStore.BucketData.EMPTY);
    }

    /**
     * On an object store, a writer whose create of a change's start marker races another writer's
     * waits for that marker instead of failing.
     */
    @Test
    void testWritersOfAChangeMarkItStartedConcurrently() throws Exception {
        FsPath lakeDir = new FsPath(TestFileSystem.SCHEME + "://" + root.toAbsolutePath());
        FlussPositionIndexStore objectStore = new FlussPositionIndexStore(lakeDir);
        ExecutorService pool = Executors.newFixedThreadPool(16);
        TestFileSystem.simulateObjectStoreWrites(true);
        try {
            // another writer of change 10 is uploading the marker
            FsPath started = startedMarker(lakeDir, 10);
            FSDataOutputStream other =
                    started.getFileSystem().create(started, FileSystem.WriteMode.NO_OVERWRITE);
            other.write(FormatVersions.withVersion(1, "1").getBytes(StandardCharsets.UTF_8));
            Future<PositionIndexStore.BucketPart> write =
                    pool.submit(() -> objectStore.writeBucket(10, B0, entries("a", 1, 0)));
            long deadline = System.currentTimeMillis() + 30_000;
            while (TestFileSystem.getRacingCreatesRefused() == 0 && !write.isDone()) {
                assertThat(System.currentTimeMillis()).isLessThan(deadline);
                Thread.sleep(1);
            }
            other.close();
            assertThat(write.get(30, TimeUnit.SECONDS).files).hasSize(1);
            assertThat(TestFileSystem.getRacingCreatesRefused()).isPositive();
            assertThat(readText(started)).isEqualTo("v1\n1");

            // many writers of one change at once
            int writers = 16;
            CyclicBarrier barrier = new CyclicBarrier(writers);
            List<Future<PositionIndexStore.BucketPart>> writes = new ArrayList<>();
            for (int b = 0; b < writers; b++) {
                TableBucket bucket = new TableBucket(TABLE_ID, b);
                writes.add(
                        pool.submit(
                                () -> {
                                    barrier.await();
                                    return objectStore.writeBucket(11, bucket, entries("k", 1, 0));
                                }));
            }
            for (Future<PositionIndexStore.BucketPart> w : writes) {
                assertThat(w.get(30, TimeUnit.SECONDS).files).hasSize(1);
            }
        } finally {
            TestFileSystem.simulateObjectStoreWrites(false);
            pool.shutdownNow();
        }
        assertThat(Long.parseLong(readText(startedMarker(lakeDir, 11)).substring(3)))
                .isLessThanOrEqualTo(System.currentTimeMillis());
        // both changes are failed attempts by now
        assertThat(objectStore.deleteOrphanChanges(0)).isEqualTo(2);
    }

    private static FsPath startedMarker(FsPath lakeDir, long changeId) {
        return new FsPath(
                new FsPath(new FsPath(lakeDir, "rowPos"), String.valueOf(changeId)),
                FlussPositionIndexStore.CHANGE_STARTED);
    }

    private static String readText(FsPath path) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (FSDataInputStream in = path.getFileSystem().open(path)) {
            byte[] buf = new byte[256];
            int n;
            while ((n = in.read(buf)) > 0) {
                bytes.write(buf, 0, n);
            }
        }
        return new String(bytes.toByteArray(), StandardCharsets.UTF_8);
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
        assertThat(read(root.resolve(FlussPositionIndexStore.CHAIN_TRIMMED))).isEqualTo("v1\n102");
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
    // trimming with idle buckets
    // ------------------------------------------------------------------------------------------

    /**
     * The buckets of an old partition that only the first rounds write do not hold the chain. The
     * trim builds their checkpoints, the chain and the stored changes shrink to what the active
     * bucket's checkpoints need, and every bucket still rebuilds exactly.
     */
    @Test
    void testIdleBucketsOfOldPartitionsDoNotHoldTheChain() throws Exception {
        TableBucket old0 = new TableBucket(TABLE_ID, 1L, 0);
        TableBucket old1 = new TableBucket(TABLE_ID, 1L, 1);
        TableBucket active = new TableBucket(TABLE_ID, 2L, 0);
        Model model = new Model();
        int rounds = 60;
        for (int r = 0; r < rounds; r++) {
            Map<TableBucket, List<String>> puts = new HashMap<>();
            Map<TableBucket, List<String>> deletes = new HashMap<>();
            puts.put(active, Arrays.asList("a" + r, "a0"));
            if (r < 3) {
                // the old partition: inserts, an update and a delete, then nothing
                puts.put(old0, Arrays.asList("o" + r, "o0"));
                puts.put(old1, Collections.singletonList("p" + r));
                if (r == 2) {
                    deletes.put(old1, Collections.singletonList("p0"));
                }
            }
            model.round(r, puts, deletes);
        }
        // old0 is checkpointed once, before its last change, and old1 never; the active bucket's
        // checkpoint lags the chain's end
        serverCheckpoint(old0, 1);
        serverCheckpoint(active, 50);
        Set<Long> changesBefore = changeDirs();

        int trimmed = store.trim(Arrays.asList(old0, old1, active), 1);

        // held only by the active bucket's checkpoint at chain index 50
        assertThat(trimmed).isEqualTo(49);
        assertThat(store.chain()).hasSize(rounds - 49);
        assertThat(changeDirs()).hasSize(changesBefore.size() - 49);
        for (TableBucket bucket : Arrays.asList(old0, old1, active)) {
            PositionIndexStore.Checkpoint cp =
                    store.latestCheckpoint(bucket, store.chain(), store.chain().size() - 1);
            assertThat(cp).as("checkpoint of %s", bucket).isNotNull();
            assertThat(rebuild(bucket)).as("index of %s", bucket).isEqualTo(model.of(bucket));
        }
        // the active bucket's server checkpoints again; the next trim follows it and the idle
        // buckets' checkpoints are carried over to the chain's new start
        serverCheckpoint(active, store.chain().size() - 1);
        model.round(rounds, singletonPuts(active, "late"), Collections.emptyMap());
        int more = store.trim(Arrays.asList(old0, old1, active), 1);
        assertThat(more).isGreaterThan(0);
        assertThat(store.chain().size()).isLessThanOrEqualTo(3);
        for (TableBucket bucket : Arrays.asList(old0, old1, active)) {
            assertThat(rebuild(bucket)).as("index of %s", bucket).isEqualTo(model.of(bucket));
            assertThat(checkpointDirs(bucket)).hasSizeLessThanOrEqualTo(2);
        }
        // the old partition is written again: it rebuilds from the carried-over checkpoint and
        // the new change
        model.round(rounds + 1, singletonPuts(old0, "o1"), Collections.emptyMap());
        assertThat(rebuild(old0)).isEqualTo(model.of(old0));
    }

    /**
     * A checkpoint the trim leaves behind while no later change touches its bucket is carried over:
     * rebuilds start from it, the bucket's older checkpoints are deleted, and the next server
     * checkpoint keeps it as the second newest.
     */
    @Test
    void testCheckpointLeftBehindIsCarriedOver() throws Exception {
        TableBucket idle = new TableBucket(TABLE_ID, 0);
        TableBucket active = new TableBucket(TABLE_ID, 1);
        Model model = new Model();
        for (int r = 0; r < 12; r++) {
            Map<TableBucket, List<String>> puts = new HashMap<>();
            puts.put(active, Collections.singletonList("a" + r));
            if (r < 4) {
                puts.put(idle, Collections.singletonList("i" + r));
            }
            model.round(r, puts, Collections.emptyMap());
        }
        serverCheckpoint(idle, 1);
        serverCheckpoint(idle, 3); // after its last change
        serverCheckpoint(active, 9);

        // no build needed: the idle bucket's checkpoint covers it up to the chain's end
        assertThat(store.trim(Arrays.asList(idle, active), 0)).isEqualTo(9);
        long carried = model.snapshots.get(3);
        assertThat(checkpointDirs(idle)).containsExactly(String.valueOf(carried));
        assertThat(checkpointDir(null, 0).resolve(String.valueOf(carried)).resolve("_CARRIED"))
                .exists();
        PositionIndexStore.Checkpoint cp = store.latestCheckpoint(idle, store.chain(), 0);
        assertThat(cp.snapshotId).isEqualTo(carried);
        assertThat(cp.chainIndex).isEqualTo(-1);
        assertThat(rebuild(idle)).isEqualTo(model.of(idle));

        // the idle bucket is written and checkpointed again: the carried-over checkpoint stays
        // as the second newest
        model.round(12, singletonPuts(idle, "i12"), Collections.emptyMap());
        serverCheckpoint(idle, store.chain().size() - 1);
        assertThat(checkpointDirs(idle)).hasSize(2).contains(String.valueOf(carried));
        assertThat(rebuild(idle)).isEqualTo(model.of(idle));
    }

    /** A change leaves out the buckets it does not touch, and partitions without any of them. */
    @Test
    void testChangeLeavesOutUntouchedBuckets() throws Exception {
        TableBucket touched = new TableBucket(TABLE_ID, 5L, 0);
        TableBucket untouchedOfPartition = new TableBucket(TABLE_ID, 5L, 1);
        TableBucket untouchedPartition = new TableBucket(TABLE_ID, 9L, 0);
        PositionIndexDelta empty =
                new PositionIndexDelta(
                        Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        PositionIndexChangeBuilder builder = new PositionIndexChangeBuilder(store);
        builder.add(
                touched,
                new PositionIndexDelta(
                        Collections.singletonList(bytes("k")),
                        Collections.singletonList(new KeyPosition(bytes("k"), "a.parquet", 0)),
                        Collections.emptyList()),
                Collections.singletonMap("a.parquet", 1));
        builder.add(untouchedOfPartition, empty, Collections.emptyMap());
        builder.add(untouchedPartition, empty, Collections.emptyMap());
        builder.commit(11);

        assertThat(read(indexDir(11).resolve(FlussPositionIndexStore.CHANGE_COMMITTED)).trim())
                .isEqualTo("v1\n5");
        assertThat(indexDir(11).resolve("9")).doesNotExist();
        RowPosSstIndex manifest =
                RowPosSstIndex.fromJsonBytes(
                        Files.readAllBytes(indexDir(11).resolve("5").resolve("rowpos.manifest")));
        assertThat(manifest.getBucketIds()).containsExactly(0);
    }

    /** The logical index of every bucket as rounds change it, and the rounds as changes. */
    private final class Model {
        final Map<TableBucket, TreeMap<String, FilePos>> index = new HashMap<>();
        final Map<Integer, Long> snapshots = new HashMap<>();

        /** Commits and registers round r: its puts in a new data file per bucket, its deletes. */
        void round(
                int r, Map<TableBucket, List<String>> puts, Map<TableBucket, List<String>> deletes)
                throws IOException {
            PositionIndexChangeBuilder builder = new PositionIndexChangeBuilder(store);
            Set<TableBucket> buckets = new HashSet<>(puts.keySet());
            buckets.addAll(deletes.keySet());
            int fileId = store.allocateFileIds(buckets.size());
            for (TableBucket bucket : buckets) {
                String file = "r" + r + "-" + bucket.getPartitionId() + "-" + bucket.getBucket();
                List<byte[]> changed = new ArrayList<>();
                List<KeyPosition> positions = new ArrayList<>();
                List<byte[]> deleted = new ArrayList<>();
                TreeMap<String, FilePos> idx = index.computeIfAbsent(bucket, k -> new TreeMap<>());
                List<String> keys = puts.getOrDefault(bucket, Collections.emptyList());
                for (int pos = 0; pos < keys.size(); pos++) {
                    changed.add(bytes(keys.get(pos)));
                    positions.add(new KeyPosition(bytes(keys.get(pos)), file, pos));
                    idx.put(keys.get(pos), new FilePos(fileId, pos));
                }
                for (String key : deletes.getOrDefault(bucket, Collections.emptyList())) {
                    changed.add(bytes(key));
                    deleted.add(bytes(key));
                    idx.remove(key);
                }
                builder.add(
                        bucket,
                        new PositionIndexDelta(changed, positions, deleted),
                        keys.isEmpty()
                                ? Collections.emptyMap()
                                : Collections.singletonMap(file, fileId));
                fileId++;
            }
            long changeId = 1000 + r;
            long snapshotId = 100 + r;
            builder.commit(changeId);
            store.register(snapshots.getOrDefault(r - 1, -1L), snapshotId, changeId);
            snapshots.put(r, snapshotId);
        }

        Map<String, FilePos> of(TableBucket bucket) {
            return index.getOrDefault(bucket, new TreeMap<>());
        }
    }

    private static Map<TableBucket, List<String>> singletonPuts(TableBucket bucket, String key) {
        return Collections.singletonMap(bucket, Collections.singletonList(key));
    }

    /** Checkpoints a bucket at chain index {@code upTo} as its tablet server would. */
    private void serverCheckpoint(TableBucket bucket, int upTo) throws Exception {
        List<PositionIndexStore.ChainEntry> chain = store.chain();
        File dir = dl();
        File sst = new File(dir, "export.sst");
        Map<Integer, String> dict = new HashMap<>();
        long n = merge(indexParts(bucket, chain, upTo), new File(dir, "db"), sst, dict);
        store.writeCheckpoint(chain.get(upTo).snapshotId, bucket, n > 0 ? sst : null, dict);
    }

    /**
     * Rebuilds a bucket's index at the chain's end as a tablet server does: from its latest
     * checkpoint and the changes after it, or from the whole chain if it has none. Fails for a
     * bucket without a checkpoint once the chain is trimmed.
     */
    private Map<String, FilePos> rebuild(TableBucket bucket) throws Exception {
        List<PositionIndexStore.ChainEntry> chain = store.chain();
        File dir = dl();
        File sst = new File(dir, "export.sst");
        Map<Integer, String> dict = new HashMap<>();
        long n = merge(indexParts(bucket, chain, chain.size() - 1), new File(dir, "db"), sst, dict);
        Map<String, FilePos> out = new TreeMap<>();
        if (n > 0) {
            try (Options options = new Options();
                    SstFileReader reader = new SstFileReader(options);
                    ReadOptions readOptions = new ReadOptions()) {
                reader.open(sst.getPath());
                try (SstFileReaderIterator it = reader.newIterator(readOptions)) {
                    for (it.seekToFirst(); it.isValid(); it.next()) {
                        out.put(
                                new String(it.key(), StandardCharsets.UTF_8),
                                FilePos.decode(it.value()));
                    }
                }
            }
        }
        return out;
    }

    private List<PositionIndexStore.BucketData> indexParts(
            TableBucket bucket, List<PositionIndexStore.ChainEntry> chain, int upTo)
            throws IOException {
        List<PositionIndexStore.BucketData> parts = new ArrayList<>();
        PositionIndexStore.Checkpoint cp = store.latestCheckpoint(bucket, chain, upTo);
        if (cp == null && store.chainTrimmed()) {
            throw new AssertionError("no checkpoint of " + bucket + " covers the trimmed chain");
        }
        int from = 0;
        if (cp != null) {
            parts.add(store.readCheckpoint(cp, bucket, dl()));
            from = cp.chainIndex + 1;
        }
        for (int i = from; i <= upTo; i++) {
            parts.add(store.readChange(chain.get(i), bucket, dl()));
        }
        return parts;
    }

    /**
     * Applies checkpoints and changes in order to an empty index as a tablet server does (file ids,
     * SSTs, removed files) and exports it without the entries of removed files.
     */
    private static long merge(
            List<PositionIndexStore.BucketData> parts,
            File dbDir,
            File sst,
            Map<Integer, String> dict)
            throws Exception {
        RocksDB.loadLibrary();
        Map<String, Integer> idOfPath = new HashMap<>();
        try (Options options = new Options().setCreateIfMissing(true);
                RocksDB db = RocksDB.open(options, dbDir.getPath());
                IngestExternalFileOptions ingest = new IngestExternalFileOptions()) {
            for (PositionIndexStore.BucketData part : parts) {
                part.fileId2Name.forEach(
                        (id, path) -> {
                            dict.put(id, path);
                            idOfPath.put(path, id);
                        });
                if (!part.localSsts.isEmpty()) {
                    db.ingestExternalFile(part.localSsts, ingest);
                }
                for (String removed : part.removedFiles) {
                    Integer id = idOfPath.remove(removed);
                    if (id != null) {
                        dict.remove(id);
                    }
                }
            }
            long n = 0;
            try (EnvOptions envOptions = new EnvOptions();
                    SstFileWriter writer = new SstFileWriter(envOptions, options);
                    ReadOptions readOptions = new ReadOptions();
                    RocksIterator it = db.newIterator(readOptions)) {
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    if (dict.containsKey(FilePos.decode(it.value()).fileId())) {
                        if (n++ == 0) {
                            writer.open(sst.getPath());
                        }
                        writer.put(it.key(), it.value());
                    }
                }
                if (n > 0) {
                    writer.finish();
                }
            }
            return n;
        }
    }

    private Set<Long> changeDirs() throws IOException {
        Set<Long> out = new HashSet<>();
        try (Stream<Path> files = Files.list(root.resolve("rowPos"))) {
            files.map(p -> p.getFileName().toString())
                    .filter(name -> name.matches("\\d+"))
                    .forEach(name -> out.add(Long.parseLong(name)));
        }
        return out;
    }

    private List<String> checkpointDirs(TableBucket bucket) throws IOException {
        Path dir = checkpointDir(bucket.getPartitionId(), bucket.getBucket());
        try (Stream<Path> files = Files.list(dir)) {
            return files.map(p -> p.getFileName().toString()).sorted().collect(Collectors.toList());
        }
    }

    // ------------------------------------------------------------------------------------------
    // staged parts of tiering writers
    // ------------------------------------------------------------------------------------------

    @Test
    void testStagedPartIsPublishedWithItsDeletesAndKeys() throws Exception {
        TableBucket bucket = new TableBucket(TABLE_ID, 3L, 2);
        long stagingId = StagedIndexPart.stagingIdOf(TABLE_ID, 1234L);
        byte[] high = new byte[] {(byte) 0x80, 1};
        PositionIndexStore.BucketPart staged;
        try (PositionIndexStore.StagedPartWriter writer =
                store.stageBucket(stagingId, bucket, "w1_", tempDir.toFile())) {
            writer.put(bytes("k1"), new FilePos(0, 0));
            writer.delete(bytes("k2"));
            writer.put(bytes("k3"), new FilePos(1, 4));
            writer.put(high, new FilePos(0, 1));
            // keys must ascend
            assertThatThrownBy(() -> writer.put(bytes("k0"), new FilePos(0, 2)))
                    .isInstanceOf(IOException.class);
            staged = writer.finish();
        }
        assertThat(staged.files).hasSize(1);
        assertThat(staged.files.get(0).getFileName()).isEqualTo("w1_sst_0.sst");
        assertThat(root.resolve("rowPos/" + stagingId + "/3/2/w1_sst_0.sst")).exists();
        // nothing local is left behind
        try (Stream<Path> local = Files.list(tempDir)) {
            assertThat(local.map(p -> p.getFileName().toString()))
                    .noneMatch(n -> n.startsWith("fluss-pkidx-stage-"));
        }

        Map<Integer, Integer> newIds = new HashMap<>();
        newIds.put(0, 7);
        newIds.put(1, 8);
        List<String> keys = new ArrayList<>();
        PositionIndexStore.BucketPart part =
                store.publishStaged(
                        stagingId, 42, bucket, staged, newIds, k -> keys.add(keyString(k)));
        // every key, deletes included, in key order
        assertThat(keys).containsExactly("k1", "k2", "k3", "80 01");
        Map<Integer, String> added = new HashMap<>();
        added.put(7, "a.parquet");
        added.put(8, "b.parquet");
        store.commitChange(
                42,
                Collections.singletonMap(
                        bucket,
                        new PositionIndexStore.BucketChange(part, added, Collections.emptyList())));
        store.register(-1, 100, 42);
        PositionIndexStore.BucketData data = store.readChange(chainEntry(100), bucket, dl());
        assertThat(data.fileId2Name).containsOnly(entry(7, "a.parquet"), entry(8, "b.parquet"));
        assertThat(data.localSsts).hasSize(1);
        Path sst = Paths.get(data.localSsts.get(0));
        assertThat(readSst(sst))
                .containsExactly(
                        "k1=" + new FilePos(7, 0),
                        "k3=" + new FilePos(8, 4),
                        "80 01=" + new FilePos(7, 1));
        // the delete reaches the tablet servers as a tombstone
        assertThat(deletions(sst)).isEqualTo(1);

        // the staged part stays for another attempt of the round, until its staging change goes
        List<String> again = new ArrayList<>();
        store.publishStaged(stagingId, 43, bucket, staged, newIds, k -> again.add(keyString(k)));
        assertThat(again).isEqualTo(keys);
        store.abortChange(stagingId);
        assertThat(root.resolve("rowPos/" + stagingId)).doesNotExist();
        assertThatThrownBy(
                        () -> store.publishStaged(stagingId, 44, bucket, staged, newIds, k -> {}))
                .isInstanceOf(IOException.class);
    }

    @Test
    void testPublishingAStagedPartNeedsAnIdForEveryFile() throws Exception {
        long stagingId = StagedIndexPart.stagingIdOf(TABLE_ID, 1L);
        PositionIndexStore.BucketPart staged;
        try (PositionIndexStore.StagedPartWriter writer =
                store.stageBucket(stagingId, B0, "", null)) {
            writer.put(bytes("k1"), new FilePos(3, 0));
            staged = writer.finish();
        }
        assertThatThrownBy(
                        () ->
                                store.publishStaged(
                                        stagingId,
                                        9,
                                        B0,
                                        staged,
                                        Collections.singletonMap(0, 5),
                                        k -> {}))
                .hasMessageContaining("no new id for file id 3");
    }

    @Test
    void testStagedPartsOfAnAbandonedRoundAreOrphans() throws Exception {
        long stagingId = StagedIndexPart.stagingIdOf(TABLE_ID, 5L);
        try (PositionIndexStore.StagedPartWriter writer =
                store.stageBucket(stagingId, B0, "", null)) {
            writer.delete(bytes("k1"));
            assertThat(writer.finish().files).hasSize(1);
        }
        // an unfinished writer stores nothing more
        PositionIndexStore.StagedPartWriter unfinished =
                store.stageBucket(stagingId, B0, "x_", null);
        unfinished.put(bytes("k1"), new FilePos(0, 0));
        unfinished.close();
        assertThat(root.resolve("rowPos/" + stagingId + "/0/x_sst_0.sst")).doesNotExist();
        assertThat(store.deleteOrphanChanges(0)).isEqualTo(1);
        assertThat(root.resolve("rowPos/" + stagingId)).doesNotExist();
    }

    // ------------------------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------------------------

    private static long deletions(Path sst) throws Exception {
        try (Options options = new Options();
                SstFileReader reader = new SstFileReader(options)) {
            reader.open(sst.toString());
            return reader.getTableProperties().getNumDeletions();
        }
    }

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

    /** Writes a committed change and its chain marker, linked to the previous marker. */
    private void marker(long sequence, long snapshotId, long changeId) throws IOException {
        Path dir = Files.createDirectories(root.resolve(FlussPositionIndexStore.CHAIN_DIR));
        Files.write(
                dir.resolve(
                        FlussPositionIndexStore.markerName(
                                sequence, lastMarkedSnapshot, snapshotId, changeId, -1)),
                new byte[0]);
        lastMarkedSnapshot = snapshotId;
        Path index = Files.createDirectories(indexDir(changeId).resolve("0"));
        Files.write(index.resolve("sst_0.sst"), new byte[] {1});
        Files.write(
                indexDir(changeId).resolve(FlussPositionIndexStore.CHANGE_COMMITTED),
                bytes(FormatVersions.withVersion(1, "-1\n")));
    }

    private void checkpoint(@Nullable Long partitionId, int bucket, long snapshotId, boolean done)
            throws IOException {
        Path dir =
                Files.createDirectories(
                        checkpointDir(partitionId, bucket).resolve(String.valueOf(snapshotId)));
        Files.write(dir.resolve(FlussPositionIndexStore.CKPT_SST), new byte[] {1});
        if (done) {
            Files.write(
                    dir.resolve(FlussPositionIndexStore.CKPT_DONE),
                    bytes(FormatVersions.withVersion(1, "sst")));
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
