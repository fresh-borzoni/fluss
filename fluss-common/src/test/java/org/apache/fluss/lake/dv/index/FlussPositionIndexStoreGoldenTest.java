/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.dv.index;

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.TestFileSystem;
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.metadata.TableBucket;

import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.ReadOptions;
import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.SstFileReader;
import io.github.fluss_contrib.rocksdb.SstFileReaderIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the files of {@link FlussPositionIndexStore}: their paths, their bytes and the order in
 * which each operation creates and deletes them. The files outlive software versions, and the order
 * of writes and deletes is what makes a crash at any point safe, so a change of anything here is a
 * change of the on-storage protocol.
 */
class FlussPositionIndexStoreGoldenTest {

    private static final long TABLE_ID = 1L;
    private static final TableBucket P7B1 = new TableBucket(TABLE_ID, 7L, 1);
    private static final TableBucket P8B0 = new TableBucket(TABLE_ID, 8L, 0);
    private static final Comparator<TableBucket> BUCKETS =
            Comparator.comparing(
                            (TableBucket b) -> b.getPartitionId() == null ? -1 : b.getPartitionId())
                    .thenComparingInt(TableBucket::getBucket);

    @TempDir Path tempDir;

    private Path root;
    private String prefix;
    private FlussPositionIndexStore store;
    private final List<String> trace = new ArrayList<>();

    @BeforeEach
    void setUp() throws IOException {
        root = Files.createDirectories(tempDir.resolve("lake"));
        prefix = root.toAbsolutePath() + "/";
        store =
                new FlussPositionIndexStore(
                        new FsPath(TestFileSystem.SCHEME + "://" + root.toAbsolutePath()),
                        tempDir.resolve("tmp").toFile());
    }

    @AfterEach
    void tearDown() {
        TestFileSystem.recordWrites(false);
    }

    @Test
    void testWritesOfAChange() throws Exception {
        long stagingId = 500;
        int[] firstId = new int[1];
        step("allocateFileIds", () -> firstId[0] = store.allocateFileIds(2));
        assertThat(firstId[0]).isZero();

        PositionIndexStore.BucketPart[] staged = new PositionIndexStore.BucketPart[1];
        step(
                "stageBucket",
                () -> {
                    try (PositionIndexStore.StagedPartWriter w =
                            store.stageBucket(stagingId, P7B1, "w0-", null)) {
                        w.put(bytes("k1"), new FilePos(0, 5));
                        w.delete(bytes("k2"));
                        w.put(bytes("k3"), new FilePos(1, 300));
                        staged[0] = w.finish();
                    }
                });
        String stagedTree = tree();

        List<String> keys = new ArrayList<>();
        PositionIndexStore.BucketPart[] published = new PositionIndexStore.BucketPart[1];
        Map<Integer, Integer> newIds = new HashMap<>();
        newIds.put(0, 0);
        newIds.put(1, 1);
        step(
                "publishStaged",
                () ->
                        published[0] =
                                store.publishStaged(
                                        stagingId,
                                        10,
                                        P7B1,
                                        staged[0],
                                        newIds,
                                        k -> keys.add(new String(k, StandardCharsets.UTF_8))));
        assertThat(keys).containsExactly("k1", "k2", "k3");

        PositionIndexStore.BucketPart[] handedOff = new PositionIndexStore.BucketPart[1];
        step(
                "writeBucket",
                () -> handedOff[0] = store.writeBucket(10, P8B0, "c-", entries("k4", 7, 2)));
        step(
                "remapFileIds",
                () ->
                        handedOff[0] =
                                store.remapFileIds(
                                        10, P8B0, handedOff[0], Collections.singletonMap(7, 2)));
        step(
                "writeReindexPlan",
                () ->
                        store.writeReindexPlan(
                                10, 0, 3, Arrays.asList(new byte[] {1, 2}, new byte[0])));
        String planHex = hex(Files.readAllBytes(changeDir(10).resolve("_plan").resolve("0")));

        Map<TableBucket, PositionIndexStore.BucketChange> change = new TreeMap<>(BUCKETS);
        Map<Integer, String> added = new TreeMap<>();
        added.put(0, "a.parquet");
        added.put(1, "b.parquet");
        change.put(
                P7B1,
                new PositionIndexStore.BucketChange(
                        published[0], added, Collections.singletonList("old.parquet")));
        change.put(
                P8B0,
                new PositionIndexStore.BucketChange(
                        handedOff[0],
                        Collections.singletonMap(2, "c.parquet"),
                        Collections.emptyList()));
        step("commitChange", () -> store.commitChange(10, change));
        step("register", () -> store.register(-1, 100, 10, "identity of 100"));
        // a manifest records each SST's length, which depends on the host that wrote it
        assertThat(published[0].files.get(0).getFileSize())
                .isEqualTo(Files.size(changeDir(10).resolve("7/1/w0-sst_0.sst")));
        assertThat(handedOff[0].files.get(0).getFileSize())
                .isEqualTo(Files.size(changeDir(10).resolve("8/0/remapped_c-sst_0.sst")));

        assertThat(trace)
                .containsExactly(
                        "# allocateFileIds",
                        "create OVERWRITE rowPos/_file_ids/v1-2",
                        "# stageBucket",
                        "create NO_OVERWRITE rowPos/500/_STARTED",
                        "create OVERWRITE rowPos/500/7/1/w0-sst_0.sst",
                        "# publishStaged",
                        "create NO_OVERWRITE rowPos/10/_STARTED",
                        "create OVERWRITE rowPos/10/7/1/w0-sst_0.sst",
                        "# writeBucket",
                        "create OVERWRITE rowPos/10/8/0/c-sst_0.sst",
                        "# remapFileIds",
                        "create OVERWRITE rowPos/10/8/0/remapped_c-sst_0.sst",
                        "delete rowPos/10/8/0/c-sst_0.sst true",
                        "# writeReindexPlan",
                        "create OVERWRITE rowPos/10/_plan/0",
                        "# commitChange",
                        "create OVERWRITE rowPos/10/7/rowpos.manifest",
                        "create OVERWRITE rowPos/10/8/rowpos.manifest",
                        "create OVERWRITE rowPos/10/_COMMITTED",
                        "delete -r rowPos/10/_plan true",
                        "# register",
                        "create OVERWRITE rowPos/_chain/v1-00000000000000000001_-1_100_10_2");
        assertThat(stagedTree)
                .isEqualTo(
                        lines(
                                "rowPos/500/7/1/w0-sst_0.sst = sst 6b31=0005 6b32= 6b33=01ac02"
                                        + " deletions=0",
                                "rowPos/500/_STARTED = v1\\n<millis>",
                                "rowPos/_file_ids/v1-2 = "));
        assertThat(planHex).isEqualTo("00000001000000030000000200000002010200000000");
        assertThat(tree())
                .isEqualTo(
                        lines(
                                "rowPos/10/7/1/w0-sst_0.sst = sst 6b31=0005 6b33=01ac02"
                                        + " deletions=1",
                                "rowPos/10/7/rowpos.manifest = {\"version\":1,\"sstValueVersion\":1,"
                                        + "\"buckets\":{\"1\":{\"rowPosSstFiles\":[{\"name\":"
                                        + "\"w0-sst_0.sst\",\"size\":<bytes>}],\"fileId2Name\":"
                                        + "[{\"fileId\":0,\"name\":\"a.parquet\"},{\"fileId\":1,"
                                        + "\"name\":\"b.parquet\"}],\"replacedFiles\":"
                                        + "[\"old.parquet\"]}}}",
                                "rowPos/10/8/0/remapped_c-sst_0.sst = sst 6b34=0202 deletions=0",
                                "rowPos/10/8/rowpos.manifest = {\"version\":1,\"sstValueVersion\":1,"
                                        + "\"buckets\":{\"0\":{\"rowPosSstFiles\":[{\"name\":"
                                        + "\"remapped_c-sst_0.sst\",\"size\":<bytes>}],"
                                        + "\"fileId2Name\":[{\"fileId\":2,\"name\":"
                                        + "\"c.parquet\"}],\"replacedFiles\":[]}}}",
                                "rowPos/10/_COMMITTED = v1\\n7\\n8\\n",
                                "rowPos/10/_STARTED = v1\\n<millis>",
                                "rowPos/500/7/1/w0-sst_0.sst = sst 6b31=0005 6b32= 6b33=01ac02"
                                        + " deletions=0",
                                "rowPos/500/_STARTED = v1\\n<millis>",
                                "rowPos/_chain/v1-00000000000000000001_-1_100_10_2 = identity"
                                        + " of 100",
                                "rowPos/_file_ids/v1-2 = "));
    }

    @Test
    void testCheckpointsTrimAndOrphans() throws Exception {
        commit(10, P7B1, "k1", 0, 5, "a.parquet");
        store.register(-1, 100, 10);
        commit(11, P8B0, "k2", 1, 6, "b.parquet");
        store.register(100, 101, 11);
        store.commitChange(12, Collections.emptyMap());
        store.register(101, 102, 12);
        store.commitChange(13, Collections.emptyMap());
        store.register(102, 103, 13);
        PositionIndexStore.BucketData change10 =
                store.readChange(store.chain().get(0), P7B1, tempDir.resolve("dl").toFile());

        step(
                "writeCheckpoint",
                () -> {
                    store.writeCheckpoint(
                            101,
                            P7B1,
                            new File(change10.localSsts.get(0)),
                            Collections.singletonMap(0, "a.parquet"));
                    store.writeCheckpoint(
                            103,
                            P7B1,
                            new File(change10.localSsts.get(0)),
                            Collections.singletonMap(0, "a.parquet"));
                });
        step("startTrim", store::startTrim);
        String[] intent = new String[1];
        FlussPositionIndexStore trimming =
                new FlussPositionIndexStore(
                        new FsPath(TestFileSystem.SCHEME + "://" + root.toAbsolutePath()),
                        tempDir.resolve("tmp").toFile()) {
                    @Override
                    boolean deletePath(FsPath path, boolean recursive) throws IOException {
                        if (intent[0] == null && path.getPath().contains("/_chain/")) {
                            try (Stream<Path> records =
                                    Files.list(root.resolve("rowPos/_trimmed"))) {
                                intent[0] = read(records.findFirst().get());
                            }
                        }
                        return super.deletePath(path, recursive);
                    }
                };
        int[] trimmed = new int[1];
        step("trim", () -> trimmed[0] = trimming.trim(Arrays.asList(P7B1, P8B0), 0));
        assertThat(trimmed[0]).isEqualTo(3);

        // a failed attempt's change and an abandoned staging part
        store.writeBucket(20, P7B1, "", entries("k9", 9, 9));
        store.stageBucket(501, P8B0, "", null).close();
        store.writeBucket(21, P7B1, "", entries("k8", 8, 8));
        Files.delete(changeDir(21).resolve("_STARTED"));
        step("abortChange", () -> store.abortChange(20));
        step("deleteOrphanChanges", () -> store.deleteOrphanChanges(0));

        assertThat(trace)
                .containsExactly(
                        "# writeCheckpoint",
                        "create OVERWRITE rowPos/_ckpt/7/1/101_<attempt>/index.sst",
                        "create OVERWRITE rowPos/_ckpt/7/1/101_<attempt>/filedict.txt",
                        "create OVERWRITE rowPos/_ckpt/7/1/101_<attempt>/_DONE",
                        "create OVERWRITE rowPos/_ckpt/7/1/103_<attempt>/index.sst",
                        "create OVERWRITE rowPos/_ckpt/7/1/103_<attempt>/filedict.txt",
                        "create OVERWRITE rowPos/_ckpt/7/1/103_<attempt>/_DONE",
                        "# startTrim",
                        "create OVERWRITE rowPos/_trims/v1-4",
                        "# trim",
                        "create OVERWRITE rowPos/_ckpt/8/0/103_<attempt>/index.sst",
                        "create OVERWRITE rowPos/_ckpt/8/0/103_<attempt>/filedict.txt",
                        "create OVERWRITE rowPos/_ckpt/8/0/103_<attempt>/_DONE",
                        "delete rowPos/_ckpt/7/1/101_<attempt>/_DONE true",
                        "delete -r rowPos/_ckpt/7/1/101_<attempt> true",
                        "create OVERWRITE rowPos/_trimmed/00000000000000000004-<attempt>",
                        "delete rowPos/_chain/v1-00000000000000000001_-1_100_10_0 true",
                        "delete rowPos/_chain/v1-00000000000000000002_100_101_11_1 true",
                        "delete rowPos/_chain/v1-00000000000000000003_101_102_12_-1 true",
                        "delete rowPos/10/_COMMITTED true",
                        "delete -r rowPos/10 true",
                        "delete rowPos/11/_COMMITTED true",
                        "delete -r rowPos/11 true",
                        "delete rowPos/12/_COMMITTED true",
                        "delete -r rowPos/12 true",
                        "# abortChange",
                        "delete rowPos/20/_COMMITTED false",
                        "delete -r rowPos/20 true",
                        "# deleteOrphanChanges",
                        "create NO_OVERWRITE rowPos/21/_STARTED",
                        "delete rowPos/21/_COMMITTED false",
                        "delete -r rowPos/21 true",
                        "delete rowPos/501/_COMMITTED false",
                        "delete -r rowPos/501 true");
        assertThat(intent[0])
                .isEqualTo(
                        "v2\nfirst 103"
                                + "\nmarker v1-00000000000000000001_-1_100_10_0"
                                + "\nmarker v1-00000000000000000002_100_101_11_1"
                                + "\nmarker v1-00000000000000000003_101_102_12_-1"
                                + "\nchange 10\nchange 11\nchange 12"
                                + "\nbucket 7 1\nbucket 8 0\nend");
        assertThat(tree())
                .isEqualTo(
                        lines(
                                "rowPos/13/_COMMITTED = v1\\n",
                                "rowPos/13/_STARTED = v1\\n<millis>",
                                "rowPos/_chain/v1-00000000000000000004_102_103_13_-1 = ",
                                "rowPos/_ckpt/7/1/103_<attempt>/_DONE = v2\\nfile index.sst <bytes>\\nfile"
                                        + " filedict.txt 12\\n",
                                "rowPos/_ckpt/7/1/103_<attempt>/filedict.txt = 0\\ta.parquet\\n",
                                "rowPos/_ckpt/7/1/103_<attempt>/index.sst = sst 6b31=0005 deletions=0",
                                "rowPos/_ckpt/8/0/103_<attempt>/_DONE = v2\\nfile index.sst <bytes>\\nfile"
                                        + " filedict.txt 12\\n",
                                "rowPos/_ckpt/8/0/103_<attempt>/filedict.txt = 1\\tb.parquet\\n",
                                "rowPos/_ckpt/8/0/103_<attempt>/index.sst = sst 6b32=0106 deletions=0",
                                "rowPos/_trimmed/00000000000000000004-<attempt> = v2\\nfirst 103\\n"
                                        + "marker v1-00000000000000000001_-1_100_10_0\\n"
                                        + "marker v1-00000000000000000002_100_101_11_1\\n"
                                        + "marker v1-00000000000000000003_101_102_12_-1\\n"
                                        + "change 10\\nchange 11\\nchange 12\\n"
                                        + "bucket 7 1\\nbucket 8 0\\nend",
                                "rowPos/_trims/v1-4 = "));
    }

    @Test
    void testEmptyCheckpointsAreWrittenAndNeverDeletedByTheirWriters() throws Exception {
        for (int i = 0; i < 3; i++) {
            store.commitChange(10 + i, Collections.emptyMap());
            store.register(i == 0 ? -1 : 99 + i, 100 + i, 10 + i);
        }
        for (int i = 0; i < 3; i++) {
            long snapshot = 100 + i;
            step(
                    "writeCheckpoint " + snapshot,
                    () -> store.writeCheckpoint(snapshot, P7B1, null, Collections.emptyMap()));
        }

        assertThat(trace)
                .containsExactly(
                        "# writeCheckpoint 100",
                        "create OVERWRITE rowPos/_ckpt/7/1/100_<attempt>/filedict.txt",
                        "create OVERWRITE rowPos/_ckpt/7/1/100_<attempt>/_DONE",
                        "# writeCheckpoint 101",
                        "create OVERWRITE rowPos/_ckpt/7/1/101_<attempt>/filedict.txt",
                        "create OVERWRITE rowPos/_ckpt/7/1/101_<attempt>/_DONE",
                        "# writeCheckpoint 102",
                        "create OVERWRITE rowPos/_ckpt/7/1/102_<attempt>/filedict.txt",
                        "create OVERWRITE rowPos/_ckpt/7/1/102_<attempt>/_DONE");
        assertThat(tree())
                .contains(
                        lines(
                                "rowPos/_ckpt/7/1/100_<attempt>/_DONE = v2\\nfile filedict.txt 0\\n",
                                "rowPos/_ckpt/7/1/100_<attempt>/filedict.txt = ",
                                "rowPos/_ckpt/7/1/101_<attempt>/_DONE = v2\\nfile filedict.txt 0\\n",
                                "rowPos/_ckpt/7/1/101_<attempt>/filedict.txt = ",
                                "rowPos/_ckpt/7/1/102_<attempt>/_DONE = v2\\nfile filedict.txt 0\\n",
                                "rowPos/_ckpt/7/1/102_<attempt>/filedict.txt = "));
    }

    /**
     * The unpartitioned layout, and the deletes the other tests do not reach: the withdraw of a
     * registration that lost a race, older trim markers, a checkpoint below the cut, and the
     * deletes that finish an interrupted trim.
     */
    @Test
    void testUnpartitionedLayoutAndTheRemainingDeletes() throws Exception {
        TableBucket b0 = new TableBucket(TABLE_ID, 0);
        step(
                "writeBucket and commitChange",
                () -> {
                    PositionIndexStore.BucketPart part =
                            store.writeBucket(10, b0, "", entries("k1", 0, 1));
                    store.commitChange(
                            10,
                            Collections.singletonMap(
                                    b0,
                                    new PositionIndexStore.BucketChange(
                                            part,
                                            Collections.singletonMap(0, "a.parquet"),
                                            Collections.emptyList())));
                });
        step("register", () -> store.register(-1, 100, 10));
        step("startTrim", store::startTrim);
        for (int i = 11; i <= 13; i++) {
            store.commitChange(i, Collections.emptyMap());
            store.register(89 + i, 90 + i, i);
        }
        step("startTrim again", store::startTrim);

        // a racer registers 103 -> 104 between this writer's check and its marker
        store.commitChange(14, Collections.emptyMap());
        store.commitChange(15, Collections.emptyMap());
        FsPath lake = new FsPath(TestFileSystem.SCHEME + "://" + root.toAbsolutePath());
        FlussPositionIndexStore loser =
                new FlussPositionIndexStore(lake) {
                    @Override
                    void beforeMarkerWrite() throws IOException {
                        store.register(103, 104, 14);
                    }
                };
        step(
                "register, withdrawn",
                () -> {
                    try {
                        loser.register(103, 105, 15);
                    } catch (IllegalStateException e) {
                        // withdrawn
                    }
                });

        store.writeCheckpoint(100, b0, null, Collections.singletonMap(0, "a.parquet"));
        store.writeCheckpoint(101, b0, null, Collections.singletonMap(0, "a.parquet"));
        // a trim that stops at its first chain marker delete
        FlussPositionIndexStore stopping =
                new FlussPositionIndexStore(lake) {
                    @Override
                    boolean deletePath(FsPath path, boolean recursive) throws IOException {
                        if (path.getPath().contains("/_chain/")) {
                            throw new IOException("stopped");
                        }
                        return super.deletePath(path, recursive);
                    }
                };
        step(
                "trim, stopped",
                () -> {
                    try {
                        stopping.trim(Collections.singletonList(b0), 0);
                    } catch (IOException e) {
                        // stopped
                    }
                });
        step("trim, finishing", () -> store.trim(Collections.singletonList(b0), 0));

        assertThat(trace)
                .containsExactly(
                        "# writeBucket and commitChange",
                        "create NO_OVERWRITE rowPos/10/_STARTED",
                        "create OVERWRITE rowPos/10/0/sst_0.sst",
                        "create OVERWRITE rowPos/10/rowpos.manifest",
                        "create OVERWRITE rowPos/10/_COMMITTED",
                        "delete -r rowPos/10/_plan false",
                        "# register",
                        "create OVERWRITE rowPos/_chain/v1-00000000000000000001_-1_100_10_0",
                        "# startTrim",
                        "create OVERWRITE rowPos/_trims/v1-1",
                        "# startTrim again",
                        "create OVERWRITE rowPos/_trims/v1-4",
                        "delete rowPos/_trims/v1-1 true",
                        "# register, withdrawn",
                        "create OVERWRITE rowPos/_chain/v1-00000000000000000005_103_104_14_-1",
                        "create OVERWRITE rowPos/_chain/v1-00000000000000000005_103_105_15_-1",
                        "delete rowPos/_chain/v1-00000000000000000005_103_105_15_-1 true",
                        "# trim, stopped",
                        "delete rowPos/_ckpt/0/100_<attempt>/_DONE true",
                        "delete -r rowPos/_ckpt/0/100_<attempt> true",
                        "create OVERWRITE rowPos/_trimmed/00000000000000000002-<attempt>",
                        "# trim, finishing",
                        "delete rowPos/_chain/v1-00000000000000000001_-1_100_10_0 true",
                        "delete rowPos/10/_COMMITTED true",
                        "delete -r rowPos/10 true");
        assertThat(tree())
                .contains(
                        lines(
                                "rowPos/_ckpt/0/101_<attempt>/_DONE = v2\\nfile filedict.txt 12\\n",
                                "rowPos/_ckpt/0/101_<attempt>/filedict.txt = 0\\ta.parquet\\n"));
    }

    /**
     * The trim lists the buckets' checkpoints before the chain it cuts by: a checkpoint written for
     * a snapshot registered after the chain's listing is never taken for one of a lineage left
     * behind. So are the partitions with checkpoints: a partition created after that listing is
     * never taken for a dropped one. A first listing of the chain comes before it asks for the
     * buckets with data.
     */
    @Test
    void testTrimListsCheckpointsBeforeTheChain() throws Exception {
        store.commitChange(10, Collections.emptyMap());
        store.register(-1, 100, 10);
        store.commitChange(11, Collections.emptyMap());
        store.register(100, 101, 11);
        store.writeCheckpoint(101, P7B1, null, Collections.emptyMap());
        store.writeCheckpoint(101, P8B0, null, Collections.emptyMap());
        TestFileSystem.recordLists(true);
        try {
            step("trim", () -> store.trim(Arrays.asList(P7B1, P8B0), 0));
        } finally {
            TestFileSystem.recordLists(false);
        }

        List<String> lists = new ArrayList<>();
        for (String op : trace) {
            if (op.startsWith("list ")) {
                lists.add(op);
            }
        }
        assertThat(lists)
                .containsExactly(
                        // before the trim asks for the buckets with data
                        "list rowPos/_chain",
                        "list rowPos/_ckpt/7/1",
                        "list rowPos/_ckpt/7/1/101_<attempt>",
                        "list rowPos/_ckpt/8/0",
                        "list rowPos/_ckpt/8/0/101_<attempt>",
                        // the partitions with checkpoints, before the dropped ones are told
                        "list rowPos/_ckpt",
                        "list rowPos/_chain",
                        "list rowPos/_chain",
                        "list rowPos/_chain",
                        "list rowPos/_trimmed");
    }

    /**
     * The trim asks for the buckets with data only after a barrier listing of the chain, and before
     * it lists the checkpoints: a caller that reads them from Fluss's registration reads one no
     * older than that listing.
     */
    @Test
    void testTrimReadsItsBucketsAfterListingTheChain() throws Exception {
        store.commitChange(10, Collections.emptyMap());
        store.register(-1, 100, 10);
        store.commitChange(11, Collections.emptyMap());
        store.register(100, 101, 11);
        store.writeCheckpoint(101, P7B1, null, Collections.emptyMap());
        List<String> before = new ArrayList<>();
        TestFileSystem.recordLists(true);
        TestFileSystem.recordWrites(true);
        try {
            store.trim(
                    () -> {
                        // what the trim did on the file system before it asked
                        for (String op : TestFileSystem.getRecordedWrites()) {
                            if (op.contains(prefix)) {
                                before.add(op.replace(prefix, ""));
                            }
                        }
                        return Collections.singletonList(P7B1);
                    },
                    0,
                    PositionIndexStore.DroppedPartitions.NONE);
        } finally {
            TestFileSystem.recordLists(false);
            TestFileSystem.recordWrites(false);
        }

        assertThat(before).contains("list rowPos/_chain");
        assertThat(before).noneMatch(op -> op.startsWith("list rowPos/_ckpt"));
    }

    // ------------------------------------------------------------------------------------------

    /** Runs {@code action} and appends the creates and deletes it made to the trace. */
    private void step(String name, IoAction action) throws Exception {
        TestFileSystem.recordWrites(true);
        action.run();
        trace.add("# " + name);
        for (String op : TestFileSystem.getRecordedWrites()) {
            if (op.contains(prefix)) {
                trace.add(masked(op.replace(prefix, "")));
            }
        }
        TestFileSystem.recordWrites(false);
    }

    /** Commits change {@code changeId} with one entry of {@code bucket} in file {@code fileId}. */
    private void commit(
            long changeId, TableBucket bucket, String key, int fileId, long pos, String fileName)
            throws IOException {
        PositionIndexStore.BucketPart part =
                store.writeBucket(changeId, bucket, "", entries(key, fileId, pos));
        store.commitChange(
                changeId,
                Collections.singletonMap(
                        bucket,
                        new PositionIndexStore.BucketChange(
                                part,
                                Collections.singletonMap(fileId, fileName),
                                Collections.emptyList())));
    }

    /** Every file under the store, one per line: its path, then its content rendered as text. */
    private String tree() throws Exception {
        List<Path> files;
        try (Stream<Path> walk = Files.walk(root)) {
            files = walk.filter(Files::isRegularFile).sorted().collect(Collectors.toList());
        }
        List<String> out = new ArrayList<>();
        for (Path f : files) {
            String name = root.relativize(f).toString();
            String content;
            if (name.endsWith(".sst")) {
                content = sst(f);
            } else {
                content =
                        read(f).replace("\n", "\\n")
                                .replace("\t", "\\t")
                                .replaceAll("^v1\\\\n[0-9]{13}$", "v1\\\\n<millis>")
                                .replaceAll("\"size\":[0-9]+", "\"size\":<bytes>");
            }
            out.add(
                    masked(name)
                            + " = "
                            + content.replaceAll(
                                    "file index.sst [0-9]+", "file index.sst <bytes>"));
        }
        return String.join("\n", out);
    }

    private static String sst(Path file) throws Exception {
        RocksDB.loadLibrary();
        StringBuilder sb = new StringBuilder("sst");
        try (Options options = new Options();
                SstFileReader reader = new SstFileReader(options);
                ReadOptions readOptions = new ReadOptions()) {
            reader.open(file.toString());
            long deletions = reader.getTableProperties().getNumDeletions();
            try (SstFileReaderIterator it = reader.newIterator(readOptions)) {
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    sb.append(' ').append(hex(it.key())).append('=').append(hex(it.value()));
                }
                it.status();
            }
            sb.append(" deletions=").append(deletions);
        }
        return sb.toString();
    }

    /** Masks the random attempt of a checkpoint directory's name. */
    private static String masked(String path) {
        return path.replaceAll("/([0-9]+)_[0-9a-f]{16}(/|$| )", "/$1_<attempt>$2")
                .replaceAll("_trimmed/([0-9]{20})-[0-9a-f]{16}", "_trimmed/$1-<attempt>");
    }

    private Path changeDir(long changeId) {
        return root.resolve("rowPos").resolve(String.valueOf(changeId));
    }

    private static Iterator<Map.Entry<byte[], FilePos>> entries(String key, int fileId, long pos) {
        TreeMap<byte[], FilePos> m = new TreeMap<>(PositionIndexChangeBuilder::compareUnsigned);
        m.put(bytes(key), new FilePos(fileId, pos));
        return m.entrySet().iterator();
    }

    private static String lines(String... lines) {
        return String.join("\n", lines);
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xff));
        }
        return sb.toString();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String read(Path path) throws IOException {
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }

    /** An action that may throw. */
    @FunctionalInterface
    private interface IoAction {
        void run() throws Exception;
    }
}
