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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Tests of the format versions of the files of {@link FlussPositionIndexStore}. */
class FlussPositionIndexStoreFormatTest {

    private static final long TABLE_ID = 1L;
    private static final TableBucket B0 = new TableBucket(TABLE_ID, 0);
    private static final String NEWER = "its format version 2 is unknown";

    @TempDir Path tempDir;

    private Path root;
    private FlussPositionIndexStore store;

    @BeforeEach
    void setUp() throws IOException {
        root = Files.createDirectories(tempDir.resolve("lake"));
        store = new FlussPositionIndexStore(new FsPath(root.toUri()));
    }

    @Test
    void testChainEntryRoundTrip() throws Exception {
        commitWithFile(1, 4);
        store.register(-1, 100, 1, "identity-100");

        assertThat(chainEntries()).containsExactly("v1-00000000000000000001_-1_100_1_4");
        PositionIndexStore.ChainEntry entry = store.chain().get(0);
        assertThat(entry.snapshotId).isEqualTo(100);
        assertThat(entry.changeId).isEqualTo(1);
        assertThat(store.snapshotIdentity(entry)).isEqualTo("identity-100");
        // the registered max file id is read back from the name
        assertThat(store.allocateFileIds(1)).isEqualTo(5);
    }

    @Test
    void testNewerChainEntryIsRejected() throws Exception {
        commitEmpty(1);
        store.register(-1, 100, 1);
        Files.write(chainDir().resolve("v2-00000000000000000002_100_101_2_-1"), new byte[0]);

        assertThatThrownBy(() -> store.chain())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Cannot read the position index chain entry"
                                + " rowPos/_chain/v2-00000000000000000002_100_101_2_-1: its format"
                                + " version 2 is unknown; this Fluss version reads format versions"
                                + " up to 1.");
        // a writer refuses to extend the chain too
        commitEmpty(3);
        assertThatThrownBy(() -> store.register(100, 101, 3)).hasMessageContaining(NEWER);
    }

    @Test
    void testChainEntryWithoutVersionIsRejected() throws Exception {
        Files.createDirectories(chainDir());
        Files.write(chainDir().resolve("00000000000000000001_-1_100_1_-1"), new byte[0]);

        assertThatThrownBy(() -> store.chain()).hasMessageContaining("it has no format version");
    }

    @Test
    void testFileIdCounterRoundTrip() throws Exception {
        assertThat(store.allocateFileIds(3)).isEqualTo(0);
        assertThat(read(root.resolve(FlussPositionIndexStore.NEXT_FILE_ID))).isEqualTo("v1\n3");
        assertThat(store.allocateFileIds(2)).isEqualTo(3);
    }

    @Test
    void testNewerFileIdCounterIsRejected() throws Exception {
        store.allocateFileIds(3);
        write(root.resolve(FlussPositionIndexStore.NEXT_FILE_ID), "v2\n3");

        assertThatThrownBy(() -> store.allocateFileIds(1))
                .hasMessageContaining("rowPos/_next_file_id")
                .hasMessageContaining(NEWER);
    }

    @Test
    void testChangeRoundTrip() throws Exception {
        PositionIndexStore.BucketPart part = store.writeBucket(1, B0, entries("k", 7, 3));
        assertThat(read(changeDir(1).resolve(FlussPositionIndexStore.CHANGE_STARTED)))
                .startsWith("v1\n");
        store.commitChange(
                1,
                Collections.singletonMap(
                        B0,
                        new PositionIndexStore.BucketChange(
                                part,
                                Collections.singletonMap(7, "f7.parquet"),
                                Collections.emptyList())));
        store.register(-1, 100, 1);

        assertThat(read(changeDir(1).resolve(FlussPositionIndexStore.CHANGE_COMMITTED)))
                .isEqualTo("v1\n-1\n");
        PositionIndexStore.BucketData data = store.readChange(store.chain().get(0), B0, dl());
        assertThat(data.fileId2Name).containsOnly(entry(7, "f7.parquet"));
        assertThat(data.localSsts).hasSize(1);
    }

    @Test
    void testNewerCommittedChangeIsRejected() throws Exception {
        commitEmpty(1);
        store.register(-1, 100, 1);
        write(changeDir(1).resolve(FlussPositionIndexStore.CHANGE_COMMITTED), "v2\n-1\n");

        assertThatThrownBy(() -> store.readChange(store.chain().get(0), B0, dl()))
                .hasMessageContaining("rowPos/1/_COMMITTED")
                .hasMessageContaining(NEWER);
    }

    @Test
    void testNewerManifestIsRejected() throws Exception {
        commitWithFile(1, 7);
        store.register(-1, 100, 1);
        write(
                changeDir(1).resolve("rowpos.manifest"),
                "{\"version\":2,\"sstValueVersion\":1,\"buckets\":{}}");

        assertThatThrownBy(() -> store.readChange(store.chain().get(0), B0, dl()))
                .hasMessageContaining("rowPos/1/rowpos.manifest")
                .hasMessageContaining(NEWER);
    }

    @Test
    void testNewerSstValuesOfAManifestAreRejected() throws Exception {
        commitWithFile(1, 7);
        store.register(-1, 100, 1);
        write(
                changeDir(1).resolve("rowpos.manifest"),
                "{\"version\":1,\"sstValueVersion\":2,\"buckets\":{}}");

        assertThatThrownBy(() -> store.readChange(store.chain().get(0), B0, dl()))
                .hasMessageContaining("the SST values of")
                .hasMessageContaining("rowPos/1/rowpos.manifest")
                .hasMessageContaining(NEWER);
    }

    @Test
    void testNewerStartedChangeIsNotDeletedAsAnOrphan() throws Exception {
        store.writeBucket(1, B0, entries("k", 0, 0));
        write(changeDir(1).resolve(FlussPositionIndexStore.CHANGE_STARTED), "v2\n0");

        assertThatThrownBy(() -> store.deleteOrphanChanges(0))
                .hasMessageContaining("rowPos/1/_STARTED")
                .hasMessageContaining(NEWER);
        assertThat(changeDir(1)).exists();
    }

    @Test
    void testStagedPartOfANewerChangeIsRejected() throws Exception {
        long stagingId = StagedIndexPart.stagingIdOf(TABLE_ID, 1L);
        PositionIndexStore.BucketPart staged;
        try (PositionIndexStore.StagedPartWriter writer =
                store.stageBucket(stagingId, B0, "", null)) {
            writer.put(bytes("k1"), new FilePos(0, 0));
            staged = writer.finish();
        }
        Path started = changeDir(stagingId).resolve(FlussPositionIndexStore.CHANGE_STARTED);
        assertThat(read(started)).startsWith("v1\n");
        write(started, "v2\n0");

        assertThatThrownBy(
                        () ->
                                store.publishStaged(
                                        stagingId,
                                        9,
                                        B0,
                                        staged,
                                        Collections.singletonMap(0, 5),
                                        k -> {}))
                .hasMessageContaining("rowPos/" + stagingId + "/_STARTED")
                .hasMessageContaining(NEWER);
    }

    @Test
    void testCompactionPartOfANewerChangeIsRejected() throws Exception {
        PositionIndexStore.BucketPart part = store.writeBucket(3, B0, entries("k", 0, 0));
        write(changeDir(3).resolve(FlussPositionIndexStore.CHANGE_STARTED), "v2\n0");

        assertThatThrownBy(() -> store.remapFileIds(3, B0, part, Collections.singletonMap(0, 5)))
                .hasMessageContaining("rowPos/3/_STARTED")
                .hasMessageContaining(NEWER);
    }

    @Test
    void testCheckpointRoundTrip() throws Exception {
        commitEmpty(1);
        store.register(-1, 100, 1);
        File sst = Files.write(tempDir.resolve("full.sst"), new byte[] {1, 2, 3}).toFile();
        store.writeCheckpoint(100, B0, sst, Collections.singletonMap(1, "a"));

        Path dir = checkpointDir().resolve("100");
        assertThat(read(dir.resolve(FlussPositionIndexStore.CKPT_DONE))).isEqualTo("v1\nsst");
        PositionIndexStore.Checkpoint checkpoint = store.latestCheckpoint(B0, store.chain(), 0);
        PositionIndexStore.BucketData data = store.readCheckpoint(checkpoint, B0, dl());
        assertThat(data.fileId2Name).containsOnly(entry(1, "a"));
        assertThat(data.localSsts).hasSize(1);
    }

    @Test
    void testNewerCheckpointIsRejected() throws Exception {
        commitEmpty(1);
        store.register(-1, 100, 1);
        store.writeCheckpoint(100, B0, null, Collections.emptyMap());
        write(checkpointDir().resolve("100").resolve(FlussPositionIndexStore.CKPT_DONE), "v2\n");

        PositionIndexStore.Checkpoint checkpoint = store.latestCheckpoint(B0, store.chain(), 0);
        assertThatThrownBy(() -> store.readCheckpoint(checkpoint, B0, dl()))
                .hasMessageContaining("rowPos/_ckpt/0/100/_DONE")
                .hasMessageContaining(NEWER);
    }

    @Test
    void testTrimMarkerHasAVersion() throws Exception {
        for (int i = 0; i < 3; i++) {
            commitEmpty(i + 1);
            store.register(i == 0 ? -1 : 99 + i, 100 + i, i + 1);
        }
        store.writeCheckpoint(102, B0, null, Collections.emptyMap());

        assertThat(store.trim(Collections.singletonList(B0), 0)).isEqualTo(2);
        assertThat(read(root.resolve(FlussPositionIndexStore.CHAIN_TRIMMED))).isEqualTo("v1\n102");
    }

    @Test
    void testPlanRoundTrip() throws Exception {
        List<byte[]> lakeSplits = Arrays.asList(new byte[] {1, 2}, new byte[0]);
        String reference = store.writeReindexPlan(7, 0, 3, lakeSplits);

        PositionIndexStore.ReindexPlan plan = PositionIndexStore.readReindexPlan(reference);
        assertThat(plan.splitSerializerVersion).isEqualTo(3);
        assertThat(plan.lakeSplits).containsExactlyElementsOf(lakeSplits);
    }

    @Test
    void testPlanWrittenTheOldWayReads() throws Exception {
        // format 1 as the first writer of plans laid it out
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(1);
            out.writeInt(4);
            out.writeInt(1);
            out.writeInt(2);
            out.write(new byte[] {5, 6});
        }
        Path file = Files.createDirectories(changeDir(7).resolve("_plan")).resolve("0");
        Files.write(file, bytes.toByteArray());

        PositionIndexStore.ReindexPlan plan =
                PositionIndexStore.readReindexPlan(new FsPath(file.toUri()).toString());
        assertThat(plan.splitSerializerVersion).isEqualTo(4);
        assertThat(plan.lakeSplits).containsExactly(new byte[] {5, 6});
    }

    @Test
    void testNewerPlanIsRejected() throws Exception {
        String reference =
                store.writeReindexPlan(7, 0, 3, Collections.singletonList(new byte[] {1}));
        Path file = changeDir(7).resolve("_plan").resolve("0");
        byte[] content = Files.readAllBytes(file);
        content[3] = 2; // the format, the first int
        Files.write(file, content);

        assertThatThrownBy(() -> PositionIndexStore.readReindexPlan(reference))
                .hasMessageContaining("the re-index plan " + reference)
                .hasMessageContaining(NEWER);
    }

    // ------------------------------------------------------------------------------------------

    private void commitEmpty(long changeId) throws IOException {
        store.commitChange(changeId, Collections.emptyMap());
    }

    /** Commits a change of B0 that adds file id {@code fileId}. */
    private void commitWithFile(long changeId, int fileId) throws IOException {
        store.commitChange(
                changeId,
                Collections.singletonMap(
                        B0,
                        new PositionIndexStore.BucketChange(
                                PositionIndexStore.BucketPart.NONE,
                                Collections.singletonMap(fileId, "f" + fileId + ".parquet"),
                                Collections.emptyList())));
    }

    private static java.util.Iterator<Map.Entry<byte[], FilePos>> entries(
            String key, int fileId, long pos) {
        TreeMap<byte[], FilePos> m = new TreeMap<>(PositionIndexChangeBuilder::compareUnsigned);
        m.put(bytes(key), new FilePos(fileId, pos));
        return m.entrySet().iterator();
    }

    private List<String> chainEntries() throws IOException {
        try (Stream<Path> files = Files.list(chainDir())) {
            return files.map(p -> p.getFileName().toString()).sorted().collect(Collectors.toList());
        }
    }

    private Path chainDir() {
        return root.resolve(FlussPositionIndexStore.CHAIN_DIR);
    }

    private Path changeDir(long changeId) {
        return root.resolve("rowPos").resolve(String.valueOf(changeId));
    }

    private Path checkpointDir() {
        return root.resolve(FlussPositionIndexStore.CKPT_DIR).resolve("0");
    }

    private File dl() throws IOException {
        return Files.createTempDirectory(tempDir, "dl").toFile();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String read(Path path) throws IOException {
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }

    private static void write(Path path, String content) throws IOException {
        Files.write(path, bytes(content));
    }
}
