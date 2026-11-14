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

package org.apache.fluss.server.kv.snapshot;

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.CloseableRegistry;
import org.apache.fluss.utils.concurrent.Executors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests of the change list of derived deletion vectors saved with a KV snapshot: its handle, its
 * metadata round trip, and the life of its segments as shared files of the snapshot.
 */
class DvChangeListHandleTest {

    private static final TableBucket BUCKET = new TableBucket(1, 1);

    @Test
    void testSegmentsCoverTheRangeInOrder(@TempDir Path dir) throws Exception {
        DvChangeListHandle.Segment a = segment(dir, "dvcl-a", 4, 8, 3);
        DvChangeListHandle.Segment b = segment(dir, "dvcl-b", 8, 10, 2);

        DvChangeListHandle handle = new DvChangeListHandle(4, 10, 2, 42, 5, Arrays.asList(a, b));

        assertThat(handle.isSupported()).isTrue();
        assertThat(handle.segments()).containsExactly(a, b);
        assertThat(handle.bytes()).isEqualTo(a.file().getKvFileHandle().getSize() * 2);
        assertThatThrownBy(() -> new DvChangeListHandle(4, 10, 2, 42, 5, Arrays.asList(b, a)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("without gaps");
        assertThatThrownBy(
                        () -> new DvChangeListHandle(4, 11, 2, 42, 5, Collections.singletonList(a)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DvChangeListHandle(4, 10, 2, 42, 6, Arrays.asList(a, b)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("entries");
        // an empty change list: nothing changed between the readable offset and the snapshot
        assertThat(new DvChangeListHandle(7, 7, 7, 1, 0, Collections.emptyList()).entries())
                .isZero();
    }

    @Test
    void testMetadataRoundTrip(@TempDir Path dir) throws Exception {
        CompletedSnapshot snapshot = snapshot(dir, 1, changeList(dir, "dvcl-a", "dvcl-b"));

        CompletedSnapshot restored =
                CompletedSnapshotJsonSerde.fromJson(CompletedSnapshotJsonSerde.toJson(snapshot));

        assertThat(restored).isEqualTo(snapshot);
        assertThat(restored.getKvSnapshotHandle().getDvChangeList())
                .isEqualTo(snapshot.getKvSnapshotHandle().getDvChangeList());
    }

    @Test
    void testUnsupportedChangeListIsNotWrittenAgain(@TempDir Path dir) throws Exception {
        String json =
                new String(
                        CompletedSnapshotJsonSerde.toJson(snapshot(dir, 1, null)),
                        java.nio.charset.StandardCharsets.UTF_8);
        String withNewer =
                json.substring(0, json.length() - 1)
                        + ",\"dv_change_list\":{\"version\":7,\"new_field\":true}}";

        CompletedSnapshot restored =
                CompletedSnapshotJsonSerde.fromJson(
                        withNewer.getBytes(java.nio.charset.StandardCharsets.UTF_8));

        DvChangeListHandle changeList = restored.getKvSnapshotHandle().getDvChangeList();
        assertThat(changeList).isNotNull();
        assertThat(changeList.isSupported()).isFalse();
        assertThat(changeList.version()).isEqualTo(7);
        assertThat(changeList.segments()).isEmpty();
        assertThat(
                        new String(
                                CompletedSnapshotJsonSerde.toJson(restored),
                                java.nio.charset.StandardCharsets.UTF_8))
                .isEqualTo(json);
    }

    @Test
    void testFailedSnapshotDiscardsOnlyItsNewSegments(@TempDir Path dir) throws Exception {
        DvChangeListHandle.Segment reused = segment(dir, "dvcl-old", 4, 8, 3);
        DvChangeListHandle.Segment uploaded = segment(dir, "dvcl-new", 8, 10, 2);
        DvChangeListHandle changeList =
                new DvChangeListHandle(
                        4, 10, 4, 1, 5, Arrays.asList(reused.asReference(), uploaded));
        CompletedSnapshot snapshot = snapshot(dir, 2, changeList);

        // the snapshot was never committed: a new handle owns its new files only
        snapshot.discardAsync(Executors.directExecutor()).get();

        assertThat(file(reused)).exists();
        assertThat(file(uploaded)).doesNotExist();
    }

    @Test
    void testRestoredSnapshotKeepsItsSegments(@TempDir Path dir) throws Exception {
        DvChangeListHandle changeList = changeList(dir, "dvcl-a", "dvcl-b");
        CompletedSnapshot restored =
                CompletedSnapshotJsonSerde.fromJson(
                        CompletedSnapshotJsonSerde.toJson(snapshot(dir, 1, changeList)));

        restored.discardAsync(Executors.directExecutor()).get();

        for (DvChangeListHandle.Segment s : changeList.segments()) {
            assertThat(file(s)).exists();
        }
    }

    @Test
    void testSegmentsLiveAsLongAsARetainedSnapshotReferencesThem(@TempDir Path dir)
            throws Exception {
        ExecutorService io = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            CompletedSnapshotStore store =
                    new CompletedSnapshotStore(
                            1,
                            new SharedKvFileRegistry(Executors.directExecutor()),
                            Collections.emptyList(),
                            TestCompletedSnapshotHandleStore.newBuilder().build(),
                            io,
                            snapshot -> false);
            DvChangeListHandle.Segment a = segment(dir, "dvcl-a", 0, 10, 4);
            DvChangeListHandle.Segment b = segment(dir, "dvcl-b", 10, 20, 4);
            DvChangeListHandle.Segment c = segment(dir, "dvcl-c", 20, 30, 4);

            // as the coordinator gets them: through the snapshot's json
            store.add(viaJson(snapshot(dir, 1, changeList(0, 10, a))));
            store.add(viaJson(snapshot(dir, 2, changeList(0, 20, a.asReference(), b))));
            // only snapshot 2 is retained, and it still references a
            assertThat(file(a)).exists();
            assertThat(file(b)).exists();

            // the readable offset passed 10: snapshot 3 no longer references a
            store.add(viaJson(snapshot(dir, 3, changeList(10, 30, b.asReference(), c))));
            assertThat(file(a)).doesNotExist();
            assertThat(file(b)).exists();
            assertThat(file(c)).exists();

            // a snapshot without a change list, e.g. taken while the change list was missing
            store.add(viaJson(snapshot(dir, 4, null)));
            assertThat(file(b)).doesNotExist();
            assertThat(file(c)).doesNotExist();
        } finally {
            io.shutdownNow();
        }
    }

    @Test
    void testSegmentsAreNotDownloadedWithTheKvData(@TempDir Path dir) throws Exception {
        Path sst = Files.write(dir.resolve("1.sst"), new byte[] {1, 2, 3});
        KvSnapshotHandle handle =
                KvSnapshotHandle.create(
                                Collections.singletonList(
                                        KvFileHandleAndLocalPath.of(
                                                new KvFileHandle(sst.toString(), 3), "1.sst")),
                                Collections.emptyList(),
                                3)
                        .withDvChangeList(changeList(dir, "dvcl-a", "dvcl-b"));
        Path destination = dir.resolve("db");
        ExecutorService io = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            new KvSnapshotDataDownloader(io)
                    .transferAllDataToDirectory(
                            new KvSnapshotDownloadSpec(handle, destination),
                            new CloseableRegistry());
        } finally {
            io.shutdownNow();
        }

        assertThat(destination.toFile().list()).containsExactly("1.sst");
    }

    // ---------------------------------------------------------------------------------------------

    private static CompletedSnapshot viaJson(CompletedSnapshot snapshot) {
        return CompletedSnapshotJsonSerde.fromJson(CompletedSnapshotJsonSerde.toJson(snapshot));
    }

    private static CompletedSnapshot snapshot(
            Path dir, long id, @javax.annotation.Nullable DvChangeListHandle changeList)
            throws Exception {
        Path location = Files.createDirectories(dir.resolve("snap-" + id));
        KvSnapshotHandle handle =
                KvSnapshotHandle.create(Collections.emptyList(), Collections.emptyList(), 0)
                        .withDvChangeList(changeList);
        return new CompletedSnapshot(
                BUCKET,
                id,
                new FsPath(location.toUri().toString().replaceAll("/$", "")),
                handle,
                changeList == null ? 0 : changeList.end(),
                null,
                null);
    }

    private static DvChangeListHandle changeList(Path dir, String first, String second)
            throws Exception {
        return changeList(4, 10, segment(dir, first, 4, 8, 3), segment(dir, second, 8, 10, 2));
    }

    private static DvChangeListHandle changeList(
            long from, long end, DvChangeListHandle.Segment... segments) {
        List<DvChangeListHandle.Segment> list = Arrays.asList(segments);
        long entries = 0;
        for (DvChangeListHandle.Segment s : list) {
            entries += s.entries();
        }
        // a referenced segment may start below the readable offset; the list starts with it
        return new DvChangeListHandle(list.get(0).from(), end, from, 7, entries, list);
    }

    private static DvChangeListHandle.Segment segment(
            Path dir, String name, long from, long end, long entries) throws Exception {
        Path shared = Files.createDirectories(dir.resolve("shared"));
        Path file = shared.resolve(name);
        if (!Files.exists(file)) {
            Files.write(file, new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
        }
        return new DvChangeListHandle.Segment(
                KvFileHandleAndLocalPath.of(new KvFileHandle(file.toString(), 8), name),
                from,
                end,
                entries);
    }

    private static File file(DvChangeListHandle.Segment segment) {
        return new File(segment.file().getKvFileHandle().getFilePath());
    }
}
