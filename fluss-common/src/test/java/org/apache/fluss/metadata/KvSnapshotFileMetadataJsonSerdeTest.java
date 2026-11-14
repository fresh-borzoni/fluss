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

package org.apache.fluss.metadata;

import org.apache.fluss.utils.json.JsonSerdeTestBase;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Compatibility test for {@link KvSnapshotFileMetadataJsonSerde}. */
class KvSnapshotFileMetadataJsonSerdeTest extends JsonSerdeTestBase<KvSnapshotFileMetadata> {

    static final String GOLDEN_JSON =
            "{\"version\":1,"
                    + "\"table_id\":1,\"partition_id\":10,\"bucket_id\":1,"
                    + "\"snapshot_id\":1,"
                    + "\"snapshot_location\":\"oss://bucket/snapshot\","
                    + "\"kv_snapshot_handle\":{"
                    + "\"shared_file_handles\":[{\"kv_file_handle\":{\"path\":\"oss://bucket/snapshot/shared/t1.sst\",\"size\":1},\"local_path\":\"localPath1\"}],"
                    + "\"private_file_handles\":[{\"kv_file_handle\":{\"path\":\"oss://bucket/snapshot/snapshot1/t2\",\"size\":2},\"local_path\":\"localPath2\"}],"
                    + "\"snapshot_incremental_size\":3},\"log_offset\":10,\"row_count\":1234,"
                    + "\"auto_inc_id_range\":[{\"column_id\":2,\"start\":10000,\"end\":20000}]}";

    // GOLDEN_JSON with the change list of derived deletion vectors, as a DV bucket's leader writes
    static final String GOLDEN_DV_JSON =
            GOLDEN_JSON.substring(0, GOLDEN_JSON.length() - 1)
                    + ",\"dv_change_list\":{\"version\":1,\"from\":4,\"end\":10,"
                    + "\"valid_from\":2,\"lake_snapshot_id\":42,\"entries\":5,\"segments\":["
                    + "{\"kv_file_handle\":{\"path\":\"oss://bucket/snapshot/shared/dvcl-a\",\"size\":30},"
                    + "\"local_path\":\"dvcl-a\",\"from\":4,\"end\":8,\"entries\":3},"
                    + "{\"kv_file_handle\":{\"path\":\"oss://bucket/snapshot/shared/dvcl-b\",\"size\":20},"
                    + "\"local_path\":\"dvcl-b\",\"from\":8,\"end\":10,\"entries\":2}]}}";

    KvSnapshotFileMetadataJsonSerdeTest() {
        super(KvSnapshotFileMetadataJsonSerde.INSTANCE);
    }

    @Test
    void testOldFormatHasNoDvChangeList() {
        KvSnapshotFileMetadata metadata =
                KvSnapshotFileMetadataJsonSerde.fromJson(
                        GOLDEN_JSON.getBytes(StandardCharsets.UTF_8));

        assertThat(metadata.getDvChangeList()).isNull();
        assertThat(metadata).isEqualTo(createObjects()[0]);
    }

    @Test
    void testDvChangeListLeavesTheSnapshotAsItIs() {
        // a reader that does not use the change list sees the same snapshot with or without it
        KvSnapshotFileMetadata with =
                KvSnapshotFileMetadataJsonSerde.fromJson(
                        GOLDEN_DV_JSON.getBytes(StandardCharsets.UTF_8));
        KvSnapshotFileMetadata without = createObjects()[0];

        assertThat(withoutDvChangeList(with)).isEqualTo(without);
        assertThat(with.getDvChangeList()).isEqualTo(dvChangeList());
    }

    @Test
    void testDvChangeListRoundTrip() {
        KvSnapshotFileMetadata metadata = withDvChangeList(createObjects()[0], dvChangeList());

        String json =
                new String(
                        KvSnapshotFileMetadataJsonSerde.toJson(metadata), StandardCharsets.UTF_8);

        assertThat(json).isEqualTo(GOLDEN_DV_JSON);
        assertThat(KvSnapshotFileMetadataJsonSerde.fromJson(json.getBytes(StandardCharsets.UTF_8)))
                .isEqualTo(metadata);
    }

    @Test
    void testUnknownDvChangeListVersionKeepsTheSnapshot() {
        // a newer change list: the snapshot stays readable, the change list is not used or written
        String json =
                GOLDEN_JSON.substring(0, GOLDEN_JSON.length() - 1)
                        + ",\"dv_change_list\":{\"version\":2,\"something\":[1,2]}}";

        KvSnapshotFileMetadata metadata =
                KvSnapshotFileMetadataJsonSerde.fromJson(json.getBytes(StandardCharsets.UTF_8));

        assertThat(withoutDvChangeList(metadata)).isEqualTo(createObjects()[0]);
        assertThat(metadata.getDvChangeList()).isNotNull();
        assertThat(metadata.getDvChangeList().isSupported()).isFalse();
        assertThat(metadata.getDvChangeList().getVersion()).isEqualTo(2);
        assertThat(metadata.getDvChangeList().getSegments()).isEmpty();
        assertThat(
                        new String(
                                KvSnapshotFileMetadataJsonSerde.toJson(metadata),
                                StandardCharsets.UTF_8))
                .isEqualTo(GOLDEN_JSON);
    }

    @Test
    void testDvChangeListWithoutVersionIsUnsupported() {
        String json =
                GOLDEN_DV_JSON.replace(
                        "\"dv_change_list\":{\"version\":1,", "\"dv_change_list\":{");

        KvSnapshotFileMetadata metadata =
                KvSnapshotFileMetadataJsonSerde.fromJson(json.getBytes(StandardCharsets.UTF_8));

        assertThat(metadata.getDvChangeList().isSupported()).isFalse();
        assertThat(metadata.getDvChangeList().getVersion()).isEqualTo(-1);
    }

    static KvSnapshotFileMetadata.DvChangeList dvChangeList() {
        return new KvSnapshotFileMetadata.DvChangeList(
                4,
                10,
                2,
                42,
                5,
                Arrays.asList(
                        new KvSnapshotFileMetadata.DvChangeListSegment(
                                new KvSnapshotFileMetadata.FileHandle(
                                        "oss://bucket/snapshot/shared/dvcl-a", 30, "dvcl-a"),
                                4,
                                8,
                                3),
                        new KvSnapshotFileMetadata.DvChangeListSegment(
                                new KvSnapshotFileMetadata.FileHandle(
                                        "oss://bucket/snapshot/shared/dvcl-b", 20, "dvcl-b"),
                                8,
                                10,
                                2)));
    }

    private static KvSnapshotFileMetadata withDvChangeList(
            KvSnapshotFileMetadata m, KvSnapshotFileMetadata.DvChangeList dvChangeList) {
        return new KvSnapshotFileMetadata(
                m.getTableBucket(),
                m.getSnapshotId(),
                m.getSnapshotLocation(),
                m.getSharedFiles(),
                m.getPrivateFiles(),
                m.getIncrementalSize(),
                m.getLogOffset(),
                m.getRowCount(),
                m.getAutoIncrementRanges(),
                dvChangeList);
    }

    private static KvSnapshotFileMetadata withoutDvChangeList(KvSnapshotFileMetadata m) {
        return withDvChangeList(m, null);
    }

    @Test
    void testOptionalFileDigestRoundTrip() {
        String sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        KvSnapshotFileMetadata metadata =
                new KvSnapshotFileMetadata(
                        new TableBucket(1, 1),
                        2,
                        "oss://bucket/snapshot",
                        Collections.emptyList(),
                        Collections.singletonList(
                                new KvSnapshotFileMetadata.FileHandle(
                                        "oss://bucket/snapshot/private.sst",
                                        10,
                                        "private.sst",
                                        sha256)),
                        10,
                        20,
                        null,
                        null);

        byte[] json = KvSnapshotFileMetadataJsonSerde.toJson(metadata);

        assertThat(new String(json, StandardCharsets.UTF_8))
                .contains("\"sha256\":\"" + sha256 + "\"");
        assertThat(KvSnapshotFileMetadataJsonSerde.fromJson(json)).isEqualTo(metadata);
    }

    @Test
    void testRejectMissingVersion() {
        byte[] json = GOLDEN_JSON.replace("{\"version\":1,", "{").getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> KvSnapshotFileMetadataJsonSerde.fromJson(json))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("version");
    }

    @Test
    void testRejectUnsupportedVersion() {
        byte[] json =
                GOLDEN_JSON
                        .replace("\"version\":1", "\"version\":2")
                        .getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> KvSnapshotFileMetadataJsonSerde.fromJson(json))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("version");
    }

    @Test
    void testRejectVersionOutsideIntegerRange() {
        byte[] json =
                GOLDEN_JSON
                        .replace("\"version\":1", "\"version\":4294967297")
                        .getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> KvSnapshotFileMetadataJsonSerde.fromJson(json))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("version");
    }

    @Override
    protected KvSnapshotFileMetadata[] createObjects() {
        return new KvSnapshotFileMetadata[] {
            new KvSnapshotFileMetadata(
                    new TableBucket(1, 10L, 1),
                    1,
                    "oss://bucket/snapshot",
                    Collections.singletonList(
                            new KvSnapshotFileMetadata.FileHandle(
                                    "oss://bucket/snapshot/shared/t1.sst", 1, "localPath1")),
                    Collections.singletonList(
                            new KvSnapshotFileMetadata.FileHandle(
                                    "oss://bucket/snapshot/snapshot1/t2", 2, "localPath2")),
                    3,
                    10,
                    1234L,
                    Collections.singletonList(
                            new KvSnapshotFileMetadata.AutoIncrementRange(2, 10000, 20000)))
        };
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {GOLDEN_JSON};
    }
}
