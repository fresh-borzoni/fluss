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

    KvSnapshotFileMetadataJsonSerdeTest() {
        super(KvSnapshotFileMetadataJsonSerde.INSTANCE);
    }

    /**
     * With row TTL the bucket's leader saves its TTL filter's state at the top level: P and the
     * slices below the snapshot's offset. A state that is not whole reads as none.
     */
    @Test
    void testRowTtlRoundTrip() {
        KvSnapshotFileMetadata.RowTtl rowTtl =
                new KvSnapshotFileMetadata.RowTtl(
                        7L, 5L, Arrays.asList(8L, 10L), Arrays.asList(-3L, 400L));
        KvSnapshotFileMetadata m = createObjects()[0];
        KvSnapshotFileMetadata metadata =
                new KvSnapshotFileMetadata(
                        m.getTableBucket(),
                        m.getSnapshotId(),
                        m.getSnapshotLocation(),
                        m.getSharedFiles(),
                        m.getPrivateFiles(),
                        m.getIncrementalSize(),
                        m.getLogOffset(),
                        m.getRowCount(),
                        m.getAutoIncrementRanges(),
                        rowTtl);

        String json =
                new String(
                        KvSnapshotFileMetadataJsonSerde.toJson(metadata), StandardCharsets.UTF_8);

        assertThat(json)
                .endsWith(
                        "\"ttl_capped_from\":7,\"ttl_slices\":{\"from\":5,\"ends\":[8,10],"
                                + "\"mins\":[-3,400]}}");
        KvSnapshotFileMetadata read =
                KvSnapshotFileMetadataJsonSerde.fromJson(json.getBytes(StandardCharsets.UTF_8));
        assertThat(read).isEqualTo(metadata);
        assertThat(read.getRowTtl()).isEqualTo(rowTtl);

        String partial = json.replace("\"mins\":[-3,400]", "\"mins\":[-3]");
        assertThat(
                        KvSnapshotFileMetadataJsonSerde.fromJson(
                                        partial.getBytes(StandardCharsets.UTF_8))
                                .getRowTtl())
                .isNull();
        assertThat(
                        KvSnapshotFileMetadataJsonSerde.fromJson(
                                        GOLDEN_JSON.getBytes(StandardCharsets.UTF_8))
                                .getRowTtl())
                .isNull();
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
