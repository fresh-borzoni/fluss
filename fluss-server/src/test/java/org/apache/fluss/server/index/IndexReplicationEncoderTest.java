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

package org.apache.fluss.server.index;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IndexTableUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.row.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link IndexReplicationEncoder}. */
class IndexReplicationEncoderTest {

    private static final TablePath MAIN_PATH = TablePath.of("db", "main");
    private static final TablePath INDEX_PATH =
            TablePath.of("db", IndexTableUtils.indexTableName("main", "idx_name"));

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void testIndexKeysPreserveCandidateAndSourceIdentity(int indexColumnCount) {
        TableDescriptor main =
                mainDescriptor(2, Arrays.asList("name", "id", "p").subList(0, indexColumnCount));
        TableDescriptor index =
                IndexTableDescriptorFactory.derive(main, 1L, main.getSchema().getIndexes().get(0));
        TabletServerMetadataCache cache = indexCache(index);
        TableInfo mainInfo = tableInfo(MAIN_PATH, 1L, main);
        IndexReplicationEncoder encoder =
                IndexReplicationEncoder.createAll(mainInfo, new TableBucket(1L, 10L, 0), cache)
                        .get(0);
        GenericRow row = GenericRow.of(1, fromString("name"), fromString("p"));
        byte[] key = encoder.encodeDataEntry(row).key();

        assertThat(encoder.encodeDataEntry(row).key()).isEqualTo(key);
        assertThat(
                        encoder.encodeDataEntry(
                                        GenericRow.of(2, fromString("name"), fromString("p")))
                                .key())
                .isNotEqualTo(key);
        assertThat(
                        encoder.encodeDataEntry(
                                        GenericRow.of(1, fromString("other"), fromString("p")))
                                .key())
                .isNotEqualTo(key);

        IndexReplicationEncoder recreatedPartition =
                IndexReplicationEncoder.createAll(mainInfo, new TableBucket(1L, 11L, 0), cache)
                        .get(0);
        assertThat(recreatedPartition.encodeDataEntry(row).key()).isNotEqualTo(key);
        assertThat(recreatedPartition.encodeProgress(0, 1L).key())
                .isNotEqualTo(encoder.encodeProgress(0, 1L).key());

        IndexReplicationEncoder anotherBucket =
                IndexReplicationEncoder.createAll(mainInfo, new TableBucket(1L, 10L, 1), cache)
                        .get(0);
        assertThat(anotherBucket.encodeProgress(0, 1L).key())
                .isNotEqualTo(encoder.encodeProgress(0, 1L).key());
        assertThat(encoder.encodeProgress(0, 2L).key())
                .isEqualTo(encoder.encodeProgress(0, 1L).key());
    }

    /**
     * The removed pointer of a row sits in the bucket of its data pointer, under its own record
     * kind and the same routing key, names its source bucket and carries the offset of the change
     * that removed it.
     */
    @Test
    void testRemovedPointersOfAMainTableWithHistory() {
        TableDescriptor main = withHistory(mainDescriptor(2));
        TableDescriptor index =
                IndexTableDescriptorFactory.derive(main, 1L, main.getSchema().getIndexes().get(0));
        TabletServerMetadataCache cache = indexCache(index);
        TableInfo mainInfo = tableInfo(MAIN_PATH, 1L, main);
        IndexReplicationEncoder encoder =
                IndexReplicationEncoder.createAll(mainInfo, new TableBucket(1L, 10L, 1), cache)
                        .get(0);
        assertThat(encoder.retainsRemovals()).isTrue();
        assertThat(encoder.getIndexBucketCount()).isEqualTo(2);

        GenericRow row = GenericRow.of(1, fromString("name"), fromString("p"));
        IndexReplicationEncoder.IndexEntry data = encoder.encodeDataEntry(row);
        byte[] dataKey = data.key();
        int bucket = data.targetBucket();
        BinaryRow dataValue = data.value().copy();
        IndexReplicationEncoder.IndexEntry removed = encoder.encodeRemovedEntry(row, 42L, bucket);
        BinaryRow removedValue = removed.value().copy();

        // [name, id, p, partition id, kind, routing key, row key, source offset]
        assertThat(removed.targetBucket()).isEqualTo(bucket);
        assertThat(dataKey[0]).isEqualTo(IndexTableUtils.DATA_RECORD_KIND);
        assertThat(removed.key()[0]).isEqualTo(IndexTableUtils.REMOVED_RECORD_KIND);
        assertThat(removedValue.getByte(4)).isEqualTo(IndexTableUtils.REMOVED_RECORD_KIND);
        assertThat(removedValue.getBytes(5)).isEqualTo(dataValue.getBytes(5));
        assertThat(removedValue.getBytes(6))
                .isEqualTo(IndexTableUtils.removedRowKey(1, dataValue.getBytes(6)));
        assertThat(IndexTableUtils.sourceBucketOf(removedValue.getBytes(6))).isEqualTo(1);
        assertThat(removedValue.getLong(7)).isEqualTo(42L);
        assertThat(dataValue.isNullAt(7)).isTrue();
        for (int i = 0; i < 4; i++) {
            assertThat(removedValue.isNullAt(i)).isFalse();
        }
        assertThat(removedValue.getString(0)).isEqualTo(fromString("name"));
        assertThat(removedValue.getInt(1)).isEqualTo(1);
        assertThat(removedValue.getLong(3)).isEqualTo(10L);

        // a later removal of the same pointer replaces it; another source bucket has its own
        assertThat(encoder.encodeRemovedEntry(row, 50L, bucket).key()).isEqualTo(removed.key());
        IndexReplicationEncoder otherBucket =
                IndexReplicationEncoder.createAll(mainInfo, new TableBucket(1L, 10L, 0), cache)
                        .get(0);
        assertThat(otherBucket.encodeRemovedEntry(row, 42L, bucket).key())
                .isNotEqualTo(removed.key());

        // a main table without history deletes pointers outright
        IndexReplicationEncoder plain =
                IndexReplicationEncoder.createAll(
                                tableInfo(MAIN_PATH, 1L, mainDescriptor(2)),
                                new TableBucket(1L, 10L, 1),
                                cache)
                        .get(0);
        assertThat(plain.retainsRemovals()).isFalse();
        assertThatThrownBy(() -> plain.encodeRemovedEntry(row, 42L, bucket))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testMainTableRescalePreservesExplicitIndexRouting() {
        TableDescriptor original = mainDescriptor(2);
        TableDescriptor index =
                IndexTableDescriptorFactory.derive(
                        original, 1L, original.getSchema().getIndexes().get(0));
        TabletServerMetadataCache cache = indexCache(index);
        TableBucket source = new TableBucket(1L, 10L, 0);
        IndexReplicationEncoder before =
                IndexReplicationEncoder.createAll(tableInfo(MAIN_PATH, 1L, original), source, cache)
                        .get(0);
        TableDescriptor rescaled = TableDescriptor.builder(original).distributedBy(5, "id").build();
        IndexReplicationEncoder after =
                IndexReplicationEncoder.createAll(tableInfo(MAIN_PATH, 1L, rescaled), source, cache)
                        .get(0);

        for (int id = 0; id < 20; id++) {
            GenericRow row = GenericRow.of(id, fromString("name" + id), fromString("p"));
            IndexReplicationEncoder.IndexEntry expected = before.encodeDataEntry(row);
            IndexReplicationEncoder.IndexEntry actual = after.encodeDataEntry(row);
            assertThat(actual.targetBucket()).isEqualTo(expected.targetBucket());
            assertThat(actual.key()).isEqualTo(expected.key());
        }
    }

    @Test
    void testExplicitIndexBucketCountMismatchIsRejected() {
        TableDescriptor main = mainDescriptor(2);
        TableDescriptor index =
                IndexTableDescriptorFactory.derive(main, 1L, main.getSchema().getIndexes().get(0));
        TabletServerMetadataCache cache =
                indexCache(
                        TableDescriptor.builder(index)
                                .distributedBy(3, index.getBucketKeys())
                                .build());

        assertThatThrownBy(
                        () ->
                                IndexReplicationEncoder.createAll(
                                        tableInfo(MAIN_PATH, 1L, main),
                                        new TableBucket(1L, 10L, 0),
                                        cache))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("bucket count 2 but found 3");
    }

    private static TableDescriptor mainDescriptor(Integer indexBucketCount) {
        return mainDescriptor(indexBucketCount, Collections.singletonList("name"));
    }

    private static TableDescriptor mainDescriptor(
            Integer indexBucketCount, List<String> indexColumns) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("p", DataTypes.STRING())
                        .primaryKey("id", "p")
                        .index(
                                "idx_name",
                                IndexType.SECONDARY,
                                indexColumns,
                                IndexVisibility.SYNC,
                                indexBucketCount)
                        .build();
        return TableDescriptor.builder()
                .schema(schema)
                .distributedBy(3, "id")
                .partitionedBy("p")
                .build()
                .withReplicationFactor(3);
    }

    private static TableDescriptor withHistory(TableDescriptor descriptor) {
        return TableDescriptor.builder()
                .schema(descriptor.getSchema())
                .distributedBy(3, "id")
                .partitionedBy("p")
                .property(ConfigOptions.TABLE_KV_MVCC_ENABLED, true)
                .build()
                .withReplicationFactor(3);
    }

    private static TabletServerMetadataCache indexCache(TableDescriptor descriptor) {
        TableMetadata metadata =
                new TableMetadata(tableInfo(INDEX_PATH, 2L, descriptor), Collections.emptyList());
        TabletServerMetadataCache cache =
                new TabletServerMetadataCache(null) {
                    @Override
                    public Optional<TableMetadata> getTableMetadata(TablePath tablePath) {
                        return INDEX_PATH.equals(tablePath)
                                ? Optional.of(metadata)
                                : Optional.empty();
                    }
                };
        cache.updateTableMetadata(metadata);
        return cache;
    }

    private static TableInfo tableInfo(TablePath path, long id, TableDescriptor descriptor) {
        return TableInfo.of(path, id, 1, descriptor, "file:///remote", 1L, 1L);
    }
}
