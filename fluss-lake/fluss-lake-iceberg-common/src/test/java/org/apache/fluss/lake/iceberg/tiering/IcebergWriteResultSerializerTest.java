/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.lake.dv.PreparedDeletionVectors;
import org.apache.fluss.lake.dv.index.CompactionIndexPart;
import org.apache.fluss.lake.dv.index.StagedIndexPart;
import org.apache.fluss.lake.iceberg.maintenance.RewriteDataFileResult;
import org.apache.fluss.lake.iceberg.utils.TestingGoldenFiles;
import org.apache.fluss.utils.InstantiationUtils;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DataFileSet;
import org.apache.iceberg.util.DeleteFileSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.lake.iceberg.utils.TestingGoldenFiles.WRITE_RESULT_DIR;
import static org.apache.fluss.lake.iceberg.utils.TestingGoldenFiles.assertWriteResultEquals;
import static org.apache.fluss.lake.iceberg.utils.TestingGoldenFiles.goldenWriteResult;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assumptions.assumeThat;

/** UT for {@link IcebergWriteResultSerializer}. */
class IcebergWriteResultSerializerTest {

    private IcebergWriteResultSerializer serializer;
    private Schema schema;
    private PartitionSpec spec;
    private GenericRecord partitionData;

    @BeforeEach
    void setUp() {
        serializer = new IcebergWriteResultSerializer();
        schema =
                new Schema(
                        Types.NestedField.required(1, "id", Types.IntegerType.get()),
                        Types.NestedField.optional(2, "category", Types.StringType.get()));
        spec = PartitionSpec.builderFor(schema).identity("category").build();
        partitionData = GenericRecord.create(spec.partitionType());
        partitionData.setField("category", "A");
    }

    @Test
    void testSerializeAndDeserializeWithAllFiles() throws IOException {
        // 1. Arrange: Create a complex write result
        DataFile dataFile = createDataFile("/data/file1.parquet", 100L);
        DeleteFile deleteFile = createDeleteFile("/data/deletes1.parquet", 10L);

        // no rewrite result
        WriteResult writeResult =
                WriteResult.builder().addDataFiles(dataFile).addDeleteFiles(deleteFile).build();

        IcebergWriteResult originalResult = new IcebergWriteResult(writeResult, specs(), null);

        // 2. Act: Serialize and then deserialize the object
        byte[] serializedData = serializer.serialize(originalResult);
        IcebergWriteResult deserializedResult =
                serializer.deserialize(serializer.getVersion(), serializedData);
        assertThat(deserializedResult.toString()).isEqualTo(originalResult.toString());

        // with rewrite result
        RewriteDataFileResult rewriteDataFileResult =
                new RewriteDataFileResult(
                        1L,
                        DataFileSet.of(Collections.singletonList(dataFile)),
                        DataFileSet.of(Collections.singletonList(dataFile)));
        originalResult = new IcebergWriteResult(writeResult, specs(), rewriteDataFileResult);
        serializedData = serializer.serialize(originalResult);
        deserializedResult = serializer.deserialize(serializer.getVersion(), serializedData);
        assertThat(deserializedResult.toString()).isEqualTo(originalResult.toString());

        // with a rewrite result that also removes delete files
        rewriteDataFileResult =
                new RewriteDataFileResult(
                        1L,
                        DataFileSet.of(Collections.singletonList(dataFile)),
                        DataFileSet.of(Collections.singletonList(dataFile)),
                        DeleteFileSet.of(Collections.singletonList(deleteFile)),
                        null,
                        null);
        originalResult = new IcebergWriteResult(writeResult, specs(), rewriteDataFileResult);
        deserializedResult =
                serializer.deserialize(
                        serializer.getVersion(), serializer.serialize(originalResult));
        assertThat(deserializedResult.toString()).isEqualTo(originalResult.toString());
        assertThat(deserializedResult.rewriteDataFileResult().deletedDeleteFiles())
                .extracting(DeleteFile::location)
                .containsExactly("/data/deletes1.parquet");

        // with the index part of a compaction of a table with deletion vectors
        CompactionIndexPart part =
                new CompactionIndexPart(
                        7L,
                        1L,
                        Arrays.asList("sst_0.sst", "sst_1.sst"),
                        Arrays.asList(10L, 20L),
                        Collections.singletonMap(5, "file1.parquet"),
                        Arrays.asList("old1.parquet", "old2.parquet"));
        rewriteDataFileResult =
                new RewriteDataFileResult(
                        1L,
                        DataFileSet.of(Collections.singletonList(dataFile)),
                        DataFileSet.of(Collections.singletonList(dataFile)),
                        DeleteFileSet.create(),
                        part,
                        null);
        originalResult = new IcebergWriteResult(writeResult, specs(), rewriteDataFileResult);
        deserializedResult =
                serializer.deserialize(
                        serializer.getVersion(), serializer.serialize(originalResult));
        assertThat(deserializedResult.toString()).isEqualTo(originalResult.toString());
        assertThat(deserializedResult.compacted()).isTrue();
        CompactionIndexPart read = deserializedResult.compactionIndexPart();
        assertThat(read).isNotNull();
        assertThat(read.changeId()).isEqualTo(7L);
        assertThat(read.readSnapshotId()).isEqualTo(1L);
        assertThat(read.sstFileNames()).containsExactly("sst_0.sst", "sst_1.sst");
        assertThat(read.sstFileSizes()).containsExactly(10L, 20L);
        assertThat(read.addedFiles()).containsExactly(entry(5, "file1.parquet"));
        assertThat(read.removedFiles()).containsExactly("old1.parquet", "old2.parquet");
    }

    @Test
    void testSerializeAndDeserializeWithStagedIndexPart() throws IOException {
        IcebergWriteResult withoutPart = goldenWriteResult();
        Map<Integer, String> files = new HashMap<>();
        files.put(0, "data-1.parquet");
        files.put(1, "data-2.parquet");
        StagedIndexPart part =
                new StagedIndexPart(
                        -77L,
                        Arrays.asList("w1_sst_0.sst", "w1_sst_1.sst"),
                        Arrays.asList(100L, 200L),
                        files,
                        30L,
                        4L);
        IcebergWriteResult original =
                new IcebergWriteResult(
                        withoutPart.getWriteResult(),
                        withoutPart.specs(),
                        withoutPart.rewriteDataFileResult(),
                        part);

        IcebergWriteResult deserialized =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(original));

        assertWriteResultEquals(deserialized, withoutPart);
        StagedIndexPart read = deserialized.stagedIndexPart();
        assertThat(read).isNotNull();
        assertThat(read.stagingId()).isEqualTo(-77L);
        assertThat(read.sstFileNames()).containsExactly("w1_sst_0.sst", "w1_sst_1.sst");
        assertThat(read.sstFileSizes()).containsExactly(100L, 200L);
        assertThat(read.files())
                .containsExactly(entry(0, "data-1.parquet"), entry(1, "data-2.parquet"));
        assertThat(read.keys()).isEqualTo(30L);
        assertThat(read.deletes()).isEqualTo(4L);

        // a write result without a part keeps the golden format and reads back without one
        assertThat(
                        serializer
                                .deserialize(
                                        serializer.getVersion(), serializer.serialize(withoutPart))
                                .stagedIndexPart())
                .isNull();
    }

    /**
     * The deletion vectors a writer prepared read back with the round they are of and the deletion
     * vector each data file had before; a write result without them reads back without them.
     */
    @Test
    void testSerializeAndDeserializeWithPreparedDeletionVectors() throws IOException {
        IcebergWriteResult withoutPrepared = goldenWriteResult();
        DeleteFile dv1 = createDeletionVector("dv-1.puffin", "data-1.parquet", 4L, 40L);
        DeleteFile dv2 = createDeletionVector("dv-1.puffin", "data-2.parquet", 44L, 50L);
        Map<String, DeletionVectorId> replaced = new HashMap<>();
        replaced.put("data-1.parquet", new DeletionVectorId("dv-0.puffin", 4L, 30L));
        // the format keeps a replaced delete file without a content offset and size too
        replaced.put("data-2.parquet", new DeletionVectorId("deletes-2.parquet", null, null));
        IcebergWriteResult original =
                withoutPrepared.withPrepared(
                        new PreparedDeletionVectorFiles(
                                new PreparedDeletionVectors(11L, 100L, 200L),
                                Arrays.asList(dv1, dv2),
                                replaced));

        IcebergWriteResult deserialized =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(original));

        assertWriteResultEquals(deserialized, withoutPrepared);
        PreparedDeletionVectorFiles read = deserialized.prepared();
        assertThat(read).isNotNull();
        assertThat(deserialized.preparedDeletionVectors()).isNotNull();
        assertThat(read.round.snapshotId()).isEqualTo(11L);
        assertThat(read.round.startOffset()).isEqualTo(100L);
        assertThat(read.round.endOffset()).isEqualTo(200L);
        assertThat(read.deletionVectors).hasSize(2);
        for (int i = 0; i < 2; i++) {
            DeleteFile expected = i == 0 ? dv1 : dv2;
            DeleteFile actual = read.deletionVectors.get(i);
            assertThat(actual.location()).isEqualTo(expected.location());
            assertThat(actual.format()).isEqualTo(FileFormat.PUFFIN);
            assertThat(actual.referencedDataFile()).isEqualTo(expected.referencedDataFile());
            assertThat(actual.contentOffset()).isEqualTo(expected.contentOffset());
            assertThat(actual.contentSizeInBytes()).isEqualTo(expected.contentSizeInBytes());
            assertThat(DeletionVectorId.of(actual)).isEqualTo(DeletionVectorId.of(expected));
        }
        assertThat(read.replaced).isEqualTo(replaced);

        assertThat(
                        serializer
                                .deserialize(
                                        serializer.getVersion(),
                                        serializer.serialize(withoutPrepared))
                                .prepared())
                .isNull();
    }

    /**
     * Versions 1 and 2 hold the golden write result; version 3 holds it with the staged index part
     * and the prepared deletion vectors of a table with deletion vectors.
     */
    @Test
    void testDeserializeGoldenFiles() throws Exception {
        for (int version = TestingGoldenFiles.firstReadableVersion();
                version <= serializer.getVersion();
                version++) {
            IcebergWriteResult actual =
                    serializer.deserialize(
                            version, TestingGoldenFiles.read(WRITE_RESULT_DIR, version));
            assertWriteResultEquals(actual, goldenWriteResult());
            if (version < 3) {
                assertThat(actual.stagedIndexPart()).isNull();
                assertThat(actual.prepared()).isNull();
            } else {
                assertDeletionVectorsOfGolden(actual);
            }
        }
    }

    @Test
    void testCurrentVersionMatchesGoldenFile() throws Exception {
        // other Iceberg versions write the Iceberg objects in the JSON differently
        assumeThat(TestingGoldenFiles.isGoldenIcebergVersion())
                .as("the golden files were written with another Iceberg version")
                .isTrue();
        // the current format must not change without bumping the version
        assertThat(serializer.serialize(goldenWriteResultWithDeletionVectors()))
                .isEqualTo(TestingGoldenFiles.read(WRITE_RESULT_DIR, serializer.getVersion()));
    }

    @Test
    void testRegenerateGoldenFiles() throws Exception {
        assumeThat(TestingGoldenFiles.shouldRegenerate())
                .as("golden files are only regenerated on request")
                .isTrue();
        TestingGoldenFiles.write(WRITE_RESULT_DIR, 1, serializeV1(goldenWriteResult()));
        TestingGoldenFiles.write(
                WRITE_RESULT_DIR,
                serializer.getVersion(),
                serializer.serialize(goldenWriteResultWithDeletionVectors()));
    }

    /**
     * The golden write result of a table with deletion vectors: with the index part its writer
     * staged and the deletion vectors it prepared.
     */
    private static IcebergWriteResult goldenWriteResultWithDeletionVectors() {
        IcebergWriteResult golden = goldenWriteResult();
        Map<Integer, String> files = new HashMap<>();
        files.put(0, "data-1.parquet");
        files.put(1, "data-2.parquet");
        Map<String, DeletionVectorId> replaced = new HashMap<>();
        replaced.put("data-1.parquet", new DeletionVectorId("dv-0.puffin", 4L, 30L));
        replaced.put("data-2.parquet", new DeletionVectorId("deletes-2.parquet", null, null));
        return new IcebergWriteResult(
                        golden.getWriteResult(),
                        golden.specs(),
                        golden.rewriteDataFileResult(),
                        new StagedIndexPart(
                                -77L,
                                Arrays.asList("w1_sst_0.sst", "w1_sst_1.sst"),
                                Arrays.asList(100L, 200L),
                                files,
                                30L,
                                4L))
                .withPrepared(
                        new PreparedDeletionVectorFiles(
                                new PreparedDeletionVectors(11L, 100L, 200L),
                                Arrays.asList(
                                        TestingGoldenFiles.deletionVector(
                                                "dv-1.puffin", "data-1.parquet", 4L, 40L),
                                        TestingGoldenFiles.deletionVector(
                                                "dv-1.puffin", "data-2.parquet", 44L, 50L)),
                                replaced));
    }

    /** Asserts the staged index part and prepared deletion vectors of the version 3 golden. */
    private static void assertDeletionVectorsOfGolden(IcebergWriteResult actual) {
        IcebergWriteResult expected = goldenWriteResultWithDeletionVectors();
        StagedIndexPart part = actual.stagedIndexPart();
        StagedIndexPart expectedPart = checkNotNull(expected.stagedIndexPart());
        assertThat(part).isNotNull();
        assertThat(part.stagingId()).isEqualTo(expectedPart.stagingId());
        assertThat(part.sstFileNames()).isEqualTo(expectedPart.sstFileNames());
        assertThat(part.sstFileSizes()).isEqualTo(expectedPart.sstFileSizes());
        assertThat(part.files()).isEqualTo(expectedPart.files());
        assertThat(part.keys()).isEqualTo(expectedPart.keys());
        assertThat(part.deletes()).isEqualTo(expectedPart.deletes());

        PreparedDeletionVectorFiles prepared = actual.prepared();
        PreparedDeletionVectorFiles expectedPrepared = checkNotNull(expected.prepared());
        assertThat(prepared).isNotNull();
        assertThat(prepared.round.snapshotId()).isEqualTo(expectedPrepared.round.snapshotId());
        assertThat(prepared.round.startOffset()).isEqualTo(expectedPrepared.round.startOffset());
        assertThat(prepared.round.endOffset()).isEqualTo(expectedPrepared.round.endOffset());
        assertThat(prepared.deletionVectors).hasSameSizeAs(expectedPrepared.deletionVectors);
        for (int i = 0; i < expectedPrepared.deletionVectors.size(); i++) {
            assertThat(DeletionVectorId.of(prepared.deletionVectors.get(i)))
                    .isEqualTo(DeletionVectorId.of(expectedPrepared.deletionVectors.get(i)));
            assertThat(prepared.deletionVectors.get(i).referencedDataFile())
                    .isEqualTo(expectedPrepared.deletionVectors.get(i).referencedDataFile());
        }
        assertThat(prepared.replaced).isEqualTo(expectedPrepared.replaced);
    }

    @Test
    void testSerializeWithoutSpecs() {
        IcebergWriteResult withoutSpecs =
                new IcebergWriteResult(goldenWriteResult().getWriteResult(), null, null);
        assertThatThrownBy(() -> serializer.serialize(withoutSpecs))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("without partition specs");
    }

    @Test
    void testDeserializeInvalidData() throws Exception {
        byte[] serialized = serializer.serialize(goldenWriteResult());
        assertThatThrownBy(() -> serializer.deserialize(2, "{}".getBytes()))
                .isInstanceOf(IOException.class)
                .hasStackTraceContaining("Missing field");
        assertThatThrownBy(() -> serializer.deserialize(2, "invalid".getBytes()))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> serializer.deserialize(4, serialized))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported IcebergWriteResult serializer version 4");
    }

    /** Serializes a write result like version 1 of {@link IcebergWriteResultSerializer} did. */
    private static byte[] serializeV1(IcebergWriteResult writeResult) throws IOException {
        byte[] writeResultBytes = InstantiationUtils.serializeObject(writeResult.getWriteResult());
        byte[] rewriteResultBytes =
                InstantiationUtils.serializeObject(writeResult.rewriteDataFileResult());
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeInt(writeResultBytes.length);
            dos.write(writeResultBytes);
            dos.writeBoolean(true);
            dos.writeInt(rewriteResultBytes.length);
            dos.write(rewriteResultBytes);
            dos.flush();
            return baos.toByteArray();
        }
    }

    private Map<Integer, PartitionSpec> specs() {
        return Collections.singletonMap(spec.specId(), spec);
    }

    private DeleteFile createDeleteFile(String path, long recordCount) {
        return FileMetadata.deleteFileBuilder(spec)
                .withPath(path)
                .withFileSizeInBytes(1024)
                .withPartition(partitionData)
                .withRecordCount(recordCount)
                .withFormat(FileFormat.PARQUET)
                .ofPositionDeletes()
                .build();
    }

    private DeleteFile createDeletionVector(
            String path, String dataFile, long contentOffset, long contentSize) {
        return FileMetadata.deleteFileBuilder(spec)
                .withPath(path)
                .withFileSizeInBytes(1024)
                .withPartition(partitionData)
                .withRecordCount(3)
                .withFormat(FileFormat.PUFFIN)
                .ofPositionDeletes()
                .withReferencedDataFile(dataFile)
                .withContentOffset(contentOffset)
                .withContentSizeInBytes(contentSize)
                .build();
    }

    private DataFile createDataFile(String path, long recordCount) {
        return DataFiles.builder(spec)
                .withPath(path)
                .withFileSizeInBytes(4096)
                .withPartition(partitionData)
                .withRecordCount(recordCount)
                .withFormat(FileFormat.PARQUET)
                .build();
    }
}
