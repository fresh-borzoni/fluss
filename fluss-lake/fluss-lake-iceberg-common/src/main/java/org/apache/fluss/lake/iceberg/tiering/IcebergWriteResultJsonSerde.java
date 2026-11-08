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

import org.apache.fluss.lake.dv.index.KeyPosition;
import org.apache.fluss.lake.dv.index.PositionIndexDelta;
import org.apache.fluss.lake.dv.index.StagedIndexPart;
import org.apache.fluss.lake.iceberg.maintenance.RewriteDataFileResult;
import org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.WriteResult;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.getArrayField;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.getField;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.readDataFiles;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.readDeleteFiles;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.readRewriteDataFileResult;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.readSpecs;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.writeContentFiles;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.writeRewriteDataFileResult;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.writeSpecs;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Json serializer and deserializer for {@link IcebergWriteResult}, used by versions 2 and 3 of
 * {@link IcebergWriteResultSerializer}.
 *
 * <p>The serialized format includes:
 *
 * <ul>
 *   <li>"specs": the partition specs of the table
 *   <li>"data_files": the data files of the {@link WriteResult}
 *   <li>"delete_files": the delete files of the {@link WriteResult}
 *   <li>"referenced_data_files": array of the locations of the data files referenced by the delete
 *       files
 *   <li>"rewritten_delete_files": the delete files rewritten by the {@link WriteResult}
 *   <li>"rewrite_result": the {@link RewriteDataFileResult}, or null if there is none
 *   <li>"position_index_delta": optional, the {@link PositionIndexDelta} of a table with derived
 *       deletion vectors, as {@code {"changed_keys": [...], "puts": [{"key": ..., "file": ...,
 *       "pos": ...}, ...], "deletes": [...]}} with keys in base64. Absent if there is none; only
 *       version 2 documents have one.
 *   <li>"staged_index_part": optional, the {@link StagedIndexPart} of a table with derived deletion
 *       vectors, as {@code {"staging_id": ..., "ssts": [{"name": ..., "size": ...}, ...], "files":
 *       [{"id": ..., "name": ...}, ...], "keys": ..., "deletes": ...}}. Absent if there is none;
 *       only version 3 documents have one.
 * </ul>
 *
 * <p>Specs, files and rewrite results are written as described in {@link IcebergJsonSerdeUtils}.
 */
public class IcebergWriteResultJsonSerde
        implements JsonSerializer<IcebergWriteResult>, JsonDeserializer<IcebergWriteResult> {

    public static final IcebergWriteResultJsonSerde INSTANCE = new IcebergWriteResultJsonSerde();

    private static final String DATA_FILES_FIELD = "data_files";
    private static final String DELETE_FILES_FIELD = "delete_files";
    private static final String REFERENCED_DATA_FILES_FIELD = "referenced_data_files";
    private static final String REWRITTEN_DELETE_FILES_FIELD = "rewritten_delete_files";
    private static final String REWRITE_RESULT_FIELD = "rewrite_result";
    private static final String POSITION_INDEX_DELTA_FIELD = "position_index_delta";
    private static final String CHANGED_KEYS_FIELD = "changed_keys";
    private static final String PUTS_FIELD = "puts";
    private static final String DELETES_FIELD = "deletes";
    private static final String KEY_FIELD = "key";
    private static final String FILE_FIELD = "file";
    private static final String POS_FIELD = "pos";
    private static final String STAGED_INDEX_PART_FIELD = "staged_index_part";
    private static final String STAGING_ID_FIELD = "staging_id";
    private static final String SSTS_FIELD = "ssts";
    private static final String NAME_FIELD = "name";
    private static final String SIZE_FIELD = "size";
    private static final String FILES_FIELD = "files";
    private static final String ID_FIELD = "id";
    private static final String KEYS_FIELD = "keys";
    private static final String DELETES_COUNT_FIELD = "deletes";

    @Override
    public void serialize(IcebergWriteResult icebergWriteResult, JsonGenerator generator)
            throws IOException {
        Map<Integer, PartitionSpec> specs = icebergWriteResult.specs();
        if (specs == null) {
            throw new IOException(
                    "Cannot serialize an IcebergWriteResult without partition specs, "
                            + "it was restored from serializer version 1.");
        }
        WriteResult writeResult = icebergWriteResult.getWriteResult();

        generator.writeStartObject();
        writeSpecs(generator, specs);
        writeContentFiles(
                generator, DATA_FILES_FIELD, Arrays.asList(writeResult.dataFiles()), specs);
        writeContentFiles(
                generator, DELETE_FILES_FIELD, Arrays.asList(writeResult.deleteFiles()), specs);
        generator.writeArrayFieldStart(REFERENCED_DATA_FILES_FIELD);
        for (CharSequence referencedDataFile : writeResult.referencedDataFiles()) {
            generator.writeString(referencedDataFile.toString());
        }
        generator.writeEndArray();
        writeContentFiles(
                generator,
                REWRITTEN_DELETE_FILES_FIELD,
                Arrays.asList(writeResult.rewrittenDeleteFiles()),
                specs);

        RewriteDataFileResult rewriteDataFileResult = icebergWriteResult.rewriteDataFileResult();
        if (rewriteDataFileResult == null) {
            generator.writeNullField(REWRITE_RESULT_FIELD);
        } else {
            generator.writeFieldName(REWRITE_RESULT_FIELD);
            writeRewriteDataFileResult(generator, rewriteDataFileResult, specs);
        }

        // Written only when present, so the documents of tables without derived deletion vectors
        // carry neither field.
        PositionIndexDelta positionIndexDelta = icebergWriteResult.positionIndexDelta();
        if (positionIndexDelta != null) {
            generator.writeFieldName(POSITION_INDEX_DELTA_FIELD);
            writePositionIndexDelta(generator, positionIndexDelta);
        }
        StagedIndexPart stagedIndexPart = icebergWriteResult.stagedIndexPart();
        if (stagedIndexPart != null) {
            generator.writeFieldName(STAGED_INDEX_PART_FIELD);
            writeStagedIndexPart(generator, stagedIndexPart);
        }
        generator.writeEndObject();
    }

    @Override
    public IcebergWriteResult deserialize(JsonNode node) {
        Map<Integer, PartitionSpec> specs = readSpecs(node);

        List<CharSequence> referencedDataFiles = new ArrayList<>();
        for (JsonNode locationNode : getArrayField(node, REFERENCED_DATA_FILES_FIELD)) {
            checkArgument(
                    locationNode.isTextual(),
                    "Field %s must contain strings.",
                    REFERENCED_DATA_FILES_FIELD);
            referencedDataFiles.add(locationNode.asText());
        }
        WriteResult writeResult =
                WriteResult.builder()
                        .addDataFiles(readDataFiles(node, DATA_FILES_FIELD, specs))
                        .addDeleteFiles(readDeleteFiles(node, DELETE_FILES_FIELD, specs))
                        .addReferencedDataFiles(referencedDataFiles)
                        .addRewrittenDeleteFiles(
                                readDeleteFiles(node, REWRITTEN_DELETE_FILES_FIELD, specs))
                        .build();

        JsonNode rewriteNode = getField(node, REWRITE_RESULT_FIELD);
        RewriteDataFileResult rewriteDataFileResult =
                rewriteNode.isNull() ? null : readRewriteDataFileResult(rewriteNode, specs);

        JsonNode deltaNode = node.get(POSITION_INDEX_DELTA_FIELD);
        PositionIndexDelta positionIndexDelta =
                deltaNode == null || deltaNode.isNull() ? null : readPositionIndexDelta(deltaNode);
        JsonNode partNode = node.get(STAGED_INDEX_PART_FIELD);
        StagedIndexPart stagedIndexPart =
                partNode == null || partNode.isNull() ? null : readStagedIndexPart(partNode);
        return new IcebergWriteResult(
                writeResult, specs, rewriteDataFileResult, stagedIndexPart, positionIndexDelta);
    }

    private static void writeStagedIndexPart(JsonGenerator generator, StagedIndexPart part)
            throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(STAGING_ID_FIELD, part.stagingId());
        generator.writeArrayFieldStart(SSTS_FIELD);
        for (int i = 0; i < part.sstFileNames().size(); i++) {
            generator.writeStartObject();
            generator.writeStringField(NAME_FIELD, part.sstFileNames().get(i));
            generator.writeNumberField(SIZE_FIELD, part.sstFileSizes().get(i));
            generator.writeEndObject();
        }
        generator.writeEndArray();
        generator.writeArrayFieldStart(FILES_FIELD);
        for (Map.Entry<Integer, String> f : new TreeMap<>(part.files()).entrySet()) {
            generator.writeStartObject();
            generator.writeNumberField(ID_FIELD, f.getKey());
            generator.writeStringField(NAME_FIELD, f.getValue());
            generator.writeEndObject();
        }
        generator.writeEndArray();
        generator.writeNumberField(KEYS_FIELD, part.keys());
        generator.writeNumberField(DELETES_COUNT_FIELD, part.deletes());
        generator.writeEndObject();
    }

    private static StagedIndexPart readStagedIndexPart(JsonNode node) {
        List<String> names = new ArrayList<>();
        List<Long> sizes = new ArrayList<>();
        for (JsonNode sst : getArrayField(node, SSTS_FIELD)) {
            names.add(readText(sst, NAME_FIELD));
            sizes.add(readLong(sst, SIZE_FIELD));
        }
        Map<Integer, String> files = new HashMap<>();
        for (JsonNode file : getArrayField(node, FILES_FIELD)) {
            JsonNode id = getField(file, ID_FIELD);
            checkArgument(id.canConvertToInt(), "Field %s must be an int.", ID_FIELD);
            files.put(id.asInt(), readText(file, NAME_FIELD));
        }
        return new StagedIndexPart(
                readLong(node, STAGING_ID_FIELD),
                names,
                sizes,
                files,
                readLong(node, KEYS_FIELD),
                readLong(node, DELETES_COUNT_FIELD));
    }

    private static String readText(JsonNode node, String field) {
        JsonNode value = getField(node, field);
        checkArgument(value.isTextual(), "Field %s must be a string.", field);
        return value.asText();
    }

    private static long readLong(JsonNode node, String field) {
        JsonNode value = getField(node, field);
        checkArgument(value.canConvertToLong(), "Field %s must be a long.", field);
        return value.asLong();
    }

    private static void writePositionIndexDelta(JsonGenerator generator, PositionIndexDelta delta)
            throws IOException {
        generator.writeStartObject();
        writeKeys(generator, CHANGED_KEYS_FIELD, delta.changedKeys());
        generator.writeArrayFieldStart(PUTS_FIELD);
        for (KeyPosition put : delta.puts()) {
            generator.writeStartObject();
            generator.writeBinaryField(KEY_FIELD, put.key());
            generator.writeStringField(FILE_FIELD, put.file());
            generator.writeNumberField(POS_FIELD, put.pos());
            generator.writeEndObject();
        }
        generator.writeEndArray();
        writeKeys(generator, DELETES_FIELD, delta.deletes());
        generator.writeEndObject();
    }

    private static void writeKeys(JsonGenerator generator, String field, List<byte[]> keys)
            throws IOException {
        generator.writeArrayFieldStart(field);
        for (byte[] key : keys) {
            generator.writeBinary(key);
        }
        generator.writeEndArray();
    }

    private static PositionIndexDelta readPositionIndexDelta(JsonNode node) {
        List<KeyPosition> puts = new ArrayList<>();
        for (JsonNode putNode : getArrayField(node, PUTS_FIELD)) {
            JsonNode fileNode = getField(putNode, FILE_FIELD);
            checkArgument(fileNode.isTextual(), "Field %s must be a string.", FILE_FIELD);
            JsonNode posNode = getField(putNode, POS_FIELD);
            checkArgument(posNode.canConvertToLong(), "Field %s must be a long.", POS_FIELD);
            puts.add(
                    new KeyPosition(
                            readKey(getField(putNode, KEY_FIELD), KEY_FIELD),
                            fileNode.asText(),
                            posNode.asLong()));
        }
        return new PositionIndexDelta(
                readKeys(node, CHANGED_KEYS_FIELD), puts, readKeys(node, DELETES_FIELD));
    }

    private static List<byte[]> readKeys(JsonNode node, String field) {
        List<byte[]> keys = new ArrayList<>();
        for (JsonNode keyNode : getArrayField(node, field)) {
            keys.add(readKey(keyNode, field));
        }
        return keys;
    }

    private static byte[] readKey(JsonNode keyNode, String field) {
        checkArgument(keyNode.isTextual(), "Field %s must contain base64 strings.", field);
        try {
            return keyNode.binaryValue();
        } catch (IOException e) {
            throw new UncheckedIOException(
                    String.format("Field %s must contain base64 strings.", field), e);
        }
    }
}
