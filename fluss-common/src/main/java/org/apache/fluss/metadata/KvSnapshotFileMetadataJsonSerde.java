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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Version-1 JSON serde for standard KV snapshot file metadata. */
@Internal
public final class KvSnapshotFileMetadataJsonSerde
        implements JsonSerializer<KvSnapshotFileMetadata>,
                JsonDeserializer<KvSnapshotFileMetadata> {

    public static final KvSnapshotFileMetadataJsonSerde INSTANCE =
            new KvSnapshotFileMetadataJsonSerde();

    private static final int VERSION = 1;
    private static final String VERSION_KEY = "version";
    private static final String TABLE_ID = "table_id";
    private static final String PARTITION_ID = "partition_id";
    private static final String BUCKET_ID = "bucket_id";
    private static final String SNAPSHOT_ID = "snapshot_id";
    private static final String SNAPSHOT_LOCATION = "snapshot_location";
    private static final String KV_SNAPSHOT_HANDLE = "kv_snapshot_handle";
    private static final String KV_SHARED_FILES_HANDLE = "shared_file_handles";
    private static final String KV_PRIVATE_FILES_HANDLE = "private_file_handles";
    private static final String KV_FILE_HANDLE = "kv_file_handle";
    private static final String KV_FILE_PATH = "path";
    private static final String KV_FILE_SIZE = "size";
    private static final String KV_FILE_SHA256 = "sha256";
    private static final String KV_FILE_LOCAL_PATH = "local_path";
    private static final String SNAPSHOT_INCREMENTAL_SIZE = "snapshot_incremental_size";
    private static final String LOG_OFFSET = "log_offset";
    private static final String ROW_COUNT = "row_count";
    private static final String AUTO_INC_ID_RANGE = "auto_inc_id_range";
    private static final String AUTO_INC_COLUMN_ID = "column_id";
    private static final String AUTO_INC_ID_START = "start";
    private static final String AUTO_INC_ID_END = "end";
    // The change list of derived deletion vectors: optional, with its own version. Readers that do
    // not know it ignore it, as they read named fields only, so the metadata stays at version 1.
    private static final String DV_CHANGE_LIST = "dv_change_list";
    private static final String DV_VERSION = "version";
    private static final String DV_FROM = "from";
    private static final String DV_END = "end";
    private static final String DV_VALID_FROM = "valid_from";
    private static final String DV_LAKE_SNAPSHOT_ID = "lake_snapshot_id";
    private static final String DV_ENTRIES = "entries";
    private static final String DV_SEGMENTS = "segments";

    private KvSnapshotFileMetadataJsonSerde() {}

    @Override
    public void serialize(KvSnapshotFileMetadata metadata, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION);

        TableBucket tableBucket = metadata.getTableBucket();
        generator.writeNumberField(TABLE_ID, tableBucket.getTableId());
        if (tableBucket.getPartitionId() != null) {
            generator.writeNumberField(PARTITION_ID, tableBucket.getPartitionId());
        }
        generator.writeNumberField(BUCKET_ID, tableBucket.getBucket());
        generator.writeNumberField(SNAPSHOT_ID, metadata.getSnapshotId());
        generator.writeStringField(SNAPSHOT_LOCATION, metadata.getSnapshotLocation());

        generator.writeObjectFieldStart(KV_SNAPSHOT_HANDLE);
        generator.writeArrayFieldStart(KV_SHARED_FILES_HANDLE);
        serializeFileHandles(generator, metadata.getSharedFiles());
        generator.writeEndArray();
        generator.writeArrayFieldStart(KV_PRIVATE_FILES_HANDLE);
        serializeFileHandles(generator, metadata.getPrivateFiles());
        generator.writeEndArray();
        generator.writeNumberField(SNAPSHOT_INCREMENTAL_SIZE, metadata.getIncrementalSize());
        generator.writeEndObject();

        generator.writeNumberField(LOG_OFFSET, metadata.getLogOffset());
        if (metadata.getRowCount() != null) {
            generator.writeNumberField(ROW_COUNT, metadata.getRowCount());
        }
        if (metadata.getAutoIncrementRanges() != null
                && !metadata.getAutoIncrementRanges().isEmpty()) {
            generator.writeArrayFieldStart(AUTO_INC_ID_RANGE);
            for (KvSnapshotFileMetadata.AutoIncrementRange range :
                    metadata.getAutoIncrementRanges()) {
                generator.writeStartObject();
                generator.writeNumberField(AUTO_INC_COLUMN_ID, range.getColumnId());
                generator.writeNumberField(AUTO_INC_ID_START, range.getStart());
                generator.writeNumberField(AUTO_INC_ID_END, range.getEnd());
                generator.writeEndObject();
            }
            generator.writeEndArray();
        }
        KvSnapshotFileMetadata.DvChangeList dvChangeList = metadata.getDvChangeList();
        if (dvChangeList != null && dvChangeList.isSupported()) {
            serializeDvChangeList(generator, dvChangeList);
        }
        generator.writeEndObject();
    }

    @Override
    public KvSnapshotFileMetadata deserialize(JsonNode node) {
        JsonNode versionNode = node.get(VERSION_KEY);
        checkArgument(
                versionNode != null
                        && versionNode.isIntegralNumber()
                        && versionNode.canConvertToInt()
                        && versionNode.intValue() == VERSION,
                "Unsupported KV snapshot metadata version.");
        JsonNode partitionIdNode = node.get(PARTITION_ID);
        TableBucket tableBucket =
                new TableBucket(
                        node.get(TABLE_ID).asLong(),
                        partitionIdNode == null ? null : partitionIdNode.asLong(),
                        node.get(BUCKET_ID).asInt());
        JsonNode snapshotHandle = node.get(KV_SNAPSHOT_HANDLE);

        Long rowCount = node.has(ROW_COUNT) ? node.get(ROW_COUNT).asLong() : null;
        List<KvSnapshotFileMetadata.AutoIncrementRange> ranges = null;
        if (node.has(AUTO_INC_ID_RANGE)) {
            ranges = new ArrayList<>();
            for (JsonNode range : node.get(AUTO_INC_ID_RANGE)) {
                ranges.add(
                        new KvSnapshotFileMetadata.AutoIncrementRange(
                                range.get(AUTO_INC_COLUMN_ID).asInt(),
                                range.get(AUTO_INC_ID_START).asLong(),
                                range.get(AUTO_INC_ID_END).asLong()));
            }
        }

        return new KvSnapshotFileMetadata(
                tableBucket,
                node.get(SNAPSHOT_ID).asLong(),
                node.get(SNAPSHOT_LOCATION).asText(),
                deserializeFileHandles(snapshotHandle, KV_SHARED_FILES_HANDLE),
                deserializeFileHandles(snapshotHandle, KV_PRIVATE_FILES_HANDLE),
                snapshotHandle.get(SNAPSHOT_INCREMENTAL_SIZE).asLong(),
                node.get(LOG_OFFSET).asLong(),
                rowCount,
                ranges,
                node.has(DV_CHANGE_LIST)
                        ? deserializeDvChangeList(node.get(DV_CHANGE_LIST))
                        : null);
    }

    /** Serializes standard KV snapshot file metadata to JSON bytes. */
    public static byte[] toJson(KvSnapshotFileMetadata metadata) {
        return JsonSerdeUtils.writeValueAsBytes(metadata, INSTANCE);
    }

    /** Deserializes standard KV snapshot file metadata from JSON bytes. */
    public static KvSnapshotFileMetadata fromJson(byte[] json) {
        return JsonSerdeUtils.readValue(json, INSTANCE);
    }

    private static void serializeFileHandles(
            JsonGenerator generator, List<KvSnapshotFileMetadata.FileHandle> fileHandles)
            throws IOException {
        for (KvSnapshotFileMetadata.FileHandle fileHandle : fileHandles) {
            generator.writeStartObject();
            serializeFileHandleFields(generator, fileHandle);
            generator.writeEndObject();
        }
    }

    private static void serializeFileHandleFields(
            JsonGenerator generator, KvSnapshotFileMetadata.FileHandle fileHandle)
            throws IOException {
        generator.writeObjectFieldStart(KV_FILE_HANDLE);
        generator.writeStringField(KV_FILE_PATH, fileHandle.getPath());
        generator.writeNumberField(KV_FILE_SIZE, fileHandle.getSize());
        if (fileHandle.getSha256() != null) {
            generator.writeStringField(KV_FILE_SHA256, fileHandle.getSha256());
        }
        generator.writeEndObject();
        generator.writeStringField(KV_FILE_LOCAL_PATH, fileHandle.getLocalPath());
    }

    private static void serializeDvChangeList(
            JsonGenerator generator, KvSnapshotFileMetadata.DvChangeList dvChangeList)
            throws IOException {
        generator.writeObjectFieldStart(DV_CHANGE_LIST);
        generator.writeNumberField(DV_VERSION, dvChangeList.getVersion());
        generator.writeNumberField(DV_FROM, dvChangeList.getFrom());
        generator.writeNumberField(DV_END, dvChangeList.getEnd());
        generator.writeNumberField(DV_VALID_FROM, dvChangeList.getValidFrom());
        generator.writeNumberField(DV_LAKE_SNAPSHOT_ID, dvChangeList.getLakeSnapshotId());
        generator.writeNumberField(DV_ENTRIES, dvChangeList.getEntries());
        generator.writeArrayFieldStart(DV_SEGMENTS);
        for (KvSnapshotFileMetadata.DvChangeListSegment segment : dvChangeList.getSegments()) {
            generator.writeStartObject();
            serializeFileHandleFields(generator, segment.getFile());
            generator.writeNumberField(DV_FROM, segment.getFrom());
            generator.writeNumberField(DV_END, segment.getEnd());
            generator.writeNumberField(DV_ENTRIES, segment.getEntries());
            generator.writeEndObject();
        }
        generator.writeEndArray();
        generator.writeEndObject();
    }

    /**
     * Reads the change list of derived deletion vectors. One of a version this build does not know
     * is kept as unsupported, without its files: the snapshot stays readable.
     */
    private static KvSnapshotFileMetadata.DvChangeList deserializeDvChangeList(JsonNode node) {
        JsonNode versionNode = node.get(DV_VERSION);
        int version =
                versionNode != null
                                && versionNode.isIntegralNumber()
                                && versionNode.canConvertToInt()
                        ? versionNode.intValue()
                        : -1;
        if (version != KvSnapshotFileMetadata.DvChangeList.VERSION) {
            return KvSnapshotFileMetadata.DvChangeList.unsupported(version);
        }
        List<KvSnapshotFileMetadata.DvChangeListSegment> segments = new ArrayList<>();
        for (JsonNode segmentNode : node.get(DV_SEGMENTS)) {
            segments.add(
                    new KvSnapshotFileMetadata.DvChangeListSegment(
                            deserializeFileHandle(segmentNode),
                            segmentNode.get(DV_FROM).asLong(),
                            segmentNode.get(DV_END).asLong(),
                            segmentNode.get(DV_ENTRIES).asLong()));
        }
        return new KvSnapshotFileMetadata.DvChangeList(
                node.get(DV_FROM).asLong(),
                node.get(DV_END).asLong(),
                node.get(DV_VALID_FROM).asLong(),
                node.get(DV_LAKE_SNAPSHOT_ID).asLong(),
                node.get(DV_ENTRIES).asLong(),
                segments);
    }

    private static List<KvSnapshotFileMetadata.FileHandle> deserializeFileHandles(
            JsonNode snapshotHandle, String fieldName) {
        List<KvSnapshotFileMetadata.FileHandle> fileHandles = new ArrayList<>();
        for (JsonNode fileNode : snapshotHandle.get(fieldName)) {
            fileHandles.add(deserializeFileHandle(fileNode));
        }
        return fileHandles;
    }

    private static KvSnapshotFileMetadata.FileHandle deserializeFileHandle(JsonNode fileNode) {
        JsonNode handleNode = fileNode.get(KV_FILE_HANDLE);
        return new KvSnapshotFileMetadata.FileHandle(
                handleNode.get(KV_FILE_PATH).asText(),
                handleNode.get(KV_FILE_SIZE).asLong(),
                fileNode.get(KV_FILE_LOCAL_PATH).asText(),
                handleNode.has(KV_FILE_SHA256) ? handleNode.get(KV_FILE_SHA256).asText() : null);
    }
}
