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

package org.apache.fluss.lake.dv;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * The manifest of one position index change: the SST files of each bucket, keyed by primary key,
 * with the file ids the change adds and the data files it removes ({@code rowpos.manifest}).
 *
 * <p>Besides its own version, the manifest records the version of the {@link FilePos} encoding of
 * the values in its SSTs. Manifests written before that field existed hold version 1.
 */
@Internal
public class RowPosSstIndex {

    private static final int VERSION = 1;
    /** File name of a manifest. */
    public static final String MANIFEST_FILE = "rowpos.manifest";

    private static final String VERSION_KEY = "version";
    private static final String SST_VALUE_VERSION_KEY = "sstValueVersion";
    private static final String BUCKETS_KEY = "buckets";
    private static final String SST_FILES_KEY = "rowPosSstFiles";
    private static final String FILE_ID_2_NAME_KEY = "fileId2Name";
    private static final String FILE_ID_KEY = "fileId";
    private static final String REPLACED_FILES_KEY = "replacedFiles";
    private static final String NAME_KEY = "name";
    private static final String SIZE_KEY = "size";

    private static final RowPosSstIndexJsonSerde JSON_SERDE =
            new RowPosSstIndexJsonSerde("a position index manifest");

    private final Map<Integer, List<SstFileEntry>> bucketFiles;
    private final Map<Integer, Map<Integer, String>> fileId2Name;
    private final Map<Integer, List<String>> replacedFiles;

    /** Creates a manifest from per-bucket SSTs, new file ids and replaced files. */
    public static RowPosSstIndex manifest(
            Map<Integer, List<SstFileEntry>> bucketFiles,
            Map<Integer, Map<Integer, String>> fileId2Name,
            Map<Integer, List<String>> replacedFiles) {
        return new RowPosSstIndex(bucketFiles, fileId2Name, replacedFiles);
    }

    private RowPosSstIndex(
            Map<Integer, List<SstFileEntry>> bucketFiles,
            Map<Integer, Map<Integer, String>> fileId2Name,
            Map<Integer, List<String>> replacedFiles) {
        this.bucketFiles = bucketFiles;
        this.fileId2Name = fileId2Name;
        this.replacedFiles = replacedFiles;
    }

    /** Returns the file ids a bucket adds, with their file names. */
    public Map<Integer, String> getFileId2Name(int bucketId) {
        Map<Integer, String> m = fileId2Name.get(bucketId);
        return m == null ? Collections.<Integer, String>emptyMap() : m;
    }

    /** Returns the data files removed from a bucket's index. */
    public List<String> getReplacedFiles(int bucketId) {
        List<String> l = replacedFiles.get(bucketId);
        return l == null ? Collections.<String>emptyList() : l;
    }

    /** Returns the buckets the manifest lists: those with SSTs, added file ids or removed files. */
    public Set<Integer> getBucketIds() {
        Set<Integer> ids = new TreeSet<>(bucketFiles.keySet());
        ids.addAll(fileId2Name.keySet());
        ids.addAll(replacedFiles.keySet());
        return Collections.unmodifiableSet(ids);
    }

    /** Returns the SST files of the given bucket, or an empty list if it has none. */
    public List<SstFileEntry> getFiles(int bucketId) {
        List<SstFileEntry> files = bucketFiles.get(bucketId);
        return files == null ? Collections.<SstFileEntry>emptyList() : files;
    }

    /** Serializes this index to JSON bytes. */
    public byte[] toJsonBytes() {
        return JsonSerdeUtils.writeValueAsBytes(this, JSON_SERDE);
    }

    /** Deserializes an index from JSON bytes. */
    public static RowPosSstIndex fromJsonBytes(byte[] bytes) {
        return JsonSerdeUtils.readValue(bytes, JSON_SERDE);
    }

    /** Deserializes an index from the JSON bytes of {@code source}, which errors name. */
    public static RowPosSstIndex fromJsonBytes(byte[] bytes, Object source) {
        return JsonSerdeUtils.readValue(bytes, new RowPosSstIndexJsonSerde(source));
    }

    /** An SST file entry within the index. */
    public static class SstFileEntry {
        private final String fileName;
        private final long fileSize;

        public SstFileEntry(String fileName, long fileSize) {
            this.fileName = fileName;
            this.fileSize = fileSize;
        }

        public String getFileName() {
            return fileName;
        }

        public long getFileSize() {
            return fileSize;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SstFileEntry that = (SstFileEntry) o;
            return fileSize == that.fileSize && Objects.equals(fileName, that.fileName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fileName, fileSize);
        }

        @Override
        public String toString() {
            return "SstFileEntry{name='" + fileName + "', size=" + fileSize + '}';
        }
    }

    // ---- JSON serialization / deserialization ----

    private static class RowPosSstIndexJsonSerde
            implements JsonSerializer<RowPosSstIndex>, JsonDeserializer<RowPosSstIndex> {

        // the manifest being read, for errors
        private final Object source;

        RowPosSstIndexJsonSerde(Object source) {
            this.source = source;
        }

        @Override
        public void serialize(RowPosSstIndex index, JsonGenerator generator) throws IOException {
            generator.writeStartObject();
            generator.writeNumberField(VERSION_KEY, VERSION);
            generator.writeNumberField(SST_VALUE_VERSION_KEY, FilePos.ENCODING_VERSION);

            generator.writeObjectFieldStart(BUCKETS_KEY);
            for (int bucket : index.getBucketIds()) {
                generator.writeObjectFieldStart(String.valueOf(bucket));
                generator.writeArrayFieldStart(SST_FILES_KEY);
                for (SstFileEntry file : index.getFiles(bucket)) {
                    generator.writeStartObject();
                    generator.writeStringField(NAME_KEY, file.getFileName());
                    generator.writeNumberField(SIZE_KEY, file.getFileSize());
                    generator.writeEndObject();
                }
                generator.writeEndArray();
                generator.writeArrayFieldStart(FILE_ID_2_NAME_KEY);
                for (Map.Entry<Integer, String> e : index.getFileId2Name(bucket).entrySet()) {
                    generator.writeStartObject();
                    generator.writeNumberField(FILE_ID_KEY, e.getKey());
                    generator.writeStringField(NAME_KEY, e.getValue());
                    generator.writeEndObject();
                }
                generator.writeEndArray();
                generator.writeArrayFieldStart(REPLACED_FILES_KEY);
                for (String f : index.getReplacedFiles(bucket)) {
                    generator.writeString(f);
                }
                generator.writeEndArray();
                generator.writeEndObject();
            }
            generator.writeEndObject();

            generator.writeEndObject();
        }

        @Override
        public RowPosSstIndex deserialize(JsonNode node) {
            Map<Integer, List<SstFileEntry>> bucketFiles = new HashMap<>();
            Map<Integer, Map<Integer, String>> fileId2Name = new HashMap<>();
            Map<Integer, List<String>> replaced = new HashMap<>();
            int version = node.has(VERSION_KEY) ? node.get(VERSION_KEY).asInt() : -1;
            FormatVersions.check(source, version, VERSION);
            // The SSTs are ingested as they are, so their values must be in an encoding this
            // release decodes.
            int sstValueVersion =
                    node.has(SST_VALUE_VERSION_KEY) ? node.get(SST_VALUE_VERSION_KEY).asInt() : 1;
            FormatVersions.check(
                    "the SST values of " + source, sstValueVersion, FilePos.ENCODING_VERSION);
            JsonNode bucketsNode = node.get(BUCKETS_KEY);
            if (bucketsNode != null) {
                Iterator<String> fieldNames = bucketsNode.fieldNames();
                while (fieldNames.hasNext()) {
                    String bucketIdStr = fieldNames.next();
                    int bucketId = Integer.parseInt(bucketIdStr);
                    JsonNode bucketNode = bucketsNode.get(bucketIdStr);
                    JsonNode filesNode = bucketNode.get(SST_FILES_KEY);
                    List<SstFileEntry> files = new ArrayList<>();
                    if (filesNode != null) {
                        for (JsonNode fileNode : filesNode) {
                            String name = fileNode.get(NAME_KEY).asText();
                            long size = fileNode.get(SIZE_KEY).asLong();
                            files.add(new SstFileEntry(name, size));
                        }
                    }
                    bucketFiles.put(bucketId, files);
                    JsonNode dictNode = bucketNode.get(FILE_ID_2_NAME_KEY);
                    if (dictNode != null) {
                        Map<Integer, String> dict = new HashMap<>();
                        for (JsonNode e : dictNode) {
                            dict.put(e.get(FILE_ID_KEY).asInt(), e.get(NAME_KEY).asText());
                        }
                        fileId2Name.put(bucketId, dict);
                    }
                    JsonNode replacedNode = bucketNode.get(REPLACED_FILES_KEY);
                    if (replacedNode != null) {
                        List<String> l = new ArrayList<>();
                        for (JsonNode e : replacedNode) {
                            l.add(e.asText());
                        }
                        replaced.put(bucketId, l);
                    }
                }
            }
            return new RowPosSstIndex(bucketFiles, fileId2Name, replaced);
        }
    }
}
