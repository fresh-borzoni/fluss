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

package org.apache.fluss.lake.source;

import org.apache.fluss.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/**
 * The result of re-indexing the rewritten files of one bucket: the new fileId to fileName mappings
 * and the metadata of the index SST files written.
 *
 * @since 0.8
 */
@PublicEvolving
public class RowPosResult {

    /** The change id of a result that names none. */
    public static final long UNKNOWN_CHANGE_ID = -1L;

    /** The index change the result's SSTs are written under. */
    private final long changeId;

    /** The re-index split the result is of. A bucket's files may span several splits. */
    private final int split;

    private final int bucketId;

    /** Partition ID for partitioned tables, null for non-partitioned tables. */
    @Nullable private final Long partitionId;

    /** Partition name for partitioned tables, null for non-partitioned tables. */
    @Nullable private final String partitionName;

    /** New fileId to fileName mappings for the re-indexed files. */
    private final Map<Integer, String> newFileDictEntries;

    /** Metadata of the written SST files, mapping primary keys to file positions. */
    private final List<SstMeta> sstMetas;

    public RowPosResult(
            int bucketId,
            @Nullable Long partitionId,
            @Nullable String partitionName,
            Map<Integer, String> newFileDictEntries,
            List<SstMeta> sstMetas) {
        this(
                UNKNOWN_CHANGE_ID,
                0,
                bucketId,
                partitionId,
                partitionName,
                newFileDictEntries,
                sstMetas);
    }

    public RowPosResult(
            long changeId,
            int split,
            int bucketId,
            @Nullable Long partitionId,
            @Nullable String partitionName,
            Map<Integer, String> newFileDictEntries,
            List<SstMeta> sstMetas) {
        this.changeId = changeId;
        this.split = split;
        this.bucketId = bucketId;
        this.partitionId = partitionId;
        this.partitionName = partitionName;
        this.newFileDictEntries = newFileDictEntries;
        this.sstMetas = sstMetas;
    }

    /**
     * Returns the index change the result belongs to, which tells it apart from results of other
     * requests. Returns {@link #UNKNOWN_CHANGE_ID} if it names none.
     */
    public long getChangeId() {
        return changeId;
    }

    /** Returns the index of the re-index split, unique among the request's splits. */
    public int getSplit() {
        return split;
    }

    /** Returns the bucket id this result belongs to. */
    public int getBucketId() {
        return bucketId;
    }

    /** Returns the partition ID, or null for non-partitioned tables. */
    @Nullable
    public Long getPartitionId() {
        return partitionId;
    }

    /** Returns the partition name, or null for non-partitioned tables. */
    @Nullable
    public String getPartitionName() {
        return partitionName;
    }

    /** Returns the new fileId to fileName mappings. */
    public Map<Integer, String> getNewFileDictEntries() {
        return newFileDictEntries;
    }

    /** Returns the metadata of generated SST files. */
    public List<SstMeta> getSstMetas() {
        return sstMetas;
    }

    /** Metadata for a generated SST file. */
    public static class SstMeta {

        private final String fileName;
        private final long fileSize;

        public SstMeta(String fileName, long fileSize) {
            this.fileName = fileName;
            this.fileSize = fileSize;
        }

        /** Returns the SST file name. */
        public String getFileName() {
            return fileName;
        }

        /** Returns the SST file size in bytes. */
        public long getFileSize() {
            return fileSize;
        }
    }
}
