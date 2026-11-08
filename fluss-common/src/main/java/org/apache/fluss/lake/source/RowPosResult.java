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

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * The result of re-indexing the rewritten files of one bucket: the new fileId to fileName mappings
 * and the metadata of the index SST files written.
 *
 * @since 1.1
 */
@PublicEvolving
public class RowPosResult {

    /** The index change the result's SSTs are written under. */
    private final long changeId;

    /** The re-index split the result is of. A bucket's files may span several splits. */
    private final int split;

    /** New fileId to fileName mappings for the re-indexed files. */
    private final Map<Integer, String> newFileDictEntries;

    /** Metadata of the written SST files, mapping primary keys to file positions. */
    private final List<SstMeta> sstMetas;

    /** Why the split could not be re-indexed, or null if it was. */
    @Nullable private final String failure;

    public RowPosResult(
            long changeId,
            int split,
            Map<Integer, String> newFileDictEntries,
            List<SstMeta> sstMetas) {
        this(changeId, split, newFileDictEntries, sstMetas, null);
    }

    public RowPosResult(
            long changeId,
            int split,
            Map<Integer, String> newFileDictEntries,
            List<SstMeta> sstMetas,
            @Nullable String failure) {
        this.changeId = changeId;
        this.split = split;
        this.newFileDictEntries = newFileDictEntries;
        this.sstMetas = sstMetas;
        this.failure = failure;
    }

    /**
     * The result of a re-index split that failed, e.g. on two live rows of one key. It fails the
     * round of its table only.
     */
    public static RowPosResult failed(long changeId, int split, String failure) {
        return new RowPosResult(
                changeId, split, Collections.emptyMap(), Collections.emptyList(), failure);
    }

    /** Whether the split could not be re-indexed; see {@link #getFailure()}. */
    public boolean isFailed() {
        return failure != null;
    }

    /** Why the split could not be re-indexed, or null if it was. */
    @Nullable
    public String getFailure() {
        return failure;
    }

    /**
     * Returns the index change the result belongs to, which tells it apart from results of other
     * requests.
     */
    public long getChangeId() {
        return changeId;
    }

    /** Returns the index of the re-index split, unique among the request's splits. */
    public int getSplit() {
        return split;
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
