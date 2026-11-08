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

package org.apache.fluss.lake.iceberg.maintenance;

import org.apache.fluss.lake.dv.index.CompactionIndexPart;

import org.apache.iceberg.util.DataFileSet;
import org.apache.iceberg.util.DeleteFileSet;

import javax.annotation.Nullable;

import java.io.Serializable;

/** The result for rewrite iceberg data files. */
public class RewriteDataFileResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long snapshotId;
    private final DataFileSet deletedDataFiles;
    private final DataFileSet addedDataFiles;
    // Null when read from a result written before this field existed.
    private final DeleteFileSet deletedDeleteFiles;
    // the positions of the rewritten rows, handed to the position index of a table with deletion
    // vectors; null if none were handed over
    @Nullable private final CompactionIndexPart indexPart;
    // the compacted bucket (see IcebergRewriteDataFiles#bucketKey), or null if unknown
    @Nullable private final String bucketKey;

    public RewriteDataFileResult(
            long snapshotId, DataFileSet deletedDataFiles, DataFileSet addedDataFiles) {
        this(snapshotId, deletedDataFiles, addedDataFiles, DeleteFileSet.create());
    }

    public RewriteDataFileResult(
            long snapshotId,
            DataFileSet deletedDataFiles,
            DataFileSet addedDataFiles,
            DeleteFileSet deletedDeleteFiles) {
        this(snapshotId, deletedDataFiles, addedDataFiles, deletedDeleteFiles, null);
    }

    public RewriteDataFileResult(
            long snapshotId,
            DataFileSet deletedDataFiles,
            DataFileSet addedDataFiles,
            DeleteFileSet deletedDeleteFiles,
            @Nullable CompactionIndexPart indexPart) {
        this(snapshotId, deletedDataFiles, addedDataFiles, deletedDeleteFiles, indexPart, null);
    }

    public RewriteDataFileResult(
            long snapshotId,
            DataFileSet deletedDataFiles,
            DataFileSet addedDataFiles,
            DeleteFileSet deletedDeleteFiles,
            @Nullable CompactionIndexPart indexPart,
            @Nullable String bucketKey) {
        this.snapshotId = snapshotId;
        this.deletedDataFiles = deletedDataFiles;
        this.addedDataFiles = addedDataFiles;
        this.deletedDeleteFiles = deletedDeleteFiles;
        this.indexPart = indexPart;
        this.bucketKey = bucketKey;
    }

    /** Returns the key of the compacted bucket, or null if unknown. */
    @Nullable
    public String bucketKey() {
        return bucketKey;
    }

    /**
     * Returns the bucket's index part of the rewrite, with the new position of every row it kept.
     * Returns null if the positions were not handed over; the rewritten files must then be
     * re-indexed from the lake.
     */
    @Nullable
    public CompactionIndexPart indexPart() {
        return indexPart;
    }

    public DataFileSet deletedDataFiles() {
        return deletedDataFiles;
    }

    public DataFileSet addedDataFiles() {
        return addedDataFiles;
    }

    /**
     * Returns the position delete files that only reference deleted data files. The rewrite applied
     * them, so they are removed with the data files.
     */
    public DeleteFileSet deletedDeleteFiles() {
        return deletedDeleteFiles == null ? DeleteFileSet.create() : deletedDeleteFiles;
    }

    public long snapshotId() {
        return snapshotId;
    }

    @Override
    public String toString() {
        return "RewriteDataFileResult{"
                + "snapshotId="
                + snapshotId
                + ", deletedDataFiles="
                + deletedDataFiles
                + ", addedDataFiles="
                + addedDataFiles
                + ", deletedDeleteFiles="
                + deletedDeleteFiles()
                + (indexPart == null ? "" : ", indexPart=" + indexPart)
                + (bucketKey == null ? "" : ", bucket=" + bucketKey)
                + '}';
    }
}
