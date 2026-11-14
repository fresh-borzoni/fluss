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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Immutable standard metadata stored in a KV snapshot {@code _METADATA} file. */
@Internal
public final class KvSnapshotFileMetadata {

    private final TableBucket tableBucket;
    private final long snapshotId;
    private final String snapshotLocation;
    private final List<FileHandle> sharedFiles;
    private final List<FileHandle> privateFiles;
    private final long incrementalSize;
    private final long logOffset;
    private final @Nullable Long rowCount;
    private final @Nullable List<AutoIncrementRange> autoIncrementRanges;
    private final @Nullable DvChangeList dvChangeList;

    /** Creates immutable standard KV snapshot file metadata. */
    public KvSnapshotFileMetadata(
            TableBucket tableBucket,
            long snapshotId,
            String snapshotLocation,
            List<FileHandle> sharedFiles,
            List<FileHandle> privateFiles,
            long incrementalSize,
            long logOffset,
            @Nullable Long rowCount,
            @Nullable List<AutoIncrementRange> autoIncrementRanges) {
        this(
                tableBucket,
                snapshotId,
                snapshotLocation,
                sharedFiles,
                privateFiles,
                incrementalSize,
                logOffset,
                rowCount,
                autoIncrementRanges,
                null);
    }

    /**
     * Creates immutable standard KV snapshot file metadata with the change list of derived deletion
     * vectors at the snapshot's log offset, if the bucket's leader attached one.
     */
    public KvSnapshotFileMetadata(
            TableBucket tableBucket,
            long snapshotId,
            String snapshotLocation,
            List<FileHandle> sharedFiles,
            List<FileHandle> privateFiles,
            long incrementalSize,
            long logOffset,
            @Nullable Long rowCount,
            @Nullable List<AutoIncrementRange> autoIncrementRanges,
            @Nullable DvChangeList dvChangeList) {
        this.tableBucket = checkNotNull(tableBucket, "Table bucket must not be null.");
        this.snapshotId = snapshotId;
        this.snapshotLocation =
                checkNotNull(snapshotLocation, "Snapshot location must not be null.");
        this.sharedFiles = immutableCopy(sharedFiles, "Shared files must not be null.");
        this.privateFiles = immutableCopy(privateFiles, "Private files must not be null.");
        this.incrementalSize = incrementalSize;
        this.logOffset = logOffset;
        this.rowCount = rowCount;
        this.autoIncrementRanges =
                autoIncrementRanges == null
                        ? null
                        : immutableCopy(
                                autoIncrementRanges,
                                "Auto-increment ranges must not contain null entries.");
        this.dvChangeList = dvChangeList;
    }

    /** Returns the table bucket described by this metadata. */
    public TableBucket getTableBucket() {
        return tableBucket;
    }

    /** Returns the snapshot ID. */
    public long getSnapshotId() {
        return snapshotId;
    }

    /** Returns the snapshot location. */
    public String getSnapshotLocation() {
        return snapshotLocation;
    }

    /** Returns the shared snapshot files. */
    public List<FileHandle> getSharedFiles() {
        return sharedFiles;
    }

    /** Returns the private snapshot files. */
    public List<FileHandle> getPrivateFiles() {
        return privateFiles;
    }

    /** Returns the incremental snapshot size. */
    public long getIncrementalSize() {
        return incrementalSize;
    }

    /** Returns the next log offset at snapshot time. */
    public long getLogOffset() {
        return logOffset;
    }

    /** Returns the row count when present. */
    @Nullable
    public Long getRowCount() {
        return rowCount;
    }

    /** Returns the auto-increment ranges when present. */
    @Nullable
    public List<AutoIncrementRange> getAutoIncrementRanges() {
        return autoIncrementRanges;
    }

    /**
     * Returns the change list of derived deletion vectors at the snapshot's log offset, or null if
     * the snapshot has none. Readers that do not use it ignore it.
     */
    @Nullable
    public DvChangeList getDvChangeList() {
        return dvChangeList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KvSnapshotFileMetadata that = (KvSnapshotFileMetadata) o;
        return snapshotId == that.snapshotId
                && incrementalSize == that.incrementalSize
                && logOffset == that.logOffset
                && Objects.equals(tableBucket, that.tableBucket)
                && Objects.equals(snapshotLocation, that.snapshotLocation)
                && Objects.equals(sharedFiles, that.sharedFiles)
                && Objects.equals(privateFiles, that.privateFiles)
                && Objects.equals(rowCount, that.rowCount)
                && Objects.equals(autoIncrementRanges, that.autoIncrementRanges)
                && Objects.equals(dvChangeList, that.dvChangeList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                tableBucket,
                snapshotId,
                snapshotLocation,
                sharedFiles,
                privateFiles,
                incrementalSize,
                logOffset,
                rowCount,
                autoIncrementRanges,
                dvChangeList);
    }

    private static <T> List<T> immutableCopy(List<T> values, String message) {
        checkNotNull(values, message);
        ArrayList<T> copy = new ArrayList<>(values.size());
        for (T value : values) {
            copy.add(checkNotNull(value, message));
        }
        return Collections.unmodifiableList(copy);
    }

    /** Immutable file reference stored in standard KV snapshot metadata. */
    @Internal
    public static final class FileHandle {

        private final String path;
        private final long size;
        private final String localPath;
        private final @Nullable String sha256;

        /** Creates an immutable file reference without a content digest. */
        public FileHandle(String path, long size, String localPath) {
            this(path, size, localPath, null);
        }

        /** Creates an immutable file reference with an optional SHA-256 content digest. */
        public FileHandle(String path, long size, String localPath, @Nullable String sha256) {
            this.path = checkNotNull(path, "File path must not be null.");
            checkArgument(size >= 0L, "File size must not be negative.");
            this.size = size;
            this.localPath = checkNotNull(localPath, "File local path must not be null.");
            checkArgument(
                    sha256 == null || sha256.matches("[0-9a-f]{64}"),
                    "File SHA-256 must contain exactly 64 lowercase hexadecimal characters.");
            this.sha256 = sha256;
        }

        /** Returns the remote file path. */
        public String getPath() {
            return path;
        }

        /** Returns the file size. */
        public long getSize() {
            return size;
        }

        /** Returns the local-path identity stored in the metadata. */
        public String getLocalPath() {
            return localPath;
        }

        /** Returns the lowercase hexadecimal SHA-256 when present. */
        @Nullable
        public String getSha256() {
            return sha256;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FileHandle that = (FileHandle) o;
            return size == that.size
                    && Objects.equals(path, that.path)
                    && Objects.equals(localPath, that.localPath)
                    && Objects.equals(sha256, that.sha256);
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, size, localPath, sha256);
        }
    }

    /** Immutable auto-increment range stored in standard KV snapshot metadata. */
    @Internal
    public static final class AutoIncrementRange {

        private final int columnId;
        private final long start;
        private final long end;

        /** Creates an immutable auto-increment range. */
        public AutoIncrementRange(int columnId, long start, long end) {
            this.columnId = columnId;
            this.start = start;
            this.end = end;
        }

        /** Returns the auto-increment column ID. */
        public int getColumnId() {
            return columnId;
        }

        /** Returns the range start. */
        public long getStart() {
            return start;
        }

        /** Returns the range end. */
        public long getEnd() {
            return end;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AutoIncrementRange that = (AutoIncrementRange) o;
            return columnId == that.columnId && start == that.start && end == that.end;
        }

        @Override
        public int hashCode() {
            return Objects.hash(columnId, start, end);
        }
    }

    /**
     * The change list of a bucket with derived deletion vectors at a KV snapshot: its updates and
     * deletes in [{@code from}, {@code end}), with the versions they superseded, in segment files
     * shared across snapshots. {@code end} is the snapshot's log offset. A superseded version is
     * named exactly, or as before {@code validFrom}. A change list of a version this build does not
     * know is kept as unsupported: it names no files and is not written again.
     */
    @Internal
    public static final class DvChangeList {

        /** The version this build writes and reads. */
        public static final int VERSION = 1;

        private final int version;
        private final long from;
        private final long end;
        private final long validFrom;
        private final long lakeSnapshotId;
        private final long entries;
        private final List<DvChangeListSegment> segments;

        /** Creates a change list of the current version. */
        public DvChangeList(
                long from,
                long end,
                long validFrom,
                long lakeSnapshotId,
                long entries,
                List<DvChangeListSegment> segments) {
            checkArgument(
                    0 <= from && from <= end, "A change list must cover [from, end), from >= 0.");
            checkArgument(
                    validFrom >= 0, "A change list's valid-from offset must not be negative.");
            checkArgument(entries >= 0, "A change list's entry count must not be negative.");
            this.version = VERSION;
            this.from = from;
            this.end = end;
            this.validFrom = validFrom;
            this.lakeSnapshotId = lakeSnapshotId;
            this.entries = entries;
            this.segments = immutableCopy(segments, "Segments must not be null.");
        }

        private DvChangeList(int version) {
            this.version = version;
            this.from = -1;
            this.end = -1;
            this.validFrom = -1;
            this.lakeSnapshotId = -1;
            this.entries = 0;
            this.segments = Collections.emptyList();
        }

        /** A change list of a version this build does not read. */
        public static DvChangeList unsupported(int version) {
            checkArgument(version != VERSION, "Version %s is supported.", version);
            return new DvChangeList(version);
        }

        /** Whether this build reads the change list's version. */
        public boolean isSupported() {
            return version == VERSION;
        }

        public int getVersion() {
            return version;
        }

        /** The first offset the change list covers. */
        public long getFrom() {
            return from;
        }

        /** The offset after the last one the change list covers: the snapshot's log offset. */
        public long getEnd() {
            return end;
        }

        /** Superseded versions before this offset are named only as before it. */
        public long getValidFrom() {
            return validFrom;
        }

        /** The lake snapshot the leader covered when it wrote the change list, for logs. */
        public long getLakeSnapshotId() {
            return lakeSnapshotId;
        }

        /** The updates and deletes in [from, end). */
        public long getEntries() {
            return entries;
        }

        /** The segment files, in offset order, covering [from, end) without gaps. */
        public List<DvChangeListSegment> getSegments() {
            return segments;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DvChangeList that = (DvChangeList) o;
            return version == that.version
                    && from == that.from
                    && end == that.end
                    && validFrom == that.validFrom
                    && lakeSnapshotId == that.lakeSnapshotId
                    && entries == that.entries
                    && Objects.equals(segments, that.segments);
        }

        @Override
        public int hashCode() {
            return Objects.hash(version, from, end, validFrom, lakeSnapshotId, entries, segments);
        }
    }

    /** One segment file of a {@link DvChangeList}: its changes in [{@code from}, {@code end}). */
    @Internal
    public static final class DvChangeListSegment {

        private final FileHandle file;
        private final long from;
        private final long end;
        private final long entries;

        public DvChangeListSegment(FileHandle file, long from, long end, long entries) {
            this.file = checkNotNull(file, "Segment file must not be null.");
            checkArgument(0 <= from && from <= end, "A segment must cover [from, end), from >= 0.");
            checkArgument(entries >= 0, "A segment's entry count must not be negative.");
            this.from = from;
            this.end = end;
            this.entries = entries;
        }

        public FileHandle getFile() {
            return file;
        }

        public long getFrom() {
            return from;
        }

        public long getEnd() {
            return end;
        }

        public long getEntries() {
            return entries;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DvChangeListSegment that = (DvChangeListSegment) o;
            return from == that.from
                    && end == that.end
                    && entries == that.entries
                    && Objects.equals(file, that.file);
        }

        @Override
        public int hashCode() {
            return Objects.hash(file, from, end, entries);
        }
    }
}
