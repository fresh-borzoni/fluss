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

package org.apache.fluss.server.kv.snapshot;

import org.apache.fluss.metadata.KvSnapshotFileMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * The change list of a bucket with derived deletion vectors, saved with a KV snapshot: the bucket's
 * updates and deletes in [{@link #from()}, {@link #end()}), each with the version it superseded, in
 * segment files. {@link #end()} is the KV snapshot's log offset. The segments are shared files of
 * the KV snapshot: a later snapshot of the same leader references the ones it still needs, and the
 * KV snapshot registry deletes a segment once no retained snapshot references it.
 *
 * <p>A superseded version is named exactly, or as before {@link #validFrom()}: a replay that built
 * the change list names a version before its start only as such. So a new leader whose readable
 * offset is at least {@link #validFrom()} classifies each change as the full replay would.
 *
 * <p>A change list of a version this build does not read is kept as unsupported: it names no files,
 * is never used, and is not written again.
 */
public final class DvChangeListHandle {

    private final int version;
    private final long from;
    private final long end;
    private final long validFrom;
    private final long lakeSnapshotId;
    private final long entries;
    private final List<Segment> segments;

    /** Creates a change list of the current version. */
    public DvChangeListHandle(
            long from,
            long end,
            long validFrom,
            long lakeSnapshotId,
            long entries,
            List<Segment> segments) {
        checkArgument(0 <= from && from <= end, "A change list covers [from, end), from >= 0.");
        checkArgument(validFrom >= 0, "A change list's valid-from offset must not be negative.");
        checkArgument(entries >= 0, "A change list's entry count must not be negative.");
        long next = from;
        long counted = 0;
        for (Segment s : checkNotNull(segments, "segments")) {
            checkArgument(
                    s.from == next,
                    "The segments of a change list cover [from, end) in order and without gaps;"
                            + " a segment starts at %s, expected %s.",
                    s.from,
                    next);
            next = s.end;
            counted += s.entries;
        }
        checkArgument(
                next == end, "The segments of a change list end at %s, not at %s.", next, end);
        checkArgument(
                counted == entries,
                "The segments of a change list hold %s entries, not %s.",
                counted,
                entries);
        this.version = KvSnapshotFileMetadata.DvChangeList.VERSION;
        this.from = from;
        this.end = end;
        this.validFrom = validFrom;
        this.lakeSnapshotId = lakeSnapshotId;
        this.entries = entries;
        this.segments = Collections.unmodifiableList(new ArrayList<>(segments));
    }

    private DvChangeListHandle(int version) {
        this.version = version;
        this.from = -1;
        this.end = -1;
        this.validFrom = -1;
        this.lakeSnapshotId = -1;
        this.entries = 0;
        this.segments = Collections.emptyList();
    }

    /** A change list of a version this build does not read. */
    public static DvChangeListHandle unsupported(int version) {
        checkArgument(
                version != KvSnapshotFileMetadata.DvChangeList.VERSION,
                "Version %s is supported.",
                version);
        return new DvChangeListHandle(version);
    }

    public int version() {
        return version;
    }

    /** Whether this build reads the change list. */
    public boolean isSupported() {
        return version == KvSnapshotFileMetadata.DvChangeList.VERSION;
    }

    public long from() {
        return from;
    }

    /** The KV snapshot's log offset: the change list holds every update and delete below it. */
    public long end() {
        return end;
    }

    public long validFrom() {
        return validFrom;
    }

    /** The lake snapshot the leader covered when it saved the change list, for logs. */
    public long lakeSnapshotId() {
        return lakeSnapshotId;
    }

    public long entries() {
        return entries;
    }

    public List<Segment> segments() {
        return segments;
    }

    /** The bytes of all segments. */
    public long bytes() {
        long bytes = 0;
        for (Segment s : segments) {
            bytes += s.file.getKvFileHandle().getSize();
        }
        return bytes;
    }

    /** One segment file: the changes in [{@link #from()}, {@link #end()}). */
    public static final class Segment {
        private final KvFileHandleAndLocalPath file;
        private final long from;
        private final long end;
        private final long entries;

        public Segment(KvFileHandleAndLocalPath file, long from, long end, long entries) {
            this.file = checkNotNull(file, "file");
            checkArgument(0 <= from && from <= end, "A segment covers [from, end), from >= 0.");
            checkArgument(entries >= 0, "A segment's entry count must not be negative.");
            this.from = from;
            this.end = end;
            this.entries = entries;
        }

        public KvFileHandleAndLocalPath file() {
            return file;
        }

        public long from() {
            return from;
        }

        public long end() {
            return end;
        }

        public long entries() {
            return entries;
        }

        /**
         * The same segment referenced by a later snapshot: its discard does nothing, as the
         * snapshot that uploaded it owns it.
         */
        public Segment asReference() {
            KvFileHandle handle = file.getKvFileHandle();
            return new Segment(
                    KvFileHandleAndLocalPath.of(
                            handle instanceof PlaceholderKvFileHandler
                                    ? handle
                                    : new PlaceholderKvFileHandler(handle),
                            file.getLocalPath()),
                    from,
                    end,
                    entries);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Segment that = (Segment) o;
            return from == that.from
                    && end == that.end
                    && entries == that.entries
                    && Objects.equals(file, that.file);
        }

        @Override
        public int hashCode() {
            return Objects.hash(file, from, end, entries);
        }

        @Override
        public String toString() {
            return "Segment{" + file + ", [" + from + ", " + end + "), entries=" + entries + '}';
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DvChangeListHandle that = (DvChangeListHandle) o;
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

    @Override
    public String toString() {
        return "DvChangeListHandle{version="
                + version
                + ", ["
                + from
                + ", "
                + end
                + "), validFrom="
                + validFrom
                + ", lakeSnapshot="
                + lakeSnapshotId
                + ", entries="
                + entries
                + ", segments="
                + segments
                + '}';
    }
}
