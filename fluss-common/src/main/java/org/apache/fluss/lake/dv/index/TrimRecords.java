/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.dv.index;

import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.FormatVersions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import static org.apache.fluss.lake.dv.index.IndexFiles.lines;
import static org.apache.fluss.lake.dv.index.IndexFiles.parseLong;
import static org.apache.fluss.lake.dv.index.IndexFiles.readText;
import static org.apache.fluss.lake.dv.index.IndexFiles.writeText;
import static org.apache.fluss.lake.dv.index.IndexLayout.CHAIN_TRIMMED;
import static org.apache.fluss.lake.dv.index.IndexLayout.TRIMMED_VERSION;
import static org.apache.fluss.lake.dv.index.IndexLayout.TRIM_BUCKET;
import static org.apache.fluss.lake.dv.index.IndexLayout.TRIM_CHANGE;
import static org.apache.fluss.lake.dv.index.IndexLayout.TRIM_END;
import static org.apache.fluss.lake.dv.index.IndexLayout.TRIM_FIRST;
import static org.apache.fluss.lake.dv.index.IndexLayout.TRIM_MARKER;

/**
 * The records of the trims of the {@link FlussPositionIndexStore}'s chain, under {@link
 * IndexLayout#CHAIN_TRIMMED}: one file per trim, under a name no other trim uses, written once
 * before the trim deletes anything. A record lists the first kept snapshot, the chain markers and
 * changes the trim deletes, and the buckets whose rows changes trimmed off the chain hold, but
 * those of dropped partitions, and ends with an end line. A record without one was cut short while
 * it was written, before its trim deleted anything, and counts for nothing.
 */
final class TrimRecords {

    // the store's logger
    private static final Logger LOG = LoggerFactory.getLogger(FlussPositionIndexStore.class);

    /** One trim's record. */
    static final class Record {
        final String name;
        // the sequence of the first kept chain entry
        final long sequence;
        final long firstKept;
        final List<String> markers;
        final Set<Long> changes;
        // "{partition key} {bucket}" of the buckets whose rows trimmed changes hold
        final Set<String> buckets;

        Record(
                String name,
                long sequence,
                long firstKept,
                List<String> markers,
                Set<Long> changes,
                Set<String> buckets) {
            this.name = name;
            this.sequence = sequence;
            this.firstKept = firstKept;
            this.markers = markers;
            this.changes = changes;
            this.buckets = buckets;
        }
    }

    private final IndexLayout layout;
    // the store's hooks: deletes
    private final FlussPositionIndexStore store;

    TrimRecords(IndexLayout layout, FlussPositionIndexStore store) {
        this.layout = layout;
        this.store = store;
    }

    /** The key of a bucket in a record. */
    static String bucketKey(long partitionKey, int bucket) {
        return partitionKey + " " + bucket;
    }

    /** Writes the record of a trim that cuts the chain at the entry of {@code sequence}. */
    Record write(
            long sequence,
            long firstKept,
            List<String> markers,
            Collection<Long> changes,
            Collection<String> buckets)
            throws IOException {
        String name =
                String.format(
                        "%020d-%s",
                        sequence, UUID.randomUUID().toString().replace("-", "").substring(0, 16));
        StringBuilder sb = new StringBuilder();
        sb.append(TRIM_FIRST).append(firstKept).append('\n');
        for (String marker : markers) {
            sb.append(TRIM_MARKER).append(marker).append('\n');
        }
        for (long changeId : changes) {
            sb.append(TRIM_CHANGE).append(changeId).append('\n');
        }
        Set<String> sorted = new TreeSet<>(buckets);
        for (String bucket : sorted) {
            sb.append(TRIM_BUCKET).append(bucket).append('\n');
        }
        sb.append(TRIM_END);
        writeText(
                new FsPath(layout.path(CHAIN_TRIMMED), name),
                FormatVersions.withVersion(TRIMMED_VERSION, sb.toString()));
        return new Record(name, sequence, firstKept, markers, new LinkedHashSet<>(changes), sorted);
    }

    /** Whether any trim recorded itself: the chain may be trimmed. */
    boolean any() throws IOException {
        return !list().isEmpty();
    }

    /** The complete record of the latest trim, or null if no trim completed its record. */
    @Nullable
    Record latest() throws IOException {
        Record latest = null;
        for (FileStatus st : list()) {
            String name = st.getPath().getName();
            Long sequence = sequenceOf(name);
            if (sequence == null || (latest != null && sequence < latest.sequence)) {
                continue;
            }
            Record r = read(st.getPath(), name, sequence);
            if (r != null
                    && (latest == null
                            || r.sequence > latest.sequence
                            || r.name.compareTo(latest.name) > 0)) {
                latest = r;
            }
        }
        return latest;
    }

    /** Deletes the records older than {@code latest}, best effort. */
    void deleteBefore(Record latest) throws IOException {
        for (FileStatus st : list()) {
            Long sequence = sequenceOf(st.getPath().getName());
            if (sequence != null && sequence < latest.sequence) {
                try {
                    store.deletePath(st.getPath(), false);
                } catch (IOException e) {
                    LOG.debug("Could not delete the old trim record {}.", st.getPath(), e);
                }
            }
        }
    }

    private List<FileStatus> list() throws IOException {
        FsPath dir = layout.path(CHAIN_TRIMMED);
        FileSystem fs = dir.getFileSystem();
        List<FileStatus> out = new ArrayList<>();
        if (fs.exists(dir)) {
            for (FileStatus st : fs.listStatus(dir)) {
                if (!st.isDir()) {
                    out.add(st);
                }
            }
        }
        return out;
    }

    @Nullable
    private static Long sequenceOf(String name) {
        int end = name.indexOf('-');
        return end <= 0 ? null : parseLong(name.substring(0, end));
    }

    /** A record, or null if it is cut short or gone. A record of a newer version throws. */
    @Nullable
    private static Record read(FsPath path, String name, long sequence) throws IOException {
        String text = readText(path);
        if (text == null || text.isEmpty()) {
            return null;
        }
        List<String> lines = lines(text);
        if (lines.isEmpty() || !lines.get(lines.size() - 1).equals(TRIM_END)) {
            return null; // cut short before its trim deleted anything
        }
        long firstKept = -1;
        List<String> markers = new ArrayList<>();
        Set<Long> changes = new LinkedHashSet<>();
        Set<String> buckets = new TreeSet<>();
        for (String line : lines(FormatVersions.withoutVersion(path, text, TRIMMED_VERSION))) {
            if (line.startsWith(TRIM_FIRST)) {
                firstKept = Long.parseLong(line.substring(TRIM_FIRST.length()));
            } else if (line.startsWith(TRIM_MARKER)) {
                markers.add(line.substring(TRIM_MARKER.length()));
            } else if (line.startsWith(TRIM_CHANGE)) {
                changes.add(Long.parseLong(line.substring(TRIM_CHANGE.length())));
            } else if (line.startsWith(TRIM_BUCKET)) {
                buckets.add(line.substring(TRIM_BUCKET.length()));
            }
        }
        return new Record(name, sequence, firstKept, markers, changes, buckets);
    }
}
