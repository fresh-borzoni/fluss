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

package org.apache.fluss.client.tiering;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.rpc.messages.PbLakeDvEntry;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * The deletes of a bucket's tiering round on a lake snapshot, as the bucket's leader derives them:
 * per data file of the snapshot, the serialized positions of the rows whose keys the round changed
 * from {@link #startOffset()}, the snapshot's offset of the bucket, up to {@link #endOffset()}.
 * They depend on these three values only, so deletes fetched by a tiering reader stand for those
 * the committer would fetch for the same values.
 */
@Internal
public final class RoundDeletes implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long snapshotId;
    private final long startOffset;
    private final long endOffset;
    // as the leader's parts list them; a file may come in several parts
    private final List<File> files;
    private final long bytes;

    public RoundDeletes(long snapshotId, long startOffset, long endOffset, List<File> files) {
        this.snapshotId = snapshotId;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.files = Collections.unmodifiableList(files);
        long bytes = 0;
        for (File file : files) {
            bytes += file.bytes();
        }
        this.bytes = bytes;
    }

    /**
     * Fetches the deletes of the bucket's round on the snapshot from its leader, part by part, and
     * checks that the parts cover the round from {@code startOffset} to {@code endOffset}. Fails
     * once they take more than {@code maxBytes}.
     */
    public static RoundDeletes fetch(
            TableBucket tb,
            long snapshotId,
            long startOffset,
            long endOffset,
            long maxBytes,
            DvRoundRequests.Fetcher fetcher)
            throws Exception {
        List<File> files = new ArrayList<>();
        long bytes = 0;
        // the first part starts at the leader's readable offset
        Long from = null;
        long partStart = startOffset;
        while (true) {
            GetDvSnapshotResponse part = fetcher.fetch(tb, snapshotId, from, endOffset);
            checkState(
                    part.hasSnapshotStartOffset() && part.getSnapshotStartOffset() == startOffset,
                    "The leader of bucket %s reads base snapshot %s from offset %s, but the round"
                            + " starts at %s.",
                    tb,
                    snapshotId,
                    part.hasSnapshotStartOffset() ? part.getSnapshotStartOffset() : null,
                    startOffset);
            for (PbLakeDvEntry entry : part.getLakeDvEntriesList()) {
                File file = new File(entry.getFilePath(), entry.getDeletedPositionsBitmap());
                bytes += file.bytes();
                if (bytes > maxBytes) {
                    throw new IOException(
                            String.format(
                                    "The deletes of the round of bucket %s on snapshot %s up to"
                                            + " offset %s take more than %s bytes.",
                                    tb, snapshotId, endOffset, maxBytes));
                }
                files.add(file);
            }
            long next = part.hasRoundNextOffset() ? part.getRoundNextOffset() : -1;
            if (next == endOffset) {
                return new RoundDeletes(snapshotId, startOffset, endOffset, files);
            }
            checkState(
                    next > partStart && next < endOffset,
                    "The leader of bucket %s answered the part from offset %s of the deletes of the"
                            + " round on snapshot %s ending at %s with the next part at %s.",
                    tb,
                    partStart,
                    snapshotId,
                    endOffset,
                    next);
            from = next;
            partStart = next;
        }
    }

    public long snapshotId() {
        return snapshotId;
    }

    public long startOffset() {
        return startOffset;
    }

    public long endOffset() {
        return endOffset;
    }

    /** The serialized positions per data file, in the order the leader's parts listed them. */
    public List<File> files() {
        return files;
    }

    /** About the bytes of the paths and positions. */
    public long bytes() {
        return bytes;
    }

    @Override
    public String toString() {
        return "RoundDeletes{snapshotId="
                + snapshotId
                + ", startOffset="
                + startOffset
                + ", endOffset="
                + endOffset
                + ", files="
                + files.size()
                + '}';
    }

    /** The serialized positions of the rows of a data file the round deletes. */
    public static final class File implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String path;
        // a serialized Roaring64Bitmap
        private final byte[] positions;

        public File(String path, byte[] positions) {
            this.path = path;
            this.positions = positions;
        }

        public String path() {
            return path;
        }

        public byte[] positions() {
            return positions;
        }

        long bytes() {
            return path.length() + (long) positions.length;
        }
    }
}
