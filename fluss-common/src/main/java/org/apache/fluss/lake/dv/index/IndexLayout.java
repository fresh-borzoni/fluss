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

import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.FormatVersions;
import org.apache.fluss.lake.dv.RowPosSstIndex;
import org.apache.fluss.lake.dv.index.PositionIndexStore.ChainEntry;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.fluss.lake.dv.index.IndexFiles.parseInteger;
import static org.apache.fluss.lake.dv.index.IndexFiles.parseLong;

/**
 * The files of the {@link FlussPositionIndexStore} under {@code rowPos/} of the table's remote lake
 * directory, their names and their format versions; no other class knows these paths.
 *
 * <pre>
 * rowPos/{changeId}/[{partitionId}/]rowpos.manifest     manifest per partition
 * rowPos/{changeId}/[{partitionId}/]{bucket}/sst_N.sst  key -> varint(fileId)||varint(pos)
 * rowPos/{changeId}/_STARTED                            when the change was first written
 * rowPos/{changeId}/_plan/{split}                       lake splits a re-index split reads
 * rowPos/{stagingId}/[{partitionId}/]{bucket}/{prefix}sst_N.sst  a tiering writer's staged part:
 *                                                       key -> position, or empty if deleted
 * rowPos/{changeId}/_COMMITTED                          the change's partitions; written last
 * rowPos/_chain/v1-{sequence}_{parent}_{snapshot}_{changeId}_{maxFileId}  one per registered
 *                                                       change, holding the snapshot's identity
 * rowPos/_trimmed/{sequence}-{attempt}                 one record per trim, written once before
 *                                                       it deletes: the first kept snapshot, the
 *                                                       markers and changes it deletes, the buckets
 *                                                       trimmed changes hold, and an end line
 * rowPos/_identities/{marker}.{attempt}                 the identity of a chain entry whose marker
 *                                                       a crash left empty, recorded by its repair
 * rowPos/_file_ids/v1-{next}                           empty; one per allocation, naming the next
 *                                                       free file id; the largest is the counter
 * rowPos/_trims/v1-{sequence}                           empty; one per trim, naming the sequence of
 *                                                       the chain's end when it began
 * rowPos/_ckpt/[{partitionId}/]{bucket}/{snapshotId}_{attempt}/  one per writer attempt:
 *                                                       index.sst, filedict.txt, _DONE (last,
 *                                                       listing the files and their sizes)
 * </pre>
 *
 * <p>Every format has a version, see {@link FormatVersions}. The text files start with a version
 * line. A change directory's {@code _STARTED} also versions the SSTs written into it before the
 * change commits, which no manifest lists yet. A manifest versions itself and the values of the
 * SSTs it lists, and a checkpoint's {@code _DONE} the whole checkpoint. A chain entry carries its
 * version in its name: listing the chain reads names only, and a newer entry must fail a listing
 * even when older entries come before it.
 */
final class IndexLayout {

    static final String ROW_POS_DIR = "rowPos";
    static final String CHAIN_DIR = ROW_POS_DIR + "/_chain";
    static final String CHAIN_TRIMMED = ROW_POS_DIR + "/_trimmed";
    static final String TRIMS_DIR = ROW_POS_DIR + "/_trims";
    static final String FILE_IDS_DIR = ROW_POS_DIR + "/_file_ids";
    // the file-id counter of the layout before write-once files, which this version refuses
    static final String OLD_NEXT_FILE_ID = ROW_POS_DIR + "/_next_file_id";
    static final String IDENTITIES_DIR = ROW_POS_DIR + "/_identities";
    static final String CKPT_DIR = ROW_POS_DIR + "/_ckpt";
    /** Written first into a change directory: its start time (for removing failed attempts). */
    static final String CHANGE_STARTED = "_STARTED";
    /** Written last into a change directory: the partitions with a manifest (-1: no partition). */
    static final String CHANGE_COMMITTED = "_COMMITTED";
    /** Written last into a checkpoint directory: "sst", or "empty" for an empty index. */
    static final String CKPT_DONE = "_DONE";

    static final String CKPT_SST = "index.sst";
    static final String FILE_DICT = "filedict.txt";

    // Format versions of the store's files.
    // A chain entry's name, and its content: the identity SupportsRewritePlanning#snapshotIdentity
    // gave the snapshot. Readers compare it with the identity the lake gives now, so a change of
    // that string is a change of this format.
    static final int CHAIN_ENTRY_VERSION = 1;
    // A change directory: _STARTED, _COMMITTED and the SSTs written before the change commits.
    // The staged SSTs hold FilePos values or empty ones for deletes.
    static final int CHANGE_VERSION = 1;
    // A checkpoint, in its _DONE: the files it lists, index.sst with FilePos values and
    // filedict.txt. Version 2 names a directory per writer attempt and lists the files in _DONE.
    static final int CHECKPOINT_VERSION = 2;
    // An identity record: the identity, then an end line.
    static final int IDENTITY_VERSION = 1;
    // A file-id counter marker's name.
    static final int COUNTER_VERSION = 1;
    // A trim record. Version 2 is one file per trim, with its buckets and an end line.
    static final int TRIMMED_VERSION = 2;
    static final int TRIM_MARKER_VERSION = 1;

    // Lines of a trim record: the first kept snapshot, a chain marker or a change the trim
    // deletes, a bucket whose rows trimmed changes hold, and the end line, written last.
    static final String TRIM_FIRST = "first ";
    static final String TRIM_MARKER = "marker ";
    static final String TRIM_CHANGE = "change ";
    static final String TRIM_BUCKET = "bucket ";
    static final String TRIM_END = "end";

    private static final String CHAIN_ENTRY_VERSION_PREFIX = "v";
    private static final String CHAIN_ENTRY_VERSION_END = "-";

    // A line of a checkpoint's _DONE: a file of the checkpoint and its size.
    static final String DONE_FILE = "file ";
    // name prefix of an SST whose file ids were remapped
    static final String REMAPPED_PREFIX = "remapped_";
    // the re-index plans of a change, one per re-index split, deleted once the change commits
    static final String REINDEX_PLAN_DIR = "_plan";
    static final int PLAN_FORMAT_VERSION = 1;

    final FsPath lakeTableDir;

    IndexLayout(FsPath lakeTableDir) {
        this.lakeTableDir = lakeTableDir;
    }

    /**
     * Throws if the table's {@code rowPos/} was written by an older build of this store, whose
     * layout this version does not read: a file-id counter in one overwritten file, or a trim
     * record that is a file. Reading such a table as empty would build indexes without its rows.
     */
    void checkLayout() throws IOException {
        FsPath counter = path(OLD_NEXT_FILE_ID);
        FsPath trimmed = path(CHAIN_TRIMMED);
        FileSystem fs = counter.getFileSystem();
        FsPath old = null;
        if (fs.exists(counter)) {
            old = counter;
        } else if (fs.exists(trimmed) && !fs.getFileStatus(trimmed).isDir()) {
            old = trimmed;
        }
        if (old != null) {
            throw new IllegalStateException(
                    String.format(
                            "The position index file %s is of a layout this Fluss version does"
                                    + " not read. Delete %s and let the tiering service index the"
                                    + " table again.",
                            old, path(ROW_POS_DIR)));
        }
    }

    /** A path under the table's remote lake directory, such as {@link #CHAIN_DIR}. */
    FsPath path(String relative) {
        return new FsPath(lakeTableDir, relative);
    }

    FsPath changeDir(long changeId) {
        return new FsPath(lakeTableDir, ROW_POS_DIR + "/" + changeId);
    }

    FsPath changeDir(long changeId, @Nullable Long partitionId) {
        return partitionId == null
                ? changeDir(changeId)
                : new FsPath(changeDir(changeId), String.valueOf(partitionId));
    }

    FsPath manifestPath(long changeId, @Nullable Long partitionId) {
        return new FsPath(changeDir(changeId, partitionId), RowPosSstIndex.MANIFEST_FILE);
    }

    FsPath bucketDir(long changeId, TableBucket bucket) {
        return new FsPath(
                changeDir(changeId, bucket.getPartitionId()), String.valueOf(bucket.getBucket()));
    }

    FsPath checkpointDir(TableBucket bucket) {
        Long pid = bucket.getPartitionId();
        return new FsPath(
                lakeTableDir, CKPT_DIR + (pid == null ? "" : "/" + pid) + "/" + bucket.getBucket());
    }

    /** The name of a checkpoint directory: the snapshot, then the writer's attempt. */
    static String checkpointName(long snapshotId, String attempt) {
        return snapshotId + "_" + attempt;
    }

    /** The snapshot of a checkpoint directory's name, or null for another name. */
    @Nullable
    static Long checkpointSnapshot(String name) {
        int end = name.indexOf('_');
        return end <= 0 || end == name.length() - 1 ? null : parseLong(name.substring(0, end));
    }

    static long partitionKey(TableBucket bucket) {
        return bucket.getPartitionId() == null ? -1L : bucket.getPartitionId();
    }

    static ChainEntry parseMarker(String name) {
        String[] f = markerFields(name).split("_");
        return new ChainEntry(
                Long.parseLong(f[0]),
                Long.parseLong(f[1]),
                Long.parseLong(f[2]),
                Long.parseLong(f[3]));
    }

    /** The name of a chain entry, its format version first. */
    static String markerName(
            long sequence, long parentSnapshotId, long snapshotId, long changeId, long maxFileId) {
        return CHAIN_ENTRY_VERSION_PREFIX
                + CHAIN_ENTRY_VERSION
                + CHAIN_ENTRY_VERSION_END
                + String.format(
                        "%020d_%d_%d_%d_%d",
                        sequence, parentSnapshotId, snapshotId, changeId, maxFileId);
    }

    /** The fields of a chain entry's name, once the version it starts with is checked. */
    static String markerFields(String name) {
        int end = name.indexOf(CHAIN_ENTRY_VERSION_END);
        Integer version = null;
        if (name.startsWith(CHAIN_ENTRY_VERSION_PREFIX) && end > 1) {
            version = parseInteger(name.substring(1, end));
        }
        String source = "the position index chain entry " + CHAIN_DIR + "/" + name;
        if (version == null) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot read %s: it has no format version; this Fluss version reads"
                                    + " format versions up to %d.",
                            source, CHAIN_ENTRY_VERSION));
        }
        FormatVersions.check(source, version, CHAIN_ENTRY_VERSION);
        return name.substring(end + 1);
    }

    /** The largest file id a marker's change added, -1 if none. */
    static long markerMaxFileId(String name) {
        return Long.parseLong(markerFields(name).split("_")[4]);
    }

    /** The name of an identity record of a chain marker, unique per writer attempt. */
    static String identityRecordName(String markerName, String attempt) {
        return markerName + "." + attempt;
    }

    /** The name of a file-id counter marker: its format version, then the next free id. */
    static String fileIdMarkerName(long nextFileId) {
        return "v" + COUNTER_VERSION + "-" + nextFileId;
    }

    /**
     * The next free file id a counter marker names. A marker of another format version throws: a
     * newer service may hand out ids this one cannot see, and a reused id masks the wrong file.
     */
    static long fileIdMarkerValue(String name) {
        String source = "the file-id counter " + FILE_IDS_DIR + "/" + name;
        int end = name.indexOf('-');
        Integer version =
                name.startsWith("v") && end > 1 ? parseInteger(name.substring(1, end)) : null;
        Long next = version == null ? null : parseLong(name.substring(end + 1));
        if (version == null || next == null) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot read %s: it has no format version; this Fluss version reads"
                                    + " format versions up to %d.",
                            source, COUNTER_VERSION));
        }
        FormatVersions.check(source, version, COUNTER_VERSION);
        return next;
    }

    static String trimMarkerName(long sequence) {
        return "v" + TRIM_MARKER_VERSION + "-" + sequence;
    }

    /**
     * The sequence a trim marker names, or null for a name of another format version, which a newer
     * service wrote. The count then starts from this version's markers only, so at worst a trim
     * comes early, which is safe.
     */
    @Nullable
    static Long trimMarkerSequence(String name) {
        String prefix = "v" + TRIM_MARKER_VERSION + "-";
        return name.startsWith(prefix) ? parseLong(name.substring(prefix.length())) : null;
    }
}
