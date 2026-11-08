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

package org.apache.fluss.lake.paimon.utils;

import org.apache.fluss.exception.LakeTableExternallyModifiedException;
import org.apache.fluss.exception.LakeTableRolledBackException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.FormatVersions;

import org.apache.paimon.Snapshot;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BASE_PROPERTY;
import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.apache.fluss.lake.writer.LakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;

/**
 * Checks that a Paimon table was not rolled back past a snapshot Fluss registered, and that no
 * other engine wrote rows to it since.
 *
 * <p>Paimon's history is linear and its snapshot ids grow with every commit. So the latest snapshot
 * descends from a registered snapshot if it is not older and the snapshot under the registered id
 * is still the one Fluss registered. Paimon's rollback deletes the later snapshots and reuses their
 * ids for the next commits, so only the commit under the id tells.
 */
public final class PaimonSnapshotLineage {

    private PaimonSnapshotLineage() {}

    /**
     * Throws {@link LakeTableRolledBackException} unless the latest snapshot of {@code table}, the
     * lake table of Fluss table {@code tableId}, is {@code snapshotId} or descends from it.
     *
     * <ul>
     *   <li>A latest snapshot older than {@code snapshotId} is a rollback.
     *   <li>A commit of this table's tiering under the id is the registered snapshot, unless it
     *       records a base not older than itself. Only a round that raced a rollback commits so.
     *   <li>A commit of another engine under the id is registered by Fluss only as the first
     *       snapshot of a lake table Fluss was enabled on, or as the readable snapshot of a round
     *       whose own commit follows it. So it replaced the registered snapshot after a rollback if
     *       the latest commit of this table's tiering is older than it.
     *   <li>An expired snapshot, or a table without snapshots, passes: the history that would tell
     *       is gone.
     * </ul>
     *
     * <p>Throws {@link LakeTableExternallyModifiedException} if a snapshot after {@code snapshotId}
     * wrote rows outside Fluss: an APPEND with records or an OVERWRITE that no Fluss tiering
     * committed, such as an INSERT, UPDATE, DELETE or MERGE of another engine. Compactions keep the
     * rows and pass.
     */
    public static void checkDescendsFrom(
            FileStoreTable table, String tableName, long tableId, long snapshotId)
            throws IOException {
        checkNotRolledBack(table, tableName, tableId, snapshotId);
        checkNoExternalWrites(table, tableName, snapshotId);
    }

    private static void checkNotRolledBack(
            FileStoreTable table, String tableName, long tableId, long snapshotId)
            throws IOException {
        SnapshotManager snapshotManager = table.snapshotManager();
        Long latestId = snapshotManager.latestSnapshotId();
        if (latestId == null) {
            return;
        }
        if (latestId < snapshotId) {
            throw LakeTableRolledBackException.ofPaimon(tableName, latestId, snapshotId, null);
        }
        Snapshot registered = tryGetSnapshot(snapshotManager, snapshotId);
        if (registered == null) {
            return;
        }
        if (isTieringCommitOf(registered, tableId)) {
            Long base = baseOf(registered);
            if (base != null && base >= snapshotId) {
                throw LakeTableRolledBackException.ofPaimon(
                        tableName, latestId, snapshotId, describe(registered));
            }
            return;
        }
        if (isTieringCommit(registered)) {
            // A tiering commit of another table, or one without offsets: nothing to tell by.
            return;
        }
        Long latestTieringCommit = latestTieringCommitOf(snapshotManager, tableId, latestId);
        if (latestTieringCommit != null && latestTieringCommit < snapshotId) {
            throw LakeTableRolledBackException.ofPaimon(
                    tableName, latestId, snapshotId, describe(registered));
        }
    }

    /**
     * Throws if a snapshot after {@code snapshotId} wrote rows outside Fluss, naming the first one.
     * Snapshot ids grow with every commit, so the snapshots after it have greater ids.
     */
    private static void checkNoExternalWrites(
            FileStoreTable table, String tableName, long snapshotId) {
        SnapshotManager snapshotManager = table.snapshotManager();
        Long latestId = snapshotManager.latestSnapshotId();
        Long earliestId = snapshotManager.earliestSnapshotId();
        if (latestId == null || earliestId == null) {
            return;
        }
        for (long id = Math.max(snapshotId + 1, earliestId); id <= latestId; id++) {
            Snapshot snapshot = tryGetSnapshot(snapshotManager, id);
            if (snapshot != null && !isTieringCommit(snapshot) && writesRows(snapshot)) {
                throw LakeTableExternallyModifiedException.ofPaimonExternalWrite(
                        tableName, id, describe(snapshot), snapshotId);
            }
        }
    }

    /** Whether a commit wrote rows: an OVERWRITE, or an APPEND with records. */
    private static boolean writesRows(Snapshot snapshot) {
        return snapshot.commitKind() == Snapshot.CommitKind.OVERWRITE
                || (snapshot.commitKind() == Snapshot.CommitKind.APPEND
                        && snapshot.deltaRecordCount() != 0);
    }

    /**
     * Whether a commit of the tiering of Fluss table {@code tableId} made the snapshot: its commit
     * user is the tiering's and its offsets file belongs to the table.
     */
    public static boolean isTieringCommitOf(Snapshot snapshot, long tableId) {
        if (!isTieringCommit(snapshot)) {
            return false;
        }
        Long commitTableId = tableIdOf(snapshot);
        return commitTableId != null && commitTableId == tableId;
    }

    private static boolean isTieringCommit(Snapshot snapshot) {
        return snapshot.commitUser() != null
                && snapshot.commitUser().startsWith(FLUSS_LAKE_TIERING_COMMIT_USER);
    }

    /** The id of the latest commit of the table's tiering at or before {@code fromId}, or null. */
    @Nullable
    private static Long latestTieringCommitOf(
            SnapshotManager snapshotManager, long tableId, long fromId) throws IOException {
        Long earliestId = snapshotManager.earliestSnapshotId();
        if (earliestId == null) {
            return null;
        }
        for (long id = fromId; id >= earliestId; id--) {
            Snapshot snapshot = tryGetSnapshot(snapshotManager, id);
            Long commitTableId =
                    snapshot != null && isTieringCommit(snapshot) ? tableIdOf(snapshot) : null;
            if (commitTableId != null) {
                // A commit of another table's tiering: that table preceded this one.
                return commitTableId == tableId ? id : null;
            }
        }
        return null;
    }

    @Nullable
    private static Long tableIdOf(Snapshot snapshot) {
        Map<String, String> properties = snapshot.properties();
        String offsetsPath =
                properties == null ? null : properties.get(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY);
        return offsetsPath == null ? null : tableIdOfOffsetsFile(offsetsPath);
    }

    @Nullable
    private static Long baseOf(Snapshot snapshot) {
        Map<String, String> properties = snapshot.properties();
        String base = properties == null ? null : properties.get(FLUSS_LAKE_SNAP_BASE_PROPERTY);
        if (base == null) {
            return null;
        }
        FormatVersions.checkSnapshotVersion(snapshot.id(), properties);
        try {
            return Long.parseLong(base);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * The Fluss table id of an offsets file: {@code
     * .../lake/{database}/{table}-{tableId}/metadata/{uuid}.offsets}.
     */
    @Nullable
    static Long tableIdOfOffsetsFile(String offsetsPath) {
        try {
            String tableDirName = new FsPath(offsetsPath).getParent().getParent().getName();
            return Long.parseLong(tableDirName.substring(tableDirName.lastIndexOf('-') + 1));
        } catch (RuntimeException e) {
            return null;
        }
    }

    @Nullable
    private static Snapshot tryGetSnapshot(SnapshotManager snapshotManager, long snapshotId) {
        try {
            return snapshotManager.tryGetSnapshot(snapshotId);
        } catch (FileNotFoundException e) {
            return null;
        }
    }

    private static String describe(Snapshot snapshot) {
        return String.format(
                "the %s commit of '%s' at %s",
                snapshot.commitKind(),
                snapshot.commitUser(),
                Instant.ofEpochMilli(snapshot.timeMillis()));
    }
}
