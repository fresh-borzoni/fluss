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

package org.apache.fluss.lake.iceberg.utils;

import org.apache.fluss.exception.LakeTableExternallyModifiedException;
import org.apache.fluss.exception.LakeTableRolledBackException;

import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.fluss.lake.writer.LakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;

/**
 * Checks that an Iceberg table was not rolled back past a snapshot Fluss registered, and that no
 * other engine changed its rows since.
 */
public final class IcebergSnapshotLineage {

    /** Summary property of the committer of a snapshot Fluss commits. */
    public static final String COMMIT_USER_PROPERTY = "commit-user";

    /**
     * Committer of a Fluss compaction of a table with deletion vectors. The compaction commits on
     * its own, before a round's deletes, so its snapshot carries no bucket offsets.
     */
    public static final String FLUSS_COMPACTION_COMMIT_USER = "fluss-lake-compaction";

    // Spark and Flink record their engine name in the snapshot summary.
    private static final String ENGINE_NAME_PROPERTY = "engine-name";

    private static final String[] ROW_CHANGE_PROPERTIES = {
        SnapshotSummary.ADDED_FILES_PROP,
        SnapshotSummary.DELETED_FILES_PROP,
        SnapshotSummary.ADDED_DELETE_FILES_PROP,
        SnapshotSummary.REMOVED_DELETE_FILES_PROP,
        SnapshotSummary.ADDED_RECORDS_PROP,
        SnapshotSummary.DELETED_RECORDS_PROP
    };

    private IcebergSnapshotLineage() {}

    /**
     * Throws {@link LakeTableRolledBackException} unless the table's current snapshot is {@code
     * snapshotId} or descends from it.
     *
     * <p>Sequence numbers grow with every commit. Walking back from the current snapshot reaches
     * one older than {@code snapshotId} without passing it only if the current snapshot does not
     * descend from it, as after a rollback or a commit on top of one. The check passes if the table
     * has no snapshot {@code snapshotId}, or if expired snapshots cut the history before the walk
     * can decide. It fails if the history starts after {@code snapshotId}.
     *
     * <p>Throws {@link LakeTableExternallyModifiedException} if a snapshot after {@code snapshotId}
     * on the walk was committed by another engine and changed rows. That is any operation but a
     * rewrite ({@code replace}) that added or removed files or records, e.g. an INSERT, UPDATE,
     * DELETE or MERGE. Rewrites keep the rows, so the position index follows them.
     */
    public static void checkDescendsFrom(Table table, long snapshotId) {
        checkDescendsFrom(table, table.currentSnapshot(), snapshotId);
    }

    /**
     * Throws {@link LakeTableRolledBackException} unless {@code current}, the snapshot the table is
     * read at, is {@code snapshotId} or descends from it; see {@link #checkDescendsFrom(Table,
     * long)}.
     */
    public static void checkDescendsFrom(Table table, @Nullable Snapshot current, long snapshotId) {
        if (current != null && current.snapshotId() == snapshotId) {
            return;
        }
        Snapshot registered = table.snapshot(snapshotId);
        if (registered == null) {
            return;
        }
        if (current == null) {
            throw LakeTableRolledBackException.of(databaseAndTable(table), null, snapshotId);
        }
        long sequenceNumber = registered.sequenceNumber();
        // the oldest snapshot after the registered one that another engine changed rows in
        Snapshot externalWrite = null;
        for (Snapshot s = current; s != null; ) {
            if (s.snapshotId() == snapshotId) {
                break;
            }
            // v1 tables have no sequence numbers. A root starts another history, e.g. after the
            // table was replaced.
            if ((sequenceNumber > 0 && s.sequenceNumber() <= sequenceNumber)
                    || s.parentId() == null) {
                throw LakeTableRolledBackException.of(
                        databaseAndTable(table), current.snapshotId(), snapshotId);
            }
            if (!isFlussCommit(s) && changesRows(s)) {
                externalWrite = s;
            }
            s = table.snapshot(s.parentId());
        }
        if (externalWrite != null) {
            throw LakeTableExternallyModifiedException.ofExternalWrite(
                    databaseAndTable(table),
                    externalWrite.snapshotId(),
                    String.valueOf(externalWrite.operation()),
                    committerOf(externalWrite),
                    externalWrite.parentId(),
                    snapshotId);
        }
    }

    /** The committer a snapshot's summary names, or "another engine". */
    private static String committerOf(Snapshot snapshot) {
        Map<String, String> summary = snapshot.summary();
        String committer = summary == null ? null : summary.get(COMMIT_USER_PROPERTY);
        if (committer == null && summary != null) {
            committer = summary.get(ENGINE_NAME_PROPERTY);
        }
        return committer == null ? "another engine" : committer;
    }

    /**
     * Throws {@link LakeTableExternallyModifiedException} if a snapshot from {@code current} back
     * to the oldest one the table keeps was committed by another engine and changed rows. This is
     * the check of a table with deletion vectors before Fluss registered any snapshot of it: its
     * index has only the rows Fluss tiered, so a round would leave the other rows live next to its
     * own versions of their keys. Rewrites that keep the rows pass.
     */
    public static void checkNoExternalWrites(Table table, @Nullable Snapshot current) {
        // the oldest snapshot that another engine changed rows in
        Snapshot externalWrite = null;
        for (Snapshot s = current; s != null; ) {
            if (!isFlussCommit(s) && changesRows(s)) {
                externalWrite = s;
            }
            s = s.parentId() == null ? null : table.snapshot(s.parentId());
        }
        if (externalWrite != null) {
            throw LakeTableExternallyModifiedException.ofExternalWriteBeforeTiering(
                    databaseAndTable(table),
                    externalWrite.snapshotId(),
                    String.valueOf(externalWrite.operation()),
                    committerOf(externalWrite),
                    externalWrite.parentId());
        }
    }

    /**
     * Whether the history of {@code current} reaches snapshot {@code snapshotId} through snapshots
     * the table still has. It does not if a snapshot in between expired, or if {@code current} does
     * not descend from it. Then the snapshots since {@code snapshotId} cannot be replayed, and
     * nothing tells what the missing ones changed.
     */
    public static boolean historyReaches(Table table, Snapshot current, long snapshotId) {
        for (Snapshot s = current; s != null; ) {
            if (s.snapshotId() == snapshotId) {
                return true;
            }
            s = s.parentId() == null ? null : table.snapshot(s.parentId());
        }
        return false;
    }

    /** Whether Fluss committed the snapshot: a tiering round or a Fluss compaction. */
    public static boolean isFlussCommit(Snapshot snapshot) {
        Map<String, String> summary = snapshot.summary();
        String user = summary == null ? null : summary.get(COMMIT_USER_PROPERTY);
        return FLUSS_LAKE_TIERING_COMMIT_USER.equals(user)
                || FLUSS_COMPACTION_COMMIT_USER.equals(user);
    }

    /**
     * Whether a snapshot changed rows: any operation but a rewrite ({@code replace}) that added or
     * removed data files, delete files or records.
     */
    static boolean changesRows(Snapshot snapshot) {
        if (DataOperations.REPLACE.equals(snapshot.operation())) {
            return false;
        }
        Map<String, String> summary = snapshot.summary();
        if (summary == null) {
            return true;
        }
        for (String property : ROW_CHANGE_PROPERTIES) {
            String value = summary.get(property);
            if (value != null && Long.parseLong(value) > 0) {
                return true;
            }
        }
        return false;
    }

    /** The table's name without the catalog's, e.g. {@code db.t}, as Spark procedures take it. */
    private static String databaseAndTable(Table table) {
        String[] parts = table.name().split("\\.");
        return parts.length >= 3
                ? parts[parts.length - 2] + "." + parts[parts.length - 1]
                : table.name();
    }
}
