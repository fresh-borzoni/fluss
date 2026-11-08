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

package org.apache.fluss.exception;

import org.apache.fluss.annotation.Internal;

import javax.annotation.Nullable;

/**
 * Thrown when a lake table with deletion vectors was rolled back outside Fluss: its current
 * snapshot is neither the latest snapshot Fluss registered nor a descendant of it.
 *
 * <p>Fluss derives the table's deletion vectors from the rows of the registered snapshot and cannot
 * follow a rollback. The table is not tiered until the rollback is undone; the message says how.
 */
@Internal
public class LakeTableRolledBackException extends LakeTableExternallyModifiedException {

    private static final long serialVersionUID = 1L;

    public LakeTableRolledBackException(String message) {
        super(message);
    }

    /**
     * Returns the error of lake table {@code table} whose current snapshot ({@code null} if none)
     * is neither {@code registeredSnapshotId} nor a descendant of it.
     */
    public static LakeTableRolledBackException of(
            String table, @Nullable Long currentSnapshotId, long registeredSnapshotId) {
        return new LakeTableRolledBackException(
                String.format(
                        "Tiering of %s stopped: its lake table was rolled back outside Fluss. Its"
                                + " current snapshot %s is not snapshot %d, the latest one Fluss"
                                + " registered, nor a descendant of it. %s To resume, make snapshot"
                                + " %d (or a later snapshot of its history) the lake table's"
                                + " current snapshot again, e.g. in Spark: CALL"
                                + " <catalog>.system.set_current_snapshot('%s', %d). To remove"
                                + " rows, delete them in Fluss instead.",
                        table,
                        currentSnapshotId == null ? "(none)" : currentSnapshotId,
                        registeredSnapshotId,
                        cannotFollow(registeredSnapshotId),
                        registeredSnapshotId,
                        table,
                        registeredSnapshotId));
    }

    /**
     * Returns the error of a union read of lake table {@code table} whose readable snapshot {@code
     * snapshotId} names another commit now ({@code found}) than when Fluss registered it ({@code
     * registered}). The table was rolled back past it and the id reused, so the snapshot's masks do
     * not apply to the rows the id names now.
     */
    public static LakeTableRolledBackException ofReusedReadableSnapshot(
            String table, long snapshotId, String registered, String found) {
        return new LakeTableRolledBackException(
                String.format(
                        "Union read of %s stopped: its lake table was rolled back outside Fluss."
                                + " Snapshot %d, the readable snapshot Fluss registered as the %s,"
                                + " is the %s now, so its deletion vectors do not apply to it. To"
                                + " resume, make the registered snapshot (or a later snapshot of"
                                + " its history) the table's latest snapshot again, e.g. restore"
                                + " it from a branch created before the rollback with Flink SQL's"
                                + " CALL sys.fast_forward('%s', '<branch>'). To remove rows,"
                                + " delete them in Fluss instead.",
                        table, snapshotId, registered, found, table));
    }

    /**
     * Returns the error of Paimon table {@code table} whose latest snapshot {@code
     * currentSnapshotId} does not descend from {@code registeredSnapshotId}. Paimon's rollback
     * deletes the later snapshots and reuses their ids. {@code replacedBy} describes the commit
     * that holds the registered id now, or is null if the table has no snapshot with that id.
     */
    public static LakeTableRolledBackException ofPaimon(
            String table,
            long currentSnapshotId,
            long registeredSnapshotId,
            @Nullable String replacedBy) {
        String what =
                replacedBy == null
                        ? String.format(
                                "Its latest snapshot %d is older than snapshot %d, the latest one"
                                        + " Fluss registered.",
                                currentSnapshotId, registeredSnapshotId)
                        : String.format(
                                "Snapshot %d, the latest one Fluss registered, is not in its"
                                        + " history any more: the table was rolled back past it"
                                        + " and the id was reused by %s. Its latest snapshot is"
                                        + " %d.",
                                registeredSnapshotId, replacedBy, currentSnapshotId);
        return new LakeTableRolledBackException(
                String.format(
                        "Tiering of %s stopped: its Paimon table was rolled back outside Fluss. %s"
                                + " %s To resume, make snapshot %d (or a later snapshot of its"
                                + " history) the table's latest snapshot again. Paimon's rollback"
                                + " deleted the later snapshots and tags, so restore them from a"
                                + " branch created on snapshot %d or later before the rollback,"
                                + " e.g. in Flink SQL: CALL sys.fast_forward('%s', '<branch>'), or"
                                + " from a copy of the table's snapshot directory. To remove rows,"
                                + " delete them in Fluss instead.",
                        table,
                        what,
                        cannotFollow(registeredSnapshotId),
                        registeredSnapshotId,
                        registeredSnapshotId,
                        table));
    }

    private static String cannotFollow(long registeredSnapshotId) {
        return String.format(
                "Fluss derives the table's deletion vectors from the rows of snapshot %d and cannot"
                        + " follow a rollback, so the table is not tiered until the rollback is"
                        + " undone; no data is lost in Fluss.",
                registeredSnapshotId);
    }
}
