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

/**
 * Thrown when the lake table of a table with deletion vectors was changed outside Fluss in a way
 * Fluss cannot follow. Either it was rolled back past the latest snapshot Fluss registered ({@link
 * LakeTableRolledBackException}), or another engine changed its rows after that snapshot, e.g. with
 * an INSERT, UPDATE, DELETE or MERGE. Rewrites that keep the rows, such as compactions, are
 * allowed.
 *
 * <p>Fluss derives the table's deletion vectors from the rows it tiered, so the table is not tiered
 * until the change is undone. The message names the snapshot and says how.
 */
@Internal
public class LakeTableExternallyModifiedException extends FlussRuntimeException {

    private static final long serialVersionUID = 1L;

    public LakeTableExternallyModifiedException(String message) {
        super(message);
    }

    /**
     * Returns the error of Iceberg table {@code table} whose snapshot {@code snapshotId}, the first
     * after {@code registeredSnapshotId} that changed rows outside Fluss, has the operation {@code
     * operation} and parent {@code parentSnapshotId}.
     */
    public static LakeTableExternallyModifiedException ofExternalWrite(
            String table,
            long snapshotId,
            String operation,
            String committedBy,
            long parentSnapshotId,
            long registeredSnapshotId) {
        return new LakeTableExternallyModifiedException(
                String.format(
                        "Tiering of %s stopped: snapshot %d of its lake table changed rows outside"
                                + " Fluss (operation '%s', committed by '%s') after snapshot %d,"
                                + " the latest one Fluss registered. %s To resume, make snapshot"
                                + " %d, the one before it, the lake table's current snapshot"
                                + " again, e.g. in Spark: CALL"
                                + " <catalog>.system.set_current_snapshot('%s', %d). Rewrites that"
                                + " keep the rows, such as rewrite_data_files or"
                                + " rewrite_position_delete_files, are allowed. To change rows,"
                                + " write them to the Fluss table instead.",
                        table,
                        snapshotId,
                        operation,
                        committedBy,
                        registeredSnapshotId,
                        cannotFollowWrites(),
                        parentSnapshotId,
                        table,
                        parentSnapshotId));
    }

    /**
     * Returns the error of Paimon table {@code table} whose snapshot {@code snapshotId}, the first
     * after {@code registeredSnapshotId} that wrote rows outside Fluss, is described by {@code
     * commit}.
     */
    public static LakeTableExternallyModifiedException ofPaimonExternalWrite(
            String table, long snapshotId, String commit, long registeredSnapshotId) {
        return new LakeTableExternallyModifiedException(
                String.format(
                        "Tiering of %s stopped: snapshot %d of its Paimon table, %s, wrote rows"
                                + " outside Fluss after snapshot %d, the latest one Fluss"
                                + " registered. %s To resume, roll the table back to snapshot %d,"
                                + " the one before it, e.g. in Flink SQL: CALL"
                                + " sys.rollback_to('%s', %d). Compactions are allowed. To change"
                                + " rows, write them to the Fluss table instead.",
                        table,
                        snapshotId,
                        commit,
                        registeredSnapshotId,
                        cannotFollowWrites(),
                        snapshotId - 1,
                        table,
                        snapshotId - 1));
    }

    private static String cannotFollowWrites() {
        return "Fluss derives the table's deletion vectors from the rows it tiered and cannot"
                + " follow rows another engine writes, so the table is not tiered until the write"
                + " is undone; no data is lost in Fluss.";
    }
}
