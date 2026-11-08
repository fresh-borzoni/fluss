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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * Lets a commit land only on a given base snapshot, or on a descendant of it whose snapshots since
 * the base changed no data file, such as a rewrite of manifests or of position delete files.
 *
 * <p>A round's deletes and index change are relative to its base. Iceberg's validations reject data
 * files added since the base by an append or overwrite, and the removal of files the deletes
 * reference. They do not reject a rewrite of other files: Iceberg's retry would rebase the round on
 * top of it, and the round's index change would miss the rewrite. The check runs on every commit
 * attempt, against the metadata the catalog swaps in atomically, so no other commit can land in
 * between. A failed check throws {@link BaseMovedException}, which Iceberg does not retry.
 */
final class IcebergBaseFence {

    private IcebergBaseFence() {}

    /**
     * Returns a view of {@code table} whose commits land on {@code baseSnapshotId} only. With a
     * null {@code baseSnapshotId}, they land only on a table whose snapshots changed no data file.
     */
    static Table on(Table table, @Nullable Long baseSnapshotId) {
        if (!(table instanceof HasTableOperations)) {
            throw new IllegalStateException(
                    "Iceberg table "
                            + table.name()
                            + " exposes no table operations to fence its commits on snapshot "
                            + baseSnapshotId
                            + ".");
        }
        TableOperations ops = ((HasTableOperations) table).operations();
        return new BaseTable(new FencedOperations(ops, baseSnapshotId), table.name());
    }

    /**
     * Describes the first snapshot after {@code baseSnapshotId}, up to {@code parentId}, that
     * changed data files. Returns null if there is none and {@code parentId} descends from the
     * base. A null base is the empty table, so every snapshot up to {@code parentId} is after it.
     */
    @Nullable
    static String firstDataChangeSince(
            TableMetadata metadata, @Nullable Long parentId, @Nullable Long baseSnapshotId) {
        String first = null;
        Long id = parentId;
        while (id != null && !id.equals(baseSnapshotId)) {
            Snapshot s = metadata.snapshot(id);
            if (s == null) {
                return "expired snapshot " + id;
            }
            if (!changesNoDataFile(s)) {
                first = String.format("snapshot %d (%s)", s.snapshotId(), s.operation());
            }
            id = s.parentId();
        }
        if (id == null && baseSnapshotId != null) {
            return String.format(
                    "snapshot %s, which does not descend from snapshot %d",
                    parentId, baseSnapshotId);
        }
        return first;
    }

    /** Whether a snapshot rewrote no data file, e.g. it rewrote manifests or delete files. */
    static boolean changesNoDataFile(Snapshot snapshot) {
        Map<String, String> summary = snapshot.summary();
        return DataOperations.REPLACE.equals(snapshot.operation())
                && summary != null
                && count(summary, SnapshotSummary.ADDED_FILES_PROP) == 0
                && count(summary, SnapshotSummary.DELETED_FILES_PROP) == 0;
    }

    private static long count(Map<String, String> summary, String property) {
        String value = summary.get(property);
        return value == null ? 0 : Long.parseLong(value);
    }

    /** A commit that would not land on its base snapshot; Iceberg does not retry it. */
    static final class BaseMovedException extends ValidationException {

        private static final long serialVersionUID = 1L;

        BaseMovedException(String message) {
            super("%s", message);
        }
    }

    /** Delegates to the table's operations, checking each commit's parent first. */
    private static final class FencedOperations implements TableOperations {

        private final TableOperations delegate;
        @Nullable private final Long baseSnapshotId;

        private FencedOperations(TableOperations delegate, @Nullable Long baseSnapshotId) {
            this.delegate = delegate;
            this.baseSnapshotId = baseSnapshotId;
        }

        @Override
        public void commit(TableMetadata base, TableMetadata metadata) {
            Snapshot committed = metadata.currentSnapshot();
            boolean newSnapshot =
                    committed != null
                            && (base == null
                                    || base.currentSnapshot() == null
                                    || base.currentSnapshot().snapshotId()
                                            != committed.snapshotId());
            if (newSnapshot) {
                String moved = firstDataChangeSince(metadata, committed.parentId(), baseSnapshotId);
                if (moved != null) {
                    throw new BaseMovedException(
                            String.format(
                                    "Snapshot %d would follow %s rather than %s.",
                                    committed.snapshotId(),
                                    moved,
                                    baseSnapshotId == null
                                            ? "a table without data"
                                            : "its base snapshot " + baseSnapshotId));
                }
            }
            delegate.commit(base, metadata);
        }

        @Override
        public TableMetadata current() {
            return delegate.current();
        }

        @Override
        public TableMetadata refresh() {
            return delegate.refresh();
        }

        @Override
        public FileIO io() {
            return delegate.io();
        }

        @Override
        public EncryptionManager encryption() {
            return delegate.encryption();
        }

        @Override
        public String metadataFileLocation(String fileName) {
            return delegate.metadataFileLocation(fileName);
        }

        @Override
        public LocationProvider locationProvider() {
            return delegate.locationProvider();
        }

        @Override
        public TableOperations temp(TableMetadata uncommittedMetadata) {
            return delegate.temp(uncommittedMetadata);
        }

        @Override
        public long newSnapshotId() {
            return delegate.newSnapshotId();
        }

        @Override
        public boolean requireStrictCleanup() {
            return delegate.requireStrictCleanup();
        }
    }
}
