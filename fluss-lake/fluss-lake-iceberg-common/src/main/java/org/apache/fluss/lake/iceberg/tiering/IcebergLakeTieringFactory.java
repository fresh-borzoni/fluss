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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.committer.CommitterInitContext;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.iceberg.source.IcebergLakeSource;
import org.apache.fluss.lake.iceberg.utils.IcebergSnapshotLineage;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.SupportsDeletionVectorTiering;
import org.apache.fluss.lake.writer.TieringTableValidator;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.IOUtils;

import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.catalog.Catalog;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;

/** Implementation of {@link LakeTieringFactory} for Iceberg. */
public class IcebergLakeTieringFactory
        implements LakeTieringFactory<IcebergWriteResult, IcebergCommittable>,
                TieringTableValidator,
                SupportsDeletionVectorTiering {

    private static final long serialVersionUID = 1L;

    private final Configuration icebergConfig;
    private final IcebergCatalogProvider icebergCatalogProvider;
    private final long expireIntervalMs;
    // what the job's committers keep across rounds, created where they run
    @Nullable private transient IcebergCommitterState committerState;

    public IcebergLakeTieringFactory(Configuration icebergConfig) {
        this(icebergConfig, IcebergCommitterState.DEFAULT_EXPIRE_INTERVAL_MS);
    }

    /** Creates a factory whose committers expire a table's snapshots at most this often. */
    @VisibleForTesting
    public IcebergLakeTieringFactory(Configuration icebergConfig, long expireIntervalMs) {
        this.icebergConfig = icebergConfig;
        this.icebergCatalogProvider = new IcebergCatalogProvider(icebergConfig);
        this.expireIntervalMs = expireIntervalMs;
    }

    @Override
    public LakeWriter<IcebergWriteResult> createLakeWriter(WriterInitContext writerInitContext)
            throws IOException {
        return new IcebergLakeWriter(icebergCatalogProvider, writerInitContext);
    }

    @Override
    public void validateTable(TableInfo tableInfo) throws IOException {
        Catalog icebergCatalog = icebergCatalogProvider.get();
        try {
            Table icebergTable = icebergCatalog.loadTable(toIceberg(tableInfo.getLakeTablePath()));
            IcebergPartitionSpecValidator.validate(icebergTable, tableInfo);
        } finally {
            closeQuietly(icebergCatalog);
        }
    }

    /**
     * Checks the Iceberg table of a table with deletion vectors: format version 2 or later, no rows
     * written by another engine before Fluss tiered any, and no rollback past {@code
     * registeredSnapshotId}.
     */
    @Override
    public void validateDeletionVectorTable(
            TableInfo tableInfo, @Nullable Long registeredSnapshotId) throws IOException {
        Catalog icebergCatalog = icebergCatalogProvider.get();
        try {
            Table icebergTable = icebergCatalog.loadTable(toIceberg(tableInfo.getLakeTablePath()));
            checkDeletesByPosition(icebergTable, tableInfo);
            if (registeredSnapshotId == null) {
                // nothing registered yet: no other engine may have written rows before
                IcebergSnapshotLineage.checkNoExternalWrites(
                        icebergTable, icebergTable.currentSnapshot());
            } else {
                IcebergSnapshotLineage.checkDescendsFrom(icebergTable, registeredSnapshotId);
            }
        } finally {
            closeQuietly(icebergCatalog);
        }
    }

    private static void closeQuietly(Catalog icebergCatalog) {
        if (icebergCatalog instanceof AutoCloseable) {
            IOUtils.closeQuietly((AutoCloseable) icebergCatalog, "iceberg-catalog-table-validator");
        }
    }

    /**
     * Refuses a table with deletion vectors whose lake table has Iceberg format version 1, which
     * has no deletes by position: every round would fail at its first delete.
     */
    static void checkDeletesByPosition(Table icebergTable, TableInfo tableInfo) throws IOException {
        int formatVersion = TableUtil.formatVersion(icebergTable);
        if (formatVersion < 2) {
            throw new IOException(
                    String.format(
                            "Iceberg table %s has format version %d, which has no deletes by"
                                    + " position, so the Fluss table %s with '%s' is not tiered."
                                    + " Upgrade the Iceberg table to format version 2 or 3.",
                            icebergTable.name(),
                            formatVersion,
                            tableInfo.getTablePath(),
                            ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key()));
        }
    }

    @Override
    public SimpleVersionedSerializer<IcebergWriteResult> getWriteResultSerializer() {
        return new IcebergWriteResultSerializer();
    }

    @Override
    public LakeCommitter<IcebergWriteResult, IcebergCommittable> createLakeCommitter(
            CommitterInitContext committerInitContext) throws IOException {
        return new IcebergLakeCommitter(
                icebergCatalogProvider,
                committerInitContext.tableInfo().getLakeTablePath(),
                // Only the committer of a table with deletion vectors expires snapshots; it keeps
                // those the index chain needs. The engine expires the snapshots of other tables.
                committerInitContext.tableInfo().isDeletionVectorsEnabled()
                        && (committerInitContext
                                        .tableInfo()
                                        .getTableConfig()
                                        .isDataLakeAutoExpireSnapshot()
                                || committerInitContext
                                        .lakeTieringConfig()
                                        .get(ConfigOptions.LAKE_TIERING_AUTO_EXPIRE_SNAPSHOT)),
                committerState());
    }

    private synchronized IcebergCommitterState committerState() {
        if (committerState == null) {
            committerState = new IcebergCommitterState(expireIntervalMs);
        }
        return committerState;
    }

    @Override
    public SimpleVersionedSerializer<IcebergCommittable> getCommittableSerializer() {
        return new IcebergCommittableSerializer();
    }

    /** Creates the lake source of the Iceberg table at the given lake table path. */
    @Override
    public LakeSource<?> createLakeSource(TablePath tablePath) {
        return new IcebergLakeSource(icebergConfig, tablePath);
    }
}
