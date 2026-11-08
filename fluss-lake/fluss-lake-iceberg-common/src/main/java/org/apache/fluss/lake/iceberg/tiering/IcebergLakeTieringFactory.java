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
    // what the job's committers keep across rounds, created where they run
    @Nullable private transient IcebergCommitterState committerState;

    public IcebergLakeTieringFactory(Configuration icebergConfig) {
        this.icebergConfig = icebergConfig;
        this.icebergCatalogProvider = new IcebergCatalogProvider(icebergConfig);
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
     * Checks the Iceberg table of a table with deletion vectors: no rollback past {@code
     * registeredSnapshotId}.
     */
    @Override
    public void validateDeletionVectorTable(
            TableInfo tableInfo, @Nullable Long registeredSnapshotId) throws IOException {
        if (registeredSnapshotId == null) {
            return;
        }
        Catalog icebergCatalog = icebergCatalogProvider.get();
        try {
            Table icebergTable = icebergCatalog.loadTable(toIceberg(tableInfo.getLakeTablePath()));
            IcebergSnapshotLineage.checkDescendsFrom(icebergTable, registeredSnapshotId);
        } finally {
            closeQuietly(icebergCatalog);
        }
    }

    private static void closeQuietly(Catalog icebergCatalog) {
        if (icebergCatalog instanceof AutoCloseable) {
            IOUtils.closeQuietly((AutoCloseable) icebergCatalog, "iceberg-catalog-table-validator");
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
                committerState());
    }

    private synchronized IcebergCommitterState committerState() {
        if (committerState == null) {
            committerState = new IcebergCommitterState();
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
