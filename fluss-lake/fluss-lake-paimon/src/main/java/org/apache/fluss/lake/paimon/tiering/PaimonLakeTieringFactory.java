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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.committer.CommitterInitContext;
import org.apache.fluss.lake.committer.PartitionMarkDoneCommitter;
import org.apache.fluss.lake.paimon.source.PaimonLakeSource;
import org.apache.fluss.lake.paimon.utils.PaimonConversions;
import org.apache.fluss.lake.paimon.utils.PaimonSnapshotLineage;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.SupportsDeletionVectorTiering;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.table.FileStoreTable;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;

/** Implementation of {@link LakeTieringFactory} for Paimon . */
public class PaimonLakeTieringFactory
        implements LakeTieringFactory<PaimonWriteResult, PaimonCommittable>,
                SupportsDeletionVectorTiering {

    private static final long serialVersionUID = 1L;

    private final Configuration paimonConfig;
    private final PaimonCatalogProvider paimonCatalogProvider;
    // what the job's committers keep across rounds, created where they run
    @Nullable private transient PaimonCommitterState committerState;

    public PaimonLakeTieringFactory(Configuration paimonConfig) {
        this.paimonConfig = paimonConfig;
        this.paimonCatalogProvider = new PaimonCatalogProvider(paimonConfig);
    }

    /** Creates a factory whose committers keep their failed commits in {@code committerState}. */
    @VisibleForTesting
    PaimonLakeTieringFactory(Configuration paimonConfig, PaimonCommitterState committerState) {
        this(paimonConfig);
        this.committerState = committerState;
    }

    @Override
    public LakeWriter<PaimonWriteResult> createLakeWriter(WriterInitContext writerInitContext)
            throws IOException {
        return new PaimonLakeWriter(paimonCatalogProvider, writerInitContext);
    }

    @Override
    public SimpleVersionedSerializer<PaimonWriteResult> getWriteResultSerializer() {
        return new PaimonWriteResultSerializer();
    }

    @Override
    public PartitionMarkDoneCommitter<PaimonWriteResult, PaimonCommittable> createLakeCommitter(
            CommitterInitContext committerInitContext) throws IOException {
        return new PaimonLakeCommitter(
                paimonCatalogProvider, committerInitContext, committerState());
    }

    @Override
    public boolean supportsPartitionMarkDone() {
        return true;
    }

    @Override
    public SimpleVersionedSerializer<PaimonCommittable> getCommittableSerializer() {
        return new PaimonCommittableSerializer();
    }

    /**
     * Checks a table with deletion vectors: its Paimon options, which another engine may have
     * changed since Fluss created the table, and that the Paimon table was not rolled back past
     * {@code registeredSnapshotId}.
     */
    @Override
    public void validateDeletionVectorTable(
            TableInfo tableInfo, @Nullable Long registeredSnapshotId) throws IOException {
        try (Catalog catalog = paimonCatalogProvider.get()) {
            FileStoreTable table =
                    (FileStoreTable) catalog.getTable(toPaimon(tableInfo.getLakeTablePath()));
            PaimonConversions.validateDeletionVectorsTable(tableInfo.getLakeTablePath(), table);
            if (registeredSnapshotId != null) {
                PaimonSnapshotLineage.checkDescendsFrom(
                        table,
                        tableInfo.getLakeTablePath().toString(),
                        tableInfo.getTableId(),
                        registeredSnapshotId);
            }
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(
                    "Failed to check the Paimon table of " + tableInfo.getTablePath() + ".", e);
        }
    }

    private synchronized PaimonCommitterState committerState() {
        if (committerState == null) {
            committerState = new PaimonCommitterState();
        }
        return committerState;
    }

    /** Paimon primary key tables merge rows by key in their merge tree. */
    @Override
    public boolean mergesByPrimaryKey() {
        return true;
    }

    @Override
    public LakeSource<?> createLakeSource(TablePath lakeTablePath) {
        return new PaimonLakeSource(paimonConfig, lakeTablePath);
    }
}
