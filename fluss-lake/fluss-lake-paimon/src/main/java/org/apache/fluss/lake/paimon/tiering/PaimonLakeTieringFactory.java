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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.committer.CommitterInitContext;
import org.apache.fluss.lake.committer.PartitionMarkDoneCommitter;
import org.apache.fluss.lake.paimon.source.PaimonLakeSource;
import org.apache.fluss.lake.paimon.utils.PaimonSnapshotLineage;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.TieringTableValidator;
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
        implements LakeTieringFactory<PaimonWriteResult, PaimonCommittable>, TieringTableValidator {

    private static final long serialVersionUID = 1L;

    private final Configuration paimonConfig;
    private final PaimonCatalogProvider paimonCatalogProvider;

    public PaimonLakeTieringFactory(Configuration paimonConfig) {
        this.paimonConfig = paimonConfig;
        this.paimonCatalogProvider = new PaimonCatalogProvider(paimonConfig);
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
        return new PaimonLakeCommitter(paimonCatalogProvider, committerInitContext);
    }

    @Override
    public boolean supportsPartitionMarkDone() {
        return true;
    }

    @Override
    public SimpleVersionedSerializer<PaimonCommittable> getCommittableSerializer() {
        return new PaimonCommittableSerializer();
    }

    @Override
    public void validateTable(TableInfo tableInfo) {}

    /** Checks that the Paimon table was not rolled back past {@code registeredSnapshotId}. */
    @Override
    public void validateTable(TableInfo tableInfo, @Nullable Long registeredSnapshotId)
            throws IOException {
        if (registeredSnapshotId == null) {
            return;
        }
        try (Catalog catalog = paimonCatalogProvider.get()) {
            FileStoreTable table =
                    (FileStoreTable) catalog.getTable(toPaimon(tableInfo.getLakeTablePath()));
            PaimonSnapshotLineage.checkDescendsFrom(
                    table,
                    tableInfo.getLakeTablePath().toString(),
                    tableInfo.getTableId(),
                    registeredSnapshotId);
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(
                    "Failed to check the Paimon table of " + tableInfo.getTablePath() + ".", e);
        }
    }

    /** Paimon primary key tables merge rows by key in their merge tree. */
    @Override
    public boolean mergesByPrimaryKey() {
        return true;
    }

    @Override
    public LakeSource<?> createLakeSource(TablePath tablePath) {
        return new PaimonLakeSource(paimonConfig, tablePath);
    }
}
