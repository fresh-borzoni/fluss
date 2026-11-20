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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.writer.DeletionVectorPreparer;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IOUtils;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The deletion vector preparers of a TaskManager share their Iceberg catalog and tables. */
class SharedIcebergTablesTest {

    private static final String DATABASE = "fluss";

    @TempDir private File warehouse;

    private Configuration icebergConfig;

    @BeforeEach
    void beforeEach() {
        icebergConfig = new Configuration();
        icebergConfig.setString("warehouse", "file://" + warehouse);
        icebergConfig.setString("catalog-impl", TestingCountingCatalog.class.getName());
        icebergConfig.setString("name", "test");
        TestingCountingCatalog.reset();
    }

    /**
     * The preparers of several readers (each with its own copy of the tiering factory) for several
     * tables hold one catalog, which loads each table once, and close it with the last of them.
     */
    @Test
    void testPreparersOfReadersAndTablesShareOneCatalog() throws Exception {
        int readers = 4;
        int tables = 3;
        List<TableInfo> tableInfos = new ArrayList<>();
        for (int t = 0; t < tables; t++) {
            tableInfos.add(createTable("t" + t, 3));
        }

        List<DeletionVectorPreparer<IcebergWriteResult>> preparers = new ArrayList<>();
        try {
            for (int r = 0; r < readers; r++) {
                IcebergLakeTieringFactory factory = new IcebergLakeTieringFactory(icebergConfig);
                for (TableInfo tableInfo : tableInfos) {
                    preparers.add(factory.createDeletionVectorPreparer(tableInfo, lakeConfig()));
                }
            }

            assertThat(preparers).hasSize(readers * tables).doesNotContainNull();
            assertThat(TestingCountingCatalog.CREATED.get()).isEqualTo(1);
            assertThat(TestingCountingCatalog.LOADS.get()).isEqualTo(tables);
            assertThat(SharedIcebergTables.INSTANCE.openCatalogs()).isEqualTo(1);

            // closing all but the last preparer of a table keeps the catalog open
            for (int i = 0; i < preparers.size() - 1; i++) {
                preparers.get(i).close();
            }
            assertThat(TestingCountingCatalog.CLOSED.get()).isZero();
        } finally {
            IOUtils.closeAll(preparers);
        }
        assertThat(TestingCountingCatalog.CLOSED.get()).isEqualTo(1);
        assertThat(SharedIcebergTables.INSTANCE.openCatalogs()).isZero();

        // a later preparer opens a catalog again
        try (DeletionVectorPreparer<IcebergWriteResult> preparer =
                new IcebergLakeTieringFactory(icebergConfig)
                        .createDeletionVectorPreparer(tableInfos.get(0), lakeConfig())) {
            assertThat(preparer).isNotNull();
            assertThat(TestingCountingCatalog.CREATED.get()).isEqualTo(2);
        }
        assertThat(TestingCountingCatalog.CLOSED.get()).isEqualTo(2);
    }

    /** A table without deletion vectors, or one the catalog lacks, holds no catalog open. */
    @Test
    void testNoPreparerHoldsNoCatalog() throws Exception {
        TableInfo version2 = createTable("v2", 2);
        IcebergLakeTieringFactory factory = new IcebergLakeTieringFactory(icebergConfig);

        assertThat(factory.createDeletionVectorPreparer(version2, lakeConfig())).isNull();
        assertThatThrownBy(
                        () ->
                                factory.createDeletionVectorPreparer(
                                        tableInfo(TablePath.of(DATABASE, "missing")), lakeConfig()))
                .isInstanceOf(NoSuchTableException.class);

        assertThat(TestingCountingCatalog.CREATED.get()).isEqualTo(2);
        assertThat(TestingCountingCatalog.CLOSED.get()).isEqualTo(2);
        assertThat(SharedIcebergTables.INSTANCE.openCatalogs()).isZero();
    }

    private TableInfo createTable(String name, int formatVersion) throws IOException {
        try (HadoopCatalog catalog =
                new HadoopCatalog(
                        new org.apache.hadoop.conf.Configuration(), "file://" + warehouse)) {
            if (!catalog.namespaceExists(Namespace.of(DATABASE))) {
                catalog.createNamespace(Namespace.of(DATABASE));
            }
            catalog.createTable(
                    TableIdentifier.of(DATABASE, name),
                    new org.apache.iceberg.Schema(
                            Types.NestedField.required(1, "c1", Types.IntegerType.get())),
                    PartitionSpec.unpartitioned(),
                    Collections.singletonMap(
                            TableProperties.FORMAT_VERSION, String.valueOf(formatVersion)));
        }
        return tableInfo(TablePath.of(DATABASE, name));
    }

    private static TableInfo tableInfo(TablePath tablePath) {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("c1", DataTypes.INT())
                                        .primaryKey("c1")
                                        .build())
                        .distributedBy(1, "c1")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DATALAKE_DELETION_VECTORS_ENABLED, true)
                        .build();
        return TableInfo.of(tablePath, 0, 1, descriptor, DEFAULT_REMOTE_DATA_DIR, 1L, 1L);
    }

    private static Configuration lakeConfig() {
        return new Configuration();
    }

    /** A Hadoop catalog that counts the instances created and closed and the tables loaded. */
    public static class TestingCountingCatalog extends HadoopCatalog {
        static final AtomicInteger CREATED = new AtomicInteger();
        static final AtomicInteger CLOSED = new AtomicInteger();
        static final AtomicInteger LOADS = new AtomicInteger();

        static void reset() {
            CREATED.set(0);
            CLOSED.set(0);
            LOADS.set(0);
        }

        @Override
        public void initialize(String name, Map<String, String> properties) {
            super.initialize(name, properties);
            CREATED.incrementAndGet();
        }

        @Override
        public Table loadTable(TableIdentifier identifier) {
            LOADS.incrementAndGet();
            return super.loadTable(identifier);
        }

        @Override
        public void close() throws IOException {
            CLOSED.incrementAndGet();
            super.close();
        }
    }
}
