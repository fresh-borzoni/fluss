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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.lake.writer.TieringTableValidator;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The tiering of a Paimon table with deletion vectors checks the Paimon table's options, which
 * another engine may have changed after Fluss created the table, before it writes any data.
 */
class PaimonDvTableOptionsValidationTest {

    private static final TablePath TABLE_PATH = TablePath.of("fluss", "dv_options");

    @TempDir private File warehouse;

    private Configuration paimonConfig;
    private Catalog catalog;
    private Identifier identifier;

    @BeforeEach
    void setUp() throws Exception {
        paimonConfig = new Configuration();
        paimonConfig.setString("metastore", "filesystem");
        paimonConfig.setString("warehouse", warehouse.getAbsolutePath());
        catalog = new PaimonCatalogProvider(paimonConfig).get();
        catalog.createDatabase(TABLE_PATH.getDatabaseName(), true);
        identifier = Identifier.create(TABLE_PATH.getDatabaseName(), TABLE_PATH.getTableName());
        catalog.createTable(
                identifier,
                Schema.newBuilder()
                        .column("c1", DataTypes.INT().notNull())
                        .column("c2", DataTypes.STRING())
                        .primaryKey("c1")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                        .build(),
                false);
    }

    @AfterEach
    void tearDown() throws Exception {
        catalog.close();
    }

    @Test
    void testTableAsFlussCreatedItPasses() throws Exception {
        validator().validateDeletionVectorTable(tableInfo(true), null);
        // a table without deletion vectors is not validated
        assertThat(validator()).isNotInstanceOf(TieringTableValidator.class);
    }

    @ParameterizedTest
    @CsvSource({
        "ignore-delete,true,'ignore-delete'",
        "first-row.ignore-delete,true,'ignore-delete'",
        "deduplicate.ignore-delete,true,'ignore-delete'",
        "partial-update.ignore-delete,true,'ignore-delete'",
        "deletion-vectors.enabled,false,'deletion-vectors.enabled'"
    })
    void testOptionsChangedOutsideFlussStopTiering(String option, String value, String reported)
            throws Exception {
        catalog.alterTable(identifier, SchemaChange.setOption(option, value), false);

        // Checked before anything is tiered, too.
        assertThatThrownBy(() -> validator().validateDeletionVectorTable(tableInfo(true), null))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key())
                .hasMessageContaining(reported)
                .hasMessageContaining(TABLE_PATH.toString());
    }

    private PaimonLakeTieringFactory validator() {
        return new PaimonLakeTieringFactory(paimonConfig);
    }

    private static TableInfo tableInfo(boolean deletionVectors) {
        return TableInfo.of(
                TABLE_PATH,
                7L,
                1,
                TableDescriptor.builder()
                        .schema(
                                org.apache.fluss.metadata.Schema.newBuilder()
                                        .column("c1", org.apache.fluss.types.DataTypes.INT())
                                        .column("c2", org.apache.fluss.types.DataTypes.STRING())
                                        .primaryKey("c1")
                                        .build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED, deletionVectors)
                        .build(),
                "/remote",
                System.currentTimeMillis(),
                System.currentTimeMillis());
    }
}
