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

package org.apache.fluss.flink.source;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.flink.FlinkConnectorOptions;
import org.apache.fluss.flink.source.reader.LeaseContext;
import org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.testutils.common.MultiVersionTest;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsEpochs;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.SupportsPagedLookup;
import org.apache.flink.table.connector.source.lookup.SupportsVersionedLookup;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** The epochs of a Fluss table source as Flink 2.3 and later see them. */
class Flink23EpochsTest {

    @Test
    @MultiVersionTest
    void versionedTableOffersFlinksEpochAbilities() {
        FlinkTableSource source = versionedSource(true);
        assertThat(source).isInstanceOf(SupportsEpochs.class);
        assertThat(((SupportsEpochs) source).applyEpochs("domain")).isTrue();

        LookupTableSource.LookupRuntimeProvider provider =
                source.getLookupRuntimeProvider(new KeyContext(new int[][] {{0}}));
        assertThat(provider).isInstanceOf(AsyncLookupFunctionProvider.class);
        assertThat(((AsyncLookupFunctionProvider) provider).createAsyncLookupFunction())
                .isInstanceOf(SupportsVersionedLookup.class)
                .isInstanceOf(SupportsPagedLookup.class);
    }

    @Test
    @MultiVersionTest
    void tableWithoutHistoryRefusesEpochs() {
        assertThat(((SupportsEpochs) versionedSource(false)).applyEpochs("domain")).isFalse();
    }

    /** A lookup context with only its keys. */
    private static final class KeyContext implements LookupTableSource.LookupContext {
        private final int[][] keys;

        KeyContext(int[][] keys) {
            this.keys = keys;
        }

        @Override
        public int[][] getKeys() {
            return keys;
        }

        @Override
        public boolean preferCustomShuffle() {
            return false;
        }

        @Override
        public <T> TypeInformation<T> createTypeInformation(DataType producedDataType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> TypeInformation<T> createTypeInformation(LogicalType producedLogicalType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DynamicTableSource.DataStructureConverter createDataStructureConverter(
                DataType producedDataType) {
            throw new UnsupportedOperationException();
        }
    }

    private static FlinkTableSource versionedSource(boolean history) {
        Configuration flussConfig = new Configuration();
        flussConfig.setString(FlinkConnectorOptions.BOOTSTRAP_SERVERS.key(), "localhost:9092");
        Configuration tableConfig = new Configuration();
        tableConfig.set(ConfigOptions.TABLE_KV_MVCC_ENABLED, history);
        FlinkConnectorOptionsUtils.StartupOptions startupOptions =
                new FlinkConnectorOptionsUtils.StartupOptions();
        startupOptions.startupMode = FlinkConnectorOptions.ScanStartupMode.EARLIEST;
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(FlinkConnectorOptions.BUCKET_NUMBER.key(), "1");
        return new FlinkTableSource(
                TablePath.of("db", "t"),
                flussConfig,
                new TableConfig(tableConfig),
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.INT().notNull()),
                                        DataTypes.FIELD("v", DataTypes.INT()))
                                .getLogicalType(),
                new int[] {0},
                new int[] {0},
                new int[0],
                true,
                startupOptions,
                FlinkConnectorOptionsUtils.BoundedOptions.unbounded(),
                true,
                false,
                null,
                LookupOptions.LookupCacheType.NONE,
                1000L,
                FlinkConnectorOptions.SCAN_SPLIT_ASSIGNMENT_BATCH_SIZE.defaultValue(),
                false,
                null,
                tableOptions,
                LeaseContext.DEFAULT);
    }
}
