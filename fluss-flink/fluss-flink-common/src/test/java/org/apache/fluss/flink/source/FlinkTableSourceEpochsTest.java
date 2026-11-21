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
import org.apache.fluss.flink.adapter.SupportsVersionedLookupAdapter;
import org.apache.fluss.flink.source.reader.LeaseContext;
import org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.PartialCachingAsyncLookupProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.runtime.connector.source.LookupRuntimeProviderContext;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the epochs that a {@link FlinkTableSource} cuts its changes into. */
class FlinkTableSourceEpochsTest {

    private static final RowType ROW_TYPE =
            (RowType)
                    DataTypes.ROW(
                                    DataTypes.FIELD("id", DataTypes.INT().notNull()),
                                    DataTypes.FIELD("v", DataTypes.INT().notNull()))
                            .getLogicalType();

    @Test
    void versionedPrimaryKeyTableCutsEpochs() {
        FlinkTableSource source = new Source().create();
        assertThat(source.getEpochDomain()).isNull();
        assertThat(source.applyEpochs("domain")).isTrue();
        assertThat(source.getEpochDomain()).isEqualTo("domain");
        assertThat(((FlinkTableSource) source.copy()).getEpochDomain()).isEqualTo("domain");
    }

    @Test
    void tableThatCannotCutEpochsRefusesThem() {
        Source[] refused = {
            new Source().withoutPrimaryKey(),
            new Source().withoutHistory(),
            new Source().inBatchMode(),
            new Source().partitioned(),
            new Source().withCache(LookupOptions.LookupCacheType.FULL),
            new Source().insertingMissingKeys(),
            new Source().withWatermarks()
        };
        for (Source definition : refused) {
            FlinkTableSource source = definition.create();
            assertThat(source.applyEpochs("domain")).as(definition.toString()).isFalse();
            assertThat(source.getEpochDomain()).isNull();
        }
    }

    @Test
    void lookupsWithEpochsReadAsOfThemWithoutCache() {
        FlinkTableSource source =
                new Source().withCache(LookupOptions.LookupCacheType.PARTIAL).create();
        LookupTableSource.LookupContext context =
                new LookupRuntimeProviderContext(new int[][] {{0}});
        // without epochs, a partial cache serves the lookups, here synchronous ones
        assertThat(source.getLookupRuntimeProvider(context))
                .isInstanceOf(PartialCachingLookupProvider.class);

        assertThat(source.applyEpochs("domain")).isTrue();
        LookupTableSource.LookupRuntimeProvider provider = source.getLookupRuntimeProvider(context);
        assertThat(provider)
                .isInstanceOf(AsyncLookupFunctionProvider.class)
                .isNotInstanceOf(PartialCachingAsyncLookupProvider.class);
        assertThat(((AsyncLookupFunctionProvider) provider).createAsyncLookupFunction())
                .isInstanceOf(SupportsVersionedLookupAdapter.class);
    }

    /** The definition of a source, versioned with a primary key unless changed. */
    private static final class Source {
        boolean primaryKey = true;
        boolean history = true;
        boolean streaming = true;
        boolean partitioned;
        boolean insertIfNotExists;
        boolean watermarks;
        LookupOptions.LookupCacheType cacheType = LookupOptions.LookupCacheType.NONE;

        Source withoutPrimaryKey() {
            primaryKey = false;
            return this;
        }

        Source withoutHistory() {
            history = false;
            return this;
        }

        Source inBatchMode() {
            streaming = false;
            return this;
        }

        Source partitioned() {
            partitioned = true;
            return this;
        }

        Source insertingMissingKeys() {
            insertIfNotExists = true;
            return this;
        }

        Source withWatermarks() {
            watermarks = true;
            return this;
        }

        Source withCache(LookupOptions.LookupCacheType cacheType) {
            this.cacheType = cacheType;
            return this;
        }

        FlinkTableSource create() {
            Configuration flussConfig = new Configuration();
            flussConfig.setString(FlinkConnectorOptions.BOOTSTRAP_SERVERS.key(), "localhost:9092");
            Configuration tableConfig = new Configuration();
            tableConfig.set(ConfigOptions.TABLE_KV_MVCC_ENABLED, history);
            FlinkConnectorOptionsUtils.StartupOptions startupOptions =
                    new FlinkConnectorOptionsUtils.StartupOptions();
            startupOptions.startupMode = FlinkConnectorOptions.ScanStartupMode.EARLIEST;
            Map<String, String> tableOptions = new HashMap<>();
            tableOptions.put(FlinkConnectorOptions.BUCKET_NUMBER.key(), "1");
            FlinkTableSource source =
                    new FlinkTableSource(
                            TablePath.of("db", "t"),
                            flussConfig,
                            new TableConfig(tableConfig),
                            ROW_TYPE,
                            primaryKey
                                    ? (partitioned ? new int[] {0, 1} : new int[] {0})
                                    : new int[0],
                            new int[] {0},
                            partitioned ? new int[] {1} : new int[0],
                            streaming,
                            startupOptions,
                            FlinkConnectorOptionsUtils.BoundedOptions.unbounded(),
                            false,
                            insertIfNotExists,
                            cacheType == LookupOptions.LookupCacheType.PARTIAL
                                    ? DefaultLookupCache.newBuilder().maximumSize(10).build()
                                    : null,
                            cacheType,
                            1000L,
                            FlinkConnectorOptions.SCAN_SPLIT_ASSIGNMENT_BATCH_SIZE.defaultValue(),
                            false,
                            null,
                            tableOptions,
                            LeaseContext.DEFAULT);
            if (watermarks) {
                source.applyWatermark(WatermarkStrategy.forMonotonousTimestamps());
            }
            return source;
        }

        @Override
        public String toString() {
            return String.format(
                    "primaryKey=%s, history=%s, streaming=%s, partitioned=%s, insert=%s, cache=%s,"
                            + " watermarks=%s",
                    primaryKey,
                    history,
                    streaming,
                    partitioned,
                    insertIfNotExists,
                    cacheType,
                    watermarks);
        }
    }
}
