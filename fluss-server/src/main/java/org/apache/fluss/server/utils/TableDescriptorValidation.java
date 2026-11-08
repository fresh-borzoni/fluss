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

package org.apache.fluss.server.utils;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.compression.LogCompressionType;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.ReadableConfig;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.TooManyBucketsException;
import org.apache.fluss.metadata.AggFunction;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.AutoPartitionStrategy;
import org.apache.fluss.utils.StringUtils;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.config.ConfigOptions.CURRENT_KV_FORMAT_VERSION;
import static org.apache.fluss.config.FlussConfigUtils.TABLE_OPTIONS;
import static org.apache.fluss.config.FlussConfigUtils.isAlterableTableOption;
import static org.apache.fluss.config.FlussConfigUtils.isTableStorageConfig;
import static org.apache.fluss.config.StatisticsConfigUtils.validateStatisticsConfig;
import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.CHANGE_TYPE_COLUMN;
import static org.apache.fluss.metadata.TableDescriptor.COMMIT_TIMESTAMP_COLUMN;
import static org.apache.fluss.metadata.TableDescriptor.LOG_OFFSET_COLUMN;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.apache.fluss.utils.PartitionUtils.PARTITION_KEY_SUPPORTED_TYPES;
import static org.apache.fluss.utils.PartitionUtils.validateTimeFormat;

/** Validator of {@link TableDescriptor}. */
public class TableDescriptorValidation {

    private static final Set<String> SYSTEM_COLUMNS =
            Collections.unmodifiableSet(
                    new LinkedHashSet<>(
                            Arrays.asList(
                                    OFFSET_COLUMN_NAME,
                                    TIMESTAMP_COLUMN_NAME,
                                    BUCKET_COLUMN_NAME,
                                    CHANGE_TYPE_COLUMN,
                                    LOG_OFFSET_COLUMN,
                                    COMMIT_TIMESTAMP_COLUMN)));

    private static final List<DataTypeRoot> KEY_UNSUPPORTED_TYPES =
            Arrays.asList(DataTypeRoot.ARRAY, DataTypeRoot.MAP, DataTypeRoot.ROW);

    /** Validate table descriptor to create is valid and contain all necessary information. */
    public static void validateTableDescriptor(
            TableDescriptor tableDescriptor,
            int maxBucketNum,
            @Nullable DataLakeFormat clusterDataLakeFormat) {
        Schema schema = tableDescriptor.getSchema();
        boolean hasPrimaryKey = schema.getPrimaryKey().isPresent();
        Configuration tableConf = Configuration.fromMap(tableDescriptor.getProperties());

        // check properties should only contain table.* options,
        // and this cluster know it, and value is valid
        for (String key : tableConf.keySet()) {
            ConfigOption<?> option = TABLE_OPTIONS.get(key);
            if (option != null) {
                validateOptionValue(tableConf, option);
                continue;
            }

            if (isTableStorageConfig(key)) {
                throw new InvalidConfigException(
                        String.format(
                                "'%s' is not a recognized Fluss table property in the current cluster version. "
                                        + "You may be using an older Fluss cluster that does not support this property.",
                                key));
            } else {
                throw new InvalidConfigException(
                        String.format(
                                "'%s' is not a Fluss table property. Please use '.customProperty(..)' to set custom properties.",
                                key));
            }
        }

        KafkaTableSchema.validate(tableDescriptor);

        // check distribution
        checkDistribution(tableDescriptor, maxBucketNum);
        checkPrimaryKey(tableDescriptor);
        // check individual options
        checkReplicationFactor(tableConf);
        checkLogFormat(tableConf, hasPrimaryKey);
        checkArrowCompression(tableConf);
        checkCompactedCompression(tableConf);
        checkMergeEngine(tableConf, hasPrimaryKey, schema);
        checkUpsertSkipUnchanged(tableConf, hasPrimaryKey);
        checkDeleteBehavior(tableConf, hasPrimaryKey);
        checkTieredLog(tableConf);
        checkHistoricalPartition(tableDescriptor, tableConf);
        checkKvTTL(tableConf, schema, hasPrimaryKey);
        checkKvFormatVersion(tableConf);
        checkKvValueLayout(tableConf, hasPrimaryKey);
        checkPartition(tableConf, tableDescriptor.getPartitionKeys(), schema.getRowType());
        checkSystemColumns(schema.getRowType());
        validateStatisticsConfig(tableDescriptor);
        checkTableLakeFormatMatchesCluster(tableConf, clusterDataLakeFormat);
        checkCustomLakePathSupported(tableConf, clusterDataLakeFormat);
        checkDeletionVectors(tableDescriptor, tableConf, clusterDataLakeFormat);
        checkAutoCompaction(tableConf);
    }

    /** Validates the schema after altering table columns. */
    @Internal
    public static void validateAlterTableSchema(TableInfo table, Schema newSchema) {
        checkSystemColumns(newSchema.getRowType());
        if (table.getTableConfig()
                .getMergeEngineType()
                .map(MergeEngineType.AGGREGATION::equals)
                .orElse(false)) {
            validateAggregationFunctionParameters(newSchema);
        } else {
            validateNoAggregationFunctions(newSchema);
        }
        TableDescriptor updated =
                TableDescriptor.builder(table.toTableDescriptor()).schema(newSchema).build();
        KafkaTableSchema.validate(updated);
        KafkaTableSchema.validateTablePath(table.getTablePath(), updated);
    }

    private static void checkTableLakeFormatMatchesCluster(
            Configuration tableConf, @Nullable DataLakeFormat clusterDataLakeFormat) {
        if (clusterDataLakeFormat == null) {
            return;
        }

        Optional<DataLakeFormat> tableDataLakeFormat =
                tableConf.getOptional(ConfigOptions.TABLE_DATALAKE_FORMAT);
        if (tableDataLakeFormat.isPresent() && tableDataLakeFormat.get() != clusterDataLakeFormat) {
            throw new InvalidConfigException(
                    String.format(
                            "'%s' ('%s') must match cluster '%s' ('%s').",
                            ConfigOptions.TABLE_DATALAKE_FORMAT.key(),
                            tableDataLakeFormat.get(),
                            ConfigOptions.DATALAKE_FORMAT.key(),
                            clusterDataLakeFormat));
        }
    }

    private static void checkHistoricalPartition(
            TableDescriptor tableDescriptor, Configuration tableConf) {
        if (!tableConf.get(ConfigOptions.TABLE_DATALAKE_HISTORICAL_PARTITION_ENABLED)) {
            return;
        }

        List<String> unmetRequirements = new ArrayList<>();
        if (!tableConf.get(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED)) {
            unmetRequirements.add(
                    String.format(
                            "'%s' must be set to true",
                            ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key()));
        }
        if (!tableConf.get(ConfigOptions.TABLE_DATALAKE_ENABLED)) {
            unmetRequirements.add(
                    String.format(
                            "'%s' must be set to true",
                            ConfigOptions.TABLE_DATALAKE_ENABLED.key()));
        }

        Optional<DataLakeFormat> dataLakeFormat =
                tableConf.getOptional(ConfigOptions.TABLE_DATALAKE_FORMAT);
        if (!dataLakeFormat.isPresent()) {
            unmetRequirements.add(
                    String.format(
                            "'%s' must be set to '%s' (currently not set)",
                            ConfigOptions.TABLE_DATALAKE_FORMAT.key(), DataLakeFormat.PAIMON));
        } else if (dataLakeFormat.get() != DataLakeFormat.PAIMON) {
            unmetRequirements.add(
                    String.format(
                            "'%s' must be set to '%s' (currently '%s')",
                            ConfigOptions.TABLE_DATALAKE_FORMAT.key(),
                            DataLakeFormat.PAIMON,
                            dataLakeFormat.get()));
        }
        int partitionKeyCount = tableDescriptor.getPartitionKeys().size();
        if (partitionKeyCount != 1) {
            unmetRequirements.add(
                    String.format(
                            "the table must define exactly one partition key (found %s)",
                            partitionKeyCount));
        }

        if (!unmetRequirements.isEmpty()) {
            throw new InvalidConfigException(
                    String.format(
                            "'%s' has unmet requirements: %s.",
                            ConfigOptions.TABLE_DATALAKE_HISTORICAL_PARTITION_ENABLED.key(),
                            String.join("; ", unmetRequirements)));
        }
    }

    private static void checkCustomLakePathSupported(
            Configuration tableConf, @Nullable DataLakeFormat clusterDataLakeFormat) {
        boolean hasLakePathOption =
                tableConf.contains(ConfigOptions.TABLE_DATALAKE_DATABASE_NAME)
                        || tableConf.contains(ConfigOptions.TABLE_DATALAKE_TABLE_NAME);
        if (!hasLakePathOption) {
            return;
        }

        tableConf
                .getOptional(ConfigOptions.TABLE_DATALAKE_DATABASE_NAME)
                .ifPresent(TablePath::validateDatabaseName);
        tableConf
                .getOptional(ConfigOptions.TABLE_DATALAKE_TABLE_NAME)
                .ifPresent(TablePath::validateTableName);

        DataLakeFormat dataLakeFormat =
                tableConf
                        .getOptional(ConfigOptions.TABLE_DATALAKE_FORMAT)
                        .orElse(clusterDataLakeFormat);
        if (dataLakeFormat != DataLakeFormat.PAIMON && dataLakeFormat != DataLakeFormat.ICEBERG) {
            throw new InvalidConfigException(
                    "Custom lake table path is only supported for Paimon and Iceberg.");
        }
    }

    /**
     * Checks the {@code table.datalake.auto-compaction.*} tuning options, which only the compaction
     * of tables with deletion vectors reads. Other tables ignore them.
     */
    private static void checkAutoCompaction(Configuration tableConf) {
        if (!tableConf.get(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED)) {
            return;
        }
        if (tableConf.get(ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION_MIN_SMALL_FILES) < 2) {
            throw new InvalidConfigException(
                    String.format(
                            "'%s' must be at least 2: a single small file is not rewritten on its"
                                    + " own.",
                            ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION_MIN_SMALL_FILES.key()));
        }
        if (tableConf.get(ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION_INTERVAL).isNegative()) {
            throw new InvalidConfigException(
                    String.format(
                            "'%s' must not be negative.",
                            ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION_INTERVAL.key()));
        }
    }

    private static void checkDeletionVectors(
            TableDescriptor tableDescriptor,
            Configuration tableConf,
            @Nullable DataLakeFormat clusterDataLakeFormat) {
        if (!tableConf.get(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED)) {
            return;
        }

        if (!tableDescriptor.hasPrimaryKey()) {
            throw new InvalidConfigException(
                    "'table.deletion-vectors.enabled' can only be enabled on primary key tables.");
        }

        if (!tableConf.get(ConfigOptions.TABLE_DATALAKE_ENABLED)) {
            throw new InvalidConfigException(
                    "'table.deletion-vectors.enabled' requires 'table.datalake.enabled' = true.");
        }

        // The historical partition keys its local state by partition-scoped keys. The position
        // index and the log replay key rows by primary key alone, so its changes would never mask
        // the lake rows they supersede.
        if (tableConf.get(ConfigOptions.TABLE_DATALAKE_HISTORICAL_PARTITION_ENABLED)) {
            throw new InvalidConfigException(
                    String.format(
                            "'%s' cannot be combined with '%s': deletion vectors do not cover the"
                                    + " historical partition yet.",
                            ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key(),
                            ConfigOptions.TABLE_DATALAKE_HISTORICAL_PARTITION_ENABLED.key()));
        }

        // Either changelog image mode works: a change names the version it supersedes by the tag
        // of the key's KV value, never by an UPDATE_BEFORE record.

        // Each KV value carries the log offset of its version, which names the version a change
        // supersedes. Row TTL by processing time is allowed: the value layout then has a slot for
        // each (see checkKvValueLayout), and the write path looks up the lake row of a key it
        // finds no value for, as row TTL may have removed it. A version that expires before it
        // reaches the lake cannot be named, which processing time bounds by the TTL. By event
        // time, a late or backfilled row expires at the next compaction, so it is refused.
        if (tableConf.getOptional(ConfigOptions.TABLE_KV_TTL_TIME_COLUMN).isPresent()) {
            throw new InvalidConfigException(kvTtlTimeColumnWithDeletionVectors());
        }

        // Iceberg tiering commits the deletes. Paimon merges by key itself, and the masks follow
        // its deletion-vector reads. Other formats implement neither.
        DataLakeFormat lakeFormat =
                tableConf
                        .getOptional(ConfigOptions.TABLE_DATALAKE_FORMAT)
                        .orElse(clusterDataLakeFormat);
        if (lakeFormat != null
                && lakeFormat != DataLakeFormat.ICEBERG
                && lakeFormat != DataLakeFormat.PAIMON) {
            throw new InvalidConfigException(
                    String.format(
                            "'table.deletion-vectors.enabled' is only supported for the %s and %s "
                                    + "lake formats, but the table uses %s.",
                            DataLakeFormat.ICEBERG, DataLakeFormat.PAIMON, lakeFormat));
        }
    }

    public static void validateAlterTableProperties(
            TableInfo currentTable, Set<String> tableKeysToChange) {
        TableConfig currentConfig = currentTable.getTableConfig();

        if (currentConfig.isDeletionVectorsEnabled()
                && tableKeysToChange.contains(ConfigOptions.TABLE_KV_TTL_TIME_COLUMN.key())) {
            throw new InvalidAlterTableException(kvTtlTimeColumnWithDeletionVectors());
        }

        List<String> unsupportedKeys =
                tableKeysToChange.stream()
                        .filter(k -> isTableStorageConfig(k) && !isAlterableTableOption(k))
                        .collect(Collectors.toList());
        if (!unsupportedKeys.isEmpty()) {
            throw new InvalidAlterTableException(
                    String.format(
                            "The following options are not supported to alter yet: %s.",
                            unsupportedKeys.stream()
                                    .map(k -> "'" + k + "'")
                                    .collect(Collectors.joining(", "))));
        }

        // Standby replica is only applicable to primary key tables
        if (tableKeysToChange.contains(ConfigOptions.TABLE_KV_STANDBY_REPLICA_ENABLED.key())
                && !currentTable.hasPrimaryKey()) {
            throw new InvalidAlterTableException(
                    String.format(
                            "'%s' can only be altered on primary key tables.",
                            ConfigOptions.TABLE_KV_STANDBY_REPLICA_ENABLED.key()));
        }

        if (!currentConfig.getDataLakeFormat().isPresent()) {
            List<String> datalakeKeys =
                    tableKeysToChange.stream()
                            .filter(k -> k.startsWith("table.datalake."))
                            .collect(Collectors.toList());
            if (!datalakeKeys.isEmpty()) {
                // Allow log tables without bucket keys to enable datalake even when
                // `table.datalake.format` was not recorded at creation time, because bucket
                // distribution does not need to stay aligned with the lake format in this case.
                boolean alterLegacyLogTableWithoutBucketKey =
                        !currentTable.hasPrimaryKey()
                                && !currentTable.hasBucketKey()
                                && datalakeKeys.stream()
                                        .allMatch(
                                                k ->
                                                        k.equals(
                                                                ConfigOptions.TABLE_DATALAKE_ENABLED
                                                                        .key()));
                if (alterLegacyLogTableWithoutBucketKey) {
                    return;
                }

                throw new InvalidAlterTableException(
                        String.format(
                                "The following options cannot be altered for tables that were"
                                        + " created before the Fluss cluster enabled datalake: %s.",
                                datalakeKeys.stream()
                                        .map(k -> "'" + k + "'")
                                        .collect(Collectors.joining(", "))));
            }
        }
    }

    private static String kvTtlTimeColumnWithDeletionVectors() {
        return String.format(
                "'%s' cannot be combined with '%s': rows would expire by event time, so a late or"
                        + " backfilled row could expire before it reaches the lake, and union"
                        + " reads could then return its key twice. Row TTL by processing time"
                        + " ('%s' without '%s') is supported.",
                ConfigOptions.TABLE_KV_TTL_TIME_COLUMN.key(),
                ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key(),
                ConfigOptions.TABLE_KV_TTL.key(),
                ConfigOptions.TABLE_KV_TTL_TIME_COLUMN.key());
    }

    private static void checkSystemColumns(RowType schema) {
        List<String> fieldNames = schema.getFieldNames();
        List<String> unsupportedColumns =
                fieldNames.stream().filter(SYSTEM_COLUMNS::contains).collect(Collectors.toList());
        if (!unsupportedColumns.isEmpty()) {
            throw new InvalidTableException(
                    String.format(
                            "%s cannot be used as column names, "
                                    + "because they are reserved system columns in Fluss. "
                                    + "Please use other names for these columns. "
                                    + "The reserved system columns are: %s",
                            String.join(", ", unsupportedColumns),
                            String.join(", ", SYSTEM_COLUMNS)));
        }
    }

    private static void checkKvTTL(Configuration tableConf, Schema schema, boolean hasPrimaryKey) {
        Optional<Duration> rowTTL = tableConf.getOptional(ConfigOptions.TABLE_KV_TTL);
        Optional<String> timeColumn = tableConf.getOptional(ConfigOptions.TABLE_KV_TTL_TIME_COLUMN);
        if (timeColumn.isPresent() && !rowTTL.isPresent()) {
            throw new InvalidConfigException(
                    String.format(
                            "'%s' requires '%s' to be set.",
                            ConfigOptions.TABLE_KV_TTL_TIME_COLUMN.key(),
                            ConfigOptions.TABLE_KV_TTL.key()));
        }

        if (!rowTTL.isPresent()) {
            return;
        }

        if (!hasPrimaryKey) {
            throw new InvalidTableException(
                    String.format(
                            "'%s' is only supported for primary key tables.",
                            ConfigOptions.TABLE_KV_TTL.key()));
        }

        validateKvTTLDuration(rowTTL.get());

        if (timeColumn.isPresent()) {
            Schema.Column column = getKvTTLTimeColumn(schema, timeColumn.get());
            validateKvTTLTimeColumnType(column.getDataType());
        }
    }

    private static Schema.Column getKvTTLTimeColumn(Schema schema, String timeColumn) {
        for (Schema.Column column : schema.getColumns()) {
            if (column.getName().equals(timeColumn)) {
                return column;
            }
        }
        throw new InvalidConfigException(
                String.format(
                        "'%s' refers to unknown column '%s'.",
                        ConfigOptions.TABLE_KV_TTL_TIME_COLUMN.key(), timeColumn));
    }

    private static void validateKvTTLTimeColumnType(DataType dataType) {
        if (dataType.is(DataTypeRoot.BIGINT)
                || dataType.is(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                || dataType.is(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
            return;
        }
        throw new InvalidConfigException(
                String.format(
                        "'%s' only supports BIGINT, TIMESTAMP, or TIMESTAMP_LTZ columns, but was %s.",
                        ConfigOptions.TABLE_KV_TTL_TIME_COLUMN.key(), dataType));
    }

    private static void validateKvTTLDuration(Duration ttl) {
        try {
            RowTtlUtils.validateAndConvertTtlDurationToMillis(ttl);
        } catch (IllegalArgumentException e) {
            throw new InvalidConfigException(
                    String.format(
                            "Invalid value for '%s': %s",
                            ConfigOptions.TABLE_KV_TTL.key(), e.getMessage()));
        }
    }

    private static void checkKvFormatVersion(Configuration tableConf) {
        Optional<Integer> kvFormatVersion =
                tableConf.getOptional(ConfigOptions.TABLE_KV_FORMAT_VERSION);
        if (kvFormatVersion.isPresent() && kvFormatVersion.get() > CURRENT_KV_FORMAT_VERSION) {
            throw new InvalidConfigException(
                    String.format(
                            "Unsupported kv format version %d. The maximum supported version is %d.",
                            kvFormatVersion.get(), CURRENT_KV_FORMAT_VERSION));
        }
    }

    private static void checkKvValueLayout(Configuration tableConf, boolean hasPrimaryKey) {
        boolean rowTtlEnabled = tableConf.getOptional(ConfigOptions.TABLE_KV_TTL).isPresent();
        boolean deletionVectors = tableConf.get(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED);
        Optional<Integer> layoutVersion =
                tableConf.getOptional(ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION);
        if (!layoutVersion.isPresent()) {
            if (rowTtlEnabled) {
                throw new InvalidConfigException(
                        String.format(
                                "'%s' must be set when '%s' is set.",
                                ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION.key(),
                                ConfigOptions.TABLE_KV_TTL.key()));
            }
            return;
        }
        if (!hasPrimaryKey) {
            throw new InvalidConfigException(
                    String.format(
                            "'%s' is only supported for primary key tables.",
                            ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION.key()));
        }

        KvValueLayout layout;
        try {
            layout = KvValueLayout.fromVersion(layoutVersion.get());
        } catch (IllegalArgumentException e) {
            throw new InvalidConfigException(
                    String.format(
                            "Invalid value for '%s': %s.",
                            ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION.key(),
                            layoutVersion.get()));
        }

        if (layout != expectedKvValueLayout(rowTtlEnabled, deletionVectors)) {
            throw new InvalidConfigException(
                    String.format(
                            "'%s' version %d is incompatible with %s.",
                            ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION.key(),
                            layout.version(),
                            describeKvValueOptions(rowTtlEnabled, deletionVectors)));
        }
    }

    /**
     * The KV value layout of a primary key table. Deletion vectors keep the log offset of each
     * value's version in the tag of the tagged layout, and row TTL keeps the value's time there.
     * With both, the versioned layout has a slot for each.
     */
    private static KvValueLayout expectedKvValueLayout(
            boolean rowTtlEnabled, boolean deletionVectors) {
        if (rowTtlEnabled && deletionVectors) {
            return KvValueLayout.VERSIONED;
        }
        return rowTtlEnabled || deletionVectors ? KvValueLayout.TAGGED : KvValueLayout.PLAIN;
    }

    private static String describeKvValueOptions(boolean rowTtlEnabled, boolean deletionVectors) {
        if (rowTtlEnabled && deletionVectors) {
            return String.format(
                    "'%s' = true and '%s'",
                    ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key(),
                    ConfigOptions.TABLE_KV_TTL.key());
        }
        if (deletionVectors) {
            return String.format(
                    "'%s' = true without '%s'",
                    ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key(),
                    ConfigOptions.TABLE_KV_TTL.key());
        }
        return String.format(
                "'%s' %s on a table without deletion vectors",
                ConfigOptions.TABLE_KV_TTL.key(), rowTtlEnabled ? "set" : "unset");
    }

    private static void checkDistribution(TableDescriptor tableDescriptor, int maxBucketNum) {
        if (!tableDescriptor.getTableDistribution().isPresent()) {
            throw new InvalidTableException("Table distribution is required.");
        }
        if (!tableDescriptor.getTableDistribution().get().getBucketCount().isPresent()) {
            throw new InvalidTableException("Bucket number must be set.");
        }
        int bucketCount = tableDescriptor.getTableDistribution().get().getBucketCount().get();
        if (bucketCount <= 0) {
            throw new InvalidTableException(
                    String.format("Bucket count must be greater than 0, but is %s.", bucketCount));
        }
        if (bucketCount > maxBucketNum) {
            throw new TooManyBucketsException(
                    String.format(
                            "Bucket count %s exceeds the maximum limit %s for a non-partitioned table or partition.",
                            bucketCount, maxBucketNum));
        }
        List<String> bucketKeys = tableDescriptor.getTableDistribution().get().getBucketKeys();
        if (!bucketKeys.isEmpty()) {
            // check bucket key type
            RowType schema = tableDescriptor.getSchema().getRowType();
            for (String bkColumn : bucketKeys) {
                int bkIndex = schema.getFieldIndex(bkColumn);
                DataType bkDataType = schema.getTypeAt(bkIndex);
                if (KEY_UNSUPPORTED_TYPES.contains(bkDataType.getTypeRoot())) {
                    throw new InvalidTableException(
                            String.format(
                                    "Bucket key column '%s' has unsupported data type %s. "
                                            + "Currently, bucket key column does not support types: %s.",
                                    bkColumn, bkDataType, KEY_UNSUPPORTED_TYPES));
                }
            }
        }
    }

    private static void checkPrimaryKey(TableDescriptor tableDescriptor) {
        if (tableDescriptor.hasPrimaryKey()) {
            // check primary key type
            RowType schema = tableDescriptor.getSchema().getRowType();
            Schema.PrimaryKey primaryKey = tableDescriptor.getSchema().getPrimaryKey().get();
            for (String pkColumn : primaryKey.getColumnNames()) {
                int pkIndex = schema.getFieldIndex(pkColumn);
                DataType pkDataType = schema.getTypeAt(pkIndex);
                if (KEY_UNSUPPORTED_TYPES.contains(pkDataType.getTypeRoot())) {
                    throw new InvalidTableException(
                            String.format(
                                    "Primary key column '%s' has unsupported data type %s. "
                                            + "Currently, primary key column does not support types: %s.",
                                    pkColumn, pkDataType, KEY_UNSUPPORTED_TYPES));
                }
            }
        }
    }

    private static void checkReplicationFactor(Configuration tableConf) {
        if (!tableConf.containsKey(ConfigOptions.TABLE_REPLICATION_FACTOR.key())) {
            throw new InvalidConfigException(
                    String.format(
                            "Missing required table property '%s'.",
                            ConfigOptions.TABLE_REPLICATION_FACTOR.key()));
        }
        if (tableConf.get(ConfigOptions.TABLE_REPLICATION_FACTOR) <= 0) {
            throw new InvalidConfigException(
                    String.format(
                            "'%s' must be greater than 0.",
                            ConfigOptions.TABLE_REPLICATION_FACTOR.key()));
        }
    }

    private static void checkLogFormat(Configuration tableConf, boolean hasPrimaryKey) {
        KvFormat kvFormat = tableConf.get(ConfigOptions.TABLE_KV_FORMAT);
        LogFormat logFormat = tableConf.get(ConfigOptions.TABLE_LOG_FORMAT);

        // Allow COMPACTED and ARROW log formats when KV format is COMPACTED for primary key tables
        if (hasPrimaryKey
                && kvFormat == KvFormat.COMPACTED
                && !(logFormat == LogFormat.ARROW || logFormat == LogFormat.COMPACTED)) {
            throw new InvalidConfigException(
                    "Currently, Primary Key Table supports ARROW or COMPACTED log format when kv format is COMPACTED.");
        }
    }

    private static void checkArrowCompression(Configuration tableConf) {
        checkZstdCompressionLevel(tableConf, ConfigOptions.TABLE_LOG_ARROW_COMPRESSION_ZSTD_LEVEL);
    }

    private static void checkCompactedCompression(Configuration tableConf) {
        if (tableConf.get(ConfigOptions.TABLE_LOG_COMPACTED_COMPRESSION_TYPE)
                        != LogCompressionType.NONE
                && tableConf.get(ConfigOptions.TABLE_LOG_FORMAT) != LogFormat.COMPACTED) {
            throw new InvalidConfigException(
                    String.format(
                            "'%s' is only supported for tables with '%s' = '%s', but the log format is '%s'.",
                            ConfigOptions.TABLE_LOG_COMPACTED_COMPRESSION_TYPE.key(),
                            ConfigOptions.TABLE_LOG_FORMAT.key(),
                            LogFormat.COMPACTED,
                            tableConf.get(ConfigOptions.TABLE_LOG_FORMAT)));
        }
        checkZstdCompressionLevel(
                tableConf, ConfigOptions.TABLE_LOG_COMPACTED_COMPRESSION_ZSTD_LEVEL);
    }

    private static void checkZstdCompressionLevel(
            Configuration tableConf, ConfigOption<Integer> levelOption) {
        if (tableConf.containsKey(levelOption.key())) {
            int compressionLevel = tableConf.get(levelOption);
            if (compressionLevel < 1 || compressionLevel > 22) {
                throw new InvalidConfigException(
                        "Invalid ZSTD compression level: "
                                + compressionLevel
                                + ". Expected a value between 1 and 22.");
            }
        }
    }

    private static void checkMergeEngine(
            Configuration tableConf, boolean hasPrimaryKey, Schema schema) {
        MergeEngineType mergeEngine = tableConf.get(ConfigOptions.TABLE_MERGE_ENGINE);
        if (mergeEngine != MergeEngineType.AGGREGATION) {
            validateNoAggregationFunctions(schema);
        }
        if (mergeEngine != null) {
            if (!hasPrimaryKey) {
                throw new InvalidConfigException(
                        "Merge engine is only supported in primary key table.");
            }
            if (mergeEngine == MergeEngineType.VERSIONED) {
                Optional<String> versionColumn =
                        tableConf.getOptional(ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN);
                if (!versionColumn.isPresent()) {
                    throw new InvalidConfigException(
                            String.format(
                                    "'%s' must be set for versioned merge engine.",
                                    ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN.key()));
                }
                RowType rowType = schema.getRowType();
                int columnIndex = rowType.getFieldIndex(versionColumn.get());
                if (columnIndex < 0) {
                    throw new InvalidConfigException(
                            String.format(
                                    "The version column '%s' for versioned merge engine doesn't exist in schema.",
                                    versionColumn.get()));
                }
                EnumSet<DataTypeRoot> supportedTypes =
                        EnumSet.of(
                                DataTypeRoot.INTEGER,
                                DataTypeRoot.BIGINT,
                                DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                                DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
                DataType columnType = rowType.getTypeAt(columnIndex);
                if (!supportedTypes.contains(columnType.getTypeRoot())) {
                    throw new InvalidConfigException(
                            String.format(
                                    "The version column '%s' for versioned merge engine must be one type of "
                                            + "[INT, BIGINT, TIMESTAMP, TIMESTAMP_LTZ]"
                                            + ", but got %s.",
                                    versionColumn.get(), columnType));
                }
            } else if (mergeEngine == MergeEngineType.AGGREGATION) {
                // Check aggregate merge engine with WAL changelog image
                ChangelogImage changelogImage = tableConf.get(ConfigOptions.TABLE_CHANGELOG_IMAGE);
                if (changelogImage == ChangelogImage.WAL) {
                    throw new InvalidConfigException(
                            String.format(
                                    "Table with 'AGGREGATION' merge engine does not support 'WAL' changelog image mode. "
                                            + "Aggregation merge engine tables require FULL changelog image mode "
                                            + "for correct UNDO recovery. Please set '%s' to 'FULL' or remove the setting.",
                                    ConfigOptions.TABLE_CHANGELOG_IMAGE.key()));
                }
                // Validate aggregation function parameters for aggregation merge engine
                validateAggregationFunctionParameters(schema);
            }
        }
    }

    private static void checkUpsertSkipUnchanged(Configuration tableConf, boolean hasPrimaryKey) {
        if (!tableConf.get(ConfigOptions.TABLE_UPSERT_SKIP_UNCHANGED)) {
            return;
        }
        String option = ConfigOptions.TABLE_UPSERT_SKIP_UNCHANGED.key();
        if (!hasPrimaryKey) {
            throw new InvalidConfigException(
                    String.format("'%s' is only supported for primary key tables.", option));
        }
        if (tableConf.getOptional(ConfigOptions.TABLE_MERGE_ENGINE).isPresent()) {
            throw new InvalidConfigException(
                    String.format(
                            "'%s' cannot be combined with '%s'.",
                            option, ConfigOptions.TABLE_MERGE_ENGINE.key()));
        }
        if (tableConf.get(ConfigOptions.TABLE_CHANGELOG_IMAGE) == ChangelogImage.WAL) {
            throw new InvalidConfigException(
                    String.format(
                            "'%s' requires '%s' to be FULL.",
                            option, ConfigOptions.TABLE_CHANGELOG_IMAGE.key()));
        }
        if (tableConf.getOptional(ConfigOptions.TABLE_KV_TTL).isPresent()) {
            throw new InvalidConfigException(
                    String.format(
                            "'%s' cannot be combined with '%s'.",
                            option, ConfigOptions.TABLE_KV_TTL.key()));
        }
    }

    /** Validates that the schema doesn't contain any aggregation functions. */
    private static void validateNoAggregationFunctions(Schema schema) {
        for (Schema.Column column : schema.getColumns()) {
            Optional<AggFunction> aggFunction = column.getAggFunction();
            if (aggFunction.isPresent()) {
                throw new InvalidConfigException(
                        String.format(
                                "Aggregation function is only supported for aggregation merge engine table, "
                                        + "but column '%s' has aggregation function '%s'.",
                                column.getName(), aggFunction.get()));
            }
        }
    }

    /**
     * Validates aggregation function parameters in the schema.
     *
     * <p>This method delegates to {@link AggFunction#validateParameters()} to ensure all parameters
     * are valid and {@link AggFunction#validateDataType(DataType)} to ensure data type are valid
     * according to the function's requirements.
     *
     * @param schema the schema to validate
     * @throws InvalidConfigException if any aggregation function has invalid parameters or data
     *     types
     */
    private static void validateAggregationFunctionParameters(Schema schema) {
        // Get primary key columns for early exit
        List<String> primaryKeys = schema.getPrimaryKeyColumnNames();

        for (Schema.Column column : schema.getColumns()) {
            // Skip primary key columns (they don't use user-defined aggregation functions)
            if (primaryKeys.contains(column.getName())) {
                continue;
            }

            Optional<AggFunction> aggFunctionOpt = column.getAggFunction();
            if (!aggFunctionOpt.isPresent()) {
                continue;
            }

            // Validate aggregation function parameters and data type
            try {
                AggFunction aggFunction = aggFunctionOpt.get();
                aggFunction.validateParameters();
                aggFunction.validateDataType(column.getDataType());
            } catch (IllegalArgumentException e) {
                throw new InvalidConfigException(
                        String.format(
                                "Invalid aggregation function for column '%s': %s",
                                column.getName(), e.getMessage()));
            }
        }
    }

    private static void checkTieredLog(Configuration tableConf) {
        if (tableConf.get(ConfigOptions.TABLE_TIERED_LOG_LOCAL_SEGMENTS) <= 0) {
            throw new InvalidConfigException(
                    String.format(
                            "'%s' must be greater than 0.",
                            ConfigOptions.TABLE_TIERED_LOG_LOCAL_SEGMENTS.key()));
        }

        Optional<Duration> localTtl = tableConf.getOptional(ConfigOptions.TABLE_LOG_LOCAL_TTL);
        if (!localTtl.isPresent()) {
            return;
        }
        Duration logTtl = tableConf.get(ConfigOptions.TABLE_LOG_TTL);
        if (!localTtl.get().isZero()
                && !localTtl.get().isNegative()
                && !logTtl.isZero()
                && !logTtl.isNegative()
                && localTtl.get().compareTo(logTtl) > 0) {
            throw new InvalidConfigException(
                    String.format(
                            "'%s' must be less than or equal to '%s'.",
                            ConfigOptions.TABLE_LOG_LOCAL_TTL.key(),
                            ConfigOptions.TABLE_LOG_TTL.key()));
        }
    }

    private static void checkPartition(
            Configuration tableConf, List<String> partitionKeys, RowType rowType) {
        boolean isPartitioned = !partitionKeys.isEmpty();
        AutoPartitionStrategy autoPartition = AutoPartitionStrategy.from(tableConf);
        boolean hasExplicitTimeFormat =
                tableConf.contains(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT);

        if (!isPartitioned && autoPartition.isAutoPartitionEnabled()) {
            throw new InvalidConfigException(
                    String.format(
                            "Currently, auto partition is only supported for partitioned table, please set table property '%s' to false.",
                            ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key()));
        }

        if (isPartitioned) {
            for (String partitionKey : partitionKeys) {
                int partitionIndex = rowType.getFieldIndex(partitionKey);
                DataType partitionDataType = rowType.getTypeAt(partitionIndex);
                if (!PARTITION_KEY_SUPPORTED_TYPES.contains(partitionDataType.getTypeRoot())) {
                    throw new InvalidTableException(
                            String.format(
                                    "Currently, partitioned table supported partition key type are %s, "
                                            + "but got partition key '%s' with data type %s.",
                                    PARTITION_KEY_SUPPORTED_TYPES,
                                    partitionKey,
                                    partitionDataType));
                }
            }

            if (hasExplicitTimeFormat) {
                try {
                    validateTimeFormat(autoPartition.timeUnit(), autoPartition);
                } catch (IllegalArgumentException e) {
                    throw new InvalidTableException(
                            String.format(
                                    "Invalid table property '%s': %s",
                                    ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT.key(),
                                    e.getMessage()));
                }
            }

            if (autoPartition.isAutoPartitionEnabled()) {
                if (partitionKeys.size() > 1) {
                    // must specify auto partition key for auto partition table when optional keys
                    // size > 1
                    if (StringUtils.isNullOrWhitespaceOnly(autoPartition.key())) {
                        throw new InvalidTableException(
                                String.format(
                                        "Currently, auto partitioned table must set one auto partition key when it "
                                                + "has multiple partition keys. Please set table property '%s'.",
                                        ConfigOptions.TABLE_AUTO_PARTITION_KEY.key()));
                    }

                    if (!partitionKeys.contains(autoPartition.key())) {
                        throw new InvalidTableException(
                                String.format(
                                        "The specified key for auto partitioned table is not a partition key. "
                                                + "Your key '%s' is not in key list %s",
                                        autoPartition.key(), partitionKeys));
                    }

                    if (autoPartition.numPreCreate() > 0) {
                        throw new InvalidTableException(
                                "For a partitioned table with multiple partition keys, auto pre-create is unsupported and this value must be set to 0, but is "
                                        + autoPartition.numPreCreate());
                    }
                }

                if (autoPartition.timeUnit() == null) {
                    throw new InvalidTableException(
                            String.format(
                                    "Currently, auto partitioned table must set auto partition time unit when auto "
                                            + "partition is enabled, please set table property '%s'.",
                                    ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT.key()));
                }

                String autoPartitionKey =
                        StringUtils.isNullOrWhitespaceOnly(autoPartition.key())
                                ? partitionKeys.get(0)
                                : autoPartition.key();
                DataType autoPartitionDataType =
                        rowType.getTypeAt(rowType.getFieldIndex(autoPartitionKey));
                checkDateAutoPartitionCompatibility(
                        autoPartition, autoPartitionKey, autoPartitionDataType);
            }
        }
    }

    private static void checkDateAutoPartitionCompatibility(
            AutoPartitionStrategy autoPartition,
            String autoPartitionKey,
            DataType autoPartitionDataType) {
        if (autoPartitionDataType.getTypeRoot() != DataTypeRoot.DATE) {
            return;
        }
        if (autoPartition.timeUnit() != AutoPartitionTimeUnit.DAY) {
            throw new InvalidTableException(
                    String.format(
                            "Table property '%s' must be '%s' when auto partition key '%s' has DATE type.",
                            ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT.key(),
                            AutoPartitionTimeUnit.DAY,
                            autoPartitionKey));
        }
        if (!"yyyy-MM-dd".equals(autoPartition.timeFormat())) {
            throw new InvalidTableException(
                    String.format(
                            "Table property '%s' must be '%s' when auto partition key '%s' has DATE type.",
                            ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT.key(),
                            "yyyy-MM-dd",
                            autoPartitionKey));
        }
    }

    private static void checkDeleteBehavior(Configuration tableConf, boolean hasPrimaryKey) {
        Optional<DeleteBehavior> deleteBehaviorOptional =
                tableConf.getOptional(ConfigOptions.TABLE_DELETE_BEHAVIOR);
        if (!hasPrimaryKey && deleteBehaviorOptional.isPresent()) {
            throw new InvalidConfigException(
                    "The 'table.delete.behavior' configuration is only supported for primary key tables.");
        }

        // For tables with merge engines, automatically set appropriate delete behavior
        MergeEngineType mergeEngine = tableConf.get(ConfigOptions.TABLE_MERGE_ENGINE);
        if (mergeEngine == MergeEngineType.FIRST_ROW
                || mergeEngine == MergeEngineType.VERSIONED
                || mergeEngine == MergeEngineType.AGGREGATION) {
            // For FIRST_ROW, VERSIONED and AGGREGATION merge engines, delete operations are not
            // supported by default
            // If user explicitly sets delete behavior to ALLOW, validate it
            if (deleteBehaviorOptional.isPresent()
                    && deleteBehaviorOptional.get() == DeleteBehavior.ALLOW) {
                // For FIRST_ROW and VERSIONED, ALLOW is not permitted
                if (mergeEngine == MergeEngineType.FIRST_ROW
                        || mergeEngine == MergeEngineType.VERSIONED) {
                    throw new InvalidConfigException(
                            String.format(
                                    "Table with '%s' merge engine does not support delete operations. "
                                            + "The 'table.delete.behavior' config must be set to 'ignore' or 'disable', but got 'allow'.",
                                    mergeEngine));
                }
                // For AGGREGATION, ALLOW is permitted (removes entire record)
            }
        }
    }

    private static void validateOptionValue(ReadableConfig options, ConfigOption<?> option) {
        try {
            options.get(option);
        } catch (Throwable t) {
            throw new InvalidConfigException(
                    String.format(
                            "Invalid value for config '%s'. Reason: %s",
                            option.key(), t.getMessage()));
        }
    }
}
