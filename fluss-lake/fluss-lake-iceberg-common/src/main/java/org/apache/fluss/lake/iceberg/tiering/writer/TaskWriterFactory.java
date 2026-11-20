/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.tiering.writer;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.util.PropertyUtil;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

/** A factory to create Iceberg {@link TaskWriter}. */
public class TaskWriterFactory {

    private static final String METRICS_PROPERTY_PREFIX = "write.metadata.metrics.";

    public static TaskWriter<Record> createTaskWriter(
            Table table, @Nullable String partition, int bucket) {
        return createTaskWriter(table, partition, bucket, false);
    }

    /**
     * Creates a task writer. With {@code dvEnabled}, the writer writes data files only, whatever
     * the table's identifier fields.
     */
    public static TaskWriter<Record> createTaskWriter(
            Table table, @Nullable String partition, int bucket, boolean dvEnabled) {
        Schema schema = table.schema();
        int[] equalityFieldIds =
                schema.identifierFieldIds().stream().mapToInt(Integer::intValue).toArray();

        // Get target file size from table properties
        long targetFileSize = targetFileSize(table);
        FileFormat format = fileFormat(table);
        OutputFileFactory outputFileFactory =
                OutputFileFactory.builderFor(
                                table,
                                bucket,
                                // task id always 0
                                0)
                        .format(format)
                        .build();

        // Tables with deletion vectors write data files only; deletes by position are written at
        // commit.
        if (dvEnabled || equalityFieldIds.length == 0) {
            FileAppenderFactory<Record> fileAppenderFactory =
                    appenderFactory(table, schema, null, null);
            return new GenericRecordAppendOnlyWriter(
                    table,
                    format,
                    fileAppenderFactory,
                    outputFileFactory,
                    table.io(),
                    targetFileSize,
                    partition,
                    bucket);

        } else {
            FileAppenderFactory<Record> appenderFactory =
                    appenderFactory(table, schema, equalityFieldIds, schema);
            List<String> columns = new ArrayList<>();
            for (Integer fieldId : schema.identifierFieldIds()) {
                columns.add(schema.findField(fieldId).name());
            }
            Schema deleteSchema = schema.select(columns);
            return new GenericRecordDeltaWriter(
                    table,
                    deleteSchema,
                    format,
                    appenderFactory,
                    outputFileFactory,
                    table.io(),
                    targetFileSize,
                    partition,
                    bucket);
        }
    }

    /**
     * The schema a rewrite of data files reads and writes: the table's, followed by the row lineage
     * fields ({@code _row_id}, {@code _last_updated_sequence_number}) on a table with row lineage
     * (format version 3). Its spec requires a writer that moves rows to keep their lineage.
     */
    public static Schema rewriteSchema(Table table) {
        return TableUtil.supportsRowLineage(table)
                ? MetadataColumns.schemaWithRowLineage(table.schema())
                : table.schema();
    }

    /**
     * Creates the writer of a rewrite of data files, e.g. a compaction, which writes records of
     * {@link #rewriteSchema}. On a table with row lineage, the rows keep their lineage, as in
     * Iceberg's own rewrites. Otherwise it is {@link #createTaskWriter(Table, String, int)}.
     */
    public static TaskWriter<Record> createRewriteWriter(
            Table table, @Nullable String partition, int bucket) {
        if (!TableUtil.supportsRowLineage(table)) {
            return createTaskWriter(table, partition, bucket);
        }
        FileFormat format = fileFormat(table);
        // A rewrite writes the live rows of files, one per key, so it writes data files only.
        return new GenericRecordAppendOnlyWriter(
                table,
                format,
                appenderFactory(table, rewriteSchema(table), null, null),
                OutputFileFactory.builderFor(table, bucket, 0).format(format).build(),
                table.io(),
                targetFileSize(table),
                partition,
                bucket);
    }

    /**
     * An appender factory that writes with the table's properties. The metrics come from the table
     * itself, so the columns of its sort order keep their bounds, which compaction sorts by.
     */
    private static FileAppenderFactory<Record> appenderFactory(
            Table table,
            Schema schema,
            @Nullable int[] equalityFieldIds,
            @Nullable Schema eqDeleteRowSchema) {
        // the table-aware constructor refuses metrics keys in the config, it reads them itself
        Map<String, String> config = new HashMap<>(table.properties());
        config.keySet().removeIf(key -> key.startsWith(METRICS_PROPERTY_PREFIX));
        return new GenericAppenderFactory(
                table, schema, table.spec(), config, equalityFieldIds, eqDeleteRowSchema, null);
    }

    private static FileFormat fileFormat(Table icebergTable) {
        String formatString =
                PropertyUtil.propertyAsString(
                        icebergTable.properties(),
                        TableProperties.DEFAULT_FILE_FORMAT,
                        TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
        return FileFormat.fromString(formatString);
    }

    private static long targetFileSize(Table icebergTable) {
        return PropertyUtil.propertyAsLong(
                icebergTable.properties(),
                WRITE_TARGET_FILE_SIZE_BYTES,
                WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    }
}
