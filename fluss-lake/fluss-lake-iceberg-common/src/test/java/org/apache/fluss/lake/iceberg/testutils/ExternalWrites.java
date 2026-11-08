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

package org.apache.fluss.lake.iceberg.testutils;

import org.apache.fluss.lake.iceberg.tiering.writer.IcebergDvFileWriter;
import org.apache.fluss.lake.iceberg.tiering.writer.IcebergPositionDeleteFileWriter;
import org.apache.fluss.lake.iceberg.tiering.writer.TaskWriterFactory;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.TaskWriter;

import java.util.Collections;

/**
 * Writes by another engine to the lake table of a table with deletion vectors, as Spark's INSERT,
 * DELETE (merge-on-read) and copy-on-write UPDATE commit them: an {@code append}, a {@code delete}
 * and an {@code overwrite}. The columns are {@code c1} (int key) and {@code c2} (string).
 */
public final class ExternalWrites {

    private ExternalWrites() {}

    /** Commits {@code append}, {@code delete} or {@code overwrite}; returns its snapshot. */
    public static long write(Table table, String operation) throws Exception {
        switch (operation) {
            case "append":
                return append(table, 1000, "external");
            case "delete":
                return deleteFirstRow(table);
            case "overwrite":
                return overwriteFirstFile(table);
            default:
                throw new IllegalArgumentException(operation);
        }
    }

    /** Appends a data file with one row; returns the snapshot. */
    public static long append(Table table, int key, String value) throws Exception {
        table.refresh();
        table.newAppend().appendFile(dataFile(table, key, value)).commit();
        return current(table);
    }

    /** Deletes the first row of a data file with a deletion vector or position delete file. */
    public static long deleteFirstRow(Table table) throws Exception {
        table.refresh();
        DataFile target = firstDataFile(table);
        DeleteWriteResult result;
        if (TableUtil.formatVersion(table) >= 3) {
            try (IcebergDvFileWriter writer = new IcebergDvFileWriter(table, 0, path -> null)) {
                writer.delete(target, 0L);
                result = writer.complete();
            }
        } else {
            try (IcebergPositionDeleteFileWriter writer =
                    new IcebergPositionDeleteFileWriter(
                            table, 0, path -> null, path -> Collections.emptyList())) {
                writer.delete(target, 0L);
                result = writer.complete();
            }
        }
        RowDelta delta = table.newRowDelta();
        for (DeleteFile delete : result.deleteFiles()) {
            delta.addDeletes(delete);
        }
        delta.commit();
        return current(table);
    }

    /** Replaces a data file by one with a single changed row, as copy-on-write does. */
    public static long overwriteFirstFile(Table table) throws Exception {
        table.refresh();
        DataFile target = firstDataFile(table);
        table.newOverwrite().deleteFile(target).addFile(dataFile(table, 0, "overwritten")).commit();
        return current(table);
    }

    private static DataFile firstDataFile(Table table) throws Exception {
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                return task.file();
            }
        }
        throw new IllegalStateException("Table " + table.name() + " has no data file.");
    }

    private static DataFile dataFile(Table table, int key, String value) throws Exception {
        try (TaskWriter<Record> writer = TaskWriterFactory.createTaskWriter(table, null, 0, true)) {
            Record record = GenericRecord.create(table.schema());
            record.setField("c1", key);
            record.setField("c2", value);
            writer.write(record);
            return writer.complete().dataFiles()[0];
        }
    }

    private static long current(Table table) {
        table.refresh();
        return table.currentSnapshot().snapshotId();
    }
}
