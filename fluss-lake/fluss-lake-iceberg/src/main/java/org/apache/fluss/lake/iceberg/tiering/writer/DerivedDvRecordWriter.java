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

package org.apache.fluss.lake.iceberg.tiering.writer;

import org.apache.fluss.lake.dv.index.KeyPosition;
import org.apache.fluss.lake.dv.index.PositionIndexDelta;
import org.apache.fluss.lake.iceberg.tiering.RecordWriter;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.encode.KeyEncoder;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Writes only the last version of each primary key changed in the round and records the data file
 * and row position of every written row in a {@link PositionIndexDelta}.
 */
public class DerivedDvRecordWriter extends RecordWriter {

    private static final class Version {
        final long offset;
        final Record row; // null if the key was deleted

        Version(long offset, Record row) {
            this.offset = offset;
            this.row = row;
        }
    }

    private final KeyEncoder keyEncoder;
    private final Types.StructType struct;
    private final Map<ByteBuffer, Version> fold = new HashMap<>();
    private PositionIndexDelta delta;

    public DerivedDvRecordWriter(
            Table icebergTable,
            WriterInitContext writerInitContext,
            TaskWriter<Record> taskWriter) {
        super(
                taskWriter,
                icebergTable.schema(),
                writerInitContext.tableInfo().getRowType(),
                writerInitContext.tableBucket(),
                true);
        TableInfo tableInfo = writerInitContext.tableInfo();
        this.keyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        tableInfo.getRowType(),
                        tableInfo.getPhysicalPrimaryKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());
        this.struct = icebergTable.schema().asStruct();
    }

    @Override
    public void write(LogRecord record) throws Exception {
        ChangeType changeType = record.getChangeType();
        if (changeType == ChangeType.UPDATE_BEFORE) {
            return; // the following +U carries the key and the new version
        }
        byte[] key = keyEncoder.encodeKey(record.getRow());
        ByteBuffer k = ByteBuffer.wrap(Arrays.copyOf(key, key.length));
        if (changeType == ChangeType.DELETE) {
            fold.put(k, new Version(record.logOffset(), null));
            return;
        }
        flussRecordAsIcebergRecord.setFlussRecord(record);
        GenericRecord copy = GenericRecord.create(struct);
        for (int i = 0; i < struct.fields().size(); i++) {
            copy.set(i, flussRecordAsIcebergRecord.get(i));
        }
        fold.put(k, new Version(record.logOffset(), copy));
    }

    @Override
    public WriteResult complete() throws Exception {
        List<Map.Entry<ByteBuffer, Version>> live = new ArrayList<>();
        List<byte[]> changed = new ArrayList<>(fold.size());
        List<byte[]> deletes = new ArrayList<>();
        for (Map.Entry<ByteBuffer, Version> e : fold.entrySet()) {
            changed.add(e.getKey().array());
            if (e.getValue().row == null) {
                deletes.add(e.getKey().array());
            } else {
                live.add(e);
            }
        }
        // Data files keep the log-offset order of the rows.
        live.sort((a, b) -> Long.compare(a.getValue().offset, b.getValue().offset));
        for (Map.Entry<ByteBuffer, Version> e : live) {
            taskWriter.write(e.getValue().row);
        }
        WriteResult result = taskWriter.complete();

        // One rolling writer per bucket completes files in write order, matching the order of live.
        List<KeyPosition> puts = new ArrayList<>(live.size());
        int i = 0;
        long written = 0;
        for (DataFile f : result.dataFiles()) {
            String loc = f.location();
            String base = loc.substring(loc.lastIndexOf('/') + 1);
            for (long pos = 0; pos < f.recordCount(); pos++) {
                puts.add(new KeyPosition(live.get(i++).getKey().array(), base, pos));
            }
            written += f.recordCount();
        }
        if (written != live.size()) {
            throw new IllegalStateException(
                    "Derived DV writer wrote " + live.size() + " rows but files hold " + written);
        }
        this.delta = new PositionIndexDelta(changed, puts, deletes);
        fold.clear();
        return result;
    }

    public PositionIndexDelta positionIndexDelta() {
        return delta;
    }
}
