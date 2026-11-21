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

package org.apache.fluss.server.index;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.CorruptRecordException;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordReadContext;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ByteArraySlice;
import org.apache.fluss.utils.IndexTableUtils;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;

import static org.apache.fluss.utils.IndexTableUtils.DATA_RECORD_KIND;
import static org.apache.fluss.utils.IndexTableUtils.REMOVED_RECORD_KIND;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * The rows of an Index Table as its buckets store them: encodes the keys and values of the records
 * that a bucket keeps per source bucket, and decodes stored values. See {@link IndexTableUtils} for
 * the record kinds.
 */
@Internal
@ThreadSafe
final class IndexTableRows {

    private final long mainTableId;
    private final int schemaId;
    private final DataType[] fieldTypes;
    private final int partitionIdPosition;
    private final int recordKindPosition;
    private final int routingKeyPosition;
    private final int rowKeyPosition;
    private final int sourceProgressPosition;
    private final CompactedKeyEncoder keyEncoder;
    private final KvRecordBatch.ReadContext readContext;

    IndexTableRows(TableInfo indexTableInfo, SchemaGetter schemaGetter) {
        checkArgument(
                indexTableInfo.isIndexTable(),
                "%s is not an Index Table",
                indexTableInfo.getTablePath());
        this.mainTableId = indexTableInfo.getMainTableId().getAsLong();
        this.schemaId = indexTableInfo.getSchemaId();
        Schema schema = indexTableInfo.getSchema();
        RowType rowType = schema.getRowType();
        this.fieldTypes = rowType.getChildren().toArray(new DataType[0]);
        List<String> columns = schema.getColumnNames();
        this.partitionIdPosition = columns.indexOf(IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN);
        this.recordKindPosition = columns.indexOf(IndexTableUtils.RECORD_KIND_SYSTEM_COLUMN);
        this.routingKeyPosition = columns.indexOf(IndexTableUtils.ROUTING_KEY_SYSTEM_COLUMN);
        this.rowKeyPosition = columns.indexOf(IndexTableUtils.ROW_KEY_SYSTEM_COLUMN);
        this.sourceProgressPosition =
                columns.indexOf(IndexTableUtils.SOURCE_PROGRESS_SYSTEM_COLUMN);
        this.keyEncoder = new CompactedKeyEncoder(rowType, schema.getPrimaryKeyIndexes());
        this.readContext =
                KvRecordReadContext.createReadContext(
                        indexTableInfo.getTableConfig().getKvFormat(), schemaGetter);
    }

    long mainTableId() {
        return mainTableId;
    }

    int schemaId() {
        return schemaId;
    }

    /** The key of the record of the given kind that a bucket keeps for a source bucket. */
    byte[] sourceRecordKey(byte recordKind, TableBucket source) {
        return keyEncoder.encodeKey(sourceRecord(recordKind, source, 0L));
    }

    /** The value of the record of the given kind that a bucket keeps for a source bucket. */
    BinaryRow sourceRecordValue(byte recordKind, TableBucket source, long sourceOffset) {
        GenericRow row = sourceRecord(recordKind, source, sourceOffset);
        // an encoder per call: encoders are not thread-safe, and these records are rare
        RowEncoder encoder = RowEncoder.create(KvFormat.COMPACTED, fieldTypes);
        encoder.startNewRow();
        for (int i = 0; i < fieldTypes.length; i++) {
            encoder.encodeField(i, row.getField(i));
        }
        return encoder.finishRow();
    }

    private GenericRow sourceRecord(byte recordKind, TableBucket source, long sourceOffset) {
        GenericRow row = new GenericRow(fieldTypes.length);
        if (partitionIdPosition >= 0) {
            row.setField(partitionIdPosition, source.getPartitionId());
        }
        row.setField(recordKindPosition, recordKind);
        // a bucket has one such record per source bucket, so it needs no routing key
        row.setField(routingKeyPosition, new byte[0]);
        row.setField(rowKeyPosition, IndexTableUtils.sourceBucketRowKey(source.getBucket()));
        row.setField(sourceProgressPosition, sourceOffset);
        return row;
    }

    /** Decodes a stored value body, or returns null if there is none. */
    @Nullable
    InternalRow decode(@Nullable ByteArraySlice valueBody) {
        if (valueBody == null) {
            return null;
        }
        if (valueBody.length() < Short.BYTES) {
            throw new CorruptRecordException("Stored Index Table value is truncated");
        }
        return readContext
                .getRowDecoder(schemaId)
                .decode(
                        MemorySegment.wrap(valueBody.array()),
                        valueBody.offset() + Short.BYTES,
                        valueBody.length() - Short.BYTES);
    }

    /** The key under which a decoded row is stored. */
    byte[] keyOf(InternalRow row) {
        return keyEncoder.encodeKey(row);
    }

    byte recordKind(InternalRow row) {
        return row.getByte(recordKindPosition);
    }

    /**
     * The source offset of a progress, removed or purge-floor record of the given kind, or {@code
     * -1} if the row is absent.
     */
    long sourceOffset(@Nullable InternalRow row, byte expectedKind) {
        if (row == null) {
            return -1L;
        }
        if (recordKind(row) != expectedKind || row.isNullAt(sourceProgressPosition)) {
            throw new CorruptRecordException(
                    "Stored Index Table record is not of kind " + expectedKind);
        }
        return row.getLong(sourceProgressPosition);
    }

    /** The source bucket of a removed, progress or purge-floor record. */
    TableBucket sourceBucketOf(InternalRow row) {
        Long partitionId = partitionIdPosition >= 0 ? row.getLong(partitionIdPosition) : null;
        return new TableBucket(
                mainTableId,
                partitionId,
                IndexTableUtils.sourceBucketOf(row.getBytes(rowKeyPosition)));
    }

    /** The prefix of the removed pointers of the data pointers under a data prefix. */
    static byte[] removedPrefix(byte[] dataPrefix) {
        // the record kind is the first key column, and a compacted TINYINT is one byte
        if (dataPrefix.length == 0 || dataPrefix[0] != DATA_RECORD_KIND) {
            throw new IllegalArgumentException(
                    "A read of an Index Table at a cut must look up the prefix of data records.");
        }
        byte[] removed = dataPrefix.clone();
        removed[0] = REMOVED_RECORD_KIND;
        return removed;
    }
}
