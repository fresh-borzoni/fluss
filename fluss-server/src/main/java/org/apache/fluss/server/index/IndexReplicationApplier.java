/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.CorruptRecordException;
import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordReadContext;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.decode.KeyDecoder;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.entity.PutIndexDataForBucket;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.utils.ByteArraySlice;
import org.apache.fluss.utils.BytesUtils;
import org.apache.fluss.utils.IndexTableUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.utils.IndexTableUtils.DATA_RECORD_KIND;
import static org.apache.fluss.utils.IndexTableUtils.PROGRESS_RECORD_KIND;
import static org.apache.fluss.utils.IndexTableUtils.PURGE_FLOOR_RECORD_KIND;
import static org.apache.fluss.utils.IndexTableUtils.REMOVED_RECORD_KIND;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Applies replicated index windows using recoverable target progress for idempotency. */
@Internal
public final class IndexReplicationApplier {

    private static final Logger LOG = LoggerFactory.getLogger(IndexReplicationApplier.class);

    private final TableInfo tableInfo;
    private final TableBucket tableBucket;
    private final TabletServerMetadataCache metadataCache;
    private final KvRecordBatch.ReadContext readContext;
    private final KeyDecoder keyDecoder;
    private final int partitionIdPosition;
    private final int partitionIdKeyPosition;
    private final int recordKindPosition;
    private final int sourceProgressPosition;
    private final IndexTableRows rows;

    /** Creates the application state for one Index Table replica. */
    public IndexReplicationApplier(
            TableInfo tableInfo,
            TableBucket tableBucket,
            TabletServerMetadataCache metadataCache,
            SchemaGetter schemaGetter) {
        this.tableInfo = tableInfo;
        this.tableBucket = tableBucket;
        this.metadataCache = metadataCache;
        Schema schema = tableInfo.getSchema();
        this.readContext =
                KvRecordReadContext.createReadContext(
                        tableInfo.getTableConfig().getKvFormat(), schemaGetter);
        this.partitionIdPosition =
                schema.getColumnNames().indexOf(IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN);
        this.recordKindPosition =
                schema.getColumnNames().indexOf(IndexTableUtils.RECORD_KIND_SYSTEM_COLUMN);
        this.sourceProgressPosition =
                schema.getColumnNames().indexOf(IndexTableUtils.SOURCE_PROGRESS_SYSTEM_COLUMN);
        this.rows = new IndexTableRows(tableInfo, schemaGetter);
        List<String> primaryKeys =
                schema.getPrimaryKey()
                        .map(Schema.PrimaryKey::getColumnNames)
                        .orElse(Collections.emptyList());
        this.partitionIdKeyPosition =
                primaryKeys.indexOf(IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN);
        this.keyDecoder =
                KeyDecoder.ofPrimaryKeyDecoder(
                        schema.getRowType(),
                        primaryKeys,
                        tableInfo
                                .getTableConfig()
                                .getKvFormatVersion()
                                .orElse(ConfigOptions.KV_FORMAT_VERSION_2)
                                .shortValue(),
                        tableInfo.getTableConfig().getDataLakeFormat().orElse(null),
                        tableInfo.isDefaultBucketKey());
    }

    /**
     * Applies one source window under the target tablet lock. The progress row must be the final
     * record of the ordinary KV batch, so a partial RocksDB flush can never expose progress ahead
     * of its data mutations.
     */
    @Nullable
    public LogAppendInfo apply(KvTablet kvTablet, PutIndexDataForBucket data) throws Exception {
        AtomicReference<LogAppendInfo> result = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        kvTablet.getGuardedExecutor()
                .execute(
                        () -> {
                            try {
                                result.set(applyLocked(kvTablet, data));
                            } catch (Throwable t) {
                                failure.set(t);
                            }
                        });
        Throwable error = failure.get();
        if (error instanceof Exception) {
            throw (Exception) error;
        }
        if (error instanceof Error) {
            throw (Error) error;
        }
        return result.get();
    }

    @Nullable
    private LogAppendInfo applyLocked(KvTablet kvTablet, PutIndexDataForBucket data)
            throws Exception {
        TableBucket sourceBucket = data.sourceBucket();
        long sourceEndOffset = data.sourceEndOffset();
        byte[] progressKey = data.progressKey();
        KvRecordBatch records = data.records();
        validateSource(sourceBucket);
        if (sourceIsTombstoned(sourceBucket)) {
            return null;
        }
        validateBatch(sourceBucket, sourceEndOffset, progressKey, records);

        // Only progress materialized in RocksDB can confirm a completed duplicate. Progress
        // still in the pre-write buffer must remain retriable until the committed batch is
        // materialized; acknowledging it early could lose the window after recovery.
        long durableProgress =
                readProgress(kvTablet.multiGet(Collections.singletonList(progressKey)));
        if (durableProgress >= sourceEndOffset) {
            return null;
        }
        long visibleProgress =
                readProgress(
                        kvTablet.multiGetFromBufferOrKv(Collections.singletonList(progressKey)));
        if (visibleProgress >= sourceEndOffset) {
            throw new KvStorageException(
                    "Index progress "
                            + visibleProgress
                            + " for source "
                            + sourceBucket
                            + " is awaiting durable flush on "
                            + tableBucket);
        }
        LogAppendInfo appended = kvTablet.putAsLeader(records, null, MergeMode.OVERWRITE);
        if (data.sourceHistoryFloor() > 0) {
            purgeRemovedBelow(kvTablet, sourceBucket, data.sourceHistoryFloor());
        }
        return appended;
    }

    /**
     * Purges the removed pointers of a source bucket below its history floor, where reads of the
     * main table expire. The purge floor is written first in the same batch, so a read at an
     * earlier position sees that it expired before it can miss a purged pointer.
     */
    private void purgeRemovedBelow(KvTablet kvTablet, TableBucket source, long historyFloor)
            throws Exception {
        byte[] floorKey = rows.sourceRecordKey(PURGE_FLOOR_RECORD_KIND, source);
        long purgedBelow =
                rows.sourceOffset(
                        rows.decode(
                                kvTablet.multiGetFromBufferOrKv(Collections.singletonList(floorKey))
                                        .get(0)),
                        PURGE_FLOOR_RECORD_KIND);
        if (historyFloor <= purgedBelow) {
            return;
        }
        // the record kind leads the key, so all removed pointers share this prefix
        List<byte[]> candidates = new ArrayList<>();
        for (ByteArraySlice value : kvTablet.prefixLookup(new byte[] {REMOVED_RECORD_KIND})) {
            InternalRow row = rows.decode(value);
            if (rows.sourceBucketOf(row).equals(source)
                    && rows.sourceOffset(row, REMOVED_RECORD_KIND) < historyFloor) {
                candidates.add(rows.keyOf(row));
            }
        }
        // a later removal of the same pointer may still wait in the pre-write buffer
        List<ByteArraySlice> latest =
                candidates.isEmpty()
                        ? Collections.emptyList()
                        : kvTablet.multiGetFromBufferOrKv(candidates);
        IndexReplicator.BucketBatchBuilder batch =
                new IndexReplicator.BucketBatchBuilder(
                        (short) rows.schemaId(), tableInfo.getTableConfig().getKvFormat());
        batch.appendUpsert(
                floorKey, rows.sourceRecordValue(PURGE_FLOOR_RECORD_KIND, source, historyFloor));
        int purged = 0;
        for (int i = 0; i < candidates.size(); i++) {
            InternalRow row = rows.decode(latest.get(i));
            if (row != null
                    && rows.recordKind(row) == REMOVED_RECORD_KIND
                    && rows.sourceOffset(row, REMOVED_RECORD_KIND) < historyFloor) {
                batch.appendDelete(candidates.get(i));
                purged++;
            }
        }
        kvTablet.putAsLeader(batch.toRecordBatch(), null, MergeMode.OVERWRITE);
        LOG.debug(
                "Purged {} removed pointers of {} below {} in {}.",
                purged,
                source,
                historyFloor,
                tableBucket);
    }

    private void validateSource(TableBucket sourceBucket) {
        checkArgument(tableInfo.isIndexTable(), "%s is not an Index Table", tableBucket);
        checkArgument(
                tableInfo.getMainTableId().getAsLong() == sourceBucket.getTableId(),
                "Index Table %s is not owned by source table %s",
                tableBucket,
                sourceBucket.getTableId());
        checkArgument(
                (partitionIdPosition >= 0) == (sourceBucket.getPartitionId() != null),
                "Source partition identity does not match Index Table schema");
    }

    private boolean sourceIsTombstoned(TableBucket sourceBucket) {
        if (sourceBucket.getPartitionId() == null) {
            return false;
        }
        PartitionTombstone tombstone =
                metadataCache
                        .getPartitionTombstoneIfInitialized(sourceBucket.getTableId())
                        .orElseThrow(
                                () ->
                                        new StaleMetadataException(
                                                "Partition tombstone baseline is not initialized for "
                                                        + sourceBucket.getTableId()));
        return tombstone.isTombstoned(sourceBucket.getPartitionId());
    }

    private void validateBatch(
            TableBucket sourceBucket,
            long sourceEndOffset,
            byte[] progressKey,
            KvRecordBatch records) {
        checkArgument(sourceEndOffset >= 0, "sourceEndOffset must not be negative");
        records.ensureValid();
        if (records.magic() != KvRecordBatch.CURRENT_KV_MAGIC_VALUE
                || records.writerId() != NO_WRITER_ID
                || records.batchSequence() != NO_BATCH_SEQUENCE) {
            throw new CorruptRecordException(
                    "PutIndex accepts only ordinary KV batches without writer state");
        }

        int recordIndex = 0;
        for (KvRecord record : records.records(readContext)) {
            boolean last = recordIndex == records.getRecordCount() - 1;
            byte[] key = BytesUtils.toArray(record.getKey());
            InternalRow physicalKey = keyDecoder.decodeKey(key);
            byte keyKind = physicalKey.getByte(0);
            if (last
                    ? keyKind != PROGRESS_RECORD_KIND
                    : keyKind != DATA_RECORD_KIND && keyKind != REMOVED_RECORD_KIND) {
                throw new CorruptRecordException("PutIndex progress must be the final record");
            }
            validatePartition(sourceBucket, physicalKey, partitionIdKeyPosition);

            BinaryRow value = record.getRow();
            if (last) {
                if (!Arrays.equals(key, progressKey) || value == null) {
                    throw new CorruptRecordException("Invalid PutIndex progress record");
                }
                if (value.getByte(recordKindPosition) != PROGRESS_RECORD_KIND
                        || value.isNullAt(sourceProgressPosition)
                        || value.getLong(sourceProgressPosition) != sourceEndOffset) {
                    throw new CorruptRecordException(
                            "PutIndex progress value does not match request");
                }
                validatePartition(sourceBucket, value, partitionIdPosition);
            } else if (value != null) {
                if (value.getByte(recordKindPosition) != keyKind) {
                    throw new CorruptRecordException("Invalid PutIndex data record kind");
                }
                validatePartition(sourceBucket, value, partitionIdPosition);
            }
            recordIndex++;
        }
        if (recordIndex == 0 || recordIndex != records.getRecordCount()) {
            throw new CorruptRecordException("PutIndex batch has an invalid record count");
        }
    }

    private static void validatePartition(
            TableBucket sourceBucket, InternalRow row, int partitionPosition) {
        if (sourceBucket.getPartitionId() != null
                && row.getLong(partitionPosition) != sourceBucket.getPartitionId()) {
            throw new CorruptRecordException(
                    "PutIndex record partition does not match its source bucket");
        }
    }

    private long readProgress(List<ByteArraySlice> values) {
        ByteArraySlice value = values.get(0);
        if (value == null) {
            return -1L;
        }
        if (value.length() < Short.BYTES) {
            throw new CorruptRecordException("Stored Index Table progress value is truncated");
        }
        RowDecoder decoder = readContext.getRowDecoder(tableInfo.getSchemaId());
        InternalRow row =
                decoder.decode(
                        MemorySegment.wrap(value.array()),
                        value.offset() + Short.BYTES,
                        value.length() - Short.BYTES);
        if (row.getByte(recordKindPosition) != PROGRESS_RECORD_KIND
                || row.isNullAt(sourceProgressPosition)) {
            throw new CorruptRecordException("Stored Index Table progress row is invalid");
        }
        return row.getLong(sourceProgressPosition);
    }
}
