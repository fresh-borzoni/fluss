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
import org.apache.fluss.exception.LookupLimitExceededException;
import org.apache.fluss.exception.LookupPositionExpiredException;
import org.apache.fluss.exception.LookupPositionNotYetReadableException;
import org.apache.fluss.metadata.BucketReadPosition;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.utils.ByteArraySlice;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.utils.IndexTableUtils.PROGRESS_RECORD_KIND;
import static org.apache.fluss.utils.IndexTableUtils.PURGE_FLOOR_RECORD_KIND;
import static org.apache.fluss.utils.IndexTableUtils.REMOVED_RECORD_KIND;

/**
 * Reads the pointers of an Index Table bucket at a cut of its main table, one exclusive next offset
 * per main table bucket. It returns the data pointers plus the removed pointers whose row still had
 * the index values at the cut: a superset of the matching rows, which the caller rechecks.
 *
 * <p>Fails with {@link LookupPositionNotYetReadableException} until the bucket applied every source
 * bucket up to the cut, and with {@link LookupPositionExpiredException} below a purge floor.
 *
 * <p>Writes apply between the reads, whose order keeps the result exact: progress first, so every
 * change below the cut is visible; data pointers before removed pointers, which are written before
 * the data pointers they replace are deleted; purge floors last, which a purge writes before it
 * deletes.
 */
@Internal
public final class IndexCutReader {

    private final IndexTableRows rows;

    public IndexCutReader(TableInfo indexTableInfo, SchemaGetter schemaGetter) {
        this.rows = new IndexTableRows(indexTableInfo, schemaGetter);
    }

    /** The id of the main table, whose buckets cuts name. */
    public long mainTableId() {
        return rows.mainTableId();
    }

    /**
     * Reads the pointers under each data prefix at the cut; removed pointers of buckets outside the
     * cut are left out.
     *
     * @param limit the most pointers per prefix, or 0 for all
     * @param failOnLimitExceeded whether to fail if a prefix has more pointers than the limit,
     *     instead of returning the first ones
     */
    public List<List<ByteArraySlice>> read(
            KvTablet kvTablet,
            List<byte[]> dataPrefixes,
            Map<TableBucket, Long> cut,
            int limit,
            boolean failOnLimitExceeded)
            throws IOException {
        List<TableBucket> sources = new ArrayList<>(cut.keySet());
        List<InternalRow> progress = readSourceRecords(kvTablet, PROGRESS_RECORD_KIND, sources);
        for (int i = 0; i < sources.size(); i++) {
            long applied = Math.max(0L, rows.sourceOffset(progress.get(i), PROGRESS_RECORD_KIND));
            TableBucket source = sources.get(i);
            if (cut.get(source) > applied) {
                throw new LookupPositionNotYetReadableException(
                        BucketReadPosition.of(source, applied));
            }
        }

        List<List<ByteArraySlice>> results = new ArrayList<>(dataPrefixes.size());
        for (byte[] dataPrefix : dataPrefixes) {
            List<ByteArraySlice> pointers =
                    new ArrayList<>(kvTablet.prefixLookup(dataPrefix, limit, failOnLimitExceeded));
            for (ByteArraySlice value :
                    kvTablet.prefixLookup(IndexTableRows.removedPrefix(dataPrefix))) {
                InternalRow row = rows.decode(value);
                Long position = cut.get(rows.sourceBucketOf(row));
                // the row had the index values at every position up to the removing change
                if (position != null && rows.sourceOffset(row, REMOVED_RECORD_KIND) >= position) {
                    if (limit > 0 && pointers.size() == limit) {
                        if (failOnLimitExceeded) {
                            throw new LookupLimitExceededException(
                                    "Prefix lookup exceeded the candidate limit of " + limit + '.');
                        }
                        break;
                    }
                    pointers.add(value);
                }
            }
            results.add(pointers);
        }

        List<InternalRow> floors = readSourceRecords(kvTablet, PURGE_FLOOR_RECORD_KIND, sources);
        for (int i = 0; i < sources.size(); i++) {
            long purgedBelow = rows.sourceOffset(floors.get(i), PURGE_FLOOR_RECORD_KIND);
            TableBucket source = sources.get(i);
            if (cut.get(source) < purgedBelow) {
                throw new LookupPositionExpiredException(
                        BucketReadPosition.of(source, purgedBelow));
            }
        }
        return results;
    }

    private List<InternalRow> readSourceRecords(
            KvTablet kvTablet, byte recordKind, List<TableBucket> sources) throws IOException {
        List<byte[]> keys = new ArrayList<>(sources.size());
        for (TableBucket source : sources) {
            keys.add(rows.sourceRecordKey(recordKind, source));
        }
        List<ByteArraySlice> values = kvTablet.multiGet(keys);
        List<InternalRow> records = new ArrayList<>(values.size());
        for (ByteArraySlice value : values) {
            records.add(rows.decode(value));
        }
        return records;
    }
}
