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

package org.apache.fluss.utils;

import org.apache.fluss.config.ConfigOption;

import java.nio.ByteBuffer;

import static org.apache.fluss.config.ConfigBuilder.key;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Utility constants and helpers shared by the Global Secondary Index Table layer. */
public final class IndexTableUtils {

    /** Persisted ownership link of an internal Index Table. */
    public static final ConfigOption<Long> TABLE_INDEX_META_MAIN_TABLE_ID =
            key("table.index-meta.main-table-id")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "The main table id associated with an internal Index Table. "
                                    + "This option is managed by the system.");

    /**
     * Single system column appended to partitioned Index Tables. Carries the source main table's
     * {@code partitionId} so that filtering and compaction can identify deleted partitions using
     * the cached partition tombstone.
     */
    public static final String PARTITION_ID_SYSTEM_COLUMN = "__partition_id";

    /** Distinguishes ordinary index entries from source-progress entries. */
    public static final String RECORD_KIND_SYSTEM_COLUMN = "__idx_record_kind";

    /** Routes records to an Index Table bucket and forms the lookup prefix for data rows. */
    public static final String ROUTING_KEY_SYSTEM_COLUMN = "__idx_routing_key";

    /** Uniquely identifies a record within one routing-key prefix. */
    public static final String ROW_KEY_SYSTEM_COLUMN = "__idx_row_key";

    /**
     * A source WAL offset: the applied end offset of a progress record, the offset of the removing
     * change of a removed record, or the purge offset of a purge-floor record. Null for data
     * records.
     */
    public static final String SOURCE_PROGRESS_SYSTEM_COLUMN = "__idx_source_progress";

    /** A pointer from the current index values of a main row to its primary key. */
    public static final byte DATA_RECORD_KIND = 0;

    /** How far an Index Table bucket applied the changes of one source bucket. */
    public static final byte PROGRESS_RECORD_KIND = 1;

    /**
     * A pointer the main row no longer has, kept for reads at earlier positions of a main table
     * with bucket history. It shares the routing key and bucket of the data pointer; its row key is
     * the source bucket followed by the data row key, and its source offset is that of the removing
     * change.
     */
    public static final byte REMOVED_RECORD_KIND = 2;

    /**
     * Per source bucket, the offset below which removed pointers were purged, so reads at earlier
     * positions expire. Its row key is the source bucket.
     */
    public static final byte PURGE_FLOOR_RECORD_KIND = 3;

    private static final int SOURCE_BUCKET_BYTES = Integer.BYTES;

    /** Reserved, operationally distinct prefix for internal Index Tables. */
    public static final String INDEX_TABLE_NAME_PREFIX = "__idx__";

    /** Separator between the owning main-table name and index name. */
    public static final String INDEX_TABLE_NAME_SEPARATOR = "__";

    private IndexTableUtils() {}

    /** Builds the internal Index Table name from its owning main-table name and index name. */
    public static String indexTableName(String mainTableName, String indexName) {
        return INDEX_TABLE_NAME_PREFIX + mainTableName + INDEX_TABLE_NAME_SEPARATOR + indexName;
    }

    /** The row key of a progress or purge-floor record of a source bucket. */
    public static byte[] sourceBucketRowKey(int sourceBucket) {
        return removedRowKey(sourceBucket, new byte[0]);
    }

    /** The row key of a removed pointer: the big-endian source bucket, then the data row key. */
    public static byte[] removedRowKey(int sourceBucket, byte[] dataRowKey) {
        return ByteBuffer.allocate(SOURCE_BUCKET_BYTES + dataRowKey.length)
                .putInt(sourceBucket)
                .put(dataRowKey)
                .array();
    }

    /** The source bucket at the start of the row key of a removed, progress or floor record. */
    public static int sourceBucketOf(byte[] rowKey) {
        checkArgument(
                rowKey.length >= SOURCE_BUCKET_BYTES,
                "A row key of %s bytes names no source bucket.",
                rowKey.length);
        return ByteBuffer.wrap(rowKey).getInt();
    }

    /** Returns whether a table name is reserved for a system-managed Index Table. */
    public static boolean isIndexTableName(String tableName) {
        return tableName.startsWith(INDEX_TABLE_NAME_PREFIX);
    }
}
