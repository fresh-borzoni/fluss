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

package org.apache.fluss.server.entity;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.KvRecordBatch;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** One application-level secondary-index write targeting an Index Table bucket. */
public final class PutIndexDataForBucket {

    private final TableBucket targetBucket;
    private final TableBucket sourceBucket;
    private final long sourceEndOffset;
    private final byte[] progressKey;
    private final KvRecordBatch records;
    private final long sourceHistoryFloor;

    public PutIndexDataForBucket(
            TableBucket targetBucket,
            TableBucket sourceBucket,
            long sourceEndOffset,
            byte[] progressKey,
            KvRecordBatch records) {
        this(targetBucket, sourceBucket, sourceEndOffset, progressKey, records, -1L);
    }

    /**
     * @param sourceHistoryFloor the earliest retained position of the source bucket, below which
     *     the target may purge removed pointers of the source bucket, or a negative value if none
     */
    public PutIndexDataForBucket(
            TableBucket targetBucket,
            TableBucket sourceBucket,
            long sourceEndOffset,
            byte[] progressKey,
            KvRecordBatch records,
            long sourceHistoryFloor) {
        this.sourceHistoryFloor = sourceHistoryFloor;
        this.targetBucket = checkNotNull(targetBucket, "targetBucket");
        this.sourceBucket = checkNotNull(sourceBucket, "sourceBucket");
        this.sourceEndOffset = sourceEndOffset;
        this.progressKey = checkNotNull(progressKey, "progressKey");
        this.records = checkNotNull(records, "records");
    }

    public TableBucket targetBucket() {
        return targetBucket;
    }

    public TableBucket sourceBucket() {
        return sourceBucket;
    }

    public long sourceEndOffset() {
        return sourceEndOffset;
    }

    public byte[] progressKey() {
        return progressKey;
    }

    public KvRecordBatch records() {
        return records;
    }

    /** The earliest retained position of the source bucket, or a negative value if unknown. */
    public long sourceHistoryFloor() {
        return sourceHistoryFloor;
    }
}
