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

package org.apache.fluss.client.table.scanner.batch.dv;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketPartReader.KeyReader;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.RowWithPosResult;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.utils.CloseableIterator;

import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Derives the masks of a cut from the keys of a part's lake files and of its bucket's log, as the
 * leader would: for a bucket read by key, and for a part whose pinned cut the leader lost.
 */
@Internal
final class DvKeyFolder {

    private static final Duration KEY_POLL_TIMEOUT = Duration.ofMillis(500);
    // Polls without progress after which deriving masks gives up on the log.
    static final int MAX_IDLE_KEY_POLLS = 600;

    private final TableBucket bucket;
    private final DvBucketPartReader.Context context;

    DvKeyFolder(TableBucket bucket, DvBucketPartReader.Context context) {
        this.bucket = bucket;
        this.context = context;
    }

    /**
     * Derives the masks of cut {@code cut} as the server does. A lake row is masked if the log
     * before the cut changed its key. A log version is masked if a later change of its key precedes
     * the cut. Reads the log from {@code logStart} and the keys of {@code target}'s lake files.
     */
    DvBucketMaskState deriveMasks(DvBucketPart target, long logStart, long cut) throws Exception {
        KeyReader keys = context.keyReader();
        // the offset of each changed key's live version, -1 if deleted
        Map<ByteBuffer, Long> changed = new HashMap<>();
        Roaring64Bitmap superseded = new Roaring64Bitmap();
        if (logStart < cut) {
            try (LogScanner scanner = keys.createLogScanner()) {
                DvPartLogReader.subscribe(
                        scanner, bucket, DvPartLogReader.subscribeOffset(logStart, -1L));
                long next = Math.max(0L, logStart);
                int idlePolls = 0;
                while (next < cut) {
                    ScanRecords records = scanner.poll(KEY_POLL_TIMEOUT);
                    boolean progressed = false;
                    for (ScanRecord record : records.records(bucket)) {
                        progressed = true;
                        long offset = record.logOffset();
                        if (offset >= cut) {
                            next = cut;
                            break;
                        }
                        next = offset + 1;
                        ChangeType changeType = record.getChangeType();
                        if (changeType == ChangeType.UPDATE_BEFORE) {
                            continue;
                        }
                        Long previous =
                                changed.put(
                                        ByteBuffer.wrap(keys.encode(record.getRow())),
                                        changeType == ChangeType.DELETE ? -1L : offset);
                        if (previous != null && previous >= 0) {
                            superseded.addLong(previous);
                        }
                    }
                    Long consumedUpTo = records.consumedUpToOffset(bucket);
                    if (consumedUpTo != null) {
                        progressed = true;
                        next = Math.max(next, consumedUpTo);
                    }
                    if (progressed) {
                        idlePolls = 0;
                    } else if (bucket.getPartitionId() != null
                            && context.isPartitionDropped(bucket.getPartitionId())) {
                        throw new IOException(
                                String.format(
                                        "Partition '%s' was dropped before its log was read.",
                                        target.getPartitionName()));
                    } else if (++idlePolls >= MAX_IDLE_KEY_POLLS) {
                        throw new IOException(
                                String.format(
                                        "The log of %s made no progress at offset %d.",
                                        bucket, next));
                    }
                }
            }
        }
        Map<String, byte[]> lakeMasks = new HashMap<>();
        if (target.getLakeSplits() != null && !changed.isEmpty()) {
            for (LakeSplit lakeSplit : target.getLakeSplits()) {
                List<String> names = DvLakeCalls.fileNames(lakeSplit);
                String onlyFile = names != null && names.size() == 1 ? names.get(0) : null;
                Map<String, Roaring64Bitmap> fileMasks = new HashMap<>();
                try (CloseableIterator<RowWithPosResult> rows = keys.readLakeKeys(lakeSplit)) {
                    while (rows.hasNext()) {
                        RowWithPosResult row = rows.next();
                        if (!changed.containsKey(ByteBuffer.wrap(keys.encode(row.getRow())))) {
                            continue;
                        }
                        String file = row.getFileName() != null ? row.getFileName() : onlyFile;
                        if (file == null) {
                            throw new IOException(
                                    String.format(
                                            "A row of %s does not name its data file.", lakeSplit));
                        }
                        fileMasks
                                .computeIfAbsent(
                                        DvBucketMasks.fileNameOf(file), f -> new Roaring64Bitmap())
                                .addLong(row.getPos());
                    }
                }
                for (Map.Entry<String, Roaring64Bitmap> e : fileMasks.entrySet()) {
                    lakeMasks.put(e.getKey(), DvBucketMasks.serialize(e.getValue()));
                }
            }
        }
        byte[] logMask = target.readsLog() ? DvBucketMasks.serialize(superseded) : null;
        return new DvBucketMaskState(logStart, cut, null, lakeMasks, logMask);
    }
}
