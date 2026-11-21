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

package org.apache.fluss.flink.source.epochs;

import org.apache.fluss.metadata.TableBucket;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/** An {@link EpochStore} in memory, whose writes can wait until a test completes them. */
final class TestingEpochStore implements EpochStore {

    private final Map<TableBucket, TreeMap<Long, Long>> ends = new HashMap<>();
    private final Map<TableBucket, Long> progress = new HashMap<>();
    private final List<List<EpochRecord>> pendingRecords = new ArrayList<>();
    private final List<CompletableFuture<Void>> pendingWrites = new ArrayList<>();

    final List<List<EpochRecord>> writes = new ArrayList<>();
    final AtomicInteger reads = new AtomicInteger();

    /** Whether writes wait for {@link #completeWrites()}. */
    boolean holdWrites;

    synchronized void putEnd(TableBucket bucket, long epoch, long end) {
        ends.computeIfAbsent(bucket, b -> new TreeMap<>()).put(epoch, end);
    }

    synchronized void putProgress(TableBucket bucket, long endedThrough) {
        progress.put(bucket, endedThrough);
    }

    synchronized TreeMap<Long, Long> ends(TableBucket bucket) {
        return new TreeMap<>(ends.getOrDefault(bucket, new TreeMap<>()));
    }

    synchronized Long progress(TableBucket bucket) {
        return progress.get(bucket);
    }

    synchronized int pendingWrites() {
        return pendingWrites.size();
    }

    /** Applies and completes the writes that wait. */
    void completeWrites() {
        List<CompletableFuture<Void>> completed;
        synchronized (this) {
            for (List<EpochRecord> records : pendingRecords) {
                apply(records);
            }
            pendingRecords.clear();
            completed = new ArrayList<>(pendingWrites);
            pendingWrites.clear();
        }
        completed.forEach(write -> write.complete(null));
    }

    @Override
    public synchronized CompletableFuture<Long> endedThrough(TableBucket bucket) {
        reads.incrementAndGet();
        return CompletableFuture.completedFuture(progress.get(bucket));
    }

    @Override
    public synchronized CompletableFuture<List<EpochEnd>> latestEnds(
            TableBucket bucket, long epoch, int limit) {
        reads.incrementAndGet();
        List<EpochEnd> result = new ArrayList<>();
        TreeMap<Long, Long> bucketEnds = ends.get(bucket);
        if (bucketEnds != null) {
            NavigableMap<Long, Long> atOrBefore = bucketEnds.headMap(epoch, true).descendingMap();
            for (Map.Entry<Long, Long> end : atOrBefore.entrySet()) {
                if (result.size() == limit) {
                    break;
                }
                result.add(new EpochEnd(end.getKey(), end.getValue()));
            }
        }
        return CompletableFuture.completedFuture(result);
    }

    @Override
    public synchronized CompletableFuture<Void> write(List<EpochRecord> records) {
        writes.add(new ArrayList<>(records));
        if (holdWrites) {
            CompletableFuture<Void> write = new CompletableFuture<>();
            pendingRecords.add(new ArrayList<>(records));
            pendingWrites.add(write);
            return write;
        }
        apply(records);
        return CompletableFuture.completedFuture(null);
    }

    private void apply(List<EpochRecord> records) {
        for (EpochRecord record : records) {
            if (record.isProgress()) {
                progress.put(record.getBucket(), record.getEpoch());
            } else {
                putEnd(record.getBucket(), record.getEpoch(), record.getEnd());
            }
        }
    }
}
