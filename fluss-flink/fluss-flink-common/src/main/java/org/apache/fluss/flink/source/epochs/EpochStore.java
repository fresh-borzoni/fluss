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

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Where the readers of a scan record the epochs they end, and its lookups find them; one store
 * serves one domain. Per bucket, it keeps the end of every epoch in which the bucket changed, keyed
 * by the epoch, and how far a reader ended epochs: an epoch up to that one without an end of its
 * own ends where the latest earlier epoch with an end ended.
 */
public interface EpochStore extends AutoCloseable {

    /** The last epoch ended in the bucket, or null if none was. */
    CompletableFuture<Long> endedThrough(TableBucket bucket);

    /**
     * The ends in the bucket of epochs at or before the given one, latest first, at most {@code
     * limit} of them.
     */
    CompletableFuture<List<EpochEnd>> latestEnds(TableBucket bucket, long epoch, int limit);

    /** Records the rows; completes once they are durable and visible to every reader. */
    CompletableFuture<Void> write(List<EpochRecord> records);

    /** The end in the bucket of the latest epoch at or before the given one, or null. */
    default CompletableFuture<EpochEnd> latestEnd(TableBucket bucket, long epoch) {
        return latestEnds(bucket, epoch, 1).thenApply(ends -> ends.isEmpty() ? null : ends.get(0));
    }

    @Override
    default void close() throws Exception {}
}
