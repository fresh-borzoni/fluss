/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.dv.index;

import org.apache.fluss.lake.dv.RowPosSstIndex;
import org.apache.fluss.lake.dv.index.PositionIndexStore.ChainEntry;
import org.apache.fluss.metadata.TableBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.fluss.lake.dv.index.IndexFiles.lines;
import static org.apache.fluss.lake.dv.index.IndexLayout.partitionKey;

/**
 * The buckets each committed change of the {@link FlussPositionIndexStore} touches, read from its
 * manifests through the store, which a caching store overrides. A committed change never changes,
 * so each is read once.
 */
final class TouchedBuckets {

    // the store's logger
    private static final Logger LOG = LoggerFactory.getLogger(FlussPositionIndexStore.class);

    // the store's hooks: the reads of committed metadata
    private final FlussPositionIndexStore store;
    // change id -> partition key -> the buckets the change touches
    private final Map<Long, Map<Long, Set<Integer>>> cache = new ConcurrentHashMap<>();

    TouchedBuckets(FlussPositionIndexStore store) {
        this.store = store;
    }

    /** Whether a registered change touches the bucket; true if that cannot be told. */
    boolean touches(ChainEntry entry, TableBucket bucket) {
        Map<Long, Set<Integer>> touched = of(entry.changeId);
        if (touched == null) {
            return true;
        }
        Set<Integer> buckets = touched.get(partitionKey(bucket));
        return buckets != null && buckets.contains(bucket.getBucket());
    }

    /**
     * The buckets a committed change touches, by partition key: those its manifests give SSTs,
     * added or removed files. Null if they cannot be read.
     */
    @Nullable
    Map<Long, Set<Integer>> of(long changeId) {
        Map<Long, Set<Integer>> cached = cache.get(changeId);
        if (cached != null) {
            return cached;
        }
        Map<Long, Set<Integer>> touched = new HashMap<>();
        try {
            String partitions = store.readCommittedPartitions(changeId);
            if (partitions == null) {
                return null;
            }
            for (String line : lines(partitions)) {
                long pid = Long.parseLong(line);
                RowPosSstIndex index = store.readManifest(changeId, pid == -1L ? null : pid);
                Set<Integer> buckets = new HashSet<>();
                for (int b : index.getBucketIds()) {
                    if (!index.getFiles(b).isEmpty()
                            || !index.getFileId2Name(b).isEmpty()
                            || !index.getReplacedFiles(b).isEmpty()) {
                        buckets.add(b);
                    }
                }
                touched.put(pid, buckets);
            }
        } catch (IOException | RuntimeException e) {
            LOG.debug("Cannot tell the buckets index change {} touches.", changeId, e);
            return null;
        }
        cache.put(changeId, touched);
        return touched;
    }

    /** Forgets a change a trim deleted. */
    void forget(long changeId) {
        cache.remove(changeId);
    }
}
