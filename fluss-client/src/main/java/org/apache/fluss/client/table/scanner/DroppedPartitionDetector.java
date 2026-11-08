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

package org.apache.fluss.client.table.scanner;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TablePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Tells whether a partition a bounded read subscribed to was dropped from Fluss. A log scanner
 * keeps polling the buckets of a dropped partition without an error, so a bounded log read whose
 * bucket stops making progress asks here before it waits further. The partitions are listed at most
 * once per {@link #CHECK_INTERVAL_MS}.
 */
@Internal
public class DroppedPartitionDetector {

    private static final Logger LOG = LoggerFactory.getLogger(DroppedPartitionDetector.class);

    static final long CHECK_INTERVAL_MS = 2_000;

    private final Supplier<Admin> admin;
    private final TablePath tablePath;

    @Nullable private Set<Long> partitionIds;
    private long listedAtMs;

    public DroppedPartitionDetector(Supplier<Admin> admin, TablePath tablePath) {
        this.admin = admin;
        this.tablePath = tablePath;
    }

    /**
     * Returns whether partition {@code partitionId} was dropped, as of a listing at most {@link
     * #CHECK_INTERVAL_MS} old. If the listing fails, the partition is taken to exist.
     */
    public synchronized boolean isDropped(long partitionId) {
        long now = System.currentTimeMillis();
        if (partitionIds == null || now - listedAtMs >= CHECK_INTERVAL_MS) {
            listedAtMs = now;
            try {
                List<PartitionInfo> partitions = admin.get().listPartitionInfos(tablePath).get();
                Set<Long> ids = new HashSet<>();
                for (PartitionInfo partition : partitions) {
                    ids.add(partition.getPartitionId());
                }
                partitionIds = ids;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (Exception e) {
                LOG.warn("Failed to list the partitions of {}.", tablePath, e);
                return false;
            }
        }
        return !partitionIds.contains(partitionId);
    }
}
