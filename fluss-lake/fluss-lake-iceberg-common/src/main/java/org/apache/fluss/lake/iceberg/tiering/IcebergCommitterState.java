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

package org.apache.fluss.lake.iceberg.tiering;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * What the committers of a tiering job keep across rounds: the live files of each table's last
 * base, and when each table's snapshots were expired last. The job's {@link
 * IcebergLakeTieringFactory} owns it.
 */
final class IcebergCommitterState {

    /** How often a table's snapshots are expired at most. */
    static final long DEFAULT_EXPIRE_INTERVAL_MS = 10 * 60_000L;

    final LiveFilesIndex.Cache liveFiles = new LiveFilesIndex.Cache();
    // when each table's snapshots were expired last, by table location
    private final Map<String, Long> lastExpiration = new ConcurrentHashMap<>();
    private final long expireIntervalMs;

    IcebergCommitterState(long expireIntervalMs) {
        this.expireIntervalMs = expireIntervalMs;
    }

    /** Whether the table's snapshots are due to expire at {@code now}; records it if so. */
    boolean expirationDue(String tableLocation, long now) {
        Long last = lastExpiration.get(tableLocation);
        if (last != null && now - last < expireIntervalMs) {
            return false;
        }
        lastExpiration.put(tableLocation, now);
        return true;
    }
}
