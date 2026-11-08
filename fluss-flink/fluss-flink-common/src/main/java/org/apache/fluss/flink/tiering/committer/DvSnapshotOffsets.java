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

package org.apache.fluss.flink.tiering.committer;

import org.apache.fluss.metadata.TableBucket;

import java.util.HashMap;
import java.util.Map;

/**
 * The tiered offsets of every bucket of a lake snapshot, as a round of a table with deletion
 * vectors records them in the snapshot property {@code fluss-dv-offsets}.
 */
final class DvSnapshotOffsets {

    private DvSnapshotOffsets() {}

    /** Encodes "partitionId|bucket=offset,...", with an empty partitionId if unpartitioned. */
    static String encode(Map<TableBucket, Long> offsets) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<TableBucket, Long> e : offsets.entrySet()) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            Long pid = e.getKey().getPartitionId();
            sb.append(pid == null ? "" : pid)
                    .append('|')
                    .append(e.getKey().getBucket())
                    .append('=')
                    .append(e.getValue());
        }
        return sb.toString();
    }

    static Map<TableBucket, Long> decode(long tableId, String encoded) {
        Map<TableBucket, Long> out = new HashMap<>();
        if (encoded == null || encoded.isEmpty()) {
            return out;
        }
        for (String part : encoded.split(",")) {
            String[] kv = part.split("=");
            String[] pb = kv[0].split("\\|", -1);
            Long pid = pb[0].isEmpty() ? null : Long.parseLong(pb[0]);
            out.put(new TableBucket(tableId, pid, Integer.parseInt(pb[1])), Long.parseLong(kv[1]));
        }
        return out;
    }
}
