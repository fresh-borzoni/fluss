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

package org.apache.fluss.server.kv.dv;

import java.util.Map;

/**
 * The masks of one version of a bucket, exact up to a cut: the lake masks of the version's snapshot
 * and the log mask of [its readable offset, the cut), serialized once. A frontier is immutable, so
 * readers share it without a lock.
 *
 * <p>A frontier is of the mask state it was built from: the owner drops it as the latest when that
 * state's epoch is renewed, and keeps it as the {@link DvState#kept kept} frontier across one
 * switch.
 */
final class DvFrontier {

    // the snapshot of the version the masks are of
    final long snapshotId;
    final DvSnapshot masks;
    final long builtAtNanos;
    // estimated heap bytes, for the budget
    final long bytes;

    DvFrontier(long snapshotId, DvSnapshot masks, long builtAtNanos) {
        this.snapshotId = snapshotId;
        this.masks = masks;
        this.builtAtNanos = builtAtNanos;
        this.bytes = bytesOf(masks);
    }

    /** The log offset the masks go up to. */
    long cut() {
        return masks.getLogEndOffset();
    }

    /** The log offset the masks start at: the readable offset of the version. */
    long readableOffset() {
        return masks.getSnapshotStartOffset();
    }

    /** The masks, answering a read at high watermark {@code hw}. */
    DvSnapshot at(long hw) {
        return masks.getHighWatermark() == hw ? masks : masks.withHighWatermark(hw);
    }

    /** Estimated heap bytes of serialized masks. */
    private static long bytesOf(DvSnapshot masks) {
        long bytes = 128;
        for (Map.Entry<String, byte[]> e : masks.getLakeDvEntries().entrySet()) {
            bytes += 64 + 2L * e.getKey().length() + e.getValue().length;
        }
        byte[] log = masks.getLogDvBitmap();
        return log == null ? bytes : bytes + log.length;
    }
}
