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

/**
 * A simple data object carrying information for one -U (update-before) or -D (delete) event during
 * the KvTablet write path.
 *
 * <p>The {@code oldRowId} is the changelog offset of the superseded +I/+U record (set as RowId
 * during applyInsert/applyUpdate). It serves three purposes:
 *
 * <ul>
 *   <li>LogDv deletion mark: marks the old changelog offset as superseded
 *   <li>RowPosIndex lookup key: checks if the old record's lake position is known
 *   <li>PendingDeletes key: records the delete for future resolution
 * </ul>
 */
public class DvEntry {

    private final long oldRowId;

    // Set only by derived(); recordOffset is the last log record the change produced.
    private final byte[] key;
    private final long recordOffset;
    private final long versionOffset;

    public DvEntry(long oldRowId) {
        this(oldRowId, null, -1, -1);
    }

    private DvEntry(long oldRowId, byte[] key, long recordOffset, long versionOffset) {
        this.oldRowId = oldRowId;
        this.key = key;
        this.recordOffset = recordOffset;
        this.versionOffset = versionOffset;
    }

    /** A derived-mode change of {@code key}; {@code versionOffset} is -1 for a delete. */
    public static DvEntry derived(byte[] key, long recordOffset, long versionOffset) {
        return new DvEntry(-1, key, recordOffset, versionOffset);
    }

    public byte[] getKey() {
        return key;
    }

    public long getRecordOffset() {
        return recordOffset;
    }

    public long getVersionOffset() {
        return versionOffset;
    }

    /** Returns the RowId (changelog offset) of the superseded +I/+U record. */
    public long getOldRowId() {
        return oldRowId;
    }
}
