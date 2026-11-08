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
 * One change of a primary key on the KV write path, handed to the {@link DerivedDvManager} once its
 * changelog is appended.
 */
public class DvEntry {

    private final byte[] key;
    private final long recordOffset;
    private final long versionOffset;
    private final boolean insert;
    // the offset of the version the change supersedes, the tag of the key's KV value, or -1
    private final long previousVersionOffset;

    /**
     * @param key the encoded primary key
     * @param recordOffset the offset of the last log record the change produced
     * @param versionOffset the offset of the record holding the key's new version, or -1 if the
     *     change deleted the key
     * @param insert whether the key had no live version before the change. A lake snapshot whose
     *     readable offset is at or before the change then has no row of the key either.
     * @param previousVersionOffset the offset of the version the change supersedes, which the key's
     *     KV value is tagged with, or -1 if there is none or it is not known: a version before the
     *     log tail, in the lake
     */
    public DvEntry(
            byte[] key,
            long recordOffset,
            long versionOffset,
            boolean insert,
            long previousVersionOffset) {
        this.key = key;
        this.recordOffset = recordOffset;
        this.versionOffset = versionOffset;
        this.insert = insert;
        this.previousVersionOffset = previousVersionOffset;
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

    public boolean isInsert() {
        return insert;
    }

    public long getPreviousVersionOffset() {
        return previousVersionOffset;
    }
}
