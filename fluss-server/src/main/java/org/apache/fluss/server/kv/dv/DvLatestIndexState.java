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

import org.apache.fluss.lake.dv.index.PositionIndexStore;

import java.util.List;

/**
 * What the latest index of a bucket holds beyond the version it covers. A load ingests into the
 * latest index while the covered version is read through its own RocksDB snapshot, so a load that
 * fails part way leaves the latest index ahead of the version:
 *
 * <ul>
 *   <li>{@code CLEAN}: the latest index is the covered version's.
 *   <li>{@code THROUGH(T)}: it may hold the index changes up to those of snapshot T. A switch must
 *       reach T or a later snapshot to build on it; one to an earlier snapshot would keep T's
 *       changes, so planning refuses it.
 *   <li>{@code FULL}: a full load cleared it and did not complete. Only another full load, which
 *       clears it first, may build on it.
 * </ul>
 */
final class DvLatestIndexState {

    private boolean through;
    private long throughSnapshot = Long.MIN_VALUE;
    private boolean full;
    // a load ingests into the latest index now
    private boolean ingesting;

    /** A load starts to ingest into the latest index. */
    void startIngest() {
        ingesting = true;
    }

    /** A load that started to ingest ended without ingesting anything. */
    void ingestEnded() {
        ingesting = false;
    }

    /** The snapshot whose changes a failed load may have ingested last, or Long.MIN_VALUE. */
    long throughSnapshot() {
        return through ? throughSnapshot : Long.MIN_VALUE;
    }

    /** Whether a load ingests into the latest index now. */
    boolean ingesting() {
        return ingesting;
    }

    /**
     * A load's ingest failed or was cut short, after it may have ingested the changes up to chain
     * index {@code reached}.
     */
    void ingestFailed(List<PositionIndexStore.ChainEntry> chain, int reached) {
        ingesting = false;
        markThrough(chain, reached);
    }

    /** Whether the latest index holds anything the covered version does not. */
    boolean isDirty() {
        return through || full;
    }

    /** Whether a full load cleared the latest index and did not complete. */
    boolean isFull() {
        return full;
    }

    /**
     * The chain index of the last change a failed load may have put in the latest index, -1 if it
     * is clean or that change was trimmed from {@code chain}.
     */
    int throughIndex(List<PositionIndexStore.ChainEntry> chain) {
        if (!through) {
            return -1;
        }
        for (int i = chain.size() - 1; i >= 0; i--) {
            if (chain.get(i).snapshotId == throughSnapshot) {
                return i;
            }
        }
        return -1;
    }

    /** Records that the latest index may hold the changes up to chain index {@code reached}. */
    void markThrough(List<PositionIndexStore.ChainEntry> chain, int reached) {
        if (!through || reached > throughIndex(chain)) {
            throughSnapshot = chain.get(reached).snapshotId;
        }
        through = true;
    }

    /** Records that a full load cleared the latest index. */
    void markFull() {
        full = true;
    }

    /**
     * The bucket covers chain index {@code covered} now, loaded into the latest index; {@code
     * fullLoad} if a full load built it from scratch. It is clean once it includes every change a
     * failed load put in it.
     */
    void covered(int covered, List<PositionIndexStore.ChainEntry> chain, boolean fullLoad) {
        ingesting = false;
        if (fullLoad) {
            full = false;
        }
        if (through && covered >= throughIndex(chain)) {
            through = false;
            throughSnapshot = Long.MIN_VALUE;
        }
    }

    @Override
    public String toString() {
        return full ? "FULL" : through ? "THROUGH(" + throughSnapshot + ")" : "CLEAN";
    }
}
