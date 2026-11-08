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

import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.TieringStats;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registers lake snapshots in Fluss and keeps what the index chain needs.
 *
 * <p>A snapshot is registered in Fluss only once its index change is in the chain, so a tablet
 * server can always cover it. Each registration keeps the registered snapshots from the chain's
 * oldest one on ({@link #earliestSnapshotToKeep}).
 *
 * <p>Crash rule: every step here is repeatable. A registration the chain already has is registered
 * again as a no-op.
 */
final class DerivedDvRegistrations<WriteResult, Committable> {

    private static final Logger LOG = DerivedDvContext.LOG;

    private final DerivedDvContext<WriteResult, Committable> context;

    // Per table: the oldest snapshot Fluss keeps registrations from; known to be registered.
    private final Map<Long, Long> retainedFrom = new ConcurrentHashMap<>();

    DerivedDvRegistrations(DerivedDvContext<WriteResult, Committable> context) {
        this.context = context;
    }

    /**
     * The result of a commit of a lake that takes deletes: the deletes are in the same commit, so
     * it is readable at its tiered offsets. Fluss keeps the registered snapshots from {@code
     * earliestToKeep} on (see {@link #earliestSnapshotToKeep}).
     */
    static LakeCommitResult readableAtTieredOffsets(
            long snapshotId,
            Map<TableBucket, Long> offsets,
            @Nullable TieringStats stats,
            @Nullable Long earliestToKeep) {
        return LakeCommitResult.withReadableSnapshot(
                snapshotId, snapshotId, offsets, offsets, earliestToKeep, stats);
    }

    /**
     * Returns the earliest registered lake snapshot Fluss must keep, with its offsets files: the
     * snapshot of the chain's oldest entry. Tablet servers catch up only to snapshots in the chain,
     * and a server rebuilding a bucket without a checkpoint of a trimmed chain reads the bucket's
     * offset in the chain's oldest snapshot. Nothing reads the registrations before it. Trimming
     * the chain thus moves the boundary on: Fluss discards the older registrations and their
     * offsets files, so the table's lake snapshot list stays as long as the chain.
     *
     * <p>Fluss keeps everything if the boundary is not registered, so this takes the first chain
     * snapshot Fluss has registered. Returns null (keep the new snapshot only) if there is none,
     * e.g. on the first round. Registrations before the boundary never come back, so a boundary
     * that is still in the chain is not checked again.
     */
    @Nullable
    Long earliestSnapshotToKeep(long tableId, TablePath tablePath) throws Exception {
        List<PositionIndexStore.ChainEntry> chain = context.indexStore(tableId, tablePath).chain();
        Long known = retainedFrom.get(tableId);
        if (known != null && chainIndexOf(chain, known) >= 0) {
            return known;
        }
        for (PositionIndexStore.ChainEntry e : chain) {
            if (isRegistered(tablePath, e.snapshotId)) {
                retainedFrom.put(tableId, e.snapshotId);
                return e.snapshotId;
            }
        }
        return LakeCommitResult.KEEP_LATEST;
    }

    /** Whether Fluss has the lake snapshot registered. */
    private boolean isRegistered(TablePath tablePath, long snapshotId) throws Exception {
        try {
            context.admin.getLakeSnapshot(tablePath, snapshotId).get();
            return true;
        } catch (Exception e) {
            if (ExceptionUtils.findThrowable(e, LakeTableSnapshotNotExistException.class)
                    .isPresent()) {
                return false;
            }
            throw e;
        }
    }

    static int chainIndexOf(List<PositionIndexStore.ChainEntry> chain, long snapshotId) {
        for (int i = 0; i < chain.size(); i++) {
            if (chain.get(i).snapshotId == snapshotId) {
                return i;
            }
        }
        return -1;
    }
}
