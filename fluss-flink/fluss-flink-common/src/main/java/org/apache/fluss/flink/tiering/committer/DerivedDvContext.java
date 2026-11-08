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

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.tiering.FlussTableLakeSnapshotCommitter;
import org.apache.fluss.client.tiering.TableBucketWriteResult;
import org.apache.fluss.client.tiering.TieringCommitterInitContext;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.FlussConfigUtils;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.committer.PartitionMarkDoneCommitter;
import org.apache.fluss.lake.committer.SupportsDeletionVectorCommit;
import org.apache.fluss.lake.committer.SupportsPositionDeleteCommit;
import org.apache.fluss.lake.dv.DeletionVectorCapabilities;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.SupportsRewritePlanning;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.lake.writer.SupportsDeletionVectorTiering;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * What the derived DV committer and the steps of its rounds share: their configuration and clients,
 * and the lookups every step makes (a table's index store and remote lake directory, its registered
 * lake snapshot, and its partitions).
 */
final class DerivedDvContext<WriteResult, Committable> {

    // Every step logs as the committer: the tiering service's log configuration and tests filter
    // on its category.
    static final Logger LOG = LoggerFactory.getLogger(DerivedDvTieringCommitter.class);

    /** Snapshot property: the id of the position index change a round published. */
    static final String DERIVED_INDEX_ID_PROPERTY = "fluss-dv-index-id";
    /** Snapshot property: the tiered offset of every bucket of the snapshot. */
    static final String DERIVED_OFFSETS_PROPERTY = "fluss-dv-offsets";
    /** Snapshot property: the snapshot a round's index change applies to (its parent). */
    static final String DERIVED_BASE_PROPERTY =
            SupportsDeletionVectorCommit.FLUSS_LAKE_SNAP_BASE_PROPERTY;

    final Admin admin;
    final Configuration flussConfig;
    final Configuration lakeTieringConfig;
    final LakeTieringFactory<WriteResult, Committable> lakeTieringFactory;
    final FlussTableLakeSnapshotCommitter flussTableLakeSnapshotCommitter;
    final DerivedDvMetrics metrics;
    // Directory of the index stores' temporary files; null for the JVM's temporary directory.
    @Nullable final File localTmpDir;

    // One store per table, since it owns the table's file-id counter.
    final Map<Long, PositionIndexStore> indexStores = new ConcurrentHashMap<>();
    // Per table: its remote lake directory, which holds the position index. It is under the
    // table's own remote data directory, which dropping the table deletes.
    final Map<Long, FsPath> lakeTableDirs = new ConcurrentHashMap<>();

    DerivedDvContext(
            Admin admin,
            Configuration flussConfig,
            Configuration lakeTieringConfig,
            LakeTieringFactory<WriteResult, Committable> lakeTieringFactory,
            FlussTableLakeSnapshotCommitter flussTableLakeSnapshotCommitter,
            DerivedDvMetrics metrics,
            @Nullable File localTmpDir) {
        this.admin = admin;
        this.flussConfig = flussConfig;
        this.lakeTieringConfig = lakeTieringConfig;
        this.lakeTieringFactory = lakeTieringFactory;
        this.flussTableLakeSnapshotCommitter = flussTableLakeSnapshotCommitter;
        this.metrics = metrics;
        this.localTmpDir = localTmpDir;
    }

    /**
     * Fails the round if the table was dropped and recreated during tiering: the current table id
     * differs from the committable's {@code tableId}, and committing would write to the new table.
     */
    static void checkSameTable(long tableId, TablePath tablePath, TableInfo tableInfo) {
        if (tableInfo.getTableId() != tableId) {
            throw new IllegalStateException(
                    String.format(
                            "The current table id %s for table path %s is different from the table id %s in the committable. "
                                    + "This usually happens when a table was dropped and recreated during tiering. "
                                    + "Aborting commit to prevent dirty commit.",
                            tableInfo.getTableId(), tablePath, tableId));
        }
    }

    /** Returns the table's index store; one per table, since it owns the file-id counter. */
    PositionIndexStore indexStore(long tableId, TablePath tablePath) {
        PositionIndexStore store = indexStores.get(tableId);
        return store != null ? store : indexStore(tableId, lakeTableDir(tableId, tablePath));
    }

    /** Returns the store of the table whose remote lake directory is {@code lakeDir}. */
    PositionIndexStore indexStore(long tableId, FsPath lakeDir) {
        return indexStores.computeIfAbsent(
                tableId, id -> PositionIndexStore.forTable(lakeDir, localTmpDir));
    }

    /** Remembers the remote lake directory of a table, the round's table being current. */
    void rememberLakeTableDir(long tableId, TableInfo tableInfo) {
        if (tableInfo.getTableId() == tableId) {
            lakeTableDirs.computeIfAbsent(
                    tableId,
                    id ->
                            PositionIndexStore.lakeTableDirOf(
                                    tableInfo,
                                    () -> FlussConfigUtils.getDefaultRemoteDataDir(flussConfig)));
        }
    }

    /** Returns the remote lake directory of a table, which holds its position index. */
    FsPath lakeTableDir(long tableId, TablePath tablePath) {
        FsPath known = lakeTableDirs.get(tableId);
        if (known != null) {
            return known;
        }
        TableInfo tableInfo;
        try {
            tableInfo = admin.getTableInfo(tablePath).get();
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    "Failed to resolve the remote lake directory of " + tablePath, e);
        }
        if (tableInfo.getTableId() != tableId) {
            throw new IllegalStateException(
                    String.format(
                            "Table %s (id %d) was dropped; its position index is not resolved.",
                            tablePath, tableId));
        }
        rememberLakeTableDir(tableId, tableInfo);
        return lakeTableDirs.get(tableId);
    }

    @Nullable
    LakeSnapshot getLatestLakeSnapshot(TablePath tablePath) throws Exception {
        try {
            return admin.getLatestLakeSnapshot(tablePath).get();
        } catch (Exception e) {
            if (e.getCause() instanceof LakeTableSnapshotNotExistException) {
                return null;
            }
            throw e;
        }
    }

    /**
     * Returns the offsets without those of partitions that were dropped from the table: no tablet
     * server has their buckets any more, so a snapshot must not carry them on.
     */
    Map<TableBucket, Long> withoutDroppedPartitions(
            TablePath tablePath, Map<TableBucket, Long> offsets) throws Exception {
        if (offsets.keySet().stream().allMatch(tb -> tb.getPartitionId() == null)) {
            return offsets;
        }
        Set<Long> partitionIds = new HashSet<>();
        // Includes system partitions such as the historical partition, which are tiered too.
        for (PartitionInfo partition : admin.listPartitionInfos(tablePath, true).get()) {
            partitionIds.add(partition.getPartitionId());
        }
        Map<TableBucket, Long> kept = new HashMap<>();
        Set<Long> dropped = new HashSet<>();
        for (Map.Entry<TableBucket, Long> e : offsets.entrySet()) {
            Long partitionId = e.getKey().getPartitionId();
            if (partitionId == null || partitionIds.contains(partitionId)) {
                kept.put(e.getKey(), e.getValue());
            } else {
                dropped.add(partitionId);
            }
        }
        if (!dropped.isEmpty()) {
            LOG.info(
                    "The dropped partitions {} of {} leave the lake snapshot's offsets.",
                    dropped,
                    tablePath);
        }
        return kept;
    }

    /**
     * Returns the round's results without those of partitions the Fluss table dropped, and deletes
     * their data files. A dropped partition's buckets are gone from the tablet servers, so a round
     * with deletes cannot look up and delete the old versions of its rows there; committing the new
     * ones would leave both live in the lake. A registration also leaves the partition's offsets
     * out, which the round's own offsets would put back. Its compactions are left out too, and its
     * staged index parts go with the round's staging change. Nothing reads the partition's log
     * again, so its rows stay as the last registered snapshot has them.
     */
    List<TableBucketWriteResult<WriteResult>> withoutResultsOfDroppedPartitions(
            TablePath tablePath,
            TableInfo tableInfo,
            List<TableBucketWriteResult<WriteResult>> results)
            throws Exception {
        if (!tableInfo.isPartitioned()) {
            return results;
        }
        Set<Long> partitionIds = new HashSet<>();
        // Includes system partitions such as the historical partition, which are tiered too.
        for (PartitionInfo partition : admin.listPartitionInfos(tablePath, true).get()) {
            partitionIds.add(partition.getPartitionId());
        }
        List<TableBucketWriteResult<WriteResult>> kept = new ArrayList<>();
        List<WriteResult> droppedData = new ArrayList<>();
        Set<String> dropped = new TreeSet<>();
        for (TableBucketWriteResult<WriteResult> r : results) {
            Long partitionId = r.tableBucket().getPartitionId();
            if (partitionId == null || partitionIds.contains(partitionId)) {
                kept.add(r);
                continue;
            }
            dropped.add(
                    r.partitionName() == null ? String.valueOf(partitionId) : r.partitionName());
            if (r.writeResult() != null) {
                droppedData.add(r.writeResult());
            }
        }
        if (dropped.isEmpty()) {
            return results;
        }
        LOG.info(
                "Partitions {} of {} were dropped; the round commits none of their {}"
                        + " written buckets.",
                dropped,
                tablePath,
                droppedData.size());
        if (!droppedData.isEmpty()) {
            try (LakeCommitter<WriteResult, Committable> lakeCommitter =
                    lakeTieringFactory.createLakeCommitter(
                            new TieringCommitterInitContext(
                                    tablePath, tableInfo, lakeTieringConfig, flussConfig))) {
                lakeCommitter.abort(lakeCommitter.toCommittable(droppedData));
            } catch (Exception e) {
                LOG.warn(
                        "Could not delete the data files of the dropped partitions {} of {}.",
                        dropped,
                        tablePath,
                        e);
            }
        }
        return kept;
    }

    /**
     * The lake's tiering of tables with deletion vectors. The tiering enumerator refuses such a
     * table on a lake without it before any round, so this fails only if the table was created
     * otherwise.
     */
    SupportsDeletionVectorTiering dvTiering(TablePath tablePath) {
        return DeletionVectorCapabilities.tiering(lakeTieringFactory, tablePath);
    }

    /** The lake source of the table's lake table, which may live under a custom lake path. */
    @SuppressWarnings("unchecked")
    SupportsRewritePlanning<LakeSplit> rewritePlanning(TablePath tablePath, TableInfo tableInfo) {
        LakeSource<LakeSplit> lakeSource =
                (LakeSource<LakeSplit>)
                        dvTiering(tablePath).createLakeSource(tableInfo.getLakeTablePath());
        return DeletionVectorCapabilities.rewritePlanning(lakeSource, tablePath);
    }

    /**
     * Creates the lake committer of a round of a table with deletion vectors, and checks that it
     * commits the way the lake does: it takes deletes ({@link SupportsPositionDeleteCommit}). The
     * round's other deletion-vector calls on it then need no check ({@link #dvCommit}, {@link
     * #positionDeletes}).
     */
    LakeCommitter<WriteResult, Committable> createDvLakeCommitter(
            TablePath tablePath, TableInfo tableInfo) throws IOException {
        SupportsDeletionVectorTiering tiering = dvTiering(tablePath);
        LakeCommitter<WriteResult, Committable> lakeCommitter =
                lakeTieringFactory.createLakeCommitter(
                        new TieringCommitterInitContext(
                                tablePath, tableInfo, lakeTieringConfig, flussConfig));
        try {
            // the tiering enumerator checked it before the table's first round wrote any data
            DeletionVectorCapabilities.checkCommitter(tiering, lakeCommitter, tablePath);
        } catch (RuntimeException e) {
            IOUtils.closeQuietly(lakeCommitter, "lake committer");
            throw e;
        }
        return lakeCommitter;
    }

    /** The deletion-vector calls of a committer of {@link #createDvLakeCommitter}. */
    static SupportsDeletionVectorCommit dvCommit(LakeCommitter<?, ?> lakeCommitter) {
        return (SupportsDeletionVectorCommit) lakeCommitter;
    }

    /**
     * The delete commits of a committer of {@link #createDvLakeCommitter} of a lake that takes
     * deletes.
     */
    static <WriteResult, Committable>
            SupportsPositionDeleteCommit<WriteResult, Committable> positionDeletes(
                    LakeCommitter<WriteResult, Committable> lakeCommitter) {
        return (SupportsPositionDeleteCommit<WriteResult, Committable>) lakeCommitter;
    }

    /** Prepares the partition mark-done of a round; returns whether the state changed. */
    @SuppressWarnings("unchecked")
    boolean preparePartitionMarkDone(
            LakeCommitter<WriteResult, Committable> lakeCommitter, Committable committable)
            throws IOException {
        checkState(
                lakeCommitter instanceof PartitionMarkDoneCommitter,
                "Lake tiering factory %s declares partition mark-done support but its committer does not.",
                lakeTieringFactory.getClass().getName());
        return ((PartitionMarkDoneCommitter<WriteResult, Committable>) lakeCommitter)
                .preparePartitionMarkDone(committable);
    }

    /** The name of the split's partition, or null if the table is not partitioned. */
    @Nullable
    static String partitionName(LakeSplit split) {
        List<String> values = split.partition();
        return values == null || values.isEmpty()
                ? null
                : String.join(ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR, values);
    }
}
