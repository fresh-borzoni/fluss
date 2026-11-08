/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.LakeTableExternallyModifiedException;
import org.apache.fluss.exception.LakeTableRolledBackException;
import org.apache.fluss.lake.paimon.source.PaimonTableLoader.CatalogTableLoader;
import org.apache.fluss.lake.paimon.source.PaimonTableLoader.PaimonTableHandle;
import org.apache.fluss.lake.paimon.source.PaimonTableLoader.ProvidedTableLoader;
import org.apache.fluss.lake.paimon.utils.FlussToPaimonPredicateConverter;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.DataDeltaPlan;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.SupportsRewritePlanning;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.utils.ArrayUtils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toFlussPartitionValues;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toFlussRowType;

/**
 * Paimon Lake format implementation of {@link org.apache.fluss.lake.source.LakeSource} for reading
 * paimon table.
 */
public class PaimonLakeSource
        implements LakeSource<PaimonSplit>, SupportsRewritePlanning<PaimonSplit> {
    private static final long serialVersionUID = 1L;

    private final PaimonTableLoader tableLoader;

    private @Nullable int[][] project;
    private @Nullable org.apache.paimon.predicate.Predicate predicate;

    public PaimonLakeSource(Configuration paimonConfig, TablePath tablePath) {
        this.tableLoader = new CatalogTableLoader(paimonConfig, tablePath);
    }

    /**
     * Creates a source from a loaded table without query-specific scan options.
     *
     * <p>The caller manages metadata freshness and the table's resources.
     */
    public PaimonLakeSource(FileStoreTable table) {
        this.tableLoader = new ProvidedTableLoader(table);
    }

    private PaimonLakeSource(PaimonLakeSource source) {
        this.tableLoader = source.tableLoader;
        this.project = ArrayUtils.deepCopy(source.project);
        this.predicate = source.predicate;
    }

    @Override
    public PaimonLakeSource copy() {
        return new PaimonLakeSource(this);
    }

    @Override
    public void withProject(int[][] project) {
        this.project = ArrayUtils.deepCopy(project);
    }

    @Override
    public void withLimit(int limit) {
        throw new UnsupportedOperationException("Not impl.");
    }

    @Override
    public FilterPushDownResult withFilters(List<Predicate> predicates) {
        if (predicates.isEmpty()) {
            predicate = null;
            return FilterPushDownResult.of(Collections.emptyList(), Collections.emptyList());
        }

        List<Predicate> unConsumedPredicates = new ArrayList<>();
        List<Predicate> consumedPredicates = new ArrayList<>();
        List<org.apache.paimon.predicate.Predicate> converted = new ArrayList<>();
        RowType rowType = getRowType();
        for (Predicate predicate : predicates) {
            Optional<org.apache.paimon.predicate.Predicate> optPredicate =
                    FlussToPaimonPredicateConverter.convert(rowType, predicate);
            if (optPredicate.isPresent()) {
                consumedPredicates.add(predicate);
                converted.add(optPredicate.get());
            } else {
                unConsumedPredicates.add(predicate);
            }
        }
        predicate = converted.isEmpty() ? null : PredicateBuilder.and(converted);
        return FilterPushDownResult.of(consumedPredicates, unConsumedPredicates);
    }

    @Override
    public Planner<PaimonSplit> createPlanner(PlannerContext plannerContext) {
        return new PaimonSplitPlanner(tableLoader, predicate, plannerContext.snapshotId());
    }

    @Override
    public RecordReader createRecordReader(ReaderContext<PaimonSplit> context) throws IOException {
        try (PaimonTableHandle handle = tableLoader.load()) {
            FileStoreTable fileStoreTable = handle.table();
            if (fileStoreTable.primaryKeys().isEmpty()) {
                return new PaimonRecordReader(
                        fileStoreTable, context.lakeSplit(), project, predicate);
            } else {
                return new PaimonSortedRecordReader(
                        fileStoreTable, context.lakeSplit(), project, predicate);
            }
        } catch (Exception e) {
            throw new IOException("Fail to create record reader.", e);
        }
    }

    @Override
    public SimpleVersionedSerializer<PaimonSplit> getSplitSerializer() {
        return new PaimonSplitSerializer();
    }

    /**
     * Identifies a snapshot by its commit (kind, identifier, user, time) and its delta manifest
     * list, a new file per commit. Paimon's rollback deletes the later snapshots and reuses their
     * ids, so the id alone does not identify a snapshot.
     */
    @Override
    public String snapshotIdentity(long snapshotId) throws IOException {
        try (PaimonTableHandle handle = tableLoader.load()) {
            org.apache.paimon.Snapshot snapshot;
            try {
                snapshot = handle.table().snapshotManager().tryGetSnapshot(snapshotId);
            } catch (java.io.FileNotFoundException e) {
                return "none";
            }
            return String.format(
                    "%s commit %d of '%s' at %s (%s)",
                    snapshot.commitKind(),
                    snapshot.commitIdentifier(),
                    snapshot.commitUser(),
                    java.time.Instant.ofEpochMilli(snapshot.timeMillis()),
                    snapshot.deltaManifestList());
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to read snapshot " + snapshotId + " of the table.", e);
        }
    }

    /**
     * Plans the position index change of a table with deletion vectors from {@code baseSnapshotId}
     * to {@code targetSnapshotId}.
     *
     * <p>The readable data files are those a deletion-vector read of the snapshot returns: level 1
     * and up, as level-0 files are skipped. So a file is planned once it becomes readable, whether
     * a compaction wrote it or moved it up from level 0, and is read with the target's deletion
     * vectors. Retained files are not read again: a row the target's deletion vectors newly delete
     * was superseded by a row in a planned file, or its key was deleted.
     */
    @Nullable
    @Override
    public DataDeltaPlan<PaimonSplit> planRewrite(
            long baseSnapshotId, @Nullable Long targetSnapshotId) {
        try (PaimonTableHandle handle = tableLoader.load()) {
            FileStoreTable table = handle.table();
            if (!table.coreOptions().deletionVectorsEnabled()) {
                throw new IllegalStateException(
                        String.format(
                                "Paimon table %s needs '%s' = true for deletion vector masks.",
                                table.name(), CoreOptions.DELETION_VECTORS_ENABLED.key()));
            }
            Long target =
                    targetSnapshotId != null
                            ? targetSnapshotId
                            : table.snapshotManager().latestSnapshotId();
            if (target == null || target == baseSnapshotId) {
                return null;
            }
            // The plan is a change of the base's index; a target older than the base (a
            // rollback) has no such change. Snapshot ids only grow with commits.
            if (baseSnapshotId > 0 && target < baseSnapshotId) {
                throw LakeTableRolledBackException.ofPaimon(
                        table.fullName(), target, baseSnapshotId, null);
            }
            org.apache.fluss.types.RowType partitionType =
                    toFlussRowType(table.schema().logicalPartitionType());
            Map<String, Map<Integer, Set<String>>> baseFiles = new HashMap<>();
            // An expired base is lost: the target's readable files are planned in full.
            boolean baseLost =
                    baseSnapshotId < 0 || !table.snapshotManager().snapshotExists(baseSnapshotId);
            if (!baseLost) {
                for (DataSplit split : readableSplits(table, baseSnapshotId)) {
                    Set<String> files =
                            baseFiles
                                    .computeIfAbsent(
                                            partitionName(split, partitionType),
                                            k -> new HashMap<>())
                                    .computeIfAbsent(split.bucket(), k -> new HashSet<>());
                    for (DataFileMeta file : split.dataFiles()) {
                        files.add(file.fileName());
                    }
                }
            }
            List<PaimonSplit> added = new ArrayList<>();
            Map<String, Map<Integer, Set<String>>> targetFiles = new HashMap<>();
            for (DataSplit split : readableSplits(table, target)) {
                String partitionName = partitionName(split, partitionType);
                Set<String> inBase =
                        baseFiles
                                .getOrDefault(partitionName, Collections.emptyMap())
                                .getOrDefault(split.bucket(), Collections.emptySet());
                Set<String> inTarget =
                        targetFiles
                                .computeIfAbsent(partitionName, k -> new HashMap<>())
                                .computeIfAbsent(split.bucket(), k -> new HashSet<>());
                List<String> partition =
                        split.partition().getFieldCount() == 0
                                ? Collections.emptyList()
                                : toFlussPartitionValues(split.partition(), partitionType);
                for (DataFileMeta file : split.dataFiles()) {
                    String fileName = file.fileName();
                    inTarget.add(fileName);
                    if (!inBase.contains(fileName)) {
                        // one split per file, carrying the file's deletion vector
                        DataSplit single =
                                split.filterDataFile(f -> f.fileName().equals(fileName))
                                        .orElseThrow(IllegalStateException::new);
                        added.add(new PaimonSplit(single, false, partition));
                    }
                }
            }
            Map<String, Map<Integer, List<String>>> removed = new HashMap<>();
            for (Map.Entry<String, Map<Integer, Set<String>>> pe : baseFiles.entrySet()) {
                for (Map.Entry<Integer, Set<String>> be : pe.getValue().entrySet()) {
                    Set<String> inTarget =
                            targetFiles
                                    .getOrDefault(pe.getKey(), Collections.emptyMap())
                                    .getOrDefault(be.getKey(), Collections.emptySet());
                    for (String fileName : be.getValue()) {
                        if (!inTarget.contains(fileName)) {
                            removed.computeIfAbsent(pe.getKey(), k -> new HashMap<>())
                                    .computeIfAbsent(be.getKey(), k -> new ArrayList<>())
                                    .add(fileName);
                        }
                    }
                }
            }
            if (!baseLost && added.isEmpty() && removed.isEmpty()) {
                return null;
            }
            return new DataDeltaPlan<>(target, added, removed);
        } catch (IllegalStateException | LakeTableExternallyModifiedException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to plan the position index change from snapshot %s to %s.",
                            baseSnapshotId, targetSnapshotId),
                    e);
        }
    }

    /** The splits of a deletion-vector read of a snapshot: level 1 and up, with their DVs. */
    private static List<DataSplit> readableSplits(FileStoreTable table, long snapshotId) {
        FileStoreTable atSnapshot =
                table.copyWithoutTimeTravel(
                        Collections.singletonMap(
                                CoreOptions.SCAN_SNAPSHOT_ID.key(), String.valueOf(snapshotId)));
        List<DataSplit> splits = new ArrayList<>();
        for (Split split : atSnapshot.newScan().plan().splits()) {
            splits.add((DataSplit) split);
        }
        return splits;
    }

    /** The Fluss partition name of a split, or null if the table is not partitioned. */
    @Nullable
    private static String partitionName(
            DataSplit split, org.apache.fluss.types.RowType partitionType) {
        if (split.partition().getFieldCount() == 0) {
            return null;
        }
        return String.join(
                ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR,
                toFlussPartitionValues(split.partition(), partitionType));
    }

    private RowType getRowType() {
        try (PaimonTableHandle handle = tableLoader.load()) {
            return handle.table().rowType();
        } catch (Exception e) {
            throw new RuntimeException("Fail to get row type for paimon.", e);
        }
    }
}
