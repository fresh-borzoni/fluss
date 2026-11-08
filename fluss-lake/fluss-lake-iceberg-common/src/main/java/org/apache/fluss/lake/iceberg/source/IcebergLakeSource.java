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

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.iceberg.utils.FlussToIcebergPredicateConverter;
import org.apache.fluss.lake.iceberg.utils.IcebergCatalogUtils;
import org.apache.fluss.lake.iceberg.utils.IcebergSnapshotLineage;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.DataDeltaPlan;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.utils.ArrayUtils;

import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;

/** Iceberg lake source. */
public class IcebergLakeSource implements LakeSource<IcebergSplit> {

    // the most snapshots a rewrite plan replays since the base; with more, both are planned
    private static final int MAX_REPLAYED_SNAPSHOTS = 1_000;
    private static final long serialVersionUID = 1L;
    private final Configuration icebergConfig;
    private final TablePath tablePath;
    private @Nullable int[][] project;
    private @Nullable Expression filter;

    public IcebergLakeSource(Configuration icebergConfig, TablePath tablePath) {
        this.icebergConfig = icebergConfig;
        this.tablePath = tablePath;
    }

    private IcebergLakeSource(IcebergLakeSource source) {
        this.icebergConfig = new Configuration(source.icebergConfig);
        this.tablePath = source.tablePath;
        this.project = ArrayUtils.deepCopy(source.project);
        this.filter = source.filter;
    }

    @Override
    public IcebergLakeSource copy() {
        return new IcebergLakeSource(this);
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
            filter = null;
            return FilterPushDownResult.of(Collections.emptyList(), Collections.emptyList());
        }

        List<Predicate> unConsumedPredicates = new ArrayList<>();
        List<Predicate> consumedPredicates = new ArrayList<>();
        List<Expression> converted = new ArrayList<>();
        Schema schema = getSchema(tablePath);
        for (Predicate predicate : predicates) {
            Optional<Expression> optPredicate =
                    FlussToIcebergPredicateConverter.convert(schema, predicate);
            if (optPredicate.isPresent()) {
                consumedPredicates.add(predicate);
                converted.add(optPredicate.get());
            } else {
                unConsumedPredicates.add(predicate);
            }
        }
        filter = converted.stream().reduce(Expressions::and).orElse(null);
        return FilterPushDownResult.of(consumedPredicates, unConsumedPredicates);
    }

    @Override
    public Planner<IcebergSplit> createPlanner(PlannerContext context) throws IOException {
        return new IcebergSplitPlanner(icebergConfig, tablePath, context.snapshotId(), filter);
    }

    @Override
    public RecordReader createRecordReader(ReaderContext<IcebergSplit> context) throws IOException {
        // The reader takes ownership of the catalog and closes it (releasing its S3 connection
        // pool) when its row iterator closes; otherwise every split leaks an S3FileIO.
        Catalog catalog = IcebergCatalogUtils.createIcebergCatalog(icebergConfig);
        Table table = catalog.loadTable(toIceberg(tablePath));
        return new IcebergRecordReader(context.lakeSplit().fileScanTask(), table, project, catalog);
    }

    @Override
    @Nullable
    public DataDeltaPlan<IcebergSplit> planRewrite(
            long baseSnapshotId, @Nullable Long targetSnapshotId) {
        Catalog catalog = IcebergCatalogUtils.createIcebergCatalog(icebergConfig);
        try {
            Table table = catalog.loadTable(toIceberg(tablePath));
            Snapshot current =
                    targetSnapshotId == null
                            ? table.currentSnapshot()
                            : table.snapshot(targetSnapshotId);
            if (baseSnapshotId <= 0 || current == null || current.snapshotId() == baseSnapshotId) {
                return null;
            }
            // The plan is a change of the base's index. A target that does not descend from the
            // base (a rollback) has no such change.
            IcebergSnapshotLineage.checkDescendsFrom(table, current, baseSnapshotId);
            Function<FileScanTask, Integer> bucketOf =
                    IcebergSplitPlanner.createBucketExtractor(table);
            Function<FileScanTask, List<String>> partitionOf =
                    IcebergSplitPlanner.createPartitionExtractor(table);
            boolean baseLost = table.snapshot(baseSnapshotId) == null;
            if (!baseLost) {
                Replayed replayed =
                        planFromChanges(table, baseSnapshotId, current, bucketOf, partitionOf);
                if (replayed != null) {
                    return replayed.plan;
                }
            }
            Map<String, FileScanTask> baseFiles = new LinkedHashMap<>();
            if (!baseLost) {
                try (CloseableIterable<FileScanTask> tasks =
                        table.newScan().useSnapshot(baseSnapshotId).planFiles()) {
                    for (FileScanTask task : tasks) {
                        baseFiles.put(task.file().location(), task);
                    }
                }
            }
            List<IcebergSplit> added = new ArrayList<>();
            Set<String> currentFiles = new HashSet<>();
            try (CloseableIterable<FileScanTask> tasks =
                    table.newScan().useSnapshot(current.snapshotId()).planFiles()) {
                for (FileScanTask task : tasks) {
                    currentFiles.add(task.file().location());
                    if (!baseFiles.containsKey(task.file().location())) {
                        added.add(
                                new IcebergSplit(
                                        task, bucketOf.apply(task), partitionOf.apply(task)));
                    }
                }
            }
            Map<String, Map<Integer, List<String>>> removed = new HashMap<>();
            for (Map.Entry<String, FileScanTask> e : baseFiles.entrySet()) {
                if (!currentFiles.contains(e.getKey())) {
                    List<String> part = partitionOf.apply(e.getValue());
                    String partitionName =
                            part == null || part.isEmpty()
                                    ? null
                                    : String.join(
                                            ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR, part);
                    removed.computeIfAbsent(partitionName, k -> new HashMap<>())
                            .computeIfAbsent(bucketOf.apply(e.getValue()), k -> new ArrayList<>())
                            .add(IcebergSplit.fileNameOf(e.getValue()));
                }
            }
            if (!baseLost && removed.isEmpty()) {
                return null; // only appends or metadata changes since the base
            }
            return new DataDeltaPlan<>(current.snapshotId(), added, removed);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to plan the re-index of rewritten files.", e);
        } finally {
            closeCatalogQuietly(catalog);
        }
    }

    /**
     * Plans the rewrite from the files the snapshots after the base added and removed, instead of
     * planning both snapshots. Only the manifests those snapshots wrote list these files. The
     * current snapshot is planned only if files were added, for their scan tasks.
     *
     * <p>Returns null if a snapshot in between expired or there are too many to replay. The result
     * holds no plan if files were only added (appends or metadata changes), which is no rewrite.
     */
    @Nullable
    private static Replayed planFromChanges(
            Table table,
            long baseSnapshotId,
            Snapshot current,
            Function<FileScanTask, Integer> bucketOf,
            Function<FileScanTask, List<String>> partitionOf)
            throws IOException {
        List<Snapshot> since = new ArrayList<>();
        for (Snapshot s = current; s.snapshotId() != baseSnapshotId; ) {
            if (since.size() == MAX_REPLAYED_SNAPSHOTS) {
                return null;
            }
            since.add(s);
            s = s.parentId() == null ? null : table.snapshot(s.parentId());
            if (s == null) {
                return null;
            }
        }
        Collections.reverse(since);
        // net changes from the base: a file added and removed in between is in neither
        Map<String, DataFile> added = new LinkedHashMap<>();
        Map<String, DataFile> removed = new LinkedHashMap<>();
        for (Snapshot s : since) {
            for (DataFile file : s.removedDataFiles(table.io())) {
                if (added.remove(file.location()) == null) {
                    removed.put(file.location(), file);
                }
            }
            for (DataFile file : s.addedDataFiles(table.io())) {
                added.put(file.location(), file);
            }
        }
        if (removed.isEmpty()) {
            // only appends or metadata changes since the base
            return new Replayed(null);
        }
        Function<ContentFile<?>, Integer> bucketOfFile =
                IcebergSplitPlanner.createFileBucketExtractor(table);
        Function<ContentFile<?>, List<String>> partitionOfFile =
                IcebergSplitPlanner.createFilePartitionExtractor(table);
        Map<String, Map<Integer, List<String>>> removedByBucket = new HashMap<>();
        for (DataFile file : removed.values()) {
            List<String> part = partitionOfFile.apply(file);
            String partitionName =
                    part == null || part.isEmpty()
                            ? null
                            : String.join(ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR, part);
            String location = file.location();
            removedByBucket
                    .computeIfAbsent(partitionName, k -> new HashMap<>())
                    .computeIfAbsent(bucketOfFile.apply(file), k -> new ArrayList<>())
                    .add(location.substring(location.lastIndexOf('/') + 1));
        }
        List<IcebergSplit> splits = new ArrayList<>();
        if (!added.isEmpty()) {
            // the added files' tasks, with the deletes that apply to them
            try (CloseableIterable<FileScanTask> tasks =
                    table.newScan().useSnapshot(current.snapshotId()).planFiles()) {
                for (FileScanTask task : tasks) {
                    if (added.containsKey(task.file().location())) {
                        splits.add(
                                new IcebergSplit(
                                        task, bucketOf.apply(task), partitionOf.apply(task)));
                    }
                }
            }
        }
        return new Replayed(new DataDeltaPlan<>(current.snapshotId(), splits, removedByBucket));
    }

    /** The rewrite plan replayed from the snapshots since the base, or null for no rewrite. */
    private static final class Replayed {
        @Nullable final DataDeltaPlan<IcebergSplit> plan;

        Replayed(@Nullable DataDeltaPlan<IcebergSplit> plan) {
            this.plan = plan;
        }
    }

    @Override
    public SimpleVersionedSerializer<IcebergSplit> getSplitSerializer() {
        return new IcebergSplitSerializer();
    }

    private Schema getSchema(TablePath tablePath) {
        Catalog catalog = IcebergCatalogUtils.createIcebergCatalog(icebergConfig);
        return catalog.loadTable(toIceberg(tablePath)).schema();
    }

    /** Closes the catalog and its FileIO (S3 connection pool). Never throws. */
    private static void closeCatalogQuietly(Catalog catalog) {
        if (catalog instanceof AutoCloseable) {
            try {
                ((AutoCloseable) catalog).close();
            } catch (Exception ignored) {
                // best-effort; a failed catalog close must not fail planning
            }
        }
    }
}
