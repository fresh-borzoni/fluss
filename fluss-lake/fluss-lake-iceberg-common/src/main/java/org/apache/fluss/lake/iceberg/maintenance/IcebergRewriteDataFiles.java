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

package org.apache.fluss.lake.iceberg.maintenance;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.lake.dv.FormatVersions;
import org.apache.fluss.lake.dv.index.CompactionIndexPart;
import org.apache.fluss.lake.iceberg.source.IcebergSplitPlanner;
import org.apache.fluss.lake.iceberg.tiering.writer.TaskWriterFactory;
import org.apache.fluss.lake.iceberg.utils.IcebergPartitionSpecUtils;
import org.apache.fluss.metadata.TableBucket;

import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenericReader;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.util.BinPacking;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.DataFileSet;
import org.apache.iceberg.util.DeleteFileSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toFilterExpression;
import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Concrete implementation for Fluss's Iceberg integration. Handles bin-packing compaction of small
 * files into larger ones.
 */
public class IcebergRewriteDataFiles {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergRewriteDataFiles.class);

    /**
     * Snapshot property of a Fluss compaction of a table with deletion vectors: the buckets it
     * compacted, as {@link #bucketKey} values separated by commas.
     */
    public static final String COMPACTED_BUCKETS_PROPERTY = "fluss-compacted-buckets";

    // rows between two checks for a requested stop
    private static final int STOP_CHECK_ROWS = 1024;

    // TODO: make compaction strategy configurable
    private static final int MIN_FILES_TO_COMPACT = 3;

    private final Table table;
    private final String partition;
    private final TableBucket bucket;
    private final Expression filter;
    private long targetSizeInBytes = 128 * 1024 * 1024; // 128MB default
    // hands the new positions of the rewritten rows to the position index, for deletion vectors
    @Nullable private CompactionIndexWriter indexWriter;
    // the tiered policy of tables with deletion vectors, or bin-packing of every file if unset
    private int minSmallFiles = -1;
    private long intervalMillis;
    // set to stop between and within file groups; the groups done so far are kept
    private volatile boolean stopRequested;

    public IcebergRewriteDataFiles(Table table, @Nullable String partition, TableBucket bucket) {
        this.table = table;
        this.partition = partition;
        this.bucket = bucket;
        this.filter = toFilterExpression(table, partition, bucket.getBucket());
    }

    public IcebergRewriteDataFiles targetSizeInBytes(long targetSize) {
        this.targetSizeInBytes = targetSize;
        return this;
    }

    /**
     * Hands the new position of every rewritten row to the position index of a table with deletion
     * vectors, so the rewrite needs no re-index (see {@link RewriteDataFileResult#indexPart()}).
     */
    public IcebergRewriteDataFiles handOffPositions(CompactionIndexWriter indexWriter) {
        this.indexWriter = indexWriter;
        return this;
    }

    /**
     * Compacts with the {@link TieredCompactionPolicy} instead of bin-packing every file. The
     * bucket is compacted only once {@code interval} has passed since its last compaction, as the
     * table's snapshots record it, and only the file groups the policy selects are rewritten.
     */
    public IcebergRewriteDataFiles tiered(int minSmallFiles, Duration interval) {
        this.minSmallFiles = minSmallFiles;
        this.intervalMillis = interval.toMillis();
        return this;
    }

    /**
     * Stops the compaction at the next row: the file group in progress is dropped with its files,
     * and the result holds the groups done so far, or nothing.
     */
    public void requestStop() {
        stopRequested = true;
    }

    /** Runs after a file group is rewritten; a hook for tests. */
    @VisibleForTesting
    protected void groupCompleted(int groupIndex) {}

    /**
     * The key of a bucket in the {@link #COMPACTED_BUCKETS_PROPERTY} of a compaction snapshot:
     * {@code partitionId|bucket}, with an empty partition id if the table is not partitioned.
     */
    public static String bucketKey(TableBucket bucket) {
        return (bucket.getPartitionId() == null ? "" : bucket.getPartitionId())
                + "|"
                + bucket.getBucket();
    }

    private List<CombinedScanTask> planRewriteFileGroups(long snapshotId) throws IOException {
        List<FileScanTask> fileScanTasks = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks =
                table.newScan()
                        .useSnapshot(snapshotId)
                        .includeColumnStats()
                        .filter(filter)
                        .ignoreResiduals()
                        .planFiles()) {
            tasks.forEach(fileScanTasks::add);
        }
        // A clean table has no __bucket column to filter on. If its files are partitioned by
        // bucket(bucketKey), keep those of this bucket by their partition value.
        if (table.schema().findField(BUCKET_COLUMN_NAME) == null
                && table.spec().fields().stream()
                        .anyMatch(IcebergPartitionSpecUtils::isBucketTransform)) {
            Function<FileScanTask, Integer> bucketOf =
                    IcebergSplitPlanner.createBucketExtractor(table);
            fileScanTasks.removeIf(task -> bucketOf.apply(task) != bucket.getBucket());
        }

        if (minSmallFiles > 0) {
            return planTiered(fileScanTasks);
        }

        // the files < targetSizeInBytes is less than MIN_FILES_TO_COMPACT, don't compact
        if (fileScanTasks.stream()
                        .filter(fileScanTask -> fileScanTask.length() < targetSizeInBytes)
                        .count()
                < MIN_FILES_TO_COMPACT) {
            // return empty file group
            return Collections.emptyList();
        }

        // then, pack the fileScanTasks into compaction units which contains compactable
        // fileScanTasks, after compaction, we want to it still keep order by __offset column,
        // so, let's first sort by __offset column. FIP-27: a clean table has no __offset column,
        // so this ordering is skipped (files are packed as-is).
        org.apache.iceberg.types.Types.NestedField offsetField =
                table.schema().findField(OFFSET_COLUMN_NAME);
        if (offsetField != null) {
            fileScanTasks.sort(sortFileScanTask(offsetField.fieldId()));
        }

        // do package now
        BinPacking.ListPacker<FileScanTask> packer =
                new BinPacking.ListPacker<>(targetSizeInBytes, 1, false);
        return packer.pack(fileScanTasks, ContentScanTask::length).stream()
                .filter(tasks -> tasks.size() > 1)
                .map(BaseCombinedScanTask::new)
                .collect(Collectors.toList());
    }

    /** Groups the files the {@link TieredCompactionPolicy} selects, smallest tier first. */
    private List<CombinedScanTask> planTiered(List<FileScanTask> fileScanTasks) {
        Map<String, FileScanTask> byPath = new HashMap<>();
        List<TieredCompactionPolicy.FileStats> stats = new ArrayList<>();
        for (FileScanTask task : fileScanTasks) {
            String path = task.file().location();
            byPath.put(path, task);
            stats.add(
                    new TieredCompactionPolicy.FileStats(
                            path,
                            task.file().fileSizeInBytes(),
                            task.file().recordCount(),
                            deletedRows(task)));
        }
        List<CombinedScanTask> groups = new ArrayList<>();
        for (List<TieredCompactionPolicy.FileStats> group :
                TieredCompactionPolicy.select(
                        stats,
                        targetSizeInBytes,
                        minSmallFiles,
                        TieredCompactionPolicy.UNLIMITED_BYTES)) {
            List<FileScanTask> tasks = new ArrayList<>();
            for (TieredCompactionPolicy.FileStats f : group) {
                tasks.add(byPath.get(f.path()));
            }
            groups.add(new BaseCombinedScanTask(tasks));
        }
        return groups;
    }

    /**
     * The rows of a data file that its deletion vector or file-scoped position deletes hide.
     * Deletes that may cover other files too are not counted.
     */
    private static long deletedRows(FileScanTask task) {
        long deleted = 0;
        for (DeleteFile delete : task.deletes()) {
            if (delete.content() == FileContent.POSITION_DELETES
                    && delete.referencedDataFile() != null
                    && delete.referencedDataFile().toString().equals(task.file().location())) {
                deleted += delete.recordCount();
            }
        }
        return deleted;
    }

    /**
     * Whether the bucket's last Fluss compaction is at least the interval old: no compaction
     * snapshot that lists the bucket was committed within the interval.
     */
    private boolean intervalPassed(Snapshot current) {
        long cutoff = System.currentTimeMillis() - intervalMillis;
        String key = bucketKey(bucket);
        for (Snapshot s = current; s != null && s.timestampMillis() >= cutoff; ) {
            Map<String, String> summary = s.summary();
            String buckets = summary == null ? null : summary.get(COMPACTED_BUCKETS_PROPERTY);
            if (buckets != null) {
                FormatVersions.checkSnapshotVersion(s.snapshotId(), summary);
                if (Arrays.asList(buckets.split(",")).contains(key)) {
                    return false;
                }
            }
            s = s.parentId() == null ? null : table.snapshot(s.parentId());
        }
        return true;
    }

    private Comparator<FileScanTask> sortFileScanTask(int sortFiledId) {
        return (f1, f2) -> {
            ByteBuffer buffer1 =
                    f1.file()
                            .lowerBounds()
                            .get(sortFiledId)
                            .order(ByteOrder.LITTLE_ENDIAN)
                            .rewind();
            long offset1 = buffer1.getLong();
            ByteBuffer buffer2 =
                    f2.file()
                            .lowerBounds()
                            .get(sortFiledId)
                            .order(ByteOrder.LITTLE_ENDIAN)
                            .rewind();
            long offset2 = buffer2.getLong();
            return Long.compare(offset1, offset2);
        };
    }

    @Nullable
    public RewriteDataFileResult execute() {
        try {
            // plan the file groups to be rewrite
            Snapshot snapshot = table.currentSnapshot();
            // if no snapshot, just return
            if (snapshot == null) {
                return null;
            }
            if (minSmallFiles > 0 && !intervalPassed(snapshot)) {
                return null;
            }
            List<CombinedScanTask> tasksToRewrite = planRewriteFileGroups(snapshot.snapshotId());
            if (tasksToRewrite.isEmpty()) {
                return null;
            }
            LOG.info("Start to rewrite files {}.", tasksToRewrite);
            DataFileSet deletedDataFiles = DataFileSet.create();
            DataFileSet addedDataFiles = DataFileSet.create();
            List<CombinedScanTask> rewritten = new ArrayList<>();
            for (CombinedScanTask combinedScanTask : tasksToRewrite) {
                DataFileSet added = stopRequested ? null : rewriteFileGroup(combinedScanTask);
                if (added == null) {
                    LOG.info(
                            "Stopped compacting bucket {} of {} after {} of {} file groups.",
                            bucket,
                            table.name(),
                            rewritten.size(),
                            tasksToRewrite.size());
                    break;
                }
                addedDataFiles.addAll(added);
                deletedDataFiles.addAll(
                        combinedScanTask.files().stream()
                                .map(ContentScanTask::file)
                                .collect(Collectors.toList()));
                rewritten.add(combinedScanTask);
                groupCompleted(rewritten.size() - 1);
            }
            if (rewritten.isEmpty()) {
                return null;
            }
            DeleteFileSet deletedDeleteFiles =
                    deleteFilesOfReplacedFiles(rewritten, deletedDataFiles);
            LOG.info(
                    "Finish rewriting files from {} to {}, dropping delete files {}.",
                    deletedDataFiles,
                    addedDataFiles,
                    deletedDeleteFiles);
            CompactionIndexPart indexPart = null;
            if (indexWriter != null) {
                List<String> removed = new ArrayList<>();
                deletedDataFiles.forEach(
                        f -> removed.add(CompactionIndexWriter.fileName(f.location())));
                indexPart = indexWriter.complete(snapshot.snapshotId(), removed);
            }
            return new RewriteDataFileResult(
                    snapshot.snapshotId(),
                    deletedDataFiles,
                    addedDataFiles,
                    deletedDeleteFiles,
                    indexPart,
                    bucketKey(bucket));
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Fail to compact bucket %s of table %s.", bucket, table.name()),
                    e);
        } finally {
            if (indexWriter != null) {
                // complete() deletes the sorted runs, but a stopped or failed compaction never
                // calls it
                indexWriter.close();
            }
        }
    }

    /**
     * Returns the position delete files that reference only replaced data files. The rewrite
     * applied them, and they would otherwise stay in the table pointing at removed files. Deletion
     * vectors are left out, as Iceberg drops those of removed data files itself. A delete file
     * without a referenced data file may cover other files, so it is kept.
     */
    private static DeleteFileSet deleteFilesOfReplacedFiles(
            List<CombinedScanTask> tasks, DataFileSet replacedDataFiles) {
        Set<String> replaced = new HashSet<>();
        replacedDataFiles.forEach(f -> replaced.add(f.location()));
        DeleteFileSet result = DeleteFileSet.create();
        for (CombinedScanTask combinedScanTask : tasks) {
            for (FileScanTask task : combinedScanTask.files()) {
                for (DeleteFile delete : task.deletes()) {
                    if (delete.content() == FileContent.POSITION_DELETES
                            && !ContentFileUtil.isDV(delete)
                            && delete.referencedDataFile() != null
                            && replaced.contains(delete.referencedDataFile().toString())) {
                        result.add(delete);
                    }
                }
            }
        }
        return result;
    }

    /**
     * Rewrites one file group and returns its new files. Returns null if the compaction was stopped
     * meanwhile; the files written for the group are then deleted.
     */
    @Nullable
    private DataFileSet rewriteFileGroup(CombinedScanTask combinedScanTask) throws IOException {
        try (CloseableIterable<Record> records = readDataFile(combinedScanTask);
                TaskWriter<Record> taskWriter =
                        TaskWriterFactory.createRewriteWriter(
                                table, partition, bucket.getBucket())) {
            long rows = 0;
            for (Record record : records) {
                if (++rows % STOP_CHECK_ROWS == 0 && stopRequested) {
                    taskWriter.abort();
                    if (indexWriter != null) {
                        indexWriter.groupDiscarded();
                    }
                    return null;
                }
                taskWriter.write(record);
                if (indexWriter != null) {
                    indexWriter.add(record);
                }
            }
            WriteResult rewriteResult = taskWriter.complete();
            checkState(
                    rewriteResult.deleteFiles().length == 0,
                    "the delete files should be empty, but got "
                            + Arrays.toString(rewriteResult.deleteFiles()));
            if (indexWriter != null) {
                // one rolling writer completes its files in write order
                indexWriter.groupWritten(rewriteResult.dataFiles());
            }
            return DataFileSet.of(Arrays.asList(rewriteResult.dataFiles()));
        }
    }

    private CloseableIterable<Record> readDataFile(CombinedScanTask combinedScanTask) {
        // The reader applies the files' deletes, so the rewritten files hold live rows only. On a
        // table with row lineage, it reads every row's lineage too, which the rewrite keeps: the
        // values a file stores, or those it inherits from its first row id and sequence number.
        IcebergGenericReader reader =
                new IcebergGenericReader(
                        table.newScan().project(TaskWriterFactory.rewriteSchema(table)), true);
        return reader.open(combinedScanTask);
    }
}
