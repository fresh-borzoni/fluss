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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.lake.dv.PreparedDeletionVectors;
import org.apache.fluss.lake.iceberg.tiering.writer.IcebergDvFileWriter;
import org.apache.fluss.lake.writer.DeletionVectorPreparer;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.DeleteWriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Prepares the deletion vectors of a bucket's round on the tiering readers of an Iceberg table with
 * deletion vectors (format version 3), in one Puffin file per bucket (see {@link
 * DeletionVectorPreparer}). The base's data files the round deletes rows in, and their current
 * deletion vectors, come from a {@link LiveFilesIndex} of the base that the committer and the
 * preparers of a TaskManager share and advance snapshot by snapshot, so a round reads only the
 * manifests the snapshots since the last round wrote.
 */
final class IcebergDeletionVectorPreparer implements DeletionVectorPreparer<IcebergWriteResult> {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergDeletionVectorPreparer.class);

    // the table, which the preparers of the TaskManager share with its catalog
    private final SharedIcebergTables.Handle tableHandle;
    private final Table table;
    // the live files of the TaskManager's tables, which its committer shares
    private final LiveFilesIndex.Cache liveFiles;
    private final PositionDeletesLoader loader;
    // advances the live files to the snapshots rounds start on, while the rounds are read, and
    // reads the current deletion vectors of the files their deletes name once those arrive
    @Nullable private ExecutorService prefetcher;
    private long prefetched = -1L;
    // by bucket, the files the deletes of its round name, with their current deletion vectors
    private final Map<TableBucket, Prefetched> prefetchedFiles = new ConcurrentHashMap<>();

    IcebergDeletionVectorPreparer(
            SharedIcebergTables.Handle tableHandle,
            LiveFilesIndex.Cache liveFiles,
            int loadThreads,
            long loadMaxBytes) {
        this.tableHandle = tableHandle;
        this.table = tableHandle.table();
        this.liveFiles = liveFiles;
        this.loader = new PositionDeletesLoader(table, loadThreads, loadMaxBytes);
    }

    @Override
    @Nullable
    public IcebergWriteResult prepare(
            IcebergWriteResult writeResult,
            TableBucket bucket,
            long snapshotId,
            long startOffset,
            long endOffset,
            Map<String, long[]> deletedPositionsByFile)
            throws IOException {
        Prefetched prefetched = prefetchedFiles.remove(bucket);
        // the round rewrote the bucket's files, which its deletes on the base name
        if (writeResult.stagedIndexPart() == null || writeResult.compacted()) {
            return null;
        }
        BaseFiles base =
                prefetched != null
                                && prefetched.snapshotId == snapshotId
                                && prefetched.dataFiles.equals(deletedPositionsByFile.keySet())
                        ? prefetched.join()
                        : null;
        if (base == null) {
            base = baseFiles(snapshotId, deletedPositionsByFile.keySet());
        }
        if (base.files == null) {
            return null;
        }
        Map<DataFile, long[]> deletes = new HashMap<>();
        for (Map.Entry<String, long[]> e : deletedPositionsByFile.entrySet()) {
            deletes.put(base.files.get(e.getKey()), e.getValue());
        }
        PreparedDeletionVectors round =
                new PreparedDeletionVectors(snapshotId, startOffset, endOffset);
        if (deletes.isEmpty()) {
            return writeResult.withPrepared(
                    new PreparedDeletionVectorFiles(
                            round, Collections.emptyList(), Collections.emptyMap()));
        }
        DeleteWriteResult written;
        try (IcebergDvFileWriter writer =
                new IcebergDvFileWriter(table, bucket.getBucket(), base.loaded)) {
            for (Map.Entry<DataFile, long[]> e : deletes.entrySet()) {
                writer.delete(e.getKey(), e.getValue());
            }
            written = writer.complete();
        }
        Map<String, DeletionVectorId> replaced = new HashMap<>();
        for (Map.Entry<String, List<DeleteFile>> e : base.previous.entrySet()) {
            replaced.put(e.getKey(), DeletionVectorId.of(e.getValue().get(0)));
        }
        return writeResult.withPrepared(
                new PreparedDeletionVectorFiles(round, written.deleteFiles(), replaced));
    }

    /**
     * Advances the TaskManager's live files to the snapshot in the background: the first preparer
     * of the TaskManager to get there reads the manifests the snapshots since its last round wrote,
     * and the round's preparations find them read.
     */
    @Override
    public synchronized void prefetch(long snapshotId) {
        if (snapshotId == prefetched) {
            return;
        }
        prefetched = snapshotId;
        prefetcher()
                .execute(
                        () -> {
                            try {
                                refreshTo(snapshotId);
                                LiveFilesIndex.read(liveFiles, table, snapshotId, live -> null);
                            } catch (Exception e) {
                                LOG.debug(
                                        "Could not read the live files of {} on snapshot {} ahead.",
                                        table.name(),
                                        snapshotId,
                                        e);
                            }
                        });
    }

    /**
     * Reads the base files the round's deletes name, and their current deletion vectors, in the
     * background. Entries of rounds on earlier snapshots, whose preparations did not come, go.
     */
    @Override
    public synchronized void prefetch(
            TableBucket bucket, long snapshotId, Collection<String> dataFiles) {
        prefetchedFiles.values().removeIf(p -> p.snapshotId < snapshotId);
        Set<String> names = new HashSet<>(dataFiles);
        prefetchedFiles.put(
                bucket,
                new Prefetched(
                        snapshotId,
                        names,
                        CompletableFuture.supplyAsync(
                                () -> {
                                    try {
                                        return baseFiles(snapshotId, names);
                                    } catch (IOException e) {
                                        throw new UncheckedIOException(e);
                                    }
                                },
                                prefetcher())));
    }

    @Override
    public void discard(IcebergWriteResult prepared) {
        PreparedDeletionVectorFiles files = prepared.prepared();
        if (files != null) {
            deleteFiles(table, files.deletionVectors);
        }
    }

    @Override
    public void close() throws Exception {
        synchronized (this) {
            if (prefetcher != null) {
                prefetcher.shutdownNow();
            }
        }
        prefetchedFiles.clear();
        tableHandle.close();
    }

    /**
     * Puts the base's data files of the given names into {@code files}, by name, and the deletion
     * vectors of those that have one into {@code previous}, by location. Returns false if the base
     * lacks one of them, or one has deletes other than one deletion vector.
     */
    private boolean liveFiles(
            long snapshotId,
            Collection<String> names,
            Map<String, DataFile> files,
            Map<String, List<DeleteFile>> previous)
            throws IOException {
        refreshTo(snapshotId);
        return LiveFilesIndex.readWithFiles(
                liveFiles,
                table,
                snapshotId,
                names,
                live -> {
                    for (String name : names) {
                        DataFile file = live.file(name);
                        if (file == null) {
                            return false;
                        }
                        files.put(name, file);
                        List<DeleteFile> deletes =
                                PositionDeletesLoader.positionDeletesOf(
                                        live.deletesOf(file.location()));
                        if (deletes.isEmpty()) {
                            continue;
                        }
                        if (DeletionVectorId.onlyDeletionVector(deletes) == null) {
                            return false;
                        }
                        previous.put(file.location(), deletes);
                    }
                    return true;
                });
    }

    private synchronized ExecutorService prefetcher() {
        if (prefetcher == null) {
            prefetcher =
                    Executors.newSingleThreadExecutor(
                            new ExecutorThreadFactory("fluss-iceberg-prefetch"));
        }
        return prefetcher;
    }

    /**
     * The base's data files of the given names and the current deletion vectors of those that have
     * one, read; without files if the base lacks one of them, or one has deletes other than one
     * deletion vector.
     */
    private BaseFiles baseFiles(long snapshotId, Collection<String> names) throws IOException {
        Map<String, DataFile> files = new HashMap<>();
        Map<String, List<DeleteFile>> previous = new HashMap<>();
        if (!names.isEmpty() && !liveFiles(snapshotId, names, files, previous)) {
            return new BaseFiles(null, previous, dataFile -> null);
        }
        return new BaseFiles(files, previous, loader.loadAhead(previous, Collections.emptyMap()));
    }

    /** What a round's deletes name in the base, and the deletion vectors they replace, read. */
    private static final class BaseFiles {
        // by name; null if the deletes do not hold on the base
        @Nullable final Map<String, DataFile> files;
        // by data file location
        final Map<String, List<DeleteFile>> previous;
        // by data file location, the current deletes, some loaded ahead
        final Function<String, PositionDeleteIndex> loaded;

        BaseFiles(
                @Nullable Map<String, DataFile> files,
                Map<String, List<DeleteFile>> previous,
                Function<String, PositionDeleteIndex> loaded) {
            this.files = files;
            this.previous = previous;
            this.loaded = loaded;
        }
    }

    /** The base files of a bucket's round, read ahead. */
    private static final class Prefetched {
        final long snapshotId;
        final Set<String> dataFiles;
        final CompletableFuture<BaseFiles> read;

        Prefetched(long snapshotId, Set<String> dataFiles, CompletableFuture<BaseFiles> read) {
            this.snapshotId = snapshotId;
            this.dataFiles = dataFiles;
            this.read = read;
        }

        /** The base files read, or null if reading them failed; then they are read again. */
        @Nullable
        BaseFiles join() {
            try {
                return read.join();
            } catch (CancellationException | CompletionException e) {
                return null;
            }
        }
    }

    /** Refreshes the table if it does not know the snapshot yet. */
    private void refreshTo(long snapshotId) {
        if (table.snapshot(snapshotId) == null) {
            synchronized (table) {
                if (table.snapshot(snapshotId) == null) {
                    table.refresh();
                }
            }
        }
    }

    /** Deletes the Puffin files of deletion vectors, several of which may share one. */
    static void deleteFiles(Table table, List<DeleteFile> deletionVectors) {
        List<String> locations =
                deletionVectors.stream()
                        .map(ContentFile::location)
                        .distinct()
                        .collect(Collectors.toList());
        if (!locations.isEmpty()) {
            CatalogUtil.deleteFiles(table.io(), locations, "delete file", true);
        }
    }
}
