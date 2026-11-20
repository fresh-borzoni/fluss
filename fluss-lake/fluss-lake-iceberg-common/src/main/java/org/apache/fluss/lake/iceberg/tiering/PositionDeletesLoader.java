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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.deletes.PositionDeleteIndex;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

/**
 * Loads the position deletes of a table's data files, several files at once, and at most a bound of
 * bytes of them ahead ({@code lake.tiering.deletion-vectors.load-max-bytes}).
 */
final class PositionDeletesLoader {

    private final Table table;
    private final DeleteLoader deleteLoader;
    private final int threads;
    private final long maxBytesAhead;

    PositionDeletesLoader(Table table, int threads, long maxBytesAhead) {
        this.table = table;
        this.deleteLoader = new BaseDeleteLoader(df -> table.io().newInputFile(df.location()));
        this.threads = threads;
        this.maxBytesAhead = maxBytesAhead;
    }

    /** Loads the position deletes of one data file. */
    PositionDeleteIndex load(String dataFile, List<DeleteFile> deletes) {
        return deleteLoader.loadPositionDeletes(deletes, dataFile);
    }

    /**
     * Returns the position deletes of a data file by its location, null if it has none: those in
     * {@code kept} as they are, and of the others, those of the first files whose delete files fit
     * in the bound loaded now, {@code threads} files at once. Each of these is handed out once; the
     * others, and one asked for again, are loaded when asked for.
     */
    LoadedDeletes loadAhead(
            Map<String, List<DeleteFile>> deletesByFile, Map<String, PositionDeleteIndex> kept)
            throws IOException {
        Map<String, List<DeleteFile>> ahead = new HashMap<>();
        long bytes = 0;
        for (Map.Entry<String, List<DeleteFile>> e : deletesByFile.entrySet()) {
            List<DeleteFile> deletes = positionDeletesOf(e.getValue());
            if (deletes.isEmpty() || kept.containsKey(e.getKey())) {
                continue;
            }
            long size = sizeOf(deletes);
            if (bytes + size <= maxBytesAhead) {
                ahead.put(e.getKey(), deletes);
                bytes += size;
            }
        }
        Map<String, PositionDeleteIndex> loaded = loadAll(ahead);
        int loadedAhead = loaded.size();
        loaded.putAll(kept);
        return new LoadedDeletes(deletesByFile, loaded, loadedAhead);
    }

    /** The position deletes of data files, some loaded ahead; see {@link #loadAhead}. */
    final class LoadedDeletes implements Function<String, PositionDeleteIndex> {
        private final Map<String, List<DeleteFile>> deletesByFile;
        private final Map<String, PositionDeleteIndex> loaded;
        private final int loadedAhead;

        private LoadedDeletes(
                Map<String, List<DeleteFile>> deletesByFile,
                Map<String, PositionDeleteIndex> loaded,
                int loadedAhead) {
            this.deletesByFile = deletesByFile;
            this.loaded = loaded;
            this.loadedAhead = loadedAhead;
        }

        @Override
        @Nullable
        public PositionDeleteIndex apply(String dataFile) {
            PositionDeleteIndex deletes = loaded.remove(dataFile);
            if (deletes != null) {
                return deletes;
            }
            List<DeleteFile> previous = positionDeletesOf(deletesByFile.get(dataFile));
            return previous.isEmpty() ? null : load(dataFile, previous);
        }

        /** How many files' deletes were loaded ahead. */
        @VisibleForTesting
        int loadedAhead() {
            return loadedAhead;
        }
    }

    /** Loads the position deletes of the data files, by data file location. */
    private Map<String, PositionDeleteIndex> loadAll(Map<String, List<DeleteFile>> deletesByFile)
            throws IOException {
        Map<String, PositionDeleteIndex> loaded = new HashMap<>();
        if (deletesByFile.isEmpty()) {
            return loaded;
        }
        ExecutorService loaders =
                Executors.newFixedThreadPool(
                        Math.min(threads, deletesByFile.size()),
                        new ExecutorThreadFactory("fluss-iceberg-deletes-load"));
        try {
            Map<String, Future<PositionDeleteIndex>> futures = new HashMap<>();
            for (Map.Entry<String, List<DeleteFile>> e : deletesByFile.entrySet()) {
                futures.put(e.getKey(), loaders.submit(() -> load(e.getKey(), e.getValue())));
            }
            for (Map.Entry<String, Future<PositionDeleteIndex>> f : futures.entrySet()) {
                loaded.put(f.getKey(), f.getValue().get());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("Interrupted while loading deletion vectors.");
        } catch (ExecutionException e) {
            throw new IOException(
                    "Could not load the deletes of the data files of " + table.name(),
                    e.getCause());
        } finally {
            loaders.shutdownNow();
        }
        return loaded;
    }

    /** The bytes of the delete files: a deletion vector's blob, or the whole file. */
    private static long sizeOf(List<DeleteFile> deletes) {
        long size = 0;
        for (DeleteFile delete : deletes) {
            Long blob = delete.contentSizeInBytes();
            size += blob != null ? blob : delete.fileSizeInBytes();
        }
        return size;
    }

    /** The position deletes among a data file's delete files. */
    static List<DeleteFile> positionDeletesOf(List<DeleteFile> deletes) {
        List<DeleteFile> out = new ArrayList<>();
        if (deletes != null) {
            for (DeleteFile d : deletes) {
                if (d.content() == FileContent.POSITION_DELETES) {
                    out.add(d);
                }
            }
        }
        return out;
    }
}
