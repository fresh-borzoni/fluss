/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.dv.index;

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.FormatVersions;
import org.apache.fluss.lake.dv.RowPosSstIndex;
import org.apache.fluss.lake.dv.index.PositionIndexStore.BucketData;
import org.apache.fluss.lake.dv.index.PositionIndexStore.ChainEntry;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.fluss.lake.dv.index.IndexFiles.download;
import static org.apache.fluss.lake.dv.index.IndexFiles.lines;
import static org.apache.fluss.lake.dv.index.IndexFiles.readBytes;
import static org.apache.fluss.lake.dv.index.IndexFiles.readText;
import static org.apache.fluss.lake.dv.index.IndexLayout.CHANGE_COMMITTED;
import static org.apache.fluss.lake.dv.index.IndexLayout.CHANGE_VERSION;
import static org.apache.fluss.lake.dv.index.IndexLayout.partitionKey;

/**
 * Reads the committed changes of the {@link FlussPositionIndexStore}. A change never changes once
 * committed, so its metadata may be cached: {@link #readChange} reads it through the store, which a
 * caching store overrides.
 */
final class IndexChangeReader {

    private final IndexLayout layout;
    // the store's hooks: the reads of committed metadata
    private final FlussPositionIndexStore store;

    IndexChangeReader(IndexLayout layout, FlussPositionIndexStore store) {
        this.layout = layout;
        this.store = store;
    }

    /**
     * Reads one bucket's part of a registered change, see {@link PositionIndexStore#readChange}.
     */
    BucketData readChange(ChainEntry entry, TableBucket bucket, File localDir) throws IOException {
        String partitions = store.readCommittedPartitions(entry.changeId);
        if (partitions == null) {
            throw new IOException(
                    "Index change of " + entry + " is gone (trimmed, or never committed).");
        }
        if (!lines(partitions).contains(String.valueOf(partitionKey(bucket)))) {
            return BucketData.EMPTY; // the change does not touch this partition
        }
        // Throws if the manifest is gone.
        RowPosSstIndex index = store.readManifest(entry.changeId, bucket.getPartitionId());
        int b = bucket.getBucket();
        FsPath bucketDir = layout.bucketDir(entry.changeId, bucket);
        List<String> local = new ArrayList<>();
        List<RowPosSstIndex.SstFileEntry> files = index.getFiles(b);
        if (!files.isEmpty()) {
            Files.createDirectories(localDir.toPath());
            for (RowPosSstIndex.SstFileEntry f : files) {
                File target = new File(localDir, f.getFileName());
                download(new FsPath(bucketDir, f.getFileName()), target); // throws if gone
                local.add(target.getPath());
            }
        }
        return new BucketData(
                local,
                new HashMap<>(index.getFileId2Name(b)),
                new ArrayList<>(index.getReplacedFiles(b)));
    }

    /**
     * The partitions a committed change touches, one per line; null if the change is not committed
     * or is gone. The list never changes once written, so a reader may cache it.
     */
    @Nullable
    String readCommittedPartitions(long changeId) throws IOException {
        FsPath committed = new FsPath(layout.changeDir(changeId), CHANGE_COMMITTED);
        String text = readText(committed);
        return text == null ? null : FormatVersions.withoutVersion(committed, text, CHANGE_VERSION);
    }

    /**
     * The manifest of a committed change's part of a partition; throws if it is gone. It never
     * changes once written, so a reader may cache it.
     */
    RowPosSstIndex readManifest(long changeId, @Nullable Long partitionId) throws IOException {
        FsPath manifest = layout.manifestPath(changeId, partitionId);
        return RowPosSstIndex.fromJsonBytes(readBytes(manifest), manifest);
    }
}
