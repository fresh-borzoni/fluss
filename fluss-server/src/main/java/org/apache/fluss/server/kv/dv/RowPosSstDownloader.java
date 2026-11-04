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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.RowPosSstIndex;
import org.apache.fluss.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Downloads RowPos SST files from remote storage to a local directory.
 *
 * <p>Used by TabletServer during the Prepare phase to fetch SST files before Readable Switch
 * ingests them into the RowPosIndex.
 */
@Internal
public class RowPosSstDownloader {

    private static final String ROW_POS_DIR = "rowPos";
    private static final String INDEX_FILE = "index.json";
    private static final int DOWNLOAD_BUFFER_SIZE = 16 * 1024;

    private final FsPath remoteLakeTableSnapshotDir;

    public RowPosSstDownloader(FsPath remoteLakeTableSnapshotDir) {
        this.remoteLakeTableSnapshotDir = remoteLakeTableSnapshotDir;
    }

    /**
     * Downloads SST files for a specific bucket within a snapshot to a local directory.
     *
     * @param snapshotId the lake snapshot ID
     * @param partitionId the partition ID, or null for non-partitioned tables
     * @param bucketId the bucket to download
     * @param localDir local directory to store downloaded SST files
     * @return list of local SST file paths; empty if the bucket is not in the index
     */
    public List<String> downloadBucketSst(
            long snapshotId, @Nullable Long partitionId, int bucketId, String localDir)
            throws IOException {
        RowPosSstIndex index = readIndex(snapshotId, partitionId);
        List<RowPosSstIndex.SstFileEntry> files = index.getFiles(bucketId);
        if (files.isEmpty()) {
            return Collections.emptyList();
        }

        FsPath baseDir = buildBaseDir(snapshotId, partitionId);
        FsPath bucketDir = new FsPath(baseDir, String.valueOf(bucketId));
        FileSystem fs = bucketDir.getFileSystem();

        File localDirFile = new File(localDir);
        if (!localDirFile.exists()) {
            localDirFile.mkdirs();
        }

        List<String> localPaths = new ArrayList<>();
        for (RowPosSstIndex.SstFileEntry entry : files) {
            FsPath remoteSstPath = new FsPath(bucketDir, entry.getFileName());
            File localFile = new File(localDir, entry.getFileName());
            downloadFile(fs, remoteSstPath, localFile);
            localPaths.add(localFile.getPath());
        }

        return localPaths;
    }

    /** Reads the index.json for the given snapshot. */
    public RowPosSstIndex readIndex(long snapshotId, @Nullable Long partitionId)
            throws IOException {
        FsPath baseDir = buildBaseDir(snapshotId, partitionId);
        // A version-2 rowpos.manifest, when present, supersedes the version-1 index.json.
        FsPath manifestPath = new FsPath(baseDir, RowPosSstIndex.MANIFEST_FILE);
        FsPath indexPath =
                manifestPath.getFileSystem().exists(manifestPath)
                        ? manifestPath
                        : new FsPath(baseDir, INDEX_FILE);
        FileSystem fs = indexPath.getFileSystem();

        FSDataInputStream in = null;
        ByteArrayOutputStream out = null;
        try {
            in = fs.open(indexPath);
            out = new ByteArrayOutputStream();
            IOUtils.copyBytes(in, out, false);
            return RowPosSstIndex.fromJsonBytes(out.toByteArray());
        } finally {
            IOUtils.closeQuietly(in);
            IOUtils.closeQuietly(out);
        }
    }

    /** Builds the base directory path for a snapshot, optionally scoped to a partition. */
    private FsPath buildBaseDir(long snapshotId, @Nullable Long partitionId) {
        String path = ROW_POS_DIR + "/" + snapshotId;
        if (partitionId != null) {
            path += "/" + partitionId;
        }
        return new FsPath(remoteLakeTableSnapshotDir, path);
    }

    private void downloadFile(FileSystem fs, FsPath remotePath, File localFile) throws IOException {
        FSDataInputStream in = null;
        FileOutputStream out = null;
        try {
            in = fs.open(remotePath);
            out = new FileOutputStream(localFile);
            byte[] buffer = new byte[DOWNLOAD_BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        } finally {
            IOUtils.closeQuietly(in);
            IOUtils.closeQuietly(out);
        }
    }
}
