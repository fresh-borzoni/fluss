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
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.RowPosSstIndex;
import org.apache.fluss.lake.dv.index.PositionIndexStore.BucketPart;
import org.apache.fluss.lake.dv.index.PositionIndexStore.StagedPartWriter;
import org.apache.fluss.utils.FileUtils;

import io.github.fluss_contrib.rocksdb.EnvOptions;
import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.RocksDBException;
import io.github.fluss_contrib.rocksdb.SstFileWriter;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.lake.dv.index.IndexFiles.upload;
import static org.apache.fluss.lake.dv.index.IndexSsts.MAX_ENTRIES_PER_SST;

/**
 * Writes a staged part into local SSTs of at most {@link IndexSsts#MAX_ENTRIES_PER_SST} entries and
 * stores each once it is full. A delete is an empty value; no encoded position is empty.
 */
final class FsStagedPartWriter implements StagedPartWriter {
    private static final byte[] DELETED = new byte[0];

    private final File localDir;
    private final FsPath dir;
    private final String namePrefix;
    private final List<RowPosSstIndex.SstFileEntry> stored = new ArrayList<>();
    @Nullable private EnvOptions envOptions;
    @Nullable private Options options;
    @Nullable private SstFileWriter writer;
    @Nullable private File file;
    private int entries;
    private boolean closed;

    FsStagedPartWriter(File localDir, FsPath dir, String namePrefix) {
        this.localDir = localDir;
        this.dir = dir;
        this.namePrefix = namePrefix;
    }

    @Override
    public void put(byte[] key, FilePos pos) throws IOException {
        add(key, pos.encode());
    }

    @Override
    public void delete(byte[] key) throws IOException {
        add(key, DELETED);
    }

    private void add(byte[] key, byte[] value) throws IOException {
        if (closed) {
            throw new IllegalStateException("The staged part writer is closed.");
        }
        try {
            if (writer == null) {
                if (options == null) {
                    envOptions = new EnvOptions();
                    options = new Options();
                }
                file = new File(localDir, namePrefix + "sst_" + stored.size() + ".sst");
                writer = new SstFileWriter(envOptions, options);
                writer.open(file.getPath());
            }
            // RocksDB rejects keys out of strictly ascending order
            writer.put(key, value);
        } catch (RocksDBException e) {
            throw new IOException("Failed to write a staged index SST in " + localDir, e);
        }
        if (++entries == MAX_ENTRIES_PER_SST) {
            storeCurrent();
        }
    }

    private void storeCurrent() throws IOException {
        SstFileWriter w = writer;
        File f = file;
        writer = null;
        file = null;
        entries = 0;
        try {
            w.finish();
        } catch (RocksDBException e) {
            throw new IOException("Failed to write the staged index SST " + f, e);
        } finally {
            w.close();
        }
        upload(f, new FsPath(dir, f.getName()));
        stored.add(new RowPosSstIndex.SstFileEntry(f.getName(), f.length()));
        Files.delete(f.toPath());
    }

    @Override
    public BucketPart finish() throws IOException {
        try {
            if (writer != null) {
                storeCurrent();
            }
            return stored.isEmpty() ? BucketPart.NONE : new BucketPart(new ArrayList<>(stored));
        } finally {
            close();
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (writer != null) {
            writer.close();
            writer = null;
        }
        if (options != null) {
            options.close();
            envOptions.close();
        }
        FileUtils.deleteDirectoryQuietly(localDir);
    }
}
