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

import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.RowPosSstIndex;
import org.apache.fluss.lake.dv.index.PositionIndexStore.BucketData;
import org.apache.fluss.lake.dv.index.PositionIndexStore.KeyConsumer;

import io.github.fluss_contrib.rocksdb.EnvOptions;
import io.github.fluss_contrib.rocksdb.IngestExternalFileOptions;
import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.ReadOptions;
import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.RocksDBException;
import io.github.fluss_contrib.rocksdb.RocksIterator;
import io.github.fluss_contrib.rocksdb.SstFileReader;
import io.github.fluss_contrib.rocksdb.SstFileReaderIterator;
import io.github.fluss_contrib.rocksdb.SstFileWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * The SSTs of the index store: written from sorted entries, copied with new file ids, and merged
 * into a bucket's full index. Keys sort as RocksDB's default comparator sorts them.
 */
final class IndexSsts {

    static final int MAX_ENTRIES_PER_SST = 1_000_000;

    static List<RowPosSstIndex.SstFileEntry> writeSsts(
            File dir, String namePrefix, Iterator<Map.Entry<byte[], FilePos>> entries)
            throws IOException {
        List<RowPosSstIndex.SstFileEntry> out = new ArrayList<>();
        int n = 0;
        SstFileWriter writer = null;
        File file = null;
        try (EnvOptions envOptions = new EnvOptions();
                Options options = new Options()) {
            while (entries.hasNext()) {
                Map.Entry<byte[], FilePos> e = entries.next();
                if (writer == null) {
                    file = new File(dir, namePrefix + "sst_" + out.size() + ".sst");
                    writer = new SstFileWriter(envOptions, options);
                    writer.open(file.getPath());
                }
                // RocksDB rejects keys out of strictly ascending order
                if (e.getValue() == null) {
                    writer.delete(e.getKey());
                } else {
                    writer.put(e.getKey(), e.getValue().encode());
                }
                if (++n == MAX_ENTRIES_PER_SST) {
                    writer.finish();
                    writer.close();
                    writer = null;
                    out.add(new RowPosSstIndex.SstFileEntry(file.getName(), file.length()));
                    n = 0;
                }
            }
            if (writer != null) {
                writer.finish();
                writer.close();
                writer = null;
                out.add(new RowPosSstIndex.SstFileEntry(file.getName(), file.length()));
            }
        } catch (RocksDBException ex) {
            throw new IOException("Failed to write position index SST in " + dir, ex);
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
        return out;
    }

    /** Copies an index SST, replacing the file id of every position by its new id. */
    static void remapSst(File source, File target, Map<Integer, Integer> newIds)
            throws IOException {
        try (Options options = new Options();
                EnvOptions envOptions = new EnvOptions();
                SstFileReader reader = new SstFileReader(options);
                ReadOptions readOptions = new ReadOptions();
                SstFileWriter writer = new SstFileWriter(envOptions, options)) {
            reader.open(source.getPath());
            if (reader.getTableProperties().getNumDeletions() > 0) {
                // the reader's iterator skips tombstones, which a copy would then lose
                throw new IOException(
                        "Cannot remap the file ids of " + source + ": it has deletes.");
            }
            writer.open(target.getPath());
            try (SstFileReaderIterator it = reader.newIterator(readOptions)) {
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    FilePos pos = FilePos.decode(it.value());
                    Integer id = newIds.get(pos.fileId());
                    if (id == null) {
                        throw new IOException(
                                "Cannot remap "
                                        + source
                                        + ": no new id for file id "
                                        + pos.fileId());
                    }
                    writer.put(it.key(), new FilePos(id, pos.rowPosition()).encode());
                }
                it.status();
            }
            writer.finish();
        } catch (RocksDBException e) {
            throw new IOException("Failed to remap the file ids of " + source, e);
        }
    }

    /**
     * Copies a staged SST into an index SST: a staged delete becomes a tombstone, and every
     * position gets the new id of its file; passes every key to {@code keys}.
     */
    static void publishSst(File source, File target, Map<Integer, Integer> newIds, KeyConsumer keys)
            throws IOException {
        try (Options options = new Options();
                EnvOptions envOptions = new EnvOptions();
                SstFileReader reader = new SstFileReader(options);
                ReadOptions readOptions = new ReadOptions();
                SstFileWriter writer = new SstFileWriter(envOptions, options)) {
            reader.open(source.getPath());
            writer.open(target.getPath());
            try (SstFileReaderIterator it = reader.newIterator(readOptions)) {
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    byte[] key = it.key();
                    byte[] value = it.value();
                    if (value.length == 0) {
                        writer.delete(key);
                    } else {
                        FilePos pos = FilePos.decode(value);
                        Integer id = newIds.get(pos.fileId());
                        if (id == null) {
                            throw new IOException(
                                    "Cannot publish "
                                            + source
                                            + ": no new id for file id "
                                            + pos.fileId());
                        }
                        writer.put(key, new FilePos(id, pos.rowPosition()).encode());
                    }
                    keys.accept(key);
                }
                it.status();
            }
            writer.finish();
        } catch (RocksDBException e) {
            throw new IOException("Failed to publish the staged index SST " + source, e);
        }
    }

    /**
     * Applies checkpoints and changes in order to an empty index, as a tablet server does: file ids
     * first, then the SSTs, then the removed files. Exports the index into {@code sst} without the
     * entries of files missing from {@code fileDict}, which receives the remaining file ids.
     * Returns the number of entries exported; writes no file for an empty index.
     */
    static long mergeIndex(
            List<BucketData> parts, File dbDir, File sst, Map<Integer, String> fileDict)
            throws IOException {
        RocksDB.loadLibrary();
        Map<String, Integer> idOfPath = new HashMap<>();
        try (Options options = new Options().setCreateIfMissing(true);
                RocksDB db = RocksDB.open(options, dbDir.getPath());
                IngestExternalFileOptions ingest = new IngestExternalFileOptions()) {
            ingest.setMoveFiles(true);
            for (BucketData part : parts) {
                for (Map.Entry<Integer, String> e : part.fileId2Name.entrySet()) {
                    fileDict.put(e.getKey(), e.getValue());
                    idOfPath.put(e.getValue(), e.getKey());
                }
                if (!part.localSsts.isEmpty()) {
                    db.ingestExternalFile(part.localSsts, ingest);
                }
                for (String removed : part.removedFiles) {
                    Integer id = idOfPath.remove(removed);
                    if (id != null) {
                        fileDict.remove(id);
                    }
                }
            }
            long n = 0;
            try (EnvOptions envOptions = new EnvOptions();
                    SstFileWriter writer = new SstFileWriter(envOptions, options);
                    ReadOptions readOptions = new ReadOptions();
                    RocksIterator it = db.newIterator(readOptions)) {
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    if (!fileDict.containsKey(FilePos.decode(it.value()).fileId())) {
                        continue;
                    }
                    if (n == 0) {
                        writer.open(sst.getPath());
                    }
                    writer.put(it.key(), it.value());
                    n++;
                }
                it.status();
                if (n > 0) {
                    writer.finish();
                }
            }
            return n;
        } catch (RocksDBException e) {
            throw new IOException("Failed to merge the position index in " + dbDir, e);
        }
    }

    /** Compares keys as RocksDB's default comparator does: bytes unsigned, shorter first. */
    static int compareUnsigned(byte[] a, byte[] b) {
        int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            int c = (a[i] & 0xff) - (b[i] & 0xff);
            if (c != 0) {
                return c;
            }
        }
        return a.length - b.length;
    }

    private IndexSsts() {}
}
