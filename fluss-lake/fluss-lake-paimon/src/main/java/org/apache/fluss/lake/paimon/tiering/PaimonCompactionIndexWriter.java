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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.RowPosSstIndex;
import org.apache.fluss.lake.dv.index.CompactionIndexPart;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.lake.dv.index.SpillingPositionSorter;
import org.apache.fluss.lake.paimon.source.PaimonRecordReader;
import org.apache.fluss.lake.paimon.source.PaimonSplit;
import org.apache.fluss.lake.source.RowWithPosResult;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.utils.FileStorePathFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Hands the positions of the rows a Paimon compaction in the tiering writer wrote to the position
 * index of a table with deletion vectors. The committer publishes them as the index change of the
 * readable snapshot the compaction makes, so the round does not hold for a re-index.
 *
 * <p>After the writer prepared its commit, the files the bucket's compaction wrote are read the way
 * a re-index reads them. These are the compact increment's after files, also those the compaction
 * only moved up a level. The read yields their primary keys with the row positions Paimon's
 * deletion vectors use, without the rows the compaction's deletion vectors delete. The positions
 * refer to the files by provisional ids from 0, which the committer replaces with ids from the
 * table's counter. The files the compaction replaced leave the index; only those readable before,
 * above level 0, are named.
 */
public class PaimonCompactionIndexWriter {

    private final FileStoreTable table;
    private final TableInfo tableInfo;
    private final TableBucket bucket;
    private final List<String> partitionValues;
    private final String positionIndexDir;
    private final long roundTimestamp;
    @Nullable private final File localTmpDir;

    public PaimonCompactionIndexWriter(
            FileStoreTable table,
            TableInfo tableInfo,
            TableBucket bucket,
            List<String> partitionValues,
            String positionIndexDir,
            long roundTimestamp,
            @Nullable File localTmpDir) {
        this.table = table;
        this.tableInfo = tableInfo;
        this.bucket = bucket;
        this.partitionValues = partitionValues;
        this.positionIndexDir = positionIndexDir;
        this.roundTimestamp = roundTimestamp;
        this.localTmpDir = localTmpDir;
    }

    /** Whether the writer's commit messages hold a compaction of the bucket. */
    static boolean compacted(List<CommitMessage> messages) {
        for (CommitMessage message : messages) {
            if (message instanceof CommitMessageImpl
                    && !((CommitMessageImpl) message).compactIncrement().compactAfter().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the index part of the bucket's compaction in the writer's only commit message,
     * written under the round's compaction change.
     */
    CompactionIndexPart handOff(List<CommitMessage> messages) throws IOException {
        if (messages.size() != 1 || !(messages.get(0) instanceof CommitMessageImpl)) {
            throw new IOException("Expected one commit message, got " + messages);
        }
        CommitMessageImpl message = (CommitMessageImpl) messages.get(0);
        CompactIncrement increment = message.compactIncrement();
        List<DataFileMeta> written = increment.compactAfter();
        Map<String, DeletionFile> deletionFiles =
                deletionFiles(message.partition(), message.bucket(), increment.newIndexFiles());

        RowType rowType = tableInfo.getRowType();
        List<String> pkNames = tableInfo.getPhysicalPrimaryKeys();
        int[] pkIndexes = new int[pkNames.size()];
        int[][] projection = new int[pkNames.size()][];
        for (int i = 0; i < pkNames.size(); i++) {
            pkIndexes[i] = rowType.getFieldIndex(pkNames.get(i));
            projection[i] = new int[] {pkIndexes[i]};
        }
        KeyEncoder keyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        rowType.project(pkIndexes),
                        pkNames,
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());
        String bucketPath =
                table.store()
                        .pathFactory()
                        .bucketPath(message.partition(), message.bucket())
                        .toString();
        Map<Integer, String> addedFiles = new LinkedHashMap<>();
        long changeId = CompactionIndexPart.changeIdOf(tableInfo.getTableId(), roundTimestamp, -1L);
        PositionIndexStore.BucketPart part;
        try (SpillingPositionSorter entries =
                new SpillingPositionSorter(
                        localTmpDir,
                        SpillingPositionSorter.DEFAULT_MAX_BUFFERED,
                        (key, first, second) ->
                                new IllegalStateException(
                                        String.format(
                                                "DV hand-off of the Paimon compaction of %s: two"
                                                        + " live rows of one key, in %s at %d and"
                                                        + " in %s at %d.",
                                                bucket,
                                                addedFiles.get(first.fileId()),
                                                first.rowPosition(),
                                                addedFiles.get(second.fileId()),
                                                second.rowPosition())))) {
            for (DataFileMeta file : written) {
                int fileId = addedFiles.size();
                addedFiles.put(fileId, file.fileName());
                DeletionFile deletionFile = deletionFiles.get(file.fileName());
                DataSplit split =
                        DataSplit.builder()
                                .withPartition(message.partition())
                                .withBucket(message.bucket())
                                .withBucketPath(bucketPath)
                                .withTotalBuckets(message.totalBuckets())
                                .withDataFiles(Collections.singletonList(file))
                                .withDataDeletionFiles(Collections.singletonList(deletionFile))
                                .rawConvertible(true)
                                .build();
                try (CloseableIterator<RowWithPosResult> rows =
                        new PaimonRecordReader(
                                        table,
                                        new PaimonSplit(split, false, partitionValues),
                                        projection,
                                        null)
                                .readWithPos()) {
                    while (rows.hasNext()) {
                        RowWithPosResult row = rows.next();
                        byte[] key = keyEncoder.encodeKey(row.getRow());
                        entries.add(
                                Arrays.copyOf(key, key.length), new FilePos(fileId, row.getPos()));
                    }
                }
            }
            // Compactions of a round's buckets share the change. Each bucket's SSTs go to a
            // directory of their own, so they need no name prefix.
            part =
                    PositionIndexStore.forTable(new FsPath(positionIndexDir), localTmpDir)
                            .writeBucket(changeId, bucket, "", entries.sorted());
        }
        Set<String> kept = new HashSet<>(addedFiles.values());
        List<String> removed = new ArrayList<>();
        for (DataFileMeta file : increment.compactBefore()) {
            // level-0 files were never readable, so never indexed
            if (file.level() > 0 && !kept.contains(file.fileName())) {
                removed.add(file.fileName());
            }
        }
        List<String> sstNames = new ArrayList<>();
        List<Long> sstSizes = new ArrayList<>();
        for (RowPosSstIndex.SstFileEntry sst : part.files) {
            sstNames.add(sst.getFileName());
            sstSizes.add(sst.getFileSize());
        }
        return new CompactionIndexPart(changeId, -1L, sstNames, sstSizes, addedFiles, removed);
    }

    /** The deletion vectors the commit's new index files hold, by data file. */
    private Map<String, DeletionFile> deletionFiles(
            BinaryRow partition, int bucketId, List<IndexFileMeta> newIndexFiles) {
        Map<String, DeletionFile> out = new HashMap<>();
        IndexPathFactory paths = null;
        for (IndexFileMeta index : newIndexFiles) {
            if (!DeletionVectorsIndexFile.DELETION_VECTORS_INDEX.equals(index.indexType())
                    || index.dvRanges() == null) {
                continue;
            }
            if (paths == null) {
                FileStorePathFactory factory = table.store().pathFactory();
                paths = factory.indexFileFactory(partition, bucketId);
            }
            String path = paths.toPath(index).toString();
            for (DeletionVectorMeta dv : index.dvRanges().values()) {
                out.put(
                        dv.dataFileName(),
                        new DeletionFile(path, dv.offset(), dv.length(), dv.cardinality()));
            }
        }
        return out;
    }
}
