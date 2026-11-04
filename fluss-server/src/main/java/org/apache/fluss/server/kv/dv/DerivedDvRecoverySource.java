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

import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;
import org.apache.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** A {@link DerivedDvManager.RecoverySource} over ZooKeeper, the index store and the local log. */
public class DerivedDvRecoverySource implements DerivedDvManager.RecoverySource {

    private static final int FETCH_BYTES = 8 * 1024 * 1024;

    private final TableInfo tableInfo;
    private final TableBucket tableBucket;
    private final LogTablet logTablet;
    private final ZooKeeperClient zkClient;
    private final SchemaGetter schemaGetter;
    private final PositionIndexStore store;
    private final File localTmp;

    public DerivedDvRecoverySource(
            String remoteDataDir,
            TableInfo tableInfo,
            TableBucket tableBucket,
            LogTablet logTablet,
            ZooKeeperClient zkClient,
            SchemaGetter schemaGetter,
            File localTmp) {
        this.tableInfo = tableInfo;
        this.tableBucket = tableBucket;
        this.logTablet = logTablet;
        this.zkClient = zkClient;
        this.schemaGetter = schemaGetter;
        this.store =
                PositionIndexStore.forTable(
                        checkNotNull(remoteDataDir, "remote data dir"),
                        tableInfo.getTablePath(),
                        tableInfo.getTableId());
        this.localTmp = localTmp;
    }

    @Override
    @Nullable
    public long[] readableSnapshot() throws Exception {
        Optional<LakeTableSnapshot> s =
                zkClient.getLatestReadableLakeTableSnapshot(tableBucket.getTableId());
        if (!s.isPresent()) {
            return null;
        }
        Long off = s.get().getBucketLogEndOffset().get(tableBucket);
        return new long[] {s.get().getSnapshotId(), off == null ? 0L : off};
    }

    @Override
    @Nullable
    public Long snapshotOffset(long snapshotId) throws Exception {
        Optional<LakeTableSnapshot> s =
                zkClient.getLakeTableSnapshot(tableBucket.getTableId(), snapshotId);
        if (!s.isPresent()) {
            return null;
        }
        Long off = s.get().getBucketLogEndOffset().get(tableBucket);
        return off == null ? 0L : off;
    }

    @Override
    public List<PositionIndexStore.ChainEntry> chain() throws IOException {
        return store.chain();
    }

    @Override
    public boolean chainTrimmed() throws IOException {
        return store.chainTrimmed();
    }

    @Override
    public PositionIndexStore.BucketData readChange(PositionIndexStore.ChainEntry entry)
            throws IOException {
        // A fresh directory per read, since ingestion moves the files away.
        return store.readChange(entry, tableBucket, freshDir(String.valueOf(entry.changeId)));
    }

    @Override
    @Nullable
    public PositionIndexStore.Checkpoint latestCheckpoint(
            List<PositionIndexStore.ChainEntry> chain, int upTo) throws IOException {
        return store.latestCheckpoint(tableBucket, chain, upTo);
    }

    @Override
    public PositionIndexStore.BucketData readCheckpoint(PositionIndexStore.Checkpoint cp)
            throws IOException {
        return store.readCheckpoint(cp, tableBucket, freshDir("ckpt-" + cp.snapshotId));
    }

    @Override
    public void uploadCheckpoint(
            long snapshotId, @Nullable String localSst, Map<Integer, String> fileDict)
            throws IOException {
        store.writeCheckpoint(
                snapshotId, tableBucket, localSst == null ? null : new File(localSst), fileDict);
    }

    private File freshDir(String prefix) {
        return new File(localTmp, prefix + "-" + System.nanoTime());
    }

    @Override
    public void replayLog(long fromOffset, DerivedDvManager.ChangeConsumer consumer)
            throws Exception {
        KeyEncoder keyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        tableInfo.getRowType(),
                        tableInfo.getPhysicalPrimaryKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());
        long localStart = logTablet.localLogStartOffset();
        if (fromOffset < localStart) {
            // Skipping a prefix would let superseded versions through, so fail instead.
            throw new IllegalStateException(
                    String.format(
                            "Derived DV: cannot rebuild %s from offset %d, the local log starts at"
                                    + " %d.",
                            tableBucket, fromOffset, localStart));
        }
        long next = fromOffset;
        try (LogRecordReadContext readContext =
                LogRecordReadContext.createReadContext(tableInfo, false, null, schemaGetter)) {
            while (true) {
                LogRecords records =
                        logTablet
                                .read(next, FETCH_BYTES, FetchIsolation.LOG_END, true, null, null)
                                .getRecords();
                if (records == MemoryLogRecords.EMPTY) {
                    break;
                }
                boolean any = false;
                for (LogRecordBatch batch : records.batches()) {
                    any = true;
                    try (CloseableIterator<LogRecord> it = batch.records(readContext)) {
                        while (it.hasNext()) {
                            LogRecord r = it.next();
                            if (r.logOffset() < fromOffset
                                    || r.getChangeType() == ChangeType.UPDATE_BEFORE) {
                                continue;
                            }
                            byte[] key = keyEncoder.encodeKey(r.getRow());
                            long v = r.getChangeType() == ChangeType.DELETE ? -1 : r.logOffset();
                            consumer.accept(Arrays.copyOf(key, key.length), r.logOffset(), v);
                        }
                    }
                    next = batch.nextLogOffset();
                }
                if (!any) {
                    break;
                }
            }
        }
    }
}
