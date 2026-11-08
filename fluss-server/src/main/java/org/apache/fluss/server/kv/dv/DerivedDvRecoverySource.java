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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.lake.LakeTable;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;
import org.apache.fluss.utils.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** A {@link DerivedDvManager.RecoverySource} over ZooKeeper and the index store. */
public class DerivedDvRecoverySource implements DerivedDvManager.RecoverySource {

    private static final Logger LOG = LoggerFactory.getLogger(DerivedDvRecoverySource.class);

    private final TableBucket tableBucket;
    private final ZooKeeperClient zkClient;
    private final DerivedDvMetadataCache cache;
    private final DerivedDvMetadataCache.CachingStore store;
    // the offsets file of the last readable registration this bucket read; chain listings made
    // after it are cached under it
    @Nullable private volatile String lastRegistration;
    private final File localTmp;

    public DerivedDvRecoverySource(
            String remoteDataDir,
            TableInfo tableInfo,
            TableBucket tableBucket,
            ZooKeeperClient zkClient,
            File localTmp,
            DerivedDvMetadataCache cache) {
        this.tableBucket = tableBucket;
        this.zkClient = zkClient;
        this.cache = cache;
        this.store =
                cache.store(
                        checkNotNull(remoteDataDir, "remote data dir"),
                        tableInfo.getTablePath(),
                        tableInfo.getTableId());
        this.localTmp = localTmp;
        // removes downloads that an earlier leadership of the bucket left, e.g. on a crash
        FileUtils.deleteDirectoryQuietly(localTmp);
    }

    @Override
    @Nullable
    public long[] readableSnapshot() throws Exception {
        Optional<LakeTable> table = zkClient.getLakeTable(tableBucket.getTableId());
        LakeTableSnapshot s =
                table.isPresent()
                        ? table.get().getOrReadLatestReadableTableSnapshot(this::offsets)
                        : null;
        if (s == null) {
            return null;
        }
        Long off = s.getBucketLogEndOffset().get(tableBucket);
        return new long[] {s.getSnapshotId(), off == null ? 0L : off};
    }

    /**
     * Returns the bucket's readable offset in the snapshot, where its log tail starts, or null
     * until the snapshot is registered as readable. A lake that takes deletes is readable at its
     * tiered offsets. A lake that merges by key (Paimon) may be readable from earlier offsets.
     */
    @Override
    @Nullable
    public Long snapshotOffset(long snapshotId) throws Exception {
        Optional<LakeTable> table = zkClient.getLakeTable(tableBucket.getTableId());
        LakeTableSnapshot s =
                table.isPresent()
                        ? table.get().getOrReadReadableTableSnapshot(snapshotId, this::offsets)
                        : null;
        if (s == null) {
            return null;
        }
        Long off = s.getBucketLogEndOffset().get(tableBucket);
        return off == null ? 0L : off;
    }

    /** The offsets of a readable registration, read once per tablet server. */
    private Map<TableBucket, Long> offsets(FsPath offsetsFile) throws IOException {
        lastRegistration = offsetsFile.toString();
        return cache.offsets(offsetsFile);
    }

    @Override
    public List<PositionIndexStore.ChainEntry> chain() throws IOException {
        // The ZooKeeper registration is read first, and its chain marker is written before it.
        String registration = lastRegistration;
        return registration == null ? store.chain() : store.chain(registration);
    }

    @Nullable
    @Override
    public String snapshotIdentity(PositionIndexStore.ChainEntry entry) throws IOException {
        return store.snapshotIdentity(entry);
    }

    @Override
    public boolean chainTrimmed() throws IOException {
        return store.chainTrimmed();
    }

    @Override
    public PositionIndexStore.BucketData readChange(PositionIndexStore.ChainEntry entry)
            throws IOException {
        // A fresh directory per read, since ingestion moves the files away.
        try {
            return store.readChange(entry, tableBucket, freshDir(String.valueOf(entry.changeId)));
        } catch (IOException e) {
            forgetChain();
            throw e;
        }
    }

    @Override
    @Nullable
    public PositionIndexStore.Checkpoint latestCheckpoint(
            List<PositionIndexStore.ChainEntry> chain, int upTo) throws IOException {
        try {
            return store.latestCheckpoint(tableBucket, chain, upTo);
        } catch (IOException e) {
            forgetChain();
            throw e;
        }
    }

    @Override
    public PositionIndexStore.BucketData readCheckpoint(PositionIndexStore.Checkpoint cp)
            throws IOException {
        try {
            return store.readCheckpoint(cp, tableBucket, freshDir("ckpt-" + cp.snapshotId));
        } catch (IOException e) {
            forgetChain();
            throw e;
        }
    }

    /**
     * Forgets the cached chain listing a failed read came from: a trim may have dropped what it
     * names, which only a fresh listing shows. The next plan lists the chain again.
     */
    private void forgetChain() {
        String registration = lastRegistration;
        if (registration != null) {
            store.forgetChain(registration);
        }
    }

    /** Lists the chain as the registration of {@code offsetsFile} was seen, as a test needs. */
    @VisibleForTesting
    void useRegistration(String offsetsFile) {
        lastRegistration = offsetsFile;
    }

    @Override
    public void uploadCheckpoint(
            long snapshotId, @Nullable String localSst, Map<Integer, String> fileDict)
            throws IOException {
        store.writeCheckpoint(
                snapshotId, tableBucket, localSst == null ? null : new File(localSst), fileDict);
    }

    /**
     * Returns a new directory to download into. Also removes the empty download directories of
     * earlier reads, whose files ingestion has moved away. Reads of a bucket run one at a time.
     */
    private File freshDir(String prefix) {
        File[] dirs = localTmp.listFiles(File::isDirectory);
        if (dirs != null) {
            for (File dir : dirs) {
                String[] left = dir.list();
                if (left != null && left.length == 0 && !dir.delete()) {
                    LOG.debug("Could not delete the deletion vector download directory {}.", dir);
                }
            }
        }
        return new File(localTmp, prefix + "-" + System.nanoTime());
    }
}
