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

package org.apache.fluss.server.kv.dv;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.RowPosSstIndex;
import org.apache.fluss.lake.dv.index.FlussPositionIndexStore;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.data.lake.LakeTable;
import org.apache.fluss.utils.FlussPaths;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Caches the remote metadata a tablet server's leaders read to catch their deletion vectors up to a
 * new lake snapshot, so the server reads it once rather than once per bucket. In a round, every
 * bucket of a table reads the same offsets file, chain listing, change markers and manifests, each
 * covering all buckets, so per-bucket reads grow with the square of the bucket count.
 *
 * <p>Only what never changes once written is cached by its name: an offsets file (written once
 * under a unique name), a chain marker's content, a committed change's partitions and manifests. A
 * chain listing is cached per readable registration a bucket saw. A listing made after one
 * registration is never served for a newer one, nor for an id that a rollback reused, because that
 * registration has a new offsets file.
 */
@Internal
public final class DerivedDvMetadataCache {

    private static final Duration EXPIRY = Duration.ofMinutes(10);

    /** For servers and tests without a cache of their own. */
    public static final DerivedDvMetadataCache DEFAULT = new DerivedDvMetadataCache();

    private final Cache<String, Map<TableBucket, Long>> offsets = cache(256);
    private final Cache<String, List<PositionIndexStore.ChainEntry>> chains = cache(256);
    private final Cache<String, String> identities = cache(4096);
    private final Cache<String, String> committed = cache(4096);
    private final Cache<String, RowPosSstIndex> manifests = cache(1024);

    private final AtomicLong offsetsLoads = new AtomicLong();
    private final AtomicLong chainLoads = new AtomicLong();
    private final AtomicLong identityLoads = new AtomicLong();
    private final AtomicLong committedLoads = new AtomicLong();
    private final AtomicLong manifestLoads = new AtomicLong();

    private static <V> Cache<String, V> cache(int entries) {
        return Caffeine.newBuilder().maximumSize(entries).expireAfterAccess(EXPIRY).build();
    }

    /** The index store of a table, reading its immutable metadata through this cache. */
    public CachingStore store(String remoteDataDir, TablePath tablePath, long tableId) {
        return new CachingStore(
                FlussPaths.remoteLakeTableSnapshotDir(remoteDataDir, tablePath, tableId));
    }

    /** The bucket offsets of a readable registration's offsets file. */
    public Map<TableBucket, Long> offsets(FsPath offsetsFile) throws IOException {
        return load(
                offsets,
                offsetsFile.toString(),
                offsetsLoads,
                () -> LakeTable.readOffsets(offsetsFile));
    }

    @VisibleForTesting
    long[] loads() {
        return new long[] {
            offsetsLoads.get(),
            chainLoads.get(),
            identityLoads.get(),
            committedLoads.get(),
            manifestLoads.get()
        };
    }

    private interface Loader<V> {
        @Nullable
        V load() throws IOException;
    }

    /** Returns the cached value. One caller loads it while others wait. A null is not cached. */
    @Nullable
    private static <V> V load(
            Cache<String, V> cache, String key, AtomicLong loads, Loader<V> loader)
            throws IOException {
        try {
            return cache.get(
                    key,
                    k -> {
                        loads.incrementAndGet();
                        try {
                            return loader.load();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    /** An index store whose immutable metadata reads go through the cache. */
    public final class CachingStore extends FlussPositionIndexStore {
        private final String dir;

        private CachingStore(FsPath lakeTableDir) {
            super(lakeTableDir);
            this.dir = lakeTableDir.toString();
        }

        /**
         * Returns the chain as listed once after the readable registration {@code registration}
         * (its offsets file) was seen. The listing has every marker registered up to it.
         */
        public List<PositionIndexStore.ChainEntry> chain(String registration) throws IOException {
            return load(chains, dir + "|" + registration, chainLoads, super::chain);
        }

        @Nullable
        @Override
        public String snapshotIdentity(PositionIndexStore.ChainEntry entry) throws IOException {
            return load(
                    identities,
                    dir + "|" + entry,
                    identityLoads,
                    () -> super.snapshotIdentity(entry));
        }

        @Nullable
        @Override
        protected String readCommittedPartitions(long changeId) throws IOException {
            return load(
                    committed,
                    dir + "|" + changeId,
                    committedLoads,
                    () -> super.readCommittedPartitions(changeId));
        }

        @Override
        protected RowPosSstIndex readManifest(long changeId, @Nullable Long partitionId)
                throws IOException {
            return load(
                    manifests,
                    dir + "|" + changeId + "|" + partitionId,
                    manifestLoads,
                    () -> super.readManifest(changeId, partitionId));
        }
    }
}
