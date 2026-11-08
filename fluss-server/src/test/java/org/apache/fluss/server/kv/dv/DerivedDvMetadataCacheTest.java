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

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.RowPosSstIndex;
import org.apache.fluss.lake.dv.index.FlussPositionIndexStore;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.json.TableBucketOffsets;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests {@link DerivedDvMetadataCache}. */
class DerivedDvMetadataCacheTest {

    private static final long TABLE_ID = 7L;
    private static final TablePath TABLE = TablePath.of("db", "t");
    private static final int BUCKETS = 16;

    @TempDir Path tempDir;

    /**
     * Buckets of a table on one tablet server that catch up to a round read its chain listing,
     * marker, committed partitions and manifest once, not once per bucket.
     */
    @Test
    void testBucketsOfATableReadTheRoundsIndexMetadataOnce() throws Exception {
        String remoteDataDir = tempDir.resolve("remote").toUri().toString();
        FsPath lakeDir = FlussPaths.remoteLakeTableSnapshotDir(remoteDataDir, TABLE, TABLE_ID);
        FlussPositionIndexStore writer = new FlussPositionIndexStore(lakeDir);
        Map<TableBucket, PositionIndexStore.BucketChange> change = new HashMap<>();
        for (int b = 0; b < BUCKETS; b++) {
            TableBucket bucket = new TableBucket(TABLE_ID, b);
            byte[] key = ("k" + b).getBytes(StandardCharsets.UTF_8);
            change.put(
                    bucket,
                    new PositionIndexStore.BucketChange(
                            writer.writeBucket(
                                    5,
                                    bucket,
                                    Collections.<Map.Entry<byte[], FilePos>>singletonList(
                                                    new AbstractMap.SimpleEntry<>(
                                                            key, new FilePos(b + 1, 0)))
                                            .iterator()),
                            Collections.singletonMap(b + 1, "f" + b + ".parquet"),
                            Collections.emptyList()));
        }
        writer.commitChange(5, change);
        writer.register(-1, 100, 5, "snapshot 100");

        // without the cache, each bucket's store reads them
        AtomicInteger manifestReads = new AtomicInteger();
        for (int b = 0; b < BUCKETS; b++) {
            FlussPositionIndexStore perBucket =
                    new FlussPositionIndexStore(lakeDir) {
                        @Override
                        protected RowPosSstIndex readManifest(
                                long changeId, @Nullable Long partitionId) throws IOException {
                            manifestReads.incrementAndGet();
                            return super.readManifest(changeId, partitionId);
                        }
                    };
            perBucket.readChange(
                    perBucket.chain().get(0), new TableBucket(TABLE_ID, b), download(b));
        }
        assertThat(manifestReads.get()).isEqualTo(BUCKETS);

        DerivedDvMetadataCache cache = new DerivedDvMetadataCache();
        for (int b = 0; b < BUCKETS; b++) {
            // each bucket's recovery source has a store of its own
            DerivedDvMetadataCache.CachingStore store = cache.store(remoteDataDir, TABLE, TABLE_ID);
            List<PositionIndexStore.ChainEntry> chain = store.chain("registration-1");
            assertThat(chain).hasSize(1);
            assertThat(store.snapshotIdentity(chain.get(0))).isEqualTo("snapshot 100");
            PositionIndexStore.BucketData data =
                    store.readChange(chain.get(0), new TableBucket(TABLE_ID, b), download(b));
            assertThat(data.localSsts).hasSize(1);
            assertThat(data.fileId2Name).containsOnlyKeys(b + 1);
        }
        // {offsets, chain, identity, committed, manifest}
        assertThat(cache.loads()).containsExactly(0, 1, 1, 1, 1);

        // a newer registration lists the chain again: it may have markers the old listing lacks
        writer.commitChange(6, Collections.emptyMap());
        writer.register(100, 200, 6, "snapshot 200");
        assertThat(cache.store(remoteDataDir, TABLE, TABLE_ID).chain("registration-2")).hasSize(2);
        assertThat(cache.store(remoteDataDir, TABLE, TABLE_ID).chain("registration-1")).hasSize(1);
        assertThat(cache.loads()).containsExactly(0, 2, 1, 1, 1);
    }

    /** An offsets file, which holds the offsets of every bucket, is read once per server. */
    @Test
    void testOffsetsFileIsReadOnce() throws Exception {
        Map<TableBucket, Long> offsets = new HashMap<>();
        for (int b = 0; b < BUCKETS; b++) {
            offsets.put(new TableBucket(TABLE_ID, b), 1000L + b);
        }
        Path file = tempDir.resolve("metadata").resolve("x.offsets");
        Files.createDirectories(file.getParent());
        Files.write(file, new TableBucketOffsets(TABLE_ID, offsets).toJsonBytes());

        DerivedDvMetadataCache cache = new DerivedDvMetadataCache();
        for (int b = 0; b < BUCKETS; b++) {
            assertThat(cache.offsets(new FsPath(file.toUri())).get(new TableBucket(TABLE_ID, b)))
                    .isEqualTo(1000L + b);
        }
        assertThat(cache.loads()).containsExactly(1, 0, 0, 0, 0);
    }

    private File download(int bucket) throws IOException {
        return Files.createTempDirectory(tempDir, "dl-" + bucket).toFile();
    }
}
