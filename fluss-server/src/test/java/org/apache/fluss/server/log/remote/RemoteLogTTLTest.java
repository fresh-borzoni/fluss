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

package org.apache.fluss.server.log.remote;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.entity.LakeBucketOffset;
import org.apache.fluss.server.entity.NotifyLakeTableOffsetData;
import org.apache.fluss.server.kv.dv.DerivedDvManager;
import org.apache.fluss.server.log.FetchParams;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.remote.RemoteLogIndexCache.Entry;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.zk.data.lake.LakeTable;
import org.apache.fluss.utils.json.TableBucketOffsets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for remote log ttl in {@link RemoteLogManager}. */
final class RemoteLogTTLTest extends RemoteLogTestBase {

    @BeforeEach
    public void setup() throws Exception {
        super.setup();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRemoteLogTTL(boolean partitionTable) throws Exception {
        TableBucket tb = makeTableBucket(partitionTable);
        // Need to make leader by ReplicaManager.
        makeLogTableAsLeader(tb, partitionTable);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        // enable data lake
        updateTableConfig(replica, ConfigOptions.TABLE_DATALAKE_ENABLED, "true");
        // add 5 segments, so 4 segments will be uploaded to remote (exclude active segment)
        // segment offsets: [0,10), [10,20), [20,30), [30,40), [40,50) active
        addMultiSegmentsToLogTablet(logTablet, 5);
        // run RLMTask to copy local log segment to remote and commit snapshot.
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        RemoteLogTablet remoteLog = remoteLogManager.remoteLogTablet(tb);
        assertThat(remoteLog.relevantRemoteLogSegments(0L).size()).isEqualTo(4);
        assertThat(remoteLog.allRemoteLogSegments().size()).isEqualTo(4);
        assertThat(remoteLog.getRemoteLogStartOffset()).isEqualTo(0L);
        assertThat(remoteLog.getRemoteLogEndOffset()).hasValue(40L);

        // Materialize the index cache entries for all remote log segments.
        RemoteLogIndexCache indexCache =
                remoteLogManager.getRemoteLogIndexCache(logTablet.getDataDir());
        for (RemoteLogSegment remoteLogSegment : remoteLog.allRemoteLogSegments()) {
            remoteLogManager.lookupPositionForOffset(
                    remoteLogSegment, remoteLogSegment.remoteLogStartOffset());
        }
        assertThat(indexCache.getInternalCache().asMap()).hasSize(4);
        RemoteLogSegment expiredSegment =
                remoteLog.allRemoteLogSegments().stream()
                        .filter(segment -> segment.remoteLogStartOffset() < 20L)
                        .findFirst()
                        .get();
        RemoteLogSegment retainedSegment =
                remoteLog.allRemoteLogSegments().stream()
                        .filter(segment -> segment.remoteLogStartOffset() >= 20L)
                        .findFirst()
                        .get();
        Entry expiredIndexEntry =
                indexCache.getInternalCache().getIfPresent(expiredSegment.remoteLogSegmentId());
        Entry retainedIndexEntry =
                indexCache.getInternalCache().getIfPresent(retainedSegment.remoteLogSegmentId());
        assertThat(expiredIndexEntry).isNotNull();
        assertThat(retainedIndexEntry).isNotNull();

        // advance time past TTL (7 days)
        manualClock.advanceTime(Duration.ofDays(7).plusHours(1));

        // since data lake is enabled and no data has been tiered to data lake,
        // the expired segments should not be deleted.
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        assertThat(remoteLog.allRemoteLogSegments()).hasSize(4);
        assertThat(remoteLog.getRemoteLogStartOffset()).isEqualTo(0L);

        // set lake log end offset to 20, meaning only the first 2 segments
        // ([0,10) and [10,20)) have been tiered to lake
        logTablet.updateLakeLogEndOffset(20L);

        // trigger RLMTask to delete expired segments
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        // only segments with remoteLogEndOffset <= 20 should be deleted (first 2 segments)
        // remaining segments: [20,30) and [30,40)
        assertThat(remoteLog.allRemoteLogSegments()).hasSize(2);
        assertThat(remoteLog.getRemoteLogStartOffset()).isEqualTo(20L);
        assertThat(remoteLog.getRemoteLogEndOffset()).hasValue(40L);
        // verify remaining segments have the expected offsets
        assertThat(remoteLog.allRemoteLogSegments())
                .allSatisfy(
                        segment ->
                                assertThat(segment.remoteLogStartOffset())
                                        .isGreaterThanOrEqualTo(20L));
        assertThat(indexCache.getInternalCache().asMap())
                .hasSize(2)
                .doesNotContainKeys(expiredSegment.remoteLogSegmentId())
                .containsKey(retainedSegment.remoteLogSegmentId());
        assertThat(expiredIndexEntry.offsetIndex().file()).doesNotExist();
        assertThat(expiredIndexEntry.timeIndex().file()).doesNotExist();
        assertThat(retainedIndexEntry.offsetIndex().file()).exists();
        assertThat(retainedIndexEntry.timeIndex().file()).exists();

        // now advance lake log end offset to include all remaining segments
        logTablet.updateLakeLogEndOffset(40L);
        // trigger again, remaining expired segments should now be deleted
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        assertThat(remoteLog.allRemoteLogSegments()).isEmpty();
        assertThat(remoteLog.getRemoteLogStartOffset()).isEqualTo(Long.MAX_VALUE);
        assertThat(remoteLog.getHighestCopiedEndOffset()).isEqualTo(40L);

        // Fetch records from remote. Mock the empty manifest state propagated by
        // NotifyRemoteLogOffsetsRequest.
        logTablet.updateRemoteLogOffsets(Long.MAX_VALUE, -1L, 40L);
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                new FetchParams(-1, Integer.MAX_VALUE),
                Collections.singletonMap(tb, new FetchReqInfo(tb.getTableId(), 0L, 1024 * 1024)),
                null,
                future::complete);
        Map<TableBucket, FetchLogResultForBucket> result = future.get();
        assertThat(result.size()).isEqualTo(1);
        FetchLogResultForBucket resultForBucket = result.get(tb);
        assertThat(resultForBucket.getErrorCode())
                .isEqualTo(Errors.LOG_OFFSET_OUT_OF_RANGE_EXCEPTION.code());
    }

    /**
     * On Paimon, a bucket's readable offset is the first offset still in level 0, behind its tiered
     * offset. A new leader rebuilds the deletion vectors of the bucket by replaying the log from
     * the readable offset, so expired remote segments are kept from there although they are tiered.
     * They go once a compaction moves the readable offset past them.
     */
    @Test
    void testRemoteLogTTLKeepsTheLogDeletionVectorsNeed() throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "dv_paimon_retention");
        long tableId = 160001L;
        Map<String, String> properties = new HashMap<>();
        properties.put(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true");
        properties.put(ConfigOptions.TABLE_DATALAKE_FORMAT.key(), DataLakeFormat.PAIMON.toString());
        properties.put(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key(), "true");
        registerTableInZkClient(
                tablePath, DATA1_SCHEMA_PK, tableId, Collections.emptyList(), properties);
        TableBucket tb = new TableBucket(tableId, 0);
        makeKvTableAsLeader(tableId, tablePath, 0);
        Replica replica = replicaManager.getReplicaOrException(tb);
        DerivedDvManager dvManager = replica.getKvTablet().getDvManager();
        assertThat(dvManager).isNotNull();
        LogTablet logTablet = replica.getLogTablet();
        // segment offsets: [0,10), [10,20), [20,30), [30,40), [40,50) active
        addMultiSegmentsToLogTablet(logTablet, 5);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        RemoteLogTablet remoteLog = remoteLogManager.remoteLogTablet(tb);
        assertThat(remoteLog.allRemoteLogSegments()).hasSize(4);
        assertThat(remoteLog.getRemoteLogEndOffset()).hasValue(40L);

        // Tiered into level 0 up to offset 40, but nothing is compacted, so no snapshot is
        // readable: a rebuild replays the log from offset 0, and all of it is kept.
        logTablet.updateLakeLogEndOffset(40L);
        manualClock.advanceTime(Duration.ofDays(7).plusHours(1));
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        assertThat(replica.getDvLogRetainOffset()).isZero();
        assertThat(remoteLog.allRemoteLogSegments()).hasSize(4);
        assertThat(remoteLog.getRemoteLogStartOffset()).isEqualTo(0L);
        assertThat(remoteLog.getDvRetainedSegments()).isEqualTo(4);
        assertThat(remoteLog.getDvRetainedSizeInBytes())
                .isEqualTo(
                        remoteLog.allRemoteLogSegments().stream()
                                .mapToLong(RemoteLogSegment::segmentSizeInBytes)
                                .sum());

        // A compaction makes snapshot 1 readable from offset 20, while it is tiered up to 40.
        PositionIndexStore index =
                PositionIndexStore.forTable(
                        replicaManager.getKvSnapshotContext().getRemoteDataDir(),
                        tablePath,
                        tableId,
                        null);
        List<LakeTable.LakeSnapshotMetadata> registered = new ArrayList<>();
        registerReadableSnapshot(index, tb, registered, 1L, 40L, 20L);
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(dvManager.getReadableSnapshotId()).isEqualTo(1L));
        assertThat(replica.getDvLogRetainOffset()).isEqualTo(20L);

        // the segments below the readable offset go, those at or above it stay
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        assertThat(remoteLog.allRemoteLogSegments())
                .hasSize(2)
                .allSatisfy(
                        segment ->
                                assertThat(segment.remoteLogStartOffset())
                                        .isGreaterThanOrEqualTo(20L));
        assertThat(remoteLog.getRemoteLogStartOffset()).isEqualTo(20L);
        assertThat(remoteLog.getDvRetainedSegments()).isEqualTo(2);

        // the next compaction makes all of it readable, and the rest goes
        registerReadableSnapshot(index, tb, registered, 2L, 40L, 40L);
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(dvManager.getReadableSnapshotId()).isEqualTo(2L));
        assertThat(replica.getDvLogRetainOffset()).isEqualTo(40L);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        assertThat(remoteLog.allRemoteLogSegments()).isEmpty();
        assertThat(remoteLog.getDvRetainedSegments()).isZero();
        assertThat(remoteLog.getDvRetainedSizeInBytes()).isZero();
        assertThat(remoteLog.getHighestCopiedEndOffset()).isEqualTo(40L);
    }

    /**
     * Registers lake snapshot {@code snapshotId} as the tiering service does: its position index
     * change in the index chain, then its tiered and readable offsets in ZooKeeper, then the
     * notification of the bucket's leader.
     */
    private void registerReadableSnapshot(
            PositionIndexStore index,
            TableBucket tb,
            List<LakeTable.LakeSnapshotMetadata> registered,
            long snapshotId,
            long tieredOffset,
            long readableOffset)
            throws Exception {
        byte[] key = ("k" + snapshotId).getBytes(StandardCharsets.UTF_8);
        index.commitChange(
                snapshotId,
                Collections.singletonMap(
                        tb,
                        new PositionIndexStore.BucketChange(
                                index.writeBucket(
                                        snapshotId,
                                        tb,
                                        Collections.<Map.Entry<byte[], FilePos>>singletonList(
                                                        new AbstractMap.SimpleEntry<>(
                                                                key,
                                                                new FilePos((int) snapshotId, 0)))
                                                .iterator()),
                                Collections.singletonMap((int) snapshotId, "f" + snapshotId),
                                Collections.emptyList())));
        index.register(registered.isEmpty() ? -1L : snapshotId - 1, snapshotId, snapshotId);

        registered.add(
                new LakeTable.LakeSnapshotMetadata(
                        snapshotId,
                        writeOffsets("tiered-" + snapshotId, tb, tieredOffset),
                        writeOffsets("readable-" + snapshotId, tb, readableOffset)));
        zkClient.upsertLakeTable(
                tb.getTableId(), new LakeTable(new ArrayList<>(registered)), registered.size() > 1);

        CompletableFuture<Void> notified = new CompletableFuture<>();
        replicaManager.notifyLakeTableOffset(
                new NotifyLakeTableOffsetData(
                        0,
                        Collections.singletonMap(
                                tb, new LakeBucketOffset(snapshotId, null, tieredOffset, null))),
                response -> notified.complete(null));
        notified.get();
    }

    private FsPath writeOffsets(String name, TableBucket tb, long offset) throws Exception {
        File file = new File(tempDir, name + ".offsets");
        Files.write(
                file.toPath(),
                new TableBucketOffsets(tb.getTableId(), Collections.singletonMap(tb, offset))
                        .toJsonBytes());
        return new FsPath(file.getAbsolutePath());
    }
}
