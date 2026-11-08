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

package org.apache.fluss.flink.lake.reader;

import org.apache.fluss.client.table.scanner.batch.dv.DvBucketMaskState;
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketMasks;
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketPart.ReadMode;
import org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket;
import org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.PinningServer;
import org.apache.fluss.exception.LakeTableRolledBackException;
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.flink.lake.LakeRecordRecordEmitter;
import org.apache.fluss.flink.lake.split.DvBucketSplit;
import org.apache.fluss.flink.lake.state.DvBucketSplitState;
import org.apache.fluss.flink.source.reader.RecordAndPos;
import org.apache.fluss.flink.source.split.SourceSplitSerializer;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.rpc.messages.GetDvSnapshotRequest;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.BUCKET;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.EXACT;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.SNAPSHOT;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.bitmap;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.covered;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.masks;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.rowString;
import static org.apache.fluss.client.table.scanner.batch.dv.DvTestBucket.serialize;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests of {@link DvBucketSplitScanner}: the state a {@link DvBucketSplit} keeps, its serialized
 * form, and its restores. The reader core it wraps is tested in fluss-client.
 */
class DvBucketSplitScannerTest {

    private final DvTestBucket bucket = new DvTestBucket();
    private final DvTestBucket.MaskServer server = bucket.server;
    private final DvTestBucket.FileLakeSource lake = bucket.lake;
    private final DvTestBucket.FakeLogScanner log = bucket.log;
    private final DvTestBucket.Context context = bucket.context;

    @Test
    void testMaskedReadIsExactAndStopsAtTheCut() throws Exception {
        bucket.standardBucket();

        Read read = read(split(), Integer.MAX_VALUE);

        assertThat(read.rows).containsExactlyInAnyOrderElementsOf(EXACT);
        assertThat(read.state.getReadMode()).isEqualTo(ReadMode.MASKED);
        DvBucketSplit checkpointed = (DvBucketSplit) read.state.toSourceSplit();
        assertThat(checkpointed.getMaskLogEndOffset()).isEqualTo(106L);
        assertThat(checkpointed.getNextLogOffset()).isEqualTo(106L);
        // one pin; the other requests fetch the masks of its cut
        assertThat(bucket.pins()).hasSize(1);
        assertThat(bucket.pins().get(0).hasPinRequestId()).isTrue();
        assertThat(log.subscribedFrom).isEqualTo(100L);
    }

    /** Masks larger than an assignment RPC can carry are fetched by the reader and applied. */
    @Test
    void testMasksOverEightMegabytes() throws Exception {
        bucket.standardBucket();
        GetDvSnapshotResponse large = masks(SNAPSHOT, 100, 106);
        // other files of the bucket, each half masked
        Roaring64Bitmap halfMasked = new Roaring64Bitmap();
        for (long pos = 0; pos < 65536; pos += 2) {
            halfMasked.addLong(pos);
        }
        byte[] halfMaskedBytes = serialize(halfMasked);
        int files = (int) ((9L << 20) / halfMaskedBytes.length) + 1;
        for (int i = 0; i < files; i++) {
            large.addLakeDvEntry()
                    .setFilePath("other-" + i + ".parquet")
                    .setDeletedPositionsBitmap(halfMaskedBytes);
        }
        assertThat(large.totalSize()).isGreaterThan(8 << 20);
        server.answer = request -> large;

        Read read = read(split(), Integer.MAX_VALUE);

        assertThat(read.rows).containsExactlyInAnyOrderElementsOf(EXACT);
        // only the masks of the split's own files are kept
        DvBucketSplit checkpointed = (DvBucketSplit) read(split(), 2).state.toSourceSplit();
        assertThat(checkpointed.getMaskState().getLakeMasks())
                .containsOnlyKeys("f0.parquet", "f1.parquet");
    }

    /**
     * A started split keeps masks over 8 MB in its state and restores exactly from it. The state
     * goes to checkpoint storage, not through an assignment RPC.
     */
    @Test
    void testStartedSplitKeepsLargeMasksInItsState() throws Exception {
        bucket.standardBucket();
        Roaring64Bitmap lakeMask = new Roaring64Bitmap();
        lakeMask.addLong(1);
        // masked positions far past the file's rows, never read
        for (long pos = 1L << 32; pos < (1L << 32) + (1L << 27); pos += 2) {
            lakeMask.addLong(pos);
        }
        GetDvSnapshotResponse large = masks(SNAPSHOT, 100, 106);
        large.clearLakeDvEntries();
        large.addLakeDvEntry()
                .setFilePath("f0.parquet")
                .setDeletedPositionsBitmap(serialize(lakeMask));
        large.addLakeDvEntry().setFilePath("f1.parquet").setDeletedPositionsBitmap(bitmap(1));
        server.answer = request -> large;

        Read first = read(split(), 2);
        DvBucketSplit checkpointed = (DvBucketSplit) first.state.toSourceSplit();
        assertThat(serializedSize(checkpointed)).isGreaterThan(8 << 20);
        server.clearRequests();
        Read rest = read(roundTrip(checkpointed), Integer.MAX_VALUE);

        List<String> all = new ArrayList<>(first.rows);
        all.addAll(rest.rows);
        assertThat(all).containsExactlyInAnyOrderElementsOf(EXACT);
        assertThat(server.requests()).isEmpty();
    }

    /** A planned split carries no masks, and a checkpoint only those the rest of the read needs. */
    @Test
    void testSplitStateSizeIsBounded() throws Exception {
        bucket.standardBucket();
        DvBucketSplit planned = split();
        assertThat(planned.getMaskState()).isNull();

        // resolved but nothing emitted: no masks kept, a restore fetches them again
        DvBucketSplit resolvedOnly = (DvBucketSplit) resolveOnly(planned).toSourceSplit();
        assertThat(resolvedOnly.isStarted()).isFalse();
        assertThat(resolvedOnly.getMaskState()).isNull();
        assertThat(serializedSize(resolvedOnly) - serializedSize(planned)).isLessThan(64);

        // in the lake phase: the masks of the files still to read and the log mask
        DvBucketSplit inLake = (DvBucketSplit) read(planned, 3).state.toSourceSplit();
        assertThat(inLake.getCurrentLakeSplitIndex()).isEqualTo(1);
        assertThat(inLake.getMaskState().getLakeMasks()).containsOnlyKeys("f1.parquet");
        assertThat(inLake.getMaskState().getLogMask()).isNotNull();
        assertThat((long) serializedSize(inLake) - serializedSize(planned))
                .isLessThan(256 + inLake.getMaskState().maskBytes());

        // in the log phase: the log mask only
        DvBucketSplit inLog = (DvBucketSplit) read(planned, 5).state.toSourceSplit();
        assertThat(inLog.getNextLogOffset()).isEqualTo(102L);
        assertThat(inLog.getMaskState().getLakeMasks()).isEmpty();
        assertThat(inLog.getMaskState().maskBytes())
                .isEqualTo(inLog.getMaskState().getLogMask().length);
    }

    /**
     * A split restored mid-read uses the masks its state kept, although the server now serves
     * another cut. Without them it could not resume exactly.
     */
    @Test
    void testRestoreReadsWithKeptMasksWhenTheServerMovedOn() throws Exception {
        bucket.standardBucket();
        Read first = read(split(), 2);
        DvBucketSplit checkpointed = (DvBucketSplit) first.state.toSourceSplit();
        assertThat(checkpointed.getMaskState().getLogEndOffset()).isEqualTo(106L);
        checkpointed = roundTrip(checkpointed);

        // new writes moved the high watermark, and the server now refuses cut 106
        log.append(107, ChangeType.UPDATE_AFTER, 2, "later");
        server.answer = request -> masks(SNAPSHOT, 100, 108);
        server.failures.add(new StaleSnapshotException(SNAPSHOT + 1, SNAPSHOT));
        server.clearRequests();
        Read rest = read(checkpointed, Integer.MAX_VALUE);

        List<String> all = new ArrayList<>(first.rows);
        all.addAll(rest.rows);
        assertThat(all).containsExactlyInAnyOrderElementsOf(EXACT);
        assertThat(server.requests()).isEmpty();
    }

    /** A split restored before it emitted anything fetches its masks again, at a new cut. */
    @Test
    void testRestoreOfUnstartedSplitRefetches() throws Exception {
        bucket.standardBucket();
        DvBucketSplit checkpointed = (DvBucketSplit) resolveOnly(split()).toSourceSplit();
        log.append(107, ChangeType.UPDATE_AFTER, 2, "later");
        // the masks of cut 108: keys 0, 1 and 2 changed in the lake's first file
        server.answer =
                request -> {
                    GetDvSnapshotResponse response = masks(SNAPSHOT, 100, 108);
                    response.clearLakeDvEntries();
                    response.addLakeDvEntry()
                            .setFilePath("f0.parquet")
                            .setDeletedPositionsBitmap(bitmap(0, 1, 2));
                    response.addLakeDvEntry()
                            .setFilePath("f1.parquet")
                            .setDeletedPositionsBitmap(bitmap(1));
                    return response;
                };
        server.clearRequests();

        Read read = read(roundTrip(checkpointed), Integer.MAX_VALUE);

        assertThat(bucket.pins()).hasSize(1);
        assertThat(read.rows)
                .containsExactlyInAnyOrder("0=late", "1=b", "2=later", "3=a", "5=a", "6=b");
    }

    /** After a restore, kept masks are not applied to a snapshot id that a rollback reused. */
    @Test
    void testRestoreChecksTheKeptSnapshotIdentity() throws Exception {
        bucket.standardBucket();
        Function<GetDvSnapshotRequest, GetDvSnapshotResponse> base = server.answer;
        server.answer = request -> base.apply(request).setLakeSnapshotIdentity("commit-a");
        lake.identities.put(SNAPSHOT, "commit-a");
        DvBucketSplit checkpointed = (DvBucketSplit) read(split(), 2).state.toSourceSplit();
        assertThat(checkpointed.getMaskState().getLakeSnapshotIdentity()).isEqualTo("commit-a");

        lake.identities.put(SNAPSHOT, "commit-b");
        assertThatThrownBy(() -> read(roundTrip(checkpointed), Integer.MAX_VALUE))
                .isInstanceOf(LakeTableRolledBackException.class)
                .hasMessageContaining("commit-b");
    }

    /** A restore in the log phase goes on after the last emitted log row. */
    @Test
    void testRestoreInTheLogPhase() throws Exception {
        bucket.standardBucket();
        // the lake rows and key 1 from offset 101
        Read first = read(split(), 5);
        DvBucketSplit checkpointed = (DvBucketSplit) first.state.toSourceSplit();
        assertThat(checkpointed.getNextLogOffset()).isEqualTo(102L);

        server.clearRequests();
        Read rest = read(roundTrip(checkpointed), Integer.MAX_VALUE);

        List<String> all = new ArrayList<>(first.rows);
        all.addAll(rest.rows);
        assertThat(all).containsExactlyInAnyOrderElementsOf(EXACT);
        assertThat(log.subscribedFrom).isEqualTo(102L);
        assertThat(server.requests()).isEmpty();
    }

    /** A restored read without the masks of its cut fails rather than lose or repeat rows. */
    @Test
    void testRestoreWithoutKeptMasksFails() throws Exception {
        bucket.standardBucket();
        DvBucketSplit checkpointed =
                ((DvBucketSplit) read(split(), 2).state.toSourceSplit()).withMaskState(null);
        server.clearRequests();

        assertThatThrownBy(() -> read(checkpointed, Integer.MAX_VALUE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot resume the union read")
                .hasMessageContaining("up to log offset 106")
                .hasMessageContaining("kept no masks");
        assertThat(server.requests()).isEmpty();
    }

    /** A started split of a pinned bucket restores from the masks its state kept. */
    @Test
    void testRestoreOfAPinnedSplitUsesItsKeptMasks() throws Exception {
        bucket.standardBucket();
        PinningServer pins = bucket.pinningServer();
        long cut = pins.pin();
        DvBucketSplit group1 = pinnedSplit(lake.plans.get(SNAPSHOT).subList(1, 2), 1, cut);
        Read first = read(group1, 1);
        DvBucketSplit checkpointed = roundTrip((DvBucketSplit) first.state.toSourceSplit());
        assertThat(checkpointed.getMaskState().getLakeMasks()).containsOnlyKeys("f1.parquet");
        pins.cuts.clear();
        server.clearRequests();
        log.append(107, ChangeType.DELETE, 5, "x");

        Read rest = read(checkpointed, Integer.MAX_VALUE);

        List<String> all = new ArrayList<>(first.rows);
        all.addAll(rest.rows);
        assertThat(all).containsExactlyInAnyOrder("3=a", "5=a");
        assertThat(server.requests()).isEmpty();
    }

    /**
     * A split read by key keeps the masks it derived, which are those the leader serves for the
     * cut. Restored after it emitted rows, it reads with them and asks the leader nothing, though
     * the log moved on. Restored before its first row, it derives them again, of the same cut.
     */
    @Test
    void testRestoreOfASplitReadByKey() throws Exception {
        bucket.standardBucket();
        context.waitForCatchUp = false;
        server.answers.add(new GetDvSnapshotResponse().setFoldCut(106));
        Read first = read(split(), 1);
        DvBucketSplit checkpointed = roundTrip((DvBucketSplit) first.state.toSourceSplit());
        assertThat(checkpointed.getReadMode()).isEqualTo(ReadMode.FOLDED);
        assertThat(checkpointed.getMaskLogEndOffset()).isEqualTo(106L);
        DvBucketMaskState kept = checkpointed.getMaskState();
        GetDvSnapshotResponse served = masks(SNAPSHOT, 100, 106);
        for (String file : Arrays.asList("f0.parquet", "f1.parquet")) {
            assertThat(bitmapOf(kept.getLakeMasks().get(file)))
                    .isEqualTo(bitmapOf(DvBucketMasks.lakeMasksOf(served).get(file)));
        }
        assertThat(bitmapOf(kept.getLogMask())).isEqualTo(bitmapOf(served.getLogDvBitmap()));
        server.clearRequests();
        context.keyReads.clear();
        log.append(107, ChangeType.DELETE, 2, "x");

        Read rest = read(checkpointed, Integer.MAX_VALUE);

        List<String> all = new ArrayList<>(first.rows);
        all.addAll(rest.rows);
        assertThat(all).containsExactlyInAnyOrderElementsOf(EXACT);
        assertThat(server.requests()).isEmpty();
        assertThat(context.keyReads).isEmpty();

        server.answers.add(new GetDvSnapshotResponse().setFoldCut(106));
        DvBucketSplit unstarted =
                roundTrip((DvBucketSplit) resolveOnly(split(), ReadMode.FOLDED).toSourceSplit());
        assertThat(unstarted.getMaskState()).isNull();
        server.clearRequests();
        context.keyReads.clear();

        Read again = read(unstarted, Integer.MAX_VALUE);

        assertThat(again.rows).containsExactlyInAnyOrderElementsOf(EXACT);
        assertThat(server.requests()).isEmpty();
        assertThat(context.keyReads).containsExactly("log", "f0.parquet", "f1.parquet");
    }

    /**
     * A split pinned at planning whose cut is gone resolves afresh on the snapshot its leader
     * covers now, which reads the bucket's log from another offset. Checkpointed before it emitted
     * a row and restored, it reads the log from that offset again, not from the one planning
     * pinned.
     */
    @Test
    void testRestoreOfASplitPinnedAgainReadsTheLogFromItsNewStart() throws Exception {
        bucket.standardBucket();
        // snapshot 6 holds none of the bucket's files: key 1 was tiered into it up to offset 102
        long newer = SNAPSHOT + 1;
        List<Long> held = new ArrayList<>();
        server.answer =
                request -> {
                    long snapshotId = request.getReadableSnapshotId();
                    if (!request.hasLogEndOffset()) {
                        if (snapshotId == SNAPSHOT) {
                            return covered(newer, false, 102);
                        }
                        held.add(106L);
                        return new GetDvSnapshotResponse()
                                .setLogEndOffset(106)
                                .setSnapshotStartOffset(102)
                                .setLogDvBitmap(bitmap(103));
                    }
                    if (snapshotId != newer || !held.contains(request.getLogEndOffset())) {
                        throw new CompletionException(
                                new StaleSnapshotException(newer, snapshotId));
                    }
                    return new GetDvSnapshotResponse()
                            .setLogEndOffset(request.getLogEndOffset())
                            .setSnapshotStartOffset(102)
                            .setLogDvBitmap(bitmap(103));
                };
        context.waitForCatchUp = false;
        DvBucketSplit planned =
                DvBucketSplit.pinned(
                        BUCKET,
                        null,
                        SNAPSHOT,
                        100L,
                        106L,
                        null,
                        DvBucketSplit.WHOLE_BUCKET,
                        106L,
                        100L);

        DvBucketSplit checkpointed =
                roundTrip((DvBucketSplit) resolveOnly(planned).toSourceSplit());

        assertThat(checkpointed.getSnapshotId()).isEqualTo(newer);
        assertThat(checkpointed.getMaskState()).isNull();
        // the leader holds cut 106 of snapshot 6 still, for the split that is restored
        server.clearRequests();
        Read restored = read(checkpointed, Integer.MAX_VALUE);

        // as of cut 106 from offset 102: the update of key 1 at offset 101 is in snapshot 6, and
        // offset 103 is superseded
        assertThat(restored.rows).containsExactly("6=b");
        assertThat(log.subscribedFrom).isEqualTo(102L);
        assertThat(server.requests())
                .filteredOn(GetDvSnapshotRequest::hasLogDvFromOffset)
                .extracting(GetDvSnapshotRequest::getLogDvFromOffset)
                .doesNotContain(100L);
    }

    /**
     * A split of a bucket with too many lake files to carry plans them when it starts. Restored
     * after it emitted rows, it reads on with the files and masks its state kept.
     */
    @Test
    void testSplitPlanningItsLakeFilesRestoresWithThem() throws Exception {
        bucket.standardBucket();
        DvBucketSplit planned =
                new DvBucketSplit(bucket.bucketPart().withLakeSplitsPlannedAtStart());
        DvBucketSplit assigned = roundTrip(planned);
        assertThat(assigned.getLakeSplits()).isNull();
        assertThat(assigned.isLakeSplitsPlannedAtStart()).isTrue();

        Read first = read(assigned, 2);
        DvBucketSplit checkpointed = roundTrip((DvBucketSplit) first.state.toSourceSplit());
        assertThat(checkpointed.getLakeSplits()).hasSize(2);
        assertThat(checkpointed.isLakeSplitsPlannedAtStart()).isFalse();
        context.plannedSnapshots.clear();
        server.clearRequests();
        Read rest = read(checkpointed, Integer.MAX_VALUE);

        List<String> all = new ArrayList<>(first.rows);
        all.addAll(rest.rows);
        assertThat(all).containsExactlyInAnyOrderElementsOf(EXACT);
        assertThat(context.plannedSnapshots).isEmpty();
        assertThat(server.requests()).isEmpty();
    }

    // ------------------------------------------------------------------------------------------

    private DvBucketSplit split() {
        return new DvBucketSplit(bucket.bucketPart());
    }

    private DvBucketSplit pinnedSplit(@Nullable List<LakeSplit> files, int fileGroup, long cut) {
        return new DvBucketSplit(bucket.pinnedPart(files, fileGroup, cut));
    }

    /** The rows a reader emits for a split, and its state, stopping after {@code maxRows}. */
    private Read read(DvBucketSplit split, int maxRows) throws Exception {
        DvBucketSplitState state = new DvBucketSplitState(split);
        List<String> rows = new ArrayList<>();
        LakeRecordRecordEmitter<Void> emitter =
                new LakeRecordRecordEmitter<>(
                        (record, output) -> rows.add(rowString(record.getRow())));
        try (DvBucketSplitReader reader =
                new DvBucketSplitReader(new DvBucketSplitScanner(split, context), split)) {
            while (rows.size() < maxRows) {
                CloseableIterator<RecordAndPos> batch = reader.readBatch();
                if (batch == null) {
                    break;
                }
                while (rows.size() < maxRows && batch.hasNext()) {
                    emitter.emitRecord(state, null, batch.next());
                }
                batch.close();
            }
        }
        return new Read(rows, state);
    }

    /** The state of a split whose reader resolved it and emitted no row yet. */
    private DvBucketSplitState resolveOnly(DvBucketSplit split) throws Exception {
        return resolveOnly(split, ReadMode.MASKED);
    }

    private DvBucketSplitState resolveOnly(DvBucketSplit split, ReadMode mode) throws Exception {
        DvBucketSplitState state = new DvBucketSplitState(split);
        LakeRecordRecordEmitter<Void> emitter = new LakeRecordRecordEmitter<>((r, o) -> {});
        try (DvBucketSplitReader reader =
                new DvBucketSplitReader(new DvBucketSplitScanner(split, context), split)) {
            CloseableIterator<RecordAndPos> marker = reader.readBatch();
            emitter.emitRecord(state, null, marker.next());
            assertThat(marker.hasNext()).isFalse();
            marker.close();
        }
        assertThat(state.getReadMode()).isEqualTo(mode);
        return state;
    }

    private static final class Read {
        final List<String> rows;
        final DvBucketSplitState state;

        Read(List<String> rows, DvBucketSplitState state) {
            this.rows = rows;
            this.state = state;
        }
    }

    private int serializedSize(DvBucketSplit split) throws IOException {
        return new SourceSplitSerializer(lake).serialize(split).length;
    }

    /** The split as a checkpoint stores and restores it. */
    private DvBucketSplit roundTrip(DvBucketSplit split) throws IOException {
        SourceSplitSerializer serializer = new SourceSplitSerializer(lake);
        return (DvBucketSplit)
                serializer.deserialize(serializer.getVersion(), serializer.serialize(split));
    }

    private static Roaring64Bitmap bitmapOf(byte[] bytes) throws IOException {
        return DvBucketMasks.deserialize(bytes, "a test mask");
    }
}
