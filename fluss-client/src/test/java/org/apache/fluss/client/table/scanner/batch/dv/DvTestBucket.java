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

package org.apache.fluss.client.table.scanner.batch.dv;

import org.apache.fluss.client.admin.DvAdmin;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.exception.LogOffsetOutOfRangeException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.DataDeltaPlan;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.RecordReaders;
import org.apache.fluss.lake.source.RowWithPosResult;
import org.apache.fluss.lake.source.SupportsDataFiles;
import org.apache.fluss.lake.source.SupportsRewritePlanning;
import org.apache.fluss.lake.source.SupportsRowPositions;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.messages.GetDvSnapshotRequest;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.utils.CloseableIterator;

import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * The bucket the tests of union reads with deletion vectors read, with its leader, lake and log,
 * and the reader context over them. Engine tests read it through their split types.
 *
 * <p>The standard bucket ({@link #standardBucket()}) has two lake files of keys 0..5 tiered up to
 * offset 100, and a log tail that updates key 1, deletes key 4, inserts key 6 and updates key 6
 * again. Read up to cut 106 it holds the rows {@link #EXACT}.
 */
public final class DvTestBucket {

    public static final TablePath TABLE_PATH = TablePath.of("db", "t");
    public static final long TABLE_ID = 1L;
    public static final TableBucket BUCKET = new TableBucket(TABLE_ID, 0);
    public static final long SNAPSHOT = 5L;

    /** The rows of the standard bucket up to cut 106. */
    public static final List<String> EXACT =
            Arrays.asList("0=a", "1=b", "2=a", "3=a", "5=a", "6=b");

    public final MaskServer server = new MaskServer();
    public final FileLakeSource lake = new FileLakeSource();
    public final FakeLogScanner log = new FakeLogScanner();
    public final Context context = new Context();

    /**
     * Fills the standard bucket: two lake files of keys 0..5 tiered up to offset 100, and a log
     * tail that updates key 1, deletes key 4, inserts key 6 and updates key 6 again. Offset 106 is
     * written after the masks of cut 106 were taken. The leader serves the masks of cut 106.
     */
    public void standardBucket() {
        lake.plan(
                SNAPSHOT,
                new FileSplit("f0.parquet", new int[] {0, 1, 2}, "a"),
                new FileSplit("f1.parquet", new int[] {3, 4, 5}, "a"));
        log.append(100, ChangeType.UPDATE_BEFORE, 1, "a");
        log.append(101, ChangeType.UPDATE_AFTER, 1, "b");
        log.append(102, ChangeType.DELETE, 4, "a");
        log.append(103, ChangeType.INSERT, 6, "a");
        log.append(104, ChangeType.UPDATE_BEFORE, 6, "a");
        log.append(105, ChangeType.UPDATE_AFTER, 6, "b");
        // written after the masks were taken
        log.append(106, ChangeType.UPDATE_AFTER, 0, "late");
        server.answer = request -> masks(SNAPSHOT, 100, 106);
    }

    /** The planned part that reads all of the standard bucket. */
    public DvBucketPart bucketPart() {
        return DvBucketPart.ofBucket(BUCKET, null, SNAPSHOT, 100L, 106L, lake.plans.get(SNAPSHOT));
    }

    /** A part of the standard bucket read with cut {@code cut} pinned at planning. */
    public DvBucketPart pinnedPart(@Nullable List<LakeSplit> files, int fileGroup, long cut) {
        return DvBucketPart.pinned(BUCKET, null, SNAPSHOT, 100L, cut, files, fileGroup, cut, 100L);
    }

    /** The masks of the standard bucket: keys 1 and 4 changed, offset 103 superseded. */
    public static GetDvSnapshotResponse masks(long snapshotId, long start, long cut) {
        GetDvSnapshotResponse response =
                new GetDvSnapshotResponse().setLogEndOffset(cut).setSnapshotStartOffset(start);
        response.addLakeDvEntry()
                .setFilePath("s3://bucket/data/f0.parquet")
                .setDeletedPositionsBitmap(bitmap(1));
        response.addLakeDvEntry().setFilePath("f1.parquet").setDeletedPositionsBitmap(bitmap(1));
        response.setLogDvBitmap(bitmap(103));
        return response;
    }

    /**
     * The no-wait answer of a leader that covers {@code snapshotId}, readable from {@code from}.
     */
    public static GetDvSnapshotResponse covered(long snapshotId, boolean behind, long from) {
        return new GetDvSnapshotResponse()
                .setCoveredSnapshotId(snapshotId)
                .setBehind(behind)
                .setSnapshotStartOffset(from);
    }

    /** The requests that pinned a cut: those that name none. */
    public List<GetDvSnapshotRequest> pins() {
        List<GetDvSnapshotRequest> pins = new ArrayList<>();
        for (GetDvSnapshotRequest request : server.requests()) {
            if (!request.hasLogEndOffset()) {
                pins.add(request);
            }
        }
        return pins;
    }

    /** The requests that released a cut. */
    public List<GetDvSnapshotRequest> releases() {
        List<GetDvSnapshotRequest> releases = new ArrayList<>();
        for (GetDvSnapshotRequest request : server.requests()) {
            if (request.hasReleaseCut() && request.isReleaseCut()) {
                releases.add(request);
            }
        }
        return releases;
    }

    /** The rows a reader returns for a part, at most {@code maxRows}, and how it resolved it. */
    public Read read(DvBucketPart part, int maxRows) throws IOException {
        List<String> rows = new ArrayList<>();
        DvBucketPart resolved;
        try (DvBucketPartReader reader = new DvBucketPartReader(part, context)) {
            resolved = reader.resolve();
            CloseableIterator<InternalRow> batch;
            while (rows.size() < maxRows
                    && (batch = reader.pollBatch(Duration.ofMillis(10))) != null) {
                while (rows.size() < maxRows && batch.hasNext()) {
                    rows.add(rowString(batch.next()));
                }
                batch.close();
            }
        }
        return new Read(rows, resolved);
    }

    /** What a reader read of a part. */
    public static final class Read {
        public final List<String> rows;
        public final DvBucketPart resolved;

        Read(List<String> rows, DvBucketPart resolved) {
            this.rows = rows;
            this.resolved = resolved;
        }
    }

    public static String rowString(InternalRow row) {
        return row.getInt(0) + "=" + row.getString(1);
    }

    public static InternalRow row(int key, String value) {
        return GenericRow.of(key, BinaryString.fromString(value));
    }

    public static byte[] bitmap(long... values) {
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        for (long value : values) {
            bitmap.addLong(value);
        }
        return serialize(bitmap);
    }

    public static byte[] serialize(Roaring64Bitmap bitmap) {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bytes)) {
            bitmap.serialize(out);
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Roaring64Bitmap deserialize(byte[] bytes) {
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        try {
            bitmap.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return bitmap;
    }

    /** The answer of a server holding {@code all}, with log pages of two offsets. */
    public static GetDvSnapshotResponse page(
            GetDvSnapshotResponse all, GetDvSnapshotRequest request) {
        GetDvSnapshotResponse page =
                new GetDvSnapshotResponse()
                        .setLogEndOffset(all.getLogEndOffset())
                        .setSnapshotStartOffset(all.getSnapshotStartOffset());
        if (all.hasLakeSnapshotIdentity()) {
            page.setLakeSnapshotIdentity(all.getLakeSnapshotIdentity());
        }
        for (String name : request.getLakeDvFileNamesList()) {
            for (int i = 0; i < all.getLakeDvEntriesCount(); i++) {
                String path = all.getLakeDvEntryAt(i).getFilePath();
                if (path.endsWith(name)) {
                    page.addLakeDvEntry()
                            .setFilePath(path)
                            .setDeletedPositionsBitmap(
                                    all.getLakeDvEntryAt(i).getDeletedPositionsBitmap());
                }
            }
        }
        if (request.hasLogDvFromOffset()) {
            long from = request.getLogDvFromOffset();
            long to = Math.min(from + 2, all.getLogEndOffset());
            Roaring64Bitmap logMask =
                    all.hasLogDvBitmap()
                            ? deserialize(all.getLogDvBitmap())
                            : new Roaring64Bitmap();
            Roaring64Bitmap range = new Roaring64Bitmap();
            logMask.forEach(
                    offset -> {
                        if (offset >= from && offset < to) {
                            range.addLong(offset);
                        }
                    });
            page.setLogDvBitmap(serialize(range)).setLogDvToOffset(to);
        }
        return page;
    }

    // ------------------------------------------------------------------------------------------

    /**
     * The bucket's leader. {@link #answer} gives all masks of the cut a request gets, and the
     * response carries the parts the request asks for. Failures and queued answers come first, in
     * order. Releases succeed, unless a release failure is queued.
     */
    public static final class MaskServer implements DvAdmin {
        public Function<GetDvSnapshotRequest, GetDvSnapshotResponse> answer;
        private final List<GetDvSnapshotRequest> requests = new ArrayList<>();
        public final Deque<Exception> failures = new ArrayDeque<>();
        public final Deque<GetDvSnapshotResponse> answers = new ArrayDeque<>();
        public final Deque<Exception> releaseFailures = new ArrayDeque<>();
        @Nullable public LakeSnapshot readable;

        @Override
        public synchronized CompletableFuture<GetDvSnapshotResponse> getDvSnapshot(
                TablePath tablePath, GetDvSnapshotRequest request) {
            requests.add(request);
            CompletableFuture<GetDvSnapshotResponse> response = new CompletableFuture<>();
            if (request.hasReleaseCut() && request.isReleaseCut()) {
                if (releaseFailures.isEmpty()) {
                    response.complete(new GetDvSnapshotResponse());
                } else {
                    response.completeExceptionally(releaseFailures.poll());
                }
            } else if (!failures.isEmpty()) {
                response.completeExceptionally(failures.poll());
            } else if (!answers.isEmpty()) {
                response.complete(answers.poll());
            } else {
                try {
                    GetDvSnapshotResponse all = answer.apply(request);
                    response.complete(
                            all.hasFoldCut() || all.hasCoveredSnapshotId()
                                    ? all
                                    : page(all, request));
                } catch (RuntimeException e) {
                    response.completeExceptionally(
                            e instanceof CompletionException && e.getCause() != null
                                    ? e.getCause()
                                    : e);
                }
            }
            return response;
        }

        @Override
        public CompletableFuture<GetDvSnapshotResponse> lookupDvPositions(
                TablePath tablePath,
                long tableId,
                @Nullable Long partitionId,
                int bucketId,
                long snapshotId,
                List<byte[]> keys) {
            throw new UnsupportedOperationException("lookupDvPositions");
        }

        /** The requests the leader got, in order. */
        public synchronized List<GetDvSnapshotRequest> requests() {
            return new ArrayList<>(requests);
        }

        public synchronized void clearRequests() {
            requests.clear();
        }
    }

    /**
     * A leader that pins cuts of the standard bucket. A request without a cut pins the high
     * watermark. Later requests get parts of the masks of a pinned cut, or a stale error once the
     * cut is gone.
     */
    public PinningServer pinningServer() {
        return new PinningServer();
    }

    /** See {@link #pinningServer()}. */
    public final class PinningServer {
        public final Set<Long> cuts = new HashSet<>();

        private PinningServer() {
            server.answer = this::answer;
        }

        /** Pins the high watermark and returns it. */
        public long pin() {
            return answer(
                            new GetDvSnapshotRequest()
                                    .setTableId(TABLE_ID)
                                    .setBucketId(0)
                                    .setReadableSnapshotId(SNAPSHOT))
                    .getLogEndOffset();
        }

        public GetDvSnapshotResponse answer(GetDvSnapshotRequest request) {
            long cut;
            if (request.hasLogEndOffset()) {
                cut = request.getLogEndOffset();
                if (!cuts.contains(cut)) {
                    throw new CompletionException(new StaleSnapshotException(SNAPSHOT, SNAPSHOT));
                }
            } else {
                cut = log.highWatermark();
                cuts.add(cut);
            }
            return masksAt(cut);
        }

        /** The masks of the standard bucket's lake files and its log up to {@code cut}. */
        public GetDvSnapshotResponse masksAt(long cut) {
            Map<Integer, Long> live = new HashMap<>();
            Set<Integer> changed = new HashSet<>();
            Roaring64Bitmap dead = new Roaring64Bitmap();
            for (ScanRecord record : log.records) {
                long offset = record.logOffset();
                if (offset < 100 || offset >= cut) {
                    continue;
                }
                ChangeType type = record.getChangeType();
                if (type == ChangeType.UPDATE_BEFORE) {
                    continue;
                }
                int key = record.getRow().getInt(0);
                changed.add(key);
                Long previous = live.remove(key);
                if (previous != null) {
                    dead.addLong(previous);
                }
                if (type != ChangeType.DELETE) {
                    live.put(key, offset);
                }
            }
            GetDvSnapshotResponse response =
                    new GetDvSnapshotResponse().setLogEndOffset(cut).setSnapshotStartOffset(100);
            for (LakeSplit split : lake.plans.get(SNAPSHOT)) {
                FileSplit file = (FileSplit) split;
                Roaring64Bitmap mask = new Roaring64Bitmap();
                for (int pos = 0; pos < file.keys.length; pos++) {
                    if (changed.contains(file.keys[pos])) {
                        mask.addLong(pos);
                    }
                }
                response.addLakeDvEntry()
                        .setFilePath(file.fileName)
                        .setDeletedPositionsBitmap(serialize(mask));
            }
            return response.setLogDvBitmap(serialize(dead));
        }
    }

    /** The reader's context over the bucket's leader, lake and log. */
    public final class Context implements DvBucketPartReader.Context {
        public final List<Long> sleeps = new ArrayList<>();
        public final List<String> keyReads = new ArrayList<>();
        // the snapshots whose lake files the reader planned
        public final List<Long> plannedSnapshots = new ArrayList<>();
        public boolean partitionDropped;
        public boolean waitForCatchUp = true;
        // waits are interrupted, as when the engine cancels the read
        public boolean interruptSleeps;
        // fails planning lake files, and reading lake keys
        @Nullable public RuntimeException planFailure;
        @Nullable public RuntimeException keyReadFailure;

        @Override
        public boolean waitForCatchUp() {
            return waitForCatchUp;
        }

        @Override
        public TablePath tablePath() {
            return TABLE_PATH;
        }

        @Override
        public DvMaskFetcher maskFetcher() {
            return new DvMaskFetcher(server, TABLE_PATH, this::sleep);
        }

        @Override
        public LakeSource<LakeSplit> lakeSource() {
            return lake;
        }

        @Nullable
        @Override
        public List<LakeSplit> planLakeSplits(
                long snapshotId, TableBucket bucket, @Nullable String partitionName) {
            plannedSnapshots.add(snapshotId);
            if (planFailure != null) {
                throw planFailure;
            }
            return lake.plans.get(snapshotId);
        }

        @Override
        public LakeSnapshot readableLakeSnapshot() {
            return server.readable;
        }

        @Nullable
        @Override
        public String snapshotIdentity(long snapshotId) {
            return lake.identities.get(snapshotId);
        }

        @Override
        public LogScanner createLogScanner() {
            return log;
        }

        @Override
        public DvBucketPartReader.KeyReader keyReader() {
            return new DvBucketPartReader.KeyReader() {
                @Override
                public CloseableIterator<RowWithPosResult> readLakeKeys(LakeSplit lakeSplit)
                        throws IOException {
                    keyReads.add(((FileSplit) lakeSplit).fileName());
                    if (keyReadFailure != null) {
                        throw keyReadFailure;
                    }
                    return RecordReaders.readWithPos(lake.createRecordReader(() -> lakeSplit));
                }

                @Override
                public LogScanner createLogScanner() {
                    keyReads.add("log");
                    return log;
                }

                @Override
                public byte[] encode(InternalRow keyRow) {
                    return Integer.toString(keyRow.getInt(0)).getBytes(StandardCharsets.UTF_8);
                }
            };
        }

        @Override
        public boolean isPartitionDropped(long partitionId) {
            return partitionDropped;
        }

        @Override
        public void sleep(long millis) throws InterruptedException {
            sleeps.add(millis);
            if (interruptSleeps) {
                throw new InterruptedException("the read is cancelled");
            }
        }
    }

    /** A lake data file of the bucket: rows of {@code keys} at their positions, of one value. */
    public static final class FileSplit implements LakeSplit, SupportsDataFiles {
        private final String fileName;
        private final int[] keys;
        private final String value;
        private final long rowCount;

        public FileSplit(String fileName, int[] keys, String value) {
            this(fileName, keys, value, -1L);
        }

        public FileSplit(String fileName, int[] keys, String value, long rowCount) {
            this.fileName = fileName;
            this.keys = keys;
            this.value = value;
            this.rowCount = rowCount;
        }

        @Override
        public int bucket() {
            return BUCKET.getBucket();
        }

        @Override
        public List<String> partition() {
            return Collections.emptyList();
        }

        public String fileName() {
            return fileName;
        }

        @Override
        public List<String> fileNames() {
            return Collections.singletonList(fileName);
        }

        @Override
        public long rowCount() {
            return rowCount;
        }

        @Override
        public String toString() {
            return fileName;
        }
    }

    /** A lake record reader that reads rows with their positions. */
    private interface PositionReader extends RecordReader, SupportsRowPositions {}

    /** A lake whose files hold rows at their positions. */
    public static final class FileLakeSource
            implements LakeSource<LakeSplit>, SupportsRewritePlanning<LakeSplit> {
        private static final long serialVersionUID = 1L;

        public final Map<Long, List<LakeSplit>> plans = new HashMap<>();
        public final Map<Long, String> identities = new HashMap<>();
        // the record readers created and closed
        public final AtomicInteger readersOpened = new AtomicInteger();
        public final AtomicInteger readersClosed = new AtomicInteger();

        public void plan(long snapshotId, LakeSplit... splits) {
            plans.put(snapshotId, Arrays.asList(splits));
        }

        @Override
        public LakeSource<LakeSplit> copy() {
            return this;
        }

        @Override
        public void withProject(int[][] project) {}

        @Override
        public void withLimit(int limit) {}

        @Override
        public FilterPushDownResult withFilters(List<Predicate> predicates) {
            return FilterPushDownResult.of(Collections.emptyList(), predicates);
        }

        @Override
        public Planner<LakeSplit> createPlanner(PlannerContext context) {
            return () -> plans.getOrDefault(context.snapshotId(), Collections.emptyList());
        }

        @Override
        public RecordReader createRecordReader(ReaderContext<LakeSplit> context) {
            FileSplit split = (FileSplit) context.lakeSplit();
            readersOpened.incrementAndGet();
            return new PositionReader() {
                @Override
                public void close() {
                    readersClosed.incrementAndGet();
                }

                @Override
                public CloseableIterator<LogRecord> read() {
                    List<LogRecord> records = new ArrayList<>();
                    for (int key : split.keys) {
                        records.add(
                                new GenericRecord(
                                        -1, -1, ChangeType.INSERT, row(key, split.value)));
                    }
                    return CloseableIterator.wrap(records.iterator());
                }

                @Override
                public CloseableIterator<RowWithPosResult> readWithPos() {
                    return new CloseableIterator<RowWithPosResult>() {
                        int pos = 0;

                        @Override
                        public boolean hasNext() {
                            return pos < split.keys.length;
                        }

                        @Override
                        public RowWithPosResult next() {
                            RowWithPosResult result =
                                    new RowWithPosResult()
                                            .set(
                                                    row(split.keys[pos], split.value),
                                                    pos,
                                                    split.fileName);
                            pos++;
                            return result;
                        }

                        @Override
                        public void close() {}
                    };
                }
            };
        }

        @Override
        public SimpleVersionedSerializer<LakeSplit> getSplitSerializer() {
            return new SimpleVersionedSerializer<LakeSplit>() {
                @Override
                public int getVersion() {
                    return 0;
                }

                @Override
                public byte[] serialize(LakeSplit split) {
                    return ((FileSplit) split).fileName.getBytes(StandardCharsets.UTF_8);
                }

                @Override
                public LakeSplit deserialize(int version, byte[] serialized) {
                    String fileName = new String(serialized, StandardCharsets.UTF_8);
                    for (List<LakeSplit> splits : plans.values()) {
                        for (LakeSplit split : splits) {
                            if (((FileSplit) split).fileName.equals(fileName)) {
                                return split;
                            }
                        }
                    }
                    throw new IllegalArgumentException(fileName);
                }
            };
        }

        @Nullable
        @Override
        public String snapshotIdentity(long snapshotId) {
            return identities.get(snapshotId);
        }

        @Nullable
        @Override
        public DataDeltaPlan<LakeSplit> planRewrite(
                long baseSnapshotId, @Nullable Long targetSnapshotId) {
            throw new UnsupportedOperationException("union reads plan no rewrites");
        }
    }

    /** The bucket's log, polled two records at a time. */
    public static final class FakeLogScanner implements LogScanner {
        public final List<ScanRecord> records = new ArrayList<>();
        public TableBucket bucket = BUCKET;
        @Nullable public Long subscribedFrom;
        // the partition is gone: polls return nothing, as a real scanner's do
        public boolean dropped;
        public boolean subscribeFails;
        private long position = -1;

        public long highWatermark() {
            return records.isEmpty() ? 0 : records.get(records.size() - 1).logOffset() + 1;
        }

        public void append(long offset, ChangeType changeType, int key, String value) {
            records.add(new ScanRecord(offset, offset * 10, changeType, row(key, value)));
        }

        @Override
        public ScanRecords poll(Duration timeout) {
            if (dropped || position < 0) {
                return ScanRecords.EMPTY;
            }
            List<ScanRecord> batch = new ArrayList<>();
            for (ScanRecord record : records) {
                if (record.logOffset() >= position && batch.size() < 2) {
                    batch.add(record);
                }
            }
            if (batch.isEmpty()) {
                return ScanRecords.EMPTY;
            }
            position = batch.get(batch.size() - 1).logOffset() + 1;
            return new ScanRecords(
                    Collections.singletonMap(bucket, batch),
                    Collections.singletonMap(bucket, position));
        }

        @Override
        public void subscribe(int bucket, long offset) {
            if (subscribeFails) {
                throw new PartitionNotExistException("dropped");
            }
            if (offset < 0 && offset != LogScanner.EARLIEST_OFFSET) {
                // as a tablet server answers a fetch below the log start
                throw new LogOffsetOutOfRangeException("Log offset " + offset);
            }
            subscribedFrom = offset;
            position = Math.max(0L, offset);
        }

        @Override
        public void subscribe(long partitionId, int bucket, long offset) {
            subscribe(bucket, offset);
        }

        @Override
        public void unsubscribe(long partitionId, int bucket) {}

        @Override
        public void unsubscribe(int bucket) {}

        @Override
        public void wakeup() {}

        @Override
        public void close() {
            position = -1;
        }
    }
}
