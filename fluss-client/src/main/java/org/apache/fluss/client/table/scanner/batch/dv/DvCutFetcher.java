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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketPartReader.ReadStats;
import org.apache.fluss.exception.LakeTableRolledBackException;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.GetDvSnapshotRequest;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;

import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

/**
 * Sends a part's requests to its bucket's leader: the pin of a cut, the fetch of the cut's masks in
 * parts, which it checks are all of one cut and one start, and the check of the lake snapshot's
 * identity. Counts their time and requests in the part's {@link ReadStats}.
 */
@Internal
final class DvCutFetcher {

    private final DvBucketPart part;
    private final TableBucket bucket;
    private final DvBucketPartReader.Context context;
    private final ReadStats stats;

    DvCutFetcher(DvBucketPart part, DvBucketPartReader.Context context, ReadStats stats) {
        this.part = part;
        this.bucket = part.getTableBucket();
        this.context = context;
        this.stats = stats;
    }

    /**
     * Pins a cut of lake snapshot {@code snapshotId} for the part. The answer names the cut and the
     * offset its masks start at, and carries the masks of the first of {@code lakeSplits}.
     */
    GetDvSnapshotResponse pin(long snapshotId, @Nullable List<LakeSplit> lakeSplits)
            throws Exception {
        List<String> firstFiles = fileNamesOf(lakeSplits, 0);
        // the same in every retry of this pin, so a retry the leader took pins nothing more
        long pinRequestId = ThreadLocalRandom.current().nextLong();
        return fetchMasks(
                () -> request(snapshotId, null, firstFiles).setPinRequestId(pinRequestId));
    }

    /**
     * Fetches the masks of cut {@code cut}: those of the lake files of {@code lakeSplits} and, if
     * {@code readsLog}, the log mask in pages. {@code pinned} is the leader's answer to the part's
     * own pin of the cut, which carries the masks of the first lake split; they are not fetched
     * again.
     *
     * <p>Every answer names the offset the masks start at, which is where the log is read from. The
     * fetch fails if the leader answers for another cut, or names another start than the one it
     * named when the cut was pinned: that of {@code pinned}, or else {@code logStart}.
     */
    DvBucketMaskState fetchCut(
            long snapshotId,
            long cut,
            @Nullable List<LakeSplit> lakeSplits,
            @Nullable GetDvSnapshotResponse pinned,
            long logStart,
            boolean readsLog)
            throws Exception {
        CutAnswers answers =
                new CutAnswers(cut, pinned == null ? logStart : startOffsetOf(pinned, cut));
        Map<String, byte[]> lakeMasks = new HashMap<>();
        int fromLakeSplit = 0;
        if (pinned != null) {
            answers.check(pinned);
            lakeMasks.putAll(DvBucketMasks.lakeMasksOf(pinned));
            fromLakeSplit = 1;
        }
        List<String> fileNames = new ArrayList<>();
        int lakeSplitCount = lakeSplits == null ? 0 : lakeSplits.size();
        for (int i = 0; i < lakeSplitCount; i++) {
            LakeSplit lakeSplit = lakeSplits.get(i);
            List<String> names = DvLakeCalls.fileNames(lakeSplit);
            if (names == null) {
                throw new IOException(
                        String.format(
                                "Lake split %s of %s names no data files, by which its masks"
                                        + " are keyed.",
                                lakeSplit, part.describe()));
            }
            if (i >= fromLakeSplit) {
                fileNames.addAll(names);
            }
        }
        if (!fileNames.isEmpty()) {
            GetDvSnapshotResponse response = fetchMasks(() -> request(snapshotId, cut, fileNames));
            answers.check(response);
            lakeMasks.putAll(DvBucketMasks.lakeMasksOf(response));
        }
        byte[] logMask = null;
        if (readsLog && answers.logStart < cut) {
            Roaring64Bitmap log = new Roaring64Bitmap();
            // the leader pages the log from offset 0 at the earliest
            for (long from = Math.max(0L, answers.logStart); from < cut; ) {
                long pageFrom = from;
                GetDvSnapshotResponse page =
                        fetchMasks(
                                () -> request(snapshotId, cut, null).setLogDvFromOffset(pageFrom));
                answers.check(page);
                long to = page.hasLogDvToOffset() ? page.getLogDvToOffset() : cut;
                if (to <= from) {
                    throw new IOException(
                            String.format(
                                    "The log mask page of %s from offset %d ends at %d.",
                                    part.describe(), from, to));
                }
                if (page.hasLogDvBitmap()) {
                    log.or(DvBucketMasks.deserialize(page.getLogDvBitmap(), "the log"));
                }
                from = to;
            }
            logMask = DvBucketMasks.serialize(log);
        }
        return new DvBucketMaskState(answers.logStart, cut, answers.identity, lakeMasks, logMask);
    }

    /** The offset the masks of an answer for cut {@code cut} start at. */
    private long startOffsetOf(GetDvSnapshotResponse response, long cut) throws IOException {
        if (!response.hasSnapshotStartOffset()) {
            throw new IOException(
                    String.format(
                            "The leader answered the masks of %s up to log offset %d without the"
                                    + " offset they start at.",
                            part.describe(), cut));
        }
        return response.getSnapshotStartOffset();
    }

    /**
     * The leader's answers for one cut, which must name the cut and the offset its masks start at.
     */
    private final class CutAnswers {
        private final long cut;
        private final long expectedLogStart;
        private long logStart;
        @Nullable private String identity;

        CutAnswers(long cut, long expectedLogStart) {
            this.cut = cut;
            this.expectedLogStart = expectedLogStart;
            this.logStart = expectedLogStart;
        }

        void check(GetDvSnapshotResponse response) throws IOException {
            if (!response.hasLogEndOffset() || response.getLogEndOffset() != cut) {
                throw new IOException(
                        String.format(
                                "The leader answered masks up to log offset %s instead of those"
                                        + " of the pinned cut %d.",
                                response.hasLogEndOffset() ? response.getLogEndOffset() : "none",
                                cut));
            }
            long start = startOffsetOf(response, cut);
            // an empty lake's masks start at the log's first offset, named 0 or -1
            if (Math.max(0L, start) != Math.max(0L, expectedLogStart)) {
                throw new IOException(
                        String.format(
                                "The leader answered masks of %s up to log offset %d that start at"
                                        + " log offset %d, not at %d as when the cut was pinned.",
                                part.describe(), cut, start, expectedLogStart));
            }
            logStart = start;
            if (identity == null && response.hasLakeSnapshotIdentity()) {
                identity = response.getLakeSnapshotIdentity();
            }
        }
    }

    /**
     * Fails the read if the masks were derived for another commit than the one snapshot {@code
     * snapshotId} names now. A lake that reuses snapshot ids after a rollback (Paimon) may hold a
     * new commit under the readable snapshot's id. The registered snapshot's masks would hide and
     * resurrect the wrong rows of it.
     */
    void checkSnapshotIdentity(long snapshotId, GetDvSnapshotResponse response) throws IOException {
        if (response.hasLakeSnapshotIdentity()) {
            checkSnapshotIdentity(snapshotId, response.getLakeSnapshotIdentity());
        }
    }

    void checkSnapshotIdentity(long snapshotId, String registered) throws IOException {
        long start = System.nanoTime();
        String current;
        try {
            current = context.snapshotIdentity(snapshotId);
        } finally {
            stats.identityNanos += System.nanoTime() - start;
        }
        if (!registered.equals(current)) {
            throw LakeTableRolledBackException.ofReusedReadableSnapshot(
                    context.tablePath().toString(),
                    snapshotId,
                    registered,
                    current == null ? "unknown commit" : current);
        }
    }

    /**
     * Fetches masks from the bucket's leader, counting the time, including retries and the leader's
     * waits, and the requests sent.
     */
    private GetDvSnapshotResponse fetchMasks(Supplier<GetDvSnapshotRequest> request)
            throws Exception {
        long start = System.nanoTime();
        stats.maskFetches++;
        try {
            return context.maskFetcher()
                    .fetch(
                            () -> {
                                stats.maskRpcs++;
                                return request.get();
                            });
        } finally {
            stats.maskFetchNanos += System.nanoTime() - start;
        }
    }

    private GetDvSnapshotRequest request(
            long snapshotId, @Nullable Long cut, @Nullable List<String> fileNames) {
        GetDvSnapshotRequest request =
                new GetDvSnapshotRequest()
                        .setTableId(bucket.getTableId())
                        .setBucketId(bucket.getBucket())
                        .setReadableSnapshotId(snapshotId);
        if (bucket.getPartitionId() != null) {
            request.setPartitionId(bucket.getPartitionId());
        }
        if (!context.waitForCatchUp()) {
            request.setWaitForCatchUp(false);
        }
        if (cut != null) {
            request.setLogEndOffset(cut);
        }
        if (fileNames != null) {
            for (String fileName : fileNames) {
                request.addLakeDvFileName(fileName);
            }
        }
        return request;
    }

    @Nullable
    private static List<String> fileNamesOf(@Nullable List<LakeSplit> lakeSplits, int index) {
        if (lakeSplits == null || index >= lakeSplits.size()) {
            return Collections.emptyList();
        }
        return DvLakeCalls.fileNames(lakeSplits.get(index));
    }
}
