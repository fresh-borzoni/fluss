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

package org.apache.fluss.flink.tiering.committer;

import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.tiering.DvRoundRequests;
import org.apache.fluss.client.tiering.RoundDeletes;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.index.FlussPositionIndexStore;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.lake.dv.index.StagedIndexPart;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/**
 * Tests the deleted positions a round with deletion vectors collects from the buckets' leaders
 * while it publishes the writers' staged parts.
 */
class DerivedDvStagedPartsTest {

    private static final long BASE = 7L;
    private static final long BASE_OFFSET = 100;
    private static final long LOG_LENGTH = 300;
    private static final long EARLY_END = 180;
    private static final int KEYS = 200;
    private static final int PART_CHANGES = 16;
    // the lake snapshots before the base changed no data file
    private static final PublishedParts.ChangedDataFiles NOTHING_CHANGED =
            snapshotId -> Collections.emptySet();
    // a new index change for every publish
    private static final AtomicLong INDEX_IDS = new AtomicLong(90);

    @TempDir Path tempDir;

    @Test
    void testDeletedPositionsAreArraysOnlyWhenRead() throws IOException {
        DeletedPositions deleted = new DeletedPositions();
        deleted.addSerialized("a.parquet", serialize(bitmap(7, 3)));
        deleted.addSerialized("a.parquet", serialize(bitmap(7)));
        deleted.addSerialized("b.parquet", serialize(bitmap(1L << 33)));
        DeletedPositions other = new DeletedPositions();
        other.addSerialized("a.parquet", serialize(bitmap(5)));
        other.addSerialized("c.parquet", serialize(bitmap(0)));
        deleted.addAll(other);

        assertThat(deleted.positions()).isEqualTo(5);
        assertThat(deleted).hasSize(3).containsKeys("a.parquet", "b.parquet", "c.parquet");
        assertThat(deleted.containsKey("d.parquet")).isFalse();
        assertThat(deleted.get("a.parquet")).containsExactly(3, 5, 7);
        Map<String, long[]> read = new HashMap<>();
        for (Map.Entry<String, long[]> e : deleted.entrySet()) {
            read.put(e.getKey(), e.getValue());
        }
        assertThat(read.get("b.parquet")).containsExactly(1L << 33);
        assertThat(read.get("c.parquet")).containsExactly(0L);
    }

    /**
     * The deletes a round takes from the leaders are, per data file, the base rows of the keys each
     * bucket changed from the base's offset up to its round's end. Each leader derives them in
     * parts: one bucket's round ends below the changes its leader holds. A bucket not in the base
     * asks nothing. Every bucket's part is published, with file ids from the table's counter.
     */
    @Test
    void testRoundDeletesAreTheBaseRowsOfTheChangedKeys() throws Exception {
        PositionIndexStore store = store();
        TableBucket full = new TableBucket(1L, 0);
        TableBucket early = new TableBucket(1L, 1);
        TableBucket fresh = new TableBucket(1L, 2);
        Round round = new Round(store, 6L, Arrays.asList(full, early, fresh));
        round.roundEnds.put(early, EARLY_END);
        round.inBase.remove(fresh);
        round.stage();

        List<String> requests = Collections.synchronizedList(new ArrayList<>());
        PublishedParts published =
                round.publish(
                        (bucket, snapshotId, from, end) -> {
                            assertThat(snapshotId).isEqualTo(BASE);
                            requests.add(bucket + ":" + from + "-" + end);
                            return round.leaderPart(bucket, from, end);
                        });

        Map<String, List<Long>> expected = new TreeMap<>();
        for (TableBucket bucket : Arrays.asList(full, early)) {
            for (int key : round.changedKeys(bucket)) {
                if (inBase(key)) {
                    expected.computeIfAbsent(baseFile(bucket, key), f -> new ArrayList<>())
                            .add((long) key);
                }
            }
        }
        assertThat(positionsOf(published.deleted)).isNotEmpty().isEqualTo(expected);
        assertThat(published.deletedRows)
                .isEqualTo(expected.values().stream().mapToLong(List::size).sum());
        // the early bucket's deletes go up to its round's end, the others' to theirs, in parts
        assertThat(requests)
                .filteredOn(r -> r.startsWith(early + ":"))
                .allMatch(r -> r.endsWith("-" + EARLY_END))
                .hasSizeGreaterThan(1);
        assertThat(requests).filteredOn(r -> r.startsWith(full + ":")).hasSizeGreaterThan(1);
        assertThat(requests).noneMatch(r -> r.startsWith(fresh + ":"));
        assertThat(published.keys).isEqualTo(round.keys);
        assertThat(published.changes).hasSize(3);
        Set<Integer> fileIds = new TreeSet<>();
        for (PositionIndexStore.BucketChange change : published.changes.values()) {
            assertThat(change.part.files).hasSize(1);
            assertThat(change.addedFiles).hasSize(1);
            fileIds.addAll(change.addedFiles.keySet());
        }
        assertThat(fileIds).hasSize(3);
    }

    /** A bucket's deletes are fetched while its part is published, not after. */
    @Test
    void testDeletesAreFetchedWhileThePartIsPublished() throws Exception {
        TableBucket bucket = new TableBucket(1L, 0);
        CountDownLatch fetching = new CountDownLatch(1);
        PositionIndexStore real = store();
        PositionIndexStore store =
                (PositionIndexStore)
                        Proxy.newProxyInstance(
                                PositionIndexStore.class.getClassLoader(),
                                new Class<?>[] {PositionIndexStore.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("publishStaged")) {
                                        assertThat(fetching.await(30, TimeUnit.SECONDS))
                                                .as("the fetch started")
                                                .isTrue();
                                    }
                                    try {
                                        return method.invoke(real, args);
                                    } catch (InvocationTargetException e) {
                                        throw e.getCause();
                                    }
                                });
        Round round = new Round(store, 7L, Collections.singletonList(bucket));
        round.stage();

        PublishedParts published =
                round.publish(
                        (b, snapshotId, from, end) -> {
                            fetching.countDown();
                            return round.leaderPart(b, from, end);
                        });

        assertThat(published.deletedRows).isPositive();
        assertThat(published.changes).containsOnlyKeys(bucket);
    }

    /**
     * A round fails, before it publishes anything, if a bucket's round does not start at the base's
     * offset, as the leader's deletes would miss the keys changed in between, or has no end.
     */
    @Test
    void testARoundThatDoesNotStartAtTheBaseFails() throws Exception {
        TableBucket bucket = new TableBucket(1L, 0);
        Round round = new Round(store(), 8L, Collections.singletonList(bucket));
        round.stage();
        List<String> requests = Collections.synchronizedList(new ArrayList<>());
        DvRoundRequests.Fetcher fetcher =
                (b, snapshotId, from, end) -> {
                    requests.add(b + ":" + from);
                    return round.leaderPart(b, from, end);
                };

        round.roundStarts.put(bucket, BASE_OFFSET - 10);
        assertThatThrownBy(() -> round.publish(fetcher))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("starts at offset " + (BASE_OFFSET - 10));
        round.roundStarts.remove(bucket);
        assertThatThrownBy(() -> round.publish(fetcher)).isInstanceOf(IllegalStateException.class);
        round.roundStarts.put(bucket, BASE_OFFSET);
        round.roundEnds.remove(bucket);
        assertThatThrownBy(() -> round.publish(fetcher))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no end offset");
        assertThat(requests).isEmpty();
    }

    /**
     * A leader that reads the base from another offset, a part that does not advance, and a part
     * whose next one would start past the round's end each fail the round.
     */
    @Test
    void testALeaderAnswerOutsideTheRoundFailsIt() throws Exception {
        TableBucket bucket = new TableBucket(1L, 0);
        long end = BASE_OFFSET + LOG_LENGTH;
        Round round = new Round(store(), 9L, Collections.singletonList(bucket));
        round.stage();

        assertThatThrownBy(
                        () ->
                                round.publish(
                                        (b, snapshotId, from, e) ->
                                                round.leaderPart(b, from, e)
                                                        .setSnapshotStartOffset(BASE_OFFSET - 1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("from offset " + (BASE_OFFSET - 1));
        assertThatThrownBy(
                        () ->
                                round.publish(
                                        (b, snapshotId, from, e) ->
                                                round.leaderPart(b, from, e)
                                                        .setRoundNextOffset(
                                                                from == null
                                                                        ? BASE_OFFSET + 5
                                                                        : from)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("next part at " + (BASE_OFFSET + 5));
        assertThatThrownBy(
                        () ->
                                round.publish(
                                        (b, snapshotId, from, e) ->
                                                round.leaderPart(b, from, e)
                                                        .setRoundNextOffset(e + 1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("next part at " + (end + 1));
    }

    /**
     * Deletes the readers fetched for the buckets' rounds on the base are used without a fetch, and
     * the round deletes the same positions as with the leaders' deletes.
     */
    @Test
    void testPrefetchedDeletesOfTheRoundAreUsedWithoutFetching() throws Exception {
        List<TableBucket> buckets = buckets(3);
        PublishedParts expected = fetchedFromTheLeaders(11L, buckets);
        Round round = round("prefetched", 11L, buckets);

        PublishedParts published =
                round.publish(
                        round.prefetched(),
                        (b, snapshotId, from, end) -> {
                            throw new AssertionError("fetched the deletes of " + b);
                        });

        assertThat(positionsOf(published.deleted))
                .isNotEmpty()
                .isEqualTo(positionsOf(expected.deleted));
        assertThat(published.deletedRows).isEqualTo(expected.deletedRows);
        assertThat(published.deletesFromReaders).isEqualTo(3);
        assertThat(published.deletesFromLeaders).isZero();
        assertThat(expected.deletesFromReaders).isZero();
        assertThat(expected.deletesFromLeaders).isEqualTo(3);
    }

    /**
     * Deletes a reader fetched for another round than the one on the committer's base are fetched
     * again from the leader: of other offsets, or on a snapshot before the base after which a file
     * they name changed. The round deletes what the leaders' deletes name.
     */
    @ParameterizedTest
    @ValueSource(strings = {"snapshot", "start", "end"})
    void testPrefetchedDeletesOfAnotherRoundAreFetchedAgain(String differs) throws Exception {
        List<TableBucket> buckets = buckets(3);
        PublishedParts expected = fetchedFromTheLeaders(12L, buckets);
        Round round = round("stale-" + differs, 12L, buckets);
        TableBucket stale = buckets.get(0);
        Map<TableBucket, RoundDeletes> prefetched = round.prefetched();
        long end = round.roundEnds.get(stale);
        prefetched.put(
                stale,
                new RoundDeletes(
                        differs.equals("snapshot") ? BASE - 1 : BASE,
                        differs.equals("start") ? BASE_OFFSET - 1 : BASE_OFFSET,
                        differs.equals("end") ? end - 1 : end,
                        Collections.singletonList(
                                new RoundDeletes.File("stale.parquet", serialize(bitmap(1))))));
        Set<TableBucket> fetched = Collections.synchronizedSet(new HashSet<>());

        PublishedParts published =
                round.publish(
                        BASE,
                        prefetched,
                        snapshotId ->
                                snapshotId == BASE - 1
                                        ? Collections.singleton("stale.parquet")
                                        : Collections.emptySet(),
                        Collections.emptySet(),
                        (b, snapshotId, from, e) -> {
                            fetched.add(b);
                            return round.leaderPart(b, from, e);
                        });

        assertThat(fetched).containsExactly(stale);
        assertThat(positionsOf(published.deleted)).isEqualTo(positionsOf(expected.deleted));
        assertThat(published.deletesFromReaders).isEqualTo(2);
        assertThat(published.deletesFromLeaders).isEqualTo(1);
    }

    /**
     * Of a round's buckets, those with deletes from their readers use them, the others fetch
     * theirs, and a bucket not in the base deletes nothing whatever its reader handed over.
     */
    @Test
    void testPrefetchedAndFetchedDeletesMix() throws Exception {
        List<TableBucket> buckets = buckets(4);
        TableBucket fresh = buckets.get(3);
        TableBucket unfetched = buckets.get(2);
        Round expectedRound = round("mixed-fetched", 13L, buckets);
        expectedRound.inBase.remove(fresh);
        PublishedParts expected =
                expectedRound.publish(
                        (b, snapshotId, from, end) -> expectedRound.leaderPart(b, from, end));
        Round round = round("mixed", 13L, buckets);
        round.inBase.remove(fresh);
        Map<TableBucket, RoundDeletes> prefetched = round.prefetched();
        prefetched.remove(unfetched);
        prefetched.put(
                fresh,
                new RoundDeletes(
                        BASE,
                        BASE_OFFSET,
                        round.roundEnds.get(fresh),
                        Collections.singletonList(
                                new RoundDeletes.File("fresh.parquet", serialize(bitmap(1))))));
        Set<TableBucket> fetched = Collections.synchronizedSet(new HashSet<>());

        PublishedParts published =
                round.publish(
                        prefetched,
                        (b, snapshotId, from, e) -> {
                            fetched.add(b);
                            return round.leaderPart(b, from, e);
                        });

        assertThat(fetched).containsExactly(unfetched);
        assertThat(positionsOf(published.deleted))
                .doesNotContainKey("fresh.parquet")
                .isEqualTo(positionsOf(expected.deleted));
        assertThat(published.deletesFromReaders).isEqualTo(2);
        assertThat(published.deletesFromLeaders).isEqualTo(1);
    }

    /**
     * The round runs on a snapshot after the one its readers fetched the deletes on, e.g. on its
     * own compaction or on another engine's rewrite. The deletes are kept for the buckets whose
     * named data files the snapshots since left alone, and fetched again on the new base for a
     * bucket whose deletes name a changed file. All are fetched again if the changes are not known.
     */
    @Test
    void testDeletesNamingNoFileChangedSinceAreKept() throws Exception {
        List<TableBucket> buckets = buckets(3);
        PublishedParts expected = fetchedFromTheLeaders(14L, buckets);
        long rewrite = BASE + 100;
        Round round = round("rewrite", 14L, buckets);
        Map<TableBucket, RoundDeletes> prefetched = round.prefetched();
        TableBucket rewritten = buckets.get(0);
        // a file the round deletes rows in, and one of another bucket it does not
        Set<String> changed = new HashSet<>();
        changed.add(prefetched.get(rewritten).files().get(0).path());
        changed.add("old-" + buckets.size() + "-0");
        Map<TableBucket, Long> fetchedOn = new ConcurrentHashMap<>();
        Map<Long, Integer> asked = new ConcurrentHashMap<>();

        PublishedParts published =
                round.publish(
                        rewrite,
                        prefetched,
                        snapshotId -> {
                            asked.merge(snapshotId, 1, Integer::sum);
                            return changed;
                        },
                        Collections.emptySet(),
                        (b, snapshotId, from, e) -> {
                            fetchedOn.put(b, snapshotId);
                            return round.leaderPart(b, from, e);
                        });

        assertThat(fetchedOn).containsOnly(entry(rewritten, rewrite));
        assertThat(asked).as("asked once per snapshot").containsOnly(entry(BASE, 1));
        assertThat(positionsOf(published.deleted)).isEqualTo(positionsOf(expected.deleted));
        assertThat(published.deletesFromReaders).isEqualTo(2);
        assertThat(published.deletesFromLeaders).isEqualTo(1);

        Round unknown = round("rewrite-unknown", 14L, buckets);
        fetchedOn.clear();
        unknown.publish(
                rewrite,
                unknown.prefetched(),
                snapshotId -> null,
                Collections.emptySet(),
                (b, snapshotId, from, e) -> {
                    fetchedOn.put(b, snapshotId);
                    return unknown.leaderPart(b, from, e);
                });
        assertThat(fetchedOn).hasSize(3);
    }

    /**
     * A bucket whose writer prepared its deletion vectors deletes nothing at the committer and
     * fetches nothing. Taken at the committer after all, e.g. as its prepared deletion vectors
     * clash with another bucket's, it deletes what its leader's deletes name.
     */
    @Test
    void testBucketsWithPreparedDeletionVectorsDeleteNothingHere() throws Exception {
        List<TableBucket> buckets = buckets(3);
        PublishedParts expected = fetchedFromTheLeaders(15L, buckets);
        Round round = round("prepared", 15L, buckets);
        TableBucket prepared = buckets.get(0);
        Set<TableBucket> fetched = Collections.synchronizedSet(new HashSet<>());
        DvRoundRequests.Fetcher fetcher =
                (b, snapshotId, from, e) -> {
                    fetched.add(b);
                    return round.leaderPart(b, from, e);
                };

        PublishedParts published =
                round.publish(
                        BASE,
                        round.prefetched(),
                        NOTHING_CHANGED,
                        Collections.singleton(prepared),
                        fetcher);

        assertThat(fetched).isEmpty();
        assertThat(positionsOf(published.deleted).keySet())
                .noneMatch(file -> file.startsWith("old-" + prepared.getBucket() + "-"))
                .isNotEmpty();
        assertThat(published.vectorsFromWriters).isEqualTo(1);
        assertThat(published.deletesFromReaders).isEqualTo(2);
        assertThat(published.deletesFromLeaders).isZero();

        published.addCentrally(prepared, BASE, BASE_OFFSET, round.roundEnds.get(prepared), fetcher);

        assertThat(fetched).containsExactly(prepared);
        assertThat(positionsOf(published.deleted)).isEqualTo(positionsOf(expected.deleted));
        assertThat(published.deletedRows).isEqualTo(expected.deletedRows);
        assertThat(published.vectorsFromWriters).isZero();
        assertThat(published.deletesFromLeaders).isEqualTo(1);
    }

    private static List<TableBucket> buckets(int count) {
        List<TableBucket> buckets = new ArrayList<>();
        for (int bucket = 0; bucket < count; bucket++) {
            buckets.add(new TableBucket(1L, bucket));
        }
        return buckets;
    }

    /** A staged round of the buckets, the second of which ends early, in a store of its own. */
    private Round round(String name, long seed, List<TableBucket> buckets) throws IOException {
        Round round =
                new Round(
                        new FlussPositionIndexStore(new FsPath(tempDir.resolve(name).toUri())),
                        seed,
                        buckets);
        round.roundEnds.put(buckets.get(1), EARLY_END);
        round.stage();
        return round;
    }

    /** Publishes the round with every bucket's deletes from its leader. */
    private PublishedParts fetchedFromTheLeaders(long seed, List<TableBucket> buckets)
            throws Exception {
        Round round = round("fetched-" + seed, seed, buckets);
        return round.publish((b, snapshotId, from, end) -> round.leaderPart(b, from, end));
    }

    private PositionIndexStore store() {
        return new FlussPositionIndexStore(new FsPath(tempDir.resolve("lake").toUri()));
    }

    /**
     * The buckets of a round on base {@link #BASE}: each leader holds random changes from {@link
     * #BASE_OFFSET} on, also past the round's end, and each writer staged the keys its round
     * changed.
     */
    private static final class Round {
        final PositionIndexStore store;
        final long stagingId;
        final List<TableBucket> buckets;
        final Map<TableBucket, TreeMap<Long, Integer>> logs = new HashMap<>();
        final Map<TableBucket, Long> inBase = new HashMap<>();
        final Map<TableBucket, Long> roundStarts = new HashMap<>();
        final Map<TableBucket, Long> roundEnds = new HashMap<>();
        final Map<TableBucket, StagedIndexPart> parts = new HashMap<>();
        long keys;

        Round(PositionIndexStore store, long round, List<TableBucket> buckets) {
            this.store = store;
            this.stagingId = StagedIndexPart.stagingIdOf(1L, round);
            this.buckets = buckets;
            Random random = new Random(round);
            for (TableBucket bucket : buckets) {
                TreeMap<Long, Integer> log = new TreeMap<>();
                for (long offset = BASE_OFFSET; offset < BASE_OFFSET + LOG_LENGTH; offset++) {
                    if (random.nextInt(3) > 0) {
                        log.put(offset, random.nextInt(KEYS));
                    }
                }
                logs.put(bucket, log);
                inBase.put(bucket, BASE_OFFSET);
                roundStarts.put(bucket, BASE_OFFSET);
                roundEnds.put(bucket, BASE_OFFSET + LOG_LENGTH);
            }
        }

        /** The keys the bucket's round changed. */
        Set<Integer> changedKeys(TableBucket bucket) {
            return new TreeSet<>(logs.get(bucket).headMap(roundEnds.get(bucket)).values());
        }

        /** Stages each bucket's part: a fifth of its keys deleted, the others at new rows. */
        void stage() throws IOException {
            for (TableBucket bucket : buckets) {
                Set<Integer> changed = changedKeys(bucket);
                parts.put(bucket, stagePart(store, stagingId, bucket, changed));
                keys += changed.size();
            }
        }

        PublishedParts publish(DvRoundRequests.Fetcher fetcher) throws Exception {
            return publish(Collections.emptyMap(), fetcher);
        }

        PublishedParts publish(
                Map<TableBucket, RoundDeletes> prefetched, DvRoundRequests.Fetcher fetcher)
                throws Exception {
            return publish(BASE, prefetched, NOTHING_CHANGED, Collections.emptySet(), fetcher);
        }

        PublishedParts publish(
                long snapshot,
                Map<TableBucket, RoundDeletes> prefetched,
                PublishedParts.ChangedDataFiles changedDataFiles,
                Set<TableBucket> preparedByWriters,
                DvRoundRequests.Fetcher fetcher)
                throws Exception {
            return PublishedParts.publish(
                    store,
                    INDEX_IDS.getAndIncrement(),
                    new LakeSnapshot(snapshot, inBase),
                    parts,
                    roundStarts,
                    roundEnds,
                    prefetched,
                    changedDataFiles,
                    preparedByWriters,
                    fetcher,
                    PublishedParts.publishThreads(new Configuration()));
        }

        /** The deletes of each bucket's round in the base, as its reader fetches them. */
        Map<TableBucket, RoundDeletes> prefetched() throws Exception {
            Map<TableBucket, RoundDeletes> prefetched = new HashMap<>();
            for (TableBucket bucket : buckets) {
                if (inBase.containsKey(bucket)) {
                    prefetched.put(
                            bucket,
                            RoundDeletes.fetch(
                                    bucket,
                                    BASE,
                                    BASE_OFFSET,
                                    roundEnds.get(bucket),
                                    Long.MAX_VALUE,
                                    (b, snapshotId, from, end) -> leaderPart(b, from, end)));
                }
            }
            return prefetched;
        }

        /**
         * A leader's part of a round's deletes, as a tablet server derives it: the base rows of the
         * keys changed in [from, end), {@link #PART_CHANGES} changes a part.
         */
        GetDvSnapshotResponse leaderPart(TableBucket bucket, @Nullable Long from, long end)
                throws IOException {
            Map<String, Roaring64Bitmap> files = new TreeMap<>();
            long next = end;
            int read = 0;
            for (Map.Entry<Long, Integer> c :
                    logs.get(bucket).subMap(from == null ? BASE_OFFSET : from, end).entrySet()) {
                if (read == PART_CHANGES) {
                    next = c.getKey();
                    break;
                }
                read++;
                int key = c.getValue();
                if (inBase(key)) {
                    files.computeIfAbsent(baseFile(bucket, key), f -> new Roaring64Bitmap())
                            .addLong(key);
                }
            }
            GetDvSnapshotResponse response = new GetDvSnapshotResponse();
            for (Map.Entry<String, Roaring64Bitmap> e : files.entrySet()) {
                response.addLakeDvEntry()
                        .setFilePath(e.getKey())
                        .setDeletedPositionsBitmap(serialize(e.getValue()));
            }
            return response.setSnapshotStartOffset(BASE_OFFSET).setRoundNextOffset(next);
        }
    }

    private static StagedIndexPart stagePart(
            PositionIndexStore store,
            long stagingId,
            TableBucket bucket,
            Collection<Integer> changedKeys)
            throws IOException {
        int deletes = 0;
        try (PositionIndexStore.StagedPartWriter writer =
                store.stageBucket(stagingId, bucket, "w_", null)) {
            int position = 0;
            for (int k : new TreeSet<>(changedKeys)) {
                if (k % 5 == 0) {
                    writer.delete(key(k));
                    deletes++;
                } else {
                    writer.put(key(k), new FilePos(0, position++));
                }
            }
            return StagedIndexPart.of(
                    stagingId,
                    writer.finish(),
                    Collections.singletonMap(0, "new-" + bucket.getBucket() + ".parquet"),
                    changedKeys.size(),
                    deletes);
        }
    }

    private static Roaring64Bitmap bitmap(long... positions) {
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        for (long p : positions) {
            bitmap.addLong(p);
        }
        return bitmap;
    }

    private static byte[] serialize(Roaring64Bitmap bitmap) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            bitmap.serialize(out);
        }
        return bytes.toByteArray();
    }

    /** Two of three keys have a row in the base, at the key's position of one of two files. */
    private static boolean inBase(int key) {
        return key % 3 != 0;
    }

    private static String baseFile(TableBucket bucket, int key) {
        return "old-" + bucket.getBucket() + "-" + (key % 2);
    }

    private static Map<String, List<Long>> positionsOf(DeletedPositions deleted) {
        Map<String, List<Long>> out = new TreeMap<>();
        for (Map.Entry<String, long[]> e : deleted.entrySet()) {
            List<Long> positions = new ArrayList<>();
            for (long p : e.getValue()) {
                positions.add(p);
            }
            out.put(e.getKey(), positions);
        }
        return out;
    }

    private static byte[] key(int k) {
        return new byte[] {(byte) (k >>> 24), (byte) (k >>> 16), (byte) (k >>> 8), (byte) k};
    }
}
