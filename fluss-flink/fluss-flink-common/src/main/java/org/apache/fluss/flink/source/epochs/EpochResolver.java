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

package org.apache.fluss.flink.source.epochs;

import org.apache.fluss.metadata.TableBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Resolves the epochs that lookups read as of to positions in the buckets of the scanned table: an
 * epoch ends in a bucket where the latest epoch at or before it that changed the bucket ended, once
 * the scan ended the epoch in the bucket. Until then, a resolution waits.
 *
 * <p>Resolutions never change, so the resolver keeps them: per bucket, the intervals of epochs
 * between an end and a later epoch, which all end at that end.
 */
public final class EpochResolver {

    private static final Logger LOG = LoggerFactory.getLogger(EpochResolver.class);

    /** The most intervals the resolver keeps per bucket. */
    private static final int MAX_INTERVALS = 1024;

    private static final long WAIT_LOG_INTERVAL_MS = 30_000L;

    private final EpochStore store;
    private final ScheduledExecutorService retries;
    private final long pollDelayMs;

    private final Map<TableBucket, Resolved> resolved = new ConcurrentHashMap<>();
    private final Map<Request, CompletableFuture<Long>> inFlight = new ConcurrentHashMap<>();

    /**
     * @param retries schedules the reads of epochs that did not end yet
     * @param pollDelayMs how long to wait before reading an epoch that did not end again
     */
    public EpochResolver(EpochStore store, ScheduledExecutorService retries, long pollDelayMs) {
        this.store = store;
        this.retries = retries;
        this.pollDelayMs = pollDelayMs;
    }

    /**
     * The position of the bucket at the end of the epoch: the next log offset after its last change
     * up to the epoch, or {@link Epochs#EMPTY} if the bucket is empty then. Completes once the scan
     * ended the epoch in the bucket.
     */
    public CompletableFuture<Long> endOf(TableBucket bucket, long epoch) {
        Long known = known(bucket, epoch);
        if (known != null) {
            return CompletableFuture.completedFuture(known);
        }
        Request request = new Request(bucket, epoch);
        CompletableFuture<Long> result = new CompletableFuture<>();
        CompletableFuture<Long> running = inFlight.putIfAbsent(request, result);
        if (running != null) {
            return running;
        }
        long now = System.currentTimeMillis();
        resolve(request, result, now, now);
        return result;
    }

    private void resolve(
            Request request, CompletableFuture<Long> result, long since, long loggedAt) {
        Resolved bucketEnds = resolved.computeIfAbsent(request.bucket, b -> new Resolved());
        CompletableFuture<Long> endedThrough =
                bucketEnds.endedThrough() >= request.epoch
                        ? CompletableFuture.completedFuture(bucketEnds.endedThrough())
                        : store.endedThrough(request.bucket);
        // leave the client network thread: a full lookup queue would block it and its own replies
        (endedThrough.isDone() ? endedThrough : endedThrough.thenApplyAsync(e -> e, retries))
                .thenCompose(
                        ended -> {
                            if (ended == null || ended < request.epoch) {
                                return CompletableFuture.completedFuture((EpochEnd) null);
                            }
                            bucketEnds.endedThrough(ended);
                            return store.latestEnd(request.bucket, request.epoch)
                                    .thenApply(
                                            end -> {
                                                if (end == null) {
                                                    throw new IllegalStateException(
                                                            String.format(
                                                                    "The epochs of %s ended"
                                                                            + " through %s but"
                                                                            + " record no start.",
                                                                    request.bucket, ended));
                                                }
                                                return end;
                                            });
                        })
                .whenComplete(
                        (end, error) -> {
                            if (error != null) {
                                inFlight.remove(request);
                                result.completeExceptionally(error);
                            } else if (end != null) {
                                bucketEnds.remember(end, request.epoch);
                                inFlight.remove(request);
                                result.complete(end.getEnd());
                            } else {
                                long now = System.currentTimeMillis();
                                long logged = loggedAt;
                                if (now - loggedAt >= WAIT_LOG_INTERVAL_MS) {
                                    LOG.warn(
                                            "Waiting {} ms for the scan to end epoch {} in {}.",
                                            now - since,
                                            request.epoch,
                                            request.bucket);
                                    logged = now;
                                }
                                long next = logged;
                                try {
                                    retries.schedule(
                                            () -> resolve(request, result, since, next),
                                            pollDelayMs,
                                            TimeUnit.MILLISECONDS);
                                } catch (Exception e) {
                                    inFlight.remove(request);
                                    result.completeExceptionally(e);
                                }
                            }
                        });
    }

    private Long known(TableBucket bucket, long epoch) {
        Resolved bucketEnds = resolved.get(bucket);
        return bucketEnds == null ? null : bucketEnds.endAt(epoch);
    }

    /** The resolved ends of one bucket. */
    private static final class Resolved {

        /** From the first epoch of an interval to its last epoch and its end. */
        private final TreeMap<Long, long[]> intervals = new TreeMap<>();

        private long endedThrough = Long.MIN_VALUE;

        synchronized long endedThrough() {
            return endedThrough;
        }

        synchronized void endedThrough(long ended) {
            endedThrough = Math.max(endedThrough, ended);
        }

        /** The epochs from the end's epoch up to the given one end at the end. */
        synchronized void remember(EpochEnd end, long upTo) {
            long[] interval = intervals.get(end.getEpoch());
            if (interval == null) {
                intervals.put(end.getEpoch(), new long[] {upTo, end.getEnd()});
                if (intervals.size() > MAX_INTERVALS) {
                    intervals.pollFirstEntry();
                }
            } else {
                interval[0] = Math.max(interval[0], upTo);
            }
        }

        synchronized Long endAt(long epoch) {
            Map.Entry<Long, long[]> entry = intervals.floorEntry(epoch);
            if (entry == null || entry.getValue()[0] < epoch) {
                return null;
            }
            return entry.getValue()[1];
        }
    }

    private static final class Request {
        final TableBucket bucket;
        final long epoch;

        Request(TableBucket bucket, long epoch) {
            this.bucket = bucket;
            this.epoch = epoch;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Request)) {
                return false;
            }
            Request request = (Request) o;
            return epoch == request.epoch && bucket.equals(request.bucket);
        }

        @Override
        public int hashCode() {
            return Objects.hash(bucket, epoch);
        }
    }
}
