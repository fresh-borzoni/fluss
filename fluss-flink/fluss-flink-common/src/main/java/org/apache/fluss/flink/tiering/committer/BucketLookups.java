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

import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.rpc.messages.PbDvKeyPosition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

/**
 * Looks up the base positions of a bucket's keys as publishing streams them, in chunks, and
 * collects the found ones per data file.
 */
final class BucketLookups {

    private final TableBucket bucket;
    private final long base;
    private final boolean lookUp;
    private final DerivedDvLookups.PositionLookup lookup;
    private final ExecutorService executor;
    private final Semaphore inFlight;
    private final List<Future<?>> pending = new ArrayList<>();
    private List<byte[]> chunk = new ArrayList<>();
    // written by lookups, read once they are done
    final DeletedPositions deleted = new DeletedPositions();
    long found;
    PositionIndexStore.BucketPart published;

    BucketLookups(
            TableBucket bucket,
            long base,
            boolean lookUp,
            DerivedDvLookups.PositionLookup lookup,
            ExecutorService executor,
            Semaphore inFlight) {
        this.bucket = bucket;
        this.base = base;
        this.lookUp = lookUp;
        this.lookup = lookup;
        this.executor = executor;
        this.inFlight = inFlight;
    }

    void accept(byte[] key) throws IOException {
        if (!lookUp) {
            return; // a new bucket has nothing in the base
        }
        chunk.add(key);
        if (chunk.size() == PublishedParts.LOOKUP_CHUNK) {
            submit();
        }
    }

    private void submit() throws IOException {
        checkDone(false);
        List<byte[]> keys = chunk;
        chunk = new ArrayList<>();
        try {
            inFlight.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while looking up positions.", e);
        }
        try {
            pending.add(
                    executor.submit(
                            () -> {
                                try {
                                    GetDvSnapshotResponse response =
                                            lookup.lookUp(bucket, base, keys);
                                    synchronized (deleted) {
                                        for (PbDvKeyPosition kp : response.getKeyPositionsList()) {
                                            deleted.add(kp.getFilePath(), kp.getPos());
                                            found++;
                                        }
                                    }
                                } finally {
                                    inFlight.release();
                                }
                                return null;
                            }));
        } catch (RuntimeException e) {
            inFlight.release();
            throw e;
        }
    }

    /** Waits for the bucket's lookups, after its last key. */
    void finish() throws IOException {
        if (!chunk.isEmpty()) {
            submit();
        }
        checkDone(true);
    }

    /** Fails on a failed lookup; with {@code all}, waits for all of them. */
    private void checkDone(boolean all) throws IOException {
        for (Iterator<Future<?>> it = pending.iterator(); it.hasNext(); ) {
            Future<?> f = it.next();
            if (!all && !f.isDone()) {
                continue;
            }
            try {
                f.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while looking up positions.", e);
            } catch (ExecutionException e) {
                throw new IOException(
                        "Looking up the positions of bucket " + bucket + " failed.", e.getCause());
            }
            // a done lookup's writes are visible once its future returned
            it.remove();
        }
    }
}
