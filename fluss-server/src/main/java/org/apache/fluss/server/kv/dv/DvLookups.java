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

import org.apache.fluss.exception.DeletionVectorsNotReadyException;
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.lake.dv.FilePos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.fluss.server.kv.dv.DvMaskReads.NOT_READY_WAIT_MS;
import static org.apache.fluss.server.kv.dv.DvOwner.RESOLVE_CHUNK;

/**
 * Tiering's lookups of a bucket's lake positions, on the request thread: each reads the version the
 * owner published ({@link DvState#version}), retained for the lookup, so it never waits for a step
 * of the owner. A lookup of a snapshot the bucket does not cover yet asks the owner to catch up to
 * it and waits for that a moment.
 */
final class DvLookups {

    // the bucket's owner: lookups read its published state only, and post the catch-ups they need
    private final DvOwner m;

    DvLookups(DvOwner m) {
        this.m = m;
    }

    /**
     * Looks keys up in the version of {@code snapshotId}. If the bucket covers another snapshot, it
     * starts the catch-up to it and waits a moment for it.
     */
    List<Object[]> lookup(long snapshotId, List<byte[]> keys)
            throws IOException, StaleSnapshotException {
        long deadline = System.currentTimeMillis() + NOT_READY_WAIT_MS;
        CompletableFuture<Boolean> reached = null;
        while (true) {
            DvState s = m.state;
            if (m.closed || s.kind == DvState.Kind.CLOSED) {
                throw DerivedDvManager.closedError();
            }
            if (s.kind == DvState.Kind.UNRECOVERED) {
                // A new leader that has not started recovering yet covers nothing so far.
                throw new DeletionVectorsNotReadyException(
                        String.format(
                                "Deletion vectors of this bucket are not recovered yet; lake"
                                        + " snapshot %d is requested.",
                                snapshotId));
            }
            DvVersion v = s.version;
            if (v != null && v.snapshotId == snapshotId) {
                if (v.tryRetain()) {
                    try {
                        return lookupIn(v, keys);
                    } finally {
                        v.release();
                    }
                }
                // a newer version is published in a moment, unless the owner cannot publish it
                if (System.currentTimeMillis() >= deadline) {
                    throw new DeletionVectorsNotReadyException(
                            String.format(
                                    "Deletion vectors of lake snapshot %d are being replaced.",
                                    snapshotId));
                }
                Thread.yield();
                continue;
            }
            if (snapshotId < 0 || m.passed(snapshotId) || s.unreachable == snapshotId) {
                throw m.stale(snapshotId, s);
            }
            if (reached != null && reached.isDone()) {
                try {
                    reached.join();
                } catch (CompletionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof DeletionVectorsNotReadyException) {
                        throw (DeletionVectorsNotReadyException) cause;
                    }
                    // the catch-up this lookup waited for failed: retried after a backoff
                    throw new DeletionVectorsNotReadyException(
                            String.format(
                                    "Deletion vectors cover lake snapshot %d; switching to"
                                            + " snapshot %d failed (%s) and is retried.",
                                    s.coveredSnapshotId, snapshotId, cause),
                            cause);
                }
                // ended without covering it: the state tells why, or it is asked for again
                reached = null;
                continue;
            }
            if (reached == null) {
                reached = m.work.reachForLookup(snapshotId);
            }
            long left = deadline - System.currentTimeMillis();
            if (left <= 0) {
                throw new DeletionVectorsNotReadyException(
                        String.format(
                                "Deletion vectors cover lake snapshot %d and are switching to"
                                        + " snapshot %d.",
                                s.coveredSnapshotId, snapshotId));
            }
            try {
                reached.get(left, TimeUnit.MILLISECONDS);
            } catch (TimeoutException | ExecutionException e) {
                // answered above
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        }
    }

    /**
     * Looks keys up in version {@code v} on the request thread, so tiering lookups do not block the
     * write path. The answer is exact for the version, whatever the bucket does meanwhile: a lookup
     * that spans a switch answers with the positions of the snapshot it asked for.
     */
    private List<Object[]> lookupIn(DvVersion v, List<byte[]> keys) throws IOException {
        List<Object[]> out = new ArrayList<>();
        Map<Integer, String> paths = new HashMap<>();
        for (int from = 0; from < keys.size(); from += RESOLVE_CHUNK) {
            if (m.closed) {
                throw DerivedDvManager.closedError();
            }
            List<byte[]> chunk = keys.subList(from, Math.min(keys.size(), from + RESOLVE_CHUNK));
            List<FilePos> fps = v.positions(chunk);
            for (int i = 0; i < chunk.size(); i++) {
                FilePos fp = fps.get(i);
                if (fp == null) {
                    continue;
                }
                String path = paths.get(fp.fileId());
                if (path == null) {
                    path = v.filePath(fp.fileId());
                    if (path != null) {
                        paths.put(fp.fileId(), path);
                    }
                }
                if (path != null) {
                    out.add(new Object[] {chunk.get(i), path, fp.rowPosition()});
                }
            }
        }
        m.testHooks.afterLookupRead();
        return out;
    }
}
