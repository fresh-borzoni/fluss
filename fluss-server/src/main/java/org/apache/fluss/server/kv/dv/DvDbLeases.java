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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * The leases of a bucket's DV RocksDB: who may still call it. A lease covers one RocksDB call, such
 * as an ingest, a chunk of lookups or an export, or a RocksDB snapshot held open, such as a
 * version's. Leases never cover remote I/O, and jobs take a new one per unit, so a closing bucket
 * waits only for the calls in progress. A call that may take long, a full load's compaction, is
 * cancelled instead ({@link #tryAcquire(Runnable)}).
 *
 * <p>Once {@link #close} is called no lease is granted. The RocksDB closes when the last lease is
 * released, by {@link #close} itself if none is held within its wait, else by the last {@link
 * #release}. {@link #dbClosed} completes then: the RocksDB shares the tablet server's block cache
 * and write buffer manager, which must outlive it.
 */
final class DvDbLeases {

    private static final Logger LOG = LoggerFactory.getLogger(DvDbLeases.class);

    private final DvRocksDB db;
    private final CompletableFuture<Void> dbClosed = new CompletableFuture<>();
    // guarded by this
    private int held;
    private boolean closing;
    private boolean closed;
    // what ends early each call under a lease that a close cancels rather than waits for
    private final Set<Runnable> cancels = new HashSet<>();

    DvDbLeases(DvRocksDB db) {
        this.db = db;
    }

    /** Takes a lease, or returns false once the bucket closes. */
    synchronized boolean tryAcquire() {
        if (closing) {
            return false;
        }
        held++;
        return true;
    }

    /**
     * Takes a lease for a call that {@code cancel} ends early, which a close runs rather than wait
     * for the call; false once the bucket closes. {@link #release(Runnable)} releases it.
     */
    synchronized boolean tryAcquire(Runnable cancel) {
        if (!tryAcquire()) {
            return false;
        }
        cancels.add(cancel);
        return true;
    }

    /** Releases a lease {@link #tryAcquire(Runnable)} took. */
    void release(Runnable cancel) {
        synchronized (this) {
            cancels.remove(cancel);
        }
        release();
    }

    /**
     * Takes a lease for one unit of a job.
     *
     * @throws DvClosedException once the bucket closes, which ends the job
     */
    void acquire() {
        if (!tryAcquire()) {
            throw new DvClosedException();
        }
    }

    /** Releases a lease; the last one after {@link #close} closes the RocksDB. */
    void release() {
        boolean closeNow;
        synchronized (this) {
            if (held <= 0) {
                throw new IllegalStateException(
                        "A lease of the deletion vector RocksDB was released twice");
            }
            held--;
            closeNow = closing && held == 0 && !closed;
            if (closeNow) {
                closed = true;
            }
            notifyAll();
        }
        if (closeNow) {
            closeDb();
        }
    }

    /**
     * Grants no more leases, cancels the calls that end early, and waits up to {@code waitMs} for
     * the held leases. Closes the RocksDB if none is held then, else leaves that to the last
     * release. An interrupt does not cut the wait short, as the RocksDB would outlive the close.
     */
    void close(long waitMs) {
        boolean closeNow;
        int left;
        synchronized (this) {
            closing = true;
            if (!cancels.isEmpty()) {
                for (Runnable cancel : cancels) {
                    cancel.run();
                }
                // A cancelled compaction waiting for an automatic one sees the cancel only when
                // RocksDB wakes its waiters; this binding cannot disable manual compactions.
                db.stopBackgroundWork();
            }
            long deadline = System.currentTimeMillis() + waitMs;
            boolean interrupted = false;
            while (held > 0 && System.currentTimeMillis() < deadline) {
                try {
                    wait(Math.max(1, Math.min(500, deadline - System.currentTimeMillis())));
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            left = held;
            closeNow = left == 0 && !closed;
            if (closeNow) {
                closed = true;
            }
        }
        if (closeNow) {
            closeDb();
        } else if (left > 0) {
            LOG.warn(
                    "{} calls still use the deletion vector RocksDB; the last of them"
                            + " closes it.",
                    left);
        }
    }

    private void closeDb() {
        try {
            db.close();
        } finally {
            dbClosed.complete(null);
        }
    }

    /** The leases held now. */
    synchronized int held() {
        return held;
    }

    /** Completes once the RocksDB is closed. */
    CompletableFuture<Void> dbClosed() {
        return dbClosed;
    }

    /** Ends a job of a bucket that closed: the RocksDB is about to close. */
    static final class DvClosedException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        DvClosedException() {
            super("The deletion vectors of the bucket are closed", null, false, false);
        }
    }
}
