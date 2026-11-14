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

import org.apache.fluss.server.kv.dv.DvFrontiers.KeepReady;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The failure and close paths of a bucket's owner ({@link DvOwner}): a step that fails
 * unexpectedly, a lost change list, and the close of the bucket. Only a {@link VirtualMachineError}
 * stops the tablet server, once its fault is handled. Its methods run as owner steps unless their
 * javadoc names another thread.
 */
final class DvOwnerFaults {

    private static final Logger LOG = LoggerFactory.getLogger(DvOwnerFaults.class);

    private final DvOwner o;

    DvOwnerFaults(DvOwner o) {
        this.o = o;
    }

    /**
     * A step failed unexpectedly: the change list is dropped and replayed before masks are served
     * again, and the running work ends with the fault, once a load it has in flight stopped and
     * released what it holds. The mailbox then rethrows a {@link VirtualMachineError}.
     */
    void fault(Throwable t) {
        LOG.error(
                "A step of the deletion vectors of {} failed unexpectedly; the change"
                        + " list is replayed.",
                o.logName,
                t);
        o.metrics.ownerFault();
        try {
            ownerFault(t);
            DvOwnerWork.Work w = o.work.running;
            if (w != null) {
                Exception cause = t instanceof Exception ? (Exception) t : new Exception(t);
                DvLoad l = o.work.load;
                if (l != null) {
                    // The load's job runs: it stops at its next unit, and its swap step closes
                    // it and ends the work with the fault. The next load starts only then, so
                    // two loads never ingest at once and the stopped one swaps nothing in.
                    w.fault = cause;
                    l.cancel();
                } else {
                    o.work.finish(w, false, cause);
                }
            }
            o.afterStep();
        } catch (Throwable again) {
            LOG.error(
                    "The fault of the deletion vectors of {} could not be handled.",
                    o.logName,
                    again);
        }
    }

    /**
     * A job of the bucket failed with an error, not an exception: it is logged at ERROR and counted
     * as a fault, and the job posts it to the owner as its failure, so its work ends and what it
     * holds is released. Thread-safe; returns the failure to post.
     */
    Exception jobError(Throwable t, String job) {
        LOG.error("{} of the deletion vectors of {} failed unexpectedly.", job, o.logName, t);
        o.metrics.ownerFault();
        return t instanceof Exception ? (Exception) t : new Exception(t);
    }

    /**
     * Rethrows a fatal error of the virtual machine once a job posted its failure, so it reaches
     * the pool thread's uncaught exception handler and stops the tablet server; other errors end
     * with the failure posted.
     */
    static void rethrowIfFatal(Throwable t) {
        if (t instanceof VirtualMachineError) {
            throw (VirtualMachineError) t;
        }
    }

    /** Drops the change list after a hand-off failed or overflowed. */
    void changeListLost(Throwable cause) {
        poisonFinalPass();
        o.maskState.renew(KeepReady.SAME_SNAPSHOT);
        o.maskState.resetTail();
        o.maskState.reportTail(true);
        o.maskState.tailFailure =
                cause instanceof Exception
                        ? (Exception) cause
                        : new IOException(
                                "A hand-off of key changes to the deletion vectors failed", cause);
    }

    /**
     * The change list was lost while a replay is in its final pass: batches that came after the
     * replay's final read may be lost with it, so the replay installs nothing.
     */
    private void poisonFinalPass() {
        DvReplay pass = o.finalPass;
        if (pass != null) {
            pass.lost = true;
        }
    }

    /**
     * A step of the owner failed unexpectedly: the change list may miss what the step did, so it is
     * dropped and replayed from the log before masks are served again.
     */
    void ownerFault(Throwable cause) {
        if (o.closed) {
            return;
        }
        poisonFinalPass();
        o.maskState.renew(KeepReady.SAME_SNAPSHOT);
        o.maskState.resetTail();
        o.maskState.reportTail(true);
        o.maskState.tailFailure =
                cause instanceof Exception
                        ? (Exception) cause
                        : new IOException("A step of the deletion vectors failed", cause);
    }

    /**
     * Closes the state: the work queued ends, the waiting reads are answered as sent to a server
     * that is not the leader, the heap is released, and the covered version goes with its last
     * reader.
     */
    void closeStep() {
        o.work.closeWork();
        o.changeListExport.close();
        o.maskReads.closed();
        o.finalPass = null;
        o.maskState.reportTail(true);
        o.version.release();
    }

    /**
     * Closes the bucket: sets {@code closed}, which the write path, jobs and pins see, closes the
     * state in a step, and closes the RocksDB once its leases are released; see {@link
     * DerivedDvManager#close}.
     */
    void close(DerivedDvManager bucket) {
        // one deadline for the close step and the leases together
        long deadline = System.currentTimeMillis() + o.closeWaitMs;
        if (!o.closed) {
            // set first, so the write path, the jobs and the pins see it
            o.closed = true;
            // a pin racing the close fails instead of adding a cut nobody drops
            o.heapBudget.pinnedCuts().dropAll(bucket);
            CompletableFuture<Void> stepped = new CompletableFuture<>();
            o.post(
                    () -> {
                        try {
                            closeStep();
                        } finally {
                            stepped.complete(null);
                        }
                    });
            try {
                stepped.get(
                        Math.max(1, deadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException | TimeoutException e) {
                LOG.warn("Closing the deletion vectors of {} did not end in time.", o.logName);
            }
        }
        o.leases.close(Math.max(0, deadline - System.currentTimeMillis()));
    }
}
