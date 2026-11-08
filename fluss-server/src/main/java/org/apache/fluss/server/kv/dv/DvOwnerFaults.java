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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The failure and close paths of a bucket's owner ({@link DvOwner}): a step that fails
 * unexpectedly, and the close of the bucket. Only a {@link VirtualMachineError} stops the tablet
 * server, once its fault is handled. Its methods run as owner steps unless their javadoc names
 * another thread.
 */
final class DvOwnerFaults {

    private static final Logger LOG = LoggerFactory.getLogger(DvOwnerFaults.class);

    private final DvOwner o;

    DvOwnerFaults(DvOwner o) {
        this.o = o;
    }

    /**
     * A step failed unexpectedly: the running work ends with the fault, once a load it has in
     * flight stopped and released what it holds. The mailbox then rethrows a {@link
     * VirtualMachineError}.
     */
    void fault(Throwable t) {
        LOG.error("A step of the deletion vectors of {} failed unexpectedly.", o.logName, t);
        o.metrics.ownerFault();
        try {
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

    /**
     * Closes the state: the work queued ends, and the covered version goes with its last reader.
     */
    void closeStep() {
        o.work.closeWork();
        o.version.release();
    }

    /**
     * Closes the bucket: sets {@code closed}, which jobs and lookups see, closes the state in a
     * step, and closes the RocksDB once its leases are released; see {@link
     * DerivedDvManager#close}.
     */
    void close() {
        // one deadline for the close step and the leases together
        long deadline = System.currentTimeMillis() + o.closeWaitMs;
        if (!o.closed) {
            // set first, so the jobs and lookups see it
            o.closed = true;
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
