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

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/** {@link DvTestHooks} whose waits and faults a test sets while the code under test runs. */
public final class TestingDvHooks implements DvTestHooks {

    @Nullable private volatile CountDownLatch rebuildGate;
    @Nullable private volatile CountDownLatch switchGate;
    private volatile boolean resolvesPaused;
    @Nullable private volatile Runnable beforeResolveLookup;
    @Nullable private volatile Runnable afterLookupRead;
    @Nullable private volatile Runnable afterIngest;
    @Nullable private volatile Consumer<BooleanSupplier> beforeCompaction;
    @Nullable private volatile Consumer<String> beforeFileDrop;
    @Nullable private volatile Runnable afterDerivationChunk;
    @Nullable private volatile Runnable beforeReadyServe;
    @Nullable private volatile Runnable faultInNextDrain;
    @Nullable private volatile IOException changeListWriteFailure;
    private final AtomicInteger changeListWritesFailed = new AtomicInteger();

    /** Holds the rebuilds and replays until {@code gate} counts down; none if null. */
    public void holdRebuilds(@Nullable CountDownLatch gate) {
        this.rebuildGate = gate;
    }

    /** Holds the switches to newer snapshots until {@code gate} counts down; none if null. */
    public void holdSwitches(@Nullable CountDownLatch gate) {
        this.switchGate = gate;
    }

    /** Pauses the background resolves, which then neither apply nor look up. */
    public void setResolvesPaused(boolean paused) {
        this.resolvesPaused = paused;
    }

    public void setBeforeResolveLookup(@Nullable Runnable hook) {
        this.beforeResolveLookup = hook;
    }

    public void setAfterLookupRead(@Nullable Runnable hook) {
        this.afterLookupRead = hook;
    }

    public void setAfterIngest(@Nullable Runnable hook) {
        this.afterIngest = hook;
    }

    public void setBeforeCompaction(@Nullable Consumer<BooleanSupplier> hook) {
        this.beforeCompaction = hook;
    }

    public void setBeforeFileDrop(@Nullable Consumer<String> hook) {
        this.beforeFileDrop = hook;
    }

    public void setAfterDerivationChunk(@Nullable Runnable hook) {
        this.afterDerivationChunk = hook;
    }

    public void setBeforeReadyServe(@Nullable Runnable hook) {
        this.beforeReadyServe = hook;
    }

    /** Runs {@code fault} in the owner's next drain of the hand-off, once. */
    public void setFaultInNextDrain(@Nullable Runnable fault) {
        this.faultInNextDrain = fault;
    }

    /** Makes the writes of the change list fail, as a full or broken disk does; none if null. */
    public void failChangeListWrites(@Nullable IOException failure) {
        this.changeListWriteFailure = failure;
    }

    /** The writes of the change list {@link #failChangeListWrites} failed so far. */
    public int changeListWritesFailed() {
        return changeListWritesFailed.get();
    }

    @Override
    public void beforeRebuild() throws InterruptedIOException {
        CountDownLatch held = rebuildGate;
        if (held == null) {
            return;
        }
        try {
            held.await(5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("Interrupted while a test held the rebuild");
        }
    }

    @Override
    public boolean holdsSwitches() {
        CountDownLatch held = switchGate;
        return held != null && held.getCount() > 0;
    }

    @Override
    public boolean pausesResolves() {
        return resolvesPaused;
    }

    @Override
    public void beforeResolveLookup() {
        run(beforeResolveLookup);
    }

    @Override
    public void afterLookupRead() {
        run(afterLookupRead);
    }

    @Override
    public void afterIngest() {
        run(afterIngest);
    }

    @Override
    public void beforeCompaction(BooleanSupplier cancelled) {
        Consumer<BooleanSupplier> hook = beforeCompaction;
        if (hook != null) {
            hook.accept(cancelled);
        }
    }

    @Override
    public void beforeFileDrop(String fileName) {
        Consumer<String> hook = beforeFileDrop;
        if (hook != null) {
            hook.accept(fileName);
        }
    }

    @Override
    public void afterDerivationChunk() {
        run(afterDerivationChunk);
    }

    @Override
    public void beforeReadyServe() {
        run(beforeReadyServe);
    }

    @Override
    public void onDrain() {
        Runnable fault = faultInNextDrain;
        if (fault != null) {
            faultInNextDrain = null;
            fault.run();
        }
    }

    @Override
    public void beforeChangeListWrite() throws IOException {
        IOException failure = changeListWriteFailure;
        if (failure != null) {
            changeListWritesFailed.incrementAndGet();
            throw failure;
        }
    }

    private static void run(@Nullable Runnable hook) {
        if (hook != null) {
            hook.run();
        }
    }
}
