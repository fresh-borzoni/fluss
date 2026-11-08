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

import java.io.InterruptedIOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/** {@link DvTestHooks} whose waits and faults a test sets while the code under test runs. */
public final class TestingDvHooks implements DvTestHooks {

    @Nullable private volatile CountDownLatch rebuildGate;
    @Nullable private volatile CountDownLatch switchGate;
    @Nullable private volatile Runnable afterLookupRead;
    @Nullable private volatile Runnable afterIngest;
    @Nullable private volatile Consumer<BooleanSupplier> beforeCompaction;
    @Nullable private volatile Consumer<String> beforeFileDrop;

    /** Holds the rebuilds until {@code gate} counts down; none if null. */
    public void holdRebuilds(@Nullable CountDownLatch gate) {
        this.rebuildGate = gate;
    }

    /** Holds the switches to newer snapshots until {@code gate} counts down; none if null. */
    public void holdSwitches(@Nullable CountDownLatch gate) {
        this.switchGate = gate;
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

    private static void run(@Nullable Runnable hook) {
        if (hook != null) {
            hook.run();
        }
    }
}
