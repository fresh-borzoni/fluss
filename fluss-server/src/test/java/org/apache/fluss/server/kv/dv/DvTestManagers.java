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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.lake.dv.index.PositionIndexStore;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Creates bucket managers as tests use them, and hands their changes over. The managers and budgets
 * that tests build share one set of executors, as a tablet server's buckets do. Its threads are
 * daemons, started when first used and shut down when the JVM exits, so test classes never add
 * threads of their own.
 */
public final class DvTestManagers {

    /** The default number of index changes between two checkpoints. */
    public static final int CHECKPOINT_INTERVAL =
            ConfigOptions.KV_DELETION_VECTORS_CHECKPOINT_INTERVAL.defaultValue();

    private DvTestManagers() {}

    /** The executors the tests' managers and budgets share. */
    public static DerivedDvRuntime executors() {
        return Shared.EXECUTORS;
    }

    /** A manager with the default checkpoint interval and no metrics. */
    public static DerivedDvManager newManager(DvRocksDB db, LongSupplier highWatermark) {
        return newManager(db, highWatermark, CHECKPOINT_INTERVAL, DerivedDvMetrics.noop());
    }

    /** A manager of {@code db} with its own {@link TestingDvHooks}. */
    public static DerivedDvManager newManager(
            DvRocksDB db, LongSupplier highWatermark, int checkpointInterval, DerivedDvMetrics m) {
        return new DerivedDvManager(
                db,
                checkpointInterval,
                m,
                executors(),
                executors().bucketState(),
                new TestingDvHooks());
    }

    /** The hooks of a manager created with {@link TestingDvHooks}. */
    public static TestingDvHooks hooks(DerivedDvManager manager) {
        return (TestingDvHooks) manager.testHooks();
    }

    /** Counts a background reader of the manager's RocksDB, which a close then waits for. */
    public static void holdReader(DerivedDvManager manager) {
        manager.holdDb();
    }

    /**
     * Ends a reader {@link #holdReader} counted; the last one closes a closed manager's RocksDB.
     */
    public static void releaseReader(DerivedDvManager manager) {
        manager.releaseDb();
    }

    /** How long the manager's close waits for its background readers. */
    public static void setCloseWaitMillis(DerivedDvManager manager, long millis) {
        manager.setCloseWaitMillis(millis);
    }

    /** The manager's DV RocksDB. */
    public static DvRocksDB db(DerivedDvManager manager) {
        return manager.db();
    }

    /** Makes the manager cover the empty lake, as a new leader does once it recovers. */
    public static void coverTheEmptyLake(DerivedDvManager manager, LongSupplier logEnd)
            throws Exception {
        manager.recover(new EmptyLake());
    }

    /** The durable state of a bucket whose lake has no snapshot yet. */
    private static final class EmptyLake implements DerivedDvManager.RecoverySource {

        @Nullable
        @Override
        public long[] readableSnapshot() {
            return null;
        }

        @Nullable
        @Override
        public Long snapshotOffset(long snapshotId) {
            return null;
        }

        @Override
        public List<PositionIndexStore.ChainEntry> chain() {
            return Collections.emptyList();
        }

        @Override
        public PositionIndexStore.BucketData readChange(PositionIndexStore.ChainEntry entry) {
            throw new UnsupportedOperationException("the empty lake has no index changes");
        }

        @Override
        public boolean chainTrimmed() {
            return false;
        }

        @Nullable
        @Override
        public PositionIndexStore.Checkpoint latestCheckpoint(
                List<PositionIndexStore.ChainEntry> chain, int upTo) {
            return null;
        }

        @Override
        public PositionIndexStore.BucketData readCheckpoint(PositionIndexStore.Checkpoint cp) {
            throw new UnsupportedOperationException("the empty lake has no checkpoints");
        }

        @Override
        public void uploadCheckpoint(
                long snapshotId, @Nullable String localSst, Map<Integer, String> fileDict) {}
    }

    // created on first use
    private static final class Shared {
        static final DerivedDvRuntime EXECUTORS = new DerivedDvRuntime();

        static {
            Runtime.getRuntime()
                    .addShutdownHook(new Thread(EXECUTORS::shutdown, "derived-dv-test-shutdown"));
        }
    }
}
