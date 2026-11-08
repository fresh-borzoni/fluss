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

import org.apache.fluss.annotation.Internal;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.function.BooleanSupplier;

/**
 * Points in the derived deletion vector work where tests wait or inject faults. Production code
 * runs {@link #NONE}. A test passes its own to a bucket's {@link DerivedDvManager}, or to a tablet
 * server, whose rebuild limiter runs the server-wide ones.
 */
@Internal
public interface DvTestHooks {

    DvTestHooks NONE = new DvTestHooks() {};

    // ---- a tablet server's, run by its rebuild limiter ----

    /** Runs before a rebuild or replay of a bucket starts; it may wait. */
    default void beforeRebuild() throws InterruptedIOException {}

    /** Whether the server's leaders put off their switches to newer snapshots. */
    default boolean holdsSwitches() {
        return false;
    }

    // ---- a bucket's ----

    /** Whether the background resolves neither apply nor look up. */
    default boolean pausesResolves() {
        return false;
    }

    /** Runs before each lookup of a resolve, on a key-lookup thread. */
    default void beforeResolveLookup() {}

    /** Runs after a tiering lookup read the index, before it answers. */
    default void afterLookupRead() {}

    /** Runs after a load ingested an index change, on the load's thread. */
    default void afterIngest() {}

    /**
     * Runs before a full load compacts the index it ingested in one call, on the load's thread,
     * once a close or a fault can cancel the compaction; {@code cancelled} tells whether one did.
     */
    default void beforeCompaction(BooleanSupplier cancelled) {}

    /** Runs for each file a switch or rebuild drops from the file dictionary, before the drop. */
    default void beforeFileDrop(String fileName) {}

    /** Runs after a switch's derivation added a chunk of keys and their old positions. */
    default void afterDerivationChunk() {}

    /** Runs before a read that does not wait is served the published frontier, on its thread. */
    default void beforeReadyServe() {}

    /** Runs at the start of each drain of the hand-off, an owner step. */
    default void onDrain() {}

    /** Runs before each write of the change list to the bucket's RocksDB. */
    default void beforeChangeListWrite() throws IOException {}
}
