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

import org.apache.fluss.exception.StaleSnapshotException;

import java.io.IOException;

/** Reads all masks of a union read at once, as tests compare them. */
public final class DvTestReads {

    private DvTestReads() {}

    /** All masks a union read of {@code snapshotId} reads, waiting for the bucket's leader. */
    public static DvSnapshot unionRead(DerivedDvManager manager, long snapshotId)
            throws IOException, StaleSnapshotException {
        return unionRead(manager, snapshotId, true);
    }

    /**
     * All masks a union read of {@code snapshotId} reads: pins a cut, takes its masks and releases
     * it, so the read leaves no pin behind.
     */
    public static DvSnapshot unionRead(
            DerivedDvManager manager, long snapshotId, boolean waitForCatchUp)
            throws IOException, StaleSnapshotException {
        DvPinnedCut cut = manager.pinCut(snapshotId, waitForCatchUp, null, 1);
        try {
            return cut.toDvSnapshot();
        } finally {
            manager.releaseCut(cut.snapshotId(), cut.logEndOffset(), 1);
        }
    }
}
