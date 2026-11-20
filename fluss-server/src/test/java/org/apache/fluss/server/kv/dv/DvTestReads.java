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
import org.apache.fluss.lake.dv.FilePos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Reads all masks of a union read at once, and the index of a bucket, as tests compare them. */
public final class DvTestReads {

    private DvTestReads() {}

    /** All masks a union read of {@code snapshotId} reads, waiting for the bucket's leader. */
    public static DvSnapshot unionRead(DerivedDvManager manager, long snapshotId)
            throws IOException, StaleSnapshotException {
        return unionRead(manager, snapshotId, true);
    }

    /**
     * All masks a union read of {@code snapshotId} reads: pins a cut, takes its masks and drops it,
     * as its idle timeout would, so the read leaves no pin behind.
     */
    public static DvSnapshot unionRead(
            DerivedDvManager manager, long snapshotId, boolean waitForCatchUp)
            throws IOException, StaleSnapshotException {
        DvPinnedCut cut = manager.pinCut(snapshotId, waitForCatchUp, null);
        try {
            return cut.toDvSnapshot();
        } finally {
            manager.dropPinnedCut(cut.snapshotId(), cut.logEndOffset());
        }
    }

    /**
     * {key, file path, row position} of each of {@code keys} the index of {@code snapshotId} holds,
     * read from the version the bucket publishes, without a step of its owner. Waits for the bucket
     * to publish the snapshot's version.
     */
    public static List<Object[]> indexed(
            DerivedDvManager manager, long snapshotId, List<byte[]> keys) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (true) {
            DvVersion v = manager.state().version;
            if (v != null && v.snapshotId == snapshotId && v.tryRetain()) {
                try {
                    List<FilePos> found = v.positions(keys);
                    List<Object[]> out = new ArrayList<>();
                    for (int i = 0; i < keys.size(); i++) {
                        FilePos fp = found.get(i);
                        String path = fp == null ? null : v.filePath(fp.fileId());
                        if (path != null) {
                            out.add(new Object[] {keys.get(i), path, fp.rowPosition()});
                        }
                    }
                    return out;
                } finally {
                    v.release();
                }
            }
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError(
                        "The bucket publishes " + manager.state() + ", not snapshot " + snapshotId);
            }
            Thread.sleep(10);
        }
    }
}
