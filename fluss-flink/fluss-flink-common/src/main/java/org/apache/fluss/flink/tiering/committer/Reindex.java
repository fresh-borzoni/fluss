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

package org.apache.fluss.flink.tiering.committer;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.flink.tiering.event.RowPosRequestEvent;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/** A re-index of rewritten lake files that a held round waits for. */
final class Reindex {
    final TablePath tablePath;
    final long baseSnapshotId;
    final long compactSnapshotId;
    final long changeId;
    @Nullable final Map<String, Map<Integer, List<String>>> removedFiles;
    // the events that request the re-index's splits
    final List<RowPosRequestEvent> requests;
    // one result per split
    final int expectedResults;
    final long startedAtMs;

    @VisibleForTesting
    Reindex(
            TablePath tablePath,
            long baseSnapshotId,
            long compactSnapshotId,
            long changeId,
            @Nullable Map<String, Map<Integer, List<String>>> removedFiles,
            List<RowPosRequestEvent> requests,
            int expectedResults,
            long startedAtMs) {
        this.tablePath = tablePath;
        this.baseSnapshotId = baseSnapshotId;
        this.compactSnapshotId = compactSnapshotId;
        this.changeId = changeId;
        this.removedFiles = removedFiles;
        this.requests = requests;
        this.expectedResults = expectedResults;
        this.startedAtMs = startedAtMs;
    }
}
