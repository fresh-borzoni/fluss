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

import org.apache.fluss.annotation.VisibleForTesting;

/** A bucket's DV state as a test sees it, read at one point by an owner step. */
@VisibleForTesting
final class DvInspection {
    final long coveredSnapshotId;
    // the updates and deletes kept, and the changes waiting for the high watermark
    final long changeCount;
    final int queued;
    // the superseded versions in the log the change list masks
    final long deadVersions;
    // the lake files the masks name
    final int maskedLakeFiles;
    // the keys waiting for their lake positions
    final int unresolvedKeys;
    // the highest cut served to a union read
    final long servedCut;
    // whether lake positions are being looked up
    final boolean resolving;
    // whether the change list of the covered snapshot is in place
    final boolean changeListPresent;

    DvInspection(
            long coveredSnapshotId,
            long changeCount,
            int queued,
            long deadVersions,
            int maskedLakeFiles,
            int unresolvedKeys,
            long servedCut,
            boolean resolving,
            boolean changeListPresent) {
        this.coveredSnapshotId = coveredSnapshotId;
        this.changeCount = changeCount;
        this.queued = queued;
        this.deadVersions = deadVersions;
        this.maskedLakeFiles = maskedLakeFiles;
        this.unresolvedKeys = unresolvedKeys;
        this.servedCut = servedCut;
        this.resolving = resolving;
        this.changeListPresent = changeListPresent;
    }

    @Override
    public String toString() {
        return "covered="
                + coveredSnapshotId
                + ", changes="
                + changeCount
                + ", queued="
                + queued
                + ", deadVersions="
                + deadVersions
                + ", maskedLakeFiles="
                + maskedLakeFiles
                + ", unresolvedKeys="
                + unresolvedKeys
                + ", servedCut="
                + servedCut;
    }
}
