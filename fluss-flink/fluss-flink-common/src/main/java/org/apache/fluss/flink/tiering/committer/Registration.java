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

import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * A merging round committed to the lake, registered in Fluss once its readable snapshot is indexed.
 */
final class Registration<Committable> {
    final LakeCommitResult result;
    final String offsetsFile;
    final Map<TableBucket, Long> logEndOffsets;
    final Map<TableBucket, Long> logMaxTieredTimestamps;
    // null for the lost commit of an earlier round
    @Nullable final Committable committable;

    Registration(
            LakeCommitResult result,
            String offsetsFile,
            Map<TableBucket, Long> logEndOffsets,
            Map<TableBucket, Long> logMaxTieredTimestamps,
            @Nullable Committable committable) {
        this.result = result;
        this.offsetsFile = offsetsFile;
        this.logEndOffsets = logEndOffsets;
        this.logMaxTieredTimestamps = logMaxTieredTimestamps;
        this.committable = committable;
    }
}
