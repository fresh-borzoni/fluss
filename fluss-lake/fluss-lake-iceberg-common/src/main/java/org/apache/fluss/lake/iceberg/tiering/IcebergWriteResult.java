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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.lake.dv.index.CompactionIndexPart;
import org.apache.fluss.lake.dv.index.CompactionIndexPartProvider;
import org.apache.fluss.lake.dv.index.PositionIndexDelta;
import org.apache.fluss.lake.dv.index.PositionIndexDeltaProvider;
import org.apache.fluss.lake.dv.index.StagedIndexPart;
import org.apache.fluss.lake.dv.index.StagedIndexPartProvider;
import org.apache.fluss.lake.iceberg.maintenance.RewriteDataFileResult;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.WriteResult;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Map;

/** The write result of Iceberg lake writer to pass to committer to commit. */
public class IcebergWriteResult
        implements Serializable,
                StagedIndexPartProvider,
                PositionIndexDeltaProvider,
                CompactionIndexPartProvider {

    private static final long serialVersionUID = 1L;

    // the normal result of tiering writing to iceberg
    private final WriteResult writeResult;

    // the partition specs of the table by spec id, to serialize the files of the write result.
    // Null if restored from version 1 of the serializer, which did not persist the specs.
    @Nullable private final Map<Integer, PartitionSpec> specs;

    // the rewrite result
    @Nullable private final RewriteDataFileResult rewriteDataFileResult;

    // the bucket's index part, staged by the writer of a table with deletion vectors
    @Nullable private final StagedIndexPart stagedIndexPart;

    // the bucket's index change held in memory; only restored from results written by version 2
    // of the serializer
    @Nullable private final PositionIndexDelta positionIndexDelta;

    public IcebergWriteResult(
            WriteResult writeResult,
            @Nullable Map<Integer, PartitionSpec> specs,
            @Nullable RewriteDataFileResult rewriteDataFileResult) {
        this(writeResult, specs, rewriteDataFileResult, null, null);
    }

    public IcebergWriteResult(
            WriteResult writeResult,
            @Nullable Map<Integer, PartitionSpec> specs,
            @Nullable RewriteDataFileResult rewriteDataFileResult,
            @Nullable StagedIndexPart stagedIndexPart,
            @Nullable PositionIndexDelta positionIndexDelta) {
        this.writeResult = writeResult;
        this.specs = specs;
        this.rewriteDataFileResult = rewriteDataFileResult;
        this.stagedIndexPart = stagedIndexPart;
        this.positionIndexDelta = positionIndexDelta;
    }

    @Override
    @Nullable
    public StagedIndexPart stagedIndexPart() {
        return stagedIndexPart;
    }

    @Override
    @Nullable
    public PositionIndexDelta positionIndexDelta() {
        return positionIndexDelta;
    }

    @Override
    public boolean compacted() {
        return rewriteDataFileResult != null;
    }

    @Override
    @Nullable
    public CompactionIndexPart compactionIndexPart() {
        return rewriteDataFileResult == null ? null : rewriteDataFileResult.indexPart();
    }

    public WriteResult getWriteResult() {
        return writeResult;
    }

    @Nullable
    public Map<Integer, PartitionSpec> specs() {
        return specs;
    }

    @Nullable
    public RewriteDataFileResult rewriteDataFileResult() {
        return rewriteDataFileResult;
    }

    @Override
    public String toString() {
        return "IcebergWriteResult{"
                + "dataFiles="
                + writeResult.dataFiles().length
                + ", deleteFiles="
                + writeResult.deleteFiles().length
                + (rewriteDataFileResult != null
                        ? (", rewriteDataFiles=" + rewriteDataFileResult)
                        : "")
                + (stagedIndexPart != null ? (", stagedIndexPart=" + stagedIndexPart) : "")
                + '}';
    }
}
