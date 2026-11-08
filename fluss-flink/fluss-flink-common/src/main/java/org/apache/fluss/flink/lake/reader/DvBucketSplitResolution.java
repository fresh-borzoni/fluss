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

package org.apache.fluss.flink.lake.reader;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.flink.lake.split.DvBucketSplit;
import org.apache.fluss.flink.source.reader.RecordAndPos;

/**
 * Tells the split's state how its reader resolved a {@link DvBucketSplit}: the read mode, snapshot,
 * files and cut. It carries no row and precedes the split's rows, so a checkpoint that holds any of
 * them holds the resolution too.
 */
@Internal
public class DvBucketSplitResolution extends RecordAndPos {

    private final DvBucketSplit resolved;

    public DvBucketSplitResolution(DvBucketSplit resolved) {
        super(null);
        this.resolved = resolved;
    }

    public DvBucketSplit getResolved() {
        return resolved;
    }
}
