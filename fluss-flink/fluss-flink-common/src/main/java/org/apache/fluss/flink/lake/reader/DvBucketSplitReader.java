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
import org.apache.fluss.flink.source.reader.BoundedSplitReader;
import org.apache.fluss.flink.source.reader.RecordAndPos;
import org.apache.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;

/**
 * Reads a {@link DvBucketSplit}. It first emits how the split was resolved, as a {@link
 * DvBucketSplitResolution}, then the split's rows.
 */
@Internal
public class DvBucketSplitReader extends BoundedSplitReader {

    private final DvBucketSplitScanner scanner;
    private boolean resolutionHandedOut;

    public DvBucketSplitReader(DvBucketSplitScanner scanner, DvBucketSplit split) {
        // A part skips the rows it emitted from the lake split it was in, which the scanner starts
        // at; a part in its log reads on from its next log offset.
        super(scanner, split.getNextLogOffset() >= 0 ? 0 : split.getRecordsToSkip());
        this.scanner = scanner;
    }

    @Nullable
    @Override
    public CloseableIterator<RecordAndPos> readBatch() throws IOException {
        if (!resolutionHandedOut) {
            DvBucketSplit resolved = scanner.resolve();
            resolutionHandedOut = true;
            return CloseableIterator.wrap(
                    Collections.<RecordAndPos>singletonList(new DvBucketSplitResolution(resolved))
                            .iterator());
        }
        return super.readBatch();
    }
}
