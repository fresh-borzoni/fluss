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
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketPartReader;
import org.apache.fluss.flink.lake.split.DvBucketSplit;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;

/** Reads a {@link DvBucketSplit} with the {@link DvBucketPartReader} of its part. */
@Internal
public class DvBucketSplitScanner implements BatchScanner {

    private final DvBucketPartReader reader;
    @Nullable private DvBucketSplit resolved;

    public DvBucketSplitScanner(DvBucketSplit split, DvBucketPartReader.Context context) {
        this.reader = new DvBucketPartReader(split.getPart(), context);
    }

    /**
     * Resolves how the split is read, fetching the bucket's masks for a masked read. Returns the
     * resolved split, which the split's state records before any row.
     */
    public DvBucketSplit resolve() throws IOException {
        if (resolved == null) {
            resolved = new DvBucketSplit(reader.resolve());
        }
        return resolved;
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        return reader.pollBatch(timeout);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
