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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;

/** Reads a {@link DvBucketSplit} with the {@link DvBucketPartReader} of its part. */
@Internal
public class DvBucketSplitScanner implements BatchScanner {

    private final DvBucketPartReader reader;
    @Nullable private DvBucketSplit resolved;

    public DvBucketSplitScanner(DvBucketSplit split, DvBucketPartReader.Context context) {
        this(split, context, null);
    }

    /** A scanner that adds where its read's time went to {@code metrics} once the read ends. */
    public DvBucketSplitScanner(
            DvBucketSplit split,
            DvBucketPartReader.Context context,
            @Nullable ReadMetrics metrics) {
        this.reader = new DvBucketPartReader(split.getPart(), context);
        if (metrics != null) {
            reader.onReadEnd(metrics::record);
        }
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

    /**
     * The source reader's counters of the splits of union reads of tables with deletion vectors, in
     * the group {@code dvUnionRead}: the splits read, and the sums of the times and rows their
     * parts' log lines show ({@link DvBucketPartReader.ReadStats}).
     */
    public static final class ReadMetrics {

        private final Counter splits;
        private final Counter totalMs;
        private final Counter resolveMs;
        private final Counter maskFetchMs;
        private final Counter lakeOpenMs;
        private final Counter lakeRowsRead;
        private final Counter lakeRowsMasked;
        private final Counter logReadMs;
        private final Counter logRecords;
        private final Counter logRecordsMasked;
        private final Counter replansBehind;
        private final Counter foldedReads;
        private final Counter deriveMs;

        public ReadMetrics(MetricGroup sourceReaderGroup) {
            MetricGroup group = sourceReaderGroup.addGroup("dvUnionRead");
            this.splits = group.counter("splits");
            this.totalMs = group.counter("totalMs");
            this.resolveMs = group.counter("resolveMs");
            this.maskFetchMs = group.counter("maskFetchMs");
            this.lakeOpenMs = group.counter("lakeOpenMs");
            this.lakeRowsRead = group.counter("lakeRowsRead");
            this.lakeRowsMasked = group.counter("lakeRowsMasked");
            this.logReadMs = group.counter("logReadMs");
            this.logRecords = group.counter("logRecords");
            this.logRecordsMasked = group.counter("logRecordsMasked");
            this.replansBehind = group.counter("replansBehind");
            this.foldedReads = group.counter("foldedReads");
            this.deriveMs = group.counter("deriveMs");
        }

        void record(DvBucketPartReader.ReadStats stats) {
            splits.inc();
            totalMs.inc(stats.totalMs());
            resolveMs.inc(stats.resolveMs());
            maskFetchMs.inc(stats.maskFetchMs());
            lakeOpenMs.inc(stats.lakeOpenMs());
            lakeRowsRead.inc(stats.lakeRowsRead());
            lakeRowsMasked.inc(stats.lakeRowsMasked());
            logReadMs.inc(stats.logReadMs());
            logRecords.inc(stats.logRecords());
            logRecordsMasked.inc(stats.logRecordsMasked());
            replansBehind.inc(stats.replansBehind());
            if (stats.isFolded()) {
                foldedReads.inc();
            }
            deriveMs.inc(stats.deriveMs());
        }
    }
}
