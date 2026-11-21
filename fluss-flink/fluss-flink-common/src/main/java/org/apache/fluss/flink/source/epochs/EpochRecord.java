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

package org.apache.fluss.flink.source.epochs;

import org.apache.fluss.metadata.TableBucket;

import java.util.Objects;

/**
 * A row of the epochs of a bucket that a reader records: the end of an epoch in which the bucket
 * changed, or how far the reader has ended epochs in the bucket.
 */
public final class EpochRecord {

    private final TableBucket bucket;
    private final boolean progress;
    private final long epoch;
    private final long end;

    private EpochRecord(TableBucket bucket, boolean progress, long epoch, long end) {
        this.bucket = bucket;
        this.progress = progress;
        this.epoch = epoch;
        this.end = end;
    }

    /** The end of an epoch in which the bucket changed, or of {@link Epochs#BEFORE_FIRST}. */
    public static EpochRecord end(TableBucket bucket, long epoch, long end) {
        return new EpochRecord(bucket, false, epoch, end);
    }

    /** The reader ended every epoch up to the given one in the bucket. */
    public static EpochRecord progress(TableBucket bucket, long endedThrough) {
        return new EpochRecord(bucket, true, endedThrough, 0L);
    }

    public TableBucket getBucket() {
        return bucket;
    }

    /** Whether this records progress, else the end of an epoch. */
    public boolean isProgress() {
        return progress;
    }

    /** The epoch that ended, or the last ended epoch of a progress record. */
    public long getEpoch() {
        return epoch;
    }

    /** The position of the end; not set for progress. */
    public long getEnd() {
        return end;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EpochRecord)) {
            return false;
        }
        EpochRecord that = (EpochRecord) o;
        return progress == that.progress
                && epoch == that.epoch
                && end == that.end
                && bucket.equals(that.bucket);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucket, progress, epoch, end);
    }

    @Override
    public String toString() {
        return progress
                ? bucket + " ended through epoch " + epoch
                : bucket + ": epoch " + epoch + " ends at " + Epochs.positionToString(end);
    }
}
