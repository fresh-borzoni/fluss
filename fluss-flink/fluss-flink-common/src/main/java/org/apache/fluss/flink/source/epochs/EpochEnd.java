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

import java.util.Objects;

/**
 * The end of an epoch in one bucket: the position of the bucket after the last change of the epoch,
 * see {@link Epochs}.
 */
public final class EpochEnd {

    private final long epoch;
    private final long end;

    public EpochEnd(long epoch, long end) {
        this.epoch = epoch;
        this.end = end;
    }

    public long getEpoch() {
        return epoch;
    }

    public long getEnd() {
        return end;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EpochEnd)) {
            return false;
        }
        EpochEnd that = (EpochEnd) o;
        return epoch == that.epoch && end == that.end;
    }

    @Override
    public int hashCode() {
        return Objects.hash(epoch, end);
    }

    @Override
    public String toString() {
        return "epoch " + epoch + " ends at " + Epochs.positionToString(end);
    }
}
