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

import java.io.Serializable;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * How a scan cuts its changes into epochs, and how its lookups read them: the domain of the epochs
 * and their length.
 *
 * <p>Epoch numbers come from the wall clock, the current time divided by the interval, so that the
 * scans of different tables count the same epochs at about the same time. A scan ends every epoch
 * it passes; a number in which no change fell is skipped.
 */
public final class EpochSettings implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String domain;
    private final long intervalMs;
    private final long closeAfterMs;

    public EpochSettings(String domain, long intervalMs) {
        this(domain, intervalMs, 0L);
    }

    /**
     * @param closeAfterMs if positive, epochs end that long after they open, once they hold a
     *     change, as the job's coordinators decide; otherwise every {@code intervalMs} by the wall
     *     clock
     */
    public EpochSettings(String domain, long intervalMs, long closeAfterMs) {
        this.domain = checkNotNull(domain, "domain");
        this.closeAfterMs = Math.max(0L, closeAfterMs);
        checkArgument(
                intervalMs > 0, "The epoch interval must be positive, but is %s.", intervalMs);
        this.intervalMs = intervalMs;
    }

    /** The domain that the scans and lookups that count the same epochs share. */
    public String getDomain() {
        return domain;
    }

    public long getIntervalMs() {
        return intervalMs;
    }

    /** If positive, epochs end that long after they open, see {@link CoordinatedEpochs}. */
    public long getCloseAfterMs() {
        return closeAfterMs;
    }

    /** The epoch at the given wall-clock time. */
    public long epochAt(long timeMs) {
        return timeMs / intervalMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EpochSettings)) {
            return false;
        }
        EpochSettings that = (EpochSettings) o;
        return intervalMs == that.intervalMs
                && closeAfterMs == that.closeAfterMs
                && domain.equals(that.domain);
    }

    @Override
    public int hashCode() {
        return Objects.hash(domain, intervalMs, closeAfterMs);
    }

    @Override
    public String toString() {
        return "EpochSettings{domain='"
                + domain
                + "', intervalMs="
                + intervalMs
                + ", closeAfterMs="
                + closeAfterMs
                + '}';
    }
}
