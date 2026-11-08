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

import java.util.concurrent.atomic.AtomicLong;

/**
 * The highest cut a bucket's leader served to a union read, with masks or to read by key up to. No
 * read, waiting or not, is served a lower cut, so a bucket's reads never go back in time, also
 * across a leader change: a new leader starts at what the bucket had when it took over.
 *
 * <p>Every answer passes {@link #tryServe}, which raises the served cut by compare-and-set and
 * refuses a cut below it. A read whose cut was refused, as a concurrent read served a higher one,
 * looks for masks again.
 */
final class ServedCuts {

    private final AtomicLong served = new AtomicLong(-1);

    /** The highest cut served. */
    long get() {
        return served.get();
    }

    /**
     * Serves {@code cut}: raises the served cut to it. Returns false, serving nothing, if a higher
     * cut was served.
     */
    boolean tryServe(long cut) {
        while (true) {
            long current = served.get();
            if (cut < current) {
                return false;
            }
            if (cut == current || served.compareAndSet(current, cut)) {
                return true;
            }
        }
    }

    /** Raises the served cut to at least {@code cut}: a cut handed out, or a takeover's floor. */
    void raise(long cut) {
        served.accumulateAndGet(cut, Math::max);
    }
}
