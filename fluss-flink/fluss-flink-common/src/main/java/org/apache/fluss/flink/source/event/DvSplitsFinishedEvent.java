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

package org.apache.fluss.flink.source.event;

import org.apache.flink.api.connector.source.SourceEvent;

import java.util.Objects;
import java.util.Set;

/**
 * Tells the enumerator that a reader finished reading splits of a union read of a table with
 * deletion vectors. The enumerator releases their holds of the mask cuts it pinned for them.
 */
public class DvSplitsFinishedEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final Set<String> splitIds;

    public DvSplitsFinishedEvent(Set<String> splitIds) {
        this.splitIds = splitIds;
    }

    public Set<String> getSplitIds() {
        return splitIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return Objects.equals(splitIds, ((DvSplitsFinishedEvent) o).splitIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitIds);
    }

    @Override
    public String toString() {
        return "DvSplitsFinishedEvent{splitIds=" + splitIds + '}';
    }
}
