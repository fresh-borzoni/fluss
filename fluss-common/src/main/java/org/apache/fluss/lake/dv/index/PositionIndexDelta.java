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

package org.apache.fluss.lake.dv.index;

import java.io.Serializable;
import java.util.List;

/**
 * One bucket's position-index change for one tiering round, built by the lake writer. The base
 * snapshot's rows of {@code changedKeys} must be deleted in the same lake commit; {@code puts} are
 * the live rows written in the round and {@code deletes} the keys it ends with deleted.
 */
public final class PositionIndexDelta implements Serializable {
    private static final long serialVersionUID = 1L;

    private final List<byte[]> changedKeys;
    private final List<KeyPosition> puts;
    private final List<byte[]> deletes;

    public PositionIndexDelta(
            List<byte[]> changedKeys, List<KeyPosition> puts, List<byte[]> deletes) {
        this.changedKeys = changedKeys;
        this.puts = puts;
        this.deletes = deletes;
    }

    public List<byte[]> changedKeys() {
        return changedKeys;
    }

    public List<KeyPosition> puts() {
        return puts;
    }

    public List<byte[]> deletes() {
        return deletes;
    }

    @Override
    public String toString() {
        return "PositionIndexDelta{changed="
                + changedKeys.size()
                + ", puts="
                + puts.size()
                + ", deletes="
                + deletes.size()
                + '}';
    }
}
