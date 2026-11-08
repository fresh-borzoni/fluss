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

package org.apache.fluss.flink.tiering.committer;

import javax.annotation.Nullable;

/**
 * Unwinds a round that waits for a re-index, or must run again: on a published re-index, or on the
 * snapshot the lake table moved to during the round.
 */
final class ReindexPending extends Exception {
    private static final long serialVersionUID = 1L;

    // null if the re-index is already published, so the round runs again at once
    @Nullable final transient Reindex reindex;
    // whether the round runs again because the lake rejected it on a moved base
    final boolean baseChanged;

    ReindexPending(@Nullable Reindex reindex) {
        this(reindex, false);
    }

    private ReindexPending(@Nullable Reindex reindex, boolean baseChanged) {
        super(null, null, false, false);
        this.reindex = reindex;
        this.baseChanged = baseChanged;
    }

    static ReindexPending baseChanged() {
        return new ReindexPending(null, true);
    }
}
