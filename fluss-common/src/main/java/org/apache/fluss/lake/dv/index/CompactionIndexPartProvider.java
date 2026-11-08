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

import org.apache.fluss.annotation.Internal;

import javax.annotation.Nullable;

/**
 * Implemented by lake write results of tables with deletion vectors whose writer may compact the
 * bucket's lake files. Tells whether the writer compacted and, if so, the positions it handed to
 * the index.
 */
@Internal
public interface CompactionIndexPartProvider {

    /** Whether the bucket's writer rewrote lake files in this round. */
    boolean compacted();

    /**
     * The index part of the bucket's compaction, or null if it did not compact or could not hand
     * the positions over. The rewritten files are then re-indexed from the lake.
     */
    @Nullable
    CompactionIndexPart compactionIndexPart();
}
