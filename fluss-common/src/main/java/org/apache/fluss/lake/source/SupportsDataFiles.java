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

package org.apache.fluss.lake.source;

import org.apache.fluss.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.util.List;

/**
 * A {@link LakeSplit} that names the data files it reads and counts their rows. Deletion vectors
 * are keyed by data file, so tables with deletion vectors need it to apply the masks of a split's
 * own files and to split work over lake files. Fluss checks a split for this capability before it
 * applies deletion vectors to it, and fails with a clear error otherwise.
 *
 * <p>The splits of a lake source that implements {@link SupportsRewritePlanning} implement it.
 *
 * @since 1.1
 */
@PublicEvolving
public interface SupportsDataFiles {

    /**
     * Returns the names of the data files this split reads. The position index and the union-read
     * masks refer to files by these names, and each row read with {@link
     * SupportsRowPositions#readWithPos()} names its file so. A split of several data files is
     * masked by the masks of its own files only.
     *
     * @return the file names, or null if unknown
     */
    @Nullable
    List<String> fileNames();

    /**
     * Returns the number of rows in the data files of this split as their metadata gives it,
     * deleted rows included. Work over lake files, such as re-indexing them, is split by it.
     *
     * @return the number of rows, or -1 if unknown
     */
    long rowCount();
}
