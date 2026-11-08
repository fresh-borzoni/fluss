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

package org.apache.fluss.lake.writer;

import org.apache.fluss.annotation.PublicEvolving;

import javax.annotation.Nullable;

/**
 * A {@link WriterInitContext} with what Fluss gives the lake writers of a tiering task beyond the
 * plain context: the position index directory of a table with deletion vectors, and the memory the
 * writers share. Fluss gives every lake writer this context, of every table; a lake writer checks
 * for it with {@code instanceof}, so a writer of another engine's tiering may get a plain {@link
 * WriterInitContext}.
 *
 * @since 1.1
 */
@PublicEvolving
public interface DeletionVectorWriterInitContext extends WriterInitContext {

    /**
     * Returns the remote directory of the position index of a table with deletion vectors, or null
     * for other tables or if unknown. It is the table's lake directory under the Fluss remote data
     * directory. A lake writer stores there the positions of the rows its compaction rewrote.
     *
     * @return the position index directory, or null
     */
    @Nullable
    String positionIndexDir();

    /**
     * Returns the memory that the lake writers of the tiering task share for what they buffer of a
     * round, of any table, or null if the lake writer should use its own.
     *
     * @return the shared writer memory, or null
     */
    @Nullable
    WriterMemoryPool writerMemoryPool();
}
