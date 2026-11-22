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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableInfo;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * A {@link LakeTieringFactory} whose tiering writers can prepare the deletion vectors of their
 * buckets' rounds ({@link DeletionVectorPreparer}). Its lake committers then implement {@link
 * org.apache.fluss.lake.committer.SupportsPositionDeleteCommit} and commit them.
 *
 * @param <WriteResult> the write result type of the lake writer
 * @since 1.1
 */
@PublicEvolving
public interface SupportsDeletionVectorPreparation<WriteResult> {

    /**
     * Creates the preparer of a table with deletion vectors, with the tiering service's options
     * {@code lakeTieringConfig}, or returns null if its writers do not prepare them, e.g. the lake
     * table has no deletion vectors.
     */
    @Nullable
    DeletionVectorPreparer<WriteResult> createDeletionVectorPreparer(
            TableInfo tableInfo, Configuration lakeTieringConfig) throws IOException;
}
