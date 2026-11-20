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

package org.apache.fluss.lake.dv;

import org.apache.fluss.annotation.PublicEvolving;

import javax.annotation.Nullable;

/**
 * A lake write result that may carry the deletion vectors its tiering writer prepared for the
 * bucket's round ({@link org.apache.fluss.lake.writer.DeletionVectorPreparer}).
 *
 * @since 1.1
 */
@PublicEvolving
public interface PreparedDeletionVectorsProvider {

    /** The round the write result's deletion vectors were prepared for, or null if none were. */
    @Nullable
    PreparedDeletionVectors preparedDeletionVectors();
}
