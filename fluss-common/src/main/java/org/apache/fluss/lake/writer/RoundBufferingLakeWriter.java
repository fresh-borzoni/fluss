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

/**
 * A {@link LakeWriter} that holds what a tiering round writes until the round completes, such as
 * the versions of the keys the round changed, in memory or spilled to local disk. Once the writer
 * holds as much as one round may, the tiering service ends the round of the writer's bucket early,
 * right after a record. The next round tiers the rest of the bucket's log from there.
 *
 * @since 1.1
 */
@PublicEvolving
public interface RoundBufferingLakeWriter {

    /**
     * Returns the number of distinct keys the writer holds for the current round, or an upper bound
     * of it.
     */
    long bufferedKeys();

    /** Returns an estimate of the bytes the writer holds for the current round. */
    long bufferedBytes();
}
