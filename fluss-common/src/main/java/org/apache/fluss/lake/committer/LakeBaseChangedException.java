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

package org.apache.fluss.lake.committer;

import org.apache.fluss.annotation.Internal;

/**
 * Thrown by a {@link LakeCommitter} when a commit on top of a base snapshot is rejected because
 * another engine committed to the lake table after the base. As for any {@link
 * LakeCommitRejectedException}, nothing of the commit is in the lake. The caller may plan the
 * commit again on the table's current snapshot instead of failing.
 */
@Internal
public class LakeBaseChangedException extends LakeCommitRejectedException {

    private static final long serialVersionUID = 1L;

    public LakeBaseChangedException(String message, Throwable cause) {
        super(message, cause);
    }
}
