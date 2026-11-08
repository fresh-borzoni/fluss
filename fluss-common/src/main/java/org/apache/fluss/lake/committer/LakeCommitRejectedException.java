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

import org.apache.fluss.annotation.PublicEvolving;

import java.io.IOException;

/**
 * Thrown by a {@link LakeCommitter} when the lake definitely did not take a commit, e.g. it failed
 * validation against a conflicting commit. Nothing of the commit is in the lake, so the files
 * written for it can be deleted. Any other commit failure leaves the outcome unknown, and the files
 * must be kept, as a committed snapshot may reference them.
 *
 * @since 1.1
 */
@PublicEvolving
public class LakeCommitRejectedException extends IOException {

    private static final long serialVersionUID = 1L;

    public LakeCommitRejectedException(String message, Throwable cause) {
        super(message, cause);
    }
}
