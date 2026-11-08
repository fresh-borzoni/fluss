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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.lake.dv.index.CompactionIndexPart;
import org.apache.fluss.lake.dv.index.CompactionIndexPartProvider;

import org.apache.paimon.table.sink.CommitMessage;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** The write result of Paimon lake writer to pass to committer to commit. */
public final class PaimonWriteResult implements Serializable, CompactionIndexPartProvider {

    private static final long serialVersionUID = 1L;

    private final List<CommitMessage> commitMessages;
    // whether the writer compacted the bucket, and the positions it handed to the index if so
    private final boolean compacted;
    @Nullable private final CompactionIndexPart compactionIndexPart;

    /** Creates a write result containing all commit messages produced by one lake writer. */
    public PaimonWriteResult(List<CommitMessage> commitMessages) {
        this(commitMessages, false, null);
    }

    /**
     * Creates a write result of a writer that may have compacted the bucket of a table with
     * deletion vectors, with the index part of its compaction if it handed the positions over.
     */
    public PaimonWriteResult(
            List<CommitMessage> commitMessages,
            boolean compacted,
            @Nullable CompactionIndexPart compactionIndexPart) {
        checkNotNull(commitMessages, "commitMessages must not be null");
        checkArgument(!commitMessages.isEmpty(), "commitMessages must not be empty");
        this.commitMessages = Collections.unmodifiableList(new ArrayList<>(commitMessages));
        this.compacted = compacted;
        this.compactionIndexPart = compactionIndexPart;
    }

    @Override
    public boolean compacted() {
        return compacted;
    }

    @Nullable
    @Override
    public CompactionIndexPart compactionIndexPart() {
        return compactionIndexPart;
    }

    /** Returns all commit messages produced by the lake writer. */
    public List<CommitMessage> commitMessages() {
        return commitMessages;
    }
}
