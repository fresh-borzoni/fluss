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

package org.apache.fluss.flink.tiering.source;

import org.apache.fluss.lake.writer.DeletionVectorPreparer;
import org.apache.fluss.utils.IOUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The deletion vector preparers of the tables a tiering source reader reads, by table id, empty for
 * a table without one. Flink closes an idle split reader and creates a new one for the next split,
 * so the split readers of a source reader share them, and the source reader closes them.
 */
final class DeletionVectorPreparers<WriteResult> implements AutoCloseable {

    private final Map<Long, Optional<DeletionVectorPreparer<WriteResult>>> byTable =
            new ConcurrentHashMap<>();

    /** The table's preparer, empty if it has none, or null if none was created yet. */
    @Nullable
    Optional<DeletionVectorPreparer<WriteResult>> get(long tableId) {
        return byTable.get(tableId);
    }

    void put(long tableId, Optional<DeletionVectorPreparer<WriteResult>> preparer) {
        Optional<DeletionVectorPreparer<WriteResult>> replaced = byTable.put(tableId, preparer);
        if (replaced != null && replaced.isPresent()) {
            IOUtils.closeQuietly(replaced.get());
        }
    }

    @Override
    public void close() throws Exception {
        List<AutoCloseable> preparers = new ArrayList<>();
        for (Optional<DeletionVectorPreparer<WriteResult>> preparer : byTable.values()) {
            preparer.ifPresent(preparers::add);
        }
        byTable.clear();
        IOUtils.closeAll(preparers);
    }
}
