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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.WriterInitContext;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Paimon tiering factory whose writers can fail the hand-off of their compaction's positions, so
 * that their rounds re-index the compaction instead. A failed hand-off leaves the write result
 * without its index part, as a hand-off that throws does.
 */
public class HandOffFailingTieringFactory extends PaimonLakeTieringFactory {

    private static final long serialVersionUID = 1L;

    // shared by the writers of the test's mini cluster, which run in its JVM
    private static final AtomicInteger FAILURES_TO_INJECT = new AtomicInteger();

    private final PaimonCatalogProvider catalogProvider;

    public HandOffFailingTieringFactory(Configuration paimonConfig) {
        super(paimonConfig);
        this.catalogProvider = new PaimonCatalogProvider(paimonConfig);
    }

    /** Fails the next {@code count} hand-offs. */
    public static void failNextHandOffs(int count) {
        FAILURES_TO_INJECT.set(count);
    }

    @Override
    public LakeWriter<PaimonWriteResult> createLakeWriter(WriterInitContext writerInitContext)
            throws IOException {
        return new PaimonLakeWriter(catalogProvider, writerInitContext) {
            @Override
            public PaimonWriteResult complete() throws IOException {
                PaimonWriteResult result = super.complete();
                if (result.compactionIndexPart() != null
                        && FAILURES_TO_INJECT.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                    return new PaimonWriteResult(result.commitMessages(), true, null);
                }
                return result;
            }
        };
    }
}
