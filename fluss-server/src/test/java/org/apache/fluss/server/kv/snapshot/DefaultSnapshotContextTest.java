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

package org.apache.fluss.server.kv.snapshot;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.IllegalConfigurationException;
import org.apache.fluss.server.kv.KvSnapshotResource;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DefaultSnapshotContext}. */
class DefaultSnapshotContextTest {

    /**
     * A tablet server without a remote data directory starts: only a bucket with deletion vectors
     * asks for the directory, and fails clearly then.
     */
    @Test
    void testRemoteDataDirIsResolvedWhenFirstAsked() {
        ExecutorService ioExecutor = Executors.newSingleThreadExecutor();
        Configuration withoutRemoteDir = new Configuration();
        KvSnapshotResource resource = KvSnapshotResource.create(0, withoutRemoteDir, ioExecutor);
        try {
            DefaultSnapshotContext context =
                    DefaultSnapshotContext.create(
                            null,
                            new TestingCompletedKvSnapshotCommitter(),
                            resource,
                            withoutRemoteDir);
            assertThatThrownBy(context::getRemoteDataDir)
                    .isInstanceOf(IllegalConfigurationException.class)
                    .hasMessageContaining(ConfigOptions.REMOTE_DATA_DIR.key());

            Configuration withRemoteDir = new Configuration();
            withRemoteDir.set(ConfigOptions.REMOTE_DATA_DIR, "/tmp/fluss-remote");
            assertThat(
                            DefaultSnapshotContext.create(
                                            null,
                                            new TestingCompletedKvSnapshotCommitter(),
                                            resource,
                                            withRemoteDir)
                                    .getRemoteDataDir())
                    .isEqualTo("/tmp/fluss-remote");
        } finally {
            resource.close();
            ioExecutor.shutdownNow();
        }
    }
}
