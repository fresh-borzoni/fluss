/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.dv.index;

import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.FlussPaths;

import org.junit.jupiter.api.Test;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests where the position index of a table lives. */
class PositionIndexStoreLocationTest {

    private static final TablePath TABLE = TablePath.of("db", "t");
    private static final long TABLE_ID = 11L;

    /**
     * The index lives under the table's own remote data directory, which dropping the table
     * deletes, whatever directory a process has first in its configuration.
     */
    @Test
    void testIndexLivesUnderTheTablesRemoteDataDir() {
        TableInfo table = table("oss://bucket-b/fluss");

        assertThat(PositionIndexStore.remoteDataDirOf(table, () -> "oss://bucket-a/fluss"))
                .isEqualTo("oss://bucket-b/fluss");
        assertThat(PositionIndexStore.lakeTableDirOf(table, () -> "oss://bucket-a/fluss"))
                .isEqualTo(
                        FlussPaths.remoteLakeTableSnapshotDir(
                                "oss://bucket-b/fluss", TABLE, TABLE_ID));
    }

    /** A table created without a remote data directory of its own uses the default one. */
    @Test
    void testTableWithoutARemoteDataDirUsesTheDefault() {
        assertThat(PositionIndexStore.remoteDataDirOf(table(null), () -> "oss://bucket-a/fluss"))
                .isEqualTo("oss://bucket-a/fluss");
    }

    private static TableInfo table(String remoteDataDir) {
        return TableInfo.of(
                TABLE,
                TABLE_ID,
                1,
                TableDescriptor.builder().schema(DATA1_SCHEMA_PK).distributedBy(3, "a").build(),
                remoteDataDir,
                0L,
                0L);
    }
}
