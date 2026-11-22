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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.HashMap;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the cache of {@link LiveFilesIndex} the committer and the preparers of a TaskManager share.
 */
class LiveFilesIndexTest {

    @TempDir private File warehouse;

    private HadoopCatalog catalog;

    @BeforeEach
    void beforeEach() {
        catalog = new HadoopCatalog();
        catalog.setConf(new org.apache.hadoop.conf.Configuration());
        HashMap<String, String> properties = new HashMap<>();
        properties.put("warehouse", "file://" + warehouse);
        catalog.initialize("test", properties);
    }

    /**
     * The cache holds the indexes of at most its budget of files over all tables, and drops the
     * least recently used tables' indexes first, e.g. of tables dropped or no longer tiered here.
     */
    @Test
    void testTheCacheHoldsAtMostItsBudgetOfFiles() throws Exception {
        LiveFilesIndex.Cache cache = new LiveFilesIndex.Cache(4);
        Table first = tableWithFiles("first", 2);
        Table second = tableWithFiles("second", 2);
        Table third = tableWithFiles("third", 2);

        index(cache, first);
        index(cache, second);
        assertThat(cache.cachedFiles()).isEqualTo(4);
        index(cache, third);
        assertThat(cache.cachedFiles()).isEqualTo(4);
        assertThat(cache.scans()).isEqualTo(3);

        // the third and second are cached, the first was dropped
        index(cache, third);
        index(cache, second);
        assertThat(cache.scans()).isEqualTo(3);
        index(cache, first);
        assertThat(cache.scans()).isEqualTo(4);
        assertThat(cache.cachedFiles()).isEqualTo(4);

        // a table with more files than the budget is not cached
        Table large = tableWithFiles("large", 5);
        index(cache, large);
        index(cache, large);
        assertThat(cache.scans()).isEqualTo(6);
        assertThat(cache.cachedFiles()).isEqualTo(4);
    }

    /**
     * The committer and the preparers of a TaskManager share one cache. A reader of a later
     * snapshot advances the table's index, and one of an earlier snapshot, as from a preparer's
     * round on a base the committer committed past, gets that snapshot's files without moving the
     * index back: the reader of the later snapshot after it does not plan it again.
     */
    @Test
    void testAReaderOfAnEarlierSnapshotDoesNotMoveTheSharedIndexBack() throws Exception {
        LiveFilesIndex.Cache shared = new LiveFilesIndex.Cache();
        Table committerTable = tableWithFiles("shared", 2);
        long first = committerTable.currentSnapshot().snapshotId();
        appendFile(committerTable);
        long second = committerTable.currentSnapshot().snapshotId();
        appendFile(committerTable);
        long third = committerTable.currentSnapshot().snapshotId();
        // a preparer's own handle on the table
        Table preparerTable = catalog.loadTable(TableIdentifier.of("db", "shared"));

        assertThat(filesOf(shared, committerTable, second)).isEqualTo(3);
        assertThat(shared.scans()).isEqualTo(1);
        // advances the index of the second snapshot to the third
        assertThat(filesOf(shared, preparerTable, third)).isEqualTo(4);
        assertThat(shared.scans()).isEqualTo(1);

        // an earlier snapshot is planned, and the index stays on the third
        assertThat(filesOf(shared, preparerTable, first)).isEqualTo(2);
        assertThat(shared.scans()).isEqualTo(2);
        assertThat(filesOf(shared, committerTable, third)).isEqualTo(4);
        assertThat(shared.scans()).isEqualTo(2);

        // and goes on from there
        appendFile(committerTable);
        assertThat(filesOf(shared, committerTable, committerTable.currentSnapshot().snapshotId()))
                .isEqualTo(5);
        assertThat(shared.scans()).isEqualTo(2);
    }

    private static int filesOf(LiveFilesIndex.Cache cache, Table table, long snapshotId)
            throws Exception {
        return LiveFilesIndex.read(cache, table, snapshotId, live -> live.locations().size());
    }

    private void appendFile(Table table) {
        table.newAppend()
                .appendFile(
                        DataFiles.builder(table.spec())
                                .withPath(warehouse + "/" + UUID.randomUUID() + ".parquet")
                                .withFileSizeInBytes(10)
                                .withRecordCount(1)
                                .build())
                .commit();
    }

    private static void index(LiveFilesIndex.Cache cache, Table table) throws Exception {
        table.refresh();
        LiveFilesIndex.of(cache, table, table.currentSnapshot().snapshotId());
    }

    private Table tableWithFiles(String name, int files) {
        Table table =
                catalog.createTable(
                        TableIdentifier.of("db", name),
                        new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
                        PartitionSpec.unpartitioned());
        org.apache.iceberg.AppendFiles append = table.newAppend();
        for (int i = 0; i < files; i++) {
            append.appendFile(
                    DataFiles.builder(table.spec())
                            .withPath(warehouse + "/" + UUID.randomUUID() + ".parquet")
                            .withFileSizeInBytes(10)
                            .withRecordCount(1)
                            .build());
        }
        append.commit();
        return table;
    }
}
