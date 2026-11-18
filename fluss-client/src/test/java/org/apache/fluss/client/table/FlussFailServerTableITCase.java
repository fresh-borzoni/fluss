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

package org.apache.fluss.client.table;

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.apache.fluss.testutils.InternalRowListAssert.assertThatRows;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** IT case for {@link FlussTable} in the case of one tablet server fails. */
class FlussFailServerTableITCase extends ClientToServerITCaseBase {

    private static final int SERVER = 0;

    @BeforeEach
    void beforeEach() throws Exception {
        // since we kill and start one tablet server in each test,
        // we need to wait for metadata to be updated to servers
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();
        super.setup();
    }

    @Test
    void testAppend() throws Exception {
        createTable(DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR, false);
        try (Table table = conn.getTable(DATA1_TABLE_PATH)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            GenericRow row = row(1, "a");

            // append a row
            appendWriter.append(row).get();

            // now, kill one server and try to append data again
            FLUSS_CLUSTER_EXTENSION.stopTabletServer(SERVER);

            try {
                // append some rows again, should success
                for (int i = 0; i < 10; i++) {
                    appendWriter.append(row).get();
                }
            } finally {
                FLUSS_CLUSTER_EXTENSION.startTabletServer(SERVER);
            }
        }
    }

    @Test
    void testPut() throws Exception {
        createTable(DATA1_TABLE_PATH_PK, DATA1_TABLE_DESCRIPTOR_PK, false);
        // put one row
        try (Table table = conn.getTable(DATA1_TABLE_PATH_PK)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            InternalRow row = row(1, "a");
            upsertWriter.upsert(row).get();

            // kill first tablet server
            FLUSS_CLUSTER_EXTENSION.stopTabletServer(SERVER);

            try {
                // append some rows again, should success
                for (int i = 0; i < 10; i++) {
                    // mock a row
                    upsertWriter.upsert(row(i, "a" + i)).get();
                }
            } finally {
                // todo: try to get value when get is re-triable in FLUSS-56857409
                FLUSS_CLUSTER_EXTENSION.startTabletServer(SERVER);
            }
        }
    }

    /**
     * An upsert whose write was acknowledged is visible to lookups also after the leader of its
     * bucket changed: a lookup never reads a state without an acknowledged write. Lookups that read
     * the rows a sink wrote rely on this.
     */
    @Test
    void testAcknowledgedUpsertsAreVisibleAfterLeaderChange() throws Exception {
        long tableId = createTable(DATA1_TABLE_PATH_PK, DATA1_TABLE_DESCRIPTOR_PK, false);
        TableBucket bucket = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(bucket);
        int leader = leaderOf(bucket);
        try (Table table = conn.getTable(DATA1_TABLE_PATH_PK)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            int rows = 30;
            for (String version : new String[] {"v1", "v2"}) {
                for (int i = 0; i < rows; i++) {
                    // acknowledged one by one: every write is acknowledged before the next
                    upsertWriter.upsert(row(i, version)).get();
                }
            }

            FLUSS_CLUSTER_EXTENSION.stopTabletServer(leader);
            try {
                retry(
                        Duration.ofMinutes(1),
                        () -> assertThat(leaderOf(bucket)).isNotEqualTo(leader));
                Lookuper lookuper = table.newLookup().createLookuper();
                for (int i = 0; i < rows; i++) {
                    assertThatRow(lookupRow(lookuper, row(i)))
                            .withSchema(DATA1_ROW_TYPE)
                            .isEqualTo(row(i, "v2"));
                }
            } finally {
                FLUSS_CLUSTER_EXTENSION.startTabletServer(leader);
            }
        }
    }

    private static int leaderOf(TableBucket bucket) throws Exception {
        return FLUSS_CLUSTER_EXTENSION
                .getZooKeeperClient()
                .getLeaderAndIsr(bucket)
                .orElseThrow(() -> new IllegalStateException("No leader of " + bucket))
                .leader();
    }

    @Test
    void testLogScan() throws Exception {
        createTable(DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR, false);
        // append one row.
        GenericRow row = row(1, "a");
        try (Table table = conn.getTable(DATA1_TABLE_PATH);
                LogScanner logScanner = createLogScanner(table)) {
            subscribeFromBeginning(logScanner, table);
            AppendWriter appendWriter = table.newAppend().createWriter();
            appendWriter.append(row).get();

            // poll data util we get one record
            ScanRecords scanRecords;
            do {
                scanRecords = logScanner.poll(Duration.ofSeconds(1));
            } while (scanRecords.isEmpty());

            int rowCount = 10;
            // append some rows
            List<GenericRow> expectRows = new ArrayList<>(rowCount);
            for (int i = 0; i < rowCount; i++) {
                appendWriter.append(row).get();
                expectRows.add(row);
            }
            // kill first tablet server
            FLUSS_CLUSTER_EXTENSION.stopTabletServer(SERVER);

            try {
                // now, poll records utils we can get all the records
                int counts = 0;
                int expectCounts = 10;
                List<InternalRow> actualRows = new ArrayList<>(rowCount);
                do {
                    scanRecords = logScanner.poll(Duration.ofSeconds(1));
                    actualRows.addAll(toRows(scanRecords));
                    counts += scanRecords.count();
                } while (counts < expectCounts);
                assertThatRows(actualRows).withSchema(DATA1_ROW_TYPE).isEqualTo(expectRows);
            } finally {
                FLUSS_CLUSTER_EXTENSION.startTabletServer(SERVER);
            }
        }
    }

    private List<InternalRow> toRows(ScanRecords scanRecords) {
        List<InternalRow> rows = new ArrayList<>();
        for (ScanRecord scanRecord : scanRecords) {
            rows.add(scanRecord.getRow());
        }
        return rows;
    }
}
