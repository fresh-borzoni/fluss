/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.tiering.source;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.tiering.TableBucketWriteResult;
import org.apache.fluss.client.tiering.TieringLogSplit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.flink.tiering.TestingLakeTieringFactory;
import org.apache.fluss.flink.tiering.TestingWriteResult;
import org.apache.fluss.flink.tiering.event.TieringReachMaxDurationEvent;
import org.apache.fluss.flink.tiering.source.split.FlinkTieringSplit;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.lake.writer.DeletionVectorWriterInitContext;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TieringSourceReader}. */
class TieringSourceReaderTest extends FlinkTestBase {

    /**
     * The deletion vector options of the tiering service, such as {@code
     * --lake.tiering.deletion-vectors.write-buffer-size}, are options of the tiering job: its
     * readers take them from the job's lake tiering config, not from the Fluss client config.
     */
    @Test
    void testDeletionVectorOptionsComeFromTheLakeTieringConfig() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "test_tiering_dv_options");
        long tableId =
                createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(DEFAULT_PK_TABLE_SCHEMA)
                                .distributedBy(1, "id")
                                .build());
        Configuration flussConf = new Configuration(FLUSS_CLUSTER_EXTENSION.getClientConfig());
        try (Connection connection = ConnectionFactory.createConnection(flussConf)) {
            writeRows(
                    connection,
                    tablePath,
                    Arrays.asList(row(0, "v0"), row(1, "v1"), row(2, "v2")),
                    false);
        }
        Configuration lakeTieringConfig = new Configuration();
        lakeTieringConfig.setString(
                ConfigOptions.LAKE_TIERING_DELETION_VECTORS_WRITE_BUFFER_SIZE.key(), "32kb");
        List<WriterInitContext> contexts = new CopyOnWriteArrayList<>();
        TestingLakeTieringFactory factory =
                new TestingLakeTieringFactory() {
                    @Override
                    public LakeWriter<TestingWriteResult> createLakeWriter(
                            WriterInitContext writerInitContext) throws IOException {
                        contexts.add(writerInitContext);
                        return super.createLakeWriter(writerInitContext);
                    }
                };
        TieringSource<TestingWriteResult> source =
                new TieringSource.Builder<>(flussConf, lakeTieringConfig, factory).build();
        try (SourceReader<TableBucketWriteResult<TestingWriteResult>, FlinkTieringSplit> reader =
                source.createReader(new TestingReaderContext())) {
            reader.start();
            reader.addSplits(
                    Collections.singletonList(
                            new FlinkTieringSplit(
                                    new TieringLogSplit(
                                            tablePath,
                                            new TableBucket(tableId, 0),
                                            null,
                                            EARLIEST_OFFSET,
                                            3))));
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        reader.pollNext(new TestingReaderOutput<>());
                        assertThat(contexts).isNotEmpty();
                    });
            assertThat(
                            ((DeletionVectorWriterInitContext) contexts.get(0))
                                    .writerMemoryPool()
                                    .capacity())
                    .isEqualTo(MemorySize.parse("32kb").getBytes());
        }
    }

    @Test
    void testHandleTieringReachMaxDurationEvent() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "test_tiering_reach_max_duration");
        long tableId = createTable(tablePath, DEFAULT_LOG_TABLE_DESCRIPTOR);
        Configuration conf = new Configuration(FLUSS_CLUSTER_EXTENSION.getClientConfig());
        conf.set(
                ConfigOptions.CLIENT_WRITER_BUCKET_NO_KEY_ASSIGNER,
                ConfigOptions.NoKeyAssigner.ROUND_ROBIN);
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            FutureCompletingBlockingQueue<
                            RecordsWithSplitIds<TableBucketWriteResult<TestingWriteResult>>>
                    elementsQueue = new FutureCompletingBlockingQueue<>(16);
            TestingReaderContext readerContext = new TestingReaderContext();
            try (TieringSourceReader<TestingWriteResult> reader =
                    new TieringSourceReader<>(
                            elementsQueue,
                            readerContext,
                            connection,
                            new TestingLakeTieringFactory(),
                            Duration.ofMillis(500))) {

                reader.start();

                // no data, add a split for the table,
                // should be force be complete after reach max duration
                TieringLogSplit split =
                        new TieringLogSplit(
                                tablePath, new TableBucket(tableId, 0), null, EARLIEST_OFFSET, 100);
                reader.addSplits(Collections.singletonList(new FlinkTieringSplit(split)));

                // send TieringReachMaxDurationEvent
                TieringReachMaxDurationEvent event = new TieringReachMaxDurationEvent(tableId);
                reader.handleSourceEvents(event);

                retry(
                        Duration.ofMinutes(1),
                        () -> {
                            TestingReaderOutput<TableBucketWriteResult<TestingWriteResult>> output =
                                    new TestingReaderOutput<>();
                            // should force to finish, the write result is null
                            reader.pollNext(output);
                            assertThat(output.getEmittedRecords()).hasSize(1);
                            TableBucketWriteResult<TestingWriteResult> result =
                                    output.getEmittedRecords().get(0);
                            assertThat(result.writeResult()).isNull();
                        });

                // write some data
                writeRows(
                        connection,
                        tablePath,
                        Arrays.asList(row(0, "v0"), row(1, "v1"), row(2, "v2")),
                        true);
                split =
                        new TieringLogSplit(
                                tablePath,
                                new TableBucket(tableId, 2),
                                null,
                                EARLIEST_OFFSET,
                                // use 100L as end offset, so that
                                // tiering won't be finished if no tiering reach max duration logic
                                100L);

                reader.addSplits(Collections.singletonList(new FlinkTieringSplit(split)));

                // wait to run one round of tiering to do some tiering
                FutureCompletingBlockingQueue<
                                RecordsWithSplitIds<TableBucketWriteResult<TestingWriteResult>>>
                        blockingQueue = getElementsQueue(reader);
                // wait blockingQueue is not empty to make sure we have one fetch
                // in tiering source reader
                waitUntil(
                        () -> !blockingQueue.isEmpty(),
                        Duration.ofSeconds(30),
                        "Fail to wait element queue is not empty.");

                // send TieringReachMaxDurationEvent
                event = new TieringReachMaxDurationEvent(tableId);
                reader.handleSourceEvents(event);

                // make sure tiering will be finished, still maintain the result
                // of previous tiering
                retry(
                        Duration.ofMinutes(1),
                        () -> {
                            TestingReaderOutput<TableBucketWriteResult<TestingWriteResult>>
                                    output1 = new TestingReaderOutput<>();

                            // should force to finish, the write result isn't null
                            reader.pollNext(output1);
                            assertThat(output1.getEmittedRecords()).hasSize(1);
                            TableBucketWriteResult<TestingWriteResult> result =
                                    output1.getEmittedRecords().get(0);
                            TestingWriteResult testingWriteResult = result.writeResult();
                            assertThat(testingWriteResult).isNotNull();
                            assertThat(result.logEndOffset()).isEqualTo(1);
                        });

                // test add split with skipCurrentRound
                split =
                        new TieringLogSplit(
                                tablePath,
                                new TableBucket(tableId, 1),
                                null,
                                EARLIEST_OFFSET,
                                100L);
                split.skipCurrentRound();
                reader.addSplits(Collections.singletonList(new FlinkTieringSplit(split)));
                // should skip tiering for this split
                retry(
                        Duration.ofMinutes(1),
                        () -> {
                            TestingReaderOutput<TableBucketWriteResult<TestingWriteResult>>
                                    output1 = new TestingReaderOutput<>();
                            // should force to finish, and the result is null
                            reader.pollNext(output1);
                            assertThat(output1.getEmittedRecords()).hasSize(1);
                            TableBucketWriteResult<TestingWriteResult> result =
                                    output1.getEmittedRecords().get(0);
                            assertThat(result.writeResult()).isNull();
                        });
            }
        }
    }

    /**
     * Get the elementsQueue from TieringSourceReader using reflection.
     *
     * @param reader the TieringSourceReader instance
     * @return the elementsQueue field value
     */
    @SuppressWarnings("unchecked")
    private FutureCompletingBlockingQueue<
                    RecordsWithSplitIds<TableBucketWriteResult<TestingWriteResult>>>
            getElementsQueue(TieringSourceReader<TestingWriteResult> reader) throws Exception {
        Class<?> clazz = reader.getClass();
        while (clazz != null) {
            try {
                Field elementsQueueField = clazz.getDeclaredField("elementsQueue");
                elementsQueueField.setAccessible(true);
                return (FutureCompletingBlockingQueue<
                                RecordsWithSplitIds<TableBucketWriteResult<TestingWriteResult>>>)
                        elementsQueueField.get(reader);
            } catch (NoSuchFieldException e) {
                // Try parent class
                clazz = clazz.getSuperclass();
            }
        }
        throw new RuntimeException("No elementsQueue field found");
    }
}
