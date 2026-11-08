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

package org.apache.fluss.flink.tiering.committer;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.tiering.FlussTableLakeSnapshotCommitter;
import org.apache.fluss.client.tiering.TableBucketWriteResult;
import org.apache.fluss.client.tiering.TieringCommitResult;
import org.apache.fluss.client.tiering.TieringCommitter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.tiering.event.FailedTieringEvent;
import org.apache.fluss.flink.tiering.event.FinishedTieringEvent;
import org.apache.fluss.flink.tiering.event.RowPosRequestEvent;
import org.apache.fluss.flink.tiering.source.TieringSource;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.source.RowPosResult;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox.MailboxClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.runtime.util.Runnables.withUncaughtExceptionHandler;
import static org.apache.fluss.config.ConfigOptions.LAKE_TIERING_COMMIT_THREAD_NUM;
import static org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils.getLakeTieringIoTmpDirs;
import static org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.Uninterruptibles.awaitTerminationUninterruptibly;
import static org.apache.fluss.utils.IOUtils.closeAll;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * A Flink operator to aggregate {@link WriteResult}s by table to {@link Committable} which will
 * then be committed to lake & Fluss cluster.
 *
 * <p>It will collect all {@link TableBucketWriteResult}s which wraps {@link WriteResult} written by
 * {@link LakeWriter} in {@link TieringSource} operator.
 *
 * <p>When it collects all {@link TableBucketWriteResult}s of a round of tiering for a table, it
 * will combine all the {@link WriteResult}s to {@link Committable} via method {@link
 * LakeCommitter#toCommittable(List)}, and then call method {@link LakeCommitter#commit(Object,
 * LakeCommitter.CommitContext)} to commit to lake.
 *
 * <p>Finally, it will also commit the committed lake snapshot to Fluss cluster to make Fluss aware
 * of the tiering progress.
 *
 * <p>Rounds of tables with deletion vectors are committed by a {@link DerivedDvTieringCommitter}.
 * Such a round may be held until the tiering readers re-index lake files rewritten since its base.
 * The operator then requests the re-index from the enumerator ({@link RowPosRequestEvent}),
 * collects the readers' results and commits the round again. A held round takes no commit thread.
 */
public class TieringCommitOperator<WriteResult, Committable>
        extends AbstractStreamOperator<CommittableMessage<Committable>>
        implements OneInputStreamOperator<
                TableBucketWriteResult<WriteResult>, CommittableMessage<Committable>> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(TieringCommitOperator.class);

    /** Consecutive failed rounds of a table from which its commit failures are logged at ERROR. */
    @VisibleForTesting static final int CONSECUTIVE_FAILURES_TO_LOG_ERROR = 3;

    /**
     * Tables whose consecutive commit failures are counted, under 1 MB in total. Bounds the counts
     * of tables that are dropped while failing, as they never commit again.
     */
    private static final int MAX_TABLES_WITH_COMMIT_FAILURES = 10_000;

    private final Configuration flussConfig;
    private final Configuration lakeTieringConfig;
    private final LakeTieringFactory<WriteResult, Committable> lakeTieringFactory;
    private final FlussTableLakeSnapshotCommitter flussTableLakeSnapshotCommitter;
    private final MailboxExecutor mailboxExecutor;
    private final int commitThreads;
    // Mailbox-thread confined, including completed commits whose callbacks have not run yet.
    private final Set<Long> committingTables = new HashSet<>();
    private ExecutorService commitExecutor;
    private boolean closed;
    private TieringCommitter<WriteResult, Committable> tieringCommitter;
    private DerivedDvTieringCommitter<WriteResult, Committable> dvCommitter;
    private Connection connection;
    private Admin admin;

    // gateway to send event to flink source coordinator
    private final OperatorEventGateway operatorEventGateway;

    // Table ID -> accumulator for the current tiering round.
    private final Map<Long, TableWriteResultAccumulator<WriteResult>> tableWriteResultAccumulators;

    // Table ID -> round of a table with deletion vectors held until its re-index is done. A held
    // round is not in committingTables. Mailbox-thread confined.
    private final Map<Long, HeldRound<WriteResult>> heldRounds = new HashMap<>();

    // Only picks the log level of commit failures. Mailbox-thread confined.
    private final ConsecutiveCommitFailureCounter consecutiveCommitFailures =
            new ConsecutiveCommitFailureCounter(MAX_TABLES_WITH_COMMIT_FAILURES);

    /** Creates an operator that collects and commits table tiering rounds. */
    public TieringCommitOperator(
            StreamOperatorParameters<CommittableMessage<Committable>> parameters,
            MailboxExecutor mailboxExecutor,
            Configuration flussConf,
            Configuration lakeTieringConfig,
            LakeTieringFactory<WriteResult, Committable> lakeTieringFactory) {
        this.lakeTieringFactory = lakeTieringFactory;
        this.flussTableLakeSnapshotCommitter = new FlussTableLakeSnapshotCommitter(flussConf);
        this.tableWriteResultAccumulators = new HashMap<>();
        this.flussConfig = flussConf;
        this.lakeTieringConfig = lakeTieringConfig;
        this.mailboxExecutor = mailboxExecutor;
        this.commitThreads = lakeTieringConfig.get(LAKE_TIERING_COMMIT_THREAD_NUM);
        checkArgument(
                commitThreads > 0, "%s must be positive.", LAKE_TIERING_COMMIT_THREAD_NUM.key());
        this.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());
        this.operatorEventGateway =
                parameters
                        .getOperatorEventDispatcher()
                        .getOperatorEventGateway(TieringSource.TIERING_SOURCE_OPERATOR_UID);
    }

    @Override
    public void open() {
        flussTableLakeSnapshotCommitter.open();
        connection = ConnectionFactory.createConnection(flussConfig);
        admin = connection.getAdmin();
        tieringCommitter =
                new TieringCommitter<>(
                        admin,
                        flussConfig,
                        lakeTieringConfig,
                        lakeTieringFactory,
                        flussTableLakeSnapshotCommitter);
        // The index stores' temporary files, e.g. checkpoints the trim builds, go to the
        // tiering service's local directories.
        String[] ioTmpDirs =
                getLakeTieringIoTmpDirs(
                        lakeTieringConfig,
                        getContainingTask()
                                .getEnvironment()
                                .getTaskManagerInfo()
                                .getConfiguration());
        dvCommitter =
                new DerivedDvTieringCommitter<>(
                        admin,
                        flussConfig,
                        lakeTieringConfig,
                        lakeTieringFactory,
                        flussTableLakeSnapshotCommitter,
                        getMetricGroup(),
                        ioTmpDirs.length == 0
                                ? null
                                : new File(
                                        ioTmpDirs[
                                                ThreadLocalRandom.current()
                                                        .nextInt(ioTmpDirs.length)],
                                        "dv-committer"));
        commitExecutor =
                Executors.newFixedThreadPool(
                        commitThreads,
                        new ExecutorThreadFactory("tiering-committer", getUserCodeClassloader()));
    }

    @Override
    public void processElement(StreamRecord<TableBucketWriteResult<WriteResult>> streamRecord)
            throws Exception {
        TableBucketWriteResult<WriteResult> tableBucketWriteResult = streamRecord.getValue();
        if (tableBucketWriteResult.isRowPosResult()) {
            handleRowPosResult(tableBucketWriteResult);
            return;
        }
        TableBucket tableBucket = tableBucketWriteResult.tableBucket();
        long tableId = tableBucket.getTableId();
        TableWriteResultAccumulator<WriteResult> accumulator =
                tableWriteResultAccumulators.computeIfAbsent(
                        tableId,
                        ignored ->
                                new TableWriteResultAccumulator<>(
                                        tableBucketWriteResult.numberOfWriteResults()));
        accumulator.add(tableBucketWriteResult);

        if (accumulator.isComplete()) {
            awaitCommitCapacity();
            tableWriteResultAccumulators.remove(tableId);
            HeldRound<WriteResult> stale = heldRounds.remove(tableId);
            if (stale != null) {
                // The enumerator starts a table's next round only after the previous one ended,
                // so the held round failed there, e.g. because its re-index could not be
                // requested. This round registers any lake commit it made before it was held.
                LOG.warn(
                        "Derived DV: dropping the held round of {} (re-index of snapshot {}), as"
                                + " the table's next round started.",
                        stale.reindex.tablePath,
                        stale.reindex.compactSnapshotId);
            }
            checkState(committingTables.add(tableId), "Table %s is already committing.", tableId);
            commitAsync(
                    tableId, tableBucketWriteResult.tablePath(), accumulator.bucketWriteResults);
        }
    }

    /** Bounds submitted rounds as well as threads; yields so completions can free capacity. */
    private void awaitCommitCapacity() throws InterruptedException {
        // Completions never wait for stream input, so yielding to them cannot deadlock.
        while (committingTables.size() >= commitThreads) {
            mailboxExecutor.yield();
        }
    }

    /**
     * Collects a bucket's re-index result of a held round. Once every split of the re-index
     * reported, commits the round again on the re-indexed snapshot.
     */
    private void handleRowPosResult(TableBucketWriteResult<WriteResult> tableBucketWriteResult)
            throws InterruptedException {
        long tableId = tableBucketWriteResult.tableBucket().getTableId();
        HeldRound<WriteResult> held = heldRounds.get(tableId);
        if (held == null) {
            LOG.warn(
                    "Dropping a re-index result of table {} that has no held round.",
                    tableBucketWriteResult.tablePath());
            return;
        }
        RowPosResult result = tableBucketWriteResult.getRowPosResult();
        if (result.getChangeId() != held.reindex.changeId) {
            // A result of an earlier request, e.g. of a round that failed meanwhile.
            LOG.warn(
                    "Dropping a re-index result of {} for index change {}; the held round waits"
                            + " for change {}.",
                    tableBucketWriteResult.tableBucket(),
                    result.getChangeId(),
                    held.reindex.changeId);
            return;
        }
        if (held.rowPosResults
                        .computeIfAbsent(tableBucketWriteResult.tableBucket(), k -> new HashMap<>())
                        .put(result.getSplit(), result)
                != null) {
            // A reader that failed over reports its split again.
            LOG.info(
                    "Re-index result of split {} of {} for index change {} arrived again.",
                    result.getSplit(),
                    tableBucketWriteResult.tableBucket(),
                    result.getChangeId());
        } else {
            held.received++;
        }
        if (held.received < held.reindex.expectedResults) {
            return;
        }
        heldRounds.remove(tableId);
        awaitCommitCapacity();
        checkState(committingTables.add(tableId), "Table %s is already committing.", tableId);
        resumeAsync(tableId, held);
    }

    @Override
    public void close() throws Exception {
        closed = true;
        if (commitExecutor != null) {
            commitExecutor.shutdownNow();
            awaitTerminationUninterruptibly(commitExecutor);
        }
        // Release the clients before restoring a pending cancellation interrupt.
        boolean interrupted = Thread.interrupted();
        try {
            closeAll(flussTableLakeSnapshotCommitter, admin, connection, super::close);
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void commitAsync(
            long tableId, TablePath tablePath, List<TableBucketWriteResult<WriteResult>> results) {
        submit(
                tableId,
                tablePath,
                results,
                () -> {
                    TableInfo tableInfo = admin.getTableInfo(tablePath).get();
                    if (tableInfo.isDeletionVectorsEnabled()) {
                        return dvCommitter.commitRound(tableId, tablePath, tableInfo, results);
                    }
                    return DerivedDvTieringCommitter.Outcome.committed(
                            tieringCommitter.commitWriteResults(tableId, tablePath, results));
                });
    }

    /** Publishes the re-index of a held round and commits the round again. */
    private void resumeAsync(long tableId, HeldRound<WriteResult> held) {
        submit(
                tableId,
                held.reindex.tablePath,
                held.writeResults,
                () ->
                        dvCommitter.resumeRound(
                                tableId, held.reindex, held.results(), held.writeResults));
    }

    private void submit(
            long tableId,
            TablePath tablePath,
            List<TableBucketWriteResult<WriteResult>> results,
            RoundCommit<Committable> roundCommit) {
        Runnable commit =
                () -> {
                    DerivedDvTieringCommitter.Outcome<Committable> outcome;
                    try {
                        // the round's commits to Fluss carry its epoch, so a round of a table the
                        // coordinator took from this service registers nothing
                        flussTableLakeSnapshotCommitter.fenceOnTieringEpoch(
                                tableId, TableBucketWriteResult.tieringEpochOf(results));
                        outcome = roundCommit.commit();
                    } catch (Exception e) {
                        postCompletion(tableId, () -> reportFailed(tableId, tablePath, e));
                        return;
                    }
                    postCompletion(tableId, () -> complete(tableId, results, outcome));
                };
        commitExecutor.execute(
                withUncaughtExceptionHandler(
                        commit,
                        (thread, error) ->
                                getContainingTask().getEnvironment().failExternally(error)));
    }

    private void complete(
            long tableId,
            List<TableBucketWriteResult<WriteResult>> results,
            DerivedDvTieringCommitter.Outcome<Committable> outcome) {
        DerivedDvTieringCommitter.Reindex reindex = outcome.reindex();
        if (reindex == null) {
            reportFinished(
                    tableId,
                    outcome.result(),
                    results.stream().anyMatch(TableBucketWriteResult::endedEarly));
            return;
        }
        // Registered before the request is sent, so no result arrives without its held round.
        heldRounds.put(tableId, new HeldRound<>(results, reindex));
        for (RowPosRequestEvent request : reindex.requests) {
            operatorEventGateway.sendEventToCoordinator(new SourceEventWrapper(request));
        }
        LOG.info(
                "Derived DV: round of {} holds until rewritten files are re-indexed (snapshot {}).",
                reindex.tablePath,
                reindex.compactSnapshotId);
    }

    @VisibleForTesting
    void postCompletion(long tableId, Runnable completion) {
        try {
            mailboxExecutor.execute(
                    () -> {
                        if (!closed) {
                            committingTables.remove(tableId);
                            completion.run();
                        }
                    },
                    "Complete tiering commit for table %s",
                    tableId);
        } catch (RejectedExecutionException e) {
            if (!(e.getCause() instanceof MailboxClosedException)) {
                throw e;
            }
        }
    }

    @VisibleForTesting
    int getConsecutiveCommitFailures(long tableId) {
        return consecutiveCommitFailures.getFailures(tableId);
    }

    /**
     * Reports a committed round. {@code moreToTier} is set if a bucket's round ended early, so the
     * table is tiered again at once.
     */
    private void reportFinished(
            long tableId, TieringCommitResult<Committable> result, boolean moreToTier) {
        consecutiveCommitFailures.recordSuccess(tableId);
        if (result.getCommittable() != null) {
            output.collect(new StreamRecord<>(new CommittableMessage<>(result.getCommittable())));
        }
        operatorEventGateway.sendEventToCoordinator(
                new SourceEventWrapper(
                        new FinishedTieringEvent(tableId, result.getStats(), moreToTier)));
    }

    private void reportFailed(long tableId, TablePath tablePath, Exception cause) {
        operatorEventGateway.sendEventToCoordinator(
                new SourceEventWrapper(
                        new FailedTieringEvent(tableId, ExceptionUtils.stringifyException(cause))));
        int failures = consecutiveCommitFailures.recordFailure(tableId);
        // A permanent failure retries forever without failing the job, so escalate it to make a
        // table that stopped tiering visible.
        if (failures >= CONSECUTIVE_FAILURES_TO_LOG_ERROR) {
            LOG.error(
                    "Failed to commit the tiering round of table {} (id {}) {} times in a row, "
                            + "its lake data does not advance until a commit succeeds. "
                            + "Will try to tier it again in the next round.",
                    tablePath,
                    tableId,
                    failures,
                    cause);
        } else {
            LOG.warn(
                    "Failed to commit the tiering round of table {} (id {}), "
                            + "will try to tier it again in the next round.",
                    tablePath,
                    tableId,
                    cause);
        }
    }

    /** Commits a round on a commit thread. */
    @FunctionalInterface
    private interface RoundCommit<Committable> {
        DerivedDvTieringCommitter.Outcome<Committable> commit() throws Exception;
    }

    /** A round of a table with deletion vectors held until its re-index is done. */
    private static final class HeldRound<WriteResult> {
        private final List<TableBucketWriteResult<WriteResult>> writeResults;
        private final DerivedDvTieringCommitter.Reindex reindex;
        // one result per split of the re-index, by bucket and split
        private final Map<TableBucket, Map<Integer, RowPosResult>> rowPosResults =
                new LinkedHashMap<>();
        private int received;

        private HeldRound(
                List<TableBucketWriteResult<WriteResult>> writeResults,
                DerivedDvTieringCommitter.Reindex reindex) {
            this.writeResults = writeResults;
            this.reindex = reindex;
        }

        private List<RowPosResult> results() {
            List<RowPosResult> results = new ArrayList<>();
            rowPosResults.values().forEach(splits -> results.addAll(splits.values()));
            return results;
        }
    }

    /** Collects one table's tiering round with constant-time validation of each result. */
    private static final class TableWriteResultAccumulator<WriteResult> {
        private final int expectedNumberOfWriteResults;
        private final Set<TableBucket> collectedBuckets = new HashSet<>();
        private final List<TableBucketWriteResult<WriteResult>> bucketWriteResults =
                new ArrayList<>();

        private TableWriteResultAccumulator(int expectedNumberOfWriteResults) {
            this.expectedNumberOfWriteResults = expectedNumberOfWriteResults;
        }

        private void add(TableBucketWriteResult<WriteResult> bucketWriteResult) {
            TableBucket tableBucket = bucketWriteResult.tableBucket();
            checkState(
                    !collectedBuckets.contains(tableBucket),
                    "Found duplicate write results for bucket %s of table %s.",
                    tableBucket,
                    tableBucket.getTableId());
            checkState(
                    expectedNumberOfWriteResults == bucketWriteResult.numberOfWriteResults(),
                    "numberOfWriteResults is not same across TableBucketWriteResults for table %s, got %s and %s.",
                    tableBucket.getTableId(),
                    expectedNumberOfWriteResults,
                    bucketWriteResult.numberOfWriteResults());
            collectedBuckets.add(tableBucket);
            bucketWriteResults.add(bucketWriteResult);
        }

        private boolean isComplete() {
            return bucketWriteResults.size() == expectedNumberOfWriteResults;
        }
    }
}
