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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.DvRequestRetries;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.ArrowScanRecords;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.LogScannerImpl;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.tiering.DvRoundRequests;
import org.apache.fluss.client.tiering.RoundDeletes;
import org.apache.fluss.client.tiering.TableBucketWriteResult;
import org.apache.fluss.client.tiering.TieringLogSplit;
import org.apache.fluss.client.tiering.TieringRowPosSplit;
import org.apache.fluss.client.tiering.TieringSnapshotSplit;
import org.apache.fluss.client.tiering.TieringSplit;
import org.apache.fluss.client.tiering.TieringWriterInitContext;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.FlussConfigUtils;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.exception.UnknownTableOrBucketException;
import org.apache.fluss.flink.source.reader.BoundedSplitReader;
import org.apache.fluss.flink.source.reader.RecordAndPos;
import org.apache.fluss.flink.tiering.committer.DeletedPositions;
import org.apache.fluss.flink.tiering.source.metrics.TieringMetrics;
import org.apache.fluss.flink.tiering.source.split.FlinkTieringSplit;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.batch.ArrowRecordBatch;
import org.apache.fluss.lake.dv.DeletionVectorCapabilities;
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.RowPosSstIndex;
import org.apache.fluss.lake.dv.index.PositionIndexStore;
import org.apache.fluss.lake.dv.index.SpillingPositionSorter;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.RecordReaders;
import org.apache.fluss.lake.source.RowPosResult;
import org.apache.fluss.lake.source.RowWithPosResult;
import org.apache.fluss.lake.writer.DeletionVectorPreparer;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.RoundMaskMismatchException;
import org.apache.fluss.lake.writer.SupportsDeletionVectorPreparation;
import org.apache.fluss.lake.writer.SupportsRecordBatchWrite;
import org.apache.fluss.lake.writer.WriterMemoryPool;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ArrowBatchData;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;
import org.apache.fluss.utils.function.SupplierWithException;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/** The {@link SplitReader} implementation which will read Fluss and write to lake. */
public class TieringSplitReader<WriteResult>
        implements SplitReader<TableBucketWriteResult<WriteResult>, FlinkTieringSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(TieringSplitReader.class);

    public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(10_000L);

    // unknown bucket timestamp for empty split or snapshot split
    private static final long UNKNOWN_BUCKET_TIMESTAMP = -1;

    // unknown bucket offset for empty split or snapshot split
    private static final long UNKNOWN_BUCKET_OFFSET = -1;

    // how often the reader checks the planned rounds still running past the time limit
    private static final long OVERDUE_ROUNDS_CHECK_INTERVAL_MS = 1_000;
    // past the time limit, a planned round that got nothing for this long ends without writing:
    // its bucket has no leader, or its log or mask cannot be read
    private static final long STALLED_ROUND_TIMEOUT_MS = 120_000;
    // the threads that fetch the log masks of planned rounds; more fetches wait for one
    private static final int MASK_FETCH_THREADS = 4;

    private final LakeTieringFactory<WriteResult, ?> lakeTieringFactory;
    private final Configuration lakeTieringConfig;

    private final Duration pollTimeout;

    // the id for the pending tables to be tiered
    private final Queue<Long> pendingTieringTables;
    // the table_id to the pending splits
    private final Map<Long, Set<TieringSplit>> pendingTieringSplits;

    private final Set<Long> reachTieringMaxDurationTables;
    // when the current table, past its time limit, has its planned rounds checked again
    private long nextOverdueRoundsCheckMs;

    private final Map<TableBucket, LakeWriter<WriteResult>> lakeWriters;
    private final Connection connection;

    @Nullable private Long currentTableId;
    @Nullable private TablePath currentTablePath;
    @Nullable private LogScanner currentLogScanner;
    @Nullable private Table currentTable;

    private final Queue<TieringSnapshotSplit> currentPendingSnapshotSplits;
    @Nullable private BoundedSplitReader currentSnapshotSplitReader;
    @Nullable private TieringSnapshotSplit currentSnapshotSplit;
    @Nullable private Integer currentTableNumberOfSplits;
    // whether the current table uses the Arrow record batch path for tiering
    @Nullable private Boolean currentTableUseRecordBatchPath;

    // map from table bucket to split id
    private final Map<TableBucket, TieringSplit> currentTableSplitsByBucket;
    private final Map<TableBucket, Long> currentTableStoppingOffsets;

    // partition id -> actual bucket count for the current table's partitions
    private final Map<Long, Integer> currentTablePartitionBucketCounts = new HashMap<>();

    private final Map<TableBucket, LogOffsetAndTimestamp> currentTableTieredOffsetAndTimestamp;

    private final Set<TieringSplit> currentEmptySplits;

    private final TieringMetrics tieringMetrics;
    private final boolean unshadedArrowAvailable;
    @Nullable private final String[] ioTmpDirs;
    private final Queue<TieringRowPosSplit> pendingRowPosSplits;

    // The most entries a re-index split holds in memory before it spills a sorted run.
    private final int reindexSortBufferEntries;
    // The memory this reader's lake writers share to buffer their rounds.
    private final WriterMemoryPool writerMemoryPool;
    // Whether planned rounds are read through their leaders' log masks; else in sort mode.
    private final boolean roundMasks;
    // The most heap the log masks of the rounds being read take at once, besides the writers'.
    private final long maskMemory;
    // the rounds of the current table that their buckets' leaders planned, in the order they came
    private final Map<TableBucket, PlannedRound> plannedRounds = new LinkedHashMap<>();
    // fetches the log masks of planned rounds while the reader reads other buckets
    @Nullable private ExecutorService maskFetchers;
    private long stalledRoundTimeoutMs = STALLED_ROUND_TIMEOUT_MS;
    // the buckets of the current table the log scanner reads
    private final Set<TableBucket> subscribedBuckets = new HashSet<>();
    // How long a planned round whose writer completed waits for its deletes; 0 if not fetched.
    private final long roundDeletesTimeoutMs;
    // The most bytes of fetched round deletes the reader holds at once.
    private final long roundDeletesMaxBytes;
    // The threads that fetch the deletes of planned rounds.
    private final int roundDeletesFetchThreads;
    // fetches the deletes of planned rounds while the reader reads them
    @Nullable private ExecutorService deletesFetchers;
    // the write results of planned rounds whose writers completed, waiting for their deletes
    private final Map<TableBucket, AwaitingDeletes> awaitingDeletes = new LinkedHashMap<>();
    // completed by wakeUp(), so a wait for masks or deletes returns, e.g. for a new split
    private volatile CompletableFuture<Void> wokenUp = new CompletableFuture<>();
    // Whether the writers prepare their buckets' deletion vectors if the lake supports it.
    private final boolean writersPrepareDeletionVectors;
    // the preparers of the deletion vectors of the tables read; the source reader's, or own
    private final DeletionVectorPreparers<WriteResult> preparers;
    private final boolean ownsPreparers;
    // the rounds, by table id and snapshot, whose failed preparation was logged already
    private final Set<String> preparationFailuresLogged = ConcurrentHashMap.newKeySet();

    public TieringSplitReader(
            Connection connection,
            LakeTieringFactory<WriteResult, ?> lakeTieringFactory,
            ClassLoader userClassLoader,
            TieringMetrics tieringMetrics) {
        this(connection, lakeTieringFactory, userClassLoader, tieringMetrics, new Configuration());
    }

    public TieringSplitReader(
            Connection connection,
            LakeTieringFactory<WriteResult, ?> lakeTieringFactory,
            ClassLoader userClassLoader,
            TieringMetrics tieringMetrics,
            Configuration lakeTieringConfig) {
        this(
                connection,
                lakeTieringFactory,
                userClassLoader,
                DEFAULT_POLL_TIMEOUT,
                tieringMetrics,
                null,
                lakeTieringConfig);
    }

    /**
     * Creates a reader. The options of the tiering service, {@code lake.tiering.*}, come from
     * {@code lakeTieringConfig}; the connection's configuration is the Fluss client's.
     */
    @VisibleForTesting
    protected TieringSplitReader(
            Connection connection,
            LakeTieringFactory<WriteResult, ?> lakeTieringFactory,
            ClassLoader userClassLoader,
            Duration pollTimeout,
            TieringMetrics tieringMetrics,
            @Nullable String[] ioTmpDirs,
            Configuration lakeTieringConfig) {
        this(
                connection,
                lakeTieringFactory,
                userClassLoader,
                pollTimeout,
                tieringMetrics,
                ioTmpDirs,
                lakeTieringConfig,
                null);
    }

    /**
     * Creates a reader as above that prepares deletion vectors with {@code preparers}, which its
     * source reader owns, or with its own if null.
     */
    TieringSplitReader(
            Connection connection,
            LakeTieringFactory<WriteResult, ?> lakeTieringFactory,
            ClassLoader userClassLoader,
            Duration pollTimeout,
            TieringMetrics tieringMetrics,
            @Nullable String[] ioTmpDirs,
            Configuration lakeTieringConfig,
            @Nullable DeletionVectorPreparers<WriteResult> preparers) {
        this.ownsPreparers = preparers == null;
        this.preparers = preparers == null ? new DeletionVectorPreparers<>() : preparers;
        this.lakeTieringFactory = lakeTieringFactory;
        // owned by TieringSourceReader
        this.connection = connection;
        this.pendingTieringTables = new ArrayDeque<>();
        this.pendingTieringSplits = new HashMap<>();
        this.currentTableStoppingOffsets = new HashMap<>();
        this.currentTableTieredOffsetAndTimestamp = new HashMap<>();
        this.currentEmptySplits = new HashSet<>();
        this.currentTableSplitsByBucket = new HashMap<>();
        this.lakeWriters = new HashMap<>();
        this.currentPendingSnapshotSplits = new ArrayDeque<>();
        this.reachTieringMaxDurationTables = new HashSet<>();
        this.pollTimeout = pollTimeout;
        this.tieringMetrics = tieringMetrics;
        this.unshadedArrowAvailable = checkUnshadedArrowAvailable(userClassLoader);
        this.ioTmpDirs = ioTmpDirs;
        this.pendingRowPosSplits = new ArrayDeque<>();
        // a split of fewer rows never spills
        this.reindexSortBufferEntries =
                (int)
                        Math.max(
                                1,
                                Math.min(
                                        SpillingPositionSorter.DEFAULT_MAX_BUFFERED,
                                        lakeTieringConfig.get(
                                                ConfigOptions
                                                        .LAKE_TIERING_DELETION_VECTORS_REINDEX_ROWS_PER_SPLIT)));
        long writeBufferSize =
                lakeTieringConfig
                        .get(ConfigOptions.LAKE_TIERING_DELETION_VECTORS_WRITE_BUFFER_SIZE)
                        .getBytes();
        checkArgument(
                writeBufferSize > 0,
                "%s must be positive.",
                ConfigOptions.LAKE_TIERING_DELETION_VECTORS_WRITE_BUFFER_SIZE.key());
        this.writerMemoryPool = new WriterMemoryPool(writeBufferSize);
        this.maskMemory = writeBufferSize;
        this.roundMasks =
                lakeTieringConfig.get(
                        ConfigOptions.LAKE_TIERING_DELETION_VECTORS_ROUND_MASK_ENABLED);
        this.roundDeletesTimeoutMs =
                lakeTieringConfig
                        .get(
                                ConfigOptions
                                        .LAKE_TIERING_DELETION_VECTORS_ROUND_DELETES_PREFETCH_TIMEOUT)
                        .toMillis();
        checkArgument(
                roundDeletesTimeoutMs >= 0,
                "%s must not be negative.",
                ConfigOptions.LAKE_TIERING_DELETION_VECTORS_ROUND_DELETES_PREFETCH_TIMEOUT.key());
        this.roundDeletesMaxBytes =
                lakeTieringConfig
                        .get(
                                ConfigOptions
                                        .LAKE_TIERING_DELETION_VECTORS_ROUND_DELETES_PREFETCH_MAX_BYTES)
                        .getBytes();
        this.roundDeletesFetchThreads =
                lakeTieringConfig.get(
                        ConfigOptions.LAKE_TIERING_DELETION_VECTORS_ROUND_DELETES_PREFETCH_THREADS);
        this.writersPrepareDeletionVectors =
                lakeTieringConfig.get(
                        ConfigOptions.LAKE_TIERING_DELETION_VECTORS_WRITER_PREPARED_ENABLED);
        this.lakeTieringConfig = lakeTieringConfig;
        checkArgument(
                roundDeletesFetchThreads > 0,
                "%s must be positive.",
                ConfigOptions.LAKE_TIERING_DELETION_VECTORS_ROUND_DELETES_PREFETCH_THREADS.key());
    }

    @Override
    public RecordsWithSplitIds<TableBucketWriteResult<WriteResult>> fetch() throws IOException {
        TableBucketWriteResultWithSplitIds withDeletes = resultsDoneWaitingForDeletes();
        if (withDeletes != null) {
            return withDeletes;
        }
        dropRoundDeletesOverBudget();
        if (!pendingRowPosSplits.isEmpty()) {
            return processRowPosSplit(pendingRowPosSplits.poll());
        }

        // check empty splits
        if (!currentEmptySplits.isEmpty()) {
            LOG.info("Empty split(s) {} finished.", currentEmptySplits);
            TableBucketWriteResultWithSplitIds records = forEmptySplits(currentEmptySplits);
            currentEmptySplits.forEach(
                    split -> currentTableSplitsByBucket.remove(split.getTableBucket()));
            mayFinishCurrentTable();
            currentEmptySplits.clear();
            return records;
        }
        checkSplitOrStartNext();

        // may read snapshot firstly
        if (currentSnapshotSplitReader != null) {
            // for snapshot split, we don't force to complete it
            // since we rely on the log offset for the snapshot to
            // do next tiering, if force to complete, we can't get the log offset
            CloseableIterator<RecordAndPos> recordIterator = currentSnapshotSplitReader.readBatch();
            if (recordIterator == null) {
                LOG.info("Split {} is finished", currentSnapshotSplit.splitId());
                return finishCurrentSnapshotSplit();
            } else {
                return forSnapshotSplitRecords(
                        currentSnapshotSplit.getTableBucket(), recordIterator);
            }
        } else {
            if (currentLogScanner != null) {
                Map<TableBucket, String> gone = subscribeDecidedRounds();
                if (!gone.isEmpty()) {
                    return endRoundsWithoutWriting(gone);
                }
                // force to complete records
                if (reachTieringMaxDurationTables.contains(currentTableId)
                        && System.currentTimeMillis() >= nextOverdueRoundsCheckMs) {
                    return forceCompleteTieringLogRecords();
                }
                if (subscribedBuckets.isEmpty()) {
                    // every round of the table waits for its mask: nothing to read yet
                    waitForAMask();
                    return emptyTableBucketWriteResultWithSplitIds();
                }
                Duration timeout = nextPollTimeout();
                if (useRecordBatchPath()) {
                    try (ArrowScanRecords arrowScanRecords =
                            ((LogScannerImpl) currentLogScanner).pollRecordBatch(timeout)) {
                        return processLogRecords(
                                arrowScanRecords.buckets(),
                                arrowScanRecords::records,
                                this::handleArrowBatchRecords,
                                arrowScanRecords::consumedUpToOffset,
                                bucket -> Collections.emptyList());
                    }
                } else {
                    ScanRecords scanRecords = currentLogScanner.poll(timeout);
                    return processLogRecords(
                            scanRecords.buckets(),
                            scanRecords::records,
                            this::handleLogRecords,
                            scanRecords::consumedUpToOffset,
                            scanRecords::offsetsWithoutRecords);
                }
            } else {
                if (!awaitingDeletes.isEmpty()) {
                    awaitAny(Collections.emptyList());
                }
                return emptyTableBucketWriteResultWithSplitIds();
            }
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<FlinkTieringSplit> splitsChange) {
        if (!(splitsChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChange.getClass()));
        }
        for (FlinkTieringSplit flinkSplit : splitsChange.splits()) {
            TieringSplit split = flinkSplit.unwrap();
            LOG.info("add split {}", split.splitId());
            if (split.isTieringRowPosSplit()) {
                pendingRowPosSplits.add(split.asTieringRowPosSplit());
                continue;
            }
            if (split.shouldSkipCurrentRound()) {
                // if the split is forced to ignore,
                // mark it as empty
                LOG.info(
                        "ignore split {} since the split is set to skip the current round of tiering.",
                        split.splitId());
                currentEmptySplits.add(split);
                continue;
            }
            long tableId = split.getTableBucket().getTableId();
            // the split belongs to the current table
            if (currentTableId != null && currentTableId == tableId) {
                addSplitToCurrentTable(split);
            } else {
                Set<TieringSplit> alreadyPendingSplits = pendingTieringSplits.get(tableId);
                if (alreadyPendingSplits != null) {
                    // add to the already pending splits
                    alreadyPendingSplits.add(split);
                } else {
                    Set<TieringSplit> pendingSplits = new HashSet<>();
                    pendingSplits.add(split);
                    pendingTieringSplits.put(tableId, pendingSplits);
                    pendingTieringTables.add(tableId);
                }
            }
        }
    }

    private void addSplitToCurrentTable(TieringSplit split) {
        this.currentTableSplitsByBucket.put(split.getTableBucket(), split);
        if (split.isTieringSnapshotSplit()) {
            this.currentPendingSnapshotSplits.add((TieringSnapshotSplit) split);
        } else if (split.isTieringLogSplit()) {
            subscribeLog((TieringLogSplit) split);
        }
    }

    private void checkSplitOrStartNext() {
        if (currentSnapshotSplitReader != null) {
            return;
        }

        // may poll next snapshot split to read
        TieringSnapshotSplit nextSnapshotSplit = currentPendingSnapshotSplits.poll();
        if (nextSnapshotSplit != null) {
            Table table = getOrMoveToTable(nextSnapshotSplit);
            currentSnapshotSplit = nextSnapshotSplit;
            currentSnapshotSplitReader =
                    new BoundedSplitReader(
                            table.newScan()
                                    .createBatchScanner(
                                            currentSnapshotSplit.getTableBucket(),
                                            currentSnapshotSplit.getSnapshotId()),
                            0);
            return;
        }

        // use current log scanner to read
        if (currentLogScanner != null) {
            return;
        }

        // may poll next table to read
        Long pendingTableId = pendingTieringTables.poll();
        if (pendingTableId == null) {
            return;
        }

        Set<TieringSplit> pendingSplits = pendingTieringSplits.remove(pendingTableId);
        for (TieringSplit split : pendingSplits) {
            getOrMoveToTable(split);
            addSplitToCurrentTable(split);
        }
    }

    private Table getOrMoveToTable(TieringSplit split) {
        if (currentTable == null) {
            TablePath tablePath = split.getTablePath();
            currentTable = connection.getTable(tablePath);
            currentTablePath = tablePath;
            currentTableId = split.getTableBucket().getTableId();
            currentTableNumberOfSplits = split.getNumberOfSplits();
            TableInfo currentTableInfo = checkNotNull(currentTable).getTableInfo();
            // check currentTable's id for the table path is same with table id of the tiering
            // split, if not, it means the tiering split is for a previous dropped table. let's fail
            // directly
            // todo: we should skip and notify enumerator that the table id is not tiering now
            // instead of fail directly
            checkArgument(
                    currentTableInfo.getTableId() == split.getTableBucket().getTableId(),
                    "The current table id %s for table path %s is different from the table id %s in TieringSplit split.",
                    currentTableInfo.getTableId(),
                    tablePath,
                    split.getTableBucket().getTableId());
            // Snapshot each partition's actual bucket count so lake writers can stamp per-partition
            // bucket layouts correctly after an ALTER bucket.num.
            if (currentTableInfo.isPartitioned()) {
                try {
                    // the admin is a shared per-connection instance, so it must not be closed here
                    Admin admin = connection.getAdmin();
                    for (PartitionInfo partitionInfo :
                            admin.listPartitionInfos(tablePath, true).get()) {
                        currentTablePartitionBucketCounts.put(
                                partitionInfo.getPartitionId(), partitionInfo.getBucketCount());
                    }
                } catch (Exception e) {
                    throw new FlussRuntimeException(
                            "Failed to list partition infos for table " + tablePath, e);
                }
            }
            LOG.info("Start to tier table {} with table id {}.", currentTablePath, currentTableId);
        }
        return currentTable;
    }

    private void mayCreateLogScanner() {
        if (currentLogScanner == null) {
            currentLogScanner = checkNotNull(currentTable).newScan().createLogScanner();
        }
    }

    private RecordsWithSplitIds<TableBucketWriteResult<WriteResult>>
            forceCompleteTieringLogRecords() throws IOException {
        Map<TableBucket, TableBucketWriteResult<WriteResult>> writeResults = new HashMap<>();
        Map<TableBucket, String> finishedSplitIds = new HashMap<>();

        // Force finish all splits, but those of rounds their leaders planned. Such a round's log
        // mask is exact at its planned end only, and its caps bound how long it takes. It ends
        // early only without writing anything: if its bucket is gone, or it cannot be read.
        Map<TableBucket, String> ended = plannedRoundsToEnd();
        boolean plannedRoundsLeft = false;
        Iterator<Map.Entry<TableBucket, TieringSplit>> currentTieringSplitsIterator =
                currentTableSplitsByBucket.entrySet().iterator();
        while (currentTieringSplitsIterator.hasNext()) {
            Map.Entry<TableBucket, TieringSplit> entry = currentTieringSplitsIterator.next();
            TableBucket bucket = entry.getKey();
            TieringSplit split = entry.getValue();
            String why = ended.get(bucket);
            if (why != null) {
                writeResults.put(
                        bucket,
                        endRoundWithoutWriting(
                                split.asTieringLogSplit(), "it is past the time limit and " + why));
                finishedSplitIds.put(bucket, split.splitId());
                currentTieringSplitsIterator.remove();
                continue;
            }
            if (split != null
                    && split.isTieringLogSplit()
                    && split.asTieringLogSplit().isPlannedRound()) {
                plannedRoundsLeft = true;
            }
            if (split != null
                    && split.isTieringLogSplit()
                    && !split.asTieringLogSplit().isPlannedRound()) {
                // get the current offset, timestamp that tiered so far
                LogOffsetAndTimestamp logOffsetAndTimestamp =
                        currentTableTieredOffsetAndTimestamp.get(bucket);
                long logEndOffset =
                        logOffsetAndTimestamp == null
                                ? UNKNOWN_BUCKET_OFFSET
                                // logEndOffset is equal to offset tiered + 1
                                : logOffsetAndTimestamp.logOffset + 1;
                long timestamp =
                        logOffsetAndTimestamp == null
                                ? UNKNOWN_BUCKET_TIMESTAMP
                                : logOffsetAndTimestamp.timestamp;
                TableBucketWriteResult<WriteResult> bucketWriteResult =
                        completeLakeWriter(
                                bucket,
                                split.getPartitionName(),
                                split.asTieringLogSplit().getStartingOffset(),
                                logEndOffset,
                                timestamp,
                                false);

                if (logEndOffset == UNKNOWN_BUCKET_OFFSET) {
                    // when the log end offset is unknown, the write result must be
                    // null, otherwise, we should throw exception directly to avoid data
                    // inconsistent
                    checkState(
                            bucketWriteResult.writeResult() == null,
                            "bucketWriteResult must be null when log end offset is unknown when tiering "
                                    + split);
                }

                writeResults.put(bucket, bucketWriteResult);
                finishedSplitIds.put(bucket, split.splitId());
                // the records polled for it from now on belong to no split
                currentTableStoppingOffsets.remove(bucket);
                LOG.info(
                        "Split {} is forced to be finished due to tiering reach max duration, "
                                + "write result {}, logEndOffset {}, timestamp {}",
                        split.splitId(),
                        bucketWriteResult,
                        logEndOffset,
                        timestamp);
                currentTieringSplitsIterator.remove();
            }
        }
        if (plannedRoundsLeft) {
            nextOverdueRoundsCheckMs =
                    System.currentTimeMillis() + OVERDUE_ROUNDS_CHECK_INTERVAL_MS;
        } else {
            reachTieringMaxDurationTables.remove(this.currentTableId);
        }
        mayFinishCurrentTable();
        return new TableBucketWriteResultWithSplitIds(writeResults, finishedSplitIds);
    }

    /**
     * The planned rounds of the current table to end without writing, past the time limit, and why:
     * their table or partition was dropped, their mask could not be fetched as their leader is not
     * reachable, or they got nothing for {@link #STALLED_ROUND_TIMEOUT_MS} since their mask fetch
     * started. A bucket whose existence cannot be checked counts as existing; it is checked again
     * later.
     */
    private Map<TableBucket, String> plannedRoundsToEnd() {
        Map<TableBucket, String> ended = new HashMap<>();
        TieringLogSplit any = null;
        long now = System.currentTimeMillis();
        for (Map.Entry<TableBucket, TieringSplit> e : currentTableSplitsByBucket.entrySet()) {
            TieringSplit split = e.getValue();
            if (split == null
                    || !split.isTieringLogSplit()
                    || !split.asTieringLogSplit().isPlannedRound()) {
                continue;
            }
            any = split.asTieringLogSplit();
            PlannedRound round = plannedRounds.get(e.getKey());
            if (round == null || round.waitsToFetch()) {
                // one waiting for memory for its mask waits for rounds that run or end
                continue;
            }
            Throwable failure = round.failure();
            if (failure != null) {
                ended.put(
                        e.getKey(),
                        "its log mask could not be fetched: "
                                + ExceptionUtils.stripExecutionException(failure).getMessage());
            } else if (now - round.lastProgressMs() >= stalledRoundTimeoutMs) {
                ended.put(
                        e.getKey(),
                        String.format(
                                "it read nothing for %d ms; its bucket may have no leader",
                                now - round.lastProgressMs()));
            }
        }
        if (any == null) {
            return ended;
        }
        String dropped = "its table or partition was dropped";
        if (isDropped(any)) {
            for (Map.Entry<TableBucket, TieringSplit> e : currentTableSplitsByBucket.entrySet()) {
                if (plannedRounds.containsKey(e.getKey())) {
                    ended.put(e.getKey(), dropped);
                }
            }
            return ended;
        }
        if (any.getTableBucket().getPartitionId() == null) {
            return ended;
        }
        Set<Long> partitions = new HashSet<>();
        try {
            for (PartitionInfo partition :
                    connection.getAdmin().listPartitionInfos(any.getTablePath()).get()) {
                partitions.add(partition.getPartitionId());
            }
        } catch (Exception e) {
            LOG.info(
                    "Could not list the partitions of {} to check its planned rounds; checking"
                            + " again later.",
                    any.getTablePath(),
                    e);
            return ended;
        }
        for (TableBucket bucket : plannedRounds.keySet()) {
            if (!partitions.contains(bucket.getPartitionId())) {
                ended.put(bucket, dropped);
            }
        }
        return ended;
    }

    /**
     * Ends a planned round without writing: its writer is closed without completing, which drops
     * its files, so the round tiers nothing of the bucket and its offset stays.
     */
    private TableBucketWriteResult<WriteResult> endRoundWithoutWriting(
            TieringLogSplit split, String why) {
        TableBucket bucket = split.getTableBucket();
        PlannedRound round = plannedRounds.remove(bucket);
        if (round != null) {
            round.release();
        }
        IOUtils.closeQuietly(lakeWriters.remove(bucket), "lake writer for bucket " + bucket);
        currentTableStoppingOffsets.remove(bucket);
        unsubscribe(bucket);
        LOG.warn(
                "The round of bucket {} of {} up to offset {} ends without writing, as {}; split {}"
                        + " tiers nothing.",
                bucket,
                split.getTablePath(),
                split.getStoppingOffset(),
                why,
                split.splitId());
        return toTableBucketWriteResult(
                split.getTablePath(),
                bucket,
                split.getPartitionName(),
                null,
                UNKNOWN_BUCKET_OFFSET,
                UNKNOWN_BUCKET_TIMESTAMP,
                split.getNumberOfSplits());
    }

    /** How long a planned round past the time limit may get nothing before it ends. */
    @VisibleForTesting
    void setStalledRoundTimeout(long timeoutMs) {
        this.stalledRoundTimeoutMs = timeoutMs;
    }

    /**
     * Determines whether the current table should use the Arrow record batch path for tiering. The
     * batch path is used when the table is an ARROW format append-only (log) table and the lake
     * writer supports batch writing.
     */
    private boolean useRecordBatchPath() {
        if (currentTableUseRecordBatchPath != null) {
            return currentTableUseRecordBatchPath;
        }
        TableInfo tableInfo = checkNotNull(currentTable).getTableInfo();

        currentTableUseRecordBatchPath =
                unshadedArrowAvailable
                        && !tableInfo.hasPrimaryKey()
                        && tableInfo.getTableConfig().getLogFormat() == LogFormat.ARROW
                        && tableInfo.getTableConfig().getDataLakeFormat().orElse(null)
                                == DataLakeFormat.PAIMON;
        return currentTableUseRecordBatchPath;
    }

    /**
     * Generic template method for processing tiering log records. Encapsulates the shared workflow
     * of bucket traversal, stopping offset checks, LakeWriter management, offset/timestamp
     * tracking, split completion, and table completion.
     *
     * @param buckets the set of buckets that have records
     * @param recordsExtractor function to extract records for a given bucket
     * @param handler callback for processing records within a single bucket
     * @param consumedUpToOffsetExtractor function to extract the consumed-up-to offset for a given
     *     bucket. The offset is used for progress tracking and split completion even when records
     *     are empty.
     * @param <R> the record type
     * @return the write results and finished split IDs
     * @throws IOException if an I/O error occurs during processing
     */
    private <R> RecordsWithSplitIds<TableBucketWriteResult<WriteResult>> processLogRecords(
            Set<TableBucket> buckets,
            Function<TableBucket, List<R>> recordsExtractor,
            BucketRecordsHandler<R> handler,
            Function<TableBucket, Long> consumedUpToOffsetExtractor,
            Function<TableBucket, List<long[]>> offsetsWithoutRecordsExtractor)
            throws IOException {
        Map<TableBucket, TableBucketWriteResult<WriteResult>> writeResults = new HashMap<>();
        Map<TableBucket, String> finishedSplitIds = new HashMap<>();
        boolean splitsEnded = false;

        // Iterate every polled bucket, including those that only advanced their offset.
        for (TableBucket bucket : buckets) {
            Long stoppingOffset = currentTableStoppingOffsets.get(bucket);
            if (stoppingOffset == null) {
                continue;
            }

            List<R> records = recordsExtractor.apply(bucket);

            // consumedUpToOffset is an exclusive upper bound: all offsets before it have been
            // consumed by the scanner in this poll round. It may advance even when records is
            // empty, e.g. when FIRST_ROW filters duplicate upserts into empty WAL batches.
            Long consumedUpToOffset = consumedUpToOffsetExtractor.apply(bucket);
            checkState(
                    consumedUpToOffset != null,
                    "Missing consumed-up-to offset for polled bucket %s.",
                    bucket);

            // Write records to the lake; returns the last written timestamp,
            // or UNKNOWN_BUCKET_TIMESTAMP if no records were actually written.
            long lastWrittenTimestamp;
            try {
                lastWrittenTimestamp =
                        handler.handleRecords(
                                bucket,
                                records,
                                () -> {
                                    TieringSplit split =
                                            checkNotNull(currentTableSplitsByBucket.get(bucket));
                                    return getOrCreateLakeWriter(
                                            bucket,
                                            split.getPartitionName(),
                                            split.getSplitIndex(),
                                            split.getTieringRoundTimestamp());
                                },
                                stoppingOffset);
            } catch (RoundMaskMismatchException e) {
                readAgainInSortMode(bucket, e);
                continue;
            }
            // The split owns offsets before stoppingOffset only. If the scanner consumed past
            // the split boundary, cap the tiered progress at stoppingOffset so the next split
            // still owns later data.
            long tieredLogEndOffset = Math.min(consumedUpToOffset, stoppingOffset);
            long tieredTimestamp;
            if (lastWrittenTimestamp >= 0) {
                tieredTimestamp = lastWrittenTimestamp;
            } else {
                LogOffsetAndTimestamp latest = currentTableTieredOffsetAndTimestamp.get(bucket);
                tieredTimestamp = latest != null ? latest.timestamp : UNKNOWN_BUCKET_TIMESTAMP;
            }
            currentTableTieredOffsetAndTimestamp.put(
                    bucket, new LogOffsetAndTimestamp(tieredLogEndOffset - 1, tieredTimestamp));

            PlannedRound round = plannedRounds.get(bucket);
            if (round != null) {
                for (long[] range : offsetsWithoutRecordsExtractor.apply(bucket)) {
                    round.readWithoutRecords(range[0], range[1]);
                }
                round.consumed(consumedUpToOffset, System.currentTimeMillis());
            }
            // The split owns offsets below stoppingOffset. If the scanner has not consumed up
            // to that exclusive bound yet, keep the split active.
            if (consumedUpToOffset < stoppingOffset) {
                continue;
            }
            TieringSplit currentTieringSplit = checkNotNull(currentTableSplitsByBucket.get(bucket));
            // the caps cut the planned round below the leader's high watermark: the next round
            // starts at once
            boolean capped = currentTieringSplit.asTieringLogSplit().isRoundCapped();
            CompletableFuture<RoundDeletes> roundDeletes = null;
            TableBucketWriteResult<WriteResult> writeResult;
            if (round != null) {
                // nothing of a planned round read only in part, or whose mask does not match it,
                // is committed
                String notReadWhole = round.notReadWhole();
                boolean masked = round.mask() != null;
                if (notReadWhole != null && !masked) {
                    throw new IOException(
                            String.format(
                                    "The round of bucket %s of %s up to offset %d was not read"
                                            + " whole: %s.",
                                    bucket, currentTablePath, stoppingOffset, notReadWhole));
                }
                try {
                    if (notReadWhole != null) {
                        throw new RoundMaskMismatchException(
                                String.format(
                                        "The round of bucket %s was not read whole: %s.",
                                        bucket, notReadWhole));
                    }
                    if (masked) {
                        round.mask().finish();
                    }
                    // the writer of a round read through its mask finds a key whose version the
                    // mask missed as it completes
                    writeResult =
                            completeLakeWriter(
                                    currentTieringSplit, stoppingOffset, tieredTimestamp, capped);
                } catch (RoundMaskMismatchException e) {
                    if (!masked) {
                        // a writer in sort mode keeps the last version of each key itself
                        throw new IOException(e.getMessage(), e);
                    }
                    readAgainInSortMode(bucket, e);
                    continue;
                }
                plannedRounds.remove(bucket);
                roundDeletes = round.takeDeletes();
                round.release();
            } else {
                writeResult =
                        completeLakeWriter(
                                currentTieringSplit, stoppingOffset, tieredTimestamp, capped);
            }

            // Split completion: unsubscribe, remove split.
            currentTableStoppingOffsets.remove(bucket);
            if (bucket.getPartitionId() != null || round != null) {
                unsubscribe(bucket);
            } else {
                // todo: should unsubscribe the log split if unsubscribe bucket for
                // un-partitioned table is supported
            }
            currentTableSplitsByBucket.remove(bucket);
            String currentSplitId = currentTieringSplit.splitId();
            splitsEnded = true;
            DeletionVectorPreparer<WriteResult> preparer =
                    roundDeletes == null || writeResult.writeResult() == null
                            ? null
                            : preparer(
                                    bucket,
                                    currentTieringSplit.asTieringLogSplit().getRoundSnapshotId(),
                                    true);
            CompletableFuture<TableBucketWriteResult<WriteResult>> prepared =
                    preparer == null
                            ? null
                            : roundDeletes.thenApplyAsync(
                                    deletes -> prepared(writeResult, deletes, preparer),
                                    deletesFetchers());
            if (roundDeletes != null
                    && writeResult.writeResult() != null
                    && !(prepared != null ? prepared : roundDeletes).isDone()) {
                awaitDeletes(writeResult, currentSplitId, roundDeletes, prepared, preparer);
            } else {
                writeResults.put(
                        bucket,
                        prepared != null && !prepared.isCompletedExceptionally()
                                ? prepared.join()
                                : withRoundDeletes(writeResult, roundDeletes));
                finishedSplitIds.put(bucket, currentSplitId);
            }
            if (capped) {
                LOG.info(
                        "Finish tier bucket {} for table {}, split: {}, at the end its leader"
                                + " planned within the round's caps; the next round tiers the"
                                + " rest.",
                        bucket,
                        currentTablePath,
                        currentSplitId);
            } else {
                LOG.info(
                        "Finish tier bucket {} for table {}, split: {}.",
                        bucket,
                        currentTablePath,
                        currentSplitId);
            }
        }

        if (splitsEnded) {
            mayFinishCurrentTable();
        }

        return new TableBucketWriteResultWithSplitIds(writeResults, finishedSplitIds);
    }

    /**
     * Handles row-based ScanRecord writing for the log path.
     *
     * @return the timestamp of the last written record, or -1 if no records were written
     */
    private long handleLogRecords(
            TableBucket bucket,
            List<ScanRecord> records,
            SupplierWithException<LakeWriter<?>, IOException> lakeWriterSupplier,
            long stoppingOffset)
            throws IOException {
        long lastWrittenTimestamp = UNKNOWN_BUCKET_TIMESTAMP;
        LakeWriter<?> lakeWriter = null;
        PlannedRound round = plannedRounds.get(bucket);
        RoundMask mask = round == null ? null : round.mask();
        long now = System.currentTimeMillis();
        for (ScanRecord record : records) {
            if (record.logOffset() < stoppingOffset) {
                if (round != null) {
                    round.read(record.logOffset(), now);
                }
                if (mask != null && !mask.pass(record.logOffset(), record.getChangeType())) {
                    // tiered all the same: the round's end covers it
                    lastWrittenTimestamp = record.timestamp();
                    if (record.getSizeInBytes() > 0) {
                        tieringMetrics.recordBytesRead(record.getSizeInBytes());
                    }
                    continue;
                }
                if (lakeWriter == null) {
                    lakeWriter = lakeWriterSupplier.get();
                }
                lakeWriter.write(record);
                lastWrittenTimestamp = record.timestamp();
                if (record.getSizeInBytes() > 0) {
                    tieringMetrics.recordBytesRead(record.getSizeInBytes());
                }
            }
        }
        return lastWrittenTimestamp;
    }

    /**
     * A local directory for temporary files under one of the tiering service's IO directories, or
     * null for the JVM's temporary directory.
     */
    @Nullable
    private File localTmpDir(String name) {
        return FileUtils.randomTmpDir(ioTmpDirs, name);
    }

    /**
     * Handles Arrow batch writing for the record batch path.
     *
     * @return the timestamp of the last written batch, or -1 if no batches were written
     */
    private long handleArrowBatchRecords(
            TableBucket bucket,
            List<ArrowBatchData> batches,
            SupplierWithException<LakeWriter<?>, IOException> lakeWriterSupplier,
            long stoppingOffset)
            throws IOException {
        SupportsRecordBatchWrite batchWriter = null;
        long lastWrittenTimestamp = UNKNOWN_BUCKET_TIMESTAMP;
        for (ArrowBatchData batch : batches) {
            long batchBaseOffset = batch.getBaseLogOffset();
            long batchRecordCount = batch.getRecordCount();
            long batchTimestamp = batch.getTimestamp();
            if (batchBaseOffset >= stoppingOffset) {
                batch.close();
                continue;
            }

            long writableRowCount = stoppingOffset - batchBaseOffset;
            int writableRows = (int) Math.min(batchRecordCount, writableRowCount);
            if (writableRows <= 0) {
                batch.close();
                continue;
            }

            if (batchWriter == null) {
                LakeWriter<?> lakeWriter = lakeWriterSupplier.get();
                if (!(lakeWriter instanceof SupportsRecordBatchWrite)) {
                    throw new IOException(
                            "LakeWriter does not support RecordBatch writes: "
                                    + lakeWriter.getClass().getName());
                }
                batchWriter = (SupportsRecordBatchWrite) lakeWriter;
            }

            ArrowBatchData batchToWrite = batch;
            if (writableRows < batchRecordCount) {
                batchToWrite = batch.truncateAndTransferOwnership(writableRows);
            }
            long batchSizeInBytes = batchToWrite.getSizeInBytes();
            try (ArrowRecordBatch arrowRecordBatch = new ArrowRecordBatch(batchToWrite)) {
                batchWriter.write(arrowRecordBatch);
            }
            if (batchSizeInBytes > 0) {
                tieringMetrics.recordBytesRead(batchSizeInBytes);
            }
            lastWrittenTimestamp = batchTimestamp;
        }
        return lastWrittenTimestamp;
    }

    /** Returns the bucket's lake writer, created on its first record. */
    private LakeWriter<WriteResult> getOrCreateLakeWriter(
            TableBucket bucket,
            @Nullable String partitionName,
            int splitIndex,
            long tieringRoundTimestamp)
            throws IOException {
        LakeWriter<WriteResult> lakeWriter = lakeWriters.get(bucket);
        if (lakeWriter == null) {
            Integer partitionBucketCount =
                    bucket.getPartitionId() != null
                            ? currentTablePartitionBucketCounts.get(bucket.getPartitionId())
                            : null;
            lakeWriter =
                    lakeTieringFactory.createLakeWriter(
                            new TieringWriterInitContext(
                                    currentTablePath,
                                    bucket,
                                    partitionName,
                                    currentTable.getTableInfo(),
                                    splitIndex,
                                    tieringRoundTimestamp,
                                    partitionBucketCount,
                                    ioTmpDirs,
                                    positionIndexDir(currentTable.getTableInfo()),
                                    writerMemoryPool,
                                    roundMasked(bucket)));
            lakeWriters.put(bucket, lakeWriter);
        }
        return lakeWriter;
    }

    /**
     * The remote directory of the position index of a table with deletion vectors, resolved as the
     * committer and the tablet servers resolve it. Null for other tables, or without a remote data
     * directory.
     */
    @Nullable
    private String positionIndexDir(TableInfo tableInfo) {
        if (!tableInfo.isDeletionVectorsEnabled()) {
            return null;
        }
        try {
            return PositionIndexStore.lakeTableDirOf(
                            tableInfo,
                            () ->
                                    FlussConfigUtils.getDefaultRemoteDataDir(
                                            connection.getConfiguration()))
                    .toString();
        } catch (RuntimeException e) {
            return null; // the committer fails such rounds anyway
        }
    }

    /** Completes the lake writer of a log split read up to {@code logEndOffset}. */
    private TableBucketWriteResult<WriteResult> completeLakeWriter(
            TieringSplit split, long logEndOffset, long maxTimestamp, boolean moreToTier)
            throws IOException {
        return completeLakeWriter(
                split.getTableBucket(),
                split.getPartitionName(),
                split.asTieringLogSplit().getStartingOffset(),
                logEndOffset,
                maxTimestamp,
                moreToTier);
    }

    private TableBucketWriteResult<WriteResult> completeLakeWriter(
            TableBucket bucket,
            @Nullable String partitionName,
            long logStartOffset,
            long logEndOffset,
            long maxTimestamp,
            boolean moreToTier)
            throws IOException {
        LakeWriter<WriteResult> lakeWriter = lakeWriters.remove(bucket);
        WriteResult writeResult = null;
        if (lakeWriter != null) {
            try {
                writeResult = lakeWriter.complete();
            } catch (Exception e) {
                // make sure the lake writer is always closed to release resources
                // when complete fails, otherwise resources like arrow buffers leak
                IOUtils.closeQuietly(lakeWriter, "lake writer for bucket " + bucket);
                throw e;
            }
            lakeWriter.close();
        }
        return new TableBucketWriteResult<>(
                currentTablePath,
                bucket,
                partitionName,
                writeResult,
                logStartOffset,
                logEndOffset,
                maxTimestamp,
                checkNotNull(currentTableNumberOfSplits),
                null,
                moreToTier,
                TieringSplit.UNKNOWN_TIERING_EPOCH);
    }

    private TableBucketWriteResultWithSplitIds forEmptySplits(Set<TieringSplit> emptySplits) {
        Map<TableBucket, TableBucketWriteResult<WriteResult>> writeResults = new HashMap<>();
        Map<TableBucket, String> finishedSplitIds = new HashMap<>();
        for (TieringSplit tieringSplit : emptySplits) {
            TableBucket tableBucket = tieringSplit.getTableBucket();
            finishedSplitIds.put(tableBucket, tieringSplit.splitId());
            writeResults.put(
                    tableBucket,
                    toTableBucketWriteResult(
                            tieringSplit.getTablePath(),
                            tableBucket,
                            tieringSplit.getPartitionName(),
                            null,
                            UNKNOWN_BUCKET_OFFSET,
                            UNKNOWN_BUCKET_TIMESTAMP,
                            tieringSplit.getNumberOfSplits()));
        }
        return new TableBucketWriteResultWithSplitIds(writeResults, finishedSplitIds);
    }

    private void mayFinishCurrentTable() throws IOException {
        // no any pending splits for the table, just finish the table
        if (currentTableSplitsByBucket.isEmpty()) {
            finishCurrentTable();
        }
    }

    private TableBucketWriteResultWithSplitIds finishCurrentSnapshotSplit() throws IOException {
        TableBucket tableBucket = currentSnapshotSplit.getTableBucket();
        long logEndOffset = currentSnapshotSplit.getLogOffsetOfSnapshot();
        String splitId = currentTableSplitsByBucket.remove(tableBucket).splitId();
        TableBucketWriteResult<WriteResult> writeResult =
                completeLakeWriter(
                        tableBucket,
                        currentSnapshotSplit.getPartitionName(),
                        TableBucketWriteResult.UNKNOWN_LOG_START_OFFSET,
                        logEndOffset,
                        UNKNOWN_BUCKET_TIMESTAMP,
                        false);
        LOG.info(
                "Finish tier bucket {} for table {}, split: {}.",
                tableBucket,
                currentTablePath,
                splitId);
        closeCurrentSnapshotSplit();
        mayFinishCurrentTable();
        return new TableBucketWriteResultWithSplitIds(
                Collections.singletonMap(tableBucket, writeResult),
                Collections.singletonMap(tableBucket, splitId));
    }

    private TableBucketWriteResultWithSplitIds forSnapshotSplitRecords(
            TableBucket bucket, CloseableIterator<RecordAndPos> recordIterator) throws IOException {
        LakeWriter<WriteResult> lakeWriter = null;
        while (recordIterator.hasNext()) {
            ScanRecord scanRecord = recordIterator.next().record();
            if (lakeWriter == null) {
                lakeWriter =
                        getOrCreateLakeWriter(
                                bucket,
                                checkNotNull(currentSnapshotSplit).getPartitionName(),
                                currentSnapshotSplit.getSplitIndex(),
                                currentSnapshotSplit.getTieringRoundTimestamp());
            }
            lakeWriter.write(scanRecord);
            if (scanRecord.getSizeInBytes() > 0) {
                tieringMetrics.recordBytesRead(scanRecord.getSizeInBytes());
            }
        }
        recordIterator.close();
        return emptyTableBucketWriteResultWithSplitIds();
    }

    private TableBucketWriteResultWithSplitIds emptyTableBucketWriteResultWithSplitIds() {
        return new TableBucketWriteResultWithSplitIds();
    }

    private void closeCurrentSnapshotSplit() throws IOException {
        try {
            currentSnapshotSplitReader.close();
        } catch (Exception e) {
            throw new IOException("Fail to close current snapshot split reader.", e);
        }
        currentSnapshotSplitReader = null;
        currentSnapshotSplit = null;
    }

    private void finishCurrentTable() throws IOException {
        // defensive cleanup: lake writers should have been completed and closed when
        // their splits finished, close any residual writers before closing the log
        // scanner since their resources may be allocated from the scanner
        closeAllLakeWriters();
        try {
            if (currentLogScanner != null) {
                currentLogScanner.close();
                currentLogScanner = null;
            }

            if (currentSnapshotSplitReader != null) {
                currentSnapshotSplitReader.close();
                currentSnapshotSplitReader = null;
            }

            if (currentTable != null) {
                currentTable.close();
                currentTable = null;
            }
        } catch (Exception e) {
            throw new IOException("Fail to finish current table.", e);
        }
        reachTieringMaxDurationTables.remove(currentTableId);
        nextOverdueRoundsCheckMs = 0;
        subscribedBuckets.clear();
        // before switch to a new table, mark all as empty or null
        currentTableId = null;
        currentTablePath = null;
        currentTableNumberOfSplits = null;
        currentTableUseRecordBatchPath = null;
        currentPendingSnapshotSplits.clear();
        currentTableStoppingOffsets.clear();
        currentTableTieredOffsetAndTimestamp.clear();
        currentTableSplitsByBucket.clear();
        currentTablePartitionBucketCounts.clear();
        releasePlannedRounds();
    }

    /**
     * Handle a table reach max tiering duration. This will mark the current table as reaching max
     * duration, and it will be force completed in the next fetch cycle.
     */
    public void handleTableReachTieringMaxDuration(long tableId) {
        LOG.info(
                "handleTableReachTieringMaxDuration, currentTableId: {}, pendingTieringSplits: {}",
                currentTableId,
                pendingTieringSplits);
        if ((currentTableId != null && currentTableId.equals(tableId))
                || pendingTieringSplits.containsKey(tableId)) {
            LOG.info("Table {} reach tiering max duration, will force to complete.", tableId);
            reachTieringMaxDurationTables.add(tableId);
        }
    }

    @Override
    public void wakeUp() {
        wokenUp.complete(null);
        // also called by the fetches of round deletes, while the reader may close the scanner
        LogScanner scanner = currentLogScanner;
        if (scanner != null) {
            scanner.wakeup();
        }
    }

    @Override
    public void close() throws Exception {
        // lake writers must be closed before the log scanner since their resources
        // (e.g. arrow buffers) may be allocated from the scanner;
        // the connection is owned and closed by TieringSourceReader
        List<AutoCloseable> closeables = new ArrayList<>(lakeWriters.values());
        closeables.add(currentSnapshotSplitReader);
        closeables.add(currentLogScanner);
        closeables.add(currentTable);

        // clear the fields to make close() idempotent
        lakeWriters.clear();
        releasePlannedRounds();
        if (maskFetchers != null) {
            maskFetchers.shutdownNow();
            maskFetchers = null;
        }
        for (AwaitingDeletes awaiting : awaitingDeletes.values()) {
            awaiting.deletes.cancel(true);
            discardWhenPrepared(awaiting);
        }
        awaitingDeletes.clear();
        if (deletesFetchers != null) {
            deletesFetchers.shutdownNow();
            deletesFetchers = null;
        }
        if (ownsPreparers) {
            closeables.add(preparers);
        }
        currentSnapshotSplitReader = null;
        currentLogScanner = null;
        currentTable = null;

        IOUtils.closeAll(closeables);
    }

    private void closeAllLakeWriters() {
        if (lakeWriters.isEmpty()) {
            return;
        }
        LOG.warn(
                "{} lake writer(s) of table {} are still in-flight, closing them.",
                lakeWriters.size(),
                currentTablePath);
        for (Map.Entry<TableBucket, LakeWriter<WriteResult>> entry : lakeWriters.entrySet()) {
            IOUtils.closeQuietly(entry.getValue(), "lake writer for bucket " + entry.getKey());
        }
        lakeWriters.clear();

        // don't need to close connection, will be closed by TieringSourceReader
    }

    /**
     * Processes a row position split: re-indexes the rewritten lake files of one bucket. A split of
     * a table dropped meanwhile ends without indexing. Its held round then fails in the committer,
     * and the other tables' rounds go on. A split that fails otherwise, e.g. on two live rows of
     * one key, ends with a failed result, which fails only its table's round too.
     */
    @SuppressWarnings("unchecked")
    private TableBucketWriteResultWithSplitIds processRowPosSplit(TieringRowPosSplit rowPosSplit)
            throws IOException {
        TableInfo rowPosTableInfo;
        try (Table table = connection.getTable(rowPosSplit.getTablePath())) {
            rowPosTableInfo = table.getTableInfo();
        } catch (Exception e) {
            if (isDropped(rowPosSplit)) {
                return droppedTableResult(rowPosSplit);
            }
            return failedResult(
                    rowPosSplit,
                    new IOException(
                            "Failed to load table info for " + rowPosSplit.getTablePath(), e));
        }
        if (rowPosTableInfo.getTableId() != rowPosSplit.getTableBucket().getTableId()) {
            return droppedTableResult(rowPosSplit);
        }
        try {
            // the lake table may live under a custom lake table path
            LakeSource<?> lakeSource =
                    DeletionVectorCapabilities.tiering(
                                    lakeTieringFactory, rowPosSplit.getTablePath())
                            .createLakeSource(rowPosTableInfo.getLakeTablePath());
            return processRowPosSplitInternal(
                    rowPosSplit, (LakeSource<LakeSplit>) lakeSource, rowPosTableInfo);
        } catch (IOException | RuntimeException e) {
            // Dropping the table may drop its lake table too, while its files are read.
            if (isDropped(rowPosSplit)) {
                return droppedTableResult(rowPosSplit);
            }
            if (e instanceof InterruptedIOException || Thread.currentThread().isInterrupted()) {
                throw e; // the reader is closing
            }
            return failedResult(rowPosSplit, e);
        }
    }

    /**
     * The result of a re-index split that failed. Failing the reader would fail every table's
     * round, and the job again on every retry if the failure is in the data. The result fails the
     * held round of the split's table instead.
     */
    private TableBucketWriteResultWithSplitIds failedResult(
            TieringRowPosSplit rowPosSplit, Exception cause) {
        LOG.warn(
                "Re-index split {} of table {} failed; the round of the table fails.",
                rowPosSplit.splitId(),
                rowPosSplit.getTablePath(),
                cause);
        TableBucketWriteResult<WriteResult> result =
                new TableBucketWriteResult<>(
                        rowPosSplit.getTablePath(),
                        rowPosSplit.getTableBucket(),
                        rowPosSplit.getPartitionName(),
                        null,
                        -1,
                        -1,
                        rowPosSplit.getNumberOfSplits(),
                        RowPosResult.failed(
                                rowPosSplit.getChangeId(),
                                rowPosSplit.getSplit(),
                                ExceptionUtils.stringifyException(cause)));
        return new TableBucketWriteResultWithSplitIds(
                Collections.singletonMap(rowPosSplit.getTableBucket(), result),
                Collections.singletonMap(rowPosSplit.getTableBucket(), rowPosSplit.splitId()));
    }

    /** Whether the split's table was dropped, or dropped and created again under its path. */
    private boolean isDropped(TieringSplit split) {
        try {
            // the admin is a shared per-connection instance, so it must not be closed here
            TableInfo tableInfo = connection.getAdmin().getTableInfo(split.getTablePath()).get();
            return tableInfo.getTableId() != split.getTableBucket().getTableId();
        } catch (Exception e) {
            return ExceptionUtils.findThrowable(e, TableNotExistException.class).isPresent();
        }
    }

    /**
     * The result of a re-index split of a dropped table. It has no index part, so the held round
     * completes and fails on the dropped table instead of the reader failing the job.
     */
    private TableBucketWriteResultWithSplitIds droppedTableResult(TieringRowPosSplit rowPosSplit) {
        LOG.info(
                "Table {} (id {}) was dropped; its re-index split {} ends without indexing.",
                rowPosSplit.getTablePath(),
                rowPosSplit.getTableBucket().getTableId(),
                rowPosSplit.splitId());
        TableBucketWriteResult<WriteResult> result =
                new TableBucketWriteResult<>(
                        rowPosSplit.getTablePath(),
                        rowPosSplit.getTableBucket(),
                        rowPosSplit.getPartitionName(),
                        null,
                        -1,
                        -1,
                        rowPosSplit.getNumberOfSplits(),
                        new RowPosResult(
                                rowPosSplit.getChangeId(),
                                rowPosSplit.getSplit(),
                                Collections.emptyMap(),
                                Collections.emptyList()));
        return new TableBucketWriteResultWithSplitIds(
                Collections.singletonMap(rowPosSplit.getTableBucket(), result),
                Collections.singletonMap(rowPosSplit.getTableBucket(), rowPosSplit.splitId()));
    }

    /**
     * Indexes every live row of the split's files by its encoded primary key, the way tablet
     * servers look keys up, and writes the result as the bucket's index part of the re-index.
     */
    private <Split extends LakeSplit> TableBucketWriteResultWithSplitIds processRowPosSplitInternal(
            TieringRowPosSplit rowPosSplit, LakeSource<Split> lakeSource, TableInfo tableInfo)
            throws IOException {
        long t0 = System.currentTimeMillis();
        RowType rowType = tableInfo.getRowType();
        List<String> pkNames = tableInfo.getPhysicalPrimaryKeys();
        int[] pkIndexes = new int[pkNames.size()];
        int[][] projection = new int[pkNames.size()][];
        for (int i = 0; i < pkNames.size(); i++) {
            pkIndexes[i] = rowType.getFieldIndex(pkNames.get(i));
            projection[i] = new int[] {pkIndexes[i]};
        }
        lakeSource.withProject(projection);
        KeyEncoder keyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        rowType.project(pkIndexes),
                        pkNames,
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());

        SimpleVersionedSerializer<Split> splitSerializer = lakeSource.getSplitSerializer();
        int[] fileIds = rowPosSplit.getFileIds();
        PositionIndexStore.ReindexPlan plan =
                PositionIndexStore.readReindexPlan(rowPosSplit.getPlanReference());
        if (plan.lakeSplits.size() != fileIds.length) {
            throw new IOException(
                    String.format(
                            "The re-index plan %s has %d files, but the split has %d file ids.",
                            rowPosSplit.getPlanReference(),
                            plan.lakeSplits.size(),
                            fileIds.length));
        }
        Map<Integer, String> newFileDictEntries = new HashMap<>();
        File localTmpDir = localTmpDir("dv-reindex");
        PositionIndexStore.BucketPart part;
        long rows;
        // Sorts the keys in bounded memory: a split's files may hold more rows than fit in the
        // heap. Two live rows of one key fail the split, since picking one would leave the other
        // unmasked.
        try (SpillingPositionSorter entries =
                new SpillingPositionSorter(
                        localTmpDir,
                        reindexSortBufferEntries,
                        (key, first, second) ->
                                new IllegalStateException(
                                        String.format(
                                                "DV re-index of %s bucket %s: two live rows of one"
                                                        + " key, in %s at %d and in %s at %d.",
                                                rowPosSplit.getTablePath(),
                                                rowPosSplit.getTableBucket(),
                                                newFileDictEntries.get(first.fileId()),
                                                first.rowPosition(),
                                                newFileDictEntries.get(second.fileId()),
                                                second.rowPosition())))) {
            for (int idx = 0; idx < fileIds.length; idx++) {
                Split lakeSplit =
                        splitSerializer.deserialize(
                                plan.splitSerializerVersion, plan.lakeSplits.get(idx));
                int fileId = fileIds[idx];
                newFileDictEntries.put(fileId, DeletionVectorCapabilities.onlyFileName(lakeSplit));
                try (CloseableIterator<RowWithPosResult> iter =
                        RecordReaders.readWithPos(lakeSource.createRecordReader(() -> lakeSplit))) {
                    while (iter.hasNext()) {
                        RowWithPosResult rwp = iter.next();
                        byte[] key = keyEncoder.encodeKey(rwp.getRow());
                        entries.add(
                                Arrays.copyOf(key, key.length), new FilePos(fileId, rwp.getPos()));
                    }
                }
            }
            rows = entries.size();
            // The committer commits and registers this part along with the other buckets'.
            part =
                    PositionIndexStore.forTable(
                                    new FsPath(rowPosSplit.getRemoteUploadBasePath()), localTmpDir)
                            .writeBucket(
                                    rowPosSplit.getChangeId(),
                                    rowPosSplit.getTableBucket(),
                                    // a bucket's splits write different SSTs
                                    "s" + rowPosSplit.getSplit() + "_",
                                    entries.sorted());
        }
        List<RowPosResult.SstMeta> resultMetas = new ArrayList<>();
        for (RowPosSstIndex.SstFileEntry f : part.files) {
            resultMetas.add(new RowPosResult.SstMeta(f.getFileName(), f.getFileSize()));
        }
        LOG.info(
                "DV re-index of {} bucket {} (split {}): {} files, {} live rows in {} ms.",
                rowPosSplit.getTablePath(),
                rowPosSplit.getTableBucket(),
                rowPosSplit.getSplit(),
                fileIds.length,
                rows,
                System.currentTimeMillis() - t0);
        TableBucketWriteResult<WriteResult> result =
                new TableBucketWriteResult<>(
                        rowPosSplit.getTablePath(),
                        rowPosSplit.getTableBucket(),
                        rowPosSplit.getPartitionName(),
                        null,
                        -1,
                        -1,
                        rowPosSplit.getNumberOfSplits(),
                        new RowPosResult(
                                rowPosSplit.getChangeId(),
                                rowPosSplit.getSplit(),
                                newFileDictEntries,
                                resultMetas));
        return new TableBucketWriteResultWithSplitIds(
                Collections.singletonMap(rowPosSplit.getTableBucket(), result),
                Collections.singletonMap(rowPosSplit.getTableBucket(), rowPosSplit.splitId()));
    }

    private void subscribeLog(TieringLogSplit logSplit) {
        // assign bucket offset dynamically
        TableBucket tableBucket = logSplit.getTableBucket();
        long stoppingOffset = logSplit.getStoppingOffset();
        long startingOffset = logSplit.getStartingOffset();
        if (startingOffset >= stoppingOffset || stoppingOffset <= 0) {
            currentEmptySplits.add(logSplit);
            return;
        } else {
            currentTableStoppingOffsets.put(tableBucket, stoppingOffset);
        }
        mayCreateLogScanner();
        if (logSplit.isPlannedRound()) {
            PlannedRound round = new PlannedRound(logSplit, roundMasks, System.currentTimeMillis());
            // the bucket is subscribed once the reader decided how its round is written
            plannedRounds.put(tableBucket, round);
            return;
        }
        subscribe(tableBucket, startingOffset);
        LOG.info(
                "Subscribe to read log for split {} from starting offset {} to end offset {}.",
                logSplit.splitId(),
                startingOffset,
                stoppingOffset);
    }

    /**
     * Starts the mask fetches of the planned rounds of the current table that fit in the masks'
     * memory, and subscribes the rounds whose masks came. A round whose mask fetch failed because
     * its leader is not reachable, or not ready for longer than a round waits, is fetched again;
     * past the time limit such a round ends without writing instead. Returns the rounds whose mask
     * fetch failed because their table or partition was dropped, which end without writing now. Any
     * other failure fails the round.
     */
    private Map<TableBucket, String> subscribeDecidedRounds() throws IOException {
        long maskBytes = 0;
        for (PlannedRound round : plannedRounds.values()) {
            maskBytes += round.maskBytes();
        }
        Map<TableBucket, String> gone = new HashMap<>();
        for (PlannedRound round : plannedRounds.values()) {
            if (round.subscribed()) {
                continue;
            }
            if (round.waitsToFetch()) {
                // a mask that does not fit waits for the rounds before it to end; one always fits
                long maxBytes = PlannedRound.maxMaskBytes(round.split());
                if (maskBytes > 0 && maskBytes + maxBytes > maskMemory) {
                    continue;
                }
                maskBytes += maxBytes;
                round.startFetch(fetchRoundMask(round.split()), System.currentTimeMillis());
            }
            Throwable failure = round.failure();
            if (failure != null) {
                TieringLogSplit split = round.split();
                if (bucketGone(failure) && bucketGone(split)) {
                    gone.put(
                            split.getTableBucket(),
                            "its table or partition was dropped: " + failure.getMessage());
                    continue;
                }
                if (!transientFailure(failure) && !bucketGone(failure)) {
                    throw new IOException(
                            String.format(
                                    "Failed to get the log mask of the round of bucket %s up to"
                                            + " offset %d.",
                                    split.getTableBucket(), split.getStoppingOffset()),
                            failure);
                }
                if (!reachTieringMaxDurationTables.contains(currentTableId)) {
                    LOG.info(
                            "The log mask of the round of bucket {} up to offset {} is fetched"
                                    + " again: {}",
                            split.getTableBucket(),
                            split.getStoppingOffset(),
                            failure.getMessage());
                    round.fetchAgain(fetchRoundMask(split));
                }
                continue;
            }
            subscribeDecidedRound(round);
        }
        return gone;
    }

    /** Ends planned rounds of the current table without writing, for the given reasons. */
    private RecordsWithSplitIds<TableBucketWriteResult<WriteResult>> endRoundsWithoutWriting(
            Map<TableBucket, String> rounds) throws IOException {
        Map<TableBucket, TableBucketWriteResult<WriteResult>> writeResults = new HashMap<>();
        Map<TableBucket, String> finishedSplitIds = new HashMap<>();
        for (Map.Entry<TableBucket, String> e : rounds.entrySet()) {
            TieringSplit split = checkNotNull(currentTableSplitsByBucket.remove(e.getKey()));
            writeResults.put(
                    e.getKey(), endRoundWithoutWriting(split.asTieringLogSplit(), e.getValue()));
            finishedSplitIds.put(e.getKey(), split.splitId());
        }
        mayFinishCurrentTable();
        return new TableBucketWriteResultWithSplitIds(writeResults, finishedSplitIds);
    }

    private void subscribeDecidedRound(PlannedRound round) throws IOException {
        if (!round.tryDecide(System.currentTimeMillis())) {
            return;
        }
        TieringLogSplit split = round.split();
        if (round.sortModeBecause() != null) {
            tieringMetrics.dvRoundMaskMismatch();
            LOG.warn(
                    "{} The round of bucket {} of {} up to offset {} is written in sort mode.",
                    round.sortModeBecause(),
                    split.getTableBucket(),
                    currentTablePath,
                    split.getStoppingOffset());
        }
        round.subscribed(true);
        subscribe(split.getTableBucket(), split.getStartingOffset());
        // the round's deletes depend on its snapshot and offsets only, not on how it is written;
        // on the empty lake it deletes nothing
        if (roundDeletesTimeoutMs > 0 && split.getRoundSnapshotId() > 0) {
            CompletableFuture<RoundDeletes> deletes = fetchRoundDeletes(split);
            round.startDeletesFetch(deletes);
            // what preparing the round's deletion vectors reads is read while the round is
            DeletionVectorPreparer<WriteResult> preparer =
                    preparer(split.getTableBucket(), split.getRoundSnapshotId(), false);
            if (preparer != null) {
                prefetch(preparer, split, deletes);
            }
        }
        RoundMask mask = round.mask();
        LOG.info(
                "Subscribe to read log for split {} from starting offset {} to end offset {}{}.",
                split.splitId(),
                split.getStartingOffset(),
                split.getStoppingOffset(),
                mask != null
                        ? String.format(
                                ", through its leader's log mask of %d offsets in %d bytes",
                                mask.maskedOffsets(), mask.bytes())
                        : ", in sort mode");
    }

    /** Whether the split's table or partition was dropped, as the admin confirms. */
    private boolean bucketGone(TieringLogSplit split) {
        if (isDropped(split)) {
            return true;
        }
        Long partitionId = split.getTableBucket().getPartitionId();
        if (partitionId == null) {
            return false;
        }
        try {
            for (PartitionInfo partition :
                    connection.getAdmin().listPartitionInfos(split.getTablePath()).get()) {
                if (partition.getPartitionId() == partitionId) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            return ExceptionUtils.findThrowable(e, TableNotExistException.class).isPresent();
        }
    }

    /** Whether a mask fetch failed as the bucket's table or partition is gone. */
    private static boolean bucketGone(Throwable failure) {
        return ExceptionUtils.findThrowable(failure, PartitionNotExistException.class).isPresent()
                || ExceptionUtils.findThrowable(failure, TableNotExistException.class).isPresent()
                || ExceptionUtils.findThrowable(failure, UnknownTableOrBucketException.class)
                        .isPresent();
    }

    /** Whether a mask fetch failed only for now: the leader moved or was not ready long enough. */
    private static boolean transientFailure(Throwable failure) {
        return DvRequestRetries.isLeaderUnavailable(failure)
                || DvRequestRetries.isNotReady(failure);
    }

    /** Stops reading the bucket, if the log scanner reads it. */
    private void unsubscribe(TableBucket tableBucket) {
        if (!subscribedBuckets.remove(tableBucket)) {
            return;
        }
        Long partitionId = tableBucket.getPartitionId();
        if (partitionId != null) {
            checkNotNull(currentLogScanner).unsubscribe(partitionId, tableBucket.getBucket());
        } else {
            checkNotNull(currentLogScanner).unsubscribe(tableBucket.getBucket());
        }
    }

    private void subscribe(TableBucket tableBucket, long startingOffset) {
        Long partitionId = tableBucket.getPartitionId();
        int bucket = tableBucket.getBucket();
        checkNotNull(currentLogScanner, "current log scanner shouldn't be null.");
        subscribedBuckets.add(tableBucket);
        if (partitionId != null) {
            currentLogScanner.subscribe(partitionId, bucket, startingOffset);
        } else {
            // If no partition id, subscribe by bucket only.
            currentLogScanner.subscribe(bucket, startingOffset);
        }
    }

    /** The requests of the rounds of the table, which wait for an answer at most the client's. */
    private DvRoundRequests roundRequests(TablePath tablePath) {
        return new DvRoundRequests(
                connection.getAdmin(),
                tablePath,
                Thread::sleep,
                connection.getConfiguration().get(ConfigOptions.CLIENT_REQUEST_TIMEOUT).toMillis());
    }

    /**
     * Fetches the log mask of a planned round from its bucket's leader in the background, so the
     * reader reads other buckets meanwhile.
     */
    @VisibleForTesting
    protected CompletableFuture<RoundMask.Fetched> fetchRoundMask(TieringLogSplit split) {
        DvRoundRequests requests = roundRequests(split.getTablePath());
        TableBucket tb = split.getTableBucket();
        long snapshotId = split.getRoundSnapshotId();
        long end = split.getStoppingOffset();
        CompletableFuture<RoundMask.Fetched> fetched = new CompletableFuture<>();
        if (maskFetchers == null) {
            ThreadPoolExecutor fetchers =
                    new ThreadPoolExecutor(
                            MASK_FETCH_THREADS,
                            MASK_FETCH_THREADS,
                            1,
                            TimeUnit.MINUTES,
                            new LinkedBlockingQueue<>(),
                            new ExecutorThreadFactory("fluss-tiering-round-masks"));
            fetchers.allowCoreThreadTimeOut(true);
            maskFetchers = fetchers;
        }
        maskFetchers.execute(
                () -> {
                    try {
                        fetched.complete(
                                RoundMask.fetch(
                                        (from, waitMs) ->
                                                requests.fetchMask(
                                                        tb, snapshotId, from, end, waitMs),
                                        split,
                                        DvRoundRequests.MAX_NOT_READY_WAIT_MS));
                    } catch (Throwable t) {
                        fetched.completeExceptionally(t);
                    }
                });
        return fetched;
    }

    /**
     * Waits a poll's time at most for the mask of a planned round that waits for one; past the time
     * limit, or with no fetch running, until the next check of the rounds past the time limit.
     */
    private void waitForAMask() throws IOException {
        List<CompletableFuture<?>> fetches = new ArrayList<>();
        for (PlannedRound round : plannedRounds.values()) {
            CompletableFuture<?> fetch = round.fetch();
            if (!round.subscribed() && fetch != null && !fetch.isDone()) {
                fetches.add(fetch);
            }
        }
        if (fetches.isEmpty() || reachTieringMaxDurationTables.contains(currentTableId)) {
            awaitAny(fetches, Math.min(pollTimeout.toMillis(), OVERDUE_ROUNDS_CHECK_INTERVAL_MS));
        } else {
            awaitAny(fetches);
        }
    }

    /**
     * Waits a poll's time at most for one of {@code fetches} or of the deletes the write results of
     * rounds wait for, and no longer than the first of those may wait.
     */
    private void awaitAny(List<CompletableFuture<?>> fetches) throws IOException {
        awaitAny(fetches, pollTimeout.toMillis());
    }

    private void awaitAny(List<CompletableFuture<?>> fetches, long maxWaitMs) throws IOException {
        CompletableFuture<Void> woken = wokenUp;
        List<CompletableFuture<?>> awaited = new ArrayList<>(fetches);
        awaited.add(woken);
        for (AwaitingDeletes awaiting : awaitingDeletes.values()) {
            awaited.add(awaiting.prepared != null ? awaiting.prepared : awaiting.deletes);
        }
        long waitMs = Math.min(maxWaitMs, untilFirstDeletesDeadlineMs());
        try {
            CompletableFuture.anyOf(awaited.toArray(new CompletableFuture<?>[0]))
                    .get(waitMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for a round's leader", e);
        } catch (ExecutionException | CancellationException | TimeoutException e) {
            // the next fetch looks at the masks and deletes again
        }
        if (woken.isDone()) {
            wokenUp = new CompletableFuture<>();
        }
    }

    /** The poll's timeout, cut to when the first write result waiting for deletes goes on. */
    private Duration nextPollTimeout() {
        long untilMs = untilFirstDeletesDeadlineMs();
        return untilMs < pollTimeout.toMillis() ? Duration.ofMillis(untilMs) : pollTimeout;
    }

    /** How long until the first write result waiting for its deletes stops waiting. */
    private long untilFirstDeletesDeadlineMs() {
        long untilMs = Long.MAX_VALUE;
        long now = System.currentTimeMillis();
        for (AwaitingDeletes awaiting : awaitingDeletes.values()) {
            untilMs = Math.min(untilMs, Math.max(0, awaiting.deadlineMs - now));
        }
        return untilMs;
    }

    /**
     * Fetches the deletes of a planned round from its bucket's leader in the background, while the
     * reader reads the round. Waits for a leader that is not ready no longer than a round's write
     * result waits for them; cancelling the future stops the fetch.
     */
    @VisibleForTesting
    protected CompletableFuture<RoundDeletes> fetchRoundDeletes(TieringLogSplit split) {
        DvRoundRequests requests = roundRequests(split.getTablePath());
        CompletableFuture<RoundDeletes> fetched = new CompletableFuture<>();
        Future<?> task =
                deletesFetchers()
                        .submit(
                                () -> {
                                    try {
                                        fetched.complete(
                                                RoundDeletes.fetch(
                                                        split.getTableBucket(),
                                                        split.getRoundSnapshotId(),
                                                        split.getStartingOffset(),
                                                        split.getStoppingOffset(),
                                                        roundDeletesMaxBytes,
                                                        (tb, snapshotId, from, end) ->
                                                                requests.fetch(
                                                                        tb,
                                                                        snapshotId,
                                                                        from,
                                                                        end,
                                                                        roundDeletesTimeoutMs)));
                                    } catch (Throwable t) {
                                        fetched.completeExceptionally(t);
                                    }
                                });
        fetched.whenComplete(
                (deletes, failure) -> {
                    if (fetched.isCancelled()) {
                        task.cancel(true);
                    }
                });
        return fetched;
    }

    /**
     * Drops the fetched deletes of the rounds being read that do not fit in {@code
     * lake.tiering.deletion-vectors.round-deletes.prefetch-max-bytes}, keeping those of the rounds
     * that came first. The committer fetches the dropped ones itself.
     */
    private void dropRoundDeletesOverBudget() {
        for (PlannedRound round :
                PlannedRound.dropDeletesOverBudget(plannedRounds.values(), roundDeletesMaxBytes)) {
            LOG.info(
                    "The deletes of the round of bucket {} up to offset {} are dropped, as the"
                            + " reader holds {} bytes of round deletes at most; the committer"
                            + " fetches them from the leader.",
                    round.split().getTableBucket(),
                    round.split().getStoppingOffset(),
                    roundDeletesMaxBytes);
        }
    }

    /**
     * Holds the write result of a round whose writer completed until its deletes are fetched, for
     * {@code lake.tiering.deletion-vectors.round-deletes.prefetch-timeout} at most. Its split ends
     * when the result goes on.
     */
    private void awaitDeletes(
            TableBucketWriteResult<WriteResult> writeResult,
            String splitId,
            CompletableFuture<RoundDeletes> deletes,
            @Nullable CompletableFuture<TableBucketWriteResult<WriteResult>> prepared,
            @Nullable DeletionVectorPreparer<WriteResult> preparer) {
        long now = System.currentTimeMillis();
        AwaitingDeletes replaced =
                awaitingDeletes.put(
                        writeResult.tableBucket(),
                        new AwaitingDeletes(
                                writeResult,
                                splitId,
                                deletes,
                                prepared,
                                preparer,
                                now,
                                now + roundDeletesTimeoutMs));
        checkState(
                replaced == null,
                "The write result of split %s waits for its round's deletes already.",
                replaced == null ? null : replaced.splitId);
        // a poll that waits for records returns once the deletes are fetched and prepared
        (prepared != null ? prepared : deletes).whenComplete((done, failure) -> wakeUp());
    }

    /**
     * The write result with the deletion vectors its writer prepared from the round's deletes, or
     * with the deletes if they are not prepared, e.g. the round compacted the bucket. Preparing is
     * best effort: on a failure the committer merges the deletes as it does without it.
     */
    private TableBucketWriteResult<WriteResult> prepared(
            TableBucketWriteResult<WriteResult> writeResult,
            RoundDeletes deletes,
            DeletionVectorPreparer<WriteResult> preparer) {
        try {
            WriteResult prepared =
                    preparer.prepare(
                            checkNotNull(writeResult.writeResult()),
                            writeResult.tableBucket(),
                            deletes.snapshotId(),
                            deletes.startOffset(),
                            deletes.endOffset(),
                            DeletedPositions.of(deletes));
            return prepared == null
                    ? writeResult.withRoundDeletes(deletes)
                    : writeResult.withWriteResult(prepared);
        } catch (Exception e) {
            preparationFailed(writeResult.tableBucket(), deletes.snapshotId(), e);
            return writeResult.withRoundDeletes(deletes);
        }
    }

    /**
     * The preparer of the deletion vectors of the current table's rounds, or null if the lake, the
     * table or the options have none. Creating it is best effort too: a failure counts as one of
     * the bucket's round on {@code snapshotId} if {@code counted}, and the next round tries again.
     */
    @Nullable
    @SuppressWarnings("unchecked")
    private DeletionVectorPreparer<WriteResult> preparer(
            TableBucket bucket, long snapshotId, boolean counted) {
        Table table = checkNotNull(currentTable);
        TableInfo tableInfo = table.getTableInfo();
        if (!writersPrepareDeletionVectors
                || !tableInfo.isDeletionVectorsEnabled()
                || !(lakeTieringFactory instanceof SupportsDeletionVectorPreparation)) {
            return null;
        }
        Optional<DeletionVectorPreparer<WriteResult>> preparer =
                preparers.get(tableInfo.getTableId());
        if (preparer == null) {
            try {
                preparer =
                        Optional.ofNullable(
                                ((SupportsDeletionVectorPreparation<WriteResult>)
                                                lakeTieringFactory)
                                        .createDeletionVectorPreparer(
                                                tableInfo, lakeTieringConfig));
            } catch (Exception e) {
                if (counted) {
                    preparationFailed(bucket, snapshotId, e);
                }
                return null;
            }
            preparers.put(tableInfo.getTableId(), preparer);
        }
        return preparer.orElse(null);
    }

    /** Counts a failed preparation, and logs the first of the table's round. */
    private void preparationFailed(TableBucket bucket, long snapshotId, Exception e) {
        tieringMetrics.recordVectorsNotPrepared();
        if (preparationFailuresLogged.add(bucket.getTableId() + "@" + snapshotId)) {
            LOG.warn(
                    "Could not prepare the deletion vectors of the round of bucket {} on snapshot"
                            + " {}; the committer merges its deletes. Later failures of the"
                            + " table's round are logged at DEBUG.",
                    bucket,
                    snapshotId,
                    e);
        } else {
            LOG.debug(
                    "Could not prepare the deletion vectors of the round of bucket {}.", bucket, e);
        }
    }

    /**
     * Lets the preparer read ahead: the live files of the round's snapshot now, and the current
     * deletion vectors of the files the round's deletes name once they arrive. Best effort.
     */
    private void prefetch(
            DeletionVectorPreparer<WriteResult> preparer,
            TieringLogSplit split,
            CompletableFuture<RoundDeletes> deletes) {
        long snapshotId = split.getRoundSnapshotId();
        try {
            preparer.prefetch(snapshotId);
        } catch (Exception e) {
            LOG.debug(
                    "Could not read ahead for the round of bucket {}.", split.getTableBucket(), e);
        }
        deletes.thenAccept(
                fetched -> {
                    List<String> dataFiles = new ArrayList<>();
                    for (RoundDeletes.File file : fetched.files()) {
                        dataFiles.add(file.path());
                    }
                    try {
                        preparer.prefetch(split.getTableBucket(), snapshotId, dataFiles);
                    } catch (Exception e) {
                        LOG.debug(
                                "Could not read ahead for the round of bucket {}.",
                                split.getTableBucket(),
                                e);
                    }
                });
    }

    /** The threads that fetch, and prepare, the deletes of planned rounds. */
    private ExecutorService deletesFetchers() {
        if (deletesFetchers == null) {
            ThreadPoolExecutor fetchers =
                    new ThreadPoolExecutor(
                            roundDeletesFetchThreads,
                            roundDeletesFetchThreads,
                            1,
                            TimeUnit.MINUTES,
                            new LinkedBlockingQueue<>(),
                            new ExecutorThreadFactory("fluss-tiering-round-deletes"));
            fetchers.allowCoreThreadTimeOut(true);
            deletesFetchers = fetchers;
        }
        return deletesFetchers;
    }

    /** Deletes the deletion vectors the writer of a write result that did not go on prepares. */
    private void discardWhenPrepared(AwaitingDeletes awaiting) {
        if (awaiting.prepared == null) {
            return;
        }
        DeletionVectorPreparer<WriteResult> preparer = checkNotNull(awaiting.preparer);
        awaiting.prepared.thenAccept(
                result -> {
                    if (result.roundDeletes() != null || result.writeResult() == null) {
                        return;
                    }
                    try {
                        preparer.discard(result.writeResult());
                    } catch (Exception e) {
                        LOG.warn(
                                "Could not delete the deletion vectors prepared for bucket {}.",
                                result.tableBucket(),
                                e);
                    }
                });
    }

    /**
     * The write results whose deletes were fetched, failed or are late, with the deletes if
     * fetched; null if none.
     */
    @Nullable
    private TableBucketWriteResultWithSplitIds resultsDoneWaitingForDeletes() {
        if (awaitingDeletes.isEmpty()) {
            return null;
        }
        Map<TableBucket, TableBucketWriteResult<WriteResult>> writeResults = new HashMap<>();
        Map<TableBucket, String> finishedSplitIds = new HashMap<>();
        long now = System.currentTimeMillis();
        Iterator<AwaitingDeletes> it = awaitingDeletes.values().iterator();
        while (it.hasNext()) {
            AwaitingDeletes awaiting = it.next();
            if (!awaiting.done() && now < awaiting.deadlineMs) {
                continue;
            }
            it.remove();
            TableBucket bucket = awaiting.writeResult.tableBucket();
            if (awaiting.done()) {
                LOG.info(
                        "The write result of the round of bucket {} waited {} ms for its deletes.",
                        bucket,
                        now - awaiting.sinceMs);
            }
            if (awaiting.prepared != null
                    && awaiting.prepared.isDone()
                    && !awaiting.prepared.isCompletedExceptionally()) {
                writeResults.put(bucket, awaiting.prepared.join());
            } else {
                // late: the deletes go as they are, and what the writer prepares is deleted
                discardWhenPrepared(awaiting);
                writeResults.put(bucket, withRoundDeletes(awaiting.writeResult, awaiting.deletes));
            }
            finishedSplitIds.put(bucket, awaiting.splitId);
        }
        return writeResults.isEmpty()
                ? null
                : new TableBucketWriteResultWithSplitIds(writeResults, finishedSplitIds);
    }

    /**
     * The write result with the round's deletes, if they were fetched; else as it is, and the
     * committer fetches them itself.
     */
    private TableBucketWriteResult<WriteResult> withRoundDeletes(
            TableBucketWriteResult<WriteResult> writeResult,
            @Nullable CompletableFuture<RoundDeletes> deletes) {
        if (deletes == null) {
            return writeResult;
        }
        if (writeResult.writeResult() == null) {
            // the round wrote nothing, so the committer takes no deletes for it
            deletes.cancel(true);
            return writeResult;
        }
        if (!deletes.isDone()) {
            deletes.cancel(true);
            LOG.info(
                    "The deletes of the round of bucket {} up to offset {} were not fetched within"
                            + " {} ms; the committer fetches them from the leader.",
                    writeResult.tableBucket(),
                    writeResult.logEndOffset(),
                    roundDeletesTimeoutMs);
            return writeResult;
        }
        try {
            return writeResult.withRoundDeletes(deletes.join());
        } catch (CancellationException | CompletionException e) {
            LOG.info(
                    "The deletes of the round of bucket {} up to offset {} could not be fetched;"
                            + " the committer fetches them from the leader.",
                    writeResult.tableBucket(),
                    writeResult.logEndOffset(),
                    ExceptionUtils.stripCompletionException(e));
            return writeResult;
        }
    }

    /** Whether the bucket's lake writer gets its round through the round's log mask. */
    private boolean roundMasked(TableBucket bucket) {
        PlannedRound round = plannedRounds.get(bucket);
        return round != null && round.mask() != null;
    }

    /**
     * Reads a planned round again from its start and writes it in sort mode, as its mask did not
     * match the round, its lake writer found a key the mask missed a version of, or it was not read
     * whole. The lake writer of the first read is closed without a result, which drops its files,
     * so nothing of it is committed; the round's end stays.
     */
    private void readAgainInSortMode(TableBucket bucket, IOException mismatch) {
        PlannedRound round = checkNotNull(plannedRounds.get(bucket));
        tieringMetrics.dvRoundMaskMismatch();
        LOG.warn(
                "{} The round of bucket {} of {} is read again from offset {} and written in sort"
                        + " mode; nothing of the first read is committed.",
                mismatch.getMessage(),
                bucket,
                currentTablePath,
                round.split().getStartingOffset());
        round.readAgainInSortMode(mismatch.getMessage());
        IOUtils.closeQuietly(lakeWriters.remove(bucket), "lake writer for bucket " + bucket);
        currentTableTieredOffsetAndTimestamp.remove(bucket);
        subscribe(bucket, round.split().getStartingOffset());
    }

    /** Ends the planned rounds of the current table, returning the memory of their masks. */
    private void releasePlannedRounds() {
        for (PlannedRound round : plannedRounds.values()) {
            round.release();
        }
        plannedRounds.clear();
    }

    private TableBucketWriteResult<WriteResult> toTableBucketWriteResult(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable WriteResult writeResult,
            long endLogOffset,
            long maxTimestamp,
            int numberOfSplits) {
        return new TableBucketWriteResult<>(
                tablePath,
                tableBucket,
                partitionName,
                writeResult,
                endLogOffset,
                maxTimestamp,
                numberOfSplits);
    }

    private class TableBucketWriteResultWithSplitIds
            implements RecordsWithSplitIds<TableBucketWriteResult<WriteResult>> {

        private final Iterator<TableBucket> bucketIterator;

        private final Map<TableBucket, TableBucketWriteResult<WriteResult>> bucketWriteResults;
        private final Map<TableBucket, String> bucketSplits;

        @Nullable private TableBucketWriteResult<WriteResult> writeResultForCurrentSplit;

        public TableBucketWriteResultWithSplitIds() {
            this(Collections.emptyMap(), Collections.emptyMap());
        }

        public TableBucketWriteResultWithSplitIds(
                Map<TableBucket, TableBucketWriteResult<WriteResult>> bucketWriteResults,
                Map<TableBucket, String> bucketSplits) {
            this.bucketIterator = bucketWriteResults.keySet().iterator();
            this.bucketWriteResults = bucketWriteResults;
            this.bucketSplits = bucketSplits;
        }

        @Nullable
        @Override
        public String nextSplit() {
            if (bucketIterator.hasNext()) {
                TableBucket currentBucket = bucketIterator.next();
                writeResultForCurrentSplit = bucketWriteResults.get(currentBucket);
                return bucketSplits.get(currentBucket);
            } else {
                writeResultForCurrentSplit = null;
                return null;
            }
        }

        @Nullable
        @Override
        public TableBucketWriteResult<WriteResult> nextRecordFromSplit() {
            if (writeResultForCurrentSplit != null) {
                TableBucketWriteResult<WriteResult> bucketWriteResult = writeResultForCurrentSplit;
                writeResultForCurrentSplit = null;
                return bucketWriteResult;
            } else {
                return null;
            }
        }

        @Override
        public Set<String> finishedSplits() {
            return new HashSet<>(bucketSplits.values());
        }
    }

    /**
     * Callback interface for processing records within a single bucket. Encapsulates the
     * differences in write strategy between the row-based (ScanRecord) and Arrow batch
     * (ArrowBatchData) paths.
     *
     * @param <R> the record type (ScanRecord or ArrowBatchData)
     */
    @FunctionalInterface
    private interface BucketRecordsHandler<R> {

        /**
         * Processes the records for a bucket and writes them to the lake.
         *
         * @param bucket the bucket
         * @param records the records for this bucket
         * @param lakeWriterSupplier supplier for lazily creating the lake writer
         * @param stoppingOffset the stopping offset for this bucket
         * @return the timestamp of the last written record, or -1 if no records were written
         * @throws IOException if an I/O error occurs during writing
         */
        long handleRecords(
                TableBucket bucket,
                List<R> records,
                SupplierWithException<LakeWriter<?>, IOException> lakeWriterSupplier,
                long stoppingOffset)
                throws IOException;
    }

    private static boolean checkUnshadedArrowAvailable(ClassLoader classLoader) {
        try {
            Class.forName("org.apache.arrow.vector.VectorSchemaRoot", false, classLoader);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    /** The write result of a round whose writer completed, waiting for the round's deletes. */
    private final class AwaitingDeletes {
        private final TableBucketWriteResult<WriteResult> writeResult;
        private final String splitId;
        private final CompletableFuture<RoundDeletes> deletes;
        // the write result with the deletion vectors its writer prepares, null if it does not
        @Nullable private final CompletableFuture<TableBucketWriteResult<WriteResult>> prepared;
        @Nullable private final DeletionVectorPreparer<WriteResult> preparer;
        private final long sinceMs;
        private final long deadlineMs;

        private AwaitingDeletes(
                TableBucketWriteResult<WriteResult> writeResult,
                String splitId,
                CompletableFuture<RoundDeletes> deletes,
                @Nullable CompletableFuture<TableBucketWriteResult<WriteResult>> prepared,
                @Nullable DeletionVectorPreparer<WriteResult> preparer,
                long sinceMs,
                long deadlineMs) {
            this.writeResult = writeResult;
            this.splitId = splitId;
            this.deletes = deletes;
            this.prepared = prepared;
            this.preparer = preparer;
            this.sinceMs = sinceMs;
            this.deadlineMs = deadlineMs;
        }

        /** Whether the deletes are fetched, and prepared if the writer prepares them. */
        private boolean done() {
            return prepared != null ? prepared.isDone() : deletes.isDone();
        }
    }

    private static final class LogOffsetAndTimestamp {

        private final long logOffset;
        private final long timestamp;

        public LogOffsetAndTimestamp(long logOffset, long timestamp) {
            this.logOffset = logOffset;
            this.timestamp = timestamp;
        }
    }
}
