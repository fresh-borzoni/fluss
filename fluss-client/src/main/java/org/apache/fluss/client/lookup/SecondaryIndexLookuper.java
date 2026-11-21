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

package org.apache.fluss.client.lookup;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.LookupPositionExpiredException;
import org.apache.fluss.exception.LookupPositionNotYetReadableException;
import org.apache.fluss.metadata.BucketReadPosition;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.predicate.Equal;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IndexTableUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Two-hop {@link Lookuper} for a Global Secondary Index.
 *
 * <p>Hop 1: prefix-scan the Index Table by the provided {@code lookupKey} to obtain candidate Index
 * Table rows, each carrying the base table's complete logical primary key.
 *
 * <p>Hop 2: extract the logical basePK via {@code basePkExtractorFromIndexRow}, deduplicate
 * physical candidates by that key, and point-get the main table once per key. The deduplication
 * prevents old and new physical Index Table rows for a recreated partition from emitting the same
 * current main row twice. A main lookup returning an empty result list means the row was deleted
 * (stale Index Table pointer) and is skipped from the aggregated output.
 *
 * <p>Recheck: after Hop 2 returns, every surviving main row is re-validated against the user's
 * original {@code lookupKey}. Index columns are extracted from both sides using positional {@link
 * InternalRow.FieldGetter}s and compared according to their Fluss data types. Any row whose current
 * {@code idxCols} disagree with the lookup key is discarded as a stale index pointer (covers both
 * the async-visibility window and the natural lag during partition-tombstone cleanup).
 *
 * <p>Reads at a cut: for a main table with bucket history, whose Index Table keeps removed pointers
 * with the offset of the removing change, {@link #lookup(InternalRow, Map)} reads at one position
 * per main table bucket. Hop 1 returns the data pointers plus the removed pointers still valid at
 * the cut, and Hop 2 and the recheck run at the position of each candidate's bucket.
 *
 * <p>Threading: Hop 2 is dispatched on {@code continuationExecutor} instead of the thread that
 * completes the Hop 1 future (a Netty I/O thread in production). Hop 2 submits point lookups
 * against the main table, which may synchronously block on a metadata refresh when partition
 * metadata is not cached yet; running that on an I/O thread would stall every RPC response served
 * by that thread and can even deadlock the metadata fetch itself.
 */
@Internal
@ThreadSafe
public final class SecondaryIndexLookuper implements Lookuper {

    /** The columns of the Index Table by which its data rows of one lookup key are read. */
    private static final List<String> INDEX_LOOKUP_COLUMNS =
            Arrays.asList(
                    IndexTableUtils.RECORD_KIND_SYSTEM_COLUMN,
                    IndexTableUtils.ROUTING_KEY_SYSTEM_COLUMN);

    /** The position of a main table bucket that a read at a cut reads as empty. */
    public static final long EMPTY_BUCKET_POSITION = -1L;

    /** Reads the pointers of an Index Table prefix at a cut of the main table. */
    @FunctionalInterface
    interface CutReader {
        CompletableFuture<LookupResult> read(InternalRow indexKey, Map<TableBucket, Long> cut);
    }

    /** What a read at a cut needs of the main table. */
    static final class MainTableCuts {
        private final CutReader cutReader;
        private final Set<TableBucket> buckets;
        private final Function<InternalRow, TableBucket> bucketOfPrimaryKey;

        /**
         * @param buckets every bucket of the main table, which a cut names
         * @param bucketOfPrimaryKey the bucket of a primary key of the main table
         */
        MainTableCuts(
                CutReader cutReader,
                Set<TableBucket> buckets,
                Function<InternalRow, TableBucket> bucketOfPrimaryKey) {
            this.cutReader = checkNotNull(cutReader, "cutReader");
            this.buckets = Collections.unmodifiableSet(new HashSet<>(buckets));
            this.bucketOfPrimaryKey = checkNotNull(bucketOfPrimaryKey, "bucketOfPrimaryKey");
        }
    }

    private final Lookuper indexTablePrefixLookuper;
    private final Lookuper mainTablePointLookuper;
    private final Function<InternalRow, InternalRow> indexLookupKeyEncoder;
    private final InternalRow.FieldGetter[] idxColumnGettersInLookupKey;
    private final InternalRow.FieldGetter[] idxColumnGettersInMainRow;
    private final DataType[] idxColumnTypes;
    private final Function<InternalRow, GenericRow> basePkExtractorFromIndexRow;
    private final Executor continuationExecutor;

    /** The Index Table, which pages scan; null if the lookuper does not read in pages. */
    private final @Nullable Table indexTable;

    /** The most candidates a lookup of the Index Table may read, which bounds a page. */
    private final int maxCandidates;

    /** Reads at cuts of the main table; null if the lookuper does not read at cuts. */
    private final @Nullable MainTableCuts cuts;

    /**
     * Assembles a lookuper for one secondary index of {@code mainSchema}.
     *
     * <p>The wiring establishes the invariants this class depends on: the two getter arrays are
     * parallel to {@code idxCols}, and the base-PK extractor yields a row shaped like the main
     * table's primary key so {@code mainTablePointLookuper} can consume it directly.
     */
    public static SecondaryIndexLookuper create(
            Schema mainSchema,
            List<String> idxCols,
            Table indexTable,
            Lookuper mainTablePointLookuper,
            Executor continuationExecutor,
            int maxCandidates) {
        return create(
                mainSchema,
                null,
                idxCols,
                indexTable,
                mainTablePointLookuper,
                continuationExecutor,
                maxCandidates);
    }

    /**
     * Assembles a lookuper for one secondary index of a main table, which also reads at cuts of the
     * main table if it has no partitions, see {@link #lookup(InternalRow, Map)}.
     */
    public static SecondaryIndexLookuper create(
            TableInfo mainTableInfo,
            List<String> idxCols,
            Table indexTable,
            Lookuper mainTablePointLookuper,
            Executor continuationExecutor,
            int maxCandidates) {
        return create(
                mainTableInfo.getSchema(),
                mainTableInfo,
                idxCols,
                indexTable,
                mainTablePointLookuper,
                continuationExecutor,
                maxCandidates);
    }

    private static SecondaryIndexLookuper create(
            Schema mainSchema,
            @Nullable TableInfo mainTableInfo,
            List<String> idxCols,
            Table indexTable,
            Lookuper mainTablePointLookuper,
            Executor continuationExecutor,
            int maxCandidates) {
        Lookuper indexTablePrefixLookuper =
                indexTable.newLookup().lookupBy(INDEX_LOOKUP_COLUMNS).createLookuper();
        CompactedKeyEncoder logicalIndexKeyEncoder =
                new CompactedKeyEncoder(mainSchema.getRowType().project(idxCols));
        Function<InternalRow, InternalRow> indexLookupKeyEncoder =
                lookupKey ->
                        GenericRow.of(
                                IndexTableUtils.DATA_RECORD_KIND,
                                logicalIndexKeyEncoder.encodeKey(lookupKey));

        RowType mainRowType = mainSchema.getRowType();
        int idxColCount = idxCols.size();
        InternalRow.FieldGetter[] idxColumnGettersInLookupKey =
                new InternalRow.FieldGetter[idxColCount];
        InternalRow.FieldGetter[] idxColumnGettersInMainRow =
                new InternalRow.FieldGetter[idxColCount];
        DataType[] idxColumnTypes = new DataType[idxColCount];
        for (int i = 0; i < idxColCount; i++) {
            int posInMain = mainRowType.getFieldIndex(idxCols.get(i));
            DataType idxColumnType = mainRowType.getTypeAt(posInMain);
            idxColumnTypes[i] = idxColumnType;
            // The lookup-key row carries idx columns in declaration order at positions 0..n-1.
            idxColumnGettersInLookupKey[i] = InternalRow.createFieldGetter(idxColumnType, i);
            // The main row carries each idx column at its position in the main schema.
            idxColumnGettersInMainRow[i] = InternalRow.createFieldGetter(idxColumnType, posInMain);
        }

        // Index Table column ordering can dedup overlap between idx columns and PK columns, so each
        // base-PK position is resolved by name rather than assumed to be a trailing suffix.
        RowType indexRowType = indexTable.getTableInfo().getSchema().getRowType();
        List<String> basePk = mainSchema.getPrimaryKeyColumnNames();
        int pkSize = basePk.size();
        InternalRow.FieldGetter[] basePkGettersInIndexRow = new InternalRow.FieldGetter[pkSize];
        for (int i = 0; i < pkSize; i++) {
            int posInIndex = indexRowType.getFieldIndex(basePk.get(i));
            basePkGettersInIndexRow[i] =
                    InternalRow.createFieldGetter(indexRowType.getTypeAt(posInIndex), posInIndex);
        }
        Function<InternalRow, GenericRow> basePkExtractor =
                indexRow -> {
                    GenericRow pk = new GenericRow(pkSize);
                    for (int i = 0; i < pkSize; i++) {
                        pk.setField(i, basePkGettersInIndexRow[i].getFieldOrNull(indexRow));
                    }
                    return pk;
                };

        MainTableCuts cuts = null;
        if (mainTableInfo != null
                && !mainTableInfo.isPartitioned()
                && indexTablePrefixLookuper instanceof PrefixKeyLookuper) {
            cuts =
                    new MainTableCuts(
                            ((PrefixKeyLookuper) indexTablePrefixLookuper)::lookupAtCut,
                            bucketsOf(mainTableInfo),
                            bucketOfPrimaryKey(mainTableInfo));
        }
        return new SecondaryIndexLookuper(
                indexTablePrefixLookuper,
                mainTablePointLookuper,
                indexLookupKeyEncoder,
                idxColumnGettersInLookupKey,
                idxColumnGettersInMainRow,
                idxColumnTypes,
                basePkExtractor,
                continuationExecutor,
                indexTable,
                maxCandidates,
                cuts);
    }

    private static Set<TableBucket> bucketsOf(TableInfo mainTableInfo) {
        Set<TableBucket> buckets = new HashSet<>();
        for (int bucket = 0; bucket < mainTableInfo.getNumBuckets(); bucket++) {
            buckets.add(new TableBucket(mainTableInfo.getTableId(), bucket));
        }
        return buckets;
    }

    /** Routes a primary key of a table without partitions to its bucket, as writes do. */
    private static Function<InternalRow, TableBucket> bucketOfPrimaryKey(TableInfo mainTableInfo) {
        RowType primaryKeyType = mainTableInfo.getRowType().project(mainTableInfo.getPrimaryKeys());
        KeyEncoder primaryKeyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        primaryKeyType,
                        mainTableInfo.getPhysicalPrimaryKeys(),
                        mainTableInfo.getTableConfig(),
                        mainTableInfo.isDefaultBucketKey());
        KeyEncoder bucketKeyEncoder =
                KeyEncoder.ofBucketKeyEncoder(
                        primaryKeyType,
                        mainTableInfo.getBucketKeys(),
                        mainTableInfo.getTableConfig(),
                        mainTableInfo.isDefaultBucketKey(),
                        primaryKeyEncoder);
        BucketingFunction bucketing =
                BucketingFunction.of(
                        mainTableInfo.getTableConfig().getDataLakeFormat().orElse(null));
        long tableId = mainTableInfo.getTableId();
        int numBuckets = mainTableInfo.getNumBuckets();
        return primaryKey ->
                new TableBucket(
                        tableId,
                        bucketing.bucketing(bucketKeyEncoder.encodeKey(primaryKey), numBuckets));
    }

    SecondaryIndexLookuper(
            Lookuper indexTablePrefixLookuper,
            Lookuper mainTablePointLookuper,
            Function<InternalRow, InternalRow> indexLookupKeyEncoder,
            InternalRow.FieldGetter[] idxColumnGettersInLookupKey,
            InternalRow.FieldGetter[] idxColumnGettersInMainRow,
            DataType[] idxColumnTypes,
            Function<InternalRow, GenericRow> basePkExtractorFromIndexRow,
            Executor continuationExecutor) {
        this(
                indexTablePrefixLookuper,
                mainTablePointLookuper,
                indexLookupKeyEncoder,
                idxColumnGettersInLookupKey,
                idxColumnGettersInMainRow,
                idxColumnTypes,
                basePkExtractorFromIndexRow,
                continuationExecutor,
                null,
                0,
                null);
    }

    SecondaryIndexLookuper(
            Lookuper indexTablePrefixLookuper,
            Lookuper mainTablePointLookuper,
            Function<InternalRow, InternalRow> indexLookupKeyEncoder,
            InternalRow.FieldGetter[] idxColumnGettersInLookupKey,
            InternalRow.FieldGetter[] idxColumnGettersInMainRow,
            DataType[] idxColumnTypes,
            Function<InternalRow, GenericRow> basePkExtractorFromIndexRow,
            Executor continuationExecutor,
            @Nullable Table indexTable,
            int maxCandidates,
            @Nullable MainTableCuts cuts) {
        this.indexTable = indexTable;
        this.maxCandidates = maxCandidates;
        this.cuts = cuts;
        this.indexTablePrefixLookuper =
                checkNotNull(indexTablePrefixLookuper, "indexTablePrefixLookuper");
        this.mainTablePointLookuper =
                checkNotNull(mainTablePointLookuper, "mainTablePointLookuper");
        this.indexLookupKeyEncoder = checkNotNull(indexLookupKeyEncoder, "indexLookupKeyEncoder");
        this.idxColumnGettersInLookupKey =
                checkNotNull(idxColumnGettersInLookupKey, "idxColumnGettersInLookupKey").clone();
        this.idxColumnGettersInMainRow =
                checkNotNull(idxColumnGettersInMainRow, "idxColumnGettersInMainRow").clone();
        this.idxColumnTypes = checkNotNull(idxColumnTypes, "idxColumnTypes").clone();
        checkArgument(
                this.idxColumnGettersInLookupKey.length == this.idxColumnGettersInMainRow.length
                        && this.idxColumnGettersInLookupKey.length == this.idxColumnTypes.length,
                "Index column getters and types must have the same length");
        this.basePkExtractorFromIndexRow =
                checkNotNull(basePkExtractorFromIndexRow, "basePkExtractorFromIndexRow");
        this.continuationExecutor = checkNotNull(continuationExecutor, "continuationExecutor");
    }

    @Override
    public CompletableFuture<LookupResult> lookup(InternalRow lookupKey) {
        for (int i = 0; i < idxColumnGettersInLookupKey.length; i++) {
            if (lookupKey.isNullAt(i)) {
                return CompletableFuture.completedFuture(new LookupResult(Collections.emptyList()));
            }
        }
        Object[] expectedIdxValues = snapshotLookupKey(lookupKey);
        return indexTablePrefixLookuper
                .lookup(indexLookupKeyEncoder.apply(lookupKey))
                .thenComposeAsync(
                        hop1Result -> doHop2(hop1Result, expectedIdxValues), continuationExecutor);
    }

    /**
     * Reads the main rows whose index columns equal the key at a cut of a main table with bucket
     * history.
     *
     * <p>The future fails without retrying with {@link LookupPositionNotYetReadableException} while
     * the Index Table or the main table has not applied the cut yet, and with {@link
     * LookupPositionExpiredException} once the history of a position is gone.
     *
     * @param cut the exclusive next offset of every bucket of the main table, or {@link
     *     #EMPTY_BUCKET_POSITION} for a bucket read as empty
     */
    public CompletableFuture<LookupResult> lookup(
            InternalRow lookupKey, Map<TableBucket, Long> cut) {
        if (cuts == null) {
            return FutureUtils.completedExceptionally(
                    new UnsupportedOperationException(
                            "This secondary index lookuper does not read at cuts; only the"
                                    + " indexes of tables without partitions do."));
        }
        if (!cut.keySet().equals(cuts.buckets)) {
            return FutureUtils.completedExceptionally(
                    new IllegalArgumentException(
                            "A cut must name every bucket of the main table "
                                    + cuts.buckets
                                    + ", but names "
                                    + cut.keySet()
                                    + '.'));
        }
        Map<TableBucket, Long> readCut = new HashMap<>();
        for (Map.Entry<TableBucket, Long> position : cut.entrySet()) {
            long nextOffset = checkNotNull(position.getValue(), "position");
            if (nextOffset < EMPTY_BUCKET_POSITION) {
                return FutureUtils.completedExceptionally(
                        new IllegalArgumentException(
                                "Invalid position " + nextOffset + " of " + position.getKey()));
            }
            if (nextOffset != EMPTY_BUCKET_POSITION) {
                readCut.put(position.getKey(), nextOffset);
            }
        }
        if (hasNull(lookupKey) || readCut.isEmpty()) {
            return CompletableFuture.completedFuture(new LookupResult(Collections.emptyList()));
        }
        Object[] expectedIdxValues = snapshotLookupKey(lookupKey);
        return cuts.cutReader
                .read(indexLookupKeyEncoder.apply(lookupKey), readCut)
                .thenComposeAsync(
                        hop1Result ->
                                mainRowsAt(hop1Result.getRowList(), expectedIdxValues, readCut)
                                        .thenApply(LookupResult::new),
                        continuationExecutor);
    }

    /**
     * Hop 2 and the recheck at a cut, which drop the pointers added after the cut and those removed
     * before it.
     */
    private CompletableFuture<List<InternalRow>> mainRowsAt(
            List<InternalRow> candidateIndexRows,
            Object[] expectedIdxValues,
            Map<TableBucket, Long> readCut) {
        List<CompletableFuture<LookupResult>> mainFutures = new ArrayList<>();
        Set<GenericRow> seenBasePrimaryKeys = new HashSet<>();
        for (InternalRow indexRow : candidateIndexRows) {
            GenericRow basePk = copyRow(basePkExtractorFromIndexRow.apply(indexRow));
            if (!seenBasePrimaryKeys.add(basePk)) {
                continue;
            }
            TableBucket bucket = cuts.bucketOfPrimaryKey.apply(basePk);
            Long position = readCut.get(bucket);
            if (position != null) {
                mainFutures.add(
                        mainTablePointLookuper.lookup(
                                basePk, BucketReadPosition.of(bucket, position)));
            }
        }
        return CompletableFuture.allOf(mainFutures.toArray(new CompletableFuture[0]))
                .thenApply(
                        v -> {
                            List<InternalRow> aggregated = new ArrayList<>();
                            for (CompletableFuture<LookupResult> f : mainFutures) {
                                for (InternalRow mainRow : f.join().getRowList()) {
                                    if (idxColsMatch(expectedIdxValues, mainRow)) {
                                        aggregated.add(mainRow);
                                    }
                                }
                            }
                            return aggregated;
                        });
    }

    private CompletableFuture<LookupResult> doHop2(
            LookupResult hop1Result, Object[] expectedIdxValues) {
        return mainRowsOf(hop1Result.getRowList(), expectedIdxValues, new HashSet<>())
                .thenApply(LookupResult::new);
    }

    /** Hop 2 and the recheck, for candidates not in {@code seenBasePrimaryKeys} yet. */
    private CompletableFuture<List<InternalRow>> mainRowsOf(
            List<InternalRow> candidateIndexRows,
            Object[] expectedIdxValues,
            Set<GenericRow> seenBasePrimaryKeys) {
        if (candidateIndexRows.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.<InternalRow>emptyList());
        }
        List<CompletableFuture<LookupResult>> mainFutures =
                new ArrayList<>(candidateIndexRows.size());
        for (InternalRow indexRow : candidateIndexRows) {
            GenericRow basePk = copyRow(basePkExtractorFromIndexRow.apply(indexRow));
            if (seenBasePrimaryKeys.add(basePk)) {
                mainFutures.add(mainTablePointLookuper.lookup(basePk));
            }
        }
        return CompletableFuture.allOf(mainFutures.toArray(new CompletableFuture[0]))
                .thenApply(
                        v -> {
                            List<InternalRow> aggregated = new ArrayList<>();
                            for (CompletableFuture<LookupResult> f : mainFutures) {
                                LookupResult r = f.join();
                                for (InternalRow mainRow : r.getRowList()) {
                                    if (idxColsMatch(expectedIdxValues, mainRow)) {
                                        aggregated.add(mainRow);
                                    }
                                }
                            }
                            return aggregated;
                        });
    }

    /**
     * Reads the rows matching a key in pages of at most {@code pageSize} index candidates, so any
     * number of matches fits the candidate limit. Each page resolves and rechecks its candidates
     * like {@link #lookup}; a main row comes at most once.
     *
     * @param pageSize the most candidates of a page, at most {@link
     *     ConfigOptions#CLIENT_LOOKUP_SECONDARY_INDEX_MAX_CANDIDATES}
     * @param scanExecutor runs the blocking reads of the scan of the Index Table
     */
    public PrefixPages openPages(InternalRow lookupKey, int pageSize, Executor scanExecutor) {
        checkArgument(indexTable != null, "This lookuper does not read in pages.");
        checkArgument(
                pageSize > 0 && pageSize <= maxCandidates,
                "The page size of a secondary index lookup must be positive and at most %s (%s),"
                        + " but is %s.",
                maxCandidates,
                ConfigOptions.CLIENT_LOOKUP_SECONDARY_INDEX_MAX_CANDIDATES.key(),
                pageSize);
        // NULL never equals anything
        boolean nullKey = hasNull(lookupKey);
        // the pages keep the key, which they read later
        InternalRow ownKey;
        if (nullKey) {
            ownKey = GenericRow.of(IndexTableUtils.DATA_RECORD_KIND, new byte[0]);
        } else {
            InternalRow indexKey = indexLookupKeyEncoder.apply(lookupKey);
            ownKey = GenericRow.of(indexKey.getByte(0), indexKey.getBytes(1).clone());
        }
        Object[] expectedIdxValues = nullKey ? null : snapshotLookupKey(lookupKey);
        Set<GenericRow> seenBasePrimaryKeys = new HashSet<>();
        return new PrefixPages(
                indexTable,
                INDEX_LOOKUP_COLUMNS,
                ownKey,
                pageSize,
                limit ->
                        nullKey
                                ? CompletableFuture.completedFuture(
                                        new LookupResult(Collections.<InternalRow>emptyList()))
                                : indexTablePrefixLookuper.lookupFirst(ownKey, limit),
                scanExecutor,
                candidates ->
                        // not on the thread that completed the lookup, see the class comment
                        CompletableFuture.supplyAsync(() -> candidates, continuationExecutor)
                                .thenCompose(
                                        c ->
                                                mainRowsOf(
                                                        c,
                                                        expectedIdxValues,
                                                        seenBasePrimaryKeys)));
    }

    private boolean hasNull(InternalRow lookupKey) {
        for (int i = 0; i < idxColumnGettersInLookupKey.length; i++) {
            if (lookupKey.isNullAt(i)) {
                return true;
            }
        }
        return false;
    }

    /** A copy of a row whose values own their memory. */
    private static GenericRow copyRow(GenericRow row) {
        GenericRow copy = new GenericRow(row.getFieldCount());
        for (int i = 0; i < row.getFieldCount(); i++) {
            copy.setField(i, copyRecheckValue(row.getField(i)));
        }
        return copy;
    }

    private Object[] snapshotLookupKey(InternalRow lookupKey) {
        Object[] expectedIdxValues = new Object[idxColumnGettersInLookupKey.length];
        for (int i = 0; i < idxColumnGettersInLookupKey.length; i++) {
            expectedIdxValues[i] =
                    copyRecheckValue(idxColumnGettersInLookupKey[i].getFieldOrNull(lookupKey));
        }
        return expectedIdxValues;
    }

    private boolean idxColsMatch(Object[] expectedIdxValues, InternalRow mainRow) {
        for (int i = 0; i < expectedIdxValues.length; i++) {
            Object expected = expectedIdxValues[i];
            Object actual = idxColumnGettersInMainRow[i].getFieldOrNull(mainRow);
            if (expected == null
                    || actual == null
                    || !Equal.INSTANCE.test(idxColumnTypes[i], actual, expected)) {
                return false;
            }
        }
        return true;
    }

    private static Object copyRecheckValue(Object value) {
        if (value instanceof byte[]) {
            return ((byte[]) value).clone();
        }
        if (value instanceof BinaryString) {
            return ((BinaryString) value).copy();
        }
        return value;
    }
}
