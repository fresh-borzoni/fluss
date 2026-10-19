// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Hash-overlay merge of a primary-key lake baseline and its changelog tail.
//!
//! The bounded tail is small relative to the lake snapshot, so the small side
//! is hashed and the big side is streamed: the tail folds into an overlay of
//! `PkKey -> Option<row>` (`None` = tombstone), the lake current state streams
//! through a probe that drops superseded keys, and the overlay's survivors are
//! emitted once the lake side drains.
//!
//! Sort-merge would emit progressively and yield key-ordered output, but it
//! requires the lake side delivered in key order under a public contract with
//! an exposed comparator, which paimon-rust does not offer today. Hash overlay
//! needs neither, and for deletion-vector tables — the tables the readable
//! snapshot mechanism targets — per-file reads are already merge-free, so an
//! imposed sort would pay for ordering the layout no longer needs.

use crate::{FlussLakeError, FlussLakeRecordBatchStream, FlussLakeResult};
use arrow::array::{BooleanArray, RecordBatch};
use arrow::compute::{cast, filter_record_batch};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::row::{RowConverter, Rows, SortField};
use fluss::record::ChangeType;
use hashbrown::HashMap;
use hashbrown::hash_map::EntryRef;
use std::collections::VecDeque;

/// Per-overlay-entry bookkeeping charged against the memory limit.
///
/// Covers the hash-map slot plus the owned key allocation header. It only has
/// to be a stable order-of-magnitude estimate: the limit exists to fail an
/// unbounded tail explicitly rather than to account for bytes exactly.
const OVERLAY_ENTRY_OVERHEAD_BYTES: usize = 64;

/// Where one surviving tail row lives inside the retained tail batches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TailRowRef {
    batch_index: usize,
    row_index: usize,
}

/// The folded changelog tail of one primary-key bucket.
///
/// `None` marks a tombstone (DELETE / UPDATE_BEFORE): the key must disappear
/// from the result, so it suppresses the lake row without contributing one.
pub(crate) struct PkOverlay {
    entries: HashMap<Box<[u8]>, Option<TailRowRef>>,
    tail_batches: Vec<RecordBatch>,
    /// Per retained tail batch, whether each row is still its key's current
    /// value. Kept up to date while folding, so emitting the survivors never
    /// has to walk the map.
    survives: Vec<Vec<bool>>,
    key_converter: RowConverter,
    key_positions: Vec<usize>,
    schema: SchemaRef,
    memory_limit_bytes: Option<usize>,
    charged_bytes: usize,
}

impl PkOverlay {
    /// Creates an empty overlay over the physical read schema.
    ///
    /// `key_positions` are the primary-key columns' positions **within the
    /// physical projection**, which the executor widened to include every key
    /// column even when the engine did not request them.
    pub(crate) fn try_new(
        schema: SchemaRef,
        key_positions: Vec<usize>,
        memory_limit_bytes: Option<usize>,
    ) -> FlussLakeResult<Self> {
        if key_positions.is_empty() {
            return Err(FlussLakeError::InvalidSplit(
                "primary-key merge requires at least one key column".to_string(),
            ));
        }
        let mut sort_fields = Vec::with_capacity(key_positions.len());
        for position in &key_positions {
            let field = schema.fields().get(*position).ok_or_else(|| {
                FlussLakeError::InvalidSplit(format!(
                    "primary-key column position {position} exceeds the physical read width {}",
                    schema.fields().len()
                ))
            })?;
            sort_fields.push(SortField::new(field.data_type().clone()));
        }
        let key_converter = RowConverter::new(sort_fields).map_err(|error| {
            FlussLakeError::Execution(format!(
                "failed to create the primary-key row encoder: {error}"
            ))
        })?;

        Ok(Self {
            entries: HashMap::new(),
            tail_batches: Vec::new(),
            survives: Vec::new(),
            key_converter,
            key_positions,
            schema,
            memory_limit_bytes,
            charged_bytes: 0,
        })
    }

    /// Encodes the primary keys of one batch into comparable row bytes.
    ///
    /// Both merge inputs go through this one converter, so the encoded bytes
    /// are comparable across the lake and log sides.
    fn encode_keys(&self, batch: &RecordBatch) -> FlussLakeResult<Rows> {
        let key_columns: Vec<_> = self
            .key_positions
            .iter()
            .map(|position| batch.column(*position).clone())
            .collect();
        self.key_converter
            .convert_columns(&key_columns)
            .map_err(|error| {
                FlussLakeError::Execution(format!("failed to encode primary keys: {error}"))
            })
    }

    /// Restates a merge input under the plan's frozen physical schema.
    ///
    /// The lake and log sides describe the same columns but arrive from
    /// different decoders, so field metadata, nullability and the names of
    /// nested fields may differ (Parquet calls a list element `element`, Arrow
    /// builders call it `item`). Rows are rebuilt under the frozen schema —
    /// which keeps the emitted batches matching the plan's output schema —
    /// while any real disagreement about column names or types fails
    /// explicitly instead of being reinterpreted.
    fn normalize(&self, batch: &RecordBatch) -> FlussLakeResult<RecordBatch> {
        if batch.schema_ref() == &self.schema {
            return Ok(batch.clone());
        }
        if batch.num_columns() != self.schema.fields().len() {
            return Err(FlussLakeError::Execution(format!(
                "primary-key merge input has {} columns but the frozen physical read has {}",
                batch.num_columns(),
                self.schema.fields().len()
            )));
        }
        let mut columns = Vec::with_capacity(batch.num_columns());
        for (position, expected) in self.schema.fields().iter().enumerate() {
            let actual = batch.schema_ref().field(position);
            if actual.name() != expected.name()
                || !same_shape(actual.data_type(), expected.data_type())
            {
                return Err(FlussLakeError::Execution(format!(
                    "primary-key merge input column {position} is {}:{} but the frozen physical read expects {}:{}",
                    actual.name(),
                    actual.data_type(),
                    expected.name(),
                    expected.data_type()
                )));
            }
            let column = batch.column(position);
            if actual.data_type() == expected.data_type() {
                columns.push(column.clone());
            } else {
                // Only nested names differ: relabel without touching the values.
                columns.push(cast(column, expected.data_type()).map_err(|error| {
                    FlussLakeError::Execution(format!(
                        "failed to restate primary-key merge input column {position} as {}: {error}",
                        expected.data_type()
                    ))
                })?);
            }
        }
        RecordBatch::try_new(self.schema.clone(), columns).map_err(|error| {
            FlussLakeError::Execution(format!(
                "failed to restate a primary-key merge input under the frozen read schema: {error}"
            ))
        })
    }

    /// Folds one changelog batch into the overlay in offset order.
    ///
    /// `change_types` must be aligned with the batch rows. Later offsets
    /// overwrite earlier ones, so last writer wins per key.
    pub(crate) fn fold_tail_batch(
        &mut self,
        batch: RecordBatch,
        change_types: &[ChangeType],
    ) -> FlussLakeResult<()> {
        if change_types.len() != batch.num_rows() {
            return Err(FlussLakeError::Execution(format!(
                "changelog batch has {} rows but {} change types",
                batch.num_rows(),
                change_types.len()
            )));
        }
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let batch = self.normalize(&batch)?;
        let keys = self.encode_keys(&batch)?;
        let batch_index = self.tail_batches.len();
        // The batch is retained only because surviving rows point into it;
        // its memory counts against the overlay limit.
        self.charged_bytes += batch.get_array_memory_size() + batch.num_rows();
        self.survives.push(vec![false; batch.num_rows()]);
        self.tail_batches.push(batch);
        self.check_memory_limit()?;

        for (row_index, change_type) in change_types.iter().enumerate() {
            let value = match change_type {
                ChangeType::Insert | ChangeType::UpdateAfter => Some(TailRowRef {
                    batch_index,
                    row_index,
                }),
                ChangeType::Delete | ChangeType::UpdateBefore => None,
                ChangeType::AppendOnly => {
                    return Err(FlussLakeError::Execution(format!(
                        "primary-key merge received an append-only record at row {row_index}; a changelog is required to reconstruct the current view"
                    )));
                }
            };
            let key = keys.row(row_index);
            // A record whose key the very next record names again is overwritten
            // at once, so it is skipped: in a changelog that is every
            // update-before, which halves the map operations of an update.
            if row_index + 1 < change_types.len() && keys.row(row_index + 1) == key {
                continue;
            }
            if value.is_some() {
                self.survives[batch_index][row_index] = true;
            }
            // Hashed once; the key is copied only the first time it is seen.
            match self.entries.entry_ref(key.as_ref()) {
                EntryRef::Occupied(mut entry) => {
                    if let Some(previous) = entry.insert(value) {
                        self.survives[previous.batch_index][previous.row_index] = false;
                    }
                }
                EntryRef::Vacant(entry) => {
                    self.charged_bytes += key.as_ref().len() + OVERLAY_ENTRY_OVERHEAD_BYTES;
                    entry.insert(value);
                    self.check_memory_limit()?;
                }
            }
        }
        Ok(())
    }

    /// Drops the lake rows whose keys the tail supersedes or deletes.
    ///
    /// Returns `None` when no lake row survives, so callers can skip emitting
    /// an empty batch.
    pub(crate) fn probe_lake_batch(
        &self,
        batch: &RecordBatch,
    ) -> FlussLakeResult<Option<RecordBatch>> {
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        let batch = self.normalize(batch)?;
        let keys = self.encode_keys(&batch)?;
        // Probing borrows the encoded key bytes: the streamed lake side is the
        // big input, so it must not allocate one owned key per row.
        let keep: BooleanArray = (0..batch.num_rows())
            .map(|row_index| Some(!self.entries.contains_key(keys.row(row_index).as_ref())))
            .collect();
        if keep.true_count() == 0 {
            return Ok(None);
        }
        let filtered = filter_record_batch(&batch, &keep).map_err(|error| {
            FlussLakeError::Execution(format!("failed to filter superseded lake rows: {error}"))
        })?;
        Ok(Some(filtered))
    }

    /// Emits the tail rows that survive as the current value of their key.
    ///
    /// Called once the lake side has drained; tombstones contribute nothing.
    /// Each retained tail batch is filtered down to its survivors, so rows come
    /// out in offset order — emission order is unspecified by contract, but a
    /// deterministic one keeps results reproducible — without sorting or
    /// gathering them into one batch.
    pub(crate) fn into_surviving_batches(self) -> FlussLakeResult<VecDeque<RecordBatch>> {
        let mut batches = VecDeque::new();
        for (batch, survives) in self.tail_batches.iter().zip(self.survives) {
            let survives = BooleanArray::from(survives);
            if survives.true_count() == 0 {
                continue;
            }
            batches.push_back(filter_record_batch(batch, &survives).map_err(|error| {
                FlussLakeError::Execution(format!(
                    "failed to emit surviving changelog rows: {error}"
                ))
            })?);
        }
        Ok(batches)
    }

    /// Fails the read when the folded tail outgrows its memory budget.
    ///
    /// v1 does not spill: availability degrades explicitly, never
    /// correctness. A tail this large means tiering has fallen far behind.
    fn check_memory_limit(&self) -> FlussLakeResult<()> {
        if let Some(limit) = self.memory_limit_bytes
            && self.charged_bytes > limit
        {
            return Err(FlussLakeError::Execution(format!(
                "primary-key merge overlay needs at least {} bytes for {} distinct keys, exceeding the {limit} byte limit; the changelog tail is too large to merge in memory (v1 does not spill), so re-run with a higher limit or after lake tiering catches up",
                self.charged_bytes,
                self.entries.len()
            )));
        }
        Ok(())
    }
}

/// Produces the merged current view of one primary-key bucket.
///
/// The overlay is already folded, so this streams the lake side through the
/// probe and appends the overlay's survivors as final batches. Nothing is
/// emitted before the tail is fully folded: time to first batch is bounded
/// below by the tail read, an accepted v1 tradeoff of hash overlay.
pub(crate) fn merged_stream(
    overlay: PkOverlay,
    lake_stream: FlussLakeRecordBatchStream,
) -> FlussLakeRecordBatchStream {
    enum Phase {
        Lake {
            overlay: PkOverlay,
            lake_stream: FlussLakeRecordBatchStream,
        },
        Survivors(VecDeque<RecordBatch>),
    }

    Box::pin(futures::stream::try_unfold(
        Phase::Lake {
            overlay,
            lake_stream,
        },
        move |phase| async move {
            let mut phase = phase;
            loop {
                match phase {
                    Phase::Lake {
                        overlay,
                        mut lake_stream,
                    } => {
                        use futures::StreamExt;
                        match lake_stream.next().await {
                            Some(Ok(batch)) => match overlay.probe_lake_batch(&batch)? {
                                Some(kept) => {
                                    return Ok(Some((
                                        kept,
                                        Phase::Lake {
                                            overlay,
                                            lake_stream,
                                        },
                                    )));
                                }
                                None => {
                                    phase = Phase::Lake {
                                        overlay,
                                        lake_stream,
                                    };
                                }
                            },
                            Some(Err(error)) => return Err(error),
                            None => phase = Phase::Survivors(overlay.into_surviving_batches()?),
                        }
                    }
                    Phase::Survivors(mut survivors) => {
                        return Ok(survivors
                            .pop_front()
                            .map(|batch| (batch, Phase::Survivors(survivors))));
                    }
                }
            }
        },
    ))
}

/// Whether two types hold the same values, ignoring the names and nullability
/// of nested fields, which decoders and builders choose differently.
fn same_shape(actual: &DataType, expected: &DataType) -> bool {
    match (actual, expected) {
        (DataType::List(a), DataType::List(b))
        | (DataType::LargeList(a), DataType::LargeList(b)) => {
            same_shape(a.data_type(), b.data_type())
        }
        (DataType::FixedSizeList(a, n), DataType::FixedSizeList(b, m)) => {
            n == m && same_shape(a.data_type(), b.data_type())
        }
        // Map entry structs are named by their builder (`key`/`value` or
        // `keys`/`values`); compare them by position.
        (DataType::Map(a, sorted_a), DataType::Map(b, sorted_b)) => {
            sorted_a == sorted_b
                && match (a.data_type(), b.data_type()) {
                    (DataType::Struct(x), DataType::Struct(y)) => {
                        x.len() == y.len()
                            && x.iter()
                                .zip(y.iter())
                                .all(|(f, g)| same_shape(f.data_type(), g.data_type()))
                    }
                    (x, y) => same_shape(x, y),
                }
        }
        (DataType::Struct(a), DataType::Struct(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .all(|(f, g)| f.name() == g.name() && same_shape(f.data_type(), g.data_type()))
        }
        _ => actual == expected,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::{StreamExt, TryStreamExt};
    use std::sync::Arc;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Int64, true),
        ]))
    }

    fn batch(ids: Vec<i32>, names: Vec<&str>, amounts: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(names)) as ArrayRef,
                Arc::new(Int64Array::from(amounts)) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn overlay() -> PkOverlay {
        PkOverlay::try_new(schema(), vec![0], None).unwrap()
    }

    #[test]
    fn list_inputs_differing_only_in_element_name_are_restated() {
        use arrow::array::{Array, ListArray};
        use arrow::datatypes::Int32Type;

        let list = |name: &str| DataType::List(Arc::new(Field::new(name, DataType::Int32, true)));
        let frozen = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("recent", list("item"), true),
        ]));
        let overlay = PkOverlay::try_new(frozen.clone(), vec![0], None).unwrap();
        let values = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            None,
        ]);
        let parquet_named = ListArray::new(
            Arc::new(Field::new("element", DataType::Int32, true)),
            values.offsets().clone(),
            values.values().clone(),
            values.nulls().cloned(),
        );
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("recent", list("element"), true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(parquet_named),
            ],
        )
        .unwrap();

        let restated = overlay.normalize(&input).unwrap();
        assert_eq!(restated.schema(), frozen);
        assert_eq!(restated.column(1).as_ref(), &values as &dyn Array);

        assert!(!same_shape(
            &list("element"),
            &DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))
        ));
    }

    fn rows_of(batch: &RecordBatch) -> Vec<(i32, String, i64)> {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let amounts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        (0..batch.num_rows())
            .map(|row| {
                (
                    ids.value(row),
                    names.value(row).to_string(),
                    amounts.value(row),
                )
            })
            .collect()
    }

    async fn collect_merged(
        overlay: PkOverlay,
        lake_batches: Vec<RecordBatch>,
        _output_column_count: usize,
    ) -> Vec<(i32, String, i64)> {
        let lake: FlussLakeRecordBatchStream =
            Box::pin(futures::stream::iter(lake_batches.into_iter().map(Ok)));
        let merged: Vec<RecordBatch> = merged_stream(overlay, lake).try_collect().await.unwrap();
        let mut rows: Vec<(i32, String, i64)> = merged.iter().flat_map(rows_of).collect();
        rows.sort();
        rows
    }

    #[tokio::test]
    async fn tail_updates_supersede_lake_rows() {
        let mut overlay = overlay();
        overlay
            .fold_tail_batch(
                batch(vec![2], vec!["two-v2"], vec![22]),
                &[ChangeType::UpdateAfter],
            )
            .unwrap();

        let rows = collect_merged(
            overlay,
            vec![batch(
                vec![1, 2, 3],
                vec!["one", "two", "three"],
                vec![10, 20, 30],
            )],
            3,
        )
        .await;

        assert_eq!(
            rows,
            vec![
                (1, "one".to_string(), 10),
                (2, "two-v2".to_string(), 22),
                (3, "three".to_string(), 30),
            ]
        );
    }

    #[tokio::test]
    async fn tombstones_remove_keys_from_the_current_view() {
        let mut overlay = overlay();
        overlay
            .fold_tail_batch(batch(vec![2], vec!["two"], vec![20]), &[ChangeType::Delete])
            .unwrap();

        let rows = collect_merged(
            overlay,
            vec![batch(vec![1, 2], vec!["one", "two"], vec![10, 20])],
            3,
        )
        .await;

        assert_eq!(rows, vec![(1, "one".to_string(), 10)]);
    }

    /// An update-before must suppress the lake row even though it carries no
    /// surviving value; only the later update-after may reinstate the key.
    #[tokio::test]
    async fn update_before_then_after_keeps_the_later_value() {
        let mut overlay = overlay();
        overlay
            .fold_tail_batch(
                batch(vec![1], vec!["one"], vec![10]),
                &[ChangeType::UpdateBefore],
            )
            .unwrap();
        overlay
            .fold_tail_batch(
                batch(vec![1], vec!["one-v2"], vec![11]),
                &[ChangeType::UpdateAfter],
            )
            .unwrap();

        let rows = collect_merged(overlay, vec![batch(vec![1], vec!["one"], vec![10])], 3).await;

        assert_eq!(rows, vec![(1, "one-v2".to_string(), 11)]);
    }

    #[tokio::test]
    async fn last_writer_wins_within_one_batch() {
        let mut overlay = overlay();
        overlay
            .fold_tail_batch(
                batch(vec![1, 1], vec!["first", "second"], vec![1, 2]),
                &[ChangeType::Insert, ChangeType::UpdateAfter],
            )
            .unwrap();

        let rows = collect_merged(overlay, Vec::new(), 3).await;

        assert_eq!(rows, vec![(1, "second".to_string(), 2)]);
    }

    /// Survivors are tracked while folding: a row a later batch supersedes is
    /// never emitted, and of adjacent records naming one key only the last
    /// counts, including a delete that ends an insert.
    #[tokio::test]
    async fn superseded_rows_are_never_emitted() {
        let mut overlay = overlay();
        overlay
            .fold_tail_batch(
                batch(
                    vec![1, 1, 1, 2, 2, 3],
                    vec!["one", "one", "one-v2", "two", "two", "three"],
                    vec![1, 1, 2, 2, 2, 3],
                ),
                &[
                    ChangeType::Insert,
                    ChangeType::UpdateBefore,
                    ChangeType::UpdateAfter,
                    ChangeType::Insert,
                    ChangeType::Delete,
                    ChangeType::Insert,
                ],
            )
            .unwrap();
        overlay
            .fold_tail_batch(
                batch(vec![3, 1], vec!["three-v2", "one-v3"], vec![33, 3]),
                &[ChangeType::UpdateAfter, ChangeType::UpdateAfter],
            )
            .unwrap();

        let rows = collect_merged(
            overlay,
            vec![batch(vec![2, 4], vec!["two-lake", "four"], vec![20, 40])],
            3,
        )
        .await;

        assert_eq!(
            rows,
            vec![
                (1, "one-v3".to_string(), 3),
                (3, "three-v2".to_string(), 33),
                (4, "four".to_string(), 40),
            ]
        );
    }

    /// Inserts of keys absent from the lake must appear in the result even
    /// when the lake side yields nothing at all.
    #[tokio::test]
    async fn tail_inserts_appear_without_any_lake_side() {
        let mut overlay = overlay();
        overlay
            .fold_tail_batch(
                batch(vec![7, 8], vec!["seven", "eight"], vec![70, 80]),
                &[ChangeType::Insert, ChangeType::Insert],
            )
            .unwrap();

        let rows = collect_merged(overlay, Vec::new(), 3).await;

        assert_eq!(
            rows,
            vec![(7, "seven".to_string(), 70), (8, "eight".to_string(), 80)]
        );
    }

    /// The overlay keeps the widened physical schema for downstream filtering.
    #[tokio::test]
    async fn hidden_key_columns_remain_available_after_overlay() {
        // A request for `name` and `amount` on a table keyed by `id`: the
        // physical read appends `id` last so the merge can compare keys.
        let physical_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Int64, true),
            Field::new("id", DataType::Int32, false),
        ]));
        let physical_batch = |names: Vec<&str>, amounts: Vec<i64>, ids: Vec<i32>| {
            RecordBatch::try_new(
                physical_schema.clone(),
                vec![
                    Arc::new(StringArray::from(names)) as ArrayRef,
                    Arc::new(Int64Array::from(amounts)) as ArrayRef,
                    Arc::new(Int32Array::from(ids)) as ArrayRef,
                ],
            )
            .unwrap()
        };
        let mut overlay = PkOverlay::try_new(physical_schema.clone(), vec![2], None).unwrap();
        overlay
            .fold_tail_batch(
                physical_batch(vec!["two-v2"], vec![22], vec![2]),
                &[ChangeType::UpdateAfter],
            )
            .unwrap();
        let lake: FlussLakeRecordBatchStream = Box::pin(futures::stream::iter(vec![Ok(
            physical_batch(vec!["one", "two"], vec![10, 20], vec![1, 2]),
        )]));

        let mut merged = merged_stream(overlay, lake);

        let mut emitted = 0;
        while let Some(batch) = merged.next().await {
            let batch = batch.unwrap();
            assert_eq!(batch.num_columns(), 3);
            assert_eq!(batch.schema().field(0).name(), "name");
            assert_eq!(batch.schema().field(1).name(), "amount");
            assert_eq!(batch.schema().field(2).name(), "id");
            emitted += 1;
        }
        assert_eq!(emitted, 2, "one surviving lake batch and the overlay batch");
    }

    #[tokio::test]
    async fn append_only_records_are_rejected() {
        let mut overlay = overlay();

        let result = overlay.fold_tail_batch(
            batch(vec![1], vec!["one"], vec![10]),
            &[ChangeType::AppendOnly],
        );

        assert!(matches!(result, Err(FlussLakeError::Execution(_))));
    }

    /// The overlay is bounded by `memory_limit_bytes`; exceeding it must fail
    /// explicitly rather than grow without limit (v1 does not spill).
    #[test]
    fn overlay_fails_explicitly_at_the_memory_limit() {
        let mut overlay = PkOverlay::try_new(schema(), vec![0], Some(1)).unwrap();

        let result = overlay.fold_tail_batch(
            batch(vec![1, 2], vec!["one", "two"], vec![10, 20]),
            &[ChangeType::Insert, ChangeType::Insert],
        );

        match result {
            Err(FlussLakeError::Execution(message)) => {
                assert!(
                    message.contains("byte limit"),
                    "unexpected error: {message}"
                );
            }
            other => panic!("expected a memory-limit execution error, got: {other:?}"),
        }
    }

    #[test]
    fn rejects_key_positions_outside_the_physical_read() {
        assert!(matches!(
            PkOverlay::try_new(schema(), vec![9], None),
            Err(FlussLakeError::InvalidSplit(_))
        ));
        assert!(matches!(
            PkOverlay::try_new(schema(), Vec::new(), None),
            Err(FlussLakeError::InvalidSplit(_))
        ));
    }

    /// Lake and tail batches must describe the same columns: a real
    /// disagreement means the frozen projection was not applied identically
    /// on both sides, which no reinterpretation can repair.
    #[test]
    fn rejects_inputs_that_disagree_on_columns() {
        let overlay = overlay();
        let narrow = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
        )
        .unwrap();

        assert!(matches!(
            overlay.probe_lake_batch(&narrow),
            Err(FlussLakeError::Execution(_))
        ));
    }

    /// Nullability and field metadata legitimately differ between the Paimon
    /// and log decoders, so equal columns must merge rather than fail.
    #[test]
    fn accepts_inputs_that_differ_only_in_nullability() {
        let overlay = overlay();
        let relaxed_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Int64, true),
        ]));
        let relaxed = RecordBatch::try_new(
            relaxed_schema,
            vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(StringArray::from(vec!["one"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![10])) as ArrayRef,
            ],
        )
        .unwrap();

        let kept = overlay.probe_lake_batch(&relaxed).unwrap().unwrap();

        assert_eq!(kept.schema_ref(), &schema());
        assert_eq!(rows_of(&kept), vec![(1, "one".to_string(), 10)]);
    }
}
