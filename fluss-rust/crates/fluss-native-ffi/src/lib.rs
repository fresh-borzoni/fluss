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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// C ABI over fluss-rs and the fluss-lake kernel for engines that read Fluss splits natively.
//
// A reader is opened from one JSON split descriptor and yields Arrow batches through the
// Arrow C Data Interface, so the caller links no Rust and no particular Arrow C++ version.
//
//   int fluss_native_open(const char* descriptor_json, FlussNativeReader** out, char* err, size_t err_len);
//   int fluss_native_next(FlussNativeReader*, struct ArrowArray*, struct ArrowSchema*, char* err, size_t err_len);
//   void fluss_native_close(FlussNativeReader*);
//
// fluss_native_next returns 1 with a batch, 0 at the end of the split, -1 on error.
//
// Split kinds:
//   log     bounded [start, stop) of one bucket, optionally one share of it, with filter pushdown
//   pk      primary-key union read of one bucket: lake baseline folded with the changelog tail
//   lake    one share of the lake files of a pinned snapshot
//   lookup  primary-key point lookups
//   changelog  bounded [start, stop) of a primary-key table's changelog, every change type kept
//
// A descriptor with table_id 0 has the table id resolved from the table path.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ffi::{CStr, c_char, c_int};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use arrow::array::{Array, StructArray};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, to_ffi};
use arrow::record_batch::RecordBatch;
use fluss::client::{
    BoundedLogReadRange, EARLIEST_OFFSET, FlussConnection, LogScanner, Lookuper,
    RecordBatchLogReader,
};
use fluss::config::Config;
use fluss::metadata::{TableBucket, TablePath};
use fluss::predicate::{LeafFunction, Literal, Predicate};
use fluss::row::{Datum, GenericRow, TimestampLtz, TimestampNtz};
use fluss::rpc::message::OffsetSpec;
use fluss_lake::{
    FlussLakeExecutionContext, FlussLakeLiteral, FlussLakePinnedBoundary, FlussLakePinnedRange,
    FlussLakePredicate, FlussLakeReadMode, FlussLakeRecordBatchStream, FlussLakeTable,
};
use futures::StreamExt;
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
struct Descriptor {
    bootstrap_servers: String,
    database: String,
    table: String,
    columns: Vec<String>,
    split: SplitDescriptor,
    #[serde(default)]
    lake_properties: HashMap<String, String>,
    #[serde(default)]
    client_options: BTreeMap<String, String>,
    #[serde(default)]
    filters: Vec<Filter>,
    #[serde(default)]
    limit: Option<u64>,
    #[serde(default)]
    keys: Vec<Vec<Value>>,
    #[serde(default)]
    key_types: Vec<String>,
}

#[derive(Deserialize)]
struct SplitDescriptor {
    kind: String,
    #[serde(default)]
    table_id: i64,
    #[serde(default)]
    bucket: i32,
    partition_id: Option<i64>,
    partition_name: Option<String>,
    partition_qualified_name: Option<String>,
    #[serde(default)]
    start_offset: i64,
    #[serde(default)]
    stop_offset: i64,
    lake_snapshot_id: Option<i64>,
    // `kv`: the bucket's kv snapshot (negative or absent: never snapshotted) that the log range follows.
    kv_snapshot_id: Option<i64>,
    // A large range or a lake snapshot is read by `parts` scanners; this one reads share `part`.
    #[serde(default)]
    part: Option<u32>,
    #[serde(default)]
    parts: Option<u32>,
}

// One `column op literal...` conjunct. The engine keeps evaluating the full predicate,
// so a filter only has to be a superset: the server skips batches whose statistics cannot match.
#[derive(Deserialize)]
struct Filter {
    column: String,
    op: String,
    #[serde(rename = "type")]
    value_type: String,
    #[serde(default)]
    values: Vec<Value>,
}

enum Source {
    Log(RecordBatchLogReader),
    Lake(FlussLakeRecordBatchStream),
    Batches(VecDeque<RecordBatch>),
    Changelog(ChangelogReader),
}

// A primary-key table's changelog over [next_offset, stop_offset), every change type kept.
// The Arrow batch scanner refuses primary-key tables, so this polls the record scanner and
// hands back the rows of each backing batch it selected.
struct ChangelogReader {
    scanner: LogScanner,
    bucket: TableBucket,
    next_offset: i64,
    stop_offset: i64,
    pending: VecDeque<RecordBatch>,
}

const CHANGELOG_POLL_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);
const CHANGELOG_IDLE_LIMIT: std::time::Duration = std::time::Duration::from_secs(60);

impl ChangelogReader {
    async fn next(&mut self) -> Result<Option<RecordBatch>, String> {
        let mut idle_since = std::time::Instant::now();
        while self.pending.is_empty() && self.next_offset < self.stop_offset {
            let records = self
                .scanner
                .poll(CHANGELOG_POLL_TIMEOUT)
                .await
                .map_err(|e| format!("poll changelog: {e}"))?;
            let before = self.next_offset;
            self.take(records.records(&self.bucket))?;
            if self.next_offset == before {
                if idle_since.elapsed() > CHANGELOG_IDLE_LIMIT {
                    return Err(format!(
                        "changelog of bucket {} made no progress at offset {} (stop {})",
                        self.bucket.bucket_id(),
                        self.next_offset,
                        self.stop_offset
                    ));
                }
            } else {
                idle_since = std::time::Instant::now();
            }
        }
        Ok(self.pending.pop_front())
    }

    // Records of one poll arrive grouped by their backing batch: one output batch per run.
    fn take(&mut self, records: &[fluss::record::ScanRecord]) -> Result<(), String> {
        let mut run: Option<(*const RecordBatch, RecordBatch, Vec<u32>)> = None;
        for record in records {
            if record.offset() < self.next_offset || record.offset() >= self.stop_offset {
                continue;
            }
            let row = record.row();
            let batch = row
                .get_record_batch()
                .ok_or_else(|| "changelog record has no backing Arrow batch".to_string())?;
            let address = batch as *const RecordBatch;
            if run.as_ref().map(|(a, _, _)| *a) != Some(address) {
                if let Some((_, b, rows)) = run.take() {
                    self.pending.push_back(select_rows(&b, &rows)?);
                }
                run = Some((address, batch.clone(), Vec::new()));
            }
            run.as_mut().unwrap().2.push(row.get_row_id() as u32);
            self.next_offset = record.offset() + 1;
        }
        if let Some((_, b, rows)) = run.take() {
            self.pending.push_back(select_rows(&b, &rows)?);
        }
        Ok(())
    }
}

fn select_rows(batch: &RecordBatch, rows: &[u32]) -> Result<RecordBatch, String> {
    let contiguous = rows.windows(2).all(|w| w[1] == w[0] + 1);
    if contiguous && !rows.is_empty() {
        return Ok(batch.slice(rows[0] as usize, rows.len()));
    }
    let indices = arrow::array::UInt32Array::from(rows.to_vec());
    arrow::compute::take_record_batch(batch, &indices)
        .map_err(|e| format!("select changelog rows: {e}"))
}

pub struct FlussNativeReader {
    source: Source,
    remaining: Option<u64>,
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        // The engine already runs one scanner thread per split; a worker per core on top of
        // that only adds contention. FLUSS_NATIVE_WORKER_THREADS overrides the default.
        let workers = std::env::var("FLUSS_NATIVE_WORKER_THREADS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(4);
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .thread_name("fluss-native")
            .enable_all()
            .build()
            .expect("failed to start the fluss-native runtime")
    })
}

fn apply_client_options(
    config: &mut Config,
    options: &BTreeMap<String, String>,
) -> Result<(), String> {
    for (key, value) in options {
        let bad = |e: std::num::ParseIntError| format!("client option {key}={value}: {e}");
        match key.as_str() {
            "scanner_log_fetch_max_bytes" => {
                config.scanner_log_fetch_max_bytes = value.parse().map_err(bad)?
            }
            "scanner_log_fetch_max_bytes_for_bucket" => {
                config.scanner_log_fetch_max_bytes_for_bucket = value.parse().map_err(bad)?
            }
            "scanner_log_fetch_min_bytes" => {
                config.scanner_log_fetch_min_bytes = value.parse().map_err(bad)?
            }
            "scanner_log_fetch_wait_max_time_ms" => {
                config.scanner_log_fetch_wait_max_time_ms = value.parse().map_err(bad)?
            }
            "scanner_log_max_poll_records" => {
                config.scanner_log_max_poll_records = value.parse().map_err(bad)?
            }
            "scanner_remote_log_prefetch_num" => {
                config.scanner_remote_log_prefetch_num = value.parse().map_err(bad)?
            }
            "scanner_remote_log_read_concurrency" => {
                config.scanner_remote_log_read_concurrency = value.parse().map_err(bad)?
            }
            "remote_file_download_thread_num" => {
                config.remote_file_download_thread_num = value.parse().map_err(bad)?
            }
            "lookup_batch_timeout_ms" => {
                config.lookup_batch_timeout_ms = value.parse().map_err(bad)?
            }
            "lookup_max_batch_size" => config.lookup_max_batch_size = value.parse().map_err(bad)?,
            // Applied on every open, not per connection; see `open`.
            "skip_arrow_validation" => {}
            other => return Err(format!("unknown client option {other}")),
        }
    }
    Ok(())
}

// One connection per (bootstrap servers, client options): a connection's fetch settings are fixed.
async fn connection(
    bootstrap_servers: &str,
    options: &BTreeMap<String, String>,
) -> Result<Arc<FlussConnection>, String> {
    static CONNECTIONS: OnceLock<Mutex<HashMap<String, Arc<FlussConnection>>>> = OnceLock::new();
    let cache = CONNECTIONS.get_or_init(|| Mutex::new(HashMap::new()));
    let key = format!("{bootstrap_servers}|{options:?}");
    if let Some(connection) = cache.lock().unwrap().get(&key) {
        return Ok(connection.clone());
    }
    let mut config = Config {
        bootstrap_servers: bootstrap_servers.to_string(),
        // A split submits all of its keys at once, so waiting for more keys only adds latency: send
        // what is queued as soon as a lookup arrives. Lookups that arrive while a batch is in flight
        // still go out together in the next one.
        lookup_batch_timeout_ms: 0,
        // Every split stops at an offset that was already written when it was planned, so a fetch
        // never has new data to wait for. It comes back empty only when the server skipped every
        // batch it read by statistics, and then the server would hold it for the whole wait.
        scanner_log_fetch_wait_max_time_ms: 10,
        ..Default::default()
    };
    apply_client_options(&mut config, options)?;
    let connection = Arc::new(
        FlussConnection::new(config)
            .await
            .map_err(|e| format!("connect to {bootstrap_servers}: {e}"))?,
    );
    Ok(cache
        .lock()
        .unwrap()
        .entry(key)
        .or_insert(connection)
        .clone())
}

// Resolving a table id by name costs a table-info request and a metadata update, which the servers
// answer from ZooKeeper: for a point lookup that is more work than the lookup itself. Keep it for a few
// seconds, like the engines' own metadata caches; a recreated table is seen once its entry expires.
const TABLE_ID_TTL: Duration = Duration::from_secs(10);

async fn resolve_table_id(
    connection: &Arc<FlussConnection>,
    connection_key: &str,
    path: &TablePath,
) -> Result<i64, String> {
    static TABLE_IDS: OnceLock<Mutex<HashMap<String, (i64, Instant)>>> = OnceLock::new();
    let cache = TABLE_IDS.get_or_init(|| Mutex::new(HashMap::new()));
    let key = format!("{connection_key}|{path}");
    if let Some((table_id, resolved_at)) = cache.lock().unwrap().get(&key) {
        if resolved_at.elapsed() < TABLE_ID_TTL {
            return Ok(*table_id);
        }
    }
    let table_id = connection
        .get_admin()
        .map_err(|e| format!("admin: {e}"))?
        .get_table_info(path)
        .await
        .map_err(|e| format!("get table info {path}: {e}"))?
        .table_id;
    cache
        .lock()
        .unwrap()
        .insert(key, (table_id, Instant::now()));
    Ok(table_id)
}

// Creating a lookuper fetches table and bucket metadata; reuse it across queries.
// Keyed by table id, so a dropped and recreated table gets a fresh one.
async fn lookuper(
    connection: &Arc<FlussConnection>,
    connection_key: &str,
    path: &TablePath,
    table_id: i64,
) -> Result<Arc<Lookuper>, String> {
    static LOOKUPERS: OnceLock<Mutex<HashMap<String, Arc<Lookuper>>>> = OnceLock::new();
    let cache = LOOKUPERS.get_or_init(|| Mutex::new(HashMap::new()));
    let key = format!("{connection_key}|{path}|{table_id}");
    if let Some(lookuper) = cache.lock().unwrap().get(&key) {
        return Ok(lookuper.clone());
    }
    let table = connection
        .get_table(path)
        .await
        .map_err(|e| format!("get table {path}: {e}"))?;
    let lookuper = Arc::new(
        table
            .new_lookup()
            .and_then(|lookup| lookup.create_lookuper())
            .map_err(|e| format!("create lookuper for {path}: {e}"))?,
    );
    Ok(cache.lock().unwrap().entry(key).or_insert(lookuper).clone())
}

// paimon-rust reads `s3.path-style-access`; StarRocks catalogs spell it `s3.path.style.access`.
fn normalize_lake_properties(mut properties: HashMap<String, String>) -> HashMap<String, String> {
    if let Some(value) = properties
        .get("table.datalake.paimon.s3.path.style.access")
        .cloned()
    {
        properties
            .entry("table.datalake.paimon.s3.path-style-access".to_string())
            .or_insert(value);
    }
    properties
        .entry("table.datalake.paimon.s3.region".to_string())
        .or_insert_with(|| "us-east-1".to_string());
    properties
}

fn literal(value_type: &str, value: &Value) -> Result<Literal, String> {
    let bad = || format!("cannot read {value} as {value_type}");
    Ok(match value_type {
        "boolean" => Literal::Bool(value.as_bool().ok_or_else(bad)?),
        "tinyint" => Literal::Int8(value.as_i64().ok_or_else(bad)? as i8),
        "smallint" => Literal::Int16(value.as_i64().ok_or_else(bad)? as i16),
        "int" => Literal::Int32(value.as_i64().ok_or_else(bad)? as i32),
        "bigint" => Literal::Int64(value.as_i64().ok_or_else(bad)?),
        "float" => Literal::Float32(value.as_f64().ok_or_else(bad)? as f32),
        "double" => Literal::Float64(value.as_f64().ok_or_else(bad)?),
        "string" => Literal::String(value.as_str().ok_or_else(bad)?.to_string()),
        "date" => Literal::Date(value.as_i64().ok_or_else(bad)? as i32),
        "timestamp" => Literal::TimestampNtz(
            TimestampNtz::from_millis_nanos(value.as_i64().ok_or_else(bad)?, 0)
                .map_err(|e| e.to_string())?,
        ),
        "timestamp_ltz" => Literal::TimestampLtz(
            TimestampLtz::from_millis_nanos(value.as_i64().ok_or_else(bad)?, 0)
                .map_err(|e| e.to_string())?,
        ),
        other => return Err(format!("unsupported filter type {other}")),
    })
}

fn predicate(filters: &[Filter]) -> Result<Option<Predicate>, String> {
    let mut leaves = Vec::new();
    for filter in filters {
        let function = match filter.op.as_str() {
            "=" => LeafFunction::Equal,
            "!=" => LeafFunction::NotEqual,
            "<" => LeafFunction::LessThan,
            "<=" => LeafFunction::LessOrEqual,
            ">" => LeafFunction::GreaterThan,
            ">=" => LeafFunction::GreaterOrEqual,
            "in" => LeafFunction::In,
            "is_null" => LeafFunction::IsNull,
            "is_not_null" => LeafFunction::IsNotNull,
            other => return Err(format!("unsupported filter operator {other}")),
        };
        let literals = filter
            .values
            .iter()
            .map(|v| literal(&filter.value_type, v))
            .collect::<Result<Vec<_>, _>>()?;
        leaves.push(Predicate::Leaf {
            field: filter.column.clone(),
            function,
            literals,
        });
    }
    Ok(Predicate::and_all(leaves))
}

// The same conjuncts as a lake file filter: Paimon skips data files whose statistics cannot match.
// A conjunct that does not translate is left out, which only keeps more files.
fn lake_file_filter(filters: &[Filter]) -> FlussLakePredicate {
    let lake_literal = |value_type: &str, value: &Value| -> Option<FlussLakeLiteral> {
        Some(match value_type {
            "boolean" => FlussLakeLiteral::Boolean(value.as_bool()?),
            "tinyint" | "smallint" | "int" => {
                FlussLakeLiteral::Int32(i32::try_from(value.as_i64()?).ok()?)
            }
            "bigint" => FlussLakeLiteral::Int64(value.as_i64()?),
            "double" => FlussLakeLiteral::Float64(value.as_f64()?),
            "string" => FlussLakeLiteral::String(value.as_str()?.to_string()),
            "date" => FlussLakeLiteral::Date(i32::try_from(value.as_i64()?).ok()?),
            // Millisecond literals from the frontend; the lake predicate counts microseconds.
            "timestamp" | "timestamp_ltz" => {
                FlussLakeLiteral::Timestamp(value.as_i64()?.checked_mul(1_000)?)
            }
            _ => return None,
        })
    };
    let conjuncts = filters.iter().filter_map(|filter| {
        let column = filter.column.clone();
        let mut literals = filter
            .values
            .iter()
            .map(|v| lake_literal(&filter.value_type, v));
        Some(match filter.op.as_str() {
            "is_null" => FlussLakePredicate::is_null(column),
            "is_not_null" => FlussLakePredicate::is_not_null(column),
            "in" => FlussLakePredicate::in_list(column, literals.collect::<Option<Vec<_>>>()?),
            op => {
                let literal = literals.next()??;
                match op {
                    "=" => FlussLakePredicate::eq(column, literal),
                    "!=" => FlussLakePredicate::not_eq(column, literal),
                    "<" => FlussLakePredicate::lt(column, literal),
                    "<=" => FlussLakePredicate::le(column, literal),
                    ">" => FlussLakePredicate::gt(column, literal),
                    ">=" => FlussLakePredicate::ge(column, literal),
                    _ => return None,
                }
            }
        })
    });
    FlussLakePredicate::and(conjuncts)
}

fn key_datum<'a>(value_type: &str, value: &'a Value) -> Result<Datum<'a>, String> {
    let bad = || format!("cannot read key {value} as {value_type}");
    Ok(match value_type {
        "string" => Datum::from(value.as_str().ok_or_else(bad)?),
        "int" => Datum::Int32(value.as_i64().ok_or_else(bad)? as i32),
        "bigint" => Datum::Int64(value.as_i64().ok_or_else(bad)?),
        "smallint" => Datum::Int16(value.as_i64().ok_or_else(bad)? as i16),
        "tinyint" => Datum::Int8(value.as_i64().ok_or_else(bad)? as i8),
        "boolean" => Datum::Bool(value.as_bool().ok_or_else(bad)?),
        other => return Err(format!("unsupported key type {other}")),
    })
}

fn project(batch: &RecordBatch, columns: &[String]) -> Result<RecordBatch, String> {
    let schema = batch.schema();
    let indices = columns
        .iter()
        .map(|c| schema.index_of(c).map_err(|e| e.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    batch.project(&indices).map_err(|e| e.to_string())
}

// The offset range this scanner reads. Shares are cut after resolving an EARLIEST start,
// so every scanner of the bucket agrees on the same boundaries.
async fn log_range(
    connection: &FlussConnection,
    path: &TablePath,
    split: &SplitDescriptor,
) -> Result<(i64, i64), String> {
    let (part, parts) = match (split.part, split.parts) {
        (Some(part), Some(parts)) if parts > 1 => (part as i64, parts as i64),
        _ => return Ok((split.start_offset, split.stop_offset)),
    };
    let start = if split.start_offset == EARLIEST_OFFSET {
        let admin = connection.get_admin().map_err(|e| format!("admin: {e}"))?;
        let offsets = match &split.partition_name {
            Some(name) => {
                admin
                    .list_partition_offsets(path, name, &[split.bucket], OffsetSpec::Earliest)
                    .await
            }
            None => {
                admin
                    .list_offsets(path, &[split.bucket], OffsetSpec::Earliest)
                    .await
            }
        }
        .map_err(|e| format!("earliest offset of {path} bucket {}: {e}", split.bucket))?;
        *offsets
            .get(&split.bucket)
            .ok_or_else(|| format!("no earliest offset for {path} bucket {}", split.bucket))?
    } else {
        split.start_offset
    };
    let len = (split.stop_offset - start).max(0);
    Ok((start + len * part / parts, start + len * (part + 1) / parts))
}

async fn lake_projection(
    connection: &FlussConnection,
    path: &TablePath,
    columns: &[String],
) -> Result<Vec<usize>, String> {
    let admin = connection.get_admin().map_err(|e| format!("admin: {e}"))?;
    let info = admin
        .get_table_info(path)
        .await
        .map_err(|e| format!("get table info {path}: {e}"))?;
    let field_names = info.row_type().get_field_names();
    columns
        .iter()
        .map(|c| {
            field_names
                .iter()
                .position(|f| f == c)
                .ok_or_else(|| format!("column {c} not found in {path}"))
        })
        .collect()
}

// The kernel keeps secrets out of split descriptors and takes them back at execution time.
fn lake_context(properties: &HashMap<String, String>) -> FlussLakeExecutionContext {
    let credentials = properties
        .iter()
        .filter_map(|(key, value)| {
            key.strip_prefix("table.datalake.paimon.")
                .map(|suffix| (suffix.to_string(), value.clone()))
        })
        .collect();
    FlussLakeExecutionContext::default().with_lake_credentials(credentials)
}

async fn open(descriptor: Descriptor) -> Result<Source, String> {
    // Process-wide in fluss-rs, so every split restates it rather than inheriting the last query's.
    fluss::record::set_skip_arrow_validation(
        descriptor
            .client_options
            .get("skip_arrow_validation")
            .is_some_and(|v| v == "true"),
    );
    let connection = connection(&descriptor.bootstrap_servers, &descriptor.client_options).await?;
    let path = TablePath::new(descriptor.database.clone(), descriptor.table.clone());
    let split = &descriptor.split;
    // Engines that address a table by name alone (Doris) send no table id; resolve it here.
    let table_id = if split.table_id != 0 {
        split.table_id
    } else {
        let connection_key = format!(
            "{}|{:?}",
            descriptor.bootstrap_servers, descriptor.client_options
        );
        resolve_table_id(&connection, &connection_key, &path).await?
    };
    let bucket = match split.partition_id {
        Some(partition_id) => {
            TableBucket::new_with_partition(table_id, Some(partition_id), split.bucket)
        }
        None => TableBucket::new(table_id, split.bucket),
    };
    match split.kind.as_str() {
        "changelog" => {
            let table = connection
                .get_table(&path)
                .await
                .map_err(|e| format!("get table {path}: {e}"))?;
            let names: Vec<&str> = descriptor.columns.iter().map(String::as_str).collect();
            let scanner = table
                .new_scan()
                .project_by_name(&names)
                .map_err(|e| format!("project {path}: {e}"))?
                .create_log_scanner()
                .map_err(|e| format!("create changelog scanner for {path}: {e}"))?;
            match split.partition_id {
                Some(partition_id) => scanner
                    .subscribe_partition(partition_id, split.bucket, split.start_offset)
                    .await
                    .map_err(|e| {
                        format!(
                            "subscribe {path} partition {partition_id} bucket {}: {e}",
                            split.bucket
                        )
                    })?,
                None => scanner
                    .subscribe(split.bucket, split.start_offset)
                    .await
                    .map_err(|e| format!("subscribe {path} bucket {}: {e}", split.bucket))?,
            }
            Ok(Source::Changelog(ChangelogReader {
                scanner,
                bucket,
                next_offset: split.start_offset,
                stop_offset: split.stop_offset,
                pending: VecDeque::new(),
            }))
        }
        "log" => {
            let table = connection
                .get_table(&path)
                .await
                .map_err(|e| format!("get table {path}: {e}"))?;
            let names: Vec<&str> = descriptor.columns.iter().map(String::as_str).collect();
            let mut scan = table
                .new_scan()
                .project_by_name(&names)
                .map_err(|e| format!("project {path}: {e}"))?;
            if let Some(predicate) = predicate(&descriptor.filters)? {
                scan = scan
                    .filter(predicate)
                    .map_err(|e| format!("filter {path}: {e}"))?;
            }
            let scanner = scan
                .create_record_batch_log_scanner()
                .map_err(|e| format!("create log scanner for {path}: {e}"))?;
            let (starting_offset, stopping_offset) = log_range(&connection, &path, split).await?;
            let reader = RecordBatchLogReader::new_from_ranges(
                scanner,
                vec![BoundedLogReadRange {
                    bucket,
                    starting_offset,
                    stopping_offset,
                }],
            )
            .await
            .map_err(|e| {
                format!(
                    "open bounded log reader for {path} bucket {}: {e}",
                    split.bucket
                )
            })?;
            Ok(Source::Log(reader))
        }
        "pk" | "lake" => {
            let lake_properties = normalize_lake_properties(descriptor.lake_properties.clone());
            let lake_table = FlussLakeTable::open_with_properties(
                connection.clone(),
                &path,
                lake_properties.clone(),
            )
            .await
            .map_err(|e| format!("open lake table {path}: {e}"))?;
            let projection = lake_projection(&connection, &path, &descriptor.columns).await?;
            if split.kind == "lake" {
                let scan = lake_table
                    .new_scan()
                    .with_read_mode(FlussLakeReadMode::LakeOnly)
                    .with_projection(projection)
                    .with_file_filter(lake_file_filter(&descriptor.filters))
                    .with_pinned_boundary(FlussLakePinnedBoundary {
                        lake_snapshot_id: split.lake_snapshot_id,
                        ranges: Vec::new(),
                    });
                let plan = scan
                    .plan_lake_files()
                    .await
                    .map_err(|e| format!("plan lake {path}: {e}"))?;
                let part = split.part.unwrap_or(0) as usize;
                let parts = split.parts.unwrap_or(1).max(1) as usize;
                let mine: Vec<_> = plan
                    .splits()
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| index % parts == part)
                    .map(|(_, s)| s.clone())
                    .collect();
                if mine.is_empty() {
                    return Ok(Source::Lake(Box::pin(futures::stream::empty())));
                }
                let reader = scan
                    .new_reader(lake_context(&lake_properties))
                    .map_err(|e| format!("reader for {path}: {e}"))?;
                let stream = reader
                    .read_splits(&mine)
                    .await
                    .map_err(|e| format!("read lake {path} part {part}/{parts}: {e}"))?;
                return Ok(Source::Lake(stream));
            }
            let scan = lake_table
                .new_scan()
                .with_read_mode(FlussLakeReadMode::Union)
                .with_projection(projection)
                .with_pinned_boundary(FlussLakePinnedBoundary {
                    lake_snapshot_id: split.lake_snapshot_id,
                    ranges: vec![FlussLakePinnedRange {
                        bucket,
                        partition_name: split.partition_name.clone(),
                        partition_qualified_name: split.partition_qualified_name.clone(),
                        start_offset: split.start_offset,
                        stop_offset: split.stop_offset,
                    }],
                });
            let plan = scan.plan().await.map_err(|e| format!("plan {path}: {e}"))?;
            let splits = plan.splits().to_vec();
            if splits.is_empty() {
                return Ok(Source::Lake(Box::pin(futures::stream::empty())));
            }
            let reader = scan
                .new_reader(lake_context(&lake_properties))
                .map_err(|e| format!("reader for {path}: {e}"))?;
            let stream = reader
                .read_splits(&splits)
                .await
                .map_err(|e| format!("read {path} bucket {}: {e}", split.bucket))?;
            Ok(Source::Lake(stream))
        }
        "kv" => {
            let projection = lake_projection(&connection, &path, &descriptor.columns).await?;
            let stream = fluss_lake::read_kv_snapshot_bucket(
                connection.clone(),
                &path,
                bucket,
                split.kv_snapshot_id.filter(|id| *id >= 0),
                split.start_offset,
                split.stop_offset,
                Some(projection),
                None,
            )
            .await
            .map_err(|e| format!("read kv snapshot of {path} bucket {}: {e}", split.bucket))?;
            Ok(Source::Lake(stream))
        }
        "lookup" => {
            let connection_key = format!(
                "{}|{:?}",
                descriptor.bootstrap_servers, descriptor.client_options
            );
            let lookuper = lookuper(&connection, &connection_key, &path, table_id).await?;
            let mut rows = Vec::with_capacity(descriptor.keys.len());
            for key in &descriptor.keys {
                if key.len() != descriptor.key_types.len() {
                    return Err(format!(
                        "key {key:?} does not match key types {:?}",
                        descriptor.key_types
                    ));
                }
                let datums = key
                    .iter()
                    .zip(&descriptor.key_types)
                    .map(|(value, value_type)| key_datum(value_type, value))
                    .collect::<Result<Vec<_>, _>>()?;
                rows.push(GenericRow::from_data(datums));
            }
            // Issue every key at once: the lookup client batches them into one request per bucket.
            let results =
                futures::future::try_join_all(rows.iter().map(|row| lookuper.lookup(row)))
                    .await
                    .map_err(|e| format!("lookup {path}: {e}"))?;
            let mut batches = VecDeque::new();
            for result in results {
                let batch = result
                    .to_record_batch()
                    .map_err(|e| format!("lookup result {path}: {e}"))?;
                if batch.num_rows() > 0 {
                    batches.push_back(project(&batch, &descriptor.columns)?);
                }
            }
            Ok(Source::Batches(batches))
        }
        other => Err(format!("unknown split kind {other}")),
    }
}

async fn next(source: &mut Source) -> Result<Option<RecordBatch>, String> {
    match source {
        Source::Log(reader) => reader
            .next_batch()
            .await
            .map(|batch| batch.map(|b| b.into_batch()))
            .map_err(|e| e.to_string()),
        Source::Lake(stream) => match stream.next().await {
            Some(batch) => batch.map(Some).map_err(|e| e.to_string()),
            None => Ok(None),
        },
        Source::Batches(batches) => Ok(batches.pop_front()),
        Source::Changelog(reader) => reader.next().await,
    }
}

fn write_error(message: &str, err: *mut c_char, err_len: usize) {
    if err.is_null() || err_len == 0 {
        return;
    }
    let bytes = message.as_bytes();
    let n = bytes.len().min(err_len - 1);
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), err as *mut u8, n);
        *err.add(n) = 0;
    }
}

/// # Safety
/// `descriptor_json` must be a NUL-terminated string and `out` a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fluss_native_open(
    descriptor_json: *const c_char,
    out: *mut *mut FlussNativeReader,
    err: *mut c_char,
    err_len: usize,
) -> c_int {
    let result = std::panic::catch_unwind(|| -> Result<Box<FlussNativeReader>, String> {
        let json = unsafe { CStr::from_ptr(descriptor_json) }
            .to_str()
            .map_err(|e| format!("descriptor is not UTF-8: {e}"))?;
        let descriptor: Descriptor =
            serde_json::from_str(json).map_err(|e| format!("bad split descriptor: {e}"))?;
        let remaining = descriptor.limit;
        let source = runtime().block_on(open(descriptor))?;
        Ok(Box::new(FlussNativeReader { source, remaining }))
    });
    match result {
        Ok(Ok(reader)) => {
            unsafe { *out = Box::into_raw(reader) };
            0
        }
        Ok(Err(message)) => {
            write_error(&message, err, err_len);
            -1
        }
        Err(_) => {
            write_error("panic while opening the Fluss reader", err, err_len);
            -1
        }
    }
}

/// # Safety
/// `reader` must come from `fluss_native_open`; `array` and `schema` must point to
/// writable, released Arrow C structs owned by the caller.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fluss_native_next(
    reader: *mut FlussNativeReader,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    err: *mut c_char,
    err_len: usize,
) -> c_int {
    let reader = unsafe { &mut *reader };
    if reader.remaining == Some(0) {
        return 0;
    }
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        runtime().block_on(next(&mut reader.source))
    }));
    match result {
        Ok(Ok(Some(mut batch))) => {
            // A limit is per split: stop as soon as this split has produced enough rows.
            if let Some(remaining) = reader.remaining.as_mut() {
                if (batch.num_rows() as u64) > *remaining {
                    batch = batch.slice(0, *remaining as usize);
                }
                *remaining -= batch.num_rows() as u64;
            }
            let data = StructArray::from(batch).into_data();
            match to_ffi(&data) {
                Ok((ffi_array, ffi_schema)) => {
                    unsafe {
                        std::ptr::write(array, ffi_array);
                        std::ptr::write(schema, ffi_schema);
                    }
                    1
                }
                Err(e) => {
                    write_error(&format!("export batch: {e}"), err, err_len);
                    -1
                }
            }
        }
        Ok(Ok(None)) => 0,
        Ok(Err(message)) => {
            write_error(&message, err, err_len);
            -1
        }
        Err(_) => {
            write_error("panic while reading the Fluss split", err, err_len);
            -1
        }
    }
}

/// # Safety
/// `reader` must come from `fluss_native_open` and must not be used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fluss_native_close(reader: *mut FlussNativeReader) {
    if !reader.is_null() {
        drop(unsafe { Box::from_raw(reader) });
    }
}
