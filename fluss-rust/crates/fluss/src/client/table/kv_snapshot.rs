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

//! Reads one primary-key bucket's KV snapshot: the RocksDB checkpoint its tablet
//! server uploads to remote storage, the compact current state of the bucket as
//! of [`KvSnapshotReader::log_offset`].
//!
//! The files are downloaded into a temporary directory, opened read-only, and
//! iterated on a dedicated thread. Each value is `[schema id | row]` in the
//! table's KV format, so it decodes exactly like a lookup result: rows written
//! under an older schema are projected onto the current one.

use crate::client::ClientSchemaGetter;
use crate::client::credentials::SecurityTokenManager;
use crate::error::{Error, Result};
use crate::io::{FileIO, Storage};
use crate::metadata::{KvFormat, RowType, Schema, SchemaInfo, TableBucket, TableInfo};
use crate::record::RowAppendRecordBatchBuilder;
use crate::record::kv::SCHEMA_ID_LENGTH;
use crate::row::FixedSchemaDecoder;
use crate::row::compacted::IndexedCompactedRow;
use arrow::array::RecordBatch;
use byteorder::{ByteOrder, LittleEndian};
use futures::{Stream, TryStreamExt};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::mpsc;

use super::FlussTable;

/// Rows per emitted batch.
const BATCH_ROWS: usize = 8192;
/// Snapshot files downloaded at once.
const DOWNLOAD_CONCURRENCY: usize = 8;
/// Decoded batches buffered ahead of the consumer.
const BATCHES_AHEAD: usize = 2;
/// `table.kv.value-layout-version` 2 prefixes every value with an eight-byte tag.
const TAGGED_LAYOUT_VERSION: &str = "2";
const VALUE_TAG_LENGTH: usize = 8;

/// The rows of one KV snapshot of one bucket, as Arrow batches in key order.
pub struct KvSnapshotReader {
    log_offset: i64,
    receiver: mpsc::Receiver<Result<RecordBatch>>,
}

impl KvSnapshotReader {
    /// The log offset the snapshot was taken at: the changelog from here on is
    /// not in the snapshot.
    pub fn log_offset(&self) -> i64 {
        self.log_offset
    }

    /// The next batch, or `None` once the snapshot is exhausted.
    pub async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.receiver.recv().await.transpose()
    }

    pub fn into_stream(self) -> impl Stream<Item = Result<RecordBatch>> + Send + 'static {
        futures::stream::unfold(self.receiver, |mut receiver| async move {
            receiver.recv().await.map(|item| (item, receiver))
        })
    }
}

impl FlussTable<'_> {
    /// Opens snapshot `snapshot_id` of `bucket`, projected onto `projected_fields`
    /// (indexes into the table's row type; all columns when `None`).
    pub async fn new_kv_snapshot_reader(
        &self,
        bucket: &TableBucket,
        snapshot_id: i64,
        projected_fields: Option<Vec<usize>>,
    ) -> Result<KvSnapshotReader> {
        if !self.has_primary_key() {
            return Err(Error::UnsupportedOperation {
                message: format!(
                    "{} has no primary key, so no kv snapshot",
                    self.table_path()
                ),
            });
        }
        let table_info = self.get_table_info().clone();
        let admin = self.conn.get_admin()?;
        let snapshot = admin
            .get_kv_snapshot_metadata(
                table_info.table_id,
                bucket.partition_id(),
                bucket.bucket_id(),
                snapshot_id,
            )
            .await?;
        let (remote_fs_props, _) =
            SecurityTokenManager::fetch_token(&self.conn.get_connections(), self.metadata())
                .await?;

        let directory = TempDir::with_prefix("fluss-kv-snapshot")?;
        futures::stream::iter(snapshot.snapshot_files.iter().map(Ok::<_, Error>))
            .try_for_each_concurrent(DOWNLOAD_CONCURRENCY, |file| {
                let target = directory.path().join(&file.local_file_name);
                let props = &remote_fs_props;
                async move { download(&file.remote_path, &target, props).await }
            })
            .await?;

        let latest = SchemaInfo::new(table_info.get_schema().clone(), table_info.get_schema_id());
        let schema_getter = Arc::new(ClientSchemaGetter::new(
            self.table_path().clone(),
            admin,
            latest,
        ));
        let (sender, receiver) = mpsc::channel(BATCHES_AHEAD);
        let runtime = tokio::runtime::Handle::current();
        std::thread::Builder::new()
            .name(format!("kv-snapshot-{bucket}"))
            .spawn(move || {
                let result = iterate(
                    directory.path(),
                    &table_info,
                    projected_fields.as_deref(),
                    &schema_getter,
                    &runtime,
                    &sender,
                );
                if let Err(error) = result {
                    let _ = sender.blocking_send(Err(error));
                }
                // The database is closed by now; dropping the directory deletes its files.
                drop(directory);
            })
            .map_err(|e| Error::UnexpectedError {
                message: format!("failed to start the kv snapshot reader thread: {e}"),
                source: None,
            })?;
        Ok(KvSnapshotReader {
            log_offset: snapshot.log_offset,
            receiver,
        })
    }
}

async fn download(
    remote_path: &str,
    target: &Path,
    remote_fs_props: &HashMap<String, String>,
) -> Result<()> {
    let url = if remote_path.contains("://") {
        remote_path.to_string()
    } else {
        format!("file://{remote_path}")
    };
    let mut builder = FileIO::from_url(&url)?;
    if url.starts_with("s3://") || url.starts_with("s3a://") || url.starts_with("oss://") {
        builder = builder.with_props(
            remote_fs_props
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str())),
        );
    }
    // A server whose token names an endpoint but no region (S3-compatible stores) leaves region
    // to the client; Hadoop's S3A, which the Java client reads through, falls back to us-east-1.
    if (url.starts_with("s3://") || url.starts_with("s3a://"))
        && !remote_fs_props.contains_key("region")
    {
        builder = builder.with_prop("region", "us-east-1");
    }
    let storage = Storage::build(builder)?;
    let (operator, relative_path) = storage.create(&url)?;
    let mut writer = tokio::fs::File::create(target).await?;
    let mut stream = operator
        .reader_with(relative_path)
        .chunk(8 * 1024 * 1024)
        .await?
        .into_bytes_stream(..)
        .await?;
    while let Some(chunk) = stream.try_next().await? {
        tokio::io::AsyncWriteExt::write_all(&mut writer, &chunk).await?;
    }
    Ok(())
}

/// Decodes every value of the snapshot into batches of [`BATCH_ROWS`] rows and
/// hands them to `sender`, stopping early once the reader is dropped.
fn iterate(
    directory: &Path,
    table_info: &TableInfo,
    projected_fields: Option<&[usize]>,
    schema_getter: &ClientSchemaGetter,
    runtime: &tokio::runtime::Handle,
    sender: &mpsc::Sender<Result<RecordBatch>>,
) -> Result<()> {
    let options = rocksdb::Options::default();
    let db = rocksdb::DB::open_for_read_only(&options, directory, false).map_err(|e| {
        Error::UnexpectedError {
            message: format!(
                "failed to open the kv snapshot in {}: {e}",
                directory.display()
            ),
            source: None,
        }
    })?;
    let mut read_options = rocksdb::ReadOptions::default();
    read_options.fill_cache(false);
    read_options.set_readahead_size(4 * 1024 * 1024);
    let mut iterator = db.raw_iterator_opt(read_options);
    iterator.seek_to_first();

    let kv_format = table_info.get_table_config().get_kv_format()?;
    let tag_length = match table_info
        .get_properties()
        .get("table.kv.value-layout-version")
        .map(String::as_str)
    {
        Some(TAGGED_LAYOUT_VERSION) => VALUE_TAG_LENGTH,
        _ => 0,
    };
    let row_type = table_info.get_row_type();
    let target_schema = table_info.get_schema();
    let target_schema_id = table_info.get_schema_id() as i16;
    let mut decoders: HashMap<i16, FixedSchemaDecoder> = HashMap::new();
    // Rows in the current schema are read in place, without a decoded row per value; rows written
    // under an older schema go through a decoder that projects them onto the current one.
    let in_place = kv_format == KvFormat::COMPACTED && IndexedCompactedRow::supports(row_type);
    let mut positions = Vec::with_capacity(row_type.fields().len());
    let mut builder = RowAppendRecordBatchBuilder::new(row_type)?;
    let mut rows = 0;
    while iterator.valid() {
        let value = iterator.value().unwrap_or_default();
        let payload = value.get(tag_length..).unwrap_or_default();
        if payload.len() < SCHEMA_ID_LENGTH {
            return Err(Error::UnexpectedError {
                message: format!("kv snapshot value too short: {} bytes", value.len()),
                source: None,
            });
        }
        let schema_id = LittleEndian::read_i16(payload);
        if in_place && schema_id == target_schema_id {
            let row =
                IndexedCompactedRow::new(row_type, &payload[SCHEMA_ID_LENGTH..], &mut positions)?;
            builder.append(&row)?;
        } else {
            let schema_decoder = match decoders.entry(schema_id) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => entry.insert(decoder(
                    kv_format,
                    target_schema,
                    target_schema_id,
                    schema_id,
                    schema_getter,
                    runtime,
                )?),
            };
            builder.append(&schema_decoder.decode(payload)?)?;
        }
        rows += 1;
        if rows == BATCH_ROWS {
            let batch = finish(&mut builder, row_type, projected_fields)?;
            if sender.blocking_send(Ok(batch)).is_err() {
                return Ok(());
            }
            rows = 0;
        }
        iterator.next();
    }
    iterator.status().map_err(|e| Error::UnexpectedError {
        message: format!("failed to iterate the kv snapshot: {e}"),
        source: None,
    })?;
    if rows > 0 {
        let _ = sender.blocking_send(Ok(finish(&mut builder, row_type, projected_fields)?));
    }
    Ok(())
}

fn decoder(
    kv_format: KvFormat,
    target_schema: &Schema,
    target_schema_id: i16,
    schema_id: i16,
    schema_getter: &ClientSchemaGetter,
    runtime: &tokio::runtime::Handle,
) -> Result<FixedSchemaDecoder> {
    if schema_id == target_schema_id {
        return FixedSchemaDecoder::new_no_projection(kv_format, target_schema);
    }
    let source = runtime.block_on(schema_getter.get_schema(schema_id as i32))?;
    FixedSchemaDecoder::new(kv_format, source.as_ref(), target_schema)
}

fn finish(
    builder: &mut RowAppendRecordBatchBuilder,
    row_type: &RowType,
    projected_fields: Option<&[usize]>,
) -> Result<RecordBatch> {
    let batch = Arc::unwrap_or_clone(builder.build_arrow_record_batch()?);
    *builder = RowAppendRecordBatchBuilder::new(row_type)?;
    match projected_fields {
        Some(fields) => batch.project(fields).map_err(|e| Error::UnexpectedError {
            message: format!("failed to project a kv snapshot batch: {e}"),
            source: None,
        }),
        None => Ok(batch),
    }
}
