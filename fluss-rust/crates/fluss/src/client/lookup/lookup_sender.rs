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

use super::{LookupQueue, QueuedLookup};
use crate::client::lookup::lookup_query::LookupQuery;
use crate::client::metadata::Metadata;
use crate::error::{Error, FlussError, Result};
use crate::metadata::{PhysicalTablePath, TableBucket, TablePath};
use crate::proto::{LookupResponse, PrefixLookupResponse};
use crate::rpc::message::{
    BucketLookupKeys, LookupRequest, PrefixLookupRequest, ReadType, RequestBody, WriteType,
};
use crate::rpc::{ApiError, ServerConnection};
use crate::{BucketId, PartitionId, TableId};
use bytes::Bytes;
use log::{debug, error, warn};
use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::task::JoinSet;

type ServerId = i32;

/// A bucket and, for a historical lookup, the expired partition asked for: several expired
/// partitions can share one bucket of the historical system partition.
type BatchKey = (TableBucket, Option<String>);

type BatchesByLeader<T> = HashMap<ServerId, HashMap<BatchKey, LookupBatch<T>>>;
type PrimaryBatches = BatchesByLeader<Option<Vec<u8>>>;
type PrefixBatches = BatchesByLeader<Vec<Vec<u8>>>;

struct BucketResponse<V> {
    partition_id: Option<PartitionId>,
    bucket_id: BucketId,
    original_partition_name: Option<String>,
    error_code: Option<i32>,
    error_message: Option<String>,
    values: Vec<V>,
}

trait LookupProtocol {
    type Request: RequestBody<ResponseBody = Self::Response> + Send + WriteType<Vec<u8>>;
    type Response: ReadType<Cursor<Vec<u8>>> + Send;
    type Value: Send + 'static;

    const OP_NAME: &'static str;

    fn build_request(table_id: TableId, keys_by_bucket: Vec<BucketLookupKeys>) -> Self::Request;

    fn decode_buckets(
        response: Self::Response,
    ) -> impl Iterator<Item = BucketResponse<Self::Value>>;
}

struct Primary;
impl LookupProtocol for Primary {
    type Request = LookupRequest;
    type Response = LookupResponse;
    type Value = Option<Vec<u8>>;

    const OP_NAME: &'static str = "Lookup";

    fn build_request(table_id: TableId, keys_by_bucket: Vec<BucketLookupKeys>) -> Self::Request {
        LookupRequest::new_batched(table_id, keys_by_bucket)
    }

    fn decode_buckets(
        response: Self::Response,
    ) -> impl Iterator<Item = BucketResponse<Self::Value>> {
        response.buckets_resp.into_iter().map(|r| BucketResponse {
            partition_id: r.partition_id,
            bucket_id: r.bucket_id,
            original_partition_name: r.original_partition_name,
            error_code: r.error_code,
            error_message: r.error_message,
            values: r.values.into_iter().map(|pb| pb.values).collect(),
        })
    }
}

struct Prefix;
impl LookupProtocol for Prefix {
    type Request = PrefixLookupRequest;
    type Response = PrefixLookupResponse;
    type Value = Vec<Vec<u8>>;

    const OP_NAME: &'static str = "Prefix lookup";

    fn build_request(table_id: TableId, keys_by_bucket: Vec<BucketLookupKeys>) -> Self::Request {
        PrefixLookupRequest::new_batched(table_id, keys_by_bucket)
    }

    fn decode_buckets(
        response: Self::Response,
    ) -> impl Iterator<Item = BucketResponse<Self::Value>> {
        response.buckets_resp.into_iter().map(|r| BucketResponse {
            partition_id: r.partition_id,
            bucket_id: r.bucket_id,
            original_partition_name: None,
            error_code: r.error_code,
            error_message: r.error_message,
            values: r.value_lists.into_iter().map(|pb| pb.values).collect(),
        })
    }
}

struct GroupByLeaderResult {
    primary: PrimaryBatches,
    prefix: PrefixBatches,
    unknown_leader_tables: HashSet<TablePath>,
    unknown_leader_partition_ids: HashSet<PartitionId>,
}

impl GroupByLeaderResult {
    fn is_empty(&self) -> bool {
        self.primary.is_empty() && self.prefix.is_empty()
    }

    /// Assumes no `(server, bucket)` overlap — safe because the second pass only
    /// re-groups items unknown in the first.
    fn merge_batches(&mut self, other: GroupByLeaderResult) {
        for (server, inner) in other.primary {
            self.primary.entry(server).or_default().extend(inner);
        }
        for (server, inner) in other.prefix {
            self.prefix.entry(server).or_default().extend(inner);
        }
    }
}

struct GroupingResult {
    groups: GroupByLeaderResult,
    unknowns: Vec<QueuedLookup>,
}

pub struct LookupSender {
    queue: LookupQueue,
    dispatcher: Arc<LookupDispatcher>,
    requests: JoinSet<()>,
    running: AtomicBool,
    force_close: AtomicBool,
    shutdown_rx: watch::Receiver<bool>,
}

/// Sends lookups to their leaders and completes, retries or fails them. Shared by the sender and
/// the tasks that wait for responses.
struct LookupDispatcher {
    metadata: Arc<Metadata>,
    re_enqueue_tx: mpsc::UnboundedSender<QueuedLookup>,
    inflight_semaphore: Arc<Semaphore>,
    max_retries: i32,
}

struct LookupBatch<T> {
    table_bucket: TableBucket,
    original_partition_name: Option<String>,
    lookups: Vec<LookupQuery<T>>,
    keys: Vec<Bytes>,
}

impl<T> LookupBatch<T> {
    fn new((table_bucket, original_partition_name): BatchKey) -> Self {
        Self {
            table_bucket,
            original_partition_name,
            lookups: Vec::new(),
            keys: Vec::new(),
        }
    }

    fn key(&self) -> BatchKey {
        (
            self.table_bucket.clone(),
            self.original_partition_name.clone(),
        )
    }

    fn is_historical(&self) -> bool {
        self.original_partition_name.is_some()
    }

    fn add_lookup(&mut self, lookup: LookupQuery<T>) {
        self.keys.push(lookup.key().clone());
        self.lookups.push(lookup);
    }

    fn complete(&mut self, values: Vec<T>) {
        if values.len() != self.lookups.len() {
            let err_msg = format!(
                "The number of return values ({}) does not match the number of lookups ({})",
                values.len(),
                self.lookups.len()
            );
            for lookup in &mut self.lookups {
                lookup.complete_with_error(Error::UnexpectedError {
                    message: err_msg.clone(),
                    source: None,
                });
            }
            return;
        }

        for (lookup, value) in self.lookups.iter_mut().zip(values) {
            lookup.complete(Ok(value));
        }
    }

    fn complete_all_with_error(&mut self, error_msg: &str) {
        for lookup in &mut self.lookups {
            lookup.complete_with_error(Error::UnexpectedError {
                message: error_msg.to_string(),
                source: None,
            });
        }
    }

    fn take_keys(&mut self) -> BucketLookupKeys {
        // All lookups of a bucket share one routing count.
        let routing_bucket_count = self
            .lookups
            .first()
            .map(|lookup| lookup.routing_bucket_count())
            .filter(|count| *count > 0);
        BucketLookupKeys {
            bucket_id: self.table_bucket.bucket_id(),
            partition_id: self.table_bucket.partition_id(),
            routing_bucket_count,
            original_partition_name: self.original_partition_name.clone(),
            keys: std::mem::take(&mut self.keys),
        }
    }
}

impl LookupSender {
    pub fn new(
        metadata: Arc<Metadata>,
        queue: LookupQueue,
        re_enqueue_tx: mpsc::UnboundedSender<QueuedLookup>,
        max_inflight_requests: usize,
        max_retries: i32,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            queue,
            dispatcher: Arc::new(LookupDispatcher {
                metadata,
                re_enqueue_tx,
                inflight_semaphore: Arc::new(Semaphore::new(max_inflight_requests)),
                max_retries,
            }),
            requests: JoinSet::new(),
            running: AtomicBool::new(true),
            force_close: AtomicBool::new(false),
            shutdown_rx,
        }
    }

    pub async fn run(&mut self) {
        debug!("Starting Fluss lookup sender");

        let mut shutdown_rx = self.shutdown_rx.clone();

        while self.running.load(Ordering::Acquire) {
            if *shutdown_rx.borrow() {
                debug!("Lookup sender received shutdown signal");
                self.initiate_close();
                break;
            }

            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        debug!("Lookup sender received shutdown signal during select");
                        self.initiate_close();
                    }
                }
                result = self.run_once(false) => {
                    if let Err(e) = result {
                        error!("Error in lookup sender: {}", e);
                    }
                }
            }
        }

        debug!("Beginning shutdown of lookup sender, sending remaining lookups");

        if !self.force_close.load(Ordering::Acquire) {
            if self.queue.has_undrained() {
                if let Err(e) = self.run_once(true).await {
                    error!("Error during lookup sender shutdown: {}", e);
                }
            }
            // Wait for the requests in flight.
            while self.requests.join_next().await.is_some() {}
        }

        // TODO: If force close failed, add logic to abort incomplete lookup requests.
        debug!("Lookup sender shutdown complete");
    }

    async fn run_once(&mut self, drain_all: bool) -> Result<()> {
        let lookups = if drain_all {
            self.queue.drain_all()
        } else {
            self.queue.drain().await
        };

        // Remove the requests that have finished from the set.
        while self.requests.try_join_next().is_some() {}
        self.dispatcher
            .send_lookups(lookups, &mut self.requests)
            .await
    }

    pub fn initiate_close(&mut self) {
        self.running.store(false, Ordering::Release);
    }

    #[allow(dead_code)]
    pub fn force_close(&mut self) {
        self.force_close.store(true, Ordering::Release);
        self.initiate_close();
    }
}

impl LookupDispatcher {
    async fn send_lookups(
        self: &Arc<Self>,
        mut lookups: Vec<QueuedLookup>,
        requests: &mut JoinSet<()>,
    ) -> Result<()> {
        lookups.retain(|lookup| !lookup.is_done());
        if lookups.is_empty() {
            return Ok(());
        }

        let GroupingResult {
            mut groups,
            unknowns,
        } = self.group_by_leader(lookups);

        if !unknowns.is_empty() {
            let table_paths_refs: HashSet<&TablePath> =
                groups.unknown_leader_tables.iter().collect();
            let partition_ids: Vec<PartitionId> = groups
                .unknown_leader_partition_ids
                .iter()
                .copied()
                .collect();
            let mut deleted_partitions = HashSet::new();
            if let Err(e) = self
                .metadata
                .update_tables_metadata(&table_paths_refs, &HashSet::new(), partition_ids.clone())
                .await
            {
                if e.api_error() == Some(FlussError::PartitionNotExists) {
                    deleted_partitions = self
                        .deleted_partitions(&table_paths_refs, &partition_ids)
                        .await;
                }
                warn!("Failed to update metadata for unknown leader tables: {}", e);
            } else {
                debug!(
                    "Updated metadata due to unknown leader tables during lookup: {:?}",
                    groups.unknown_leader_tables
                );
            }

            // Re-group with fresh cluster state; dispatch what resolved, re-enqueue the rest.
            let retry = self.group_by_leader(unknowns);
            groups.merge_batches(retry.groups);
            // Like the Java client: a lookup whose partition the refresh confirmed deleted fails with
            // PartitionNotExists, so that a lookuper can route it by partition name again, instead of
            // being queued forever with the bucket of a partition that is gone.
            for mut lookup in retry.unknowns {
                match lookup.table_bucket().partition_id() {
                    Some(partition_id) if deleted_partitions.contains(&partition_id) => {
                        lookup.complete_with_error(Error::partition_not_exist(format!(
                            "Partition {partition_id} of table {} no longer exists",
                            lookup.table_path()
                        )));
                    }
                    _ => self.re_enqueue_lookup(lookup),
                }
            }

            // Nothing to dispatch even after refresh — back off to avoid a tight RPC loop.
            if groups.is_empty() {
                let mut cluster_rx = self.metadata.subscribe_cluster_changes();
                tokio::select! {
                    _ = cluster_rx.changed() => {}
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                }
                return Ok(());
            }
        }

        for (server, batches) in groups.primary {
            self.send_requests::<Primary>(server, batches, requests)
                .await;
        }
        for (server, batches) in groups.prefix {
            self.send_requests::<Prefix>(server, batches, requests)
                .await;
        }

        Ok(())
    }

    /// Which of `partition_ids` the cluster says no longer exist. Asked one at a time, because a
    /// refresh naming a deleted partition fails as a whole without saying which one it was.
    async fn deleted_partitions(
        &self,
        table_paths: &HashSet<&TablePath>,
        partition_ids: &[PartitionId],
    ) -> HashSet<PartitionId> {
        let mut deleted = HashSet::new();
        for &partition_id in partition_ids {
            if let Err(e) = self
                .metadata
                .update_tables_metadata(table_paths, &HashSet::new(), vec![partition_id])
                .await
            {
                if e.api_error() == Some(FlussError::PartitionNotExists) {
                    deleted.insert(partition_id);
                }
            }
        }
        deleted
    }

    fn group_by_leader(&self, lookups: Vec<QueuedLookup>) -> GroupingResult {
        let cluster = self.metadata.get_cluster();
        let mut primary: PrimaryBatches = HashMap::new();
        let mut prefix: PrefixBatches = HashMap::new();
        let mut unknown_leader_tables: HashSet<TablePath> = HashSet::new();
        let mut unknown_leader_partition_ids: HashSet<PartitionId> = HashSet::new();
        let mut unknowns: Vec<QueuedLookup> = Vec::new();

        for query in lookups {
            let table_bucket = query.table_bucket().clone();
            let batch_key = (
                table_bucket.clone(),
                query.original_partition_name().cloned(),
            );

            let leader = match cluster.leader_for(&table_bucket) {
                Some(leader) => leader.id(),
                None => {
                    warn!(
                        "No leader found for table bucket {} during lookup",
                        table_bucket
                    );
                    unknown_leader_tables.insert(query.table_path().clone());
                    if let Some(partition_id) = table_bucket.partition_id() {
                        unknown_leader_partition_ids.insert(partition_id);
                    }
                    unknowns.push(query);
                    continue;
                }
            };

            match query {
                QueuedLookup::Primary(q) => {
                    primary
                        .entry(leader)
                        .or_default()
                        .entry(batch_key.clone())
                        .or_insert_with(|| LookupBatch::new(batch_key))
                        .add_lookup(q);
                }
                QueuedLookup::Prefix(q) => {
                    prefix
                        .entry(leader)
                        .or_default()
                        .entry(batch_key.clone())
                        .or_insert_with(|| LookupBatch::new(batch_key))
                        .add_lookup(q);
                }
            }
        }

        GroupingResult {
            groups: GroupByLeaderResult {
                primary,
                prefix,
                unknown_leader_tables,
                unknown_leader_partition_ids,
            },
            unknowns,
        }
    }

    /// Sends `destination` one request per table, each on a task of its own in `requests`, so a
    /// server that is slow to answer holds up only the lookups sent to it. Returns once every
    /// request has started; while all in-flight permits are taken, it waits for one.
    ///
    /// Like the Java client, normal and historical lookups go in separate requests: the server takes
    /// a request as historical when its first bucket names an original partition.
    async fn send_requests<P: LookupProtocol>(
        self: &Arc<Self>,
        destination: ServerId,
        batches_by_bucket: HashMap<BatchKey, LookupBatch<P::Value>>,
        requests: &mut JoinSet<()>,
    ) where
        LookupQuery<P::Value>: Into<QueuedLookup>,
    {
        for (table_id, batches) in group_by_table(batches_by_bucket) {
            let (historical, normal): (Vec<_>, Vec<_>) =
                batches.into_iter().partition(|b| b.is_historical());
            for mut batches in [normal, historical] {
                if batches.is_empty() {
                    continue;
                }
                let permit = match self.acquire_inflight_permit(&mut batches).await {
                    Some(p) => p,
                    None => continue,
                };
                let dispatcher = Arc::clone(self);
                requests.spawn(async move {
                    dispatcher
                        .send_table_lookup::<P>(table_id, destination, batches)
                        .await;
                    drop(permit);
                });
            }
        }
    }

    async fn connect_or_fail<T>(
        &self,
        destination: ServerId,
        batches: &mut [LookupBatch<T>],
    ) -> Option<ServerConnection>
    where
        LookupQuery<T>: Into<QueuedLookup>,
    {
        let cluster = self.metadata.get_cluster();
        let tablet_server = match cluster.get_tablet_server(destination) {
            Some(server) => server.clone(),
            None => {
                let error = Error::UnexpectedError {
                    message: format!("Server {destination} is not found in metadata cache"),
                    source: None,
                };
                self.fail_all_batches(&error, true, batches);
                return None;
            }
        };

        match self.metadata.get_connection(&tablet_server).await {
            Ok(conn) => Some(conn),
            Err(e) => {
                let error = with_context(
                    e,
                    format!("Failed to get connection to server {destination}"),
                );
                self.fail_all_batches(&error, true, batches);
                None
            }
        }
    }

    fn fail_all_batches<T>(&self, error: &Error, is_retriable: bool, batches: &mut [LookupBatch<T>])
    where
        LookupQuery<T>: Into<QueuedLookup>,
    {
        for batch in batches {
            self.handle_batch_error(error, is_retriable, batch);
        }
    }

    async fn send_table_lookup<P: LookupProtocol>(
        &self,
        table_id: TableId,
        destination: ServerId,
        mut batches: Vec<LookupBatch<P::Value>>,
    ) where
        LookupQuery<P::Value>: Into<QueuedLookup>,
    {
        let connection = match self.connect_or_fail(destination, &mut batches).await {
            Some(conn) => conn,
            None => return,
        };
        let keys_by_bucket: Vec<_> = batches.iter_mut().map(|b| b.take_keys()).collect();
        let request = P::build_request(table_id, keys_by_bucket);

        match connection.request(request).await {
            Ok(response) => {
                self.handle_response::<P>(table_id, destination, response, &mut batches);
            }
            Err(e) => {
                let is_retriable = e.is_retriable();
                let error = with_context(e, format!("{} request failed", P::OP_NAME));
                for batch in &mut batches {
                    self.handle_batch_error(&error, is_retriable, batch);
                }
            }
        }
    }

    async fn acquire_inflight_permit<T>(
        &self,
        batches: &mut [LookupBatch<T>],
    ) -> Option<OwnedSemaphorePermit> {
        match self.inflight_semaphore.clone().acquire_owned().await {
            Ok(p) => Some(p),
            Err(_) => {
                error!("Semaphore closed during lookup");
                for batch in batches.iter_mut() {
                    batch.complete_all_with_error("Lookup sender shutdown");
                }
                None
            }
        }
    }

    fn handle_response<P: LookupProtocol>(
        &self,
        table_id: TableId,
        destination: ServerId,
        response: P::Response,
        batches: &mut [LookupBatch<P::Value>],
    ) where
        LookupQuery<P::Value>: Into<QueuedLookup>,
    {
        let bucket_to_index = build_bucket_index(batches);
        let mut processed = vec![false; batches.len()];

        for bucket_resp in P::decode_buckets(response) {
            let table_bucket = TableBucket::new_with_partition(
                table_id,
                bucket_resp.partition_id,
                bucket_resp.bucket_id,
            );
            let batch_key = (table_bucket, bucket_resp.original_partition_name);
            let Some(&idx) = bucket_to_index.get(&batch_key) else {
                error!(
                    "Received {} response for unknown bucket {} ({:?}) from server {}",
                    P::OP_NAME,
                    batch_key.0,
                    batch_key.1,
                    destination
                );
                continue;
            };
            let table_bucket = batch_key.0;
            processed[idx] = true;
            let batch = &mut batches[idx];

            if let Some(api_error) = bucket_error(bucket_resp.error_code, bucket_resp.error_message)
            {
                if FlussError::for_code(api_error.code).invalidates_metadata() {
                    self.invalidate_bucket_metadata(&table_bucket);
                }
                let is_retriable = api_error.is_retriable();
                self.handle_batch_error(&Error::FlussAPIError { api_error }, is_retriable, batch);
                continue;
            }

            batch.complete(bucket_resp.values);
        }

        self.fail_unprocessed_batches(&processed, batches, destination, P::OP_NAME);
    }

    /// Drops the cached leaders of `table_bucket`'s table or partition, so the next lookup
    /// for it waits for a metadata refresh.
    fn invalidate_bucket_metadata(&self, table_bucket: &TableBucket) {
        let cluster = self.metadata.get_cluster();
        let Some(table_path) = cluster.get_table_path_by_id(table_bucket.table_id()) else {
            return;
        };
        let table_path = Arc::new(table_path.clone());
        let physical_table_path = match table_bucket.partition_id() {
            Some(partition_id) => match cluster.get_partition_name(partition_id) {
                Some(partition_name) => {
                    PhysicalTablePath::of_partitioned(table_path, Some(partition_name.clone()))
                }
                None => return,
            },
            None => PhysicalTablePath::of(table_path),
        };
        self.metadata
            .invalidate_physical_table_meta(&HashSet::from([physical_table_path]));
    }

    fn fail_unprocessed_batches<T>(
        &self,
        processed: &[bool],
        batches: &mut [LookupBatch<T>],
        destination: ServerId,
        op_name: &'static str,
    ) where
        LookupQuery<T>: Into<QueuedLookup>,
    {
        for (idx, was_processed) in processed.iter().enumerate() {
            if !was_processed {
                let batch = &mut batches[idx];
                let error = Error::UnexpectedError {
                    message: format!(
                        "Bucket {} {} response missing from server {}",
                        batch.table_bucket.bucket_id(),
                        op_name,
                        destination
                    ),
                    source: None,
                };
                self.handle_batch_error(&error, true, batch);
            }
        }
    }

    fn handle_batch_error<T>(&self, error: &Error, is_retriable: bool, batch: &mut LookupBatch<T>)
    where
        LookupQuery<T>: Into<QueuedLookup>,
    {
        let mut retried = 0usize;
        let mut failed = 0usize;
        let table_bucket = batch.table_bucket.clone();
        let throttled = error.api_error() == Some(FlussError::HistoricalPartitionThrottled);

        for mut lookup in batch.lookups.drain(..) {
            if is_retriable && lookup.retries() < self.max_retries && !lookup.is_done() {
                // A server throttling historical lookups is told apart from a stale route: the
                // retry waits, so the lake reads it is shedding are not sent straight back.
                let delay = throttled.then(|| historical_throttle_backoff(lookup.retries()));
                lookup.increment_retries();
                match delay {
                    Some(delay) => self.re_enqueue_lookup_after(lookup.into(), delay),
                    None => self.re_enqueue_lookup(lookup.into()),
                }
                retried += 1;
            } else {
                lookup.complete_with_error(copy_error(error));
                failed += 1;
            }
        }

        if retried > 0 && !throttled {
            warn!(
                "Lookup error for bucket {}, retrying {} lookups: {}",
                table_bucket, retried, error
            );
        }
        if failed > 0 {
            warn!(
                "Lookup failed for bucket {} ({} lookups): {}",
                table_bucket, failed, error
            );
        }
    }

    fn re_enqueue_lookup(&self, lookup: QueuedLookup) {
        if let Err(e) = self.re_enqueue_tx.send(lookup) {
            error!("Failed to re-enqueue lookup: {}", e);
            let mut failed_lookup = e.0;
            failed_lookup.complete_with_error(Error::UnexpectedError {
                message: "Failed to re-enqueue lookup: channel closed".to_string(),
                source: None,
            });
        }
    }

    fn re_enqueue_lookup_after(&self, lookup: QueuedLookup, delay: Duration) {
        let re_enqueue_tx = self.re_enqueue_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            if let Err(e) = re_enqueue_tx.send(lookup) {
                let mut failed_lookup = e.0;
                failed_lookup.complete_with_error(Error::UnexpectedError {
                    message: "Failed to re-enqueue lookup: channel closed".to_string(),
                    source: None,
                });
            }
        });
    }
}

fn group_by_table<T>(
    batches_by_bucket: HashMap<BatchKey, LookupBatch<T>>,
) -> HashMap<TableId, Vec<LookupBatch<T>>> {
    let mut out: HashMap<TableId, Vec<LookupBatch<T>>> = HashMap::new();
    for ((table_bucket, _), batch) in batches_by_bucket {
        out.entry(table_bucket.table_id()).or_default().push(batch);
    }
    out
}

fn build_bucket_index<T>(batches: &[LookupBatch<T>]) -> HashMap<BatchKey, usize> {
    batches
        .iter()
        .enumerate()
        .map(|(idx, batch)| (batch.key(), idx))
        .collect()
}

/// The Java client's backoff for throttled historical lookups: 100 ms doubling up to 5 s, with
/// 20% jitter.
fn historical_throttle_backoff(retries: i32) -> Duration {
    const INITIAL_MS: f64 = 100.0;
    const MAX_MS: f64 = 5000.0;
    let term = INITIAL_MS * 2f64.powi(retries.clamp(0, 16));
    let jitter = rand::random_range(0.8..1.2);
    Duration::from_millis((term * jitter).min(MAX_MS) as u64)
}

fn bucket_error(error_code: Option<i32>, error_message: Option<String>) -> Option<ApiError> {
    let code = error_code?;
    let fluss_error = FlussError::for_code(code);
    if fluss_error == FlussError::None {
        return None;
    }
    let message = error_message.unwrap_or_else(|| fluss_error.message().to_string());
    Some(ApiError { code, message })
}

/// Leaves API errors as they are, so callers keep the error code.
fn with_context(error: Error, context: String) -> Error {
    match error {
        Error::FlussAPIError { .. } => error,
        other => Error::UnexpectedError {
            message: format!("{context}: {other}"),
            source: None,
        },
    }
}

fn copy_error(error: &Error) -> Error {
    match error {
        Error::FlussAPIError { api_error } => Error::FlussAPIError {
            api_error: api_error.clone(),
        },
        Error::UnexpectedError { message, .. } => Error::UnexpectedError {
            message: message.clone(),
            source: None,
        },
        other => Error::UnexpectedError {
            message: other.to_string(),
            source: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::lookup::{LookupQueue, PrimaryLookupQuery};
    use crate::cluster::ServerType;
    use crate::proto::{ApiVersionsResponse, PbApiVersion, PbLookupRespForBucket, PbValue};
    use crate::test_utils::build_cluster_arc_with_port;
    use prost::Message;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::oneshot;

    const TABLE_ID: TableId = 1;

    type PrimaryResult = Result<Option<Vec<u8>>>;

    fn table_path() -> TablePath {
        TablePath::new("db", "tbl")
    }

    /// A sender whose only tablet server listens on `port`, with the channels that feed and
    /// stop it.
    fn sender_on_port(
        port: u16,
        max_retries: i32,
    ) -> (
        LookupSender,
        mpsc::Sender<QueuedLookup>,
        watch::Sender<bool>,
    ) {
        let cluster = build_cluster_arc_with_port(&table_path(), TABLE_ID, 1, u32::from(port));
        let metadata = Arc::new(Metadata::new_for_test(cluster));
        let (queue, lookup_tx, re_enqueue_tx) =
            LookupQueue::new(8, 8, 100, metadata.subscribe_cluster_changes());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let sender = LookupSender::new(metadata, queue, re_enqueue_tx, 1, max_retries, shutdown_rx);
        (sender, lookup_tx, shutdown_tx)
    }

    fn sender(max_retries: i32) -> LookupSender {
        sender_on_port(9092, max_retries).0
    }

    fn lookup(bucket_id: BucketId) -> (PrimaryLookupQuery, oneshot::Receiver<PrimaryResult>) {
        let (result_tx, result_rx) = oneshot::channel();
        let query = LookupQuery::new(
            table_path(),
            TableBucket::new(TABLE_ID, bucket_id),
            1,
            Bytes::from_static(b"key"),
            result_tx,
        );
        (query, result_rx)
    }

    fn fail_with_bucket_error(sender: &LookupSender, error_code: i32) -> Error {
        let table_bucket = TableBucket::new(TABLE_ID, 0);
        let (result_tx, mut result_rx) = oneshot::channel();
        let mut batch = LookupBatch::new((table_bucket.clone(), None));
        batch.add_lookup(LookupQuery::new(
            table_path(),
            table_bucket,
            1,
            Bytes::from_static(b"key"),
            result_tx,
        ));
        let response = LookupResponse {
            buckets_resp: vec![PbLookupRespForBucket {
                bucket_id: 0,
                error_code: Some(error_code),
                error_message: Some("from the server".to_string()),
                ..Default::default()
            }],
        };
        sender.dispatcher.handle_response::<Primary>(
            TABLE_ID,
            1,
            response,
            std::slice::from_mut(&mut batch),
        );
        result_rx
            .try_recv()
            .expect("lookup completed")
            .expect_err("lookup failed")
    }

    #[test]
    fn a_bucket_error_fails_its_lookups_with_the_api_error() {
        let error = fail_with_bucket_error(&sender(3), FlussError::InvalidBucketRouting.code());
        assert_eq!(error.api_error(), Some(FlussError::InvalidBucketRouting));
        assert!(error.to_string().contains("from the server"), "{error}");
    }

    #[test]
    fn a_retriable_bucket_error_keeps_its_code_once_retries_run_out() {
        let error = fail_with_bucket_error(&sender(0), FlussError::NotLeaderOrFollower.code());
        assert_eq!(error.api_error(), Some(FlussError::NotLeaderOrFollower));
    }

    #[test]
    fn a_routing_error_drops_the_cached_leaders() {
        let sender = sender(3);
        let table_bucket = TableBucket::new(TABLE_ID, 0);
        assert!(
            sender
                .dispatcher
                .metadata
                .get_cluster()
                .leader_for(&table_bucket)
                .is_some()
        );

        fail_with_bucket_error(&sender, FlussError::InvalidBucketRouting.code());

        assert!(
            sender
                .dispatcher
                .metadata
                .get_cluster()
                .leader_for(&table_bucket)
                .is_none()
        );
    }

    #[test]
    fn a_lookup_whose_caller_stopped_waiting_is_not_retried() {
        let mut sender = sender(3);
        let (query, result_rx) = lookup(0);
        drop(result_rx);
        let mut batch = LookupBatch::new((query.table_bucket().clone(), None));
        batch.add_lookup(query);
        let error = Error::UnexpectedError {
            message: "connection reset".to_string(),
            source: None,
        };

        sender
            .dispatcher
            .handle_batch_error(&error, true, &mut batch);

        assert!(sender.queue.drain_all().is_empty());
    }

    #[tokio::test]
    async fn a_lookup_whose_caller_stopped_waiting_is_not_sent() {
        let mut sender = sender(3);
        // The test cluster has no bucket 1, so this lookup would wait for a leader.
        let (query, result_rx) = lookup(1);
        drop(result_rx);

        sender
            .dispatcher
            .send_lookups(vec![query.into()], &mut sender.requests)
            .await
            .unwrap();

        assert!(sender.queue.drain_all().is_empty());
    }

    #[tokio::test]
    async fn sending_does_not_wait_for_the_server_to_answer() {
        // Accepts connections and never answers.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (mut sender, _lookup_tx, _shutdown_tx) =
            sender_on_port(listener.local_addr().unwrap().port(), 3);
        let (query, mut result_rx) = lookup(0);

        tokio::time::timeout(
            Duration::from_secs(10),
            sender
                .dispatcher
                .send_lookups(vec![query.into()], &mut sender.requests),
        )
        .await
        .expect("sent without waiting for the server")
        .unwrap();

        let _connection = listener.accept().await.unwrap();
        assert!(matches!(
            result_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(sender.dispatcher.inflight_semaphore.available_permits(), 0);
    }

    #[tokio::test]
    async fn closing_waits_for_the_requests_in_flight() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (mut sender, lookup_tx, shutdown_tx) =
            sender_on_port(listener.local_addr().unwrap().port(), 0);
        let (query, result_rx) = lookup(0);
        lookup_tx.send(query.into()).await.unwrap();
        let run = tokio::spawn(async move { sender.run().await });
        let (connection, _) = listener.accept().await.unwrap();

        shutdown_tx.send(true).unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!run.is_finished());

        // The server goes away, so the request fails and returns its permit.
        drop(connection);
        tokio::time::timeout(Duration::from_secs(10), run)
            .await
            .expect("closed once the request failed")
            .unwrap();
        assert!(result_rx.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn dropping_the_sender_aborts_its_requests_in_flight() {
        // Accepts connections and never answers.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (mut sender, _lookup_tx, _shutdown_tx) =
            sender_on_port(listener.local_addr().unwrap().port(), 3);
        let (query, _result_rx) = lookup(0);
        tokio::time::timeout(
            Duration::from_secs(10),
            sender
                .dispatcher
                .send_lookups(vec![query.into()], &mut sender.requests),
        )
        .await
        .expect("sent without waiting for the server")
        .unwrap();
        let (mut connection, _) = listener.accept().await.unwrap();

        drop(sender);

        let mut received = Vec::new();
        tokio::time::timeout(
            Duration::from_secs(10),
            connection.read_to_end(&mut received),
        )
        .await
        .expect("the client closed its connection")
        .unwrap();
    }

    /// Answers the connection handshake, then returns the request id of the next lookup.
    async fn read_lookup_request(stream: &mut TcpStream) -> i32 {
        loop {
            let len = stream.read_i32().await.expect("request length");
            let mut payload = vec![0u8; len as usize];
            stream.read_exact(&mut payload).await.expect("request");
            let api_key = i16::from_be_bytes(payload[..2].try_into().unwrap());
            let request_id = i32::from_be_bytes(payload[4..8].try_into().unwrap());
            if api_key == 1017 {
                return request_id;
            }
            assert_eq!(api_key, 1000, "expected ApiVersions or Lookup");
            let versions = ApiVersionsResponse {
                api_versions: [1000, 1017]
                    .map(|api_key| PbApiVersion {
                        api_key,
                        min_version: 0,
                        max_version: 0,
                    })
                    .to_vec(),
                server_type: Some(ServerType::TabletServer.to_type_id()),
            };
            respond(stream, request_id, versions).await;
        }
    }

    async fn respond(stream: &mut TcpStream, request_id: i32, response: impl Message) {
        let body = response.encode_to_vec();
        stream
            .write_i32((5 + body.len()) as i32)
            .await
            .expect("response length");
        stream.write_u8(0).await.expect("success response type");
        stream.write_i32(request_id).await.expect("request id");
        stream.write_all(&body).await.expect("response body");
    }

    fn found(value: &[u8]) -> LookupResponse {
        LookupResponse {
            buckets_resp: vec![PbLookupRespForBucket {
                bucket_id: 0,
                values: vec![PbValue {
                    values: Some(value.to_vec()),
                }],
                ..Default::default()
            }],
        }
    }

    fn submit(
        lookup_tx: &mpsc::Sender<QueuedLookup>,
    ) -> oneshot::Receiver<Result<Option<Vec<u8>>>> {
        let (result_tx, result_rx) = oneshot::channel();
        let lookup = LookupQuery::new(
            table_path(),
            TableBucket::new(TABLE_ID, 0),
            1,
            Bytes::from_static(b"key"),
            result_tx,
        );
        lookup_tx.try_send(lookup.into()).expect("queued");
        result_rx
    }

    /// Like `read_lookup_request`, and also decodes the request.
    async fn read_lookup(stream: &mut TcpStream) -> (i32, crate::proto::LookupRequest) {
        loop {
            let len = stream.read_i32().await.expect("request length");
            let mut payload = vec![0u8; len as usize];
            stream.read_exact(&mut payload).await.expect("request");
            let api_key = i16::from_be_bytes(payload[..2].try_into().unwrap());
            let request_id = i32::from_be_bytes(payload[4..8].try_into().unwrap());
            if api_key == 1017 {
                let request =
                    crate::proto::LookupRequest::decode(&payload[8..]).expect("lookup request");
                return (request_id, request);
            }
            let versions = ApiVersionsResponse {
                api_versions: [1000, 1017]
                    .map(|api_key| PbApiVersion {
                        api_key,
                        min_version: 0,
                        max_version: 0,
                    })
                    .to_vec(),
                server_type: Some(ServerType::TabletServer.to_type_id()),
            };
            respond(stream, request_id, versions).await;
        }
    }

    /// The server takes a request as historical when its first bucket names an original
    /// partition, so a round holding both kinds sends two requests; within the historical one,
    /// expired partitions sharing a bucket are answered apart by name.
    #[tokio::test]
    async fn historical_lookups_go_in_their_own_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("address").port();
        let cluster = build_cluster_arc_with_port(&table_path(), TABLE_ID, 1, port as u32);
        let metadata = Arc::new(Metadata::new_for_test(cluster));
        let (queue, lookup_tx, re_enqueue_tx) =
            LookupQueue::new(8, 8, 200, metadata.subscribe_cluster_changes());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut sender = LookupSender::new(metadata, queue, re_enqueue_tx, 4, 3, shutdown_rx);
        let running = tokio::spawn(async move { sender.run().await });

        let submit_named = |key: &'static [u8], partition: Option<&str>| {
            let (result_tx, result_rx) = oneshot::channel();
            let lookup = LookupQuery::<Option<Vec<u8>>>::new(
                table_path(),
                TableBucket::new(TABLE_ID, 0),
                1,
                Bytes::from_static(key),
                result_tx,
            )
            .with_original_partition_name(partition.map(str::to_string));
            lookup_tx.try_send(lookup.into()).expect("queued");
            result_rx
        };
        // Connect first, so that the two requests below share the connection rather than race to
        // open one each, which this one-connection server would not see.
        let warm_up = submit_named(b"warm-up", None);
        let (mut stream, _) = listener.accept().await.expect("accept");
        let (warm_up_id, _) = read_lookup(&mut stream).await;
        respond(&mut stream, warm_up_id, found(b"warm-up row")).await;
        assert_eq!(
            warm_up.await.unwrap().unwrap(),
            Some(b"warm-up row".to_vec())
        );

        let live = submit_named(b"live", None);
        let older = submit_named(b"older", Some("2026101510"));
        let newer = submit_named(b"newer", Some("2026101611"));

        let mut requests = [
            read_lookup(&mut stream).await,
            read_lookup(&mut stream).await,
        ];
        requests.sort_by_key(|(_, r)| r.buckets_req[0].original_partition_name.is_some());
        let (live_id, live_request) = &requests[0];
        let (historical_id, historical_request) = &requests[1];
        assert_eq!(live_request.buckets_req.len(), 1);
        assert_eq!(live_request.buckets_req[0].original_partition_name, None);
        let mut names: Vec<_> = historical_request
            .buckets_req
            .iter()
            .map(|b| b.original_partition_name.clone().expect("historical"))
            .collect();
        names.sort();
        assert_eq!(names, ["2026101510", "2026101611"]);

        respond(&mut stream, *live_id, found(b"live row")).await;
        let historical_response = LookupResponse {
            buckets_resp: historical_request
                .buckets_req
                .iter()
                .map(|b| PbLookupRespForBucket {
                    bucket_id: 0,
                    original_partition_name: b.original_partition_name.clone(),
                    values: vec![PbValue {
                        values: b.original_partition_name.clone().map(String::into_bytes),
                    }],
                    ..Default::default()
                })
                .collect(),
        };
        respond(&mut stream, *historical_id, historical_response).await;

        assert_eq!(live.await.unwrap().unwrap(), Some(b"live row".to_vec()));
        assert_eq!(older.await.unwrap().unwrap(), Some(b"2026101510".to_vec()));
        assert_eq!(newer.await.unwrap().unwrap(), Some(b"2026101611".to_vec()));

        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(10), running)
            .await
            .expect("the sender stopped")
            .unwrap();
    }

    #[test]
    fn a_throttled_historical_lookup_backs_off_like_the_java_client() {
        for (retries, low, high) in [(0, 80, 120), (1, 160, 240), (3, 640, 960), (10, 5000, 5000)] {
            let delay = historical_throttle_backoff(retries).as_millis() as u64;
            assert!((low..=high).contains(&delay), "retry {retries}: {delay} ms");
        }
    }

    /// Like the Java client: a lookup that arrives while a round awaits its response goes out in a
    /// round of its own rather than waiting for the first one to be answered.
    #[tokio::test]
    async fn a_round_is_sent_while_the_previous_one_awaits_its_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("address").port();
        let cluster = build_cluster_arc_with_port(&table_path(), TABLE_ID, 1, port as u32);
        let metadata = Arc::new(Metadata::new_for_test(cluster));
        let (queue, lookup_tx, re_enqueue_tx) =
            LookupQueue::new(8, 8, 0, metadata.subscribe_cluster_changes());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut sender = LookupSender::new(metadata, queue, re_enqueue_tx, 2, 3, shutdown_rx);
        let running = tokio::spawn(async move { sender.run().await });

        let first = submit(&lookup_tx);
        let (mut stream, _) = listener.accept().await.expect("accept");
        let first_request = read_lookup_request(&mut stream).await;
        let second = submit(&lookup_tx);
        let second_request =
            tokio::time::timeout(Duration::from_secs(10), read_lookup_request(&mut stream))
                .await
                .expect("the second round waited for the first one's response");

        respond(&mut stream, second_request, found(b"second")).await;
        respond(&mut stream, first_request, found(b"first")).await;
        assert_eq!(second.await.unwrap().unwrap(), Some(b"second".to_vec()));
        assert_eq!(first.await.unwrap().unwrap(), Some(b"first".to_vec()));

        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(10), running)
            .await
            .expect("the sender stopped")
            .unwrap();
    }
}
