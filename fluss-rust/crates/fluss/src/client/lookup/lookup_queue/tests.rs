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

use super::LookupQueue;
use crate::client::lookup::{PrimaryLookupQuery, QueuedLookup};
use crate::metadata::{TableBucket, TablePath};
use bytes::Bytes;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;

fn lookup() -> QueuedLookup {
    let (result_tx, _result_rx) = oneshot::channel();
    PrimaryLookupQuery::new(
        TablePath::new("db", "tbl"),
        TableBucket::new(1, 0),
        1,
        Bytes::from_static(b"key"),
        result_tx,
    )
    .into()
}

fn queue(batch_timeout_ms: u64) -> (LookupQueue, mpsc::Sender<QueuedLookup>, watch::Sender<u64>) {
    let (cluster_tx, cluster_rx) = watch::channel(0);
    let (queue, lookup_tx, _re_enqueue_tx) = LookupQueue::new(16, 8, batch_timeout_ms, cluster_rx);
    (queue, lookup_tx, cluster_tx)
}

#[tokio::test(start_paused = true)]
async fn an_idle_drain_waits_for_a_lookup_instead_of_returning_empty() {
    let (mut queue, _lookup_tx, _cluster_tx) = queue(100);
    // With the clock paused, time jumps to the next timer whenever every task waits: a drain that
    // returned at the end of its window would finish here instead of reaching the timeout.
    let drained = tokio::time::timeout(Duration::from_secs(3600), queue.drain()).await;
    assert!(
        drained.is_err(),
        "drained {} lookups",
        drained.map(|l| l.len()).unwrap_or(0)
    );
}

#[tokio::test(start_paused = true)]
async fn a_lookup_after_an_idle_window_waits_until_the_end_of_its_window() {
    let (mut queue, lookup_tx, _cluster_tx) = queue(100);
    let start = Instant::now();
    let drain = tokio::spawn(async move {
        let lookups = queue.drain().await;
        (lookups.len(), Instant::now())
    });
    tokio::time::sleep(Duration::from_millis(250)).await;
    lookup_tx.send(lookup()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    lookup_tx.send(lookup()).await.unwrap();
    let (drained, at) = drain.await.unwrap();
    assert_eq!(drained, 2);
    assert_eq!(at - start, Duration::from_millis(300));
}

#[tokio::test(start_paused = true)]
async fn a_zero_timeout_sends_what_is_queued_at_once() {
    let (mut queue, lookup_tx, _cluster_tx) = queue(0);
    for _ in 0..3 {
        lookup_tx.send(lookup()).await.unwrap();
    }
    let start = Instant::now();
    assert_eq!(queue.drain().await.len(), 3);
    assert_eq!(Instant::now(), start);
}

#[tokio::test(start_paused = true)]
async fn a_busy_queue_fills_its_batch_for_the_whole_window() {
    let (mut queue, lookup_tx, _cluster_tx) = queue(100);
    lookup_tx.send(lookup()).await.unwrap();
    let start = Instant::now();
    assert_eq!(queue.drain().await.len(), 1);
    assert_eq!(Instant::now() - start, Duration::from_millis(100));
}

fn keyed_lookup(key: &'static [u8]) -> QueuedLookup {
    let (result_tx, _result_rx) = oneshot::channel();
    PrimaryLookupQuery::new(
        TablePath::new("db", "tbl"),
        TableBucket::new(1, 0),
        1,
        Bytes::from_static(key),
        result_tx,
    )
    .into()
}

#[tokio::test]
async fn a_batch_full_of_retries_still_takes_a_new_lookup() {
    let (_cluster_tx, cluster_rx) = watch::channel(0);
    let (mut queue, lookup_tx, re_enqueue_tx) = LookupQueue::new(8, 2, 60_000, cluster_rx);
    re_enqueue_tx.send(keyed_lookup(b"retry")).unwrap();
    re_enqueue_tx.send(keyed_lookup(b"retry")).unwrap();
    lookup_tx.send(keyed_lookup(b"new")).await.unwrap();

    let lookups = tokio::time::timeout(Duration::from_secs(10), queue.drain())
        .await
        .expect("a full batch does not wait for the batch timeout");

    let keys: Vec<&[u8]> = lookups.iter().map(|lookup| lookup.key().as_ref()).collect();
    assert_eq!(keys, [b"retry".as_slice(), b"retry", b"new"]);
}
