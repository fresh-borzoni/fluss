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

//! Lookup queue for buffering pending lookup operations.
//!
//! This queue buffers lookup operations and provides batched draining
//! to improve throughput by reducing network round trips.

use super::QueuedLookup;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

/// A queue that buffers pending lookup operations and provides batched draining.
///
/// The queue supports two types of entries:
/// - New lookups from client calls
/// - Re-enqueued lookups from retry logic
///
/// Re-enqueued lookups are prioritized over new lookups to ensure fair processing.
pub struct LookupQueue {
    /// Channel for receiving lookup requests
    lookup_rx: mpsc::Receiver<QueuedLookup>,
    /// Channel for receiving re-enqueued lookups
    re_enqueue_rx: mpsc::UnboundedReceiver<QueuedLookup>,
    /// Maximum batch size for draining
    max_batch_size: usize,
    /// Timeout for batch collection
    batch_timeout: Duration,
    /// Wakes `drain()` early when the cluster changes.
    cluster_rx: watch::Receiver<u64>,
}

impl LookupQueue {
    pub fn new(
        queue_size: usize,
        max_batch_size: usize,
        batch_timeout_ms: u64,
        cluster_rx: watch::Receiver<u64>,
    ) -> (
        Self,
        mpsc::Sender<QueuedLookup>,
        mpsc::UnboundedSender<QueuedLookup>,
    ) {
        let (lookup_tx, lookup_rx) = mpsc::channel(queue_size);
        let (re_enqueue_tx, re_enqueue_rx) = mpsc::unbounded_channel();

        let queue = Self {
            lookup_rx,
            re_enqueue_rx,
            max_batch_size,
            batch_timeout: Duration::from_millis(batch_timeout_ms),
            cluster_rx,
        };

        (queue, lookup_tx, re_enqueue_tx)
    }

    /// Drains a batch of lookup queries from the queue.
    ///
    /// A batch fills for up to `batch_timeout` from the start of the drain. When that window
    /// closes empty, the drain sleeps until a lookup arrives instead of returning an empty batch,
    /// so an idle client does not wake every `batch_timeout`; the batch then fills until the end
    /// of the window the lookup arrived in, as if the windows had kept running.
    pub async fn drain(&mut self) -> Vec<QueuedLookup> {
        let mut lookups = Vec::with_capacity(self.max_batch_size);
        let start = tokio::time::Instant::now();
        let mut deadline = start + self.batch_timeout;

        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                if !lookups.is_empty() {
                    break;
                }
                match self.wait_for_lookup().await {
                    Some(lookup) => lookups.push(lookup),
                    None => break,
                }
                self.take_queued(&mut lookups);
                deadline = Self::window_end(start, self.batch_timeout, tokio::time::Instant::now());
                if lookups.len() >= self.max_batch_size {
                    break;
                }
                continue;
            }

            // Re-enqueued lookups go first. A batch they fill still takes one new lookup, so
            // retries for a bucket without a leader can't hold back the other lookups.
            while lookups.len() < self.max_batch_size {
                match self.re_enqueue_rx.try_recv() {
                    Ok(lookup) => lookups.push(lookup),
                    Err(_) => break,
                }
            }

            let sleep = tokio::time::sleep(remaining);
            tokio::select! {
                biased;
                maybe = self.lookup_rx.recv() => {
                    match maybe {
                        Some(lookup) => {
                            lookups.push(lookup);
                            while lookups.len() < self.max_batch_size {
                                match self.lookup_rx.try_recv() {
                                    Ok(lookup) => lookups.push(lookup),
                                    Err(_) => break,
                                }
                            }
                        }
                        None => break,
                    }
                }
                _ = self.cluster_rx.changed() => {
                    if !lookups.is_empty() {
                        break;
                    }
                }
                // The window closed: send what it collected, or wait for a lookup (see above).
                _ = sleep => {}
            }

            if lookups.len() >= self.max_batch_size {
                break;
            }
        }

        lookups
    }

    /// Waits, without a timer, for the next lookup, re-enqueued ones first. `None` once every
    /// handle that submits lookups is gone.
    async fn wait_for_lookup(&mut self) -> Option<QueuedLookup> {
        tokio::select! {
            biased;
            Some(lookup) = self.re_enqueue_rx.recv() => Some(lookup),
            lookup = self.lookup_rx.recv() => lookup,
        }
    }

    /// Moves lookups that are already queued into `lookups`, re-enqueued ones first, up to a
    /// full batch; like `drain`, a batch the re-enqueued ones fill still takes one new lookup.
    fn take_queued(&mut self, lookups: &mut Vec<QueuedLookup>) {
        while lookups.len() < self.max_batch_size {
            match self.re_enqueue_rx.try_recv() {
                Ok(lookup) => lookups.push(lookup),
                Err(_) => break,
            }
        }
        let limit = self.max_batch_size.max(lookups.len() + 1);
        while lookups.len() < limit {
            match self.lookup_rx.try_recv() {
                Ok(lookup) => lookups.push(lookup),
                Err(_) => break,
            }
        }
    }

    /// The end of the `batch_timeout` window, counted in whole windows from `start`, that `now`
    /// falls in: now itself for a zero timeout.
    fn window_end(
        start: tokio::time::Instant,
        batch_timeout: Duration,
        now: tokio::time::Instant,
    ) -> tokio::time::Instant {
        let window = batch_timeout.as_nanos();
        if window == 0 {
            return now;
        }
        let windows = now
            .saturating_duration_since(start)
            .as_nanos()
            .div_ceil(window);
        start + Duration::from_nanos(u64::try_from(windows * window).unwrap_or(u64::MAX))
    }

    /// Drains all remaining lookups from the queue.
    pub fn drain_all(&mut self) -> Vec<QueuedLookup> {
        let mut lookups = Vec::new();

        // Drain re-enqueued lookups
        while let Ok(lookup) = self.re_enqueue_rx.try_recv() {
            lookups.push(lookup);
        }

        // Drain main queue
        while let Ok(lookup) = self.lookup_rx.try_recv() {
            lookups.push(lookup);
        }

        lookups
    }

    /// Returns true if there are undrained lookups in the queue.
    pub fn has_undrained(&self) -> bool {
        !self.lookup_rx.is_empty() || !self.re_enqueue_rx.is_empty()
    }
}

#[cfg(test)]
mod tests;
