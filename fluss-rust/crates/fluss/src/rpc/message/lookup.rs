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

use crate::proto::LookupResponse;
use crate::rpc::frame::ReadError;

use crate::rpc::api_key::ApiKey;
use crate::rpc::frame::WriteError;
use crate::rpc::message::{ReadType, RequestBody, WriteType};
use crate::{BucketId, PartitionId, TableId, impl_read_type, impl_write_type, proto};
use bytes::Bytes;
use prost::Message;

use bytes::{Buf, BufMut};

/// The keys to look up in one bucket, with what the server needs to route them.
pub struct BucketLookupKeys {
    pub bucket_id: BucketId,
    pub partition_id: Option<PartitionId>,
    /// The bucket count `bucket_id` was computed with.
    pub routing_bucket_count: Option<i32>,
    /// Set for a historical lookup: the expired partition asked for. `partition_id` is then the
    /// historical system partition. Prefix lookups do not support it.
    pub original_partition_name: Option<String>,
    pub keys: Vec<Bytes>,
}

pub struct LookupRequest {
    pub(crate) inner_request: proto::LookupRequest,
}

impl LookupRequest {
    pub fn new_batched(table_id: TableId, buckets: Vec<BucketLookupKeys>) -> Self {
        let buckets_req: Vec<proto::PbLookupReqForBucket> = buckets
            .into_iter()
            .map(|bucket| proto::PbLookupReqForBucket {
                partition_id: bucket.partition_id,
                bucket_id: bucket.bucket_id,
                keys: bucket.keys,
                original_partition_name: bucket.original_partition_name,
                routing_bucket_count: bucket.routing_bucket_count,
            })
            .collect();

        let request = proto::LookupRequest {
            table_id,
            buckets_req,
            insert_if_not_exists: None,
            acks: None,
            timeout_ms: None,
        };

        Self {
            inner_request: request,
        }
    }
}

impl RequestBody for LookupRequest {
    type ResponseBody = LookupResponse;

    const API_KEY: ApiKey = ApiKey::Lookup;
}

impl_write_type!(LookupRequest);
impl_read_type!(LookupResponse);
