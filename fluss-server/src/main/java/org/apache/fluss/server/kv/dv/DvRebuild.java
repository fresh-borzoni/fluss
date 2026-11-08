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

package org.apache.fluss.server.kv.dv;

/**
 * The rebuild bookkeeping of one bucket, for its {@link DvOwner}: the tablet server's {@link
 * DerivedDvRebuildLimiter} its full loads run on, and how many of them are pending. Its fields are
 * the owner's.
 */
final class DvRebuild {

    // The tablet server's rebuild slots and threads; volatile as the caller of a background
    // recovery sets them.
    volatile DerivedDvRebuildLimiter limiter;
    // Recoveries and full loads started or queued; while any is, the state is REBUILDING.
    int pending;
}
