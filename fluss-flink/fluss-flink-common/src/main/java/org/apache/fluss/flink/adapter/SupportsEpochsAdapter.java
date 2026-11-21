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

package org.apache.fluss.flink.adapter;

/**
 * Version-neutral adapter for Flink's ability of a scan to cut its changes into epochs.
 *
 * <p>The implementation of this interface for Flink versions that have the ability extends {@code
 * SupportsEpochs}; other Flink versions use this interface only as an internal connector contract.
 */
public interface SupportsEpochsAdapter {

    /**
     * A scan cuts its changes into the epochs of the given domain; a lookup source reads as of
     * them. Returns false if the source cannot.
     *
     * @param domain identifies the epochs of one job; sources and lookups that apply the same
     *     domain count the same epochs
     */
    boolean applyEpochs(String domain);
}
