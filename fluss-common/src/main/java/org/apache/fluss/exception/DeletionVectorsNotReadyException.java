/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.exception;

import org.apache.fluss.annotation.PublicEvolving;

/**
 * Thrown when a bucket's deletion vector state cannot answer a request yet. The bucket is catching
 * up to or rebuilding at the requested lake snapshot, or replaying its log tail, in the background.
 * The request is retried after a backoff.
 */
@PublicEvolving
public class DeletionVectorsNotReadyException extends RetriableException {

    private static final long serialVersionUID = 1L;

    public DeletionVectorsNotReadyException(String message) {
        super(message);
    }

    public DeletionVectorsNotReadyException(String message, Throwable cause) {
        super(message, cause);
    }
}
