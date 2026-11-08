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

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

/**
 * Rows of one inner split of a bounded read that reads several in turn, such as the lake files of a
 * bucket. A read position is an inner split's index and the rows read of it, so a reader that
 * resumes counts the rows it read per inner split.
 */
@Internal
public interface IndexedRowIterator extends CloseableIterator<InternalRow> {

    /** Returns the index of the inner split the rows are from. */
    int getCurrentLakeSplitIndex();
}
