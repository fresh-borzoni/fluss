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

package org.apache.fluss.client.tiering;

import org.apache.fluss.config.ConfigOptions;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for the caps of the rounds a {@link TieringSplitGenerator} has leaders plan. */
class TieringSplitGeneratorTest {

    /** A round without a positive cap would not be planned at its leaders, so it is refused. */
    @Test
    void testNonPositiveRoundCapsAreRefused() {
        // the caps are checked before the admin is used
        for (long records : new long[] {0, -1}) {
            assertThatThrownBy(() -> new TieringSplitGenerator(null, records, 1 << 20))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(
                            ConfigOptions.LAKE_TIERING_DELETION_VECTORS_MAX_ROUND_RECORDS_PER_BUCKET
                                    .key())
                    .hasMessageContaining("must be positive, but is " + records);
        }
        assertThatThrownBy(() -> new TieringSplitGenerator(null, 100, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        ConfigOptions.LAKE_TIERING_DELETION_VECTORS_MAX_ROUND_BYTES_PER_BUCKET
                                .key());
    }
}
