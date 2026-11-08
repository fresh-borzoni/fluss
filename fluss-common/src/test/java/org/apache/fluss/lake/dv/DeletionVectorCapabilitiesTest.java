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

package org.apache.fluss.lake.dv;

import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.SupportsDataFiles;
import org.apache.fluss.lake.source.SupportsRewritePlanning;
import org.apache.fluss.lake.source.TestingLakeSource;
import org.apache.fluss.lake.source.TestingLakeSplit;
import org.apache.fluss.metadata.TablePath;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/** Tests for {@link DeletionVectorCapabilities}. */
class DeletionVectorCapabilitiesTest {

    private static final TablePath TABLE_PATH = TablePath.of("db", "dv_table");

    @Test
    void testAnObjectWithTheCapabilityIsReturnedAsIt() {
        TestingLakeSource source = new TestingLakeSource();
        SupportsRewritePlanning<LakeSplit> planning =
                DeletionVectorCapabilities.rewritePlanning(source, TABLE_PATH);
        assertThat(planning).isSameAs(source);
        TestingLakeSplit split = new TestingLakeSplit(0, Collections.emptyList());
        SupportsDataFiles dataFiles = DeletionVectorCapabilities.dataFiles(split);
        assertThat(dataFiles).isSameAs(split);
    }

    @Test
    void testAnObjectWithoutTheCapabilityIsRefusedWithTheTableAndTheOption() {
        @SuppressWarnings("unchecked")
        LakeSource<LakeSplit> plain = mock(LakeSource.class);
        assertThatThrownBy(() -> DeletionVectorCapabilities.rewritePlanning(plain, TABLE_PATH))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("lake source " + plain.getClass().getName())
                .hasMessageContaining(SupportsRewritePlanning.class.getName())
                .hasMessageContaining("'table.deletion-vectors.enabled'")
                .hasMessageContaining(TABLE_PATH.toString());
    }

    @Test
    void testAMissingObjectIsRefusedWithTheTable() {
        assertThatThrownBy(() -> DeletionVectorCapabilities.rewritePlanning(null, TABLE_PATH))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("No lake source for table " + TABLE_PATH + ".");
    }
}
