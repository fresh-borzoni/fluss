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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests of a version of a bucket's index ({@link DvVersion}). */
class DvVersionTest {

    @TempDir Path tempDir;

    /**
     * A version released for good, also by a release too many, is never retained again: a reader
     * would read through a released RocksDB snapshot.
     */
    @Test
    void testAReleasedVersionIsNeverRetainedAgain() throws Exception {
        DvRocksDB db = DvRocksDB.open(tempDir.resolve("dv").toString());
        try {
            DvDbLeases leases = new DvDbLeases(db);
            DvVersion v = DvVersion.open(db, leases, 100, 10, null, 1);
            assertThat(v.tryRetain()).isTrue();
            v.release();
            v.release();
            assertThat(v.tryRetain()).isFalse();
            assertThatThrownBy(v::release).isInstanceOf(IllegalStateException.class);
            assertThat(v.tryRetain()).isFalse();
            assertThat(leases.held()).isZero();
        } finally {
            db.close();
        }
    }
}
