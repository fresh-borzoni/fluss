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

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Test for {@link RowPosSstIndex}. */
class RowPosSstIndexTest {

    @Test
    void testManifestRoundTrip() {
        Map<Integer, List<RowPosSstIndex.SstFileEntry>> ssts = new HashMap<>();
        ssts.put(
                0,
                Arrays.asList(
                        new RowPosSstIndex.SstFileEntry("sst_0.sst", 12345),
                        new RowPosSstIndex.SstFileEntry("sst_1.sst", 67890)));
        ssts.put(3, Collections.singletonList(new RowPosSstIndex.SstFileEntry("sst_0.sst", 11)));
        Map<Integer, Map<Integer, String>> added = new HashMap<>();
        added.put(0, Collections.singletonMap(7, "a.parquet"));
        // a bucket whose change only removes files
        Map<Integer, List<String>> removed =
                Collections.singletonMap(5, Arrays.asList("b.parquet", "c.parquet"));

        RowPosSstIndex restored =
                RowPosSstIndex.fromJsonBytes(
                        RowPosSstIndex.manifest(ssts, added, removed).toJsonBytes(), "manifest");

        assertThat(restored.getBucketIds()).containsExactly(0, 3, 5);
        assertThat(restored.getFiles(0)).isEqualTo(ssts.get(0));
        assertThat(restored.getFiles(3)).isEqualTo(ssts.get(3));
        assertThat(restored.getFiles(5)).isEmpty();
        assertThat(restored.getFileId2Name(0)).containsOnly(entry(7, "a.parquet"));
        assertThat(restored.getFileId2Name(3)).isEmpty();
        assertThat(restored.getReplacedFiles(5)).containsExactly("b.parquet", "c.parquet");
        assertThat(restored.getReplacedFiles(0)).isEmpty();
        // a bucket the change does not touch
        assertThat(restored.getFiles(999)).isEmpty();
    }

    @Test
    void testEmptyManifest() {
        RowPosSstIndex restored =
                RowPosSstIndex.fromJsonBytes(
                        RowPosSstIndex.manifest(
                                        Collections.emptyMap(),
                                        Collections.emptyMap(),
                                        Collections.emptyMap())
                                .toJsonBytes(),
                        "manifest");

        assertThat(restored.getBucketIds()).isEmpty();
        assertThat(restored.getFiles(0)).isEmpty();
    }

    @Test
    void testManifestRecordsItsVersions() {
        String json =
                new String(
                        RowPosSstIndex.manifest(
                                        Collections.emptyMap(),
                                        Collections.emptyMap(),
                                        Collections.emptyMap())
                                .toJsonBytes(),
                        StandardCharsets.UTF_8);

        assertThat(json).contains("\"version\":1").contains("\"sstValueVersion\":1");
    }

    @Test
    void testNewerVersionIsRejected() {
        byte[] json = "{\"version\":2,\"buckets\":{}}".getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> RowPosSstIndex.fromJsonBytes(json, "rowPos/7/rowpos.manifest"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Cannot read rowPos/7/rowpos.manifest: its format version 2 is unknown;"
                                + " this Fluss version reads format versions up to 1.");
    }

    @Test
    void testNewerSstValueVersionIsRejected() {
        byte[] json =
                "{\"version\":1,\"sstValueVersion\":2,\"buckets\":{}}"
                        .getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> RowPosSstIndex.fromJsonBytes(json, "rowPos/7/rowpos.manifest"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Cannot read the SST values of rowPos/7/rowpos.manifest: its format"
                                + " version 2 is unknown; this Fluss version reads format versions"
                                + " up to 1.");
    }

    @Test
    void testManifestWithoutSstValueVersionReads() {
        // a version 1 manifest written before it recorded the version of its SST values
        byte[] json =
                ("{\"version\":1,\"buckets\":{\"0\":{"
                                + "\"rowPosSstFiles\":[{\"name\":\"sst_0.sst\",\"size\":11}],"
                                + "\"fileId2Name\":[{\"fileId\":7,\"name\":\"a.parquet\"}],"
                                + "\"replacedFiles\":[\"b.parquet\"]}}}")
                        .getBytes(StandardCharsets.UTF_8);

        RowPosSstIndex restored = RowPosSstIndex.fromJsonBytes(json, "manifest");

        assertThat(restored.getFiles(0))
                .containsExactly(new RowPosSstIndex.SstFileEntry("sst_0.sst", 11));
        assertThat(restored.getFileId2Name(0)).containsOnly(entry(7, "a.parquet"));
        assertThat(restored.getReplacedFiles(0)).containsExactly("b.parquet");
    }
}
