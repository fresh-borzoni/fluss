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

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FormatVersions}. */
class FormatVersionsTest {

    @Test
    void testVersionedTextRoundTrip() {
        String text = FormatVersions.withVersion(1, "-1\n7\n");

        assertThat(text).isEqualTo("v1\n-1\n7\n");
        assertThat(FormatVersions.withoutVersion("f", text, 1)).isEqualTo("-1\n7\n");
        assertThat(FormatVersions.withoutVersion("f", FormatVersions.withVersion(1, ""), 1))
                .isEmpty();
        // a reader of a later version reads it too
        assertThat(FormatVersions.withoutVersion("f", text, 2)).isEqualTo("-1\n7\n");
    }

    @Test
    void testNewerTextVersionIsRejected() {
        assertThatThrownBy(
                        () ->
                                FormatVersions.withoutVersion(
                                        "rowPos/_next_file_id",
                                        FormatVersions.withVersion(2, "12"),
                                        1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Cannot read rowPos/_next_file_id: its format version 2 is unknown;"
                                + " this Fluss version reads format versions up to 1.");
    }

    @Test
    void testTextWithoutVersionIsRejected() {
        assertThatThrownBy(() -> FormatVersions.withoutVersion("rowPos/_next_file_id", "12", 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Cannot read rowPos/_next_file_id: it has no format version;"
                                + " this Fluss version reads format versions up to 1.");
        assertThatThrownBy(() -> FormatVersions.withoutVersion("f", "v\n12", 1))
                .hasMessageContaining("it has no format version");
        assertThatThrownBy(() -> FormatVersions.withoutVersion("f", "", 1))
                .hasMessageContaining("it has no format version");
    }

    @Test
    void testUnknownOlderVersionIsRejected() {
        assertThatThrownBy(() -> FormatVersions.check("f", 0, 1))
                .hasMessageContaining("its format version 0 is unknown");
    }

    @Test
    void testSnapshotVersionRoundTrip() {
        Map<String, String> properties = new HashMap<>();
        properties.put("fluss-dv-base", "5");
        FormatVersions.putSnapshotVersion(properties);

        assertThat(properties).containsEntry("fluss-dv-format-version", "1");
        FormatVersions.checkSnapshotVersion(6L, properties);
    }

    @Test
    void testNewerSnapshotVersionIsRejected() {
        Map<String, String> properties = new HashMap<>();
        properties.put("fluss-dv-base", "5");
        properties.put(FormatVersions.SNAPSHOT_FORMAT_VERSION_PROPERTY, "2");

        assertThatThrownBy(() -> FormatVersions.checkSnapshotVersion(6L, properties))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Cannot read the deletion vector properties of lake snapshot 6: its"
                                + " format version 2 is unknown; this Fluss version reads format"
                                + " versions up to 1.");
    }

    @Test
    void testSnapshotWithoutVersionIsRejected() {
        Map<String, String> properties = new HashMap<>();
        properties.put("fluss-dv-base", "5");

        assertThatThrownBy(() -> FormatVersions.checkSnapshotVersion(6L, properties))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("lake snapshot 6")
                .hasMessageContaining("it has no fluss-dv-format-version");
        properties.put(FormatVersions.SNAPSHOT_FORMAT_VERSION_PROPERTY, "x");
        assertThatThrownBy(() -> FormatVersions.checkSnapshotVersion(6L, properties))
                .hasMessageContaining("'x' is not a number");
    }
}
