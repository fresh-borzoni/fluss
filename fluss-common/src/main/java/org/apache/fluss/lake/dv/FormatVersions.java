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

import org.apache.fluss.annotation.Internal;

import java.util.Map;

/**
 * Format versions of what deletion vectors keep in durable storage: the files of the position index
 * store and the Fluss properties of lake snapshots. Writers write the newest version they know.
 * Readers check the version before they parse and refuse one they do not know, so a later release
 * can change a layout and still tell the old one apart.
 */
@Internal
public final class FormatVersions {

    /**
     * Lake snapshot property: the format version of the snapshot's other deletion vector
     * properties, such as {@code fluss-dv-index-id}, {@code fluss-dv-offsets}, {@code
     * fluss-dv-base} and {@code fluss-compacted-buckets}.
     */
    public static final String SNAPSHOT_FORMAT_VERSION_PROPERTY = "fluss-dv-format-version";

    /** The format version of the deletion vector properties of lake snapshots. */
    public static final int SNAPSHOT_PROPERTIES_VERSION = 1;

    private static final char TEXT_VERSION_PREFIX = 'v';

    private FormatVersions() {}

    /**
     * Throws unless {@code version} is one this release reads: 1 to {@code newest}.
     *
     * @param source what has the version, such as a file, for the error message
     */
    public static void check(Object source, int version, int newest) {
        if (version < 1 || version > newest) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot read %s: its format version %d is unknown; this Fluss version"
                                    + " reads format versions up to %d.",
                            source, version, newest));
        }
    }

    /** Text whose first line is its format version, {@code v<version>}, followed by the body. */
    public static String withVersion(int version, String body) {
        return TEXT_VERSION_PREFIX + String.valueOf(version) + '\n' + body;
    }

    /**
     * The body of text written by {@link #withVersion}, once its version is checked against {@code
     * newest}.
     *
     * @param source where the text was read from, for the error message
     */
    public static String withoutVersion(Object source, String text, int newest) {
        int end = text.indexOf('\n');
        String header = end < 0 ? text : text.substring(0, end);
        Integer version = null;
        if (header.length() > 1 && header.charAt(0) == TEXT_VERSION_PREFIX) {
            try {
                version = Integer.parseInt(header.substring(1));
            } catch (NumberFormatException e) {
                // not a version line
            }
        }
        if (version == null) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot read %s: it has no format version; this Fluss version reads"
                                    + " format versions up to %d.",
                            source, newest));
        }
        check(source, version, newest);
        return end < 0 ? "" : text.substring(end + 1);
    }

    /** Adds the format version of the deletion vector properties to a snapshot's properties. */
    public static void putSnapshotVersion(Map<String, String> snapshotProperties) {
        snapshotProperties.put(
                SNAPSHOT_FORMAT_VERSION_PROPERTY, String.valueOf(SNAPSHOT_PROPERTIES_VERSION));
    }

    /**
     * Checks the format version of a lake snapshot's deletion vector properties before any of them
     * is parsed. A snapshot with such properties always has a version.
     *
     * @param snapshot the snapshot, for the error message
     */
    public static void checkSnapshotVersion(
            Object snapshot, Map<String, String> snapshotProperties) {
        String source = "the deletion vector properties of lake snapshot " + snapshot;
        String value = snapshotProperties.get(SNAPSHOT_FORMAT_VERSION_PROPERTY);
        if (value == null) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot read %s: it has no %s; this Fluss version reads format"
                                    + " versions up to %d.",
                            source, SNAPSHOT_FORMAT_VERSION_PROPERTY, SNAPSHOT_PROPERTIES_VERSION));
        }
        int version;
        try {
            version = Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot read %s: its %s '%s' is not a number.",
                            source, SNAPSHOT_FORMAT_VERSION_PROPERTY, value));
        }
        check(source, version, SNAPSHOT_PROPERTIES_VERSION);
    }
}
