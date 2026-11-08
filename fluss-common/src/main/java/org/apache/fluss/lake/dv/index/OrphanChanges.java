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

package org.apache.fluss.lake.dv.index;

import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.FormatVersions;
import org.apache.fluss.lake.dv.index.PositionIndexStore.ChainEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import static org.apache.fluss.lake.dv.index.IndexFiles.parseLong;
import static org.apache.fluss.lake.dv.index.IndexFiles.readText;
import static org.apache.fluss.lake.dv.index.IndexFiles.writeBytes;
import static org.apache.fluss.lake.dv.index.IndexLayout.CHANGE_STARTED;
import static org.apache.fluss.lake.dv.index.IndexLayout.CHANGE_VERSION;
import static org.apache.fluss.lake.dv.index.IndexLayout.ROW_POS_DIR;

/**
 * Removes the orphan changes of the {@link FlussPositionIndexStore}: the changes no chain marker
 * references, such as those of failed rounds and abandoned staging parts, once they are old enough.
 * A change ages from its {@code _STARTED}.
 */
final class OrphanChanges {

    // the store's logger
    private static final Logger LOG = LoggerFactory.getLogger(FlussPositionIndexStore.class);

    private final IndexLayout layout;
    private final IndexChain indexChain;
    private final IndexChangeWriter writer;

    OrphanChanges(IndexLayout layout, IndexChain indexChain, IndexChangeWriter writer) {
        this.layout = layout;
        this.indexChain = indexChain;
        this.writer = writer;
    }

    /** See {@link PositionIndexStore#deleteOrphanChanges}. */
    int deleteOrphanChanges(long minAgeMillis) throws IOException {
        FsPath rowPos = layout.path(ROW_POS_DIR);
        FileSystem fs = rowPos.getFileSystem();
        if (!fs.exists(rowPos)) {
            return 0;
        }
        Set<Long> registered = new HashSet<>();
        for (ChainEntry e : indexChain.rawChain()) {
            registered.add(e.changeId);
        }
        long now = System.currentTimeMillis();
        int deleted = 0;
        for (FileStatus st : fs.listStatus(rowPos)) {
            Long changeId = parseLong(st.getPath().getName());
            if (changeId == null || registered.contains(changeId)) {
                continue;
            }
            FsPath startedPath = new FsPath(layout.changeDir(changeId), CHANGE_STARTED);
            String started = readText(startedPath);
            if (started == null) {
                // A delete that stopped half-way can leave a change without its start marker.
                // It ages from now, so it goes like any other orphan.
                started = stampStarted(startedPath, now);
            }
            Long startedAt;
            if (started != null && started.isEmpty()) {
                // Being written, or left empty by a crash on HDFS or a local file system: it ages
                // from when the file system wrote it. Without that time it is kept.
                startedAt = modificationTime(fs, startedPath);
            } else {
                startedAt =
                        started == null
                                ? null
                                : parseLong(
                                        FormatVersions.withoutVersion(
                                                        startedPath, started, CHANGE_VERSION)
                                                .trim());
            }
            if (startedAt != null && now - startedAt >= minAgeMillis) {
                writer.deleteChange(changeId);
                deleted++;
            }
        }
        return deleted;
    }

    /** When the file system last wrote a file, or null if it cannot tell. */
    @Nullable
    private static Long modificationTime(FileSystem fs, FsPath path) {
        try {
            long time = fs.getFileStatus(path).getModificationTime();
            return time == Long.MAX_VALUE ? null : time;
        } catch (IOException e) {
            return null; // gone meanwhile
        }
    }

    /**
     * Writes the start marker of a change found without one, unless a writer wrote it meanwhile.
     * Returns the marker's text, or null if it cannot be told.
     */
    @Nullable
    private String stampStarted(FsPath startedPath, long now) throws IOException {
        try {
            writeBytes(
                    startedPath,
                    FormatVersions.withVersion(CHANGE_VERSION, String.valueOf(now))
                            .getBytes(StandardCharsets.UTF_8),
                    FileSystem.WriteMode.NO_OVERWRITE);
        } catch (IOException e) {
            LOG.debug("Could not mark the orphan index change {} started.", startedPath, e);
        }
        return readText(startedPath);
    }
}
