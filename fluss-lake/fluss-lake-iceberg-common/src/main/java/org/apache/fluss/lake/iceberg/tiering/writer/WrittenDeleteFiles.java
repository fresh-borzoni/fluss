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

package org.apache.fluss.lake.iceberg.tiering.writer;

import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The {@link FileIO} of a delete file writer, which records the files the writer creates. A writer
 * that fails before it hands its files over deletes them, as nothing else knows them: the writers
 * of Iceberg create a file before they report it, and some only on close.
 */
final class WrittenDeleteFiles implements FileIO {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(WrittenDeleteFiles.class);

    private final FileIO delegate;
    private final List<String> locations = new ArrayList<>();

    WrittenDeleteFiles(FileIO delegate) {
        this.delegate = delegate;
    }

    @Override
    public InputFile newInputFile(String path) {
        return delegate.newInputFile(path);
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        return delegate.newInputFile(path, length);
    }

    @Override
    public OutputFile newOutputFile(String path) {
        locations.add(path);
        return delegate.newOutputFile(path);
    }

    @Override
    public void deleteFile(String path) {
        delegate.deleteFile(path);
    }

    @Override
    public Map<String, String> properties() {
        return delegate.properties();
    }

    @Override
    public void close() {
        // the table's FileIO is closed by its owner
    }

    /** Deletes every file the writer created. Best effort: a file left behind is an orphan. */
    void deleteAll() {
        for (String location : locations) {
            try {
                delegate.deleteFile(location);
            } catch (RuntimeException e) {
                LOG.warn("Could not delete the delete file {} of a failed write.", location, e);
            }
        }
        locations.clear();
    }
}
