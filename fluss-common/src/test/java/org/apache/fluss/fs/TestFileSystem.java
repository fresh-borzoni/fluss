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

package org.apache.fluss.fs;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.local.LocalDataOutputStream;
import org.apache.fluss.fs.local.LocalFileStatus;
import org.apache.fluss.fs.local.LocalFileSystem;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A test file system. This also has a service entry in the test resources, to be loaded during
 * tests.
 */
public class TestFileSystem extends LocalFileSystem {

    public static final String SCHEME = "test";

    // number of (input) stream opened
    private static final AtomicInteger streamOpenCounter = new AtomicInteger(0);

    // current number of created, unclosed (output) stream
    private static final Map<FsPath, Integer> currentUnclosedOutputStream =
            new ConcurrentHashMap<>();

    // number of upcoming create() calls that throw an IOException
    private static final AtomicInteger createFailuresRemaining = new AtomicInteger(0);

    // whether files are written as on an object store: see simulateObjectStoreWrites
    private static volatile boolean objectStoreWrites = false;

    // number of creates refused because another stream was writing the file
    private static final AtomicInteger racingCreatesRefused = new AtomicInteger(0);

    // runs at the start of every create while set: see beforeCreate
    private static volatile CreateHook createHook = null;

    // the creates and deletes since recordWrites, in call order, while it is on
    private static final List<String> recordedWrites = new ArrayList<>();
    private static volatile boolean recordingWrites = false;
    // whether listings are recorded with the writes: see recordLists
    private static volatile boolean recordingLists = false;

    private final Configuration configuration;

    public TestFileSystem(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public static int getNumtimeStreamOpened() {
        return streamOpenCounter.get();
    }

    public static void resetStreamOpenCounter() {
        streamOpenCounter.set(0);
    }

    public static int getNumberOfUnclosedOutputStream(FsPath path) {
        return currentUnclosedOutputStream.getOrDefault(path, 0);
    }

    @Override
    public FSDataInputStream open(FsPath f) throws IOException {
        streamOpenCounter.incrementAndGet();
        return super.open(f);
    }

    /** Makes the next {@code failures} calls to {@link #create} throw an {@link IOException}. */
    public static void failNextCreates(int failures) {
        createFailuresRemaining.set(failures);
    }

    /**
     * Turns on or off writing files as an object store such as S3 does. A file is visible only once
     * the stream writing it is closed. A create of a file another stream is writing fails, as S3A's
     * does with "is a directory".
     */
    public static void simulateObjectStoreWrites(boolean enabled) {
        objectStoreWrites = enabled;
        racingCreatesRefused.set(0);
    }

    /** The creates refused since {@link #simulateObjectStoreWrites} as another stream wrote. */
    public static int getRacingCreatesRefused() {
        return racingCreatesRefused.get();
    }

    /**
     * Turns on or off recording the creates and deletes of files, and clears the record. A create
     * is recorded as {@code create <mode> <path>} once it succeeds, a delete as {@code delete
     * <path>} or {@code delete -r <path>} with its result.
     */
    public static void recordWrites(boolean enabled) {
        synchronized (recordedWrites) {
            recordedWrites.clear();
            recordingWrites = enabled;
        }
    }

    /**
     * Turns on or off recording the listings of directories, as {@code list <path>}, with the
     * creates and deletes {@link #recordWrites} records.
     */
    public static void recordLists(boolean enabled) {
        recordingLists = enabled;
    }

    /** The creates and deletes recorded since {@link #recordWrites}, in call order. */
    public static List<String> getRecordedWrites() {
        synchronized (recordedWrites) {
            return new ArrayList<>(recordedWrites);
        }
    }

    private static void record(String operation) {
        if (recordingWrites) {
            synchronized (recordedWrites) {
                recordedWrites.add(operation);
            }
        }
    }

    /** Code that runs before a create. */
    @FunctionalInterface
    public interface CreateHook {
        void beforeCreate(FsPath path) throws IOException;
    }

    /**
     * Runs {@code hook} at the start of every create until it is reset with null. The hook can run
     * another writer between two creates of one, or throw to fail the create.
     */
    public static void beforeCreate(@Nullable CreateHook hook) {
        createHook = hook;
    }

    @Override
    public boolean delete(final FsPath f, final boolean recursive) throws IOException {
        boolean deleted = super.delete(f, recursive);
        record("delete " + (recursive ? "-r " : "") + f.getPath() + " " + deleted);
        return deleted;
    }

    @Override
    public FSDataOutputStream create(final FsPath filePath, final WriteMode overwrite)
            throws IOException {
        CreateHook hook = createHook;
        if (hook != null) {
            hook.beforeCreate(filePath);
        }
        if (createFailuresRemaining.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
            throw new IOException("Injected create failure for " + filePath);
        }
        if (isBeingWritten(filePath)) {
            racingCreatesRefused.incrementAndGet();
            throw new FileAlreadyExistsException(filePath + " is a directory");
        }
        LocalDataOutputStream stream = (LocalDataOutputStream) super.create(filePath, overwrite);
        currentUnclosedOutputStream.compute(filePath, (k, v) -> v == null ? 1 : v + 1);
        record("create " + overwrite + " " + filePath.getPath());
        return new TestOutputStream(stream, filePath);
    }

    @Override
    public boolean exists(FsPath f) throws IOException {
        return !isBeingWritten(f) && super.exists(f);
    }

    @Override
    public FileStatus getFileStatus(FsPath f) throws IOException {
        if (isBeingWritten(f)) {
            throw new FileNotFoundException(f + " is being written.");
        }
        LocalFileStatus status = (LocalFileStatus) super.getFileStatus(f);
        return new LocalFileStatus(status.getFile(), this);
    }

    private static boolean isBeingWritten(FsPath f) {
        return objectStoreWrites && currentUnclosedOutputStream.getOrDefault(f, 0) > 0;
    }

    @Override
    public FileStatus[] listStatus(FsPath f) throws IOException {
        if (recordingLists) {
            record("list " + f.getPath());
        }
        FileStatus[] stati = super.listStatus(f);
        LocalFileStatus[] newStati = new LocalFileStatus[stati.length];
        for (int i = 0; i < stati.length; i++) {
            newStati[i] = new LocalFileStatus(((LocalFileStatus) stati[i]).getFile(), this);
        }
        return newStati;
    }

    @Override
    public URI getUri() {
        return URI.create(SCHEME + ":///");
    }

    // ------------------------------------------------------------------------

    private static final class TestOutputStream extends FSDataOutputStream {

        private final LocalDataOutputStream stream;
        private final FsPath path;

        private TestOutputStream(LocalDataOutputStream stream, FsPath path) {
            this.stream = stream;
            this.path = path;
        }

        @Override
        public long getPos() throws IOException {
            return stream.getPos();
        }

        @Override
        public void write(int b) throws IOException {
            stream.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            stream.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            stream.flush();
        }

        @Override
        public void close() throws IOException {
            currentUnclosedOutputStream.compute(
                    path, (k, v) -> checkNotNull(v) == 1 ? null : v - 1);
            stream.close();
        }
    }

    // ------------------------------------------------------------------------

    /** Plugin for {@link FileSystemPlugin}. */
    public static final class TestFileSystemPlugin implements FileSystemPlugin {

        @Override
        public String getScheme() {
            return SCHEME;
        }

        @Override
        public FileSystem create(URI fsUri, Configuration configuration) throws IOException {
            return new TestFileSystem(configuration);
        }
    }
}
