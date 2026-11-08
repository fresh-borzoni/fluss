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

import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Reads and writes of the index store's files, and the parsing of their text. */
final class IndexFiles {

    private IndexFiles() {}

    /** A new local directory for temporary files, under {@code parent} if not null. */
    static File createTempDirectory(@Nullable File parent, String prefix) throws IOException {
        if (parent == null) {
            return Files.createTempDirectory(prefix).toFile();
        }
        Files.createDirectories(parent.toPath());
        return Files.createTempDirectory(parent.toPath(), prefix).toFile();
    }

    @Nullable
    static Integer parseInteger(String s) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Nullable
    static Long parseLong(String s) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    static Map<Integer, String> readDict(@Nullable String text) {
        Map<Integer, String> out = new HashMap<>();
        for (String line : lines(text)) {
            String[] f = line.split("\t", 2);
            out.put(Integer.parseInt(f[0]), f[1]);
        }
        return out;
    }

    static List<String> lines(@Nullable String text) {
        List<String> out = new ArrayList<>();
        if (text != null) {
            for (String line : text.split("\n")) {
                if (!line.trim().isEmpty()) {
                    out.add(line.trim());
                }
            }
        }
        return out;
    }

    static boolean exists(FsPath p) throws IOException {
        return p.getFileSystem().exists(p);
    }

    @Nullable
    static String readText(FsPath p) throws IOException {
        return exists(p) ? new String(readBytes(p), StandardCharsets.UTF_8) : null;
    }

    static byte[] readBytes(FsPath p) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (FSDataInputStream in = p.getFileSystem().open(p)) {
            copy(in, bos);
        }
        return bos.toByteArray();
    }

    static void writeText(FsPath path, String content) throws IOException {
        writeBytes(path, content.getBytes(StandardCharsets.UTF_8));
    }

    static void writeBytes(FsPath path, byte[] content) throws IOException {
        writeBytes(path, content, FileSystem.WriteMode.OVERWRITE);
    }

    static void writeBytes(FsPath path, byte[] content, FileSystem.WriteMode mode)
            throws IOException {
        try (FSDataOutputStream out = path.getFileSystem().create(path, mode)) {
            out.write(content);
        }
    }

    static void upload(File local, FsPath target) throws IOException {
        try (InputStream in = Files.newInputStream(local.toPath());
                FSDataOutputStream out =
                        target.getFileSystem().create(target, FileSystem.WriteMode.OVERWRITE)) {
            copy(in, out);
        }
    }

    static void download(FsPath src, File target) throws IOException {
        try (FSDataInputStream in = src.getFileSystem().open(src);
                OutputStream out = Files.newOutputStream(target.toPath())) {
            copy(in, out);
        }
    }

    private static void copy(InputStream in, OutputStream out) throws IOException {
        byte[] buf = new byte[1 << 16];
        int n;
        while ((n = in.read(buf)) > 0) {
            out.write(buf, 0, n);
        }
    }
}
