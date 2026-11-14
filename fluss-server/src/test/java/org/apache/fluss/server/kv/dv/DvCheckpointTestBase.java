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

import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.local.LocalFileSystem;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.kv.dv.DerivedDvManagerTest.Change;
import org.apache.fluss.server.kv.dv.DerivedDvManagerTest.FakeSource;
import org.apache.fluss.server.kv.dv.DerivedDvManagerTest.Index;
import org.apache.fluss.server.kv.dv.DvChangeListWorkload.Kind;
import org.apache.fluss.server.kv.dv.DvChangeListWorkload.Record;
import org.apache.fluss.server.kv.snapshot.DvChangeListCheckpoints;
import org.apache.fluss.server.kv.snapshot.DvChangeListHandle;
import org.apache.fluss.server.kv.snapshot.SnapshotLocation;

import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.RocksDB;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.dict;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.key;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.lakeMasks;
import static org.apache.fluss.server.kv.dv.DerivedDvManagerTest.offsets;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The harness of the change-list checkpoint tests: leaders of a bucket, each on its own RocksDB,
 * with the log and the lake of a {@link DvChangeListWorkload}. A live leader saves its change list
 * with a KV snapshot, a new leader rebuilds from it, and their masks are compared with a new leader
 * that replays the whole log.
 */
abstract class DvCheckpointTestBase {

    static final TableBucket BUCKET = new TableBucket(7, 3);
    static final long SNAPSHOT = 100;
    static final String F1 = "s3://lake/t/data/f1.parquet";

    @TempDir protected Path dir;

    final List<Leader> leaders = new ArrayList<>();
    final AtomicInteger dirs = new AtomicInteger();

    @AfterEach
    void tearDownLeaders() {
        for (Leader l : leaders) {
            l.close();
        }
        leaders.clear();
    }

    // ---- harness --------------------------------------------------------------------------------

    /** A live leader with a saved change list of a random workload. */
    Fixture fixture() throws Exception {
        DvChangeListWorkload w = DvChangeListWorkload.random(new Random(5), 12, 30, 5);
        List<Long> b = new ArrayList<>(w.kvAt.keySet());
        long o = b.get(b.size() / 3);
        long x = b.get(2 * b.size() / 3);
        Leader live = liveLeader(w, o);
        return new Fixture(w, o, x, live, export(live, 1, x));
    }

    static final class Fixture {
        final DvChangeListWorkload w;
        final long o;
        final long x;
        final Leader live;
        final DvChangeListHandle part;

        Fixture(DvChangeListWorkload w, long o, long x, Leader live, DvChangeListHandle part) {
            this.w = w;
            this.o = o;
            this.x = x;
            this.live = live;
            this.part = part;
        }
    }

    void assertFallsBack(Fixture f, Leader b, String reason) throws Exception {
        assertThat(b.manager.lastRebuildCheckpointFields())
                .contains("ckpt=" + reason, "replay_from=" + f.o);
        assertThat(b.src.replayedFrom).contains(f.o);
        assertThat(masks(b)).isEqualTo(masks(fullReplay(f.w, f.o)));
    }

    /** A bucket's leader on its own RocksDB, with the log and the lake of {@code w}. */
    final class Leader {
        final DvRocksDB db;
        final AtomicLong hw = new AtomicLong();
        final DerivedDvManager manager;
        final FakeSource src;

        Leader(String name) throws Exception {
            this(name, DerivedDvMetrics.noop());
        }

        Leader(String name, DerivedDvMetrics metrics) throws Exception {
            int n = dirs.incrementAndGet();
            db = DvRocksDB.open(dir.resolve(name + "-" + n + "-dv").toString());
            manager =
                    DvTestManagers.newManager(
                            db, hw::get, DvTestManagers.CHECKPOINT_INTERVAL, metrics);
            manager.setMaskCacheMillis(0);
            manager.setCheckpointJitterMillis(0);
            manager.setLogContext(TablePath.of("db", "t"), BUCKET);
            src = new FakeSource(Files.createDirectories(dir.resolve(name + "-" + n + "-src")));
            leaders.add(this);
        }

        void close() {
            manager.close();
            db.close();
        }
    }

    void closeAll() {
        for (Leader l : leaders) {
            l.close();
        }
        leaders.clear();
    }

    /**
     * A leader of {@code w} whose lake is readable at {@code o} (none if -1), with the log below
     * {@code logEnd} and the high watermark there.
     */
    Leader leader(String name, DvChangeListWorkload w, long o, long logEnd) throws Exception {
        Leader l = new Leader(name);
        seed(l, w, o, logEnd);
        return l;
    }

    /**
     * Publishes the lake of {@code w} at {@code o} (none if -1) and the log below {@code logEnd}.
     */
    static void seed(Leader l, DvChangeListWorkload w, long o, long logEnd) {
        if (o >= 0) {
            Index index = new Index();
            int pos = 0;
            for (String k : new TreeMap<>(w.kvAt.get(o)).keySet()) {
                index.put(k, 1, pos++);
            }
            l.src.publish(SNAPSHOT, 1, o, dict(1, F1), index.build());
            l.src.readable = new long[] {SNAPSHOT, o};
        }
        for (Record r : w.log) {
            if (r.offset < logEnd && r.kind != Kind.UPDATE_BEFORE) {
                l.src.log.add(change(r));
            }
        }
        l.hw.set(logEnd);
    }

    /** The live leader: covers the lake at o, and gets every later change from the write path. */
    Leader liveLeader(DvChangeListWorkload w, long o) throws Exception {
        Leader live = leader("live", w, o, o);
        live.manager.recover(live.src);
        handOverFrom(live, w, o);
        return live;
    }

    static void handOverFrom(Leader l, DvChangeListWorkload w, long from) {
        handOverFrom(l, w, from, true);
    }

    static void handOverFrom(
            Leader l, DvChangeListWorkload w, long from, boolean raiseHighWatermark) {
        for (Record r : w.log) {
            if (r.offset >= from && r.kind != Kind.UPDATE_BEFORE) {
                l.src.log.add(change(r));
                DvTestManagers.handOver(
                        l.manager,
                        Collections.singletonList(
                                l.src.written(
                                        r.key, r.offset, r.version(), r.kind == Kind.INSERT)));
            }
        }
        if (raiseHighWatermark) {
            l.hw.set(w.end());
        }
    }

    /** Hands every change from {@code from} over as the write path does, in batches of 256. */
    static void handOverFast(Leader l, DvChangeListWorkload w, long from) {
        Map<String, Long> kv = new HashMap<>();
        List<Change> log = new ArrayList<>();
        List<DvEntry> batch = new ArrayList<>();
        for (Record r : w.log) {
            if (r.kind == Kind.UPDATE_BEFORE) {
                continue;
            }
            long previous = kv.containsKey(r.key) ? kv.get(r.key) : -1;
            if (r.kind == Kind.DELETE) {
                kv.remove(r.key);
            } else {
                kv.put(r.key, r.offset);
            }
            if (r.offset < from) {
                continue;
            }
            log.add(change(r));
            batch.add(
                    new DvEntry(
                            r.keyBytes(), r.offset, r.version(), r.kind == Kind.INSERT, previous));
            if (batch.size() >= 256) {
                DvTestManagers.handOver(l.manager, new ArrayList<>(batch));
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            DvTestManagers.handOver(l.manager, batch);
        }
        l.src.log.addAll(log);
    }

    Leader fullReplay(DvChangeListWorkload w, long o) throws Exception {
        Leader full = leader("full", w, o, w.end());
        full.manager.recover(full.src);
        return full;
    }

    Leader rebuild(DvChangeListWorkload w, long o, long x, DvChangeListHandle part)
            throws Exception {
        Leader b = leader("rebuilt", w, o, w.end());
        b.src.restored = restored(part, x, view(w.kvAt.get(x)), false);
        b.manager.recover(b.src);
        return b;
    }

    Leader rebuildFromCheckpoint(DvChangeListWorkload w, long o, long x) throws Exception {
        return rebuild(w, o, x, export(liveLeader(w, o), 1, x));
    }

    DvRestoredChangeList restored(DvChangeListHandle part, long x, DvKvView view, boolean rowTtl) {
        return DvRestoredChangeList.restored("b", BUCKET, true, rowTtl, 1, x, part, view, 1);
    }

    @Nullable
    DvChangeListHandle export(Leader l, long kvSnapshotId, long x) throws Exception {
        DvChangeListCheckpoints.Checkpoint checkpoint =
                l.manager.prepare(kvSnapshotId, x, location());
        DvChangeListHandle part = checkpoint.attach();
        checkpoint.committed(() -> true);
        if (part != null) {
            // the committed metadata is read back off the snapshot's thread
            ((DvOwnerChangeListExport.Export) checkpoint).readBackPosted.get(30, TimeUnit.SECONDS);
        }
        // the owner takes the commit in a step; a read waits for the steps before it
        l.manager.inspect();
        return part;
    }

    SnapshotLocation location() throws Exception {
        return location(LocalFileSystem.getSharedInstance());
    }

    /** The KV snapshot's location, with its files written through {@code fs}. */
    SnapshotLocation location(FileSystem fs) throws Exception {
        File snapshots = Files.createDirectories(dir.resolve("kv-snapshots")).toFile();
        File shared = Files.createDirectories(dir.resolve("kv-shared")).toFile();
        return new SnapshotLocation(
                fs, FsPath.fromLocalFile(snapshots), FsPath.fromLocalFile(shared), 4096);
    }

    /** The files in the KV snapshots' shared directory. */
    List<String> sharedFiles() throws Exception {
        List<String> names = new ArrayList<>();
        File[] files = Files.createDirectories(dir.resolve("kv-shared")).toFile().listFiles();
        if (files != null) {
            for (File f : files) {
                names.add(f.getName());
            }
        }
        Collections.sort(names);
        return names;
    }

    /**
     * The installed change list of {@code l} from {@code o} on, as the model writes it: a
     * superseded version before o is -1.
     */
    static List<DvChangeListWorkload.Entry> changeList(Leader l, long o) {
        List<DvChangeList.Pending> changes = l.manager.changeListFrom(o);
        assertThat(changes).as("the change list is installed").isNotNull();
        List<DvChangeListWorkload.Entry> entries = new ArrayList<>();
        for (DvChangeList.Pending p : changes) {
            entries.add(
                    new DvChangeListWorkload.Entry(
                                    p.offset, new String(p.key, StandardCharsets.UTF_8), p.previous)
                            .classified(o));
        }
        return entries;
    }

    /** The KV at a batch boundary: each live key's version offset at the start of its value. */
    DvKvView view(Map<String, Long> kv) throws Exception {
        Path viewDir = dir.resolve("view-" + dirs.incrementAndGet());
        try (Options options = new Options().setCreateIfMissing(true);
                RocksDB db = RocksDB.open(options, viewDir.toString())) {
            for (Map.Entry<String, Long> e : kv.entrySet()) {
                db.put(key(e.getKey()), ByteBuffer.allocate(12).putLong(e.getValue()).array());
            }
        }
        return DvKvView.of(
                viewDir, 0, System.currentTimeMillis(), v -> ByteBuffer.wrap(v).getLong());
    }

    static Change change(Record r) {
        return new Change(r.keyBytes(), r.offset, r.version(), r.kind == Kind.INSERT);
    }

    static long count(DvChangeListWorkload w, long from, long to) {
        long n = 0;
        for (DvChangeListWorkload.Entry e : w.handedOver) {
            if (e.offset >= from && e.offset < to) {
                n++;
            }
        }
        return n;
    }

    static File localFile(DvChangeListHandle.Segment s) {
        return new File(new FsPath(path(s)).toUri().getPath());
    }

    static String path(DvChangeListHandle.Segment s) {
        return s.file().getKvFileHandle().getFilePath();
    }

    /** The masks of a union read of the lake snapshot, in a comparable form. */
    static String masks(Leader l) throws Exception {
        return describe(read(l, SNAPSHOT));
    }

    static String emptyLakeMasks(Leader l) throws Exception {
        return describe(read(l, -1));
    }

    static String describe(org.apache.fluss.server.kv.dv.DvSnapshot s) throws Exception {
        return "lake="
                + new TreeMap<>(lakeMasks(s))
                + " log="
                + offsets(s.getLogDvBitmap())
                + " from="
                + s.getSnapshotStartOffset()
                + " to="
                + s.getLogEndOffset();
    }

    static org.apache.fluss.server.kv.dv.DvSnapshot read(Leader l, long snapshotId)
            throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (true) {
            try {
                return DvTestReads.unionRead(l.manager, snapshotId);
            } catch (RetriableException e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(20);
            }
        }
    }

    /** A local file system whose output streams block on their first write until released. */
    static final class BlockingFs extends LocalFileSystem {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        volatile StackTraceElement[] stack;

        @Override
        public FSDataOutputStream create(FsPath filePath, WriteMode overwrite) throws IOException {
            FSDataOutputStream inner = super.create(filePath, overwrite);
            return new FSDataOutputStream() {
                private void block() throws IOException {
                    if (entered.getCount() > 0) {
                        stack = Thread.currentThread().getStackTrace();
                        entered.countDown();
                    }
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        throw new IOException(e);
                    }
                }

                @Override
                public void write(int b) throws IOException {
                    block();
                    inner.write(b);
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    block();
                    inner.write(b, off, len);
                }

                @Override
                public long getPos() throws IOException {
                    return inner.getPos();
                }

                @Override
                public void flush() throws IOException {
                    inner.flush();
                }

                @Override
                public void close() throws IOException {
                    inner.close();
                }
            };
        }
    }
}
