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
import org.apache.fluss.lake.dv.RowPosSstIndex;
import org.apache.fluss.lake.dv.index.PositionIndexStore.ChainEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.apache.fluss.lake.dv.index.IndexFiles.exists;
import static org.apache.fluss.lake.dv.index.IndexFiles.lines;
import static org.apache.fluss.lake.dv.index.IndexFiles.readText;
import static org.apache.fluss.lake.dv.index.IndexFiles.writeText;
import static org.apache.fluss.lake.dv.index.IndexLayout.CHAIN_DIR;
import static org.apache.fluss.lake.dv.index.IndexLayout.CHANGE_COMMITTED;
import static org.apache.fluss.lake.dv.index.IndexLayout.IDENTITIES_DIR;
import static org.apache.fluss.lake.dv.index.IndexLayout.IDENTITY_VERSION;
import static org.apache.fluss.lake.dv.index.IndexLayout.TRIM_END;
import static org.apache.fluss.lake.dv.index.IndexLayout.identityRecordName;
import static org.apache.fluss.lake.dv.index.IndexLayout.markerMaxFileId;
import static org.apache.fluss.lake.dv.index.IndexLayout.markerName;
import static org.apache.fluss.lake.dv.index.IndexLayout.parseMarker;

/**
 * The chain of the {@link FlussPositionIndexStore}: one marker per registered change under {@link
 * IndexLayout#CHAIN_DIR}, listed by name. It is ordered by sequence numbers from the single writer,
 * not by a clock. A registration writes its marker after checking the chain's end, then lists the
 * chain again and withdraws if another writer registered a different step under the same sequence.
 */
final class IndexChain {

    // the store's logger
    private static final Logger LOG = LoggerFactory.getLogger(FlussPositionIndexStore.class);

    private final IndexLayout layout;
    // the store's hooks: the reads of committed metadata and the test hooks
    private final FlussPositionIndexStore store;

    IndexChain(IndexLayout layout, FlussPositionIndexStore store) {
        this.layout = layout;
        this.store = store;
    }

    /**
     * Returns the registered changes in commit order; throws if they do not link up, and a
     * retriable {@link IOException} while the chain's end is a fork being repaired.
     */
    List<ChainEntry> list() throws IOException {
        return list(null);
    }

    /**
     * Returns the chain, taking the branch of a fork at its end that ends at {@code preferredEnd}.
     *
     * <p>A fork is two different steps under one sequence: two writers registered at the same chain
     * end, and the later one crashed after its marker and before it listed the chain again, which
     * would have withdrawn it. A fork inside the chain follows the branch later entries extend. A
     * fork at the end stays until the live tiering service registers again: its step extends, or
     * repeats, the branch whose snapshot Fluss registered ({@link #resolveFork}).
     */
    private List<ChainEntry> list(@Nullable Long preferredEnd) throws IOException {
        List<ChainEntry> entries = withoutEquivalentDuplicates(rawChain());
        List<ChainEntry> chain = new ArrayList<>(entries.size());
        for (int i = 0; i < entries.size(); ) {
            int j = i + 1;
            while (j < entries.size() && entries.get(j).sequence == entries.get(i).sequence) {
                j++;
            }
            if (j - i == 1) {
                chain.add(entries.get(i));
            } else {
                Long end =
                        j < entries.size()
                                ? Long.valueOf(entries.get(j).parentSnapshotId)
                                : preferredEnd;
                ChainEntry branch = null;
                for (ChainEntry e : entries.subList(i, j)) {
                    if (end != null && e.snapshotId == end) {
                        branch = e;
                    }
                }
                if (branch == null) {
                    throw new IOException(
                            String.format(
                                    "The position index chain is being repaired: it forks at"
                                            + " sequence %d into %s, as a registration crashed"
                                            + " before it could withdraw. The next registration of"
                                            + " the tiering service resolves it.",
                                    entries.get(i).sequence, entries.subList(i, j)));
                }
                chain.add(branch);
            }
            i = j;
        }
        for (int i = 1; i < chain.size(); i++) {
            ChainEntry e = chain.get(i);
            if (e.parentSnapshotId != chain.get(i - 1).snapshotId) {
                throw new IllegalStateException(
                        "The position index chain is broken: "
                                + e
                                + " follows "
                                + chain.get(i - 1));
            }
        }
        return chain;
    }

    /** See {@link PositionIndexStore#repairFork}. */
    void repairFork(long registeredSnapshotId) throws IOException {
        resolveFork(Long.MIN_VALUE, registeredSnapshotId);
    }

    /**
     * Withdraws the other branches of a fork at the chain's end if the step {@code
     * parentSnapshotId} to {@code snapshotId} repeats one of its branches or extends one. The live
     * tiering service registers only on top of the snapshot Fluss registered, or the snapshot it is
     * about to register in Fluss, so the branch it keeps is the one Fluss registered. A parent of
     * {@link Long#MIN_VALUE} keeps the branch that ends at {@code snapshotId}, whatever its parent.
     */
    private void resolveFork(long parentSnapshotId, long snapshotId) throws IOException {
        List<ChainEntry> entries = withoutEquivalentDuplicates(rawChain());
        if (entries.isEmpty()) {
            return;
        }
        long last = entries.get(entries.size() - 1).sequence;
        List<ChainEntry> branches = new ArrayList<>();
        for (ChainEntry e : entries) {
            if (e.sequence == last) {
                branches.add(e);
            }
        }
        if (branches.size() < 2) {
            return;
        }
        ChainEntry keep = null;
        for (ChainEntry e : branches) {
            if (e.snapshotId == snapshotId
                    && (parentSnapshotId == Long.MIN_VALUE
                            || e.parentSnapshotId == parentSnapshotId)) {
                keep = e;
            }
        }
        for (ChainEntry e : branches) {
            if (keep == null && e.snapshotId == parentSnapshotId) {
                keep = e;
            }
        }
        if (keep == null) {
            return; // not this writer's to resolve: the chain-end check refuses it
        }
        FsPath chainDir = layout.path(CHAIN_DIR);
        for (String name : markerNames()) {
            ChainEntry e = parseMarker(name);
            if (e.sequence == last
                    && (e.parentSnapshotId != keep.parentSnapshotId
                            || e.snapshotId != keep.snapshotId)) {
                LOG.warn(
                        "Withdrawing the position index chain entry {} of {}, which a crashed"
                                + " registration left next to {}.",
                        e,
                        layout.lakeTableDir,
                        keep);
                store.deleteOrThrow(new FsPath(chainDir, name), false);
            }
        }
    }

    /** Every marker in sequence order. */
    List<ChainEntry> rawChain() throws IOException {
        List<ChainEntry> entries = new ArrayList<>();
        for (String name : markerNames()) {
            entries.add(parseMarker(name));
        }
        // the markers of one step next to each other, so duplicates of a step read as one
        entries.sort(
                Comparator.comparingLong((ChainEntry e) -> e.sequence)
                        .thenComparingLong(e -> e.parentSnapshotId)
                        .thenComparingLong(e -> e.snapshotId));
        return entries;
    }

    /**
     * Keeps one of the markers that register the same step under the same sequence: the one with
     * the smallest change id. Writers racing on the same step, such as two re-indexes of the same
     * rewrite, may both stay registered, and either change describes the step.
     */
    private static List<ChainEntry> withoutEquivalentDuplicates(List<ChainEntry> entries) {
        List<ChainEntry> kept = new ArrayList<>(entries.size());
        for (ChainEntry e : entries) {
            ChainEntry last = kept.isEmpty() ? null : kept.get(kept.size() - 1);
            if (last != null
                    && last.sequence == e.sequence
                    && last.parentSnapshotId == e.parentSnapshotId
                    && last.snapshotId == e.snapshotId) {
                if (Long.compareUnsigned(e.changeId, last.changeId) < 0) {
                    kept.set(kept.size() - 1, e);
                }
                continue;
            }
            kept.add(e);
        }
        return kept;
    }

    List<String> markerNames() throws IOException {
        FsPath chainDir = layout.path(CHAIN_DIR);
        FileSystem fs = chainDir.getFileSystem();
        List<String> names = new ArrayList<>();
        if (fs.exists(chainDir)) {
            for (FileStatus st : fs.listStatus(chainDir)) {
                names.add(st.getPath().getName());
            }
        }
        return names;
    }

    /** The names of the chain's markers in sequence order. */
    List<String> markerNamesInOrder() throws IOException {
        List<String> names = markerNames();
        names.sort(Comparator.comparingLong(name -> parseMarker(name).sequence));
        return names;
    }

    /** The sequence of the chain's last marker, or 0 if there is none. */
    long lastSequence() throws IOException {
        long last = 0;
        for (String name : markerNames()) {
            last = Math.max(last, parseMarker(name).sequence);
        }
        return last;
    }

    void checkRegistrable(long parentSnapshotId, long changeId) throws IOException {
        // before a round's lake commit, so an index of an older build fails it, not a later read
        layout.checkLayout();
        checkCommitted(changeId);
        // as register() would see the chain once it resolved a fork at the end
        List<ChainEntry> chain = list(parentSnapshotId);
        checkChainEnd(chain, parentSnapshotId, -1);
        checkFileIds(changeId, fileIdRange(changeId));
    }

    void register(
            long parentSnapshotId,
            long snapshotId,
            long changeId,
            @Nullable String snapshotIdentity)
            throws IOException {
        layout.checkLayout();
        checkCommitted(changeId);
        resolveFork(parentSnapshotId, snapshotId);
        List<ChainEntry> chain = list();
        for (ChainEntry e : chain) {
            if (e.snapshotId == snapshotId) {
                if (e.changeId == changeId) {
                    // A retried or repaired round. A crash during the marker's write may have
                    // left it empty (HDFS, local): the identity is recorded again then.
                    if (snapshotIdentity != null) {
                        recordLostIdentity(e, snapshotIdentity);
                    }
                    return;
                }
                throw new IllegalStateException(
                        "Snapshot " + snapshotId + " is registered with another change: " + e);
            }
        }
        checkChainEnd(chain, parentSnapshotId, snapshotId);
        store.afterChainChecked();
        // New file ids must be above every registered id, or an id handed out twice would let one
        // file's positions mask another's.
        int[] ids = fileIdRange(changeId);
        checkFileIds(changeId, ids);
        // The sequence follows the end this writer checked. A racer that registered after the
        // check took the same sequence, which the check after the write finds.
        long sequence = chain.isEmpty() ? 1 : chain.get(chain.size() - 1).sequence + 1;
        String name =
                markerName(
                        sequence,
                        parentSnapshotId,
                        snapshotId,
                        changeId,
                        ids == null ? -1 : ids[1]);
        FsPath marker = new FsPath(layout.path(CHAIN_DIR), name);
        store.beforeMarkerWrite();
        writeText(marker, snapshotIdentity == null ? "" : snapshotIdentity);
        // The checks above and this write are not atomic, on object stores in particular, so a
        // writer racing on the same chain end can pass them too. After its write, each writer
        // looks for another marker of its sequence. Of two racing writers at most one finds none
        // (whoever lists first wrote first), so at most one stays registered. A racer that
        // registers the same step stays too, as either change describes it.
        for (String other : markerNames()) {
            ChainEntry e = parseMarker(other);
            if (!other.equals(name)
                    && e.sequence == sequence
                    && (e.parentSnapshotId != parentSnapshotId || e.snapshotId != snapshotId)) {
                marker.getFileSystem().delete(marker, false);
                throw new IllegalStateException(
                        String.format(
                                "Another writer registered %s at the end of the position index"
                                        + " chain concurrently; the change %d -> %d (%d) is"
                                        + " withdrawn.",
                                e, parentSnapshotId, snapshotId, changeId));
            }
        }
    }

    /**
     * Records the identity of an entry whose marker a crash left empty, under a name of its own in
     * {@link IndexLayout#IDENTITIES_DIR}: a marker is never written twice.
     */
    private void recordLostIdentity(ChainEntry entry, String snapshotIdentity) throws IOException {
        FsPath chainDir = layout.path(CHAIN_DIR);
        for (String name : markerNames()) {
            if (parseMarker(name).equals(entry)) {
                String identity = readText(new FsPath(chainDir, name));
                if (identity != null && identity.isEmpty() && recordedIdentity(name) == null) {
                    writeText(
                            new FsPath(
                                    layout.path(IDENTITIES_DIR),
                                    identityRecordName(
                                            name,
                                            UUID.randomUUID()
                                                    .toString()
                                                    .replace("-", "")
                                                    .substring(0, 16))),
                            FormatVersions.withVersion(
                                    IDENTITY_VERSION, snapshotIdentity + "\n" + TRIM_END));
                }
            }
        }
    }

    /** Deletes the identity records of chain markers a trim deleted, best effort. */
    void deleteIdentities(Collection<String> markers) throws IOException {
        FsPath dir = layout.path(IDENTITIES_DIR);
        FileSystem fs = dir.getFileSystem();
        if (markers.isEmpty() || !fs.exists(dir)) {
            return;
        }
        Set<String> deleted = new HashSet<>(markers);
        for (FileStatus st : fs.listStatus(dir)) {
            String name = st.getPath().getName();
            int end = name.lastIndexOf('.');
            if (end > 0 && deleted.contains(name.substring(0, end))) {
                try {
                    store.deletePath(st.getPath(), false);
                } catch (IOException e) {
                    LOG.debug("Could not delete the identity record {}.", st.getPath(), e);
                }
            }
        }
    }

    /** The identity a complete identity record of a marker holds, or null if none. */
    @Nullable
    private String recordedIdentity(String markerName) throws IOException {
        FsPath dir = layout.path(IDENTITIES_DIR);
        FileSystem fs = dir.getFileSystem();
        if (!fs.exists(dir)) {
            return null;
        }
        for (FileStatus st : fs.listStatus(dir)) {
            if (!st.getPath().getName().startsWith(markerName + ".")) {
                continue;
            }
            String text = readText(st.getPath());
            if (text == null || !text.endsWith("\n" + TRIM_END)) {
                continue; // cut short
            }
            String body = FormatVersions.withoutVersion(st.getPath(), text, IDENTITY_VERSION);
            return body.substring(0, body.length() - TRIM_END.length() - 1);
        }
        return null;
    }

    private void checkCommitted(long changeId) throws IOException {
        if (!exists(new FsPath(layout.changeDir(changeId), CHANGE_COMMITTED))) {
            throw new IllegalStateException(
                    "Index change " + changeId + " is not committed; cannot register it.");
        }
    }

    private static void checkChainEnd(
            List<ChainEntry> chain, long parentSnapshotId, long snapshotId) {
        long tail = chain.isEmpty() ? -1L : chain.get(chain.size() - 1).snapshotId;
        if (parentSnapshotId != tail) {
            throw new IllegalStateException(
                    String.format(
                            "The position index chain ends at snapshot %d; the change %d -> %d"
                                    + " does not apply to it.",
                            tail, parentSnapshotId, snapshotId));
        }
    }

    /** Refuses a change whose new file ids are not all above every registered id. */
    private void checkFileIds(long changeId, @Nullable int[] ids) throws IOException {
        if (ids == null) {
            return;
        }
        long registeredMax = -1;
        for (String name : markerNames()) {
            registeredMax = Math.max(registeredMax, markerMaxFileId(name));
        }
        if (ids[0] <= registeredMax) {
            throw new IllegalStateException(
                    String.format(
                            "Index change %d uses file ids %d..%d, but ids up to %d are registered"
                                    + " already (another tiering writer?).",
                            changeId, ids[0], ids[1], registeredMax));
        }
    }

    /** {min, max} of the file ids a committed change adds, or null if it adds none. */
    @Nullable
    private int[] fileIdRange(long changeId) throws IOException {
        int[] range = null;
        for (String pid : lines(store.readCommittedPartitions(changeId))) {
            long p = Long.parseLong(pid);
            RowPosSstIndex index = store.readManifest(changeId, p == -1L ? null : p);
            for (int b : index.getBucketIds()) {
                for (int id : index.getFileId2Name(b).keySet()) {
                    range =
                            range == null
                                    ? new int[] {id, id}
                                    : new int[] {Math.min(range[0], id), Math.max(range[1], id)};
                }
            }
        }
        return range;
    }

    @Nullable
    String snapshotIdentity(ChainEntry entry) throws IOException {
        FsPath chainDir = layout.path(CHAIN_DIR);
        for (String name : markerNames()) {
            if (parseMarker(name).equals(entry)) {
                String identity = readText(new FsPath(chainDir, name));
                if (identity == null) {
                    return null;
                }
                // empty: registered without one, or a crash cut it; a repair records it apart
                return identity.isEmpty() ? recordedIdentity(name) : identity;
            }
        }
        return null;
    }
}
