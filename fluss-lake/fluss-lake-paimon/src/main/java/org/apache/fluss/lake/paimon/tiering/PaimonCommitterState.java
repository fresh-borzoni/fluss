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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.annotation.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * What the Paimon committers of a tiering job keep across rounds: the failed commits of tables with
 * deletion vectors whose files wait for a decision. A committer decides on them at its next commit
 * of the table, once they are old enough for any request of the failed commit still in flight to
 * have finished. Ages are measured with a monotonic clock.
 *
 * <p>It lives in memory only and is bounded. A record is dropped, and its files stay for the lake's
 * orphan-file removal as after a failover, when the job has not committed its table for {@link
 * #DEFAULT_ABANDON_AFTER_NANOS} (the table was dropped, or another tiering service tiers it now),
 * or when the job holds more than {@link #DEFAULT_MAX_RECORDS} records (the oldest goes, with a
 * warning).
 */
final class PaimonCommitterState {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonCommitterState.class);

    /** How old a failed commit must be before a committer decides on its files. */
    static final long DEFAULT_MIN_AGE_NANOS = TimeUnit.MINUTES.toNanos(10);

    /** How long the job may not commit a table before the table's records are dropped. */
    static final long DEFAULT_ABANDON_AFTER_NANOS = TimeUnit.HOURS.toNanos(24);

    /** The most records the job keeps, over all its tables. */
    static final int DEFAULT_MAX_RECORDS = 1_000;

    private final long minAgeNanos;
    private final long abandonAfterNanos;
    private final int maxRecords;
    private final LongSupplier nanoClock;

    // guarded by this
    private final Map<Long, List<FailedCommit>> failedCommits = new HashMap<>();
    private final Map<Long, Long> lastCommitNanos = new HashMap<>();
    private int records;

    PaimonCommitterState() {
        this(
                DEFAULT_MIN_AGE_NANOS,
                DEFAULT_ABANDON_AFTER_NANOS,
                DEFAULT_MAX_RECORDS,
                System::nanoTime);
    }

    @VisibleForTesting
    PaimonCommitterState(
            long minAgeNanos, long abandonAfterNanos, int maxRecords, LongSupplier nanoClock) {
        this.minAgeNanos = minAgeNanos;
        this.abandonAfterNanos = abandonAfterNanos;
        this.maxRecords = maxRecords;
        this.nanoClock = nanoClock;
    }

    long nanoTime() {
        return nanoClock.getAsLong();
    }

    /**
     * Records a failed commit of table {@code tableId} whose files wait for a decision. Drops the
     * oldest record of the job if it holds more than its maximum then.
     */
    synchronized void addFailedCommit(long tableId, FailedCommit failedCommit) {
        committed(tableId);
        failedCommits.computeIfAbsent(tableId, id -> new ArrayList<>()).add(failedCommit);
        records++;
        while (records > maxRecords) {
            dropOldest();
        }
    }

    /**
     * Removes and returns the failed commits of table {@code tableId} old enough to decide on. A
     * committer calls it at each commit of the table, which also drops the records of the tables
     * the job has not committed for too long.
     */
    synchronized List<FailedCommit> takeDue(long tableId) {
        committed(tableId);
        long now = nanoTime();
        List<FailedCommit> due = new ArrayList<>();
        List<FailedCommit> commits = failedCommits.get(tableId);
        if (commits != null) {
            for (Iterator<FailedCommit> it = commits.iterator(); it.hasNext(); ) {
                FailedCommit commit = it.next();
                if (now - commit.failedAtNanos >= minAgeNanos) {
                    due.add(commit);
                    it.remove();
                    records--;
                }
            }
            if (commits.isEmpty()) {
                failedCommits.remove(tableId);
            }
        }
        return due;
    }

    /** Notes that the job commits table {@code tableId}, and drops abandoned tables' records. */
    private void committed(long tableId) {
        long now = nanoTime();
        lastCommitNanos.put(tableId, now);
        for (Iterator<Map.Entry<Long, Long>> it = lastCommitNanos.entrySet().iterator();
                it.hasNext(); ) {
            Map.Entry<Long, Long> table = it.next();
            if (now - table.getValue() < abandonAfterNanos) {
                continue;
            }
            it.remove();
            List<FailedCommit> abandoned = failedCommits.remove(table.getKey());
            if (abandoned != null) {
                records -= abandoned.size();
                LOG.warn(
                        "Dropping {} undecided failed commits of the Paimon table of table id {},"
                                + " which this job has not committed for {} h: their files stay"
                                + " for the lake's orphan-file removal.",
                        abandoned.size(),
                        table.getKey(),
                        TimeUnit.NANOSECONDS.toHours(abandonAfterNanos));
            }
        }
    }

    private void dropOldest() {
        long oldestTable = -1;
        FailedCommit oldest = null;
        for (Map.Entry<Long, List<FailedCommit>> table : failedCommits.entrySet()) {
            for (FailedCommit commit : table.getValue()) {
                if (oldest == null || commit.failedAtNanos - oldest.failedAtNanos < 0) {
                    oldest = commit;
                    oldestTable = table.getKey();
                }
            }
        }
        if (oldest == null) {
            records = 0;
            return;
        }
        List<FailedCommit> commits = failedCommits.get(oldestTable);
        commits.remove(oldest);
        if (commits.isEmpty()) {
            failedCommits.remove(oldestTable);
        }
        records--;
        LOG.warn(
                "Dropping the oldest undecided failed commit (by {}) of the Paimon table of table"
                        + " id {}: the job keeps at most {}. Its files stay for the lake's"
                        + " orphan-file removal.",
                oldest.commitUser,
                oldestTable,
                maxRecords);
    }

    @VisibleForTesting
    synchronized int failedCommits(long tableId) {
        List<FailedCommit> commits = failedCommits.get(tableId);
        return commits == null ? 0 : commits.size();
    }

    /**
     * A commit that had not landed when its call failed, with what the decision on it needs: its
     * commit user, the latest snapshot before it, the files it wrote and its offsets file.
     */
    static final class FailedCommit {
        final String commitUser;
        final long baseId;
        final long baseTimeMillis;
        // the files the commit wrote, as Paimon's abort selects them; a compaction's upgraded
        // files are not among them
        final List<String> files;
        @Nullable final String offsetsFile;
        final long failedAtNanos;

        FailedCommit(
                String commitUser,
                long baseId,
                long baseTimeMillis,
                List<String> files,
                @Nullable String offsetsFile,
                long failedAtNanos) {
            this.commitUser = commitUser;
            this.baseId = baseId;
            this.baseTimeMillis = baseTimeMillis;
            this.files = files;
            this.offsetsFile = offsetsFile;
            this.failedAtNanos = failedAtNanos;
        }
    }
}
