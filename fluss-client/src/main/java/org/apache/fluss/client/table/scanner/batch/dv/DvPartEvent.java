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

package org.apache.fluss.client.table.scanner.batch.dv;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.DeletionVectorsNotReadyException;
import org.apache.fluss.exception.InvalidMetadataException;
import org.apache.fluss.exception.LakeTableRolledBackException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.utils.ExceptionUtils;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * What a part learns that changes its {@link DvPartState}: an answer of its bucket's leader, the
 * end of an action on the lake or the log, or a call of the engine. {@link DvPartMachine#apply}
 * applies it.
 */
@Internal
final class DvPartEvent {

    /** The kinds of events. */
    enum Kind {
        /** The leader pinned a cut (in {@code PLANNED}), or served all masks of a cut. */
        MASKS,
        /** A leader with no masks yet named a cut to read the bucket by key up to. */
        REBUILDING,
        /**
         * The leader covers another lake snapshot, or, for a request of a cut, no longer holds it.
         */
        COVERED,
        /** The leader had no masks to serve within the fetch's wait. */
        NOT_READY,
        /** No leader of the bucket was found within the fetch's retries. */
        LEADER_LOST,
        /** The bucket's partition was dropped. */
        PARTITION_DROPPED,
        /** The lake snapshot names another commit than the one the masks are of. */
        ROLLED_BACK,
        /** The reading thread was interrupted: the engine cancels the read. */
        INTERRUPTED,
        /** Any other failure, also an answer inconsistent with the cut. */
        ERROR,
        /** The masks of a cut were derived from the keys. */
        DERIVED,
        /** The bucket's lake files were planned on a target snapshot. */
        REPLANNED,
        /** The engine asks for the part's first rows. */
        POLL,
        /** All lake files were read. */
        LAKE_END,
        /** The log was read up to the cut. */
        LOG_END,
        /** The engine closes the part's reader. */
        CLOSE
    }

    final Kind kind;
    @Nullable final Throwable failure;
    // MASKS of a pin: the leader's answer, which carries the masks of the first lake split
    @Nullable final GetDvSnapshotResponse pinAnswer;
    // MASKS of a cut, or DERIVED
    @Nullable final DvBucketMaskState masks;
    // COVERED
    @Nullable final StaleSnapshotException covered;
    // REBUILDING
    final long foldCut;
    // REPLANNED
    @Nullable final DvPartMachine.Target target;

    private DvPartEvent(
            Kind kind,
            @Nullable Throwable failure,
            @Nullable GetDvSnapshotResponse pinAnswer,
            @Nullable DvBucketMaskState masks,
            @Nullable StaleSnapshotException covered,
            long foldCut,
            @Nullable DvPartMachine.Target target) {
        this.kind = kind;
        this.failure = failure;
        this.pinAnswer = pinAnswer;
        this.masks = masks;
        this.covered = covered;
        this.foldCut = foldCut;
        this.target = target;
    }

    static DvPartEvent of(Kind kind) {
        return new DvPartEvent(kind, null, null, null, null, -1L, null);
    }

    /** The leader's answer to a pin. */
    static DvPartEvent pinned(GetDvSnapshotResponse answer) {
        return new DvPartEvent(Kind.MASKS, null, checkNotNull(answer), null, null, -1L, null);
    }

    /** All masks of a cut: fetched, or kept by a restored part. */
    static DvPartEvent masks(DvBucketMaskState masks) {
        return new DvPartEvent(Kind.MASKS, null, null, checkNotNull(masks), null, -1L, null);
    }

    static DvPartEvent derived(DvBucketMaskState masks) {
        return new DvPartEvent(Kind.DERIVED, null, null, checkNotNull(masks), null, -1L, null);
    }

    static DvPartEvent replanned(DvPartMachine.Target target) {
        return new DvPartEvent(Kind.REPLANNED, null, null, null, null, -1L, checkNotNull(target));
    }

    /**
     * The failure of a request to the bucket's leader, as the part tells them apart. A no-wait
     * answer without masks comes as a failure too ({@link DvMaskFetcher}).
     */
    static DvPartEvent of(Throwable e) {
        if (e instanceof InterruptedException) {
            return failed(Kind.INTERRUPTED, e);
        }
        if (ExceptionUtils.findThrowable(e, LakeTableRolledBackException.class).isPresent()) {
            return failed(Kind.ROLLED_BACK, e);
        }
        if (ExceptionUtils.findThrowable(e, PartitionNotExistException.class).isPresent()) {
            return failed(Kind.PARTITION_DROPPED, e);
        }
        long foldCut = DvMaskFetcher.rebuildingCut(e);
        if (foldCut >= 0) {
            return new DvPartEvent(Kind.REBUILDING, e, null, null, null, foldCut, null);
        }
        StaleSnapshotException stale = DvMaskFetcher.stale(e);
        if (stale != null) {
            return new DvPartEvent(Kind.COVERED, e, null, null, stale, -1L, null);
        }
        if (ExceptionUtils.findThrowable(e, DeletionVectorsNotReadyException.class).isPresent()) {
            return failed(Kind.NOT_READY, e);
        }
        if (ExceptionUtils.findThrowable(e, InvalidMetadataException.class).isPresent()) {
            return failed(Kind.LEADER_LOST, e);
        }
        return failed(Kind.ERROR, e);
    }

    /**
     * A failure that is not an answer of the leader about its masks, such as a failed lake or log
     * read, a lake snapshot check or a plan: an interrupt, a rolled-back lake, or an error.
     */
    static DvPartEvent failed(Throwable e) {
        if (e instanceof InterruptedException) {
            return failed(Kind.INTERRUPTED, e);
        }
        if (ExceptionUtils.findThrowable(e, LakeTableRolledBackException.class).isPresent()) {
            return failed(Kind.ROLLED_BACK, e);
        }
        return failed(Kind.ERROR, e);
    }

    private static DvPartEvent failed(Kind kind, Throwable e) {
        return new DvPartEvent(kind, checkNotNull(e), null, null, null, -1L, null);
    }

    @Override
    public String toString() {
        return kind.name();
    }
}
