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

package org.apache.fluss.flink.source.epochs;

/**
 * Constants of epochs: the numbers that a scan cuts its changes into, and the positions in a bucket
 * at which it ends them.
 *
 * <p>An epoch ends at a position per bucket: the next log offset of the bucket after the last
 * change of the epoch, so that a lookup at that position sees exactly the changes of the epochs up
 * to it. A bucket whose scan starts with a snapshot has two more positions: {@link #EMPTY} before
 * the snapshot, where the table is empty, and {@link #IN_SNAPSHOT} while the scan emits it, where
 * no epoch can end.
 */
public final class Epochs {

    /** The epoch before the first one: the rows a scan started from. */
    public static final long BEFORE_FIRST = -1L;

    /** An epoch after every epoch a scan ends; a scan ends it at the end of its input. */
    public static final long AFTER_LAST = Long.MAX_VALUE - 3;

    /** The position of a bucket before the snapshot that its scan emits: an empty table. */
    public static final long EMPTY = -1L;

    /** The position of a bucket while its scan emits a snapshot, where no epoch can end. */
    public static final long IN_SNAPSHOT = Long.MIN_VALUE;

    private Epochs() {}

    /**
     * Orders positions: {@link #EMPTY} before {@link #IN_SNAPSHOT} before every log offset.
     * Positions compare by their ranks.
     */
    public static long rank(long position) {
        if (position == IN_SNAPSHOT) {
            return -1;
        } else if (position == EMPTY) {
            return -2;
        }
        return 2 * position;
    }

    /** A position for logs, with the special positions by name. */
    public static String positionToString(long position) {
        if (position == IN_SNAPSHOT) {
            return "in-snapshot";
        } else if (position == EMPTY) {
            return "empty";
        }
        return Long.toString(position);
    }
}
