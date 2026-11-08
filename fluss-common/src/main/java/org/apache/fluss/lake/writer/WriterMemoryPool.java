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

package org.apache.fluss.lake.writer;

import org.apache.fluss.annotation.PublicEvolving;

import java.io.IOException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * A heap budget that the lake writers of one tiering task share for what they buffer of a round,
 * such as the rows a writer sorts before it writes them. A writer reserves memory before it buffers
 * more. When the budget is used up, the writer that holds the most spills to local disk and
 * releases its memory; that may be the reserving writer itself. The design follows Paimon's memory
 * pool of spillable write buffers.
 *
 * <p>The writers of a pool run on one thread, the task's, so a reservation may spill another writer
 * in the middle of its round. The budget bounds what the writers hold, not the transient memory of
 * their work, such as sorting or writing a file.
 *
 * @since 1.1
 */
@PublicEvolving
public final class WriterMemoryPool {

    /** A writer that holds memory of the pool and can spill it. */
    public interface MemoryOwner {

        /** The bytes {@link #spill()} would release; 0 if the owner holds nothing to spill. */
        long spillableBytes();

        /**
         * Writes what the owner holds to local disk and releases its memory, at least {@link
         * #spillableBytes()} of it.
         */
        void spill() throws IOException;
    }

    private final long capacity;
    private final Set<MemoryOwner> owners = Collections.newSetFromMap(new IdentityHashMap<>());
    private long used;
    private long spills;

    public WriterMemoryPool(long capacityBytes) {
        checkArgument(capacityBytes > 0, "The capacity must be positive: %s", capacityBytes);
        this.capacity = capacityBytes;
    }

    public long capacity() {
        return capacity;
    }

    /** The bytes the owners hold. */
    public synchronized long used() {
        return used;
    }

    /** How many times an owner was asked to spill. */
    public synchronized long spills() {
        return spills;
    }

    public synchronized void register(MemoryOwner owner) {
        owners.add(owner);
    }

    /** Unregisters an owner, which must have released its memory. */
    public synchronized void unregister(MemoryOwner owner) {
        owners.remove(owner);
    }

    /**
     * Reserves {@code bytes} for {@code requester}, spilling the owners that hold the most until
     * the bytes fit. If nothing is left to spill, such as for a single record larger than the
     * budget, the reservation exceeds the budget instead of failing.
     */
    public synchronized void reserve(MemoryOwner requester, long bytes) throws IOException {
        checkArgument(bytes >= 0, "Cannot reserve %s bytes.", bytes);
        while (used + bytes > capacity) {
            MemoryOwner largest = null;
            long most = 0;
            for (MemoryOwner owner : owners) {
                long spillable = owner.spillableBytes();
                if (spillable > most) {
                    most = spillable;
                    largest = owner;
                }
            }
            if (largest == null) {
                break;
            }
            spills++;
            largest.spill();
        }
        used += bytes;
    }

    /** Releases bytes an owner reserved. */
    public synchronized void release(long bytes) {
        checkArgument(bytes >= 0 && bytes <= used, "Cannot release %s of %s bytes.", bytes, used);
        used -= bytes;
    }
}
