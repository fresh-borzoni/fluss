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

package org.apache.fluss.lake.paimon.tiering.mergetree;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.lake.writer.WriterMemoryPool;

import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryPoolFactory;

import java.util.Map;
import java.util.WeakHashMap;

/**
 * The Paimon write buffer that the primary key writers of one tiering task share, of the capacity
 * of the task's {@link WriterMemoryPool}. Paimon gives each table write a buffer of its own of
 * 'write-buffer-size', 256 MB by default, and tiering opens a table write per bucket, so a task
 * held that much per bucket it tiered at once. When the shared buffer is full, Paimon flushes the
 * writer that holds the most of it, as for the writers of one Flink sink task. The buffer reserves
 * nothing of the pool: it is a second allowance of the pool's capacity, next to what the pool's
 * owners reserve.
 *
 * <p>The writers of a task run on its thread, so they use its buffer one at a time. A task's buffer
 * is made when its first writer opens and dropped when its last one closes, so the writers Paimon
 * tracks in it do not pile up over rounds.
 */
@Internal
public final class SharedWriteBuffers {

    // the buffers of the tasks of this JVM, by the task's pool, which has identity equality
    private static final Map<WriterMemoryPool, Shared> BUFFERS = new WeakHashMap<>();

    private SharedWriteBuffers() {}

    /** Leases the task's buffer to a new writer, which closes the lease when it closes. */
    static Lease acquire(WriterMemoryPool taskMemory, int pageSize) {
        synchronized (BUFFERS) {
            Shared shared = BUFFERS.get(taskMemory);
            if (shared == null) {
                // at least one page
                long bytes = Math.max(taskMemory.capacity(), pageSize);
                shared =
                        new Shared(
                                new MemoryPoolFactory(new HeapMemorySegmentPool(bytes, pageSize)));
                BUFFERS.put(taskMemory, shared);
            }
            shared.writers++;
            return new Lease(taskMemory, shared);
        }
    }

    /** The bytes the writers of the task hold in its buffer, or 0 if it has none. */
    @VisibleForTesting
    public static long usedBytes(WriterMemoryPool taskMemory) {
        synchronized (BUFFERS) {
            Shared shared = BUFFERS.get(taskMemory);
            return shared == null ? 0 : shared.factory.usedBufferSize();
        }
    }

    /** Whether the task has a buffer, i.e. one of its writers is open. */
    @VisibleForTesting
    public static boolean hasBuffer(WriterMemoryPool taskMemory) {
        synchronized (BUFFERS) {
            return BUFFERS.containsKey(taskMemory);
        }
    }

    private static final class Shared {
        final MemoryPoolFactory factory;
        int writers;

        Shared(MemoryPoolFactory factory) {
            this.factory = factory;
        }
    }

    /** A writer's use of its task's write buffer. */
    static final class Lease implements AutoCloseable {
        private final WriterMemoryPool taskMemory;
        private final Shared shared;
        private boolean closed;

        private Lease(WriterMemoryPool taskMemory, Shared shared) {
            this.taskMemory = taskMemory;
            this.shared = shared;
        }

        MemoryPoolFactory factory() {
            return shared.factory;
        }

        @Override
        public void close() {
            synchronized (BUFFERS) {
                if (closed) {
                    return;
                }
                closed = true;
                if (--shared.writers == 0 && BUFFERS.get(taskMemory) == shared) {
                    BUFFERS.remove(taskMemory);
                }
            }
        }
    }
}
