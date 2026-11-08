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

package org.apache.fluss.row.encode;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.utils.ByteArraySlice;
import org.apache.fluss.utils.UnsafeUtils;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Physical layout of raw KV value bytes.
 *
 * <p>PLAIN (version 1): {@code [little-endian schema id][BinaryRow payload]}.
 *
 * <p>TAGGED (version 2): {@code [big-endian long tag][little-endian schema id][BinaryRow payload]}.
 * The tag is an opaque signed 64-bit value whose semantics are defined by the producer and consumer
 * of the selected layout. Its byte order follows FRocksDB's native {@code DeserializeTimestamp}
 * contract, which reads the eight-byte value most-significant byte first.
 *
 * <p>VERSIONED (version 3): {@code [big-endian long version offset][big-endian long TTL
 * timestamp][little-endian schema id][BinaryRow payload]}. Two fixed slots with fixed meanings, for
 * a table with deletion vectors and row TTL: the version offset is the log offset of the change
 * that wrote the value, and the TTL timestamp is the time row TTL expires the value by, or {@link
 * #NEVER_EXPIRE_TIMESTAMP} for a value that never expires. So row TTL and the version offsets do
 * not share a field, and one read of the value returns both. The TTL timestamp follows the byte
 * order of the tag, so the native TTL compaction filter reads it at its slot as it reads the tag of
 * TAGGED at 0.
 *
 * <pre>
 *   byte  0                8                16      18
 *         +----------------+----------------+-------+------------------+
 *         | version offset | TTL timestamp  | schema| BinaryRow payload |
 *         | (BE long)      | (BE long)      | id(LE)|                   |
 *         +----------------+----------------+-------+------------------+
 * </pre>
 */
@Internal
public final class KvValueLayout {

    private static final int SCHEMA_ID_LENGTH = 2;
    private static final int VALUE_TAG_LENGTH = 8;
    private static final int SLOT_LENGTH = 8;
    private static final int NO_OFFSET = -1;

    /**
     * The TTL timestamp of a value row TTL never expires: of a table without row TTL, or of a row
     * whose TTL time column is null. Adding a TTL to it cannot overflow.
     */
    public static final long NEVER_EXPIRE_TIMESTAMP = Long.MAX_VALUE / 2;

    public static final KvValueLayout PLAIN =
            new KvValueLayout(1, 0, 0, 2, NO_OFFSET, NO_OFFSET, NO_OFFSET);
    public static final KvValueLayout TAGGED =
            new KvValueLayout(2, 8, 8, 10, 0, NO_OFFSET, NO_OFFSET);
    public static final KvValueLayout VERSIONED = new KvValueLayout(3, 16, 16, 18, NO_OFFSET, 0, 8);

    private final int version;
    private final int valueBodyOffset;
    private final int schemaIdOffset;
    private final int rowPayloadOffset;
    private final int valueTagOffset;
    private final int versionOffsetSlot;
    private final int ttlTimestampSlot;

    private KvValueLayout(
            int version,
            int valueBodyOffset,
            int schemaIdOffset,
            int rowPayloadOffset,
            int valueTagOffset,
            int versionOffsetSlot,
            int ttlTimestampSlot) {
        this.version = version;
        this.valueBodyOffset = valueBodyOffset;
        this.schemaIdOffset = schemaIdOffset;
        this.rowPayloadOffset = rowPayloadOffset;
        this.valueTagOffset = valueTagOffset;
        this.versionOffsetSlot = versionOffsetSlot;
        this.ttlTimestampSlot = ttlTimestampSlot;
    }

    /** Returns the layout for the persisted version. */
    public static KvValueLayout fromVersion(int version) {
        switch (version) {
            case 1:
                return PLAIN;
            case 2:
                return TAGGED;
            case 3:
                return VERSIONED;
            default:
                throw new IllegalArgumentException(
                        "Unsupported KV value layout version " + version + ".");
        }
    }

    /** Returns the layout persisted in the table config, defaulting to the plain layout. */
    public static KvValueLayout fromTableConfig(TableConfig tableConfig) {
        if (tableConfig.getKvValueLayoutVersion().isPresent()) {
            return fromVersion(tableConfig.getKvValueLayoutVersion().get());
        }
        if (!tableConfig.isDeletionVectorsEnabled()) {
            return PLAIN;
        }
        // Deletion vectors keep the log offset of each value's version in the tag. Row TTL needs a
        // field of its own then, which only the versioned layout has.
        return tableConfig.getKvTTL().isPresent() ? VERSIONED : TAGGED;
    }

    /** Returns the persisted version of this layout. */
    public int version() {
        return version;
    }

    /** Returns the byte offset of the RPC value body in a raw KV value. */
    public int valueBodyOffset() {
        return valueBodyOffset;
    }

    /** Returns the RPC value body length for a raw KV value length. */
    public int valueBodyLength(int valueLength) {
        return valueLength - valueBodyOffset;
    }

    /** Returns the RPC value body as a zero-copy slice of a raw KV value. */
    public @Nullable ByteArraySlice toValueBodySlice(@Nullable byte[] value) {
        if (value == null) {
            return null;
        }
        return ByteArraySlice.wrap(value, valueBodyOffset(), valueBodyLength(value.length));
    }

    /** Returns the byte offset of schema id in a raw KV value. */
    public int schemaIdOffset() {
        return schemaIdOffset;
    }

    /** Returns the encoded schema id length in bytes. */
    public int schemaIdLength() {
        return SCHEMA_ID_LENGTH;
    }

    /**
     * Returns whether the raw KV value has an internal value tag, the opaque field of TAGGED. The
     * slots of VERSIONED have fixed meanings and are read and written by their own methods.
     */
    public boolean hasValueTag() {
        return valueTagOffset != NO_OFFSET;
    }

    /** Returns the byte offset of the internal value tag. */
    public int valueTagOffset() {
        checkState(hasValueTag(), "KV value layout does not have a value tag.");
        return valueTagOffset;
    }

    /** Returns the internal value tag length in bytes. */
    public int valueTagLength() {
        return hasValueTag() ? VALUE_TAG_LENGTH : 0;
    }

    /** Returns the byte offset of row payload in a raw KV value. */
    public int rowPayloadOffset() {
        return rowPayloadOffset;
    }

    /** Returns the row payload length for a raw KV value length. */
    public int rowPayloadLength(int valueLength) {
        if (valueLength < rowPayloadOffset) {
            throw new IllegalArgumentException(
                    "valueLength must be at least row payload offset "
                            + rowPayloadOffset
                            + ", but was "
                            + valueLength
                            + ".");
        }
        return valueLength - rowPayloadOffset;
    }

    /** Returns whether the raw KV value has a version offset slot. */
    public boolean hasVersionOffsetSlot() {
        return versionOffsetSlot != NO_OFFSET;
    }

    /** Returns the byte offset of the version offset slot. */
    public int versionOffsetSlot() {
        checkState(hasVersionOffsetSlot(), "KV value layout does not have a version offset slot.");
        return versionOffsetSlot;
    }

    /** Returns whether the raw KV value has a TTL timestamp slot. */
    public boolean hasTtlTimestampSlot() {
        return ttlTimestampSlot != NO_OFFSET;
    }

    /** Returns the byte offset of the TTL timestamp slot. */
    public int ttlTimestampSlot() {
        checkState(hasTtlTimestampSlot(), "KV value layout does not have a TTL timestamp slot.");
        return ttlTimestampSlot;
    }

    /** Reads the version offset from a raw KV value embedded at the given offset. */
    public long readVersionOffset(MemorySegment value, int valueOffset) {
        return value.getLongBigEndian(valueOffset + versionOffsetSlot());
    }

    /** Writes the version offset to a raw KV value embedded at the given offset. */
    public void writeVersionOffset(byte[] value, int valueOffset, long versionOffset) {
        writeLongBigEndian(value, valueOffset + versionOffsetSlot(), versionOffset);
    }

    /** Reads the TTL timestamp from a raw KV value embedded at the given offset. */
    public long readTtlTimestamp(MemorySegment value, int valueOffset) {
        return value.getLongBigEndian(valueOffset + ttlTimestampSlot());
    }

    /** Writes the TTL timestamp to a raw KV value embedded at the given offset. */
    public void writeTtlTimestamp(byte[] value, int valueOffset, long ttlTimestamp) {
        writeLongBigEndian(value, valueOffset + ttlTimestampSlot(), ttlTimestamp);
    }

    /** Reads the schema id from a raw KV value. */
    public short readSchemaId(MemorySegment value) {
        return readSchemaId(value, 0);
    }

    /** Reads the schema id from a raw KV value embedded at the given offset. */
    public short readSchemaId(MemorySegment value, int valueOffset) {
        return value.getShort(valueOffset + schemaIdOffset);
    }

    /** Writes the schema id to a raw KV value. */
    public void writeSchemaId(byte[] value, short schemaId) {
        writeSchemaId(value, 0, schemaId);
    }

    /** Writes the schema id to a raw KV value embedded at the given offset. */
    public void writeSchemaId(byte[] value, int valueOffset, short schemaId) {
        short littleEndianSchemaId =
                MemorySegment.LITTLE_ENDIAN ? schemaId : Short.reverseBytes(schemaId);
        UnsafeUtils.putShort(value, valueOffset + schemaIdOffset, littleEndianSchemaId);
    }

    /**
     * Reads the opaque signed 64-bit value tag from a raw KV value. The layout does not assign
     * semantics to this field.
     */
    public long readValueTag(MemorySegment value) {
        return readValueTag(value, 0);
    }

    /**
     * Reads the opaque signed 64-bit value tag from a raw KV value embedded at the given offset.
     * The layout does not assign semantics to this field.
     */
    public long readValueTag(MemorySegment value, int valueOffset) {
        return value.getLongBigEndian(valueOffset + valueTagOffset());
    }

    /**
     * Writes an opaque signed 64-bit value tag to a raw KV value. The producer defines the tag's
     * semantics.
     */
    public void writeValueTag(byte[] value, long valueTag) {
        writeValueTag(value, 0, valueTag);
    }

    /**
     * Writes an opaque signed 64-bit value tag to a raw KV value embedded at the given offset. The
     * producer defines the tag's semantics.
     */
    public void writeValueTag(byte[] value, int valueOffset, long valueTag) {
        writeLongBigEndian(value, valueOffset + valueTagOffset(), valueTag);
    }

    private static void writeLongBigEndian(byte[] value, int offset, long field) {
        if (offset < 0 || offset > value.length - SLOT_LENGTH) {
            throw new IndexOutOfBoundsException(
                    "Cannot write an 8-byte field at offset "
                            + offset
                            + " to value of length "
                            + value.length
                            + ".");
        }
        UnsafeUtils.putLong(
                value, offset, MemorySegment.LITTLE_ENDIAN ? Long.reverseBytes(field) : field);
    }
}
