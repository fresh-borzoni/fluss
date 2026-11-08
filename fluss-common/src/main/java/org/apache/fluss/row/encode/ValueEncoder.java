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

import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.BinaryRow;

import javax.annotation.Nullable;

import java.util.function.ToLongFunction;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/** An encoder to encode {@link BinaryRow} with a schema id as value to be stored in kv store. */
public final class ValueEncoder {

    private static final ValueEncoder PLAIN_ENCODER = new ValueEncoder(KvValueLayout.PLAIN, null);

    private final KvValueLayout kvValueLayout;

    /**
     * Generates tags for {@link #encodeValue(BinaryValue)}, or the TTL timestamps of the versioned
     * layout. {@code null} for plain values, when callers supply each tag to {@link
     * #encodeValue(BinaryValue, long)}, or for versioned values that never expire.
     */
    @Nullable private final ToLongFunction<BinaryRow> valueTagProvider;

    private ValueEncoder(
            KvValueLayout kvValueLayout, @Nullable ToLongFunction<BinaryRow> valueTagProvider) {
        this.kvValueLayout = kvValueLayout;
        this.valueTagProvider = valueTagProvider;
    }

    /**
     * Returns an encoder for the given layout. Tagged values must supply their tag to {@link
     * #encodeValue(BinaryValue, long)}, and versioned values their version offset, with a TTL
     * timestamp that never expires.
     */
    public static ValueEncoder forLayout(KvValueLayout kvValueLayout) {
        checkNotNull(kvValueLayout, "kvValueLayout must not be null.");
        return kvValueLayout == KvValueLayout.PLAIN
                ? PLAIN_ENCODER
                : new ValueEncoder(kvValueLayout, null);
    }

    /**
     * Returns an encoder for a layout with an internal value tag, or for the versioned layout,
     * whose TTL timestamps {@code valueTagProvider} gives. Versioned values must supply their
     * version offset to {@link #encodeValue(BinaryValue, long)}.
     */
    public static ValueEncoder forLayout(
            KvValueLayout kvValueLayout, ToLongFunction<BinaryRow> valueTagProvider) {
        checkNotNull(kvValueLayout, "kvValueLayout must not be null.");
        checkNotNull(valueTagProvider, "valueTagProvider must not be null.");
        if (!kvValueLayout.hasValueTag() && !kvValueLayout.hasTtlTimestampSlot()) {
            throw new IllegalArgumentException(
                    "A value tag provider is not supported for this KV value layout.");
        }
        return new ValueEncoder(kvValueLayout, valueTagProvider);
    }

    /**
     * Whether the encoder writes a tag, or a version offset, that each caller supplies ({@link
     * #encodeValue(BinaryValue, long)}).
     */
    public boolean takesExplicitTags() {
        return kvValueLayout.hasVersionOffsetSlot()
                || (kvValueLayout.hasValueTag() && valueTagProvider == null);
    }

    /** Returns whether this encoder writes an internal value tag before the row bytes. */
    public boolean hasValueTag() {
        return kvValueLayout.hasValueTag();
    }

    /**
     * Encodes a binary value using the layout bound to this encoder.
     *
     * <p>For tagged layouts, this method requires a value tag provider supplied when creating the
     * encoder. Callers supplying tags explicitly must use {@link #encodeValue(BinaryValue, long)}.
     *
     * @throws IllegalStateException if the layout is tagged and no value tag provider was supplied,
     *     or the layout is versioned, whose values need their version offset
     */
    public byte[] encodeValue(BinaryValue value) {
        checkState(
                !kvValueLayout.hasVersionOffsetSlot(),
                "An explicit version offset is required for this KV value encoder.");
        if (kvValueLayout.hasValueTag()) {
            checkState(
                    valueTagProvider != null,
                    "An explicit value tag is required for this KV value encoder.");
            return encodeValue(value, valueTagProvider.applyAsLong(value.row));
        }
        return encodeValueBody(value);
    }

    /**
     * Encodes a binary value with the supplied opaque value tag, or, in the versioned layout, with
     * the supplied version offset and the TTL timestamp of its row.
     */
    public byte[] encodeValue(BinaryValue value, long valueTag) {
        if (kvValueLayout.hasVersionOffsetSlot()) {
            byte[] values = encodeValueBody(value);
            kvValueLayout.writeVersionOffset(values, 0, valueTag);
            kvValueLayout.writeTtlTimestamp(
                    values,
                    0,
                    valueTagProvider == null
                            ? KvValueLayout.NEVER_EXPIRE_TIMESTAMP
                            : valueTagProvider.applyAsLong(value.row));
            return values;
        }
        checkState(
                kvValueLayout.hasValueTag(),
                "An explicit value tag is not supported for this KV value layout.");
        byte[] values = encodeValueBody(value);
        kvValueLayout.writeValueTag(values, valueTag);
        return values;
    }

    private byte[] encodeValueBody(BinaryValue value) {
        int rowPayloadOffset = kvValueLayout.rowPayloadOffset();
        byte[] values = new byte[rowPayloadOffset + value.row.getSizeInBytes()];
        kvValueLayout.writeSchemaId(values, value.schemaId);
        value.row.copyTo(values, rowPayloadOffset);
        return values;
    }

    /** Encodes a plain binary value for callers that do not select a layout explicitly. */
    public static byte[] encodeValue(short schemaId, BinaryRow row) {
        return PLAIN_ENCODER.encodeValue(new BinaryValue(schemaId, row));
    }
}
