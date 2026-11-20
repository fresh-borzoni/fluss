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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.util.ContentFileUtil;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/** A deletion vector by where its blob is: the Puffin file, the blob's offset and its size. */
final class DeletionVectorId implements Serializable {

    private static final long serialVersionUID = 1L;

    final String location;
    @Nullable final Long offset;
    @Nullable final Long size;

    DeletionVectorId(String location, @Nullable Long offset, @Nullable Long size) {
        this.location = location;
        this.offset = offset;
        this.size = size;
    }

    static DeletionVectorId of(DeleteFile dv) {
        return new DeletionVectorId(dv.location(), dv.contentOffset(), dv.contentSizeInBytes());
    }

    /**
     * The deletion vector among a data file's current delete files, if they are exactly one
     * deletion vector; else null.
     */
    @Nullable
    static DeleteFile onlyDeletionVector(List<DeleteFile> deletes) {
        return deletes.size() == 1 && ContentFileUtil.isDV(deletes.get(0)) ? deletes.get(0) : null;
    }

    boolean isOf(DeleteFile dv) {
        return location.equals(dv.location())
                && Objects.equals(offset, dv.contentOffset())
                && Objects.equals(size, dv.contentSizeInBytes());
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DeletionVectorId)) {
            return false;
        }
        DeletionVectorId that = (DeletionVectorId) o;
        return location.equals(that.location)
                && Objects.equals(offset, that.offset)
                && Objects.equals(size, that.size);
    }

    @Override
    public int hashCode() {
        return Objects.hash(location, offset, size);
    }

    @Override
    public String toString() {
        return location + "@" + offset + "+" + size;
    }
}
