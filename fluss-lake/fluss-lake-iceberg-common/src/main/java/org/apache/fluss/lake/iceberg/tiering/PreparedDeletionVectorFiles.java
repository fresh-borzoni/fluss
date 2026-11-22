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

import org.apache.fluss.lake.dv.PreparedDeletionVectors;

import org.apache.iceberg.DeleteFile;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * The deletion vectors a tiering writer prepared for its bucket's round, in one Puffin file, and
 * for each data file they delete rows in, the deletion vector they replace if it had one.
 */
final class PreparedDeletionVectorFiles implements Serializable {

    private static final long serialVersionUID = 1L;

    final PreparedDeletionVectors round;
    final List<DeleteFile> deletionVectors;
    // data file location -> the deletion vector its new one replaces; absent if it had none
    final Map<String, DeletionVectorId> replaced;

    PreparedDeletionVectorFiles(
            PreparedDeletionVectors round,
            List<DeleteFile> deletionVectors,
            Map<String, DeletionVectorId> replaced) {
        this.round = round;
        this.deletionVectors = Collections.unmodifiableList(deletionVectors);
        this.replaced = Collections.unmodifiableMap(replaced);
    }

    @Override
    public String toString() {
        return "PreparedDeletionVectorFiles{"
                + round
                + ", deletionVectors="
                + deletionVectors.size()
                + ", replaced="
                + replaced.size()
                + '}';
    }
}
