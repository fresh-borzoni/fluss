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

/**
 * The log mask of a tiering round does not match the round ({@link
 * DeletionVectorWriterInitContext#roundMasked()}). The tiering service finds most mismatches as it
 * reads the round; a lake writer throws it from {@link LakeWriter#complete()} when a key it wrote a
 * row of came again later in the round, a version the mask missed. The tiering service then reads
 * the round again from its start and has it written in sort mode; nothing of the first read is
 * committed.
 *
 * @since 1.1
 */
@PublicEvolving
public class RoundMaskMismatchException extends IOException {

    private static final long serialVersionUID = 1L;

    public RoundMaskMismatchException(String message) {
        super(message);
    }
}
