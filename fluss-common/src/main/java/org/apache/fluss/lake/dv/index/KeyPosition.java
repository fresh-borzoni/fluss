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

package org.apache.fluss.lake.dv.index;

import java.io.Serializable;

/** A primary key and the position of its live row in a lake data file (basename). */
public final class KeyPosition implements Serializable {
    private static final long serialVersionUID = 1L;

    private final byte[] key;
    private final String file;
    private final long pos;

    public KeyPosition(byte[] key, String file, long pos) {
        this.key = key;
        this.file = file;
        this.pos = pos;
    }

    public byte[] key() {
        return key;
    }

    public String file() {
        return file;
    }

    public long pos() {
        return pos;
    }
}
