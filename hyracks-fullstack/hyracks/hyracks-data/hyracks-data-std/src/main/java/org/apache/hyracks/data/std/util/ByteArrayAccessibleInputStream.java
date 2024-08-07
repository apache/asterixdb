/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.data.std.util;

import java.io.ByteArrayInputStream;

public class ByteArrayAccessibleInputStream extends ByteArrayInputStream {

    public ByteArrayAccessibleInputStream(byte[] buf, int offset, int length) {
        super(buf, offset, length);
    }

    public void setContent(byte[] buf, int offset, int length) {
        this.buf = buf;
        this.pos = offset;
        this.count = Math.min(offset + length, buf == null ? 0 : buf.length);
        this.mark = offset;
    }

    public byte[] getArray() {
        return buf;
    }

    public int getPosition() {
        return pos;
    }

    public int getCount() {
        return count;
    }

}
