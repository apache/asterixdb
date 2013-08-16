/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.pregelix.dataflow.util;

import java.io.InputStream;

public class ResetableByteArrayInputStream extends InputStream {

    private byte[] data;
    private int position;

    public ResetableByteArrayInputStream() {
    }

    public void setByteArray(byte[] data, int position) {
        this.data = data;
        this.position = position;
    }

    @Override
    public int read() {
        int remaining = data.length - position;
        int value = remaining > 0 ? (data[position++] & 0xff) : -1;
        return value;
    }

    @Override
    public int read(byte[] bytes, int offset, int length) {
        int remaining = data.length - position;
        if (remaining == 0) {
            return -1;
        }
        int l = Math.min(length, remaining);
        System.arraycopy(data, position, bytes, offset, l);
        position += l;
        return l;
    }

    @Override
    public int available() {
        return data.length - position;
    }
}