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

import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

public class UTF8CharSequence implements CharSequence {

    private char[] buf;
    private int length;

    @Override
    public char charAt(int index) {
        if (index >= length || index < 0) {
            throw new IndexOutOfBoundsException("No index " + index + " for string of length " + length);
        }
        return buf[index];
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        UTF8CharSequence carSeq = new UTF8CharSequence();
        carSeq.length = end - start;
        if (end != start) {
            carSeq.buf = new char[carSeq.length];
            System.arraycopy(buf, start, carSeq.buf, 0, carSeq.length);
        }
        return carSeq;
    }

    public void reset(UTF8StringPointable valuePtr) {
        int utfLen = valuePtr.getUTF8Length();
        if (buf == null || buf.length < utfLen) {
            buf = new char[utfLen];
        }
        int bytePos = 0;
        int charPos = 0;
        while (bytePos < utfLen) {
            buf[charPos++] = valuePtr.charAt(valuePtr.getMetaDataLength() + bytePos);
            bytePos += valuePtr.charSize(valuePtr.getMetaDataLength() + bytePos);
        }
        this.length = charPos;
    }

    @Override
    public String toString() {
        return new String(buf, 0, length);
    }

}
