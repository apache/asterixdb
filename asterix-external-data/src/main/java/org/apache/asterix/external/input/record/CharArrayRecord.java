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
package org.apache.asterix.external.input.record;

import java.util.Arrays;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.util.ExternalDataConstants;

public class CharArrayRecord implements IRawRecord<char[]> {

    private char[] value;
    private int size;

    @Override
    public byte[] getBytes() {
        return new String(value).getBytes();
    }

    @Override
    public char[] get() {
        return value;
    }

    @Override
    public int size() {
        return size;
    }

    public CharArrayRecord(int initialCapacity) {
        value = new char[initialCapacity];
        size = 0;
    }

    public CharArrayRecord() {
        value = new char[ExternalDataConstants.DEFAULT_BUFFER_SIZE];
        size = 0;
    }

    public void setValue(char[] recordBuffer, int offset, int length) {
        if (value.length < length) {
            value = new char[length];
        }
        System.arraycopy(recordBuffer, offset, value, 0, length);
        size = length;
    }

    private void ensureCapacity(int len) {
        if (value.length < len) {
            value = Arrays.copyOf(value, (int) (len * 1.25));
        }
    }

    public void append(char[] recordBuffer, int offset, int length) {
        ensureCapacity(size + length);
        System.arraycopy(recordBuffer, offset, value, size, length);
        size += length;
    }

    @Override
    public void reset() {
        size = 0;
    }

    @Override
    public String toString() {
        return String.valueOf(value, 0, size);
    }

    public void setValue(char[] value) {
        this.value = value;
    }

    public void endRecord() {
        if (value[size - 1] != ExternalDataConstants.LF) {
            appendChar(ExternalDataConstants.LF);
        }
    }

    private void appendChar(char c) {
        ensureCapacity(size + 1);
        value[size] = c;
        size++;
    }

    @Override
    public Class<char[]> getRecordClass() {
        return char[].class;
    }
}