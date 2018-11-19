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

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.Arrays;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.util.ExternalDataConstants;

public class CharArrayRecord implements IRawRecord<char[]> {

    private char[] value;
    private int size;

    public CharArrayRecord(int initialCapacity) {
        value = new char[initialCapacity];
        size = 0;
    }

    public CharArrayRecord() {
        value = new char[ExternalDataConstants.DEFAULT_BUFFER_SIZE];
        size = 0;
    }

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

    public void setValue(char[] recordBuffer, int offset, int length) {
        if (value.length < length) {
            value = new char[length];
        }
        System.arraycopy(recordBuffer, offset, value, 0, length);
        size = length;
    }

    private void ensureCapacity(int len) throws IOException {
        if (value.length < len) {
            if (len > ExternalDataConstants.MAX_RECORD_SIZE) {
                throw new RuntimeDataException(ErrorCode.INPUT_RECORD_READER_CHAR_ARRAY_RECORD_TOO_LARGE,
                        ExternalDataConstants.MAX_RECORD_SIZE);
            }
            int newSize = Math.min((int) (len * ExternalDataConstants.DEFAULT_BUFFER_INCREMENT_FACTOR),
                    ExternalDataConstants.MAX_RECORD_SIZE);
            value = Arrays.copyOf(value, newSize);
        }
    }

    public void append(char[] recordBuffer, int offset, int length) throws IOException {
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
        return String.valueOf(value, 0, size == 0 ? 0 : size - 1);
    }

    public void endRecord() throws IOException {
        if (size > 0 && value[size - 1] != ExternalDataConstants.LF) {
            appendChar(ExternalDataConstants.LF);
        }
    }

    private void appendChar(char c) throws IOException {
        ensureCapacity(size + 1);
        value[size] = c;
        size++;
    }

    public void append(char[] recordBuffer) throws IOException {
        ensureCapacity(size + recordBuffer.length);
        System.arraycopy(recordBuffer, 0, value, size, recordBuffer.length);
        size += recordBuffer.length;
    }

    public void append(CharBuffer chars) throws IOException {
        ensureCapacity(size + chars.limit());
        chars.get(value, size, chars.limit());
        size += chars.limit();
    }

    @Override
    public void set(char[] value) {
        this.value = value;
        this.size = value.length;
    }

    public void set(StringBuilder builder) throws IOException {
        ensureCapacity(builder.length());
        builder.getChars(0, builder.length(), value, 0);
        this.size = builder.length();
    }

    public void set(String strValue) throws IOException {
        ensureCapacity(strValue.length());
        strValue.getChars(0, strValue.length(), value, 0);
        this.size = strValue.length();
    }
}
