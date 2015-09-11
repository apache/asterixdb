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

package org.apache.hyracks.dataflow.common.data.parsers;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class ByteArrayHexParserFactory implements IValueParserFactory {
    public static ByteArrayHexParserFactory INSTANCE = new ByteArrayHexParserFactory();

    private ByteArrayHexParserFactory() {
    }

    @Override public IValueParser createValueParser() {
        return new IValueParser() {
            private byte[] buffer = new byte[] { };

            @Override public void parse(char[] input, int start, int length, DataOutput out)
                    throws HyracksDataException {
                try {
                    buffer = extractPointableArrayFromHexString(input, start, length, buffer);
                    out.write(buffer, 0, ByteArrayPointable.getFullLength(buffer, 0));
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }

    public static boolean isValidHexChar(char c) {
        if (c >= '0' && c <= '9'
                || c >= 'a' && c <= 'f'
                || c >= 'A' && c <= 'F') {
            return true;
        }
        return false;
    }

    public static byte[] extractPointableArrayFromHexString(char[] input, int start, int length,
            byte[] bufferNeedToReset) throws HyracksDataException {
        if (length % 2 != 0) {
            throw new HyracksDataException(
                    "Invalid hex string for binary type: the string length should be a muliple of 2.");
        }
        int byteLength = length / 2;
        bufferNeedToReset = ensureCapacity(byteLength + ByteArrayPointable.SIZE_OF_LENGTH, bufferNeedToReset);
        extractByteArrayFromHexString(input, start, length, bufferNeedToReset,
                ByteArrayPointable.SIZE_OF_LENGTH);
        if (byteLength > ByteArrayPointable.MAX_LENGTH) {
            throw new HyracksDataException("The decoded byte array is too long.");
        }
        ByteArrayPointable.putLength(byteLength, bufferNeedToReset, 0);
        return bufferNeedToReset;
    }

    public static byte[] extractPointableArrayFromHexString(byte[] input, int start, int length,
            byte[] bufferNeedToReset) throws HyracksDataException {
        if (length % 2 != 0) {
            throw new HyracksDataException(
                    "Invalid hex string for binary type: the string length should be a muliple of 2.");
        }
        int byteLength = length / 2;
        bufferNeedToReset = ensureCapacity(byteLength + ByteArrayPointable.SIZE_OF_LENGTH, bufferNeedToReset);
        extractByteArrayFromHexString(input, start, length, bufferNeedToReset,
                ByteArrayPointable.SIZE_OF_LENGTH);
        if (byteLength > ByteArrayPointable.MAX_LENGTH) {
            throw new HyracksDataException("The decoded byte array is too long.");
        }
        ByteArrayPointable.putLength(byteLength, bufferNeedToReset, 0);
        return bufferNeedToReset;
    }

    static byte[] ensureCapacity(int capacity, byte[] original) {
        if (original == null) {
            return new byte[capacity];
        }
        if (original.length < capacity) {
            return Arrays.copyOf(original, capacity);
        }
        return original;
    }

    private static int getValueFromValidHexChar(char c) throws HyracksDataException {
        if (!isValidHexChar(c)) {
            throw new HyracksDataException("Invalid hex character : " + c);
        }
        if (c >= '0' && c <= '9') {
            return c - '0';
        }
        if (c >= 'a' && c <= 'f') {
            return 10 + c - 'a';
        }
        return 10 + c - 'A';
    }

    private static void extractByteArrayFromHexString(char[] input, int start, int length, byte[] output,
            int offset) throws HyracksDataException {
        for (int i = 0; i < length; i += 2) {
            output[offset + i / 2] = (byte) ((getValueFromValidHexChar(input[start + i]) << 4) +
                    getValueFromValidHexChar(input[start + i + 1]));
        }
    }

    private static void extractByteArrayFromHexString(byte[] input, int start, int length, byte[] output,
            int offset) throws HyracksDataException {
        for (int i = 0; i < length; i += 2) {
            output[offset + i / 2] = (byte) ((getValueFromValidHexChar((char)input[start + i]) << 4) +
                    getValueFromValidHexChar((char)input[start + i + 1]));
        }
    }
}
