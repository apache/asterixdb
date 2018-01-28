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

package org.apache.hyracks.util.bytes;

public class HexParser {
    public static boolean isValidHexChar(char c) {
        if (c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
            return true;
        }
        return false;
    }

    public static int getValueFromValidHexChar(char c) {
        if (c >= '0' && c <= '9') {
            return c - '0';
        }
        if (c >= 'a' && c <= 'f') {
            return 10 + c - 'a';
        }
        if (c >= 'A' && c <= 'F') {
            return 10 + c - 'A';
        }
        throw new IllegalArgumentException("Invalid hex character : " + c);
    }

    private byte[] storage;
    private int length;

    public byte[] getByteArray() {
        return storage;
    }

    public int getLength() {
        return length;
    }

    public void generateByteArrayFromHexString(char[] input, int start, int length) {
        if (length % 2 != 0) {
            throw new IllegalArgumentException(
                    "Invalid hex string for binary type: the string length should be a muliple of 2.");
        }
        this.length = length / 2;
        ensureCapacity(this.length);
        generateByteArrayFromHexString(input, start, length, storage, 0);
    }

    public void generateByteArrayFromHexString(byte[] input, int start, int length) {
        if (length % 2 != 0) {
            throw new IllegalArgumentException(
                    "Invalid hex string for binary type: the string length should be a muliple of 2.");
        }
        this.length = length / 2;
        ensureCapacity(this.length);
        generateByteArrayFromHexString(input, start, length, storage, 0);
    }

    private void ensureCapacity(int capacity) {
        if (storage == null || storage.length < capacity) {
            storage = new byte[capacity];
        }
    }

    public static void generateByteArrayFromHexString(char[] input, int start, int length, byte[] output, int offset) {
        for (int i = 0; i < length; i += 2) {
            output[offset + i / 2] = (byte) ((getValueFromValidHexChar(input[start + i]) << 4)
                    + getValueFromValidHexChar(input[start + i + 1]));
        }
    }

    public static void generateByteArrayFromHexString(byte[] input, int start, int length, byte[] output, int offset) {
        for (int i = 0; i < length; i += 2) {
            output[offset + i / 2] = (byte) ((getValueFromValidHexChar((char) input[start + i]) << 4)
                    + getValueFromValidHexChar((char) input[start + i + 1]));
        }
    }
}
