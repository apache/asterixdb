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

package org.apache.hyracks.util;

public class IntSerDeUtils {

    public static int getInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 24) + ((bytes[offset + 1] & 0xff) << 16) + ((bytes[offset + 2] & 0xff) << 8)
                + ((bytes[offset + 3] & 0xff) << 0);
    }

    /**
     * put integer value into the array bytes at the offset offset
     *
     * @param bytes
     *            byte array to put data in
     * @param offset
     *            offset from the beginning of the array to write the {@code value} in
     * @param value
     *            value to write to {@code bytes[offset]}
     */
    public static void putInt(byte[] bytes, int offset, int value) {
        // TODO: there is another implementation in IntegerPointable
        bytes[offset++] = (byte) (value >> 24);
        bytes[offset++] = (byte) (value >> 16);
        bytes[offset++] = (byte) (value >> 8);
        bytes[offset++] = (byte) (value);
    }
}
