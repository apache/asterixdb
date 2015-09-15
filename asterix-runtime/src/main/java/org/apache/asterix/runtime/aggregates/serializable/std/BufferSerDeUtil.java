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
package org.apache.asterix.runtime.aggregates.serializable.std;

public class BufferSerDeUtil {

    public static double getDouble(byte[] bytes, int offset) {
        return Double.longBitsToDouble(getLong(bytes, offset));
    }

    public static float getFloat(byte[] bytes, int offset) {
        return Float.intBitsToFloat(getInt(bytes, offset));
    }

    public static boolean getBoolean(byte[] bytes, int offset) {
        if (bytes[offset] == 0)
            return false;
        else
            return true;
    }

    public static int getInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 24) + ((bytes[offset + 1] & 0xff) << 16) + ((bytes[offset + 2] & 0xff) << 8)
                + ((bytes[offset + 3] & 0xff) << 0);
    }

    public static long getLong(byte[] bytes, int offset) {
        return (((long) (bytes[offset] & 0xff)) << 56) + (((long) (bytes[offset + 1] & 0xff)) << 48)
                + (((long) (bytes[offset + 2] & 0xff)) << 40) + (((long) (bytes[offset + 3] & 0xff)) << 32)
                + (((long) (bytes[offset + 4] & 0xff)) << 24) + (((long) (bytes[offset + 5] & 0xff)) << 16)
                + (((long) (bytes[offset + 6] & 0xff)) << 8) + (((long) (bytes[offset + 7] & 0xff)) << 0);
    }

    public static void writeBoolean(boolean value, byte[] bytes, int offset) {
        if (value)
            bytes[offset] = (byte) 1;
        else
            bytes[offset] = (byte) 0;
    }

    public static void writeInt(int value, byte[] bytes, int offset) {
        bytes[offset++] = (byte) (value >> 24);
        bytes[offset++] = (byte) (value >> 16);
        bytes[offset++] = (byte) (value >> 8);
        bytes[offset++] = (byte) (value);
    }

    public static void writeLong(long value, byte[] bytes, int offset) {
        bytes[offset++] = (byte) (value >> 56);
        bytes[offset++] = (byte) (value >> 48);
        bytes[offset++] = (byte) (value >> 40);
        bytes[offset++] = (byte) (value >> 32);
        bytes[offset++] = (byte) (value >> 24);
        bytes[offset++] = (byte) (value >> 16);
        bytes[offset++] = (byte) (value >> 8);
        bytes[offset++] = (byte) (value);
    }

    public static void writeDouble(double value, byte[] bytes, int offset) {
        long lValue = Double.doubleToLongBits(value);
        writeLong(lValue, bytes, offset);
    }

    public static void writeFloat(float value, byte[] bytes, int offset) {
        int iValue = Float.floatToIntBits(value);
        writeInt(iValue, bytes, offset);
    }

}
