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
package org.apache.asterix.om.utils;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

/**
 * Unsafe accessor to serialize/deserialize bytes. This is intended for little-indian machines
 */
@SuppressWarnings("restriction")
public class UnsafeUtil {
    private static sun.misc.Unsafe UNSAFE = getUnsafe();

    private static final long BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    private UnsafeUtil() {

    }

    static sun.misc.Unsafe getUnsafe() {
        sun.misc.Unsafe unsafe = null;
        try {
            unsafe = AccessController.doPrivileged(new PrivilegedExceptionAction<>() {
                @Override
                public sun.misc.Unsafe run() throws Exception {
                    Class<sun.misc.Unsafe> k = sun.misc.Unsafe.class;

                    for (Field f : k.getDeclaredFields()) {
                        f.setAccessible(true);
                        Object x = f.get(null);
                        if (k.isInstance(x)) {
                            return k.cast(x);
                        }
                    }
                    return null;
                }
            });
        } catch (Throwable e) { //NOSONAR

        }
        return unsafe;
    }

    public static byte getByte(byte[] target, int offset) {
        return UNSAFE.getByte(target, BYTE_ARRAY_BASE_OFFSET + offset);
    }

    public static void putByte(Object target, int offset, byte value) {
        UNSAFE.putByte(target, BYTE_ARRAY_BASE_OFFSET + offset, value);
    }

    public static short getShort(byte[] target, int offset) {
        return Short.reverseBytes(UNSAFE.getShort(target, BYTE_ARRAY_BASE_OFFSET + offset));
    }

    public static void putShort(Object target, int offset, short value) {
        UNSAFE.putShort(target, BYTE_ARRAY_BASE_OFFSET + offset, Short.reverseBytes(value));
    }

    public static int getInt(byte[] target, int offset) {
        return Integer.reverseBytes(UNSAFE.getInt(target, BYTE_ARRAY_BASE_OFFSET + offset));
    }

    public static void putInt(byte[] target, int offset, int value) {
        UNSAFE.putInt(target, BYTE_ARRAY_BASE_OFFSET + offset, Integer.reverseBytes(value));
    }

    public static long getLong(byte[] target, int offset) {
        return Long.reverseBytes(UNSAFE.getLong(target, BYTE_ARRAY_BASE_OFFSET + offset));
    }

    public static void putLong(byte[] target, int offset, long value) {
        UNSAFE.putLong(target, BYTE_ARRAY_BASE_OFFSET + offset, Long.reverseBytes(value));
    }

    public static float getFloat(byte[] target, int offset) {
        return Float.intBitsToFloat(Integer.reverseBytes(UNSAFE.getInt(target, BYTE_ARRAY_BASE_OFFSET + offset)));
    }

    public static void putFloat(byte[] target, int offset, float value) {
        UNSAFE.putInt(target, BYTE_ARRAY_BASE_OFFSET + offset, Integer.reverseBytes(Float.floatToIntBits(value)));
    }

    public static double getDouble(byte[] target, int offset) {
        return Double.longBitsToDouble(Long.reverseBytes(UNSAFE.getLong(target, BYTE_ARRAY_BASE_OFFSET + offset)));
    }

    public static void putDouble(byte[] target, int offset, double value) {
        UNSAFE.putLong(target, BYTE_ARRAY_BASE_OFFSET + offset, Long.reverseBytes(Double.doubleToLongBits(value)));
    }
}
