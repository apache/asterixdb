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
package org.apache.asterix.common.utils;

import it.unimi.dsi.fastutil.longs.Long2ByteMap;
import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;

public class MathUtil {

    private static final Long2ByteMap LOG2_MAP;

    static {
        LOG2_MAP = new Long2ByteOpenHashMap();
        for (byte i = 0; i < Long.SIZE; i++) {
            LOG2_MAP.put(1L << i, i);
        }
    }

    private MathUtil() {
    }

    public static long maxUnsigned(long a, long b) {
        return Long.compareUnsigned(a, b) > 0 ? a : b;
    }

    public static long minUnsigned(long a, long b) {
        return Long.compareUnsigned(a, b) < 0 ? a : b;
    }

    public static long log2Unsigned(long value) {
        final byte result = LOG2_MAP.getOrDefault(value, Byte.MIN_VALUE);
        if (result < 0) {
            throw new IllegalArgumentException("cannot resolve log2 value for " + Long.toUnsignedString(value, 16));
        }
        return result;
    }
}
