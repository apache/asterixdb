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

import java.util.NoSuchElementException;

import it.unimi.dsi.fastutil.shorts.ShortIterator;

public class ShortUtil {
    private ShortUtil() {
        throw new AssertionError("do not instantiate");
    }

    /**
     * Returns a compact string representation of the supplied {@link ShortIterator}, enclosed
     * by square-brackets ([]), comma-delimited, collapsing ranges together with a hyphen (-).
     * Only provides reasonable results if the contents of the iterator are sorted.
     */
    public static String toCompactString(ShortIterator iter) {
        return toCompactString(iter, (short) 0);
    }

    /**
     * Returns a compact string representation of the supplied {@link ShortIterator}, enclosed
     * by square-brackets ([]), comma-delimited, collapsing ranges together with a hyphen (-).
     * Only provides reasonable results if the contents of the iterator are sorted.
     */
    static String toCompactString(ShortIterator iter, short delta) {
        if (!iter.hasNext()) {
            return "[]";
        }
        StringBuilder builder = new StringBuilder();
        builder.append('[');
        appendCompact(iter, builder, delta);
        builder.append(']');
        return builder.toString();
    }

    /**
     * Returns a compact string representation of the supplied short array, enclosed
     * by square-brackets ([]), comma-delimited, collapsing ranges together with a hyphen (-).
     * Only provides reasonable results if the contents of the iterator are sorted.
     */
    public static String toCompactString(short[] iter) {
        return toCompactString(wrap(iter));
    }

    /**
     * Appends the contents of the supplied {@link ShortIterator} to the {@link StringBuilder} instance,
     * comma-delimited, collapsing ranges together with a hyphen (-).  Only provides reasonable
     * results if the contents of the iterator are sorted.
     */
    public static void appendCompact(ShortIterator iter, StringBuilder builder) {
        appendCompact(iter, builder, (short) 0);
    }

    /**
     * Appends the contents of the supplied {@link ShortIterator} to the {@link StringBuilder} instance,
     * comma-delimited, collapsing ranges together with a hyphen (-).  Only provides reasonable
     * results if the contents of the iterator are sorted.
     */
    static void appendCompact(ShortIterator iter, StringBuilder builder, short outputDelta) {
        if (!iter.hasNext()) {
            return;
        }
        short rangeStart = iter.nextShort();
        builder.append(rangeStart + outputDelta);
        short current = rangeStart;
        short prev = current;
        while (iter.hasNext()) {
            current = iter.nextShort();
            if (current != prev + 1) {
                // end any range we were in:
                if (rangeStart != prev) {
                    builder.append('-').append(prev + outputDelta);
                }
                builder.append(",").append(current + outputDelta);
                rangeStart = current;
            }
            prev = current;
        }
        if (rangeStart != prev) {
            builder.append('-').append(prev + outputDelta);
        }
    }

    public static ShortIterator wrap(short... shorts) {
        return new ShortIterator() {
            int index = 0;

            @Override
            public short nextShort() {
                try {
                    return shorts[index++];
                } catch (ArrayIndexOutOfBoundsException e) {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public boolean hasNext() {
                return index < shorts.length;
            }
        };
    }
}
