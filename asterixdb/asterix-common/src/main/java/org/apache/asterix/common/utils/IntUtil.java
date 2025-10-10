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

import it.unimi.dsi.fastutil.ints.IntIterator;

public class IntUtil {
    private IntUtil() {
        throw new AssertionError("do not instantiate");
    }

    /**
     * Returns a compact string representation of the supplied {@link IntIterator}, enclosed
     * by square-brackets ([]), comma-delimited, collapsing ranges together with a hyphen (-).
     * Only provides reasonable results if the contents of the iterator are sorted.
     */
    public static String toCompactString(IntIterator iter) {
        return toCompactString(iter, 0);
    }

    /**
     * Returns a compact string representation of the supplied {@link IntIterator}, enclosed
     * by square-brackets ([]), comma-delimited, collapsing ranges together with a hyphen (-).
     * Only provides reasonable results if the contents of the iterator are sorted.
     */
    static String toCompactString(IntIterator iter, int delta) {
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
     * Returns a compact string representation of the supplied int array, enclosed
     * by square-brackets ([]), comma-delimited, collapsing ranges together with a hyphen (-).
     * Only provides reasonable results if the contents of the iterator are sorted.
     */
    public static String toCompactString(int[] iter) {
        return toCompactString(wrap(iter));
    }

    /**
     * Appends the contents of the supplied {@link IntIterator} to the {@link StringBuilder} instance,
     * comma-delimited, collapsing ranges together with a hyphen (-).  Only provides reasonable
     * results if the contents of the iterator are sorted.
     */
    public static void appendCompact(IntIterator iter, StringBuilder builder) {
        appendCompact(iter, builder, 0);
    }

    /**
     * Appends the contents of the supplied {@link IntIterator} to the {@link StringBuilder} instance,
     * comma-delimited, collapsing ranges together with a hyphen (-).  Only provides reasonable
     * results if the contents of the iterator are sorted.
     */
    static void appendCompact(IntIterator iter, StringBuilder builder, int outputDelta) {
        int rangeStart = iter.nextInt();
        builder.append(rangeStart + outputDelta);
        int current = rangeStart;
        int prev = current;
        while (iter.hasNext()) {
            current = iter.nextInt();
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

    public static IntIterator wrap(int... ints) {
        return new IntIterator() {
            int index = 0;

            @Override
            public int nextInt() {
                try {
                    return ints[index++];
                } catch (ArrayIndexOutOfBoundsException e) {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public boolean hasNext() {
                return index < ints.length;
            }
        };
    }
}
