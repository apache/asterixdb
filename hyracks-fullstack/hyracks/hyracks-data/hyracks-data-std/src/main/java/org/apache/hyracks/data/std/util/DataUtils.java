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
package org.apache.hyracks.data.std.util;

import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

public class DataUtils {

    private DataUtils() {
    }

    /**
     * Copies the content of this pointable to the passed byte array.
     * the array is expected to be at least of length = length of this pointable
     *
     * @param value
     *            the value to be copied
     * @param copy
     *            the array to write into
     * @throws ArrayIndexOutOfBoundsException
     *             if the passed array size is smaller than length
     */
    public static void copyInto(IValueReference value, byte[] copy) {
        System.arraycopy(value.getByteArray(), value.getStartOffset(), copy, 0, value.getLength());
    }

    /**
     * Copies the content of this pointable to the passed byte array.
     * the array is expected to be at least of length = offset + length of this pointable
     *
     * @param value
     *            the value to be copied
     * @param copy
     *            the array to write into
     * @param offset
     *            the offset to start writing from
     * @throws ArrayIndexOutOfBoundsException
     *             if the passed array size - offset is smaller than length
     */
    public static void copyInto(IValueReference value, byte[] copy, int offset) {
        System.arraycopy(value.getByteArray(), value.getStartOffset(), copy, offset, value.getLength());
    }

    /**
     * Check whether two value references are equals
     *
     * @param first
     *            first value
     * @param second
     *            second value
     * @return true if the two values are equal, false otherwise
     */
    public static boolean equals(IValueReference first, IValueReference second) { // NOSONAR
        if (first.getLength() != second.getLength()) {
            return false;
        }
        return equalsInRange(first.getByteArray(), first.getStartOffset(), second.getByteArray(),
                second.getStartOffset(), first.getLength());
    }

    /**
     * Check whether subranges of two byte arrays are equal
     *
     * @param arr1
     *            first array
     * @param offset1
     *            first offset
     * @param arr2
     *            second array
     * @param offset2
     *            second offset
     * @param length
     *            the length of the window
     * @return true if the two arrays have equal subranges, false otherwise
     */
    public static boolean equalsInRange(byte[] arr1, int offset1, byte[] arr2, int offset2, int length) {
        for (int i = 0; i < length; i++) {
            if (arr1[offset1 + i] != arr2[offset2 + i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Compare two value references using given comparator
     *
     * @param first
     *            first value
     * @param second
     *            second value
     * @param cmp
     *            comparator
     */
    public static int compare(IValueReference first, IValueReference second, IBinaryComparator cmp)
            throws HyracksDataException {
        return cmp.compare(first.getByteArray(), first.getStartOffset(), first.getLength(), second.getByteArray(),
                second.getStartOffset(), second.getLength());
    }

    public static void ensureLengths(int requiredLength, int length1, int length2) {
        if (length1 != requiredLength || length2 != requiredLength) {
            throw new IllegalStateException();
        }
    }
}
