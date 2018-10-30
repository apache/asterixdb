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

package org.apache.asterix.runtime.evaluators.common;

import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

/**
 * Utility methods for number handling
 */
public final class NumberUtils {

    public static final UTF8StringPointable POSITIVE_INF = UTF8StringPointable.generateUTF8Pointable("INF");

    public static final UTF8StringPointable NEGATIVE_INF = UTF8StringPointable.generateUTF8Pointable("-INF");

    public static final UTF8StringPointable NAN = UTF8StringPointable.generateUTF8Pointable("NaN");

    public static final long NAN_BITS = Double.doubleToLongBits(Double.NaN);

    public static final long POSITIVE_ZERO_BITS = Double.doubleToLongBits(+0.0d);

    public static final long NEGATIVE_ZERO_BITS = Double.doubleToLongBits(-0.0d);

    /**
     * Parses string as double
     * @param textPtr input string
     * @param result placeholder for the result
     * @return {@code true} if parsing was successful, {@code false} otherwise
     */
    public static boolean parseDouble(UTF8StringPointable textPtr, AMutableDouble result) {
        double v;
        if (POSITIVE_INF.compareTo(textPtr) == 0) {
            v = Double.POSITIVE_INFINITY;
        } else if (NEGATIVE_INF.compareTo(textPtr) == 0) {
            v = Double.NEGATIVE_INFINITY;
        } else if (NAN.compareTo(textPtr) == 0) {
            v = Double.NaN;
        } else {
            try {
                v = Double.parseDouble(textPtr.toString());
            } catch (NumberFormatException e) {
                return false;
            }
        }
        result.setValue(v);
        return true;
    }

    /**
     * Parses string as bigint
     * @param textPtr input string
     * @param result placeholder for the result
     * @return {@code true} if parsing was successful, {@code false} otherwise
     */
    public static boolean parseInt64(UTF8StringPointable textPtr, AMutableInt64 result) {
        byte[] bytes = textPtr.getByteArray();
        int offset = textPtr.getCharStartOffset();
        //accumulating value in negative domain
        //otherwise Long.MIN_VALUE = -(Long.MAX_VALUE + 1) would have caused overflow
        long value = 0;
        boolean positive = true;
        long limit = -Long.MAX_VALUE;
        if (bytes[offset] == '+') {
            offset++;
        } else if (bytes[offset] == '-') {
            offset++;
            positive = false;
            limit = Long.MIN_VALUE;
        }
        int end = textPtr.getStartOffset() + textPtr.getLength();
        for (; offset < end; offset++) {
            int digit;
            if (bytes[offset] >= '0' && bytes[offset] <= '9') {
                value *= 10;
                digit = bytes[offset] - '0';
            } else if (bytes[offset] == 'i' && bytes[offset + 1] == '6' && bytes[offset + 2] == '4'
                    && offset + 3 == end) {
                break;
            } else {
                return false;
            }
            if (value < limit + digit) {
                return false;
            }
            value -= digit;
        }
        if (value > 0) {
            return false;
        }
        if (value < 0 && positive) {
            value *= -1;
        }

        result.setValue(value);
        return true;
    }

    private NumberUtils() {
    }

    /**
     * Checks if 2 strings are numeric and of the same type
     * @param value1 first string value
     * @param value2 second string value
     * @return {@code true} if value1 and value2 are numeric values and of the same type, {@code false} otherwise
     */
    public static boolean isSameTypeNumericStrings(String value1, String value2) {
        // Step 1: Confirm numeric strings
        if (isNumericString(value1) && isNumericString(value2)) {
            // Step 2: Confirm same type (2 ints or 2 floats = true, otherwise false)
            return isIntegerNumericString(value1) == isIntegerNumericString(value2);
        }

        // Not numeric string
        return false;
    }

    /**
     * Checks if a string is a numeric value
     * @param value string to be checked
     * @return {@code true} if the string is a valid number, {@code false} otherwise
     */
    public static boolean isNumericString(String value) {
        try {
            Double.parseDouble(value);
            return true;
        } catch (NumberFormatException ignored) {
            return false;
        }
    }

    /**
     * Checks if a numeric string is of type int
     * @param value numeric string value
     * @return {@code true} if the string is of type int, {@code false} otherwise
     */
    public static boolean isIntegerNumericString(String value) {
        try {
            Integer.parseInt(value);
            return true;
        } catch (NumberFormatException ignored) {
            return false;
        }
    }
}
