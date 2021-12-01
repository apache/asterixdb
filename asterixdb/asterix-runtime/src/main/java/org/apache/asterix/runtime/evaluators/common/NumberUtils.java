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
import org.apache.asterix.om.base.AMutableFloat;
import org.apache.asterix.om.base.AMutableInt16;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.AMutableInt8;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.util.StringUtil;

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
     * Parses string as float
     * @param textPtr input string
     * @param result placeholder for the result
     * @return {@code true} if parsing was successful, {@code false} otherwise
     */
    public static boolean parseFloat(UTF8StringPointable textPtr, AMutableFloat result) {
        float v;
        if (POSITIVE_INF.compareTo(textPtr) == 0) {
            v = Float.POSITIVE_INFINITY;
        } else if (NEGATIVE_INF.compareTo(textPtr) == 0) {
            v = Float.NEGATIVE_INFINITY;
        } else if (NAN.compareTo(textPtr) == 0) {
            v = Float.NaN;
        } else {
            try {
                v = Float.parseFloat(textPtr.toString());
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
     * @param maybeNumeric if parsing was unsuccessful indicates whether the input string might
     *                     contain a non-integer numeric value
     * @return {@code true} if parsing was successful, {@code false} otherwise
     */
    public static boolean parseInt64(UTF8StringPointable textPtr, AMutableInt64 result, MutableBoolean maybeNumeric) {
        byte[] bytes = textPtr.getByteArray();
        int offset = textPtr.getCharStartOffset();
        int end = textPtr.getStartOffset() + textPtr.getLength();
        return parseInt64(bytes, offset, end, StringUtil.getByteArrayAsCharAccessor(), result, maybeNumeric);
    }

    public static <T> boolean parseInt64(T input, int begin, int end, StringUtil.ICharAccessor<T> charAccessor,
            AMutableInt64 result, MutableBoolean maybeNumeric) {
        if (maybeNumeric != null) {
            maybeNumeric.setFalse();
        }
        int offset = begin;
        //accumulating value in negative domain
        //otherwise Long.MIN_VALUE = -(Long.MAX_VALUE + 1) would have caused overflow
        long value = 0;
        boolean positive = true;
        long limit = -Long.MAX_VALUE;
        char c = charAccessor.charAt(input, offset);
        if (c == '+') {
            offset++;
        } else if (c == '-') {
            offset++;
            positive = false;
            limit = Long.MIN_VALUE;
        }
        for (; offset < end; offset++) {
            int digit;
            c = charAccessor.charAt(input, offset);
            if (c >= '0' && c <= '9') {
                value *= 10;
                digit = c - '0';
            } else if (c == 'i' && charAccessor.charAt(input, offset + 1) == '6'
                    && charAccessor.charAt(input, offset + 2) == '4' && offset + 3 == end) {
                break;
            } else {
                if (maybeNumeric != null) {
                    maybeNumeric.setValue(isNumericNonDigitOrSignChar(c));
                }
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

    /**
     * Parses string as integer
     * @param textPtr input string
     * @param result placeholder for the result
     * @param maybeNumeric if parsing was unsuccessful indicates whether the input string might
     *                     contain a non-integer numeric value
     * @return {@code true} if parsing was successful, {@code false} otherwise
     */
    public static boolean parseInt32(UTF8StringPointable textPtr, AMutableInt32 result, MutableBoolean maybeNumeric) {
        if (maybeNumeric != null) {
            maybeNumeric.setFalse();
        }
        byte[] bytes = textPtr.getByteArray();
        int offset = textPtr.getCharStartOffset();
        //accumulating value in negative domain
        //otherwise Integer.MIN_VALUE = -(Integer.MAX_VALUE + 1) would have caused overflow
        int value = 0;
        boolean positive = true;
        int limit = -Integer.MAX_VALUE;
        if (bytes[offset] == '+') {
            offset++;
        } else if (bytes[offset] == '-') {
            offset++;
            positive = false;
            limit = Integer.MIN_VALUE;
        }
        int end = textPtr.getStartOffset() + textPtr.getLength();
        for (; offset < end; offset++) {
            int digit;
            if (bytes[offset] >= '0' && bytes[offset] <= '9') {
                value *= 10;
                digit = bytes[offset] - '0';
            } else if (bytes[offset] == 'i' && bytes[offset + 1] == '3' && bytes[offset + 2] == '2'
                    && offset + 3 == end) {
                break;
            } else {
                if (maybeNumeric != null) {
                    maybeNumeric.setValue(isNumericNonDigitOrSignChar(bytes[offset]));
                }
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

    /**
     * Parses string as smallint
     * @param textPtr input string
     * @param result placeholder for the result
     * @param maybeNumeric if parsing was unsuccessful indicates whether the input string might
     *                     contain a non-integer numeric value
     * @return {@code true} if parsing was successful, {@code false} otherwise
     */
    public static boolean parseInt16(UTF8StringPointable textPtr, AMutableInt16 result, MutableBoolean maybeNumeric) {
        if (maybeNumeric != null) {
            maybeNumeric.setFalse();
        }
        byte[] bytes = textPtr.getByteArray();
        int offset = textPtr.getCharStartOffset();
        //accumulating value in negative domain
        //otherwise Short.MIN_VALUE = -(Short.MAX_VALUE + 1) would have caused overflow
        short value = 0;
        boolean positive = true;
        short limit = -Short.MAX_VALUE;
        if (bytes[offset] == '+') {
            offset++;
        } else if (bytes[offset] == '-') {
            offset++;
            positive = false;
            limit = Short.MIN_VALUE;
        }
        int end = textPtr.getStartOffset() + textPtr.getLength();
        for (; offset < end; offset++) {
            int digit;
            if (bytes[offset] >= '0' && bytes[offset] <= '9') {
                value = (short) (value * 10);
                digit = bytes[offset] - '0';
            } else if (bytes[offset] == 'i' && bytes[offset + 1] == '1' && bytes[offset + 2] == '6'
                    && offset + 3 == end) {
                break;
            } else {
                if (maybeNumeric != null) {
                    maybeNumeric.setValue(isNumericNonDigitOrSignChar(bytes[offset]));
                }
                return false;
            }
            if (value < limit + digit) {
                return false;
            }
            value = (short) (value - digit);
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

    /**
     * Parses string as tinyint
     * @param textPtr input string
     * @param result placeholder for the result
     * @param maybeNumeric if parsing was unsuccessful indicates whether the input string might
     *                     contain a non-integer numeric value
     * @return {@code true} if parsing was successful, {@code false} otherwise
     */
    public static boolean parseInt8(UTF8StringPointable textPtr, AMutableInt8 result, MutableBoolean maybeNumeric) {
        if (maybeNumeric != null) {
            maybeNumeric.setFalse();
        }
        byte[] bytes = textPtr.getByteArray();
        int offset = textPtr.getCharStartOffset();
        //accumulating value in negative domain
        //otherwise Byte.MIN_VALUE = -(Byte.MAX_VALUE + 1) would have caused overflow
        byte value = 0;
        boolean positive = true;
        byte limit = -Byte.MAX_VALUE;
        if (bytes[offset] == '+') {
            offset++;
        } else if (bytes[offset] == '-') {
            offset++;
            positive = false;
            limit = Byte.MIN_VALUE;
        }
        int end = textPtr.getStartOffset() + textPtr.getLength();
        for (; offset < end; offset++) {
            int digit;
            if (bytes[offset] >= '0' && bytes[offset] <= '9') {
                value = (byte) (value * 10);
                digit = bytes[offset] - '0';
            } else if (bytes[offset] == 'i' && bytes[offset + 1] == '8' && offset + 2 == end) {
                break;
            } else {
                if (maybeNumeric != null) {
                    maybeNumeric.setValue(isNumericNonDigitOrSignChar(bytes[offset]));
                }
                return false;
            }
            if (value < limit + digit) {
                return false;
            }
            value = (byte) (value - digit);
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

    private static boolean isNumericNonDigitOrSignChar(char v) {
        switch (v) {
            case '.':
            case 'E':
            case 'e':
            case 'I': // INF
            case 'N': // NaN
                return true;
            default:
                return false;
        }
    }

    private static boolean isNumericNonDigitOrSignChar(byte v) {
        return isNumericNonDigitOrSignChar((char) v);
    }
}
