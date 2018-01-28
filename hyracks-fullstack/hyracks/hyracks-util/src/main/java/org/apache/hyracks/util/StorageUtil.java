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

import java.util.HashMap;
import java.util.Map;

public class StorageUtil {

    public static final int BASE = 1024;

    public enum StorageUnit {
        BYTE("B", "b", 1),
        KILOBYTE("KB", "kb", BASE),
        MEGABYTE("MB", "m", KILOBYTE.multiplier * BASE),
        GIGABYTE("GB", "g", MEGABYTE.multiplier * BASE),
        TERABYTE("TB", "t", GIGABYTE.multiplier * BASE),
        PETABYTE("PB", "p", TERABYTE.multiplier * BASE);

        private final String unitTypeInLetter;
        private final String linuxUnitTypeInLetter;
        private final long multiplier;
        private static final Map<String, StorageUnit> SUFFIX_TO_UNIT_MAP = new HashMap<>();

        static {
            for (StorageUnit unit : values()) {
                SUFFIX_TO_UNIT_MAP.put(unit.unitTypeInLetter, unit);
            }
        }

        StorageUnit(String unitTypeInLetter, String linuxUnitTypeInLetter, long multiplier) {
            this.unitTypeInLetter = unitTypeInLetter;
            this.linuxUnitTypeInLetter = linuxUnitTypeInLetter;
            this.multiplier = multiplier;
        }

        @Override
        public String toString() {
            return this.unitTypeInLetter;
        }

        public double toBytes(double value) {
            return value * multiplier;
        }

        public String getLinuxUnitTypeInLetter() {
            return linuxUnitTypeInLetter;
        }

        public static StorageUnit lookupBySuffix(String name) {
            return SUFFIX_TO_UNIT_MAP.get(name);
        }
    }

    private StorageUtil() {
        throw new AssertionError("This util class should not be initialized.");
    }

    public static int getIntSizeInBytes(final int size, final StorageUnit unit) {
        double result = unit.toBytes(size);
        if (result > Integer.MAX_VALUE || result < Integer.MIN_VALUE) {
            throw new IllegalArgumentException("The given value:" + result + " is not within the integer range.");
        } else {
            return (int) result;
        }
    }

    public static long getLongSizeInBytes(final long size, final StorageUnit unit) {
        double result = unit.toBytes(size);
        if (result > Long.MAX_VALUE || result < Long.MIN_VALUE) {
            throw new IllegalArgumentException("The given value:" + result + " is not within the long range.");
        } else {
            return (long) result;
        }
    }

    /**
     * Helper method to parse a byte unit string to its double value in bytes
     *
     * @throws IllegalArgumentException
     */
    public static double getSizeInBytes(String s) {
        String sSpaceRemoved = s.replaceAll(" ", "");
        String sUpper = sSpaceRemoved.toUpperCase();

        // Default type
        StorageUtil.StorageUnit unitType;

        // If the length is 1, it should only contain a digit number.
        if (sUpper.length() == 1) {
            if (Character.isDigit(sUpper.charAt(0))) {
                unitType = StorageUnit.BYTE;
            } else {
                throw invalidFormatException(s);
            }
        } else if (sUpper.length() > 1) {
            String checkStr = sUpper.substring(sUpper.length() - 2);
            unitType = StorageUnit.lookupBySuffix(checkStr);

            if (unitType == null) {
                // The last suffix should be at least "B" or a digit to be qualified as byte unit string.
                char lastChar = sUpper.charAt(sUpper.length() - 1);
                if (sUpper.substring(sUpper.length() - 1).equals(StorageUnit.BYTE.toString())
                        || Character.isDigit(lastChar)) {
                    unitType = StorageUnit.BYTE;
                } else {
                    throw invalidFormatException(s);
                }
            }
        } else {
            // String length is zero. We can't parse this string.
            throw invalidFormatException(s);
        }

        // Strip all unit suffixes such as KB, MB ...
        String sFinalVal = sUpper.replaceAll("[^-\\.0123456789]", "");

        // Return the bytes.
        return unitType.toBytes(Double.parseDouble(sFinalVal));
    }

    private static IllegalArgumentException invalidFormatException(String s) {
        return new IllegalArgumentException(
                "The given string: " + s + " is not a byte unit string (e.g., 320KB or 1024).");
    }

    /**
     * Return byte value for the given string (e.g., 0.1KB, 100kb, 1mb, 3MB, 8.5GB ...)
     *
     * @throws IllegalArgumentException
     */
    public static long getByteValue(String s) {
        double result = getSizeInBytes(s);
        if (result > Long.MAX_VALUE || result < Long.MIN_VALUE) {
            throw new IllegalArgumentException("The given value:" + result + " is not within the long range.");
        } else {
            return (long) result;
        }
    }

    /**
     * Returns a human readable value in storage units rounded up to two decimal places.
     * e.g. toHumanReadableSize(1024L * 1024L * 1024l * 10L *) + 1024l * 1024l * 59) returns 1.06 GB
     *
     * @param bytes
     * @return Value in storage units.
     */
    public static String toHumanReadableSize(final long bytes) {
        if (bytes < BASE) {
            return bytes + " B";
        }
        final int baseValue = (63 - Long.numberOfLeadingZeros(bytes)) / 10;
        final char bytePrefix = " kMGTPE".charAt(baseValue);
        final long divisor = 1L << (baseValue * 10);
        if (bytes % divisor == 0) {
            return String.format("%d %sB", bytes / divisor, bytePrefix);
        } else {
            return String.format("%.2f %sB", (double) bytes / divisor, bytePrefix);
        }
    }
}
