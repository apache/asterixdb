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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StorageUtil {

    public static final int BASE = 1024;
    private static final Pattern PATTERN = Pattern.compile("^(-?[.0-9]+)([A-Z]{0,3})$");

    public enum StorageUnit {
        BYTE("B", "b", 1),
        KILOBYTE("KiB", "kb", BASE),
        MEGABYTE("MiB", "m", KILOBYTE.multiplier * BASE),
        GIGABYTE("GiB", "g", MEGABYTE.multiplier * BASE),
        TERABYTE("TiB", "t", GIGABYTE.multiplier * BASE),
        PETABYTE("PiB", "p", TERABYTE.multiplier * BASE);

        private final String unitTypeInLetter;
        private final String linuxUnitTypeInLetter;
        private final long multiplier;
        private static final Map<String, StorageUnit> SUFFIX_TO_UNIT_MAP = new HashMap<>();

        static {
            for (StorageUnit unit : values()) {
                SUFFIX_TO_UNIT_MAP.put(unit.unitTypeInLetter.toUpperCase(), unit);
                SUFFIX_TO_UNIT_MAP.put(unit.unitTypeInLetter.replace("i", ""), unit);
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
        String valueAndUnit = s.replace(" ", "").toUpperCase();
        Matcher matcher = PATTERN.matcher(valueAndUnit);
        if (!matcher.find()) {
            throw invalidFormatException(s);
        }

        String value = matcher.group(1);
        String unit = matcher.group(2);

        // Default to bytes or find provided unit
        StorageUnit unitType = !unit.isEmpty() ? StorageUnit.lookupBySuffix(unit) : StorageUnit.BYTE;
        if (unitType == null) {
            throw invalidFormatException(s);
        }

        try {
            // Return the bytes.
            return unitType.toBytes(Double.parseDouble(value));
        } catch (NumberFormatException ex) {
            throw invalidFormatException(s);
        }
    }

    private static IllegalArgumentException invalidFormatException(String s) {
        return new IllegalArgumentException(
                "The given string: " + s + " is not a byte unit string (e.g., 320KiB or 1024).");
    }

    /**
     * Return byte value for the given string (e.g., 0.1KiB, 100kb, 1mb, 3MiB, 8.5GiB ...)
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
     * e.g. toHumanReadableSize(1024L * 1024L * 1024l * 10L *) + 1024l * 1024l * 59) returns 1.06 GiB
     *
     * @param bytes
     * @return Value in storage units.
     */
    public static String toHumanReadableSize(final long bytes) {
        if (bytes < BASE) {
            return bytes + " B";
        }
        final int baseValue = (63 - Long.numberOfLeadingZeros(bytes)) / 10;
        final String bytePrefix = new String[] { " ", "Ki", "Mi", "Gi", "Ti", "Pi" }[baseValue];
        final long divisor = 1L << (baseValue * 10);
        if (bytes % divisor == 0) {
            return String.format("%d %sB", bytes / divisor, bytePrefix);
        } else {
            return String.format("%.2f %sB", (double) bytes / divisor, bytePrefix);
        }
    }
}
