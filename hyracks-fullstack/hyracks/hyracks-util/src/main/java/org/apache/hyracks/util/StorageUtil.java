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

public class StorageUtil {

    private static final int BASE = 1024, KB = BASE, MB = KB * BASE, GB = MB * BASE, TB = GB * BASE, PB = TB * BASE;

    public enum StorageUnit {
        BYTE("B"),
        KILOBYTE("KB"),
        MEGABYTE("MB"),
        GIGABYTE("GB"),
        TERABYTE("TB"),
        PETABYTE("PB");

        private final String unitTypeInLetter;

        private StorageUnit(String unitTypeInLetter) {
            this.unitTypeInLetter = unitTypeInLetter;
        }

        @Override
        public String toString() {
            return this.unitTypeInLetter;
        }
    }

    private StorageUtil() {
        throw new AssertionError("This util class should not be initialized.");
    }

    public static int getSizeInBytes(final int size, final StorageUnit unit) {
        double result = getSizeInBytes((double) size, unit);
        if (result > Integer.MAX_VALUE) {
            throw new IllegalStateException("The given value:" + result + " is not within the integer range.");
        } else {
            return (int) result;
        }
    }

    public static long getSizeInBytes(final long size, final StorageUnit unit) {
        double result = getSizeInBytes((double) size, unit);
        if (result > Long.MAX_VALUE) {
            throw new IllegalStateException("The given value:" + result + " is not within the long range.");
        } else {
            return (long) result;
        }
    }

    public static double getSizeInBytes(final double size, final StorageUnit unit) {
        switch (unit) {
            case BYTE:
                return size;
            case KILOBYTE:
                return size * KB;
            case MEGABYTE:
                return size * MB;
            case GIGABYTE:
                return size * GB;
            case TERABYTE:
                return size * TB;
            case PETABYTE:
                return size * PB;
            default:
                throw new IllegalStateException("Unsupported unit: " + unit);
        }
    }

    /**
     * Helper method to parse a byte unit string to its double value and unit
     * (e.g., 10,345.8MB becomes Pair<10345.8, StorageUnit.MB>.)
     *
     * @throws HyracksException
     */
    public static ByteValueStringInfo parseByteUnitString(String s) {
        String sSpaceRemoved = s.replaceAll(" ", "");
        String sUpper = sSpaceRemoved.toUpperCase();
        ByteValueStringInfo valueAndUnitType = new ByteValueStringInfo();

        // Default type
        StorageUtil.StorageUnit unitType = StorageUnit.BYTE;

        // If the length is 1, it should only contain a digit number.
        if (sUpper.length() == 1) {
            if (Character.isDigit(sUpper.charAt(0))) {
                unitType = StorageUnit.BYTE;
            } else {
                throw new IllegalStateException(
                        "The given string: " + s + " is not a byte unit string (e.g., 320KB or 1024).");
            }
        } else if (sUpper.length() > 1) {
            String checkStr = sUpper.substring(sUpper.length() - 2);
            boolean found = false;
            for (StorageUnit unit : StorageUnit.values()) {
                if (checkStr.equals(unit.toString())) {
                    unitType = unit;
                    found = true;
                    break;
                }
            }

            if (!found) {
                // The last suffix should be at least "B" or a digit to be qualified as byte unit string.
                char lastChar = sUpper.charAt(sUpper.length() - 1);
                if (sUpper.substring(sUpper.length() - 1).equals(StorageUnit.BYTE.toString())
                        || Character.isDigit(lastChar)) {
                    unitType = StorageUnit.BYTE;
                } else {
                    throw new IllegalStateException(
                            "The given string: " + s + " is not a byte unit string (e.g., 320KB or 1024).");
                }
            }
        } else {
            // String length is zero. We can't parse this string.
            throw new IllegalStateException(
                    "The given string: " + s + " is not a byte unit string (e.g., 320KB or 1024).");
        }

        // Strip all unit suffixes such as KB, MB ...
        String sFinalVal = sUpper.replaceAll("[^\\.0123456789]", "");

        // Return the digit and its unit type.
        valueAndUnitType.value = Double.parseDouble(sFinalVal);
        valueAndUnitType.unitType = unitType;

        return valueAndUnitType;
    }

    /**
     * Return byte value for the given string (e.g., 0.1KB, 100kb, 1mb, 3MB, 8.5GB ...)
     *
     * @throws HyracksException
     */
    public static long getByteValue(String s) {
        try {
            ByteValueStringInfo valueAndUnitType = parseByteUnitString(s);

            double result = getSizeInBytes(valueAndUnitType.value, valueAndUnitType.unitType);
            if (result > Long.MAX_VALUE) {
                throw new IllegalStateException("The given value:" + result + " is not within the long range.");
            } else {
                return (long) result;
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(e);
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
        return String.format("%.2f %sB", (double) bytes / (1L << (baseValue * 10)), " kMGTPE".charAt(baseValue));
    }

    private static class ByteValueStringInfo {
        double value;
        StorageUnit unitType;

        ByteValueStringInfo() {
            value = 0.0;
            unitType = StorageUnit.BYTE;
        }
    }
}
