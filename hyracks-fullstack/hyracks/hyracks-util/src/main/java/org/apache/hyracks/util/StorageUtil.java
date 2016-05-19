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
        BYTE,
        KILOBYTE,
        MEGABYTE,
        GIGABYTE,
        TERABYTE,
        PETABYTE
    }

    private StorageUtil() {
        throw new AssertionError("This util class should not be initialized.");
    }

    public static int getSizeInBytes(final int size, final StorageUnit unit) {
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
            default:
                throw new IllegalStateException("Unsupported unti: " + unit);
        }
    }

    public static long getSizeInBytes(final long size, final StorageUnit unit) {
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
                throw new IllegalStateException("Unsupported unti: " + unit);
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
}
