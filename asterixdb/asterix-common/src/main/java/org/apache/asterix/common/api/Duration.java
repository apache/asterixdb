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
package org.apache.asterix.common.api;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public enum Duration {
    SEC("s", 9),
    MILLI("ms", 6),
    MICRO("µs", 3),
    NANO("ns", 0);

    static final Duration[] VALUES = values();

    static final long NANOSECONDS = 1;
    static final long MICROSECONDS = 1000 * NANOSECONDS;
    static final long MILLISECONDS = 1000 * MICROSECONDS;
    static final long SECONDS = 1000 * MILLISECONDS;
    static final long MINUTES = 60 * SECONDS;
    static final long HOURS = 60 * MINUTES;

    String unit;
    int nanoDigits;

    Duration(String unit, int nanoDigits) {
        this.unit = unit;
        this.nanoDigits = nanoDigits;
    }

    public String asciiSafeUnit() {
        return this == MICRO ? "us" : unit;
    }

    public static String formatNanos(long nanoTime) {
        return formatNanos(nanoTime, false);
    }

    public static String formatNanos(long nanoTime, boolean asciiSafe) {
        StringBuilder sb = new StringBuilder();
        formatNanos(nanoTime, sb, asciiSafe);
        return sb.toString();
    }

    public static void formatNanos(long nanoTime, StringBuilder out) {
        formatNanos(nanoTime, out, false);
    }

    public static void formatNanos(long nanoTime, StringBuilder out, boolean asciiSafe) {
        final String strTime = String.valueOf(Math.abs(nanoTime));
        final int len = strTime.length();
        for (Duration tu : VALUES) {
            int n = len - tu.nanoDigits;
            if (n > 0) {
                if (nanoTime < 0) {
                    out.append('-');
                }
                out.append(strTime, 0, n);
                int k = lastIndexOf(strTime, n, '1', '9');
                if (k > 0) {
                    out.append('.').append(strTime, n, k + 1);
                }
                out.append(asciiSafe ? tu.asciiSafeUnit() : tu.unit);
                break;
            }
        }
    }

    // ParseDuration parses a duration string.
    // A duration string is a possibly signed sequence of
    // decimal numbers, each with optional fraction and a unit suffix,
    // such as "300ms", "-1.5h" or "2h45m".
    // Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".
    // returns the duration in nano seconds
    public static long parseDurationStringToNanos(String orig) throws HyracksDataException {
        // [-+]?([0-9]*(\.[0-9]*)?[a-z]+)+
        String s = orig;
        long d = 0;
        boolean neg = false;
        char c;
        // Consume [-+]?
        if (!s.isEmpty()) {
            c = s.charAt(0);
            if (c == '-' || c == '+') {
                neg = c == '-';
                s = s.substring(1);
            }
        }

        // Special case: if all that is left is "0", this is zero.
        if ("0".equals(s)) {
            return 0L;
        }

        if (s.isEmpty()) {
            throw new RuntimeDataException(ErrorCode.INVALID_DURATION, orig);
        }

        while (!s.isEmpty()) {
            long v = 0L; // integers before decimal
            long f = 0L; // integers after decimal
            double scale = 1.0; // value = v + f/scale
            // The next character must be [0-9.]
            if (!(s.charAt(0) == '.' || '0' <= s.charAt(0) && s.charAt(0) <= '9')) {
                throw new RuntimeDataException(ErrorCode.INVALID_DURATION, orig);
            }
            // Consume [0-9]*
            int pl = s.length();
            Pair<Long, String> pair = leadingInt(s);
            v = pair.getLeft();
            s = pair.getRight();
            boolean pre = pl != s.length(); // whether we consumed anything before a period

            // Consume (\.[0-9]*)?
            boolean post = false;
            if (!s.isEmpty() && s.charAt(0) == '.') {
                s = s.substring(1);
                pl = s.length();
                Triple<Long, Double, String> triple = leadingFraction(s);
                f = triple.getLeft();
                scale = triple.getMiddle();
                s = triple.getRight();
                post = pl != s.length();
            }
            if (!pre && !post) {
                // no digits (e.g. ".s" or "-.s")
                throw new RuntimeDataException(ErrorCode.INVALID_DURATION, orig);
            }

            // Consume unit.
            int i = 0;
            for (; i < s.length(); i++) {
                c = s.charAt(i);
                if (c == '.' || '0' <= c && c <= '9') {
                    break;
                }
            }
            if (i == 0) {
                throw new RuntimeDataException(ErrorCode.INVALID_DURATION, orig);
            }
            String u = s.substring(0, i);
            s = s.substring(i);
            long unit = getUnit(u);
            if (v > Long.MAX_VALUE / unit) {
                // overflow
                throw new RuntimeDataException(ErrorCode.INVALID_DURATION, orig);
            }
            v *= unit;
            if (f > 0) {
                // float64 is needed to be nanosecond accurate for fractions of hours.
                // v >= 0 && (f*unit/scale) <= 3.6e+12 (ns/h, h is the largest unit)
                v += (long) (((double) f * (double) unit) / scale);
                if (v < 0) {
                    // overflow
                    throw new RuntimeDataException(ErrorCode.INVALID_DURATION, orig);
                }
            }
            d += v;
            if (d < 0) {
                // overflow
                throw new RuntimeDataException(ErrorCode.INVALID_DURATION, orig);
            }
        }

        if (neg) {
            d = -d;
        }
        return d;
    }

    private static final long getUnit(String unit) throws HyracksDataException {
        switch (unit) {
            case "ns":
                return NANOSECONDS;
            case "us":
            case "µs":// U+00B5 = micro symbol
            case "μs":// U+03BC = Greek letter mu
                return MICROSECONDS;
            case "ms":
                return MILLISECONDS;
            case "s":
                return SECONDS;
            case "m":
                return MINUTES;
            case "h":
                return HOURS;
            default:
                throw new RuntimeDataException(ErrorCode.UNKNOWN_DURATION_UNIT, unit);
        }
    }

    // leadingInt consumes the leading [0-9]* from s.
    static Pair<Long, String> leadingInt(String origin) throws HyracksDataException {
        String s = origin;
        long x = 0L;
        int i = 0;
        for (; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') {
                break;
            }
            if (x > Long.MAX_VALUE / 10) {
                throw new RuntimeDataException(ErrorCode.INVALID_DURATION, origin);
            }
            x = x * 10 + Character.getNumericValue(c);
            if (x < 0) {
                throw new RuntimeDataException(ErrorCode.INVALID_DURATION, origin);
            }
        }
        return Pair.of(x, s.substring(i));
    }

    // leadingFraction consumes the leading [0-9]* from s.
    // It is used only for fractions, so does not return an error on overflow,
    // it just stops accumulating precision.
    static Triple<Long, Double, String> leadingFraction(String s) {
        int i = 0;
        long x = 0L;
        double scale = 1.0;
        boolean overflow = false;
        for (; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') {
                break;
            }
            if (overflow) {
                continue;
            }
            if (x > (1 << 63 - 1) / 10) {
                // It's possible for overflow to give a positive number, so take care.
                overflow = true;
                continue;
            }
            long y = x * 10 + Character.getNumericValue(c);
            if (y < 0) {
                overflow = true;
                continue;
            }
            x = y;
            scale *= 10;
        }
        return Triple.of(x, scale, s.substring(i));
    }

    private static int lastIndexOf(CharSequence seq, int fromIndex, char rangeStart, char rangeEnd) {
        for (int i = seq.length() - 1; i >= fromIndex; i--) {
            char c = seq.charAt(i);
            if (c >= rangeStart && c <= rangeEnd) {
                return i;
            }
        }
        return -1;
    }
}