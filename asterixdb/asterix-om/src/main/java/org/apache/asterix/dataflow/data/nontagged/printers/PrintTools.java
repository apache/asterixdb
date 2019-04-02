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
package org.apache.asterix.dataflow.data.nontagged.printers;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.hyracks.algebricks.data.utils.WriteValueTools;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.bytes.HexPrinter;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class PrintTools {

    private static final GregorianCalendarSystem gCalInstance = GregorianCalendarSystem.getInstance();
    private static final long CHRONON_OF_DAY = TimeUnit.DAYS.toMillis(1);

    public static void printDateString(byte[] b, int s, int l, PrintStream ps) throws HyracksDataException {
        long chrononTime = AInt32SerializerDeserializer.getInt(b, s + 1) * CHRONON_OF_DAY;

        try {
            gCalInstance.getExtendStringRepUntilField(chrononTime, 0, ps, GregorianCalendarSystem.Fields.YEAR,
                    GregorianCalendarSystem.Fields.DAY, false);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public static void printDateTimeString(byte[] b, int s, int l, PrintStream ps) throws HyracksDataException {
        long chrononTime = AInt64SerializerDeserializer.getLong(b, s + 1);

        try {
            gCalInstance.getExtendStringRepUntilField(chrononTime, 0, ps, GregorianCalendarSystem.Fields.YEAR,
                    GregorianCalendarSystem.Fields.MILLISECOND, true);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public static void printDayTimeDurationString(byte[] b, int s, int l, PrintStream ps) throws HyracksDataException {
        boolean positive = true;
        long milliseconds = AInt64SerializerDeserializer.getLong(b, s + 1);

        // set the negative flag. "||" is necessary in case that months field is not there (so it is 0)
        if (milliseconds < 0) {
            milliseconds *= -1;
            positive = false;
        }

        int millisecond = gCalInstance.getDurationMillisecond(milliseconds);
        int second = gCalInstance.getDurationSecond(milliseconds);
        int minute = gCalInstance.getDurationMinute(milliseconds);
        int hour = gCalInstance.getDurationHour(milliseconds);
        int day = gCalInstance.getDurationDay(milliseconds);

        if (!positive) {
            ps.print("-");
        }
        try {
            ps.print("P");
            if (day != 0) {
                WriteValueTools.writeInt(day, ps);
                ps.print("D");
            }
            if (hour != 0 || minute != 0 || second != 0 || millisecond != 0) {
                ps.print("T");
            }
            if (hour != 0) {
                WriteValueTools.writeInt(hour, ps);
                ps.print("H");
            }
            if (minute != 0) {
                WriteValueTools.writeInt(minute, ps);
                ps.print("M");
            }
            if (second != 0 || millisecond != 0) {
                WriteValueTools.writeInt(second, ps);
            }
            if (millisecond > 0) {
                ps.print(".");
                WriteValueTools.writeInt(millisecond, ps);
            }
            if (second != 0 || millisecond != 0) {
                ps.print("S");
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public static void printYearMonthDurationString(byte[] b, int s, int l, PrintStream ps)
            throws HyracksDataException {
        final GregorianCalendarSystem gCalInstance = GregorianCalendarSystem.getInstance();
        boolean positive = true;
        int months = AInt32SerializerDeserializer.getInt(b, s + 1);

        // set the negative flag. "||" is necessary in case that months field is not there (so it is 0)
        if (months < 0) {
            months *= -1;
            positive = false;
        }

        int month = gCalInstance.getDurationMonth(months);
        int year = gCalInstance.getDurationYear(months);

        if (!positive) {
            ps.print("-");
        }
        try {
            ps.print("P");
            if (year != 0) {
                WriteValueTools.writeInt(year, ps);
                ps.print("Y");
            }
            if (month != 0) {
                WriteValueTools.writeInt(month, ps);
                ps.print("M");
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public static void printDurationString(byte[] b, int s, int l, PrintStream ps) throws HyracksDataException {
        boolean positive = true;
        int months = AInt32SerializerDeserializer.getInt(b, s + 1);
        long milliseconds = AInt64SerializerDeserializer.getLong(b, s + 5);

        // set the negative flag. "||" is necessary in case that months field is not there (so it is 0)
        if (months < 0 || milliseconds < 0) {
            months *= -1;
            milliseconds *= -1;
            positive = false;
        }

        int month = gCalInstance.getDurationMonth(months);
        int year = gCalInstance.getDurationYear(months);
        int millisecond = gCalInstance.getDurationMillisecond(milliseconds);
        int second = gCalInstance.getDurationSecond(milliseconds);
        int minute = gCalInstance.getDurationMinute(milliseconds);
        int hour = gCalInstance.getDurationHour(milliseconds);
        int day = gCalInstance.getDurationDay(milliseconds);

        if (!positive) {
            ps.print("-");
        }
        try {
            ps.print("P");
            if (year != 0) {
                WriteValueTools.writeInt(year, ps);
                ps.print("Y");
            }
            if (month != 0) {
                WriteValueTools.writeInt(month, ps);
                ps.print("M");
            }
            if (day != 0) {
                WriteValueTools.writeInt(day, ps);
                ps.print("D");
            }
            if (hour != 0 || minute != 0 || second != 0 || millisecond != 0) {
                ps.print("T");
            }
            if (hour != 0) {
                WriteValueTools.writeInt(hour, ps);
                ps.print("H");
            }
            if (minute != 0) {
                WriteValueTools.writeInt(minute, ps);
                ps.print("M");
            }
            if (second != 0 || millisecond != 0) {
                WriteValueTools.writeInt(second, ps);
            }
            if (millisecond > 0) {
                ps.print(".");
                WriteValueTools.writeInt(millisecond, ps);
            }
            if (second != 0 || millisecond != 0) {
                ps.print("S");
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public static void printTimeString(byte[] b, int s, int l, PrintStream ps) throws HyracksDataException {
        int time = AInt32SerializerDeserializer.getInt(b, s + 1);

        try {
            gCalInstance.getExtendStringRepUntilField(time, 0, ps, GregorianCalendarSystem.Fields.HOUR,
                    GregorianCalendarSystem.Fields.MILLISECOND, true);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public static void printDoubleForJson(byte[] b, int s, PrintStream ps) {
        final double d = ADoubleSerializerDeserializer.getDouble(b, s + 1);
        if (Double.isFinite(d)) {
            ps.print(d);
        } else {
            ps.append('"');
            ps.print(printNonFiniteDouble(d));
            ps.append('"');
        }
    }

    public static void printDouble(byte[] b, int s, PrintStream ps) {
        final double d = ADoubleSerializerDeserializer.getDouble(b, s + 1);
        if (Double.isFinite(d)) {
            ps.print(d);
        } else {
            ps.print(printNonFiniteDouble(d));
        }
    }

    public static void printFloatForJson(byte[] b, int s, PrintStream ps) {
        final float f = AFloatSerializerDeserializer.getFloat(b, s + 1);
        if (Float.isFinite(f)) {
            ps.print(f);
        } else {
            ps.print('"');
            ps.print(printNonFiniteFloat(f));
            ps.print('"');
        }
    }

    public static void printFloat(byte[] b, int s, PrintStream ps) {
        final float f = AFloatSerializerDeserializer.getFloat(b, s + 1);
        if (Float.isFinite(f)) {
            ps.print(f);
        } else {
            ps.print(printNonFiniteFloat(f));
        }
    }

    @SuppressWarnings("squid:S1244") // equality comparison w/ infinity is not problematic
    private static String printNonFiniteDouble(double d) {
        return Double.isNaN(d) ? "NaN" : d == Double.POSITIVE_INFINITY ? "INF" : "-INF";
    }

    @SuppressWarnings("squid:S1244") // equality comparison w/ infinity is not problematic
    private static String printNonFiniteFloat(float f) {
        return Float.isNaN(f) ? "NaN" : f == Float.POSITIVE_INFINITY ? "INF" : "-INF";
    }

    public enum CASE {
        LOWER_CASE,
        UPPER_CASE,
    }

    public static void writeUTF8StringAsCSV(byte[] b, int s, int l, OutputStream os) throws IOException {
        int stringLength = UTF8StringUtil.getUTFLength(b, s);
        int position = s + UTF8StringUtil.getNumBytesToStoreLength(stringLength);
        int maxPosition = position + stringLength;
        os.write('"');
        while (position < maxPosition) {
            char c = UTF8StringUtil.charAt(b, position);
            int sz = UTF8StringUtil.charSize(b, position);
            if (c == '"') {
                os.write('"');
            }
            os.write(c);
            position += sz;
        }
        os.write('"');
    }

    public static void writeUTF8StringAsJSON(byte[] b, int s, int l, OutputStream os) throws IOException {
        int utfLength = UTF8StringUtil.getUTFLength(b, s);
        int position = s + UTF8StringUtil.getNumBytesToStoreLength(utfLength); // skip 2 bytes containing string size
        int maxPosition = position + utfLength;
        os.write('"');
        while (position < maxPosition) {
            char c = UTF8StringUtil.charAt(b, position);
            int sz = UTF8StringUtil.charSize(b, position);
            switch (c) {
                // escape
                case '\b':
                    os.write('\\');
                    os.write('b');
                    position += sz;
                    break;
                case '\f':
                    os.write('\\');
                    os.write('f');
                    position += sz;
                    break;
                case '\n':
                    os.write('\\');
                    os.write('n');
                    position += sz;
                    break;
                case '\r':
                    os.write('\\');
                    os.write('r');
                    position += sz;
                    break;
                case '\t':
                    os.write('\\');
                    os.write('t');
                    position += sz;
                    break;
                case '\\':
                case '"':
                    os.write('\\');
                default:
                    switch (sz) {
                        case 1:
                            if (c <= (byte) 0x1f || c == (byte) 0x7f) {
                                // this is to print out "control code points" (single byte UTF-8 representation,
                                // value up to 0x1f or 0x7f) in the 'uXXXX' format
                                writeUEscape(os, c);
                                ++position;
                                sz = 0; // no more processing
                            }
                            break;

                        case 2:
                            // 2-byte encodings of some code points in modified UTF-8 as described in
                            // DataInput.html#modified-utf-8
                            //
                            //         110xxxxx  10xxxxxx
                            // U+0000     00000    000000   C0 80
                            // U+0080     00010    000000   C2 80
                            // U+009F     00010    011111   C2 9F
                            switch (b[position]) {
                                case (byte) 0xc0:
                                    if (b[position + 1] == (byte) 0x80) {
                                        // special treatment for the U+0000 code point as described in
                                        // DataInput.html#modified-utf-8
                                        writeUEscape(os, c);
                                        position += 2;
                                        sz = 0; // no more processing
                                    }
                                    break;
                                case (byte) 0xc2:
                                    if (b[position + 1] <= (byte) 0x9f) {
                                        // special treatment for the U+0080 to U+009F code points
                                        writeUEscape(os, c);
                                        position += 2;
                                        sz = 0; // no more processing
                                    }
                                    break;
                            }
                            break;
                        case 3:
                            // special treatment for surrogates
                            if (Character.isHighSurrogate(c)) {
                                position += writeSupplementaryChar(os, b, maxPosition, position, c, sz);
                                sz = 0;
                            }
                            break;
                    }
                    while (sz > 0) {
                        os.write(b[position]);
                        ++position;
                        --sz;
                    }
                    break;
            }
        }
        os.write('\"');
    }

    private static void writeUEscape(OutputStream os, char c) throws IOException {
        os.write('\\');
        os.write('u');
        os.write('0');
        os.write('0');
        os.write(HexPrinter.hex((c >>> 4) & 0x0f, HexPrinter.Case.LOWER_CASE));
        os.write(HexPrinter.hex(c & 0x0f, HexPrinter.Case.LOWER_CASE));
    }

    /**
     * Writes a supplementary char consisting of high and low surrogates
     *
     * @return The length of the surrogates
     * @throws IOException
     */
    private static int writeSupplementaryChar(OutputStream os, byte[] src, int limit, int highSurrogatePos,
            char highSurrogate, int highSurrogateSize) throws IOException {
        final int lowSurrogatePos = highSurrogatePos + highSurrogateSize;
        if (lowSurrogatePos >= limit) {
            throw new IllegalStateException("malformed utf8 input");
        }
        final char lowSurrogate = UTF8StringUtil.charAt(src, lowSurrogatePos);
        final int lowSurrogateSize = UTF8StringUtil.charSize(src, lowSurrogatePos);
        os.write(new String(new char[] { highSurrogate, lowSurrogate }).getBytes(StandardCharsets.UTF_8));
        return highSurrogateSize + lowSurrogateSize;
    }

}
