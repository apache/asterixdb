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
package org.apache.asterix.om.base.temporal;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.TimeZone;

import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * {@link DateTimeFormatUtils} provides the utility methods to parse and print a date/time/datetime
 * value based on the given format string. The format string may contain the following <b>format characters</b> (note that
 * format string is <b>case-sensitive</b>):
 * <p/>
 * - <b>Y</b>: a digit for the year field. At most 4 year format characters are allowed for a valid format string.<br/>
 * - <b>M</b>: a digit or character for the month field. At most 3 month format characters are allowed for a valid format string. When three month format characters are used, the shorten month names (like JAN, FEB etc.) are expected in the string to be parsed. Otherwise digits are expected.<br/>
 * - <b>Q</b>: a digit for the quarter field (1-4). At most 2 format characters are allowed.<br/>
 * - <b>D</b>: a digit for the day field. At most 2 day format characters are allowed.<br/>
 * - <b>h</b>: a digit for the hour field. At most 2 hour format characters are allowed.<br/>
 * - <b>m</b>: a digit for the minute field. At most 2 minute format characters are allowed.<br/>
 * - <b>s</b>: a digit for the second field. At most 2 second format characters are allowed.<br/>
 * - <b>n</b>: a digit for the millisecond field. At most 3 millisecond format characters are allowed.<br/>
 * - <b>a</b>: the AM/PM field. At most 1 am/pm format character is allowed, and it matches with AM and PM case-insensitively. <br/>
 * - <b>z</b>: (parse only) the timezone field. At most 1 timezone format characters are allowed. The valid timezone string matching with this format character include:<br/>
 * -- <b>Z</b>: a single upper-case character representing the UTC timezone;<br/>
 * -- <b>[UTC|GMT]+xx[:]xx</b>: representing a timezone by providing the actual offset time from the UTC time;<br/>
 * -- A string representation of a timezone like PST, Asia/Shanghai. The names of the timezones are following the Zoneinfo database provided by the JDK library. See {@link TimeZone} for more details on this.<br/>
 * - <b>Separators</b>: separators that can be used to separate the different fields. Currently only the following characters can be used as separator: <b>-(hyphen), :(colon), /(solidus), .(period) and ,(comma)</b>.
 * <p/>
 * For the matching algorithm, both the format string and the data string are scanned from the beginning to the end, and the algorithm tried to match the format with the characters/digits/separators in the data string. The format string represents the <b>minimum</b> length of the required field (similar to the C-style printf formatting). This means that something like a year <it>1990</it> will match with the format strings <it>Y, YY, YYY and YYYY</it>.
 * <p/>
 * If the given string cannot be parsed by the given format string, an {@link AsterixTemporalTypeParseException} will be returned.
 */
public class DateTimeFormatUtils {

    private static final GregorianCalendarSystem CAL = GregorianCalendarSystem.getInstance();

    private static final Charset ENCODING = StandardCharsets.UTF_8;

    // For time
    private static final char HOUR_CHAR = 'h';
    private static final char MINUTE_CHAR = 'm';
    private static final char SECOND_CHAR = 's';
    private static final char MILLISECOND_CHAR = 'n';
    private static final char MILLISECOND_CHAR_ALT = 'S';
    private static final char AMPM_CHAR = 'a';
    private static final char TIMEZONE_CHAR = 'z';

    private static final int MAX_HOUR_CHARS = 2;
    private static final int MAX_MINUTE_CHARS = 2;
    private static final int MAX_SECOND_CHARS = 2;
    private static final int MAX_MILLISECOND_CHARS = 3;
    private static final int MAX_AMPM_CHARS = 1;
    private static final int MAX_TIMEZONE_CHARS = 1;

    private enum DateTimeProcessState {
        YEAR,
        QUARTER,
        MONTH,
        DAY,
        WEEKDAY,
        HOUR,
        MINUTE,
        SECOND,
        MILLISECOND,
        AMPM,
        TIMEZONE,
        SKIPPER,
        SEPARATOR
    }

    // For date
    private static final char YEAR_CHAR = 'Y';
    private static final char QUARTER_CHAR = 'Q';
    private static final char MONTH_CHAR = 'M';
    private static final char DAY_CHAR = 'D';
    private static final char WEEKDAY_CHAR = 'E';

    private static final int MAX_YEAR_CHARS = 4;
    private static final int MAX_QUARTER_CHARS = 2;
    private static final int MAX_MONTH_CHARS = 4;
    private static final int MAX_DAY_CHARS_PARSE = 2;
    private static final int MAX_DAY_CHARS_PRINT = 3; // + DDD = Day of Year
    private static final int MIN_WEEKDAY_CHAR = 3;
    private static final int MAX_WEEKDAY_CHAR = 4;

    private static final byte[][] MONTH_NAMES = new byte[][] { "jan".getBytes(ENCODING), "feb".getBytes(ENCODING),
            "mar".getBytes(ENCODING), "apr".getBytes(ENCODING), "may".getBytes(ENCODING), "jun".getBytes(ENCODING),
            "jul".getBytes(ENCODING), "aug".getBytes(ENCODING), "sep".getBytes(ENCODING), "oct".getBytes(ENCODING),
            "nov".getBytes(ENCODING), "dec".getBytes(ENCODING) };

    private static final byte[][] MONTH_FULL_NAMES =
            new byte[][] { "january".getBytes(ENCODING), "february".getBytes(ENCODING), "march".getBytes(ENCODING),
                    "april".getBytes(ENCODING), "may".getBytes(ENCODING), "june".getBytes(ENCODING),
                    "july".getBytes(ENCODING), "august".getBytes(ENCODING), "september".getBytes(ENCODING),
                    "october".getBytes(ENCODING), "november".getBytes(ENCODING), "december".getBytes(ENCODING) };

    private static final byte[][] WEEKDAY_NAMES = new byte[][] { "sun".getBytes(ENCODING), "mon".getBytes(ENCODING),
            "tue".getBytes(ENCODING), "wed".getBytes(ENCODING), "thu".getBytes(ENCODING), "fri".getBytes(ENCODING),
            "sat".getBytes(ENCODING) };

    private static final byte[][] WEEKDAY_FULL_NAMES = new byte[][] { "sunday".getBytes(ENCODING),
            "monday".getBytes(ENCODING), "tuesday".getBytes(ENCODING), "wednesday".getBytes(ENCODING),
            "thursday".getBytes(ENCODING), "friday".getBytes(ENCODING), "saturday".getBytes(ENCODING) };

    private static final byte[] UTC_BYTEARRAY = "utc".getBytes(ENCODING);
    private static final byte[] GMT_BYTEARRAY = "gmt".getBytes(ENCODING);

    private static final byte[] AM_BYTEARRAY = "am".getBytes(ENCODING);
    private static final byte[] PM_BYTEARRAY = "pm".getBytes(ENCODING);

    // Separators, for both time and date
    private static final char HYPHEN_CHAR = '-';
    private static final char COLON_CHAR = ':';
    private static final char SOLIDUS_CHAR = '/';
    private static final char PERIOD_CHAR = '.';
    private static final char COMMA_CHAR = ',';
    private static final char T_CHAR = 'T';

    // Skipper, representing a field with characters and numbers that to be skipped
    private static final char SKIPPER_CHAR = 'O';
    private static final int MAX_SKIPPER_CHAR = 1;

    private static final byte TO_LOWER_OFFSET = 'A' - 'a';

    private static Comparator<byte[]> byteArrayComparator = new Comparator<byte[]>() {
        @Override
        public int compare(byte[] o1, byte[] o2) {
            int i = 0;
            for (; i < o1.length && i < o2.length; i++) {
                if (o1[i] != o2[i]) {
                    return o1[i] - o2[i];
                }
            }
            if (i < o1.length) {
                return -1;
            } else if (i < o2.length) {
                return 1;
            }
            return 0;
        }
    };

    private static final byte[][] TIMEZONE_IDS;
    private static final TimeZone[] TIMEZONE_VALUES;

    static {
        String[] tzIds = TimeZone.getAvailableIDs();
        int tzCount = tzIds.length;
        TIMEZONE_IDS = new byte[tzCount][];
        TIMEZONE_VALUES = new TimeZone[tzCount];

        for (int i = 0; i < tzCount; i++) {
            TIMEZONE_IDS[i] = tzIds[i].getBytes(ENCODING);
        }
        Arrays.sort(TIMEZONE_IDS, byteArrayComparator);
        for (int i = 0; i < tzCount; i++) {
            TIMEZONE_VALUES[i] = TimeZone.getTimeZone(new String(TIMEZONE_IDS[i], ENCODING));
        }
    }

    private static final DateTimeFormatUtils INSTANCE = new DateTimeFormatUtils();

    public static DateTimeFormatUtils getInstance() {
        return INSTANCE;
    }

    private DateTimeFormatUtils() {
    }

    private int parseFormatField(byte[] format, int formatStart, int formatLength, int formatPointer, char formatChar,
            int maxAllowedFormatCharCopied) throws AsterixTemporalTypeParseException {

        int formatCharCopies = 0;

        formatPointer++;
        formatCharCopies++;
        while (formatPointer < formatLength && format[formatStart + formatPointer] == formatChar) {
            formatPointer++;
            formatCharCopies++;
        }
        if (formatCharCopies > maxAllowedFormatCharCopied) {
            throw new AsterixTemporalTypeParseException(
                    "The format string for " + formatChar + " is too long: expected no more than "
                            + maxAllowedFormatCharCopied + " but got " + formatCharCopies);
        }

        return formatCharCopies;
    }

    public enum DateTimeParseMode {
        DATE_ONLY,
        TIME_ONLY,
        DATETIME
    }

    public static boolean byteArrayEqualToString(byte[] barray, int start, int length, byte[] str) {
        if (length != str.length) {
            return false;
        } else {
            return byteArrayBeingWithString(barray, start, length, str);
        }
    }

    public static boolean byteArrayBeingWithString(byte[] barray, int start, int length, byte[] str) {
        boolean beginWith = true;
        if (length <= str.length) {
            for (int i = 0; i < length; i++) {
                if (toLower(barray[start + i]) != str[i]) {
                    beginWith = false;
                    break;
                }
            }
        } else {
            beginWith = false;
        }
        return beginWith;
    }

    private int monthIDSearch(byte[] barray, int start, int length, boolean useShortNames) {
        byte[][] monthNames = useShortNames ? MONTH_NAMES : MONTH_FULL_NAMES;
        for (int i = 0; i < monthNames.length; i++) {
            if (byteArrayEqualToString(barray, start, length, monthNames[i])) {
                return i;
            }
        }
        return -1;
    }

    @Deprecated
    public static int weekdayIDSearchLax(byte[] barray, int start, int length, boolean useShortNames) {
        byte[][] weekdayNames = useShortNames ? WEEKDAY_NAMES : WEEKDAY_FULL_NAMES;
        for (int i = 0; i < weekdayNames.length; i++) {
            if (byteArrayBeingWithString(barray, start, length, weekdayNames[i])) {
                return i;
            }
        }
        return -1;
    }

    public static int weekdayIDSearch(byte[] barray, int start, int length, boolean useShortNames) {
        byte[][] weekdayNames = useShortNames ? WEEKDAY_NAMES : WEEKDAY_FULL_NAMES;
        for (int i = 0; i < weekdayNames.length; i++) {
            if (byteArrayEqualToString(barray, start, length, weekdayNames[i])) {
                return i;
            }
        }
        return -1;
    }

    public static TimeZone findTimeZone(byte[] barray, int start, int length) {
        int idx = Arrays.binarySearch(TIMEZONE_IDS, 0, TIMEZONE_IDS.length,
                Arrays.copyOfRange(barray, start, start + length), byteArrayComparator);
        return idx >= 0 ? TIMEZONE_VALUES[idx] : null;
    }

    private int indexOf(byte[] barray, int start, int length, char c) {
        int i = 0;
        for (; i < length; i++) {
            if (barray[start + i] == c) {
                return i;
            }
        }
        return -1;
    }

    private static byte toLower(byte b) {
        if (b >= 'A' && b <= 'Z') {
            return (byte) (b - TO_LOWER_OFFSET);
        }
        return b;
    }

    private static byte toUpper(byte b) {
        if (b >= 'a' && b <= 'z') {
            return (byte) (b + TO_LOWER_OFFSET);
        }
        return b;
    }

    public boolean parseDateTime(AMutableInt64 outChronon, byte[] data, int dataStart, int dataLength, byte[] format,
            int formatStart, int formatLength, DateTimeParseMode parseMode, boolean raiseParseDataError)
            throws AsterixTemporalTypeParseException {
        return parseDateTime(outChronon, null, null, null, data, dataStart, dataLength, format, formatStart,
                formatLength, parseMode, raiseParseDataError, '\0', false);
    }

    public boolean parseDateTime(AMutableInt64 outChronon, Mutable<Boolean> outTimeZoneExists,
            AMutableInt32 outTimeZone, Mutable<Character> dateTimeSeparatorOut, byte[] data, int dataStart,
            int dataLength, byte[] format, int formatStart, int formatLength, DateTimeParseMode parseMode,
            boolean raiseParseDataError, char altSeparatorChar, boolean adjustChrononByTimezone)
            throws AsterixTemporalTypeParseException {
        int year = 0, month = 0, day = 0, hour = 0, min = 0, sec = 0, ms = 0, timezone = 0;
        boolean timezoneExists = false;

        boolean negativeYear = false;
        int formatCharCopies;

        int dataStringPointer = 0, formatPointer = 0;

        char separatorChar = '\0';

        char lastSeparatorChar = '\0';

        char dateTimeSeparatorChar = 'T'; //default dateTimeSeparator

        DateTimeProcessState processState;

        int pointerMove;

        while (dataStringPointer < dataLength && formatPointer < formatLength) {
            formatCharCopies = 0;
            switch (format[formatStart + formatPointer]) {
                case YEAR_CHAR:
                    processState = DateTimeProcessState.YEAR;
                    pointerMove = parseFormatField(format, formatStart, formatLength, formatPointer, YEAR_CHAR,
                            MAX_YEAR_CHARS);
                    formatPointer += pointerMove;
                    formatCharCopies += pointerMove;
                    break;
                case QUARTER_CHAR:
                    processState = DateTimeProcessState.QUARTER;
                    pointerMove = parseFormatField(format, formatStart, formatLength, formatPointer, QUARTER_CHAR,
                            MAX_QUARTER_CHARS);
                    formatPointer += pointerMove;
                    formatCharCopies += pointerMove;
                    break;
                case MONTH_CHAR:
                    processState = DateTimeProcessState.MONTH;
                    pointerMove = parseFormatField(format, formatStart, formatLength, formatPointer, MONTH_CHAR,
                            MAX_MONTH_CHARS);
                    formatPointer += pointerMove;
                    formatCharCopies += pointerMove;
                    break;
                case DAY_CHAR:
                    processState = DateTimeProcessState.DAY;
                    pointerMove = parseFormatField(format, formatStart, formatLength, formatPointer, DAY_CHAR,
                            MAX_DAY_CHARS_PARSE);
                    formatPointer += pointerMove;
                    formatCharCopies += pointerMove;
                    break;
                case WEEKDAY_CHAR:
                    processState = DateTimeProcessState.WEEKDAY;
                    pointerMove = parseFormatField(format, formatStart, formatLength, formatPointer, WEEKDAY_CHAR,
                            MAX_WEEKDAY_CHAR);
                    if (pointerMove < MIN_WEEKDAY_CHAR) {
                        throw new AsterixTemporalTypeParseException(
                                String.format("Expected at least %d '%s' characters but got %d", MIN_WEEKDAY_CHAR,
                                        WEEKDAY_CHAR, pointerMove));
                    }
                    formatPointer += pointerMove;
                    formatCharCopies += pointerMove;
                    break;
                case HOUR_CHAR:
                    processState = DateTimeProcessState.HOUR;
                    pointerMove = parseFormatField(format, formatStart, formatLength, formatPointer, HOUR_CHAR,
                            MAX_HOUR_CHARS);
                    formatPointer += pointerMove;
                    formatCharCopies += pointerMove;
                    break;
                case MINUTE_CHAR:
                    processState = DateTimeProcessState.MINUTE;
                    pointerMove = parseFormatField(format, formatStart, formatLength, formatPointer, MINUTE_CHAR,
                            MAX_MINUTE_CHARS);
                    formatPointer += pointerMove;
                    formatCharCopies += pointerMove;
                    break;
                case SECOND_CHAR:
                    processState = DateTimeProcessState.SECOND;
                    pointerMove = parseFormatField(format, formatStart, formatLength, formatPointer, SECOND_CHAR,
                            MAX_SECOND_CHARS);
                    formatPointer += pointerMove;
                    formatCharCopies += pointerMove;
                    break;
                case MILLISECOND_CHAR:
                    processState = DateTimeProcessState.MILLISECOND;
                    pointerMove = parseFormatField(format, formatStart, formatLength, formatPointer, MILLISECOND_CHAR,
                            MAX_MILLISECOND_CHARS);
                    formatPointer += pointerMove;
                    formatCharCopies += pointerMove;
                    break;
                case MILLISECOND_CHAR_ALT:
                    processState = DateTimeProcessState.MILLISECOND;
                    pointerMove = parseFormatField(format, formatStart, formatLength, formatPointer,
                            MILLISECOND_CHAR_ALT, MAX_MILLISECOND_CHARS);
                    formatPointer += pointerMove;
                    formatCharCopies += pointerMove;
                    break;
                case AMPM_CHAR:
                    processState = DateTimeProcessState.AMPM;
                    pointerMove = parseFormatField(format, formatStart, formatLength, formatPointer, AMPM_CHAR,
                            MAX_AMPM_CHARS);
                    formatPointer += pointerMove;
                    formatCharCopies += pointerMove;
                    break;

                case TIMEZONE_CHAR:
                    processState = DateTimeProcessState.TIMEZONE;
                    pointerMove = parseFormatField(format, formatStart, formatLength, formatPointer, TIMEZONE_CHAR,
                            MAX_TIMEZONE_CHARS);
                    formatPointer += pointerMove;
                    formatCharCopies += pointerMove;
                    break;
                case SKIPPER_CHAR:
                    processState = DateTimeProcessState.SKIPPER;
                    pointerMove = parseFormatField(format, formatStart, formatLength, formatPointer, SKIPPER_CHAR,
                            MAX_SKIPPER_CHAR);
                    formatPointer += pointerMove;
                    formatCharCopies += pointerMove;
                    break;
                case ' ':
                case HYPHEN_CHAR:
                case COLON_CHAR:
                case SOLIDUS_CHAR:
                case PERIOD_CHAR:
                case COMMA_CHAR:
                case T_CHAR:
                    // separator
                    separatorChar = (char) format[formatStart + formatPointer];
                    processState = DateTimeProcessState.SEPARATOR;
                    formatPointer++;
                    formatCharCopies++;
                    while (formatPointer < formatLength
                            && (char) (format[formatStart + formatPointer]) == separatorChar) {
                        formatPointer++;
                        formatCharCopies++;
                    }
                    break;

                default:
                    throw new AsterixTemporalTypeParseException("Unexpected date format string at "
                            + (formatStart + formatPointer) + ": " + (char) format[formatStart + formatPointer]);
            }

            // check whether the process state is valid for the parse mode

            switch (processState) {
                case YEAR:
                case QUARTER:
                case MONTH:
                case DAY:
                    if (parseMode == DateTimeParseMode.TIME_ONLY) {
                        throw new AsterixTemporalTypeParseException(
                                "Unexpected date format string when parsing a time value");
                    }
                    break;
                case HOUR:
                case MINUTE:
                case SECOND:
                case MILLISECOND:
                case AMPM:
                case TIMEZONE:
                    if (parseMode == DateTimeParseMode.DATE_ONLY) {
                        throw new AsterixTemporalTypeParseException(
                                "Unexpected time format string when parsing a date value");
                    }
                    break;
                default:
                    // do nothing
            }

            switch (processState) {
                case YEAR:
                    if (dataStringPointer < dataLength && data[dataStart + dataStringPointer] == HYPHEN_CHAR) {
                        negativeYear = true;
                        dataStringPointer++;
                    }
                case DAY:
                    int maxAllowedFormatCharCopies = (processState == DateTimeProcessState.YEAR) ? 4 : 2;
                    int parsedValue = 0;
                    int processedFieldsCount = 0;
                    for (int i = 0; i < formatCharCopies; i++) {
                        if (data[dataStart + dataStringPointer] < '0' || data[dataStart + dataStringPointer] > '9') {
                            if (raiseParseDataError) {
                                throw new AsterixTemporalTypeParseException("Unexpected char for year field at "
                                        + (dataStart + dataStringPointer) + ": " + data[dataStart + dataStringPointer]);
                            } else {
                                return false;
                            }
                        }
                        parsedValue = parsedValue * 10 + (data[dataStart + dataStringPointer] - '0');
                        dataStringPointer++;
                        processedFieldsCount++;
                    }
                    // for more digits
                    while (processedFieldsCount < maxAllowedFormatCharCopies && dataStringPointer < dataLength
                            && data[dataStart + dataStringPointer] >= '0'
                            && data[dataStart + dataStringPointer] <= '9') {
                        parsedValue = parsedValue * 10 + (data[dataStart + dataStringPointer] - '0');
                        dataStringPointer++;
                        processedFieldsCount++;
                    }
                    if (processState == DateTimeProcessState.YEAR) {
                        year = parsedValue;
                        if (negativeYear) {
                            year *= -1;
                        }
                        // Allow month and day to be missing if we parsed year
                        if (month == 0) {
                            month = GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.MONTH.ordinal()];
                        }
                        if (day == 0) {
                            day = GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.DAY.ordinal()];
                        }
                    } else {
                        if (parsedValue == 0) {
                            if (raiseParseDataError) {
                                throw new AsterixTemporalTypeParseException(
                                        "Incorrect day value at " + (dataStart + dataStringPointer));
                            } else {
                                return false;
                            }
                        }
                        day = parsedValue;
                    }
                    break;
                case QUARTER:
                    // the month is in the number format
                    parsedValue = 0;
                    int processedQuarterFieldsCount = 0;
                    for (int i = 0; i < formatCharCopies; i++) {
                        if (data[dataStart + dataStringPointer] < '0' || data[dataStart + dataStringPointer] > '9') {
                            if (raiseParseDataError) {
                                throw new AsterixTemporalTypeParseException("Unexpected char for quarter field at "
                                        + (dataStart + dataStringPointer) + ": " + data[dataStart + dataStringPointer]);
                            } else {
                                return false;
                            }
                        }
                        parsedValue = parsedValue * 10 + (data[dataStart + dataStringPointer] - '0');
                        dataStringPointer++;
                        if (processedQuarterFieldsCount++ > 2) {
                            if (raiseParseDataError) {
                                throw new AsterixTemporalTypeParseException("Unexpected char for quarter field at "
                                        + (dataStart + dataStringPointer) + ": " + data[dataStart + dataStringPointer]);
                            } else {
                                return false;
                            }
                        }
                    }
                    // if there are more than 2 digits for the quarter string
                    while (processedQuarterFieldsCount < 2 && dataStringPointer < dataLength
                            && data[dataStart + dataStringPointer] >= '0'
                            && data[dataStart + dataStringPointer] <= '9') {
                        parsedValue = parsedValue * 10 + (data[dataStart + dataStringPointer] - '0');
                        dataStringPointer++;
                        processedQuarterFieldsCount++;
                    }
                    if (parsedValue == 0) {
                        if (raiseParseDataError) {
                            throw new AsterixTemporalTypeParseException(
                                    "Incorrect quarter value at " + (dataStart + dataStringPointer));
                        } else {
                            return false;
                        }
                    }
                    month = (parsedValue - 1) * 3 + 1;
                    // Allow day to be missing if we parsed quarter
                    if (day == 0) {
                        day = GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.DAY.ordinal()];
                    }
                    break;
                case MONTH:
                    if (formatCharCopies >= 3) {
                        // the month is in the text format
                        int processedMonthFieldsCount = 0;
                        while (((dataStringPointer + processedMonthFieldsCount < dataLength)) && ((data[dataStart
                                + dataStringPointer + processedMonthFieldsCount] >= 'a'
                                && data[dataStart + dataStringPointer + processedMonthFieldsCount] <= 'z')
                                || (data[dataStart + dataStringPointer + processedMonthFieldsCount] >= 'A'
                                        && data[dataStart + dataStringPointer + processedMonthFieldsCount] <= 'Z'))) {
                            processedMonthFieldsCount++;
                        }
                        boolean useShortNames = formatCharCopies == 3;
                        int monthNameMatch = monthIDSearch(data, dataStart + dataStringPointer,
                                processedMonthFieldsCount, useShortNames);
                        if (monthNameMatch >= 0) {
                            month = monthNameMatch + 1;
                            dataStringPointer += processedMonthFieldsCount;
                        } else {
                            if (raiseParseDataError) {
                                throw new AsterixTemporalTypeParseException(
                                        "Unrecognizable month string " + (char) data[dataStart + dataStringPointer]
                                                + " " + (char) data[dataStart + dataStringPointer + 1] + " "
                                                + (char) data[dataStart + dataStringPointer + 2]);
                            } else {
                                return false;
                            }
                        }
                    } else {
                        // the month is in the number format
                        parsedValue = 0;
                        int processedMonthFieldsCount = 0;
                        for (int i = 0; i < formatCharCopies; i++) {
                            if (data[dataStart + dataStringPointer] < '0'
                                    || data[dataStart + dataStringPointer] > '9') {
                                if (raiseParseDataError) {
                                    throw new AsterixTemporalTypeParseException(
                                            "Unexpected char for month field at " + (dataStart + dataStringPointer)
                                                    + ": " + data[dataStart + dataStringPointer]);
                                } else {
                                    return false;
                                }
                            }
                            parsedValue = parsedValue * 10 + (data[dataStart + dataStringPointer] - '0');
                            dataStringPointer++;
                            if (processedMonthFieldsCount++ > 2) {
                                if (raiseParseDataError) {
                                    throw new AsterixTemporalTypeParseException(
                                            "Unexpected char for month field at " + (dataStart + dataStringPointer)
                                                    + ": " + data[dataStart + dataStringPointer]);
                                } else {
                                    return false;
                                }
                            }
                        }
                        // if there are more than 2 digits for the month string
                        while (processedMonthFieldsCount < 2 && dataStringPointer < dataLength
                                && data[dataStart + dataStringPointer] >= '0'
                                && data[dataStart + dataStringPointer] <= '9') {
                            parsedValue = parsedValue * 10 + (data[dataStart + dataStringPointer] - '0');
                            dataStringPointer++;
                            processedMonthFieldsCount++;
                        }
                        if (parsedValue == 0) {
                            if (raiseParseDataError) {
                                throw new AsterixTemporalTypeParseException(
                                        "Incorrect month value at " + (dataStart + dataStringPointer));
                            } else {
                                return false;
                            }
                        }
                        month = parsedValue;
                    }
                    // Allow day to be missing if we parsed month
                    if (day == 0) {
                        day = GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.DAY.ordinal()];
                    }
                    break;
                case WEEKDAY:
                    int processedWeekdayFieldsCount = 0;
                    while ((dataStringPointer + processedWeekdayFieldsCount < dataLength) && ((data[dataStart
                            + dataStringPointer + processedWeekdayFieldsCount] >= 'a'
                            && data[dataStart + dataStringPointer + processedWeekdayFieldsCount] <= 'z')
                            || (data[dataStart + dataStringPointer + processedWeekdayFieldsCount] >= 'A'
                                    && data[dataStart + dataStringPointer + processedWeekdayFieldsCount] <= 'Z'))) {
                        processedWeekdayFieldsCount++;
                    }
                    // match the weekday name
                    boolean useShortNames = formatCharCopies == 3;
                    int weekdayNameMatch = weekdayIDSearch(data, dataStart + dataStringPointer,
                            processedWeekdayFieldsCount, useShortNames);
                    if (weekdayNameMatch < 0) {
                        if (raiseParseDataError) {
                            throw new AsterixTemporalTypeParseException("Unexpected string for day-of-week: "
                                    + new String(data, dataStart + dataStringPointer,
                                            dataStart + dataStringPointer + processedWeekdayFieldsCount, ENCODING));
                        } else {
                            return false;
                        }
                    }
                    dataStringPointer += processedWeekdayFieldsCount;
                    break;
                case HOUR:
                    dateTimeSeparatorChar = lastSeparatorChar;
                case MINUTE:
                case SECOND:
                case MILLISECOND:
                    int processFieldsCount = 0;
                    int expectedMaxCount = (processState == DateTimeProcessState.MILLISECOND) ? 3 : 2;
                    parsedValue = 0;
                    for (int i = 0; i < formatCharCopies; i++) {
                        if (data[dataStart + dataStringPointer] < '0' || data[dataStart + dataStringPointer] > '9') {
                            if (raiseParseDataError) {
                                throw new AsterixTemporalTypeParseException("Unexpected char for " + processState.name()
                                        + " field at " + (dataStart + dataStringPointer) + ": "
                                        + data[dataStart + dataStringPointer]);
                            } else {
                                return false;
                            }

                        }
                        parsedValue = parsedValue * 10 + (data[dataStart + dataStringPointer] - '0');
                        dataStringPointer++;
                        if (processFieldsCount++ > expectedMaxCount) {
                            if (raiseParseDataError) {
                                throw new AsterixTemporalTypeParseException(
                                        "Unexpected char for " + processState.name() + " field at " + dataStringPointer
                                                + ": " + data[dataStart + dataStringPointer]);
                            } else {
                                return false;
                            }
                        }
                    }
                    // if there are more than formatCharCopies digits for the hour string
                    while (processFieldsCount < expectedMaxCount && dataStringPointer < dataLength
                            && data[dataStart + dataStringPointer] >= '0'
                            && data[dataStart + dataStringPointer] <= '9') {
                        parsedValue = parsedValue * 10 + (data[dataStart + dataStringPointer] - '0');
                        dataStringPointer++;
                        processFieldsCount++;
                    }
                    if (processState == DateTimeProcessState.HOUR) {
                        hour = parsedValue;
                    } else if (processState == DateTimeProcessState.MINUTE) {
                        min = parsedValue;
                    } else if (processState == DateTimeProcessState.SECOND) {
                        sec = parsedValue;
                    } else if (processState == DateTimeProcessState.MILLISECOND) {
                        //read remaining millis values
                        while (dataStringPointer < dataLength && data[dataStart + dataStringPointer] >= '0'
                                && data[dataStart + dataStringPointer] <= '9') {
                            //parsedValue = parsedValue * 10 + (data[dataStart + dataStringPointer] - '0');
                            dataStringPointer++;
                            processFieldsCount++;
                        }
                        ms = parsedValue;
                        for (int i = processFieldsCount; i < 3; i++) {
                            ms *= 10;
                        }
                    }
                    break;
                case TIMEZONE:
                    if (data[dataStart + dataStringPointer] == 'Z'
                            && ((dataStringPointer + 1 >= dataLength) || (data[dataStart + dataStringPointer + 1] < 'A'
                                    && data[dataStart + dataStringPointer + 1] > 'Z'
                                    && data[dataStart + dataStringPointer + 1] < 'a'
                                    && data[dataStart + dataStringPointer + 1] > 'z'))) {
                        // UTC as Z
                        timezone = 0;
                        dataStringPointer++;
                    } else if ((data[dataStart + dataStringPointer] == '+'
                            || data[dataStart + dataStringPointer] == '-')
                            || (dataStringPointer + 3 < dataLength && (data[dataStart + dataStringPointer + 3] == '+'
                                    || data[dataStart + dataStringPointer + 3] == '-'))) {
                        // UTC+ or GMT+ format
                        if (dataStringPointer + 3 < dataLength && (byteArrayEqualToString(data,
                                dataStart + dataStringPointer, 3, UTC_BYTEARRAY)
                                || byteArrayEqualToString(data, dataStart + dataStringPointer, 3, GMT_BYTEARRAY))) {
                            dataStringPointer += 3;
                        }
                        // parse timezone as +zz:zz or +zzzz
                        boolean negativeTimeZone = false;
                        if (data[dataStart + dataStringPointer] == '-') {
                            negativeTimeZone = true;
                            dataStringPointer++;
                        } else if (data[dataStart + dataStringPointer] == '+') {
                            dataStringPointer++;
                        } else {
                            if (raiseParseDataError) {
                                throw new AsterixTemporalTypeParseException(
                                        "Incorrect timezone hour field: expecting sign + or - but got: "
                                                + data[dataStart + dataStringPointer]);
                            } else {
                                return false;
                            }
                        }
                        parsedValue = 0;
                        // timezone hours
                        for (int i = 0; i < 2; i++) {
                            if (data[dataStart + dataStringPointer + i] >= '0'
                                    && data[dataStart + dataStringPointer + i] <= '9') {
                                parsedValue = parsedValue * 10 + (data[dataStart + dataStringPointer + i] - '0');
                            } else {
                                if (raiseParseDataError) {
                                    throw new AsterixTemporalTypeParseException(
                                            "Unexpected character for timezone hour field at "
                                                    + (dataStart + dataStringPointer) + ": "
                                                    + data[dataStart + dataStringPointer]);
                                } else {
                                    return false;
                                }
                            }
                        }
                        dataStringPointer += 2;
                        // skip the ":" separator
                        if (data[dataStart + dataStringPointer] == ':') {
                            dataStringPointer++;
                        }
                        timezone = (int) (parsedValue * GregorianCalendarSystem.CHRONON_OF_HOUR);
                        parsedValue = 0;
                        // timezone minutes
                        for (int i = 0; i < 2; i++) {
                            if (data[dataStart + dataStringPointer + i] >= '0'
                                    && data[dataStart + dataStringPointer + i] <= '9') {
                                parsedValue = parsedValue * 10 + (data[dataStart + dataStringPointer + i] - '0');
                            } else {
                                if (raiseParseDataError) {
                                    throw new AsterixTemporalTypeParseException(
                                            "Unexpected character for timezone minute field at "
                                                    + (dataStart + dataStringPointer) + ": "
                                                    + data[dataStart + dataStringPointer]);
                                } else {
                                    return false;
                                }
                            }
                        }
                        timezone += (int) (parsedValue * GregorianCalendarSystem.CHRONON_OF_MINUTE);
                        dataStringPointer += 2;
                        if (!negativeTimeZone) {
                            timezone *= -1;
                        }
                    } else {
                        // do lookup from the zoneinfor database
                        int timezoneEndField = dataStringPointer;
                        while (timezoneEndField < dataLength && ((data[dataStart + timezoneEndField] >= '0'
                                && data[dataStart + timezoneEndField] <= '9')
                                || (data[dataStart + timezoneEndField] >= 'a'
                                        && data[dataStart + timezoneEndField] <= 'z')
                                || (data[dataStart + timezoneEndField] >= 'A'
                                        && data[dataStart + timezoneEndField] <= 'Z')
                                || data[dataStart + timezoneEndField] == '/'
                                || data[dataStart + timezoneEndField] == '_')) {
                            timezoneEndField++;
                        }
                        TimeZone tz =
                                findTimeZone(data, dataStart + dataStringPointer, timezoneEndField - dataStringPointer);
                        if (tz != null) {
                            timezone = tz.getRawOffset();
                        } else {
                            if (raiseParseDataError) {
                                throw new AsterixTemporalTypeParseException("Unexpected timezone string: " + new String(
                                        data, dataStart + dataStringPointer, dataStart + timezoneEndField, ENCODING));
                            } else {
                                return false;
                            }
                        }
                        dataStringPointer = timezoneEndField;
                    }
                    timezoneExists = true;
                    break;
                case AMPM:
                    if (dataStringPointer + 1 < dataLength) {
                        if (hour > 12 || hour <= 0) {
                            if (raiseParseDataError) {
                                throw new AsterixTemporalTypeParseException(
                                        "Hour " + hour + " cannot be a time for AM/PM.");
                            } else {
                                return false;
                            }
                        }
                        if (byteArrayEqualToString(data, dataStart + dataStringPointer, 2, AM_BYTEARRAY)) {
                            // do nothing
                        } else if (byteArrayEqualToString(data, dataStart + dataStringPointer, 2, PM_BYTEARRAY)) {
                            hour += 12;
                            if (hour == 24) {
                                hour = 0;
                            }
                        } else {
                            if (raiseParseDataError) {
                                throw new AsterixTemporalTypeParseException("Unexpected string for AM/PM marker "
                                        + new String(data, dataStart + dataStringPointer,
                                                dataStart + dataStringPointer + 2, ENCODING));
                            } else {
                                return false;
                            }
                        }
                        dataStringPointer += 2;
                    } else {
                        if (raiseParseDataError) {
                            throw new AsterixTemporalTypeParseException("Cannot find valid AM/PM marker.");
                        } else {
                            return false;
                        }
                    }
                    break;
                case SKIPPER:
                    // just skip all continuous character and numbers
                    while ((data[dataStart + dataStringPointer] >= 'a' && data[dataStart + dataStringPointer] <= 'z')
                            || (data[dataStart + dataStringPointer] >= 'A'
                                    && data[dataStart + dataStringPointer] <= 'Z')
                            || (data[dataStart + dataStringPointer] >= '0'
                                    && data[dataStart + dataStringPointer] <= '9')) {
                        dataStringPointer++;
                    }
                    break;
                case SEPARATOR:
                    for (int i = 0; i < formatCharCopies; i++) {
                        byte b = data[dataStart + dataStringPointer];
                        boolean match =
                                (char) b == separatorChar || (altSeparatorChar != '\0' && (char) b == altSeparatorChar);
                        if (!match) {
                            if (raiseParseDataError) {
                                throw new AsterixTemporalTypeParseException(
                                        "Expecting separator " + separatorChar + " but got " + b);
                            } else {
                                return false;
                            }
                        }
                        lastSeparatorChar = (char) b;
                        dataStringPointer++;
                    }
                    break;
                default:
                    throw new AsterixTemporalTypeParseException(
                            "Unexpected time format information when parsing a date value");
            }
        }

        if (dataStringPointer < dataLength) {
            if (raiseParseDataError) {
                throw new AsterixTemporalTypeParseException(
                        "The given data string is not fully parsed by the given format string");
            } else {
                return false;
            }
        }

        if (formatPointer < formatLength) {
            if (raiseParseDataError) {
                throw new AsterixTemporalTypeParseException(
                        "The given format string is not fully used for the given data string");
            } else {
                return false;
            }
        }

        long chronon;
        if (parseMode == DateTimeParseMode.TIME_ONLY) {
            int minYear = GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.YEAR.ordinal()];
            int minMonth = GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.MONTH.ordinal()];
            int minDay = GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.DAY.ordinal()];
            if (!CAL.validate(minYear, minMonth, minDay, hour, min, sec, ms)) {
                if (raiseParseDataError) {
                    throw new AsterixTemporalTypeParseException("Invalid time value");
                } else {
                    return false;
                }
            }
            chronon = CAL.getChronon(hour, min, sec, ms);
        } else {
            if (!CAL.validate(year, month, day, hour, min, sec, ms)) {
                if (raiseParseDataError) {
                    throw new AsterixTemporalTypeParseException("Invalid date/time value");
                } else {
                    return false;
                }
            }
            chronon = CAL.getChronon(year, month, day, hour, min, sec, ms);
        }

        if (timezoneExists && adjustChrononByTimezone) {
            if (!CAL.validateTimeZone(timezone)) {
                if (raiseParseDataError) {
                    throw new AsterixTemporalTypeParseException("Invalid time zone");
                } else {
                    return false;
                }
            }
            chronon += timezone;
        }

        outChronon.setValue(chronon);
        if (dateTimeSeparatorOut != null) {
            dateTimeSeparatorOut.setValue(dateTimeSeparatorChar);
        }
        if (outTimeZoneExists != null) {
            outTimeZoneExists.setValue(timezoneExists);
        }
        if (outTimeZone != null) {
            outTimeZone.setValue(timezone);
        }
        return true;
    }

    public void printDateTime(long chronon, byte[] format, int formatStart, int formatLength, Appendable appender,
            DateTimeParseMode parseMode) throws HyracksDataException {
        int year = CAL.getYear(chronon);
        int month = CAL.getMonthOfYear(chronon, year);
        int day = CAL.getDayOfMonthYear(chronon, year, month);
        int dayOfYear = CAL.getDayOfYear(chronon, year);
        int dayOfWeek = CAL.getDayOfWeek(chronon);
        int hour = CAL.getHourOfDay(chronon);
        int min = CAL.getMinOfHour(chronon);
        int sec = CAL.getSecOfMin(chronon);
        int ms = CAL.getMillisOfSec(chronon);

        int formatCharCopies;

        int formatPointer = 0;

        char separatorChar = '\0';

        DateTimeProcessState processState;

        int pointerMove;

        boolean usePM = false;
        if (indexOf(format, formatStart, formatLength, 'a') >= 0) {
            if (hour >= 12) {
                usePM = true;
                hour -= 12;
            }
            if (hour == 0) {
                hour = 12;
            }
        }

        while (formatPointer < formatLength) {

            formatCharCopies = 0;

            switch (format[formatStart + formatPointer]) {
                case YEAR_CHAR:
                    processState = DateTimeProcessState.YEAR;
                    pointerMove = parseFormatField(format, formatStart, formatLength, formatPointer, YEAR_CHAR,
                            MAX_YEAR_CHARS);
                    formatPointer += pointerMove;
                    formatCharCopies += pointerMove;
                    break;
                case QUARTER_CHAR:
                    processState = DateTimeProcessState.QUARTER;
                    pointerMove = parseFormatField(format, formatStart, formatLength, formatPointer, QUARTER_CHAR,
                            MAX_QUARTER_CHARS);
                    formatPointer += pointerMove;
                    formatCharCopies += pointerMove;
                    break;
                case MONTH_CHAR:
                    processState = DateTimeProcessState.MONTH;
                    pointerMove = parseFormatField(format, formatStart, formatLength, formatPointer, MONTH_CHAR,
                            MAX_MONTH_CHARS);
                    formatPointer += pointerMove;
                    formatCharCopies += pointerMove;
                    break;
                case DAY_CHAR:
                    processState = DateTimeProcessState.DAY;
                    pointerMove = parseFormatField(format, formatStart, formatLength, formatPointer, DAY_CHAR,
                            MAX_DAY_CHARS_PRINT);
                    formatPointer += pointerMove;
                    formatCharCopies += pointerMove;
                    break;
                case WEEKDAY_CHAR:
                    processState = DateTimeProcessState.WEEKDAY;
                    pointerMove = parseFormatField(format, formatStart, formatLength, formatPointer, WEEKDAY_CHAR,
                            MAX_WEEKDAY_CHAR);
                    if (pointerMove < MIN_WEEKDAY_CHAR) {
                        throw new AsterixTemporalTypeParseException(
                                String.format("Expected at least %d '%s' characters but got %d", MIN_WEEKDAY_CHAR,
                                        WEEKDAY_CHAR, pointerMove));
                    }
                    formatPointer += pointerMove;
                    formatCharCopies += pointerMove;
                    break;
                case HOUR_CHAR:
                    processState = DateTimeProcessState.HOUR;
                    pointerMove = parseFormatField(format, formatStart, formatLength, formatPointer, HOUR_CHAR,
                            MAX_HOUR_CHARS);
                    formatPointer += pointerMove;
                    formatCharCopies += pointerMove;
                    break;
                case MINUTE_CHAR:
                    processState = DateTimeProcessState.MINUTE;
                    pointerMove = parseFormatField(format, formatStart, formatLength, formatPointer, MINUTE_CHAR,
                            MAX_MINUTE_CHARS);
                    formatPointer += pointerMove;
                    formatCharCopies += pointerMove;
                    break;
                case SECOND_CHAR:
                    processState = DateTimeProcessState.SECOND;
                    pointerMove = parseFormatField(format, formatStart, formatLength, formatPointer, SECOND_CHAR,
                            MAX_SECOND_CHARS);
                    formatPointer += pointerMove;
                    formatCharCopies += pointerMove;
                    break;
                case MILLISECOND_CHAR:
                    processState = DateTimeProcessState.MILLISECOND;
                    pointerMove = parseFormatField(format, formatStart, formatLength, formatPointer, MILLISECOND_CHAR,
                            MAX_MILLISECOND_CHARS);
                    formatPointer += pointerMove;
                    formatCharCopies += pointerMove;
                    break;
                case MILLISECOND_CHAR_ALT:
                    processState = DateTimeProcessState.MILLISECOND;
                    pointerMove = parseFormatField(format, formatStart, formatLength, formatPointer,
                            MILLISECOND_CHAR_ALT, MAX_MILLISECOND_CHARS);
                    formatPointer += pointerMove;
                    formatCharCopies += pointerMove;
                    break;
                case AMPM_CHAR:
                    processState = DateTimeProcessState.AMPM;
                    pointerMove = parseFormatField(format, formatStart, formatLength, formatPointer, AMPM_CHAR,
                            MAX_AMPM_CHARS);
                    formatPointer += pointerMove;
                    formatCharCopies += pointerMove;
                    break;

                case ' ':
                case HYPHEN_CHAR:
                case COLON_CHAR:
                case SOLIDUS_CHAR:
                case PERIOD_CHAR:
                case COMMA_CHAR:
                case T_CHAR:
                    // separator
                    separatorChar = (char) format[formatStart + formatPointer];
                    processState = DateTimeProcessState.SEPARATOR;
                    formatPointer++;
                    formatCharCopies++;
                    while (formatPointer < formatLength
                            && format[formatStart + formatPointer] == (byte) separatorChar) {
                        formatPointer++;
                        formatCharCopies++;
                    }
                    break;

                default:
                    throw new HyracksDataException("Unexpected format string at " + (formatStart + formatPointer) + ": "
                            + (char) (format[formatStart + formatPointer]));
            }

            // check whether the process state is valid for the parse mode

            switch (processState) {
                case YEAR:
                case QUARTER:
                case MONTH:
                case DAY:
                case WEEKDAY:
                    if (parseMode == DateTimeParseMode.TIME_ONLY) {
                        throw new HyracksDataException("Unexpected date format string when parsing a time value");
                    }
                    break;
                case HOUR:
                case MINUTE:
                case SECOND:
                case MILLISECOND:
                case AMPM:
                    if (parseMode == DateTimeParseMode.DATE_ONLY) {
                        throw new HyracksDataException("Unexpected time format string when parsing a date value");
                    }
                    break;
                default:
                    // do nothing
            }

            try {
                switch (processState) {
                    case YEAR:
                        if (year < 0) {
                            appender.append('-');
                            year *= -1;
                        }
                    case QUARTER:
                    case MONTH:
                        if (processState == DateTimeProcessState.MONTH && formatCharCopies >= 3) {
                            byte[][] monthNames = formatCharCopies > 3 ? MONTH_FULL_NAMES : MONTH_NAMES;
                            for (byte b : monthNames[month - 1]) {
                                appender.append((char) toUpper(b));
                            }
                            break;
                        }
                    case DAY:
                        int val;
                        if (processState == DateTimeProcessState.YEAR) {
                            val = year;
                        } else if (processState == DateTimeProcessState.QUARTER) {
                            val = ((month - 1) / 3) + 1;
                        } else if (processState == DateTimeProcessState.MONTH) {
                            val = month;
                        } else {
                            val = formatCharCopies == 3 ? dayOfYear : day;
                        }
                        String strVal = String.valueOf(val);
                        int valFieldCount = strVal.length();
                        for (int i = 0; i < formatCharCopies - valFieldCount; i++) {
                            appender.append('0');
                        }
                        appender.append(strVal);
                        break;
                    case WEEKDAY:
                        byte[][] weekdayNames = formatCharCopies == 3 ? WEEKDAY_NAMES : WEEKDAY_FULL_NAMES;
                        byte[] weekday = weekdayNames[dayOfWeek];
                        for (int i = 0; i < weekday.length; i++) {
                            byte b = weekday[i];
                            appender.append((char) (i == 0 ? toUpper(b) : b));
                        }
                        break;
                    case HOUR:
                    case MINUTE:
                    case SECOND:
                        val = 0;
                        if (processState == DateTimeProcessState.HOUR) {
                            val = hour;
                        } else if (processState == DateTimeProcessState.MINUTE) {
                            val = min;
                        } else if (processState == DateTimeProcessState.SECOND) {
                            val = sec;
                        }

                        if (val < 10) {
                            for (int i = 0; i < formatCharCopies - 1; i++) {
                                appender.append('0');
                            }
                        }
                        appender.append(String.valueOf(val));
                        break;
                    case MILLISECOND:
                        String strMS = String.valueOf(ms);
                        int msFieldCount = strMS.length();
                        for (int i = 0; i < 3 - msFieldCount; i++) {
                            appender.append('0');
                        }
                        if (formatCharCopies < 3) {

                            if (formatCharCopies == 1) {
                                if (ms % 100 == 0) {
                                    // the tailing two zeros can be removed
                                    ms = ms / 100;
                                } else if (ms % 10 == 0) {
                                    // the tailing one zero can be removed
                                    ms = ms / 10;
                                }
                            } else {
                                if (ms % 10 == 0) {
                                    // the tailing one zero can be removed
                                    ms = ms / 10;
                                }
                            }
                            appender.append(String.valueOf(ms));
                        } else {
                            appender.append(strMS);
                        }
                        break;
                    /* TODO: enable when we support "datetime with timezone" datatype
                    case TIMEZONE:
                        if (timezone == 0) {
                            appender.append('Z');
                            break;
                        }
                        if (timezone < 0) {
                            appender.append('-');
                            timezone *= -1;
                        }
                        int timezoneField = (int) (timezone / GregorianCalendarSystem.CHRONON_OF_HOUR);
                        if (timezoneField < 10) {
                            appender.append('0');
                        }
                        appender.append(String.valueOf(timezoneField));
                        timezoneField = (int) (timezone % GregorianCalendarSystem.CHRONON_OF_HOUR
                                / GregorianCalendarSystem.CHRONON_OF_MINUTE);
                        if (timezoneField < 10) {
                            appender.append('0');
                        }
                        appender.append(String.valueOf(timezoneField));
                        break;
                     */
                    case AMPM:
                        if (usePM) {
                            appender.append("PM");
                        } else {
                            appender.append("AM");
                        }
                        break;
                    case SEPARATOR:
                        if (separatorChar == '\0') {
                            throw new HyracksDataException(
                                    "Incorrect separator: separator char is not initialized properly!");
                        }
                        for (int i = 0; i < formatCharCopies; i++) {
                            appender.append(separatorChar);
                        }
                        break;
                    default:
                        throw new HyracksDataException("Unexpected time state when printing a date value");
                }
            } catch (IOException ex) {
                throw HyracksDataException.create(ex);
            }
        }
    }
}
