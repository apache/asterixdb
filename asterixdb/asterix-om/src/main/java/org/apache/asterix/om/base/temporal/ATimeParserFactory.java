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

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParser;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;

public class ATimeParserFactory implements IValueParserFactory {

    public static final IValueParserFactory INSTANCE = new ATimeParserFactory();

    private static final long serialVersionUID = 1L;

    private static final String timeErrorMessage = "Wrong Input Format for a Time Value";

    private ATimeParserFactory() {

    }

    @Override
    public IValueParser createValueParser() {

        return new IValueParser() {

            @Override
            public void parse(char[] buffer, int start, int length, DataOutput out) throws HyracksDataException {
                try {
                    out.writeInt(parseTimePart(buffer, start, length));
                } catch (IOException ex) {
                    throw HyracksDataException.create(ex);
                }
            }
        };
    }

    /**
     * Parse the given string as a time string, and return the milliseconds represented by the time.
     *
     * @param timeString
     * @param start
     * @param length
     * @return
     * @throws HyracksDataException
     */
    public static int parseTimePart(String timeString, int start, int length) throws HyracksDataException {
        int offset = 0;

        int hour = 0, min = 0, sec = 0, millis = 0;
        int timezone = 0;

        boolean isExtendedForm = false;
        if (timeString.charAt(start + offset + 2) == ':') {
            isExtendedForm = true;
        }

        if (isExtendedForm
                && (timeString.charAt(start + offset + 2) != ':' || timeString.charAt(start + offset + 5) != ':')) {
            throw new HyracksDataException(timeErrorMessage + ": Missing colon in an extended time format.");
        }
        // hour
        for (int i = 0; i < 2; i++) {
            if ((timeString.charAt(start + offset + i) >= '0' && timeString.charAt(start + offset + i) <= '9')) {
                hour = hour * 10 + timeString.charAt(start + offset + i) - '0';
            } else {
                throw new HyracksDataException(timeErrorMessage + ": Non-numeric value in hour field");
            }
        }

        if (hour < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.HOUR.ordinal()]
                || hour > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.Fields.HOUR.ordinal()]) {
            throw new HyracksDataException(timeErrorMessage + ": hour " + hour);
        }

        offset += (isExtendedForm) ? 3 : 2;

        // minute
        for (int i = 0; i < 2; i++) {
            if ((timeString.charAt(start + offset + i) >= '0' && timeString.charAt(start + offset + i) <= '9')) {
                min = min * 10 + timeString.charAt(start + offset + i) - '0';
            } else {
                throw new HyracksDataException(timeErrorMessage + ": Non-numeric value in minute field");
            }
        }

        if (min < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.MINUTE.ordinal()]
                || min > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.Fields.MINUTE.ordinal()]) {
            throw new HyracksDataException(timeErrorMessage + ": min " + min);
        }

        offset += (isExtendedForm) ? 3 : 2;

        // second
        for (int i = 0; i < 2; i++) {
            if ((timeString.charAt(start + offset + i) >= '0' && timeString.charAt(start + offset + i) <= '9')) {
                sec = sec * 10 + timeString.charAt(start + offset + i) - '0';
            } else {
                throw new HyracksDataException(timeErrorMessage + ": Non-numeric value in second field");
            }
        }

        if (sec < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.SECOND.ordinal()]
                || sec > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.Fields.SECOND.ordinal()]) {
            throw new HyracksDataException(timeErrorMessage + ": sec " + sec);
        }

        offset += 2;

        if ((isExtendedForm && length > offset && timeString.charAt(start + offset) == '.')
                || (!isExtendedForm && length > offset)) {

            offset += (isExtendedForm) ? 1 : 0;
            int i = 0;
            for (; i < 3 && offset + i < length; i++) {
                if (timeString.charAt(start + offset + i) >= '0' && timeString.charAt(start + offset + i) <= '9') {
                    millis = millis * 10 + timeString.charAt(start + offset + i) - '0';
                } else {
                    break;
                }
            }

            offset += i;

            for (; i < 3; i++) {
                millis = millis * 10;
            }

            // error is thrown if more than three digits are seen for the millisecond part
            if (length > offset && timeString.charAt(start + offset) >= '0'
                    && timeString.charAt(start + offset) <= '9') {
                throw new HyracksDataException(timeErrorMessage + ": too many fields for millisecond.");
            }
        }

        if (length > offset) {
            timezone = parseTimezonePart(timeString, start + offset);
        }

        return GregorianCalendarSystem.getInstance().getChronon(hour, min, sec, millis, timezone);
    }

    /**
     * Parse the given string as a time string, and parse the timezone field.
     *
     * @param timeString
     * @param start
     * @return
     * @throws HyracksDataException
     */
    public static int parseTimezonePart(String timeString, int start) throws HyracksDataException {
        int timezone = 0;

        if (timeString.charAt(start) != 'Z') {
            if ((timeString.charAt(start) != '+' && timeString.charAt(start) != '-')) {
                throw new HyracksDataException("Wrong timezone format: missing sign or missing colon for a time zone");
            }

            short timezoneHour = 0;
            short timezoneMinute = 0;

            for (int i = 0; i < 2; i++) {
                if ((timeString.charAt(start + 1 + i) >= '0' && timeString.charAt(start + 1 + i) <= '9')) {
                    timezoneHour = (short) (timezoneHour * 10 + timeString.charAt(start + 1 + i) - '0');
                } else {
                    throw new HyracksDataException(timeErrorMessage + ": Non-numeric value in timezone hour field");
                }
            }

            int temp_offset = (timeString.charAt(start + 3) == ':') ? 1 : 0;

            for (int i = 0; i < 2; i++) {
                if ((timeString.charAt(start + temp_offset + 3 + i) >= '0'
                        && timeString.charAt(start + temp_offset + 3 + i) <= '9')) {
                    timezoneMinute =
                            (short) (timezoneMinute * 10 + timeString.charAt(start + temp_offset + 3 + i) - '0');
                } else {
                    throw new HyracksDataException(timeErrorMessage + ": Non-numeric value in timezone minute field");
                }
            }

            timezone = (int) (timezoneHour * GregorianCalendarSystem.CHRONON_OF_HOUR
                    + timezoneMinute * GregorianCalendarSystem.CHRONON_OF_MINUTE);

            if (timeString.charAt(start) == '+') {
                timezone *= -1;
            }
        }
        return timezone;
    }

    /**
     * Similar to {@link #parseTimePart(String, int, int)} but use a char array as input; although this is almost
     * a copy-and-past code but it avoids object creation.
     *
     * @param timeString
     * @param start
     * @param length
     * @return
     * @throws HyracksDataException
     */
    public static int parseTimePart(char[] timeString, int start, int length) throws HyracksDataException {

        int offset = 0;

        int hour = 0, min = 0, sec = 0, millis = 0;
        int timezone = 0;

        boolean isExtendedForm = false;
        if (timeString[start + offset + 2] == ':') {
            isExtendedForm = true;
        }

        if (isExtendedForm && (timeString[start + offset + 2] != ':' || timeString[start + offset + 5] != ':')) {
            throw new HyracksDataException(timeErrorMessage + ": Missing colon in an extended time format.");
        }
        // hour
        for (int i = 0; i < 2; i++) {
            if ((timeString[start + offset + i] >= '0' && timeString[start + offset + i] <= '9')) {
                hour = hour * 10 + timeString[start + offset + i] - '0';
            } else {
                throw new HyracksDataException(timeErrorMessage + ": Non-numeric value in hour field");
            }
        }

        if (hour < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.HOUR.ordinal()]
                || hour > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.Fields.HOUR.ordinal()]) {
            throw new HyracksDataException(timeErrorMessage + ": hour " + hour);
        }

        offset += (isExtendedForm) ? 3 : 2;

        // minute
        for (int i = 0; i < 2; i++) {
            if ((timeString[start + offset + i] >= '0' && timeString[start + offset + i] <= '9')) {
                min = min * 10 + timeString[start + offset + i] - '0';
            } else {
                throw new HyracksDataException(timeErrorMessage + ": Non-numeric value in minute field");
            }
        }

        if (min < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.MINUTE.ordinal()]
                || min > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.Fields.MINUTE.ordinal()]) {
            throw new HyracksDataException(timeErrorMessage + ": min " + min);
        }

        offset += (isExtendedForm) ? 3 : 2;

        // second
        for (int i = 0; i < 2; i++) {
            if ((timeString[start + offset + i] >= '0' && timeString[start + offset + i] <= '9')) {
                sec = sec * 10 + timeString[start + offset + i] - '0';
            } else {
                throw new HyracksDataException(timeErrorMessage + ": Non-numeric value in second field");
            }
        }

        if (sec < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.SECOND.ordinal()]
                || sec > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.Fields.SECOND.ordinal()]) {
            throw new HyracksDataException(timeErrorMessage + ": sec " + sec);
        }

        offset += 2;

        if ((isExtendedForm && length > offset && timeString[start + offset] == '.')
                || (!isExtendedForm && length > offset)) {

            offset += (isExtendedForm) ? 1 : 0;
            int i = 0;
            for (; i < 3 && offset + i < length; i++) {
                if (timeString[start + offset + i] >= '0' && timeString[start + offset + i] <= '9') {
                    millis = millis * 10 + timeString[start + offset + i] - '0';
                } else {
                    break;
                }
            }

            offset += i;

            for (; i < 3; i++) {
                millis = millis * 10;
            }

            // error is thrown if more than three digits are seen for the millisecond part
            if (length > offset && timeString[start + offset] >= '0' && timeString[start + offset] <= '9') {
                throw new HyracksDataException(timeErrorMessage + ": too many fields for millisecond.");
            }
        }

        if (length > offset) {
            timezone = parseTimezonePart(timeString, start + offset);
        }

        return GregorianCalendarSystem.getInstance().getChronon(hour, min, sec, millis, timezone);
    }

    /**
     * Similar to {@link #parseTimezonePart(String, int)} but use a char array as input; although this is almost
     * a copy-and-past code but it avoids object creation.
     *
     * @param timeString
     * @param start
     * @param length
     * @return
     * @throws HyracksDataException
     */
    public static int parseTimezonePart(char[] timeString, int start) throws HyracksDataException {
        int timezone = 0;

        if (timeString[start] != 'Z') {
            if ((timeString[start] != '+' && timeString[start] != '-')) {
                throw new HyracksDataException("Wrong timezone format: missing sign or missing colon for a time zone");
            }

            short timezoneHour = 0;
            short timezoneMinute = 0;

            for (int i = 0; i < 2; i++) {
                if ((timeString[start + 1 + i] >= '0' && timeString[start + 1 + i] <= '9')) {
                    timezoneHour = (short) (timezoneHour * 10 + timeString[start + 1 + i] - '0');
                } else {
                    throw new HyracksDataException(timeErrorMessage + ": Non-numeric value in timezone hour field");
                }
            }

            int temp_offset = (timeString[start + 3] == ':') ? 1 : 0;

            for (int i = 0; i < 2; i++) {
                if ((timeString[start + temp_offset + 3 + i] >= '0'
                        && timeString[start + temp_offset + 3 + i] <= '9')) {
                    timezoneMinute = (short) (timezoneMinute * 10 + timeString[start + temp_offset + 3 + i] - '0');
                } else {
                    throw new HyracksDataException(timeErrorMessage + ": Non-numeric value in timezone minute field");
                }
            }

            timezone = (int) (timezoneHour * GregorianCalendarSystem.CHRONON_OF_HOUR
                    + timezoneMinute * GregorianCalendarSystem.CHRONON_OF_MINUTE);

            if (timeString[start] == '+') {
                timezone *= -1;
            }
        }
        return timezone;
    }

    /**
     * Similar to {@link #parseTimePart(String, int, int)} but use a byte array as input; although this is almost
     * a copy-and-past code but it avoids object creation.
     *
     * @param timeString
     * @param start
     * @param length
     * @return
     * @throws HyracksDataException
     */
    public static int parseTimePart(byte[] timeString, int start, int length) throws HyracksDataException {

        int offset = 0;

        int hour = 0, min = 0, sec = 0, millis = 0;
        int timezone = 0;

        boolean isExtendedForm = false;
        if (timeString[start + offset + 2] == ':') {
            isExtendedForm = true;
        }

        if (isExtendedForm && (timeString[start + offset + 2] != ':' || timeString[start + offset + 5] != ':')) {
            throw new HyracksDataException(timeErrorMessage + ": Missing colon in an extended time format.");
        }
        // hour
        for (int i = 0; i < 2; i++) {
            if ((timeString[start + offset + i] >= '0' && timeString[start + offset + i] <= '9')) {
                hour = hour * 10 + timeString[start + offset + i] - '0';
            } else {
                throw new HyracksDataException(timeErrorMessage + ": Non-numeric value in hour field");
            }
        }

        if (hour < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.HOUR.ordinal()]
                || hour > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.Fields.HOUR.ordinal()]) {
            throw new HyracksDataException(timeErrorMessage + ": hour " + hour);
        }

        offset += (isExtendedForm) ? 3 : 2;

        // minute
        for (int i = 0; i < 2; i++) {
            if ((timeString[start + offset + i] >= '0' && timeString[start + offset + i] <= '9')) {
                min = min * 10 + timeString[start + offset + i] - '0';
            } else {
                throw new HyracksDataException(timeErrorMessage + ": Non-numeric value in minute field");
            }
        }

        if (min < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.MINUTE.ordinal()]
                || min > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.Fields.MINUTE.ordinal()]) {
            throw new HyracksDataException(timeErrorMessage + ": min " + min);
        }

        offset += (isExtendedForm) ? 3 : 2;

        // second
        for (int i = 0; i < 2; i++) {
            if ((timeString[start + offset + i] >= '0' && timeString[start + offset + i] <= '9')) {
                sec = sec * 10 + timeString[start + offset + i] - '0';
            } else {
                throw new HyracksDataException(timeErrorMessage + ": Non-numeric value in second field");
            }
        }

        if (sec < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.SECOND.ordinal()]
                || sec > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.Fields.SECOND.ordinal()]) {
            throw new HyracksDataException(timeErrorMessage + ": sec " + sec);
        }

        offset += 2;

        if ((isExtendedForm && length > offset && timeString[start + offset] == '.')
                || (!isExtendedForm && length > offset)) {

            offset += (isExtendedForm) ? 1 : 0;
            int i = 0;
            for (; i < 3 && offset + i < length; i++) {
                if (timeString[start + offset + i] >= '0' && timeString[start + offset + i] <= '9') {
                    millis = millis * 10 + timeString[start + offset + i] - '0';
                } else {
                    break;
                }
            }

            offset += i;

            for (; i < 3; i++) {
                millis = millis * 10;
            }

            // error is thrown if more than three digits are seen for the millisecond part
            if (length > offset && timeString[start + offset] >= '0' && timeString[start + offset] <= '9') {
                throw new HyracksDataException(timeErrorMessage + ": too many fields for millisecond.");
            }
        }

        if (length > offset) {
            timezone = parseTimezonePart(timeString, start + offset);
        }

        return GregorianCalendarSystem.getInstance().getChronon(hour, min, sec, millis, timezone);
    }

    /**
     * Similar to {@link #parseTimezonePart(String, int)} but use a byte array as input; although this is almost
     * a copy-and-past code but it avoids object creation.
     *
     * @param timeString
     * @param start
     * @param length
     * @return
     * @throws HyracksDataException
     */
    public static int parseTimezonePart(byte[] timeString, int start) throws HyracksDataException {
        int timezone = 0;

        if (timeString[start] != 'Z') {
            if ((timeString[start] != '+' && timeString[start] != '-')) {
                throw new HyracksDataException("Wrong timezone format: missing sign or missing colon for a time zone");
            }

            short timezoneHour = 0;
            short timezoneMinute = 0;

            for (int i = 0; i < 2; i++) {
                if ((timeString[start + 1 + i] >= '0' && timeString[start + 1 + i] <= '9')) {
                    timezoneHour = (short) (timezoneHour * 10 + timeString[start + 1 + i] - '0');
                } else {
                    throw new HyracksDataException(timeErrorMessage + ": Non-numeric value in timezone hour field");
                }
            }

            int temp_offset = (timeString[start + 3] == ':') ? 1 : 0;

            for (int i = 0; i < 2; i++) {
                if ((timeString[start + temp_offset + 3 + i] >= '0'
                        && timeString[start + temp_offset + 3 + i] <= '9')) {
                    timezoneMinute = (short) (timezoneMinute * 10 + timeString[start + temp_offset + 3 + i] - '0');
                } else {
                    throw new HyracksDataException(timeErrorMessage + ": Non-numeric value in timezone minute field");
                }
            }

            timezone = (int) (timezoneHour * GregorianCalendarSystem.CHRONON_OF_HOUR
                    + timezoneMinute * GregorianCalendarSystem.CHRONON_OF_MINUTE);

            if (timeString[start] == '+') {
                timezone *= -1;
            }
        }
        return timezone;
    }

}
