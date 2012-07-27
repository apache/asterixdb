/*
 * Copyright 2009-2011 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.om.base.temporal;

public class ADateAndTimeParser {

    private static final GregorianCalendarSystem gCalInstance = GregorianCalendarSystem.getInstance();

    private static final String dateErrorMessage = "Wrong date format!";
    private static final String timeErrorMessage = "Wrong time format!";

    /**
     * Parse the given char sequence as a date string, and return the milliseconds represented by the date.
     * 
     * @param charAccessor
     *            accessor for the char sequence
     * @param isDateOnly
     *            indicating whether it is a single date string, or it is the date part of a datetime string
     * @param errorMessage
     * @return
     * @throws Exception
     */
    public static <T> long parseDatePart(ICharSequenceAccessor<T> charAccessor, boolean isDateOnly) throws Exception {

        int length = charAccessor.getLength();
        int offset = 0;

        int year = 0, month = 0, day = 0;
        boolean positive = true;

        boolean isExtendedForm = false;

        if (charAccessor.getCharAt(offset) == '-') {
            offset++;
            positive = false;
        }

        if ((isDateOnly) && charAccessor.getCharAt(offset + 4) == '-' || (!isDateOnly)
                && charAccessor.getCharAt(offset + 13) == ':') {
            isExtendedForm = true;
        }

        if (isExtendedForm) {
            if (charAccessor.getCharAt(offset + 4) != '-' || charAccessor.getCharAt(offset + 7) != '-') {
                throw new Exception(dateErrorMessage);
            }
        }

        // year
        for (int i = 0; i < 4; i++) {
            if (charAccessor.getCharAt(offset + i) >= '0' && charAccessor.getCharAt(offset + i) <= '9') {
                year = year * 10 + charAccessor.getCharAt(offset + i) - '0';
            } else {
                throw new Exception(dateErrorMessage);
            }
        }

        if (year < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.YEAR.ordinal()]
                || year > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.Fields.YEAR.ordinal()]) {
            throw new Exception(dateErrorMessage + ": year " + year);
        }

        offset += (isExtendedForm) ? 5 : 4;

        // month
        for (int i = 0; i < 2; i++) {
            if ((charAccessor.getCharAt(offset + i) >= '0' && charAccessor.getCharAt(offset + i) <= '9')) {
                month = month * 10 + charAccessor.getCharAt(offset + i) - '0';
            } else {
                throw new Exception(dateErrorMessage);
            }
        }

        if (month < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.MONTH.ordinal()]
                || month > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.Fields.MONTH.ordinal()]) {
            throw new Exception(dateErrorMessage + ": month " + month);
        }
        offset += (isExtendedForm) ? 3 : 2;

        // day
        for (int i = 0; i < 2; i++) {
            if ((charAccessor.getCharAt(offset + i) >= '0' && charAccessor.getCharAt(offset + i) <= '9')) {
                day = day * 10 + charAccessor.getCharAt(offset + i) - '0';
            } else {
                throw new Exception(dateErrorMessage);
            }
        }

        if (day < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.DAY.ordinal()]
                || day > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.Fields.DAY.ordinal()]) {
            throw new Exception(dateErrorMessage + ": day " + day);
        }

        offset += 2;

        if (!positive) {
            year *= -1;
        }

        if (isDateOnly && length > offset) {
            throw new Exception(dateErrorMessage);
        }
        return gCalInstance.getChronon(year, month, day, 0, 0, 0, 0, 0);
    }

    /**
     * Parse the given char sequence as a time string, and return the milliseconds represented by the time.
     * 
     * @param charAccessor
     * @return
     * @throws Exception
     */
    public static <T> int parseTimePart(ICharSequenceAccessor<T> charAccessor) throws Exception {

        int length = charAccessor.getLength();
        int offset = 0;

        int hour = 0, min = 0, sec = 0, millis = 0;
        int timezone = 0;

        boolean isExtendedForm = false;
        if (charAccessor.getCharAt(offset + 2) == ':') {
            isExtendedForm = true;
        }

        if (isExtendedForm && (charAccessor.getCharAt(offset + 2) != ':' || charAccessor.getCharAt(offset + 5) != ':')) {
            throw new Exception(timeErrorMessage);
        }
        // hour
        for (int i = 0; i < 2; i++) {
            if ((charAccessor.getCharAt(offset + i) >= '0' && charAccessor.getCharAt(offset + i) <= '9')) {
                hour = hour * 10 + charAccessor.getCharAt(offset + i) - '0';
            } else {
                throw new Exception(timeErrorMessage);
            }
        }

        if (hour < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.HOUR.ordinal()]
                || hour > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.Fields.HOUR.ordinal()]) {
            throw new Exception(timeErrorMessage + ": hour " + hour);
        }

        offset += (isExtendedForm) ? 3 : 2;

        // minute
        for (int i = 0; i < 2; i++) {
            if ((charAccessor.getCharAt(offset + i) >= '0' && charAccessor.getCharAt(offset + i) <= '9')) {
                min = min * 10 + charAccessor.getCharAt(offset + i) - '0';
            } else {
                throw new Exception(timeErrorMessage);
            }
        }

        if (min < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.MINUTE.ordinal()]
                || min > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.Fields.MINUTE.ordinal()]) {
            throw new Exception(timeErrorMessage + ": min " + min);
        }

        offset += (isExtendedForm) ? 3 : 2;

        // second
        for (int i = 0; i < 2; i++) {
            if ((charAccessor.getCharAt(offset + i) >= '0' && charAccessor.getCharAt(offset + i) <= '9')) {
                sec = sec * 10 + charAccessor.getCharAt(offset + i) - '0';
            } else {
                throw new Exception(timeErrorMessage);
            }
        }

        if (sec < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.SECOND.ordinal()]
                || sec > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.Fields.SECOND.ordinal()]) {
            throw new Exception(timeErrorMessage + ": sec " + sec);
        }

        offset += 2;

        if ((isExtendedForm && length > offset && charAccessor.getCharAt(offset) == '.')
                || (!isExtendedForm && length > offset)) {

            offset += (isExtendedForm) ? 1 : 0;
            int i = 0;
            for (; i < 3 && offset + i < length; i++) {
                if (charAccessor.getCharAt(offset + i) >= '0' && charAccessor.getCharAt(offset + i) <= '9') {
                    millis = millis * 10 + charAccessor.getCharAt(offset + i) - '0';
                } else {
                    break;
                }
            }

            offset += i;

            for (; i < 3; i++) {
                millis = millis * 10;
            }

            // error is thrown if more than three digits are seen for the millisecond part
            if (charAccessor.getCharAt(offset) >= '0' && charAccessor.getCharAt(offset) <= '9') {
                throw new Exception("Wrong format of time instance: too many fields for millisecond.");
            }
        }

        if (length > offset) {
            if (charAccessor.getCharAt(offset) != 'Z') {
                if ((charAccessor.getCharAt(offset) != '+' && charAccessor.getCharAt(offset) != '-')
                        || (isExtendedForm && charAccessor.getCharAt(offset + 3) != ':')) {
                    throw new Exception(timeErrorMessage);
                }

                short timezoneHour = 0;
                short timezoneMinute = 0;

                for (int i = 0; i < 2; i++) {
                    if ((charAccessor.getCharAt(offset + 1 + i) >= '0' && charAccessor.getCharAt(offset + 1 + i) <= '9')) {
                        timezoneHour = (short) (timezoneHour * 10 + charAccessor.getCharAt(offset + 1 + i) - '0');
                    } else {
                        throw new Exception(timeErrorMessage);
                    }
                }

                if (timezoneHour < GregorianCalendarSystem.TIMEZONE_HOUR_MIN
                        || timezoneHour > GregorianCalendarSystem.TIMEZONE_HOUR_MAX) {
                    throw new Exception(timeErrorMessage + ": time zone hour " + timezoneHour);
                }

                int temp_offset = (isExtendedForm) ? 1 : 0;

                for (int i = 0; i < 2; i++) {
                    if ((charAccessor.getCharAt(offset + temp_offset + 3 + i) >= '0' && charAccessor.getCharAt(offset
                            + temp_offset + 3 + i) <= '9')) {
                        timezoneMinute = (short) (timezoneMinute * 10
                                + charAccessor.getCharAt(offset + temp_offset + 3 + i) - '0');
                    } else {
                        throw new Exception(timeErrorMessage);
                    }
                }

                if (timezoneMinute < GregorianCalendarSystem.TIMEZONE_MIN_MIN
                        || timezoneMinute > GregorianCalendarSystem.TIMEZONE_MIN_MAX) {
                    throw new Exception(timeErrorMessage + ": time zone minute " + timezoneMinute);
                }

                if (charAccessor.getCharAt(offset) == '-') {
                    timezone = (byte) -((timezoneHour * 4) + timezoneMinute / 15);
                } else {
                    timezone = (byte) ((timezoneHour * 4) + timezoneMinute / 15);
                }
            }
        }

        return gCalInstance.getChronon(hour, min, sec, millis, timezone);
    }
}
