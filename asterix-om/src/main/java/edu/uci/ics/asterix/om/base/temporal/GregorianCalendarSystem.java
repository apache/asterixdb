/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Joda API (http://joda-time.sourceforge.net/) is under the protection of:
 * 
 * Copyright 2001-2005 Stephen Colebourne
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.om.base.temporal;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.om.base.AMutableDateTime;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * A simple implementation of the Gregorian calendar system.
 * <p/>
 */
public class GregorianCalendarSystem implements ICalendarSystem {

    private static final int[] DAYS_OF_MONTH_ORDI = { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    private static final int[] DAYS_OF_MONTH_LEAP = { 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

    private static final int[] DAYS_SINCE_MONTH_BEGIN_ORDI = { 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334 };

    private static final String ERROR_FORMAT = ": unrecognizable datetime format.";

    private static final int CHRONON_OF_SECOND = 1000;
    private static final int CHRONON_OF_MINUTE = 60 * CHRONON_OF_SECOND;
    private static final int CHRONON_OF_HOUR = 60 * CHRONON_OF_MINUTE;
    private static final long CHRONON_OF_DAY = 24 * CHRONON_OF_HOUR;

    /**
     * Minimum feasible value of each field
     */
    private static final int[] FIELD_MINS = { Integer.MIN_VALUE, // year
            1, // month
            1, // day
            0, // hour
            0, // minute
            0, // second
            0 // millisecond
    };

    private static final int[] FIELD_MAXS = { Integer.MAX_VALUE, // year
            12, // month
            31, // day
            23, // hour
            59, // minute
            59, // second
            999 // millisecond
    };

    /**
     * From Joda API: GregorianChronology.java
     */
    private static final long CHRONON_OF_YEAR = (long) (365.2425 * CHRONON_OF_DAY);

    /**
     * From Joda API: GregorianChronology.java
     */
    private static final int DAYS_0000_TO_1970 = 719527;

    private static final GregorianCalendarSystem instance = new GregorianCalendarSystem();

    private GregorianCalendarSystem() {
    }

    public static GregorianCalendarSystem getInstance() {
        return instance;
    }

    /**
     * Check whether the given date time value is a valid date time following the gregorian calendar system.
     * 
     * @param fields
     * @return
     */
    public boolean validate(int year, int month, int day, int hour, int min, int sec, int millis) {
        // Check whether each field is within the value domain
        if (year < FIELD_MINS[0] || year > FIELD_MAXS[0])
            return false;
        if (month < FIELD_MINS[1] || month > FIELD_MAXS[1])
            return false;
        if (day < FIELD_MINS[2] || day > FIELD_MAXS[2])
            return false;
        if (hour < FIELD_MINS[3] || hour > FIELD_MAXS[3])
            return false;
        if (min < FIELD_MINS[4] || min > FIELD_MAXS[4])
            return false;
        if (sec < FIELD_MINS[5] || sec > FIELD_MAXS[5])
            return false;
        if (millis < FIELD_MINS[6] || millis > FIELD_MAXS[6])
            return false;

        // Check whether leap month.
        if (month == 2)
            if (isLeapYear(year)) {
                if (month > DAYS_OF_MONTH_LEAP[1])
                    return false;
            } else {
                if (month > DAYS_OF_MONTH_ORDI[1])
                    return false;
            }
        return true;
    }

    /**
     * Check whether the given time zone value is a valid time zone following the gregorian calendar system.
     * 
     * @param timezone
     * @return
     */
    public boolean validateTimeZone(int timezone) {
        short tzMin = (short) ((timezone % 4) * 15);

        if (tzMin < -60 || tzMin >= 60) {
            return false;
        }

        short tzHr = (short) (timezone / 4);

        if (tzHr < -12 && tzHr > 14) {
            return false;
        }

        return true;
    }

    /**
     * Validate the given chronon time and time zone.
     * 
     * @param year
     * @param month
     * @param day
     * @param hour
     * @param min
     * @param sec
     * @param millis
     * @param timezone
     * @return
     */
    public boolean validate(int year, int month, int day, int hour, int min, int sec, int millis, int timezone) {
        return validate(year, month, day, hour, min, sec, millis) && validateTimeZone(timezone);
    }

    /**
     * Get the chronon time of the given date time and time zone.
     * 
     * @param year
     * @param month
     * @param day
     * @param hour
     * @param min
     * @param sec
     * @param millis
     * @param timezone
     * @return
     */
    public long getChronon(int year, int month, int day, int hour, int min, int sec, int millis, int timezone) {
        // Added milliseconds for all fields but month and day
        long chrononTime = chrononizeBeginningOfYear(year) + (hour - timezone / 4) * CHRONON_OF_HOUR
                + (min - (timezone % 4) * 15) * CHRONON_OF_MINUTE + sec * CHRONON_OF_SECOND + millis;

        // Added milliseconds for days of the month. 
        chrononTime += (day - 1 + DAYS_SINCE_MONTH_BEGIN_ORDI[month - 1]) * CHRONON_OF_DAY;

        // Adjust the leap year
        if (month > 2 && isLeapYear(year)) {
            chrononTime += CHRONON_OF_DAY;
        }

        return chrononTime;
    }

    /**
     * Get the ISO8601 compatible representation of the given chronon time, using the extended form as<br/>
     * [-]YYYY-MM-DDThh:mm:ss.xxx[+|-]hh:mm
     */
    @Override
    public void getStringRep(long chrononTime, StringBuilder sbder) {
        getExtendStringRepWithTimezone(chrononTime, 0, sbder);
    }

    public void getStringRep(long chrononTime, DataOutput out) throws IOException {
        int year = getYear(chrononTime);
        int month = getMonthOfYear(chrononTime, year);

        out.writeUTF(String.format(year < 0 ? "%05d" : "%04d", year) + "-" + String.format("%02d", month) + "-"
                + String.format("%02d", getDayOfMonthYear(chrononTime, year, month)) + "T"
                + String.format("%02d", getHourOfDay(chrononTime)) + ":"
                + String.format("%02d", getMinOfHour(chrononTime)) + ":"
                + String.format("%02d", getSecOfMin(chrononTime)) + "."
                + String.format("%03d", getMillisOfSec(chrononTime)) + "Z");
    }

    public void getExtendStringRepWithTimezone(long chrononTime, int timezone, StringBuilder sbder) {
        int year = getYear(chrononTime);
        int month = getMonthOfYear(chrononTime, year);

        sbder.append(String.format(year < 0 ? "%05d" : "%04d", year)).append("-").append(String.format("%02d", month)).append("-")
                .append(String.format("%02d", getDayOfMonthYear(chrononTime, year, month))).append("T")
                .append(String.format("%02d", getHourOfDay(chrononTime))).append(":")
                .append(String.format("%02d", getMinOfHour(chrononTime))).append(":")
                .append(String.format("%02d", getSecOfMin(chrononTime))).append(".")
                .append(String.format("%03d", getMillisOfSec(chrononTime)));

        if (timezone == 0) {
            sbder.append("Z");
        } else {
            short tzMin = (short) ((timezone % 4) * 15);
            short tzHr = (short) (timezone / 4);
            sbder.append((tzHr >= 0 ? "+" : "-")).append(String.format("%02d", (tzHr < 0 ? -tzHr : tzHr))).append(":")
                    .append(String.format("%02d", tzMin));
        }
    }

    public void getBasicStringRepWithTimezone(long chrononTime, int timezone, StringBuilder sbder) {
        int year = getYear(chrononTime);
        int month = getMonthOfYear(chrononTime, year);

        sbder.append(String.format(year < 0 ? "%05d" : "%04d", year)).append(String.format("%02d", month))
                .append(String.format("%02d", getDayOfMonthYear(chrononTime, year, month))).append("T")
                .append(String.format("%02d", getHourOfDay(chrononTime)))
                .append(String.format("%02d", getMinOfHour(chrononTime)))
                .append(String.format("%02d", getSecOfMin(chrononTime)))
                .append(String.format("%03d", getMillisOfSec(chrononTime)));

        if (timezone == 0) {
            sbder.append("Z");
        } else {
            short tzMin = (short) ((timezone % 4) * 15);
            short tzHr = (short) (timezone / 4);
            sbder.append((tzHr >= 0 ? "+" : "-")).append(String.format("%02d", (tzHr < 0 ? -tzHr : tzHr)))
                    .append(String.format("%02d", tzMin));
        }
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.asterix.om.base.temporal.ICalendarSystem#parseStringRep(java.lang.String, java.io.DataOutput)
     */
    @Override
    public void parseStringRep(String datetime, DataOutput out) throws HyracksDataException {
        int offset = 0;
        int year, month, day, hour, min, sec, millis = 0;
        int timezone = 0;
        boolean positive = true;
        if (datetime.contains(":")) {
            // parse extended form
            if (datetime.charAt(offset) == '-') {
                offset++;
                positive = false;
            }

            if (datetime.charAt(offset + 4) != '-' || datetime.charAt(offset + 7) != '-')
                throw new HyracksDataException(datetime + ERROR_FORMAT);

            year = Short.parseShort(datetime.substring(offset, offset + 4));
            month = Short.parseShort(datetime.substring(offset + 5, offset + 7));
            day = Short.parseShort(datetime.substring(offset + 8, offset + 10));

            if (!positive)
                year *= -1;

            offset += 11;

            if (datetime.charAt(offset + 2) != ':' || datetime.charAt(offset + 5) != ':')
                throw new HyracksDataException(datetime + ERROR_FORMAT);

            hour = Short.parseShort(datetime.substring(offset, offset + 2));
            min = Short.parseShort(datetime.substring(offset + 3, offset + 5));
            sec = Short.parseShort(datetime.substring(offset + 6, offset + 8));

            offset += 8;
            if (datetime.length() > offset && datetime.charAt(offset) == '.') {
                millis = Short.parseShort(datetime.substring(offset + 1, offset + 4));
                offset += 4;
            }

            if (datetime.length() > offset) {
                if (datetime.charAt(offset) != 'Z') {
                    if ((datetime.charAt(offset) != '+' && datetime.charAt(offset) != '-')
                            || (datetime.charAt(offset + 3) != ':'))
                        throw new HyracksDataException(datetime + ERROR_FORMAT);

                    short timezoneHour = Short.parseShort(datetime.substring(offset + 1, offset + 3));
                    short timezoneMinute = Short.parseShort(datetime.substring(offset + 4, offset + 6));

                    if (datetime.charAt(offset) == '-')
                        timezone = (byte) -((timezoneHour * 4) + timezoneMinute / 15);
                    else
                        timezone = (byte) ((timezoneHour * 4) + timezoneMinute / 15);
                }
            }

        } else {
            // parse basic form
            if (datetime.charAt(offset) == '-') {
                offset++;
                positive = false;
            }

            year = Short.parseShort(datetime.substring(offset, offset + 4));
            month = Short.parseShort(datetime.substring(offset + 4, offset + 6));
            day = Short.parseShort(datetime.substring(offset + 6, offset + 8));

            if (!positive)
                day *= -1;

            offset += 9;

            hour = Short.parseShort(datetime.substring(offset, offset + 2));
            min = Short.parseShort(datetime.substring(offset + 2, offset + 4));
            sec = Short.parseShort(datetime.substring(offset + 4, offset + 6));

            offset += 6;
            if (datetime.length() > offset && datetime.charAt(offset) == '.') {
                millis = Short.parseShort(datetime.substring(offset + 1, offset + 4));
                offset += 4;
            }

            if (datetime.length() > offset) {
                if (datetime.charAt(offset) != 'Z') {
                    if ((datetime.charAt(offset) != '+' && datetime.charAt(offset) != '-'))
                        throw new HyracksDataException(datetime + ERROR_FORMAT);

                    short timezoneHour = Short.parseShort(datetime.substring(offset + 1, offset + 3));
                    short timezoneMinute = Short.parseShort(datetime.substring(offset + 3, offset + 5));

                    if (datetime.charAt(offset) == '-')
                        timezone = (byte) -((timezoneHour * 4) + timezoneMinute / 15);
                    else
                        timezone = (byte) ((timezoneHour * 4) + timezoneMinute / 15);
                }
            }
        }

        if (!validate(year, month, day, hour, min, sec, millis, timezone)) {
            throw new HyracksDataException(datetime + ERROR_FORMAT);
        }

        long chrononTime = getChronon(year, month, day, hour, min, sec, (int) millis, timezone);

        try {
            out.writeLong(chrononTime);
            out.writeInt(timezone);
        } catch (IOException e) {
            throw new HyracksDataException(e.toString());
        }
    }

    public void parseStringForADatetime(String datetime, AMutableDateTime aMutableDatetime) throws HyracksDataException {
        int offset = 0;
        int year, month, day, hour, min, sec, millis = 0;
        int timezone = 0;
        boolean positive = true;
        if (datetime.contains(":")) {
            // parse extended form
            if (datetime.charAt(offset) == '-') {
                offset++;
                positive = false;
            }

            if (datetime.charAt(offset + 4) != '-' || datetime.charAt(offset + 7) != '-')
                throw new HyracksDataException(datetime + ERROR_FORMAT);

            year = Short.parseShort(datetime.substring(offset, offset + 4));
            month = Short.parseShort(datetime.substring(offset + 5, offset + 7));
            day = Short.parseShort(datetime.substring(offset + 8, offset + 10));

            if (!positive)
                year *= -1;

            offset += 11;

            if (datetime.charAt(offset + 2) != ':' || datetime.charAt(offset + 5) != ':')
                throw new HyracksDataException(datetime + ERROR_FORMAT);

            hour = Short.parseShort(datetime.substring(offset, offset + 2));
            min = Short.parseShort(datetime.substring(offset + 3, offset + 5));
            sec = Short.parseShort(datetime.substring(offset + 6, offset + 8));

            offset += 8;
            if (datetime.length() > offset && datetime.charAt(offset) == '.') {
                millis = Short.parseShort(datetime.substring(offset + 1, offset + 4));
                offset += 4;
            }

            if (datetime.length() > offset) {
                if (datetime.charAt(offset) != 'Z') {
                    if ((datetime.charAt(offset) != '+' && datetime.charAt(offset) != '-')
                            || (datetime.charAt(offset + 3) != ':'))
                        throw new HyracksDataException(datetime + ERROR_FORMAT);

                    short timezoneHour = Short.parseShort(datetime.substring(offset + 1, offset + 3));
                    short timezoneMinute = Short.parseShort(datetime.substring(offset + 4, offset + 6));

                    if (datetime.charAt(offset) == '-')
                        timezone = (byte) -((timezoneHour * 4) + timezoneMinute / 15);
                    else
                        timezone = (byte) ((timezoneHour * 4) + timezoneMinute / 15);
                }
            }

        } else {
            // parse basic form
            if (datetime.charAt(offset) == '-') {
                offset++;
                positive = false;
            }

            year = Short.parseShort(datetime.substring(offset, offset + 4));
            month = Short.parseShort(datetime.substring(offset + 4, offset + 6));
            day = Short.parseShort(datetime.substring(offset + 6, offset + 8));

            if (!positive)
                day *= -1;

            offset += 9;

            hour = Short.parseShort(datetime.substring(offset, offset + 2));
            min = Short.parseShort(datetime.substring(offset + 2, offset + 4));
            sec = Short.parseShort(datetime.substring(offset + 4, offset + 6));

            offset += 6;
            if (datetime.length() > offset && datetime.charAt(offset) == '.') {
                millis = Short.parseShort(datetime.substring(offset + 1, offset + 4));
                offset += 4;
            }

            if (datetime.length() > offset) {
                if (datetime.charAt(offset) != 'Z') {
                    if ((datetime.charAt(offset) != '+' && datetime.charAt(offset) != '-'))
                        throw new HyracksDataException(datetime + ERROR_FORMAT);

                    short timezoneHour = Short.parseShort(datetime.substring(offset + 1, offset + 3));
                    short timezoneMinute = Short.parseShort(datetime.substring(offset + 3, offset + 5));

                    if (datetime.charAt(offset) == '-')
                        timezone = (byte) -((timezoneHour * 4) + timezoneMinute / 15);
                    else
                        timezone = (byte) ((timezoneHour * 4) + timezoneMinute / 15);
                }
            }
        }

        if (!validate(year, month, day, hour, min, sec, millis, timezone)) {
            throw new HyracksDataException(datetime + ERROR_FORMAT);
        }

        aMutableDatetime.setValue(getChronon(year, month, day, hour, min, sec, (int) millis, timezone));
    }

    public void parseStringForADatetime(byte[] datetime, int offset, AMutableDateTime aMutableDatetime)
            throws AlgebricksException {
        int year, month, day, hour, min, sec, millis = 0;
        int timezone = 0;
        boolean positive = true;
        boolean isExtendedForm = false;
        if (datetime[offset + 13] == ':' || datetime[offset + 14] == ':') {
            isExtendedForm = true;
        }
        if (isExtendedForm) {
            // parse extended form
            if (datetime[offset] == '-') {
                offset++;
                positive = false;
            }

            if (datetime[offset + 4] != '-' || datetime[offset + 7] != '-')
                throw new AlgebricksException(datetime + ERROR_FORMAT);

            year = getValue(datetime, offset, 4);
            month = getValue(datetime, offset + 5, 2);
            day = getValue(datetime, offset + 8, 2);

            if (!positive)
                year *= -1;

            offset += 11;

            if (datetime[offset + 2] != ':' || datetime[offset + 5] != ':')
                throw new AlgebricksException(datetime + ERROR_FORMAT);

            hour = getValue(datetime, offset, 2);
            min = getValue(datetime, offset + 3, 2);
            sec = getValue(datetime, offset + 6, 2);

            offset += 8;
            if (datetime.length > offset && datetime[offset] == '.') {
                millis = getValue(datetime, offset + 1, 3);
                offset += 4;
            }

            if (datetime.length > offset) {
                if (datetime[offset] != 'Z') {
                    if ((datetime[offset] != '+' && datetime[offset] != '-') || (datetime[offset + 3] != ':'))
                        throw new AlgebricksException(datetime + ERROR_FORMAT);

                    short timezoneHour = getValue(datetime, offset + 1, 2);
                    short timezoneMinute = getValue(datetime, offset + 4, 2);

                    if (datetime[offset] == '-')
                        timezone = (byte) -((timezoneHour * 4) + timezoneMinute / 15);
                    else
                        timezone = (byte) ((timezoneHour * 4) + timezoneMinute / 15);
                }
            }

        } else {
            // parse basic form
            if (datetime[offset] == '-') {
                offset++;
                positive = false;
            }

            year = getValue(datetime, offset, 4);
            month = getValue(datetime, offset + 4, 2);
            day = getValue(datetime, offset + 6, 2);

            if (!positive)
                day *= -1;

            offset += 9;

            hour = getValue(datetime, offset, 2);
            min = getValue(datetime, offset + 2, 2);
            sec = getValue(datetime, offset + 4, 2);

            offset += 6;
            if (datetime.length > offset && datetime[offset] == '.') {
                millis = getValue(datetime, offset + 1, 3);
                offset += 4;
            }

            if (datetime.length > offset) {
                if (datetime[offset] != 'Z') {
                    if ((datetime[offset] != '+' && datetime[offset] != '-'))
                        throw new AlgebricksException(datetime + ERROR_FORMAT);

                    short timezoneHour = getValue(datetime, offset + 1, 2);
                    short timezoneMinute = getValue(datetime, offset + 3, 2);

                    if (datetime[offset] == '-')
                        timezone = (byte) -((timezoneHour * 4) + timezoneMinute / 15);
                    else
                        timezone = (byte) ((timezoneHour * 4) + timezoneMinute / 15);
                }
            }
        }

        if (!validate(year, month, day, hour, min, sec, millis, timezone)) {
            throw new AlgebricksException(datetime + ERROR_FORMAT);
        }

        aMutableDatetime.setValue(getChronon(year, month, day, hour, min, sec, (int) millis, timezone));
    }

    private short getValue(byte[] b, int offset, int numberOfDigits) throws AlgebricksException {
        short value = 0;
        for (int i = 0; i < numberOfDigits; i++) {
            if ((b[offset] >= '0' && b[offset] <= '9'))
                value = (short) (value * 10 + b[offset++] - '0');
            else
                throw new AlgebricksException(ERROR_FORMAT);

        }
        return value;
    }

    /**
     * Check whether a given year is a leap year.
     * 
     * @param year
     * @return
     */
    protected boolean isLeapYear(int year) {
        return ((year & 3) == 0) && ((year % 100) != 0 || (year % 400) == 0);
    }

    /**
     * From Joda library GregorianChronology class: The basic calculation is
     * (y / 4) - (y / 100) + (y / 400). <br/>
     * Use y >> 2 ( (y + 3) >> 2 for negative y value) to replace y / 4 reveals eliminates two divisions.
     * 
     * @param year
     * @return
     */
    private long chrononizeBeginningOfYear(int year) {
        int leapYears = year / 100;
        if (year < 0) {
            // From Joda library GregorianChronology class
            // The basic calculation is (y / 4) - (y / 100) + (y / 400)
            // Use (y + 3) >> 2 to replace y / 4 reveals eliminates two divisions.
            leapYears = ((year + 3) >> 2) - leapYears + ((leapYears + 3) >> 2) - 1;
        } else {
            leapYears = (year >> 2) - leapYears + (leapYears >> 2);
            if (isLeapYear(year)) {
                leapYears--;
            }
        }
        return (year * 365L + (leapYears - DAYS_0000_TO_1970)) * CHRONON_OF_DAY;
    }

    /**
     * Get the year for the given chronon time.
     * <p/>
     * This code is directly from the Joda library BadicChronology.java.<br/>
     * The original authers are Stephen Colebourne, Brain S O'Neill and Guy Allard, and modified by JArod Wen on May 7th, 2012.
     * 
     * @param chrononTime
     * @return
     */
    public int getYear(long chrononTime) {
        // Get an initial estimate of the year, and the millis value that
        // represents the start of that year. Then verify estimate and fix if
        // necessary.

        // Initial estimate uses values divided by two to avoid overflow.
        long unitMillis = CHRONON_OF_YEAR / 2;
        long i2 = (chrononTime >> 1) + (1970L * CHRONON_OF_YEAR) / 2;
        if (i2 < 0) {
            i2 = i2 - unitMillis + 1;
        }
        int year = (int) (i2 / unitMillis);

        long yearStart = chrononizeBeginningOfYear(year);
        long diff = chrononTime - yearStart;

        if (diff < 0) {
            year--;
        } else if (diff >= CHRONON_OF_DAY * 365L) {
            // One year may need to be added to fix estimate.
            long oneYear;
            if (isLeapYear(year)) {
                oneYear = CHRONON_OF_DAY * 366L;
            } else {
                oneYear = CHRONON_OF_DAY * 365L;
            }

            yearStart += oneYear;

            if (yearStart <= chrononTime) {
                // Didn't go too far, so actually add one year.
                year++;
            }
        }

        return year;
    }

    /**
     * Get the month of the year for the given chronon time and the year.
     * <p/>
     * This code is directly from the Joda library BasicGJChronology.java.<br/>
     * The original authers are Stephen Colebourne, Brain S O'Neill and Guy Allard, and modified by JArod Wen on May 7th, 2012.
     * <p/>
     * 
     * @param millis
     * @param year
     * @return
     */
    public int getMonthOfYear(long millis, int year) {
        // Perform a binary search to get the month. To make it go even faster,
        // compare using ints instead of longs. The number of milliseconds per
        // year exceeds the limit of a 32-bit int's capacity, so divide by
        // 1024. No precision is lost (except time of day) since the number of
        // milliseconds per day contains 1024 as a factor. After the division,
        // the instant isn't measured in milliseconds, but in units of
        // (128/125)seconds.

        int i = (int) ((millis - chrononizeBeginningOfYear(year)) >> 10);

        // There are 86400000 milliseconds per day, but divided by 1024 is
        // 84375. There are 84375 (128/125)seconds per day.

        return (isLeapYear(year)) ? ((i < 182 * 84375) ? ((i < 91 * 84375) ? ((i < 31 * 84375) ? 1
                : (i < 60 * 84375) ? 2 : 3) : ((i < 121 * 84375) ? 4 : (i < 152 * 84375) ? 5 : 6))
                : ((i < 274 * 84375) ? ((i < 213 * 84375) ? 7 : (i < 244 * 84375) ? 8 : 9) : ((i < 305 * 84375) ? 10
                        : (i < 335 * 84375) ? 11 : 12)))
                : ((i < 181 * 84375) ? ((i < 90 * 84375) ? ((i < 31 * 84375) ? 1 : (i < 59 * 84375) ? 2 : 3)
                        : ((i < 120 * 84375) ? 4 : (i < 151 * 84375) ? 5 : 6))
                        : ((i < 273 * 84375) ? ((i < 212 * 84375) ? 7 : (i < 243 * 84375) ? 8 : 9)
                                : ((i < 304 * 84375) ? 10 : (i < 334 * 84375) ? 11 : 12)));
    }

    /**
     * Get the day of the given month and year for the input chronon time.
     * <p/>
     * This function is directly from Joda Library BasicChronology.java.<br/>
     * The original authers are Stephen Colebourne, Brain S O'Neill and Guy Allard, and modified by JArod Wen on May 7th, 2012.
     * <p/>
     * 
     * @param millis
     * @param year
     * @param month
     * @return
     */
    public int getDayOfMonthYear(long millis, int year, int month) {
        long dateMillis = chrononizeBeginningOfYear(year);
        dateMillis += DAYS_SINCE_MONTH_BEGIN_ORDI[month - 1] * CHRONON_OF_DAY;
        if (isLeapYear(year) && month > 2) {
            dateMillis += CHRONON_OF_DAY;
        }
        return (int) ((millis - dateMillis) / CHRONON_OF_DAY) + 1;
    }

    public int getHourOfDay(long millis) {
        int hour = (int) ((millis % CHRONON_OF_DAY) / CHRONON_OF_HOUR);

        if (millis < 0) {
            if (millis % CHRONON_OF_HOUR == 0) {
                if (hour < 0) {
                    hour += 24;
                }
            } else {
                hour += 23;
            }
        }
        return hour;
    }

    public int getMinOfHour(long millis) {
        int min = (int) ((millis % CHRONON_OF_HOUR) / CHRONON_OF_MINUTE);
        if (millis < 0) {
            if (millis % CHRONON_OF_MINUTE == 0) {
                if (min < 0) {
                    min += 60;
                }
            } else {
                min += 59;
            }
        }
        return min;
    }

    public int getSecOfMin(long millis) {
        int sec = (int) ((millis % CHRONON_OF_MINUTE) / CHRONON_OF_SECOND);
        if (millis < 0) {
            if (millis % CHRONON_OF_SECOND == 0) {
                if (sec < 0) {
                    sec += 60;
                }
            } else {
                sec += 59;
            }
        }
        return sec;
    }

    public int getMillisOfSec(long millis) {
        int ms = (int) (millis % CHRONON_OF_SECOND);
        if (millis < 0 && ms < 0) {
            ms += 1000;
        }
        return ms;
    }
}
