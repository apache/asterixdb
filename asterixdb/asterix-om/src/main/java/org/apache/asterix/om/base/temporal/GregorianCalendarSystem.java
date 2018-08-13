
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
 *
 */

/*
 *Portions of this code are based off of Joda API
 * (http://joda-time.sourceforge.net/)
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
package org.apache.asterix.om.base.temporal;

import java.io.IOException;

/**
 * A simple implementation of the Gregorian calendar system.
 * <p/>
 */
public class GregorianCalendarSystem implements ICalendarSystem {

    public enum Fields {
        YEAR,
        MONTH,
        DAY,
        HOUR,
        MINUTE,
        SECOND,
        MILLISECOND
    }

    //public static final int YEAR = 0, MONTH = 1, DAY = 2, HOUR = 3, MINUTE = 4, SECOND = 5, MILLISECOND = 6;

    public static final int[] DAYS_OF_MONTH_ORDI = { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    public static final int[] DAYS_OF_MONTH_LEAP = { 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

    public static final int[] DAYS_SINCE_MONTH_BEGIN_ORDI = { 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334 };

    public static final int MONTHS_IN_A_YEAR = 12;
    public static final int DAYS_IN_A_WEEK = 7;

    public static final long CHRONON_OF_SECOND = 1000;
    public static final long CHRONON_OF_MINUTE = 60 * CHRONON_OF_SECOND;
    public static final long CHRONON_OF_HOUR = 60 * CHRONON_OF_MINUTE;
    public static final long CHRONON_OF_DAY = 24 * CHRONON_OF_HOUR;
    public static final long CHRONON_OF_WEEK = DAYS_IN_A_WEEK * CHRONON_OF_DAY;

    /**
     * Minimum feasible value of each field
     */
    public static final int[] FIELD_MINS = { Integer.MIN_VALUE, // year
            1, // month
            1, // day
            0, // hour
            0, // minute
            0, // second
            0 // millisecond
    };

    public static final int[] FIELD_MAXS = { Integer.MAX_VALUE, // year
            12, // month
            31, // day
            23, // hour
            59, // minute
            59, // second
            999 // millisecond
    };

    public static final int TIMEZONE_HOUR_MIN = -12, TIMEZONE_HOUR_MAX = 14, TIMEZONE_MIN_MIN = -60,
            TIMEZONE_MIN_MAX = 60;

    /**
     * From Joda API: GregorianChronology.java
     */
    private static final long CHRONON_OF_YEAR = (long) (365.2425 * CHRONON_OF_DAY);

    /**
     * From Joda API: GregorianChronology.java
     */
    private static final int DAYS_0000_TO_1970 = 719527;

    // Fixed week day anchor: Thursday, 1 January 1970
    private final static int ANCHOR_WEEKDAY = 4;

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
        if (year < FIELD_MINS[0] || year > FIELD_MAXS[0]) {
            return false;
        }
        if (month < FIELD_MINS[1] || month > FIELD_MAXS[1]) {
            return false;
        }
        if (day < FIELD_MINS[2] || day > FIELD_MAXS[2]) {
            return false;
        }
        if (hour < FIELD_MINS[3] || hour > FIELD_MAXS[3]) {
            return false;
        }
        if (min < FIELD_MINS[4] || min > FIELD_MAXS[4]) {
            return false;
        }
        if (sec < FIELD_MINS[5] || sec > FIELD_MAXS[5]) {
            return false;
        }
        if (millis < FIELD_MINS[6] || millis > FIELD_MAXS[6]) {
            return false;
        }

        // Check whether leap month.
        if (month == 2) {
            if (isLeapYear(year)) {
                if (day > DAYS_OF_MONTH_LEAP[1]) {
                    return false;
                }
            } else {
                if (day > DAYS_OF_MONTH_ORDI[1]) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Check whether the given time zone value is a valid time zone following the gregorian calendar system.
     * <p/>
     * The valid timezone is within the range of:<br/>
     * - Hours: UTC -12 (Y, or Yankee Time Zone) to UTC +14 (LINT, or Line Islands Time) <br/>
     * - Minutes: currently the available minutes include 00, 30 and 45.
     * <p/>
     * Reference: http://www.timeanddate.com/library/abbreviations/timezones/
     * <p/>
     *
     * @param timezone
     * @return
     */
    public boolean validateTimeZone(int timezone) {

        if (timezone < -12 * CHRONON_OF_DAY || timezone > 14 * CHRONON_OF_DAY) {
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
     * Get the UTC chronon time of the given date time and time zone.
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
        long chrononTime = chrononizeBeginningOfYear(year) + hour * CHRONON_OF_HOUR + min * CHRONON_OF_MINUTE
                + sec * CHRONON_OF_SECOND + millis + timezone;

        // Added milliseconds for days of the month.
        chrononTime += (day - 1 + DAYS_SINCE_MONTH_BEGIN_ORDI[month - 1]) * CHRONON_OF_DAY;

        // Adjust the leap year
        if (month > 2 && isLeapYear(year)) {
            chrononTime += CHRONON_OF_DAY;
        }

        return chrononTime;
    }

    /**
     * Get the chronon time (number of milliseconds) of the given time and time zone.
     *
     * @param hour
     * @param min
     * @param sec
     * @param millis
     * @param timezone
     * @return
     */
    public int getChronon(int hour, int min, int sec, int millis, int timezone) {
        // Added milliseconds for all fields but month and day
        long chrononTime =
                hour * CHRONON_OF_HOUR + min * CHRONON_OF_MINUTE + sec * CHRONON_OF_SECOND + millis + timezone;
        return (int) chrononTime;
    }

    public long adjustChrononByTimezone(long chronon, int timezone) {
        return chronon - timezone;
    }

    public int getChrononInDays(long chronon) {
        if (chronon >= 0) {
            return (int) (chronon / CHRONON_OF_DAY);
        } else {
            if (chronon % CHRONON_OF_DAY != 0) {
                return (int) (chronon / CHRONON_OF_DAY - 1);
            } else {
                return (int) (chronon / CHRONON_OF_DAY);
            }
        }
    }

    /**
     * Get the extended string representation of the given UTC chronon time under the given time zone. Only fields
     * before
     * the given field index will be returned.
     * <p/>
     * The extended string representation is like:<br/>
     * [-]YYYY-MM-DDThh:mm:ss.xxx[Z|[+|-]hh:mm]
     *
     * @param chrononTime
     * @param timezone
     * @param sbder
     * @param untilField
     */
    public void getExtendStringRepUntilField(long chrononTime, int timezone, Appendable sbder, Fields startField,
            Fields untilField, boolean withTimezone) throws IOException {
        getExtendStringRepUntilField(chrononTime, timezone, sbder, startField, untilField, withTimezone, 'T');
    }

    /**
     * Get the extended string representation of the given UTC chronon time under the given time zone. Only fields
     * before
     * the given field index will be returned.
     * <p/>
     * The extended string representation is like:<br/>
     * [-]YYYY-MM-DDThh:mm:ss.xxx[Z|[+|-]hh:mm]
     *
     * @param chrononTime
     * @param timezone
     * @param sbder
     * @param untilField
     */
    public void getExtendStringRepUntilField(long chrononTime, int timezone, Appendable sbder, Fields startField,
            Fields untilField, boolean withTimezone, char dateTimeSeparator) throws IOException {

        int year = getYear(chrononTime);
        int month = getMonthOfYear(chrononTime, year);

        switch (startField) {
            case YEAR:
            default:
                sbder.append(String.format(year < 0 ? "%05d" : "%04d", year));
                if (untilField == Fields.YEAR) {
                    return;
                }
            case MONTH:
                if (startField != Fields.MONTH) {
                    sbder.append('-');
                }
                sbder.append(String.format("%02d", month));
                if (untilField == Fields.MONTH) {
                    return;
                }
            case DAY:
                if (startField != Fields.DAY) {
                    sbder.append('-');
                }
                sbder.append(String.format("%02d", getDayOfMonthYear(chrononTime, year, month)));
                if (untilField == Fields.DAY) {
                    break;
                }
            case HOUR:
                if (startField != Fields.HOUR) {
                    sbder.append(dateTimeSeparator);
                }
                sbder.append(String.format("%02d", getHourOfDay(chrononTime)));
                if (untilField == Fields.HOUR) {
                    break;
                }
            case MINUTE:
                if (startField != Fields.MINUTE) {
                    sbder.append(':');
                }
                sbder.append(String.format("%02d", getMinOfHour(chrononTime)));
                if (untilField == Fields.MINUTE) {
                    break;
                }
            case SECOND:
                if (startField != Fields.SECOND) {
                    sbder.append(':');
                }
                sbder.append(String.format("%02d", getSecOfMin(chrononTime)));
                if (untilField == Fields.SECOND) {
                    break;
                }
            case MILLISECOND:
                if (startField != Fields.MILLISECOND) {
                    sbder.append('.');
                }
                // add millisecond as the precision fields of a second
                sbder.append(String.format("%03d", getMillisOfSec(chrononTime)));
                break;
        }

        if (withTimezone) {
            if (timezone == 0) {
                sbder.append('Z');
            } else {
                int tzMin = (int) ((timezone % CHRONON_OF_HOUR) / CHRONON_OF_MINUTE);
                int tzHr = (int) (timezone / CHRONON_OF_HOUR);
                sbder.append(tzHr >= 0 ? '-' : '+').append(String.format("%02d", tzHr < 0 ? -tzHr : tzHr)).append(':')
                        .append(String.format("%02d", tzMin < 0 ? -tzMin : tzMin));
            }
        }
    }

    /**
     * Get the basic string representation of a chronon time with the given time zone.
     *
     * @param chrononTime
     * @param timezone
     * @param sbder
     */
    public void getBasicStringRepUntilField(long chrononTime, int timezone, Appendable sbder, Fields startField,
            Fields untilField, boolean withTimezone) throws IOException {
        int year = getYear(chrononTime);
        int month = getMonthOfYear(chrononTime, year);

        switch (startField) {
            case YEAR:
            default:
                sbder.append(String.format(year < 0 ? "%05d" : "%04d", year));
                if (untilField == Fields.YEAR) {
                    return;
                }
            case MONTH:
                sbder.append(String.format("%02d", month));
                if (untilField == Fields.MONTH) {
                    return;
                }
            case DAY:
                sbder.append(String.format("%02d", getDayOfMonthYear(chrononTime, year, month)));
                if (untilField == Fields.DAY) {
                    break;
                }
            case HOUR:
                sbder.append(String.format("%02d", getHourOfDay(chrononTime)));
                if (untilField == Fields.HOUR) {
                    break;
                }
            case MINUTE:
                sbder.append(String.format("%02d", getMinOfHour(chrononTime)));
                if (untilField == Fields.MINUTE) {
                    break;
                }
            case SECOND:
                sbder.append(String.format("%02d", getSecOfMin(chrononTime)));
                // add millisecond as the precision fields of a second
                sbder.append(String.format("%03d", getMillisOfSec(chrononTime)));
                break;
        }

        if (withTimezone) {
            if (timezone == 0) {
                sbder.append('Z');
            } else {
                int tzMin = (int) (timezone % CHRONON_OF_HOUR / CHRONON_OF_MINUTE);
                if (tzMin < 0) {
                    tzMin = (short) (-1 * tzMin);
                }
                int tzHr = (int) (timezone / CHRONON_OF_HOUR);
                sbder.append((tzHr >= 0 ? '-' : '+')).append(String.format("%02d", (tzHr < 0 ? -tzHr : tzHr)))
                        .append(String.format("%02d", tzMin));
            }
        }
    }

    /**
     * Get the extended string representation of the given months and milliseconds (duration) time. *
     * <p/>
     * The extended and simple string representation is like:<br/>
     * [-]PnYnMnDTnHnMnS
     *
     * @param milliseconds
     * @param months
     * @param sbder
     */

    public void getDurationExtendStringRepWithTimezoneUntilField(long milliseconds, int months, StringBuilder sbder) {

        boolean positive = true;

        // set the negative flag. "||" is necessary in case that months field is not there (so it is 0)
        if (months < 0 || milliseconds < 0) {
            months *= -1;
            milliseconds *= -1;
            positive = false;
        }

        int month = getDurationMonth(months);
        int year = getDurationYear(months);
        int millisecond = getDurationMillisecond(milliseconds);
        int second = getDurationSecond(milliseconds);
        int minute = getDurationMinute(milliseconds);
        int hour = getDurationHour(milliseconds);
        int day = getDurationDay(milliseconds);

        if (!positive) {
            sbder.append('-');
        }
        sbder.append('P');
        if (year != 0) {
            sbder.append(year).append('Y');
        }
        if (month != 0) {
            sbder.append(month).append('M');
        }
        if (day != 0) {
            sbder.append(day).append('D');
        }
        if (hour != 0 || minute != 0 || second != 0 || millisecond != 0) {
            sbder.append('T');
        }
        if (hour != 0) {
            sbder.append(hour).append('H');
        }
        if (minute != 0) {
            sbder.append(minute).append('M');
        }
        if (second != 0 || millisecond != 0) {
            sbder.append(second);
        }
        if (millisecond > 0) {
            sbder.append('.').append(millisecond);
        }
        if (second != 0 || millisecond != 0) {
            sbder.append('S');
        }
    }

    /**
     * Check whether a given year is a leap year.
     *
     * @param year
     * @return
     */
    public boolean isLeapYear(int year) {
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
     * The original authers are Stephen Colebourne, Brain S O'Neill and Guy Allard, and modified by JArod Wen on May 7th, 2012 and commented by Theodoros Ioannou on July 2012.
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

        int leap = 0;

        if (isLeapYear(year) == true) {
            leap = 1; //Adding one day for the leap years
        }

        if (i < (181 + leap) * 84375) { /*Days before the end of June*/
            if (i < (90 + leap) * 84375) { /*Days before the end of March*/
                if (i < 31 * 84375) { /*Days before the end of January*/
                    return 1;
                } else if (i < (59 + leap) * 84375) { /*Days before the end of February*/
                    return 2;
                } else {
                    return 3;
                }
            } else if (i < (120 + leap) * 84375) { /*Days before the end of April*/
                return 4;
            } else if (i < (151 + leap) * 84375) { /*Days before the end of May*/
                return 5;
            } else {
                return 6;
            }
        } else if (i < (273 + leap) * 84375) { /*Days before the end of September*/
            if (i < (212 + leap) * 84375) { /*Days before the end of July*/
                return 7;
            } else if (i < (243 + leap) * 84375) { /*Days before the end of August*/
                return 8;
            } else {
                return 9;
            }
        } else if (i < (304 + leap) * 84375) { /*Days before the end of October*/
            return 10;
        } else if (i < (334 + leap) * 84375) { /*Days before the end of November*/
            return 11;
        } else {
            return 12;
        }
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

    /**
     * Get the day number in the year for the input chronon time.
     * @param millis
     * @param year
     * @return
     */
    public int getDayOfYear(long millis, int year) {
        long dateMillis = chrononizeBeginningOfYear(year);
        return (int) ((millis - dateMillis) / CHRONON_OF_DAY) + 1;
    }

    /**
     * Get the week number in the year for the input chronon time.
     * @param millis
     * @param year
     * @return
     */
    public int getWeekOfYear(long millis, int year) {
        int doy = getDayOfYear(millis, year);
        int week = doy / DAYS_IN_A_WEEK;
        if (doy % DAYS_IN_A_WEEK > 0) {
            week++;
        }
        return week;
    }

    /**
     * Get the hour of the day for the given chronon time.
     *
     * @param millis
     * @return
     */
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

    /**
     * Get the minute of the hour for the given chronon time.
     *
     * @param millis
     * @return
     */
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

    /**
     * Get the second of the minute for the given chronon time.
     *
     * @param millis
     * @return
     */
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

    /**
     * Get the millisecond of the second for the given chronon time.
     *
     * @param millis
     * @return
     */
    public int getMillisOfSec(long millis) {
        int ms = (int) (millis % CHRONON_OF_SECOND);
        if (millis < 0 && ms < 0) {
            ms += 1000;
        }
        return ms;
    }

    /**
     * Get the day of week for the given chronon time. 0 (Sunday) to 7 (Saturday)
     *
     * @param millis
     * @return
     */
    public int getDayOfWeek(long millis) {
        long daysSinceAnchor = getChrononInDays(millis);

        // compute the weekday (0-based, and 0 = Sunday). Adjustment is needed as the anchor day is Thursday.
        int weekday = (int) ((daysSinceAnchor + ANCHOR_WEEKDAY) % DAYS_IN_A_WEEK);

        // handle the negative weekday
        if (weekday < 0) {
            weekday += DAYS_IN_A_WEEK;
        }

        return weekday;
    }

    public int getDurationMonth(int months) {
        return (months % 12);
    }

    public int getDurationYear(int months) {
        return (months / 12);
    }

    public int getDurationMillisecond(long milliseconds) {
        return (int) (milliseconds % 1000);
    }

    public int getDurationSecond(long milliseconds) {
        return (int) ((milliseconds % 60000) / 1000);
    }

    public int getDurationMinute(long milliseconds) {
        return (int) ((milliseconds % 3600000) / 60000);
    }

    public int getDurationHour(long milliseconds) {
        return (int) ((milliseconds % (86400000)) / 3600000);
    }

    public int getDurationDay(long milliseconds) {
        return (int) (milliseconds / (86400000));
    }
}
