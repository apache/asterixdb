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

/**
 * A simple implementation of the Gregorian calendar system.
 * <p/>
 */
public class GregorianCalendarSystem implements ICalendarSystem {

    public static final int YEAR = 0, MONTH = 1, DAY = 2, HOUR = 3, MINUTE = 4, SECOND = 5, MILLISECOND = 6;

    public static final int[] DAYS_OF_MONTH_ORDI = { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    public static final int[] DAYS_OF_MONTH_LEAP = { 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

    public static final int[] DAYS_SINCE_MONTH_BEGIN_ORDI = { 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334 };

    public static final int CHRONON_OF_SECOND = 1000;
    public static final int CHRONON_OF_MINUTE = 60 * CHRONON_OF_SECOND;
    public static final int CHRONON_OF_HOUR = 60 * CHRONON_OF_MINUTE;
    public static final long CHRONON_OF_DAY = 24 * CHRONON_OF_HOUR;

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
        int ora = (hour - timezone / 4) * CHRONON_OF_HOUR + (min - (timezone % 4) * 15) * CHRONON_OF_MINUTE + sec
                * CHRONON_OF_SECOND + millis;
        return ora;
    }

    /**
     * Get the extended string representation of the given UTC chronon time under the given time zone. Only fields before
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
    public void getExtendStringRepWithTimezoneUntilField(long chrononTime, int timezone, StringBuilder sbder,
            int startField, int untilField) {

        int year = getYear(chrononTime);
        int month = getMonthOfYear(chrononTime, year);

        switch (startField) {
            case YEAR:
                sbder.append(String.format(year < 0 ? "%05d" : "%04d", year));
                if (untilField == YEAR) {
                    return;
                }
            case MONTH:
                if (startField != MONTH)
                    sbder.append("-");
                sbder.append(String.format("%02d", month));
                if (untilField == MONTH) {
                    return;
                }
            case DAY:
                if (startField != DAY)
                    sbder.append("-");
                sbder.append(String.format("%02d", getDayOfMonthYear(chrononTime, year, month)));
                if (untilField == DAY) {
                    break;
                }
            case HOUR:
                if (startField != HOUR)
                    sbder.append("T");
                sbder.append(String.format("%02d", getHourOfDay(chrononTime)));
                if (untilField == HOUR) {
                    break;
                }
            case MINUTE:
                if (startField != MINUTE)
                    sbder.append(":");
                sbder.append(String.format("%02d", getMinOfHour(chrononTime)));
                if (untilField == MINUTE) {
                    break;
                }
            case SECOND:
                if (startField != SECOND)
                    sbder.append(":");
                sbder.append(String.format("%02d", getSecOfMin(chrononTime)));
                // add millisecond as the precision fields of a second
                sbder.append(".").append(String.format("%03d", getMillisOfSec(chrononTime)));
                break;
        }

        if (timezone == 0) {
            sbder.append("Z");
        } else {
            short tzMin = (short) ((timezone % 4) * 15);
            short tzHr = (short) (timezone / 4);
            sbder.append((tzHr >= 0 ? "+" : "-")).append(String.format("%02d", (tzHr < 0 ? -tzHr : tzHr))).append(":")
                    .append(String.format("%02d", tzMin));
        }
    }

    /**
     * Get the basic string representation of a chronon time with the given time zone.
     * 
     * @param chrononTime
     * @param timezone
     * @param sbder
     */
    public void getBasicStringRepWithTimezoneUntiField(long chrononTime, int timezone, StringBuilder sbder,
            int startField, int untilField) {
        int year = getYear(chrononTime);
        int month = getMonthOfYear(chrononTime, year);

        switch (startField) {
            case YEAR:
                sbder.append(String.format(year < 0 ? "%05d" : "%04d", year));
                if (untilField == YEAR) {
                    return;
                }
            case MONTH:
                sbder.append(String.format("%02d", month));
                if (untilField == MONTH) {
                    return;
                }
            case DAY:
                sbder.append(String.format("%02d", getDayOfMonthYear(chrononTime, year, month)));
                if (untilField == DAY) {
                    break;
                }
            case HOUR:
                sbder.append(String.format("%02d", getHourOfDay(chrononTime)));
                if (untilField == HOUR) {
                    break;
                }
            case MINUTE:
                sbder.append(String.format("%02d", getMinOfHour(chrononTime)));
                if (untilField == MINUTE) {
                    break;
                }
            case SECOND:
                sbder.append(String.format("%02d", getSecOfMin(chrononTime)));
                // add millisecond as the precision fields of a second
                sbder.append(String.format("%03d", getMillisOfSec(chrononTime)));
                break;
        }

        if (timezone == 0) {
            sbder.append("Z");
        } else {
            short tzMin = (short) ((timezone % 4) * 15);
            short tzHr = (short) (timezone / 4);
            sbder.append((tzHr >= 0 ? "+" : "-")).append(String.format("%02d", (tzHr < 0 ? -tzHr : tzHr)))
                    .append(String.format("%02d", tzMin));
        }
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
}
