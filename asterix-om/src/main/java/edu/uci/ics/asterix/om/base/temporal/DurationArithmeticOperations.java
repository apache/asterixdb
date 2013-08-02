/*
 * Copyright 2009-2013 by The Regents of the University of California
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

/**
 * Algorithms for duration related arithmetic operations.
 */
public class DurationArithmeticOperations {

    private final static GregorianCalendarSystem calSystem = GregorianCalendarSystem.getInstance();

    /**
     * Add a duration (with yearMonth and dayTime) onto a time point. The algorithm works as described in
     * <a
     * href="http://www.w3.org/TR/xmlschema-2/#adding-durations-to-dateTimes">"XML: adding durations to dateTimes"</a>.
     * <p/>
     * The basic algorithm is like this: duration is applied to the time point as two separated fields: year-month field and day-time field. Year-month field is applied firstly by reserving the correct day within the month's range (for example add 1M to 03-31 will return 04-30). Then day-time field is applied.
     * <p/>
     * 
     * @param pointChronon
     *            The time instance where the duration will be added, represented as the milliseconds since the anchored time (00:00:00 for time type, 1970-01-01T00:00:00Z for datetime and date types).
     * @param yearMonthDuration
     *            The year-month-duration to be added
     * @param dayTimeDuration
     *            The day-time-duration to be added
     * @return
     */
    public static long addDuration(long pointChronon, int yearMonthDuration, long dayTimeDuration, boolean isTimeOnly) {

        if (isTimeOnly) {
            int rtnChronon = (int) ((pointChronon + dayTimeDuration) % GregorianCalendarSystem.CHRONON_OF_DAY);
            if (rtnChronon < 0) {
                rtnChronon += GregorianCalendarSystem.CHRONON_OF_DAY;
            }

            return rtnChronon;
        }

        int year = calSystem.getYear(pointChronon);
        int month = calSystem.getMonthOfYear(pointChronon, year);
        int day = calSystem.getDayOfMonthYear(pointChronon, year, month);
        int hour = calSystem.getHourOfDay(pointChronon);
        int min = calSystem.getMinOfHour(pointChronon);
        int sec = calSystem.getSecOfMin(pointChronon);
        int ms = calSystem.getMillisOfSec(pointChronon);

        // Apply the year-month duration
        int carry = yearMonthDuration / 12;
        month += (yearMonthDuration % 12);

        if (month < 0) {
            month += 12;
            carry -= 1;
        } else if (month > 12) {
            month -= 12;
            carry += 1;
        }

        year += carry;

        boolean isLeapYear = calSystem.isLeapYear(year);

        if (isLeapYear) {
            if (day > GregorianCalendarSystem.DAYS_OF_MONTH_ORDI[month - 1]) {
                day = GregorianCalendarSystem.DAYS_OF_MONTH_ORDI[month - 1];
            }
        } else {
            if (day > GregorianCalendarSystem.DAYS_OF_MONTH_LEAP[month - 1]) {
                day = GregorianCalendarSystem.DAYS_OF_MONTH_LEAP[month - 1];
            }
        }

        return calSystem.getChronon(year, month, day, hour, min, sec, ms, 0) + dayTimeDuration;
    }

}
